#!/usr/bin/env python3
"""pubsub-log-forwarder v03-dataway - Cloud Logging -> Pub/Sub push -> Cloud Run -> TrueWatch DataWay.

Same input contract and record mapping as v02 (the DataKit edition in the repository root), plus
httpRequest -> http_* fields and resource.labels -> resource_* tags. What changes is the destination:

  v02  http://<private IPv4>:9529/v1/write/logging   DataKit JSON API, one POST per Pub/Sub message
  v03  https://<DataWay host>/v1/write/logging?token= line protocol, many messages per POST

Route (POST only; Cloud Run IAM authentication REQUIRED - deploy with --no-allow-unauthenticated):
  /pubsub   one wrapped Pub/Sub push message whose data is a base64 Cloud Logging LogEntry.
            jsonPayload (object) and textPayload (string) are supported. protoPayload is NOT.
  anything else -> 404.

Batching (why v03 is not one POST per message):
  Each push request maps its LogEntry to one line-protocol line and puts it on an in-process queue,
  then WAITS. SENDERS sender threads each take up to BATCH_MAX_RECORDS lines / BATCH_MAX_BYTES bytes,
  or whatever arrived within BATCH_MAX_WAIT_MS, and POST them in one request over a kept-alive HTTPS
  connection. Every request in the batch is answered only after that POST's outcome is known:
    DataWay 2xx                 -> 204 for every message in the batch (Pub/Sub ACK)
    non-2xx / timeout / error   -> 502 for every message in the batch (Pub/Sub redelivers)
    queue full                  -> 503 immediately, nothing queued (Pub/Sub backs off and redelivers)
  So an ACK still always means "DataWay accepted the write that contained this record". Batching only
  pays off when Cloud Run sends many concurrent requests to one instance: deploy with --concurrency
  well above 1 (for example 250).

Mapping (one LogEntry -> one line-protocol point):
  measurement          MEASUREMENT (default "gcp_pubsub"), i.e. the log source in TrueWatch
  timestamp            LogEntry.timestamp as integer Unix NANOSECONDS (no float arithmetic)
  tag project          project id from LogEntry.logName; must equal EXPECTED_PROJECT_ID
  tag resource_type    LogEntry.resource.type, "unknown" if absent
  tag severity         LogEntry.severity (default "DEFAULT")
  tag log_name         LogEntry.logName log id (text after /logs/), URL-decoded
  tag google_insert_id LogEntry.insertId
  tag resource_<key>   LogEntry.resource.labels string values; built-in tags are never overwritten
  tag <EXTRA_TAGS>     optional static tags, e.g. host=..., the role DataKit global tags used to play
  field message        textPayload, or jsonPayload as one compact sorted JSON string
  field status         severity mapped to TrueWatch log status
  field log_timestamp  original RFC 3339 timestamp string
  field receive_timestamp  LogEntry.receiveTimestamp, when present and valid
  field http_*         LogEntry.httpRequest (see HTTP_REQUEST_FIELDS) plus http_latency_ms

Line-protocol differences from the v02 DataKit JSON records:
  - integer fields (http_status, http_*_size, ...) are written as integers (v02 arrived as floats)
  - a tag whose value is empty is omitted (line protocol has no empty tag values)
  - "\\" inside a tag value is written as "%5C"
  - CR and LF inside a string field are written as the two characters "\\r" / "\\n"

Credential: DATAWAY_TOKEN must be a TrueWatch Client Token, sent with to_headless=true. A Client Token
can write logs and RUM but cannot query or write other data types, so a leaked token cannot read the
workspace. A workspace tkn_ token is refused at start. Give it to Cloud Run from Secret Manager
(--set-secrets DATAWAY_TOKEN=<secret>:latest), never in env.yaml. It is sent only as the token query
parameter of the DataWay URL and never appears in a receipt, error or start line.
"""
import base64
import binascii
import calendar
import gzip
import http.client
import json
import math
import os
import queue
import re
import socket
import ssl
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = "pubsub-log-forwarder/v03-dataway"
DATAWAY_PATH = "/v1/write/logging"
DEFAULT_MEASUREMENT = "gcp_pubsub"

MAX_PUSH_BYTES = 1_048_576        # bound the inbound push body (Pub/Sub messages are <= 10 MiB)
MAX_RECORD_BYTES = 524_288        # bound one encoded line-protocol line
DATAWAY_TIMEOUT = 15              # seconds, outbound connect+read for one batch POST
MAX_RESPONSE_READ = 4096          # drained and discarded; never logged
DRAIN_CHUNK = 65_536              # bounded read used to answer an oversize push cleanly
DRAIN_CAP = 11_534_336            # 11 MiB: above Pub/Sub's 10 MiB message limit
READ_CHUNK = 65_536               # inbound body read size
INBOUND_IDLE_TIMEOUT = 10         # seconds a single socket read may wait (also bounds header reads)
INBOUND_BODY_DEADLINE = 30        # seconds to receive a whole accepted body
DRAIN_DEADLINE = 5                # seconds spent draining an oversize body before answering 413
IDLE_REUSE_SECONDS = 30           # a kept-alive connection idle longer than this is reopened
CONTENT_LENGTH_RE = re.compile(r"^[0-9]{1,10}$")

# Batching defaults; each can be overridden by the environment variable of the same name.
BATCH_DEFAULTS = {"BATCH_MAX_RECORDS": (500, 1, 10_000),
                  "BATCH_MAX_BYTES": (2_097_152, 65_536, 8_388_608),
                  "BATCH_MAX_WAIT_MS": (200, 0, 5_000),
                  "SENDERS": (4, 1, 64),
                  "QUEUE_MAX": (5_000, 1, 100_000)}

MEASUREMENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
PROJECT_RE = re.compile(r"^[a-z][a-z0-9\-]{4,28}[a-z0-9]$")
SUBSCRIPTION_RE = re.compile(r"^projects/([a-z][a-z0-9\-]{4,28}[a-z0-9])/subscriptions/([A-Za-z][A-Za-z0-9._~%+\-]{2,254})$")
LOG_NAME_RE = re.compile(r"^projects/([a-z][a-z0-9\-]{4,28}[a-z0-9])/logs/([A-Za-z0-9._%/\-]{1,512})$")
INSERT_ID_RE = re.compile(r"^[\x21-\x7e]{1,256}$")
RESOURCE_TYPE_RE = re.compile(r"^[a-z][a-z0-9_.\-]{0,62}$")
RFC3339_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:([Zz])|([+-])(\d{2}):(\d{2}))$")
HOSTNAME_RE = re.compile(r"^(?=.{1,253}$)([a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$")
CLIENT_TOKEN_RE = re.compile(r"^[A-Za-z0-9_\-]{16,128}$")    # Client Token (sent with to_headless=true)
EXTRA_TAG_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
EXTRA_TAG_VALUE_RE = re.compile(r"^[A-Za-z0-9._:/@+\-]{1,256}$")

LABEL_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
MAX_LABEL_VALUE = 1024
LATENCY_RE = re.compile(r"^(\d{1,9})(?:\.(\d{1,9}))?s$")

# LogEntry.httpRequest (HttpRequest message) -> field name. Request/response sizes are int64 and
# arrive as JSON strings; status is an int; latency is a protobuf Duration string such as "0.0123s".
HTTP_REQUEST_FIELDS = {"requestMethod": "http_method", "requestUrl": "http_url",
                       "requestSize": "http_request_size", "status": "http_status",
                       "responseSize": "http_response_size", "userAgent": "http_user_agent",
                       "remoteIp": "http_remote_ip", "serverIp": "http_server_ip",
                       "referer": "http_referer", "latency": "http_latency",
                       "protocol": "http_protocol", "cacheLookup": "http_cache_lookup",
                       "cacheHit": "http_cache_hit",
                       "cacheValidatedWithOriginServer": "http_cache_validated",
                       "cacheFillBytes": "http_cache_fill_bytes"}
HTTP_INT_FIELDS = ("requestSize", "status", "responseSize", "cacheFillBytes")
HTTP_BOOL_FIELDS = ("cacheLookup", "cacheHit", "cacheValidatedWithOriginServer")

SEVERITY_STATUS = {"DEFAULT": "info", "DEBUG": "debug", "INFO": "info", "NOTICE": "notice",
                   "WARNING": "warning", "ERROR": "error", "CRITICAL": "critical",
                   "ALERT": "alert", "EMERGENCY": "emerg"}

# The DataWay token is the only credential this service may hold, and only under its own name.
# Any other credential-shaped variable still refuses start, so a stray secret is noticed.
FORBIDDEN_ENV_SUBSTRINGS = ("DATAWAY", "SECRET", "TOKEN", "CREDENTIAL", "PASSWORD")
ALLOWED_ENV_EXCEPTIONS = ("DATAWAY_URL", "DATAWAY_TOKEN", "DATAWAY_GZIP")


class Reject(Exception):
    """Invalid or foreign input -> 4xx. Never forwarded, never acknowledged as success."""

    def __init__(self, status, reason):
        super().__init__(reason)
        self.status, self.reason = status, reason


class DestFail(Exception):
    """DataWay did not accept the write -> 5xx so Pub/Sub retries. Message is a fixed category."""

    def __init__(self, reason, status=502):
        super().__init__(reason)
        self.status = status


class ConfigFail(Exception):
    """Misconfiguration -> refuse to start (exit 78)."""


def emit(obj):
    """One JSON line to stdout. Only categories, counts and identifiers already present in config."""
    sys.stdout.write(json.dumps(obj, separators=(",", ":"), sort_keys=True) + "\n")
    sys.stdout.flush()


# ------------------------------------------------------------------ configuration
def validate_destination(url):
    """Return (host, port, path) for https://<DNS name>[:port][/v1/write/logging], nothing else."""
    if not isinstance(url, str) or not url or any(c.isspace() for c in url):
        raise ConfigFail("DATAWAY_URL missing or contains whitespace")
    try:
        u = urllib.parse.urlsplit(url)
        port = u.port
    except ValueError:
        raise ConfigFail("DATAWAY_URL is not a URL")
    if u.scheme != "https":
        raise ConfigFail("DATAWAY_URL must be https://")
    if u.username is not None or u.password is not None or "@" in u.netloc:
        raise ConfigFail("DATAWAY_URL must not contain credentials")
    if u.query or u.fragment or "?" in url or "#" in url:
        raise ConfigFail("DATAWAY_URL must not contain a query string; the token goes in DATAWAY_TOKEN")
    if u.path not in ("", "/", DATAWAY_PATH):
        raise ConfigFail("DATAWAY_URL path must be empty or exactly " + DATAWAY_PATH)
    host = (u.hostname or "").lower()
    if not HOSTNAME_RE.match(host):
        raise ConfigFail("DATAWAY_URL host must be a DNS name")
    return host, port or 443, DATAWAY_PATH


def parse_extra_tags(raw):
    """EXTRA_TAGS="k=v,k2=v2" -> dict. Keys and values are restricted so no escaping is needed."""
    tags = {}
    if not raw:
        return tags
    for item in raw.split(","):
        key, sep, value = item.partition("=")
        if not sep or not EXTRA_TAG_KEY_RE.match(key) or not EXTRA_TAG_VALUE_RE.match(value):
            raise ConfigFail("EXTRA_TAGS must be k=v pairs separated by commas")
        if key in tags:
            raise ConfigFail("EXTRA_TAGS repeats a key")
        tags[key] = value
    return tags


def _int_env(env, name):
    default, low, high = BATCH_DEFAULTS[name]
    raw = env.get(name)
    if raw in (None, ""):
        return default
    if not re.match(r"^[0-9]{1,9}$", raw) or not low <= int(raw) <= high:
        raise ConfigFail("%s must be an integer from %d to %d" % (name, low, high))
    return int(raw)


def cfg_from_env(env=None):
    env = os.environ if env is None else env
    if "DATAWAY_TOKEN_TYPE" in env:
        raise ConfigFail("DATAWAY_TOKEN_TYPE is not supported: only a Client Token is accepted")
    offending = sorted(k for k in env
                       if k not in ALLOWED_ENV_EXCEPTIONS
                       and any(s in k.upper() for s in FORBIDDEN_ENV_SUBSTRINGS))
    if offending:
        raise ConfigFail("credential-shaped environment variable present: " + ",".join(offending))

    project = env.get("EXPECTED_PROJECT_ID", "")
    if not PROJECT_RE.match(project or ""):
        raise ConfigFail("EXPECTED_PROJECT_ID missing or not a Google project id")

    sub = env.get("EXPECTED_SUBSCRIPTION", "")
    if not SUBSCRIPTION_RE.match(sub or ""):
        raise ConfigFail("EXPECTED_SUBSCRIPTION must be a full projects/<id>/subscriptions/<name> name")

    measurement = env.get("MEASUREMENT") or DEFAULT_MEASUREMENT
    if not MEASUREMENT_RE.match(measurement):
        raise ConfigFail("MEASUREMENT must match [A-Za-z][A-Za-z0-9_]{0,63}")

    token = env.get("DATAWAY_TOKEN", "")
    if token.startswith("tkn_"):
        raise ConfigFail("DATAWAY_TOKEN is a workspace token; only a Client Token is accepted")
    if not CLIENT_TOKEN_RE.match(token):
        raise ConfigFail("DATAWAY_TOKEN missing or not a Client Token")

    gz = (env.get("DATAWAY_GZIP") or "false").lower()
    if gz not in ("true", "false"):
        raise ConfigFail("DATAWAY_GZIP must be true or false")

    host, port, path = validate_destination(env.get("DATAWAY_URL"))
    cfg = {"project": project, "subscription": sub, "measurement": measurement,
           "dest_scheme": "https", "dest_host": host, "dest_port": port, "dest_path": path,
           "token": token, "gzip": gz == "true", "extra_tags": parse_extra_tags(env.get("EXTRA_TAGS"))}
    for name in BATCH_DEFAULTS:
        cfg[name.lower()] = _int_env(env, name)
    return cfg


def public_cfg(cfg):
    """The configuration as it may be logged: everything except the token."""
    return {k: v for k, v in cfg.items() if k != "token"}


# ------------------------------------------------------------------ input validation
def _no_nonfinite(_const):
    raise Reject(400, "payload contains a non-finite JSON value")


def parse_push(body, cfg):
    """Validate the wrapped Pub/Sub push envelope and its origin subscription."""
    if len(body) > MAX_PUSH_BYTES:
        raise Reject(413, "push body too large")
    try:
        env = json.loads(body.decode("utf-8"), parse_constant=_no_nonfinite)
    except Reject:
        raise
    except (UnicodeDecodeError, ValueError):
        raise Reject(400, "push body is not JSON")
    if not isinstance(env, dict) or not isinstance(env.get("message"), dict):
        raise Reject(400, "not a Pub/Sub push envelope")
    if env.get("subscription") != cfg["subscription"]:
        raise Reject(403, "unexpected subscription origin")
    return env["message"]


def decode_logentry(message):
    """base64 message.data -> one Cloud Logging LogEntry object."""
    data = message.get("data")
    if not isinstance(data, str) or not data:
        raise Reject(400, "message data missing")
    try:
        raw = base64.b64decode(data, validate=True)
    except (binascii.Error, ValueError):
        raise Reject(400, "message data is not valid base64")
    if len(raw) > MAX_PUSH_BYTES:
        raise Reject(413, "decoded LogEntry too large")
    try:
        entry = json.loads(raw.decode("utf-8"), parse_constant=_no_nonfinite)
    except Reject:
        raise
    except (UnicodeDecodeError, ValueError):
        raise Reject(400, "decoded message data is not JSON")
    if not isinstance(entry, dict):
        raise Reject(400, "decoded LogEntry is not an object")
    return entry


def rfc3339_ns(s, what):
    """RFC 3339 -> integer Unix nanoseconds, exactly. No float arithmetic anywhere."""
    m = RFC3339_RE.match(s) if isinstance(s, str) else None
    if not m:
        raise Reject(400, what + " is not an RFC 3339 timestamp")
    y, mo, d, h, mi, sec = (int(m.group(i)) for i in range(1, 7))
    frac = (m.group(7) or "").ljust(9, "0")
    if not (1 <= mo <= 12 and 1 <= d <= calendar.monthrange(y, mo)[1] and h <= 23 and mi <= 59 and sec <= 59):
        raise Reject(400, what + " has an invalid calendar value")
    off = 0
    if m.group(9):
        oh, om = int(m.group(10)), int(m.group(11))
        if oh > 23 or om > 59:
            raise Reject(400, what + " has an invalid UTC offset")
        off = (oh * 3600 + om * 60) * (1 if m.group(9) == "+" else -1)
    ns = (calendar.timegm((y, mo, d, h, mi, sec, 0, 0, 0)) - off) * 1_000_000_000 + int(frac)
    if not 0 <= ns < 1 << 63:
        raise Reject(400, what + " is outside the representable range")
    return ns


def _finite(obj):
    """Reject NaN/Infinity that survived as Python floats inside a decoded payload."""
    if isinstance(obj, float):
        if not math.isfinite(obj):
            raise Reject(400, "payload contains a non-finite number")
    elif isinstance(obj, dict):
        for v in obj.values():
            _finite(v)
    elif isinstance(obj, list):
        for v in obj:
            _finite(v)


def resource_label_tags(resource):
    """resource.labels -> tags named resource_<key>. Keys outside LABEL_KEY_RE are skipped."""
    labels = resource.get("labels") if isinstance(resource, dict) else None
    if labels is None:
        return {}
    if not isinstance(labels, dict):
        raise Reject(400, "LogEntry resource.labels is malformed")
    tags = {}
    for key, value in labels.items():
        if isinstance(value, str) and LABEL_KEY_RE.match(key) and len(value) <= MAX_LABEL_VALUE:
            tags["resource_" + key] = value
    return tags


def http_request_fields(http_request):
    """LogEntry.httpRequest -> http_* fields, plus http_latency_ms parsed from the Duration string."""
    if http_request is None:
        return {}
    if not isinstance(http_request, dict):
        raise Reject(400, "LogEntry httpRequest is malformed")
    fields = {}
    for key, name in HTTP_REQUEST_FIELDS.items():
        value = http_request.get(key)
        if value is None:
            continue
        if key in HTTP_INT_FIELDS:
            if isinstance(value, bool) or not isinstance(value, (int, str)) \
                    or not re.match(r"^[0-9]{1,19}$", str(value)):
                raise Reject(400, "LogEntry httpRequest.%s is not an integer" % key)
            fields[name] = int(value)
        elif key in HTTP_BOOL_FIELDS:
            if not isinstance(value, bool):
                raise Reject(400, "LogEntry httpRequest.%s is not a boolean" % key)
            fields[name] = value
        elif isinstance(value, str):
            fields[name] = value
        else:
            raise Reject(400, "LogEntry httpRequest.%s is not a string" % key)
    if "http_latency" in fields:
        m = LATENCY_RE.match(fields["http_latency"])
        if not m:
            raise Reject(400, "LogEntry httpRequest.latency is not a Duration")
        nanos = int(m.group(1)) * 1_000_000_000 + int((m.group(2) or "").ljust(9, "0"))
        fields["http_latency_ms"] = nanos / 1_000_000
    return fields


def record_from_logentry(cfg, entry):
    """Validate one ordinary LogEntry and map it to one record (encoded by to_line)."""
    insert_id = entry.get("insertId")
    if not isinstance(insert_id, str) or not INSERT_ID_RE.match(insert_id):
        raise Reject(400, "LogEntry insertId missing or invalid")

    m = LOG_NAME_RE.match(entry.get("logName") or "") if isinstance(entry.get("logName"), str) else None
    if not m:
        raise Reject(400, "LogEntry logName missing or invalid")
    if m.group(1) != cfg["project"]:
        raise Reject(403, "LogEntry belongs to an unexpected project")
    log_name = urllib.parse.unquote(m.group(2))

    payload_json, payload_text = entry.get("jsonPayload"), entry.get("textPayload")
    if isinstance(payload_json, dict):
        _finite(payload_json)
        message = json.dumps(payload_json, separators=(",", ":"), sort_keys=True, allow_nan=False)
    elif isinstance(payload_text, str):
        message = payload_text
    elif entry.get("protoPayload") is not None:
        raise Reject(400, "protoPayload is not supported by this adapter")
    else:
        raise Reject(400, "LogEntry has neither jsonPayload nor textPayload")

    severity = entry.get("severity", "DEFAULT")
    if severity not in SEVERITY_STATUS:
        raise Reject(400, "LogEntry severity is not a Cloud Logging LogSeverity")

    resource = entry.get("resource")
    if resource is None:
        resource_type = "unknown"
    elif isinstance(resource, dict) and isinstance(resource.get("type"), str) \
            and RESOURCE_TYPE_RE.match(resource["type"]):
        resource_type = resource["type"]
    elif isinstance(resource, dict) and resource.get("type") is None:
        resource_type = "unknown"
    else:
        raise Reject(400, "LogEntry resource is malformed")

    t_ns = rfc3339_ns(entry.get("timestamp"), "timestamp")

    tags = {"project": cfg["project"], "resource_type": resource_type, "severity": severity,
            "log_name": log_name, "google_insert_id": insert_id}
    fields = {"message": message, "status": SEVERITY_STATUS[severity],
              "log_timestamp": entry["timestamp"]}
    if isinstance(entry.get("receiveTimestamp"), str):
        rfc3339_ns(entry["receiveTimestamp"], "receiveTimestamp")
        fields["receive_timestamp"] = entry["receiveTimestamp"]
    for key, value in resource_label_tags(resource).items():
        tags.setdefault(key, value)
    fields.update(http_request_fields(entry.get("httpRequest")))

    # The size bound is enforced on the encoded line in to_line().
    return {"measurement": cfg["measurement"], "time": t_ns, "tags": tags, "fields": fields}


# ------------------------------------------------------------------ line protocol
def _esc_key(s):
    """Measurement, tag key, field key: escape comma, equals and space."""
    return s.replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


def _esc_tag_value(s):
    """Tag value: no CR/LF, backslash percent-encoded (it has no unambiguous escape), then escaped."""
    s = s.replace("\\", "%5C").replace("\r", "%0D").replace("\n", "%0A")
    return _esc_key(s)


def _esc_str_field(s):
    """String field: backslash and double quote escaped; CR/LF written as two-character escapes."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\r", "\\r").replace("\n", "\\n")


def _field_value(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return "%di" % v
    if isinstance(v, float):
        if not math.isfinite(v):
            raise Reject(400, "field is not a finite number")
        return repr(v)
    return '"%s"' % _esc_str_field(str(v))


def to_line(record, extra_tags=None):
    """One record (as built by record_from_logentry) -> one line-protocol line, UTF-8 bytes."""
    tags = dict(record["tags"])
    for key, value in (extra_tags or {}).items():
        tags.setdefault(key, value)
    tag_part = "".join(",%s=%s" % (_esc_key(k), _esc_tag_value(v))
                       for k, v in sorted(tags.items()) if isinstance(v, str) and v != "")
    field_part = ",".join("%s=%s" % (_esc_key(k), _field_value(v)) for k, v in sorted(record["fields"].items()))
    line = ("%s%s %s %d" % (_esc_key(record["measurement"]), tag_part, field_part, record["time"])).encode("utf-8")
    if len(line) > MAX_RECORD_BYTES:
        raise Reject(413, "encoded line exceeds the record size bound")
    return line


# ------------------------------------------------------------------ destination
def _connection(cfg):
    if cfg["dest_scheme"] == "https":
        return http.client.HTTPSConnection(cfg["dest_host"], cfg["dest_port"], timeout=DATAWAY_TIMEOUT,
                                           context=ssl.create_default_context())
    # Plain http is reachable only from tests that override dest_scheme; cfg_from_env never sets it.
    return http.client.HTTPConnection(cfg["dest_host"], cfg["dest_port"], timeout=DATAWAY_TIMEOUT)


class Sender:
    """One kept-alive connection to DataWay. Used by exactly one sender thread."""

    def __init__(self, cfg, connect=_connection):
        self.cfg, self.connect = cfg, connect
        self.conn, self.last_used = None, 0.0
        # A Client Token is refused (403 kodo.tokenScopeMismatch) without to_headless=true.
        self.target = "%s?%s" % (cfg["dest_path"], urllib.parse.urlencode({"token": cfg["token"], "to_headless": "true"}))

    def _close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:                 # noqa: BLE001 - closing must never mask the outcome
                pass
        self.conn = None

    def _once(self, body, headers):
        if self.conn is None or time.monotonic() - self.last_used > IDLE_REUSE_SECONDS:
            self._close()
            self.conn = self.connect(self.cfg)
        self.conn.request("POST", self.target, body=body, headers=headers)
        r = self.conn.getresponse()
        try:
            r.read(MAX_RESPONSE_READ)         # drained so the connection can be reused; never logged
        finally:
            r.close()
        self.last_used = time.monotonic()
        if r.will_close:
            self._close()
        return r.status

    def post(self, lines):
        """POST the lines as one line-protocol body. Returns the 2xx status or raises DestFail."""
        body = b"\n".join(lines)
        headers = {"Content-Type": "text/plain; charset=utf-8", "User-Agent": VERSION}
        if self.cfg["gzip"]:
            body = gzip.compress(body, compresslevel=5)
            headers["Content-Encoding"] = "gzip"
        headers["Content-Length"] = str(len(body))
        reused = self.conn is not None
        try:
            try:
                status = self._once(body, headers)
            except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError):
                # A kept-alive connection the server already closed: retry once on a fresh one.
                if not reused:
                    raise
                self._close()
                status = self._once(body, headers)
        except TimeoutError:
            self._close()
            raise DestFail("dataway_timeout")
        except (OSError, http.client.HTTPException):
            self._close()
            raise DestFail("dataway_connection_error")
        if 200 <= status < 300:
            return status
        self._close()
        if 300 <= status < 400:
            raise DestFail("dataway_redirect_refused_%d" % status)
        raise DestFail("dataway_status_%d" % status)


class Pending:
    """One queued line and the request thread waiting for its batch outcome."""
    __slots__ = ("line", "done", "status", "error", "batch")

    def __init__(self, line):
        self.line, self.done, self.status, self.error, self.batch = line, threading.Event(), None, None, 0


class Batcher:
    """Queue plus SENDERS threads. A request is answered only after its own batch POST finished."""

    def __init__(self, cfg, sender_factory=Sender):
        self.cfg = cfg
        self.q = queue.Queue(maxsize=cfg["queue_max"])
        self.threads = []
        for i in range(cfg["senders"]):
            t = threading.Thread(target=self._run, args=(sender_factory(cfg),), name="sender-%d" % i, daemon=True)
            t.start()
            self.threads.append(t)

    def submit(self, line):
        item = Pending(line)
        try:
            self.q.put_nowait(item)
        except queue.Full:
            raise DestFail("queue_full", status=503)
        # Upper bound: the batch window, one POST with one fresh-connection retry, and slack.
        if not item.done.wait(self.cfg["batch_max_wait_ms"] / 1000 + 2 * DATAWAY_TIMEOUT + 5):
            raise DestFail("dataway_wait_timeout")
        if item.error is not None:
            raise item.error
        return item.status, item.batch

    def _collect(self, first):
        """Gather a batch starting with `first`. Returns (batch, carry): carry did not fit and
        starts this sender's next batch, so it is never left behind in the queue."""
        batch, size = [first], len(first.line) + 1
        deadline = time.monotonic() + self.cfg["batch_max_wait_ms"] / 1000
        while len(batch) < self.cfg["batch_max_records"]:
            remaining = deadline - time.monotonic()
            try:
                item = self.q.get_nowait() if remaining <= 0 else self.q.get(timeout=remaining)
            except queue.Empty:
                break
            if size + len(item.line) + 1 > self.cfg["batch_max_bytes"]:
                return batch, item
            batch.append(item)
            size += len(item.line) + 1
        return batch, None

    def _run(self, sender):
        carry = None
        while True:
            batch, carry = self._collect(carry if carry is not None else self.q.get())
            t0 = time.monotonic()
            try:
                status, error = sender.post([p.line for p in batch]), None
            except DestFail as e:
                status, error = None, e
            except Exception as e:            # noqa: BLE001 - a sender thread must never die
                status, error = None, DestFail("sender_error_" + type(e).__name__)
            emit({"receipt": VERSION, "event": "batch", "records": len(batch),
                  "bytes": sum(len(p.line) + 1 for p in batch),
                  "dataway_status": status, "reason": str(error) if error else None,
                  "post_ms": int((time.monotonic() - t0) * 1000), "queued": self.q.qsize()})
            for p in batch:
                p.status, p.error, p.batch = status, error, len(batch)
                p.done.set()


# ------------------------------------------------------------------ handler
class Forwarder:
    def __init__(self, cfg, batcher=None):
        self.cfg = cfg
        self.batcher = batcher if batcher is not None else Batcher(cfg)

    def pubsub(self, body, receipt):
        message = parse_push(body, self.cfg)
        record = record_from_logentry(self.cfg, decode_logentry(message))
        receipt["google_insert_id"] = record["tags"]["google_insert_id"]
        receipt["records"] = 1
        receipt["dataway_status"], receipt["batch_records"] = self.batcher.submit(
            to_line(record, self.cfg["extra_tags"]))
        return 204


class Framing(Reject):
    """The body boundary is unknown or was not honoured: answer, then close the connection."""


def body_length(headers):
    """Strict request framing. Returns the declared body length; raises Framing otherwise."""
    if headers.get_all("Transfer-Encoding"):
        raise Framing(400, "transfer-encoding not accepted")
    values = headers.get_all("Content-Length") or []
    if not values:
        return 0
    if len(values) != 1:
        raise Framing(400, "repeated content-length")
    value = values[0].strip(" \t")
    if not CONTENT_LENGTH_RE.match(value):
        raise Framing(400, "malformed content-length")
    return int(value)


class Handler(BaseHTTPRequestHandler):
    forwarder = None
    server_version = VERSION
    protocol_version = "HTTP/1.1"
    timeout = INBOUND_IDLE_TIMEOUT             # socket timeout for every read, headers included

    def log_message(self, *a):
        pass                                   # the JSON receipt is the only log line

    def _reply(self, status, close=False):
        try:
            self.send_response(status)
            self.send_header("Content-Length", "0")
            if close:
                self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.flush()
        except OSError:
            pass                               # the peer already went away; nothing to tell it
        if close:
            self.close_connection = True

    def _read_body(self, length):
        """Read exactly `length` bytes in bounded chunks under an idle timeout and a deadline."""
        deadline = time.monotonic() + INBOUND_BODY_DEADLINE
        chunks, got = [], 0
        while got < length:
            if time.monotonic() > deadline:
                raise Framing(408, "body deadline exceeded")
            try:
                chunk = self.rfile.read1(min(READ_CHUNK, length - got))
            except socket.timeout:
                raise Framing(408, "body read timed out")
            if not chunk:
                raise Framing(400, "body truncated")
            chunks.append(chunk)
            got += len(chunk)
        return b"".join(chunks)

    def _drain(self, length):
        """Discard at most DRAIN_CAP bytes for at most DRAIN_DEADLINE seconds, then give up."""
        if length > DRAIN_CAP:
            return
        deadline = time.monotonic() + DRAIN_DEADLINE
        remaining = length
        while remaining > 0 and time.monotonic() < deadline:
            try:
                chunk = self.rfile.read1(min(DRAIN_CHUNK, remaining))
            except socket.timeout:
                return
            if not chunk:
                return
            remaining -= len(chunk)

    def do_GET(self):
        self._reply(404)

    def do_POST(self):
        t0 = time.time()
        path = self.path.split("?", 1)[0]
        receipt = {"receipt": VERSION, "route": path if path == "/pubsub" else "unknown",
                   "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        close = False
        try:
            length = body_length(self.headers)
            if length > MAX_PUSH_BYTES:
                # Answer 413 cleanly rather than resetting mid-upload: a clean 4xx lets Pub/Sub
                # dead-letter a poison message instead of retrying a transport error forever.
                # Nothing is parsed and nothing is forwarded; the drain is byte- and time-bounded.
                self._drain(length)
                raise Framing(413, "body too large")
            body = self._read_body(length)
            if path != "/pubsub":
                raise Reject(404, "not found")
            status = self.forwarder.pubsub(body, receipt)
            receipt["outcome"] = "acked_after_dataway_2xx"
        except Framing as e:
            status, receipt["outcome"], receipt["reason"] = e.status, "rejected", e.reason[:200]
            close = True
        except Reject as e:
            status, receipt["outcome"], receipt["reason"] = e.status, "rejected", e.reason[:200]
        except DestFail as e:
            status, receipt["outcome"], receipt["reason"] = e.status, "failed_retry", str(e)[:80]
        except Exception as e:                 # never echo bodies, headers or exception text
            status, receipt["outcome"], receipt["reason"] = 500, "error", type(e).__name__
            close = True
        receipt.update(http_status=status, handler_ms=int((time.time() - t0) * 1000))
        emit(receipt)
        self._reply(status, close=close)


class Server(ThreadingHTTPServer):
    """ThreadingHTTPServer with a listen backlog sized for Cloud Run --concurrency well above 1.
    The library default of 5 resets connections when many pushes arrive at once."""
    request_queue_size = 1024
    daemon_threads = True


def main():
    try:
        cfg = cfg_from_env()
    except ConfigFail as e:
        emit({"receipt": VERSION, "event": "refused_start", "reason": str(e)[:200]})
        return 78
    Handler.forwarder = Forwarder(cfg)
    port = int(os.environ.get("PORT", "8080"))
    emit(dict(public_cfg(cfg), receipt=VERSION, event="start", port=port,
              destination="https://%s:%d%s" % (cfg["dest_host"], cfg["dest_port"], cfg["dest_path"])))
    Server(("0.0.0.0", port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
