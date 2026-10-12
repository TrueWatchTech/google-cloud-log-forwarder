#!/usr/bin/env python3
"""pubsub-log-forwarder v02 - stream-only Cloud Run adapter: Cloud Logging -> Pub/Sub push -> private DataKit.

Scope (deliberately smaller than the earlier PoC source): ONE route, ordinary customer logs, no Cloud
Storage batch route, no run-scoped markers, no origin allowlist, no DataWay credential of any kind.

Route (POST only; Cloud Run IAM authentication REQUIRED - deploy with --no-allow-unauthenticated):
  /pubsub   one wrapped Pub/Sub push message whose data is a base64 Cloud Logging LogEntry.
            jsonPayload (object) and textPayload (string) are supported. protoPayload is NOT.
  anything else -> 404.

Destination: EXACTLY http://<private IPv4>:9529/v1/write/logging - DataKit's built-in HTTP write API,
which takes a JSON array of {measurement, time(ns), tags, fields}. The adapter holds no workspace
token: DataKit owns the complete DataWay URL.

Mapping (one LogEntry -> one DataKit record), documented contract:
  measurement          MEASUREMENT (default "gcp_pubsub")
  time                 LogEntry.timestamp parsed to integer Unix NANOSECONDS (no float arithmetic)
  tags.project         project id taken from LogEntry.logName; must equal EXPECTED_PROJECT_ID
  tags.resource_type   LogEntry.resource.type (e.g. gce_instance, cloud_run_revision), "unknown" if absent
  tags.severity        LogEntry.severity, a Cloud Logging LogSeverity name (default "DEFAULT")
  tags.log_name        LogEntry.logName log id part (the text after /logs/), URL-decoded
  tags.google_insert_id LogEntry.insertId - kept so the original record stays identifiable end to end
  fields.message       textPayload verbatim, or jsonPayload as one compact sorted JSON string
  fields.status        severity mapped to DataKit's log status vocabulary
  fields.log_timestamp original RFC 3339 timestamp string, unmodified
  fields.receive_timestamp  LogEntry.receiveTimestamp, when present and valid

google_insert_id is a deliberately high-cardinality tag: it is the identifier the verification step
filters on. Resource labels are NOT mapped (they differ per resource type); that is a stated
limitation, not an implied claim of broader support.

ACK: 204 ONLY after DataKit answered 2xx to the actual write. Malformed/foreign input -> 4xx and
destination failure/timeout/redirect/non-2xx -> 502, so Pub/Sub retries and never sees a false ACK.
Retries are at-least-once: duplicates are possible, identifiable by google_insert_id + original time.
Receipts carry categories and counts only - never payload bodies, response bodies or credentials.

Inbound framing (v02): exactly one Content-Length of 1-10 ASCII digits, or none (empty body). Any
Transfer-Encoding, repeated Content-Length, sign, space-separated list or other non-digit -> 400 and
the connection is closed, because the body boundary is unknown. The body is read in bounded chunks
under a per-read idle timeout and an overall deadline: a truncated body -> 400, a stalled body ->
408, a declared length over MAX_PUSH_BYTES -> 413 after a byte- and time-bounded drain. Every one of
these closes the connection and nothing is parsed or forwarded.
"""
import base64
import binascii
import calendar
import http.client
import ipaddress
import json
import math
import os
import re
import socket
import sys
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer

VERSION = "pubsub-log-forwarder/v02"
DATAKIT_PORT = 9529
DATAKIT_PATH = "/v1/write/logging"
DEFAULT_MEASUREMENT = "gcp_pubsub"

MAX_PUSH_BYTES = 1_048_576        # bound the inbound push body (Pub/Sub messages are <= 10 MiB)
MAX_RECORD_BYTES = 524_288        # bound the serialized DataKit record
DATAKIT_TIMEOUT = 10              # seconds, outbound connect+read
MAX_RESPONSE_READ = 4096          # drained and discarded; never logged
DRAIN_CHUNK = 65_536              # bounded read used to answer an oversize push cleanly
DRAIN_CAP = 11_534_336            # 11 MiB: above Pub/Sub's 10 MiB message limit
READ_CHUNK = 65_536               # inbound body read size
INBOUND_IDLE_TIMEOUT = 10         # seconds a single socket read may wait (also bounds header reads)
INBOUND_BODY_DEADLINE = 30        # seconds to receive a whole accepted body
DRAIN_DEADLINE = 5                # seconds spent draining an oversize body before answering 413
CONTENT_LENGTH_RE = re.compile(r"^[0-9]{1,10}$")

# The destination must sit in an RFC 1918 range. ipaddress.is_private is deliberately NOT used on
# its own: it also accepts documentation (203.0.113.0/24) and other special-purpose ranges, which
# are not private networks. A customer on a non-RFC 1918 private range is a documented refusal.
RFC1918 = (ipaddress.IPv4Network("10.0.0.0/8"),
           ipaddress.IPv4Network("172.16.0.0/12"),
           ipaddress.IPv4Network("192.168.0.0/16"))

MEASUREMENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
PROJECT_RE = re.compile(r"^[a-z][a-z0-9\-]{4,28}[a-z0-9]$")
SUBSCRIPTION_RE = re.compile(r"^projects/([a-z][a-z0-9\-]{4,28}[a-z0-9])/subscriptions/([A-Za-z][A-Za-z0-9._~%+\-]{2,254})$")
LOG_NAME_RE = re.compile(r"^projects/([a-z][a-z0-9\-]{4,28}[a-z0-9])/logs/([A-Za-z0-9._%/\-]{1,512})$")
INSERT_ID_RE = re.compile(r"^[\x21-\x7e]{1,256}$")
RESOURCE_TYPE_RE = re.compile(r"^[a-z][a-z0-9_.\-]{0,62}$")
RFC3339_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:([Zz])|([+-])(\d{2}):(\d{2}))$")

SEVERITY_STATUS = {"DEFAULT": "info", "DEBUG": "debug", "INFO": "info", "NOTICE": "notice",
                   "WARNING": "warning", "ERROR": "error", "CRITICAL": "critical",
                   "ALERT": "alert", "EMERGENCY": "emerg"}

# No TrueWatch/DataWay credential belongs in this service. Refuse to start if one was configured.
FORBIDDEN_ENV_SUBSTRINGS = ("DATAWAY", "SECRET", "TOKEN", "CREDENTIAL", "PASSWORD")
ALLOWED_ENV_EXCEPTIONS = ()


class Reject(Exception):
    """Invalid or foreign input -> 4xx. Never forwarded, never acknowledged as success."""

    def __init__(self, status, reason):
        super().__init__(reason)
        self.status, self.reason = status, reason


class DestFail(Exception):
    """DataKit did not accept the write -> 502 so Pub/Sub retries. Message is a fixed category."""


class ConfigFail(Exception):
    """Misconfiguration -> refuse to start (exit 78)."""


def emit(obj):
    """One JSON line to stdout. Only categories, counts and identifiers already present in config."""
    sys.stdout.write(json.dumps(obj, separators=(",", ":"), sort_keys=True) + "\n")
    sys.stdout.flush()


# ------------------------------------------------------------------ configuration
def validate_destination(url):
    """Return (ip, port, path) for exactly http://<private IPv4>:9529/v1/write/logging."""
    if not isinstance(url, str) or not url or any(c.isspace() for c in url):
        raise ConfigFail("DATAKIT_URL missing or contains whitespace")
    try:
        u = urllib.parse.urlsplit(url)
        port = u.port
    except ValueError:
        raise ConfigFail("DATAKIT_URL is not a URL")
    if u.scheme != "http":
        raise ConfigFail("DATAKIT_URL must be http:// to the private DataKit listener")
    if u.username is not None or u.password is not None or "@" in u.netloc:
        raise ConfigFail("DATAKIT_URL must not contain credentials")
    if u.query or u.fragment or "?" in url or "#" in url:
        raise ConfigFail("DATAKIT_URL must not contain a query string or fragment")
    if u.path != DATAKIT_PATH:
        raise ConfigFail("DATAKIT_URL path must be exactly " + DATAKIT_PATH)
    if port != DATAKIT_PORT:
        raise ConfigFail("DATAKIT_URL port must be exactly %d" % DATAKIT_PORT)
    try:
        ip = ipaddress.IPv4Address(u.hostname or "")
    except ValueError:
        raise ConfigFail("DATAKIT_URL host must be an IPv4 literal (no DNS name, no IPv6)")
    if u.netloc != "%s:%d" % (ip, DATAKIT_PORT):
        raise ConfigFail("DATAKIT_URL host is not in canonical dotted-quad form")
    if not any(ip in net for net in RFC1918):
        raise ConfigFail("DATAKIT_URL host must be inside 10/8, 172.16/12 or 192.168/16 (RFC 1918)")
    for net in RFC1918:
        if ip in net and ip in (net.network_address, net.broadcast_address):
            raise ConfigFail("DATAKIT_URL host is not a usable host address")
    return str(ip), DATAKIT_PORT, DATAKIT_PATH


def cfg_from_env(env=None):
    env = os.environ if env is None else env
    offending = sorted(k for k in env
                       if k not in ALLOWED_ENV_EXCEPTIONS
                       and any(s in k.upper() for s in FORBIDDEN_ENV_SUBSTRINGS))
    if offending:
        raise ConfigFail("credential-shaped environment variable present: " + ",".join(offending))

    project = env.get("EXPECTED_PROJECT_ID", "")
    if not PROJECT_RE.match(project or ""):
        raise ConfigFail("EXPECTED_PROJECT_ID missing or not a Google project id")

    sub = env.get("EXPECTED_SUBSCRIPTION", "")
    m = SUBSCRIPTION_RE.match(sub or "")
    if not m:
        raise ConfigFail("EXPECTED_SUBSCRIPTION must be a full projects/<id>/subscriptions/<name> name")

    measurement = env.get("MEASUREMENT") or DEFAULT_MEASUREMENT
    if not MEASUREMENT_RE.match(measurement):
        raise ConfigFail("MEASUREMENT must match [A-Za-z][A-Za-z0-9_]{0,63}")

    ip, port, path = validate_destination(env.get("DATAKIT_URL"))
    return {"project": project, "subscription": sub, "measurement": measurement,
            "dest_ip": ip, "dest_port": port, "dest_path": path}


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


def record_from_logentry(cfg, entry):
    """Validate one ordinary LogEntry and map it to one DataKit record."""
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

    record = {"measurement": cfg["measurement"], "time": t_ns, "tags": tags, "fields": fields}
    if len(json.dumps([record], separators=(",", ":"), ensure_ascii=False, allow_nan=False)
            .encode("utf-8")) > MAX_RECORD_BYTES:
        raise Reject(413, "mapped record exceeds the record size bound")
    return record


# ------------------------------------------------------------------ destination
def _connection(cfg):
    return http.client.HTTPConnection(cfg["dest_ip"], cfg["dest_port"], timeout=DATAKIT_TIMEOUT)


def datakit_post(cfg, records, connect=_connection):
    """One POST of a JSON array. Returns the 2xx status; anything else raises DestFail.

    http.client is used directly against the pinned private IPv4, so there is no redirect
    following, no proxy handling and no DNS resolution at request time.
    """
    body = json.dumps(records, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    conn = None
    try:
        conn = connect(cfg)
        conn.request("POST", cfg["dest_path"], body=body,
                     headers={"Content-Type": "application/json", "User-Agent": VERSION,
                              "Content-Length": str(len(body))})
        r = conn.getresponse()
        try:
            r.read(MAX_RESPONSE_READ)        # drained so the socket closes cleanly; never logged
        finally:
            r.close()
        status = r.status
    except TimeoutError:
        raise DestFail("datakit_timeout")
    except (OSError, http.client.HTTPException):
        raise DestFail("datakit_connection_error")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:                 # noqa: BLE001 - closing must never mask the outcome
                pass
    if 200 <= status < 300:
        return status
    if 300 <= status < 400:
        raise DestFail("datakit_redirect_refused_%d" % status)
    raise DestFail("datakit_status_%d" % status)


# ------------------------------------------------------------------ handler
class Forwarder:
    def __init__(self, cfg, post=datakit_post):
        self.cfg, self.post = cfg, post

    def pubsub(self, body, receipt):
        message = parse_push(body, self.cfg)
        record = record_from_logentry(self.cfg, decode_logentry(message))
        receipt["google_insert_id"] = record["tags"]["google_insert_id"]
        receipt["records"] = 1
        receipt["datakit_status"] = self.post(self.cfg, [record])
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
            receipt["outcome"] = "acked_after_datakit_2xx"
        except Framing as e:
            status, receipt["outcome"], receipt["reason"] = e.status, "rejected", e.reason[:200]
            close = True
        except Reject as e:
            status, receipt["outcome"], receipt["reason"] = e.status, "rejected", e.reason[:200]
        except DestFail as e:
            status, receipt["outcome"], receipt["reason"] = 502, "failed_retry", str(e)[:80]
        except Exception as e:                 # never echo bodies, headers or exception text
            status, receipt["outcome"], receipt["reason"] = 500, "error", type(e).__name__
            close = True
        receipt.update(http_status=status, handler_ms=int((time.time() - t0) * 1000))
        emit(receipt)
        self._reply(status, close=close)


def main():
    try:
        cfg = cfg_from_env()
    except ConfigFail as e:
        emit({"receipt": VERSION, "event": "refused_start", "reason": str(e)[:200]})
        return 78
    Handler.forwarder = Forwarder(cfg)
    port = int(os.environ.get("PORT", "8080"))
    emit({"receipt": VERSION, "event": "start", "port": port, "measurement": cfg["measurement"],
          "project": cfg["project"], "subscription": cfg["subscription"],
          "destination": "http://%s:%d%s" % (cfg["dest_ip"], cfg["dest_port"], cfg["dest_path"])})
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
