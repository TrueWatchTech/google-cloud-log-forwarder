#!/usr/bin/env python3
"""pubsub-log-forwarder v03-dataway tests - stdlib unittest, no network, no Docker.

Every handler case goes through the REAL HTTP handler: a ThreadingHTTPServer on 127.0.0.1 serves
main.Handler, and a stub DataWay (also real HTTP/1.1 with keep-alive, on loopback) stands in for
DataWay. The stub decodes the line protocol with its own parser (written independently of main.py),
so the tests check what DataWay would actually read. A local stub proves the adapter's contract; it
is NOT evidence that TrueWatch indexed anything.

Run:  python3 tests/test_forwarder.py
"""
import base64
import contextlib
import gzip
import io
import json
import os
import socket
import sys
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, ".."))
import main as fwd  # noqa: E402

PROJECT = "example-project"
SUBSCRIPTION = "projects/example-project/subscriptions/truewatch-logs-push"
TOKEN = "ct_AbCdEf0123456789abcdef0123456789"   # fake Client Token
BASE_ENV = {"DATAWAY_URL": "https://eu1-openway.truewatch.com", "DATAWAY_TOKEN": TOKEN,
            "EXPECTED_PROJECT_ID": PROJECT, "EXPECTED_SUBSCRIPTION": SUBSCRIPTION}


# ----------------------------------------------------------------- independent line-protocol parser
def _split_unescaped(s, sep, quotes=False):
    """Split on `sep` outside backslash escapes (and, if quotes, outside double-quoted strings)."""
    parts, cur, i, inq = [], [], 0, False
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            cur.append(s[i:i + 2])
            i += 2
            continue
        if quotes and c == '"':
            inq = not inq
        if c == sep and not inq:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(c)
        i += 1
    parts.append("".join(cur))
    return parts


def _unescape_key(s):
    return s.replace("\\,", ",").replace("\\=", "=").replace("\\ ", " ")


def _unescape_str(s):
    out, i = [], 0
    while i < len(s):
        if s[i] == "\\" and i + 1 < len(s) and s[i + 1] in '\\"':
            out.append(s[i + 1])
            i += 2
        else:
            out.append(s[i])
            i += 1
    return "".join(out)


def parse_line(line):
    # Quotes mean nothing in the measurement/tag section; they only delimit string fields.
    first = _split_unescaped(line, " ")
    head, rest = first[0], line[len(first[0]) + 1:]
    fields_part, ts = _split_unescaped(rest, " ", quotes=True)
    head_parts = _split_unescaped(head, ",")
    tags = {}
    for kv in head_parts[1:]:
        k, v = _split_unescaped(kv, "=")
        tags[_unescape_key(k)] = _unescape_key(v)
    fields = {}
    for kv in _split_unescaped(fields_part, ",", quotes=True):
        k = _split_unescaped(kv, "=")[0]
        v = kv[len(k) + 1:]
        if v.startswith('"'):
            val = _unescape_str(v[1:-1])
        elif v in ("true", "false"):
            val = v == "true"
        elif v.endswith("i"):
            val = int(v[:-1])
        else:
            val = float(v)
        fields[_unescape_key(k)] = val
    return {"measurement": _unescape_key(head_parts[0]), "tags": tags, "fields": fields, "time": int(ts)}


# ----------------------------------------------------------------- stub DataWay (real HTTP/1.1)
class Stub(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    status = 200
    location = None
    hang = 0
    received = []
    lock = threading.Lock()

    def log_message(self, *a):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)
        u = urllib.parse.urlsplit(self.path)
        with Stub.lock:
            Stub.received.append({"path": u.path, "query": urllib.parse.parse_qs(u.query),
                                  "ctype": self.headers.get("Content-Type"),
                                  "encoding": self.headers.get("Content-Encoding"),
                                  "ua": self.headers.get("User-Agent"),
                                  "client_port": self.client_address[1],
                                  "lines": body.decode("utf-8").split("\n")})
        if Stub.hang:
            time.sleep(Stub.hang)
        self.send_response(Stub.status)
        if Stub.location:
            self.send_header("Location", Stub.location)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")


def start_server(handler_cls):
    srv = fwd.Server(("127.0.0.1", 0), handler_cls)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def stub_cfg(port, **over):
    cfg = dict(fwd.cfg_from_env(BASE_ENV))
    cfg.update(dest_scheme="http", dest_host="127.0.0.1", dest_port=port, batch_max_wait_ms=20)
    cfg.update(over)
    return cfg


# ----------------------------------------------------------------- fixtures
def logentry(**over):
    e = {"insertId": "abc123xyz", "logName": "projects/%s/logs/stdout" % PROJECT,
         "resource": {"type": "gce_instance", "labels": {"instance_id": "1"}},
         "severity": "INFO", "timestamp": "2026-10-12T07:15:04.123456789Z",
         "receiveTimestamp": "2026-10-12T07:15:05.000000001Z",
         "jsonPayload": {"check_id": "tw-check-001", "msg": "hello", "n": 2}}
    for k, v in over.items():
        if v is fwd:            # sentinel: delete the key
            e.pop(k, None)
        else:
            e[k] = v
    return e


def push(entry=None, subscription=SUBSCRIPTION, data=None):
    if data is None:
        data = base64.b64encode(json.dumps(entry).encode("utf-8")).decode("ascii")
    return json.dumps({"subscription": subscription,
                       "message": {"data": data, "messageId": "1", "publishTime": "2026-10-12T07:15:06Z"}})


class HandlerCase(unittest.TestCase):
    """Boots the real adapter handler against the real stub DataWay on loopback."""

    @classmethod
    def setUpClass(cls):
        Stub.status, Stub.location, Stub.hang = 200, None, 0
        cls.dw = start_server(Stub)
        cls.cfg = stub_cfg(cls.dw.server_address[1])
        fwd.Handler.forwarder = fwd.Forwarder(cls.cfg)
        cls.app = start_server(fwd.Handler)
        cls.url = "http://127.0.0.1:%d" % cls.app.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()
        cls.dw.shutdown()

    def setUp(self):
        Stub.status, Stub.location, Stub.hang = 200, None, 0
        Stub.received = []

    def post(self, body, path="/pubsub"):
        req = urllib.request.Request(self.url + path, data=body.encode("utf-8") if isinstance(body, str) else body,
                                     headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=2 * fwd.DATAWAY_TIMEOUT + 10) as r:
                return r.status
        except urllib.error.HTTPError as e:
            e.read()
            return e.code

    def records(self):
        return [parse_line(line) for w in Stub.received for line in w["lines"]]

    def sent(self):
        recs = self.records()
        self.assertEqual(len(recs), 1, "expected exactly one record written")
        return recs


# ===================================================================== success paths
class TestSuccess(HandlerCase):
    def test_01_json_payload_acked_after_dataway_2xx(self):
        self.assertEqual(self.post(push(logentry())), 204)
        rec = self.sent()[0]
        self.assertEqual(rec["measurement"], "gcp_pubsub")
        self.assertEqual(rec["fields"]["message"], '{"check_id":"tw-check-001","msg":"hello","n":2}')
        self.assertEqual(rec["fields"]["status"], "info")
        w = Stub.received[0]
        self.assertEqual(w["path"], "/v1/write/logging")
        self.assertEqual(w["query"], {"token": [TOKEN], "to_headless": ["true"]})
        self.assertTrue(w["ctype"].startswith("text/plain"))
        self.assertEqual(w["ua"], fwd.VERSION)

    def test_02_text_payload_supported(self):
        e = logentry(jsonPayload=fwd, textPayload="plain line", severity="ERROR")
        self.assertEqual(self.post(push(e)), 204)
        rec = self.sent()[0]
        self.assertEqual(rec["fields"]["message"], "plain line")
        self.assertEqual(rec["fields"]["status"], "error")
        self.assertEqual(rec["tags"]["severity"], "ERROR")

    def test_03_original_timestamp_is_exact_integer_nanoseconds(self):
        self.assertEqual(self.post(push(logentry())), 204)
        rec = self.sent()[0]
        self.assertEqual(rec["time"], 1791789304123456789)
        self.assertEqual(rec["fields"]["log_timestamp"], "2026-10-12T07:15:04.123456789Z")
        self.assertEqual(rec["fields"]["receive_timestamp"], "2026-10-12T07:15:05.000000001Z")

    def test_04_offset_timestamp_normalised_without_float(self):
        self.assertEqual(self.post(push(logentry(timestamp="2026-10-12T15:15:04.000000007+08:00"))), 204)
        self.assertEqual(self.sent()[0]["time"], 1791789304000000007)

    def test_05_identity_tags_present(self):
        self.assertEqual(self.post(push(logentry())), 204)
        tags = self.sent()[0]["tags"]
        self.assertEqual(tags, {"google_insert_id": "abc123xyz", "log_name": "stdout", "project": PROJECT,
                                "resource_instance_id": "1", "resource_type": "gce_instance",
                                "severity": "INFO"})

    def test_06_url_encoded_log_name_decoded(self):
        e = logentry(logName="projects/%s/logs/cloudaudit.googleapis.com%%2Factivity" % PROJECT)
        self.assertEqual(self.post(push(e)), 204)
        self.assertEqual(self.sent()[0]["tags"]["log_name"], "cloudaudit.googleapis.com/activity")

    def test_08_optional_resource_and_receive_timestamp_absent(self):
        self.assertEqual(self.post(push(logentry(resource=fwd, receiveTimestamp=fwd))), 204)
        rec = self.sent()[0]
        self.assertEqual(rec["tags"]["resource_type"], "unknown")
        self.assertNotIn("receive_timestamp", rec["fields"])

    def test_09_special_characters_survive_line_protocol(self):
        text = 'say "hi", a=b c\\d\nsecond line\r\nthird'
        e = logentry(jsonPayload=fwd, textPayload=text)
        e["resource"]["labels"] = {"weird": 'a b,c=d\\e"f', "empty": ""}
        self.assertEqual(self.post(push(e)), 204)
        rec = self.sent()[0]
        self.assertEqual(rec["fields"]["message"], 'say "hi", a=b c\\d\\nsecond line\\r\\nthird')
        self.assertEqual(rec["tags"]["resource_weird"], 'a b,c=d%5Ce"f')
        self.assertNotIn("resource_empty", rec["tags"])


# ===================================================================== rejection paths
class TestRejections(HandlerCase):
    def assert_no_write(self, status, expect):
        self.assertEqual(status, expect)
        self.assertEqual(Stub.received, [], "rejected input must never be forwarded")

    def test_10_wrong_subscription_rejected(self):
        self.assert_no_write(self.post(push(logentry(), subscription="projects/%s/subscriptions/other" % PROJECT)), 403)

    def test_11_foreign_project_rejected(self):
        self.assert_no_write(self.post(push(logentry(logName="projects/other-project/logs/stdout"))), 403)

    def test_12_not_an_envelope_rejected(self):
        self.assert_no_write(self.post(json.dumps({"subscription": SUBSCRIPTION, "message": "x"})), 400)

    def test_14_malformed_base64_rejected(self):
        self.assert_no_write(self.post(push(data="!!!!not base64!!!!")), 400)

    def test_16_proto_payload_unsupported(self):
        e = logentry(jsonPayload=fwd)
        e["protoPayload"] = {"@type": "type.googleapis.com/google.cloud.audit.AuditLog"}
        self.assert_no_write(self.post(push(e)), 400)

    def test_17_no_payload_rejected(self):
        self.assert_no_write(self.post(push(logentry(jsonPayload=fwd))), 400)

    def test_18_invalid_timestamp_rejected(self):
        self.assert_no_write(self.post(push(logentry(timestamp="12 October 2026"))), 400)

    def test_20_missing_insert_id_rejected(self):
        self.assert_no_write(self.post(push(logentry(insertId=fwd))), 400)

    def test_22_unknown_severity_rejected(self):
        self.assert_no_write(self.post(push(logentry(severity="LOUD"))), 400)

    def test_23_nonfinite_payload_value_rejected(self):
        data = base64.b64encode(b'{"insertId":"a","logName":"projects/' + PROJECT.encode()
                                + b'/logs/s","timestamp":"2026-10-12T07:15:04Z",'
                                  b'"jsonPayload":{"v":NaN}}').decode()
        self.assert_no_write(self.post(push(data=data)), 400)

    def test_25_oversize_line_rejected(self):
        body = push(logentry(textPayload="y" * (fwd.MAX_RECORD_BYTES + 16), jsonPayload=fwd))
        self.assertLess(len(body), fwd.MAX_PUSH_BYTES)
        self.assert_no_write(self.post(body), 413)

    def test_26_wrong_path_is_404(self):
        self.assert_no_write(self.post(push(logentry()), path="/storage"), 404)


# ===================================================================== destination failures
class TestDestinationFailures(HandlerCase):
    def test_30_dataway_500_does_not_ack(self):
        Stub.status = 500
        self.assertEqual(self.post(push(logentry())), 502)
        self.assertEqual(len(Stub.received), 1)

    def test_31_dataway_401_does_not_ack(self):
        Stub.status = 401
        self.assertEqual(self.post(push(logentry())), 502)

    def test_32_redirect_is_refused_not_followed(self):
        Stub.status, Stub.location = 302, "http://169.254.169.254/latest"
        self.assertEqual(self.post(push(logentry())), 502)
        self.assertEqual(len(Stub.received), 1, "the redirect target must never be requested")

    def test_33_timeout_does_not_ack(self):
        saved_timeout, saved_fwd = fwd.DATAWAY_TIMEOUT, fwd.Handler.forwarder
        fwd.DATAWAY_TIMEOUT, Stub.hang = 1, 2
        fwd.Handler.forwarder = fwd.Forwarder(self.cfg)
        try:
            self.assertEqual(self.post(push(logentry())), 502)
        finally:
            fwd.DATAWAY_TIMEOUT, fwd.Handler.forwarder = saved_timeout, saved_fwd

    def test_34_unreachable_dataway_does_not_ack(self):
        saved = fwd.Handler.forwarder
        fwd.Handler.forwarder = fwd.Forwarder(dict(self.cfg, dest_port=1))
        try:
            self.assertEqual(self.post(push(logentry())), 502)
        finally:
            fwd.Handler.forwarder = saved

    def test_35_token_never_logged(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.assertEqual(self.post(push(logentry())), 204)
            Stub.status = 500
            self.assertEqual(self.post(push(logentry())), 502)
            time.sleep(0.1)
        self.assertNotIn(TOKEN, buf.getvalue())
        self.assertNotIn(TOKEN[3:], buf.getvalue())


# ===================================================================== batching
class TestBatching(HandlerCase):
    def burst(self, n):
        bodies = [push(logentry(insertId="id%04d" % i)) for i in range(n)]
        with ThreadPoolExecutor(max_workers=n) as ex:
            return list(ex.map(self.post, bodies))

    def test_80_concurrent_messages_share_posts_and_all_ack(self):
        statuses = self.burst(60)
        self.assertEqual(statuses, [204] * 60)
        ids = sorted(r["tags"]["google_insert_id"] for r in self.records())
        self.assertEqual(ids, ["id%04d" % i for i in range(60)], "every record exactly once")
        self.assertLess(len(Stub.received), 60, "concurrent messages must be batched")

    def test_81_failed_batch_nacks_every_message_in_it(self):
        Stub.status = 503
        self.assertEqual(self.burst(30), [502] * 30)

    def test_82_connection_is_reused(self):
        self.assertEqual(self.post(push(logentry(insertId="a1"))), 204)
        self.assertEqual(self.post(push(logentry(insertId="a2"))), 204)
        self.assertEqual(self.post(push(logentry(insertId="a3"))), 204)
        ports = {w["client_port"] for w in Stub.received}
        self.assertLessEqual(len(ports), self.cfg["senders"], "kept-alive connections are reused")

    def test_83_gzip_body(self):
        saved = fwd.Handler.forwarder
        fwd.Handler.forwarder = fwd.Forwarder(dict(self.cfg, gzip=True))
        try:
            self.assertEqual(self.post(push(logentry())), 204)
        finally:
            fwd.Handler.forwarder = saved
        self.assertEqual(Stub.received[0]["encoding"], "gzip")
        self.assertEqual(self.sent()[0]["tags"]["google_insert_id"], "abc123xyz")

    def test_84_batch_respects_byte_bound(self):
        saved = fwd.Handler.forwarder
        fwd.Handler.forwarder = fwd.Forwarder(dict(self.cfg, batch_max_bytes=65_536, batch_max_wait_ms=300))
        try:
            bodies = [push(logentry(insertId="b%03d" % i, jsonPayload=fwd, textPayload="z" * 20_000))
                      for i in range(12)]
            with ThreadPoolExecutor(max_workers=12) as ex:
                self.assertEqual(list(ex.map(self.post, bodies)), [204] * 12)
        finally:
            fwd.Handler.forwarder = saved
        for w in Stub.received:
            self.assertLessEqual(sum(len(l) + 1 for l in w["lines"]), 65_536)
        self.assertEqual(len(self.records()), 12)

    def test_85_queue_full_answers_503_without_queueing(self):
        release = threading.Event()

        class Blocked:
            def __init__(self, cfg):
                pass

            def post(self, lines):
                release.wait(5)
                return 200

        cfg = dict(self.cfg, senders=1, queue_max=1, batch_max_records=1)
        b = fwd.Batcher(cfg, sender_factory=Blocked)
        results = []
        threads = [threading.Thread(target=lambda: results.append(self._submit(b))) for _ in range(4)]
        for t in threads:
            t.start()
            time.sleep(0.05)
        release.set()
        for t in threads:
            t.join(10)
        self.assertIn(503, results)
        self.assertIn(200, results)

    @staticmethod
    def _submit(b):
        try:
            return b.submit(b"x 1i 1")[0]
        except fwd.DestFail as e:
            return e.status


# ===================================================================== line protocol unit
class TestLineProtocol(unittest.TestCase):
    def rec(self, tags=None, fields=None):
        return {"measurement": "gcp_alb", "time": 5, "tags": tags or {"a": "1"}, "fields": fields or {"m": "x"}}

    def test_90_field_types(self):
        line = fwd.to_line(self.rec(fields={"i": 7, "b": True, "f": 1.5, "s": "v"})).decode()
        self.assertEqual(line, 'gcp_alb,a=1 b=true,f=1.5,i=7i,s="v" 5')

    def test_91_extra_tags_never_override(self):
        line = fwd.to_line(self.rec(tags={"host": "real"}), {"host": "extra", "ENV": "prod"}).decode()
        self.assertEqual(parse_line(line)["tags"], {"host": "real", "ENV": "prod"})

    def test_92_non_finite_float_rejected(self):
        with self.assertRaises(fwd.Reject):
            fwd.to_line(self.rec(fields={"f": float("inf")}))


# ===================================================================== configuration
class TestConfig(unittest.TestCase):
    def ok(self, **over):
        env = dict(BASE_ENV)
        env.update(over)
        return fwd.cfg_from_env(env)

    def fail(self, **over):
        env = dict(BASE_ENV)
        for k, v in over.items():
            if v is None:
                env.pop(k, None)
            else:
                env[k] = v
        with self.assertRaises(fwd.ConfigFail):
            fwd.cfg_from_env(env)

    def test_40_minimal_config(self):
        cfg = self.ok()
        self.assertEqual((cfg["dest_host"], cfg["dest_port"], cfg["dest_path"]),
                         ("eu1-openway.truewatch.com", 443, "/v1/write/logging"))
        self.assertEqual((cfg["batch_max_records"], cfg["senders"], cfg["gzip"]), (500, 4, False))
        self.assertNotIn(TOKEN, json.dumps(fwd.public_cfg(cfg)))

    def test_41_full_path_and_port_accepted(self):
        cfg = self.ok(DATAWAY_URL="https://dw.example.com:8443/v1/write/logging")
        self.assertEqual((cfg["dest_host"], cfg["dest_port"]), ("dw.example.com", 8443))

    def test_43_missing_required_refused(self):
        for key in ("DATAWAY_URL", "DATAWAY_TOKEN", "EXPECTED_PROJECT_ID", "EXPECTED_SUBSCRIPTION"):
            with self.subTest(missing=key):
                self.fail(**{key: None})

    def test_44_bad_destination_refused(self):
        for bad in ("http://eu1-openway.truewatch.com",
                    "https://eu1-openway.truewatch.com?token=" + TOKEN,
                    "https://user:pw@eu1-openway.truewatch.com",
                    "https://eu1-openway.truewatch.com/v1/write/metric",
                    "https://10.1.2.3",
                    "https://localhost"):
            with self.subTest(url=bad):
                self.fail(DATAWAY_URL=bad)

    def test_45_bad_token_refused(self):
        for bad in ("abc", "short_token", "has space 0123456789", "Bearer 0123456789abcdef"):
            with self.subTest(token=bad):
                self.fail(DATAWAY_TOKEN=bad)

    def test_46_other_credential_env_refuses_start(self):
        for name in ("ENV_DATAWAY", "TRUEWATCH_TOKEN", "DB_PASSWORD"):
            with self.subTest(name=name):
                self.fail(**{name: "anything"})

    def test_47_extra_tags(self):
        self.assertEqual(self.ok(EXTRA_TAGS="host=gcp-fra-forwarder,ENV=sec-waf-armor")["extra_tags"],
                         {"host": "gcp-fra-forwarder", "ENV": "sec-waf-armor"})
        for bad in ("host", "host=a b", "1x=y", "a=1,a=2", "a=x,y"):
            with self.subTest(extra=bad):
                self.fail(EXTRA_TAGS=bad)

    def test_48_batch_settings_bounded(self):
        self.assertEqual(self.ok(SENDERS="8", BATCH_MAX_WAIT_MS="0")["senders"], 8)
        for name, bad in (("SENDERS", "0"), ("BATCH_MAX_RECORDS", "-1"), ("QUEUE_MAX", "x"),
                          ("BATCH_MAX_BYTES", "100"), ("DATAWAY_GZIP", "yes")):
            with self.subTest(name=name):
                self.fail(**{name: bad})

    def test_50_client_token_only(self):
        self.assertIn("to_headless=true", fwd.Sender(self.ok()).target)
        # A workspace token is refused even though it would match the Client Token shape.
        self.fail(DATAWAY_TOKEN="tkn_0123456789abcdef0123456789abcdef")
        for value in ("client", "workspace"):
            with self.subTest(token_type=value):
                self.fail(DATAWAY_TOKEN_TYPE=value)

    def test_49_subscription_must_be_a_full_name(self):
        self.fail(EXPECTED_SUBSCRIPTION="truewatch-logs-push")


# ===================================================================== inbound framing
class TestFraming(HandlerCase):
    """Raw-socket requests, because urllib cannot send malformed framing. Each case must answer
    within its bound, close the connection, and never write to DataWay."""

    def setUp(self):
        super().setUp()
        self._saved = (fwd.Handler.timeout, fwd.INBOUND_BODY_DEADLINE, fwd.DRAIN_DEADLINE)
        fwd.Handler.timeout, fwd.INBOUND_BODY_DEADLINE, fwd.DRAIN_DEADLINE = 0.5, 1.5, 1.0

    def tearDown(self):
        fwd.Handler.timeout, fwd.INBOUND_BODY_DEADLINE, fwd.DRAIN_DEADLINE = self._saved

    def raw(self, head_lines, body=b"", half_close=False, budget=6.0):
        port = self.app.server_address[1]
        s = socket.create_connection(("127.0.0.1", port), timeout=budget)
        t0 = time.monotonic()
        try:
            head = "POST /pubsub HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n"
            head += "".join(h + "\r\n" for h in head_lines) + "\r\n"
            s.sendall(head.encode("ascii") + body)
            if half_close:
                s.shutdown(socket.SHUT_WR)
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = s.recv(4096)
                if not chunk:
                    break
                data += chunk
            elapsed = time.monotonic() - t0
            status = int(data.split(b" ", 2)[1]) if data.startswith(b"HTTP/1.1 ") else None
            rest = data.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in data else b""
            closed = (s.recv(1) == b"") and rest == b""
            return status, elapsed, closed, data
        finally:
            s.close()

    def assert_closed_without_write(self, result, status, max_seconds):
        got, elapsed, closed, data = result
        self.assertEqual(got, status, data[:80])
        self.assertLess(elapsed, max_seconds)
        self.assertTrue(closed, "connection must be closed after a framing rejection")
        self.assertIn(b"Connection: close", data)
        self.assertEqual(Stub.received, [], "nothing may be forwarded")

    def test_60_negative_length(self):
        self.assert_closed_without_write(self.raw(["Content-Length: -1"], b"{}"), 400, 1.0)

    def test_61_non_digit_length(self):
        self.assert_closed_without_write(self.raw(["Content-Length: +12"], b"{}"), 400, 1.0)

    def test_62_repeated_length(self):
        self.assert_closed_without_write(self.raw(["Content-Length: 2", "Content-Length: 40"], b"{}"), 400, 1.0)

    def test_63_transfer_encoding(self):
        self.assert_closed_without_write(self.raw(["Transfer-Encoding: chunked"], b"2\r\n{}\r\n0\r\n\r\n"), 400, 1.0)

    def test_64_truncated_body(self):
        body = push(logentry()).encode()
        self.assert_closed_without_write(
            self.raw(["Content-Length: %d" % (len(body) + 50)], body, half_close=True), 400, 1.0)

    def test_65_stalled_body(self):
        body = push(logentry()).encode()
        self.assert_closed_without_write(self.raw(["Content-Length: %d" % (len(body) + 50)], body), 408, 2.5)

    def test_66_oversize_body(self):
        self.assert_closed_without_write(
            self.raw(["Content-Length: %d" % (fwd.MAX_PUSH_BYTES + 1)], b"x" * 65536), 413, 2.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
