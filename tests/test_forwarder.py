#!/usr/bin/env python3
"""pubsub-log-forwarder v02 tests - stdlib unittest, no network, no Docker.

Every success/failure case goes through the REAL HTTP handler: a ThreadingHTTPServer bound to
127.0.0.1 serves forwarder.main.Handler, and a stub DataKit server (also real HTTP, on loopback)
stands in for the private listener. A local stub proves the adapter's contract; it is NOT evidence
that TrueWatch indexed anything.

v02 adds TestFraming: raw-socket requests with negative, malformed, repeated and chunked framing,
a truncated body, a stalled body and an oversize body. v01 fixtures were consolidated so the whole
package stays within 60 expanded inputs (run tests/run_all.py for the counted total).

Run:  python3 tests/test_forwarder.py
"""
import base64
import json
import os
import socket
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Works in both layouts: the private lane (forwarder/ beside tests/) and the published bundle
# (main.py at the package root beside tests/).
_HERE = os.path.dirname(os.path.abspath(__file__))
for _candidate in (os.path.join(_HERE, "..", "forwarder"), os.path.join(_HERE, "..")):
    if os.path.exists(os.path.join(_candidate, "main.py")):
        sys.path.insert(0, _candidate)
        break
else:
    sys.exit("cannot locate main.py next to tests/ or in a sibling forwarder/ directory")
import main as fwd  # noqa: E402

PROJECT = "example-project"
SUBSCRIPTION = "projects/example-project/subscriptions/truewatch-logs-push"
DATAKIT_IP = "10.20.30.40"
GOOD_URL = "http://%s:9529/v1/write/logging" % DATAKIT_IP
BASE_ENV = {"DATAKIT_URL": GOOD_URL, "EXPECTED_PROJECT_ID": PROJECT,
            "EXPECTED_SUBSCRIPTION": SUBSCRIPTION}


# ----------------------------------------------------------------- stub DataKit (real HTTP)
class StubDataKit(BaseHTTPRequestHandler):
    status = 200
    location = None
    hang = False
    received = []

    def log_message(self, *a):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        StubDataKit.received.append({"path": self.path, "body": body,
                                     "ctype": self.headers.get("Content-Type")})
        if StubDataKit.hang:
            import time
            time.sleep(fwd.DATAKIT_TIMEOUT + 2)
        self.send_response(StubDataKit.status)
        if StubDataKit.location:
            self.send_header("Location", StubDataKit.location)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")


def start_server(handler_cls):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


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


def push(entry=None, subscription=SUBSCRIPTION, data=None, raw_envelope=None):
    if raw_envelope is not None:
        return raw_envelope
    if data is None:
        data = base64.b64encode(json.dumps(entry).encode("utf-8")).decode("ascii")
    return json.dumps({"subscription": subscription,
                       "message": {"data": data, "messageId": "1", "publishTime": "2026-10-12T07:15:06Z"}})


class HandlerCase(unittest.TestCase):
    """Boots the real adapter handler against the real stub DataKit on loopback."""

    @classmethod
    def setUpClass(cls):
        StubDataKit.status, StubDataKit.location, StubDataKit.hang = 200, None, False
        cls.dk = start_server(StubDataKit)
        cfg = dict(fwd.cfg_from_env(BASE_ENV))
        cfg["dest_ip"], cfg["dest_port"] = "127.0.0.1", cls.dk.server_address[1]
        cls.cfg = cfg
        fwd.Handler.forwarder = fwd.Forwarder(cfg)
        cls.app = start_server(fwd.Handler)
        cls.url = "http://127.0.0.1:%d" % cls.app.server_address[1]

    @classmethod
    def tearDownClass(cls):
        cls.app.shutdown()
        cls.dk.shutdown()

    def setUp(self):
        StubDataKit.status, StubDataKit.location, StubDataKit.hang = 200, None, False
        StubDataKit.received = []

    def post(self, body, path="/pubsub", headers=None):
        req = urllib.request.Request(self.url + path, data=body.encode("utf-8") if isinstance(body, str) else body,
                                     headers=headers or {"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=fwd.DATAKIT_TIMEOUT + 10) as r:
                return r.status
        except urllib.error.HTTPError as e:
            e.read()
            return e.code

    def sent(self):
        self.assertEqual(len(StubDataKit.received), 1, "expected exactly one DataKit write")
        return json.loads(StubDataKit.received[0]["body"].decode("utf-8"))


# ===================================================================== success paths
class TestSuccess(HandlerCase):
    def test_01_json_payload_acked_after_datakit_2xx(self):
        self.assertEqual(self.post(push(logentry())), 204)
        arr = self.sent()
        self.assertEqual(len(arr), 1)
        rec = arr[0]
        self.assertEqual(rec["measurement"], "gcp_pubsub")
        self.assertEqual(rec["fields"]["message"], '{"check_id":"tw-check-001","msg":"hello","n":2}')
        self.assertEqual(rec["fields"]["status"], "info")
        self.assertEqual(StubDataKit.received[0]["path"], "/v1/write/logging")
        self.assertEqual(StubDataKit.received[0]["ctype"], "application/json")

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
        self.assertIsInstance(rec["time"], int)
        self.assertEqual(rec["time"], 1791789304123456789)
        self.assertEqual(rec["fields"]["log_timestamp"], "2026-10-12T07:15:04.123456789Z")
        # The nanosecond digits survive verbatim - this is what float seconds would destroy.
        self.assertTrue(str(rec["time"]).endswith("123456789"))

    def test_04_offset_timestamp_normalised_without_float(self):
        e = logentry(timestamp="2026-10-12T15:15:04.000000007+08:00")
        self.assertEqual(self.post(push(e)), 204)
        self.assertEqual(self.sent()[0]["time"], 1791789304000000007)

    def test_05_insert_id_and_identity_tags_present(self):
        self.assertEqual(self.post(push(logentry())), 204)
        tags = self.sent()[0]["tags"]
        self.assertEqual(tags["google_insert_id"], "abc123xyz")
        self.assertEqual(tags["log_name"], "stdout")
        self.assertEqual(tags["resource_type"], "gce_instance")
        self.assertEqual(tags["project"], PROJECT)
        self.assertEqual(sorted(tags), ["google_insert_id", "log_name", "project", "resource_type", "severity"])

    def test_06_url_encoded_log_name_decoded(self):
        e = logentry(logName="projects/%s/logs/cloudaudit.googleapis.com%%2Factivity" % PROJECT)
        self.assertEqual(self.post(push(e)), 204)
        self.assertEqual(self.sent()[0]["tags"]["log_name"], "cloudaudit.googleapis.com/activity")

    def test_07_no_poc_marker_or_origin_restriction(self):
        """An ordinary log with no marker field and a non-GKE resource is accepted."""
        e = logentry(resource={"type": "cloud_run_revision"}, jsonPayload={"message": "ordinary"})
        self.assertEqual(self.post(push(e)), 204)
        self.assertEqual(self.sent()[0]["tags"]["resource_type"], "cloud_run_revision")

    def test_08_optional_resource_and_receive_timestamp_absent(self):
        self.assertEqual(self.post(push(logentry(resource=fwd, receiveTimestamp=fwd))), 204)
        rec = self.sent()[0]
        self.assertEqual(rec["tags"]["resource_type"], "unknown")
        self.assertNotIn("receive_timestamp", rec["fields"])


# ===================================================================== rejection paths
class TestRejections(HandlerCase):
    def assert_no_write(self, status, expect):
        self.assertEqual(status, expect)
        self.assertEqual(StubDataKit.received, [], "rejected input must never be forwarded")

    def test_10_wrong_subscription_rejected(self):
        self.assert_no_write(self.post(push(logentry(), subscription="projects/%s/subscriptions/other" % PROJECT)), 403)

    def test_11_foreign_project_rejected(self):
        e = logentry(logName="projects/other-project/logs/stdout")
        self.assert_no_write(self.post(push(e)), 403)

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

    def test_25_oversize_mapped_record_rejected(self):
        """Fits the push bound but exceeds the record bound."""
        text = "y" * (fwd.MAX_RECORD_BYTES + 16)
        body = push(logentry(textPayload=text, jsonPayload=fwd))
        self.assertLess(len(body), fwd.MAX_PUSH_BYTES)
        self.assert_no_write(self.post(body), 413)

    def test_26_wrong_path_is_404(self):
        self.assert_no_write(self.post(push(logentry()), path="/storage"), 404)


# ===================================================================== destination failures
class TestDestinationFailures(HandlerCase):
    def test_30_datakit_500_does_not_ack(self):
        StubDataKit.status = 500
        self.assertEqual(self.post(push(logentry())), 502)
        self.assertEqual(len(StubDataKit.received), 1)   # attempted, not acknowledged

    def test_32_redirect_is_refused_not_followed(self):
        StubDataKit.status, StubDataKit.location = 302, "http://169.254.169.254/latest"
        self.assertEqual(self.post(push(logentry())), 502)
        self.assertEqual(len(StubDataKit.received), 1, "the redirect target must never be requested")

    def test_33_timeout_does_not_ack(self):
        StubDataKit.hang = True
        self.assertEqual(self.post(push(logentry())), 502)

    def test_34_unreachable_datakit_does_not_ack(self):
        cfg = dict(self.cfg)
        cfg["dest_port"] = 1          # nothing listens here
        saved = fwd.Handler.forwarder
        fwd.Handler.forwarder = fwd.Forwarder(cfg)
        try:
            self.assertEqual(self.post(push(logentry())), 502)
        finally:
            fwd.Handler.forwarder = saved


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

    def test_40_minimal_config_accepted_without_credentials(self):
        cfg = self.ok()
        self.assertEqual(cfg["measurement"], "gcp_pubsub")
        self.assertEqual((cfg["dest_ip"], cfg["dest_port"], cfg["dest_path"]),
                         (DATAKIT_IP, 9529, "/v1/write/logging"))
        blob = json.dumps(cfg).upper()
        for s in fwd.FORBIDDEN_ENV_SUBSTRINGS:
            self.assertNotIn(s, blob)

    def test_43_missing_required_refused(self):
        for key in ("DATAKIT_URL", "EXPECTED_PROJECT_ID", "EXPECTED_SUBSCRIPTION"):
            with self.subTest(missing=key):
                self.fail(**{key: None})

    def test_44_non_rfc1918_destination_refused(self):
        # 203.0.113.10 is a documentation range that ipaddress.is_private accepts, so this pins the
        # stricter RFC 1918 membership check; 169.254.169.254 is the metadata server.
        for bad in ("http://8.8.8.8:9529/v1/write/logging",
                    "http://203.0.113.10:9529/v1/write/logging",
                    "http://169.254.169.254:9529/v1/write/logging"):
            with self.subTest(url=bad):
                self.fail(DATAKIT_URL=bad)

    def test_45_scheme_port_and_path_are_exact(self):
        for bad in ("https://10.20.30.40:9529/v1/write/logging",
                    "http://10.20.30.40:8080/v1/write/logging",
                    "http://10.20.30.40:9529/v1/write/metrics"):
            with self.subTest(url=bad):
                self.fail(DATAKIT_URL=bad)

    def test_48_query_credentials_and_dns_refused(self):
        for bad in ("http://10.20.30.40:9529/v1/write/logging?token=x",
                    "http://user:pw@10.20.30.40:9529/v1/write/logging",
                    "http://datakit.internal:9529/v1/write/logging"):
            with self.subTest(url=bad):
                self.fail(DATAKIT_URL=bad)

    def test_49_subscription_must_be_a_full_name(self):
        self.fail(EXPECTED_SUBSCRIPTION="truewatch-logs-push")

    def test_50_credential_shaped_env_refuses_start(self):
        for name in ("ENV_DATAWAY", "TRUEWATCH_TOKEN"):
            with self.subTest(name=name):
                self.fail(**{name: "anything"})


# ===================================================================== inbound framing (v02, S04)
class TestFraming(HandlerCase):
    """Raw-socket requests, because urllib cannot send malformed framing. Each case must answer
    within its bound, close the connection, and never write to DataKit."""

    def setUp(self):
        super().setUp()
        self._saved = (fwd.Handler.timeout, fwd.INBOUND_BODY_DEADLINE, fwd.DRAIN_DEADLINE)
        fwd.Handler.timeout, fwd.INBOUND_BODY_DEADLINE, fwd.DRAIN_DEADLINE = 0.5, 1.5, 1.0

    def tearDown(self):
        fwd.Handler.timeout, fwd.INBOUND_BODY_DEADLINE, fwd.DRAIN_DEADLINE = self._saved

    def raw(self, head_lines, body=b"", stall=False, half_close=False, budget=6.0):
        """Send one request; return (status, seconds_to_answer, closed_after_answer)."""
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
        self.assertEqual(StubDataKit.received, [], "nothing may be forwarded")

    def test_60_negative_length(self):
        """The v01 hang: int("-1") then rfile.read(-1) waited for client EOF."""
        self.assert_closed_without_write(self.raw(["Content-Length: -1"], b"{}"), 400, 1.0)

    def test_61_non_digit_length(self):
        # "+12" is accepted by int(); strict framing must not accept it.
        self.assert_closed_without_write(self.raw(["Content-Length: +12"], b"{}"), 400, 1.0)

    def test_62_repeated_length(self):
        self.assert_closed_without_write(
            self.raw(["Content-Length: 2", "Content-Length: 40"], b"{}"), 400, 1.0)

    def test_63_transfer_encoding(self):
        self.assert_closed_without_write(
            self.raw(["Transfer-Encoding: chunked"], b"2\r\n{}\r\n0\r\n\r\n"), 400, 1.0)

    def test_64_truncated_body(self):
        body = push(logentry()).encode()
        self.assert_closed_without_write(
            self.raw(["Content-Length: %d" % (len(body) + 50)], body, half_close=True), 400, 1.0)

    def test_65_stalled_body(self):
        body = push(logentry()).encode()
        self.assert_closed_without_write(
            self.raw(["Content-Length: %d" % (len(body) + 50)], body), 408, 2.5)

    def test_66_oversize_body(self):
        """Declared one byte over the bound, 64 KiB sent, then stalled: 413 inside the drain bound."""
        self.assert_closed_without_write(
            self.raw(["Content-Length: %d" % (fwd.MAX_PUSH_BYTES + 1)], b"x" * 65536), 413, 2.5)

if __name__ == "__main__":
    unittest.main(verbosity=2)
