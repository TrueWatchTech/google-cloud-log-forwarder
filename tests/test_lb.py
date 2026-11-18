#!/usr/bin/env python3
"""Load balancer request logs: httpRequest -> http_* fields, resource.labels -> resource_* tags.

Reuses the real-handler harness from test_forwarder.py (loopback stub DataKit, no network).

Run:  python3 tests/test_lb.py
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_forwarder import PROJECT, HandlerCase, StubDataKit, fwd, push  # noqa: E402


def lb_entry(**over):
    """Shape of an external Application Load Balancer request log (resource.type http_load_balancer)."""
    e = {"insertId": "1lb2w3e4r5t", "logName": "projects/%s/logs/requests" % PROJECT,
         "resource": {"type": "http_load_balancer",
                      "labels": {"backend_service_name": "web-backend", "forwarding_rule_name": "web-fr",
                                 "project_id": PROJECT, "target_proxy_name": "web-proxy",
                                 "url_map_name": "web-map", "zone": "global"}},
         "httpRequest": {"requestMethod": "GET", "requestUrl": "https://example.com/api/items?page=2",
                         "requestSize": "123", "status": 502, "responseSize": "4567",
                         "userAgent": "curl/8.7.1", "remoteIp": "203.0.113.5", "serverIp": "10.0.0.5",
                         "latency": "0.012345s", "protocol": "HTTP/1.1", "cacheLookup": True},
         "jsonPayload": {"@type": "type.googleapis.com/google.cloud.loadbalancing.type.LoadBalancerLogEntry",
                         "statusDetails": "failed_to_connect_to_backend"},
         "severity": "WARNING", "timestamp": "2026-11-03T10:00:00.123456Z",
         "receiveTimestamp": "2026-11-03T10:00:01.000000Z"}
    for k, v in over.items():
        if v is fwd:
            e.pop(k, None)
        else:
            e[k] = v
    return e


class TestLoadBalancer(HandlerCase):
    def test_70_http_request_mapped_to_fields(self):
        self.assertEqual(self.post(push(lb_entry())), 204)
        f = self.sent()[0]["fields"]
        self.assertEqual(f["http_method"], "GET")
        self.assertEqual(f["http_url"], "https://example.com/api/items?page=2")
        self.assertEqual(f["http_status"], 502)
        self.assertEqual(f["http_request_size"], 123)
        self.assertEqual(f["http_response_size"], 4567)
        self.assertEqual(f["http_user_agent"], "curl/8.7.1")
        self.assertEqual(f["http_remote_ip"], "203.0.113.5")
        self.assertEqual(f["http_server_ip"], "10.0.0.5")
        self.assertEqual(f["http_latency"], "0.012345s")
        self.assertAlmostEqual(f["http_latency_ms"], 12.345)
        self.assertEqual(f["http_protocol"], "HTTP/1.1")
        self.assertIs(f["http_cache_lookup"], True)
        self.assertIn('"statusDetails":"failed_to_connect_to_backend"', f["message"])
        self.assertEqual(f["status"], "warning")

    def test_71_resource_labels_mapped_to_tags(self):
        self.assertEqual(self.post(push(lb_entry())), 204)
        t = self.sent()[0]["tags"]
        self.assertEqual(t["resource_type"], "http_load_balancer")
        self.assertEqual(t["resource_backend_service_name"], "web-backend")
        self.assertEqual(t["resource_forwarding_rule_name"], "web-fr")
        self.assertEqual(t["resource_url_map_name"], "web-map")
        self.assertEqual(t["log_name"], "requests")

    def test_72_latency_forms(self):
        for latency, ms in (("0s", 0.0), ("1.5s", 1500.0), ("2s", 2000.0), ("0.000000001s", 0.000001)):
            with self.subTest(latency=latency):
                e = lb_entry()
                e["httpRequest"]["latency"] = latency
                self.setUp()
                self.assertEqual(self.post(push(e)), 204)
                self.assertAlmostEqual(self.sent()[0]["fields"]["http_latency_ms"], ms)

    def test_73_absent_http_request_adds_no_http_fields(self):
        self.assertEqual(self.post(push(lb_entry(httpRequest=fwd))), 204)
        self.assertFalse([k for k in self.sent()[0]["fields"] if k.startswith("http_")])

    def test_74_label_cannot_overwrite_built_in_tag(self):
        e = lb_entry()
        e["resource"]["labels"]["type"] = "spoofed"
        self.assertEqual(self.post(push(e)), 204)
        self.assertEqual(self.sent()[0]["tags"]["resource_type"], "http_load_balancer")

    def test_75_odd_label_keys_and_values_skipped(self):
        e = lb_entry()
        e["resource"]["labels"].update({"Bad-Key": "x", "n": 1, "long": "v" * (fwd.MAX_LABEL_VALUE + 1)})
        self.assertEqual(self.post(push(e)), 204)
        t = self.sent()[0]["tags"]
        self.assertNotIn("resource_Bad-Key", t)
        self.assertNotIn("resource_n", t)
        self.assertNotIn("resource_long", t)

    def test_76_malformed_http_request_rejected(self):
        bad = ({"httpRequest": "GET /"}, {"httpRequest": {"status": "abc"}},
               {"httpRequest": {"status": True}}, {"httpRequest": {"cacheHit": "yes"}},
               {"httpRequest": {"requestUrl": 5}}, {"httpRequest": {"latency": "12ms"}},
               {"resource": {"type": "http_load_balancer", "labels": ["a"]}})
        for over in bad:
            with self.subTest(over=over):
                self.setUp()
                self.assertEqual(self.post(push(lb_entry(**over))), 400)
                self.assertEqual(StubDataKit.received, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
