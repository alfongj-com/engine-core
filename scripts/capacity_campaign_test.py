#!/usr/bin/env python3
"""Offline harness contracts; no Engine, Redis, validator or paid RPC."""
import json
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import os
import socket
import urllib.error
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

import capacity_campaign as campaign


class Clock:
    def __init__(self): self.now = 0
    def clock(self): return self.now
    def sleep(self, duration): self.now += duration


class SchedulingTests(unittest.TestCase):
    def test_stall_discards_missed_slots_without_catchup_or_retry(self):
        clock = Clock()
        sent, dropped = [], []
        def dispatch(offer):
            sent.append(offer.index)
            if offer.index == 0: clock.now += .55
            return True
        campaign.open_loop({"chain": 10}, 1, 0, .01, dispatch,
                           lambda offer, reason: dropped.append((offer.index, reason)), clock.clock, clock.sleep)
        self.assertEqual(sent, [0, 6, 7, 8, 9])
        self.assertEqual(dropped, [(i, "schedule_lag") for i in range(1, 6)])
        self.assertAlmostEqual(clock.now, 1)

    def test_full_dispatch_has_no_hidden_queue_and_other_chain_still_offered(self):
        clock = Clock()
        offers, drops = [], []
        def dispatch(offer):
            offers.append((offer.chain, offer.index))
            return offer.chain == "fast"
        campaign.open_loop({"slow": 3, "fast": 5}, 1, 0, .01, dispatch,
                           lambda offer, reason: drops.append((offer.chain, offer.index, reason)), clock.clock, clock.sleep)
        self.assertEqual(len(offers), 8)
        self.assertEqual(len(set(offers)), 8)
        self.assertEqual(drops, [("slow", i, "client_capacity") for i in range(3)])

    def test_pool_is_bounded_and_failure_releases_permit(self):
        pool = campaign.BoundedPool(2)
        blocked, release = threading.Event(), threading.Event()
        def task():
            blocked.set()
            release.wait(2)
            raise ValueError("fixture")
        self.assertTrue(pool.submit(task))
        self.assertTrue(blocked.wait(1))
        self.assertTrue(pool.submit(task))
        self.assertFalse(pool.submit(task))
        release.set()
        pool.close()
        self.assertEqual(pool.active, 0)
        self.assertEqual(pool.peak, 2)
        self.assertEqual(pool.failures, ["ValueError", "ValueError"])


class MeasurementTests(unittest.TestCase):
    def samples(self, attempted_rate=100, terminal_rate=100):
        return [{"seconds": time, "phase": "load", "chains": {"a": {
            "admitted": time * 100, "attempted": time * attempted_rate,
            "included": time * attempted_rate, "terminal": max(0, (time - 10) * terminal_rate)}}}
            for time in range(0, 121, 5)]

    def test_draining_everything_cannot_mask_unsigned_backlog_growth(self):
        samples = self.samples(attempted_rate=97, terminal_rate=97)
        samples.append({"seconds": 200, "phase": "drain", "chains": {"a": dict.fromkeys(("admitted", "attempted", "included", "terminal"), 12000)}})
        result = campaign.rate_assessment(samples, "a", 120, 100)
        self.assertFalse(result["sustainable"])
        self.assertEqual(result["unsigned_backlog_start"], 180)
        self.assertEqual(result["unsigned_backlog_end"], 360)
        self.assertEqual(result["unsigned_backlog_linear_trend_per_second"], 3)
        self.assertEqual(result["to_seconds"], 120)

    def test_complete_constant_lag_window_is_only_candidate_input(self):
        result = campaign.rate_assessment(self.samples(), "a", 120, 100)
        self.assertTrue(result["sustainable"])
        self.assertEqual(result["terminal_backlog_start"], 1000)
        self.assertEqual(result["terminal_backlog_end"], 1000)
        self.assertEqual(result["rates_tps"], dict.fromkeys(("admitted", "attempted", "included", "terminal"), 100.0))

    def test_recovered_proxy_failures_still_disqualify_nominal_capacity(self):
        assessment = {"capacity_candidate": True}
        exact = {"safety_pass": True, "liveness_pass": True}
        self.assertTrue(campaign.qualify_capacity(assessment, exact, {})["capacity_candidate"])
        for proxy, reason in (({"proxy_overloads": 1}, "rpc_proxy_connection_limit"),
                              ({"proxy_failures": ["ConnectionResetError"]}, "rpc_proxy_transport_errors"),
                              ({"http_transport": {"failures": [{"type": "OSError", "errno": 49, "count": 1}]}}, "rpc_proxy_transport_errors")):
            result = campaign.qualify_capacity(assessment, exact, proxy)
            self.assertFalse(result["capacity_candidate"])
            self.assertIn(reason, result["disqualifiers"])
        self.assertTrue(assessment["capacity_candidate"])

    def test_short_window_never_claims_capacity(self):
        result = campaign.rate_assessment(self.samples()[:5], "a", 20, 100)
        self.assertFalse(result["sustainable"])


class EvidenceTests(unittest.TestCase):
    def test_publish_is_private_atomic_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            campaign.private_json(path, {"outcome": "incomplete"})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError): campaign.private_json(path, {"outcome": "pass"})
            self.assertEqual(json.loads(path.read_text())["outcome"], "incomplete")
            self.assertEqual(sorted(p.name for p in path.parent.iterdir()), ["report.json"])

    def test_journal_observer_counts_ids_not_fee_bump_attempt_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite"
            with sqlite3.connect(path) as db:
                db.executescript("CREATE TABLE admissions(id TEXT,kind TEXT); CREATE TABLE attempts(sequence INTEGER PRIMARY KEY,id TEXT,payload TEXT); CREATE TABLE terminal_evidence(sequence INTEGER PRIMARY KEY,id TEXT,evidence TEXT);")
                db.executemany("INSERT INTO admissions VALUES(?,?)", [("one", "eoa"), ("two", "solana"), ("unexpected", "eoa")])
                db.executemany("INSERT INTO attempts VALUES(?,?,?)", [(1, "one", "{}"), (2, "one", "{}"), (3, "two", '{"signature":"sig"}')])
                db.execute("INSERT INTO terminal_evidence VALUES(1,'one','{\"outcome\":\"success\"}')")
            observer = campaign.JournalObserver(path, {"one": "evm", "two": "solana"}, 0, {})
            expected = {"evm": {"admitted": 1, "attempted": 1, "terminal": 1}, "solana": {"admitted": 1, "attempted": 1, "terminal": 0}}
            self.assertEqual(observer.poll(), expected)
            self.assertEqual(observer.poll(), expected)
            self.assertEqual(observer.unknown_ids, {"unexpected"})
            self.assertEqual(observer.signatures, {"sig": "two"})

    def test_local_only_urls_reject_secrets_or_nonlocal_hosts(self):
        for url in ("https://127.0.0.1:80", "http://example.com", "http://localhost:80", "http://u:p@127.0.0.1:80"):
            with self.assertRaises(ValueError): campaign.check_loopback(url)
        self.assertEqual(campaign.check_loopback("http://127.0.0.1:1234/"), "http://127.0.0.1:1234/")

    def test_transport_diagnostics_keep_errno_without_secrets(self):
        error = urllib.error.URLError(OSError(49, "secret-token-in-error-message"))
        self.assertEqual(campaign.transport_error_details(error), {"type": "URLError", "cause_type": "OSError", "errno": 49})
        self.assertNotIn("secret", json.dumps(campaign.transport_error_details(error)))

    def test_profile_bounds_and_individual_depths(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = Path(directory) / "engine"
            engine.write_text("fixture")
            common = ["--engine-bin", str(engine), "--report", directory + "/report.json"]
            args, profiles = campaign.arguments(common + ["--chain", "evm12=100", "--chain", "evm2=50", "--depth", "evm2=600"])
            self.assertEqual(profiles["evm12"]["depth"], 2)
            self.assertEqual(profiles["evm2"]["depth"], 600)
            self.assertEqual(profiles["evm2"]["network"], "optimism")
            for options in (["--chain", "evm2=100", "--max-intents", "2"],
                            ["--chain", "evm2=1", "--chaos", "redis-restart", "--redis-fsync", "everysec"],
                            ["--chain", "native=1", "--external-evm", "native=http://example.com", "--chain-id", "native=1"]):
                with self.assertRaises(ValueError): campaign.arguments(common + options)


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.http_patch = patch.object(campaign, "LOCAL_HTTP", campaign.KeepAliveHttp())
        self.http_patch.start()
        self.calls = []
        calls = self.calls
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def log_message(self, *_args): pass
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                calls.append((self.path, self.client_address[1]))
                if self.path == "/drop":
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
                body = b'{"ok":true}'
                self.send_response(302 if self.path == "/redirect" else 200)
                if self.path == "/redirect": self.send_header("Location", "http://example.invalid/secret")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        campaign.LOCAL_HTTP.close()
        self.http_patch.stop()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)

    def test_api_post_connection_reused_without_ambient_proxy(self):
        with patch.dict(os.environ, {"HTTP_PROXY": "http://127.0.0.1:9", "http_proxy": "http://127.0.0.1:9", "NO_PROXY": ""}):
            self.assertEqual(campaign.request(self.url + "/ok", {"id": 1}), (200, {"ok": True}))
            self.assertEqual(campaign.request(self.url + "/ok", {"id": 2}), (200, {"ok": True}))
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(self.calls[0][1], self.calls[1][1])

    def test_lost_accepted_post_is_not_automatically_retried(self):
        with self.assertRaises((http.client.HTTPException, OSError)):
            campaign.request(self.url + "/drop", {"id": 1})
        self.assertEqual([path for path, _ in self.calls], ["/drop"])
        self.assertEqual(campaign.request(self.url + "/ok", {"id": 2})[0], 200)
        self.assertEqual([path for path, _ in self.calls], ["/drop", "/ok"])

    def test_redirect_is_returned_without_following(self):
        self.assertEqual(campaign.request(self.url + "/redirect", {"id": 1}), (302, None))
        self.assertEqual([path for path, _ in self.calls], ["/redirect"])


if __name__ == "__main__": unittest.main()
