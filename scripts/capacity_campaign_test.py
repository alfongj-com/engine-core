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

    def test_equal_rate_phases_prevent_alphabetical_capacity_starvation(self):
        clock = Clock()
        sent, dropped, offered = [], [], []
        busy_until = 0
        def dispatch(offer):
            nonlocal busy_until
            offered.append(offer)
            if clock.now + 1e-12 < busy_until:
                return False
            busy_until = clock.now + .05
            sent.append(offer.chain)
            return True
        rates = {"z": 10, "a": 10, "m": 10}
        campaign.open_loop(rates, .9, 0, .001, dispatch,
                          lambda offer, why: dropped.append((offer, why)), clock.clock, clock.sleep)
        # One capacity slot under overload. Simultaneous sorted arrivals would
        # admit only 'a' each period; phase staggering gives each family service.
        admitted = {chain: sent.count(chain) for chain in rates}
        self.assertGreater(min(admitted.values()), 0)
        self.assertLessEqual(max(admitted.values()) - min(admitted.values()), 1)
        phases = campaign.schedule_phases(rates)
        self.assertEqual(len(offered), 27)
        self.assertEqual(len(sent) + len(dropped), 27)
        for chain in rates:
            chain_offers = [offer for offer in offered if offer.chain == chain]
            self.assertEqual([offer.index for offer in chain_offers], list(range(9)))
            for offer in chain_offers:
                self.assertAlmostEqual(offer.scheduled, phases[chain] + offer.index / 10)
                self.assertLess(offer.scheduled, .9)
        self.assertTrue(all(why == "client_capacity" for _, why in dropped))
        self.assertAlmostEqual(clock.now, .9)
        self.assertEqual(campaign.schedule_phases({"only": 10}), {"only": 0})

    def test_100ms_eligible_late_slots_are_bounded_microbursts(self):
        for stalled, expected_dropped in ((.08, []), (.125, [0, 1])):
            clock = Clock()
            clock.now = stalled
            sent, dropped = [], []
            campaign.open_loop({"chain": 50}, .2, 0, .1,
                lambda offer: sent.append((offer.index, offer.scheduled, clock.now)) or True,
                lambda offer, reason: dropped.append((offer.index, reason)), clock.clock, clock.sleep)
            self.assertEqual(dropped, [(index, "schedule_lag") for index in expected_dropped])
            self.assertEqual([index for index, _, _ in sent], [i for i in range(10) if i not in expected_dropped])
            self.assertEqual(len(sent) + len(dropped), 10)
            for index, scheduled, actual in sent:
                self.assertEqual(scheduled, index / 50)
                self.assertLessEqual(actual - scheduled, .1)
            immediate = sum(actual == stalled for _, _, actual in sent)
            self.assertGreater(immediate, 1)
            self.assertLessEqual(immediate, 6)  # floor(50 * .1) + current slot.
            self.assertAlmostEqual(clock.now, .2)

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
                              ({"methods": {"eth_sendRawTransaction": {"errors": 1}}}, "rpc_method_errors"),
                              ({"lost_http_responses": 1}, "rpc_fault_injection"),
                              ({"http_transport": {"failures": [{"type": "OSError", "errno": 49, "count": 1}]}}, "rpc_proxy_transport_errors")):
            result = campaign.qualify_capacity(assessment, exact, proxy)
            self.assertFalse(result["capacity_candidate"])
            self.assertIn(reason, result["disqualifiers"])
        self.assertTrue(assessment["capacity_candidate"])

    def test_fault_classification_preserves_safety_and_durable_fences(self):
        healthy = {"safety_pass": True, "liveness_pass": False, "outcome": "incomplete"}
        for report, expected in (({"journal_halted": True}, "fail_closed_recovery_required"),
                                 ({"durable_chain_halts": ["31337"]}, "finality_checkpoint_conflict")):
            self.assertEqual(campaign.campaign_outcome(healthy, report, execution_error=True), expected)
            # A genuine unsafe outcome must never become an expected safe halt.
            self.assertEqual(campaign.campaign_outcome({"safety_pass": False}, report, True), "unsafe")
        self.assertEqual(campaign.campaign_outcome({}, {"journal_halted": True}, True), "fail_closed_recovery_required")
        self.assertEqual(campaign.campaign_outcome(healthy, {}, True), "error")

    def test_short_window_never_claims_capacity(self):
        result = campaign.rate_assessment(self.samples()[:5], "a", 20, 100)
        self.assertFalse(result["sustainable"])


class SolanaLiveObserverTests(unittest.TestCase):
    """Actual sampling with deterministic status RPC and journal input."""
    def fixture(self, status):
        from types import SimpleNamespace
        instance = campaign.Campaign.__new__(campaign.Campaign)
        instance.started = campaign.time.monotonic()
        instance.phase = "load"
        instance.sample_lock, instance.lock = threading.Lock(), threading.Lock()
        instance.profiles = {"solana": {"family": "solana"}}
        instance.included_signatures, instance.finalized_signatures = set(), set()
        instance.samples = []
        instance.stats, instance.responses = {"solana": {}}, {"solana": {}}
        instance.resources = lambda: {}
        observer = SimpleNamespace(signatures={}, pending_signatures=campaign.OrderedDict())
        observer.poll = lambda: {"solana": {"admitted": len(observer.signatures), "attempted": len(observer.signatures), "terminal": 0}}
        instance.observer = observer
        calls = []
        def call(method, params):
            self.assertEqual(method, "getSignatureStatuses")
            batch, options = params
            self.assertEqual(options, {"searchTransactionHistory": True})
            self.assertGreater(len(batch), 0)
            self.assertLessEqual(len(batch), 256)
            self.assertEqual(len(set(batch)), len(batch))
            calls.append(list(batch))
            return {"value": status(list(batch), len(calls))}
        instance.nodes = {"solana": SimpleNamespace(call=call)}
        instance.proxies = {"solana": SimpleNamespace(snapshot=lambda: {})}
        def add(signatures):
            for signature in signatures:
                if signature not in observer.signatures:
                    observer.pending_signatures[signature] = None
                observer.signatures[signature] = signature
        return instance, calls, add

    def test_continuous_arrivals_revisit_old_signatures_before_intake_stops(self):
        seen = campaign.Counter()
        def status(batch, _):
            result = []
            for signature in batch:
                seen[signature] += 1
                result.append({"confirmationStatus": "finalized" if seen[signature] > 1 else "confirmed"})
            return result
        instance, calls, add = self.fixture(status)
        add(["old0", "old1", "old2"])
        instance.sample()
        for tick in range(4):
            add([f"new{tick}"])
            instance.sample()
        self.assertTrue({"old0", "old1", "old2", "new0", "new1", "new2"} <= instance.finalized_signatures)
        self.assertEqual(set(instance.observer.pending_signatures), {"new3"})
        self.assertEqual(calls[1], ["old0", "old1", "old2", "new0"])
        self.assertEqual(seen["old0"], 2)

    def test_removals_do_not_skip_waiting_signatures_and_wave_is_bounded(self):
        def status(batch, _):
            return [{"confirmationStatus": "finalized" if int(signature[1:]) % 2 == 0 else "confirmed"} for signature in batch]
        instance, calls, add = self.fixture(status)
        add([f"s{i}" for i in range(4100)])
        instance.sample()
        self.assertEqual(len(calls), 8)
        self.assertEqual([s for batch in calls for s in batch], [f"s{i}" for i in range(2048)])
        add(["s4100", "s4101"])
        instance.sample()
        self.assertEqual(len(calls), 16)
        self.assertEqual([s for batch in calls[8:] for s in batch], [f"s{i}" for i in range(2048, 4096)])
        instance.sample()
        self.assertEqual(calls[16][:4], ["s4096", "s4097", "s4098", "s4099"])
        self.assertNotIn("s0", instance.observer.pending_signatures)
        self.assertIn("s1", instance.observer.pending_signatures)
        self.assertEqual(set(instance.observer.signatures), {f"s{i}" for i in range(4102)})
        self.assertLessEqual(len(calls) - 16, 8)

    def test_partial_rpc_failure_keeps_unprocessed_work_for_next_sample(self):
        def status(batch, call_number):
            if call_number == 2:
                raise RuntimeError("temporary fixture failure")
            return [{"confirmationStatus": "finalized"} for _ in batch]
        instance, calls, add = self.fixture(status)
        add([f"s{i}" for i in range(600)])
        with self.assertRaisesRegex(RuntimeError, "temporary fixture"):
            instance.sample()
        self.assertEqual(len(instance.finalized_signatures), 256)
        self.assertEqual(set(instance.observer.pending_signatures), {f"s{i}" for i in range(256, 600)})
        # Failed 256..511 batch rotated; untouched work gets its turn first.
        self.assertEqual(list(instance.observer.pending_signatures)[:88], [f"s{i}" for i in range(512, 600)])
        instance.sample()
        self.assertEqual(calls[2][0], "s512")
        self.assertEqual(len(instance.finalized_signatures), 600)
        self.assertEqual(len(instance.observer.pending_signatures), 0)

    def test_repeated_first_batch_errors_do_not_starve_other_pending_work(self):
        for malformed in (False, True):
            with self.subTest(malformed=malformed):
                def status(batch, _):
                    if malformed: return []
                    raise RuntimeError("temporary fixture failure")
                instance, calls, add = self.fixture(status)
                add([f"s{i}" for i in range(600)])
                for tick in range(3):
                    add([f"new{tick}"])
                    with self.assertRaises(RuntimeError): instance.sample()
                self.assertEqual(len(calls), 3)  # No hidden second wave after errors.
                self.assertTrue({f"s{i}" for i in range(600)} <= {s for batch in calls for s in batch})
                self.assertEqual(len(instance.observer.pending_signatures), 603)
                self.assertFalse(instance.finalized_signatures)

    def test_wrong_status_count_cannot_remove_or_silently_skip_pending_work(self):
        for reply in ([], [{"confirmationStatus": "finalized"}] * 3, {}, [{"confirmationStatus": "finalized"}, 3]):
            with self.subTest(reply=reply):
                instance, calls, add = self.fixture(lambda *_: reply)
                add(["a", "b"])
                with self.assertRaisesRegex(RuntimeError, "invalid signature-status"):
                    instance.sample()
                self.assertEqual(set(instance.observer.pending_signatures), {"a", "b"})
                self.assertFalse(instance.finalized_signatures)

    def test_incremental_journal_enqueues_once_without_reviving_finalized(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.sqlite"
            with sqlite3.connect(path) as db:
                db.executescript("CREATE TABLE admissions(id TEXT,kind TEXT); CREATE TABLE attempts(sequence INTEGER PRIMARY KEY,id TEXT,payload TEXT); CREATE TABLE terminal_evidence(sequence INTEGER PRIMARY KEY,id TEXT,evidence TEXT);")
                db.execute("INSERT INTO attempts VALUES(?,?,?)", (1, "one", json.dumps({"signature": "sig1"})))
            observer = campaign.JournalObserver(path, {"one": "solana", "two": "solana"}, 0, {})
            observer.poll()
            self.assertEqual(list(observer.pending_signatures), ["sig1"])
            del observer.pending_signatures["sig1"]  # Sampling removes finalized work.
            with sqlite3.connect(path) as db:
                db.execute("INSERT INTO attempts VALUES(?,?,?)", (2, "one", json.dumps({"signature": "sig1"})))
                db.execute("INSERT INTO attempts VALUES(?,?,?)", (3, "two", json.dumps({"signature": "sig2"})))
            observer.poll()
            observer.poll()
            self.assertEqual(list(observer.pending_signatures), ["sig2"])
            self.assertEqual(observer.signatures, {"sig1": "one", "sig2": "two"})


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

    def test_http_start_lateness_is_distinct_and_excludes_retries(self):
        from types import SimpleNamespace
        instance = campaign.Campaign.__new__(campaign.Campaign)
        instance.namespace = "fixture"
        instance.infrastructure_stop = None
        instance.profiles = {"evm": {"family": "evm"}}
        instance.fixture = lambda *_: ({}, {})
        instance.base, instance.token = "http://127.0.0.1:1", "fixture-token"
        instance.args = SimpleNamespace(http_timeout=1)
        instance.lock = threading.Lock()
        instance.times, instance.statuses = {}, {}
        instance.stats = {"evm": campaign.Counter()}
        instance.responses = {"evm": campaign.Counter()}
        instance.http_latency = {"evm": []}
        instance.scheduled_latency = {"evm": []}
        instance.http_start_lateness = {"evm": []}
        offer = campaign.Offer("evm", 0, 9.98)
        with patch.object(campaign, "request", return_value=(202, {})) as request_mock, \
                patch.object(campaign.time, "monotonic", side_effect=[10, 10.04, 10.07, 11, 11.04, 11.10]):
            instance.submit(offer)
            instance.submit(offer, retry=True)
        self.assertEqual(request_mock.call_count, 2)
        self.assertEqual(instance.stats["evm"]["retry_dispatched"], 1)
        self.assertEqual(len(instance.http_start_lateness["evm"]), 1)
        self.assertAlmostEqual(instance.http_start_lateness["evm"][0], 60)
        self.assertAlmostEqual(instance.scheduled_latency["evm"][0], 90)
        self.assertEqual(campaign.distribution(instance.http_start_lateness["evm"]),
                         {"count": 1, "p50": 60, "p95": 60, "p99": 60, "max": 60})

    def test_transport_diagnostics_keep_errno_without_secrets(self):
        error = urllib.error.URLError(OSError(49, "secret-token-in-error-message"))
        self.assertEqual(campaign.transport_error_details(error), {"type": "URLError", "cause_type": "OSError", "errno": 49})
        self.assertNotIn("secret", json.dumps(campaign.transport_error_details(error)))

    def test_broadcast_concurrency_cli_reaches_actual_setup_env_without_ambient_override(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = Path(directory) / "engine"
            engine.write_text("fixture")
            common = ["--engine-bin", str(engine), "--report", directory + "/report.json", "--chain", "evm12=1"]
            for extra, expected in (([], "32"), (["--eoa-broadcast-concurrency", "64"], "64"),
                                    (["--eoa-broadcast-concurrency", "128"], "128")):
                args, _ = campaign.arguments(common + extra)
                instance = campaign.Campaign.__new__(campaign.Campaign)
                instance.args, instance.profiles = args, {}
                instance.token, instance.namespace = "fixture", "fixture"
                instance.redis_port, instance.engine_port = 1, 2
                instance.journal = Path(directory) / "journal.sqlite"
                instance.report, instance.initial = {}, {}
                instance.resource_preflight = lambda: None
                instance.start_redis = lambda: None
                seen = []
                instance.engine_command = lambda *_: seen.append(dict(instance.env))
                instance.start_engine = lambda: seen.append(dict(instance.env))
                with patch.dict(os.environ, {"APP__QUEUE__EOA_BROADCAST_CONCURRENCY": "91"}):
                    instance.setup()
                self.assertEqual(len(seen), 2)
                self.assertTrue(all(env["APP__QUEUE__EOA_BROADCAST_CONCURRENCY"] == expected for env in seen))
                self.assertEqual(instance.report["queue_settings"]["APP__QUEUE__EOA_BROADCAST_CONCURRENCY"], expected)
            for invalid in ("0", "129"):
                with self.assertRaises(ValueError):
                    campaign.arguments(common + ["--eoa-broadcast-concurrency", invalid])

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
