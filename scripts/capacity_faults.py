#!/usr/bin/env python3
"""Deterministic, loopback-only capacity faults and independent result checks.

The campaign owns processes, durable stores, chain setup and observation. This
module never signs, creates a new intent, edits Redis, or contacts public RPCs.
A passing safety check does not imply that stalled work recovered: liveness is
reported independently and requires every offered ID and every drain counter.
"""
from __future__ import annotations

import base64
from collections import Counter, deque, OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import http.client
import ipaddress
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import struct
import threading
import time
from typing import Mapping
import urllib.error
import urllib.parse
import urllib.request


class FaultError(RuntimeError):
    pass


def require(condition, message):
    # Assertions must not disappear when a campaign is invoked with python -O.
    if not condition:
        raise FaultError(message)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def loopback_url(url):
    parsed = urllib.parse.urlsplit(url)
    require(parsed.scheme == "http" and parsed.hostname is not None, "local HTTP RPC required")
    require(ipaddress.ip_address(parsed.hostname).is_loopback, "RPC must use a loopback IP literal")
    require(parsed.username is None and parsed.password is None and not parsed.fragment, "RPC credentials/fragments forbidden")
    require(parsed.port is not None, "explicit local RPC port required")
    return url


class Audit:
    """JSONL audit with bounded in-memory tail; full events remain on disk."""
    def __init__(self, path=None):
        self.path = None if path is None else Path(path)
        self.started = time.monotonic()
        self.lock = threading.Lock()
        self.sequence = 0
        self.tail = deque(maxlen=256)
        self.stream = self.path.open("x", encoding="utf-8") if self.path else None

    def record(self, event, **details):
        with self.lock:
            self.sequence += 1
            row = {"sequence": self.sequence, "utc": datetime.now(timezone.utc).isoformat(),
                   "elapsed_seconds": round(time.monotonic() - self.started, 6), "event": event, **details}
            self.tail.append(row)
            if self.stream:
                self.stream.write(json.dumps(row, sort_keys=True) + "\n")
                self.stream.flush()
            return row

    def snapshot(self):
        with self.lock:
            return {"event_count": self.sequence, "path": str(self.path) if self.path else None,
                    "tail": list(self.tail), "tail_truncated": self.sequence > len(self.tail)}

    def close(self):
        with self.lock:
            if self.stream:
                self.stream.close()
                self.stream = None


@dataclass(frozen=True)
class FaultPlan:
    # Drop only the first accepted response of each of the first N unique wires.
    # Rejected sends and retries of one wire cannot exhaust the intended trigger.
    drop_accepted_sends: int = 0
    rpc_errors: Mapping[str, int] = field(default_factory=dict)
    latency_ms: Mapping[str, int] = field(default_factory=dict)
    after_accepted: int = 0
    max_unique_wires: int = 1_000_000
    max_inflight: int = 256

    def __post_init__(self):
        require(self.drop_accepted_sends >= 0 and self.after_accepted >= 0, "negative fault count")
        require(self.max_unique_wires > 0 and 1 <= self.max_inflight <= 1024, "invalid proxy limits")
        require(all(isinstance(v, int) and 0 <= v <= 1_000_000 for v in self.rpc_errors.values()), "invalid error count")
        require(all(isinstance(v, int) and 0 <= v <= 5000 for v in self.latency_ms.values()), "invalid latency")


def safe_http_error(error):
    """Diagnostic categories only: never stringify URLs, headers or payloads."""
    cause = error
    for _ in range(4):
        nested = getattr(cause, "reason", None) or getattr(cause, "__cause__", None)
        if not isinstance(nested, BaseException):
            break
        cause = nested
    number = getattr(cause, "errno", None)
    return {"type": type(error).__name__, "cause_type": type(cause).__name__,
            "errno": number if isinstance(number, int) else None}


class _NoDelayConnection(http.client.HTTPConnection):
    def connect(self):
        super().connect()
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


class KeepAliveHttp:
    """Thread-owned bounded HTTP/1.1 sockets, without proxy/redirect/POST retry.

    An error discards its socket and propagates. Only the caller's next explicit
    request can connect again. This avoids http-client adapters that replay a
    lost accepted POST. Clients must close() after joining their worker threads;
    short-lived HTTP server threads call close_thread() before exiting.
    """
    def __init__(self, max_origins=16):
        require(1 <= max_origins <= 64, "invalid per-thread origin bound")
        self.max_origins = max_origins
        self.local = threading.local()
        self.lock = threading.Lock()
        self.connections = set()
        self.counters = Counter()
        self.failures = Counter()
        self.closed = False

    def _discard(self, cache, key):
        connection = cache.pop(key, None)
        if connection:
            connection.close()
            with self.lock:
                self.connections.discard(connection)

    @staticmethod
    def _idle_socket_unusable(sock):
        # Only inspect between fully consumed responses, before any bytes of
        # the next request. An idle HTTP/1.1 socket has no expected incoming
        # bytes: readable EOF, reset, or stray response data all require a fresh
        # connection. Never retry request()/getresponse() after they start.
        try:
            readable, _, exceptional = select.select([sock], [], [sock], 0)
            if exceptional:
                return True
            if not readable:
                return False
            sock.recv(1, socket.MSG_PEEK | getattr(socket, "MSG_DONTWAIT", 0))
            return True
        except BlockingIOError:
            return False
        except (OSError, ValueError):
            return True

    def request(self, url, body=None, headers=None, timeout=10, max_response_bytes=16 * 1024 * 1024):
        loopback_url(url)
        require(body is None or isinstance(body, bytes), "HTTP body must be bytes")
        require(0 < timeout <= 60 and 0 < max_response_bytes <= 16 * 1024 * 1024, "invalid HTTP bounds")
        parsed = urllib.parse.urlsplit(url)
        key = (parsed.hostname, parsed.port)
        path = (parsed.path or "/") + (("?" + parsed.query) if parsed.query else "")
        cache = getattr(self.local, "connections", None)
        if cache is None:
            cache = self.local.connections = OrderedDict()
        with self.lock:
            require(not self.closed, "HTTP client is closed")
            self.counters["requests"] += 1
            self.counters["active"] += 1
            self.counters["peak_active"] = max(self.counters["peak_active"], self.counters["active"])
        try:
            connection = cache.get(key)
            if connection is not None and connection.sock is not None and self._idle_socket_unusable(connection.sock):
                self._discard(cache, key)
                connection = None
                with self.lock:
                    self.counters["stale_idle_connections"] += 1
            if connection is None:
                if len(cache) >= self.max_origins:
                    self._discard(cache, next(iter(cache)))
                connection = _NoDelayConnection(parsed.hostname, parsed.port, timeout=timeout)
                cache[key] = connection
                with self.lock:
                    self.connections.add(connection)
            cache.move_to_end(key)
            connection.timeout = timeout
            if connection.sock is None:
                with self.lock:
                    self.counters["connection_attempts"] += 1
                connection.connect()
                with self.lock:
                    self.counters["connections_opened"] += 1
            else:
                connection.sock.settimeout(timeout)
                with self.lock:
                    self.counters["connection_reuses"] += 1
            # http.client never redirects or consults ambient proxy variables.
            # Its request/getresponse pair does not retry a failed POST.
            connection.request("GET" if body is None else "POST", path, body=body,
                               headers={"Content-Type": "application/json", **(headers or {})})
            response = connection.getresponse()
            declared = response.getheader("Content-Length")
            require(declared is None or 0 <= int(declared) <= max_response_bytes, "response Content-Length exceeds cap")
            raw = response.read(max_response_bytes + 1)
            require(len(raw) <= max_response_bytes, "streamed response exceeds cap")
            require(response.chunked or declared is None or len(raw) == int(declared),
                    "truncated HTTP response body")
            with self.lock:
                self.counters["responses"] += 1
                self.counters[f"status_{response.status}"] += 1
            return response.status, raw
        except Exception as error:
            self._discard(cache, key)
            detail = safe_http_error(error)
            with self.lock:
                self.failures[json.dumps(detail, sort_keys=True)] += 1
            raise
        finally:
            with self.lock:
                self.counters["active"] -= 1

    def snapshot(self):
        with self.lock:
            return {**dict(self.counters), "owned_connections": len(self.connections),
                    "failures": [{**json.loads(key), "count": count} for key, count in sorted(self.failures.items())]}

    def close_thread(self):
        cache = getattr(self.local, "connections", {})
        for key in list(cache):
            self._discard(cache, key)

    def close(self):
        with self.lock:
            self.closed = True
            connections = list(self.connections)
            self.connections.clear()
        for connection in connections:
            connection.close()


class RpcFaultProxy:
    """Instrumented single/batch JSON-RPC forwarder with finite deterministic faults.

    Accepted means a well-formed success response from the actual upstream node,
    never a locally invented hash. A lost response closes the socket *after* the
    upstream result is recorded. This does not itself prove canonical execution.
    """
    def __init__(self, upstream, plan=None, audit_path=None, port=0, preflight_bypass_ids=()):
        self.upstream = loopback_url(upstream)
        self.plan = plan or FaultPlan()
        self.audit = Audit(audit_path)
        self.condition = threading.Condition()
        self.errors_left = Counter(self.plan.rpc_errors)
        self.calls = Counter()
        self.method_stats = {}
        self.preflight_bypass_ids = frozenset(preflight_bypass_ids)
        self.preflight_overrides = 0
        self.wires = {}
        self.dropped = 0
        self.lost_http_responses = 0
        self.accepted_responses_lost = 0
        self.forwarded_sends = 0
        self.overloads = 0
        self.failures = []
        self.failure_details = Counter()
        self.active = 0
        self.http_requests = 0
        self.upstream_http_requests = 0
        self.incoming_batches = 0
        self.peak_active = 0
        self.active_connections = 0
        self.peak_connections = 0
        self.enabled = True
        self.http = KeepAliveHttp()
        self.slots = threading.BoundedSemaphore(self.plan.max_inflight)
        owner = self

        class Server(ThreadingHTTPServer):
            daemon_threads = True
            request_queue_size = 2048

            def process_request(self, request, client_address):
                if not owner.slots.acquire(blocking=False):
                    with owner.condition:
                        owner.overloads += 1
                    request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    self.shutdown_request(request)
                    return
                with owner.condition:
                    owner.active_connections += 1
                    owner.peak_connections = max(owner.peak_connections, owner.active_connections)
                super().process_request(request, client_address)

            def process_request_thread(self, request, client_address):
                try:
                    super().process_request_thread(request, client_address)
                finally:
                    owner.http.close_thread()
                    with owner.condition:
                        owner.active_connections -= 1
                    owner.slots.release()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def setup(self):
                super().setup()
                self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                self.connection.settimeout(15)

            def log_message(self, *_args):
                pass

            def do_POST(self):
                self.connection.settimeout(15)
                with owner.condition:
                    owner.active += 1
                    owner.http_requests += 1
                    owner.peak_active = max(owner.peak_active, owner.active)
                try:
                    require(self.path == "/" and self.headers.get("Origin") is None, "invalid proxy request origin/path")
                    require(self.headers.get("Host") == f"127.0.0.1:{owner.server.server_port}", "invalid proxy Host")
                    length = int(self.headers.get("Content-Length", "0"))
                    require(0 < length <= 2 * 1024 * 1024, "invalid RPC request size")
                    payload = json.loads(self.rfile.read(length))
                    batch = isinstance(payload, list)
                    if batch:
                        with owner.condition:
                            owner.incoming_batches += 1
                    calls = payload if batch else [payload]
                    require(0 < len(calls) <= 1024, "invalid RPC batch")
                    results, drop = [], False
                    for call in calls:
                        result, lost = owner._call(call)
                        results.append(result)
                        drop |= lost
                    if drop:
                        lost_identities = [result["result"] for call, result in zip(calls, results)
                                           if call["method"] in ("eth_sendRawTransaction", "sendTransaction") and "result" in result]
                        with owner.condition:
                            owner.lost_http_responses += 1
                            owner.accepted_responses_lost += len(lost_identities)
                        owner.audit.record("http_response_lost", batch=batch, accepted_identities=lost_identities)
                        # No HTTP success or fabricated RPC error reaches Engine.
                        self.close_connection = True
                        try:
                            self.connection.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        return
                    encoded = json.dumps(results if batch else results[0]).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)
                except Exception as error:
                    with owner.condition:
                        owner.failures.append(type(error).__name__)
                        owner.failure_details[json.dumps(safe_http_error(error), sort_keys=True)] += 1
                    try:
                        self.send_error(502)
                    except OSError:
                        pass
                finally:
                    with owner.condition:
                        owner.active -= 1

        self.server = Server(("127.0.0.1", port), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @staticmethod
    def _wire(call):
        if call["method"] == "eth_sendRawTransaction":
            require(len(call.get("params", [])) == 1, "invalid EVM send params")
            value = call["params"][0]
            require(isinstance(value, str) and value.startswith("0x"), "invalid EVM wire")
            wire = bytes.fromhex(value[2:])
        elif call["method"] == "sendTransaction":
            params = call.get("params", [])
            require(len(params) == 2 and params[1].get("encoding") == "base64", "explicit Solana base64 encoding required")
            require(params[1].get("maxRetries") == 0, "node-hidden Solana retries forbidden")
            wire = base64.b64decode(params[0], validate=True)
        else:
            return None
        require(bool(wire), "empty transaction wire")
        return hashlib.sha256(wire).hexdigest()

    def _call(self, call):
        started = time.monotonic()
        method = call.get("method", "invalid") if isinstance(call, dict) else "invalid"
        failed = True
        try:
            result, dropped = self._forward(call)
            failed = "error" in result
            return result, dropped
        finally:
            duration = (time.monotonic() - started) * 1000
            with self.condition:
                stats = self.method_stats.setdefault(method, {"count": 0, "errors": 0, "total_ms": 0, "max_ms": 0})
                stats["count"] += 1
                stats["errors"] += int(failed)
                stats["total_ms"] += duration
                stats["max_ms"] = max(stats["max_ms"], duration)

    def _forward(self, call):
        require(isinstance(call, dict) and call.get("jsonrpc") == "2.0" and "id" in call, "invalid JSON-RPC envelope")
        method = call.get("method")
        require(isinstance(method, str), "invalid RPC method")
        wire = self._wire(call)
        with self.condition:
            self.calls[method] += 1
            armed = self.enabled and len(self.wires) >= self.plan.after_accepted
            inject_error = armed and self.errors_left[method] > 0
            if inject_error:
                self.errors_left[method] -= 1
            latency = self.plan.latency_ms.get(method, 0) if armed else 0
        if latency:
            time.sleep(latency / 1000)
        if inject_error:
            self.audit.record("rpc_error_injected", method=method, forwarded=False)
            return {"jsonrpc": "2.0", "id": call["id"], "error": {"code": -32005, "message": "capacity fixture transient error"}}, False
        if wire:
            with self.condition:
                self.forwarded_sends += 1
            self.audit.record("send_forwarded", method=method, wire_digest=wire)
        if wire and method == "sendTransaction" and self.preflight_bypass_ids:
            decoded = decode_solana_wire(call["params"][0])
            if decoded["id"] in self.preflight_bypass_ids:
                require(decoded["has_invalid_instruction"], "preflight bypass requires designated malformed fixture")
                call = {**call, "params": [call["params"][0], {**call["params"][1], "skipPreflight": True}]}
                with self.condition:
                    self.preflight_overrides += 1
                self.audit.record("injected_preflight_bypass", intent=decoded["id"], wire_digest=wire)
        body = json.dumps(call).encode()
        with self.condition:
            self.upstream_http_requests += 1
        status, raw = self.http.request(self.upstream, body, timeout=15)
        require(status == 200, "upstream returned non-200 status")
        result = json.loads(raw)
        require(isinstance(result, dict) and result.get("jsonrpc") == "2.0" and result.get("id") == call["id"], "mismatched upstream response")
        require(("result" in result) != ("error" in result), "ambiguous upstream response")
        lost = False
        if wire and "result" in result:
            identity = result["result"]
            require(isinstance(identity, str) and bool(identity), "invalid accepted identity")
            if method == "eth_sendRawTransaction":
                require(len(identity) == 66 and identity.startswith("0x") and len(bytes.fromhex(identity[2:])) == 32, "invalid accepted EVM hash")
            if method == "sendTransaction":
                decoded = decode_solana_wire(call["params"][0])
                require(identity == decoded["signature"], "accepted signature differs from exact wire")
            with self.condition:
                old = self.wires.get(wire)
                require(old is None or old["identity"] == identity, "same wire returned conflicting identities")
                if old is None:
                    require(len(self.wires) < self.plan.max_unique_wires, "proxy unique-wire limit reached")
                    self.wires[wire] = {"identity": identity, "accepted_responses": 1}
                    lost = self.enabled and len(self.wires) > self.plan.after_accepted and self.dropped < self.plan.drop_accepted_sends
                    self.dropped += int(lost)
                else:
                    old["accepted_responses"] += 1
                self.audit.record("send_accepted", method=method, wire_digest=wire, identity=identity, response_dropped=lost)
                self.condition.notify_all()
        return result, lost

    def wait_for_accepted(self, count, timeout=60):
        require(count > 0, "fault trigger must follow a real accepted send")
        deadline = time.monotonic() + timeout
        with self.condition:
            while len(self.wires) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise FaultError(f"fault trigger did not reach {count} unique accepted sends")
                self.condition.wait(remaining)
            return len(self.wires)

    def release(self):
        with self.condition:
            self.enabled = False
        self.audit.record("rpc_faults_released")

    def snapshot(self, include_wires=False):
        with self.condition:
            result = {"calls": dict(self.calls), "accepted_unique_wires": len(self.wires),
                    "forwarded_sends": self.forwarded_sends, "dropped_responses": self.dropped,
                    "lost_http_responses": self.lost_http_responses, "accepted_responses_lost": self.accepted_responses_lost,
                    "proxy_overloads": self.overloads, "peak_inflight": self.peak_active, "active": self.active,
                    "connection_limit": self.plan.max_inflight,
                    "active_connections": self.active_connections, "peak_connections": self.peak_connections,
                    "http_requests": self.http_requests, "incoming_batches": self.incoming_batches,
                    "upstream_http_requests": self.upstream_http_requests,
                    "proxy_failures": list(self.failures),
                    "proxy_failure_details": [{**json.loads(key), "count": count} for key, count in sorted(self.failure_details.items())],
                    "http_transport": self.http.snapshot(), "audit": self.audit.snapshot(),
                    "methods": {k: dict(v) for k, v in self.method_stats.items()},
                    "injected_preflight_bypass": self.preflight_overrides}
            if include_wires:
                result["accepted_wires"] = {k: dict(v) for k, v in self.wires.items()}
            else:
                result["audit"].pop("tail", None)
            return result

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.http.close()
        self.audit.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()


def run_triggered_fault(proxy, after_accepted, name, steps, timeout=60):
    """Caller-supplied process/node actions, triggered by actual upstream success.

    Engine restart steps: kill, start-with-same-journal-and-Redis, healthy.
    Redis restart: restart-Redis, assert-writes-fail-closed, stop-Engine,
    explicit-offline-reattach, restart-Engine, healthy. Never auto-initialize.
    Reorg: assert-provisional-not-terminal, rollback-snapshot, assert-orphaned,
    assert-still-not-terminal, resume-mining. The campaign owns depth/snapshot.
    A raised step fails the scenario; later steps are not silently attempted.
    """
    observed = proxy.wait_for_accepted(after_accepted, timeout)
    proxy.audit.record("fault_triggered", fault=name, accepted_unique_wires=observed)
    for label, action in steps:
        proxy.audit.record("fault_step_started", fault=name, step=label)
        try:
            action()
        except Exception as error:
            proxy.audit.record("fault_step_failed", fault=name, step=label, error_type=type(error).__name__)
            raise
        proxy.audit.record("fault_step_completed", fault=name, step=label)
    proxy.audit.record("fault_completed", fault=name)


DRAIN_FIELDS = ("client_pending", "http_inflight", "redis_pending", "redis_borrowed", "redis_submitted",
                "redis_active", "redis_delayed", "journal_unresolved", "node_pending")


def evaluate_campaign(expected, observations, drain):
    """Check normalized *independent* journal/node/Engine observations per ID.

    The adapter must decode actual signed/chain payloads for intent_digest, query
    canonical/finality evidence independently, and enumerate all chain effects
    and journal IDs. Copying expected values into observations invalidates this
    oracle. Include rejected/missing offers in expected; never shrink the offered
    workload to fit completed transactions. Orphaned executions remain in history
    with canonical=False and cannot justify terminal status.
    """
    safety, liveness = [], []
    parked_allowance = Counter()
    parked_count = 0
    identity_owners, replay_owners = {}, {}
    ids = [item["id"] for item in expected]
    require(len(ids) == len(set(ids)), "duplicate expected ID")
    offered = set(ids)
    for extra in set(observations) - offered:
        safety.append(f"{extra}: unexpected durable/chain intent")
    for intent in expected:
        txid = intent["id"]
        actual = observations.get(txid)
        if actual is None or actual.get("admitted") is not True:
            liveness.append(f"{txid}: offered intent not admitted")
            if actual is None:
                continue
        attempts = actual.get("attempts", [])
        replay = {a.get("replay_key") for a in attempts}
        if attempts and (None in replay or len(replay) != 1):
            safety.append(f"{txid}: replay identity changed or absent")
        identities = {}
        for attempt in attempts:
            identity, wire = attempt.get("identity"), attempt.get("wire_digest")
            if not identity or not wire:
                safety.append(f"{txid}: incomplete signed-attempt evidence")
            if identity in identities and identities[identity] != wire:
                safety.append(f"{txid}: one identity represents different wire bytes")
            identities[identity] = wire
            for owners, key, label in [
                (identity_owners, (intent["family"], str(intent["chain"]), identity), "signed identity"),
                (replay_owners, (str(intent["chain"]), attempt.get("replay_key")), "replay reservation"),
            ]:
                previous = owners.setdefault(key, txid)
                if previous != txid:
                    safety.append(f"{txid}: {label} already attributed to {previous}")
            if attempt.get("wire_replay_key") != attempt.get("replay_key") or not attempt.get("wire_replay_key"):
                safety.append(f"{txid}: durable replay key differs from signed wire")
            if attempt.get("intent_digest") != intent["intent_digest"]:
                safety.append(f"{txid}: signed payload changed admitted intent")
        if intent["family"] == "solana" and len(identities) > 1:
            safety.append(f"{txid}: Solana retry created a fresh signature")
        executions = actual.get("executions", [])
        if actual.get("admitted") is not True and (attempts or executions or actual.get("terminal")):
            safety.append(f"{txid}: side effect without durable admission")
        canonical = [e for e in executions if e.get("canonical") is True]
        unique = {e.get("identity") for e in canonical}
        if len(unique) > 1 or len(canonical) > 1:
            safety.append(f"{txid}: duplicate canonical execution")
        for execution in executions:
            if execution.get("identity") not in identities:
                safety.append(f"{txid}: execution lacks durable signed attempt")
            if execution.get("outcome") not in ("success", "revert"):
                safety.append(f"{txid}: unrecognized execution outcome")
            if execution.get("canonical") is True:
                if execution.get("outcome") != intent["outcome"]:
                    safety.append(f"{txid}: wrong chain outcome")
                if execution.get("effects") != intent["effects"]:
                    safety.append(f"{txid}: wrong chain effects")
                fee = execution.get("fee")
                if not isinstance(fee, int) or isinstance(fee, bool) or fee < 0 or fee > intent.get("max_fee", fee):
                    safety.append(f"{txid}: invalid or excessive fee")
        terminal = actual.get("terminal")
        finalized = [e for e in canonical if e.get("finalized") is True]
        if terminal is not None:
            match = [e for e in finalized if e.get("identity") == terminal.get("identity")]
            if len(match) != 1 or terminal.get("outcome") != intent["outcome"] or (match and terminal.get("outcome") != match[0].get("outcome")):
                safety.append(f"{txid}: terminal result lacks matching finalized chain outcome")
        if intent.get("expected_to_park"):
            parked_count += 1
            if terminal is not None or canonical:
                safety.append(f"{txid}: expected unresolved rejection became terminal/executed")
            retained = actual.get("retained", {})
            state = retained.get("queue_state")
            if (retained.get("signed_attempt") is not True or retained.get("journal_state") != "admitted"
                    or state not in ("redis_pending", "redis_active", "redis_delayed") or len(identities) != 1):
                liveness.append(f"{txid}: missing exact retained rejected-attempt evidence")
            else:
                parked_allowance[state] += 1
                parked_allowance["journal_unresolved"] += 1
        elif terminal is None or len(finalized) != 1:
            liveness.append(f"{txid}: not completely settled")
        if not attempts:
            liveness.append(f"{txid}: no durable attempt")
    for field in DRAIN_FIELDS:
        value = drain.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value != parked_allowance[field]:
            liveness.append(f"drain {field}: {value!r}")
    return {"safety_pass": not safety, "liveness_pass": not liveness,
            "outcome": ("expected_parked" if parked_count else "pass") if not safety and not liveness else "unsafe" if safety else "incomplete",
            "eligible_for_rate_assessment": not safety and not liveness and parked_count == 0,
            "expected_parked_intents": parked_count,
            "offered_intents": len(expected), "observed_intents": len(observations),
            "safety_failures": safety, "liveness_failures": liveness}


# Increment calldata-selected storage slot, then STOP / revert all effects.
EVM_COUNTER_CODE = "0x6000358054600101905500"
EVM_REVERT_CODE = "0x6000358054600101905560006000fd"


def evm_fixture(txid, chain_id, sender, recipient, counter, reverting, index, kind):
    require(kind in ("transfer", "storage", "revert") and index >= 0, "invalid EVM fixture")
    slot = index + 1
    to = recipient if kind == "transfer" else counter if kind == "storage" else reverting
    data = "0x" if kind == "transfer" else "0x" + slot.to_bytes(32, "big").hex()
    value = 1 if kind == "transfer" else 0
    fields = {"chain": chain_id, "sender": sender.lower(), "to": to.lower(), "value": value, "data": data}
    payload = {"executionOptions": {"chainId": chain_id, "type": "EOA", "from": sender, "idempotencyKey": txid},
               "params": [{"to": to, "data": data, "value": hex(value), "gasLimit": 21000 if kind == "transfer" else 100000}]}
    effects = {f"balance:{recipient.lower()}": value} if kind == "transfer" else {f"storage:{counter.lower()}:{slot}": 1} if kind == "storage" else {}
    return payload, {"id": txid, "chain": chain_id, "family": "evm", "outcome": "revert" if kind == "revert" else "success",
                     "effects": effects, "intent_digest": digest(fields), "intent_fields": fields, "fixture": kind, "slot": slot}


def solana_fixture(txid, payer, recipient, kind, lamports=1000):
    require(kind in ("transfer", "multi_transfer", "preflight_reject"), "invalid Solana fixture")
    require(0 < lamports < 2**64, "invalid lamports")
    def transfer(amount):
        return {"programId": "11111111111111111111111111111111", "accounts": [
            {"pubkey": payer, "isSigner": True, "isWritable": True},
            {"pubkey": recipient, "isSigner": False, "isWritable": True}],
            "data": base64.b64encode(struct.pack("<IQ", 2, amount)).decode(), "encoding": "base64"}
    instructions = [transfer(lamports)]
    if kind == "multi_transfer":
        instructions.append(transfer(lamports + 1))
    elif kind == "preflight_reject":
        # Valid wire, invalid System instruction discriminant. With Engine's
        # mandatory preflight this is expected to park, not finalized failure.
        instructions[0]["data"] = base64.b64encode(struct.pack("<I", 0xFFFFFFFF)).decode()
    fields = {"chain": "solana:local", "payer": payer, "instructions": instructions}
    payload = {"idempotencyKey": txid, "instructions": instructions,
               "executionOptions": {"signerAddress": payer, "chainId": "solana:local", "commitment": "finalized"}}
    return payload, {"id": txid, "chain": "solana:local", "family": "solana", "outcome": "revert" if kind == "preflight_reject" else "success",
                     "effects": {} if kind == "preflight_reject" else {f"balance:{recipient}": lamports * 2 + 1 if kind == "multi_transfer" else lamports},
                     "intent_digest": digest(fields), "intent_fields": fields, "fixture": kind,
                     "expected_to_park": kind == "preflight_reject"}


BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
SYSTEM_PROGRAM = "11111111111111111111111111111111"
MEMO_PROGRAM = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
COUNTER_ADDRESS = "0x2222222222222222222222222222222222222222"
REVERT_ADDRESS = "0x3333333333333333333333333333333333333333"


def b58encode(value):
    number, encoded = int.from_bytes(value, "big"), ""
    while number:
        number, digit = divmod(number, 58)
        encoded = BASE58[digit] + encoded
    return "1" * (len(value) - len(value.lstrip(b"\0"))) + encoded


def decode_solana_wire(encoded, expected_txid=None):
    """Decode this fixture's v0 System instructions and unique Engine memo.

    This checks message intent independently; signature cryptography is verified
    by the local validator. Reject lookup tables and extra programs rather than
    inferring an effect from an incomplete account list.
    """
    wire = base64.b64decode(encoded, validate=True)
    require(len(wire) <= 1232, "oversized Solana fixture transaction")
    offset = 0

    def take(size):
        nonlocal offset
        require(0 <= size <= len(wire) - offset, "truncated Solana transaction")
        result = wire[offset:offset + size]
        offset += size
        return result

    def shortvec():
        value = 0
        for shift in (0, 7, 14):
            byte = take(1)[0]
            value |= (byte & 127) << shift
            if not byte & 128:
                require(shift == 0 or byte != 0, "noncanonical shortvec")
                return value
        raise FaultError("oversized shortvec")

    require(shortvec() == 1, "fixture must have exactly one signer")
    signature = b58encode(take(64))
    version = take(1)[0]
    if version & 128:
        require(version == 128, "unsupported message version")
        signers, readonly_signed, readonly_unsigned = take(3)
    else:
        signers = version
        readonly_signed, readonly_unsigned = take(2)
    require(signers == 1 and readonly_signed == 0, "unexpected signer permissions")
    count = shortvec()
    require(count >= 3 and readonly_unsigned <= count - signers, "invalid account header")
    accounts = [b58encode(take(32)) for _ in range(count)]
    blockhash = b58encode(take(32))
    instructions, memo, invalid = [], None, False
    for _ in range(shortvec()):
        program = take(1)[0]
        indices, data = list(take(shortvec())), take(shortvec())
        require(program < count and all(index < count for index in indices), "invalid account index")
        if accounts[program] == MEMO_PROGRAM:
            require(memo is None and not indices, "unexpected memo accounts or extra memo")
            memo = data.decode("utf-8")
            continue
        require(accounts[program] == SYSTEM_PROGRAM and indices == [0, 1], "unexpected fixture instruction")
        this_invalid = data == struct.pack("<I", 0xFFFFFFFF)
        invalid |= this_invalid
        require(this_invalid or (len(data) == 12 and data[:4] == struct.pack("<I", 2)), "unexpected System instruction")
        instructions.append({"programId": SYSTEM_PROGRAM, "accounts": [
            {"pubkey": accounts[index], "isSigner": index < signers,
             "isWritable": index < signers - readonly_signed if index < signers else index < count - readonly_unsigned}
            for index in indices], "data": base64.b64encode(data).decode(), "encoding": "base64"})
    require((version < 128 or shortvec() == 0) and offset == len(wire), "lookup tables or trailing bytes forbidden")
    require(memo is not None and memo.startswith("thirdweb-engine:") and instructions, "missing unique Engine memo/instructions")
    identity = memo.removeprefix("thirdweb-engine:")
    require(bool(identity), "empty fixture ID")
    require(expected_txid is None or identity == expected_txid, "signed memo does not match admitted ID")
    fields = {"chain": "solana:local", "payer": accounts[0], "instructions": instructions}
    return {"id": identity, "signature": signature, "blockhash": blockhash,
            "intent_fields": fields, "intent_digest": digest(fields),
            "wire_digest": hashlib.sha256(wire).hexdigest(), "has_invalid_instruction": invalid}


def evm_intent_fields(transaction):
    """Normalize actual eth_getTransactionByHash data, never the request copy."""
    return {"chain": int(transaction["chainId"], 16), "sender": transaction["from"].lower(),
            "to": transaction["to"].lower(), "value": int(transaction["value"], 16),
            "data": transaction.get("input", transaction.get("data", "0x")).lower()}


def build_evm_fixture(chain_id, sender, recipient, id, index, mixed=False):
    kind = ("transfer", "storage", "revert")[index % 3] if mixed else "transfer"
    payload, expected = evm_fixture(id, chain_id, sender, recipient, COUNTER_ADDRESS, REVERT_ADDRESS, index, kind)
    return {"path": "/v1/write/transaction", "payload": payload, "expected": expected}


def evm_contract_setup():
    return [("anvil_setCode", [COUNTER_ADDRESS, EVM_COUNTER_CODE]),
            ("anvil_setCode", [REVERT_ADDRESS, EVM_REVERT_CODE])]


def build_solana_fixture(payer, recipient, id, index, mixed=False, rejection=False, injected_failure=False):
    kind = "preflight_reject" if rejection or injected_failure else "multi_transfer" if mixed and index % 2 else "transfer"
    payload, expected = solana_fixture(id, payer, recipient, kind)
    if injected_failure:
        expected["expected_to_park"] = False
        expected["fixture"] = "injected_preflight_bypass_failure"
    return {"path": "/v1/solana/transaction", "payload": payload, "expected": expected}


def decode_solana_intent(wire_base64, expected_txid):
    decoded = decode_solana_wire(wire_base64)
    require(decoded["id"] == expected_txid, "signed memo does not match admitted ID")
    return decoded["intent_fields"]


def _quantity(value):
    result = int(value, 16) if isinstance(value, str) else value
    require(isinstance(result, int) and not isinstance(result, bool) and result >= 0, "invalid EVM quantity")
    return result


def _rlp_encode(value):
    if isinstance(value, list):
        body = b"".join(_rlp_encode(item) for item in value)
        short, long = 0xC0, 0xF7
    else:
        body = value.to_bytes((value.bit_length() + 7) // 8, "big") if isinstance(value, int) else value
        require(isinstance(body, bytes), "invalid RLP field")
        if len(body) == 1 and body[0] < 128:
            return body
        short, long = 0x80, 0xB7
    if len(body) < 56:
        return bytes([short + len(body)]) + body
    length = len(body).to_bytes((len(body).bit_length() + 7) // 8, "big")
    return bytes([long + len(length)]) + length + body


def evm_node_wire(transaction):
    """Re-encode an independently fetched standard signed RPC transaction.

    Comparing these bytes with the journal's raw bytes binds *all* signed fields,
    including gas, access/authorization lists and signature. This avoids one Cast
    subprocess per ordinary receipt in a large campaign. The node's hash/recovered
    sender remain subject to the campaign's honest local-node assumption.
    """
    def binary(value):
        require(value is None or (isinstance(value, str) and value.startswith("0x")), "invalid EVM byte field")
        return bytes.fromhex(value[2:]) if value else b""
    def number(key):
        return _quantity(transaction[key])
    kind = _quantity(transaction.get("type", "0x0"))
    require(kind in (0, 1, 2, 4), "unsupported fixture EVM transaction type")
    common = [number("gas"), binary(transaction.get("to")), number("value"), binary(transaction.get("input", "0x"))]
    if kind == 0:
        fields = [number("nonce"), number("gasPrice"), *common, number("v"), number("r"), number("s")]
        return _rlp_encode(fields)
    access = [[binary(entry["address"]), [binary(key) for key in entry["storageKeys"]]] for entry in transaction["accessList"]]
    fees = [number("gasPrice")] if kind == 1 else [number("maxPriorityFeePerGas"), number("maxFeePerGas")]
    fields = [number("chainId"), number("nonce"), *fees, *common, access]
    if kind == 4:
        fields.append([[_quantity(entry["chainId"]), binary(entry["address"]), _quantity(entry["nonce"]),
                        _quantity(entry.get("yParity", entry.get("v"))), _quantity(entry["r"]), _quantity(entry["s"])]
                       for entry in transaction["authorizationList"]])
    parity = _quantity(transaction.get("yParity", transaction.get("v")))
    require(parity in (0, 1), "invalid typed transaction signature parity")
    return bytes([kind]) + _rlp_encode(fields + [parity, number("r"), number("s")])


def verify_evm_node_wire(signed_hex, transaction, expected_hash=None):
    require(isinstance(signed_hex, str) and signed_hex.startswith("0x"), "invalid signed wire")
    wire = bytes.fromhex(signed_hex[2:])
    require(wire == evm_node_wire(transaction), "node transaction differs from durable signed wire")
    identity = transaction["hash"].lower()
    require(expected_hash is None or identity == expected_hash.lower(), "node transaction returned different hash")
    fields = evm_intent_fields(transaction)
    return {"identity": identity, "wire_digest": hashlib.sha256(wire).hexdigest(),
            "intent_fields": fields, "intent_digest": digest(fields),
            "wire_replay_key": f"evm:{fields['chain']}:{fields['sender']}:{_quantity(transaction['nonce'])}"}


def decode_evm_wire(signed_hex, cast_bin="cast"):
    """Offline fallback for an attempt absent from node history; never calls RPC.

    Foundry Cast decodes the actual envelope, recomputes its hash and recovers its
    signer. The caller should record the pinned Cast version alongside the report.
    Missing/failed decoder raises: it must become unverified/incomplete evidence,
    never fabricated expected fields or a claim that the intent was not sent.
    """
    require(isinstance(signed_hex, str) and signed_hex.startswith("0x"), "invalid signed wire")
    wire = bytes.fromhex(signed_hex[2:])
    require(0 < len(wire) <= 128 * 1024, "invalid signed wire size")
    env = {name: value for name, value in os.environ.items()
           if not name.startswith(("ETH_", "FOUNDRY_", "CAST_", "APP__", "ENGINE_"))}
    result = subprocess.run([str(cast_bin), "decode-transaction", "--json", "--threads", "1"],
                            input=signed_hex, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=10, env=env)
    require(result.returncode == 0 and len(result.stdout) <= 1024 * 1024, "offline EVM decoder failed")
    decoded = json.loads(result.stdout)
    if isinstance(decoded, dict) and "success" in decoded:
        require(decoded["success"] is True, "offline EVM decoder returned failure")
        decoded = decoded["data"]
    if isinstance(decoded, str):
        decoded = json.loads(decoded)
    require(isinstance(decoded, dict) and "signer" in decoded and "hash" in decoded, "offline EVM decoder missing signer/hash")
    # Re-encoding also detects unexpected decoder serialization/schema changes.
    return verify_evm_node_wire(signed_hex, {**decoded, "from": decoded["signer"]})
