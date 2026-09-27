#!/usr/bin/env python3
"""Read-only, opt-in capacity guard draft. No process/node/storage mutations.

Preflight is an admission decision, not a disk-capacity guarantee. Growth ceilings
must be selected explicitly for the entire shared job. Underestimates and sudden
consumption between observations remain possible and are reported as limitations.
"""
import argparse
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import http.client
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import threading
import time
import urllib.parse

GIB = 1024 ** 3
MIB = 1024 ** 2
LIMA = '/tmp/engine-capacity-tools/lima/bin/limactl'
GUEST_COMMAND = ['sh', '-c', 'LC_ALL=C df -PB1 / && LC_ALL=C df -Pi /']


class ProbeFailure(RuntimeError):
    """Messages are fixed categories, never subprocess output or RPC bodies."""


@dataclass(frozen=True)
class Capacity:
    identity: str
    total: int
    available: int

    def validate(self):
        if not self.identity or type(self.total) is not int or type(self.available) is not int:
            raise ProbeFailure('invalid_resource_measurement')
        if self.total <= 0 or not 0 <= self.available <= self.total:
            raise ProbeFailure('invalid_resource_measurement')


@dataclass(frozen=True)
class Budget:
    floor: int
    growth_per_second: float
    floor_fraction: float = 0.10
    setup_allowance: int = 0

    def validate(self):
        if type(self.setup_allowance) is not int or self.setup_allowance < 0:
            raise ValueError('Invalid fixed setup allowance')
        if self.floor <= 0 or not math.isfinite(self.growth_per_second) or self.growth_per_second <= 0:
            raise ValueError('Explicit positive floor and growth allowance are required')
        if not math.isfinite(self.floor_fraction) or not 0 <= self.floor_fraction <= .5:
            raise ValueError('Invalid fractional floor')


def parse_guest_df(text):
    lines = [line.split() for line in text.splitlines() if line.strip()]
    if len(lines) != 4 or len(lines[1]) != 6 or len(lines[3]) != 6:
        raise ProbeFailure('guest_df_invalid')
    disk, inode = lines[1], lines[3]
    if disk[0] != inode[0] or disk[-1] != '/' or inode[-1] != '/':
        raise ProbeFailure('guest_df_wrong_filesystem')
    try:
        result = {'guest_bytes': Capacity(disk[0], int(disk[1]), int(disk[3])),
                  'guest_inodes': Capacity(inode[0], int(inode[1]), int(inode[3]))}
        for item in result.values(): item.validate()
        return result
    except (ValueError, OverflowError):
        raise ProbeFailure('guest_df_invalid') from None


def resource_probe(host_path, vm=None, lima=LIMA, run=subprocess.run, expected_host_paths=()):
    """One local statvfs plus two cheap guest df calls in one bounded Lima session.

    host_path must be on the filesystem holding both VM disk and campaign data;
    a different-volume deployment needs separately budgeted probes.
    """
    if vm is not None and not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,63}', vm):
        raise ValueError('Invalid VM name')
    started = time.monotonic()
    try:
        stat = os.statvfs(host_path)
        host_device = os.stat(host_path).st_dev
        if any(os.stat(path).st_dev != host_device for path in expected_host_paths):
            raise ProbeFailure('multiple_host_filesystems_need_separate_budgets')
        host = Capacity('device:' + str(host_device),
                        stat.f_blocks * stat.f_frsize, stat.f_bavail * stat.f_frsize)
        host.validate()
        if vm is None:
            return {"monotonic": time.monotonic(), "duration_ms": (time.monotonic() - started) * 1000,
                    "resources": {"host_bytes": host}}
        completed = run([str(lima), 'shell', vm, '--'] + GUEST_COMMAND,
                        capture_output=True, text=True, timeout=8, check=False)
    except subprocess.TimeoutExpired:
        raise ProbeFailure('guest_resource_probe_timeout') from None
    except OSError:
        raise ProbeFailure('resource_probe_unavailable') from None
    if completed.returncode != 0:
        raise ProbeFailure('guest_resource_probe_failed')
    resources = {'host_bytes': host, **parse_guest_df(completed.stdout)}
    return {'monotonic': time.monotonic(), 'duration_ms': (time.monotonic() - started) * 1000,
            'resources': resources}


class ResourcePolicy:
    """Latch on a failed reserve or observed depletion rate; never auto-clear."""
    def __init__(self, budgets, horizon_seconds, interval_seconds=60,
                 shutdown_reserve_seconds=120, factor=2.0, setup_phase=False):
        if set(budgets) not in ({'host_bytes'}, {'host_bytes', 'guest_bytes', 'guest_inodes'}):
            raise ValueError('Host bytes required; native guest bytes and inodes must be budgeted together')
        for budget in budgets.values(): budget.validate()
        if not 1 <= horizon_seconds <= 7200 or interval_seconds not in (30, 60):
            raise ValueError('Finite horizon1..7200 and sampling30/60 seconds required')
        if not 30 <= shutdown_reserve_seconds <= 600 or not math.isfinite(factor) or not 1 <= factor <= 10:
            raise ValueError('Invalid safety margin')
        self.budgets, self.horizon = budgets, horizon_seconds
        self.interval, self.grace, self.factor = interval_seconds, shutdown_reserve_seconds, factor
        self.start = self.previous = None
        # Lifetime peak remains diagnostic; only recent depletion funds emergency
        # shutdown. Full-horizon observed growth needs a complete sustained window.
        self.peaks = {key: 0.0 for key in budgets}
        self.observations = {key: deque() for key in budgets}
        self.sustained_window = max(180, 3 * self.interval)
        self.stop = None
        self.in_setup = setup_phase
        self.initial_resources = None
        self.reconciliation_deadline = None

    def begin_reconciliation(self, now, remaining_seconds=600):
        """One-way deadline reduction after the producer has actually stopped.

        Never extend the original budget, clear a stop, or reset observed growth.
        The monitor serializes this transition with its resource observations.
        """
        if (self.start is None or self.in_setup or not math.isfinite(now) or
                now < self.previous['monotonic'] or not 1 <= remaining_seconds <= 600):
            raise ValueError('Invalid reconciliation resource transition')
        if self.reconciliation_deadline is not None:
            raise RuntimeError('Reconciliation resource transition already recorded')
        self.reconciliation_deadline = min(self.start + self.horizon, now + remaining_seconds)

    def failed_probe(self, category):
        if self.stop is None:
            self.stop = {'reason': 'resource_observation_unavailable', 'category': category}
        return {'allowed': False, 'stop': dict(self.stop)}

    def assess(self, sample, finish_setup=False):
        stamp, resources = sample['monotonic'], sample['resources']
        if not math.isfinite(stamp) or set(resources) != set(self.budgets):
            return self.failed_probe('invalid_resource_measurement')
        for resource in resources.values(): resource.validate()
        if self.start is None: self.start = stamp
        if self.initial_resources is None:
            self.initial_resources = resources
        previous = self.previous
        reset_growth = finish_setup and self.in_setup
        if reset_growth:
            self.in_setup = False  # Exact end-of-setup sample becomes the growth baseline.
        reasons, details = [], {}
        if previous and stamp <= previous['monotonic']:
            return self.failed_probe('nonmonotonic_resource_sample')
        deadline = self.start + self.horizon
        if self.reconciliation_deadline is not None:
            deadline = min(deadline, self.reconciliation_deadline)
        remaining = max(0, deadline - stamp)
        if stamp >= deadline:
            reasons.append('declared_reconciliation_horizon_exhausted' if self.reconciliation_deadline is not None
                           else 'declared_job_horizon_exhausted')
        for key, budget in self.budgets.items():
            resource = resources[key]
            if previous:
                old = previous['resources'][key]
                if (resource.identity, resource.total) != (old.identity, old.total):
                    reasons.append(key + ':filesystem_changed')
                rate = max(0, (old.available - resource.available) / (stamp - previous['monotonic']))
                if not self.in_setup and not reset_growth:
                    self.peaks[key] = max(self.peaks[key], rate)
                    self.observations[key].append((previous['monotonic'], stamp, rate))
            history = self.observations[key]
            window_start = stamp - self.sustained_window
            while history and history[0][1] <= window_start:
                history.popleft()
            # Positive interval depletion is used rather than net free-space
            # change: a cleanup cannot cancel observed consumption in the window.
            observed_seconds = sum(end - max(start, window_start) for start, end, _ in history)
            observed_consumption = sum((end - max(start, window_start)) * rate
                                       for start, end, rate in history)
            sustained_ready = bool(history) and history[0][0] <= window_start and len(history) >= 3
            sustained_rate = observed_consumption / observed_seconds if sustained_ready else 0.0
            recent_peak = max((rate for _, _, rate in history), default=0.0)
            floor = max(budget.floor, math.ceil(resource.total * budget.floor_fraction))
            projected_rate = max(budget.growth_per_second, sustained_rate) * self.factor
            emergency_rate = max(budget.growth_per_second, recent_peak) * self.factor
            setup_spent = max(0, self.initial_resources[key].available - resource.available)
            setup_remaining = max(0, budget.setup_allowance - setup_spent) if self.in_setup else 0
            required = math.ceil(floor + projected_rate * remaining
                                 + emergency_rate * (self.interval + self.grace) + setup_remaining)
            details[key] = {**asdict(resource), 'floor': floor, 'required_available': required,
                            'declared_growth_per_second': budget.growth_per_second,
                            'fixed_setup_allowance': budget.setup_allowance, 'remaining_setup_reserve': setup_remaining,
                            'peak_observed_consumption_per_second': self.peaks[key],
                            'peak_observation_scope': 'lifetime_diagnostic_only',
                            'recent_peak_consumption_per_second': recent_peak,
                            'sustained_window_seconds': self.sustained_window,
                            'observed_window_seconds': observed_seconds,
                            'observed_window_intervals': len(history),
                            'sustained_observation_ready': sustained_ready,
                            'sustained_observed_consumption_per_second': sustained_rate,
                            'projected_growth_per_second': projected_rate,
                            'emergency_growth_per_second': emergency_rate,
                            'emergency_reserve_seconds': self.interval + self.grace}
            if resource.available < required:
                reasons.append(key + ':insufficient_projected_reserve')
        self.previous = sample
        if reasons and self.stop is None:
            self.stop = {'reason': 'resource_reserve_insufficient', 'details': reasons}
        phase = 'reconciliation' if self.reconciliation_deadline is not None else ('setup' if self.in_setup else 'steady')
        return {'allowed': self.stop is None, 'resource_phase': phase, 'remaining_horizon_seconds': remaining,
                'resources': details, 'stop': dict(self.stop) if self.stop else None}


class ProviderAvailability:
    """Cheap reads establish reachability, not advancing blocks or write capacity.

    Three failures over >=10 seconds latch; success only clears an unlatched
    streak. A stalled but responsive chain needs a separate progress criterion.
    """
    def __init__(self, failures=3, span_seconds=10):
        if failures < 2 or span_seconds < 5: raise ValueError('Outage debounce too small')
        self.threshold, self.span = failures, span_seconds
        self.streak, self.first, self.last = 0, None, None
        self.stop = None

    def observe(self, now, success, category=None):
        if not math.isfinite(now) or (self.last is not None and now <= self.last):
            raise ValueError('Provider observations must be monotonic')
        self.last = now
        if self.stop is not None: return dict(self.stop)
        if success:
            self.streak, self.first = 0, None
            return None
        self.streak += 1
        if self.first is None: self.first = now
        if self.streak >= self.threshold and now - self.first >= self.span:
            self.stop = {'reason': 'provider_unavailable', 'consecutive_failed_probes': self.streak,
                         'failed_span_seconds': now - self.first, 'category': category or 'unavailable'}
        return dict(self.stop) if self.stop else None


def provider_probe(url, family, expected_chain_id=None, connection_factory=http.client.HTTPConnection):
    """One read request, no proxy, redirects, retries, secrets, or arbitrary host.

    EVM eth_chainId pins routing; Solana getHealth reports node health. Connection
    churn is bounded to one connection every5s per chain, outside Engine metrics.
    """
    parsed = urllib.parse.urlsplit(url)
    if (parsed.scheme != 'http' or parsed.hostname != '127.0.0.1' or parsed.username or
            parsed.password or parsed.query or parsed.fragment):
        raise ValueError('Explicit credential-free loopback HTTP endpoint required')
    if family not in ('evm', 'solana') or (family == 'evm' and expected_chain_id is None):
        raise ValueError('Known family and EVM chain ID required')
    method = 'eth_chainId' if family == 'evm' else 'getHealth'
    connection = connection_factory('127.0.0.1', parsed.port or 80, timeout=2)
    try:
        body = json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': []}).encode()
        connection.request('POST', parsed.path or '/', body, {'Content-Type': 'application/json'})
        response = connection.getresponse()
        raw = response.read(65537)
        if response.status != 200 or len(raw) > 65536:
            return False, 'provider_http_failure'
        try:
            value = json.loads(raw)
            if not isinstance(value, dict) or value.get('id') != 1 or value.get('jsonrpc') != '2.0' or 'error' in value:
                return False, 'provider_rpc_failure'
            if family == 'evm':
                valid = isinstance(value.get('result'), str) and int(value['result'], 16) == expected_chain_id
            else:
                valid = value.get('result') == 'ok'
            return (True, None) if valid else (False, 'provider_identity_or_health_mismatch')
        except (ValueError, TypeError):
            return False, 'provider_rpc_malformed'
    except (OSError, http.client.HTTPException) as error:
        return False, type(error).__name__
    finally:
        connection.close()


class GuardMonitor:
    """Opt-in monitor around an already successful preflight.

    on_stop must be nonblocking: latch harness abort/review and wake its owner.
    It must NOT restart, delete, reattach or reconcile in the monitor thread.
    The owner handles bounded custody capture and orderly lifecycle termination.
    Each monitor uses a new output path and never overwrites prior evidence.
    """
    def __init__(self, policy, probe, evidence_path, on_stop):
        self.policy, self.probe, self.on_stop = policy, probe, on_stop
        self.path = Path(evidence_path)
        self.closed = threading.Event()
        self.thread = None
        self.stream = None
        self.tick_lock = threading.Lock()

    def preflight(self):
        if self.stream is not None: raise RuntimeError('Preflight already performed')
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        self.stream = os.fdopen(fd, 'w')
        return self.tick('preflight')

    def tick(self, phase):
        with self.tick_lock:
            return self._tick(phase)

    def begin_load(self):
        return self.tick("load_baseline")

    def begin_reconciliation(self, stop_producer, remaining_seconds=600):
        # A monitor tick must not apply the obsolete whole-job horizon after
        # producer stop but before the phase transition. Stop failure keeps the
        # original budget, and a previously latched stop remains authoritative.
        with self.tick_lock:
            stop_producer()
            self.policy.begin_reconciliation(time.monotonic(), remaining_seconds)
            return self._tick('reconciliation_baseline')

    def _tick(self, phase):
        try:
            assessment = self.policy.assess(self.probe(), finish_setup=phase == "load_baseline")
        except Exception as error:
            # A broken observer must not silently kill the daemon and leave intake open.
            category = str(error) if isinstance(error, ProbeFailure) else type(error).__name__
            assessment = self.policy.failed_probe(category)
        record = {'utc': datetime.now(timezone.utc).isoformat(), 'phase': phase, **assessment}
        if phase == 'preflight':
            record['policy'] = {'horizon_seconds': self.policy.horizon, 'interval_seconds': self.policy.interval,
                                'shutdown_reserve_seconds': self.policy.grace, 'growth_factor': self.policy.factor,
                                'budgets': {key: asdict(value) for key, value in self.policy.budgets.items()}}
            record['guard_source_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        try:
            self.stream.write(json.dumps(record, sort_keys=True) + '\n')
            self.stream.flush()
            if not assessment['allowed']: os.fsync(self.stream.fileno())
        except OSError:
            assessment = self.policy.failed_probe('resource_evidence_write_failed')
        if not assessment['allowed']:
            self.closed.set()
            self.on_stop(assessment['stop'])
        return assessment

    def start(self):
        if self.stream is None or self.policy.stop: raise RuntimeError('Successful preflight required')
        if self.thread: raise RuntimeError('Already started')
        def loop():
            # No cumulative catch-up: one bounded resource probe per interval.
            while not self.closed.wait(self.policy.interval):
                self.tick('monitor')
        self.thread = threading.Thread(target=loop, name='capacity-resource-guard', daemon=True)
        self.thread.start()

    def close(self):
        self.closed.set()
        if self.thread:
            self.thread.join(timeout=10)
            if self.thread.is_alive(): raise RuntimeError('Resource probe did not terminate')
        if self.stream:
            self.stream.close()
            self.stream = None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host-path', type=Path, required=True)
    parser.add_argument('--data-path', type=Path, action='append', required=True,
                        help='Existing VM/campaign directories; all must share the budgeted host volume')
    parser.add_argument('--vm', help='Optional native Lima VM; omitted means host-only resource guard')
    parser.add_argument('--lima', default=LIMA)
    parser.add_argument('--horizon-seconds', type=int, required=True)
    parser.add_argument('--host-growth-mib-second', type=float, required=True)
    parser.add_argument('--guest-growth-mib-second', type=float, default=1)
    parser.add_argument('--guest-growth-inodes-second', type=float, default=4)
    parser.add_argument('--setup-reserve-gib', type=float, default=.25)
    parser.add_argument('--interval-seconds', type=int, choices=(30, 60), default=60)
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    if not math.isfinite(args.setup_reserve_gib) or not .25 <= args.setup_reserve_gib <= 8:
        parser.error('Setup reserve must be .25..8 GiB')
    budgets = {'host_bytes': Budget(8 * GIB, args.host_growth_mib_second * MIB, .02, int(args.setup_reserve_gib * GIB))}
    if args.vm:
        budgets.update({'guest_bytes': Budget(4 * GIB, args.guest_growth_mib_second * MIB),
                        'guest_inodes': Budget(100000, args.guest_growth_inodes_second, .05)})
    policy = ResourcePolicy(budgets, args.horizon_seconds, args.interval_seconds, setup_phase=True)
    monitor = GuardMonitor(policy, lambda: resource_probe(args.host_path, args.vm, args.lima, expected_host_paths=args.data_path), args.evidence, lambda _: None)
    try:
        result = monitor.preflight()
        print(json.dumps(result, sort_keys=True))
        return 0 if result['allowed'] else 2
    finally: monitor.close()


if __name__ == '__main__': raise SystemExit(main())
