#!/usr/bin/env python3
"""Frozen local campaigns. Preview by default; prepare state, then explicitly execute.

One trial per invocation by default. There is no automatic rate selection or unsafe
reset. A retained active trial or review-required latch blocks every later job.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path('/Users/alfongj/Code/engine-core')
ENGINE = ROOT / 'target/release/thirdweb-engine'
HOOK = Path('/tmp/engine-capacity-tools/nitro-drain-hook-v2.py')
CONTROL = ['/tmp/engine-capacity-tools/lima/bin/limactl', 'shell', 'engine-nitro', 'docker']
PROFILES = {'evm12', 'evm2', 'evm025', 'nitro', 'solana'}
DRAIN_FIELDS = {'client_pending', 'http_inflight', 'redis_pending', 'redis_borrowed',
                'redis_submitted', 'redis_active', 'redis_delayed', 'journal_unresolved', 'node_pending'}
EXTRAS = {'--warmup-seconds': (0, 600), '--late-window-seconds': (30, 900),
          '--max-schedule-lag-ms': (100, 100), '--eoa-broadcast-concurrency': (1, 128),
          '--solana-confirmation-poll-seconds': (1, 5), '--http-concurrency': (1, 1024)}


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''): digest.update(chunk)
    return digest.hexdigest()


def event(event_name, **fields):
    print(json.dumps({'event': event_name, 'utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), **fields}), flush=True)


def save(path, data, new=False):
    """Private durable state, with no-overwrite publication for initial state."""
    fd, temp = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(data, stream, indent=2, sort_keys=True); stream.write('\n')
            stream.flush(); os.fsync(stream.fileno())
        if new: os.link(temp, path)
        else: os.replace(temp, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if os.path.exists(temp): os.unlink(temp)


def load_jobs(path):
    jobs = json.loads(path.read_text())
    if not isinstance(jobs, list) or not 1 <= len(jobs) <= 12: raise ValueError('Need1..12 explicit jobs')
    names = set()
    for job in jobs:
        if set(job) - {'name', 'chains', 'seconds', 'extra'}: raise ValueError('Unknown job option')
        name = job['name']
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,95}', name) or name in names: raise ValueError('Invalid/duplicate name')
        names.add(name)
        if type(job['seconds']) is not int or not 60 <= job['seconds'] <= 1800: raise ValueError('Duration must be60..1800')
        seen = set()
        for entry in job['chains']:
            profile, rate = entry.split('=')
            if profile not in PROFILES or profile in seen or not 1 <= float(rate) <= 500: raise ValueError('Invalid local profile/rate')
            seen.add(profile)
        if not seen: raise ValueError('Empty profile set')
        extra = job.get('extra', [])
        if len(extra) % 2: raise ValueError('Options require values')
        options = {}
        for flag, value in zip(extra[::2], extra[1::2]):
            if flag not in EXTRAS or flag in options: raise ValueError('Unapproved/duplicate option')
            lo, hi = EXTRAS[flag]
            number = int(value)
            if not lo <= number <= hi: raise ValueError('Option outside finite bounds')
            options[flag] = number
        if not {'--max-schedule-lag-ms', '--eoa-broadcast-concurrency', '--warmup-seconds', '--late-window-seconds'} <= options.keys():
            raise ValueError('Explicit schedule, broadcast, warmup and late settings required')
        if options['--warmup-seconds'] + options['--late-window-seconds'] > job['seconds']: raise ValueError('Insufficient offer window')
    return jobs


def initialize(args):
    jobs = load_jobs(args.jobs)
    paths = [ENGINE, ROOT / 'scripts/capacity_campaign.py', ROOT / 'scripts/capacity_faults.py',
             ROOT / 'scripts/capacity_assess.py', Path(__file__).resolve(), args.jobs.resolve(),
             Path(sys.executable).resolve(), Path('/tmp/engine-finality/anvil'),
             Path('/tmp/redis-7.4.2/src/redis-server'), Path('/tmp/engine-capacity-tools/cast')]
    if any('nitro=' in c for job in jobs for c in job['chains']): paths.append(HOOK)
    if any('solana=' in c for job in jobs for c in job['chains']):
        paths += [Path('/tmp/engine-finality/solana-release/bin/solana-test-validator'),
                  Path('/tmp/engine-finality/solana-release/bin/solana-keygen'),
                  Path('/tmp/engine-finality/solana-release/bin/solana')]
    frozen = {str(path): sha(path) for path in paths}
    if frozen[str(ENGINE)] != args.expect_engine_sha: raise RuntimeError('Explicit Engine hash does not match')
    return {'schema': 'local-capacity-sequential-v3', 'id': uuid.uuid4().hex[:12], 'jobs': jobs,
            'frozen_sha256': frozen, 'runs': [], 'active': None, 'review_required': None,
            'config': {'drain_seconds': args.drain_seconds, 'setup_seconds': 120,
                       'verification_seconds': 600, 'output_dir': str(args.output_dir.resolve())}}


def verify_frozen(state):
    for path, digest in state['frozen_sha256'].items():
        if sha(path) != digest: raise RuntimeError('Frozen input changed: ' + path)


def require_safe_resume(state):
    if state.get('review_required') or state.get('active'):
        raise RuntimeError('Global stop: prior active/unresolved trial requires independent operator recovery')
    if any(not run.get('safely_settled') for run in state.get('runs', [])):
        raise RuntimeError('Global stop: historical unsafe or unresolved trial')


def choose(state, subset=None):
    require_safe_resume(state)
    names = {job['name'] for job in state['jobs']}
    if subset and set(subset) - names: raise ValueError('Subset outside frozen jobs')
    completed = {run['name'] for run in state['runs']}
    return next((job for job in state['jobs'] if job['name'] not in completed and
                 (not subset or job['name'] in subset)), None)


def safely_settled(report):
    oracle = report.get('oracle', {})
    if oracle.get('safety_pass') is not True or report.get('owned_children_stopped') is not True: return False
    if report.get('errors') or report.get('journal_halted') or report.get('durable_chain_halts'): return False
    drain = report.get('drain', {})
    if set(drain) != DRAIN_FIELDS or any(type(value) is not int or value != 0 for value in drain.values()): return False
    if any(not text.endswith(': offered intent not admitted') for text in oracle.get('liveness_failures', [])): return False
    accepted = sum(chain.get('responses', {}).get('202', 0) for chain in report.get('per_chain', {}).values())
    return accepted > 0 and accepted == oracle.get('observed_intents')


def acceptable_exit(report, code):
    if code in (0, 2): return True
    # Campaign exit1 is also used for incomplete offered liveness. Allow only
    # the missing-offer case after the independent accepted-intent drain proof.
    failures = report.get('oracle', {}).get('liveness_failures', [])
    return (code == 1 and report.get('outcome') == 'incomplete' and
            not report.get('errors') and bool(failures) and
            all(text.endswith(': offered intent not admitted') for text in failures))


def command(state, job, report):
    cmd = [sys.executable, str(ROOT / 'scripts/capacity_campaign.py'), '--engine-bin', str(ENGINE),
           '--solana-bin-dir', '/tmp/engine-finality/solana-release/bin', '--anvil-bin', '/tmp/engine-finality/anvil',
           '--redis-bin', '/tmp/redis-7.4.2/src/redis-server', '--cast-bin', '/tmp/engine-capacity-tools/cast',
           '--redis-fsync', 'everysec', '--seconds', str(job['seconds']),
           '--drain-seconds', str(state['config']['drain_seconds']), '--sample-seconds', '5',
           '--max-inflight', '4096', '--http-concurrency', '64', '--proxy-concurrency', '256',
           '--solana-confirmation-poll-seconds', '1', '--report', str(report)]
    for chain in job['chains']: cmd += ['--chain', chain]
    if any(c.startswith('nitro=') for c in job['chains']):
        cmd += ['--external-evm', 'nitro=http://127.0.0.1:18547', '--chain-id', 'nitro=412346',
                '--depth', 'nitro=2', '--external-drain-hook',
                f"nitro={HOOK} --max-seconds {state['config']['drain_seconds']}"]
    return cmd + job.get('extra', [])


def timeout_seconds(state, job):
    cfg = state['config']
    return job['seconds'] + cfg['drain_seconds'] + cfg['setup_seconds'] + cfg['verification_seconds']


def kill_group(pid, sig):
    try: os.killpg(pid, sig)
    except ProcessLookupError: pass


def run_child(cmd, log, timeout, graceful_seconds=60):
    child = subprocess.Popen(cmd, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        return child.wait(timeout=timeout)
    except BaseException:
        # SIGINT first reaches the harness's finally cleanup; all campaign node
        # children inherit its group. On timeout/interrupt no subsequent run is allowed.
        if child.poll() is None: child.send_signal(signal.SIGINT)
        try: child.wait(timeout=graceful_seconds)
        except subprocess.TimeoutExpired: pass
        finally:
            kill_group(child.pid, signal.SIGTERM)
            # Leader exit does not prove descendants exited; grant group cleanup.
            time.sleep(.25)
            try: child.wait(timeout=3)
            except subprocess.TimeoutExpired: pass
            kill_group(child.pid, signal.SIGKILL)
            child.wait(timeout=3)
        raise


def record_failure(state, path, reason):
    state['review_required'] = {'reason': reason, 'active': state.get('active'),
                                'utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
    save(path, state)


def run_one(state, path, job):
    verify_frozen(state)
    prefix = f"{job['name']}-{state['id']}"
    report_path = Path(state['config']['output_dir']) / (prefix + '.json')
    log_path = report_path.with_suffix('.log')
    assessment_path = report_path.with_name(prefix + '-assessment.json')
    if any(p.exists() for p in (report_path, log_path, assessment_path)): raise FileExistsError('Existing evidence must not be overwritten')
    cmd = command(state, job, report_path)
    state['active'] = {'name': job['name'], 'report': str(report_path), 'command': cmd,
                       'timeout_seconds': timeout_seconds(state, job)}
    save(path, state)
    event('start', **state['active'])
    native = any(c.startswith('nitro=') for c in job['chains'])
    unpause_attempted = False
    try:
        try:
            if native:
                unpause_attempted = True
                subprocess.run(CONTROL + ['unpause', 'engine-nitro-dev'], check=True, stdout=subprocess.DEVNULL, timeout=30)
            with log_path.open('x') as log:
                os.chmod(log_path, 0o600)
                code = run_child(cmd, log, timeout_seconds(state, job))
        finally:
            if unpause_attempted:
                # Child has exited or its owned group has been killed first.
                # Pause retains caller-owned Nitro; never restart/reset its state.
                subprocess.run(CONTROL + ['pause', 'engine-nitro-dev'], check=True, stdout=subprocess.DEVNULL, timeout=30)
        verify_frozen(state)
        report = json.loads(report_path.read_text())
        if report.get('engine_binary_sha256') != state['frozen_sha256'][str(ENGINE)]: raise RuntimeError('Report Engine mismatch')
        expected_harness = {name: state['frozen_sha256'][str(ROOT / 'scripts' / name)]
                            for name in ('capacity_campaign.py', 'capacity_faults.py')}
        if report.get('harness_sha256') != expected_harness: raise RuntimeError('Report harness mismatch')
        settled = safely_settled(report) and acceptable_exit(report, code)
        trial = {'name': job['name'], 'exit': code, 'report': str(report_path), 'report_sha256': sha(report_path),
                 'safely_settled': settled, 'capacity_selected': False}
        state['runs'].append(trial)
        if not settled:
            record_failure(state, path, 'Unsafe, unresolved, cleanup-incomplete or missing accepted-intent evidence')
            raise RuntimeError('Trial requires recovery/review; every later job is blocked')
        subprocess.run([sys.executable, str(ROOT / 'scripts/capacity_assess.py'), str(report_path),
                        '--output', str(assessment_path)], check=True, timeout=60)
        trial['assessment'] = str(assessment_path)
        trial['assessment_sha256'] = sha(assessment_path)
        state['active'] = None
        save(path, state)
        event('done', **trial)
    except BaseException as error:
        record_failure(state, path, type(error).__name__ + ': ' + str(error)[:300])
        raise


def main():
    def terminate(_signum, _frame):
        raise KeyboardInterrupt('Supervisor terminated by owner')
    signal.signal(signal.SIGTERM, terminate)
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument('--state', type=Path, help='Create a fresh frozen state')
    target.add_argument('--resume', type=Path, help='Existing frozen state')
    parser.add_argument('--jobs', type=Path)
    parser.add_argument('--expect-engine-sha')
    action = parser.add_mutually_exclusive_group()
    action.add_argument('--prepare', action='store_true', help='Save state, do not run anything')
    action.add_argument('--execute', action='store_true', help='Run selected jobs; default one')
    parser.add_argument('--only', nargs='+', help='Frozen job names; cannot bypass global review stop')
    parser.add_argument('--max-runs', type=int, default=1)
    parser.add_argument('--drain-seconds', type=int, default=1800)
    parser.add_argument('--output-dir', type=Path, default=Path('/tmp'))
    args = parser.parse_args()
    if not 1 <= args.max_runs <= 12 or not 60 <= args.drain_seconds <= 1800: parser.error('Invalid finite budget')
    if args.state and (not args.jobs or not re.fullmatch('[0-9a-f]{64}', args.expect_engine_sha or '')):
        parser.error('New state needs --jobs and an explicit --expect-engine-sha')
    if args.resume and (args.jobs or args.expect_engine_sha or args.prepare): parser.error('Cannot change a frozen manifest')
    path = (args.state or args.resume).resolve()
    with open('/tmp/engine-capacity-bracket.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.resume: state = json.loads(path.read_text())
        else:
            if path.exists(): raise FileExistsError(path)
            state = initialize(args)
        require_safe_resume(state); verify_frozen(state)
        job = choose(state, args.only)
        if not args.execute:
            if args.prepare:
                path.parent.mkdir(parents=True, exist_ok=True)
                save(path, state, new=True)
            event('prepared' if args.prepare else 'preview_only', state=str(path), frozen_sha256=state['frozen_sha256'],
                  next_command=command(state, job, Path(state['config']['output_dir']) / (job['name'] + '-' + state['id'] + '.json')) if job else None)
            return
        if args.state:
            path.parent.mkdir(parents=True, exist_ok=True)
            save(path, state, new=True)
        Path(state['config']['output_dir']).mkdir(parents=True, exist_ok=True)
        for _ in range(args.max_runs):
            job = choose(state, args.only)
            if job is None: break
            run_one(state, path, job)
        event('paused_or_complete', state=str(path), completed=len(state['runs']))


if __name__ == '__main__': main()
