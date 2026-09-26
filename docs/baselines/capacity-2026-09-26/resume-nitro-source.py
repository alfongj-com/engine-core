#!/usr/bin/env python3
"""One-off bounded original queue continuation, never a capacity measurement."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import threading
import time

import capacity_campaign as c
from capacity_faults import FaultError, FaultPlan, RpcFaultProxy

ORIGINAL = Path('/tmp/capacity-nitro-100-v2.json')
OUTPUT = Path('/tmp/capacity-nitro-100-v2-resumed.json')
BINARY = Path('/tmp/engine-capacity-baseline-bin/thirdweb-engine')
BINARY_SHA = 'a0b38a6444682de64cc9264f27f826cb14a4913692b5ed8e9cc837d360588ccc'
NATIVE_COMMAND = ['/tmp/engine-capacity-tools/lima/bin/limactl', 'shell', 'engine-nitro', 'docker']


def inventory(path):
    with sqlite3.connect(f'file:{path}?mode=ro', uri=True) as db:
        db.execute('BEGIN')
        control = db.execute('SELECT deployment,epoch,namespace,checkpoint,halted FROM control').fetchone()
        admissions = {row[0]: row[1:] for row in db.execute('SELECT id,kind,fingerprint,payload,state,replay_key FROM admissions')}
        attempts = {row[0]: row[1:] for row in db.execute('SELECT sequence,id,replay_key,digest,payload FROM attempts')}
        terminal = dict(db.execute('SELECT sequence,evidence FROM terminal_evidence'))
        db.rollback()
    return {'control': control, 'admissions': admissions, 'attempts': attempts, 'terminal': terminal}


class OriginalIntentProxy(RpcFaultProxy):
    """Forward only preexisting wires or the first wire for an unsigned old ID."""
    def __init__(self, url, path, before, **kwargs):
        self.journal = path
        self.before = before
        self.validation_lock = threading.Lock()
        self.validated_sequence = 0
        self.allowed = {}
        self.id_wires = {}
        self.blocked = 0
        self.refresh()
        super().__init__(url, **kwargs)

    def refresh(self):
        with sqlite3.connect(f'file:{self.journal}?mode=ro', uri=True, timeout=3) as db:
            rows = list(db.execute('SELECT sequence,id,replay_key,digest,payload FROM attempts WHERE sequence>? ORDER BY sequence', (self.validated_sequence,)))
        for sequence, txid, replay, journal_digest, encoded in rows:
            old = self.before['admissions'].get(txid)
            if old is None:
                raise FaultError('resume refuses new intent ID')
            payload = json.loads(encoded)
            wire_digest = hashlib.sha256(bytes.fromhex(payload['signedTransaction'].removeprefix('0x'))).hexdigest()
            existing = self.id_wires.get(txid)
            old_attempt = self.before['attempts'].get(sequence)
            if old_attempt is not None:
                if old_attempt != (txid, replay, journal_digest, encoded):
                    raise FaultError('resume old attempt mutated')
            elif old[4] is not None or old[3] == 'terminal' or (existing is not None and existing != wire_digest):
                raise FaultError('resume refuses replacement identity')
            self.allowed[wire_digest] = txid
            self.id_wires[txid] = wire_digest
            self.validated_sequence = sequence

    def _forward(self, call):
        if isinstance(call, dict) and call.get('method') == 'eth_sendRawTransaction':
            wire_digest = self._wire(call)
            with self.validation_lock:
                try:
                    if wire_digest not in self.allowed:
                        self.refresh()
                    if wire_digest not in self.allowed:
                        raise FaultError('resume wire lacks original durable intent')
                except Exception:
                    self.blocked += 1
                    raise
        return super()._forward(call)


def main():
    original = json.loads(ORIGINAL.read_text())
    if hashlib.sha256(BINARY.read_bytes()).hexdigest() != BINARY_SHA:
        raise RuntimeError('Baseline binary hash changed')
    old_logs = Path(original['log_directory'])
    journal = old_logs / 'recovery.sqlite'
    before = inventory(journal)
    if len(before['admissions']) != 18000 or before['control'][4]:
        raise RuntimeError('Original healthy18000-intent authority missing')
    namespace = before['control'][2]
    args, profiles = c.arguments(['--chain', 'nitro=100', '--external-evm', 'nitro=http://127.0.0.1:18547',
        '--chain-id', 'nitro=412346', '--depth', 'nitro=2', '--seconds', '180', '--drain-seconds', '600',
        '--engine-bin', str(BINARY), '--redis-bin', '/tmp/redis-7.4.2/src/redis-server',
        '--cast-bin', '/tmp/engine-capacity-tools/cast', '--redis-fsync', 'everysec', '--report', str(OUTPUT)])
    campaign = c.Campaign(args, profiles)
    campaign.namespace = campaign.projection_namespace = namespace
    campaign.journal = journal
    campaign.initial = original['initial']
    campaign.phase = 'resume_setup'
    campaign.pool.close()
    campaign.report.update({'resumed_original_report': str(ORIGINAL), 'resumed_original_journal': str(journal),
        'resume_script_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'scope': 'Bounded original queue completion after observer transport failure; not capacity evidence',
        'no_new_admissions': True, 'new_intent_ids_allowed': False, 'manual_wire_replay': False,
        'initial': campaign.initial,
        'per_chain': {'nitro': {'offered': 18000, 'late_window': {'capacity_candidate': False, 'reason': 'separate recovery drain, no capacity claim'}}},
        'resume_before': {'admissions': 18000, 'attempted_ids': len({r[0] for r in before['attempts'].values()}),
                          'terminal': sum(r[3] == 'terminal' for r in before['admissions'].values())}})
    for index in range(18000):
        txid = f'{namespace}-nitro-{index}'
        campaign.id_chain[txid] = 'nitro'
        _, expected = campaign.fixture('nitro', index)
        campaign.expected[txid] = expected
    if set(campaign.expected) != set(before['admissions']):
        raise RuntimeError('Original ID inventory differs from claimed18000 fixture')
    # Private read-only forensic snapshot before authority mutation; never opened
    # as a second Engine authority, and never changes the original failure report.
    backup = campaign.logs / 'before-authority.sqlite'
    backup.touch(mode=0o600)
    with sqlite3.connect(f'file:{journal}?mode=ro', uri=True) as source, sqlite3.connect(backup) as target:
        source.backup(target)
    resumed = False
    sampler = None
    try:
        subprocess.run(NATIVE_COMMAND + ['unpause', 'engine-nitro-dev'], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=60)
        resumed = True
        node = c.Rpc(profiles['nitro']['external_url'])
        if int(node.call('eth_chainId', []), 16) != 412346:
            raise RuntimeError('Wrong original native chain')
        if node.call('eth_getBlockByNumber', ['0x0', False])['hash'] != original['initial']['nitro']['genesis']['hash']:
            raise RuntimeError('Original native chain was reset')
        campaign.nodes['nitro'] = node
        campaign.spawn('redis', [args.redis_bin, '--bind', '127.0.0.1', '--port', campaign.redis_port,
            '--dir', old_logs / 'redis', '--save', '', '--appendonly', 'yes', '--appendfsync', 'everysec'])
        c.wait_until(lambda: c.redis_command(campaign.redis_port, 'PING') == 'PONG')
        proxy = OriginalIntentProxy(profiles['nitro']['external_url'], journal, before,
            plan=FaultPlan(max_inflight=args.proxy_concurrency, max_unique_wires=72000), audit_path=campaign.logs / 'nitro-rpc.jsonl')
        campaign.proxies['nitro'] = proxy
        campaign.env = {k: v for k, v in os.environ.items() if not k.startswith(('APP__', 'ENGINE_'))}
        campaign.env.update(original['queue_settings'])
        campaign.env.update({'APP_ENVIRONMENT': 'production', 'RUST_LOG': 'warn', 'ENGINE_PRIVATE_KEY': f'{1:064x}',
            'ENGINE_SIGNING_TOKEN': campaign.token, 'APP__REDIS__URL': f'redis://127.0.0.1:{campaign.redis_port}/',
            'APP__RECOVERY__JOURNAL_PATH': str(journal), 'APP__SERVER__HOST': '127.0.0.1',
            'APP__SERVER__PORT': str(campaign.engine_port), 'APP__EVM_RPC__ENDPOINTS__412346__URL': proxy.url,
            'APP__EVM_RPC__ENDPOINTS__412346__FINALITY__MODE': 'depth',
            'APP__EVM_RPC__ENDPOINTS__412346__FINALITY__CONFIRMATIONS': '2'})
        if not campaign.engine_command('--reattach-recovery', required=False):
            campaign.report.update(outcome='fail_closed_recovery_required', exact_marker_reattach=False)
            return
        campaign.report['exact_marker_reattach'] = True
        campaign.start_engine()
        campaign.started = time.monotonic()
        campaign.phase = 'original_queue_resume'
        campaign.observer = c.JournalObserver(journal, campaign.id_chain, campaign.started, {})
        campaign.spawn('drain-hook-nitro', ['/tmp/engine-capacity-tools/nitro-drain-hook.py'])
        sampler = threading.Thread(target=campaign.sampling, daemon=True)
        sampler.start()
        campaign.wait_drain()
        campaign.stop_sampler.set()
        sampler.join(timeout=30)
        if sampler.is_alive():
            raise RuntimeError('Resume observer did not stop')
        campaign.report['resume_drain_seconds'] = time.monotonic() - campaign.started
        campaign.reconcile()
        after = inventory(journal)
        if after['control'][:3] != before['control'][:3] or after['control'][4]:
            raise RuntimeError('Resume changed deployment/epoch/namespace or halted authority')
        if set(after['admissions']) != set(before['admissions']):
            raise RuntimeError('Resume created or lost intent IDs')
        for txid, old in before['admissions'].items():
            new = after['admissions'][txid]
            if old[:3] != new[:3] or (old[4] is not None and old[4] != new[4]):
                raise RuntimeError('Resume changed original payload or replay binding')
        if any(after['attempts'].get(seq) != row for seq, row in before['attempts'].items()):
            raise RuntimeError('Resume changed old signed attempt')
        if any(after['terminal'].get(seq) != row for seq, row in before['terminal'].items()):
            raise RuntimeError('Resume changed old terminal evidence')
        new_ids = [row[0] for seq, row in after['attempts'].items() if seq not in before['attempts']]
        if len(new_ids) != len(set(new_ids)) or any(before['admissions'][txid][4] is not None for txid in new_ids):
            raise RuntimeError('Resume added replacement identity')
        if proxy.blocked:
            raise RuntimeError('Resume wire guard blocked forbidden identity')
        campaign.report.update({'immutable_original_inventory_preserved': True, 'new_first_signatures': len(new_ids),
            'wire_guard_rejections': proxy.blocked, 'all_chain_capacity_candidate': False})
    except BaseException as error:
        campaign.errors.append({'phase': campaign.phase, 'error_type': type(error).__name__})
        campaign.report.update(outcome='error', error_type=type(error).__name__)
        raise
    finally:
        campaign.stop_sampler.set()
        if sampler: sampler.join(timeout=30)
        for child in reversed(list(campaign.children.values())):
            c.stop(child)
        for proxy in campaign.proxies.values(): proxy.close()
        for stream in campaign.streams: stream.close()
        c.LOCAL_HTTP.close()
        if resumed:
            subprocess.run(NATIVE_COMMAND + ['pause', 'engine-nitro-dev'], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=60)
        campaign.report.update({'samples': campaign.samples, 'events': campaign.events, 'errors': campaign.errors,
            'owned_children_stopped': all(p.poll() is not None for p in campaign.children.values()),
            'external_native_paused_after': resumed, 'campaign_http': c.LOCAL_HTTP.snapshot()})
        c.private_json(OUTPUT, campaign.report)
        print(json.dumps({'report': str(OUTPUT), 'outcome': campaign.report.get('outcome'), 'logs': str(campaign.logs)}), flush=True)
        if campaign.report.get('outcome') != 'pass':
            raise SystemExit(1)


if __name__ == '__main__':
    main()
