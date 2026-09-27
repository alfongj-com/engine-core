#!/usr/bin/env python3
"""Archive the closed lease600 pair only. Run explicitly in an idle window."""
import collections
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import time

ROOT = Path('/Users/alfongj/Code/engine-core')
STATE = Path('/tmp/engine-capacity-shared-lease600-state.json')
COHORT = '1c3eec03d6cb'
OUT = ROOT / 'docs/baselines/capacity-2026-09-26' / ('shared-lease600-' + COHORT)
AUDITS = Path('/tmp/engine-capacity-lease600-independent-audit')
JOBS = Path('/tmp/engine-capacity-dispatch-plan/shared-lease600-jobs.json')
DIGEST = re.compile(r'[0-9a-f]{64}')
IDENTITY = re.compile(r'(?:0x[0-9a-fA-F]{64}|[1-9A-HJ-NP-Za-km-z]{64,90})')
FORBIDDEN = {'privatekey', 'secretkey', 'signedtransaction', 'authorizationheader',
             'signingtoken', 'mnemonic', 'payload', 'environment', 'env'}
DRAIN = {'client_pending', 'http_inflight', 'journal_unresolved', 'node_pending',
         'redis_active', 'redis_borrowed', 'redis_delayed', 'redis_pending', 'redis_submitted'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def privacy(value):
    if isinstance(value, dict):
        require(not {re.sub('[^a-z]', '', key.lower()) for key in value} & FORBIDDEN,
                'Private/unsanitized field found')
        for item in value.values():
            privacy(item)
    elif isinstance(value, list):
        for item in value:
            privacy(item)


def read_json(path):
    value = json.loads(Path(path).read_bytes())
    privacy(value)
    return value


def observations(path, expected):
    seen = set()
    with gzip.open(path, 'rt') as stream:
        for line in stream:
            row = json.loads(line)
            privacy(row)
            require(set(row) == {'id', 'admitted', 'kind', 'state', 'replay_key',
                                 'attempts', 'executions', 'terminal'}, 'Unexpected observation schema')
            require(row['id'] not in seen and row['state'] == 'terminal' and row['admitted'] is True,
                    'Observation not uniquely settled')
            seen.add(row['id'])
            require(len(row['attempts']) == len(row['executions']) == 1, 'Unexpected attempt/execution count')
            attempt, execution = row['attempts'][0], row['executions'][0]
            require(set(attempt) == {'identity', 'intent_digest', 'replay_key', 'wire_digest', 'wire_replay_key'},
                    'Unsanitized attempt schema')
            require(all(DIGEST.fullmatch(attempt[key]) for key in ('intent_digest', 'wire_digest')),
                    'Invalid digest')
            require(IDENTITY.fullmatch(attempt['identity']), 'Invalid signed identity')
            require(attempt['replay_key'] == attempt['wire_replay_key'] == row['replay_key'], 'Replay mismatch')
            require(set(execution) <= {'identity', 'outcome', 'effects', 'fee', 'canonical', 'finalized',
                                       'block_hash', 'block_number', 'nonce', 'fee_components', 'slot'},
                    'Unexpected execution field')
            require(execution['canonical'] is True and execution['finalized'] is True, 'Unsettled execution')
            require(set(row['terminal']) == {'identity', 'outcome'}, 'Unexpected terminal field')
            require(attempt['identity'] == execution['identity'] == row['terminal']['identity'], 'Identity mismatch')
            require(execution['outcome'] == row['terminal']['outcome'] == 'success', 'Unexpected transfer outcome')
            require(set(execution.get('fee_components', {})) <= {'l1Fee', 'operatorFee'}, 'Unexpected fee fields')
            require(all(key.startswith('balance:') and isinstance(value, int)
                        for key, value in execution['effects'].items()), 'Unexpected transfer effect')
    require(len(seen) == expected, 'Observation count mismatch')
    return len(seen)


def rpc_audit(path, rpc):
    counts = collections.Counter()
    for number, line in enumerate(Path(path).read_text().splitlines(), 1):
        row = json.loads(line)
        require(set(row) <= {'sequence', 'utc', 'elapsed_seconds', 'event', 'method',
                             'wire_digest', 'identity', 'response_dropped'}, 'RPC audit field not allowlisted')
        require(row['event'] in {'send_forwarded', 'send_accepted', 'rpc_faults_released'}, 'Unexpected RPC event')
        require(row['sequence'] == number, 'RPC audit sequence gap')
        if 'wire_digest' in row:
            require(DIGEST.fullmatch(row['wire_digest']), 'Invalid wire digest')
        if 'identity' in row:
            require(IDENTITY.fullmatch(row['identity']), 'Invalid accepted identity')
        if 'method' in row:
            require(row['method'] in {'eth_sendRawTransaction', 'sendTransaction'}, 'Unexpected method')
        require(row.get('response_dropped', False) is False, 'Unexpected injected fault')
        counts[row['event']] += 1
    require(sum(counts.values()) == rpc['audit']['event_count'], 'RPC audit count mismatch')
    require(counts['send_forwarded'] == rpc['forwarded_sends'], 'Forward count mismatch')
    require(counts['send_accepted'] == sum(x['accepted_responses'] for x in rpc['accepted_wires'].values()),
            'Accepted RPC count mismatch')
    return dict(counts)


def archive():
    started = time.monotonic()
    state = read_json(STATE)
    require(state['id'] == COHORT and state['active'] is None and state['review_required'] is None,
            'Pair still active or requires custody review')
    require(len(state['runs']) == len(state['jobs']) == 2, 'Pair incomplete')
    require(not OUT.exists(), 'Archive already exists; never overwrite')
    planned, integrity, summaries = [], {}, {}

    def plan(path, name, compress=False):
        path = Path(path)
        require(path.is_file() and path.suffix not in {'.sqlite', '.aof', '.rdb'}, 'Missing/forbidden source')
        data = path.read_bytes()
        if path.suffix in {'.json', '.jsonl'}:
            for value in ([json.loads(data)] if path.suffix == '.json' else
                          (json.loads(line) for line in data.splitlines() if line)):
                privacy(value)
        planned.append((name, data, compress))
        integrity[name] = {'source': str(path), 'source_sha256': sha(data),
                           'source_bytes': len(data), 'lossless_gzip': compress}
        return sha(data)

    # Check every frozen Python/config source; report binary hashes bind the
    # original binary without copying executables or private runtime directories.
    source_checks = {}
    for path, expected in state['frozen_sha256'].items():
        if path.endswith('.py') or path.endswith('jobs.json'):
            actual = sha(Path(path).read_bytes())
            require(actual == expected, 'Frozen source/config drift: ' + path)
            source_checks[path] = actual
    require(sha(JOBS.read_bytes()) == state['frozen_sha256'][str(JOBS.resolve())], 'Job source mismatch')
    require(read_json(JOBS) == state['jobs'], 'Frozen job content mismatch')
    runs = {run['name']: run for run in state['runs']}
    for label, suffix in [('high', 'high'), ('low', 'scale020')]:
        run = runs['capacity-shared-lease600-' + suffix]
        require(run['safely_settled'] is True and run['capacity_selected'] is False, 'Unsettled run')
        source = Path(run['report'])
        report = read_json(source)
        require(set(report['rpc']) == {'evm12', 'evm2', 'nitro', 'solana'}, 'Unexpected chain set')
        digest = sha(source.read_bytes())
        require(digest == run['report_sha256'], 'Final report hash mismatch')
        require(report['queue_settings']['APP__QUEUE__LEASE_DURATION_SECONDS'] == '600', 'Wrong lease')
        require(report['owned_children_stopped'] and report['oracle']['safety_pass'], 'Failed accepted custody')
        require(set(report['drain']) == DRAIN and all(type(x) is int and x == 0 for x in report['drain'].values()),
                'Incomplete drain')
        require(not report['errors'] and not report.get('infrastructure_stop')
                and not report.get('operator_recovery_required'), 'Incident report is not this settled pair')
        require(report['engine_binary_sha256'] == state['frozen_sha256'][str(ROOT / 'target/release/thirdweb-engine')],
                'Engine identity mismatch')
        for name, expected in report['harness_sha256'].items():
            require(expected == source_checks[str(ROOT / 'scripts' / name)], 'Harness mismatch')
        audit_path = AUDITS / (label + '.json')
        audit = read_json(audit_path)
        verdict = audit['accepted_custody'] if label == 'high' else audit['safety_and_settlement']
        audited_count = audit['admitted_audited'] if label == 'high' else audit['exact_admitted_and_observed_ids']
        require(audit['report_sha256'] == digest and verdict == 'pass', 'Independent audit mismatch')
        require(audited_count == report['oracle']['observed_intents'], 'Audit count mismatch')
        observation_path = Path(report['observation_evidence'])
        require(sha(observation_path.read_bytes()) == audit['observations_sha256'], 'Observation digest mismatch')
        observation_count = observations(observation_path, audited_count)
        require(report['oracle']['liveness_pass'] is (label == 'low'), 'Unexpected offered-workload verdict')
        require(audit['missing_offers'] == report['oracle']['offered_intents'] - observation_count, 'Offer counts mismatch')
        require(sum(v['responses'].get('429', 0) for v in report['per_chain'].values()) == audit['missing_offers'],
                'Missing offers are not exactly known429s')
        require(sha(Path(run['assessment']).read_bytes()) == run['assessment_sha256'], 'Strict assessment drift')
        plan(source, label + '/full-report.json.gz', True)
        plan(observation_path, label + '/observations.jsonl.gz')
        plan(audit_path, label + '/independent-audit.json')
        plan(run['assessment'], label + '/strict-assessment.json')
        plan(source.with_suffix('.resource-preflight.jsonl'), label + '/resource-preflight.jsonl')
        plan(report['resource_guard']['evidence'], label + '/resources.jsonl')
        rpc_counts = {}
        for chain, rpc in report['rpc'].items():
            for wire, value in rpc['accepted_wires'].items():
                require(DIGEST.fullmatch(wire) and set(value) == {'identity', 'accepted_responses'}, 'Raw RPC payload')
                require(IDENTITY.fullmatch(value['identity']), 'Invalid accepted identity')
            rpc_counts[chain] = rpc_audit(rpc['audit']['path'], rpc)
            plan(rpc['audit']['path'], label + '/' + chain + '-rpc-audit.jsonl.gz', True)
        for kind in ('compact', 'backlog-description', 'summary'):
            path = source.with_name(source.stem + '-' + kind + '.json')
            analytic = read_json(path)
            linked = analytic['reports'][0]['report_sha256_uncompressed'] if kind == 'compact' else analytic['report_sha256']
            require(linked == digest, 'Analyst source mismatch')
            if kind == 'backlog-description':
                require(analytic['descriptor_sha256'] == sha(Path('/tmp/capacity-posthoc/describe_backlogs.py').read_bytes()),
                        'Descriptor source drift')
                require(analytic['interpretation_policy_sha256'] == sha(Path('/tmp/capacity-posthoc/interpretation-v1.md').read_bytes()),
                        'Interpretation policy drift')
            plan(path, label + '/analyst-' + kind + '.json')
        summaries[label] = {'report_sha256': digest, 'queue_lease_seconds': 600,
            'offered': report['oracle']['offered_intents'], 'accepted_settled': observation_count,
            'known429': audit['missing_offers'], 'all_offered_liveness_pass': report['oracle']['liveness_pass'],
            'capacity_selected': False, 'engine_error_count': audit['engine_error_count'],
            'late_rates': {k: v['late_window']['rates_tps'] for k, v in report['per_chain'].items()},
            'rpc_audit_event_counts': rpc_counts, 'private_state_excluded': True}

    plan(STATE, 'supervisor-final-state.json')
    plan(JOBS, 'frozen-jobs.json')
    for path, name in [(ROOT / 'docs/baselines/capacity-2026-09-26/supervisor-v4/runner.py', 'supervisor-source.py'),
                       (ROOT / 'scripts/capacity_assess.py', 'strict-assessor-source.py'),
                       (Path('/tmp/engine-capacity-report-extract/extract.py'), 'compact-extractor-source.py'),
                       (Path('/tmp/capacity-posthoc/describe_backlogs.py'), 'backlog-descriptor-source.py'),
                       (Path('/tmp/capacity-posthoc/interpretation-v1.md'), 'interpretation-policy.md'),
                       (Path(__file__), 'archive-source.py')]:
        plan(path, name)
    # No destination is created until every required artifact and privacy check passes.
    OUT.mkdir()
    for name, data, compressed in planned:
        target = OUT / name
        target.parent.mkdir(exist_ok=True, parents=True)
        archived = gzip.compress(data, compresslevel=1, mtime=0) if compressed else data
        with target.open('xb') as stream:
            stream.write(archived)
        require((gzip.decompress(target.read_bytes()) if compressed else target.read_bytes()) == data, 'Copy mismatch')
    high, low = summaries['high'], summaries['low']
    readme = f'''# Shared pair with the production600-second lease

One Engine, one SQLite FULL journal and Redis AOF every second served four local chains for360 seconds each. One EVM signer was shared across distinct chain IDs; Solana used one payer. Both runs used the production600-second queue lease, send concurrency32, HTTP concurrency128 and Solana polling5s. These other settings are explicit campaign choices, not a claim that every production default was used.

| Offered vector EVM/OP/Nitro/Solana | Accepted and settled | Known429s | Interpretation |
|---|---:|---:|---|
|60/60/50/65 =235 TPS|{high['accepted_settled']:,}|{high['known429']:,}|Overload; original offered workload incomplete|
|12/12/10/13 =47 TPS|{low['accepted_settled']:,}|{low['known429']:,}|Clean finite lower control; no sustainable maximum selected|

All accepted IDs have captured canonical/finalized receipt, original wire/replay and terminal evidence; fees/effects reconcile. Both runs drained all nine counters and stopped owned children. Independent audits compare captured observations against original private journals; they are not additional RPC sweeps. Normal reports retain aggregate HTTP counts, not a separate per-ID HTTP status map. Eventual accepted-work settlement does not erase the high run's admission rejection or backlog growth.

The earlier10-second lease screens remain separate evidence. This pair changes that configuration and must not be silently merged with them. Ordered runs share a host and retained Nitro database; neither randomization nor infinite sustainability is established. Anvil OP execution lacks a real sequencer/L1 settlement, Nitro is dev L2-only, and Agave is local. No public-chain or paidRPC capacity claim follows.

Original reports and sanitized per-ID observations are preserved losslessly; RPC audits pass explicit event/key/value allowlists. Strict assessments and descriptive summaries remain unchanged. Source/report/audit hashes and copy provenance are in source-integrity.json and manifest.json. Private journals, AOF, node snapshots, raw signed wires, key material, environment and unstructured runtime logs are excluded. Partial archives have no manifest and are not trusted completed evidence.
'''
    (OUT / 'README.md').write_text(readme)
    (OUT / 'summary.json').write_text(json.dumps(summaries, indent=2, sort_keys=True) + '\n')
    (OUT / 'source-integrity.json').write_text(json.dumps({'frozen_source_checks': source_checks, 'sources': integrity}, indent=2, sort_keys=True) + '\n')
    files = {str(p.relative_to(OUT)): {'bytes': p.stat().st_size, 'sha256': sha(p.read_bytes())}
             for p in OUT.rglob('*') if p.is_file()}
    (OUT / 'manifest.json').write_text(json.dumps({'files': files, 'archive_wall_seconds': time.monotonic() - started}, indent=2, sort_keys=True) + '\n')
    print(json.dumps({'path': str(OUT), 'files': len(files) + 1, 'seconds': time.monotonic() - started}))


if __name__ == '__main__':
    # Same lock as campaign supervisors: never compress concurrently with a load.
    with Path('/tmp/engine-capacity-bracket.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        archive()
