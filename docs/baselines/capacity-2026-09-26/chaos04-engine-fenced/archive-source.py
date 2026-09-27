#!/usr/bin/env python3
"""Copy closed chaos evidence; privacy/provenance checks are not a safety verdict."""
import argparse
import collections
import fcntl
import gzip
import hashlib
import json
from pathlib import Path
import re
import time

ROOT = Path('/Users/alfongj/Code/engine-core')
DIGEST = re.compile(r'[0-9a-f]{64}')
IDENTITY = re.compile(r'(?:0x[0-9a-fA-F]{64}|[1-9A-HJ-NP-Za-km-z]{64,90})')
FORBIDDEN = {'privatekey', 'secretkey', 'signedtransaction', 'signedwire', 'mnemonic',
             'authorizationheader', 'signingtoken', 'payload', 'environment', 'env', 'headers', 'credentials'}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def privacy(value):
    if isinstance(value, dict):
        require(not {re.sub('[^a-z]', '', key.lower()) for key in value} & FORBIDDEN,
                'Private or unsanitized field found')
        for item in value.values():
            privacy(item)
    elif isinstance(value, list):
        for item in value:
            privacy(item)


def parsed(path):
    value = json.loads(Path(path).read_bytes())
    privacy(value)
    return value


def observation_privacy(path):
    count = 0
    with gzip.open(path, 'rt') as stream:
        for line in stream:
            row = json.loads(line)
            privacy(row)
            require(set(row) <= {'id', 'admitted', 'kind', 'state', 'replay_key', 'attempts',
                                 'executions', 'terminal', 'retained', 'orphaned_execution', 'unexpected'},
                    'Observation fields not allowlisted')
            for attempt in row.get('attempts', []):
                require(set(attempt) <= {'identity', 'wire_digest', 'intent_digest', 'replay_key', 'wire_replay_key'},
                        'Attempt fields not sanitized')
                for key in ('wire_digest', 'intent_digest'):
                    if key in attempt:
                        require(DIGEST.fullmatch(attempt[key]), 'Invalid attempt digest')
                if 'identity' in attempt:
                    require(IDENTITY.fullmatch(attempt['identity']), 'Invalid signed identity')
            for execution in row.get('executions', []):
                require(set(execution) <= {'identity', 'outcome', 'effects', 'fee', 'canonical', 'finalized',
                                           'block_hash', 'block_number', 'nonce', 'fee_components', 'slot'},
                        'Execution fields not allowlisted')
                require(set(execution.get('fee_components', {})) <= {'l1Fee', 'operatorFee'}, 'Unexpected fee fields')
                for effect, value in execution.get('effects', {}).items():
                    require(effect.startswith(('balance:', 'storage:')) and type(value) is int
                            or effect == 'unexpected_recipient' and isinstance(value, str)
                            or effect == 'reverted_storage_persisted' and type(value) is int,
                            'Effect fields not allowlisted')
            for key, fields in [('terminal', {'identity', 'outcome'}),
                                ('retained', {'signed_attempt', 'journal_state', 'queue_state'}),
                                ('orphaned_execution', {'identity', 'replay_key', 'block_hash', 'block_number', 'outcome'})]:
                if row.get(key) is not None:
                    require(set(row[key]) <= fields, 'Nested observation fields not allowlisted')
            count += 1
    return count


def rpc_privacy(path, expected_count):
    counts = collections.Counter()
    with Path(path).open() as stream:
        for number, line in enumerate(stream, 1):
            row = json.loads(line)
            require(set(row) <= {'sequence', 'utc', 'elapsed_seconds', 'event', 'method', 'wire_digest',
                                 'identity', 'response_dropped', 'batch', 'accepted_identities', 'forwarded'},
                    'RPC fields not allowlisted')
            require(row['event'] in {'send_forwarded', 'send_accepted', 'http_response_lost',
                                    'rpc_faults_released', 'rpc_error_injected'}, 'RPC event not allowlisted')
            require(row['sequence'] == number, 'RPC audit sequence gap')
            if 'wire_digest' in row:
                require(DIGEST.fullmatch(row['wire_digest']), 'Invalid wire digest')
            if 'identity' in row:
                require(IDENTITY.fullmatch(row['identity']), 'Invalid accepted identity')
            if 'method' in row:
                require(row['method'] in {'eth_sendRawTransaction', 'sendTransaction'}, 'Unexpected send method')
            if 'accepted_identities' in row:
                require(isinstance(row['accepted_identities'], list)
                        and len(row['accepted_identities']) <= 1024
                        and all(isinstance(x, str) and IDENTITY.fullmatch(x) for x in row['accepted_identities']),
                        'Unsanitized lost-response identities')
            for key in ('batch', 'response_dropped', 'forwarded'):
                if key in row:
                    require(type(row[key]) is bool, 'Invalid RPC flag')
            counts[row['event']] += 1
    require(sum(counts.values()) == expected_count, 'RPC event count differs from report')
    return dict(counts)


def archive(args):
    began = time.monotonic()
    report, state = parsed(args.report), parsed(args.state)
    digest = sha(args.report.read_bytes())
    require(Path(state['report']).resolve() == args.report.resolve(), 'State names a different report')
    result = state.get('result')
    require(isinstance(result, dict) and result.get('report', {}).get('sha256') == digest,
            'No closed one-shot result matching report')
    # Deliberately retain active/review_required and every original verdict.
    # An intended quarantine is archival evidence, never automatically settled.
    for name, expected in report['harness_sha256'].items():
        require(state['frozen_sha256'][str(ROOT / 'scripts' / name)] == expected, 'Frozen harness mismatch')
    require(state['frozen_sha256'][str(ROOT / 'target/release/thirdweb-engine')] == report['engine_binary_sha256'],
            'Frozen Engine mismatch')
    require(not args.output.exists(), 'Output exists; never overwrite evidence')
    copies, origins, checks = [], {}, {}

    def plan(source, name, compress=False):
        source = Path(source)
        require(source.is_file() and source.suffix not in {'.sqlite', '.aof', '.rdb', '.log'}, 'Forbidden/missing source')
        data = source.read_bytes()
        if source.suffix in {'.json', '.jsonl'}:
            for value in ([json.loads(data)] if source.suffix == '.json' else
                          (json.loads(line) for line in data.splitlines() if line)):
                privacy(value)
        copies.append((name, data, compress))
        origins[name] = {'source': str(source), 'source_sha256': sha(data), 'source_bytes': len(data),
                         'lossless_gzip': compress}

    plan(args.report, 'full-report.json.gz', True)
    plan(args.state, 'one-shot-state.json')
    if report.get('observation_evidence'):
        source = Path(report['observation_evidence'])
        checks['observation_rows_privacy_checked'] = observation_privacy(source)
        plan(source, 'observations.jsonl.gz')
    else:
        checks['observations'] = 'Absent in original report; no completion inferred'
    for chain, rpc in report.get('rpc', {}).items():
        require(re.fullmatch(r'[A-Za-z0-9_]+', chain), 'Invalid chain filename')
        for wire, item in rpc.get('accepted_wires', {}).items():
            require(DIGEST.fullmatch(wire) and set(item) <= {'identity', 'accepted_responses'}, 'Unsanitized RPC map')
            require(IDENTITY.fullmatch(item['identity']), 'Invalid RPC identity')
        source = Path(rpc['audit']['path'])
        checks[chain + '_rpc_events'] = rpc_privacy(source, rpc['audit']['event_count'])
        plan(source, chain + '-rpc-audit.jsonl.gz', True)
    if report.get('resource_guard', {}).get('evidence'):
        plan(report['resource_guard']['evidence'], 'resources.jsonl')
    preflight = args.report.with_suffix('.resource-preflight.jsonl')
    if preflight.exists():
        plan(preflight, 'resource-preflight.jsonl')
    for label, paths in [('independent-audit', args.audit), ('analysis', args.analysis)]:
        for index, source in enumerate(paths, 1):
            data = parsed(source)
            records = data.get('reports', [data])
            linked = [item[key] for item in records for key in
                      ('report_sha256', 'report_sha256_uncompressed', 'source_report_sha256') if key in item]
            require(digest in linked and all(value == digest for value in linked), 'Supplement links different/missing report')
            if 'observations_sha256' in data:
                require(data['observations_sha256'] == sha(Path(report['observation_evidence']).read_bytes()),
                        'Supplement observation hash mismatch')
            plan(source, label + '-' + str(index) + '.json')
    plan(Path(__file__), 'archive-source.py')
    args.output.mkdir(parents=True)
    for name, data, compressed in copies:
        target = args.output / name
        with target.open('xb') as stream:
            stream.write(gzip.compress(data, compresslevel=1, mtime=0) if compressed else data)
        require((gzip.decompress(target.read_bytes()) if compressed else target.read_bytes()) == data, 'Copy mismatch')
    summary = {'original_outcome': report.get('outcome'), 'original_oracle': report.get('oracle'),
               'original_review_required': state.get('review_required'), 'original_result': result,
               'requested_fault_validated': report.get('requested_fault_validated'),
               'queue_settings': report.get('queue_settings'), 'drain': report.get('drain'),
               'archive_safety_verdict': 'None; separate operator and independent audit review remains authoritative',
               'privacy_and_provenance_checks': checks, 'sources': origins}
    (args.output / 'provenance.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
    (args.output / 'README.md').write_text('''# Closed chaos evidence

Exact original report, one-shot state, sanitized observations, digest-only full RPC audits, resources and supplied independent audit/analysis are preserved here. Read full-report.json.gz and the independent audit for the actual fault and outcome. Privacy/provenance checks do not independently prove execution safety or successful recovery.

The original active custody and review-required fields remain unchanged. This helper never clears them, selects capacity, resumes work or treats deliberate quarantine as settled. Same-ID retries may first admit original requests; the independent review must reconcile that union, not merely original HTTP202 counts. Lost HTTP envelopes can contain more accepted identities than the configured selected fault count.

Private journals, AOF, node state, keys, environment, raw signed payloads and unstructured logs are excluded. Original sources are untouched. Hashes and source paths are in provenance.json; manifest.json is written last. A partial directory without that manifest is not a completed archive. These are local-node chaos results, not public-chain capacity certification.
''')
    files = {str(p.relative_to(args.output)): {'bytes': p.stat().st_size, 'sha256': sha(p.read_bytes())}
             for p in args.output.iterdir() if p.is_file()}
    (args.output / 'manifest.json').write_text(json.dumps({'files': files, 'archive_wall_seconds': time.monotonic() - began},
                                                        indent=2, sort_keys=True) + '\n')
    print(json.dumps({'output': str(args.output), 'files': len(files) + 1, 'seconds': time.monotonic() - began}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('report', 'state', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--audit', type=Path, action='append', required=True)
    parser.add_argument('--analysis', type=Path, action='append', required=True)
    args = parser.parse_args()
    with Path('/tmp/engine-capacity-bracket.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        archive(args)
