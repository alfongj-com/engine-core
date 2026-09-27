#!/usr/bin/env python3
"""Archive one closed nominal cohort; never run alongside a workload."""
import argparse
import fcntl
import gzip
import importlib.util
import json
from pathlib import Path
import time

ROOT = Path('/Users/alfongj/Code/engine-core')
UTIL = Path('/tmp/engine-chaos-archive.py')
UTIL_SHA = '3af1736c8c27923dbd85578b6a43058910a2891cae4894fa3ace4481356aa90d'
# Reuse the reviewed privacy/provenance helpers, not their one-shot verdict logic.
import hashlib
if hashlib.sha256(UTIL.read_bytes()).hexdigest() != UTIL_SHA:
    raise RuntimeError('Reviewed archival utility changed')
spec = importlib.util.spec_from_file_location('chaos_archive_privacy', UTIL)
u = importlib.util.module_from_spec(spec)
spec.loader.exec_module(u)


def archive(args):
    began = time.monotonic()
    state = u.parsed(args.state)
    u.require(u.re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', state['id']), 'Invalid cohort identity')
    u.require(state.get('active') is None, 'Cohort still active or retained for review')
    u.require(1 <= len(state['jobs']) == len(state['runs']) <= 12, 'Nominal cohort not closed')
    names = [job['name'] for job in state['jobs']]
    u.require(len(names) == len(set(names)) and all(u.re.fullmatch(r'[a-z0-9][a-z0-9-]{0,95}', name) for name in names), 'Invalid/duplicate job names')
    audit_map = u.parsed(args.audit_map)
    u.require(isinstance(audit_map, dict) and set(audit_map) == set(names), 'Audit map must name every job exactly once')
    u.require(all(isinstance(value, str) for value in audit_map.values()), 'Audit map values must be paths')
    runs = {run['name']: run for run in state['runs']}
    u.require(set(runs) == set(names), 'Missing/duplicate closed run')
    u.require(not args.output.exists(), 'Output exists; never overwrite')
    planned, origins, checks, summary = [], {}, {}, {}

    def plan(source, name, compress=False):
        source = Path(source)
        u.require(source.is_file() and source.suffix not in {'.sqlite', '.aof', '.rdb', '.log'}, 'Missing/forbidden source')
        data = source.read_bytes()
        if source.suffix in {'.json', '.jsonl'}:
            rows = [json.loads(data)] if source.suffix == '.json' else (json.loads(line) for line in data.splitlines() if line)
            for row in rows: u.privacy(row)
        u.require(name not in origins, 'Duplicate archive name')
        planned.append((name, data, compress))
        origins[name] = {'source': str(source), 'source_sha256': u.sha(data),
                         'source_bytes': len(data), 'lossless_gzip': compress}

    frozen = state['frozen_sha256']
    checked_sources = {}
    for source, digest in frozen.items():
        if source.endswith('.py') or source.endswith('jobs.json'):
            u.require(u.sha(Path(source).read_bytes()) == digest, 'Frozen source/config drift: ' + source)
            checked_sources[source] = digest
    u.require(u.sha(args.jobs.read_bytes()) == frozen[str(args.jobs.resolve())], 'Frozen jobs hash mismatch')
    u.require(u.parsed(args.jobs) == state['jobs'], 'Frozen jobs content mismatch')
    descriptor = Path('/tmp/capacity-posthoc/describe_backlogs.py')
    policy = Path('/tmp/capacity-posthoc/interpretation-v1.md')

    for index, name in enumerate(names, 1):
        label = f'{index:02d}-' + name
        run = runs[name]
        source = Path(run['report'])
        report = u.parsed(source)
        digest = u.sha(source.read_bytes())
        u.require(digest == run['report_sha256'], 'Report differs from closed state')
        u.require(report['engine_binary_sha256'] == frozen[str(ROOT / 'target/release/thirdweb-engine')], 'Engine identity mismatch')
        for filename, expected in report['harness_sha256'].items():
            u.require(expected == checked_sources[str(ROOT / 'scripts' / filename)], 'Harness identity mismatch')
        audit_path = Path(audit_map[name])
        audit = u.parsed(audit_path)
        u.require(audit['report_sha256'] == digest, 'Independent audit report mismatch')
        observation_path = Path(report['observation_evidence'])
        u.require(audit['observations_sha256'] == u.sha(observation_path.read_bytes()), 'Independent audit observations mismatch')
        checks[label] = {'observation_rows_privacy_checked': u.observation_privacy(observation_path), 'rpc_events': {}}
        assessment = Path(run['assessment'])
        u.require(u.sha(assessment.read_bytes()) == run['assessment_sha256'], 'Strict assessment drift')
        plan(source, label + '/full-report.json.gz', True)
        plan(observation_path, label + '/observations.jsonl.gz')
        plan(audit_path, label + '/independent-audit.json')
        plan(assessment, label + '/strict-assessment.json')
        plan(source.with_suffix('.resource-preflight.jsonl'), label + '/resource-preflight.jsonl')
        plan(report['resource_guard']['evidence'], label + '/resources.jsonl')
        for chain, rpc in report['rpc'].items():
            u.require(u.re.fullmatch(r'[A-Za-z0-9_]+', chain), 'Invalid chain filename')
            for wire, accepted in rpc.get('accepted_wires', {}).items():
                u.require(u.DIGEST.fullmatch(wire) and set(accepted) <= {'identity', 'accepted_responses'}, 'Unsanitized RPC map')
                u.require(u.IDENTITY.fullmatch(accepted['identity']), 'Invalid RPC identity')
            checks[label]['rpc_events'][chain] = u.rpc_privacy(rpc['audit']['path'], rpc['audit']['event_count'])
            plan(rpc['audit']['path'], label + '/' + chain + '-rpc-audit.jsonl.gz', True)
        for kind in ('compact', 'summary', 'backlog-description'):
            path = source.with_name(source.stem + '-' + kind + '.json')
            analytic = u.parsed(path)
            links = [record[key] for record in analytic.get('reports', [analytic]) for key in
                     ('report_sha256', 'report_sha256_uncompressed', 'source_report_sha256') if key in record]
            u.require(bool(links) and all(value == digest for value in links), 'Analyst report binding mismatch')
            if kind == 'backlog-description':
                u.require(analytic['descriptor_sha256'] == u.sha(descriptor.read_bytes()), 'Descriptor source drift')
                u.require(analytic['interpretation_policy_sha256'] == u.sha(policy.read_bytes()), 'Interpretation policy drift')
            plan(path, label + '/analyst-' + kind + '.json')
        summary[label] = {'original_run_record': run, 'original_outcome': report.get('outcome'),
            'original_oracle': report.get('oracle'), 'original_raw_capacity_candidate': report.get('all_chain_capacity_candidate'),
            'queue_settings': report['queue_settings'], 'drain': report.get('drain'),
            'late_windows': {chain: value['late_window'] for chain, value in report['per_chain'].items()},
            'independent_audit_result': audit.get('accepted_custody'),
            'archive_safety_verdict': 'None; original assessments and independent audits remain authoritative'}

    plan(args.state, 'supervisor-final-state.json')
    plan(args.jobs, 'frozen-jobs.json')
    plan(args.audit_map, 'audit-map.json')
    for source, name in [
        (ROOT / 'docs/baselines/capacity-2026-09-26/supervisor-v4/runner.py', 'supervisor-source.py'),
        (ROOT / 'scripts/capacity_assess.py', 'strict-assessor-source.py'),
        (Path('/tmp/engine-capacity-report-extract/extract.py'), 'compact-extractor-source.py'),
        (descriptor, 'backlog-descriptor-source.py'), (policy, 'interpretation-policy.md'),
        (UTIL, 'archive-privacy-source.py'), (Path(__file__), 'archive-source.py')]:
        plan(source, name)
    # All prerequisites are checked before creating the destination. No journal,
    # AOF, private node snapshots, raw signed bytes or runtime logs are copied.
    args.output.mkdir(parents=True)
    for name, data, compressed in planned:
        target = args.output / name
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as stream:
            stream.write(gzip.compress(data, compresslevel=1, mtime=0) if compressed else data)
        u.require((gzip.decompress(target.read_bytes()) if compressed else target.read_bytes()) == data, 'Copy mismatch')
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
    (args.output / 'source-integrity.json').write_text(json.dumps({
        'cohort': state['id'], 'original_review_required': state.get('review_required'),
        'checked_frozen_sources': checked_sources, 'privacy_and_provenance_checks': checks,
        'sources': origins}, indent=2, sort_keys=True) + '\n')
    job_rows = '\n'.join('| ' + job['name'] + ' | ' + ', '.join(job['chains']) + ' | ' + str(job['seconds']) + ' |'
                         for job in state['jobs'])
    (args.output / 'README.md').write_text(f'''# Closed nominal cohort {state['id']}

| Ordered job | Offered profile rates | Offer seconds |
|---|---|---:|
{job_rows}

This archive preserves original reports, supervisor state/jobs, strict assessments, descriptive summaries and independent captured-evidence audits. Accepted settlement, complete offered-workload success, raw capacity candidacy and sustained qualification remain distinct. Read the unchanged verdicts and backlog analyses; this archive adds no safety or capacity verdict and does not clear any review field.

These are local single-signer-per-chain fixtures sharing one Engine and durable storage. Exact queue/durability/client settings remain in the reports. Anvil EVM/OP cadence, dev L2-only Nitro and local Agave do not qualify public-chain capacity. Ordered runs share a host and may reuse the Nitro database. Earlier failed runs remain separate evidence; later success does not repair unknown outcomes.

Full reports are losslessly compressed; sanitized observation gzip files are unchanged. Full RPC audits contain only allowlisted events, signed identities and digests. Resources, frozen jobs/state and source bindings are retained. Private journals, AOF, node snapshots, keys, environments, raw signed payloads and unstructured logs are excluded. Copy provenance is in source-integrity.json. manifest.json is written last; a partial directory without it is not a completed archive. Original files and runner state are untouched.
''')
    files = {str(path.relative_to(args.output)): {'bytes': path.stat().st_size, 'sha256': u.sha(path.read_bytes())}
             for path in sorted(args.output.rglob('*')) if path.is_file()}
    (args.output / 'manifest.json').write_text(json.dumps({'files': files,
        'archive_wall_seconds': time.monotonic() - began}, indent=2, sort_keys=True) + '\n')
    print(json.dumps({'output': str(args.output), 'files': len(files) + 1, 'seconds': time.monotonic() - began}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('state', 'jobs', 'audit-map', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    with Path('/tmp/engine-capacity-bracket.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        archive(args)
