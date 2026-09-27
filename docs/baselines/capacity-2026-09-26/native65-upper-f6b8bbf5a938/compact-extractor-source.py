#!/usr/bin/env python3
"""One-read descriptive extraction. No RPC/SQLite/audit/per-ID scans or rate verdict."""
import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import statistics

STAGES = ('admitted', 'attempted', 'included', 'terminal', 'finalized')
DRAIN = {'client_pending', 'http_inflight', 'journal_unresolved', 'node_pending',
         'redis_active', 'redis_borrowed', 'redis_delayed', 'redis_pending', 'redis_submitted'}


def number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def quantile(values, q):
    values = sorted(values)
    x = (len(values) - 1) * q
    lo, hi = math.floor(x), math.ceil(x)
    return values[lo] + (values[hi] - values[lo]) * (x - lo)


def trend(xs, ys):
    duration = xs[-1] - xs[0]
    mx, my = statistics.mean(xs), statistics.mean(ys)
    variance = sum((x - mx) ** 2 for x in xs)
    return {'start': ys[0], 'end': ys[-1], 'growth': ys[-1] - ys[0],
            'growth_per_second': (ys[-1] - ys[0]) / duration,
            'sample_ols_slope_per_second': sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / variance if variance else None,
            'time_weighted_mean': sum((b - a) * (y + z) / 2 for a, b, y, z in zip(xs, xs[1:], ys, ys[1:])) / duration,
            'sample_p10': quantile(ys, .1), 'sample_p50': quantile(ys, .5), 'sample_p90': quantile(ys, .9),
            'min': min(ys), 'max': max(ys)}


def cpu_seconds(value):
    days, value = value.split('-', 1) if '-' in value else ('0', value)
    return int(days) * 86400 + sum(float(v) * 60**i for i, v in enumerate(reversed(value.split(':'))))


def window(rows, chain, requested_start, requested_end):
    start = min(range(len(rows)), key=lambda i: abs(rows[i]['seconds'] - requested_start))
    end = min(range(len(rows)), key=lambda i: abs(rows[i]['seconds'] - requested_end))
    rows = rows[start:end + 1]
    if len(rows) < 2:
        return {'available': False, 'reason': 'fewer than two load samples'}
    xs = [r['seconds'] for r in rows]
    if any(b <= a for a, b in zip(xs, xs[1:])):
        return {'available': False, 'reason': 'non-increasing sample timestamps'}
    a, b = rows[0]['chains'][chain], rows[-1]['chains'][chain]
    duration = xs[-1] - xs[0]
    counter_errors, rates = [], {}
    for field in STAGES:
        values = [r['chains'][chain].get(field) for r in rows]
        if not all(number(v) for v in values):
            continue
        if any(y < x for x, y in zip(values, values[1:])):
            counter_errors.append(field + ': decreased between load samples')
            continue
        rates[field] = (values[-1] - values[0]) / duration
    backlogs = {}
    if all(number(r['chains'][chain].get('admitted')) for r in rows):
        for label, field in [('unsigned', 'attempted'), ('terminal', 'terminal')]:
            if all(number(r['chains'][chain].get(field)) for r in rows):
                backlogs[label] = trend(xs, [r['chains'][chain]['admitted'] - r['chains'][chain][field] for r in rows])
    methods = {}
    for name, last in b.get('rpc', {}).get('methods', {}).items():
        first = a.get('rpc', {}).get('methods', {}).get(name, {})
        count = last.get('count', 0) - first.get('count', 0)
        total = last.get('total_ms', 0) - first.get('total_ms', 0)
        errors = last.get('errors', 0) - first.get('errors', 0)
        if min(count, total, errors) < 0:
            counter_errors.append(name + ': RPC counters reset')
            continue
        if count:
            methods[name] = {'count': count, 'requests_per_second': count / duration,
                             'errors': errors, 'mean_ms': total / count,
                             'max_ms_through_window_end': last.get('max_ms'),
                             'completed_call_time_equivalent': total / (duration * 1000)}
    # This is observed stage interleaving, not a tracing-based stall attribution.
    plateau = []
    for left, right in zip(rows, rows[1:]):
        l, r = left['chains'][chain], right['chains'][chain]
        if all(number(x.get(k)) for x in (l, r) for k in ('admitted', 'attempted', 'terminal')):
            if l['attempted'] == r['attempted'] and min(l['admitted'] - l['attempted'], r['admitted'] - r['attempted']) > 0:
                dt = right['seconds'] - left['seconds']
                plateau.append({'from': left['seconds'], 'to': right['seconds'], 'seconds': dt,
                                'terminal_delta': r['terminal'] - l['terminal']})
    gaps = [y - x for x, y in zip(xs, xs[1:])]
    cpu = {}
    left_processes = {v['name']: v for v in rows[0].get('resources', {}).get('processes', [])}
    for process in rows[-1].get('resources', {}).get('processes', []):
        old = left_processes.get(process['name'])
        if old and old.get('pid') == process.get('pid') and 'cpu_time' in old and 'cpu_time' in process:
            delta = cpu_seconds(process['cpu_time']) - cpu_seconds(old['cpu_time'])
            if delta >= 0: cpu[process['name']] = delta * 100 / duration
    return {'available': True, 'requested_from_seconds': requested_start, 'requested_to_seconds': requested_end,
            'from_seconds': xs[0], 'to_seconds': xs[-1], 'seconds': duration, 'samples': len(rows),
            'sample_gap_seconds': {'median': statistics.median(gaps), 'max': max(gaps)},
            'stage_rates_per_second': rates, 'backlogs': backlogs, 'counter_errors': counter_errors,
            'engine_rpc_requests_per_second': sum(m['count'] for m in methods.values()) / duration,
            'engine_rpc_methods': methods,
            'proxy_peak_all_method_inflight_through_end': b.get('rpc', {}).get('peak_inflight'),
            'observer_duration_ms_max': max((r.get('observer_duration_ms', 0) for r in rows), default=0),
            'observed_attempt_plateaus_with_unsigned_work': {
                'intervals': len(plateau), 'total_seconds': sum(p['seconds'] for p in plateau),
                'longest_single_sample_interval_seconds': max((p['seconds'] for p in plateau), default=0),
                'intervals_with_terminal_progress': sum(p['terminal_delta'] > 0 for p in plateau),
                'terminal_delta_total': sum(p['terminal_delta'] for p in plateau),
                'first_eight_intervals': plateau[:8]},
            'cpu_one_core_percent': cpu}


def extract(path):
    data = path.read_bytes()  # One report read; no per-ID or RPC-audit read.
    if path.suffix == '.gz': data = gzip.decompress(data)
    report = json.loads(data)
    end = report.get('offered_phase_end_seconds', report.get('duration_seconds'))
    warmup = report.get('warmup_seconds')
    events = report.get('events', [])
    result = {'report': str(path), 'report_sha256_uncompressed': hashlib.sha256(data).hexdigest(),
              'binary_sha256': report.get('engine_binary_sha256'), 'harness_sha256': report.get('harness_sha256'),
              'source_commit': report.get('source_commit'), 'source_is_dirty': bool(report.get('source_status_porcelain')),
              'configuration': {k: report.get(k) for k in ('duration_seconds','warmup_seconds','http_concurrency','max_schedule_lag_ms','eoa_broadcast_concurrency','eoa_max_inflight_per_wallet','solana_workers','solana_confirmation_poll_seconds','durability')},
              'outcome': report.get('outcome'), 'oracle': report.get('oracle'), 'drain': report.get('drain'),
              'exact_oracle_safety_reported': report.get('oracle', {}).get('safety_pass') is True,
              'exact_oracle_liveness_reported': report.get('oracle', {}).get('liveness_pass') is True,
              'complete_zero_drain_reported': isinstance(report.get('drain'), dict) and set(report['drain']) == DRAIN and all(type(x) is int and x == 0 for x in report['drain'].values()),
              'owned_children_stopped': report.get('owned_children_stopped'),
              'drain_reached_load_seconds': next((e.get('load_elapsed_seconds') for e in events if e.get('event')=='drain_target_reached'), None),
              'observer_errors': sum(e.get('event') == 'observer_error' for e in events),
              'execution_errors': report.get('errors'), 'infrastructure_stop': report.get('infrastructure_stop'),
              'operator_review_required': report.get('operator_review_required'), 'chains': {}}
    for chain, profile in report.get('profiles', {}).items():
        per = report.get('per_chain', {}).get(chain, {})
        client, responses = per.get('client', {}), per.get('responses', {})
        rows = [s for s in report.get('samples', []) if s.get('phase') == 'load' and chain in s.get('chains', {})]
        value = {'profile': profile, 'offered': per.get('offered'), 'admitted_http202': responses.get('202'),
                 'client_counts': client, 'client_drop_counts': {k:v for k,v in client.items() if k.startswith('dropped_')},
                 'http_responses': responses, 'transport_errors': per.get('transport_errors'),
                 'admission_latency_ms': {k:per.get(k) for k in ('http_service_latency_ms','scheduled_to_response_latency_ms','scheduled_to_http_start_lateness_ms')},
                 'original_late_window_assessment': per.get('late_window'), 'windows': {}}
        if rows and number(end) and number(warmup):
            value['windows']['post_warmup'] = window(rows, chain, warmup, end)
            value['windows']['last180'] = window(rows, chain, max(0, end - 180), end)
        else: value['window_error'] = 'missing load samples or numeric offered/warmup duration'
        final = report.get('rpc', {}).get(chain, {})
        value['whole_run_engine_rpc'] = {k:final.get(k) for k in ('calls','forwarded_sends','accepted_unique_wires','proxy_overloads','proxy_failures','proxy_failure_details','http_transport')}
        call_count = sum(final.get('calls', {}).values())
        admitted = responses.get('202')
        value['whole_run_engine_rpc'].update(total_method_calls=call_count, calls_per_admitted=call_count/admitted if number(admitted) and admitted else None)
        result['chains'][chain] = value
    assessment_path = path.with_name(path.stem + '-assessment.json')
    if assessment_path.exists():
        assessment = json.loads(assessment_path.read_text())
        result['preserved_strict_assessments'] = {chain:{k:value.get(k) for k in ('classification','reasons','aligned_mean_slopes_tps','persistent_growth_stages')} for row in assessment.get('reports', []) for chain,value in row.get('chains', {}).items()}
        result['strict_assessment_sha256'] = hashlib.sha256(assessment_path.read_bytes()).hexdigest()
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('reports',type=Path,nargs='+');p.add_argument('--output',type=Path);a=p.parse_args()
    result={'schema':'capacity-compact-readonly-v1','new_qualification_thresholds':False,
            'method':'Nearest existing load samples to configured post-warmup and last180 boundaries; actual sample duration denominators. Descriptive queue slopes/quantiles, no significance or stability verdict. Preserve original raw and strict assessments.',
            'limitations':['Exact oracle/drain are reported results, not reverified per-ID here.','Inclusion is live observer knowledge, not direct sequencer throughput; observer errors and lag remain visible.','RPC mean uses counter differences; max is cumulative through endpoint, not a window maximum.','Completed-call time equivalent is a latency-sum signal, not measured send-only concurrent occupancy; proxy peak includes all RPC methods.','Flat observed attempt intervals may include observer delay; they do not prove disk or confirmation causation.','Local gas/chain/finality fixtures and finite windows do not establish public-chain maxima.'],
            'reports':[extract(path) for path in a.reports]}
    text=json.dumps(result,indent=2,sort_keys=True)+'\n'
    if a.output:
        with a.output.open('x') as f:f.write(text)
    else: print(text,end='')


if __name__=='__main__':main()
