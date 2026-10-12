#!/usr/bin/env python3
"""Read-only stricter finite-window screening; does not establish an absolute maximum."""
import argparse
from fractions import Fraction
import hashlib
import json
import math
from pathlib import Path
import statistics

MIN_STEADY_SECONDS = 180
MIN_GROUPS = 3
MAX_BACKLOG_GROWTH_TX = 1
MAX_BACKLOG_SLOPE_TPS = .01


def slope(xs, ys):
    xm, ym = statistics.mean(xs), statistics.mean(ys)
    den = sum((x - xm) ** 2 for x in xs)
    return sum((x - xm) * (y - ym) for x, y in zip(xs, ys)) / den if den else 0


def interpolate(points, at):
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        if x0 <= at <= x1:
            return y0 + (y1 - y0) * (at - x0) / (x1 - x0)
    if at == points[-1][0]: return points[-1][1]
    raise ValueError('interpolation outside observations')


def mean_between(points, a, b):
    interior = [(x, y) for x, y in points if a < x < b]
    cut = [(a, interpolate(points, a)), *interior, (b, interpolate(points, b))]
    return sum((x1 - x0) * (y0 + y1) / 2 for (x0, y0), (x1, y1) in zip(cut, cut[1:])) / (b - a)


def assess(report, name, profile):
    reasons, notes = [], []
    offered = profile['rate']
    end = report.get('offered_phase_end_seconds', report.get('duration_seconds', 0))
    warmup = report.get('warmup_seconds', 60)
    rows = [r for r in report.get('samples', []) if r.get('phase') == 'load'
            and name in r.get('chains', {}) and warmup <= r['seconds'] <= end + .05]
    rows = sorted({r['seconds']: r for r in rows}.values(), key=lambda r: r['seconds'])
    result = {'offered_tps': offered, 'original_candidate': report.get('per_chain', {}).get(name, {}).get('late_window', {}).get('capacity_candidate'),
              'classification': 'unconfirmed', 'reasons': reasons, 'notes': notes}
    if len(rows) < 3:
        reasons.append('insufficient_post_warmup_telemetry'); return result
    xs = [r['seconds'] for r in rows]
    sample_interval = round(statistics.median(b-a for a,b in zip(xs,xs[1:])), 2)
    cadence = profile.get('block_seconds')
    if cadence:
        # Alignment is nominal, not proof of real block boundaries. We have no
        # per-sample head timestamp in existing reports; state that limitation.
        a, b = Fraction(str(cadence)), Fraction(str(sample_interval))
        period = math.lcm(a.numerator, b.numerator) / math.gcd(a.denominator, b.denominator)
        width = period * max(1, math.ceil(30 / period))
        notes.append('nominal_cadence_alignment; actual_block_head_series_not_recorded')
    else:
        width = sample_interval * max(1, math.ceil(30 / sample_interval))
        notes.append('no_fixed_block_cadence_assumed')
    groups = int((xs[-1] - xs[0] + .05) // width)
    if groups < 1:
        reasons.append('no_complete_aggregation_window'); return result
    start, stop = xs[0], min(xs[-1], xs[0] + groups * width)
    elapsed = stop - start
    fields = {field: [(r['seconds'], r['chains'][name][field]) for r in rows]
              for field in ('admitted','attempted','included','terminal')}
    rates = {field: (interpolate(points,stop)-interpolate(points,start))/elapsed for field,points in fields.items()}
    means, endpoints, trends = {}, {}, {}
    centers = [(start+i*width + min(stop,start+(i+1)*width))/2 for i in range(groups)]
    for label, stage in (('unsigned','attempted'),('terminal','terminal')):
        points = [(r['seconds'],r['chains'][name]['admitted']-r['chains'][name][stage]) for r in rows]
        means[label] = [mean_between(points,start+i*width,min(stop,start+(i+1)*width)) for i in range(groups)]
        endpoints[label] = {'start':interpolate(points,start),'end':interpolate(points,stop)}
        endpoints[label]['growth'] = endpoints[label]['end']-endpoints[label]['start']
        trends[label] = slope(centers,means[label])
    allowance = 1/elapsed  # One transaction of counter/endpoint resolution, not a rate percentage.
    if elapsed < MIN_STEADY_SECONDS or groups < MIN_GROUPS: reasons.append('confirmation_window_too_short')
    if any(b-a > sample_interval*2.5 for a,b in zip(xs,xs[1:])): reasons.append('telemetry_gap')
    if any(rate < offered-allowance for rate in rates.values()): reasons.append('stage_rate_below_offered_beyond_one_transaction')
    for label in endpoints:
        if endpoints[label]['growth'] > MAX_BACKLOG_GROWTH_TX: reasons.append(label+'_backlog_endpoint_growth')
        if trends[label] > MAX_BACKLOG_SLOPE_TPS: reasons.append(label+'_backlog_positive_aligned_mean_trend')
    complete = report.get('oracle',{}).get('safety_pass') and report.get('oracle',{}).get('liveness_pass')
    if not complete or report.get('outcome')!='pass': reasons.append('original_exact_qualification_incomplete')
    chain_report = report.get('per_chain',{}).get(name,{})
    if set(chain_report.get('responses',{})) != {'202'} or sum(chain_report.get('responses',{}).values()) != chain_report.get('offered'):
        reasons.append('intake_not_fully_clean')
    latency=chain_report.get('scheduled_to_response_latency_ms',{}).get('p99')
    limit=chain_report.get('late_window',{}).get('admission_p99_limit_ms',1000)
    if latency is None or latency > limit: reasons.append('admission_latency_limit')
    proxy=report.get('rpc',{}).get(name,{})
    if (proxy.get('proxy_overloads') or proxy.get('proxy_failures') or proxy.get('proxy_failure_details')
        or proxy.get('http_transport',{}).get('failures') or any(v.get('errors') for v in proxy.get('methods',{}).values())):
        reasons.append('rpc_errors_or_overload')
    if any(event.get('event') == 'observer_error' for event in report.get('events', [])):
        reasons.append('observer_error')
    if report.get('chaos')!='none' or report.get('errors') or report.get('journal_halted') or report.get('durable_chain_halts'):
        reasons.append('fault_or_execution_error')
    persistent=[]
    for label in means:
        values=means[label]
        if len(values)>=3 and all(b-a>1 for a,b in zip(values,values[1:])) and trends[label]>.01:
            persistent.append(label)
    if persistent:
        result['classification']='observed_persistent_backlog_growth'
    elif not reasons:
        result['classification']='steady_window_evidence_requires_repetition'
    elif any('backlog_' in x for x in reasons):
        result['classification']='unconfirmed_growth_or_cadence_ambiguity'
    result.update({'window_seconds':elapsed,'from_seconds':start,'to_seconds':stop,'aggregation_seconds':width,
        'complete_aggregation_windows':groups,'nominal_complete_block_cycles':elapsed/cadence if cadence else None,
        'sample_interval_seconds':sample_interval,'stage_rates_tps':rates,'one_transaction_rate_allowance_tps':allowance,
        'backlog_endpoints':endpoints,'aligned_mean_backlogs':means,'aligned_mean_slopes_tps':trends,
        'persistent_growth_stages':persistent})
    return result


def main():
    parser=argparse.ArgumentParser();parser.add_argument('reports',nargs='+',type=Path);parser.add_argument('--output',required=True,type=Path);args=parser.parse_args()
    results=[]
    for path in args.reports:
        raw=path.read_bytes();report=json.loads(raw)
        results.append({'report':str(path),'report_sha256':hashlib.sha256(raw).hexdigest(),
            'chains':{name:assess(report,name,profile) for name,profile in report.get('profiles',{}).items()}})
    output={'method':'strict-posthoc-v1','analyzer_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'limitations':['Finite windows do not prove an absolute or indefinitely sustainable maximum.',
          'Aggregation integrates observed samples linearly; original raw reports are preserved.',
          'Existing reports lack per-sample block heads, so cadence alignment is nominal.',
          'Positive ambiguous trends are not promoted; long repeated windows remain required.'],
        'thresholds':{'post_warmup_seconds':MIN_STEADY_SECONDS,'min_aligned_groups':MIN_GROUPS,
          'endpoint_growth_tx':MAX_BACKLOG_GROWTH_TX,'max_backlog_slope_tps':MAX_BACKLOG_SLOPE_TPS,'stage_deficit_allowance':'one transaction per analyzed window'},'reports':results}
    with args.output.open('x') as stream:json.dump(output,stream,indent=2,sort_keys=True);stream.write('\n')

if __name__=='__main__':main()
