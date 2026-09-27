#!/usr/bin/env python3
"""Descriptive supplement only: no automatic capacity verdict or threshold edits."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics


def digest(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def at(points, target):
    for (a, x), (b, y) in zip(points, points[1:]):
        if a <= target <= b:
            return x + (y - x) * (target - a) / (b - a)
    if target == points[-1][0]: return points[-1][1]
    raise ValueError('Interpolation outside observations')


def describe(report, name):
    profile = report['profiles'][name]
    width = 60 if profile.get('block_seconds') == 12 else 30
    warmup = report['warmup_seconds']
    rows = sorted([row for row in report['samples'] if row['phase'] == 'load'
                   and row['seconds'] >= warmup], key=lambda row: row['seconds'])
    if len(rows) < 3: return {'insufficient_samples': True}
    end = rows[-1]['seconds']
    output = {'aggregation_seconds': width, 'sample_window_seconds': [rows[0]['seconds'], end],
              'method': 'sample quantiles; nominal phase only; descriptive, not a pass/fail test'}
    for label, stage in [('unsigned', 'attempted'), ('terminal', 'terminal')]:
        points = [(row['seconds'], row['chains'][name]['admitted'] - row['chains'][name][stage]) for row in rows]
        groups = []
        start = warmup
        while start + width <= end + .05:
            values = [value for sec, value in points if start <= sec < start + width]
            if len(values) >= 2:
                q = statistics.quantiles(values, n=10, method='inclusive')
                groups.append({'start_seconds': start, 'sample_count': len(values),
                               'min': min(values), 'max': max(values), 'mean': statistics.mean(values),
                               'p10': q[0], 'median': q[4], 'p90': q[8]})
            start += width
        # Fixed integral-cycle duration common to all observed start offsets.
        periods = math.floor((end - points[0][0] - width + .05) / width)
        elapsed = periods * width
        paired = []
        if periods >= 1:
            for start, value in points:
                if start >= points[0][0] + width: break
                stop = start + elapsed
                if stop <= end:
                    paired.append({'start_seconds': start, 'end_seconds': stop,
                                   'growth': at(points, stop) - value})
        tail = [value for sec, value in points if sec >= end - 180]
        output[label] = {'groups': groups, 'last_180_seconds_range': [min(tail), max(tail)],
                         'paired_phase_growth': paired,
                         'paired_phase_growth_min_median_max': [min(x['growth'] for x in paired),
                             statistics.median(x['growth'] for x in paired), max(x['growth'] for x in paired)] if paired else None}
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = json.loads(args.report.read_bytes())
    result = {'report': str(args.report), 'report_sha256': digest(args.report),
              'descriptor_sha256': digest(__file__), 'interpretation_policy_sha256': digest(Path(__file__).with_name('interpretation-v1.md')),
              'chains': {name: describe(report, name) for name in report['profiles']}}
    with args.output.open('x') as stream: json.dump(result, stream, indent=2, sort_keys=True); stream.write('\n')


if __name__ == '__main__': main()
