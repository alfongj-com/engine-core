import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path('/Users/alfongj/Code/engine-core')
spec = importlib.util.spec_from_file_location('runner', ROOT / 'docs/baselines/capacity-2026-09-26/supervisor-v4/runner.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
plans = json.loads(Path('/tmp/engine-capacity-dispatch-plan/smoke-plans.json').read_text())
assert runner.sha(runner.ENGINE) == plans['expected_engine_sha256']
with open('/tmp/engine-capacity-bracket.lock', 'a') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    for key in ('all4_transfer', 'three_mixed'):
        plan = plans[key]
        output = Path(plan['argv'][-1])
        assert not output.exists()
        native = key == 'all4_transfer'
        try:
            if native:
                subprocess.run(runner.CONTROL + ['unpause', 'engine-nitro-dev'], check=True, timeout=30)
            with output.with_suffix('.log').open('x') as stream:
                code = runner.run_child(plan['argv'], stream, 906)
        finally:
            if native:
                subprocess.run(runner.CONTROL + ['pause', 'engine-nitro-dev'], check=True, timeout=30)
        report = json.loads(output.read_text())
        settled = runner.safely_settled(report)
        summary = {'smoke': key, 'exit_code': code, 'safely_settled': settled,
                   'report': str(output), 'sha256': runner.sha(output),
                   'expected_intents': plan['scheduled_intents'],
                   'observed_intents': report.get('oracle', {}).get('observed_intents'),
                   'capacity_candidate': report.get('all_chain_capacity_candidate')}
        print(json.dumps(summary), flush=True)
        assert code in (0, 2) and settled
        assert summary['observed_intents'] == plan['scheduled_intents']
        assert summary['capacity_candidate'] is False
