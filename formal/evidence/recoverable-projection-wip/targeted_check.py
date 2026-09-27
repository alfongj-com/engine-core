import importlib.util,json,hashlib,time
from pathlib import Path
repo=Path('/Users/alfongj/Code/engine-core')
spec=importlib.util.spec_from_file_location('formal_check',repo/'formal/check.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
java='/tmp/engine-core-formal/jdk-21.0.12.1+1-jre/Contents/Home/bin/java'
jar=Path('/tmp/engine-core-formal/tla2tools-1.7.4.jar')
assert m.sha256(jar)==m.TLC_SHA256
out=Path('/tmp/engine-projection-formal-review');out.mkdir(exist_ok=True)
cases=[r for r in json.loads((repo/'formal/models.json').read_text()) if r['module'].startswith('DisasterRecovery')]
results=[]
for case in cases:
 results.append(m.run_model(case,java,jar,out,180))
 (out/'report.json').write_text(json.dumps({'scope':'targeted model checks; runtime source-map refresh explicitly deferred until runtime final','passed':all(r['passed'] for r in results),'completed':len(results),'planned':len(cases),'results':results},indent=2)+'\n')
 if not results[-1]['passed']: raise SystemExit(1)
