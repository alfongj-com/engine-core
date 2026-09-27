import gzip,hashlib,json,pathlib,re,shutil,time
ROOT=pathlib.Path('/Users/alfongj/Code/engine-core')
OUT=ROOT/'docs/baselines/capacity-2026-09-26/shared-high-83f42c85237c'
SRC=pathlib.Path('/private/tmp/capacity-shared-individual-inputs-83f42c85237c.json')
STATE=pathlib.Path('/tmp/engine-capacity-shared-high-state.json')
started=time.monotonic();report=json.loads(SRC.read_text());state=json.loads(STATE.read_text())
assert state['active'] is None and state['review_required'] is None and state['runs'][0]['safely_settled']
assert report['owned_children_stopped'] and report['oracle']['safety_pass']
OUT.mkdir(exist_ok=False)
origins={}
def sha(b):return hashlib.sha256(b).hexdigest()
def copy(src,name,compress=False):
 src=pathlib.Path(src);data=src.read_bytes();dest=OUT/name;dest.parent.mkdir(exist_ok=True,parents=True)
 dest.write_bytes(gzip.compress(data,compresslevel=1,mtime=0) if compress else data)
 assert (gzip.decompress(dest.read_bytes()) if compress else dest.read_bytes())==data
 origins[name]={'source':str(src),'source_sha256':sha(data),'source_bytes':len(data),'lossless_gzip':compress}
 return data
assert sha(SRC.read_bytes())==state['runs'][0]['report_sha256']=='45c5332cd337a17d202abfe41bf28995c976f66e9b2ed4145e11cd9fc23ef6ba'
# Only generated public report fields; no full attempt payload/raw wire export.
forbidden={'privateKey','private_key','signedTransaction','signed_transaction','secretKey','secret_key','authorizationHeader'}
def inspect(v):
 if isinstance(v,dict):
  assert not(forbidden & set(v))
  for x in v.values():inspect(x)
 elif isinstance(v,list):
  for x in v:inspect(x)
inspect(report)
for name,rpc in report['rpc'].items():
 for digest,item in rpc['accepted_wires'].items():
  assert re.fullmatch('[0-9a-f]{64}',digest) and set(item)<= {'identity','accepted_responses'}
copy(SRC,'full-report.json.gz',True)
copy(report['observation_evidence'],'observations.jsonl.gz')
copy(STATE,'supervisor-final-state.json')
copy('/tmp/engine-capacity-shared-high-supervisor.log','supervisor.log')
copy('/tmp/engine-capacity-shared-high-prepare.log','prepare.log')
copy(SRC.with_suffix('.log'),'campaign.log')
copy(SRC.with_suffix('.resource-preflight.jsonl'),'resource-preflight.jsonl')
copy(report['resource_guard']['evidence'],'resources.jsonl')
copy(state['runs'][0]['assessment'],'strict-assessment.json')
copy('/tmp/engine-capacity-dispatch-plan/shared-individual-inputs-jobs.json','frozen-jobs.json')
copy(ROOT/'docs/baselines/capacity-2026-09-26/supervisor-v4/runner.py','supervisor-source.py')
copy(ROOT/'scripts/capacity_assess.py','strict-assessor-source.py')
audit_counts={}
allowed={'sequence','utc','elapsed_seconds','event','method','wire_digest','identity','response_dropped'}
for chain,rpc in report['rpc'].items():
 path=pathlib.Path(rpc['audit']['path']);count=0;events={}
 for line in path.read_text().splitlines():
  row=json.loads(line);assert set(row)<=allowed
  assert row['event'] in {'send_forwarded','send_accepted','rpc_faults_released'}
  if 'wire_digest' in row:assert re.fullmatch('[0-9a-f]{64}',row['wire_digest'])
  if 'identity' in row:assert re.fullmatch('(?:0x[0-9a-fA-F]{64}|[1-9A-HJ-NP-Za-km-z]{64,90})',row['identity'])
  if 'method' in row:assert row['method'] in {'eth_sendRawTransaction','sendTransaction'}
  count+=1;events[row['event']]=events.get(row['event'],0)+1
 assert count==rpc['audit']['event_count']
 copy(path,f'{chain}-rpc-audit.jsonl.gz',True);audit_counts[chain]={'rows':count,'events':events}
source_checks={}
for p,digest in state['frozen_sha256'].items():
 if p.endswith('.py') or p.endswith('jobs.json'):
  source_checks[p]={'frozen_sha256':digest,'current_sha256':sha(pathlib.Path(p).read_bytes())}
  assert source_checks[p]['current_sha256']==digest
summary={'cohort':state['id'],'offered_vector':{k:v['rate'] for k,v in report['profiles'].items()},'total_offered':report['oracle']['offered_intents'],'accepted_observed':report['oracle']['observed_intents'],'safety_pass':report['oracle']['safety_pass'],'all_offer_liveness_pass':report['oracle']['liveness_pass'],'capacity_selected':False,'raw_outcome':report['outcome'],'supervisor_safely_settled':state['runs'][0]['safely_settled'],'drain':report['drain'],'owned_children_stopped':report['owned_children_stopped'],'independent_captured_evidence_audit':'pending; append without changing original report','engine_binary_sha256':report['engine_binary_sha256'],'harness_sha256':report['harness_sha256'],'per_chain':{k:{'responses':v['responses'],'client':v['client'],'late_window':v['late_window'],'rpc_method_errors':{m:x['errors'] for m,x in report['rpc'][k]['methods'].items() if x['errors']}} for k,v in report['per_chain'].items()},'privacy_scope':'Generated observations copied unchanged from pinned sanitizing harness; per-ID independent audit follows. Digest-only full RPC audits checked against event/key/value allowlists; private signed payloads, journal, AOF, node files and credentials excluded.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2,sort_keys=True)+'\n')
(OUT/'source-integrity.json').write_text(json.dumps({'report_sha256':sha(SRC.read_bytes()),'frozen_source_checks':source_checks,'audit_allowlist_checked':audit_counts,'sources':origins},indent=2,sort_keys=True)+'\n')
copy(__file__,'archive-source.py')
(OUT/'README.md').write_text('''# Shared four-chain overload screen

**235 offered TPS was not sustained.** This is one Engine, one FULL journal/Redis and one signer per family, with EVM60/OP60/Nitro50/Solana65 offered for360 seconds. The rates came from individual screens, not qualified sustainable maxima.

The original report remains `incomplete`: admission429s prevented all-offer liveness. Its captured-effect safety oracle passed and all accepted work eventually drained, with all nine counters zero and owned children stopped. That accepted-work result does not promote the offered rate. The final supervisor permits only this precisely identified missing-offer case to settle custody; it does not mark capacity selected.

See [summary](summary.json) for each chain's admitted/attempted/terminal rates, unsigned/terminal backlog, rejection counts and RPC method errors. The EVM queues grew sharply while Solana continued making progress; do not infer a root cause or fairness guarantee from this single run. Read the exact [full report](full-report.json.gz) and [strict assessment](strict-assessment.json).

The archive preserves original report/observations, full digest-only RPC audit streams, resource evidence, jobs and frozen supervisor source/state. [Source integrity](source-integrity.json) records original-byte hashes, lossless compression and RPC event/key/value checks. Private journals/AOF/node state/keys/raw signed payloads are excluded. Independent per-ID audit and descriptive analyst outputs are pending and may be appended separately.

This was local execution only: Anvil cadence/OP execution, Nitro dev L2 without L1 settlement, local Agave. No public-network capacity or paidRPC result is implied.
''')
files={str(p.relative_to(OUT)):{'bytes':p.stat().st_size,'sha256':sha(p.read_bytes())} for p in OUT.rglob('*') if p.is_file()}
(OUT/'manifest.json').write_text(json.dumps({'files':files,'archive_wall_seconds':time.monotonic()-started},indent=2,sort_keys=True)+'\n')
print(json.dumps({'path':str(OUT),'files':len(files)+1,'bytes':sum(v['bytes'] for v in files.values()),'seconds':time.monotonic()-started,'accepted':summary['accepted_observed']}))
