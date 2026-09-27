import base64,collections,gzip,hashlib,json,os,pathlib,re,sqlite3
from datetime import datetime,timezone
BASE=pathlib.Path('/tmp/engine-sol70-guard-incident')
DEST=pathlib.Path('/Users/alfongj/Code/engine-core/docs/baselines/capacity-2026-09-26/solana70-guard-incident')
DEST.mkdir(parents=True,exist_ok=True)
def sha(b):return hashlib.sha256(b).hexdigest()
def load(p):return json.loads(pathlib.Path(p).read_text())
def check(v,reason):
 if not v:raise RuntimeError(reason)
original_path=pathlib.Path('/tmp/capacity-dispatch-final-solana70-5057cdb7bab1.json'); original=load(original_path)
report_path=BASE/'reconciled-original-25155-retained.json'; report=load(report_path)
check(sha(original_path.read_bytes())==report['original_report_sha256'],'original report mismatch')
journal=pathlib.Path(original['custody']['journal']['backup']);check(sha(journal.read_bytes())==report['journal_sha256'],'journal mismatch')
responses=load(original['custody']['dispatched_id_response_evidence']);check(collections.Counter(responses.values())=={202:25155,429:45},'responses mismatch')
with sqlite3.connect('file:'+str(journal)+'?mode=ro',uri=True) as db:
 admitted={i:json.loads(v) for i,v in db.execute('select id,payload from admissions')}
 attempts={i:json.loads(v) for i,v in db.execute('select id,payload from attempts')}
 proofs={i:json.loads(v) for i,v in db.execute('select id,evidence from terminal_evidence')}
check(set(admitted)==set(attempts)==set(proofs)=={i for i,v in responses.items() if v==202},'accepted custody set mismatch')
payer=original['initial']['solana']['payer'];recipient=original['initial']['solana']['recipient']
evidence=pathlib.Path(report['observations']);check(sha(evidence.read_bytes())==report['observations_sha256'],'observations hash mismatch')
seen=set(); signatures=set(); fees=effects=0
with gzip.open(evidence,'rt') as f:
 for line in f:
  row=json.loads(line);txid=row['id'];check(txid in admitted and txid not in seen,'unexpected duplicate ID');seen.add(txid)
  check(row['admitted'] and row['kind']=='solana' and row['state']=='terminal','wrong durable state')
  check(len(row['attempts'])==len(row['executions'])==1,'multiple attempt/execution')
  a=row['attempts'][0];e=row['executions'][0];proof=proofs[txid];saved=attempts[txid]
  check(a['identity']==e['identity']==row['terminal']['identity']==saved['signature']==proof['signature'],'identity mismatch')
  check(a['identity'] not in signatures,'duplicate signature');signatures.add(a['identity'])
  check(a['wire_digest']==sha(base64.b64decode(saved['attempt']['signed_transaction'],validate=True)),'wire changed')
  check(a['replay_key']==a['wire_replay_key']==row['replay_key']=='solana:solana:local:'+a['identity'],'replay changed')
  fields={'chain':'solana:local','payer':payer,'instructions':admitted[txid]['transaction']['instructions']}
  check(a['intent_digest']==sha(json.dumps(fields,sort_keys=True,separators=(',',':')).encode()),'intent changed')
  instruction=fields['instructions']; check(len(instruction)==1 and instruction[0]['data']=='AgAAAOgDAAAAAAAA' and instruction[0]['accounts'][0]['pubkey']==payer and instruction[0]['accounts'][1]['pubkey']==recipient,'original fixture changed')
  check(e['canonical'] and e['finalized'] and e['slot']==proof['slot'] and e['outcome']==proof['outcome']==row['terminal']['outcome']=='success','proof mismatch')
  check(e['fee']==5000 and e['effects']=={'balance:'+recipient:1000},'wrong effects/fees')
  fees+=e['fee'];effects+=1000
check(seen==set(admitted),'missing per-ID evidence')
check(report['balances']=={'payer_spent':fees+effects,'receipt_fees':fees,'recipient_delta':effects},'aggregate equation mismatch')
check(report['accepted_custody_oracle']['safety_pass'] and report['accepted_custody_oracle']['liveness_pass'],'accepted oracle failed')
full=report['original_all_offer_oracle'];check(full['safety_pass'] and not full['liveness_pass'] and len(full['liveness_failures'])==45,'original offered oracle promoted')
check(not report['capacity_qualification'] and not report['read_rpc_errors'],'capacity or RPC error')
absent=[]
for f in ['clone-process.json','clone-retained-process.json']:
 pid=load(BASE/f)['pid']
 try:os.kill(pid,0)
 except ProcessLookupError:absent.append(pid)
check(len(absent)==2,'owned clone still active')
state=load('/tmp/engine-capacity-dispatch-final-state.json');check(state.get('review_required') and state.get('active'),'original global stop cleared')
audit={'reviewed_utc':datetime.now(timezone.utc).isoformat(),'result':'accepted_custody_pass','mode':'offline full-observation+original-journal comparison; no new RPC',
 'observations_verified':len(seen),'unique_signatures':len(signatures),'original_offered':25200,'known429_unadmitted':45,'unknown_http_outcomes':0,
 'raw_wire_digests_and_replay_bindings_exact':True,'terminal_signature_slot_outcome_exact':True,'per_id_finalized_effect_lamports':1000,'per_id_receipt_fee_lamports':5000,
 'recipient_delta_lamports':effects,'payer_debit_lamports':fees+effects,'receipt_fees_lamports':fees,'clone_pids_verified_absent':absent,
 'original_failed_report_unchanged':True,'original_supervisor_stop_retained':True,'capacity_qualification':False,
 'original_report_sha256':sha(original_path.read_bytes()),'reconciliation_report_sha256':sha(report_path.read_bytes()),'observations_sha256':sha(evidence.read_bytes()),
 'historical_queue_scope':report['drain_scope'],'limitation':'Offline audit validates captured original-chain RPC evidence; no fresh chain query after clone stop. Accepted custody excludes45known rejected offers and is not a capacity pass.'}
(DEST/'independent-audit.json').write_text(json.dumps(audit,indent=2,sort_keys=True)+'\n')
forbidden={'signedTransaction','signed_transaction','privateKey','private_key','signingCredential','rpcCredentials','ENGINE_PRIVATE_KEY','ENGINE_SIGNING_TOKEN','headers','payload'}
def clean(x):
 if isinstance(x,dict):
  check(not(set(x)&forbidden),'private content key in archive')
  for v in x.values():clean(v)
 elif isinstance(x,list):
  for v in x:clean(v)
files=[]
def copy(src,name,compress=False):
 b=pathlib.Path(src).read_bytes(); source_hash=sha(b)
 if name.endswith('.json'):clean(json.loads(b))
 if name.endswith('.json.gz'):clean(json.loads(b))
 if name.endswith('.jsonl.gz'):
  raw=gzip.decompress(b) if str(src).endswith('.gz') else b
  for line in raw.splitlines():clean(json.loads(line))
 if compress:b=gzip.compress(b,mtime=0)
 (DEST/name).write_bytes(b);files.append({'file':name,'bytes':len(b),'sha256':sha(b),'source_sha256':source_hash,'encoding':'gzip of exact source' if compress else 'exact source'})
copy(original_path,'original-failed-report.json.gz',True)
copy('/tmp/engine-capacity-dispatch-final-state.json','original-supervisor-stopped-state.json')
copy(report_path,'accepted-custody-report.json')
copy(evidence,'accepted-custody-observations.jsonl.gz')
for name in ['reconciled-original-25155.json','reconciled-original-25155.log','reconcile-preflight.json','reconcile-preflight.log','clone-first-health.json','clone-process.json','clone-process-exit.json','clone-retained-process.json','clone-retained-process-exit.json','clone-retained-first-check.json','retained-clone-read-diagnosis.json','retained-clone-transaction-recheck.json','inspection-original-bounds.log','inspection-used-bounds.log','inspection-original-slots.log','inspection-original-slot65-verbose.log','inspection-copy.json','working-ledger-manifest.json','working-retained-ledger-manifest.json']:
 copy(BASE/name,name)
copy(BASE/'cold-backup/manifest.json','cold-backup-manifest.json')
copy(BASE/'reconcile/frozen-manifest.json','frozen-harness-manifest.json')
copy(BASE/'reconcile/reconcile_original.py','reconcile-original.py')
copy(pathlib.Path(__file__),'independent-audit-and-archive.py')
copy(original['resource_guard']['evidence'],'resource-guard.jsonl.gz',True)
copy(pathlib.Path(original['log_directory'])/'interrupted-client-responses.json','original-client-responses.json.gz',True)
copy(DEST/'independent-audit.json','independent-audit.json')
manifest={'archived_utc':datetime.now(timezone.utc).isoformat(),'scope':'Sanitized failed capacity campaign and subsequent accepted-custody proof; no ledger/journal/AOF/keypairs/raw signed bytes exported.','capacity_qualification':False,'files':files}
(DEST/'MANIFEST.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps({'result':audit['result'],'observations':len(seen),'files':len(files),'bytes':sum(x['bytes'] for x in files),'archive':str(DEST)}))
