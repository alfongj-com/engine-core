from pathlib import Path
import sqlite3,json,gzip,hashlib,collections,subprocess
from datetime import datetime,timezone
CUSTODY=Path('/tmp/engine-nitro-disk-incident/private')
RUN=Path('/tmp/engine-nitro-incident-recovery')
OUT=Path('/Users/alfongj/Code/engine-core/docs/baselines/capacity-2026-09-26/nitro-disk-incident-recovery')

def check(ok, label):
    if not ok: raise AssertionError(label)

def inventory(path,immutable=False):
    with sqlite3.connect(path.as_uri()+'?mode=ro'+('&immutable=1' if immutable else ''),uri=True) as db:
        db.execute('BEGIN')
        result={'control':db.execute('SELECT deployment,epoch,namespace,checkpoint,halted FROM control').fetchone(),
            'admissions':dict((row[0],row[1:]) for row in db.execute('SELECT id,kind,fingerprint,payload,state,replay_key FROM admissions')),
            'attempts':dict((row[0],row[1:]) for row in db.execute('SELECT sequence,id,replay_key,digest,payload FROM attempts')),
            'terminal':dict((row[0],row[1:]) for row in db.execute('SELECT sequence,id,evidence FROM terminal_evidence')),
            'chain_halts':db.execute('SELECT COUNT(*) FROM chain_halts').fetchone()[0]}
        db.rollback()
    return result

before=inventory(CUSTODY/'recovery-consistent.sqlite',True)
after=inventory(RUN/'work/recovery.sqlite')
r=json.loads((RUN/'recovered-original-14400.json').read_text())
check(hashlib.sha256((RUN/'recovered-original-14400.json').read_bytes()).hexdigest()=='2d43b862b2613a34d6d95a20c09ed2734ef931c59960a8fb6069ed5ffe7b549e','report digest')
check(before['control'][:3]==after['control'][:3],'authority identity changed')
check(after['control'][4]==0 and after['chain_halts']==0,'durable halt')
check(len(before['admissions'])==len(after['admissions'])==14400,'admission count')
check(set(before['admissions'])==set(after['admissions']),'admission set')
for txid,old in before['admissions'].items():
    new=after['admissions'][txid]
    check(old[:3]==new[:3],'immutable admission changed')
    check(old[4] is None or old[4]==new[4],'old replay identity changed')
    check(new[3]=='terminal','unresolved admission')
check(all(after['attempts'].get(k)==v for k,v in before['attempts'].items()),'original attempt row changed')
check(all(after['terminal'].get(k)==v for k,v in before['terminal'].items()),'original terminal proof changed')
new_attempts=[v for k,v in after['attempts'].items() if k not in before['attempts']]
check(len(new_attempts)==len({v[0] for v in new_attempts})==9596,'new first signatures')
check(all(before['admissions'][v[0]][4] is None for v in new_attempts),'new signature for old signed intent')
by_id=collections.defaultdict(list)
for row in after['attempts'].values():by_id[row[0]].append(row)
check(len(after['attempts'])==len(by_id)==14400 and all(len(v)==1 for v in by_id.values()),'attempt uniqueness')
terminal_by_id=collections.defaultdict(list)
for txid,evidence in after['terminal'].values():terminal_by_id[txid].append(json.loads(evidence))
check(len(after['terminal'])==len(terminal_by_id)==14400 and all(len(v)==1 for v in terminal_by_id.values()),'terminal uniqueness')
seen=set();hashes=set();nonces=set();fees=0;effects=0
allowed_top={'id','admitted','attempts','executions','kind','replay_key','state','terminal'}
allowed_attempt={'identity','intent_digest','replay_key','wire_digest','wire_replay_key'}
allowed_execution={'block_hash','block_number','canonical','effects','fee','fee_components','finalized','identity','nonce','outcome'}
with gzip.open(r['observation_evidence'],'rt') as stream:
    for line in stream:
        row=json.loads(line);txid=row['id']
        check(set(row)==allowed_top and txid not in seen and txid in before['admissions'],'unexpected/duplicate/private observation')
        seen.add(txid);check(row['admitted'] and row['state']=='terminal' and row['kind']=='eoa','observation state')
        check(len(row['attempts'])==len(row['executions'])==1,'multiple attempt/effect observation')
        attempt=row['attempts'][0];execution=row['executions'][0];journal=by_id[txid][0];wire=json.loads(journal[3]);proof=terminal_by_id[txid][0]
        check(set(attempt)==allowed_attempt and set(execution)==allowed_execution,'unexpected/private nested observation')
        identity=wire['transactionHash'];check(identity not in hashes,'duplicate transaction identity');hashes.add(identity)
        check(attempt['identity']==execution['identity']==row['terminal']['identity']==proof['transactionHash']==identity,'per-ID identity mismatch')
        check(attempt['wire_digest']==hashlib.sha256(bytes.fromhex(wire['signedTransaction'][2:])).hexdigest(),'wire digest mismatch')
        check(row['replay_key']==journal[1]==attempt['replay_key']==attempt['wire_replay_key']==after['admissions'][txid][4],'nonce binding mismatch')
        nonce=int(journal[1].rsplit(':',1)[1]);check(nonce==execution['nonce'] and nonce not in nonces,'duplicate/wrong nonce');nonces.add(nonce)
        intent=json.loads(before['admissions'][txid][2])
        value=int(intent['value'],16) if isinstance(intent['value'],str) and intent['value'].startswith('0x') else int(intent['value'])
        expected_fields={'chain':intent['chainId'],'sender':intent['from'].lower(),'to':intent['to'].lower(),'value':value,'data':intent['data'].lower()}
        expected_digest=hashlib.sha256(json.dumps(expected_fields,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        check(attempt['intent_digest']==expected_digest,'decoded observed intent differs from immutable request')
        check(execution['canonical'] is True and execution['finalized'] is True,'noncanonical/unqualified effect')
        check(execution['outcome']==row['terminal']['outcome']==proof['outcome']=='success','wrong outcome')
        check(proof['finality']['blockHash']==execution['block_hash'] and proof['finality']['blockNumber']==execution['block_number'],'terminal proof not observed block')
        check(execution['effects']=={'balance:'+intent['to'].lower():value} and value==1,'missing/wrong/duplicate transfer effect')
        check(type(execution['fee']) is int and execution['fee']>0,'invalid fee');fees+=execution['fee'];effects+=value
check(seen==set(before['admissions']),'missing per-ID observation')
initial=r['initial']['nitro']['sender_nonce'];check(nonces==set(range(initial,initial+14400)),'nonce gaps')
balance=r['balances']['nitro'];check(balance['nonce_delta']==balance['canonical_executions']==14400,'aggregate nonce/effect count')
check(balance['recipient_delta']==effects==14400 and balance['receipt_fees']==fees and balance['sender_spent']==effects+fees,'balance/fee mismatch')
check(all(v==0 for v in r['drain'].values()) and len(r['drain'])==9,'nonzero/omitted drain')
check(not r['errors'] and not r['wire_guard_rejections'] and r['outcome']=='pass','reported failures')
proxy=r['recovery_proxy'];check(proxy['forwarded_sends']==proxy['accepted_unique_wires']==9596,'unexpected resend/new broadcast')
check(not proxy['proxy_failures'] and not proxy['proxy_overloads'] and not any(v['errors'] for v in proxy['methods'].values()),'RPC errors')
pids=[e['pid'] for e in r['events'] if e['event']=='spawn'];ps=subprocess.run(['ps','-p',','.join(map(str,pids)),'-o','pid=,comm='],capture_output=True,text=True)
check(ps.stdout.strip()=='','owned PID still present')
original_state=json.loads((CUSTODY/'engine-capacity-bounded-state.json').read_text());live_state=json.loads(Path('/tmp/engine-capacity-bounded-state.json').read_text())
check(bool(live_state.get('review_required')) and live_state.get('active')==original_state.get('active'),'original supervisor latch changed')
check(hashlib.sha256(Path('/tmp/capacity-bounded-nitro40-017a2525ad46.json').read_bytes()).digest()==hashlib.sha256((CUSTODY/'capacity-bounded-nitro40-017a2525ad46.json').read_bytes()).digest(),'original failed report changed')
summary={'reviewed_utc':datetime.now(timezone.utc).isoformat(),'review_mode':'offline SQLite+complete observation-file audit; no new node/RPC calls',
 'result':'pass','original_intents':len(seen),'old_attempt_rows_preserved':len(before['attempts']),'old_terminal_proofs_preserved':len(before['terminal']),
 'new_first_signatures_for_original_unsigned_ids':len(new_attempts),'new_ids':0,'replacement_identities':0,'distinct_canonical_finalized_successes':len(hashes),
 'nonce_range':{'minimum':min(nonces),'maximum':max(nonces),'count':len(nonces),'gapless':True},'recipient_effect_wei':effects,'receipt_fees_wei':fees,'sender_debit_wei':effects+fees,
 'exact_per_id_checks':['original ID set','immutable admission kind/fingerprint/payload','old replay identity','all old raw signed-attempt rows','all old exact terminal proofs','single new signature only for original unsigned ID','one attempt and canonical execution per ID','observation wire SHA256 vs actual saved bytes','independently observed intent digest vs original request','terminal hash/outcome/block vs observed execution','unique contiguous nonce set','one-wei effect per ID','aggregate nonce/balance/fee equation'],
 'owned_pids_verified_absent':pids,'all_nine_drain_counters_zero':True,'original_failed_report_unchanged':True,'original_supervisor_review_latch_retained':True,
 'recovery_drain_seconds':r['resume_drain_seconds'],'engine_binary_sha256':r['engine_binary_sha256'],'harness_sha256':r['harness_sha256']['capacity_campaign.py'],
 'report_sha256':hashlib.sha256((RUN/'recovered-original-14400.json').read_bytes()).hexdigest(),'observations_sha256':hashlib.sha256(Path(r['observation_evidence']).read_bytes()).hexdigest(),
 'capacity_qualification':False,'generic_oracle_eligible_for_rate_assessment':r['oracle']['eligible_for_rate_assessment'],
 'qualification_limit':'The generic eligibility flag says only that ledger effects reconciled. This was an incident drain with no new offered workload or steady-state windows; top-level capacity=false. Local depth2 Native Nitro has no rollup L1 settlement qualification.',
 'residual_assumptions':['honest restored local RPC and original-chain continuity from stored proof anchors','offline review validates captured observations, not fresh post-pause chain state','signed bytes/credentials remain in private custody; archived observations contain hashes/digests only']}
(OUT/'independent-audit.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({'result':'pass','per_id_rows':len(seen),'old_attempts_preserved':len(before['attempts']),'old_proofs_preserved':len(before['terminal']),'new_first_signatures':len(new_attempts),'nonce_min':min(nonces),'nonce_max':max(nonces),'pids_absent':pids,'latch_retained':True}))
