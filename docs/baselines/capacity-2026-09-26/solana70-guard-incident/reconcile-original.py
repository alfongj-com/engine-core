#!/usr/bin/env python3
"""Original Sol70 accepted-intent custody review. No sends, services, funding or recovery writes.
Default validates the frozen files/journal offline. --execute permits ONLY loopback read RPC.
The original25200-offer campaign remains a failed infrastructure/capacity result.
"""
import argparse, base64, concurrent.futures, gzip, hashlib, json, os
from pathlib import Path
import sqlite3, sys, threading, time, urllib.parse
from collections import Counter
from datetime import datetime, timezone
sys.dont_write_bytecode = True
BASE = Path(__file__).resolve().parent
SOURCE = Path('/tmp/capacity-dispatch-final-solana70-5057cdb7bab1.json')
SOURCE_HASH = '0113bb8da0a4d64515cd5fad6b8e5ca2dabc4f39a36ce597a9122f5b7c2f8488'

def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
    return h.hexdigest()

def need(value, category):
    if not value: raise RuntimeError(category)

def b58(raw):
    alphabet='123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'; n=int.from_bytes(raw,'big'); result=''
    while n: n,r=divmod(n,58); result=alphabet[r]+result
    return '1'*(len(raw)-len(raw.lstrip(b'\x00')))+result

def save(path, value):
    path=Path(path); path.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f: json.dump(value,f,indent=2,sort_keys=True); f.write('\n')

def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--execute',action='store_true'); ap.add_argument('--rpc',default='http://127.0.0.1:55190')
    ap.add_argument('--report',type=Path,required=True); ap.add_argument('--concurrency',type=int,default=8)
    args=ap.parse_args(); need(1<=args.concurrency<=16,'concurrency_bound'); need(not args.report.exists(),'report_exists')
    parsed=urllib.parse.urlsplit(args.rpc)
    need(parsed.scheme=='http' and parsed.hostname=='127.0.0.1' and parsed.port and parsed.path in ('','/') and not parsed.query and not parsed.fragment and not parsed.username and not parsed.password,'loopback_read_endpoint_required')
    need(sha(SOURCE)==SOURCE_HASH,'original_report_changed')
    manifest=json.loads((BASE/'frozen-manifest.json').read_text())
    for name,h in manifest.items(): need(sha(BASE/'frozen/scripts'/name)==h,'frozen_import_changed')
    sys.path.insert(0,str(BASE/'frozen/scripts'))
    import capacity_campaign as cc
    import capacity_faults as cf
    original=json.loads(SOURCE.read_text()); logs=Path(original['log_directory']); journal=Path(original['custody']['journal']['backup'])
    need(sha(journal)==original['custody']['journal']['sha256'],'custody_journal_changed')
    need(all(manifest[k]==v for k,v in original['harness_sha256'].items()),'original_harness_mismatch')
    statuses=json.loads(Path(original['custody']['dispatched_id_response_evidence']).read_text())
    need(len(statuses)==25200 and Counter(statuses.values())=={202:25155,429:45},'response_inventory_mismatch')
    prefixes={s.rsplit('-solana-',1)[0] for s in statuses}; need(len(prefixes)==1,'mixed_namespaces'); namespace=next(iter(prefixes))
    expected_ids={f'{namespace}-solana-{n}' for n in range(25200)}; need(set(statuses)==expected_ids,'offered_id_set_mismatch')
    admitted={s for s,v in statuses.items() if v==202}; rejected=set(statuses)-admitted
    initial=original['initial']['solana']; expected={}
    for txid in sorted(expected_ids): expected[txid]=cf.solana_fixture(txid,initial['payer'],initial['recipient'],'transfer')[1]
    c=cc.Campaign.__new__(cc.Campaign); c.journal=journal; c.report={}; c.recipient=initial['recipient']; c.expected=expected; c.profiles=original['profiles']; c.id_chain={i:'solana' for i in expected_ids}
    observations=c.read_journal(); need(set(observations)==admitted,'durable_admission_set_mismatch')
    need(not c.report['journal_halted'] and not c.report['durable_chain_halts'],'durable_halt')
    proofs={}; wire_seen=set(); wire_digests={}; slots=[]
    with sqlite3.connect('file:'+str(journal)+'?mode=ro',uri=True) as db:
        db.execute('BEGIN')
        for txid,raw in db.execute('SELECT id,payload FROM admissions'):
            p=json.loads(raw); need(p['transactionId']==txid,'admission_id_mismatch')
            t=p['transaction']; e=t['executionOptions']
            need(t['instructions']==expected[txid]['intent_fields']['instructions'] and e['chainId']=='solana:local' and e['commitment']=='finalized' and e['signerAddress']==initial['payer'],'admission_intent_mismatch')
        for txid,raw in db.execute('SELECT id,evidence FROM terminal_evidence'):
            need(txid not in proofs,'multiple_terminal_proofs'); proofs[txid]=json.loads(raw)
        db.rollback()
    need(set(proofs)==admitted,'terminal_proof_set_mismatch')
    for txid,row in observations.items():
        need(row['kind']=='solana' and row['state']=='terminal' and len(row['attempts'])==1,'nonterminal_or_multiple_attempts')
        a=row['attempts'][0]; p=a['payload']; signature=p['signature']; wire=p['attempt']['signed_transaction']; decoded=cf.decode_solana_wire(wire,txid)
        need(p['chainId']=='solana:local' and decoded['signature']==signature and decoded['intent_digest']==expected[txid]['intent_digest'],'signed_intent_mismatch')
        need(a['replay_key']==row['replay_key']=='solana:solana:local:'+signature,'replay_binding_mismatch')
        need(signature not in wire_seen,'duplicate_signature'); wire_seen.add(signature); wire_digests[decoded['wire_digest']]=signature
        proof=proofs[txid]; need(proof['chainId']=='solana:local' and proof['commitment']=='finalized' and proof['outcome']=='success' and proof['signature']==signature,'saved_terminal_mismatch'); slots.append(proof['slot'])
    accepted=original['rpc']['solana']['accepted_wires']
    need(set(accepted)==set(wire_digests) and all(v['identity']==wire_digests[k] and v['accepted_responses']==1 for k,v in accepted.items()),'accepted_wire_inventory_mismatch')
    last=original['samples'][-1]['chains']['solana']; need(all(last[k]==25155 for k in ('admitted','attempted','included','finalized','terminal')) and last['observer_pending_signatures']==0,'original_final_sample_incomplete')
    q=original['custody']['redis_queue_counters']; need(len(q)==6 and all(v==0 for v in q.values()),'recorded_queue_not_drained')
    need(any(v['event']=='drain_target_reached' and v.get('parked')==0 for v in original['events']),'missing_original_drain_event')
    # Recorded at original stop; no Redis is started and no current Redis claim is made.
    historical_drain={k:0 for k in cf.DRAIN_FIELDS}
    genesis=logs/'solana-ledger/genesis.bin'; genesis_hash=b58(bytes.fromhex(sha(genesis)))
    result={'schema':'solana-original-custody-v1','reviewed_utc':datetime.now(timezone.utc).isoformat(),'mode':'read_only_chain_reconciliation' if args.execute else 'offline_preflight',
        'original_report_sha256':SOURCE_HASH,'frozen_harness_sha256':manifest,'journal_sha256':sha(journal),'runner_sha256':sha(__file__),
        'original_offered':25200,'accepted':25155,'known_429_unadmitted':45,'transport_unknown':0,'signed_attempts':25155,'terminal_proofs':25155,
        'saved_slot_range':[min(slots),max(slots)],'expected_genesis_hash':genesis_hash,'capacity_qualification':False,'no_engine_redis_funding_or_send':True,
        'historical_drain':historical_drain,'drain_scope':'Original all-response inventory, drain_target_reached event, six recorded zero queue indexes and final25155/25155 finalized sample. EOA borrowed/submitted indexes not applicable. No new Redis/node-pool measurement.',
        'qualification_limit':'A successful accepted-intent custody review cannot promote the original25200-offer campaign:45known429, infrastructure interruption, unsigned backlog trend and no new offered window remain unchanged.'}
    if not args.execute:
        result['outcome']='offline_preflight_pass'; save(args.report,result); print(json.dumps({'outcome':result['outcome'],'report':str(args.report)})); return
    # Defense in depth: frozen adapters receive only a method-allowlisted read client.
    class Reads(cc.Rpc):
        ALLOWED={'getGenesisHash','getVersion','getSlot','getTransaction','getSignatureStatuses','getBalance'}
        def call(self,method,params):
            need(method in self.ALLOWED,'non_read_rpc_forbidden'); return super().call(method,params)
    node=Reads(args.rpc); c.nodes={'solana':node}; began=time.monotonic()
    try:
        need(node.call('getGenesisHash',[])==genesis_hash,'wrong_genesis')
        need(node.call('getVersion',[])['solana-core']==initial['version']['solana-core'],'validator_version_mismatch')
        result['finalized_slot_before']=node.call('getSlot',[{'commitment':'finalized'}]); need(result['finalized_slot_before']>=max(slots),'clone_has_not_replayed_original_tip')
        def one(txid):
            actual=observations[txid]; c.reconcile_solana('solana',txid,actual)
            need(len(actual['executions'])==1,'missing_or_multiple_finalized_receipts')
            e=actual['executions'][0]; proof=proofs[txid]
            need(e['canonical'] and e['finalized'] and e['slot']==proof['slot'] and e['identity']==proof['signature'] and e['outcome']=='success','receipt_terminal_proof_mismatch')
            need(e['fee']==5000,'unexpected_solana_fee')
        ordered=sorted(admitted)
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            for offset in range(0,len(ordered),256): list(pool.map(one,ordered[offset:offset+256]))
        full=cf.evaluate_campaign(list(expected.values()),observations,historical_drain)
        accepted_oracle=cf.evaluate_campaign([expected[i] for i in ordered],observations,historical_drain)
        need(full['safety_pass'] and not full['liveness_pass'] and len(full['liveness_failures'])==45 and all(s.rsplit(': offered intent not admitted',1)[0] in rejected for s in full['liveness_failures']),'full_offer_oracle_mismatch')
        need(accepted_oracle['safety_pass'] and accepted_oracle['liveness_pass'],'accepted_custody_oracle_failed')
        payer=node.call('getBalance',[initial['payer'],{'commitment':'finalized'}])['value']; recipient=node.call('getBalance',[initial['recipient'],{'commitment':'finalized'}])['value']
        fees=sum(r['executions'][0]['fee'] for r in observations.values()); effects=sum(r['executions'][0]['effects'].get('balance:'+initial['recipient'],0) for r in observations.values())
        need(effects==25155*1000 and recipient-initial['recipient_balance']==effects and initial['payer_balance']-payer==effects+fees,'balance_fee_conservation_failed')
        need(sha(SOURCE)==SOURCE_HASH and sha(journal)==result['journal_sha256'],'source_mutated_during_review')
        evidence=args.report.with_suffix('.observations.jsonl.gz'); fd=os.open(evidence,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'wb') as raw:
            with gzip.GzipFile(fileobj=raw,mode='wb',mtime=0) as z:
                for txid in ordered: z.write((json.dumps({'id':txid,**observations[txid]},sort_keys=True)+'\n').encode())
        result.update({'outcome':'accepted_custody_pass','original_all_offer_oracle':full,'accepted_custody_oracle':accepted_oracle,'observations':str(evidence),'observations_sha256':sha(evidence),
            'balances':{'recipient_delta':effects,'payer_spent':initial['payer_balance']-payer,'receipt_fees':fees},'reconciliation_seconds':time.monotonic()-began})
    except BaseException as error:
        result.update({'outcome':'unproven_or_failed','error_type':type(error).__name__,'elapsed_seconds':time.monotonic()-began})
        save(args.report,result); raise
    finally:
        cc.LOCAL_HTTP.close(); result['read_rpc_calls']=dict(node.calls); result['read_rpc_errors']=dict(node.errors)
    save(args.report,result); print(json.dumps({'outcome':result['outcome'],'accepted':25155,'known_rejected':45,'capacity_qualification':False,'report':str(args.report)}))

if __name__=='__main__': main()
