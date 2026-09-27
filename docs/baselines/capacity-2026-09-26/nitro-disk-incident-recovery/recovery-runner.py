#!/usr/bin/env python3
"""Incident-only original-intent continuation; defaults to PREPARE, never load."""
import argparse
from collections import Counter
import concurrent.futures
import hashlib
import importlib
import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
FROZEN = HERE / 'frozen'
CUSTODY = Path('/tmp/engine-nitro-disk-incident/private')
ORIGINAL_REPORT = CUSTODY / 'capacity-bounded-nitro40-017a2525ad46.json'
ENGINE_SHA = '300db9782868ddca5b6d552e7ead5d2621322a82c8297ffde8f6ecb067858b23'
HARNESS_SHA = '80d1e4c03cf456a29f318e5cc581cb6788e99e3f8d26a98bde1490105d08e763'
CHAIN = 412346
COUNT = 14400

class PreparationComplete(Exception):
    pass


def need(condition, message):
    if not condition: raise RuntimeError(message)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def import_frozen():
    metadata = json.loads((FROZEN / 'MANIFEST.json').read_text())
    for name, digest in metadata['files'].items():
        need(sha(FROZEN / name) == digest, 'Frozen recovery dependency changed')
    need(metadata['files']['thirdweb-engine'] == ENGINE_SHA, 'Wrong frozen Engine')
    need(metadata['files']['scripts/capacity_campaign.py'] == HARNESS_SHA, 'Wrong frozen harness')
    sys.path.insert(0, str(FROZEN / 'scripts'))
    c = importlib.import_module('capacity_campaign')
    faults = importlib.import_module('capacity_faults')
    need(Path(c.__file__).resolve() == FROZEN / 'scripts/capacity_campaign.py', 'Harness import escaped freeze')
    need(Path(faults.__file__).resolve() == FROZEN / 'scripts/capacity_faults.py', 'Proxy import escaped freeze')
    # Only metadata collection uses Git in Campaign.__init__. The frozen tree
    # intentionally has no mutable Git checkout; report its recorded provenance.
    class FrozenProcessMetadata:
        def __getattr__(self, name): return getattr(subprocess, name)
        def check_output(self, command, **kwargs):
            if command == ['git', 'rev-parse', 'HEAD']: return metadata['source_commit'] + '\n'
            if command == ['git', 'status', '--porcelain']: return '\n'.join(metadata['source_status_porcelain'])
            return subprocess.check_output(command, **kwargs)
    c.subprocess = FrozenProcessMetadata()
    return c, faults, metadata


def inventory(path, immutable=False):
    with sqlite3.connect(path.as_uri() + '?mode=ro' + ('&immutable=1' if immutable else ''), uri=True, timeout=5) as db:
        db.execute('BEGIN')
        control = dict(zip(('deployment','epoch','namespace','checkpoint','halted'), db.execute('SELECT deployment,epoch,namespace,checkpoint,halted FROM control').fetchone()))
        admissions = {row[0]: dict(zip(('kind','fingerprint','payload','state','replay_key'), row[1:])) for row in db.execute('SELECT id,kind,fingerprint,payload,state,replay_key FROM admissions')}
        attempts = {row[0]: dict(zip(('id','replay_key','digest','payload'), row[1:])) for row in db.execute('SELECT sequence,id,replay_key,digest,payload FROM attempts ORDER BY sequence')}
        terminal = dict(db.execute('SELECT sequence,evidence FROM terminal_evidence ORDER BY sequence'))
        checkpoints = dict(db.execute('SELECT chain_id,evidence FROM chain_checkpoints'))
        halted_chains = [row[0] for row in db.execute('SELECT chain_id FROM chain_halts')]
        db.rollback()
    return dict(control=control, admissions=admissions, attempts=attempts, terminal=terminal, checkpoints=checkpoints, halted_chains=halted_chains)


def verify_original(before, sender):
    need(len(before['admissions']) == COUNT and len(before['attempts']) == 4804 and len(before['terminal']) == 4502, 'Original custody counts differ')
    need(not before['control']['halted'] and not before['halted_chains'], 'Original authority halted')
    namespace = before['control']['namespace']
    need(set(before['admissions']) == {f'{namespace}-nitro-{i}' for i in range(COUNT)}, 'Original ID inventory differs')
    for txid, old in before['admissions'].items():
        value = json.loads(old['payload'])
        need(old['kind'] == 'eoa' and old['state'] in ('admitted','terminal'), 'Unexpected original executor/state')
        need(value['transactionId'] == txid and value['chainId'] == CHAIN and value['from'].lower() == sender.lower(), 'Original signing identity differs')
        need(value['signingCredential'] == {'Environment': {'address': sender.lower()}}, 'Original signer is not frozen environment authority')
    return namespace


def clone_files(source, destination):
    """Copy bytes only: immutable forensic flags must not propagate to work."""
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    for item in source.iterdir():
        need(not item.is_symlink(), 'Unexpected custody symlink')
        target = destination / item.name
        if item.is_dir(): clone_files(item, target)
        else: shutil.copyfile(item, target); os.chmod(target, 0o600)


def preflight(node, original, before):
    need(int(node.call('eth_chainId', []), 16) == CHAIN, 'Wrong original chain ID')
    genesis = original['initial']['nitro']['genesis']['hash']
    need(node.call('eth_getBlockByNumber', ['0x0', False])['hash'] == genesis, 'Original native genesis changed')
    checkpoint = json.loads(before['checkpoints'][str(CHAIN)])
    def check_checkpoint():
        block = node.call('eth_getBlockByNumber', [hex(checkpoint['checkpointNumber']), False])
        need(block is not None and block['hash'] == checkpoint['checkpointHash'], 'Original durable checkpoint unavailable or changed')
    check_checkpoint()
    proofs = [json.loads(value) for value in before['terminal'].values()]
    blocks = {}
    for proof in proofs:
        finality = proof['finality']; height = finality['blockNumber']
        if height not in blocks: blocks[height] = node.call('eth_getBlockByNumber', [hex(height), False])
        block = blocks[height]
        need(block is not None and block['hash'] == finality['blockHash'] and proof['transactionHash'] in block['transactions'], 'Original terminal canonical block/membership changed')
    def receipt(proof):
        value = node.call('eth_getTransactionReceipt', [proof['transactionHash']])
        finality = proof['finality']
        need(value is not None and value.get('transactionHash') == proof['transactionHash'], 'Original terminal receipt unavailable')
        need(value['blockHash'] == finality['blockHash'] and int(value['blockNumber'],16) == finality['blockNumber'], 'Original terminal receipt moved')
        need(proof['outcome'] in ('success','reverted') and int(value['status'],16) == int(proof['outcome']=='success'), 'Original terminal receipt outcome changed')
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        for offset in range(0, len(proofs), 128): list(pool.map(receipt, proofs[offset:offset+128]))
    check_checkpoint()
    attempted_nonces = [int(row['replay_key'].rsplit(':',1)[1]) for row in before['attempts'].values()]
    terminal_nonces = [int(row['replay_key'].rsplit(':',1)[1]) for row in before['admissions'].values() if row['state']=='terminal']
    from local_eoa_recovery import FROM
    latest = int(node.call('eth_getTransactionCount',[FROM,'latest']),16)
    pending = int(node.call('eth_getTransactionCount',[FROM,'pending']),16)
    need(max(terminal_nonces)+1 <= latest <= pending <= max(attempted_nonces)+1, 'Original signer nonce outside custody range')
    return {'terminal_receipts_verified':len(proofs),'canonical_blocks_verified':len(blocks),'checkpoint_number':checkpoint['checkpointNumber'],'latest_nonce':latest,'pending_nonce':pending,'genesis_matches':True,'checkpoint_matches':True,'scope':'read-only local node; prior terminal receipts revalidated, unresolved attempts still reconciled independently after ordinary worker resume'}


def validate_preserved(before, after):
    for key in ('deployment','epoch','namespace'):
        need(after['control'][key] == before['control'][key], 'Recovery changed authority identity')
    need(set(after['admissions']) == set(before['admissions']), 'Recovery created or lost original IDs')
    for txid, old in before['admissions'].items():
        new = after['admissions'][txid]
        need(all(new[k] == old[k] for k in ('kind','fingerprint','payload')), 'Original intent changed')
        need(old['replay_key'] is None or old['replay_key'] == new['replay_key'], 'Original replay identity changed')
        need(old['state'] != 'terminal' or new['state'] == 'terminal', 'Original terminal intent was reactivated')
    need(all(after['attempts'].get(k) == v for k,v in before['attempts'].items()), 'Original signed attempt changed')
    need(all(after['terminal'].get(k) == v for k,v in before['terminal'].items()), 'Original terminal proof changed')
    new_ids = [row['id'] for sequence,row in after['attempts'].items() if sequence not in before['attempts']]
    need(len(new_ids) == len(set(new_ids)), 'Recovery created multiple new wires for an original ID')
    need(all(before['admissions'][txid]['replay_key'] is None and before['admissions'][txid]['state']=='admitted' for txid in new_ids), 'Recovery replaced an original signed identity')
    return {'immutable_original_inventory_preserved':True,'new_first_signatures':len(new_ids),'current_terminal':sum(x['state']=='terminal' for x in after['admissions'].values()),'current_halted':bool(after['control']['halted'] or after['halted_chains'])}


def proxy_type(faults):
    class OriginalIntentProxy(faults.RpcFaultProxy):
        """No fresh IDs/replacements; live immutable request must match custody."""
        def __init__(self,url,journal,before,**kwargs):
            self.journal,self.before=journal,before
            self.guard_lock=threading.Lock();self.sequence=0;self.allowed={};self.id_wires={};self.blocked=0
            self.refresh()
            super().__init__(url,**kwargs)
        def refresh(self):
            with sqlite3.connect(self.journal.as_uri()+'?mode=ro',uri=True,timeout=5) as db:
                db.execute('BEGIN')
                rows=list(db.execute('SELECT sequence,id,replay_key,digest,payload FROM attempts WHERE sequence>? ORDER BY sequence',(self.sequence,)))
                for sequence,txid,replay,digest,encoded in rows:
                    old=self.before['admissions'].get(txid);need(old is not None,'Recovery refuses new intent')
                    live=db.execute('SELECT kind,fingerprint,payload,replay_key FROM admissions WHERE id=?',(txid,)).fetchone()
                    need(live is not None and live[:3] == (old['kind'],old['fingerprint'],old['payload']) and live[3]==replay,'Recovery live admission changed')
                    payload=json.loads(encoded);wire=bytes.fromhex(payload['signedTransaction'].removeprefix('0x'))
                    wire_digest=hashlib.sha256(wire).hexdigest();existing=self.id_wires.get(txid)
                    original=self.before['attempts'].get(sequence)
                    if original is not None:
                        need(original=={'id':txid,'replay_key':replay,'digest':digest,'payload':encoded},'Recovery old attempt changed')
                    else:
                        need(old['replay_key'] is None and old['state']=='admitted' and existing is None,'Recovery refuses replacement wire')
                    need(existing is None or existing==wire_digest,'Recovery ID has another wire')
                    need(wire_digest not in self.allowed or self.allowed[wire_digest]==txid,'Recovery wire belongs to another ID')
                    self.allowed[wire_digest]=txid;self.id_wires[txid]=wire_digest;self.sequence=sequence
                db.rollback()
        def _forward(self,call):
            if isinstance(call,dict) and call.get('method')=='eth_sendRawTransaction':
                with self.guard_lock:
                    try:
                        wire=self._wire(call)
                        if wire not in self.allowed:self.refresh()
                        need(wire in self.allowed,'Wire has no original durable intent')
                        need(self.before['admissions'][self.allowed[wire]]['state']!='terminal','Recovery cannot resend an original terminal intent')
                    except Exception:
                        self.blocked+=1;raise
            return super()._forward(call)
    return OriginalIntentProxy


def parse():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute',action='store_true',help='Start ordinary Engine workers after reviewed preflight; otherwise prepare/reattach only')
    p.add_argument('--report',type=Path,required=True)
    p.add_argument('--work-dir',type=Path,default=HERE/'work')
    p.add_argument('--drain-seconds',type=int,default=1800)
    p.add_argument('--advance-native-head',action='store_true',help='Use the pinned bounded separate development-account ticker; never creates Engine intent IDs')
    a=p.parse_args();need(1<=a.drain_seconds<=1800,'Drain outside bound');need(not a.report.exists(),'Refusing to overwrite report')
    need(not a.advance_native_head or a.execute,'Preparation cannot broadcast ticker transactions')
    return a


def main():
    os.umask(0o077);opts=parse();c,faults,metadata=import_frozen()
    original=json.loads(ORIGINAL_REPORT.read_text());before=inventory(CUSTODY/'recovery-consistent.sqlite',True)
    namespace=verify_original(before,c.FROM)
    if not opts.work_dir.exists():
        opts.work_dir.mkdir(parents=True,mode=0o700)
        clone_files(CUSTODY/'campaign/redis',opts.work_dir/'redis')
        shutil.copyfile(CUSTODY/'recovery-consistent.sqlite',opts.work_dir/'recovery.sqlite')
        os.chmod(opts.work_dir/'recovery.sqlite',0o600)
        shutil.copyfile(FROZEN/'thirdweb-engine',opts.work_dir/'thirdweb-engine');os.chmod(opts.work_dir/'thirdweb-engine',0o700)
    # Reattach deliberately requires an existing private owner-lock artifact.
    # Copy its bytes into the new directory; never copy an open file descriptor.
    if not (opts.work_dir/'recovery.sqlite.lock').exists():
        shutil.copyfile(CUSTODY/'campaign/recovery.sqlite.lock',opts.work_dir/'recovery.sqlite.lock')
        os.chmod(opts.work_dir/'recovery.sqlite.lock',0o600)
    need(sha(opts.work_dir/'thirdweb-engine')==ENGINE_SHA,'Mutable recovery binary changed')
    # SQLite's standalone WAL-mode backup needs writable sidecar setup before
    # frozen read-only observers can open it. Only this mutable clone is opened
    # RW, for a read-only quick_check; retain it until observer cleanup.
    working_db=sqlite3.connect(opts.work_dir/'recovery.sqlite',timeout=5)
    need(working_db.execute('PRAGMA quick_check').fetchone()[0]=='ok','Mutable clone integrity check failed')
    validate_preserved(before,inventory(opts.work_dir/'recovery.sqlite'))
    args,profiles=c.arguments(['--chain','nitro=40','--external-evm','nitro=http://127.0.0.1:18547','--chain-id','nitro=412346','--depth','nitro=2','--seconds','360','--drain-seconds',str(opts.drain_seconds),'--engine-bin',str(opts.work_dir/'thirdweb-engine'),'--redis-bin','/tmp/redis-7.4.2/src/redis-server','--cast-bin','/tmp/engine-capacity-tools/cast','--redis-fsync','everysec','--max-inflight',str(original['eoa_max_inflight_per_wallet']),'--http-concurrency',str(original['http_concurrency']),'--proxy-concurrency',str(original['rpc_proxy_concurrency']),'--eoa-broadcast-concurrency',str(original['eoa_broadcast_concurrency']),'--solana-workers',str(original['solana_workers']),'--solana-confirmation-poll-seconds',str(original['solana_confirmation_poll_seconds']),'--report',str(opts.report)])
    campaign=c.Campaign(args,profiles);campaign.pool.close();campaign.namespace=campaign.projection_namespace=namespace
    campaign.journal=opts.work_dir/'recovery.sqlite';campaign.initial=original['initial'];campaign.phase='recovery_preflight'
    campaign.report.update(scope='Original Nitro disk-incident queue recovery; no offered workload/capacity claim',initial=campaign.initial,no_new_admissions=True,new_intent_ids_allowed=False,manual_wire_replay=False,resume_script_sha256=sha(__file__),frozen_manifest_sha256=sha(FROZEN/'MANIFEST.json'),private_work_directory=str(opts.work_dir),original_report=str(ORIGINAL_REPORT),duration_seconds=None,warmup_seconds=None,minimum_late_window_seconds=None,arrival_phase_seconds=None,offered_workload=False,original_intent_count=COUNT,per_chain={'nitro':{'offered':0,'original_intent_count':COUNT,'late_window':{'capacity_candidate':False,'reason':'incident recovery only'}}})
    for i in range(COUNT):
        txid=f'{namespace}-nitro-{i}';campaign.id_chain[txid]='nitro';_,campaign.expected[txid]=campaign.fixture('nitro',i)
    campaign.nodes['nitro']=c.Rpc(profiles['nitro']['external_url'])
    prepared=False;executed=False;reconciliation_attempted=False;proxy=None
    def interrupted(_signum,_frame):raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM,interrupted)
    try:
        campaign.report['preflight']=preflight(campaign.nodes['nitro'],original,before)
        campaign.spawn('redis',[args.redis_bin,'--bind','127.0.0.1','--port',campaign.redis_port,'--dir',opts.work_dir/'redis','--save','','--appendonly','yes','--appendfsync','everysec'])
        c.wait_until(lambda:c.redis_command(campaign.redis_port,'PING')=='PONG')
        proxy=proxy_type(faults)(profiles['nitro']['external_url'],campaign.journal,before,plan=faults.FaultPlan(max_inflight=256,max_unique_wires=COUNT),audit_path=campaign.logs/'recovery-rpc.jsonl')
        campaign.proxies['nitro']=proxy
        campaign.env={k:v for k,v in os.environ.items() if not k.startswith(('APP__','ENGINE_'))}
        campaign.env.update(original['queue_settings'])
        campaign.env.update({'APP_ENVIRONMENT':'production','RUST_LOG':'warn','ENGINE_PRIVATE_KEY':f'{1:064x}','ENGINE_SIGNING_TOKEN':campaign.token,'APP__REDIS__URL':f'redis://127.0.0.1:{campaign.redis_port}/','APP__RECOVERY__JOURNAL_PATH':str(campaign.journal),'APP__SERVER__HOST':'127.0.0.1','APP__SERVER__PORT':str(campaign.engine_port),'APP__EVM_RPC__ENDPOINTS__412346__URL':proxy.url,'APP__EVM_RPC__ENDPOINTS__412346__FINALITY__MODE':'depth','APP__EVM_RPC__ENDPOINTS__412346__FINALITY__CONFIRMATIONS':'2'})
        need(campaign.engine_command('--reattach-recovery',required=False),'Exact original marker reattach refused')
        prepared=True;campaign.report['exact_marker_reattach']=True
        if not opts.execute:
            campaign.report['outcome']='prepared_not_executed';raise PreparationComplete
        campaign.phase='original_queue_recovery';executed=True;campaign.start_engine();campaign.started=time.monotonic()
        campaign.observer=c.JournalObserver(campaign.journal,campaign.id_chain,campaign.started,{})
        if opts.advance_native_head:
            state=json.loads((CUSTODY/'engine-capacity-bounded-state.json').read_text())
            for path in ('/tmp/engine-capacity-tools/nitro-drain-hook-v2.py','/tmp/engine-capacity-tools/cast'):
                need(sha(path)==state['frozen_sha256'][path],'Ticker dependency changed')
            campaign.spawn('drain-hook-nitro',['/tmp/engine-capacity-tools/nitro-drain-hook-v2.py','--max-seconds',str(opts.drain_seconds)])
        deadline=time.monotonic()+opts.drain_seconds
        while time.monotonic()<deadline:
            with sqlite3.connect(campaign.journal.as_uri()+'?mode=ro',uri=True,timeout=5) as db:
                db.execute('BEGIN')
                global_halt=db.execute('SELECT halted FROM control WHERE singleton=1').fetchone()[0]
                chain_halt=db.execute('SELECT EXISTS(SELECT 1 FROM chain_halts)').fetchone()[0]
                db.rollback()
            if global_halt or chain_halt:
                campaign.recovery_required=bool(global_halt);campaign.finality_halted=bool(chain_halt);campaign.event('durable_fence_stop');break
            campaign.sample()
            if campaign.observer.admitted<=campaign.observer.terminal and sum(campaign.redis_drain().values())==0:break
            time.sleep(min(args.sample_seconds,max(0,deadline-time.monotonic())))
        campaign.report['resume_drain_seconds']=time.monotonic()-campaign.started
        reconciliation_attempted=True;campaign.reconcile()
        campaign.report.update(validate_preserved(before,inventory(campaign.journal)))
        need(proxy.blocked==0,'Recovery guard rejected a forbidden identity')
    except PreparationComplete:
        pass
    except BaseException as error:
        campaign.errors.append({'phase':campaign.phase,'error_type':type(error).__name__})
        campaign.report.update(outcome='error',error_type=type(error).__name__)
    finally:
        # Even a live drain failure must attempt independent per-ID custody
        # reconciliation while nodes still exist; never turn a failed read to pass.
        if executed and not reconciliation_attempted:
            reconciliation_attempted=True
            try:campaign.reconcile()
            except BaseException as error:campaign.errors.append({'phase':'reconciliation','error_type':type(error).__name__})
        for child in reversed(list(campaign.children.values())):c.stop(child)
        if proxy:campaign.report['recovery_proxy']=proxy.snapshot(include_wires=True);proxy.close()
        for stream in campaign.streams:stream.close()
        c.LOCAL_HTTP.close()
        try:campaign.report.update(validate_preserved(before,inventory(campaign.journal)))
        except Exception as error:campaign.errors.append({'phase':'inventory_validation','error_type':type(error).__name__})
        campaign.report.update(samples=campaign.samples,events=campaign.events,errors=campaign.errors,owned_children_stopped=all(p.poll() is not None for p in campaign.children.values()),engine_started=executed,prepared=prepared,reconciliation_attempted=reconciliation_attempted,wire_guard_rejections=proxy.blocked if proxy else 0,all_chain_capacity_candidate=False,campaign_http=c.LOCAL_HTTP.snapshot(),node_lifecycle_mutations=False)
        if campaign.errors:campaign.report['outcome']='error'
        working_db.close()
        c.private_json(opts.report,campaign.report)
        print(json.dumps({'report':str(opts.report),'outcome':campaign.report.get('outcome'),'logs':str(campaign.logs),'work_directory':str(opts.work_dir)}),flush=True)
    return 0 if campaign.report.get('outcome') in ('prepared_not_executed','pass') else 1


if __name__=='__main__':
    raise SystemExit(main())
