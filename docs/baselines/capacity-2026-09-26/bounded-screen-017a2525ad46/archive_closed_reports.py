import datetime,gzip,hashlib,json,re
from pathlib import Path
BASE=Path('/Users/alfongj/Code/engine-core/docs/baselines/capacity-2026-09-26')
OUT=BASE/'bounded-screen-017a2525ad46';OUT.mkdir(exist_ok=True)
INC=Path('/tmp/engine-nitro-disk-incident')
def sha(b):return hashlib.sha256(b).hexdigest()
def safe(v):
 if isinstance(v,dict):
  for k,x in v.items():
   assert k.lower() not in {'privatekey','private_key','secretkey','secret_key','password','authorization','rawtransaction','raw_transaction','signedtransaction','signed_transaction','environment','env'},k
   safe(x)
 elif isinstance(v,list):
  for x in v:safe(x)
 elif isinstance(v,str):
  assert not re.search(r'0x[0-9a-fA-F]{180,}',v),'full wire-like value'
def preserve(source,name=None,compress=False,jsonl=False):
 p=Path(source);data=p.read_bytes();decoded=gzip.decompress(data) if p.suffix=='.gz' else data
 if jsonl:
  for row in decoded.splitlines():safe(json.loads(row))
 elif p.suffix=='.json':safe(json.loads(data))
 result=gzip.compress(data,compresslevel=6,mtime=0) if compress else data
 dest=OUT/(name or p.name)
 assert not dest.exists() or dest.read_bytes()==result,dest
 dest.write_bytes(result);assert (gzip.decompress(result) if compress else result)==data
 return {'source_name':p.name,'source_sha256':sha(data),'source_bytes':len(data),'archive_path':dest.name,'archive_sha256':sha(result),'archive_bytes':len(result),'encoding':'gzip' if compress or p.suffix=='.gz' else 'original','roundtrip_verified':True}
def derived(name,v):
 safe(v);(OUT/name).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n');return {'archive_path':name,'archive_sha256':sha((OUT/name).read_bytes())}
index={'schema':'capacity-bounded-screen-archive-v1','campaign_id':'017a2525ad46','archived_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Two completed local finite screens plus interrupted Native incident. No maximum or public-chain qualification; original strict verdicts preserved. Recovery of original 14400-ID Native campaign not yet complete at this archive.','runs':[],'supporting_files':[]}
for short in ['evm','op']:
 p=Path('/tmp')/f'capacity-bounded-{short}55-017a2525ad46.json';r=json.loads(p.read_text());chain=next(iter(r['profiles']));stem=p.stem
 assert r['outcome']=='pass' and r['oracle']['observed_intents']==19800 and r['oracle']['safety_pass'] and r['oracle']['liveness_pass']
 assets=[preserve(p,stem+'.full.json.gz',True),preserve(r['observation_evidence'],jsonl=True)]
 ids=set();rows=0
 with gzip.open(r['observation_evidence'],'rt') as f:
  for line in f:
   d=json.loads(line);assert d['id'] not in ids;ids.add(d['id']);rows+=1
   assert d['admitted'] and d['state']=='terminal' and len(d['attempts'])==1 and len(d['executions'])==1
   assert d['executions'][0]['canonical'] and d['executions'][0]['finalized']
 assert rows==19800
 for suffix in ['-assessment.json','-readonly-analysis.json','-backlog-description.json','.log']:
  assets.append(preserve(p.with_name(stem+suffix)))
 # Audits contain only hashes/digests and timing, never full signed transaction bytes.
 assets.append(preserve(r['rpc'][chain]['audit']['path'],stem+'.rpc-audit.jsonl.gz',True,jsonl=True))
 assessment=json.loads((p.with_name(stem+'-assessment.json')).read_text())['reports'][0]['chains'][chain]
 index['runs'].append({'name':stem,'chain':chain,'binary_sha256':r['engine_binary_sha256'],'source_commit':r['source_commit'],'source_is_dirty':bool(r['source_status_porcelain']),'harness_sha256':r['harness_sha256'],'profile':r['profiles'][chain],'http_concurrency':r['http_concurrency'],'eoa_broadcast_concurrency':r['eoa_broadcast_concurrency'],'durability':r['durability'],'per_id_rows':rows,'oracle':r['oracle'],'strict_classification':assessment['classification'],'strict_stage_rates_tps':assessment['stage_rates_tps'],'drain_seconds':next(e['load_elapsed_seconds'] for e in r['events'] if e['event']=='drain_target_reached'),'assets':assets})
 print(short,'archived',rows,flush=True)
# Deliberately export summaries, hashes and read-only observations, not private recovery files.
pre=json.loads((INC/'incident-summary.json').read_text());obs=json.loads((INC/'continuity-observed-1.json').read_text());exp=json.loads((INC/'continuity-expected.json').read_text());research=json.loads((INC/'nitro-persistence-research.json').read_text())
incident={k:pre[k] for k in ['recorded_utc','run','infrastructure_cause','custody','proxy_evidence','last_complete_observer_sample','failure']}
incident.update({'schema':'native-disk-incident-summary-v1','pre_restart_summary_sha256':sha((INC/'incident-summary.json').read_bytes()),'original_result':'infrastructure interruption; independent whole-campaign oracle absent','recovery_complete_at_archive':False,'capacity_pass':False,'fresh_manual_broadcasts_by_checker':0,'continuity_preflight':{'verdict':obs['verdict'],'head_number':obs['head_number'],'head_hash':obs['head_hash'],'genesis_hash':exp['genesis_hash'],'saved_checkpoint':exp['checkpoint'],'latest_nonce':obs['latest_nonce'],'pending_nonce':obs['pending_nonce'],'balances_at_head':obs['balances_at_head'],'terminal_block_anchors_checked':obs['terminal_anchors_checked'],'terminal_hash_memberships_checked':4502,'individually_reconciled_nonterminal_receipts':302,'depth2_qualified_at_head':sum(x['depth2_at_observed_head'] for x in obs['nonterminal_receipts']),'below_depth2_at_head':sum(not x['depth2_at_observed_head'] for x in obs['nonterminal_receipts']),'rpc_calls':obs['rpc_calls'],'limitations':'Checks terminal block/hash/transaction membership, not all4502 individual terminal receipts. Original302 attempts now canonical; remaining9596 unsigned admissions still require original-ID recovery. This is not full campaign recovery or permission to restart Engine.'},'private_evidence_hashes':{'consistent_sqlite_sha256':exp['inputs']['snapshot_sha256'],'original_report_sha256':exp['inputs']['report_sha256']},'prior_native_comparison_caveat':'Storage pressure and accumulated node history confound comparisons across earlier ordered screens. Neither regression nor concurrency/index/disk causation is established.'})
index['supporting_files'].append(derived('nitro-incident-summary.json',incident))
for source,name in [(INC/'continuity-observed-1.json','nitro-continuity-observed.json'),(INC/'restart-head-readiness-1.json','nitro-restart-head-readiness.json'),(INC/'nitro-persistence-research.json','nitro-persistence-research.json'),(INC/'node/disk-repair.json','nitro-disk-repair.json'),(INC/'node/cold-vm-backup/manifest.json','nitro-cold-backup-metadata.json'),(INC/'node/nitro-restart-replay.log','nitro-restart-replay.log'),(INC/'node/post_resize_df.log','nitro-post-resize-df.log'),(INC/'node/post_resize_lsblk.log','nitro-post-resize-lsblk.log')]:
 index['supporting_files'].append(preserve(source,name))
for source in ['/tmp/capacity-posthoc/interpretation-v1.md','/tmp/capacity-posthoc/describe_backlogs.py']:
 index['supporting_files'].append(preserve(source))
index['supporting_files'].append(preserve(__file__,'archive_closed_reports.py'))
(OUT/'index.json').write_text(json.dumps(index,indent=2,sort_keys=True)+'\n')
print('archive ready',len(list(OUT.iterdir())),'files',sum(p.stat().st_size for p in OUT.iterdir() if p.is_file()),'bytes')
