import datetime,gzip,hashlib,importlib.util,json,pathlib,shutil,statistics,sys
ROOT=pathlib.Path('/Users/alfongj/Code/engine-core')
BASE=ROOT/'docs/baselines/capacity-2026-09-26'
OUT=BASE/'final-screen-132b012af02d'
OUT.mkdir(exist_ok=True)
statepath=pathlib.Path('/tmp/engine-capacity-final-screen-state.json')
state=json.loads(statepath.read_text());assert state['id']=='132b012af02d' and state['active'] is None
spec=importlib.util.spec_from_file_location('describe_backlogs','/tmp/capacity-posthoc/describe_backlogs.py');desc=importlib.util.module_from_spec(spec);spec.loader.exec_module(desc)
def sha(data):return hashlib.sha256(data).hexdigest()
def preserve(source,name=None,compress=False):
 source=pathlib.Path(source);data=source.read_bytes();name=name or source.name;dest=OUT/name
 encoded=gzip.compress(data,compresslevel=6,mtime=0) if compress else data
 if dest.exists():assert dest.read_bytes()==encoded, f'Existing archive differs: {dest}'
 else:dest.write_bytes(encoded)
 assert (gzip.decompress(dest.read_bytes()) if compress else dest.read_bytes())==data
 return {'source_path':str(source),'source_sha256':sha(data),'source_bytes':len(data),'archive_path':name,'archive_sha256':sha(encoded),'archive_bytes':len(encoded),'encoding':'gzip' if compress else 'original'}
def ct(t):return sum(float(x)*60**i for i,x in enumerate(reversed(t.split(':'))))
def windows(r,chain):
 rows=[s for s in r['samples'] if s['phase']=='load'];result={}
 for start in [120,180,240,300,330]:
  a=min(rows,key=lambda s:abs(s['seconds']-start));b=rows[-1];dt=b['seconds']-a['seconds'];ac,bc=a['chains'][chain],b['chains'][chain];methods={}
  for m,x in bc['rpc']['methods'].items():
   old=ac['rpc']['methods'].get(m,{});n=x['count']-old.get('count',0)
   if n:methods[m]={'count':n,'rps':n/dt,'mean_ms':(x['total_ms']-old.get('total_ms',0))/n}
  ar={x['name']:ct(x['cpu_time']) for x in a['resources']['processes']};br={x['name']:ct(x['cpu_time']) for x in b['resources']['processes']}
  result[f'{start}-360']={'from_seconds':a['seconds'],'to_seconds':b['seconds'],'rates_tps':{k:(bc[k]-ac[k])/dt for k in ['admitted','attempted','included','terminal']},'backlogs':{k:[ac['admitted']-ac[v],bc['admitted']-bc[v]] for k,v in [('unsigned','attempted'),('terminal','terminal')]},'engine_rpc_rps':sum(x['count'] for x in methods.values())/dt,'methods':methods,'cpu_one_core_percent':{k:(br[k]-ar[k])*100/dt for k in ar.keys()&br.keys()}}
 return result
index={'schema':'capacity-final-screen-archive-v1','campaign_id':state['id'],'archived_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Immutable closed reports. Local finite profiles only; neither throughput maximum nor public-chain qualification. Original strict verdicts retained. No source/runtime changes.','runs':[],'supporting_files':[]}
for entry in state['runs']:
 p=pathlib.Path(entry['report']);r=json.loads(p.read_text());stem=p.stem;chain=next(iter(r['profiles']));assert sha(p.read_bytes())==entry['report_sha256']
 assets=[preserve(p,stem+'.full.json.gz',True),preserve(entry['assessment']),preserve(r['observation_evidence'])]
 audit=r['rpc'][chain]['audit']['path'];assets.append(preserve(audit,stem+'.rpc-audit.jsonl.gz',True))
 log=p.with_suffix('.log')
 if log.exists():assets.append(preserve(log))
 dp=pathlib.Path('/tmp')/(stem+'-backlog-description.json')
 if not dp.exists():
  d={'report':str(p),'report_sha256':sha(p.read_bytes()),'descriptor_sha256':sha(pathlib.Path('/tmp/capacity-posthoc/describe_backlogs.py').read_bytes()),'interpretation_policy_sha256':sha(pathlib.Path('/tmp/capacity-posthoc/interpretation-v1.md').read_bytes()),'chains':{name:desc.describe(r,name) for name in r['profiles']}}
  dp.write_text(json.dumps(d,indent=2,sort_keys=True)+'\n')
 assets.append(preserve(dp))
 derived={'report':str(p),'report_sha256':sha(p.read_bytes()),'binary_sha256':r['engine_binary_sha256'],'window_method':'Closest existing load samples; rates from cumulative counter deltas over actual sample times. CPU percent is one-core time, not a bottleneck attribution. Included Solana counts are observer-lagged.','windows':windows(r,chain)}
 out=OUT/(stem+'-window-analysis.json');out.write_text(json.dumps(derived,indent=2,sort_keys=True)+'\n')
 ids=[];rows=[]
 with gzip.open(r['observation_evidence'],'rt') as f:
  for line in f:
   d=json.loads(line);ids.append(d['id'])
   if chain=='solana':rows.append(d)
 assert len(ids)==len(set(ids))==r['oracle']['observed_intents']
 assessment=json.loads(pathlib.Path(entry['assessment']).read_text())['reports'][0]['chains'][chain]
 row={'name':entry['name'],'chain':chain,'report_sha256':entry['report_sha256'],'binary_sha256':r['engine_binary_sha256'],'source_commit':r['source_commit'],'harness_sha256':r['harness_sha256'],'profile':r['profiles'][chain],'http_concurrency':r['http_concurrency'],'broadcast_concurrency':r['eoa_broadcast_concurrency'],'solana_poll_seconds':r['solana_confirmation_poll_seconds'],'offered':r['per_chain'][chain]['offered'],'client':r['per_chain'][chain]['client'],'responses':r['per_chain'][chain]['responses'],'oracle':r['oracle'],'strict_classification':assessment['classification'],'strict_stage_rates_tps':assessment['stage_rates_tps'],'per_id_rows':len(ids),'assets':assets,'derived_window_analysis':{'path':out.name,'sha256':sha(out.read_bytes())},'drain_seconds':next(e['load_elapsed_seconds'] for e in r['events'] if e['event']=='drain_target_reached')}
 if chain=='solana':
  existing={int(i.rsplit('-',1)[1]) for i in ids};missing=sorted(set(range(r['per_chain'][chain]['offered']))-existing);assert missing==[20312,20313]
  assert all(d['admitted'] and d['state']=='terminal' and len(d['attempts'])==1 and len(d['executions'])==1 and d['executions'][0]['finalized'] and d['executions'][0]['canonical'] for d in rows)
  drops={'missing_indices':missing,'scheduled_seconds':[i/60 for i in missing],'timing_basis':'Nominal schedule derived from missing indices at 60 TPS, not measured drop timestamps; counter first changes between335.009 and340.014s.','actual_drop_timestamps_retained':False,'client_limit':64,'peak_campaign_http_active':r['campaign_http']['peak_active'],'capacity_drops':2,'schedule_lag_drops':0,'max_http_start_lag_ms':r['per_chain'][chain]['scheduled_to_http_start_lateness_ms']['max'],'accepted_observations_exactly_one_attempt_and_one_finalized_execution':len(rows),'load_end_sample':{k:r['samples'][max(i for i,s in enumerate(r['samples']) if s['phase']=='load')]['chains'][chain][k] for k in ['admitted','attempted','included','terminal','finalized','observer_pending_signatures']},'observer_scope':'Rotating pending-list cursor can track the newly appended tail while intake continues; old signatures can remain unvisited until intake stops. Included rate is not full chain inclusion throughput; finalized=0 during all load samples despite durable finalized terminals. Full finalized per-ID reconciliation occurs after drain.'}
  assert all(s['chains'][chain]['finalized']==0 for s in r['samples'] if s['phase']=='load')
  dp=OUT/'solana-drop-and-observer-analysis.json';dp.write_text(json.dumps(drops,indent=2)+'\n');row['drop_analysis']={'path':dp.name,'sha256':sha(dp.read_bytes())}
 index['runs'].append(row)
 print(entry['name'], 'archived',len(ids),'per-ID rows',flush=True)
for source,name in [(statepath,'supervisor-state.json'),('/tmp/engine-capacity-final-screen-jobs.json','jobs.json'),('/tmp/capacity-posthoc/describe_backlogs.py','describe_backlogs.py'),('/tmp/capacity-posthoc/interpretation-v1.md','interpretation-v1.md'),(BASE/'native-broadcast-comparison.md',None),(BASE/'native-broadcast-comparison-data.json',None)]:index['supporting_files'].append(preserve(source,name))
index['supporting_files'].append(preserve(__file__,'archive_closed_reports.py'))
(OUT/'index.json').write_text(json.dumps(index,indent=2,sort_keys=True)+'\n')
print('index',OUT/'index.json','totalbytes',sum(p.stat().st_size for p in OUT.iterdir() if p.is_file()))
