import subprocess,os,time,json,socket,tempfile,hashlib
from pathlib import Path
root=Path('/Users/alfongj/Code/engine-core'); src=root/'executors/src/eoa/worker/send.rs'; baseline=src.read_text()
cargo='/Users/alfongj/.cargo/bin/cargo'; env=dict(os.environ,CARGO_INCREMENTAL='0')
folder=Path(tempfile.mkdtemp(prefix='engine-dispatch-mutations-'));folder.chmod(0o700)
s=socket.socket();s.bind(('127.0.0.1',0));port=s.getsockname()[1];s.close()
log=(folder/'redis.log').open('w');redis=subprocess.Popen(['/tmp/redis-7.4.2/src/redis-server','--bind','127.0.0.1','--port',str(port),'--save','','--appendonly','no','--dir',str(folder)],stdout=log,stderr=subprocess.STDOUT)
records=[]
def run(name,test,expected_fail=False):
 path=Path('/tmp/engine-dispatch-'+name+'.log'); cmd=[cargo,'test','--locked','-p','engine-executors','--lib',test,'--','--ignored','--test-threads=1','--nocapture'];start=time.monotonic()
 with path.open('w') as f:r=subprocess.run(cmd,cwd=root,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=180)
 content=path.read_text(); row=dict(name=name,command=cmd,exit=r.returncode,seconds=round(time.monotonic()-start,3),log=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),expected_failure=expected_fail);records.append(row);print(json.dumps(row),flush=True)
 if expected_fail: assert r.returncode==101 and 'test result: FAILED' in content and 'could not compile' not in content, 'negative control failed for wrong reason'
 else: assert r.returncode==0,name+' failed'
def change(old,new):
 assert baseline.count(old)==1,(old,baseline.count(old));src.write_text(baseline.replace(old,new))
try:
 for _ in range(100):
  if redis.poll() is not None:raise RuntimeError('Redisfailed')
  try:
   with socket.create_connection(('127.0.0.1',port),timeout=.1):break
  except OSError:time.sleep(.05)
 else:raise RuntimeError('Redisreadiness')
 env['TEST_REDIS_URL']='redis://127.0.0.1:'+str(port)+'/'
 run('strong-suffix-baseline','eoa::worker::send::dispatch_tests')
 mutations=[
 ('whole-batch-barrier','authorization_overlaps_http_and_preserves_order_and_uncertain_results','    let outcomes = producer\n        .map','    let authorized = producer.collect::<Vec<_>>().await;\n    let outcomes = futures::stream::iter(authorized)\n        .map'),
 ('missing-stop','failed_authorization_drains_prefix_and_retains_whole_batch_for_exact_recovery','if stopped || index == count {','if index == count {'),
 ('missing-post-owner','owner_changes_before_or_during_authorization_stop_dispatch_without_recycling','        authorization.await?;\n        self.store.ensure_eoa_lock_owned().await?;','        authorization.await?;'),
 ('missing-first-poll-stop','first_auth_error_suppresses_ready_prefix_before_network_start','                if authorization_failed.load(std::sync::atomic::Ordering::Acquire) {\n                    return Ok(None);\n                }','')]
 for name,test,old,new in mutations:
  try:
   change(old,new);run('mutation-'+name,'eoa::worker::send::dispatch_tests::'+test,True)
  finally:src.write_text(baseline)
 run('restored-targeted','eoa::worker::send')
finally:
 src.write_text(baseline);redis.terminate()
 try:redis.wait(timeout=10)
 except subprocess.TimeoutExpired:redis.kill();redis.wait()
 log.close();result=dict(records=records,restored_source_sha256=hashlib.sha256(src.read_bytes()).hexdigest(),test_source_sha256=hashlib.sha256((root/'executors/src/eoa/worker/send_dispatch_tests.rs').read_bytes()).hexdigest(),redis=dict(port=port,pid=redis.pid,exit=redis.returncode,folder=str(folder)))
 Path('/tmp/engine-dispatch-mutations-validation.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
