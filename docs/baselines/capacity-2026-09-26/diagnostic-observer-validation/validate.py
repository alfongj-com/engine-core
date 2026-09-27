import subprocess,os,time,json,socket,tempfile,hashlib
from pathlib import Path
root=Path('/Users/alfongj/Code/engine-core')
cargo='/Users/alfongj/.cargo/bin/cargo'
env=dict(os.environ,CARGO_INCREMENTAL='0')
folder=Path(tempfile.mkdtemp(prefix='engine-diagnostic-redis-'))
s=socket.socket();s.bind(('127.0.0.1',0));port=s.getsockname()[1];s.close()
log=(folder/'redis.log').open('w')
redis=subprocess.Popen(['/tmp/redis-7.4.2/src/redis-server','--bind','127.0.0.1','--port',str(port),'--save','','--appendonly','no','--dir',str(folder)],stdout=log,stderr=subprocess.STDOUT)
records=[]
def run(name,cmd,timeout=600):
    path=Path('/tmp/engine-diagnostic-'+name+'.log')
    t=time.monotonic()
    with path.open('w') as f: p=subprocess.run(cmd,cwd=root,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=timeout)
    record=dict(name=name,command=cmd,exit=p.returncode,seconds=round(time.monotonic()-t,3),log=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    records.append(record);print(json.dumps(record),flush=True)
    if p.returncode: raise RuntimeError(name+' failed')
try:
    for _ in range(100):
        if redis.poll() is not None: raise RuntimeError('Redis failed')
        try:
            with socket.create_connection(('127.0.0.1',port),timeout=.1):break
        except OSError:time.sleep(.05)
    else: raise RuntimeError('Redis readiness timeout')
    env['TEST_REDIS_URL']='redis://127.0.0.1:'+str(port)+'/'
    run('workspace-unit',[cargo,'test','--locked','--workspace','--lib'])
    run('queue-redis',[cargo,'test','--locked','-p','twmq','--lib','--','--ignored','--test-threads=1'])
    run('executor-redis',[cargo,'test','--locked','-p','engine-executors','--lib','--','--ignored','--test-threads=1'])
    run('release',[cargo,'build','--locked','--release','-p','thirdweb-engine'])
finally:
    redis.terminate()
    try:redis.wait(timeout=10)
    except subprocess.TimeoutExpired:redis.kill();redis.wait()
    log.close()
    result=dict(records=records,redis=dict(port=port,pid=redis.pid,exit=redis.returncode,folder=str(folder)),engine_sha256=hashlib.sha256((root/'target/release/thirdweb-engine').read_bytes()).hexdigest())
    Path('/tmp/engine-diagnostic-validation.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result),flush=True)
