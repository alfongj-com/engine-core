#!/usr/bin/env python3
import collections, datetime, hashlib, json, os, platform, resource, shutil, socket, statistics, subprocess, time
from pathlib import Path

ROOT=Path('/Users/alfongj/Code/engine-core')
OUT=Path('/tmp/engine-terminal-preflight-results')
REDIS=Path('/tmp/redis-7.4.2/src/redis-server')
CLI=REDIS.with_name('redis-cli')
EXPECTED_ENGINE='a3d360eba3dc470bd830ec9d3a32e0be8e11b6764f9bbf6efeb319f080eb8515'
COUNT=768

def sha(path):
    with Path(path).open('rb') as f:
        h=hashlib.sha256()
        for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
        return h.hexdigest()
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def write(name,data):
    (OUT/name).write_text(json.dumps(data,indent=2)+'\n')
def command(args):return subprocess.check_output(args,cwd=ROOT,text=True,stderr=subprocess.STDOUT).strip()
def guard():
    lines=command(['ps','-axo','pid=,comm=,args=']).splitlines()
    for line in lines:
        p=line.strip().split(None,2)
        if len(p)==3 and (p[0]=='39474' or ('python' in p[1].lower() and 'scripts/capacity_campaign.py' in p[2])):
            raise RuntimeError('capacity campaign remains active')

guard()
assert sha(ROOT/'target/release/thirdweb-engine')==EXPECTED_ENGINE
artifacts=[]
for line in (OUT/'build.jsonl').read_text().splitlines():
    try:x=json.loads(line)
    except json.JSONDecodeError:continue
    if x.get('reason')=='compiler-artifact' and x.get('target',{}).get('name')=='engine_core' and x.get('profile',{}).get('test') and x.get('executable'):
        artifacts.append(x)
assert len(artifacts)==1,artifacts
binary=Path(artifacts[0]['executable'])
sources=['core/src/recovery/benchmark.rs','core/src/recovery.rs','core/src/recovery/tests.rs','executors/src/finality.rs','executors/src/eoa/worker/confirm.rs','Cargo.lock','Cargo.toml','core/Cargo.toml']
metadata={
 'schema':'engine-terminal-preflight-comparison-v1', 'startedAt':now(),
 'sourceScope':'Working-tree test-only patch; no production optimization. Exact source hashes below.',
 'baseCommit':command(['git','rev-parse','HEAD']),
 'sources':{p:sha(ROOT/p) for p in sources},
 'engineSha256Before':EXPECTED_ENGINE,
 'preservedEngineSha256':sha(OUT/'thirdweb-engine-preserved'),
 'testExecutable':str(binary),'testExecutableSha256':sha(binary),
 'profile':'release','cargoBuildCommand':['cargo','test','--release','--locked','-p','engine-core','--lib','--no-run','--message-format=json-render-diagnostics'],
 'rustc':command(['/Users/alfongj/.cargo/bin/rustc','--version','--verbose']),
 'cargo':command(['/Users/alfongj/.cargo/bin/cargo','--version']),
 'host':{'platform':platform.platform(),'osVersion':command(['sw_vers','-productVersion']),'architecture':platform.machine(),
         'cpuCount':os.cpu_count(),'memoryBytes':int(command(['sysctl','-n','hw.memsize'])),
         'cpu':command(['sysctl','-n','machdep.cpu.brand_string']),'loadAverageBefore':os.getloadavg(),
         'filesystem':command(['df','-T','apfs',str(ROOT)])},
 'redisVersion':command([str(REDIS),'--version']), 'redisBinarySha256':sha(REDIS),
 'countPerRun':COUNT,'concurrency':1,'pairOrders':[['bare','eoa-caller'],['eoa-caller','bare'],['bare','eoa-caller'],['eoa-caller','bare']],
 'warmups':0,'runs':[], 'scope':'Fresh private journal/namespace each run. Same release executable/inputs/Redis/server configuration. Loopback only; no blockchain RPC. Timed phase operations exclude fixture setup and signing.'}
(OUT/'benchmark-applied.patch').write_text(command(['git','diff','--','core/src/recovery/benchmark.rs'])+'\n')
write('metadata.json',metadata)
redis_dir=OUT/'redis-data';redis_dir.mkdir(exist_ok=False)
with socket.socket() as reservation:
    reservation.bind(('127.0.0.1',0));port=reservation.getsockname()[1]
redis_args=[str(REDIS),'--bind','127.0.0.1','--port',str(port),'--save','','--appendonly','yes','--appendfsync','everysec','--dir',str(redis_dir),'--protected-mode','yes']
metadata['redisArgs']=redis_args
log=(OUT/'redis.log').open('wb')
server=subprocess.Popen(redis_args,stdout=log,stderr=subprocess.STDOUT)
metadata['redisPid']=server.pid

def redis(*args):
    return subprocess.check_output([str(CLI),'-h','127.0.0.1','-p',str(port),'--raw',*args],text=True,stderr=subprocess.STDOUT,timeout=3).strip()
try:
    deadline=time.monotonic()+10
    while True:
        assert server.poll() is None,'Redis exited at startup'
        try:
            if redis('PING')=='PONG':break
        except subprocess.SubprocessError:pass
        if time.monotonic()>deadline:raise RuntimeError('Redis startup timed out')
        time.sleep(.05)
    metadata['redisConfig']={k:redis('CONFIG','GET',k).splitlines()[-1] for k in ['appendonly','appendfsync','save','no-appendfsync-on-rewrite','maxmemory','maxmemory-policy']}
    metadata['redisInfoBefore']=redis('INFO','server')
    write('metadata.json',metadata)
    for pair,order in enumerate(metadata['pairOrders'],1):
        for mode in order:
            guard()
            output=OUT/f'pair-{pair}-{mode}.json'
            assert not output.exists(),output
            argv=[str(binary),'recovery::benchmark::journal_throughput_probe','--ignored','--exact','--test-threads=1','--nocapture']
            env=os.environ.copy()
            env.update(TEST_REDIS_URL=f'redis://127.0.0.1:{port}/',RECOVERY_BENCH_OUTPUT=str(output),RECOVERY_BENCH_COUNT=str(COUNT),RECOVERY_BENCH_CONCURRENCY='1',RECOVERY_BENCH_TERMINAL_MODE=mode)
            run={'pair':pair,'mode':mode,'command':argv,'output':output.name,'startedAt':now(),'loadAverageBefore':os.getloadavg()}
            begin=time.monotonic();before=resource.getrusage(resource.RUSAGE_CHILDREN)
            with (OUT/f'pair-{pair}-{mode}.log').open('wb') as f:
                completed=subprocess.run(argv,cwd=ROOT,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=180)
            after=resource.getrusage(resource.RUSAGE_CHILDREN)
            run.update(exitCode=completed.returncode,wallSeconds=time.monotonic()-begin,finishedAt=now(),childCpuSeconds=(after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime))
            metadata['runs'].append(run);write('metadata.json',metadata)
            assert completed.returncode==0,run
            result=json.loads(output.read_text())
            assert result['schema']=='engine-recovery-journal-probe-v2' and result['terminalMode']==mode
            assert result['count']==COUNT and result['concurrency']==1
            assert result['reconciliation']==dict(admissions=COUNT,attempts=COUNT,terminal=COUNT,checkpoint=COUNT*3,healthy=True)
            run['reportSha256']=sha(output);write('metadata.json',metadata)
            terminal=next(x for x in result['phases'] if x['phase']=='terminal')
            print(f"PASS pair{pair} {mode}: terminal {terminal['elapsedSeconds']/COUNT*1000:.3f}ms/intent, {terminal['operationsPerSecond']:.2f}/s; three-stage {result['threeStageIntentsPerSecond']:.2f}/s",flush=True)
    metadata['redisInfoAfter']=redis('INFO','persistence')
    metadata['engineSha256After']=sha(ROOT/'target/release/thirdweb-engine')
    assert metadata['engineSha256After']==EXPECTED_ENGINE
    assert metadata['sources']=={p:sha(ROOT/p) for p in sources},'source changed during comparison'
    metadata['outcome']='passed'
except BaseException as e:
    metadata['outcome']='failed';metadata['failure']=repr(e)
    raise
finally:
    metadata['finishedAt']=now();metadata['host']['loadAverageAfter']=os.getloadavg()
    server.terminate()
    try:server.wait(timeout=10)
    except subprocess.TimeoutExpired:server.kill();server.wait()
    metadata['redisExitCode']=server.returncode
    log.close();write('metadata.json',metadata)
