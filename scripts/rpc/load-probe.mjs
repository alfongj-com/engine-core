#!/usr/bin/env node
/** Bounded, read-only, open-loop RPC probe through the local budget gateway.
 * No retries, no raw responses in artifacts, no direct provider credentials.
 */
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {performance, monitorEventLoopDelay} from 'node:perf_hooks';
import {setTimeout as sleep} from 'node:timers/promises';
import {pathToFileURL} from 'node:url';
import {NETWORKS,validateBatch} from './budget-gateway.mjs';

const WRITES=new Set(['eth_sendRawTransaction','sendTransaction','requestAirdrop']);
const MAX_PLANNED=100_000;
const DISCOVERY_ALLOWANCE=32;
const DEFAULT_RATES=[10,50,200,500,1000];
const ZERO='0x0000000000000000000000000000000000000000';
const DESTINATION='0x000000000000000000000000000000000000dead';
const ADDRESS=/^0x[0-9a-fA-F]{40}$/;
const HASH=/^0x[0-9a-fA-F]{64}$/;
const SIGNATURE=/^[1-9A-HJ-NP-Za-km-z]{64,88}$/;

export function gatewayUrl(value) {
  let url;try{url=new URL(value);}catch{throw Error('Invalid local gateway URL');}
  if(url.protocol!=='http:'||!['127.0.0.1','[::1]'].includes(url.hostname)||!url.port||url.username||url.password||url.pathname!=='/'||url.search||url.hash)
    throw Error('Gateway must be an explicit loopback HTTP address and port');
  return url.origin;
}
function integer(value,min,max,name){if(!Number.isSafeInteger(value)||value<min||value>max)throw Error(`Invalid ${name}`);return value;}
export function configuration(options={}) {
  const stages=options.stages??DEFAULT_RATES.map(rate=>({rate,count:rate*5}));
  if(!Array.isArray(stages)||stages.length<1||stages.length>20)throw Error('Invalid stages');
  const normalized=stages.map(stage=>({rate:integer(stage.rate,1,5000,'rate'),count:integer(stage.count,1,MAX_PLANNED,'count')}));
  const planned=normalized.reduce((sum,stage)=>sum+stage.count,0);
  if(planned>MAX_PLANNED||normalized.some(stage=>stage.count/stage.rate>1800))throw Error('Probe exceeds bounded run limits');
  if(!Object.hasOwn(NETWORKS,options.chain))throw Error('Select a supported test network');
  return {gateway:gatewayUrl(options.gateway??'http://127.0.0.1:8788'),chain:options.chain,stages:normalized,planned,
    concurrency:integer(options.concurrency??256,1,1024,'concurrency'),
    timeoutMs:integer(options.timeoutMs??20_000,1,60_000,'timeout'),
    reserve:integer(options.reserve??10_000,0,2_000_000,'reserve'),
    maxLagMs:integer(options.maxLagMs??100,1,10_000,'scheduler lag'),
    pauseMs:integer(options.pauseMs??1000,0,60_000,'pause')};
}
function summary(values) {
  if(!values.length)return {count:0,p50:null,p95:null,p99:null,max:null};
  const sorted=[...values].sort((a,b)=>a-b),at=p=>+sorted[Math.min(sorted.length-1,Math.ceil(p*sorted.length)-1)].toFixed(3);
  return {count:sorted.length,p50:at(.5),p95:at(.95),p99:at(.99),max:at(1)};
}
function increment(map,key){map[key]=(map[key]??0)+1;}
function publicBudget(metrics){return {observed:metrics.budget.observed,reserved:metrics.budget.reserved,remainingCalls:metrics.budget.remainingCalls,ceiling:metrics.budget.ceiling};}
async function readJson(response,limit=8_388_608) {
  const reader=response.body?.getReader();if(!reader)throw Error('Missing response');
  const chunks=[];let bytes=0;
  try{for(;;){const {done,value}=await reader.read();if(done)break;bytes+=value.length;if(bytes>limit)throw Error('Response limit exceeded');chunks.push(value);}
    return JSON.parse(Buffer.concat(chunks).toString());
  }finally{await reader.cancel().catch(()=>{});}
}
async function metrics(config) {
  const response=await fetch(`${config.gateway}/metrics`,{redirect:'error',signal:AbortSignal.timeout(config.timeoutMs)});
  if(response.status!==200||response.headers.get('x-engine-rpc-gateway')!=='1')throw Error('Expected upgraded local budget gateway');
  const data=await readJson(response);
  if(!data.accepting||!Number.isSafeInteger(data.budget?.remainingCalls)||!Number.isSafeInteger(data.budget?.observed))throw Error('Gateway budget unavailable');
  return data;
}
function makeCaller(config) {
  let id=0;
  return async(call)=>{
    const requestId=++id,start=performance.now();
    try{
      const response=await fetch(`${config.gateway}/${config.chain}`,{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({jsonrpc:'2.0',id:requestId,method:call.method,params:call.params}),redirect:'error',signal:AbortSignal.timeout(config.timeoutMs)});
      const status=response.status,origin=response.headers.get('x-engine-rpc-error-origin');
      const safeOrigin=['local','upstream','transport'].includes(origin)?origin:'unknown';
      if(response.headers.get('x-engine-rpc-gateway')!=='1'){await response.body?.cancel();return{kind:'invalid_gateway',status,origin:'unknown',ms:performance.now()-start};}
      if(status!==200){await response.body?.cancel();return{kind:status===402&&safeOrigin==='local'?'budget_exhausted':`${safeOrigin}_http_error`,status,origin:safeOrigin,ms:performance.now()-start};}
      const body=await readJson(response);
      if(!body||body.jsonrpc!=='2.0'||body.id!==requestId||Object.hasOwn(body,'result')===Object.hasOwn(body,'error'))
        return{kind:'invalid_rpc_response',status,origin:safeOrigin,ms:performance.now()-start};
      if(body.error)return{kind:'rpc_error',status,origin:safeOrigin,code:Number.isSafeInteger(body.error.code)?body.error.code:null,ms:performance.now()-start};
      return {kind:body.result===null?'success_null':'success',status,origin:safeOrigin,result:body.result,ms:performance.now()-start};
    }catch{return {kind:'client_transport_error',status:0,origin:'client',ms:performance.now()-start};}
  };
}
function checkedMix(chain,mix){
  if(!Array.isArray(mix)||!mix.length||mix.length>100)throw Error('Invalid RPC mix');
  for(const call of mix){validateBatch(chain,{...call,jsonrpc:'2.0',id:1});if(WRITES.has(call.method))throw Error('Probe permits read-only methods');}
  return mix;
}
async function discover(config,call,signal) {
  const counts={},samples=[];
  async function rpc(method,params){
    if(signal?.aborted)throw Error('Probe interrupted during discovery');
    increment(counts,method);if(Object.values(counts).reduce((a,b)=>a+b,0)>DISCOVERY_ALLOWANCE)throw Error('Discovery limit reached');
    const response=await call({method,params});
    samples.push({method,kind:response.kind,status:response.status,ms:+response.ms.toFixed(3)});
    if(response.kind!=='success')throw Error(`Discovery failed for ${method}; inspect gateway aggregate counters`);
    return response.result;
  }
  if(config.chain==='solana-devnet'){
    const list=await rpc('getSignaturesForAddress',['11111111111111111111111111111111',{limit:8,commitment:'confirmed'}]);
    const signatures=(Array.isArray(list)?list:[]).map(item=>item.signature).filter(sig=>typeof sig==='string'&&SIGNATURE.test(sig));
    if(!signatures.length)throw Error('No recent signatures available for a representative probe');
    await rpc('getTransaction',[signatures[0],{encoding:'json',commitment:'confirmed',maxSupportedTransactionVersion:0}]);
    const mix=signatures.flatMap(signature=>[
      {method:'getLatestBlockhash',params:[{commitment:'confirmed'}]},
      {method:'getRecentPrioritizationFees',params:[]},
      {method:'getSignatureStatuses',params:[[signature],{searchTransactionHistory:true}]},
      {method:'getTransaction',params:[signature,{encoding:'json',commitment:'confirmed',maxSupportedTransactionVersion:0}]},
    ]);
    return {mix,discovery:{counts,samples,signatureSampleSize:signatures.length,recipe:'confirmed blockhash, priority fees, recent signature status, recent transaction (equal weights)'}};
  }
  let block=await rpc('eth_getBlockByNumber',['latest',true]),transactions=[];
  for(let attempt=0;attempt<8;attempt++){
    transactions=(Array.isArray(block?.transactions)?block.transactions:[]).filter(tx=>ADDRESS.test(tx?.from)&&HASH.test(tx?.hash)).slice(0,8);
    if(transactions.length)break;
    if(!/^0x[0-9a-fA-F]+$/.test(block?.number??'')||BigInt(block.number)===0n)break;
    block=await rpc('eth_getBlockByNumber',[`0x${(BigInt(block.number)-1n).toString(16)}`,true]);
  }
  if(!transactions.length)throw Error('No recent transactions available for a representative probe');
  const receipt=await rpc('eth_getTransactionReceipt',[transactions[0].hash]);
  if(!receipt?.transactionHash||!HASH.test(receipt.transactionHash))throw Error('Recent receipt unavailable');
  let funded;
  for(const candidate of transactions.slice(0,3)){
    const balance=await rpc('eth_getBalance',[candidate.from,'latest']);
    if(typeof balance==='string'&&/^0x[0-9a-fA-F]+$/.test(balance)&&BigInt(balance)>=1_000_000_000_000_000n){funded=candidate.from;break;}
  }
  const estimate={from:funded??ZERO,to:DESTINATION,value:'0x0',data:'0x',...(funded?{}:{gasPrice:'0x0'})};
  const mix=transactions.flatMap(tx=>[
    {method:'eth_feeHistory',params:['0xa','latest',[20]]},
    {method:'eth_estimateGas',params:[estimate]},
    {method:'eth_getTransactionCount',params:[tx.from,'latest']},
    {method:'eth_getTransactionReceipt',params:[tx.hash]},
  ]);
  return {mix,discovery:{counts,samples,transactionSampleSize:transactions.length,
    estimateScenario:funded?'zero-value transfer to empty address from recently funded sender (balance >= 0.001 ETH)':'zero-value, zero-gas-price estimate with zero sender; may be rejected by provider policy',
    recipe:'fee history, transfer gas estimate, recent sender latest nonce, recent receipt (equal weights)'}};
}
export async function runStage(config,stage,mix,call,{signal}={}) {
  const outcomes={},httpStatuses={},rpcCodes={},methods={},methodResults={},latencies=[],successLatencies=[],lags=[],active=new Set();
  let localConcurrencyDrops=0,localSchedulerDrops=0,maxInflight=0,dispatched=0,stoppedReason=null;
  const delay=monitorEventLoopDelay({resolution:10});delay.enable();
  const start=performance.now(),cpu=process.cpuUsage(),util=performance.eventLoopUtilization(),interval=1000/stage.rate;
  for(let slot=0;slot<stage.count;slot++){
    if(signal?.aborted||stoppedReason){stoppedReason??='interrupted';break;}
    const target=start+slot*interval;
    while(performance.now()<target){await sleep(Math.max(1,target-performance.now()));if(signal?.aborted)break;}
    if(signal?.aborted||stoppedReason){stoppedReason??='interrupted';break;}
    const lag=performance.now()-target;
    if(lag>config.maxLagMs){localSchedulerDrops++;continue;}
    if(active.size>=config.concurrency){localConcurrencyDrops++;continue;}
    const request=mix[slot%mix.length];lags.push(lag);dispatched++;increment(methods,request.method);
    const pending=call(request).then(result=>{
      increment(outcomes,result.kind);increment(httpStatuses,String(result.status));
      const item=methodResults[request.method]??={outcomes:{},latencies:[]};increment(item.outcomes,result.kind);item.latencies.push(result.ms);
      if(result.code!==undefined)increment(rpcCodes,String(result.code));
      latencies.push(result.ms);if(result.kind==='success'||result.kind==='success_null')successLatencies.push(result.ms);
      if(result.kind==='budget_exhausted'||result.kind==='invalid_gateway')stoppedReason=result.kind;
    }).finally(()=>active.delete(pending));
    active.add(pending);maxInflight=Math.max(maxInflight,active.size);
  }
  const admissionEnd=performance.now();await Promise.all(active);
  const end=performance.now(),usage=process.cpuUsage(cpu),loop=performance.eventLoopUtilization(util);delay.disable();
  const localDrops=localConcurrencyDrops+localSchedulerDrops;
  const localGatewayErrors=outcomes.local_http_error??0;
  return {targetRps:stage.rate,scheduled:stage.count,dispatched,localConcurrencyDrops,localSchedulerDrops,
    notScheduled:stage.count-dispatched-localDrops,stoppedReason,methods,methodResults:Object.fromEntries(Object.entries(methodResults).map(([method,data])=>[method,{outcomes:data.outcomes,latencyMs:summary(data.latencies)}])),outcomes,httpStatuses,rpcCodes,maxInflight,
    targetDurationMs:stage.count*interval,admissionDurationMs:+(admissionEnd-start).toFixed(3),drainDurationMs:+(end-admissionEnd).toFixed(3),elapsedMs:+(end-start).toFixed(3),
    observedDispatchRps:+(dispatched/Math.max(stage.count*interval,admissionEnd-start)*1000).toFixed(3),
    successfulResponseRpsIncludingDrain:+(successLatencies.length/Math.max(stage.count*interval,end-start)*1000).toFixed(3),
    latencyMs:summary(latencies),successLatencyMs:summary(successLatencies),dispatchLagMs:summary(lags),
    processCpuMs:{user:usage.user/1000,system:usage.system/1000},eventLoopUtilization:loop.utilization,
    eventLoopDelayMs:{p99:delay.percentile(99)/1e6,max:delay.max/1e6},
    interpretation:localDrops||localGatewayErrors?'Local scheduling/concurrency limited this stage; it cannot establish an upstream limit.':'No observed local admission limit; upstream errors and latency still require separate interpretation.'};
}
export async function runProbe(options,{mix:providedMix,signal,onStage=()=>{}}={}) {
  const config=configuration(options),before=await metrics(config);
  if(config.planned+(providedMix?0:DISCOVERY_ALLOWANCE)+config.reserve>before.budget.remainingCalls)throw Error('Insufficient remaining campaign budget plus reserve');
  const call=makeCaller(config),startedAt=new Date().toISOString();
  const {mix,discovery}=providedMix?{mix:checkedMix(config.chain,providedMix),discovery:{recipe:'explicit read-only test fixture',counts:{}}}:await discover(config,call,signal);
  checkedMix(config.chain,mix);
  const stages=[];
  for(const stage of config.stages){
    if(signal?.aborted)break;
    const result=await runStage(config,stage,mix,call,{signal});stages.push(result);onStage(result);
    if(result.stoppedReason)break;
    if(stages.length<config.stages.length&&config.pauseMs)await sleep(config.pauseMs);
  }
  let after;try{after=await metrics(config);}catch{/* Keep results even if gateway closed during the final drain. */}
  const budgetBefore=publicBudget(before),budgetAfter=after?publicBudget(after):null;
  return {version:1,startedAt,finishedAt:new Date().toISOString(),scope:'Read-only RPC responsiveness; neither transaction submission capacity nor chain inclusion throughput.',
    limitations:['Small recent sender/hash/signature sample can benefit from provider caching.','Stages run once in fixed order; this is a short screening probe, not a sustained capacity guarantee.','Process CPU measures the probe only; gateway/Redis/provider resource use is excluded.','Budget delta includes any concurrent users of this gateway.','One RPC method per HTTP request; no retries or batching.'],
    chain:config.chain,network:NETWORKS[config.chain],config:{...config,gateway:'loopback budget gateway'},
    discovery,mixMethods:[...new Set(mix.map(call=>call.method))],mixSha256:createHash('sha256').update(JSON.stringify(mix)).digest('hex'),
    budgetBefore,budgetAfter,observedCampaignCallDelta:budgetAfter?budgetAfter.observed-budgetBefore.observed:null,stages};
}
export function parseArgs(args) {
  const values={},flags=new Set(['execute','help']);
  for(let i=0;i<args.length;i++){
    const key=args[i].replace(/^--/,'');
    if(!args[i].startsWith('--')||Object.hasOwn(values,key))throw Error('Invalid or repeated option');
    if(flags.has(key))values[key]=true;
    else{if(!['chain','gateway','rates','seconds','count','concurrency','timeout-ms','reserve','max-lag-ms','pause-ms','output'].includes(key)||!args[i+1]||args[i+1].startsWith('--'))throw Error('Unknown option or missing value');values[key]=args[++i];}
  }
  if(values.help)return {help:true};
  const rates=values.rates?values.rates.split(',').map(Number):DEFAULT_RATES;
  const seconds=Number(values.seconds??5);if(!Number.isFinite(seconds)||seconds<=0||seconds>1800)throw Error('Invalid stage seconds');
  if(values.count&&rates.length!==1)throw Error('--count requires one --rates value');
  const stages=rates.map(rate=>({rate,count:values.count?Number(values.count):rate*seconds}));
  const config=configuration({chain:values.chain,gateway:values.gateway,stages,
    concurrency:values.concurrency===undefined?undefined:Number(values.concurrency),timeoutMs:values['timeout-ms']===undefined?undefined:Number(values['timeout-ms']),
    reserve:values.reserve===undefined?undefined:Number(values.reserve),maxLagMs:values['max-lag-ms']===undefined?undefined:Number(values['max-lag-ms']),pauseMs:values['pause-ms']===undefined?undefined:Number(values['pause-ms'])});
  if(values.execute&&!values.output)throw Error('--execute requires --output for saved measurements');
  return {config,execute:!!values.execute,output:values.output};
}
async function main(){
  const args=parseArgs(process.argv.slice(2));
  if(args.help){console.log('Usage: node scripts/rpc/load-probe.mjs --chain 11155111 [--rates 10,50,200,500,1000 --seconds 5] [--rates 100 --count 500] [--concurrency 256 --reserve 10000] [--execute --output result.json]\nDefault is dry-run; --execute requires an upgraded loopback budget gateway.');return;}
  if(!args.execute){console.log(JSON.stringify({dryRun:true,...args.config,gateway:'loopback budget gateway',maximumCalls:args.config.planned+DISCOVERY_ALLOWANCE,maximumEstimatedUsd:(args.config.planned+DISCOVERY_ALLOWANCE)*.000006},null,2));return;}
  // Reserve the output name before any RPC; do not overwrite previous evidence.
  fs.mkdirSync(path.dirname(path.resolve(args.output)),{recursive:true});const fd=fs.openSync(args.output,'wx',0o600);
  const controller=new AbortController(),stop=()=>controller.abort();process.once('SIGINT',stop);process.once('SIGTERM',stop);
  try{
    const report=await runProbe(args.config,{signal:controller.signal,onStage:stage=>console.log(JSON.stringify({targetRps:stage.targetRps,dispatched:stage.dispatched,outcomes:stage.outcomes,localConcurrencyDrops:stage.localConcurrencyDrops,localSchedulerDrops:stage.localSchedulerDrops,p99Ms:stage.latencyMs.p99}))});
    fs.writeFileSync(fd,JSON.stringify(report,null,2)+'\n');fs.fsyncSync(fd);
    console.log(JSON.stringify({saved:true,observedCampaignCallDelta:report.observedCampaignCallDelta}));
  }catch{fs.writeFileSync(fd,JSON.stringify({version:1,failed:true,message:'Probe failed; provider responses and local paths withheld.',finishedAt:new Date().toISOString()})+'\n');throw Error('Probe failed; inspect local aggregate gateway counters');}
  finally{fs.closeSync(fd);process.removeListener('SIGINT',stop);process.removeListener('SIGTERM',stop);}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)main().catch(()=>{console.error('RPC probe failed; configuration and provider details withheld.');process.exitCode=1;});
