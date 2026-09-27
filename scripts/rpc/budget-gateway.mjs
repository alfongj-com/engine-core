#!/usr/bin/env node
/** Loopback test gateway. All calls are durably reserved BEFORE paid dispatch.
 * A crash forfeits unused reservations; restarting never resets the $12 ceiling.
 * Never log request payloads, provider credentials, URLs, or response bodies.
 */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { pathToFileURL } from 'node:url';

export const MAX_CAMPAIGN_CALLS = 2_000_000;
export const NETWORKS = Object.freeze({
  '11155111': 'sepolia', '421614': 'arbitrum-sepolia',
  '11155420': 'optimism-sepolia', '84532': 'base-sepolia', 'solana-devnet': 'solana-devnet',
});
const EVM = new Set(['eth_chainId','eth_blockNumber','eth_getBlockByNumber','eth_getTransactionCount',
  'eth_getBalance','eth_getCode','eth_feeHistory','eth_estimateGas','eth_gasPrice','eth_maxPriorityFeePerGas',
  'eth_sendRawTransaction','eth_getTransactionReceipt','eth_getTransactionByHash','eth_call']);
const SOL = new Set(['getGenesisHash','getVersion','getHealth','getSlot','getBlockHeight','getBalance',
  'getLatestBlockhash','getRecentPrioritizationFees','sendTransaction','getSignatureStatuses','getSignaturesForAddress',
  'isBlockhashValid','getTransaction','simulateTransaction','getAccountInfo','getFeeForMessage','requestAirdrop']);

function durableJson(file, data) {
  const temp = `${file}.tmp`;
  const fd = fs.openSync(temp, 'w', 0o600);
  try { fs.writeFileSync(fd, JSON.stringify(data, null, 2)+'\n'); fs.fsyncSync(fd); }
  finally { fs.closeSync(fd); }
  fs.renameSync(temp,file);
  const dir = fs.openSync(path.dirname(file),'r');
  try { fs.fsyncSync(dir); } finally { fs.closeSync(dir); }
}
export class Budget {
  constructor(file, ceiling=MAX_CAMPAIGN_CALLS, chunk=1000) {
    if (!Number.isSafeInteger(ceiling) || ceiling<1 || ceiling>MAX_CAMPAIGN_CALLS || !Number.isSafeInteger(chunk) || chunk<1) throw Error('Invalid budget');
    this.file=file; this.chunk=chunk; this.available=0;
    this.state=fs.existsSync(file)?JSON.parse(fs.readFileSync(file,'utf8')):
      {version:1,ceiling,reserved:0,observed:0,startedAt:new Date().toISOString()};
    if (this.state.version!==1 || this.state.ceiling!==ceiling ||
      !Number.isSafeInteger(this.state.reserved) || this.state.reserved<0 || this.state.reserved>ceiling ||
      !Number.isSafeInteger(this.state.observed) || this.state.observed<0 || this.state.observed>this.state.reserved) throw Error('Budget state or ceiling mismatch');
    durableJson(file,this.state);
  }
  take(n) {
    if (!Number.isSafeInteger(n) || n<1) throw Error('Invalid call count');
    if (n>this.available) {
      const needed=n-this.available, remaining=this.state.ceiling-this.state.reserved;
      if (needed>remaining) return false;
      const size=Math.min(remaining,Math.max(this.chunk,needed));
      // If persistence fails, forfeit this reservation and never dispatch it.
      this.state.reserved+=size;
      durableJson(this.file,this.state);
      this.available+=size;
    }
    this.available-=n; this.state.observed+=n;
    return true;
  }
  save() { durableJson(this.file,this.state); }
  report() { return {...this.state, remainingCalls:this.available+this.state.ceiling-this.state.reserved,
    estimatedUsageUsd:this.state.observed*0.000006, conservativeReservedUsd:this.state.reserved*0.000006,
    ceilingUsd:this.state.ceiling*0.000006}; }
}
export function validateBatch(chain, payload) {
  if (!Object.hasOwn(NETWORKS,chain)) throw Error('Network not allowed');
  const calls=Array.isArray(payload)?payload:[payload];
  if (!calls.length || calls.length>100) throw Error('Batch size not allowed');
  const allowed=chain==='solana-devnet'?SOL:EVM;
  for(const call of calls) {
    const validId=call?.id===null || (typeof call?.id==='string' && call.id.length<=128) || Number.isSafeInteger(call?.id);
    if(!call || call.jsonrpc!=='2.0' || !validId || !allowed.has(call.method) || !Array.isArray(call.params)) throw Error('Method or request not allowed');
  }
  return calls;
}
function loopbackRequest(req) {
  if (!['127.0.0.1','::1','::ffff:127.0.0.1'].includes(req.socket.remoteAddress)) return false;
  try {
    const host=new URL(`http://${req.headers.host}`);
    return ['127.0.0.1','[::1]','localhost'].includes(host.hostname) && !host.username && !host.password && host.pathname==='/' && !host.search && !host.hash;
  } catch { return false; }
}
function abortable(promise, signal) {
  return new Promise((resolve,reject)=>{
    const abort=()=>reject(Error('Aborted'));
    signal.addEventListener('abort',abort,{once:true});
    if(signal.aborted)abort();
    Promise.resolve(promise).then(resolve,reject).finally(()=>signal.removeEventListener('abort',abort));
  });
}
async function limitedJson(response, limit, signal) {
  if(!response.body)throw Error('Missing response');
  const reader=response.body.getReader();const chunks=[];let size=0;
  const abort=()=>reader.cancel().catch(()=>{});
  signal.addEventListener('abort',abort,{once:true});
  try {
    for(;;){const {done,value}=await reader.read();if(done)break;size+=value.byteLength;
      if(size>limit)throw Error('Response too large');chunks.push(Buffer.from(value));}
    return JSON.parse(Buffer.concat(chunks).toString());
  } finally { signal.removeEventListener('abort',abort);await reader.cancel().catch(()=>{}); }
}
function redact(value,key) {
  if(typeof value==='string')return key?value.split(key).join('[REDACTED]'):value;
  if(Array.isArray(value))return value.map(v=>redact(v,key));
  if(value && typeof value==='object')return Object.fromEntries(Object.entries(value).map(([k,v])=>[redact(k,key),redact(v,key)]));
  return value;
}
export function createGateway({budget,key,upstream=async(network,payload,{signal})=>fetch(`https://lb.drpc.live/${network}`,{
  method:'POST',headers:{'Content-Type':'application/json','Drpc-Key':key},body:JSON.stringify(payload),
  redirect:'error',signal}),maxInflight=256,timeoutMs=15000,maxResponseBytes=8_388_608}) {
  if(!Number.isSafeInteger(maxInflight)||maxInflight<1||maxInflight>1024||!Number.isSafeInteger(timeoutMs)||timeoutMs<1||!Number.isSafeInteger(maxResponseBytes)||maxResponseBytes<1)throw Error('Invalid gateway limits');
  let inflight=0,accepting=true,closePromise;
  const active=new Map();
  const stats={startedAt:new Date().toISOString(),forwarded:0,rejected:0,transportErrors:0,persistenceErrors:0,methods:{}};
  function reply(res,status,body,origin='local') {
    if(res.destroyed||res.writableEnded)return;
    res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store','X-Engine-RPC-Gateway':'1','X-Engine-RPC-Error-Origin':origin,...(!accepting?{Connection:'close'}:{})});res.end(JSON.stringify(body));
  }
  const server=http.createServer(async(req,res)=>{
    if(!loopbackRequest(req)||req.headers.origin){stats.rejected++;reply(res,403,{error:'Local requests only'});return;}
    if(req.method==='GET' && req.url==='/metrics'){reply(res,200,{...stats,inflight,accepting,budget:budget.report()});return;}
    if(!accepting){stats.rejected++;reply(res,503,{error:'Local gateway unavailable'});return;}
    if(req.method!=='POST'){reply(res,405,{error:'POST required'});return;}
    let payload,calls,chain;
    try {
      chain=req.url.slice(1); let size=0; const buffers=[];
      for await(const b of req){size+=b.length;if(size>1_048_576)throw Error('Request too large');buffers.push(b);}
      payload=JSON.parse(Buffer.concat(buffers).toString()); calls=validateBatch(chain,payload);
    } catch { stats.rejected++;reply(res,400,{error:'Invalid network, method or payload'});return; }
    // Shutdown or another body reader may have progressed while this body arrived.
    if(!accepting||inflight>=maxInflight){stats.rejected++;reply(res,503,{error:'Local concurrency limit or shutdown'});return;}
    try {
      if(!budget.take(calls.length)){stats.rejected++;reply(res,402,{error:'Local campaign RPC budget exhausted'});return;}
    } catch { accepting=false;stats.persistenceErrors++;reply(res,503,{error:'Local budget persistence unavailable'});return; }
    inflight++;stats.forwarded+=calls.length; const start=performance.now(); let status=0,errors=0;
    const controller=new AbortController();let complete;
    const done=new Promise(resolve=>{complete=resolve;});active.set(controller,{done,complete});
    const timer=setTimeout(()=>controller.abort(),timeoutMs);
    try {
      const response=await abortable(upstream(NETWORKS[chain],payload,{signal:controller.signal}),controller.signal);
      status=response.status;
      const parsed=redact(await abortable(limitedJson(response,maxResponseBytes,controller.signal),controller.signal),key);
      errors=(Array.isArray(parsed)?parsed:[parsed]).filter(v=>v?.error).length;
      reply(res,status,parsed,'upstream');
    } catch { stats.transportErrors++;errors=calls.length;reply(res,502,{error:'Upstream transport or protocol failed'},'transport'); }
    finally {
      clearTimeout(timer);controller.abort();
      const ms=performance.now()-start;
      for(const call of calls){const name=`${chain}:${call.method}`;
        const m=stats.methods[name]??={count:0,httpErrors:0,rpcErrors:0,totalMs:0,maxMs:0};
        m.count++;m.totalMs+=ms;m.maxMs=Math.max(m.maxMs,ms);if(status!==200)m.httpErrors++;
        if(errors)m.rpcErrors++;
      }
      inflight--;active.delete(controller);complete();
    }
  });
  server.requestTimeout=20_000;server.headersTimeout=10_000;
  function close({graceMs=20_000}={}) {
    if(closePromise)return closePromise;
    accepting=false;
    closePromise=new Promise((resolve,reject)=>{
      const timer=setTimeout(()=>{for(const controller of active.keys())controller.abort();server.closeAllConnections();},graceMs);
      // listening becomes false immediately; the callback is the drain barrier.
      server.close(error=>{clearTimeout(timer);
        Promise.all([...active.values()].map(job=>job.done)).then(()=>{
          if(error && error.code!=='ERR_SERVER_NOT_RUNNING')reject(error);else resolve();
        });
      });
      server.closeIdleConnections();
    });
    return closePromise;
  }
  return {server,stats,close};
}
async function main(){
  const dir=process.env.ENGINE_TEST_STATE_DIR??path.join(os.homedir(),'.config/engine-core');
  fs.mkdirSync(dir,{recursive:true,mode:0o700});
  const lock=path.join(dir,'rpc-budget.lock');let fd;
  try{fd=fs.openSync(lock,'wx',0o600);}catch{throw Error('Gateway lock exists; inspect its PID before removing it');}
  fs.writeFileSync(fd,String(process.pid));fs.closeSync(fd);
  let timer,gw,budget,stop;
  const stopped=new Promise(resolve=>{stop=resolve;});
  process.once('SIGTERM',stop);process.once('SIGINT',stop);
  try{
    budget=new Budget(path.join(dir,'rpc-budget.json'),Number(process.env.ENGINE_RPC_MAX_CALLS??MAX_CAMPAIGN_CALLS));
    const key=fs.readFileSync(path.join(dir,'drpc-key'),'utf8').trim();if(!key)throw Error('Missing key');
    gw=createGateway({budget,key,maxInflight:Number(process.env.ENGINE_RPC_MAX_INFLIGHT??256)});
    await new Promise((resolve,reject)=>{gw.server.once('error',reject);gw.server.listen(Number(process.env.ENGINE_RPC_GATEWAY_PORT??8788),'127.0.0.1',resolve);});
    timer=setInterval(()=>{try {budget.save();durableJson(path.join(dir,'rpc-metrics.json'),{...gw.stats,budget:budget.report()});}
      catch {stop();}},1000);
    console.log(JSON.stringify({listening:gw.server.address(),budget:budget.report(),networks:NETWORKS}));
    await stopped;
    clearInterval(timer);
    await gw.close();
    budget.save();
    durableJson(path.join(dir,'rpc-metrics.json'),{...gw.stats,budget:budget.report()});
  }finally{
    clearInterval(timer);process.removeListener('SIGTERM',stop);process.removeListener('SIGINT',stop);
    // Drain before saving the final observed count or releasing the process lock.
    try {await gw?.close();budget?.save();} finally {fs.unlinkSync(lock);}
  }
}
if(process.argv[1] && import.meta.url===pathToFileURL(process.argv[1]).href)main().catch(()=>{console.error('RPC gateway startup/shutdown failed; local configuration details withheld.');process.exitCode=1;});
