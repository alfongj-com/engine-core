import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import {Budget,createGateway} from './budget-gateway.mjs';
function fixture(){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'rpc-budget-test-'));return{dir,file:path.join(dir,'budget.json')};}
test('crash/restart forfeits reserved calls instead of resetting the cap',()=>{
 const {dir,file}=fixture();try{
  const first=new Budget(file,5,3);assert.equal(first.take(2),true);
  const afterCrash=new Budget(file,5,3);assert.equal(afterCrash.take(3),false);
  assert.equal(afterCrash.take(2),true);assert.equal(afterCrash.take(1),false);
  assert.throws(()=>new Budget(file,6,3),/mismatch/);
 }finally{fs.rmSync(dir,{recursive:true,force:true});}
});
test('counts individual batch calls, rejects over-budget before upstream, redacts echoed key',async()=>{
 const {dir,file}=fixture();let received=0;
 const {server}=createGateway({budget:new Budget(file,3,1),key:'secret-for-test',upstream:async(_network,payload)=>{
  received+=Array.isArray(payload)?payload.length:1;
  return new Response(JSON.stringify({jsonrpc:'2.0',id:1,error:{message:'secret-for-test'}}));
 }});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 const url=`http://127.0.0.1:${server.address().port}`;
 const call={jsonrpc:'2.0',id:1,method:'eth_blockNumber',params:[]};
 const post=(body,route='11155111',headers={})=>fetch(`${url}/${route}`,{method:'POST',headers,body:JSON.stringify(body)});
 try{
  const first=await post([call,call]);assert.equal(first.status,200);assert.ok(!(await first.text()).includes('secret-for-test'));
  assert.equal((await post([call,call])).status,402);assert.equal(received,2);
  assert.equal((await post(call,'1')).status,400);
  assert.equal((await post({...call,method:'debug_traceBlockByNumber'})).status,400);
  assert.equal((await post(call,'11155111',{Origin:'https://untrusted.example'})).status,403);
  assert.equal((await post(call)).status,200);assert.equal(received,3);
 }finally{server.closeAllConnections();await new Promise(r=>server.close(r));fs.rmSync(dir,{recursive:true,force:true});}
});

test('corrupt durable counters and a ceiling above $12 fail closed',()=>{
 const {dir,file}=fixture();try{
  assert.throws(()=>new Budget(file,2_000_001),/Invalid budget/);
  fs.writeFileSync(file,JSON.stringify({version:1,ceiling:5,reserved:3,observed:-1}));
  assert.throws(()=>new Budget(file,5),/mismatch/);
  fs.writeFileSync(file,JSON.stringify({version:1,ceiling:5,reserved:3,observed:4}));
  assert.throws(()=>new Budget(file,5),/mismatch/);
 }finally{fs.rmSync(dir,{recursive:true,force:true});}
});
async function serve(options){
 const gw=createGateway(options);await new Promise(r=>gw.server.listen(0,'127.0.0.1',r));
 return {...gw,url:`http://127.0.0.1:${gw.server.address().port}`};
}
const call={jsonrpc:'2.0',id:1,method:'eth_blockNumber',params:[]};
const post=gw=>fetch(`${gw.url}/11155111`,{method:'POST',body:JSON.stringify(call)});
test('durability failure never dispatches and latches further traffic closed',async()=>{
 const {dir,file}=fixture();let sent=0;const budget=new Budget(file,5,1);
 const gw=await serve({budget,key:'private',upstream:async()=>{sent++;return new Response('{}');}});
 try{
  budget.file=path.join(dir,'missing-directory','state');
  assert.equal((await post(gw)).status,503);
  budget.file=file;
  assert.equal((await post(gw)).status,503);
  assert.equal(sent,0);assert.equal(gw.stats.persistenceErrors,1);
 }finally{await gw.close();fs.rmSync(dir,{recursive:true,force:true});}
});
test('shutdown drains already admitted work before returning final counters',async()=>{
 const {dir,file}=fixture();let entered,release;
 const started=new Promise(r=>entered=r),pending=new Promise(r=>release=r);
 const gw=await serve({budget:new Budget(file,5,1),key:'private',upstream:async()=>{entered();await pending;return new Response(JSON.stringify({id:1,result:'0x1'}));}});
 try{
  const request=post(gw);await started;
  let closed=false;const closing=gw.close().then(()=>{closed=true;});
  await new Promise(r=>setImmediate(r));assert.equal(closed,false);
  release();assert.equal((await request).status,200);await closing;
  assert.equal(gw.stats.methods['11155111:eth_blockNumber'].count,1);
  assert.equal(gw.stats.transportErrors,0);
 }finally{release();await gw.close();fs.rmSync(dir,{recursive:true,force:true});}
});
test('forced shutdown aborts hung upstream and still finalizes counters',async()=>{
 const {dir,file}=fixture();let entered;const started=new Promise(r=>entered=r);
 const gw=await serve({budget:new Budget(file,5,1),key:'private',upstream:async()=>{entered();return new Promise(()=>{});}});
 try{
  const request=post(gw).catch(()=>null);await started;
  await Promise.race([gw.close({graceMs:10}),new Promise((_,reject)=>setTimeout(()=>reject(Error('shutdown hung')),1000).unref())]);
  await request;
  assert.equal(gw.stats.transportErrors,1);assert.equal(gw.stats.methods['11155111:eth_blockNumber'].count,1);
 }finally{await gw.close();fs.rmSync(dir,{recursive:true,force:true});}
});
test('escaped echoed keys, oversized responses, and non-loopback Host are contained',async()=>{
 const {dir,file}=fixture();let requests=0;
 const gw=await serve({budget:new Budget(file,5,1),key:'private',maxResponseBytes:100,upstream:async()=>{
  requests++;return new Response(requests===1?'{"error":{"message":"pri\\u0076ate"}}':'x'.repeat(101));
 }});
 try{
  const invalid=await new Promise((resolve,reject)=>{const req=http.request(`${gw.url}/11155111`,{method:'POST',headers:{Host:'attacker.example'}},res=>{res.resume();res.on('end',()=>resolve(res.statusCode));});req.on('error',reject);req.end(JSON.stringify(call));});
  assert.equal(invalid,403);assert.equal(requests,0);
  const response=await post(gw);assert.ok(!(await response.text()).includes('private'));
  const huge=await post(gw);assert.equal(huge.status,502);assert.equal(huge.headers.get('x-engine-rpc-error-origin'),'transport');
 }finally{await gw.close();fs.rmSync(dir,{recursive:true,force:true});}
});
test('concurrent admissions cannot exceed the durable ceiling',async()=>{
 const {dir,file}=fixture();let sent=0;
 const gw=await serve({budget:new Budget(file,7,3),key:'private',upstream:async()=>{sent++;return new Response('{"result":"0x1"}');}});
 try{
  const responses=await Promise.all(Array.from({length:30},()=>post(gw)));
  assert.equal(responses.filter(r=>r.status===200).length,7);assert.equal(sent,7);
  const persisted=JSON.parse(fs.readFileSync(file,'utf8'));assert.equal(persisted.reserved,7);
  assert.equal(new Budget(file,7).take(1),false);
 }finally{await gw.close();fs.rmSync(dir,{recursive:true,force:true});}
});
