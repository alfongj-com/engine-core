import {test} from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {performance} from 'node:perf_hooks';
import {setTimeout as sleep} from 'node:timers/promises';
import {Budget,createGateway} from './budget-gateway.mjs';
import {configuration,gatewayUrl,parseArgs,runProbe,runStage} from './load-probe.mjs';

const basicMix=[{method:'eth_blockNumber',params:[]},{method:'eth_getTransactionCount',params:['0x'+'1'.repeat(40),'latest']}];
const rpc=(payload,result)=>new Response(JSON.stringify({jsonrpc:'2.0',id:payload.id,result}));
async function fixture(upstream,{ceiling=200,maxInflight=256}={}){
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'engine-rpc-probe-'));
  const budget=new Budget(path.join(dir,'budget.json'),ceiling,1);
  const gateway=createGateway({budget,key:'provider-test-secret',upstream,maxInflight});
  await new Promise(resolve=>gateway.server.listen(0,'127.0.0.1',resolve));
  // HTTP classification fixtures must tolerate concurrent test-process fsyncs.
  // The production lag cutoff is tested separately with a controlled clock.
  return {budget,config:{chain:'11155111',gateway:`http://127.0.0.1:${gateway.server.address().port}`,reserve:0,pauseMs:0,maxLagMs:10_000,stages:[{rate:100,count:8}]},
    close:async()=>{await gateway.close();fs.rmSync(dir,{recursive:true,force:true});}};
}
test('only literal loopback routes, finite stages, explicit execution, and preserved evidence filenames',()=>{
  for(const url of ['https://127.0.0.1:8788','http://localhost:8788','http://example.com:8788','http://127.0.0.1:8788/x','http://key@127.0.0.1:8788','http://127.0.0.1:8788?secret=1'])assert.throws(()=>gatewayUrl(url));
  assert.equal(gatewayUrl('http://[::1]:8788'),'http://[::1]:8788');
  const dry=parseArgs(['--chain','11155111']);assert.equal(dry.execute,false);assert.equal(dry.config.planned,8800);
  assert.throws(()=>parseArgs(['--chain','11155111','--execute']),/output/);
  assert.throws(()=>configuration({chain:'11155111',stages:[{rate:1000,count:100001}]}));
  assert.throws(()=>parseArgs(['--chain','11155111','--rates','100,200','--count','5']));
});
test('deterministic bounded request mix, no retries, and saved report excludes raw provider text',async()=>{
  const received=[];
  const f=await fixture(async(_network,payload)=>{
    assert.equal(Array.isArray(payload),false,'each HTTP request contains one RPC call');
    received.push({id:payload.id,method:payload.method});
    if(received.length===3)return new Response(JSON.stringify({jsonrpc:'2.0',id:payload.id,error:{code:-32005,message:'provider-test-secret unrelated-private-text'}}));
    if(received.length===5)return new Response(JSON.stringify({jsonrpc:'2.0',id:payload.id,error:{code:-32000,message:'unrelated-private-text'}}),{status:429});
    return rpc(payload,'0x1');
  });
  try{
    const report=await runProbe(f.config,{mix:basicMix});
    assert.deepEqual(received.map(call=>call.id).toSorted((a,b)=>a-b),[1,2,3,4,5,6,7,8]);
    assert.deepEqual(received.toSorted((a,b)=>a.id-b.id).map(call=>call.method),Array.from({length:8},(_,i)=>basicMix[i%2].method));
    assert.equal(report.observedCampaignCallDelta,8);
    assert.deepEqual(report.stages[0].outcomes,{success:6,rpc_error:1,upstream_http_error:1});
    assert.equal(report.stages[0].httpStatuses['429'],1);
    assert.equal(report.stages[0].rpcCodes['-32005'],1);
    assert.equal(report.stages[0].localConcurrencyDrops,0);
    assert.equal(report.stages[0].localSchedulerDrops,0);
    assert.equal(report.stages[0].methodResults.eth_blockNumber.latencyMs.count,4);
    assert.ok(!JSON.stringify(report).includes('private-text'));
    assert.ok(!JSON.stringify(report).includes('provider-test-secret'));
    assert.ok(!JSON.stringify(report).includes(f.config.gateway));
  }finally{await f.close();}
});
test('scheduler drops overdue slots without catch-up requests or retries',async(t)=>{
  let now=0;
  t.mock.method(performance,'now',()=>now);
  const config=configuration({chain:'11155111',maxLagMs:5,stages:[{rate:1000,count:8}]}),received=[];
  const stage=await runStage(config,config.stages[0],basicMix,async(call)=>{
    received.push(call.method);
    // Simulate a synchronous stall after slot zero. All later deadlines have
    // elapsed; no real timer or host scheduling speed determines the outcome.
    now=100;
    return {kind:'success',status:200,ms:100};
  });
  assert.deepEqual(received,[basicMix[0].method]);
  assert.equal(stage.dispatched,1);
  assert.equal(stage.localSchedulerDrops,7);
  assert.equal(stage.localConcurrencyDrops,0);
  assert.equal(stage.notScheduled,0);
  assert.deepEqual(stage.outcomes,{success:1});
  assert.equal(stage.dispatched+stage.localSchedulerDrops,stage.scheduled);
  assert.match(stage.interpretation,/Local scheduling/);
});
test('local caller concurrency saturation drops scheduled work instead of unbounded queuing or retry',async()=>{
  let active=0,max=0,received=0;
  const f=await fixture(async(_network,payload)=>{active++;received++;max=Math.max(max,active);await sleep(70);active--;return rpc(payload,'0x1');});
  try{
    const report=await runProbe({...f.config,concurrency:1,stages:[{rate:500,count:30}]},{mix:basicMix});
    const stage=report.stages[0];assert.ok(stage.localConcurrencyDrops>0);assert.equal(max,1);
    assert.equal(received,stage.dispatched);assert.equal(stage.dispatched+stage.localConcurrencyDrops+stage.localSchedulerDrops,30);
    assert.match(stage.interpretation,/Local scheduling/);
  }finally{await f.close();}
});
test('gateway local admission rejection is distinct from upstream throttling',async()=>{
  const f=await fixture(async(_network,payload)=>{await sleep(50);return rpc(payload,'0x1');},{maxInflight:1});
  try{
    const report=await runProbe({...f.config,concurrency:8,stages:[{rate:200,count:10}]},{mix:basicMix});
    assert.ok(report.stages[0].outcomes.local_http_error>0);
    assert.equal(report.stages[0].outcomes.upstream_http_error,undefined);
    assert.match(report.stages[0].interpretation,/Local scheduling/);
  }finally{await f.close();}
});
test('budget precheck and write-method rejection make no paid dispatches',async()=>{
  let received=0;const f=await fixture(async(_network,payload)=>{received++;return rpc(payload,'0x1');},{ceiling:5});
  try{
    await assert.rejects(runProbe(f.config,{mix:basicMix}),/Insufficient/);
    await assert.rejects(runProbe({...f.config,stages:[{rate:1,count:1}]},{mix:[{method:'eth_sendRawTransaction',params:['0x1']}]}),/read-only/);
    assert.equal(received,0);
  }finally{await f.close();}
});
test('an exhausted shared campaign stops later slots and later stages without retry',async()=>{
  let received=0;let f;
  f=await fixture(async(_network,payload)=>{received++;if(received===1)assert.equal(f.budget.take(19),true);return rpc(payload,'0x1');},{ceiling:20});
  try{
    const report=await runProbe({...f.config,concurrency:1,stages:[{rate:100,count:5},{rate:100,count:5}]},{mix:basicMix});
    assert.equal(received,1);assert.equal(report.stages.length,1);
    assert.equal(report.stages[0].stoppedReason,'budget_exhausted');
    assert.equal(report.stages[0].outcomes.budget_exhausted,1);
  }finally{await f.close();}
});
test('EVM recipe discovers real recent hashes and funded senders with bounded reads',async()=>{
  const sender='0x'+'1'.repeat(40),hash='0x'+'2'.repeat(64),seen=[];
  const f=await fixture(async(_network,payload)=>{
    seen.push(payload);
    const result=({eth_getBlockByNumber:{number:'0xa',transactions:[{from:sender,hash}]},eth_getTransactionReceipt:{transactionHash:hash},eth_getBalance:'0x38d7ea4c68000',eth_feeHistory:{baseFeePerGas:['0x1']},eth_estimateGas:'0x5208',eth_getTransactionCount:'0x1'})[payload.method];
    return rpc(payload,result);
  });
  try{
    const report=await runProbe(f.config);
    assert.equal(report.observedCampaignCallDelta,11);
    assert.equal(report.discovery.transactionSampleSize,1);
    assert.match(report.discovery.estimateScenario,/funded sender/);
    const estimate=seen.find(call=>call.method==='eth_estimateGas');assert.equal(estimate.params[0].from,sender);assert.equal(estimate.params[0].value,'0x0');
    assert.equal(estimate.params[0].gasPrice,undefined);
    assert.ok(!JSON.stringify(report).includes(sender));assert.ok(!JSON.stringify(report).includes(hash));
  }finally{await f.close();}
});
test('Solana recipe probes known recent signatures and distinguishes null transaction results',async()=>{
  const signature='3'.repeat(88),seen=[];
  const f=await fixture(async(_network,payload)=>{
    seen.push(payload);
    const result=({getSignaturesForAddress:[{signature}],getTransaction:seen.length===2?{slot:1}:null,getLatestBlockhash:{value:{blockhash:'block'}},getRecentPrioritizationFees:[],getSignatureStatuses:{value:[{slot:1,err:null}]}})[payload.method];
    return rpc(payload,result);
  });
  try{
    const report=await runProbe({...f.config,chain:'solana-devnet'});
    assert.equal(report.observedCampaignCallDelta,10);assert.equal(report.discovery.signatureSampleSize,1);
    assert.equal(report.stages[0].outcomes.success_null,2);
    assert.equal(seen.find(call=>call.method==='getSignatureStatuses').params[0][0],signature);
    assert.ok(!JSON.stringify(report).includes(signature));
  }finally{await f.close();}
});

test('malformed error envelopes never count as successful RPC responses',async()=>{
  const malformed=[null,false,0,'bad error',[],{code:'-32000',message:'bad type'},{code:-32000}];
  const f=await fixture(async(_network,payload)=>new Response(JSON.stringify({jsonrpc:'2.0',id:payload.id,error:malformed[payload.id-1]})));
  try{
    const report=await runProbe({...f.config,stages:[{rate:100,count:malformed.length}]},{mix:basicMix});
    assert.deepEqual(report.stages[0].outcomes,{invalid_rpc_response:malformed.length});
    assert.equal(report.stages[0].successLatencyMs.count,0);
    assert.equal(report.stages[0].successfulResponseRpsIncludingDrain,0);
    assert.equal(report.observedCampaignCallDelta,malformed.length);
  }finally{await f.close();}
});
