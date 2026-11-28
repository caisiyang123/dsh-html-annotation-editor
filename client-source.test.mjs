import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';

const dir=path.dirname(fileURLToPath(import.meta.url));
const core=fs.readFileSync(path.join(dir,'queue-core.js'),'utf8').replace(/^export default .*;\s*$/mg,'').replace(/^export /mg,'');
const layout=fs.readFileSync(path.join(dir,'layout-source.js'),'utf8');
const client=fs.readFileSync(path.join(dir,'client-source.js'),'utf8');

/** Boots the client half against a host stub. `retain` reproduces the real contract:
 *  it throws synchronously for a session the host does not know. */
function boot({jobs,known=[],listPhase='ready',retainThrows=true}={}){
 const store=new Map([['dsh.html-annotation.queue.v1',JSON.stringify({version:1,jobs,sessions:{}})]].map(([k,v])=>[k,v]));
 const logs=[];let effectCleanup;
 const localStorage={getItem:k=>store.get(k)??null,setItem:(k,v)=>store.set(k,String(v)),removeItem:k=>store.delete(k)};
 const ctx={
  sessions:{
   list:{getSnapshot:()=>({phase:listPhase,byId:Object.fromEntries(known.map(id=>[id,{id}]))})},
   retain:(sid)=>{if(retainThrows&&!known.includes(sid))throw Error('sessions.retain: unknown session '+sid);return {ready:Promise.resolve({session:{subscribe:()=>()=>{},getSnapshot:()=>({openState:'open',running:false}),prompt:async()=>({ok:true}),projections:{faceOf:()=>({getSnapshot:()=>({}),subscribe:()=>()=>{}})}},eventSource:{getSnapshot:()=>({entries:[]}),subscribe:()=>()=>{}}}),release(){}}}},
  effect:fn=>{effectCleanup=fn()},
  slots:{inject:()=>{},register:()=>({})},
  layout:{panels:{setRightbar:()=>{}}},
  logger:{info:()=>{}},
 };
 const React={createElement:()=>({}),useState:v=>[typeof v==='function'?v():v,()=>{}],useEffect:()=>{},useRef:()=>({}),useMemo:(f)=>f()};
 const sandbox={window:{addEventListener(){},removeEventListener(){},__ModuleLoader__:{}},document:{head:{appendChild(){}},createElement:()=>({dataset:{},style:{}}),body:{},querySelector:()=>null,querySelectorAll:()=>[],documentElement:{toggleAttribute(){},removeAttribute(){}}},localStorage,navigator:{locks:{request:async(name,fn)=>fn()}},crypto:{randomUUID:()=>'0000'},React,require:n=>n==='react'?React:undefined,console:{error:(...args)=>logs.push(args.map(String).join(' ')),log(){}},setTimeout:(fn)=>0,clearTimeout(){},MutationObserver:class{observe(){}disconnect(){}},queueMicrotask:fn=>fn(),exports:{},innerWidth:1200};
 const script=core+'\n'+layout+'\n'+client;
 sandbox.window.localStorage=localStorage;
 vm.runInNewContext(script,sandbox,{filename:'client-source.js'});
 sandbox.exports.apply(ctx);
 return {exports:sandbox.exports,ctx,store,logs,cleanup:()=>effectCleanup?.(),state:()=>JSON.parse(store.get('dsh.html-annotation.queue.v1'))};
}

const job=(sessionId,status='queued')=>({id:'job-'+sessionId+'-'+status,requestId:'req-'+sessionId,sessionId,path:'a.html',prompt:'p',label:'l',status,createdAt:1,attempts:0,admitted:false});

test('plugin activation survives a host retain throw for an unknown session',()=>{
 const env=boot({jobs:[job('session-deleted')],listPhase:'pending'});
 assert.equal(typeof env.exports.apply,'function');
 assert.equal(env.logs.filter(m=>/unknown session/.test(m)).length,1);
 assert.equal(env.state().jobs[0].status,'queued','orphan policy is decided only after the catalog is ready');
});

test('unknown session with a ready catalog fails the stale job instead of retrying forever',async()=>{
 const env=boot({jobs:[job('session-deleted')]});
 for(let i=0;i<10;i++)await new Promise(r=>setTimeout(r,0));
 const state=env.state();
 assert.equal(state.jobs[0].status,'failed');
 assert.match(state.jobs[0].error,/原会话已不存在/);
});

test('a session missing only because the catalog is still loading is retried, not failed',async()=>{
 const env=boot({jobs:[job('session-pending')],listPhase:'pending'});await new Promise(r=>setImmediate(r));
 assert.equal(env.state().jobs[0].status,'queued','a pending catalog must not be mistaken for a deleted session');
});

test('known sessions still bind and a broken binding does not escape activation',async()=>{
 const env=boot({jobs:[job('session-known')],known:['session-known']});
 for(let i=0;i<10;i++)await new Promise(r=>setTimeout(r,0));
 assert.notEqual(env.state().jobs[0].status,'failed');
 assert.equal(env.logs.some(m=>/start:/.test(m)),false);
});

test('disposing while a session is still unbindable clears pending retry state without throwing',()=>{
 const env=boot({jobs:[job('session-gone')],listPhase:'pending'});
 assert.doesNotThrow(()=>env.cleanup());
});
