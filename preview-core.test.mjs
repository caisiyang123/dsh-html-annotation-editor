import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
function bridgeFixture(){
 const handlers={document:{},window:{}};let overlay;
 class Element{constructor(id,x){Object.assign(this,{id,localName:'section',nodeType:1,isConnected:true,style:{},dataset:{},innerText:id,textContent:id,outerHTML:'<section id="'+id+'">',x})}getBoundingClientRect(){return {x:this.x,y:40,left:this.x,top:40,width:120,height:60}}}
 const a=new Element('first',10),b=new Element('second',300);
 const parent={postMessage:()=>{}};
 let hover;Element.prototype.cloneNode=function(){hover=new Element('hover',0);return hover};
 const doc={createElement:tag=>{const el=new Element('overlay',0);if(tag==='div')overlay=el;return el},documentElement:{append:()=>{},setAttribute(){},removeAttribute(){}},querySelectorAll:()=>[],querySelector:s=>s==='#first'?a:b,addEventListener:(t,f)=>handlers.document[t]=f};
 vm.runInNewContext(selectionBridge('test'),{document:doc,parent,Element,CSS:{escape:s=>s},innerWidth:1000,innerHeight:800,addEventListener:(t,f)=>handlers.window[t]=f});
 const move=target=>handlers.window.pointermove({target});
 const click=target=>handlers.document.click({target,preventDefault(){},stopImmediatePropagation(){}});
 const mode=picking=>handlers.window.message({source:parent,data:{channel:'test',type:'mode',picking}});
 return {a,b,overlay,hover,move,click,mode,handlers,parent};
}
import {createEditPrompt,selectionBridge} from './preview-core.js';
test('prompt preserves immutable file target, comments and selection data',()=>{const text=createEditPrompt({path:'材料/报告.html',version:'v2',notes:[{comment:'标题改成季度经营复盘',selection:{selector:'#title',text:'旧标题'}},{comment:'调整整体留白'}]});assert.match(text,/材料\/报告.html/);assert.match(text,/标题改成季度经营复盘/);assert.match(text,/#title/);assert.match(text,/整份文档/);assert.match(text,/最新内容/);assert.match(text,/定位数据/)});
test('selected highlight stays attached when pointer moves to a different element',()=>{const f=bridgeFixture();f.move(f.a);f.click(f.a);f.move(f.b);assert.equal(f.overlay.style.left,'10px');f.click(f.b);assert.equal(f.overlay.style.left,'300px')});
test('hover preview remains available while an existing selection stays fixed',()=>{const f=bridgeFixture();f.click(f.a);f.move(f.b);assert.equal(f.overlay.style.left,'10px');assert.equal(f.hover.style.left,'300px');assert.equal(f.hover.style.display,'block');f.mode(false);assert.equal(f.hover.style.display,'none')});
test('selected highlight follows its element on scroll and stays hidden in browse mode',()=>{const f=bridgeFixture();f.click(f.a);f.a.x=45;f.handlers.window.scroll();assert.equal(f.overlay.style.left,'45px');f.mode(false);f.handlers.window.scroll();f.handlers.window.resize();assert.equal(f.overlay.style.display,'none');f.mode(true);assert.equal(f.overlay.style.left,'45px');assert.equal(f.overlay.style.display,'block')});
test('Escape releases the selection lock so a new hover can be shown',()=>{const f=bridgeFixture();f.click(f.a);f.handlers.document.keydown({key:'Escape'});f.move(f.b);assert.equal(f.overlay.style.left,'300px')});
test('browse mode leaves page form validation events untouched; selection mode intercepts them',()=>{const f=bridgeFixture();let prevented=0,stopped=0;const event={preventDefault(){prevented++},stopImmediatePropagation(){stopped++}};f.mode(false);f.handlers.document.submit(event);assert.equal(prevented,0);assert.equal(stopped,0);f.mode(true);f.handlers.document.submit(event);assert.equal(prevented,1);assert.equal(stopped,1)});
test('selection bridge compiles and uses channel plus sandbox parent messaging',()=>{const source=selectionBridge('test-channel');assert.doesNotThrow(()=>new Function(source));assert.match(source,/e.source!==parent/);assert.match(source,/stopImmediatePropagation/);assert.match(source,/slice\(0,1500\)/)});
