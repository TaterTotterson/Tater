import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import ts from '../frontend/node_modules/typescript/lib/typescript.js';
const source=await readFile(new URL('../frontend/src/spudex/workspace.ts',import.meta.url),'utf8');
const compiled=ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.ESNext,target:ts.ScriptTarget.ES2022}}).outputText;
const {mergeLogRows,taskFeed,sessionProjectId,isActive,taskMode}=await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`);
test('replayed live logs merge without duplicates or lost older history',()=>{
 const rows=Array.from({length:1500},(_,i)=>({seq:i+1,text:String(i)}));
 const merged=mergeLogRows(rows,[{seq:1500,text:'replayed'},{seq:1501,text:'latest'}]);
 assert.equal(merged.length,1501);assert.equal(merged[0].seq,1);assert.equal(merged.at(-2).text,'replayed');
 assert.deepEqual(mergeLogRows(merged,[{seq:12}],true),[{seq:12}]);
});
test('long conversations preserve chronology and group only adjacent activity',()=>{
 const rows=Array.from({length:50},(_,i)=>({seq:i+1,stream:i%2?'assistant':'user',text:String(i)}));
 rows.splice(2,0,{seq:2.1,stream:'command',text:'git status'},{seq:2.2,stream:'stdout',text:'clean'});
 const feed=taskFeed(rows);assert.equal(feed.filter(r=>r.kind==='message').length,50);
 assert.equal(feed[2].kind,'activity');assert.equal(feed[2].entries.length,2);assert.equal(feed[3].content,'2');
});
test('legacy folders select their nearest project without prefix collisions',()=>{
 const projects=[{id:'base',path:'/work'},{id:'nested',path:'/work/app'}];
 assert.equal(sessionProjectId({cwd:'/work/app/src'},projects),'nested');
 assert.equal(sessionProjectId({cwd:'/work/application'},projects),'base');
 assert.equal(sessionProjectId({cwd:'/worker'},projects),'');
 assert.equal(sessionProjectId({project:{id:'saved'},cwd:'/work'},projects),'saved');
});
test('stale running metadata stays resumable when runtime reports no active task',()=>{
 assert.equal(isActive({status:'running',active:false}),false);
 assert.equal(isActive({status:'running',active:true}),true);
 assert.equal(isActive({status:'queued'}),true);
 assert.equal(isActive(null),false);assert.equal(taskMode('old-mode'),'build');
});
