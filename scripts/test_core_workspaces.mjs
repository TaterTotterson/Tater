import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import ts from '../frontend/node_modules/typescript/lib/typescript.js';
const metadata=JSON.parse(await readFile(new URL('../frontend/src/cores/workspaces.json',import.meta.url),'utf8'));
let source=await readFile(new URL('../frontend/src/cores/workspace.ts',import.meta.url),'utf8');
source=source.replace("import defaults from './workspaces.json';",`const defaults=${JSON.stringify(metadata)};`);
const compiled=ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.ESNext,target:ts.ScriptTarget.ES2022}}).outputText;
const {coreWorkspace,workspaceTabs,initialWorkspaceTab,fieldGroups,itemFields,itemImage,acknowledgeDraft}=await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`);
test('all twelve cores have layouts with backward compatible defaults',()=>{
 assert.equal(Object.keys(metadata).length,12);
 for(const [key,row] of Object.entries(metadata)){assert.equal(row.schema,1);assert.equal(coreWorkspace(key).description,row.description);assert.equal(coreWorkspace(key,{schema:99,description:'invalid'}).description,row.description);}
 assert.equal(coreWorkspace('community_core').icon,'cores');assert.equal(coreWorkspace('community_core',{schema:1,description:'Custom'}).description,'Custom');
});
test('older installed metadata keeps new screen defaults and custom screens remain authoritative',()=>{
 const old={schema:1,views:{items:'list'},description:'Installed task core'};
 const merged=coreWorkspace('ai_task_core',old);
 assert.equal(merged.screens.items.layout,'tasks');
 assert.equal(merged.description,old.description);
 assert.equal(coreWorkspace('ai_task_core',{...old,screens:{items:{layout:'custom'}}}).screens.items.layout,'custom');
 assert.deepEqual(old,{schema:1,views:{items:'list'},description:'Installed task core'});
});
test('tab ordering and labels preserve original contracts and future tabs',()=>{
 const tabs=[{key:'settings',source:'items',action:'save'},{key:'future',source:'custom'},{key:'library',groups:[{key:'albums'}]}];const before=structuredClone(tabs);
 const result=workspaceTabs(tabs,coreWorkspace('music_core'));
 assert.deepEqual(result.map(t=>t.key),['library','settings','future']);assert.equal(result[0].label,'Library');assert.equal(result[1].action,'save');assert.deepEqual(tabs,before);
 assert.equal(initialWorkspaceTab(result,coreWorkspace('music_core'),'settings'),'settings');assert.equal(initialWorkspaceTab([{key:'listen'},...result],coreWorkspace('music_core'),'settings'),'listen');assert.equal(initialWorkspaceTab(result,{default_tab:'missing'},'future'),'future');
});
test('grouped forms retain every field exactly once, including unknown future options',()=>{
 for(const row of Object.values(metadata)){
  const fields=[{key:'name',type:'text'},{key:'schedule',type:'select'},{key:'destination',dependent_options:{source_key:'schedule'}},{key:'broadcast_audio_volume',type:'range'},{key:'brand_new',type:'checkbox'}];
  const section={label:'Specific options',fields:[{key:'future_section',type:'file'}]};const before=structuredClone(fields);
  const groups=fieldGroups(fields,[section],row);const all=groups.flatMap(g=>g.fields);assert.equal(all.length,6);for(const field of [...fields,...section.fields])assert.equal(all.filter(f=>f===field).length,1);assert.deepEqual(fields,before);
 }
});
test('core-authored section headings take priority and all popup controls remain enumerable',()=>{
 const fields=[{key:'before'},{type:'heading',label:'Trigger'},{key:'trigger'},{type:'section_heading',label:'Action'},{key:'run',action:'preview'}];const groups=fieldGroups(fields,[],coreWorkspace('automation_core'));assert.deepEqual(groups.map(g=>g.label),['Details','Trigger','Action']);assert.deepEqual(groups.flatMap(g=>g.fields),fields);
 const item={fields,sections:[{fields:[{key:'extra'}]}],popup_fields:[{key:'popup'}]};assert.equal(itemFields(item).length,7);assert.equal(itemImage({fields:[{type:'image',url:'/snapshot'}]}),'/snapshot');
});

test('broadcast controls stay together in Audio, including their enable switch',()=>{const fields=['broadcast_audio_enabled','background_audio_source','tts_start_delay_seconds','ducking_target_percent'].map(key=>({key}));const groups=fieldGroups(fields,[],coreWorkspace('ai_task_core'));assert.equal(groups.length,1);assert.equal(groups[0].label,'Audio');});

test('saving only clears submitted values and preserves newer edits',()=>{const values={title:'Submitted title',prompt:'New edit',targets:['kitchen']};const dirty=new Set(['title','prompt','targets']);acknowledgeDraft(values,dirty,{title:'Submitted title',prompt:'Old edit',targets:['kitchen']});assert.deepEqual([...dirty],['prompt']);assert.equal(values.prompt,'New edit');});
