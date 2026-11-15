import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import ts from '../frontend/node_modules/typescript/lib/typescript.js';
const metadata=JSON.parse(await readFile(new URL('../frontend/src/cores/workspaces.json',import.meta.url),'utf8'));
const source=await readFile(new URL('../frontend/src/cores/presentation.ts',import.meta.url),'utf8');
const compiled=ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.ESNext,target:ts.ScriptTarget.ES2022}}).outputText;
const {workspaceScreen,previewFacts,fieldText,readoutFields,allFields,tableRows,itemCanEdit,fieldVisible}=await import(`data:text/javascript;base64,${Buffer.from(compiled).toString('base64')}`);

test('all twelve cores declare actual workspace screens, not only generic view names',()=>{
 assert.equal(Object.keys(metadata).length,12);
 for(const [key,row] of Object.entries(metadata)){
  assert.ok(Object.keys(row.screens).length,key);
  for(const [tab,screen] of Object.entries(row.screens)){
   assert.ok(screen.layout&&screen.label&&screen.icon,`${key}:${tab}`);
   assert.equal(workspaceScreen({key:tab},row).layout,screen.layout);
  }
 }
 assert.equal(workspaceScreen({key:'new-device'},metadata.reachy_core).layout,'behavior');
 assert.deepEqual(workspaceScreen({key:'future'},metadata.ai_task_core),{});
 assert.deepEqual(workspaceScreen({key:'anything'},{}),{});
});
test('preview facts use real selected values and never include credentials or private raw data',()=>{
 const item={summary_rows:[{label:'Status',value:'Live'}],fields:[
  {key:'destination',label:'Destination',value:'kitchen',options:[{value:'kitchen',label:'Kitchen'}]},
  {key:'enabled',label:'Enabled',value:false},
  {key:'password',label:'Password',value:'secret',type:'password'},
  {key:'api_token',label:'Token',value:'secret',type:'text'},
  {key:'raw_doc_json',label:'Raw',value:'private',type:'textarea'},
 ]};
 const before=structuredClone(item);
 const facts=previewFacts(item,{preview_fields:item.fields.map(field=>field.key)});
 assert.deepEqual(facts,[{label:'Status',value:'Live'},{label:'Destination',value:'Kitchen'},{label:'Enabled',value:'Disabled'}]);
 assert.deepEqual(item,before);
 assert.equal(fieldText({value:0}), '0');
 assert.equal(fieldText({value:['a','b'],options:[{label:'Group',options:[{value:'a',label:'Alpha'},{value:'b',label:'Beta'}]}]}),'Alpha, Beta');
});
test('preview/editor contract retains every field and action, including popup and future controls',()=>{
 const item={fields:[{key:'known',type:'textarea',read_only:true},{key:'future',type:'custom_widget'}],sections:[{fields:[{key:'section',type:'text'}]}],popup_fields:[{key:'popup',type:'file'}],save_action:'save',run_action:'run',actions:[{action:'future_action'}]};
 const before=structuredClone(item);
 assert.deepEqual(allFields(item).map(field=>field.key),['known','future','section','popup']);
 assert.deepEqual(readoutFields(item).map(field=>field.key),['known']);
 assert.equal(itemCanEdit(item),true);
 assert.deepEqual(item,before);
});
test('native weather/forecast rows preserve provider columns, zero readings, and array rows',()=>{
 assert.deepEqual(tableRows({columns:[{key:'temp',label:'Temperature'},{key:'new',label:'New provider field'}],rows:[{temp:0,new:'Retained'}]}),[{'Temperature':0,'New provider field':'Retained'}]);
 assert.deepEqual(tableRows({columns:['Day','Conditions'],rows:[['Today','Cloudy']]}),[{Day:'Today',Conditions:'Cloudy'}]);
 assert.deepEqual(tableRows({columns:[{field:'temperature',label:'Temperature'}],rows:[{temperature:0}]}),[{Temperature:0}]);
});
test('conditional preview fields follow current drafts, including boolean visibility conditions',()=>{
 const field={key:'destination',label:'Destination',value:'Old',show_when_all:[{source_key:'enabled',equals:true},{key:'mode',any_of:['speech']}]};
 assert.equal(fieldVisible(field,{enabled:true,mode:'speech'}),true);
 assert.equal(fieldVisible(field,{enabled:false,mode:'speech'}),false);
 const item={fields:[field]};
 assert.deepEqual(previewFacts(item,{preview_fields:['destination']},{enabled:true,mode:'speech',destination:'Kitchen'}),[{label:'Destination',value:'Kitchen'}]);
 assert.deepEqual(previewFacts(item,{preview_fields:['destination']},{enabled:false,mode:'speech',destination:'Kitchen'}),[]);
});
