import {createApp,computed,h,reactive,ref} from 'vue';
import CorePanelRenderer from '../src/cores/components/CorePanelRenderer.vue';
import MusicCoreApp from '../src/music/MusicCoreApp.vue';
import {fixtures,musicFixture} from './core-workspace-fixtures';
import '../src/base.css';
import '../src/music/music-core.css';
import '../src/tater-ui.css';
import '../src/workspace.css';
import '../src/cores/workspaces.css';
import '../src/cores/presentation.css';

const requests:any[]=[];
(window as any).workspaceRequests=requests;
window.fetch=async (_input,init)=>{requests.push(JSON.parse(String(init?.body||'{}')));return new Response(JSON.stringify({ok:true,message:'Test action completed.'}),{status:200,headers:{'Content-Type':'application/json'}});};
// Music's event subscription is deliberately local and inert in the fixture.
(window as any).EventSource=class { addEventListener(){} close(){} };
const app=createApp({setup(){
 const core=ref(new URLSearchParams(location.search).get('core')||'ai_task_core');
 const fixtureState=reactive(fixtures);
 (window as any).workspaceFixtures=fixtureState;
 const selected=computed(()=>fixtureState[core.value]);
 const musicState=reactive({payload:musicFixture});
 (window as any).workspaceMusic=musicState;
 return()=>h('main',{class:'tater-vue-surface tcx-cores cw-cores',style:'padding:clamp(14px,2.5vw,28px);max-width:1400px;width:100%;margin:auto'},[
  h('header',{class:'cw-library-heading'},[h('div',[h('h1','Core workspaces'),h('p','Synthetic data · actual application components')]),h('select',{'aria-label':'Choose core',value:core.value,onChange:(event:Event)=>core.value=(event.target as HTMLSelectElement).value},[...Object.keys(fixtures),'music_core'].map(key=>h('option',{value:key},key)))]),
  h('section',{class:'cw-workspace-content'},[core.value==='music_core'?h(MusicCoreApp,{state:musicState,options:{coreKey:'music_core',embedded:true,initialPayload:musicFixture,tabEndpoint:'/fake/tab',actionEndpoint:'/fake/action',eventsEndpoint:'/fake/events'}}):h(CorePanelRenderer,{key:core.value,payload:selected.value,tab:{core_key:core.value,label:core.value},actionEndpoint:'/fake/action',refresh:async()=>{}})]),
 ]);
}});
app.mount('#app');

import '../src/music/music-workspace.css';
