<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import type { Terminal } from '@xterm/xterm';
import type { FitAddon } from '@xterm/addon-fit';
import { postJson } from '../shared/api';
import type { JsonRow } from './types';
import '@xterm/xterm/css/xterm.css';
const props=defineProps<{sessionId:string;inputEndpoint:string;entries:JsonRow[];active:boolean}>();
const host=ref<HTMLElement|null>(null);
const error=ref('');
let terminal:Terminal|null=null;
let fit:FitAddon|null=null;
let resize:ResizeObserver|null=null;
let theme:MutationObserver|null=null;
let disposed=false;
let cursor=0;
let inputQueue='';
let sending=false;
let resizeTimer=0;
let dimensions={cols:100,rows:26};
let sentDimensions='';
function paintTheme() {
  if(!terminal||!host.value)return;
  const style=getComputedStyle(host.value);
  terminal.options.theme={background:style.getPropertyValue('--panel-2').trim()||'#202225',foreground:style.getPropertyValue('--text').trim()||'#f1eee8',cursor:style.getPropertyValue('--accent-soft').trim()||'#f08345'};
}
async function flushInput() {
  if(sending||!props.active||disposed)return;
  sending=true;
  try {
    while(!disposed&&props.active&&(inputQueue||sentDimensions!==JSON.stringify(dimensions))) {
      const data=inputQueue.slice(0,2048);inputQueue=inputQueue.slice(data.length);
      const size={...dimensions};
      await postJson(props.inputEndpoint,{data,...size});
      sentDimensions=JSON.stringify(size);
    }
  } catch(err){error.value=err instanceof Error?err.message:'Terminal input failed.';inputQueue='';}
  finally{sending=false;}
}
function fitTerminal(){if(!host.value?.clientWidth||!terminal||!fit)return;fit.fit();dimensions={cols:terminal.cols,rows:terminal.rows};void flushInput();}
function paintOutput(){if(!terminal)return;for(const row of props.entries){if(Number(row.seq)<=cursor)continue;if(row.stream==='terminal')terminal.write(String(row.text||''));cursor=Math.max(cursor,Number(row.seq)||0);}}
onMounted(async()=>{
  const [{Terminal:TerminalClass},{FitAddon:FitClass}]=await Promise.all([import('@xterm/xterm'),import('@xterm/addon-fit')]);
  if(disposed||!host.value)return;
  terminal=new TerminalClass({fontSize:13,fontFamily:'ui-monospace, SFMono-Regular, Menlo, monospace',cursorBlink:false,disableStdin:!props.active,scrollback:5000,allowProposedApi:false,convertEol:false});
  fit=new FitClass();terminal.loadAddon(fit);terminal.open(host.value);paintTheme();
  terminal.onData(data=>{inputQueue+=data;void flushInput();});
  resize=new ResizeObserver(()=>{window.clearTimeout(resizeTimer);resizeTimer=window.setTimeout(fitTerminal,80);});resize.observe(host.value);
  theme=new MutationObserver(paintTheme);theme.observe(document.documentElement,{attributes:true,attributeFilter:['data-theme','style']});
  paintOutput();await nextTick();fitTerminal();terminal.focus();
});
watch(()=>props.entries,paintOutput,{deep:false});
watch(()=>props.active,active=>{if(terminal)terminal.options.disableStdin=!active;});
onBeforeUnmount(()=>{disposed=true;window.clearTimeout(resizeTimer);resize?.disconnect();theme?.disconnect();terminal?.dispose();});
</script>
<template><div class="txw-interactive"><p v-if="error" class="tv-notice error" role="alert">{{ error }}</p><div ref="host" class="txw-xterm" :aria-label="`Interactive terminal ${sessionId}`" /><p class="txw-muted">{{ active ? 'Interactive shell · Ctrl+C interrupts the foreground command.' : 'Shell closed. Its output is retained in this session.' }}</p></div></template>
