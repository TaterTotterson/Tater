<script setup lang="ts">
import {computed,ref} from 'vue';
import UiIcon from '../../shared/UiIcon.vue';
import type {JsonRow} from '../types';
const props=defineProps<{field:JsonRow}>();const table=ref(false);
const columns=computed<JsonRow[]>(()=>Array.isArray(props.field.columns)?props.field.columns.map((col:unknown,index:number)=>typeof col==='string'?{key:col,label:col}:col&&typeof col==='object'?col as JsonRow:{key:String(index),label:String(index)}):[]);
const rows=computed<JsonRow[]>(()=>Array.isArray(props.field.rows)?props.field.rows:[]);
function value(row:JsonRow,key:string,index=0){return Array.isArray(row)?row[index]:row[key];}
function first(row:JsonRow,keys:string[]){for(const key of keys){const index=columns.value.findIndex(c=>c.key===key);if(index>=0&&value(row,key,index))return String(value(row,key,index));}return '';}
</script>
<template><section class="cw-agenda"><header><h3>{{ field.label||'Agenda' }}</h3><button class="cw-text-button" type="button" @click="table=!table">{{ table?'Agenda view':'Table view' }}</button></header><div v-if="table" class="core-data-table-wrap"><table class="core-data-table"><thead><tr><th v-for="column in columns" :key="column.key">{{ column.label }}</th></tr></thead><tbody><tr v-for="(row,index) in rows" :key="index"><td v-for="(column,i) in columns" :key="column.key">{{ value(row,column.key,i) }}</td></tr></tbody></table></div><div v-else><article v-for="(row,index) in rows" :key="index"><time>{{ first(row,['when','starts_at','time','date']) }}</time><div><strong>{{ first(row,['title','summary','name'])||'Event' }}</strong><p>{{ first(row,['source','account','type']) }}</p><details><summary>Event details <UiIcon name="chevron" /></summary><dl><div v-for="(column,i) in columns" :key="column.key"><dt>{{ column.label }}</dt><dd>{{ value(row,column.key,i) }}</dd></div></dl></details></div></article><p v-if="!rows.length" class="cw-muted">No events in this range.</p></div></section></template>
