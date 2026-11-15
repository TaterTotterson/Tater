<script setup lang="ts">
import {computed} from 'vue';
import UiIcon from '../../shared/UiIcon.vue';
import {allFields,metric,tableRows} from '../presentation';
import type {JsonRow} from '../types';
const props=defineProps<{items:JsonRow[];contextItems?:JsonRow[];stats:JsonRow[];screen:JsonRow;metadata:JsonRow}>();
const layout=computed(()=>String(props.screen.layout||'insights'));
const fields=computed(()=>(props.contextItems||props.items).flatMap(allFields));
const temperature=computed(()=>metric(props.stats,['Outdoor Temp','Temperature']));
const condition=computed(()=>metric(props.stats,['Forecast','Condition']));
const readings=computed(()=>props.stats.filter(row=>['humidity','wind','rain today'].includes(String(row.label).toLowerCase())));
const spaces=computed(()=>fields.value.find(field=>field.key==='environment_area_overview'));
const forecast=computed(()=>fields.value.find(field=>field.key==='weatherapi_daily_forecast'));
const spacesRows=computed(()=>spaces.value?tableRows(spaces.value):[]);
const forecastRows=computed(()=>forecast.value?tableRows(forecast.value):[]);
const isWeather=computed(()=>layout.value==='weather'||layout.value==='forecast');
const context=computed(()=>isWeather.value?props.stats.filter(row=>!['outdoor temp','temperature','forecast','condition','humidity','wind','rain today'].includes(String(row.label).toLowerCase())):props.stats);
const mediaPicks=computed(()=>layout.value==='media'?(props.contextItems||[]).filter(item=>item.group==='picks'):[]);
</script>
<template>
  <div class="cw-purpose-workspace" :class="`cw-purpose-${layout}`" :data-workspace-layout="layout">
    <div class="cw-purpose-main">
      <template v-if="isWeather">
        <section v-if="layout==='weather'" class="cw-weather-now"><UiIcon name="weather" /><strong>{{ temperature||'—' }}</strong><div><h3>Current conditions</h3><p>{{ condition||'Waiting for weather data' }}</p><span v-if="metric(stats,['Last Sample'])">Updated {{ metric(stats,['Last Sample']) }}</span></div></section>
        <div v-if="readings.length&&layout==='weather'" class="cw-weather-readings"><div v-for="reading in readings" :key="reading.label"><span>{{ reading.label }}</span><strong>{{ reading.value }}</strong></div></div>
        <section v-if="spacesRows.length&&layout==='weather'" class="cw-spaces"><h3>Your spaces</h3><div v-for="(row,index) in spacesRows" :key="index" class="cw-space-reading"><UiIcon name="weather" /><dl><div v-for="(value,label) in row" :key="label"><dt>{{ label }}</dt><dd>{{ value }}</dd></div></dl></div></section>
        <section v-if="forecastRows.length" class="cw-forecast"><h3>Forecast</h3><div v-for="(row,index) in forecastRows" :key="index" class="cw-forecast-day"><dl><div v-for="(value,label) in row" :key="label"><dt>{{ label }}</dt><dd>{{ value }}</dd></div></dl></div></section>
      </template>
      <header v-else class="cw-purpose-title"><UiIcon :name="screen.icon||metadata.icon||'cores'" /><div><h3>{{ screen.label||'Overview' }}</h3><p v-if="screen.description">{{ screen.description }}</p></div></header>
      <section v-if="mediaPicks.length" class="cw-media-picks"><h3>Tater’s Picks</h3><div><article v-for="pick in mediaPicks" :key="pick.id"><UiIcon name="film" /><strong>{{ pick.title }}</strong><p>{{ pick.subtitle||pick.detail }}</p></article></div></section>
      <div class="cw-purpose-sections"><section v-for="item in items" :key="item.id" class="cw-purpose-section"><slot :item="item" /></section></div>
    </div>
    <aside class="cw-purpose-context"><h3>{{ screen.context_label||'At a glance' }}</h3><UiIcon :name="screen.icon||metadata.icon||'cores'" /><dl v-if="context.length"><div v-for="row in context" :key="row.label"><dt>{{ row.label }}</dt><dd>{{ row.value }}</dd></div></dl><p v-if="screen.context_description">{{ screen.context_description }}</p></aside>
  </div>
</template>
