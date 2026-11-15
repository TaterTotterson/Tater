<script setup lang="ts">
import {computed} from 'vue';
import CoreManagerField from './CoreManagerField.vue';
import {previewFacts,readoutFields,fieldVisible} from '../presentation';
import type {JsonRow} from '../types';
const props=defineProps<{item:JsonRow;metadata:JsonRow;values:JsonRow;deferVisuals?:boolean;deferFields?:string[]}>();
const facts=computed(()=>previewFacts(props.item,props.metadata,props.values));
const readouts=computed(()=>readoutFields(props.item).filter(field=>fieldVisible(field,props.values)));
const textFields=computed(()=>readouts.value.filter(field=>!['image','video','table','bars','bar_chart'].includes(String(field.type))));
const mediaFields=computed(()=>readouts.value.filter(field=>['image','video'].includes(String(field.type))));
const dataFields=computed(()=>readouts.value.filter(field=>['table','bars','bar_chart'].includes(String(field.type))));
const featuredTables=computed(()=>dataFields.value.filter(field=>props.deferFields?.includes(field.key)&&field.rows?.length));
const primaryData=computed(()=>dataFields.value.filter(field=>!featuredTables.value.includes(field)));
</script>
<template>
  <div class="cw-item-preview">
    <img v-if="item.hero_image_src" class="cw-preview-image" :src="item.hero_image_src" :alt="item.hero_image_alt||item.title" />
    <div v-if="item.hero_badges?.length" class="cw-preview-badges"><span v-for="badge in item.hero_badges" :key="badge.label" :class="`tone-${badge.tone||'muted'}`">{{ badge.label }}</span></div>
    <p v-if="item.subtitle" class="cw-preview-subtitle">{{ item.subtitle }}</p>
    <p v-if="item.detail" class="cw-preview-prose">{{ item.detail }}</p>
    <dl v-if="facts.length" class="cw-preview-facts"><div v-for="fact in facts" :key="fact.label"><dt>{{ fact.label }}</dt><dd>{{ fact.value }}</dd></div></dl>
    <div v-if="item.sensor_rows?.length" class="cw-sensor-readings"><div v-for="reading in item.sensor_rows" :key="reading.label"><span>{{ reading.label }}</span><strong>{{ reading.value }}</strong><small v-if="reading.meta||reading.kind">{{ reading.meta||reading.kind }}</small></div></div>
    <section v-for="field in textFields" :key="field.key" class="cw-preview-text"><h4 v-if="!field.hide_label">{{ field.label||field.key }}</h4><p>{{ values[field.key]??field.value??field.default??'' }}</p></section>
    <details v-if="mediaFields.length&&deferVisuals" class="cw-source-visuals"><summary>Charts & source visuals</summary><div class="cw-preview-media"><CoreManagerField v-for="field in mediaFields" :key="field.key" :field="field" :model-value="values[field.key]" :all-values="values" /></div></details>
    <div v-else-if="mediaFields.length" class="cw-preview-media"><CoreManagerField v-for="field in mediaFields" :key="field.key" :field="field" :model-value="values[field.key]" :all-values="values" /></div>
    <div v-if="primaryData.length" class="cw-preview-data"><CoreManagerField v-for="field in primaryData" :key="field.key" :field="field" :model-value="values[field.key]" :all-values="values" /></div>
    <details v-if="featuredTables.length" class="cw-source-visuals"><summary>Source tables</summary><div class="cw-preview-data"><CoreManagerField v-for="field in featuredTables" :key="field.key" :field="field" :model-value="values[field.key]" :all-values="values" /></div></details>
  </div>
</template>
