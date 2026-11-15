<script setup lang="ts">
import {computed,nextTick,ref,watch} from 'vue';
import CoreManagerField from './CoreManagerField.vue';
import {fieldGroups} from '../workspace';
import type {JsonRow} from '../types';
const props=defineProps<{fields:JsonRow[];sections?:JsonRow[];values:JsonRow;metadata?:JsonRow;label?:string}>();
const emit=defineEmits<{update:[field:JsonRow,value:unknown];commit:[field:JsonRow];error:[message:string]}>();
const groups=computed(()=>fieldGroups(props.fields||[],props.sections||[],props.metadata||{}));
const selected=ref('');const active=computed(()=>groups.value.find(group=>group.id===selected.value)||groups.value[0]);
watch(groups,rows=>{if(!rows.some(row=>row.id===selected.value))selected.value=rows[0]?.id||'';},{immediate:true});
function revealInvalid(event:Event){const target=event.target as HTMLElement;const group=target.closest<HTMLElement>('[data-field-group]');if(group){selected.value=group.dataset.fieldGroup||'';void nextTick(()=>target.focus());}}
</script>
<template><div class="cw-fields-editor" @invalid.capture="revealInvalid"><nav v-if="groups.length>1" class="cw-field-tabs" :aria-label="label||'Editor sections'"><button v-for="group in groups" :key="group.id" type="button" :aria-current="active?.id===group.id?'page':undefined" @click="selected=group.id">{{ group.label }}</button></nav><div v-for="group in groups" v-show="active?.id===group.id" :key="group.id" :data-field-group="group.id" class="form-grid tcx-native-fields" :aria-label="group.label"><p v-if="group.description" class="cw-field-help">{{ group.description }}</p><CoreManagerField v-for="(field,index) in group.fields" :key="`${field.key||field.label||'field'}:${index}`" :field="field" :active="active?.id===group.id" :model-value="values[field.key]" :all-values="values" @update:model-value="emit('update',field,$event)" @commit="emit('commit',field)" @error="emit('error',$event)" /></div></div></template>
