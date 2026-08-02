<script setup lang="ts">
import { nextTick, onBeforeUnmount, ref, watch } from "vue";
import type { CoreTabSpec, JsonRow } from "../types";

const props = defineProps<{
  payload: JsonRow;
  tab: CoreTabSpec;
  render?: (host: HTMLElement, payload: JsonRow, tab: CoreTabSpec) => void;
  clear?: (host: HTMLElement) => void;
}>();

const host = ref<HTMLElement | null>(null);

async function paint() {
  await nextTick();
  if (host.value) props.render?.(host.value, props.payload || {}, props.tab);
}

watch(() => [props.payload, props.tab], paint, { immediate: true, deep: false });
onBeforeUnmount(() => { if (host.value) props.clear?.(host.value); });
</script>

<template><div ref="host" class="tcx-legacy-host" /></template>
