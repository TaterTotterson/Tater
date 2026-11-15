<script setup lang="ts">
import UiIcon from "./UiIcon.vue";

defineProps<{ label: string; count: number }>();
const query = defineModel<string>({ default: "" });
</script>

<template>
  <div class="workspace-catalog-toolbar">
    <label class="workspace-search"><UiIcon name="search" /><input v-model="query" type="search" :aria-label="label" :placeholder="`${label}…`" /></label>
    <slot><span class="workspace-result-count" role="status">{{ count }} {{ count === 1 ? 'item' : 'items' }}</span></slot>
  </div>
</template>
