<script setup lang="ts">
import { reactive, watch } from "vue";
import DynamicField from "./DynamicField.vue";
import type { MusicAction, MusicField, MusicItem } from "../types";

const props = defineProps<{
  item: MusicItem;
  busy: (key: string) => boolean;
  run: (action: string, payload: Record<string, unknown>, busyKey?: string) => Promise<boolean>;
}>();

const values = reactive<Record<string, unknown>>({});
const dirty = new Set<string>();

function copyFieldValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((entry) => (entry && typeof entry === "object" ? { ...entry } : entry));
  if (value && typeof value === "object") return { ...(value as Record<string, unknown>) };
  return value;
}

watch(
  () => props.item.fields,
  (fields) => {
    for (const field of fields || []) {
      if (!dirty.has(field.key)) values[field.key] = copyFieldValue(field.value);
    }
  },
  { immediate: true, deep: true },
);

function setValue(field: MusicField, value: unknown): void {
  values[field.key] = value;
  dirty.add(field.key);
}

async function save(): Promise<void> {
  if (!props.item.save_action) return;
  const saved = await props.run(props.item.save_action, { id: props.item.id, values: { ...values } }, `item:${props.item.id}:save`);
  if (saved) dirty.clear();
}

async function runAction(entry: MusicAction): Promise<void> {
  if (entry.confirm && !window.confirm(entry.confirm)) return;
  const saved = await props.run(entry.action, { id: props.item.id, values: { ...values } }, `item:${props.item.id}:${entry.action}`);
  if (saved) dirty.clear();
}
</script>

<template>
  <article class="tm-settings-card">
    <header>
      <div>
        <h3>{{ item.title || item.id }}</h3>
        <p>{{ item.subtitle }}</p>
      </div>
      <div v-if="item.hero_badges?.length" class="tm-badges">
        <span v-for="badge in item.hero_badges" :key="badge.label" :class="`tone-${badge.tone || 'muted'}`">
          {{ badge.label }}
        </span>
      </div>
    </header>
    <p v-if="item.detail" class="tm-card-detail">{{ item.detail }}</p>

    <details v-if="item.fields_dropdown && item.fields?.length" class="tm-settings-fields">
      <summary>Connection settings</summary>
      <div class="tm-form-grid">
        <DynamicField
          v-for="field in item.fields"
          :key="field.key"
          :field="field"
          :model-value="values[field.key]"
          @update:model-value="setValue(field, $event)"
        />
      </div>
    </details>
    <div v-else-if="item.fields?.length" class="tm-form-grid">
      <DynamicField
        v-for="field in item.fields"
        :key="field.key"
        :field="field"
        :model-value="values[field.key]"
        @update:model-value="setValue(field, $event)"
      />
    </div>

    <footer v-if="item.actions?.length || item.save_action">
      <button
        v-for="entry in item.actions || []"
        :key="entry.action"
        type="button"
        class="tm-button"
        :class="entry.tone === 'danger' ? 'danger' : entry.action.includes('activate') ? 'primary' : 'secondary'"
        :disabled="busy(`item:${item.id}:${entry.action}`)"
        @click="runAction(entry)"
      >
        {{ entry.label || 'Run' }}
      </button>
      <button
        v-if="item.save_action"
        type="button"
        class="tm-button primary"
        :disabled="busy(`item:${item.id}:save`)"
        @click="save"
      >
        {{ item.save_label || 'Save' }}
      </button>
    </footer>
  </article>
</template>
