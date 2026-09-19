<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import PopupTransition from "../../shared/PopupTransition.vue";
import DynamicField from "./DynamicField.vue";
import type { MusicAction, MusicField, MusicItem } from "../types";

const props = defineProps<{
  item: MusicItem;
  busy: (key: string) => boolean;
  run: (action: string, payload: Record<string, unknown>, busyKey?: string) => Promise<boolean>;
  fieldsPopup?: boolean;
  fieldsDropdown?: boolean;
  dropdownLabel?: string;
  popupLabel?: string;
}>();

const values = reactive<Record<string, unknown>>({});
const dirty = new Set<string>();
const fieldGrid = ref<HTMLElement | null>(null);
const popupOpen = ref(false);
let fieldLayoutFrame = 0;
let fieldResizeObserver: ResizeObserver | null = null;
let fieldLayoutSignature = "";

const editableFields = computed<MusicField[]>(() => {
  const result: MusicField[] = [];
  const seen = new Set<string>();
  for (const field of [...(props.item.popup_fields || []), ...(props.item.fields || [])]) {
    const key = String(field.key || "").trim();
    if (!key || seen.has(key)) continue;
    seen.add(key);
    result.push(field);
  }
  return result;
});
const popupFields = computed<MusicField[]>(() => props.fieldsPopup
  ? editableFields.value
  : props.item.popup_fields || []);
const showPopupButton = computed(() => popupFields.value.length > 0);

function layoutFields(): void {
  fieldLayoutFrame = 0;
  const grid = fieldGrid.value;
  if (!grid) return;
  const gridStyle = window.getComputedStyle(grid);
  const rowHeight = Number.parseFloat(gridStyle.gridAutoRows) || 8;
  const rowGap = Number.parseFloat(gridStyle.rowGap) || 13;
  const fields = Array.from(grid.children).filter((field): field is HTMLElement => field instanceof HTMLElement);
  fields.forEach((field) => { field.style.gridRowEnd = "auto"; });
  fields.forEach((field) => {
    const height = field.getBoundingClientRect().height;
    const span = Math.max(1, Math.ceil((height + rowGap) / (rowHeight + rowGap)));
    field.style.gridRowEnd = `span ${span}`;
  });
}

function scheduleFieldLayout(): void {
  window.cancelAnimationFrame(fieldLayoutFrame);
  fieldLayoutFrame = window.requestAnimationFrame(layoutFields);
}

function observeFields(): void {
  fieldResizeObserver?.disconnect();
  const grid = fieldGrid.value;
  if (!grid) return;
  fieldResizeObserver = new ResizeObserver(scheduleFieldLayout);
  fieldResizeObserver.observe(grid);
  Array.from(grid.children).forEach((field) => fieldResizeObserver?.observe(field));
  scheduleFieldLayout();
}

function copyFieldValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((entry) => (entry && typeof entry === "object" ? { ...entry } : entry));
  if (value && typeof value === "object") return { ...(value as Record<string, unknown>) };
  return value;
}

function layoutSignature(fields: MusicField[]): string {
  return JSON.stringify(
    (fields || []).map((field) => ({
      key: field.key,
      type: field.type,
      label: field.label,
      description: field.description,
      placeholder: field.placeholder,
      compact: field.compact,
      disabled: field.disabled,
      read_only: field.read_only,
      required: field.required,
      min: field.min,
      max: field.max,
      step: field.step,
      suffix: field.suffix,
      options: field.options,
    })),
  );
}

function syncValues(fields: MusicField[]): void {
  for (const field of fields) {
    if (!dirty.has(field.key)) values[field.key] = copyFieldValue(field.value);
  }
}

watch(
  editableFields,
  (fields) => {
    syncValues(fields);
    const nextLayoutSignature = layoutSignature(fields);
    if (nextLayoutSignature !== fieldLayoutSignature) {
      fieldLayoutSignature = nextLayoutSignature;
      void nextTick().then(observeFields);
    }
  },
  { immediate: true },
);

watch(popupOpen, (open) => {
  if (open) void nextTick().then(observeFields);
});

onMounted(() => void nextTick().then(observeFields));
onBeforeUnmount(() => {
  window.cancelAnimationFrame(fieldLayoutFrame);
  fieldResizeObserver?.disconnect();
});

function setValue(field: MusicField, value: unknown): void {
  values[field.key] = value;
  dirty.add(field.key);
}

async function save(closePopup = false): Promise<void> {
  if (!props.item.save_action) return;
  const saved = await props.run(props.item.save_action, { id: props.item.id, values: { ...values } }, `item:${props.item.id}:save`);
  if (saved) {
    dirty.clear();
    syncValues(editableFields.value);
    if (closePopup) popupOpen.value = false;
  }
}

async function runAction(entry: MusicAction): Promise<void> {
  if (entry.confirm && !window.confirm(entry.confirm)) return;
  await props.run(entry.action, { id: props.item.id, values: { ...values } }, `item:${props.item.id}:${entry.action}`);
}
</script>

<template>
  <article
    class="tm-settings-card"
    :class="item.card_variant ? `variant-${item.card_variant}` : ''"
  >
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
    <dl v-if="item.summary_rows?.length" class="tm-settings-summary">
      <div v-for="row in item.summary_rows" :key="row.label">
        <dt>{{ row.label }}</dt>
        <dd>{{ row.value ?? '—' }}</dd>
      </div>
    </dl>

    <details v-if="!fieldsPopup && fieldsDropdown && item.fields?.length" class="tm-settings-fields" @toggle="scheduleFieldLayout">
      <summary>{{ dropdownLabel || 'Connection settings' }}</summary>
      <div ref="fieldGrid" class="tm-form-grid">
        <DynamicField
          v-for="field in item.fields"
          :key="field.key"
          :field="field"
          :model-value="values[field.key]"
          :compact="Boolean(field.compact)"
          @update:model-value="setValue(field, $event)"
        />
      </div>
    </details>
    <div v-else-if="!fieldsPopup && item.fields?.length" ref="fieldGrid" class="tm-form-grid">
      <DynamicField
        v-for="field in item.fields"
        :key="field.key"
        :field="field"
        :model-value="values[field.key]"
        :compact="Boolean(field.compact)"
        @update:model-value="setValue(field, $event)"
      />
    </div>

    <footer v-if="item.actions?.length || item.save_action || showPopupButton">
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
        v-if="item.save_action && !fieldsPopup"
        type="button"
        class="tm-button primary"
        :disabled="busy(`item:${item.id}:save`)"
        @click="save(false)"
      >
        {{ item.save_label || 'Save' }}
      </button>
      <button
        v-if="showPopupButton"
        type="button"
        class="tm-button primary"
        :aria-label="item.settings_aria_label || popupLabel"
        @click="popupOpen = true"
      >
        {{ popupLabel || 'Settings' }}
      </button>
    </footer>
  </article>

  <PopupTransition :open="popupOpen" backdrop-class="tm-modal-backdrop" @close="popupOpen = false">
    <form class="tm-modal tm-settings-modal" @submit.prevent="save(true)">
      <header>
        <div>
          <span class="tm-eyebrow">{{ item.group || 'Music settings' }}</span>
          <h3>{{ item.settings_title || `${item.title || item.id} Settings` }}</h3>
        </div>
        <button class="tm-button secondary" type="button" @click="popupOpen = false">Close</button>
      </header>
      <div class="tm-modal-body">
        <div ref="fieldGrid" class="tm-form-grid tm-modal-form-grid">
          <DynamicField
            v-for="field in popupFields"
            :key="field.key"
            :field="field"
            :model-value="values[field.key]"
            :compact="Boolean(field.compact)"
            @update:model-value="setValue(field, $event)"
          />
        </div>
      </div>
      <footer>
        <button class="tm-button secondary" type="button" @click="popupOpen = false">Cancel</button>
        <button
          v-if="item.save_action"
          class="tm-button primary"
          type="submit"
          :disabled="busy(`item:${item.id}:save`)"
        >
          {{ item.save_label || 'Save' }}
        </button>
      </footer>
    </form>
  </PopupTransition>
</template>
