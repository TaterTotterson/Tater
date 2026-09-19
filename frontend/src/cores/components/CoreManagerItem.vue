<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import PopupTransition from "../../shared/PopupTransition.vue";
import type { JsonRow } from "../types";
import CoreManagerField from "./CoreManagerField.vue";

type ActionRunner = (action: string, payload: JsonRow, busyKey?: string, successText?: string) => Promise<JsonRow | null>;

const props = defineProps<{
  item: JsonRow;
  ui: JsonRow;
  run: ActionRunner;
  busy: (key: string) => boolean;
  selected?: boolean;
}>();

const emit = defineEmits<{ select: [selected: boolean] }>();
const values = reactive<JsonRow>({});
const dirty = new Set<string>();
const status = ref("");
const popupOpen = ref(false);
const fieldsOpen = ref(false);

const itemId = computed(() => text(props.item.id));
const title = computed(() => text(props.item.title || props.item.id) || "(item)");
const fields = computed<JsonRow[]>(() => Array.isArray(props.item.fields) ? props.item.fields : []);
const sections = computed<JsonRow[]>(() => Array.isArray(props.item.sections) ? props.item.sections : []);
const customActions = computed<JsonRow[]>(() => Array.isArray(props.item.actions) ? props.item.actions.filter((action: JsonRow) => text(action.action)) : []);
const fieldsPopup = computed(() => Boolean(props.ui.item_fields_popup) && props.item.fields_popup !== false);
const fieldsDropdown = computed(() => Boolean(props.item.fields_dropdown ?? props.ui.item_fields_dropdown));
const sectionsInDropdown = computed(() => Boolean(props.item.sections_in_dropdown ?? props.ui.item_sections_in_dropdown));
const popupFields = computed<JsonRow[]>(() => {
  const rows: JsonRow[] = (Array.isArray(props.item.popup_fields) ? props.item.popup_fields : []).map((field: JsonRow) => ({ ...field }));
  if (fieldsPopup.value) {
    rows.push(...fields.value.map((field) => ({ ...field })));
    sections.value.forEach((section) => (Array.isArray(section.fields) ? section.fields : []).forEach((field: JsonRow) => rows.push({ ...field, label: `${text(section.label) || "Section"} • ${text(field.label || field.key) || "Field"}` })));
  }
  return rows;
});
const showPopupButton = computed(() => popupFields.value.length > 0 && (fieldsPopup.value || Array.isArray(props.item.popup_fields)));
const inlineSections = computed(() => sections.value.filter((section) => sectionsInDropdown.value || section.inline));
const dropdownSections = computed(() => sections.value.filter((section) => !sectionsInDropdown.value && !section.inline));
const summaryRows = computed<JsonRow[]>(() => Array.isArray(props.item.summary_rows) ? props.item.summary_rows : []);
const sensorRows = computed<JsonRow[]>(() => Array.isArray(props.item.sensor_rows) ? props.item.sensor_rows : []);
const badges = computed<JsonRow[]>(() => Array.isArray(props.item.hero_badges) ? props.item.hero_badges : []);
const hasSummary = computed(() => Boolean(props.item.hero_image_src || badges.value.length || summaryRows.value.length || sensorRows.value.length || props.item.detail));
const cardClasses = computed(() => [
  text(props.item.group) ? `core-manager-item-${token(props.item.group)}` : "",
  text(props.item.card_variant) ? `core-manager-item-variant-${token(props.item.card_variant)}` : "",
]);
const allKnownFields = computed<JsonRow[]>(() => [
  ...fields.value,
  ...sections.value.flatMap((section) => Array.isArray(section.fields) ? section.fields : []),
  ...(Array.isArray(props.item.popup_fields) ? props.item.popup_fields : []),
]);

function text(value: unknown): string { return String(value ?? "").trim(); }
function token(value: unknown): string { return text(value).toLowerCase().replace(/[^a-z0-9_-]/g, ""); }
function cloneValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(cloneValue);
  if (value && typeof value === "object") return { ...(value as JsonRow) };
  return value;
}
function initialValue(field: JsonRow): unknown {
  if (field.value !== undefined) return cloneValue(field.value);
  if (field.default !== undefined) return cloneValue(field.default);
  if (text(field.type).toLowerCase() === "checkbox") return false;
  if (["multiselect", "multi_choice_cards", "image_checklist"].includes(text(field.type).toLowerCase())) return [];
  return "";
}
function syncValues() {
  allKnownFields.value.forEach((field) => {
    const key = text(field.key);
    if (key && !dirty.has(key)) values[key] = initialValue(field);
  });
}
function setValue(field: JsonRow, value: unknown) {
  const key = text(field.key);
  if (!key) return;
  values[key] = value;
  dirty.add(key);
}
function actionKey(action: string) { return `item:${itemId.value}:${action}`; }
async function execute(action: unknown, payload: JsonRow, successText = "Done.") {
  const name = text(action);
  if (!name) return null;
  status.value = "Working…";
  const result = await props.run(name, payload, actionKey(name), successText);
  if (result) {
    status.value = text(result.message || successText);
    dirty.clear();
  } else status.value = "";
  return result;
}
async function save(closePopup = false) {
  if (!props.item.save_action) return;
  const result = await execute(props.item.save_action, { id: itemId.value, values: { ...values } }, text(props.item.save_success_text) || "Saved.");
  if (result && closePopup) popupOpen.value = false;
}
async function reset() {
  const action = text(props.item.reset_action);
  if (!action) return;
  const confirmation = text(props.item.reset_confirm) || "Reset this item to defaults?";
  if (confirmation && !window.confirm(confirmation)) return;
  await execute(action, { id: itemId.value }, "Defaults restored.");
}
async function remove() {
  const action = text(props.item.remove_action);
  if (!action || !window.confirm(text(props.item.remove_confirm) || "Remove this item?")) return;
  await execute(action, { id: itemId.value }, "Removed.");
}
async function runNow() {
  const action = text(props.item.run_action);
  const confirmation = text(props.item.run_confirm);
  if (!action || confirmation && !window.confirm(confirmation)) return;
  await execute(action, { id: itemId.value, values: { ...values } }, "Queued.");
}
async function runCustom(action: JsonRow) {
  const name = text(action.action);
  const confirmation = text(action.confirm);
  if (!name || confirmation && !window.confirm(confirmation)) return;
  await execute(name, { id: itemId.value, values: { ...values } }, text(action.success_text) || "Done.");
}
async function commitField(field: JsonRow) {
  const action = text(field.action);
  if (action) await execute(action, { id: itemId.value, values: { ...values } }, "Updated.");
}
function openCard(event: MouseEvent | KeyboardEvent) {
  if (!props.item.click_opens_fields) return;
  const target = event.target as Element | null;
  if (target?.closest("button, input, select, textarea, a, label, summary, details")) return;
  fieldsOpen.value = !fieldsOpen.value;
}
function handleKey(event: KeyboardEvent) {
  if ((event.key === "Enter" || event.key === " ") && event.target === event.currentTarget) { event.preventDefault(); openCard(event); }
}

watch(allKnownFields, syncValues, { immediate: true });
</script>

<template>
  <article class="card core-manager-item tcx-native-item" :class="cardClasses" :data-core-item-group="item.group || ''" :tabindex="item.click_opens_fields ? 0 : undefined" @click="openCard" @keydown="handleKey">
    <div class="card-head">
      <h3 class="card-title">{{ title }}</h3>
      <div class="core-manager-card-tools"><label v-if="item.selectable" class="core-manager-item-selection"><input class="toggle-input" type="checkbox" :checked="selected" :aria-label="item.selection_label || `Select ${title}`" @change="emit('select', ($event.target as HTMLInputElement).checked)" /><span>Select</span></label><span class="small">{{ item.core_key }}</span></div>
    </div>

    <div v-if="hasSummary" class="core-satellite-summary" :class="{ 'no-image': !item.hero_image_src }">
      <div v-if="item.hero_image_src" class="core-satellite-image-wrap"><img class="core-satellite-image" :src="item.hero_image_src" :alt="item.hero_image_alt || title" /></div>
      <div class="core-satellite-summary-main">
        <div v-if="item.subtitle" class="small core-satellite-subtitle">{{ item.subtitle }}</div>
        <div v-if="badges.length" class="core-satellite-badges"><span v-for="badge in badges" :key="badge.label" class="core-satellite-badge" :class="`tone-${token(badge.tone || 'muted')}`">{{ badge.label }}</span></div>
        <div v-if="item.detail" class="small core-satellite-detail">{{ item.detail }}</div>
        <div v-if="summaryRows.length" class="core-satellite-facts"><div v-for="row in summaryRows" :key="row.label" class="core-satellite-fact"><div class="small core-satellite-fact-label">{{ row.label }}</div><div class="core-satellite-fact-value">{{ row.value }}</div></div></div>
        <div v-if="sensorRows.length" class="core-satellite-sensors"><div class="small core-satellite-sensors-title">{{ item.sensor_title || 'Sensors' }}</div><div class="core-satellite-sensor-grid"><div v-for="row in sensorRows" :key="row.label" class="core-satellite-sensor-pill"><span class="core-satellite-sensor-label">{{ row.label }}</span><span class="core-satellite-sensor-value">{{ row.value }}</span><span v-if="row.meta || row.kind" class="core-satellite-sensor-meta">{{ row.meta || row.kind }}</span></div></div></div>
      </div>
    </div>
    <div v-else-if="item.subtitle" class="small">{{ item.subtitle }}</div>

    <details v-if="fieldsDropdown && (fields.length || inlineSections.length)" class="settings-dropdown" :open="fieldsOpen" @toggle="fieldsOpen = ($event.target as HTMLDetailsElement).open">
      <summary class="settings-summary">{{ item.fields_dropdown_label || ui.item_fields_dropdown_label || 'Settings' }}</summary>
      <div v-if="fields.length" class="form-grid tcx-native-fields"><CoreManagerField v-for="field in fields" :key="field.key" :field="field" :model-value="values[field.key]" :all-values="values" @update:model-value="setValue(field, $event)" @commit="commitField(field)" @error="status = $event" /></div>
      <section v-for="section in inlineSections" :key="section.label" class="core-inline-section" :class="section.tone ? `tone-${token(section.tone)}` : ''"><div class="small core-inline-section-title">{{ section.label || 'Section' }}</div><div class="form-grid tcx-native-fields"><CoreManagerField v-for="field in section.fields || []" :key="field.key" :field="field" :model-value="values[field.key]" :all-values="values" @update:model-value="setValue(field, $event)" @commit="commitField(field)" @error="status = $event" /></div></section>
    </details>
    <div v-else-if="!fieldsPopup && fields.length" class="form-grid tcx-native-fields"><CoreManagerField v-for="field in fields" :key="field.key" :field="field" :model-value="values[field.key]" :all-values="values" @update:model-value="setValue(field, $event)" @commit="commitField(field)" @error="status = $event" /></div>

    <template v-if="!fieldsPopup && !fieldsDropdown && !sectionsInDropdown"><details v-for="section in dropdownSections" :key="section.label" class="settings-dropdown"><summary class="settings-summary">{{ section.label || 'Section' }}</summary><div class="form-grid tcx-native-fields"><CoreManagerField v-for="field in section.fields || []" :key="field.key" :field="field" :model-value="values[field.key]" :all-values="values" @update:model-value="setValue(field, $event)" @commit="commitField(field)" @error="status = $event" /></div></details><section v-for="section in inlineSections" :key="`inline-${section.label}`" class="core-inline-section" :class="section.tone ? `tone-${token(section.tone)}` : ''"><div class="small core-inline-section-title">{{ section.label || 'Section' }}</div><div class="form-grid tcx-native-fields"><CoreManagerField v-for="field in section.fields || []" :key="field.key" :field="field" :model-value="values[field.key]" :all-values="values" @update:model-value="setValue(field, $event)" @commit="commitField(field)" @error="status = $event" /></div></section></template>

    <div v-if="customActions.length || item.save_action || item.reset_action || item.remove_action || item.run_action || showPopupButton" class="inline-row tcx-native-actions">
      <button v-for="action in customActions" :key="action.action" type="button" :class="action.tone === 'danger' ? 'inline-btn danger' : action.tone === 'muted' ? 'inline-btn' : 'action-btn'" :disabled="busy(actionKey(action.action))" :title="action.tooltip || action.title" @click="runCustom(action)">{{ action.label || action.title || 'Run' }}</button>
      <button v-if="item.save_action && item.show_save_button !== false && !fieldsPopup" type="button" class="action-btn" :disabled="busy(actionKey(item.save_action))" @click="save(false)">{{ item.save_label || 'Save' }}</button>
      <button v-if="item.reset_action" type="button" class="inline-btn danger" :disabled="busy(actionKey(item.reset_action))" @click="reset">{{ item.reset_label || 'Restore Default Settings' }}</button>
      <button v-if="item.remove_action" type="button" class="inline-btn danger" :disabled="busy(actionKey(item.remove_action))" @click="remove">{{ item.remove_label || 'Remove' }}</button>
      <button v-if="showPopupButton" type="button" class="action-btn" @click="popupOpen = true">{{ item.settings_label || ui.item_fields_popup_label || 'Settings' }}</button>
      <button v-if="item.run_action" type="button" class="action-btn tcx-run-action" :disabled="busy(actionKey(item.run_action))" @click="runNow">{{ item.run_label || 'Run Now' }}</button>
      <span class="small core-manager-status">{{ status }}</span>
    </div>
  </article>

  <PopupTransition :open="popupOpen" @close="popupOpen = false">
    <form class="tv-modal tcx-native-popup" @submit.prevent="save(true)">
      <header><div><span class="tv-eyebrow">{{ item.group || 'Core item' }}</span><h2>{{ item.settings_title || `${title} Settings` }}</h2></div><button class="tv-button" type="button" @click="popupOpen = false">Close</button></header>
      <div class="form-grid tcx-native-fields"><CoreManagerField v-for="field in popupFields" :key="field.key || field.label" :field="field" :model-value="values[field.key]" :all-values="values" @update:model-value="setValue(field, $event)" @commit="commitField(field)" @error="status = $event" /></div>
      <footer><span>{{ status || 'Ready' }}</span><div class="inline-row"><button v-if="item.reset_action" class="tv-button danger" type="button" @click="reset">{{ item.reset_label || 'Restore defaults' }}</button><button v-if="item.save_action" class="tv-button primary" type="submit" :disabled="busy(actionKey(item.save_action))">{{ item.save_label || 'Save' }}</button></div></footer>
    </form>
  </PopupTransition>
</template>
