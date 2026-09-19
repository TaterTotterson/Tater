<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import { postJson } from "../../shared/api";
import type { CoreTabSpec, JsonRow } from "../types";
import CoreManagerField from "./CoreManagerField.vue";
import CoreManagerItems from "./CoreManagerItems.vue";

const props = defineProps<{
  payload: JsonRow;
  tab: CoreTabSpec;
  actionEndpoint: string;
  refresh: () => Promise<void>;
  notify?: (message: string, tone?: string) => void;
}>();

const activeManagerTab = ref("");
const activeGroupTab = ref("");
const status = ref("");
const error = ref("");
const busyKeys = ref<string[]>([]);
const addValues = reactive<JsonRow>({});
const statsValues = reactive<JsonRow>({});
const addDirty = new Set<string>();

const body = computed(() => props.payload && typeof props.payload === "object" ? props.payload : {});
const ui = computed<JsonRow>(() => body.value.ui && typeof body.value.ui === "object" ? body.value.ui : {});
const isManager = computed(() => text(ui.value.kind) === "settings_manager");
const stats = computed<JsonRow[]>(() => Array.isArray(body.value.stats) ? body.value.stats : []);
const itemForms = computed<JsonRow[]>(() => Array.isArray(ui.value.item_forms) ? ui.value.item_forms : []);
const addForm = computed<JsonRow>(() => ui.value.add_form && typeof ui.value.add_form === "object" ? ui.value.add_form : {});
const addFields = computed<JsonRow[]>(() => Array.isArray(addForm.value.fields) ? addForm.value.fields : []);
const managerTabs = computed<JsonRow[]>(() => (Array.isArray(ui.value.manager_tabs) ? ui.value.manager_tabs : []).filter((tab: JsonRow) => text(tab.key)));
const currentTab = computed<JsonRow | null>(() => managerTabs.value.find((tab) => text(tab.key) === activeManagerTab.value) || managerTabs.value[0] || null);
const groupTabs = computed<JsonRow[]>(() => currentTab.value && Array.isArray(currentTab.value.groups) ? currentTab.value.groups.filter((group: JsonRow) => text(group.key)) : []);
const currentGroup = computed<JsonRow | null>(() => groupTabs.value.find((group) => text(group.key) === activeGroupTab.value) || groupTabs.value[0] || null);
const persistentItems = computed(() => {
  const groups = new Set((Array.isArray(ui.value.persistent_item_groups) ? ui.value.persistent_item_groups : []).map((value: unknown) => text(value).toLowerCase()).filter(Boolean));
  return itemForms.value.filter((item) => groups.has(text(item.group).toLowerCase()));
});
const statsControls = computed<JsonRow[]>(() => Array.isArray(ui.value.stats_controls) ? ui.value.stats_controls : []);
const appearanceClass = computed(() => text(ui.value.appearance) ? `core-settings-manager-${token(ui.value.appearance)}` : "");

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
function isBusy(key: string): boolean { return busyKeys.value.includes(key); }
function setBusy(key: string, active: boolean) {
  const next = new Set(busyKeys.value);
  if (active) next.add(key); else next.delete(key);
  busyKeys.value = [...next];
}
async function run(action: string, payload: JsonRow, busyKey = action, successText = "Done."): Promise<JsonRow | null> {
  const name = text(action);
  if (!name || isBusy(busyKey)) return null;
  setBusy(busyKey, true);
  error.value = "";
  status.value = "Working…";
  try {
    const result = await postJson<JsonRow>(props.actionEndpoint, { action: name, payload: payload || {} });
    const message = text(result?.message) || successText;
    status.value = message;
    const sampleUrl = text(result?.sample_url);
    if (sampleUrl) {
      try { await new Audio(sampleUrl).play(); }
      catch { props.notify?.("The sample is ready, but the browser could not play it.", "error"); }
    }
    await props.refresh();
    props.notify?.(message, "success");
    return result || {};
  } catch (requestError) {
    const message = requestError instanceof Error ? requestError.message : "Core action failed.";
    error.value = message;
    status.value = "";
    props.notify?.(message, "error");
    return null;
  } finally { setBusy(busyKey, false); }
}
function decorate(items: JsonRow[]): JsonRow[] { return items.map((item) => ({ ...item, core_key: props.tab.core_key })); }
function itemsFor(group: unknown): JsonRow[] {
  const wanted = text(group).toLowerCase();
  return decorate(wanted ? itemForms.value.filter((item) => text(item.group).toLowerCase() === wanted) : itemForms.value);
}
function listOptions(source: JsonRow | null): JsonRow {
  if (!source) return {};
  return {
    selector: Boolean(source.selector),
    selector_label: source.selector_label,
    item_group: source.item_group,
    page_size: source.page_size,
    server_pagination: source.server_pagination,
    bulk_actions: source.bulk_actions,
    empty_message: source.empty_message || body.value.empty_message,
  };
}
function setAddValue(field: JsonRow, value: unknown) {
  const key = text(field.key);
  if (!key) return;
  addValues[key] = value;
  addDirty.add(key);
}
async function submitAdd() {
  const action = text(addForm.value.action);
  if (!action) return;
  const result = await run(action, { ...addValues, values: { ...addValues } }, `add:${action}`, text(addForm.value.success_text) || "Saved.");
  if (result) {
    addDirty.clear();
    addFields.value.forEach((field) => { addValues[text(field.key)] = initialValue(field); });
  }
}
async function updateStats(field: JsonRow, value: unknown) {
  const key = text(field.key);
  if (!key) return;
  statsValues[key] = value;
  if (ui.value.stats_controls_auto_save !== false && ui.value.stats_controls_action) {
    await run(text(ui.value.stats_controls_action), { ...statsValues, values: { ...statsValues } }, "stats-controls", "Saved.");
  }
}
async function saveStats() {
  if (ui.value.stats_controls_action) await run(text(ui.value.stats_controls_action), { ...statsValues, values: { ...statsValues } }, "stats-controls", "Saved.");
}

watch(managerTabs, (tabs) => {
  const available = new Set(tabs.map((tab) => text(tab.key)));
  const requested = text(ui.value.default_tab);
  if (!available.has(activeManagerTab.value)) activeManagerTab.value = available.has(requested) ? requested : text(tabs[0]?.key);
}, { immediate: true });
watch(groupTabs, (tabs) => {
  const available = new Set(tabs.map((tab) => text(tab.key)));
  if (!available.has(activeGroupTab.value)) activeGroupTab.value = text(tabs[0]?.key);
}, { immediate: true });
watch(addFields, (fields) => fields.forEach((field) => { const key = text(field.key); if (key && !addDirty.has(key)) addValues[key] = initialValue(field); }), { immediate: true });
watch(statsControls, (fields) => fields.forEach((field) => { const key = text(field.key); if (key) statsValues[key] = initialValue(field); }), { immediate: true });
</script>

<template>
  <div class="tcx-native-panel" data-core-renderer="vue">
    <div v-if="body.error" class="card"><div class="card-head"><h3 class="card-title">{{ tab.label || tab.core_key }}</h3><span class="small">{{ tab.core_key }}</span></div><div class="tv-notice error">{{ body.error }}</div></div>

    <div v-else-if="isManager" class="card core-settings-manager tcx-native-manager" :class="appearanceClass" :data-core-live-updates="ui.live_updates ? '1' : '0'">
      <div class="card-head"><h3 class="card-title">{{ tab.label || tab.core_key }}</h3><span class="small">{{ tab.core_key }}</span></div>
      <div v-if="body.summary" class="small">{{ body.summary }}</div>
      <div v-if="error || status" class="tv-notice compact" :class="{ error: Boolean(error) }">{{ error || status }}</div>

      <CoreManagerItems v-if="persistentItems.length" class="core-manager-persistent" :items="decorate(persistentItems)" :ui="ui" :options="{ empty_message: body.empty_message }" :run="run" :busy="isBusy" />

      <div v-if="stats.length || statsControls.length || ui.stats_refresh_button" class="core-metric-row tcx-native-stats">
        <div v-for="entry in stats" :key="entry.label" class="core-metric-pill"><div class="small">{{ entry.label }}</div><div>{{ entry.value ?? '-' }}</div></div>
        <form v-if="statsControls.length && ui.stats_controls_action" class="inline-row tcx-native-stats-controls" @submit.prevent="saveStats"><CoreManagerField v-for="field in statsControls" :key="field.key" :field="field" :model-value="statsValues[field.key]" :all-values="statsValues" @update:model-value="updateStats(field, $event)" @error="error = $event" /><button v-if="ui.stats_controls_auto_save === false" class="action-btn" type="submit" :disabled="isBusy('stats-controls')">Save</button></form>
        <button v-if="ui.stats_refresh_button" class="action-btn" type="button" @click="refresh">{{ ui.stats_refresh_label || 'Refresh' }}</button>
      </div>

      <div class="card tcx-native-manager-body">
        <div class="card-head"><h3 class="card-title">{{ ui.title || 'Manager' }}</h3></div>
        <nav v-if="managerTabs.length" class="core-manager-tabs" aria-label="Core manager sections"><button v-for="managerTab in managerTabs" :key="managerTab.key" type="button" class="core-manager-tab-btn" :class="{ active: activeManagerTab === managerTab.key }" @click="activeManagerTab = managerTab.key">{{ managerTab.label || managerTab.key }}</button></nav>

        <template v-if="managerTabs.length">
          <form v-if="currentTab?.source === 'add_form'" class="form-grid core-manager-add-form tcx-native-add-form" @submit.prevent="submitAdd">
            <CoreManagerField v-for="field in addFields" :key="field.key" :field="field" :model-value="addValues[field.key]" :all-values="addValues" @update:model-value="setAddValue(field, $event)" @error="error = $event" />
            <div class="inline-row tcx-native-form-actions"><button class="action-btn" type="submit" :disabled="isBusy(`add:${addForm.action}`)">{{ addForm.submit_label || 'Add' }}</button></div>
          </form>

          <div v-else-if="currentTab?.source === 'grouped_items' && groupTabs.length" class="core-manager-subtabs-wrap">
            <nav class="core-manager-subtabs" aria-label="Core item groups"><button v-for="group in groupTabs" :key="group.key" type="button" class="core-manager-subtab-btn" :class="{ active: activeGroupTab === group.key }" @click="activeGroupTab = group.key">{{ group.label || group.key }}</button></nav>
            <CoreManagerItems v-if="currentGroup" :items="itemsFor(currentGroup.item_group || currentGroup.key)" :ui="ui" :options="{ ...listOptions(currentGroup), selector: currentGroup.selector !== false }" :run="run" :busy="isBusy" />
          </div>

          <CoreManagerItems v-else :items="itemsFor(currentTab?.item_group)" :ui="ui" :options="listOptions(currentTab)" :run="run" :busy="isBusy" />
        </template>
        <template v-else>
          <form v-if="addForm.action" class="form-grid core-manager-add-form tcx-native-add-form" @submit.prevent="submitAdd">
            <CoreManagerField v-for="field in addFields" :key="field.key" :field="field" :model-value="addValues[field.key]" :all-values="addValues" @update:model-value="setAddValue(field, $event)" @error="error = $event" />
            <div class="inline-row tcx-native-form-actions"><button class="action-btn" type="submit" :disabled="isBusy(`add:${addForm.action}`)">{{ addForm.submit_label || 'Add' }}</button></div>
          </form>
          <CoreManagerItems :items="itemsFor('')" :ui="ui" :options="{ empty_message: body.empty_message }" :run="run" :busy="isBusy" />
        </template>
      </div>
    </div>

    <div v-else class="card tcx-native-simple">
      <div class="card-head"><h3 class="card-title">{{ tab.label || tab.core_key }}</h3><span class="small">{{ tab.core_key }}</span></div>
      <div v-if="body.summary" class="small">{{ body.summary }}</div>
      <div v-if="stats.length" class="core-metric-row"><div v-for="entry in stats" :key="entry.label" class="core-metric-pill"><div class="small">{{ entry.label }}</div><div>{{ entry.value ?? '-' }}</div></div></div>
      <div v-if="body.items?.length" class="core-tab-items"><article v-for="(item, index) in body.items" :key="item.id || item.title || index" class="core-tab-item"><div class="card-head"><h3 class="card-title">{{ item.title || '(untitled)' }}</h3></div><div v-if="item.subtitle" class="small">{{ item.subtitle }}</div><div v-if="item.detail" class="muted">{{ item.detail }}</div></article></div>
      <div v-else class="tv-empty compact">{{ body.empty_message || 'No data available for this tab.' }}</div>
    </div>
  </div>
</template>
