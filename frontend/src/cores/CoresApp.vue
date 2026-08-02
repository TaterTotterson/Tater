<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, reactive, ref, watch } from "vue";
import MusicCoreApp from "../music/MusicCoreApp.vue";
import type { CoreTabPayload, MusicCoreMountOptions } from "../music/types";
import ManifestField from "../shared/ManifestField.vue";
import PopupTransition from "../shared/PopupTransition.vue";
import { getJson, postJson } from "../shared/api";
import LegacyCorePanel from "./components/LegacyCorePanel.vue";
import type { CoreTabSpec, CoresMountOptions, JsonRow } from "./types";

const props = defineProps<{ state: { payload: JsonRow }; options: CoresMountOptions }>();
const manageTabs = [
  { id: "installed", label: "Installed" },
  { id: "store", label: "Store" },
  { id: "manage", label: "Maintenance" },
  { id: "repos", label: "Repositories" },
];
const activeTab = ref(String(props.options.initialTab || "manage"));
const manageTab = ref("installed");
const busy = ref("");
const status = ref("");
const error = ref("");
const purgeIds = ref<Record<string, boolean>>({});
const repoName = ref("");
const repoUrl = ref("");
const draftRepos = ref<JsonRow[]>([]);
const settingsCore = ref<JsonRow | null>(null);
const fieldValues = ref<JsonRow>({});
const panelStates = reactive<Record<string, { payload: JsonRow }>>({});
const panelLoading = ref<Record<string, boolean>>({});
let activeEvents: EventSource | null = null;
let reconnectTimer = 0;

const runtime = computed(() => props.state.payload?.runtime || {});
const shop = computed(() => props.state.payload?.shop || {});
const tabsPayload = computed(() => props.state.payload?.tabs || {});
const runtimeItems = computed(() => Array.isArray(runtime.value.items) ? runtime.value.items : []);
const installed = computed(() => Array.isArray(shop.value.installed) ? shop.value.installed : []);
const catalog = computed(() => Array.isArray(shop.value.catalog) ? shop.value.catalog : []);
const available = computed(() => catalog.value.filter((row: JsonRow) => !row.installed).sort(compareRows));
const updates = computed(() => installed.value.filter((row: JsonRow) => row.update_available));
const runningCount = computed(() => runtimeItems.value.filter((row: JsonRow) => Boolean(row.running)).length);
const dynamicTabs = computed<CoreTabSpec[]>(() => (Array.isArray(tabsPayload.value.tabs) ? tabsPayload.value.tabs : [])
  .filter((tab: JsonRow) => text(tab.core_key))
  .map((tab: JsonRow) => ({ ...tab, core_key: text(tab.core_key) })));
const availableTopTabs = computed(() => new Set(["manage", ...dynamicTabs.value.map((tab) => tab.core_key)]));
const activeSpec = computed(() => dynamicTabs.value.find((tab) => tab.core_key === activeTab.value) || null);
const activePanelState = computed(() => panelStates[activeTab.value] || null);
const activePayload = computed(() => activePanelState.value?.payload || {});
const activeIsMusic = computed(() => text(activePayload.value?.ui?.appearance).toLowerCase() === "music_library");
const runtimeByKey = computed(() => new Map(runtimeItems.value.map((row: JsonRow) => [canonical(row.key), row])));
const installedByRuntimeKey = computed(() => {
  const result = new Map<string, JsonRow>();
  installed.value.forEach((row: JsonRow) => {
    const moduleKey = text(row.module_key || `${row.id}_core`);
    if (moduleKey) result.set(canonical(moduleKey), row);
    if (row.id) result.set(canonical(row.id), row);
  });
  return result;
});
const installedRows = computed(() => {
  const seen = new Set<string>();
  const rows: Array<{ key: string; runtime: JsonRow | null; shop: JsonRow | null }> = runtimeItems.value.map((runtimeRow: JsonRow) => {
    const key = text(runtimeRow.key);
    const shopRow = installedByRuntimeKey.value.get(canonical(key)) || installedByRuntimeKey.value.get(canonical(stripCoreSuffix(key))) || null;
    if (shopRow) seen.add(canonical(shopRow.id));
    return { key, runtime: runtimeRow, shop: shopRow };
  });
  installed.value.forEach((shopRow: JsonRow) => {
    if (!seen.has(canonical(shopRow.id))) rows.push({ key: text(shopRow.module_key || `${shopRow.id}_core`), runtime: null, shop: shopRow });
  });
  return rows.sort((a, b) => rowName(a).localeCompare(rowName(b), undefined, { sensitivity: "base", numeric: true }));
});
const musicOptions = computed<MusicCoreMountOptions | null>(() => {
  const key = activeSpec.value?.core_key;
  if (!key) return null;
  const encoded = encodeURIComponent(key);
  return {
    initialPayload: activePayload.value as CoreTabPayload,
    coreKey: key,
    tabEndpoint: `${props.options.endpoints.runtime}/${encoded}/tab`,
    actionEndpoint: `${props.options.endpoints.runtime}/${encoded}/tab-action`,
    eventsEndpoint: `${props.options.endpoints.runtime}/${encoded}/tab-events`,
  };
});

function text(value: unknown): string { return String(value ?? "").trim(); }
function canonical(value: unknown): string { return text(value).toLowerCase(); }
function encode(value: unknown): string { return encodeURIComponent(text(value)); }
function stripCoreSuffix(value: unknown): string { return text(value).replace(/_core$/i, ""); }
function compareRows(a: JsonRow, b: JsonRow): number { return text(a.name || a.id).localeCompare(text(b.name || b.id), undefined, { sensitivity: "base", numeric: true }); }
function rowName(row: { key: string; runtime: JsonRow | null; shop: JsonRow | null }): string { return text(row.runtime?.label || row.shop?.name || stripCoreSuffix(row.key)); }
function rowDescription(row: { runtime: JsonRow | null; shop: JsonRow | null }): string { return text(row.shop?.description || "Local Core module."); }
function runtimeForShop(row: JsonRow): JsonRow | null {
  const moduleKey = text(row.module_key || `${row.id}_core`);
  return runtimeByKey.value.get(canonical(moduleKey)) || runtimeByKey.value.get(canonical(row.id)) || null;
}
function stateLabel(row: JsonRow | null): string {
  if (!row) return "Unavailable";
  if (row.running) return "Running";
  return row.desired_running ? "Pending start" : "Stopped";
}
function notify(message: string, tone = "success") {
  status.value = message;
  error.value = tone === "error" ? message : "";
  props.options.onToast?.(message, tone);
}
function syncDraftRepos() {
  draftRepos.value = Array.isArray(shop.value.repos?.additional) ? shop.value.repos.additional.map((row: JsonRow) => ({ ...row })) : [];
}
function ensurePanelState(key: string) {
  if (!panelStates[key]) panelStates[key] = { payload: {} };
  return panelStates[key];
}

async function refresh(quiet = false) {
  if (!quiet) busy.value = "Refreshing Cores…";
  error.value = "";
  try {
    const [runtimeResult, shopResult, tabsResult] = await Promise.all([
      getJson<JsonRow>(props.options.endpoints.runtime),
      getJson<JsonRow>(props.options.endpoints.shop),
      getJson<JsonRow>(props.options.endpoints.tabs),
    ]);
    props.state.payload = { runtime: runtimeResult, shop: shopResult, tabs: tabsResult };
    syncDraftRepos();
    if (!availableTopTabs.value.has(activeTab.value)) await selectTopTab("manage");
    else if (activeTab.value !== "manage") await refreshTab(activeTab.value, true);
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : "Core refresh failed.", "error");
  } finally { if (!quiet) busy.value = ""; }
}

async function refreshTab(key: string, quiet = false) {
  const coreKey = text(key);
  if (!coreKey || coreKey === "manage") { await refresh(quiet); return; }
  panelLoading.value = { ...panelLoading.value, [coreKey]: true };
  try {
    const result = await getJson<JsonRow>(`${props.options.endpoints.runtime}/${encode(coreKey)}/tab`);
    ensurePanelState(coreKey).payload = result || {};
    if (coreKey === activeTab.value && !isMusicPayload(result)) connectEvents(coreKey, result);
  } catch (requestError) {
    ensurePanelState(coreKey).payload = { error: requestError instanceof Error ? requestError.message : "Core panel failed to load." };
  } finally {
    panelLoading.value = { ...panelLoading.value, [coreKey]: false };
  }
}

function isMusicPayload(payload: JsonRow): boolean { return text(payload?.ui?.appearance).toLowerCase() === "music_library"; }
function closeEvents() {
  activeEvents?.close();
  activeEvents = null;
  if (reconnectTimer) window.clearTimeout(reconnectTimer);
  reconnectTimer = 0;
}
function connectEvents(coreKey: string, payload: JsonRow) {
  closeEvents();
  if (activeTab.value !== coreKey || isMusicPayload(payload) || !payload?.ui?.live_updates) return;
  activeEvents = new EventSource(`${props.options.endpoints.runtime}/${encode(coreKey)}/tab-events`);
  activeEvents.addEventListener("core-tab", (event) => {
    try { ensurePanelState(coreKey).payload = JSON.parse((event as MessageEvent<string>).data); } catch { /* Keep current panel. */ }
  });
  activeEvents.addEventListener("error", () => {
    activeEvents?.close();
    activeEvents = null;
    if (activeTab.value === coreKey) reconnectTimer = window.setTimeout(() => connectEvents(coreKey, ensurePanelState(coreKey).payload), 3000);
  });
}

async function selectTopTab(key: string) {
  const next = availableTopTabs.value.has(key) ? key : "manage";
  activeTab.value = next;
  props.options.onTabChange?.(next);
  closeEvents();
  if (next !== "manage") {
    const state = ensurePanelState(next);
    if (!Object.keys(state.payload).length) await refreshTab(next);
    else if (!isMusicPayload(state.payload)) connectEvents(next, state.payload);
  }
}

async function runtimeAction(row: JsonRow, action: "start" | "stop") {
  const key = text(row.key);
  if (!key) return;
  busy.value = `${action === "start" ? "Starting" : "Stopping"} ${key}…`;
  try {
    await postJson<JsonRow>(`${props.options.endpoints.runtime}/${encode(key)}/${action}`);
    notify(`${text(row.label || key)} ${action === "start" ? "started" : "stopped"}.`);
    await refresh(true);
    props.options.onHealthRefresh?.();
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : `Core ${action} failed.`, "error"); }
  finally { busy.value = ""; }
}

async function shopAction(action: string, id = "") {
  if (action === "remove" && !window.confirm(`Remove ${id}?${purgeIds.value[id] ? " Its saved data will also be deleted." : ""}`)) return;
  busy.value = `${action.replaceAll("-", " ")} ${id || "Cores"}…`;
  try {
    const payload: JsonRow = id ? { id } : {};
    if (action === "remove") payload.purge_redis = Boolean(purgeIds.value[id]);
    const result = await postJson<JsonRow>(`${props.options.endpoints.shop}/${action}`, payload);
    const updated = Array.isArray(result.updated) ? result.updated.length : 0;
    const failed = Array.isArray(result.failed) ? result.failed.length : 0;
    notify(text(result.message) || (action === "update-all" ? `Update-all completed. Updated ${updated}, failed ${failed}.` : "Core action completed."), failed ? "error" : "success");
    await refresh(true);
    if (action === "install") manageTab.value = "installed";
    props.options.onHealthRefresh?.();
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Core action failed.", "error"); }
  finally { busy.value = ""; }
}

function normalizeValue(field: JsonRow): unknown {
  const raw = field.value ?? field.default ?? "";
  const type = text(field.type).toLowerCase();
  if (type === "checkbox") return typeof raw === "string" ? ["1", "true", "yes", "on", "enabled"].includes(raw.toLowerCase()) : Boolean(raw);
  if (type === "number" || type === "range") return raw === "" ? "" : Number(raw);
  if (type === "multiselect") return Array.isArray(raw) ? [...raw] : text(raw).split(",").map((value) => value.trim()).filter(Boolean);
  return raw;
}
function fieldVisible(field: JsonRow): boolean {
  const conditions = Array.isArray(field.show_when_all) ? field.show_when_all : field.show_when && typeof field.show_when === "object" ? [field.show_when] : [];
  return conditions.every((condition: JsonRow) => {
    const source = text(condition.source_key ?? condition.key);
    if (!source) return true;
    const allowed = [...(condition.any_of || []), ...(condition.values || []), ...(condition.equals !== undefined ? [condition.equals] : []), ...(condition.value !== undefined ? [condition.value] : [])].map((value) => String(value ?? "").trim());
    const current = typeof fieldValues.value[source] === "boolean" ? fieldValues.value[source] ? "true" : "false" : String(fieldValues.value[source] ?? "").trim();
    return !allowed.length || allowed.includes(current);
  });
}
function openSettings(row: JsonRow) {
  settingsCore.value = row;
  fieldValues.value = Object.fromEntries((Array.isArray(row.settings) ? row.settings : []).filter((field: JsonRow) => text(field.key)).map((field: JsonRow) => [text(field.key), normalizeValue(field)]));
}
async function saveSettings() {
  const core = settingsCore.value;
  if (!core) return;
  const key = text(core.key);
  busy.value = `Saving ${text(core.label || key)}…`;
  try {
    const values = Object.fromEntries((core.settings || []).filter((field: JsonRow) => text(field.key) && !["section", "header", "readonly", "read_only", "led_preview"].includes(text(field.type).toLowerCase()) && fieldVisible(field)).map((field: JsonRow) => [text(field.key), fieldValues.value[text(field.key)]]));
    await postJson<JsonRow>(`${props.options.endpoints.runtime}/${encode(key)}/settings`, { values });
    notify(`Saved settings for ${text(core.label || key)}.`);
    settingsCore.value = null;
    await refresh(true);
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Core settings save failed.", "error"); }
  finally { busy.value = ""; }
}

function addRepo() {
  const url = repoUrl.value.trim();
  if (!url) { notify("Repository URL is required.", "error"); return; }
  if (draftRepos.value.some((row) => text(row.url).toLowerCase() === url.toLowerCase())) { notify("That repository is already added.", "error"); return; }
  draftRepos.value.push({ name: repoName.value.trim(), url });
  repoName.value = ""; repoUrl.value = ""; status.value = "Repository added. Save repositories to apply it."; error.value = "";
}
async function saveRepos() {
  busy.value = "Saving Core repositories…";
  try { await postJson<JsonRow>(`${props.options.endpoints.shop}/repos`, { repos: draftRepos.value }); notify("Core repositories saved."); await refresh(true); }
  catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Repository save failed.", "error"); }
  finally { busy.value = ""; }
}
function handleEscape(event: KeyboardEvent) { if (event.key === "Escape") settingsCore.value = null; }

watch(() => props.state.payload, syncDraftRepos, { deep: false });
watch(dynamicTabs, (tabs) => { if (!new Set(["manage", ...tabs.map((tab) => tab.core_key)]).has(activeTab.value)) void selectTopTab("manage"); }, { immediate: true });
syncDraftRepos();
window.addEventListener("keydown", handleEscape);
onBeforeUnmount(() => { closeEvents(); window.removeEventListener("keydown", handleEscape); });
nextTick(() => void selectTopTab(activeTab.value));
defineExpose({ refresh: () => refresh(false), refreshTab: (key: string) => refreshTab(key, true) });
</script>

<template>
  <div class="tater-vue-surface tcx-cores">
    <header class="tv-page-heading"><div><span class="tv-eyebrow">System capabilities</span><h1>Cores</h1><p>Run, configure, browse, and update Tater’s capability modules from one live workspace.</p></div><div class="tv-heading-actions"><span class="tv-live-pill" :class="{ busy: Boolean(busy) }"><i />{{ busy || 'Live' }}</span><button class="tv-button" type="button" @click="refresh()">Refresh</button></div></header>
    <div class="tv-metrics"><div><span>Installed</span><strong>{{ installed.length || runtimeItems.length }}</strong></div><div><span>Running</span><strong>{{ runningCount }}</strong></div><div><span>Panels</span><strong>{{ dynamicTabs.length }}</strong></div><div><span>Updates</span><strong>{{ Number(shop.updates_available || updates.length) }}</strong></div></div>
    <div v-if="status || error" class="tv-notice" :class="{ error: Boolean(error) }">{{ error || status }}</div>
    <div v-if="shop.errors?.length" class="tv-notice error">{{ shop.errors.join(' • ') }}</div>

    <nav class="tv-tabs tcx-top-tabs core-top-tabs" aria-label="Core panels">
      <button v-for="tab in dynamicTabs" :key="tab.core_key" type="button" class="core-top-tab-btn" :class="{ active: activeTab === tab.core_key }" :data-core-tab="tab.core_key" @click="selectTopTab(tab.core_key)">{{ tab.label || tab.core_key }}<span v-if="tab.requires_running && !tab.running" class="tcx-tab-dot" title="Core is stopped" /></button>
      <button type="button" class="core-top-tab-btn" :class="{ active: activeTab === 'manage' }" data-core-tab="manage" @click="selectTopTab('manage')">{{ tabsPayload.manage_label || 'Manage' }}<span v-if="updates.length">{{ updates.length }}</span></button>
    </nav>

    <section v-if="activeTab !== 'manage'" class="core-top-tab-panel active tcx-core-panel" :data-core-tab-panel="activeTab" :data-core-tab-loaded="panelLoading[activeTab] ? 'loading' : '1'">
      <div v-if="panelLoading[activeTab] && !Object.keys(activePayload).length" class="tv-empty">Loading {{ activeSpec?.label || activeTab }}…</div>
      <MusicCoreApp v-else-if="activeSpec && activePanelState && activeIsMusic && musicOptions" :state="activePanelState as { payload: CoreTabPayload }" :options="musicOptions" />
      <LegacyCorePanel v-else-if="activeSpec && activePanelState" :payload="activePayload" :tab="activeSpec" :render="options.renderCorePanel" :clear="options.clearCorePanel" />
    </section>

    <section v-else class="core-top-tab-panel active tcx-manage" data-core-tab-panel="manage">
      <nav class="tv-mini-tabs tcx-manage-tabs" aria-label="Core management">
        <button v-for="tab in manageTabs" :key="tab.id" type="button" :class="{ active: manageTab === tab.id }" @click="manageTab = tab.id">{{ tab.label }}<span v-if="tab.id === 'manage' && updates.length">{{ updates.length }}</span></button>
      </nav>

      <div v-if="manageTab === 'installed'" class="tcx-card-grid">
        <article v-for="row in installedRows" :key="row.key" class="tv-panel tcx-core-card"><header><div><span class="tv-eyebrow">{{ row.key }}</span><h2>{{ rowName(row) }}</h2></div><span class="tv-state" :class="{ good: row.runtime?.running, pending: row.runtime?.desired_running && !row.runtime?.running }">{{ stateLabel(row.runtime) }}</span></header><p>{{ rowDescription(row) }}</p><div class="tp-version"><span>Installed {{ row.shop?.installed_ver || '0.0.0' }}</span><span>Store {{ row.shop?.store_ver || '-' }}</span><span>{{ row.shop?.source_label || 'local' }}</span></div><footer><button v-if="row.runtime?.settings?.length" class="tv-button" type="button" @click="openSettings(row.runtime)">Settings</button><span v-else>{{ row.runtime ? 'No configurable settings' : 'Runtime unavailable' }}</span><button v-if="row.runtime" class="tv-button" :class="{ primary: !row.runtime.running }" type="button" @click="runtimeAction(row.runtime, row.runtime.running ? 'stop' : 'start')">{{ row.runtime.running ? 'Stop' : 'Start' }}</button></footer></article>
        <div v-if="!installedRows.length" class="tv-empty">No installed Cores found.</div>
      </div>

      <div v-else-if="manageTab === 'store'" class="tcx-card-grid">
        <article v-for="row in available" :key="row.id" class="tv-panel tcx-core-card"><header><div><span class="tv-eyebrow">{{ row.id }}</span><h2>{{ row.name || row.id }}</h2></div><span class="tv-state">v{{ row.version || '-' }}</span></header><p>{{ row.description || 'No description provided.' }}</p><footer><span>{{ row.source_label || 'Tater Shop' }}</span><button class="tv-button primary" type="button" @click="shopAction('install', row.id)">Install</button></footer></article><div v-if="!available.length" class="tv-empty">No additional Cores are available from the configured repositories.</div>
      </div>

      <div v-else-if="manageTab === 'manage'" class="tcx-manage-list">
        <div class="tv-panel tcx-manage-toolbar"><div><span class="tv-eyebrow">Maintenance</span><h2>Manage installed Cores</h2><p>{{ updates.length }} update{{ updates.length === 1 ? '' : 's' }} available. Running Cores restart automatically after an update.</p></div><button class="tv-button primary" type="button" :disabled="!updates.length" @click="shopAction('update-all')">Update all</button></div>
        <article v-for="row in installed.slice().sort(compareRows)" :key="row.id" class="tv-panel tcx-manage-row"><div><strong>{{ row.name || row.id }}</strong><span>{{ row.installed_ver || '0.0.0' }} → {{ row.store_ver || '-' }} · {{ stateLabel(runtimeForShop(row)) }}</span></div><div class="ti-row-actions"><button class="tv-button" type="button" :disabled="!row.update_available" @click="shopAction('update', row.id)">{{ row.update_available ? 'Update' : 'Current' }}</button><button v-if="runtimeForShop(row)" class="tv-button" type="button" @click="runtimeAction(runtimeForShop(row)!, runtimeForShop(row)?.running ? 'stop' : 'start')">{{ runtimeForShop(row)?.running ? 'Stop' : 'Start' }}</button><label class="ti-purge"><input v-model="purgeIds[row.id]" type="checkbox" /> Delete data</label><button class="tv-button danger" type="button" @click="shopAction('remove', row.id)">Remove</button></div></article><div v-if="!installed.length" class="tv-empty">No installed Cores found.</div>
      </div>

      <div v-else class="tv-panel tcx-repos"><header><div><span class="tv-eyebrow">Trusted sources</span><h2>Core repositories</h2><p>The built-in Core repository stays available. Add other trusted manifests below.</p></div></header><article class="ti-repo-row builtin"><div><strong>{{ shop.repos?.default?.name || 'Default' }}</strong><code>{{ shop.repos?.default?.url || '(not set)' }}</code></div><span>Built-in</span></article><article v-for="(repo, index) in draftRepos" :key="`${repo.url}-${index}`" class="ti-repo-row"><div><strong>{{ repo.name || 'Additional repository' }}</strong><code>{{ repo.url }}</code></div><button class="tv-button" type="button" @click="draftRepos.splice(index, 1)">Remove</button></article><div v-if="!draftRepos.length" class="tv-empty compact">No additional repositories configured.</div><div class="tcx-repo-form"><label><span>Name (optional)</span><input v-model="repoName" type="text" placeholder="My Core Repo" /></label><label><span>Repository URL</span><input v-model="repoUrl" type="url" placeholder="https://example.com/cores.json" @keyup.enter="addRepo" /></label><button class="tv-button" type="button" @click="addRepo">Add</button><button class="tv-button primary" type="button" @click="saveRepos">Save repositories</button></div></div>
    </section>
  </div>

  <PopupTransition :open="Boolean(settingsCore)" @close="settingsCore = null"><section class="tv-modal tcx-settings-modal" role="dialog" aria-modal="true"><header><div><span class="tv-eyebrow">Core settings</span><h2>{{ settingsCore?.label || settingsCore?.key }}</h2><p>Changes are applied to this Core’s runtime configuration.</p></div><button class="tv-button" type="button" @click="settingsCore = null">Close</button></header><div class="tvb-field-grid"><ManifestField v-for="field in settingsCore?.settings || []" :key="field.key || field.label" :field="field" :model-value="fieldValues[field.key]" :all-values="fieldValues" @update:model-value="fieldValues[field.key] = $event" /></div><footer><span>{{ busy || 'Ready' }}</span><button class="tv-button primary" type="button" @click="saveSettings">Save settings</button></footer></section></PopupTransition>
</template>
