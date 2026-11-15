<script setup lang="ts">
import { computed, nextTick, onActivated, onBeforeUnmount, onDeactivated, reactive, ref, watch } from "vue";
import MusicCoreApp from "../music/MusicCoreApp.vue";
import type { CoreTabPayload, MusicCoreMountOptions } from "../music/types";
import CatalogToolbar from "../shared/CatalogToolbar.vue";
import UiIcon from "../shared/UiIcon.vue";
import { catalogMatches } from "../shared/catalogSearch";
import ManifestField from "../shared/ManifestField.vue";
import PopupTransition from "../shared/PopupTransition.vue";
import { getJson, postJson } from "../shared/api";
import CorePanelRenderer from "./components/CorePanelRenderer.vue";
import { coreWorkspace } from "./workspace";
import type { CoreTabSpec, CoresMountOptions, JsonRow } from "./types";

const props = defineProps<{ state: { payload: JsonRow }; options: CoresMountOptions }>();
const initialTab=String(props.options.initialTab||'workspaces');
const activeTab=ref(initialTab==='manage'?'workspaces':initialTab);
const activeArea=ref(['installed','store','updates','repos'].includes(activeTab.value)?activeTab.value:'workspaces');
const areas=[{id:'workspaces',label:'Workspaces'},{id:'installed',label:'Installed'},{id:'store',label:'Store'},{id:'updates',label:'Updates'},{id:'repos',label:'Repositories'}];
const manageTab=ref(activeArea.value==='updates'?'manage':activeArea.value);
const reposDirty=ref(false);
const busy = ref("");
const status = ref("");
const error = ref("");
const purgeIds = ref<Record<string, boolean>>({});
const repoName = ref("");
const repoUrl = ref("");
const draftRepos = ref<JsonRow[]>([]);
const repoSection = ref("trusted");
const settingsCore = ref<JsonRow | null>(null);
const fieldValues = ref<JsonRow>({});
const panelStates = reactive<Record<string, { payload: JsonRow }>>({});
const panelLoading = ref<Record<string, boolean>>({});
let activeEvents: EventSource | null = null;
let reconnectTimer = 0;
let suspended=false;let disposed=false;
const panelRequests=new Map<string,Promise<void>>();

const runtime = computed(() => props.state.payload?.runtime || {});
const shop = computed(() => props.state.payload?.shop || {});
const tabsPayload = computed(() => props.state.payload?.tabs || {});
const runtimeItems = computed(() => Array.isArray(runtime.value.items) ? runtime.value.items : []);
const installed = computed(() => Array.isArray(shop.value.installed) ? shop.value.installed : []);
const catalog = computed(() => Array.isArray(shop.value.catalog) ? shop.value.catalog : []);
const available = computed(() => catalog.value.filter((row: JsonRow) => !row.installed).sort(compareRows));
const updates = computed(() => installed.value.filter((row: JsonRow) => row.update_available));
const trustedRepos = computed(() => Array.isArray(shop.value.repos?.trusted) ? shop.value.repos.trusted : []);
const dynamicTabs = computed<CoreTabSpec[]>(() => (Array.isArray(tabsPayload.value.tabs) ? tabsPayload.value.tabs : [])
  .filter((tab: JsonRow) => text(tab.core_key))
  .map((tab: JsonRow) => ({ ...tab, core_key: text(tab.core_key) }))
  .sort((a: CoreTabSpec, b: CoreTabSpec) => text(a.label || a.core_key).localeCompare(text(b.label || b.core_key), undefined, { sensitivity: "base", numeric: true })));
const availableTopTabs = computed(() => new Set(["workspaces","manage","installed","store","updates","repos", ...dynamicTabs.value.map((tab) => tab.core_key)]));
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
    onToast: props.options.onToast,
    embedded: true,
  };
});

const searchQuery = ref("");
const filteredInstalledRows = computed(() => installedRows.value.filter((row) => catalogMatches(searchQuery.value, row.key, rowName(row), rowDescription(row))));
const filteredAvailable = computed(() => available.value.filter((row: JsonRow) => catalogMatches(searchQuery.value, row.id, row.name, row.description, row.source_label)));
const filteredManaged = computed(() => installed.value.filter((row: JsonRow) => (!['updates'].includes(activeArea.value)||row.update_available)&&catalogMatches(searchQuery.value, row.id, row.name, row.description, row.source_label)).slice().sort(compareRows));
const libraryRows=computed(()=>installedRows.value.map(row=>({...row,tab:dynamicTabs.value.find(tab=>canonical(tab.core_key)===canonical(row.key))||null,workspace:coreWorkspace(row.key,dynamicTabs.value.find(tab=>canonical(tab.core_key)===canonical(row.key))?.workspace)})).filter(row=>catalogMatches(searchQuery.value,workspaceName(row),row.workspace.description,row.key)));
const activeWorkspace=computed(()=>coreWorkspace(activeSpec.value?.core_key,activePayload.value.ui?.workspace||activeSpec.value?.workspace));
const activeRuntime=computed<JsonRow|null>(()=>runtimeByKey.value.get(canonical(activeSpec.value?.core_key)) as JsonRow||null);
function workspaceName(row:any){return text(row.tab?.label||row.runtime?.label||row.shop?.name||stripCoreSuffix(row.key)).replace(/(?: Core)? Settings$/i,'').replace(/ Core$/i,'');}
function selectArea(id:string){activeArea.value=id;manageTab.value=id==='updates'?'manage':id;activeTab.value=id;searchQuery.value='';closeEvents();props.options.onTabChange?.(id);}
function openAvailable(row:JsonRow){selectArea('store');searchQuery.value=text(row.name||row.id);}
function removeRepo(index:number){draftRepos.value.splice(index,1);reposDirty.value=true;}

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
function syncDraftRepos(force=false) {
  if(reposDirty.value&&!force)return;
  const trustedUrls = new Set(trustedRepos.value.map((row: JsonRow) => text(row.url).toLowerCase()).filter(Boolean));
  draftRepos.value = Array.isArray(shop.value.repos?.additional)
    ? shop.value.repos.additional.filter((row: JsonRow) => !trustedUrls.has(text(row.url).toLowerCase())).map((row: JsonRow) => ({ ...row }))
    : [];
}
function selectedTrustedRepos(): JsonRow[] {
  return trustedRepos.value.filter((row: JsonRow) => Boolean(row.enabled)).map((row: JsonRow) => ({ name: text(row.name), url: text(row.url) }));
}
function combinedRepos(): JsonRow[] { return [...selectedTrustedRepos(), ...draftRepos.value]; }
async function toggleTrustedRepo(repo: JsonRow) {
  if (busy.value) return;
  const wasEnabled = Boolean(repo.enabled);
  const enabling = !repo.enabled;
  repo.enabled = enabling;
  busy.value = `${enabling ? "Adding" : "Removing"} ${text(repo.name || repo.repository)}…`;
  try {
    await postJson<JsonRow>(`${props.options.endpoints.shop}/repos`, { repos: [...selectedTrustedRepos(),...(shop.value.repos?.additional||[]).filter((row:JsonRow)=>!trustedRepos.value.some((repo:JsonRow)=>canonical(repo.url)===canonical(row.url)))] });
    notify(`${text(repo.name || repo.repository)} ${enabling ? "added to" : "removed from"} the Core Store.`);
    await refresh(true);
  } catch (requestError) { repo.enabled = wasEnabled; notify(requestError instanceof Error ? requestError.message : "Trusted repository update failed.", "error"); }
  finally { busy.value = ""; }
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
    if (!availableTopTabs.value.has(activeTab.value)) await selectTopTab("workspaces");
    else if (activeSpec.value) await refreshTab(activeTab.value, true);
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : "Core refresh failed.", "error");
  } finally { if (!quiet) busy.value = ""; }
}

async function refreshTab(key:string,quiet=false):Promise<void>{
  const coreKey=text(key);if(!coreKey||areas.some(area=>area.id===coreKey)||coreKey==='manage'){await refresh(quiet);return;}
  const pending=panelRequests.get(coreKey);if(pending)return pending;
  const request=(async()=>{panelLoading.value={...panelLoading.value,[coreKey]:true};
    try{const result=await getJson<JsonRow>(`${props.options.endpoints.runtime}/${encode(coreKey)}/tab`);if(disposed)return;ensurePanelState(coreKey).payload=result||{};if(coreKey===activeTab.value&&!isMusicPayload(result))connectEvents(coreKey,result);}
    catch(err){if(!disposed){const state=ensurePanelState(coreKey);const message=err instanceof Error?err.message:'Core panel failed to load.';state.payload=Object.keys(state.payload).length?{...state.payload,load_error:message}:{error:message};}}
    finally{panelLoading.value={...panelLoading.value,[coreKey]:false};panelRequests.delete(coreKey);}
  })();panelRequests.set(coreKey,request);return request;
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
  if (disposed||suspended||activeTab.value !== coreKey || isMusicPayload(payload) || !payload?.ui?.live_updates) return;
  const source = new EventSource(`${props.options.endpoints.runtime}/${encode(coreKey)}/tab-events`);activeEvents=source;
  source.addEventListener("core-tab", (event) => {
    try { if(activeEvents===source&&activeTab.value===coreKey&&!suspended)ensurePanelState(coreKey).payload = JSON.parse((event as MessageEvent<string>).data); } catch { /* Keep current panel. */ }
  });
  source.addEventListener("error", () => {
    if(activeEvents!==source)return;source.close();
    activeEvents = null;
    if (!disposed&&!suspended&&activeTab.value === coreKey) reconnectTimer = window.setTimeout(() => connectEvents(coreKey, ensurePanelState(coreKey).payload), 3000);
  });
}

function workspaceRefresher(key:string){return ()=>refreshTab(key,true);}

async function selectTopTab(key:string){
  const next=key==='manage'?'installed':availableTopTabs.value.has(key)?key:'workspaces';
  if(areas.some(area=>area.id===next)){selectArea(next);return;}
  activeArea.value='workspaces';activeTab.value=next;props.options.onTabChange?.(next);closeEvents();
  const state=ensurePanelState(next);if(!Object.keys(state.payload).length)await refreshTab(next);else if(!isMusicPayload(state.payload))connectEvents(next,state.payload);
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
    if (action === "install") selectArea("installed");
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
  if ([...draftRepos.value, ...trustedRepos.value].some((row) => text(row.url).toLowerCase() === url.toLowerCase())) { notify("That repository is already listed.", "error"); return; }
  draftRepos.value.push({ name: repoName.value.trim(), url });reposDirty.value=true;
  repoName.value = ""; repoUrl.value = ""; status.value = "Repository added. Save repositories to apply it."; error.value = "";
}
async function saveRepos() {
  busy.value = "Saving Core repositories…";
  try { await postJson<JsonRow>(`${props.options.endpoints.shop}/repos`, { repos: combinedRepos() }); reposDirty.value=false;notify("Core repositories saved."); await refresh(true);syncDraftRepos(true); }
  catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Repository save failed.", "error"); }
  finally { busy.value = ""; }
}
function handleEscape(event: KeyboardEvent) { if (event.key === "Escape") settingsCore.value = null; }

watch(() => props.state.payload, ()=>syncDraftRepos(), { deep: false });
watch(dynamicTabs, (tabs) => { if (!new Set(["workspaces","installed","store","updates","repos","manage", ...tabs.map((tab) => tab.core_key)]).has(activeTab.value)) void selectTopTab("workspaces"); }, { immediate: true });
syncDraftRepos();
window.addEventListener("keydown", handleEscape);
onDeactivated(()=>{suspended=true;closeEvents();});
onActivated(()=>{if(!suspended)return;suspended=false;if(activeSpec.value)void refreshTab(activeTab.value,true);});
onBeforeUnmount(() => { disposed=true;closeEvents(); window.removeEventListener("keydown", handleEscape); });
nextTick(() => void selectTopTab(activeTab.value));
defineExpose({ refresh: () => refresh(false), refreshTab: (key: string) => refreshTab(key, true), select: selectTopTab });
</script>

<template>
  <div class="tater-vue-surface tcx-cores cw-cores">
    <header class="cw-library-heading"><div><h1>Cores</h1><p>Your capabilities, organized into focused workspaces.</p></div><div class="tv-heading-actions"><span v-if="busy" class="cw-muted" role="status">{{ busy }}</span><button class="tv-button" type="button" :disabled="Boolean(busy)" @click="refresh()"><UiIcon name="refresh" />Refresh</button></div></header>
    <nav class="cw-library-tabs" aria-label="Cores navigation"><button v-for="area in areas" :key="area.id" type="button" :aria-current="activeArea===area.id?'page':undefined" @click="selectArea(area.id)">{{ area.label }}<span v-if="area.id==='updates'&&updates.length">{{ updates.length }}</span></button></nav>
    <p v-if="error||status" class="tv-notice" :class="{error:Boolean(error)}" :role="error?'alert':'status'">{{ error||status }}<button class="cw-text-button" type="button" @click="status='';error=''">Dismiss</button></p>
    <div v-if="shop.errors?.length" class="tv-notice error">{{ shop.errors.join(' • ') }}</div>
    <section v-if="activeTab==='workspaces'" class="cw-library-home" aria-label="Core workspaces"><header><h2>Your workspaces</h2><span>{{ installedRows.length }} installed · {{ available.length }} available in Store</span></header><CatalogToolbar v-model="searchQuery" label="Find a core workspace" :count="libraryRows.length" /><div class="cw-workspace-grid"><button v-for="row in libraryRows" :key="row.key" class="cw-workspace-card" type="button" @click="row.tab?selectTopTab(row.key):selectArea('installed')"><header><UiIcon :name="row.workspace.icon" /><span class="cw-core-state" :class="{running:row.runtime?.running}"><i />{{ stateLabel(row.runtime) }}</span></header><h3>{{ workspaceName(row) }}</h3><p>{{ row.workspace.description }}</p><footer><span>{{ row.tab?'Open workspace':'Manage core' }}</span><UiIcon name="external" /></footer></button></div><p v-if="!libraryRows.length" class="tv-empty">{{ searchQuery?'No matching workspaces.':'Install a core from the Store to add a workspace.' }}</p><template v-if="available.length&&!searchQuery"><header class="cw-store-suggestions"><h2>Explore the Store</h2><button class="cw-text-button" type="button" @click="selectArea('store')">View all {{ available.length }} <UiIcon name="chevron" /></button></header><div class="cw-workspace-grid"><button v-for="row in available.slice(0,3)" :key="row.id" class="cw-workspace-card cw-available-core" type="button" @click="openAvailable(row)"><header><UiIcon :name="coreWorkspace(row.module_key||`${row.id}_core`).icon" /><span class="cw-core-state">Available</span></header><h3>{{ text(row.name||row.id).replace(/ Core$/i,'') }}</h3><p>{{ coreWorkspace(row.module_key||`${row.id}_core`,row.workspace).description }}</p><footer><span>View in Store</span><UiIcon name="external" /></footer></button></div></template></section>
    <section v-else-if="activeSpec" class="core-top-tab-panel active tcx-core-panel cw-core-workspace" :data-core-tab-panel="activeTab" :data-core-tab-loaded="panelLoading[activeTab]?'loading':'1'"><button class="cw-text-button cw-back-library" type="button" @click="selectArea('workspaces')"><UiIcon name="back" />All workspaces</button><header class="cw-workspace-heading"><div><h2><UiIcon :name="activeWorkspace.icon" />{{ String(activeSpec.label||activeSpec.core_key).replace(/(?: Core)? Settings$/i,'').replace(/ Core$/i,'') }}</h2><p>{{ activeWorkspace.description }}</p></div><div class="cw-workspace-actions"><span class="cw-core-state" :class="{running:activeSpec.running}"><i />{{ activeSpec.running?'Running':'Stopped' }}</span><button v-if="activeRuntime&&!activeRuntime.running" class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="runtimeAction(activeRuntime,'start')">Start core</button><button class="tv-button" type="button" aria-label="Refresh workspace" :disabled="panelLoading[activeTab]" @click="refreshTab(activeTab,true)"><UiIcon name="refresh" /></button><button v-if="activeRuntime?.settings?.length" class="tv-button" type="button" aria-label="Core runtime settings" @click="openSettings(activeRuntime)"><UiIcon name="sliders" /></button></div></header><p v-if="!activeSpec.running&&activeSpec.requires_running" class="cw-stopped-note">This core is stopped. Start it to resume its background work.</p><p v-if="activePayload.load_error" class="tv-notice error" role="alert">{{ activePayload.load_error }}</p><div v-if="panelLoading[activeTab]&&!Object.keys(activePayload).length" class="tv-empty">Loading {{ activeSpec.label }}…</div>

    </section>
    <div v-show="Boolean(activeSpec)" class="cw-workspace-content"><KeepAlive><MusicCoreApp v-if="activeSpec&&activePanelState&&activeIsMusic&&musicOptions" :key="`music:${activeSpec.core_key}`" :state="activePanelState" :options="musicOptions" /><CorePanelRenderer v-else-if="activeSpec&&activePanelState" :key="activeSpec.core_key" :payload="activePayload" :tab="activeSpec" :action-endpoint="`${options.endpoints.runtime}/${encode(activeSpec.core_key)}/tab-action`" :refresh="workspaceRefresher(activeSpec.core_key)" :notify="notify" /></KeepAlive></div>
    <section v-if="activeArea!=='workspaces'" class="core-top-tab-panel active tcx-manage" data-core-tab-panel="manage">
      <div v-if="manageTab !== 'repos'" class="workspace-manager-toolbar"><CatalogToolbar v-model="searchQuery" label="Search Cores" :count="manageTab === 'installed' ? filteredInstalledRows.length : manageTab === 'store' ? filteredAvailable.length : filteredManaged.length" /></div>

      <div v-if="manageTab === 'installed'" class="tcx-card-grid">
        <article v-for="row in filteredInstalledRows" :key="row.key" class="tv-panel tcx-core-card"><span class="workspace-item-icon"><UiIcon name="cores" /></span><header><div><span class="tv-eyebrow">{{ row.key }}</span><h2>{{ rowName(row) }}</h2></div><span class="tv-state" :class="{ good: row.runtime?.running, pending: row.runtime?.desired_running && !row.runtime?.running }">{{ stateLabel(row.runtime) }}</span></header><p>{{ rowDescription(row) }}</p><details class="workspace-item-details"><summary>Version & details</summary><code class="workspace-item-id">{{ row.key }}</code><div class="tp-version"><span>Installed {{ row.shop?.installed_ver || '0.0.0' }}</span><span>Store {{ row.shop?.store_ver || '-' }}</span><span>{{ row.shop?.source_label || 'local' }}</span></div></details><footer><button v-if="row.runtime?.settings?.length" class="tv-button" type="button" @click="openSettings(row.runtime)">Settings</button><button v-if="dynamicTabs.some(tab=>tab.core_key===row.key)" class="tv-button" type="button" @click="selectTopTab(row.key)">Open workspace</button><span v-else-if="!row.runtime">Runtime unavailable</span><button v-if="row.runtime" class="tv-button" :class="{ primary: !row.runtime.running }" type="button" @click="runtimeAction(row.runtime, row.runtime.running ? 'stop' : 'start')">{{ row.runtime.running ? 'Stop' : 'Start' }}</button></footer><details v-if="row.shop" class="cw-package-details"><summary>Updates & removal</summary><div class="cw-package-actions"><span>{{ row.shop.update_available?'Update available':'Up to date' }}</span><button class="tv-button" type="button" :disabled="!row.shop.update_available||Boolean(busy)" @click="shopAction('update',row.shop.id)">Update</button><label><input v-model="purgeIds[row.shop.id]" type="checkbox" />Delete data</label><button class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="shopAction('remove',row.shop.id)">Remove</button></div></details></article>
        <div v-if="!filteredInstalledRows.length" class="tv-empty">{{ searchQuery ? 'No matching Cores.' : 'No installed Cores found.' }}</div>
      </div>

      <div v-else-if="manageTab === 'store'" class="tcx-card-grid">
        <article v-for="row in filteredAvailable" :key="row.id" class="tv-panel tcx-core-card"><span class="workspace-item-icon"><UiIcon name="cores" /></span><header><div><span class="tv-eyebrow">{{ row.id }}</span><h2>{{ row.name || row.id }}</h2></div><span class="tv-state">v{{ row.version || '-' }}</span></header><p>{{ row.description || 'No description provided.' }}</p><details class="workspace-item-details"><summary>Package details</summary><code>{{ row.id }}</code></details><footer><span>{{ row.source_label || 'Tater Shop' }}</span><button class="tv-button primary" type="button" @click="shopAction('install', row.id)">Install</button></footer></article><div v-if="!filteredAvailable.length" class="tv-empty">No additional Cores are available from the configured repositories.</div>
      </div>

      <div v-else-if="manageTab === 'manage'" class="tcx-manage-list tv-manage-workspace">
        <div class="tv-panel tcx-manage-toolbar tv-manage-hero">
          <div class="tv-manage-hero-copy"><span class="tv-eyebrow">Manage library</span><h2>Core control center</h2><p>Update system capabilities, control their runtimes, and remove Cores with optional data cleanup. Running Cores restart automatically after an update.</p></div>
          <div class="tv-manage-overview"><div><span>Installed</span><strong>{{ installed.length }}</strong></div><div :class="{ attention: updates.length }"><span>Updates ready</span><strong>{{ updates.length }}</strong></div><button class="tv-button primary" type="button" :disabled="!updates.length" @click="shopAction('update-all')">Update all</button></div>
        </div>
        <article v-for="row in filteredManaged" :key="row.id" class="tv-panel tcx-manage-row tv-manage-card" :class="{ 'has-update': row.update_available }">
          <div class="tv-manage-identity"><span class="tv-manage-monogram">{{ text(row.name || row.id).charAt(0).toUpperCase() }}</span><div><span class="tv-eyebrow">{{ row.id }}</span><h3>{{ row.name || row.id }}</h3><small>{{ row.source_label || 'Local Core' }}</small></div></div>
          <div class="tv-manage-version"><div><span>Installed</span><strong>{{ row.installed_ver || '0.0.0' }}</strong></div><i>→</i><div><span>Latest</span><strong>{{ row.store_ver || '-' }}</strong></div><span class="tv-state" :class="row.update_available ? 'pending' : 'good'">{{ row.update_available ? 'Update ready' : 'Current' }}</span></div>
          <div class="tv-manage-actions"><span class="tv-manage-runtime" :class="{ online: runtimeForShop(row)?.running }"><i />{{ stateLabel(runtimeForShop(row)) }}</span><button class="tv-button" :class="{ primary: row.update_available }" type="button" :disabled="!row.update_available" @click="shopAction('update', row.id)">{{ row.update_available ? 'Update' : 'Current' }}</button><button v-if="runtimeForShop(row)" class="tv-button" type="button" @click="runtimeAction(runtimeForShop(row)!, runtimeForShop(row)?.running ? 'stop' : 'start')">{{ runtimeForShop(row)?.running ? 'Stop' : 'Start' }}</button><label class="ti-purge"><input v-model="purgeIds[row.id]" type="checkbox" /> Delete data</label><button class="tv-button danger" type="button" @click="shopAction('remove', row.id)">Remove</button></div>
        </article>
        <div v-if="!filteredManaged.length" class="tv-empty">{{ searchQuery ? 'No matching updates.' : 'All installed Cores are up to date.' }}</div>
      </div>

      <div v-else class="tv-panel tcx-repos tv-repository-manager">
        <header class="tv-repository-heading"><div><span class="tv-eyebrow">Repository library</span><h2>Core repositories</h2><p>Choose a Tater-trusted source or add your own manifest.</p></div></header>
        <nav class="tv-repository-tabs" aria-label="Core repository sources"><button type="button" :class="{ active: repoSection === 'trusted' }" @click="repoSection = 'trusted'">Trusted repositories</button><button type="button" :class="{ active: repoSection === 'custom' }" @click="repoSection = 'custom'">Custom repositories</button></nav>
        <div v-if="repoSection === 'trusted'" class="tv-trusted-repositories">
          <p class="tv-repository-intro">Curated sources are reviewed by Tater. Select a card to add or remove its Cores from your Store automatically.</p>
          <div v-if="shop.repos?.trusted_error" class="tv-repository-warning">The trusted directory is temporarily unavailable. Your enabled repositories are unchanged.</div>
          <div class="tv-trusted-repo-grid">
            <article class="tv-trusted-repo-card builtin selected" aria-label="Built-in Tater Core Shop repository"><span class="tv-repo-check">✓</span><div class="tv-repo-card-top"><span class="tv-repo-monogram">T</span><div><span class="tv-eyebrow">Always available</span><h3>{{ shop.repos?.default?.name || 'Tater Core Shop' }}</h3><p>by <strong>Tater Assistant</strong></p></div></div><p>The official built-in Core catalog.</p><footer><span class="tv-repo-enabled">Built in</span></footer></article>
            <article v-for="repo in trustedRepos" :key="repo.id || repo.url" class="tv-trusted-repo-card" :class="{ selected: repo.enabled }" role="button" tabindex="0" :aria-pressed="Boolean(repo.enabled)" @click="toggleTrustedRepo(repo)" @keydown.enter.prevent="toggleTrustedRepo(repo)" @keydown.space.prevent="toggleTrustedRepo(repo)"><span class="tv-repo-check">{{ repo.enabled ? '✓' : '+' }}</span><div class="tv-repo-card-top"><span class="tv-repo-monogram">{{ text(repo.repository || repo.name).charAt(0).toUpperCase() }}</span><div><span class="tv-eyebrow">Trusted Core source</span><h3>{{ repo.repository || repo.name }}</h3><p>by <a v-if="repo.author_url" :href="repo.author_url" target="_blank" rel="noreferrer" @click.stop>{{ repo.author || 'Community author' }}</a><strong v-else>{{ repo.author || 'Community author' }}</strong></p></div></div><p>{{ repo.description || 'Additional Cores for the Tater Store.' }}</p><div v-if="repo.tags?.length" class="ti-tags"><span v-for="tag in repo.tags" :key="tag">{{ tag }}</span></div><footer><a v-if="repo.homepage" :href="repo.homepage" target="_blank" rel="noreferrer" @click.stop>View repository ↗</a><span :class="repo.enabled ? 'tv-repo-enabled' : 'tv-repo-available'">{{ repo.enabled ? 'Added to Store' : 'Select to add' }}</span></footer></article>
          </div>
          <div v-if="!trustedRepos.length" class="tv-empty compact">No additional trusted Core repositories are listed yet.</div>
        </div>
        <div v-else class="tv-custom-repositories">
          <p class="tv-repository-intro">Custom manifests are managed by you and are not reviewed by Tater.</p>
          <article v-for="(repo, index) in draftRepos" :key="`${repo.url}-${index}`" class="ti-repo-row"><div><strong>{{ repo.name || 'Custom repository' }}</strong><code>{{ repo.url }}</code></div><button class="tv-button" type="button" @click="removeRepo(index)">Remove</button></article>
          <div v-if="!draftRepos.length" class="tv-empty compact">No custom repositories configured.</div>
          <div class="tcx-repo-form"><label><span>Name (optional)</span><input v-model="repoName" type="text" placeholder="My Core Repo" /></label><label><span>Manifest URL</span><input v-model="repoUrl" type="url" placeholder="https://example.com/cores.json" @keyup.enter="addRepo" /></label><button class="tv-button" type="button" @click="addRepo">Add</button><button class="tv-button primary" type="button" @click="saveRepos">Save custom repositories</button></div>
        </div>
      </div>
    </section>
  </div>

  <PopupTransition :open="Boolean(settingsCore)" @close="settingsCore = null"><section class="tv-modal tcx-settings-modal" role="dialog" aria-modal="true"><header><div><span class="tv-eyebrow">Core settings</span><h2>{{ settingsCore?.label || settingsCore?.key }}</h2><p>Changes are applied to this Core’s runtime configuration.</p></div><button class="tv-button" type="button" @click="settingsCore = null">Close</button></header><div class="tvb-field-grid"><ManifestField v-for="field in settingsCore?.settings || []" :key="field.key || field.label" :field="field" :model-value="fieldValues[field.key]" :all-values="fieldValues" @update:model-value="fieldValues[field.key] = $event" /></div><footer><span>{{ busy || 'Ready' }}</span><button class="tv-button primary" type="button" @click="saveSettings">Save settings</button></footer></section></PopupTransition>
</template>
