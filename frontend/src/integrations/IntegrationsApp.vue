<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { getJson, postJson } from "../shared/api";
import PopupTransition from "../shared/PopupTransition.vue";
import type { IntegrationSettingsPayload, IntegrationsMountOptions, JsonRow } from "./types";

const props = defineProps<{
  state: { settings: IntegrationSettingsPayload };
  options: IntegrationsMountOptions;
}>();

const validTabs = ["manager", "devices", "rooms", "runtime"];
const activeTab = ref(validTabs.includes(props.options.initialTab || "") ? String(props.options.initialTab) : "manager");
const shopTab = ref("installed");
const busy = ref("");
const status = ref("");
const error = ref("");
const activeCategory = ref("");
const settingsIntegration = ref<JsonRow | null>(null);
const fieldValues = ref<Record<string, any>>({});
const purgeIds = ref<Record<string, boolean>>({});
const repoName = ref("");
const repoUrl = ref("");
const draftRepos = ref<JsonRow[]>([]);
const registry = ref<JsonRow>(props.state.settings.integration_device_registry || {});
const runtime = ref<JsonRow>(props.state.settings.integration_runtime || {});
const runtimeStates = ref<JsonRow>({});
const runtimeEvents = ref<JsonRow>({});
const newRoomName = ref("");
const roomNames = ref<Record<string, string>>({});
const deviceNames = ref<Record<string, string>>({});
let activityTimer = 0;
let registryRefreshRunning = false;
let registryWarmupRunning = false;
let disposed = false;

const settings = computed(() => props.state.settings || {});
const integrations = computed(() => (Array.isArray(settings.value.integrations) ? settings.value.integrations : []));
const shop = computed(() => settings.value.integration_shop || {});
const installed = computed(() => (Array.isArray(shop.value.installed) ? shop.value.installed : []));
const available = computed(() => (Array.isArray(shop.value.catalog) ? shop.value.catalog.filter((row: JsonRow) => !row.installed) : []));
const updates = computed(() => installed.value.filter((row: JsonRow) => row.update_available));
const enabledCount = computed(() => {
  const count = installed.value.filter((row: JsonRow) => row.enabled).length;
  return count || (!installed.value.length ? integrations.value.length : 0);
});
const integrationById = computed(() => new Map(integrations.value.map((row: JsonRow) => [canonicalId(row.id), row])));
const installedRuntimeRows = computed(() => {
  const seen = new Set<string>();
  const rows: Array<{ id: string; shop: JsonRow | null; integration: JsonRow | null }> = installed.value.map((shopRow: JsonRow) => {
    const id = text(shopRow.id || shopRow.module_key || shopRow.key);
    seen.add(canonicalId(id));
    return { id, shop: shopRow, integration: integrationById.value.get(canonicalId(id)) || null };
  });
  integrations.value.forEach((integration: JsonRow) => {
    const id = text(integration.id);
    if (id && !seen.has(canonicalId(id))) rows.push({ id, shop: null, integration });
  });
  return rows.sort((a, b) => text(a.integration?.name || a.shop?.name || a.id).localeCompare(text(b.integration?.name || b.shop?.name || b.id)));
});
const categories = computed(() => (Array.isArray(registry.value.categories) ? registry.value.categories.filter((row: JsonRow) => Number(row.device_count || 0) > 0) : []));
const selectedCategory = computed(() => categories.value.find((row: JsonRow) => text(row.id) === activeCategory.value) || categories.value[0] || null);
const rooms = computed(() => {
  const rows = Array.isArray(registry.value.rooms) ? registry.value.rooms.slice() : [];
  const extra = Array.isArray(registry.value.room_overrides?.rooms) ? registry.value.room_overrides.rooms : [];
  const seen = new Set(rows.map((row: JsonRow) => text(row.id)));
  extra.forEach((row: JsonRow) => { if (!seen.has(text(row.id))) rows.push({ ...row, devices: [], categories: [], source: "tater" }); });
  return rows.sort((a: JsonRow, b: JsonRow) => text(a.name).localeCompare(text(b.name)));
});
const mediaPlayers = computed(() => (Array.isArray(registry.value.room_media_player_options) ? registry.value.room_media_player_options : []));
const recentEvents = computed(() => {
  const rows = Array.isArray(runtimeEvents.value.events) ? runtimeEvents.value.events : [];
  return rows.filter((row: JsonRow) => {
    const kind = text(row.kind || row.type).toLowerCase();
    return !["snapshot", "poll", "heartbeat", "runtime_status"].some((token) => kind.includes(token));
  }).sort((a: JsonRow, b: JsonRow) => Number(b.ts || 0) - Number(a.ts || 0)).slice(0, 40);
});

function text(value: unknown): string { return String(value ?? "").trim(); }
function canonicalId(value: unknown): string { const id = text(value); return id === "ecobee_homekit" ? "homekit" : id; }
function encode(value: unknown): string { return encodeURIComponent(text(value)); }
function notify(message: string, tone = "success") { status.value = message; props.options.onToast?.(message, tone); }
function displayName(row: JsonRow): string { return text(row.name || row.friendly_name || row.label || row.title || row.id || row.ref || "Device"); }
function roomId(row: JsonRow): string { return text(row.id || "unassigned") || "unassigned"; }
function deviceId(row: JsonRow): string { return text(row.id || row.ref); }
function deviceIntegration(row: JsonRow): string { return text(row.integration_id); }
function fieldValue(integration: JsonRow, field: JsonRow): unknown {
  const raw = integration.values && Object.prototype.hasOwnProperty.call(integration.values, field.key) ? integration.values[field.key] : field.default ?? "";
  if (text(field.type).toLowerCase() === "checkbox") {
    if (typeof raw === "string") return ["1", "true", "yes", "on"].includes(raw.trim().toLowerCase());
    return Boolean(raw);
  }
  return raw;
}
function collectedFieldValues(integration: JsonRow): JsonRow {
  const values = { ...fieldValues.value };
  (Array.isArray(integration.fields) ? integration.fields : []).forEach((field: JsonRow) => {
    const key = text(field.key);
    if (key && text(field.type).toLowerCase() === "number") values[key] = Number(values[key] ?? field.default ?? 0);
  });
  return values;
}
function eventPayload(row: JsonRow): JsonRow { return row.payload && typeof row.payload === "object" ? row.payload : {}; }
function eventTitle(row: JsonRow): string {
  const body = eventPayload(row);
  return text(body.name || body.friendly_name || body.device_name || body.entity_name || body.entity_id || body.ref || row.provider || "Device change");
}
function eventState(row: JsonRow): string {
  const body = eventPayload(row);
  const value = body.state ?? body.value ?? body.status ?? body.current_state ?? row.kind ?? "changed";
  return text(value).replaceAll("_", " ");
}
function relativeTime(value: unknown): string {
  const seconds = Math.max(0, Date.now() / 1000 - Number(value || 0));
  if (seconds < 60) return "now";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

async function refreshSettings(quiet = false) {
  if (!quiet) busy.value = "Refreshing integrations…";
  error.value = "";
  try {
    props.state.settings = await getJson<IntegrationSettingsPayload>(props.options.endpoints.settings);
    registry.value = props.state.settings.integration_device_registry || registry.value;
    runtime.value = props.state.settings.integration_runtime || runtime.value;
    draftRepos.value = Array.isArray(props.state.settings.integration_shop?.repos?.additional)
      ? props.state.settings.integration_shop.repos.additional.map((row: JsonRow) => ({ ...row })) : [];
  } catch (requestError) {
    error.value = requestError instanceof Error ? requestError.message : "Integration refresh failed.";
    if (!quiet) notify(error.value, "error");
  } finally { if (!quiet) busy.value = ""; }
}

async function shopAction(action: string, id = "") {
  if (action === "remove" && !window.confirm(`Remove ${id}?${purgeIds.value[id] ? " Its saved data will also be deleted." : ""}`)) return;
  busy.value = `${action.replaceAll("-", " ")} ${id || "integrations"}…`;
  error.value = "";
  try {
    const payload: JsonRow = id ? { id } : {};
    if (action === "remove") payload.purge_redis = Boolean(purgeIds.value[id]);
    const result = await postJson<JsonRow>(`${props.options.endpoints.shop}/${action}`, payload);
    notify(text(result.message) || "Integration action completed.");
    await refreshSettings(true);
  } catch (requestError) {
    error.value = requestError instanceof Error ? requestError.message : "Integration action failed.";
    notify(error.value, "error");
  } finally { busy.value = ""; }
}

function openSettings(integration: JsonRow) {
  settingsIntegration.value = integration;
  fieldValues.value = Object.fromEntries((Array.isArray(integration.fields) ? integration.fields : []).map((field: JsonRow) => [text(field.key), fieldValue(integration, field)]));
}

async function saveIntegrationSettings() {
  const integration = settingsIntegration.value;
  if (!integration) return;
  busy.value = `Saving ${displayName(integration)}…`;
  try {
    await postJson<JsonRow>(`${props.options.endpoints.integrationSettings}/${encode(integration.id)}/settings`, { settings: collectedFieldValues(integration) });
    notify(`${displayName(integration)} settings saved.`);
    settingsIntegration.value = null;
    await refreshSettings(true);
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Settings save failed.", "error"); }
  finally { busy.value = ""; }
}

async function runIntegrationAction(action: JsonRow) {
  const integration = settingsIntegration.value;
  if (!integration) return;
  busy.value = text(action.status || `Running ${action.label || action.id}…`);
  try {
    const result = await postJson<JsonRow>(`${props.options.endpoints.integrationActions}/${encode(integration.id)}/actions/${encode(action.id)}`, { payload: collectedFieldValues(integration) });
    const returnedValues = result.values && typeof result.values === "object" ? result.values : result;
    const fieldKeys = new Set((integration.fields || []).map((field: JsonRow) => text(field.key)));
    fieldValues.value = { ...fieldValues.value, ...Object.fromEntries(Object.entries(returnedValues).filter(([key]) => fieldKeys.has(key))) };
    notify(text(result.message) || `${action.label || action.id} complete.`, result.ok === false ? "error" : "success");
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Integration action failed.", "error"); }
  finally { busy.value = ""; }
}

async function saveRepos() {
  busy.value = "Saving integration repositories…";
  try {
    await postJson<JsonRow>(`${props.options.endpoints.shop}/repos`, { repos: draftRepos.value });
    notify("Integration repositories saved.");
    await refreshSettings(true);
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Repository save failed.", "error"); }
  finally { busy.value = ""; }
}

function addRepo() {
  const url = repoUrl.value.trim();
  if (!url) { notify("Repo URL is required.", "error"); return; }
  if (draftRepos.value.some((row) => text(row.url).toLowerCase() === url.toLowerCase())) { notify("That repo is already added.", "error"); return; }
  draftRepos.value.push({ name: repoName.value.trim(), url });
  repoName.value = ""; repoUrl.value = ""; status.value = "Repo added. Save repositories to apply it.";
}

async function loadRegistry(forRooms = false) {
  try {
    const endpoint = forRooms ? props.options.endpoints.rooms : props.options.endpoints.deviceRegistry;
    const result = await getJson<JsonRow>(endpoint);
    registry.value = result.registry || result;
    if (!activeCategory.value) activeCategory.value = text(categories.value[0]?.id);
    if (text(registry.value.cache?.source) === "building") void waitForRegistryWarmup();
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Device load failed.", "error"); }
}

async function waitForRegistryWarmup() {
  if (registryWarmupRunning) return;
  registryWarmupRunning = true;
  try {
    for (let attempt = 0; attempt < 120 && !disposed; attempt += 1) {
      await new Promise((resolve) => window.setTimeout(resolve, 500));
      const forRooms = activeTab.value === "rooms";
      const endpoint = forRooms ? props.options.endpoints.rooms : props.options.endpoints.deviceRegistry;
      const result = await getJson<JsonRow>(endpoint);
      const nextRegistry = result.registry || result;
      if (text(nextRegistry.cache?.source) === "building") continue;
      registry.value = nextRegistry;
      if (!activeCategory.value) activeCategory.value = text(categories.value[0]?.id);
      return;
    }
  } catch {
    // The normal five-minute fallback or manual Refresh can retry a failed warmup.
  } finally { registryWarmupRunning = false; }
}

async function refreshRegistry(forRooms = false) {
  if (registryRefreshRunning) return;
  registryRefreshRunning = true;
  busy.value = forRooms ? "Refreshing rooms in background…" : "Refreshing devices in background…";
  try {
    const queued = await postJson<JsonRow>(`${props.options.endpoints.systemTasks}/integration_device_registry/run`);
    const startingRunCount = Number(queued.task?.run_count || 0);
    for (let attempt = 0; attempt < 120 && !disposed; attempt += 1) {
      await new Promise((resolve) => window.setTimeout(resolve, 500));
      const snapshot = await getJson<JsonRow>(props.options.endpoints.systemTasks);
      const task = (Array.isArray(snapshot.tasks) ? snapshot.tasks : []).find((row: JsonRow) => text(row.id) === "integration_device_registry");
      if (!task || task.running || Number(task.run_count || 0) <= startingRunCount) continue;
      if (text(task.last_error)) throw new Error(text(task.last_error));
      await loadRegistry(activeTab.value === "rooms");
      notify("Integration devices refreshed.");
      return;
    }
    if (!disposed) throw new Error("The integration device refresh is still running. You can follow it in System Tasks.");
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Device refresh failed.", "error"); }
  finally { registryRefreshRunning = false; busy.value = ""; }
}

async function roomAction(action: string, payload: JsonRow) {
  busy.value = "Saving organization changes…";
  try {
    const result = await postJson<JsonRow>(props.options.endpoints.rooms, { action, payload });
    registry.value = result.registry || result;
    notify("Organization changes saved.");
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Organization update failed.", "error"); }
  finally { busy.value = ""; }
}

async function createRoom() {
  const name = newRoomName.value.trim(); if (!name) return;
  await roomAction("create_room", { name }); newRoomName.value = "";
}
async function renameRoom(room: JsonRow) { const name = text(roomNames.value[roomId(room)] || room.name); if (name) await roomAction("rename_room", { room_id: roomId(room), name }); }
async function moveDevice(device: JsonRow, nextRoom: string) {
  if (!nextRoom || nextRoom === "unassigned") await roomAction("clear_device_room", { integration_id: deviceIntegration(device), device_id: deviceId(device) });
  else await roomAction("assign_device_room", { integration_id: deviceIntegration(device), device_id: deviceId(device), room_id: nextRoom, room_name: text(rooms.value.find((row: JsonRow) => roomId(row) === nextRoom)?.name) });
}
async function renameDevice(device: JsonRow) { const name = text(deviceNames.value[`${deviceIntegration(device)}:${deviceId(device)}`] || displayName(device)); if (name) await roomAction("rename_device", { integration_id: deviceIntegration(device), device_id: deviceId(device), name }); }
async function preferredPlayer(room: JsonRow, target: string) {
  if (target) await roomAction("set_room_preferred_media_player", { room_id: roomId(room), room_name: room.name, target });
  else await roomAction("clear_room_preferred_media_player", { room_id: roomId(room) });
}

async function refreshActivity(quiet = false) {
  if (!quiet) busy.value = "Refreshing activity…";
  try {
    const [runtimeResult, statesResult, eventsResult] = await Promise.all([
      getJson<JsonRow>(props.options.endpoints.runtime), getJson<JsonRow>(props.options.endpoints.runtimeStates), getJson<JsonRow>(`${props.options.endpoints.runtimeEvents}?limit=1000`),
    ]);
    runtime.value = runtimeResult.runtime || runtimeResult; runtimeStates.value = statesResult; runtimeEvents.value = eventsResult;
  } catch (requestError) { if (!quiet) notify(requestError instanceof Error ? requestError.message : "Activity refresh failed.", "error"); }
  finally { if (!quiet) busy.value = ""; }
}

function selectTab(tab: string) {
  activeTab.value = tab; props.options.onTabChange?.(tab);
}

watch(() => props.state.settings, () => {
  registry.value = props.state.settings.integration_device_registry || registry.value;
  runtime.value = props.state.settings.integration_runtime || runtime.value;
});
watch(categories, (rows) => { if (!rows.some((row: JsonRow) => text(row.id) === activeCategory.value)) activeCategory.value = text(rows[0]?.id); }, { immediate: true });
watch(rooms, (rows) => {
  const nextRoomNames: Record<string, string> = {}; const nextDeviceNames: Record<string, string> = {};
  rows.forEach((room: JsonRow) => { nextRoomNames[roomId(room)] = text(room.name); (room.devices || []).forEach((device: JsonRow) => { nextDeviceNames[`${deviceIntegration(device)}:${deviceId(device)}`] = displayName(device); }); });
  roomNames.value = nextRoomNames; deviceNames.value = nextDeviceNames;
}, { immediate: true });
watch(activeTab, (tab) => {
  window.clearInterval(activityTimer);
  if (tab === "devices") void loadRegistry(false);
  if (tab === "rooms") void loadRegistry(true);
  if (tab === "runtime") {
    void refreshActivity();
    activityTimer = window.setInterval(() => void refreshActivity(true), 10000);
  }
}, { immediate: true });
onBeforeUnmount(() => { disposed = true; window.clearInterval(activityTimer); });

draftRepos.value = Array.isArray(shop.value.repos?.additional) ? shop.value.repos.additional.map((row: JsonRow) => ({ ...row })) : [];
</script>

<template>
  <div class="tater-vue-surface ti-integrations">
    <header class="tv-page-heading">
      <div><span class="tv-eyebrow">Connected home</span><h1>Integrations</h1><p>Services, devices, rooms, live state, and Tater Shop updates in one place.</p></div>
      <div class="tv-heading-actions"><span class="tv-live-pill" :class="{ busy: Boolean(busy) }"><i />{{ busy || "Live" }}</span><button class="tv-button" type="button" @click="refreshSettings()">Refresh</button></div>
    </header>
    <div class="tv-metrics ti-summary">
      <div><span>Installed</span><strong>{{ installed.length || integrations.length }}</strong></div><div><span>Enabled</span><strong>{{ enabledCount }}</strong></div><div><span>Devices</span><strong>{{ Number(registry.total || 0) }}</strong></div><div><span>Updates</span><strong>{{ Number(shop.updates_available || updates.length) }}</strong></div>
    </div>
    <div v-if="status || error" class="tv-notice" :class="{ error: Boolean(error) }">{{ error || status }}</div>
    <nav class="tv-tabs" aria-label="Integration sections"><button v-for="tab in [{id:'manager',label:'Manager'},{id:'devices',label:'Devices'},{id:'rooms',label:'Organize'},{id:'runtime',label:'Activity'}]" :key="tab.id" type="button" :class="{ active: activeTab === tab.id }" @click="selectTab(tab.id)">{{ tab.label }}</button></nav>

    <section v-if="activeTab === 'manager'" class="ti-manager">
      <nav class="tv-mini-tabs"><button v-for="tab in [{id:'installed',label:'Installed'},{id:'store',label:'Store'},{id:'manage',label:'Manage'},{id:'repos',label:'Repositories'}]" :key="tab.id" :class="{ active: shopTab === tab.id }" type="button" @click="shopTab = tab.id">{{ tab.label }}<span v-if="tab.id === 'manage' && updates.length">{{ updates.length }}</span></button></nav>
      <div v-if="shop.errors?.length" class="tv-notice error">{{ shop.errors.join(' • ') }}</div>
      <div v-if="shopTab === 'installed'" class="ti-card-grid">
        <article v-for="row in installedRuntimeRows" :key="row.id" class="tv-panel ti-integration-card">
          <header><div><span class="tv-eyebrow">{{ row.id }}</span><h2>{{ row.integration?.name || row.shop?.name || row.id }}</h2></div><span class="tv-state" :class="{ good: row.shop?.enabled !== false }">{{ row.shop?.enabled === false ? 'Disabled' : 'Enabled' }}</span></header>
          <p>{{ row.integration?.description || row.shop?.description || 'Connected integration.' }}</p>
          <div class="ti-version" v-if="row.shop">Installed {{ row.shop.installed_ver || '0.0.0' }} <span>Store {{ row.shop.store_ver || '-' }}</span></div>
          <div v-if="row.integration?.capabilities?.length" class="ti-tags"><span v-for="capability in row.integration.capabilities" :key="capability">{{ capability }}</span></div>
          <footer><button v-if="row.integration && (row.integration.fields?.length || row.integration.actions?.length)" class="tv-button" type="button" @click="openSettings(row.integration)">Settings</button><span v-else>No configurable settings</span><button v-if="row.shop && !row.shop.required" class="tv-button" type="button" @click="shopAction(row.shop.enabled ? 'disable' : 'enable', row.id)">{{ row.shop.enabled ? 'Disable' : 'Enable' }}</button></footer>
        </article>
        <div v-if="!installedRuntimeRows.length" class="tv-empty">No installed integrations found.</div>
      </div>
      <div v-else-if="shopTab === 'store'" class="ti-card-grid">
        <article v-for="row in available" :key="row.id" class="tv-panel ti-integration-card"><header><div><span class="tv-eyebrow">{{ row.id }}</span><h2>{{ row.name || row.id }}</h2></div><span class="tv-state">v{{ row.version || '-' }}</span></header><p>{{ row.description }}</p><footer><span>{{ row.source_label || 'Tater Shop' }}</span><button class="tv-button primary" type="button" @click="shopAction('install', row.id)">Download</button></footer></article>
        <div v-if="!available.length" class="tv-empty">No additional integrations are available.</div>
      </div>
      <div v-else-if="shopTab === 'manage'" class="ti-manage-list">
        <div class="ti-manage-toolbar"><div><h2>Manage installed integrations</h2><p>{{ updates.length }} update{{ updates.length === 1 ? '' : 's' }} available.</p></div><button class="tv-button primary" type="button" :disabled="!updates.length" @click="shopAction('update-all')">Update all</button></div>
        <article v-for="row in installed" :key="row.id" class="tv-panel ti-manage-row"><div><strong>{{ row.name || row.id }}</strong><span>{{ row.installed_ver || '0.0.0' }} → {{ row.store_ver || '-' }}</span></div><div class="ti-row-actions"><button class="tv-button" type="button" :disabled="!row.update_available" @click="shopAction('update', row.id)">{{ row.update_available ? 'Update' : 'Current' }}</button><button v-if="!row.required" class="tv-button" type="button" @click="shopAction(row.enabled ? 'disable' : 'enable', row.id)">{{ row.enabled ? 'Disable' : 'Enable' }}</button><label v-if="!row.required" class="ti-purge"><input v-model="purgeIds[row.id]" type="checkbox" /> Delete data</label><button v-if="!row.required" class="tv-button danger" type="button" @click="shopAction('remove', row.id)">Remove</button><span v-else class="tv-state good">Required</span></div></article>
      </div>
      <div v-else class="tv-panel ti-repos">
        <header><div><span class="tv-eyebrow">Sources</span><h2>Integration repositories</h2><p>The built-in repository stays available; add trusted sources below.</p></div></header>
        <article class="ti-repo-row builtin"><div><strong>{{ shop.repos?.default?.name || 'Default' }}</strong><code>{{ shop.repos?.default?.url || '(not set)' }}</code></div><span>Built-in</span></article>
        <article v-for="(repo, index) in draftRepos" :key="`${repo.url}-${index}`" class="ti-repo-row"><div><strong>{{ repo.name || 'Additional repo' }}</strong><code>{{ repo.url }}</code></div><button class="tv-button" type="button" @click="draftRepos.splice(index, 1)">Remove</button></article>
        <div class="ti-repo-form"><label><span>Name (optional)</span><input v-model="repoName" type="text" placeholder="My Integration Repo" /></label><label><span>Repo URL</span><input v-model="repoUrl" type="url" placeholder="https://example.com/integrations.json" @keyup.enter="addRepo" /></label><button class="tv-button" type="button" @click="addRepo">Add</button><button class="tv-button primary" type="button" @click="saveRepos">Save repositories</button></div>
      </div>
    </section>

    <section v-else-if="activeTab === 'devices'" class="tv-panel ti-browser">
      <header class="tv-panel-head"><div><span class="tv-eyebrow">Device registry</span><h2>Browse devices</h2><p>Grouped by category, room, and integration.</p></div><button class="tv-button" type="button" @click="refreshRegistry(false)">Refresh devices</button></header>
      <div v-if="categories.length" class="ti-browser-layout"><aside><button v-for="category in categories" :key="category.id" type="button" :class="{ active: selectedCategory?.id === category.id }" @click="activeCategory = text(category.id)"><strong>{{ category.name }}</strong><span>{{ category.device_count }} devices · {{ category.room_count }} rooms</span></button></aside><div class="ti-device-content"><header><div><span class="tv-eyebrow">{{ selectedCategory?.id }}</span><h2>{{ selectedCategory?.name }}</h2><p>{{ selectedCategory?.description }}</p></div></header><div v-for="room in selectedCategory?.rooms || []" :key="room.id" class="ti-device-room"><div><strong>{{ room.name }}</strong><span>{{ room.devices?.length || 0 }} devices</span></div><article v-for="device in room.devices || []" :key="deviceId(device)" class="ti-device-row"><div><strong>{{ displayName(device) }}</strong><span>{{ [device.integration_name || device.integration_id, device.type, device.ref || device.id].filter(Boolean).join(' / ') }}</span></div><div><span class="tv-state">{{ device.state || device.status || 'unknown' }}</span><small>{{ device.room || device.area || 'Unassigned' }}</small></div><div class="ti-tags"><span v-for="tag in (device.features?.length ? device.features : device.actions || device.capabilities || []).slice(0, 6)" :key="tag">{{ text(tag).replaceAll('_', ' ') }}</span></div></article></div></div></div>
      <div v-else class="tv-empty">No devices are available from enabled integrations yet.</div>
    </section>

    <section v-else-if="activeTab === 'rooms'" class="ti-rooms">
      <div class="tv-panel ti-room-toolbar"><div><span class="tv-eyebrow">Organization</span><h2>Rooms and device names</h2><p>Set Tater-friendly names, room assignments, and preferred playback targets.</p></div><div><input v-model="newRoomName" type="text" placeholder="New room name" @keyup.enter="createRoom" /><button class="tv-button primary" type="button" @click="createRoom">Create room</button><button class="tv-button" type="button" @click="refreshRegistry(true)">Refresh</button></div></div>
      <div class="ti-room-grid"><article v-for="room in rooms" :key="roomId(room)" class="tv-panel ti-room-card"><header><div><span class="tv-eyebrow">{{ room.source || 'integration' }}</span><h2>{{ room.name || 'Unassigned' }}</h2></div><span>{{ room.devices?.length || 0 }} devices</span></header><div v-if="roomId(room) !== 'unassigned'" class="ti-room-controls"><label><span>Room name</span><div><input v-model="roomNames[roomId(room)]" type="text" /><button class="tv-button" type="button" @click="renameRoom(room)">Rename</button></div></label><label><span>Preferred player</span><select :value="room.preferred_media_player || ''" @change="preferredPlayer(room, ($event.target as HTMLSelectElement).value)"><option value="">Auto</option><option v-if="room.preferred_media_player && !mediaPlayers.some((row: JsonRow) => text(row.value) === text(room.preferred_media_player))" :value="room.preferred_media_player">{{ room.preferred_media_player }} (saved)</option><option v-for="player in mediaPlayers" :key="player.value" :value="player.value">{{ player.label || player.value }}</option></select></label></div><div class="ti-room-devices"><article v-for="device in room.devices || []" :key="`${deviceIntegration(device)}:${deviceId(device)}`"><div><strong>{{ displayName(device) }}</strong><span>{{ device.integration_name || device.integration_id }} · {{ device.type || 'device' }}</span></div><label><span>Room</span><select :value="device.room_id || roomId(room)" @change="moveDevice(device, ($event.target as HTMLSelectElement).value)"><option value="unassigned">Unassigned</option><option v-for="targetRoom in rooms.filter((row: JsonRow) => roomId(row) !== 'unassigned')" :key="roomId(targetRoom)" :value="roomId(targetRoom)">{{ targetRoom.name }}</option></select></label><label><span>Tater name</span><div><input v-model="deviceNames[`${deviceIntegration(device)}:${deviceId(device)}`]" type="text" /><button class="tv-button" type="button" @click="renameDevice(device)">Save</button><button v-if="device.device_name_source === 'tater_override'" class="tv-button" type="button" @click="roomAction('clear_device_name', { integration_id: deviceIntegration(device), device_id: deviceId(device) })">Use integration</button></div></label></article><div v-if="!room.devices?.length" class="tv-empty compact">No devices assigned.</div></div></article></div>
    </section>

    <section v-else class="tv-panel ti-activity">
      <header class="tv-panel-head"><div><span class="tv-eyebrow">Live integrations</span><h2>Activity</h2><p>Connection health and recent device-level changes.</p></div><button class="tv-button" type="button" @click="refreshActivity()">Refresh</button></header>
      <div class="tv-metrics"><div v-for="provider in runtime.enabled_integrations || []" :key="provider"><span>{{ text(provider).replaceAll('_', ' ') }}</span><strong>{{ runtime[`${provider}_ws_connected`] || runtime[`${provider}_connected`] ? 'Connected' : 'Enabled' }}</strong></div><div><span>Events</span><strong>{{ runtime.last_event_seq || 0 }}</strong></div><div><span>Tracked states</span><strong>{{ runtimeStates.count || runtime.state_count || 0 }}</strong></div></div>
      <div class="ti-event-list"><article v-for="event in recentEvents" :key="event.seq"><span class="ti-provider">{{ text(event.provider).replaceAll('_', ' ') }}</span><div><strong>{{ eventTitle(event) }}</strong><small>{{ eventPayload(event).room || eventPayload(event).area || eventPayload(event).entity_id || eventPayload(event).ref || '' }}</small></div><span class="tv-state">{{ eventState(event) }}</span><time>{{ relativeTime(event.ts) }}</time></article><div v-if="!recentEvents.length" class="tv-empty">No recent device changes in the current activity window.</div></div>
    </section>

    <PopupTransition :open="Boolean(settingsIntegration)" @close="settingsIntegration = null"><form class="tv-modal" @submit.prevent="saveIntegrationSettings"><header><div><span class="tv-eyebrow">{{ settingsIntegration?.id }}</span><h2>{{ settingsIntegration ? displayName(settingsIntegration) : '' }} settings</h2></div><button class="tv-button" type="button" @click="settingsIntegration = null">Close</button></header><div class="tv-form-grid"><label v-for="field in settingsIntegration?.fields || []" :key="field.key" :class="{ full: field.full_width || field.type === 'textarea' }"><span>{{ field.label || field.key }}</span><input v-if="field.type === 'checkbox'" v-model="fieldValues[field.key]" class="tv-checkbox" type="checkbox" /><textarea v-else-if="field.type === 'textarea'" v-model="fieldValues[field.key]" :rows="field.rows || 3" :placeholder="field.placeholder" /><input v-else v-model="fieldValues[field.key]" :type="['password','number','email','url'].includes(field.type) ? field.type : 'text'" :min="field.min" :max="field.max" :step="field.step" :placeholder="field.placeholder" /><small>{{ field.description }}</small></label></div><div v-if="settingsIntegration?.actions?.length" class="ti-modal-actions"><span>Actions</span><button v-for="action in settingsIntegration?.actions || []" :key="action.id" class="tv-button" type="button" @click="runIntegrationAction(action)">{{ action.label || action.id }}</button></div><footer><span>{{ busy || status }}</span><button v-if="settingsIntegration?.fields?.length" class="tv-button primary" type="submit">Save settings</button></footer></form></PopupTransition>
  </div>
</template>
