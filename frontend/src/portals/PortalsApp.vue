<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { getJson, postJson } from "../shared/api";
import ManifestField from "../shared/ManifestField.vue";
import PopupTransition from "../shared/PopupTransition.vue";
import type { JsonRow, PortalPayload, PortalsMountOptions } from "./types";

const props = defineProps<{
  state: { payload: PortalPayload };
  options: PortalsMountOptions;
}>();

const tabs = [
  { id: "installed", label: "Installed" },
  { id: "store", label: "Store" },
  { id: "manage", label: "Manage" },
  { id: "repos", label: "Repositories" },
];
const activeTab = ref(tabs.some((tab) => tab.id === props.options.initialTab) ? String(props.options.initialTab) : "installed");
const busy = ref("");
const status = ref("");
const error = ref("");
const purgeIds = ref<Record<string, boolean>>({});
const repoName = ref("");
const repoUrl = ref("");
const draftRepos = ref<JsonRow[]>([]);
const settingsPortal = ref<JsonRow | null>(null);
const fieldValues = ref<JsonRow>({});

const runtime = computed(() => props.state.payload?.runtime || {});
const shop = computed(() => props.state.payload?.shop || {});
const runtimeItems = computed(() => Array.isArray(runtime.value.items) ? runtime.value.items : []);
const installed = computed(() => Array.isArray(shop.value.installed) ? shop.value.installed : []);
const catalog = computed(() => Array.isArray(shop.value.catalog) ? shop.value.catalog : []);
const available = computed(() => catalog.value.filter((row: JsonRow) => !row.installed).sort(compareRows));
const updates = computed(() => installed.value.filter((row: JsonRow) => row.update_available));
const runningCount = computed(() => runtimeItems.value.filter((row: JsonRow) => Boolean(row.running)).length);
const runtimeByKey = computed(() => new Map(runtimeItems.value.map((row: JsonRow) => [canonical(row.key), row])));
const installedByRuntimeKey = computed(() => {
  const result = new Map<string, JsonRow>();
  installed.value.forEach((row: JsonRow) => {
    const moduleKey = text(row.module_key || `${row.id}_portal`);
    if (moduleKey) result.set(canonical(moduleKey), row);
    if (row.id) result.set(canonical(row.id), row);
  });
  return result;
});
const installedRows = computed(() => {
  const seen = new Set<string>();
  const rows: Array<{ key: string; runtime: JsonRow | null; shop: JsonRow | null }> = runtimeItems.value.map((runtimeRow: JsonRow) => {
    const key = text(runtimeRow.key);
    const shopRow = installedByRuntimeKey.value.get(canonical(key)) || installedByRuntimeKey.value.get(canonical(stripPortalSuffix(key))) || null;
    if (shopRow) seen.add(canonical(shopRow.id));
    return { key, runtime: runtimeRow, shop: shopRow };
  });
  installed.value.forEach((shopRow: JsonRow) => {
    if (!seen.has(canonical(shopRow.id))) rows.push({ key: text(shopRow.module_key || `${shopRow.id}_portal`), runtime: null, shop: shopRow });
  });
  return rows.sort((a, b) => rowName(a).localeCompare(rowName(b), undefined, { sensitivity: "base", numeric: true }));
});

function text(value: unknown): string { return String(value ?? "").trim(); }
function encode(value: unknown): string { return encodeURIComponent(text(value)); }
function canonical(value: unknown): string { return text(value).toLowerCase(); }
function stripPortalSuffix(value: unknown): string { return text(value).replace(/_portal$/i, ""); }
function compareRows(a: JsonRow, b: JsonRow): number { return text(a.name || a.id).localeCompare(text(b.name || b.id), undefined, { sensitivity: "base", numeric: true }); }
function rowName(row: { key: string; runtime: JsonRow | null; shop: JsonRow | null }): string { return text(row.runtime?.label || row.shop?.name || stripPortalSuffix(row.key)); }
function rowDescription(row: { key: string; runtime: JsonRow | null; shop: JsonRow | null }): string { return text(row.shop?.description || "Local Portal module."); }
function runtimeForShop(row: JsonRow): JsonRow | null {
  const moduleKey = text(row.module_key || `${row.id}_portal`);
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

async function refresh(quiet = false) {
  if (!quiet) busy.value = "Refreshing Portals…";
  error.value = "";
  try {
    const [runtimeResult, shopResult] = await Promise.all([
      getJson<JsonRow>(props.options.endpoints.runtime),
      getJson<JsonRow>(props.options.endpoints.shop),
    ]);
    props.state.payload = { runtime: runtimeResult, shop: shopResult };
    syncDraftRepos();
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : "Portal refresh failed.", "error");
  } finally { if (!quiet) busy.value = ""; }
}

async function runtimeAction(row: JsonRow, action: "start" | "stop") {
  const key = text(row.key);
  if (!key) return;
  busy.value = `${action === "start" ? "Starting" : "Stopping"} ${key}…`;
  try {
    await postJson<JsonRow>(`${props.options.endpoints.runtime}/${encode(key)}/${action}`);
    notify(`${key} ${action === "start" ? "started" : "stopped"}.`);
    await refresh(true);
    props.options.onHealthRefresh?.();
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : `Portal ${action} failed.`, "error");
  } finally { busy.value = ""; }
}

async function shopAction(action: string, id = "") {
  if (action === "remove" && !window.confirm(`Remove ${id}?${purgeIds.value[id] ? " Its saved data will also be deleted." : ""}`)) return;
  busy.value = `${action.replaceAll("-", " ")} ${id || "Portals"}…`;
  error.value = "";
  try {
    const payload: JsonRow = id ? { id } : {};
    if (action === "remove") payload.purge_redis = Boolean(purgeIds.value[id]);
    const result = await postJson<JsonRow>(`${props.options.endpoints.shop}/${action}`, payload);
    const updated = Array.isArray(result.updated) ? result.updated.length : 0;
    const failed = Array.isArray(result.failed) ? result.failed.length : 0;
    const fallback = action === "update-all" ? `Update-all completed. Updated ${updated}, failed ${failed}.` : "Portal action completed.";
    notify(text(result.message) || fallback, failed ? "error" : "success");
    await refresh(true);
    if (action === "install") activeTab.value = "installed";
    props.options.onHealthRefresh?.();
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : "Portal action failed.", "error");
  } finally { busy.value = ""; }
}

function normalizeValue(field: JsonRow): unknown {
  const raw = field.value ?? field.default ?? "";
  const type = text(field.type).toLowerCase();
  if (type === "checkbox") {
    if (typeof raw === "string") return ["1", "true", "yes", "on", "enabled"].includes(raw.toLowerCase());
    return Boolean(raw);
  }
  if (type === "number" || type === "range") return raw === "" ? "" : Number(raw);
  if (type === "multiselect") {
    if (Array.isArray(raw)) return [...raw];
    const value = text(raw);
    if (!value) return [];
    try { const parsed = JSON.parse(value); if (Array.isArray(parsed)) return parsed; } catch { /* Use CSV. */ }
    return value.split(",").map((part) => part.trim()).filter(Boolean);
  }
  return raw;
}
function fieldVisible(field: JsonRow): boolean {
  const conditions = Array.isArray(field.show_when_all) ? field.show_when_all : field.show_when && typeof field.show_when === "object" ? [field.show_when] : [];
  return conditions.every((condition: JsonRow) => {
    const source = text(condition.source_key ?? condition.key);
    if (!source) return true;
    const allowed = [...(condition.any_of || []), ...(condition.values || []), ...(condition.equals !== undefined ? [condition.equals] : []), ...(condition.value !== undefined ? [condition.value] : [])].map((value) => String(value ?? "").trim());
    if (!allowed.length) return true;
    const current = typeof fieldValues.value[source] === "boolean" ? fieldValues.value[source] ? "true" : "false" : String(fieldValues.value[source] ?? "").trim();
    return allowed.includes(current);
  });
}
function openSettings(row: JsonRow) {
  settingsPortal.value = row;
  fieldValues.value = Object.fromEntries((Array.isArray(row.settings) ? row.settings : []).filter((field: JsonRow) => text(field.key)).map((field: JsonRow) => [text(field.key), normalizeValue(field)]));
}
async function saveSettings() {
  const portal = settingsPortal.value;
  if (!portal) return;
  const key = text(portal.key);
  busy.value = `Saving ${text(portal.label || key)}…`;
  try {
    const values = Object.fromEntries((portal.settings || []).filter((field: JsonRow) => {
      const type = text(field.type).toLowerCase();
      return text(field.key) && !["section", "header", "readonly", "read_only", "led_preview"].includes(type) && fieldVisible(field);
    }).map((field: JsonRow) => [text(field.key), fieldValues.value[text(field.key)]]));
    await postJson<JsonRow>(`${props.options.endpoints.runtime}/${encode(key)}/settings`, { values });
    notify(`Saved settings for ${text(portal.label || key)}.`);
    settingsPortal.value = null;
    await refresh(true);
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : "Portal settings save failed.", "error");
  } finally { busy.value = ""; }
}

function addRepo() {
  const url = repoUrl.value.trim();
  if (!url) { notify("Repository URL is required.", "error"); return; }
  if (draftRepos.value.some((row) => text(row.url).toLowerCase() === url.toLowerCase())) { notify("That repository is already added.", "error"); return; }
  draftRepos.value.push({ name: repoName.value.trim(), url });
  repoName.value = "";
  repoUrl.value = "";
  status.value = "Repository added. Save repositories to apply it.";
  error.value = "";
}
async function saveRepos() {
  busy.value = "Saving Portal repositories…";
  try {
    await postJson<JsonRow>(`${props.options.endpoints.shop}/repos`, { repos: draftRepos.value });
    notify("Portal repositories saved.");
    await refresh(true);
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : "Repository save failed.", "error");
  } finally { busy.value = ""; }
}
function handleEscape(event: KeyboardEvent) { if (event.key === "Escape") settingsPortal.value = null; }

watch(() => props.state.payload, syncDraftRepos, { deep: false });
syncDraftRepos();
window.addEventListener("keydown", handleEscape);
onBeforeUnmount(() => window.removeEventListener("keydown", handleEscape));
</script>

<template>
  <div class="tater-vue-surface tp-portals">
    <header class="tv-page-heading">
      <div><span class="tv-eyebrow">Conversation surfaces</span><h1>Portals</h1><p>Manage where Tater listens and responds, along with every Portal’s runtime and updates.</p></div>
      <div class="tv-heading-actions"><span class="tv-live-pill" :class="{ busy: Boolean(busy) }"><i />{{ busy || 'Live' }}</span><button class="tv-button" type="button" @click="refresh()">Refresh</button></div>
    </header>

    <div class="tv-metrics">
      <div><span>Installed</span><strong>{{ installed.length || runtimeItems.length }}</strong></div>
      <div><span>Running</span><strong>{{ runningCount }}</strong></div>
      <div><span>Store</span><strong>{{ catalog.length }}</strong></div>
      <div><span>Updates</span><strong>{{ Number(shop.updates_available || updates.length) }}</strong></div>
    </div>
    <div v-if="status || error" class="tv-notice" :class="{ error: Boolean(error) }">{{ error || status }}</div>
    <div v-if="shop.errors?.length" class="tv-notice error">{{ shop.errors.join(' • ') }}</div>

    <nav class="tv-tabs tp-tabs" aria-label="Portal sections">
      <button v-for="tab in tabs" :key="tab.id" type="button" :class="{ active: activeTab === tab.id }" @click="activeTab = tab.id">{{ tab.label }}<span v-if="tab.id === 'manage' && updates.length">{{ updates.length }}</span></button>
    </nav>

    <section v-if="activeTab === 'installed'" class="tp-card-grid">
      <article v-for="row in installedRows" :key="row.key" class="tv-panel tp-portal-card">
        <header><div><span class="tv-eyebrow">{{ row.key }}</span><h2>{{ rowName(row) }}</h2></div><span class="tv-state" :class="{ good: row.runtime?.running, pending: row.runtime?.desired_running && !row.runtime?.running }">{{ stateLabel(row.runtime) }}</span></header>
        <p>{{ rowDescription(row) }}</p>
        <div class="tp-version"><span>Installed {{ row.shop?.installed_ver || '0.0.0' }}</span><span>Store {{ row.shop?.store_ver || '-' }}</span><span>{{ row.shop?.source_label || 'local' }}</span></div>
        <footer><button v-if="row.runtime?.settings?.length" class="tv-button" type="button" @click="openSettings(row.runtime)">Settings</button><span v-else>{{ row.runtime ? 'No configurable settings' : 'Runtime unavailable' }}</span><button v-if="row.runtime" class="tv-button" :class="{ primary: !row.runtime.running }" type="button" @click="runtimeAction(row.runtime, row.runtime.running ? 'stop' : 'start')">{{ row.runtime.running ? 'Stop' : 'Start' }}</button></footer>
      </article>
      <div v-if="!installedRows.length" class="tv-empty">No installed Portals found.</div>
    </section>

    <section v-else-if="activeTab === 'store'" class="tp-card-grid">
      <article v-for="row in available" :key="row.id" class="tv-panel tp-portal-card"><header><div><span class="tv-eyebrow">{{ row.id }}</span><h2>{{ row.name || row.id }}</h2></div><span class="tv-state">v{{ row.version || '-' }}</span></header><p>{{ row.description || 'No description provided.' }}</p><footer><span>{{ row.source_label || 'Tater Shop' }}</span><button class="tv-button primary" type="button" @click="shopAction('install', row.id)">Install</button></footer></article>
      <div v-if="!available.length" class="tv-empty">No additional Portals are available from the configured repositories.</div>
    </section>

    <section v-else-if="activeTab === 'manage'" class="tp-manage-list">
      <div class="tv-panel tp-manage-toolbar"><div><span class="tv-eyebrow">Maintenance</span><h2>Manage installed Portals</h2><p>{{ updates.length }} update{{ updates.length === 1 ? '' : 's' }} available. Running Portals restart automatically after an update.</p></div><button class="tv-button primary" type="button" :disabled="!updates.length" @click="shopAction('update-all')">Update all</button></div>
      <article v-for="row in installed.slice().sort(compareRows)" :key="row.id" class="tv-panel tp-manage-row"><div><strong>{{ row.name || row.id }}</strong><span>{{ row.installed_ver || '0.0.0' }} → {{ row.store_ver || '-' }} · {{ stateLabel(runtimeForShop(row)) }}</span></div><div class="ti-row-actions"><button class="tv-button" type="button" :disabled="!row.update_available" @click="shopAction('update', row.id)">{{ row.update_available ? 'Update' : 'Current' }}</button><button v-if="runtimeForShop(row)" class="tv-button" type="button" @click="runtimeAction(runtimeForShop(row)!, runtimeForShop(row)?.running ? 'stop' : 'start')">{{ runtimeForShop(row)?.running ? 'Stop' : 'Start' }}</button><label class="ti-purge"><input v-model="purgeIds[row.id]" type="checkbox" /> Delete data</label><button class="tv-button danger" type="button" @click="shopAction('remove', row.id)">Remove</button></div></article>
      <div v-if="!installed.length" class="tv-empty">No installed Portals found.</div>
    </section>

    <section v-else class="tv-panel tp-repos">
      <header><div><span class="tv-eyebrow">Trusted sources</span><h2>Portal repositories</h2><p>The built-in Portal repository stays available. Add other trusted manifests below.</p></div></header>
      <article class="ti-repo-row builtin"><div><strong>{{ shop.repos?.default?.name || 'Default' }}</strong><code>{{ shop.repos?.default?.url || '(not set)' }}</code></div><span>Built-in</span></article>
      <article v-for="(repo, index) in draftRepos" :key="`${repo.url}-${index}`" class="ti-repo-row"><div><strong>{{ repo.name || 'Additional repository' }}</strong><code>{{ repo.url }}</code></div><button class="tv-button" type="button" @click="draftRepos.splice(index, 1)">Remove</button></article>
      <div v-if="!draftRepos.length" class="tv-empty compact">No additional repositories configured.</div>
      <div class="tp-repo-form"><label><span>Name (optional)</span><input v-model="repoName" type="text" placeholder="My Portal Repo" /></label><label><span>Repository URL</span><input v-model="repoUrl" type="url" placeholder="https://example.com/portals.json" @keyup.enter="addRepo" /></label><button class="tv-button" type="button" @click="addRepo">Add</button><button class="tv-button primary" type="button" @click="saveRepos">Save repositories</button></div>
    </section>

    <PopupTransition :open="Boolean(settingsPortal)" @close="settingsPortal = null"><form class="tv-modal tp-settings-modal" @submit.prevent="saveSettings"><header><div><span class="tv-eyebrow">{{ settingsPortal?.key }}</span><h2>{{ settingsPortal?.label || settingsPortal?.key }} settings</h2></div><button class="tv-button" type="button" @click="settingsPortal = null">Close</button></header><div class="tvb-field-grid"><ManifestField v-for="(field, index) in settingsPortal?.settings || []" :key="field.key || index" v-model="fieldValues[field.key]" :field="field" :all-values="fieldValues" @error="notify($event, 'error')" @notify="notify" /></div><footer><span>{{ busy || status }}</span><button class="tv-button primary" type="submit">Save settings</button></footer></form></PopupTransition>
  </div>
</template>
