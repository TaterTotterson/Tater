<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { getJson, postJson } from "../shared/api";
import ManifestField from "../shared/ManifestField.vue";
import PopupTransition from "../shared/PopupTransition.vue";
import type { JsonRow, VerbaPayload, VerbasMountOptions } from "./types";

const props = defineProps<{
  state: { payload: VerbaPayload };
  options: VerbasMountOptions;
}>();

const tabs = [
  { id: "installed", label: "Installed" },
  { id: "store", label: "Store" },
  { id: "manage", label: "Manage" },
  { id: "repos", label: "Repositories" },
];
const activeTab = ref(tabs.some((tab) => tab.id === props.options.initialTab) ? String(props.options.initialTab) : "installed");
const selectTab = (tab: string) => { if (tabs.some((row) => row.id === tab)) activeTab.value = tab; };
const busy = ref("");
const status = ref("");
const error = ref("");
const purgeIds = ref<Record<string, boolean>>({});
const repoName = ref("");
const repoUrl = ref("");
const draftRepos = ref<JsonRow[]>([]);
const repoSection = ref("trusted");
const settingsVerba = ref<JsonRow | null>(null);
const fieldValues = ref<JsonRow>({});

const runtime = computed(() => props.state.payload?.runtime || {});
const shop = computed(() => props.state.payload?.shop || {});
const runtimeItems = computed(() => Array.isArray(runtime.value.items) ? runtime.value.items : []);
const installed = computed(() => Array.isArray(shop.value.installed) ? shop.value.installed : []);
const catalog = computed(() => Array.isArray(shop.value.catalog) ? shop.value.catalog : []);
const available = computed(() => catalog.value.filter((row: JsonRow) => !row.installed).sort(compareRows));
const updates = computed(() => installed.value.filter((row: JsonRow) => row.update_available));
const enabledCount = computed(() => runtimeItems.value.filter((row: JsonRow) => Boolean(row.enabled)).length);
const trustedRepos = computed(() => Array.isArray(shop.value.repos?.trusted) ? shop.value.repos.trusted : []);
const runtimeById = computed(() => new Map(runtimeItems.value.map((row: JsonRow) => [canonicalId(row.id), row])));
const installedRows = computed(() => {
  const seen = new Set<string>();
  const rows: Array<{ id: string; runtime: JsonRow | null; shop: JsonRow | null }> = installed.value.map((shopRow: JsonRow) => {
    const id = text(shopRow.id || shopRow.module_key || shopRow.key);
    seen.add(canonicalId(id));
    return { id, runtime: runtimeById.value.get(canonicalId(id)) || null, shop: shopRow };
  });
  runtimeItems.value.forEach((runtimeRow: JsonRow) => {
    const id = text(runtimeRow.id);
    if (id && !seen.has(canonicalId(id))) rows.push({ id, runtime: runtimeRow, shop: null });
  });
  return rows.sort((a, b) => rowName(a).localeCompare(rowName(b), undefined, { sensitivity: "base", numeric: true }));
});

function text(value: unknown): string { return String(value ?? "").trim(); }
function encode(value: unknown): string { return encodeURIComponent(text(value)); }
function canonicalId(value: unknown): string { return text(value).toLowerCase(); }
function compareRows(a: JsonRow, b: JsonRow): number { return text(a.name || a.id).localeCompare(text(b.name || b.id), undefined, { sensitivity: "base", numeric: true }); }
function rowName(row: { id: string; runtime: JsonRow | null; shop: JsonRow | null }): string { return text(row.runtime?.name || row.shop?.name || row.id); }
function rowDescription(row: { id: string; runtime: JsonRow | null; shop: JsonRow | null }): string { return text(row.shop?.description || row.runtime?.description || "No description provided."); }
function rowPlatforms(row: { id: string; runtime: JsonRow | null; shop: JsonRow | null }): string[] {
  const values = Array.isArray(row.runtime?.platforms) && row.runtime?.platforms.length ? row.runtime.platforms : Array.isArray(row.shop?.platforms) ? row.shop.platforms : [];
  return values.map((value: unknown) => text(value).replaceAll("_", " ")).filter(Boolean);
}
function notify(message: string, tone = "success") {
  status.value = message;
  error.value = tone === "error" ? message : "";
  props.options.onToast?.(message, tone);
}
function syncDraftRepos() {
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
    await postJson<JsonRow>(`${props.options.endpoints.shop}/repos`, { repos: combinedRepos() });
    notify(`${text(repo.name || repo.repository)} ${enabling ? "added to" : "removed from"} the Verba Store.`);
    await refresh(true);
  } catch (requestError) {
    repo.enabled = wasEnabled;
    notify(requestError instanceof Error ? requestError.message : "Trusted repository update failed.", "error");
  } finally { busy.value = ""; }
}

async function refresh(quiet = false) {
  if (!quiet) busy.value = "Refreshing Verba…";
  error.value = "";
  try {
    const [runtimeResult, shopResult] = await Promise.all([
      getJson<JsonRow>(props.options.endpoints.runtime),
      getJson<JsonRow>(props.options.endpoints.shop),
    ]);
    props.state.payload = { runtime: runtimeResult, shop: shopResult };
    syncDraftRepos();
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : "Verba refresh failed.", "error");
  } finally { if (!quiet) busy.value = ""; }
}

async function toggleVerba(id: string, enabled: boolean) {
  busy.value = `${enabled ? "Enabling" : "Disabling"} ${id}…`;
  try {
    await postJson<JsonRow>(`${props.options.endpoints.runtime}/${encode(id)}/enabled`, { enabled });
    notify(`${id} ${enabled ? "enabled" : "disabled"}.`);
    await refresh(true);
    props.options.onHealthRefresh?.();
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : "Verba toggle failed.", "error");
  } finally { busy.value = ""; }
}

async function shopAction(action: string, id = "") {
  if (action === "remove" && !window.confirm(`Remove ${id}?${purgeIds.value[id] ? " Its saved data will also be deleted." : ""}`)) return;
  busy.value = `${action.replaceAll("-", " ")} ${id || "Verba"}…`;
  error.value = "";
  try {
    const payload: JsonRow = id ? { id } : {};
    if (action === "remove") payload.purge_redis = Boolean(purgeIds.value[id]);
    const result = await postJson<JsonRow>(`${props.options.endpoints.shop}/${action}`, payload);
    const updated = Array.isArray(result.updated) ? result.updated.length : 0;
    const failed = Array.isArray(result.failed) ? result.failed.length : 0;
    const fallback = action === "update-all" ? `Update-all completed. Updated ${updated}, failed ${failed}.` : "Verba action completed.";
    notify(text(result.message) || fallback, failed ? "error" : "success");
    await refresh(true);
    if (action === "install") activeTab.value = "installed";
    props.options.onHealthRefresh?.();
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : "Verba action failed.", "error");
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
  settingsVerba.value = row;
  fieldValues.value = Object.fromEntries((Array.isArray(row.settings) ? row.settings : []).filter((field: JsonRow) => text(field.key)).map((field: JsonRow) => [text(field.key), normalizeValue(field)]));
}
async function saveSettings() {
  const verba = settingsVerba.value;
  if (!verba) return;
  busy.value = `Saving ${text(verba.name || verba.id)}…`;
  try {
    const values = Object.fromEntries((verba.settings || []).filter((field: JsonRow) => {
      const type = text(field.type).toLowerCase();
      return text(field.key) && !["section", "header", "readonly", "read_only", "led_preview"].includes(type) && fieldVisible(field);
    }).map((field: JsonRow) => [text(field.key), fieldValues.value[text(field.key)]]));
    await postJson<JsonRow>(`${props.options.endpoints.runtime}/${encode(verba.id)}/settings`, { values });
    notify(`Saved settings for ${text(verba.name || verba.id)}.`);
    settingsVerba.value = null;
    await refresh(true);
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : "Settings save failed.", "error");
  } finally { busy.value = ""; }
}

function addRepo() {
  const url = repoUrl.value.trim();
  if (!url) { notify("Repo URL is required.", "error"); return; }
  if ([...draftRepos.value, ...trustedRepos.value].some((row) => text(row.url).toLowerCase() === url.toLowerCase())) { notify("That repository is already listed.", "error"); return; }
  draftRepos.value.push({ name: repoName.value.trim(), url });
  repoName.value = "";
  repoUrl.value = "";
  status.value = "Repository added. Save repositories to apply it.";
  error.value = "";
}
async function saveRepos() {
  busy.value = "Saving Verba repositories…";
  try {
    await postJson<JsonRow>(`${props.options.endpoints.shop}/repos`, { repos: combinedRepos() });
    notify("Verba repositories saved.");
    await refresh(true);
  } catch (requestError) {
    notify(requestError instanceof Error ? requestError.message : "Repository save failed.", "error");
  } finally { busy.value = ""; }
}
function handleEscape(event: KeyboardEvent) { if (event.key === "Escape") settingsVerba.value = null; }

watch(() => props.state.payload, syncDraftRepos, { deep: false });
syncDraftRepos();
window.addEventListener("keydown", handleEscape);
onBeforeUnmount(() => window.removeEventListener("keydown", handleEscape));
defineExpose({ select: selectTab });
</script>

<template>
  <div class="tater-vue-surface tvb-verbas">
    <header class="tv-page-heading">
      <div><span class="tv-eyebrow">Tater tools</span><h1>Verba</h1><p>Enable Tater’s tools, manage their settings, and keep every Verba current.</p></div>
      <div class="tv-heading-actions"><span class="tv-live-pill" :class="{ busy: Boolean(busy) }"><i />{{ busy || 'Ready' }}</span><button class="tv-button" type="button" @click="refresh()">Refresh</button></div>
    </header>

    <div class="tv-metrics">
      <div><span>Installed</span><strong>{{ installed.length || runtimeItems.length }}</strong></div>
      <div><span>Enabled</span><strong>{{ enabledCount }}</strong></div>
      <div><span>Store</span><strong>{{ catalog.length }}</strong></div>
      <div><span>Updates</span><strong>{{ Number(shop.updates_available || updates.length) }}</strong></div>
    </div>
    <div v-if="status || error" class="tv-notice" :class="{ error: Boolean(error) }">{{ error || status }}</div>
    <div v-if="shop.errors?.length" class="tv-notice error">{{ shop.errors.join(' • ') }}</div>

    <nav class="tv-tabs tvb-tabs" aria-label="Verba sections">
      <button v-for="tab in tabs" :key="tab.id" type="button" :class="{ active: activeTab === tab.id }" @click="selectTab(tab.id)">{{ tab.label }}<span v-if="tab.id === 'manage' && updates.length">{{ updates.length }}</span></button>
    </nav>

    <section v-if="activeTab === 'installed'" class="tvb-card-grid">
      <article v-for="row in installedRows" :key="row.id" class="tv-panel tvb-verba-card">
        <header><div><span class="tv-eyebrow">{{ row.id }}</span><h2>{{ rowName(row) }}</h2></div><span class="tv-state" :class="{ good: row.runtime?.enabled }">{{ row.runtime?.enabled ? 'Enabled' : 'Disabled' }}</span></header>
        <p>{{ rowDescription(row) }}</p>
        <div class="tvb-version"><span>Installed {{ row.shop?.installed_ver || '0.0.0' }}</span><span>Store {{ row.shop?.store_ver || '-' }}</span><span>{{ row.shop?.source_label || 'local' }}</span></div>
        <div v-if="rowPlatforms(row).length" class="ti-tags"><span v-for="platform in rowPlatforms(row).slice(0, 12)" :key="platform">{{ platform }}</span></div>
        <footer><button v-if="row.runtime?.settings?.length" class="tv-button" type="button" @click="openSettings(row.runtime)">Settings</button><span v-else>{{ row.runtime ? 'No configurable settings' : 'Runtime unavailable' }}</span><button v-if="row.runtime" class="tv-button" :class="{ primary: !row.runtime.enabled }" type="button" @click="toggleVerba(row.id, !row.runtime.enabled)">{{ row.runtime.enabled ? 'Disable' : 'Enable' }}</button></footer>
      </article>
      <div v-if="!installedRows.length" class="tv-empty">No installed Verba found.</div>
    </section>

    <section v-else-if="activeTab === 'store'" class="tvb-card-grid">
      <article v-for="row in available" :key="row.id" class="tv-panel tvb-verba-card">
        <header><div><span class="tv-eyebrow">{{ row.id }}</span><h2>{{ row.name || row.id }}</h2></div><span class="tv-state">v{{ row.version || '-' }}</span></header>
        <p>{{ row.description || 'No description provided.' }}</p>
        <div v-if="row.platforms?.length" class="ti-tags"><span v-for="platform in row.platforms.slice(0, 12)" :key="platform">{{ text(platform).replaceAll('_', ' ') }}</span></div>
        <footer><span>{{ row.source_label || 'Tater Shop' }}</span><button class="tv-button primary" type="button" @click="shopAction('install', row.id)">Install</button></footer>
      </article>
      <div v-if="!available.length" class="tv-empty">No additional Verba are available from the configured repositories.</div>
    </section>

    <section v-else-if="activeTab === 'manage'" class="tvb-manage-list tv-manage-workspace">
      <div class="tv-panel tvb-manage-toolbar tv-manage-hero">
        <div class="tv-manage-hero-copy"><span class="tv-eyebrow">Manage library</span><h2>Verba control center</h2><p>Keep Tater’s tools current, choose what is enabled, and remove tools you no longer use.</p></div>
        <div class="tv-manage-overview"><div><span>Installed</span><strong>{{ installed.length }}</strong></div><div :class="{ attention: updates.length }"><span>Updates ready</span><strong>{{ updates.length }}</strong></div><button class="tv-button primary" type="button" :disabled="!updates.length" @click="shopAction('update-all')">Update all</button></div>
      </div>
      <article v-for="row in installed.slice().sort(compareRows)" :key="row.id" class="tv-panel tvb-manage-row tv-manage-card" :class="{ 'has-update': row.update_available }">
        <div class="tv-manage-identity"><span class="tv-manage-monogram">{{ text(row.name || row.id).charAt(0).toUpperCase() }}</span><div><span class="tv-eyebrow">{{ row.id }}</span><h3>{{ row.name || row.id }}</h3><small>{{ row.source_label || 'Local Verba' }}</small></div></div>
        <div class="tv-manage-version"><div><span>Installed</span><strong>{{ row.installed_ver || '0.0.0' }}</strong></div><i>→</i><div><span>Latest</span><strong>{{ row.store_ver || '-' }}</strong></div><span class="tv-state" :class="row.update_available ? 'pending' : 'good'">{{ row.update_available ? 'Update ready' : 'Current' }}</span></div>
        <div class="tv-manage-actions"><span v-if="runtimeById.has(canonicalId(row.id))" class="tv-manage-runtime" :class="{ online: runtimeById.get(canonicalId(row.id))?.enabled }"><i />{{ runtimeById.get(canonicalId(row.id))?.enabled ? 'Enabled' : 'Disabled' }}</span><button class="tv-button" :class="{ primary: row.update_available }" type="button" :disabled="!row.update_available" @click="shopAction('update', row.id)">{{ row.update_available ? 'Update' : 'Current' }}</button><button v-if="runtimeById.has(canonicalId(row.id))" class="tv-button" type="button" @click="toggleVerba(row.id, !runtimeById.get(canonicalId(row.id))?.enabled)">{{ runtimeById.get(canonicalId(row.id))?.enabled ? 'Disable' : 'Enable' }}</button><label v-if="!row.required" class="ti-purge"><input v-model="purgeIds[row.id]" type="checkbox" /> Delete data</label><button v-if="!row.required" class="tv-button danger" type="button" @click="shopAction('remove', row.id)">Remove</button><span v-else class="tv-state good">Required</span></div>
      </article>
      <div v-if="!installed.length" class="tv-empty">No installed Verba found.</div>
    </section>

    <section v-else class="tv-panel tvb-repos tv-repository-manager">
      <header class="tv-repository-heading"><div><span class="tv-eyebrow">Repository library</span><h2>Verba repositories</h2><p>Choose a Tater-trusted source or add your own manifest.</p></div></header>
      <nav class="tv-repository-tabs" aria-label="Verba repository sources"><button type="button" :class="{ active: repoSection === 'trusted' }" @click="repoSection = 'trusted'">Trusted repositories</button><button type="button" :class="{ active: repoSection === 'custom' }" @click="repoSection = 'custom'">Custom repositories</button></nav>
      <div v-if="repoSection === 'trusted'" class="tv-trusted-repositories">
        <p class="tv-repository-intro">Curated sources are reviewed by Tater. Select a card to add or remove its Verba from your Store automatically.</p>
        <div v-if="shop.repos?.trusted_error" class="tv-repository-warning">The trusted directory is temporarily unavailable. Your enabled repositories are unchanged.</div>
        <div class="tv-trusted-repo-grid">
          <article class="tv-trusted-repo-card builtin selected" aria-label="Built-in Tater Shop repository"><span class="tv-repo-check">✓</span><div class="tv-repo-card-top"><span class="tv-repo-monogram">T</span><div><span class="tv-eyebrow">Always available</span><h3>{{ shop.repos?.default?.name || 'Tater Shop' }}</h3><p>by <strong>Tater Assistant</strong></p></div></div><p>The official built-in Verba catalog.</p><footer><span class="tv-repo-enabled">Built in</span></footer></article>
          <article v-for="repo in trustedRepos" :key="repo.id || repo.url" class="tv-trusted-repo-card" :class="{ selected: repo.enabled }" role="button" tabindex="0" :aria-pressed="Boolean(repo.enabled)" @click="toggleTrustedRepo(repo)" @keydown.enter.prevent="toggleTrustedRepo(repo)" @keydown.space.prevent="toggleTrustedRepo(repo)"><span class="tv-repo-check">{{ repo.enabled ? '✓' : '+' }}</span><div class="tv-repo-card-top"><span class="tv-repo-monogram">{{ text(repo.repository || repo.name).charAt(0).toUpperCase() }}</span><div><span class="tv-eyebrow">Trusted Verba source</span><h3>{{ repo.repository || repo.name }}</h3><p>by <a v-if="repo.author_url" :href="repo.author_url" target="_blank" rel="noreferrer" @click.stop>{{ repo.author || 'Community author' }}</a><strong v-else>{{ repo.author || 'Community author' }}</strong></p></div></div><p>{{ repo.description || 'Additional Verba for the Tater Store.' }}</p><div v-if="repo.tags?.length" class="ti-tags"><span v-for="tag in repo.tags" :key="tag">{{ tag }}</span></div><footer><a v-if="repo.homepage" :href="repo.homepage" target="_blank" rel="noreferrer" @click.stop>View repository ↗</a><span :class="repo.enabled ? 'tv-repo-enabled' : 'tv-repo-available'">{{ repo.enabled ? 'Added to Store' : 'Select to add' }}</span></footer></article>
        </div>
        <div v-if="!trustedRepos.length" class="tv-empty compact">No additional trusted Verba repositories are listed yet.</div>
      </div>
      <div v-else class="tv-custom-repositories">
        <p class="tv-repository-intro">Custom manifests are managed by you and are not reviewed by Tater.</p>
        <article v-for="(repo, index) in draftRepos" :key="`${repo.url}-${index}`" class="ti-repo-row"><div><strong>{{ repo.name || 'Custom repository' }}</strong><code>{{ repo.url }}</code></div><button class="tv-button" type="button" @click="draftRepos.splice(index, 1)">Remove</button></article>
        <div v-if="!draftRepos.length" class="tv-empty compact">No custom repositories configured.</div>
        <div class="tvb-repo-form"><label><span>Name (optional)</span><input v-model="repoName" type="text" placeholder="My Verba Repo" /></label><label><span>Manifest URL</span><input v-model="repoUrl" type="url" placeholder="https://example.com/verbas.json" @keyup.enter="addRepo" /></label><button class="tv-button" type="button" @click="addRepo">Add</button><button class="tv-button primary" type="button" @click="saveRepos">Save custom repositories</button></div>
      </div>
    </section>

    <PopupTransition :open="Boolean(settingsVerba)" @close="settingsVerba = null">
        <form class="tv-modal tvb-settings-modal" @submit.prevent="saveSettings">
          <header><div><span class="tv-eyebrow">{{ settingsVerba?.id }}</span><h2>{{ settingsVerba?.name || settingsVerba?.id }} settings</h2></div><button class="tv-button" type="button" @click="settingsVerba = null">Close</button></header>
          <div class="tvb-field-grid"><ManifestField v-for="(field, index) in settingsVerba?.settings || []" :key="field.key || index" v-model="fieldValues[field.key]" :field="field" :all-values="fieldValues" @error="notify($event, 'error')" @notify="notify" /></div>
          <footer><span>{{ busy || status }}</span><button class="tv-button primary" type="submit">Save settings</button></footer>
        </form>
    </PopupTransition>
  </div>
</template>
