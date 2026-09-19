<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from "vue";
import PopupTransition from "../../../shared/PopupTransition.vue";
import type { JsonRow } from "../../types";
import ModelFields from "../models/ModelFields.vue";
import { fieldsToValues, itemForms, voiceAction } from "./runtime";

const props = defineProps<{ payload: JsonRow; actionEndpoint: string }>();
const emit = defineEmits<{ refresh: []; notify: [message: string, tone?: string] }>();

const drafts = reactive<Record<string, JsonRow>>({});
const settingsDrafts = reactive<Record<string, JsonRow>>({});
const displayDrafts = reactive<Record<string, JsonRow>>({});
const volumes = reactive<Record<string, number>>({});
const dirty = reactive<Record<string, boolean>>({});
const settingsDirty = reactive<Record<string, boolean>>({});
const displayDirty = reactive<Record<string, boolean>>({});
const busy = ref("");
const error = ref("");
const pairing = ref<JsonRow | null>(null);
const pairingOpen = ref(false);
const pairingDuration = ref(0);
const settingsItemId = ref("");
const infoItemId = ref("");
let pairingTimer: number | null = null;

const satellites = computed(() => itemForms(props.payload, "satellite"));
const connectedCount = computed(() => satellites.value.filter((item) => Boolean(item.connected)).length);
const settingsItem = computed(() => satellites.value.find((item) => id(item) === settingsItemId.value) || null);
const infoItem = computed(() => satellites.value.find((item) => id(item) === infoItemId.value) || null);
const settingsDisplayProfile = computed(() => settingsItem.value ? profileFor(settingsItem.value) : null);
const settingsGroups = computed<JsonRow[]>(() => settingsItem.value ? groupSettingsFields(settingsItem.value) : []);
const settingsSummaryRows = computed<JsonRow[]>(() => {
  const rows = settingsItem.value?.summary_rows;
  return Array.isArray(rows) ? rows.slice(0, 4) : [];
});
const ui = computed<JsonRow>(() => props.payload.ui && typeof props.payload.ui === "object" ? props.payload.ui as JsonRow : {});
const pairingConfig = computed<JsonRow>(() => ui.value.native_pairing && typeof ui.value.native_pairing === "object" ? ui.value.native_pairing as JsonRow : {});
const displayProfiles = computed<JsonRow[]>(() => {
  const body = props.payload.display_sensors && typeof props.payload.display_sensors === "object" ? props.payload.display_sensors as JsonRow : {};
  return Array.isArray(body.profiles) ? body.profiles : [];
});
const displayReady = computed(() => Boolean((props.payload.display_sensors as JsonRow | undefined)?.ready));
const pairingState = computed(() => String(pairing.value?.state || pairing.value?.status || "waiting").toLowerCase());
const pairingRemaining = computed(() => Math.max(0, Number(pairing.value?.expires_in_s || 0)));
const pairingProgress = computed(() => pairingDuration.value > 0 ? Math.max(0, Math.min(100, (pairingRemaining.value / pairingDuration.value) * 100)) : 0);
const pairingStatusLabel = computed(() => pairingState.value === "paired" ? "Satellite connected" : pairingState.value === "expired" ? "Code expired" : "Waiting for satellite");

function id(item: JsonRow) { return String(item.id || ""); }
function section(label: string, fields: JsonRow[]): JsonRow[] { return [{ label, fields }]; }
function isSetupMode(item: JsonRow): boolean { return String(item.run_action || "") === "voice_native_satellite_setup_mode"; }
function profileFor(item: JsonRow): JsonRow | null {
  const tokens = [item.id, item.selector, item.title, item.display_target].map((value) => String(value || "").trim().toLowerCase()).filter(Boolean);
  return displayProfiles.value.find((profile) => [profile.selector, profile.target, profile.title].map((value) => String(value || "").trim().toLowerCase()).some((value) => tokens.includes(value))) || null;
}

function groupSettingsFields(item: JsonRow): JsonRow[] {
  const groups: JsonRow[] = [];
  let current: JsonRow = {
    label: "Everyday controls",
    description: "Core behavior and local diagnostics for this satellite.",
    fields: [],
  };
  (Array.isArray(item.popup_fields) ? item.popup_fields : []).forEach((field) => {
    if (String(field.type || "").toLowerCase() === "section") {
      if ((current.fields as JsonRow[]).length) groups.push(current);
      current = {
        label: String(field.label || "Satellite settings"),
        description: String(field.description || ""),
        fields: [],
      };
      return;
    }
    (current.fields as JsonRow[]).push(field);
  });
  if ((current.fields as JsonRow[]).length) groups.push(current);
  return groups;
}

function infoSections(item: JsonRow): JsonRow[] {
  const sections: JsonRow[] = [];
  let current: JsonRow | null = null;
  (Array.isArray(item.info_fields) ? item.info_fields : []).forEach((field) => {
    if (String(field.type || "") === "section") {
      current = { title: String(field.label || "Details"), rows: [] };
      sections.push(current);
      return;
    }
    if (!current) {
      current = { title: "Details", rows: [] };
      sections.push(current);
    }
    (current.rows as JsonRow[]).push({ label: field.label, value: field.value });
  });
  return sections;
}

function formatDuration(seconds: number): string {
  const safe = Math.max(0, Math.floor(seconds));
  const minutes = Math.floor(safe / 60);
  const remainder = safe % 60;
  return `${minutes}:${String(remainder).padStart(2, "0")}`;
}

function hydrate() {
  const active = new Set<string>();
  satellites.value.forEach((item) => {
    const token = id(item);
    if (!token) return;
    active.add(token);
    if (!dirty[token]) drafts[token] = fieldsToValues(Array.isArray(item.fields) ? item.fields : []);
    if (!settingsDirty[token]) settingsDrafts[token] = fieldsToValues(Array.isArray(item.popup_fields) ? item.popup_fields : []);
    if (!dirty[`${token}:volume`]) volumes[token] = Number((item.volume_control as JsonRow | undefined)?.value ?? 80);
    const profile = profileFor(item);
    if (profile && !displayDirty[token]) displayDrafts[token] = fieldsToValues(Array.isArray(profile.fields) ? profile.fields : []);
  });
  [drafts, settingsDrafts, displayDrafts].forEach((map) => Object.keys(map).forEach((token) => { if (!active.has(token)) delete map[token]; }));
}

function update(map: Record<string, JsonRow>, flags: Record<string, boolean>, item: JsonRow, field: string, value: unknown) {
  const token = id(item);
  if (!map[token]) map[token] = {};
  map[token][field] = value;
  flags[token] = true;
  error.value = "";
}

async function act(action: string, payload: JsonRow, busyKey: string, fallback: string, refresh = true): Promise<JsonRow | null> {
  if (!action || busy.value) return null;
  busy.value = busyKey;
  error.value = "";
  try {
    const result = await voiceAction(props.actionEndpoint, action, payload);
    emit("notify", String(result.message || fallback), "success");
    if (refresh) emit("refresh");
    return result;
  } catch (actionError) {
    error.value = actionError instanceof Error ? actionError.message : fallback;
    emit("notify", error.value, "error");
    return null;
  } finally { busy.value = ""; }
}

async function saveSatellite(item: JsonRow) {
  const token = id(item);
  const result = await act(String(item.save_action || "voice_satellite_save"), { id: token, selector: token, values: { ...(drafts[token] || {}) } }, `${token}:save`, "Satellite saved.");
  if (result) dirty[token] = false;
}

async function saveSettings(item: JsonRow): Promise<boolean> {
  const token = id(item);
  const action = String(item.settings_save_action || "voice_native_satellite_settings_save");
  const result = await act(action, { id: token, selector: token, values: { ...(settingsDrafts[token] || {}) } }, `${token}:settings`, "Satellite settings saved.");
  if (result) settingsDirty[token] = false;
  return Boolean(result);
}

function openSettings(item: JsonRow) { settingsItemId.value = id(item); }
function closeSettings() { settingsItemId.value = ""; }
function openInfo(item: JsonRow) { infoItemId.value = id(item); }
function closeInfo() { infoItemId.value = ""; }
function updateActiveSettings(field: string, value: unknown) {
  const item = settingsItem.value;
  if (item) update(settingsDrafts, settingsDirty, item, field, value);
}
function updateActiveDisplay(field: string, value: unknown) {
  const item = settingsItem.value;
  if (item) update(displayDrafts, displayDirty, item, field, value);
}
async function saveAndCloseSettings(item: JsonRow) {
  if (Array.isArray(item.popup_fields) && item.popup_fields.length && !(await saveSettings(item))) return;
  const profile = profileFor(item);
  if (profile && displayReady.value && displayDirty[id(item)] && !(await saveDisplay(item, profile))) return;
  closeSettings();
}

async function saveVolume(item: JsonRow) {
  const token = id(item);
  const previous = Number((item.volume_control as JsonRow | undefined)?.value ?? 80);
  const value = Math.max(0, Math.min(100, Math.round(Number(volumes[token] || 0))));
  const action = String((item.volume_control as JsonRow | undefined)?.action || "voice_native_satellite_settings_save");
  const result = await act(action, { id: token, selector: token, values: { volume_percent: value } }, `${token}:volume`, "Satellite volume saved.", false);
  if (result) dirty[`${token}:volume`] = false;
  else volumes[token] = previous;
}

async function identify(item: JsonRow) {
  await act(String(item.identify_action || "voice_satellite_identify"), { id: id(item), selector: id(item) }, `${id(item)}:identify`, "Identify message played.", false);
}

async function run(item: JsonRow) {
  if (item.run_confirm && !window.confirm(String(item.run_confirm))) return;
  await act(String(item.run_action || ""), { id: id(item), selector: id(item) }, `${id(item)}:run`, "Satellite action completed.");
}

async function remove(item: JsonRow) {
  if (!window.confirm(String(item.remove_confirm || `Forget ${item.title || "this satellite"}?`))) return;
  await act(String(item.remove_action || "voice_satellite_remove"), { id: id(item), selector: id(item) }, `${id(item)}:remove`, "Satellite forgotten.");
}

async function saveDisplay(item: JsonRow, profile: JsonRow): Promise<boolean> {
  const token = id(item);
  const result = await act("voice_display_sensors_save", {
    target: profile.target,
    target_label: profile.target_label,
    selector: profile.selector || token,
    display_url: profile.display_url,
    slots: { ...(displayDrafts[token] || {}) },
  }, `${token}:display`, "Display settings saved.");
  if (result) displayDirty[token] = false;
  return Boolean(result);
}

async function startPairing() {
  const result = await act(String(pairingConfig.value.start_action || "voice_native_satellite_pairing_start"), {}, "pairing", "Pairing code created.", false);
  if (!result) return;
  pairing.value = result;
  pairingDuration.value = Number(result.expires_in_s || 0);
  pairingOpen.value = true;
  schedulePairing();
}


async function openPairing() {
  if (pairing.value && pairingState.value === "waiting" && pairingRemaining.value > 0) {
    pairingOpen.value = true;
    return;
  }
  await startPairing();
}

async function pollPairing() {
  const pairingId = String(pairing.value?.pairing_id || pairing.value?.id || "");
  if (!pairingId) return;
  try {
    const result = await voiceAction(props.actionEndpoint, String(pairingConfig.value.status_action || "voice_native_satellite_pairing_status"), { pairing_id: pairingId, id: pairingId });
    pairing.value = result;
    pairingDuration.value = Math.max(pairingDuration.value, Number(result.expires_in_s || 0));
    const state = String(result.state || result.status || "").toLowerCase();
    if (["paired", "connected", "complete"].includes(state) || result.paired || result.connected) {
      stopPairing();
      emit("notify", String(result.message || "Satellite paired."), "success");
      emit("refresh");
    } else if (state === "expired" || result.expired) {
      stopPairing();
    } else schedulePairing();
  } catch (pairError) {
    error.value = pairError instanceof Error ? pairError.message : "Pairing status could not be checked.";
    stopPairing();
  }
}

function schedulePairing() { stopPairing(); pairingTimer = window.setTimeout(pollPairing, 2000); }
function stopPairing() { if (pairingTimer !== null) window.clearTimeout(pairingTimer); pairingTimer = null; }

watch(() => props.payload, hydrate, { deep: true, immediate: true });
onBeforeUnmount(stopPairing);
</script>

<template>
  <section class="tm-stack tvoice-satellites">
    <div v-if="error" class="tv-notice error">{{ error }}</div>

    <article class="tm-form-card tvoice-pairing-card">
      <header><div><span class="tv-eyebrow">Tater Native</span><h3>Add a satellite</h3><p>Pair official Tater hardware with a secure, short-lived setup code.</p></div><button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="openPairing">{{ busy === "pairing" ? "Creating…" : pairingState === "waiting" && pairingRemaining > 0 ? "View pairing code" : "Add Satellite" }}</button></header>
      <div class="tvoice-pairing-steps" aria-label="Satellite pairing steps"><span><b>1</b>Start pairing</span><span><b>2</b>Enter the code on the satellite</span><span><b>3</b>Tater connects it automatically</span></div>
    </article>

    <div v-if="satellites.length" class="tvoice-section-heading tvoice-device-list-heading"><div><span class="tv-eyebrow">Your satellite network</span><h3>{{ connectedCount }} connected · {{ satellites.length }} known</h3><p>Core controls stay on each card; deeper device and display settings open in a focused popup.</p></div><span class="tvoice-network-state"><i />Live</span></div>

    <article v-for="item in satellites" :key="id(item)" class="tm-form-card tvoice-satellite-card">
      <header class="tvoice-device-head">
        <div class="tvoice-device-identity"><img v-if="item.hero_image_src" :src="String(item.hero_image_src)" :alt="String(item.hero_image_alt || item.title)" /><div><h3>{{ item.title || id(item) }}</h3><p>{{ item.subtitle }}</p><div v-if="Array.isArray(item.hero_badges)" class="tvoice-badges"><span v-for="badge in item.hero_badges" :key="String(badge.label)" :class="`tone-${badge.tone || 'muted'}`">{{ badge.label }}</span></div></div></div>
        <span class="tv-live-pill" :class="{ warning: !item.connected }"><i />{{ item.connected ? "Connected" : "Offline" }}</span>
      </header>

      <dl v-if="Array.isArray(item.summary_rows) && item.summary_rows.length" class="tm-detail-list tvoice-summary-list"><template v-for="row in item.summary_rows" :key="String(row.label)"><dt>{{ row.label }}</dt><dd>{{ row.value || "—" }}</dd></template></dl>
      <section v-for="detail in item.detail_sections || []" :key="String(detail.title)" class="tvoice-device-overview"><header><span>{{ detail.title }}</span><button v-if="Array.isArray(item.info_fields) && item.info_fields.length" class="tm-link-button" type="button" @click="openInfo(item)">View all device info</button></header><dl><div v-for="row in detail.rows || []" :key="String(row.label)"><dt>{{ row.label }}</dt><dd>{{ row.value || "—" }}</dd></div></dl></section>

      <label v-if="item.volume_control && Object.keys(item.volume_control).length" class="tvoice-volume"><span><strong>Volume</strong><output>{{ volumes[id(item)] ?? item.volume_control.value }}%</output></span><input v-model.number="volumes[id(item)]" type="range" :min="item.volume_control.min ?? 0" :max="item.volume_control.max ?? 100" :step="item.volume_control.step ?? 1" :disabled="Boolean(busy)" @input="dirty[`${id(item)}:volume`] = true" @change="saveVolume(item)" /></label>

      <ModelFields v-if="Array.isArray(item.fields) && item.fields.length" :sections="section('Room and playback', item.fields)" :values="drafts[id(item)] || {}" :disabled="Boolean(busy)" @change="(field, value) => update(drafts, dirty, item, field, value)" />

      <div v-if="Array.isArray(item.sensor_rows) && item.sensor_rows.length" class="tvoice-sensors"><h4>{{ item.sensor_title || "Live entities" }}</h4><div class="tm-metrics"><article v-for="sensor in item.sensor_rows" :key="String(sensor.key)"><span>{{ sensor.label }}</span><strong>{{ sensor.value || "—" }}</strong></article></div></div>

      <div class="tm-inline-actions tvoice-item-actions">
        <button v-if="item.save_action" class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="saveSatellite(item)">{{ busy === `${id(item)}:save` ? "Saving…" : item.save_label || "Save" }}</button>
        <button v-if="(Array.isArray(item.popup_fields) && item.popup_fields.length) || profileFor(item)" class="tv-button tvoice-settings-trigger" type="button" :disabled="Boolean(busy)" @click="openSettings(item)">{{ item.settings_label || "Satellite Settings" }}<span v-if="settingsDirty[id(item)] || displayDirty[id(item)]" class="tvoice-unsaved-dot" aria-label="Unsaved changes" /></button>
        <button v-if="item.identify_action" class="tv-button" type="button" :disabled="Boolean(busy) || !item.connected" @click="identify(item)">{{ item.identify_label || "Identify" }}</button>
        <button v-if="item.run_action" class="tv-button" :class="{ danger: isSetupMode(item), 'tvoice-setup-mode': isSetupMode(item) }" type="button" :disabled="Boolean(busy)" :title="isSetupMode(item) ? 'Unpairs this satellite and restarts it in setup mode' : ''" @click="run(item)">{{ busy === `${id(item)}:run` ? "Working…" : item.run_label || "Run" }}</button>
        <button v-if="item.remove_action" class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="remove(item)">{{ item.remove_label || "Forget" }}</button>
        <span v-if="dirty[id(item)]" class="tm-unsaved-label">Unsaved changes</span>
      </div>
    </article>

    <div v-if="!satellites.length" class="tv-notice">{{ payload.empty_message || "No native satellites are connected yet." }}</div>

    <PopupTransition :open="Boolean(settingsItem)" backdrop-class="tv-modal-backdrop tset-modal" @close="closeSettings">
      <section v-if="settingsItem" class="tv-modal tvoice-settings-modal" role="dialog" aria-modal="true" aria-labelledby="tvoice-settings-title">
        <header class="tvoice-settings-modal-head">
          <div class="tvoice-settings-device">
            <img v-if="settingsItem.hero_image_src" :src="String(settingsItem.hero_image_src)" :alt="String(settingsItem.hero_image_alt || settingsItem.title)" />
            <div><span class="tv-eyebrow">Tater satellite controls</span><h2 id="tvoice-settings-title">{{ settingsItem.title || settingsItem.settings_title || "Satellite Settings" }}</h2><p>Fine-tune device behavior, lighting, and display sensors in one place.</p></div>
          </div>
          <button class="tv-button tvoice-settings-close" type="button" @click="closeSettings">Close</button>
        </header>
        <div class="tvoice-settings-overview">
          <span class="tv-live-pill" :class="{ warning: !settingsItem.connected }"><i />{{ settingsItem.connected ? "Connected" : "Offline" }}</span>
          <dl v-if="settingsSummaryRows.length"><div v-for="row in settingsSummaryRows" :key="String(row.label)"><dt>{{ row.label }}</dt><dd>{{ row.value || "—" }}</dd></div></dl>
        </div>
        <div class="tvoice-settings-sections">
          <section v-if="settingsGroups.length" class="tvoice-settings-block"><header><span class="tvoice-section-mark">SAT</span><div><h3>Satellite behavior</h3><p>Each group applies only to this device. LED previews update as you choose an animation.</p></div></header><ModelFields class="tvoice-settings-groups" :sections="settingsGroups" :values="settingsDrafts[id(settingsItem)] || {}" :disabled="Boolean(busy)" @change="updateActiveSettings" /></section>
          <section v-if="settingsDisplayProfile"><header><span class="tvoice-section-mark">DSP</span><div><h3>Display sensors</h3><p>{{ settingsDisplayProfile.detail || "Choose which Environment Core readings this display shows." }}</p></div></header><ModelFields :sections="section('Sensor slots', Array.isArray(settingsDisplayProfile.fields) ? settingsDisplayProfile.fields : [])" :values="displayDrafts[id(settingsItem)] || {}" :disabled="Boolean(busy) || !displayReady" @change="updateActiveDisplay" /><div v-if="!displayReady" class="tv-notice warning">{{ (payload.display_sensors as JsonRow | undefined)?.message || "Environment Core readings are not available yet." }}</div></section>
        </div>
        <footer class="tvoice-settings-footer"><span class="tvoice-settings-sync" :class="{ dirty: settingsDirty[id(settingsItem)] || displayDirty[id(settingsItem)] }"><i />{{ settingsDirty[id(settingsItem)] || displayDirty[id(settingsItem)] ? "Unsaved changes" : "Satellite settings are synchronized" }}</span><button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="saveAndCloseSettings(settingsItem)">{{ busy ? "Saving…" : "Save Satellite Settings" }}</button></footer>
      </section>
    </PopupTransition>

    <PopupTransition :open="pairingOpen" backdrop-class="tv-modal-backdrop tset-modal" @close="pairingOpen = false">
      <section class="tv-modal tvoice-pairing-modal" role="dialog" aria-modal="true" aria-labelledby="tvoice-pairing-title">
        <header><div><span class="tv-eyebrow">Secure satellite pairing</span><h2 id="tvoice-pairing-title">Connect a Tater satellite</h2><p>The code works once and expires automatically.</p></div><button class="tv-button" type="button" @click="pairingOpen = false">Close</button></header>
        <div class="tvoice-pairing-code" :class="`state-${pairingState}`"><span>{{ pairingStatusLabel }}</span><strong>{{ pairing?.pairing_code || pairing?.display_code || pairing?.code || "—" }}</strong><small v-if="pairingState === 'waiting'">Expires in {{ formatDuration(pairingRemaining) }}</small><small v-else-if="pairingState === 'paired'">The satellite is paired and ready to appear in this list.</small><small v-else>Create a fresh code to continue.</small><progress v-if="pairingState === 'waiting'" :value="pairingProgress" max="100" /></div>
        <ol class="tvoice-pairing-guide"><li><b>Open setup on the satellite.</b><span>Connect the satellite to power and continue until it asks for a Tater pairing code.</span></li><li><b>Enter the six-digit code.</b><span>Keep this window open while the satellite connects.</span></li><li><b>Wait for confirmation.</b><span>This dialog updates automatically when Tater recognizes the device.</span></li></ol>
        <a v-if="pairing?.pairing_url || pairing?.url" class="tv-button" :href="String(pairing.pairing_url || pairing.url)" target="_blank" rel="noreferrer">Open setup page ↗</a>
        <footer><span>{{ pairingState === "waiting" ? "Checking for your satellite…" : pairingStatusLabel }}</span><button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="startPairing">{{ busy === "pairing" ? "Creating…" : "Create new code" }}</button></footer>
      </section>
    </PopupTransition>

    <PopupTransition :open="Boolean(infoItem)" backdrop-class="tv-modal-backdrop tset-modal" @close="closeInfo">
      <section v-if="infoItem" class="tv-modal tvoice-info-modal" role="dialog" aria-modal="true" aria-labelledby="tvoice-info-title">
        <header><div><span class="tv-eyebrow">{{ infoItem.title || "Satellite" }}</span><h2 id="tvoice-info-title">{{ infoItem.info_title || "Device information" }}</h2><p>Live identity, connection, diagnostics, and firmware details.</p></div><button class="tv-button" type="button" @click="closeInfo">Close</button></header>
        <div class="tvoice-info-sections"><section v-for="sectionItem in infoSections(infoItem)" :key="String(sectionItem.title)"><h3>{{ sectionItem.title }}</h3><dl><div v-for="row in sectionItem.rows || []" :key="String(row.label)"><dt>{{ row.label }}</dt><dd>{{ row.value || "—" }}</dd></div></dl></section></div>
      </section>
    </PopupTransition>

  </section>
</template>
