<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from "vue";
import PopupTransition from "../../../shared/PopupTransition.vue";
import UiIcon from "../../../shared/UiIcon.vue";
import FocusedWorkspace from "../FocusedWorkspace.vue";
import type { JsonRow } from "../../types";
import ModelFields from "../models/ModelFields.vue";
import { fieldsToValues, itemForms, voiceAction } from "./runtime";
import { satelliteSettingsChange, satelliteSettingsTab, satelliteWorkspaceTabs } from "./satelliteWorkspace";
import type { SatelliteWorkspaceTab } from "./satelliteWorkspace";

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
const settingsTab = ref<SatelliteWorkspaceTab>("room");
let pairingTimer: number | null = null;

const satellites = computed(() => itemForms(props.payload, "satellite"));
const connectedCount = computed(() => satellites.value.filter((item) => Boolean(item.connected)).length);
const settingsItem = computed(() => satellites.value.find((item) => id(item) === settingsItemId.value) || null);
const settingsDisplayProfile = computed(() => settingsItem.value ? profileFor(settingsItem.value) : null);
const settingsGroups = computed<JsonRow[]>(() => settingsItem.value ? groupSettingsFields(settingsItem.value) : []);
const activeSettingsGroups = computed(() => settingsGroups.value.filter(group => satelliteSettingsTab(group) === settingsTab.value));
const activeDeviceDirty = computed(() => settingsItem.value ? deviceDirty(settingsItem.value) : false);
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
function displayPhotos(item: JsonRow): JsonRow[] { return Array.isArray(item.display_photos) ? item.display_photos as JsonRow[] : []; }
function section(label: string, fields: JsonRow[]): JsonRow[] { return [{ label, fields }]; }
function hasSafeRunAction(item: JsonRow): boolean {
  const action = String(item.run_action || "");
  return Boolean(action) && action !== "voice_native_satellite_setup_mode";
}
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
        key: String(field.key || ""),
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
  return Boolean(result);
}

async function saveSettings(item: JsonRow): Promise<boolean> {
  const token = id(item);
  const action = String(item.settings_save_action || "voice_native_satellite_settings_save");
  const result = await act(action, { id: token, selector: token, values: { ...(settingsDrafts[token] || {}) } }, `${token}:settings`, "Satellite settings saved.");
  if (result) settingsDirty[token] = false;
  return Boolean(result);
}

function openSettings(item: JsonRow) { settingsTab.value = "room"; settingsItemId.value = id(item); }
function closeSettings() { settingsItemId.value = ""; }
function deviceDirty(item: JsonRow) { const token = id(item); return Boolean(dirty[token] || settingsDirty[token] || displayDirty[token]); }
function updateActiveSettings(field: string, value: unknown) {
  const item = settingsItem.value;
  if (!item) return;
  const patch = satelliteSettingsChange(Array.isArray(item.popup_fields) ? item.popup_fields : [], field, value);
  Object.entries(patch).forEach(([key, next]) => update(settingsDrafts, settingsDirty, item, key, next));
}
function updateActiveDisplay(field: string, value: unknown) {
  const item = settingsItem.value;
  if (item) update(displayDrafts, displayDirty, item, field, value);
}
async function saveDeviceSettings(item: JsonRow) {
  const token = id(item);
  if (dirty[token] && !(await saveSatellite(item))) return;
  if (settingsDirty[token] && !(await saveSettings(item))) return;
  const profile = profileFor(item);
  if (profile && displayDirty[token]) {
    if (!displayReady.value) { error.value = "Display settings are unavailable. Your display changes are still here; reconnect the integration before saving."; return; }
    await saveDisplay(item, profile);
  }
}

function fileBase64(file: File): Promise<string> {
  return file.arrayBuffer().then((buffer) => {
    const bytes = new Uint8Array(buffer);
    let binary = "";
    const chunkSize = 0x8000;
    for (let offset = 0; offset < bytes.length; offset += chunkSize) {
      binary += String.fromCharCode(...bytes.subarray(offset, Math.min(bytes.length, offset + chunkSize)));
    }
    return window.btoa(binary);
  });
}

async function uploadDisplayPhotos(event: Event) {
  const input = event.target as HTMLInputElement;
  const item = settingsItem.value;
  const files = Array.from(input.files || []);
  input.value = "";
  if (!item || !files.length || busy.value) return;
  const remaining = Math.max(0, Number(item.display_photo_limit || 12) - displayPhotos(item).length);
  if (remaining <= 0) {
    error.value = `This satellite already has ${item.display_photo_limit || 12} display photos.`;
    return;
  }
  let uploaded = 0;
  for (const file of files.slice(0, remaining)) {
    const dataBase64 = await fileBase64(file);
    const result = await act(
      String(item.display_photo_upload_action || "voice_native_display_photo_upload"),
      { id: id(item), selector: id(item), filename: file.name, content_type: file.type, data_base64: dataBase64 },
      `${id(item)}:photo-upload`,
      "Display photo uploaded.",
      false,
    );
    if (!result) break;
    uploaded += 1;
  }
  if (uploaded) emit("refresh");
}

async function removeDisplayPhoto(item: JsonRow, photo: JsonRow) {
  await act(
    String(item.display_photo_remove_action || "voice_native_display_photo_remove"),
    { id: id(item), selector: id(item), photo_id: String(photo.id || "") },
    `${id(item)}:photo-remove`,
    "Display photo removed.",
  );
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
    profile_kind: profile.profile_kind,
    screen_target: profile.screen_target,
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
defineExpose({ hasUnsavedChanges: () => satellites.value.some(deviceDirty) });
</script>

<template>
  <section class="tm-stack tvoice-satellites">
    <div v-if="error && !settingsItem" class="tv-notice error" role="alert">{{ error }}</div>

    <article class="tm-form-card tvoice-pairing-card">
      <header><div><span class="tv-eyebrow">Tater Native</span><h3>Add a satellite</h3><p>Pair official Tater hardware with a secure, short-lived setup code.</p></div><button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="openPairing">{{ busy === "pairing" ? "Creating…" : pairingState === "waiting" && pairingRemaining > 0 ? "View pairing code" : "Add Satellite" }}</button></header>
      <div class="tvoice-pairing-steps" aria-label="Satellite pairing steps"><span><b>1</b>Start pairing</span><span><b>2</b>Enter the code on the satellite</span><span><b>3</b>Tater connects it automatically</span></div>
    </article>

    <div v-if="satellites.length" class="tvoice-section-heading tvoice-device-list-heading"><div><h3>Satellite devices</h3><p>{{ connectedCount }} connected · {{ satellites.length }} known. Open Device settings for room, playback, and device controls.</p></div><span class="tvoice-network-state"><i />Live</span></div>

    <div class="tvoice-device-roster">
      <article v-for="item in satellites" :key="id(item)" class="tm-form-card tvoice-satellite-card">
        <header class="tvoice-device-head">
          <div class="tvoice-device-identity"><img v-if="item.hero_image_src" :src="String(item.hero_image_src)" :alt="String(item.hero_image_alt || item.title)" /><div><h3>{{ item.title || id(item) }}</h3><p>{{ item.subtitle }}</p><div v-if="Array.isArray(item.hero_badges)" class="tvoice-badges"><span v-for="badge in item.hero_badges" :key="String(badge.label)" :class="`tone-${badge.tone || 'muted'}`">{{ badge.label }}</span></div><span v-if="deviceDirty(item)" class="tm-unsaved-label">Unsaved changes</span></div></div>
          <div class="tvoice-device-actions">
        <span
          class="tv-live-pill tvoice-connection-pill"
          :class="{ offline: !item.connected, degraded: item.connected && item.connection_degraded }"
          :title="String(item.connection_note || (item.connected ? 'Connected by Wi-Fi.' : 'Satellite is offline.'))"
          :aria-label="String(item.connection_note || (item.connected ? 'Connected by Wi-Fi.' : 'Satellite is offline.'))"
        >
          <i />
          <svg v-if="item.connected && item.connection_transport === 'ethernet'" class="tvoice-connection-icon" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M6 8V5a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v3M3 8h18v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8Zm5 4v3m4-3v3m4-3v3" /></svg>
          <svg v-else-if="item.connected" class="tvoice-connection-icon" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M4.5 10.5a11 11 0 0 1 15 0M7.5 13.5a6.7 6.7 0 0 1 9 0M10.5 16.5a2.4 2.4 0 0 1 3 0" /><circle cx="12" cy="19" r="1" fill="currentColor" stroke="none" /></svg>
          {{ item.connected ? (item.connection_degraded ? "Wi-Fi rescue" : "Connected") : "Offline" }}
        </span>
            <button class="tv-button" type="button" :aria-label="`Device settings for ${item.title || id(item)}`" @click="openSettings(item)">Device settings<UiIcon name="chevron" /></button>
          </div>
        </header>
      </article>
    </div>
    <div v-if="!satellites.length" class="tv-notice">{{ payload.empty_message || "No native satellites are connected yet." }}</div>

    <FocusedWorkspace :open="Boolean(settingsItem)" :title="String(settingsItem?.title || settingsItem?.settings_title || 'Satellite settings')" eyebrow="Satellites · Device settings" :description="String(settingsItem?.subtitle || 'Room, playback, voice, display, and device information.')" back-label="All satellites" @close="closeSettings">
      <template #status><span v-if="settingsItem" class="tv-live-pill" :class="{ offline: !settingsItem.connected, degraded: settingsItem.connection_degraded }"><i />{{ settingsItem.connected ? (settingsItem.connection_degraded ? 'Wi-Fi rescue' : 'Connected') : 'Offline' }}</span></template>
      <div v-if="settingsItem" class="tvoice-device-workspace">
        <div v-if="error" class="tv-notice error" role="alert">{{ error }}</div>
        <div v-if="settingsSummaryRows.length" class="tvoice-settings-overview"><dl><div v-for="row in settingsSummaryRows" :key="String(row.label)"><dt>{{ row.label }}</dt><dd>{{ row.value || '—' }}</dd></div></dl></div>
        <nav class="tset-subsection-nav tvoice-workspace-tabs" aria-label="Satellite settings sections"><button v-for="tab in satelliteWorkspaceTabs" :key="tab.id" type="button" :aria-current="settingsTab === tab.id ? 'page' : undefined" @click="settingsTab = tab.id">{{ tab.label }}</button></nav>

        <section v-if="settingsTab === 'room'" class="tm-form-card tvoice-room-controls">
          <header><h2>Room and playback</h2><p>Choose this satellite’s room and playback level.</p></header>
          <label v-if="settingsItem.volume_control && Object.keys(settingsItem.volume_control).length" class="tvoice-volume"><span><strong>Volume</strong><output>{{ volumes[id(settingsItem)] ?? settingsItem.volume_control.value }}%</output></span><input v-model.number="volumes[id(settingsItem)]" type="range" :min="settingsItem.volume_control.min ?? 0" :max="settingsItem.volume_control.max ?? 100" :step="settingsItem.volume_control.step ?? 1" :disabled="Boolean(busy)" @input="dirty[`${id(settingsItem)}:volume`] = true" @change="saveVolume(settingsItem)" /><small>Volume applies immediately.</small></label>
          <ModelFields v-if="Array.isArray(settingsItem.fields) && settingsItem.fields.length" :sections="section('Room settings', settingsItem.fields)" :values="drafts[id(settingsItem)] || {}" :disabled="Boolean(busy)" @change="(field, value) => update(drafts, dirty, settingsItem!, field, value)" />
          <div v-if="settingsItem.identify_action" class="tvoice-workspace-identify"><div><h3>Identify this satellite</h3><p>Locate the device in your room.</p></div><button class="tv-button" type="button" :disabled="Boolean(busy) || !settingsItem.connected" @click="identify(settingsItem)">{{ settingsItem.identify_label || 'Identify' }}</button></div>
        </section>

        <ModelFields v-if="activeSettingsGroups.length" class="tvoice-workspace-fields" :sections="activeSettingsGroups" :values="settingsDrafts[id(settingsItem)] || {}" :disabled="Boolean(busy)" @change="updateActiveSettings" />
        <p v-if="settingsTab === 'voice' && !activeSettingsGroups.length" class="tv-notice">This device has no additional voice or lighting controls.</p>

        <div v-if="settingsTab === 'display'" class="tvoice-workspace-display">
          <section v-if="settingsItem.display_photos_supported" class="tvoice-settings-block tvoice-display-photos"><header><span class="tvoice-section-mark">PIC</span><div><h3>Home screen photos</h3><p>With no uploads, Tater rotates seven built-in landscapes using the Photo Rotation setting above. After you add a photo, this satellite rotates only your photos; a single photo stays fixed.</p></div></header><div v-if="displayPhotos(settingsItem).length" class="tvoice-display-photo-grid"><article v-for="photo in displayPhotos(settingsItem)" :key="String(photo.id)"><img :src="String(photo.url)" alt="Uploaded satellite background" /><button type="button" :disabled="Boolean(busy)" aria-label="Remove display photo" @click="removeDisplayPhoto(settingsItem, photo)">Remove</button></article></div><div v-else class="tvoice-display-photo-default"><strong>Seven Tater scenery photos are active</strong><span>Mountains, forest, ocean, desert, meadow, waterfall, and night sky follow this satellite's rotation schedule.</span></div><label class="tv-button tvoice-display-photo-upload" :class="{ disabled: Boolean(busy) || displayPhotos(settingsItem).length >= Number(settingsItem.display_photo_limit || 12) }"><input type="file" multiple accept="image/jpeg,image/png,image/webp" :disabled="Boolean(busy) || displayPhotos(settingsItem).length >= Number(settingsItem.display_photo_limit || 12)" @change="uploadDisplayPhotos" />{{ busy === `${id(settingsItem)}:photo-upload` ? "Uploading…" : "Add photos" }}</label><small>{{ displayPhotos(settingsItem).length }} / {{ settingsItem.display_photo_limit || 12 }} custom photos</small></section>
          <section v-if="settingsDisplayProfile"><header><span class="tvoice-section-mark">DSP</span><div><h3>Weather &amp; climate</h3><p>{{ settingsDisplayProfile.detail || "Choose this satellite's thermostat, weather readings, and room-temperature sensors." }}</p></div></header><ModelFields :sections="section('Display settings', Array.isArray(settingsDisplayProfile.fields) ? settingsDisplayProfile.fields : [])" :values="displayDrafts[id(settingsItem)] || {}" :disabled="Boolean(busy) || !displayReady" @change="updateActiveDisplay" /><div v-if="!displayReady" class="tv-notice warning">{{ (payload.display_sensors as JsonRow | undefined)?.message || "No compatible display integrations or sensors are available yet." }}</div></section>
          <p v-if="!activeSettingsGroups.length && !settingsItem.display_photos_supported && !settingsDisplayProfile" class="tv-notice">Display controls are not available for this satellite.</p>
        </div>

        <div v-if="settingsTab === 'info'" class="tvoice-workspace-info">
          <section class="tm-form-card"><header><h2>Device information</h2><p>Live identity, connection, diagnostics, and firmware details.</p></header>
            <dl v-if="Array.isArray(settingsItem.summary_rows)" class="tm-detail-list"><template v-for="row in settingsItem.summary_rows" :key="String(row.label)"><dt>{{ row.label }}</dt><dd>{{ row.value || '—' }}</dd></template></dl>
            <section v-for="detail in settingsItem.detail_sections || []" :key="String(detail.title)" class="tvoice-device-overview"><header><span>{{ detail.title }}</span></header><dl><div v-for="row in detail.rows || []" :key="String(row.label)"><dt>{{ row.label }}</dt><dd>{{ row.value || '—' }}</dd></div></dl></section>
            <div class="tvoice-info-sections"><section v-for="info in infoSections(settingsItem)" :key="String(info.title)"><h3>{{ info.title }}</h3><dl><div v-for="row in info.rows || []" :key="String(row.label)"><dt>{{ row.label }}</dt><dd>{{ row.value || '—' }}</dd></div></dl></section></div>
          </section>
          <div v-if="Array.isArray(settingsItem.sensor_rows) && settingsItem.sensor_rows.length" class="tvoice-sensors"><h3>{{ settingsItem.sensor_title || 'Live entities' }}</h3><div class="tm-metrics"><article v-for="sensor in settingsItem.sensor_rows" :key="String(sensor.key)"><span>{{ sensor.label }}</span><strong>{{ sensor.value || '—' }}</strong></article></div></div>
          <div class="tm-inline-actions tvoice-item-actions">
            <button v-if="hasSafeRunAction(settingsItem)" class="tv-button" type="button" :disabled="Boolean(busy)" @click="run(settingsItem)">{{ busy === `${id(settingsItem)}:run` ? 'Working…' : settingsItem.run_label || 'Run' }}</button>
            <button v-if="settingsItem.remove_action" class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="remove(settingsItem)">{{ settingsItem.remove_label || 'Forget' }}</button>
          </div>
        </div>
      </div>
      <template #footer><span class="tvoice-settings-sync" :class="{ dirty: activeDeviceDirty }"><i />{{ activeDeviceDirty ? 'Unsaved changes' : 'Satellite settings are synchronized' }}</span><button class="tv-button primary" type="button" :disabled="Boolean(busy) || !activeDeviceDirty" @click="settingsItem && saveDeviceSettings(settingsItem)">{{ busy ? 'Saving…' : 'Save device settings' }}</button></template>
    </FocusedWorkspace>

    <PopupTransition :open="pairingOpen" backdrop-class="tv-modal-backdrop tset-modal" @close="pairingOpen = false">
      <section class="tv-modal tvoice-pairing-modal" role="dialog" aria-modal="true" aria-labelledby="tvoice-pairing-title">
        <header><div><span class="tv-eyebrow">Secure satellite pairing</span><h2 id="tvoice-pairing-title">Connect a Tater satellite</h2><p>The code works once and expires automatically.</p></div><button class="tv-button" type="button" @click="pairingOpen = false">Close</button></header>
        <div class="tvoice-pairing-code" :class="`state-${pairingState}`"><span>{{ pairingStatusLabel }}</span><strong>{{ pairing?.pairing_code || pairing?.display_code || pairing?.code || "—" }}</strong><small v-if="pairingState === 'waiting'">Expires in {{ formatDuration(pairingRemaining) }}</small><small v-else-if="pairingState === 'paired'">The satellite is paired and ready to appear in this list.</small><small v-else>Create a fresh code to continue.</small><progress v-if="pairingState === 'waiting'" :value="pairingProgress" max="100" /></div>
        <ol class="tvoice-pairing-guide"><li><b>Open setup on the satellite.</b><span>Connect the satellite to power and continue until it asks for a Tater pairing code.</span></li><li><b>Enter the six-digit code.</b><span>Keep this window open while the satellite connects.</span></li><li><b>Wait for confirmation.</b><span>This dialog updates automatically when Tater recognizes the device.</span></li></ol>
        <a v-if="pairing?.pairing_url || pairing?.url" class="tv-button" :href="String(pairing.pairing_url || pairing.url)" target="_blank" rel="noreferrer">Open setup page ↗</a>
        <footer><span>{{ pairingState === "waiting" ? "Checking for your satellite…" : pairingStatusLabel }}</span><button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="startPairing">{{ busy === "pairing" ? "Creating…" : "Create new code" }}</button></footer>
      </section>
    </PopupTransition>
  </section>
</template>
