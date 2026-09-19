<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import PopupTransition from "../../../shared/PopupTransition.vue";
import type { JsonRow } from "../../types";
import { optionLabel, optionValue, voiceAction } from "./runtime";

const props = defineProps<{ payload: JsonRow; actionEndpoint: string }>();
const emit = defineEmits<{ refresh: []; notify: [message: string, tone?: string] }>();

const transport = ref<"ota" | "local_usb" | "browser_usb">("ota");
const selectedTemplate = ref("");
const selectedSelector = ref("");
const flashKind = ref<"factory" | "ota">("factory");
const ports = ref<JsonRow[]>([]);
const selectedPort = ref("");
const busy = ref("");
const error = ref("");
const session = ref<JsonRow | null>(null);
const sessionEntries = ref<JsonRow[]>([]);
const artifact = ref<JsonRow | null>(null);
const pollFailureCount = ref(0);
const manualOpen = ref(false);
const progressOpen = ref(false);
const operationTitle = ref("Firmware update");
const operationTarget = ref("");
const batchRows = ref<JsonRow[]>([]);
const batchCurrentIndex = ref(-1);
const batchCancelled = ref(false);
let pollTimer: number | null = null;

const firmware = computed<JsonRow>(() => props.payload.firmware && typeof props.payload.firmware === "object" ? props.payload.firmware as JsonRow : {});
const templates = computed<JsonRow[]>(() => Array.isArray(firmware.value.templates) ? firmware.value.templates : []);
const devices = computed<JsonRow[]>(() => Array.isArray(firmware.value.devices) ? firmware.value.devices : []);
const connectedDevices = computed(() => devices.value.filter((device) => Boolean(device.connected) && String(device.value || "") !== "__usb_recovery__"));
const updates = computed<JsonRow[]>(() => Array.isArray(firmware.value.firmware_updates) ? firmware.value.firmware_updates : []);
const flashTargets = computed<JsonRow[]>(() => Array.isArray(firmware.value.firmware_flash_targets) ? firmware.value.firmware_flash_targets : []);
const warnings = computed<string[]>(() => Array.isArray(firmware.value.warnings) ? firmware.value.warnings.map(String) : []);
const prebuiltSummary = computed<JsonRow>(() => firmware.value.prebuilt_firmware && typeof firmware.value.prebuilt_firmware === "object" ? firmware.value.prebuilt_firmware as JsonRow : {});
const variants = computed<JsonRow>(() => firmware.value.variants && typeof firmware.value.variants === "object" ? firmware.value.variants as JsonRow : {});
const usbTemplates = computed(() => templates.value.filter((template) => {
  const bySelector = variants.value[optionValue(template)];
  return Boolean(bySelector && typeof bySelector === "object" && (bySelector as JsonRow).__usb_recovery__);
}));
const targetOptions = computed(() => transport.value === "ota" ? connectedDevices.value : usbTemplates.value);
const variant = computed<JsonRow>(() => {
  const templateKey = selectedTemplate.value;
  const selector = transport.value === "ota" ? selectedSelector.value : "__usb_recovery__";
  const bySelector = variants.value[templateKey];
  return bySelector && typeof bySelector === "object" && (bySelector as JsonRow)[selector] && typeof (bySelector as JsonRow)[selector] === "object"
    ? (bySelector as JsonRow)[selector] as JsonRow
    : {};
});
const prebuilt = computed<JsonRow>(() => variant.value.prebuilt_firmware && typeof variant.value.prebuilt_firmware === "object" ? variant.value.prebuilt_firmware as JsonRow : {});
const artifacts = computed<JsonRow>(() => prebuilt.value.artifacts && typeof prebuilt.value.artifacts === "object" ? prebuilt.value.artifacts as JsonRow : {});
const factoryArtifact = computed<JsonRow>(() => artifacts.value.factory && typeof artifacts.value.factory === "object" ? artifacts.value.factory as JsonRow : {});
const otaArtifact = computed<JsonRow>(() => artifacts.value.ota && typeof artifacts.value.ota === "object" ? artifacts.value.ota as JsonRow : {});
const selectedArtifact = computed(() => flashKind.value === "ota" ? otaArtifact.value : factoryArtifact.value);
const flashTransport = computed(() => String(factoryArtifact.value.flash_transport || "esp_serial").toLowerCase());
const canOta = computed(() => Boolean(selectedSelector.value && selectedTemplate.value && otaArtifact.value.path && (variant.value.connected ?? true)));
const canUsb = computed(() => Boolean(selectedTemplate.value && selectedArtifact.value.path));
const sessionRunning = computed(() => {
  const state = String(session.value?.phase || session.value?.status || "").toLowerCase();
  if (["completed", "failed", "cancelled", "stopped"].includes(state)) return false;
  return Boolean(busy.value || session.value?.session_id);
});
const sessionProgress = computed(() => Math.max(0, Math.min(100, Number(session.value?.progress_percent ?? session.value?.progress ?? 0))));
const completedBatchCount = computed(() => batchRows.value.filter((row) => String(row.status) === "completed").length);
const progress = computed(() => {
  if (!batchRows.value.length) return sessionProgress.value;
  return Math.max(0, Math.min(100, ((completedBatchCount.value + (sessionRunning.value ? sessionProgress.value / 100 : 0)) / batchRows.value.length) * 100));
});
const progressTone = computed(() => String(session.value?.phase || session.value?.status || "starting").toLowerCase());
const progressLabel = computed(() => {
  if (batchRows.value.length) return `${completedBatchCount.value} of ${batchRows.value.length} complete`;
  return `${Math.round(sessionProgress.value)}% complete`;
});
const currentBatchRow = computed(() => batchCurrentIndex.value >= 0 ? batchRows.value[batchCurrentIndex.value] || null : null);

function templateForDevice(selector: string): string {
  const device = devices.value.find((row) => String(row.value || "") === selector);
  return String(device?.template_key || "");
}

function hydrateSelection() {
  if (transport.value === "ota") {
    const selectors = connectedDevices.value.map((row) => String(row.value || ""));
    if (!selectors.includes(selectedSelector.value)) selectedSelector.value = String(firmware.value.active_selector || selectors[0] || "");
    selectedTemplate.value = templateForDevice(selectedSelector.value) || String(firmware.value.active_template_key || "");
  } else {
    const keys = usbTemplates.value.map(optionValue);
    if (!keys.includes(selectedTemplate.value)) selectedTemplate.value = String(firmware.value.active_template_key || keys[0] || "");
    selectedSelector.value = "__usb_recovery__";
  }
}

function chooseTransport(next: "ota" | "local_usb" | "browser_usb") {
  transport.value = next;
  artifact.value = null;
  ports.value = [];
  selectedPort.value = "";
  hydrateSelection();
  if (next === "local_usb") void loadPorts(false);
}

function chooseTarget(value: string) {
  if (transport.value === "ota") {
    selectedSelector.value = value;
    selectedTemplate.value = templateForDevice(value);
  } else selectedTemplate.value = value;
  artifact.value = null;
  ports.value = [];
  selectedPort.value = "";
  if (transport.value === "local_usb") void loadPorts(false);
}

function targetLabel(selector: string, templateKey = ""): string {
  const device = devices.value.find((row) => String(row.value || row.selector || "") === selector);
  const template = templates.value.find((row) => optionValue(row) === templateKey);
  return optionLabel(device || template || { label: selector || templateKey || "Satellite" });
}

function openManualFlasher() {
  hydrateSelection();
  manualOpen.value = true;
  if (transport.value === "local_usb") void loadPorts(false);
}

function closeProgress() { progressOpen.value = false; }
function showProgress() { progressOpen.value = true; }

async function run(action: string, payload: JsonRow = {}): Promise<JsonRow> {
  return voiceAction(props.actionEndpoint, action, payload);
}

function basePayload(selector = selectedSelector.value, templateKey = selectedTemplate.value): JsonRow {
  return { id: selector, selector, template_key: templateKey };
}

function artifactUrl(value: unknown): string {
  const raw = String(value || "").trim();
  if (!raw || /^(?:https?:)?\/\//i.test(raw)) return raw;
  const marker = "/api/settings/voice/runtime/action";
  const base = props.actionEndpoint.endsWith(marker) ? props.actionEndpoint.slice(0, -marker.length) : "";
  return `${base}${raw.startsWith("/") ? raw : `/${raw}`}`;
}

function appendEntries(result: JsonRow) {
  const rows = Array.isArray(result.entries) ? result.entries as JsonRow[] : [];
  const sessionKey = String(result.session_id || session.value?.session_id || "session");
  const seen = new Set(sessionEntries.value.map((entry) => `${entry._session_id || "session"}:${entry.seq ?? `${entry.time}:${entry.message || entry.display}`}`));
  rows.forEach((entry) => {
    const key = `${sessionKey}:${entry.seq ?? `${entry.time}:${entry.message || entry.display}`}`;
    if (!seen.has(key)) { seen.add(key); sessionEntries.value.push({ ...entry, _session_id: sessionKey }); }
  });
  if (sessionEntries.value.length > 500) sessionEntries.value = sessionEntries.value.slice(-500);
}

function applySession(result: JsonRow) {
  session.value = { ...(session.value || {}), ...result };
  appendEntries(result);
  if (currentBatchRow.value) {
    currentBatchRow.value.progress = sessionProgress.value;
    currentBatchRow.value.message = result.message || result.phase || result.status || "Updating";
  }
}

function lastSessionSequence(sessionId: string): number {
  return sessionEntries.value
    .filter((entry) => String(entry._session_id || "session") === sessionId)
    .reduce((highest, entry) => Math.max(highest, Number(entry.seq || 0)), 0);
}

async function pollSession() {
  const sessionId = String(session.value?.session_id || "");
  if (!sessionId) return;
  try {
    const afterSeq = lastSessionSequence(sessionId);
    const result = await run("voice_firmware_flash_poll", { session_id: sessionId, after_seq: afterSeq });
    pollFailureCount.value = 0;
    error.value = "";
    applySession(result);
    if (sessionRunning.value) schedulePoll();
    else {
      busy.value = "";
      emit("notify", String(result.message || (String(result.phase) === "completed" ? "Firmware operation completed." : "Firmware operation stopped.")), String(result.phase) === "failed" ? "error" : "success");
      emit("refresh");
    }
  } catch (pollError) {
    pollFailureCount.value += 1;
    if (sessionRunning.value && pollFailureCount.value <= 120) {
      const restarting = Boolean(session.value?.self_ota_recovery);
      error.value = restarting
        ? "Tater Embedded is restarting. Reconnecting to verify the update…"
        : "Firmware progress is temporarily unavailable. Reconnecting…";
      schedulePoll();
      return;
    }
    error.value = pollError instanceof Error ? pollError.message : "Firmware progress could not be refreshed.";
    session.value = { ...(session.value || {}), phase: "failed", message: error.value };
    busy.value = "";
  }
}

function schedulePoll() { stopPoll(); pollTimer = window.setTimeout(pollSession, 1100); }
function stopPoll() { if (pollTimer !== null) window.clearTimeout(pollTimer); pollTimer = null; }

async function startSession(action: string, payload: JsonRow, label: string, title = "Firmware update", target = "") {
  if (busy.value) return;
  busy.value = label;
  error.value = "";
  session.value = null;
  sessionEntries.value = [];
  batchRows.value = [];
  batchCurrentIndex.value = -1;
  batchCancelled.value = false;
  operationTitle.value = title;
  operationTarget.value = target;
  manualOpen.value = false;
  progressOpen.value = true;
  pollFailureCount.value = 0;
  try {
    const result = await run(action, payload);
    applySession(result);
    emit("notify", String(result.message || "Firmware operation started."), "success");
    if (result.session_id && sessionRunning.value) schedulePoll();
    else busy.value = "";
  } catch (startError) {
    error.value = startError instanceof Error ? startError.message : "Firmware operation could not start.";
    session.value = { phase: "failed", progress_percent: 0, message: error.value };
    busy.value = "";
    emit("notify", error.value, "error");
  }
}

async function flashOta(row?: JsonRow) {
  const selector = String(row?.selector || selectedSelector.value);
  const templateKey = String(row?.template_key || selectedTemplate.value);
  await startSession("voice_firmware_flash_start", { ...basePayload(selector, templateKey), follow_logs: true }, "ota", "Installing firmware", String(row?.title || targetLabel(selector, templateKey)));
}

async function updateAll(rows = updates.value.length ? updates.value : flashTargets.value) {
  if (!rows.length || busy.value) return;
  busy.value = "batch";
  error.value = "";
  session.value = null;
  sessionEntries.value = [];
  batchCancelled.value = false;
  batchRows.value = rows.map((row, index) => ({
    key: `${row.selector || "satellite"}:${row.template_key || index}`,
    title: row.title || targetLabel(String(row.selector || ""), String(row.template_key || "")),
    subtitle: `${row.installed || "unknown"} → ${row.latest || "latest"}`,
    status: "queued",
    progress: 0,
  }));
  batchCurrentIndex.value = 0;
  operationTitle.value = rows.length === 1 ? "Installing firmware" : "Updating satellites";
  operationTarget.value = `${rows.length} satellite${rows.length === 1 ? "" : "s"}`;
  progressOpen.value = true;
  for (let index = 0; index < rows.length; index += 1) {
    if (batchCancelled.value) break;
    const row = rows[index];
    batchCurrentIndex.value = index;
    batchRows.value[index].status = "running";
    sessionEntries.value.push({ level: "info", display: `Starting ${index + 1} of ${rows.length}: ${row.title || row.selector}` });
    try {
      let result = await run("voice_firmware_flash_start", { ...basePayload(String(row.selector || ""), String(row.template_key || "")), follow_logs: true });
      applySession(result);
      while (!batchCancelled.value && result.session_id && !["completed", "failed", "cancelled", "stopped"].includes(String(result.phase || result.status || "").toLowerCase())) {
        await new Promise((resolve) => window.setTimeout(resolve, 1100));
        const afterSeq = lastSessionSequence(String(result.session_id || ""));
        result = await run("voice_firmware_flash_poll", { session_id: result.session_id, after_seq: afterSeq });
        applySession(result);
      }
      if (batchCancelled.value) {
        batchRows.value[index].status = "stopped";
        break;
      }
      if (String(result.phase || "").toLowerCase() === "failed") throw new Error(String(result.error || result.message || "Firmware update failed."));
      batchRows.value[index].status = "completed";
      batchRows.value[index].progress = 100;
    } catch (batchError) {
      batchRows.value[index].status = "failed";
      batchRows.value[index].message = batchError instanceof Error ? batchError.message : "Firmware update failed.";
      error.value = batchError instanceof Error ? batchError.message : "Firmware batch stopped.";
      emit("notify", error.value, "error");
      busy.value = "";
      return;
    }
  }
  busy.value = "";
  batchCurrentIndex.value = -1;
  if (batchCancelled.value) {
    emit("notify", "Firmware update queue stopped.", "error");
    return;
  }
  session.value = { ...(session.value || {}), phase: "completed", progress_percent: 100, message: `Updated ${rows.length} satellite${rows.length === 1 ? "" : "s"}.` };
  emit("notify", `Updated ${rows.length} satellite${rows.length === 1 ? "" : "s"}.`, "success");
  emit("refresh");
}

async function stopSession() {
  const sessionId = String(session.value?.session_id || "");
  stopPoll();
  batchCancelled.value = true;
  if (!sessionId) {
    session.value = { ...(session.value || {}), phase: "stopped", message: "Firmware operation stopped." };
    busy.value = "";
    return;
  }
  try { applySession(await run("voice_firmware_flash_stop", { session_id: sessionId })); }
  catch (stopError) { error.value = stopError instanceof Error ? stopError.message : "Firmware session could not be stopped."; }
  busy.value = "";
}

async function loadPorts(notify = true) {
  if (!selectedTemplate.value || flashTransport.value === "amlogic_usb_burn") return;
  try {
    const result = await run("voice_firmware_esp_usb_ports", basePayload("__usb_recovery__", selectedTemplate.value));
    ports.value = Array.isArray(result.ports) ? result.ports as JsonRow[] : [];
    if (!ports.value.some((row) => optionValue(row) === selectedPort.value)) selectedPort.value = optionValue(ports.value[0] || "");
    if (notify) emit("notify", String(result.message || `Found ${ports.value.length} serial port(s).`), result.available === false ? "error" : "success");
  } catch (portError) {
    error.value = portError instanceof Error ? portError.message : "USB ports could not be loaded.";
  }
}

async function localUsbFlash() {
  if (flashTransport.value === "amlogic_usb_burn") {
    await startSession("voice_firmware_amlogic_flash_start", { ...basePayload("__usb_recovery__"), flash_kind: "factory" }, "local-usb", "USB recovery", targetLabel("", selectedTemplate.value));
    return;
  }
  if (!selectedPort.value) { await loadPorts(); if (!selectedPort.value) return; }
  await startSession("voice_firmware_esp_usb_flash_start", { ...basePayload("__usb_recovery__"), serial_port: selectedPort.value, flash_kind: flashKind.value }, "local-usb", "USB firmware flash", targetLabel("", selectedTemplate.value));
}

async function localUsbLogs() {
  if (!selectedPort.value) {
    try {
      const result = await run("voice_firmware_local_usb_log_ports", { template_key: selectedTemplate.value });
      ports.value = Array.isArray(result.ports) ? result.ports as JsonRow[] : [];
      selectedPort.value = optionValue(ports.value[0] || "");
    } catch (portError) { error.value = portError instanceof Error ? portError.message : "USB log ports could not be loaded."; return; }
  }
  if (!selectedPort.value) { error.value = "Connect a USB serial device first."; return; }
  await startSession("voice_firmware_local_usb_log_start", { ...basePayload("__usb_recovery__"), serial_port: selectedPort.value }, "usb-logs", "Live USB logs", targetLabel("", selectedTemplate.value));
}

async function prepareBrowserImage() {
  if (busy.value || !canUsb.value) return;
  busy.value = "browser";
  error.value = "";
  try {
    artifact.value = await run("voice_firmware_browser_build", { ...basePayload("__usb_recovery__"), flash_kind: flashKind.value });
    emit("notify", String(artifact.value.message || "Browser USB image prepared."), "success");
  } catch (buildError) {
    error.value = buildError instanceof Error ? buildError.message : "Browser USB image could not be prepared.";
    emit("notify", error.value, "error");
  } finally { busy.value = ""; }
}

async function clean() {
  if (!window.confirm("Clean downloaded firmware files and completed firmware sessions?")) return;
  busy.value = "clean";
  try {
    const result = await run("voice_firmware_clean");
    emit("notify", String(result.message || "Firmware cache cleaned."), "success");
    emit("refresh");
  } catch (cleanError) {
    error.value = cleanError instanceof Error ? cleanError.message : "Firmware cache could not be cleaned.";
  } finally { busy.value = ""; }
}

watch(firmware, hydrateSelection, { deep: true, immediate: true });
watch(selectedSelector, (selector) => { if (transport.value === "ota") selectedTemplate.value = templateForDevice(selector); });
watch(otaArtifact, (next) => { if (!next.path) flashKind.value = "factory"; }, { deep: true, immediate: true });
onBeforeUnmount(stopPoll);
</script>

<template>
  <section class="tm-stack tvoice-firmware">
    <div v-if="error" class="tv-notice error">{{ error }}</div>
    <div v-for="warning in warnings" :key="warning" class="tv-notice warning">{{ warning }}</div>

    <article class="tm-form-card tvoice-firmware-overview">
      <header><div><span class="tv-eyebrow">Official Tater firmware</span><h3>{{ prebuiltSummary.available ? prebuiltSummary.version || "Latest release ready" : "Firmware manifest unavailable" }}</h3><p>Tater matches each connected satellite to its verified firmware family and installs updates one device at a time.</p></div><span class="tv-live-pill" :class="{ warning: !prebuiltSummary.available }"><i />{{ prebuiltSummary.available ? `${prebuiltSummary.device_count || 0} families ready` : "Unavailable" }}</span></header>
      <div class="tvoice-firmware-summary"><div><span>Connected targets</span><strong>{{ flashTargets.length }}</strong></div><div :class="{ attention: updates.length }"><span>Updates ready</span><strong>{{ updates.length }}</strong></div><div><span>Install method</span><strong>Verified OTA</strong></div></div>
      <section v-if="updates.length" class="tvoice-updates"><header><div><h4>Ready to update</h4><p>Current and available versions are shown for every satellite.</p></div><button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="updateAll(updates)">Update All ({{ updates.length }})</button></header><div class="tvoice-update-list"><div v-for="row in updates" :key="`${row.selector}:${row.template_key}`"><span class="tvoice-update-mark">↑</span><div><strong>{{ row.title || row.selector }}</strong><span><b>{{ row.installed || "unknown" }}</b><i>→</i><b>{{ row.latest || "latest" }}</b></span></div><button class="tv-button" type="button" :disabled="Boolean(busy)" @click="flashOta(row)">Update</button></div></div></section>
      <div v-else class="tm-status-card live"><div><strong>Connected satellites are current</strong><span>{{ flashTargets.length ? "No newer firmware is available for the matched devices." : "Connect a supported satellite to check its installed version." }}</span></div></div>
      <div class="tm-inline-actions tvoice-firmware-primary-actions"><button v-if="!updates.length && flashTargets.length" class="tv-button" type="button" :disabled="Boolean(busy)" @click="updateAll(flashTargets)">Reinstall All Connected ({{ flashTargets.length }})</button><button class="tv-button" type="button" :disabled="Boolean(busy)" @click="openManualFlasher">Manual install or recovery</button></div>
    </article>

    <article v-if="session || sessionEntries.length" class="tm-form-card tvoice-progress-dock" :class="`state-${progressTone}`"><div><span class="tvoice-progress-icon">{{ sessionRunning ? "↻" : progressTone === "failed" ? "!" : "✓" }}</span><div><strong>{{ operationTitle }}</strong><small>{{ session?.message || progressLabel }}<template v-if="operationTarget"> · {{ operationTarget }}</template></small></div></div><div><span>{{ Math.round(progress) }}%</span><button class="tv-button" type="button" @click="showProgress">View progress</button></div></article>

    <PopupTransition :open="manualOpen" backdrop-class="tv-modal-backdrop tset-modal" @close="manualOpen = false">
      <section class="tv-modal tvoice-flasher-modal" role="dialog" aria-modal="true" aria-labelledby="tvoice-flasher-title">
        <header><div><span class="tv-eyebrow">Install or recover</span><h2 id="tvoice-flasher-title">Manual firmware tools</h2><p>Choose a connection method, then select the exact satellite or hardware family.</p></div><button class="tv-button" type="button" @click="manualOpen = false">Close</button></header>
        <div class="tvoice-flash-methods" role="tablist" aria-label="Firmware install method"><button type="button" :class="{ active: transport === 'ota' }" @click="chooseTransport('ota')"><b>OTA</b><span>Update a connected satellite over the network.</span></button><button type="button" :class="{ active: transport === 'local_usb' }" @click="chooseTransport('local_usb')"><b>Local USB</b><span>Flash a device connected directly to this Tater host.</span></button><button type="button" :class="{ active: transport === 'browser_usb' }" @click="chooseTransport('browser_usb')"><b>Browser USB</b><span>Prepare an image for the secure web flasher.</span></button></div>

        <section class="tvoice-flash-step"><header><span>1</span><div><h3>{{ transport === "ota" ? "Choose a connected satellite" : "Choose the hardware family" }}</h3><p>Tater only shows targets compatible with this install method.</p></div></header><div class="tvoice-target-picker"><button v-for="option in targetOptions" :key="optionValue(option)" type="button" :class="{ active: (transport === 'ota' ? selectedSelector : selectedTemplate) === optionValue(option), 'no-image': !option.hero_image_src }" @click="chooseTarget(optionValue(option))"><img v-if="option.hero_image_src" :src="String(option.hero_image_src)" :alt="String(option.hero_image_alt || optionLabel(option))" /><span><b>{{ option.title || optionLabel(option) }}</b><small>{{ option.detail || option.host || option.template_key || "Official Tater firmware" }}</small></span><i /></button></div><div v-if="!targetOptions.length" class="tv-notice">{{ firmware.empty_message || "No compatible firmware target is available." }}</div></section>

        <section v-if="transport !== 'ota' && otaArtifact.path" class="tvoice-flash-step"><header><span>2</span><div><h3>Choose what to preserve</h3><p>A factory image is best for recovery; keep settings for a normal reinstall.</p></div></header><div class="tvoice-image-picker"><button type="button" :class="{ active: flashKind === 'factory' }" @click="flashKind = 'factory'"><b>Factory · erase settings</b><span>Clean recovery image and fresh setup.</span></button><button type="button" :class="{ active: flashKind === 'ota' }" @click="flashKind = 'ota'"><b>Keep settings</b><span>Install firmware without clearing provisioning.</span></button></div></section>

        <section v-if="transport === 'local_usb' && flashTransport !== 'amlogic_usb_burn'" class="tvoice-flash-step"><header><span>{{ otaArtifact.path ? "3" : "2" }}</span><div><h3>Select the USB serial port</h3><p>Connect the satellite with a data cable before refreshing ports.</p></div></header><label class="tm-field"><span class="tm-field-label">Serial port</span><select v-model="selectedPort"><option value="">Select a port</option><option v-for="port in ports" :key="optionValue(port)" :value="optionValue(port)">{{ optionLabel(port) }}</option></select></label><button class="tm-link-button" type="button" @click="loadPorts()">Refresh connected ports</button></section>

        <section v-if="Object.keys(variant).length" class="tvoice-firmware-target"><img v-if="variant.hero_image_src" :src="String(variant.hero_image_src)" :alt="String(variant.hero_image_alt || variant.title)" /><div><span class="tv-eyebrow">Ready target</span><h4>{{ variant.title || selectedSelector || selectedTemplate }}</h4><p>{{ variant.subtitle || variant.detail }}</p><dl class="tm-detail-list"><dt>Installed</dt><dd>{{ variant.installed_firmware_version || "unknown" }}</dd><dt>Available</dt><dd>{{ prebuilt.version || variant.firmware_version || "unknown" }}</dd><dt>Method</dt><dd>{{ transport === "ota" ? "Network OTA" : flashTransport === "amlogic_usb_burn" ? "Amlogic USB" : "ESP serial" }}</dd></dl></div></section>

        <div v-if="artifact" class="tm-status-card live"><div><strong>Browser image ready</strong><span>{{ artifact.message }}</span></div><a v-if="artifact.binary_url" class="tv-button" :href="artifactUrl(artifact.binary_url)" target="_blank" rel="noreferrer">Download firmware image</a></div>
        <footer class="tvoice-flasher-footer"><button class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="clean">Clean firmware cache</button><div><button v-if="transport === 'ota'" class="tv-button primary" type="button" :disabled="Boolean(busy) || !canOta" @click="flashOta()">{{ busy === "ota" ? "Starting…" : "Install Latest OTA" }}</button><template v-else-if="transport === 'local_usb'"><button class="tv-button" type="button" :disabled="Boolean(busy)" @click="localUsbLogs">Open USB Logs</button><button class="tv-button primary" type="button" :disabled="Boolean(busy) || !canUsb" @click="localUsbFlash">{{ busy === "local-usb" ? "Starting…" : "Start Local USB Flash" }}</button></template><template v-else><a class="tv-button" href="https://taterassistant.com/usb-flasher/" target="_blank" rel="noreferrer">Open Secure Web Flasher ↗</a><button class="tv-button primary" type="button" :disabled="Boolean(busy) || !canUsb" @click="prepareBrowserImage">{{ busy === "browser" ? "Preparing…" : "Prepare Browser USB Image" }}</button></template></div></footer>
      </section>
    </PopupTransition>

    <PopupTransition :open="progressOpen" backdrop-class="tv-modal-backdrop tset-modal" @close="closeProgress">
      <section class="tv-modal tvoice-progress-modal" role="dialog" aria-modal="true" aria-labelledby="tvoice-progress-title">
        <header><div><span class="tv-eyebrow">{{ sessionRunning ? "Firmware operation in progress" : progressTone === "failed" ? "Firmware operation needs attention" : "Firmware operation complete" }}</span><h2 id="tvoice-progress-title">{{ operationTitle }}</h2><p>{{ operationTarget || "Tater satellite firmware" }}</p></div><button class="tv-button" type="button" @click="closeProgress">{{ sessionRunning ? "Hide" : "Close" }}</button></header>
        <div class="tvoice-progress-hero" :class="`state-${progressTone}`"><div><span>{{ Math.round(progress) }}%</span><small>{{ progressLabel }}</small></div><div><strong>{{ currentBatchRow?.title || operationTarget || "Preparing firmware" }}</strong><p>{{ session?.message || currentBatchRow?.message || "Waiting for firmware output…" }}</p><progress :value="progress" max="100" /></div></div>
        <div v-if="batchRows.length" class="tvoice-update-queue"><header><h3>Update queue</h3><span>One satellite at a time</span></header><ol><li v-for="(row, index) in batchRows" :key="String(row.key)" :class="`state-${row.status}`"><i>{{ row.status === "completed" ? "✓" : row.status === "running" ? "↻" : row.status === "failed" ? "!" : index + 1 }}</i><span><strong>{{ row.title }}</strong><small>{{ row.message || row.subtitle || (row.status === "queued" ? "Waiting" : row.status) }}</small></span><b>{{ row.status === "running" ? `${Math.round(Number(row.progress || 0))}%` : row.status }}</b></li></ol></div>
        <details class="tvoice-firmware-session"><summary><span><b>Technical details</b><small>{{ sessionEntries.length }} log entr{{ sessionEntries.length === 1 ? "y" : "ies" }}</small></span><span>{{ session?.phase || session?.status || "starting" }}</span></summary><pre>{{ sessionEntries.map((entry) => entry.display || entry.message || entry.text).filter(Boolean).join('\n') || "Waiting for device output…" }}</pre></details>
        <footer><span>{{ sessionRunning ? "You can hide this window; the update will keep running." : session?.message || progressLabel }}</span><button v-if="sessionRunning" class="tv-button danger" type="button" :disabled="!session?.session_id" @click="stopSession">{{ session?.session_id ? "Stop" : "Starting…" }}</button><button v-else class="tv-button primary" type="button" @click="closeProgress">Done</button></footer>
      </section>
    </PopupTransition>
  </section>
</template>
