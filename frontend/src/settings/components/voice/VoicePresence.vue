<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { getJson, postJson } from "../../../shared/api";
import PopupTransition from "../../../shared/PopupTransition.vue";
import type { JsonRow } from "../../types";

const props = defineProps<{
  snapshotEndpoint: string;
  eventsEndpoint: string;
}>();

const emit = defineEmits<{ notify: [message: string, tone?: string] }>();

type PresenceView = "locations" | "devices" | "history" | "calibration";
type DeviceEditor = {
  id: string;
  configured: boolean;
  address: string;
  name: string;
  owner: string;
  category: string;
  track: boolean;
  referencePower: string;
  maxRadius: string;
  homeTimeout: string;
  ibeaconId: string;
  irk: string;
  identities: JsonRow[];
};

const ROOM_PREVIEW_LIMIT = 6;
const snapshot = ref<JsonRow>({ devices: [], sources: [], rooms: {}, history: [], settings: {}, diagnostics: {} });
const configuration = ref<JsonRow>({ settings: {}, devices: [], scanners: {}, capabilities: {} });
const loading = ref(true);
const saving = ref(false);
const error = ref("");
const streamState = ref<"connecting" | "live" | "reconnecting" | "unsupported">("connecting");
const activeView = ref<PresenceView>("locations");
const search = ref("");
const registrySearch = ref("");
const roomFilter = ref("all");
const selectedId = ref("");
const roomPopup = ref("");
const editorOpen = ref(false);
const nowTs = ref(Date.now() / 1000);
const updatedAt = ref(0);
const editor = ref<DeviceEditor>(emptyEditor());
const settingsDraft = ref<JsonRow>({});
const scannerDrafts = ref<Record<string, string>>({});
let eventSource: EventSource | null = null;
let fallbackTimer = 0;
let clockTimer = 0;

const configEndpoint = computed(() => `${props.snapshotEndpoint.replace(/\/$/, "")}/config`);
const devices = computed<JsonRow[]>(() => Array.isArray(snapshot.value.devices) ? snapshot.value.devices : []);
const sources = computed<JsonRow[]>(() => Array.isArray(snapshot.value.sources) ? snapshot.value.sources : []);
const history = computed<JsonRow[]>(() => Array.isArray(snapshot.value.history) ? snapshot.value.history : []);
const trackedDevices = computed(() => devices.value.filter((device) => Boolean(device.tracked)));
const registeredDevices = computed(() => devices.value
  .filter((device) => Boolean(device.configured))
  .slice()
  .sort(compareRegistryDevices));
const availableDevices = computed(() => {
  const query = registrySearch.value.trim().toLowerCase();
  return devices.value
    .filter((device) => !device.configured)
    .filter((device) => !query || [deviceName(device), device.address, device.advertised_name]
      .some((value) => String(value || "").toLowerCase().includes(query)))
    .slice()
    .sort(compareRegistryDevices);
});
const homeCount = computed(() => trackedDevices.value.filter((device) => device.home_state === "home").length);
const awayCount = computed(() => trackedDevices.value.filter((device) => device.home_state === "away").length);
const onlineSources = computed(() => sources.value.filter((source) => ageSeconds(source.last_seen_ts) <= 15).length);
const activeCount = computed(() => devices.value.filter((device) => Boolean(device.present)).length);
const roomOptions = computed(() => {
  const values = new Set(
    devices.value
      .filter((device) => device.home_state !== "away")
      .map((device) => roomName(device))
      .filter((room) => room && room !== "Away"),
  );
  return [...values].sort((left, right) => left === "Unknown" ? 1 : right === "Unknown" ? -1 : left.localeCompare(right));
});
const filteredDevices = computed(() => {
  const query = search.value.trim().toLowerCase();
  return devices.value.filter((device) => {
    const room = roomName(device);
    if (roomFilter.value !== "all" && room !== roomFilter.value) return false;
    if (!query) return true;
    return [device.display_name, device.advertised_name, device.address, device.owner, device.category, room]
      .some((value) => String(value || "").toLowerCase().includes(query));
  });
});
const roomGroups = computed(() => roomOptions.value
  .filter((room) => roomFilter.value === "all" || room === roomFilter.value)
  .map((room) => ({
    room,
    devices: filteredDevices.value
      .filter((device) => device.home_state !== "away" && roomName(device) === room)
      .sort(compareByDistance),
  }))
  .filter((group) => group.devices.length || roomFilter.value !== "all"));
const popupGroup = computed(() => roomGroups.value.find((group) => group.room === roomPopup.value) || null);
const selectedDevice = computed(() => devices.value.find((device) => deviceKey(device) === selectedId.value) || null);
const diagnostics = computed<JsonRow>(() => (snapshot.value.diagnostics || {}) as JsonRow);
const settings = computed<JsonRow>(() => (configuration.value.settings || snapshot.value.settings || {}) as JsonRow);
const capabilities = computed<JsonRow>(() => (configuration.value.capabilities || {}) as JsonRow);
const streamLabel = computed(() => ({ connecting: "Connecting", live: "Live", reconnecting: "Reconnecting", unsupported: "Polling" })[streamState.value]);

function emptyEditor(): DeviceEditor {
  return { id: "", configured: false, address: "", name: "", owner: "", category: "device", track: true, referencePower: "", maxRadius: "", homeTimeout: "", ibeaconId: "", irk: "", identities: [] };
}

function deviceKey(device: JsonRow): string {
  return String(device.presence_id || device.id || device.address || "");
}

function roomName(device: JsonRow): string {
  return String(device.location_room || device.location || device.strongest_room || "Unknown").trim() || "Unknown";
}

function deviceName(device: JsonRow): string {
  return String(device.display_name || device.advertised_name || `BLE ${String(device.address || "").slice(-8).toUpperCase()}`);
}

function ageSeconds(timestamp: unknown): number {
  const parsed = Number(timestamp || 0);
  return parsed > 0 ? Math.max(0, nowTs.value - parsed) : Number.POSITIVE_INFINITY;
}

function ageLabel(timestamp: unknown): string {
  const age = ageSeconds(timestamp);
  if (!Number.isFinite(age)) return "never";
  if (age < 2) return "now";
  if (age < 60) return `${Math.floor(age)}s ago`;
  if (age < 3600) return `${Math.floor(age / 60)}m ago`;
  if (age < 86400) return `${Math.floor(age / 3600)}h ago`;
  return `${Math.floor(age / 86400)}d ago`;
}

function distanceLabel(value: unknown): string {
  const metres = Number(value);
  if (!Number.isFinite(metres) || metres <= 0 || metres >= 999) return "Distance unavailable";
  if (metres < 1) return `${Math.round(metres * 100)} cm away`;
  return `~${metres < 10 ? metres.toFixed(1) : Math.round(metres)} m away`;
}

function signalBars(value: unknown): number {
  const rssi = Number(value ?? -127);
  if (rssi >= -55) return 4;
  if (rssi >= -67) return 3;
  if (rssi >= -78) return 2;
  return 1;
}

function compareByDistance(left: JsonRow, right: JsonRow): number {
  const leftDistance = Number(left.distance_m ?? 999);
  const rightDistance = Number(right.distance_m ?? 999);
  if (Math.abs(leftDistance - rightDistance) >= 0.35) return leftDistance - rightDistance;
  return deviceName(left).localeCompare(deviceName(right));
}

function compareRegistryDevices(left: JsonRow, right: JsonRow): number {
  const byName = deviceName(left).localeCompare(deviceName(right), undefined, { numeric: true, sensitivity: "base" });
  if (byName) return byName;
  return deviceKey(left).localeCompare(deviceKey(right), undefined, { numeric: true, sensitivity: "base" });
}

function roomInitial(room: string): string {
  return room === "Unknown" ? "?" : room.split(/\s+/).map((part) => part[0] || "").join("").slice(0, 2).toUpperCase();
}

function categoryIcon(device: JsonRow): string {
  const category = String(device.category || "").toLowerCase();
  if (category.includes("phone")) return "▯";
  if (category.includes("watch")) return "◉";
  if (category.includes("person")) return "●";
  if (category.includes("pet")) return "◆";
  if (device.ibeacon || category.includes("beacon")) return "⌁";
  return "•";
}

function eventIcon(event: JsonRow): string {
  if (event.type === "arrived_home") return "↘";
  if (event.type === "left_home") return "↗";
  return "→";
}

function eventText(event: JsonRow): string {
  if (event.type === "arrived_home") return `Arrived home${event.room ? ` · ${event.room}` : ""}`;
  if (event.type === "left_home") return "Left home";
  return `${event.from || "Unknown"} → ${event.to || event.room || "Unknown"}`;
}

function identitySummary(device: JsonRow): string {
  const identities = Array.isArray(device.identities) ? device.identities as JsonRow[] : [];
  if (!identities.length) return String(device.address || "Unidentified BLE device");
  return identities.map((identity) => String(identity.type || "BLE").toUpperCase()).join(" + ");
}

function applySnapshot(next: JsonRow) {
  snapshot.value = next;
  const drafts = { ...scannerDrafts.value };
  const nextSources = Array.isArray(next.sources) ? next.sources as JsonRow[] : [];
  nextSources.forEach((source) => {
    const selector = String(source.selector || "");
    if (selector && drafts[selector] === undefined) drafts[selector] = String(source.rssi_offset_db ?? 0);
  });
  scannerDrafts.value = drafts;
  updatedAt.value = Date.now() / 1000;
  loading.value = false;
  error.value = "";
  if (selectedId.value && !devices.value.some((device) => deviceKey(device) === selectedId.value)) selectedId.value = "";
}

function endpointWithDefaults(endpoint: string): string {
  const url = new URL(endpoint, window.location.href);
  url.searchParams.set("limit", "500");
  url.searchParams.set("max_age_s", "60");
  url.searchParams.set("include_observations", "false");
  url.searchParams.set("history_limit", "100");
  return url.toString();
}

async function refresh(silent = false) {
  if (!silent) loading.value = true;
  try {
    applySnapshot(await getJson<JsonRow>(endpointWithDefaults(props.snapshotEndpoint)));
  } catch (refreshError) {
    error.value = refreshError instanceof Error ? refreshError.message : "Presence data could not be loaded.";
    loading.value = false;
  }
}

async function loadConfiguration() {
  try {
    const next = await getJson<JsonRow>(configEndpoint.value);
    configuration.value = next;
    settingsDraft.value = { ...((next.settings || {}) as JsonRow) };
    const drafts: Record<string, string> = {};
    Object.entries((next.scanners || {}) as JsonRow).forEach(([selector, value]) => {
      drafts[selector] = String((value as JsonRow)?.rssi_offset_db ?? 0);
    });
    sources.value.forEach((source) => {
      const selector = String(source.selector || "");
      if (selector && drafts[selector] === undefined) drafts[selector] = String(source.rssi_offset_db ?? 0);
    });
    scannerDrafts.value = drafts;
  } catch (configError) {
    error.value = configError instanceof Error ? configError.message : "Presence configuration could not be loaded.";
  }
}

async function presenceAction(action: string, payload: JsonRow = {}): Promise<boolean> {
  saving.value = true;
  try {
    configuration.value = await postJson<JsonRow>(configEndpoint.value, { action, payload });
    settingsDraft.value = { ...((configuration.value.settings || {}) as JsonRow) };
    await refresh(true);
    emit("notify", "Tater Presence updated.", "success");
    return true;
  } catch (actionError) {
    emit("notify", actionError instanceof Error ? actionError.message : "Presence update failed.", "error");
    return false;
  } finally {
    saving.value = false;
  }
}

function selectDevice(device: JsonRow, closeRoom = false) {
  const key = deviceKey(device);
  selectedId.value = selectedId.value === key ? "" : key;
  if (closeRoom) roomPopup.value = "";
}

function openEditor(device: JsonRow) {
  const identities = Array.isArray(device.identities) ? (device.identities as JsonRow[]).map((row) => ({ ...row })) : [];
  const beaconIdentity = identities.find((identity) => identity.type === "ibeacon");
  editor.value = {
    id: deviceKey(device),
    configured: Boolean(device.configured),
    address: String(device.address || ""),
    name: deviceName(device),
    owner: String(device.owner || ""),
    category: String(device.category || "device"),
    track: device.configured ? Boolean(device.tracked) : true,
    referencePower: device.configured && device.reference_power_override != null ? String(device.reference_power_override) : "",
    maxRadius: device.configured && device.max_radius_m_override != null ? String(device.max_radius_m_override) : "",
    homeTimeout: device.configured && device.home_timeout_s_override != null ? String(device.home_timeout_s_override) : "",
    ibeaconId: String(beaconIdentity?.value || (device.ibeacon as JsonRow | undefined)?.id || ""),
    irk: "",
    identities,
  };
  editorOpen.value = true;
}

async function saveDevice() {
  const draft = editor.value;
  const identities: JsonRow[] = draft.identities
    .filter((identity) => identity.type !== "ibeacon" || !draft.ibeaconId.trim())
    .map((identity) => ({ type: identity.type, value: identity.value, fingerprint: identity.fingerprint }));
  if (draft.ibeaconId.trim()) identities.push({ type: "ibeacon", value: draft.ibeaconId.trim() });
  if (draft.irk.trim()) identities.push({ type: "irk", value: draft.irk.trim() });
  if (!identities.some((identity) => identity.type === "address") && draft.address) identities.push({ type: "address", value: draft.address });
  const saved = await presenceAction("upsert_device", {
    id: draft.id,
    address: draft.address,
    name: draft.name,
    owner: draft.owner,
    category: draft.category,
    track: draft.track,
    reference_power: draft.referencePower === "" ? null : Number(draft.referencePower),
    max_radius_m: draft.maxRadius === "" ? null : Number(draft.maxRadius),
    home_timeout_s: draft.homeTimeout === "" ? null : Number(draft.homeTimeout),
    identities,
  });
  if (!saved) return;
  editorOpen.value = false;
  selectedId.value = draft.id;
}

async function removeDevice() {
  if (!editor.value.id) return;
  const removed = await presenceAction("remove_device", { id: editor.value.id });
  if (!removed) return;
  editorOpen.value = false;
  selectedId.value = "";
}

async function saveSettings() {
  await presenceAction("save_settings", {
    reference_power: Number(settingsDraft.value.reference_power),
    attenuation: Number(settingsDraft.value.attenuation),
    home_timeout_s: Number(settingsDraft.value.home_timeout_s),
    max_home_radius_m: Number(settingsDraft.value.max_home_radius_m),
    max_room_radius_m: Number(settingsDraft.value.max_room_radius_m),
    history_days: Number(settingsDraft.value.history_days),
  });
}

async function saveScanner(selector: string) {
  await presenceAction("save_scanner", { selector, rssi_offset_db: Number(scannerDrafts.value[selector] || 0) });
}

async function copyEndpoint(value: string, label: string) {
  try {
    await navigator.clipboard.writeText(new URL(value, window.location.href).toString());
    emit("notify", `${label} endpoint copied.`, "success");
  } catch {
    emit("notify", `Could not copy the ${label.toLowerCase()} endpoint.`, "error");
  }
}

function stopStream() {
  eventSource?.close();
  eventSource = null;
}

function startStream() {
  stopStream();
  if (typeof window.EventSource !== "function") {
    streamState.value = "unsupported";
    return;
  }
  streamState.value = "connecting";
  const source = new EventSource(endpointWithDefaults(props.eventsEndpoint));
  eventSource = source;
  source.onopen = () => { streamState.value = "live"; };
  source.addEventListener("presence.snapshot", (event) => {
    try {
      applySnapshot(JSON.parse((event as MessageEvent).data) as JsonRow);
      streamState.value = "live";
    } catch {
      error.value = "A live presence update could not be read.";
    }
  });
  source.addEventListener("presence.event", () => { void refresh(true); });
  source.onerror = () => { streamState.value = "reconnecting"; };
}

function handleVisibility() {
  if (document.visibilityState !== "visible") return;
  void refresh(true);
  if (!eventSource) startStream();
}

onMounted(() => {
  void Promise.all([refresh(), loadConfiguration()]);
  startStream();
  fallbackTimer = window.setInterval(() => {
    if (document.visibilityState === "visible" && streamState.value !== "live") void refresh(true);
  }, 5000);
  clockTimer = window.setInterval(() => { nowTs.value = Date.now() / 1000; }, 1000);
  document.addEventListener("visibilitychange", handleVisibility);
});

onBeforeUnmount(() => {
  stopStream();
  window.clearInterval(fallbackTimer);
  window.clearInterval(clockTimer);
  document.removeEventListener("visibilitychange", handleVisibility);
});
</script>

<template>
  <section class="tm-stack tvoice-presence tpresence-next">
    <section class="tm-form-card tvoice-presence-hero tpresence-hero">
      <div class="tvoice-presence-radar" aria-hidden="true"><i /><i /><span /></div>
      <div class="tvoice-presence-copy">
        <span class="tv-eyebrow">Tater Presence Intelligence</span>
        <h3>Know what is home—and which room it is in</h3>
        <p>Native satellites resolve stable BLE identities, calibrated distance, room movement, and home or away state locally inside Tater.</p>
      </div>
      <div class="tvoice-presence-live" :class="`state-${streamState}`"><i /><span><strong>{{ streamLabel }}</strong><small>{{ updatedAt ? `Updated ${ageLabel(updatedAt)}` : "Waiting for data" }}</small></span></div>
    </section>

    <nav class="tpresence-nav" aria-label="Presence views">
      <button v-for="view in ([['locations', 'Locations'], ['devices', 'Tracked devices'], ['history', 'History'], ['calibration', 'Calibration']] as const)" :key="view[0]" type="button" :class="{ active: activeView === view[0] }" @click="activeView = view[0]">{{ view[1] }}</button>
    </nav>

    <div class="tm-metrics tvoice-presence-metrics tpresence-metrics">
      <article class="home"><span>Home</span><strong>{{ homeCount }}</strong><small>tracked devices</small></article>
      <article class="away"><span>Away</span><strong>{{ awayCount }}</strong><small>tracked devices</small></article>
      <article><span>Nearby</span><strong>{{ activeCount }}</strong><small>seen in 15 seconds</small></article>
      <article><span>Rooms</span><strong>{{ roomOptions.length }}</strong><small>with live coverage</small></article>
      <article><span>Scanners</span><strong>{{ onlineSources }}/{{ sources.length }}</strong><small>satellites reporting</small></article>
    </div>

    <div v-if="error" class="tv-notice error" aria-live="polite">{{ error }}</div>
    <div v-if="loading" class="tv-notice">Building the live Tater presence map…</div>

    <template v-else-if="activeView === 'locations'">
      <section class="tm-form-card tvoice-presence-board tpresence-location-board">
        <header class="tvoice-presence-board-head">
          <div><span class="tv-eyebrow">Live location view</span><h3>Room-by-room location</h3><p>Estimated distance is calibrated per device and satellite. Tater waits for sustained movement before changing rooms.</p></div>
          <div class="tvoice-presence-filters">
            <label><span>Search</span><input v-model="search" type="search" placeholder="Device, owner, or address" /></label>
            <label><span>Room</span><select v-model="roomFilter"><option value="all">All rooms</option><option v-for="room in roomOptions" :key="room" :value="room">{{ room }}</option></select></label>
          </div>
        </header>

        <div v-if="roomGroups.length" class="tvoice-presence-rooms tpresence-house">
          <article v-for="group in roomGroups" :key="group.room" class="tvoice-presence-room tpresence-room" :class="{ unknown: group.room === 'Unknown' }">
            <header><span>{{ roomInitial(group.room) }}</span><div><strong>{{ group.room }}</strong><small>{{ group.devices.length }} located · nearest first</small></div><i /></header>
            <div class="tvoice-presence-devices">
              <button v-for="device in group.devices.slice(0, ROOM_PREVIEW_LIMIT)" :key="deviceKey(device)" type="button" :class="{ selected: selectedId === deviceKey(device), stale: !device.present, tracked: device.tracked }" @click="selectDevice(device)">
                <span class="tpresence-device-icon">{{ categoryIcon(device) }}</span>
                <span class="tvoice-presence-device-copy"><strong>{{ deviceName(device) }}</strong><small><b>{{ distanceLabel(device.distance_m) }}</b> · {{ device.owner || identitySummary(device) }}</small></span>
                <span class="tvoice-presence-signal" :title="`${device.strongest_rssi} dBm`"><i v-for="bar in 4" :key="bar" :class="{ active: bar <= signalBars(device.strongest_rssi) }" /><b>{{ device.strongest_rssi }}</b></span>
              </button>
            </div>
            <button v-if="group.devices.length > ROOM_PREVIEW_LIMIT" class="tvoice-presence-room-more" type="button" @click="roomPopup = group.room"><span>View all {{ group.devices.length }}</span><small>+{{ group.devices.length - ROOM_PREVIEW_LIMIT }} more</small><b>→</b></button>
          </article>
        </div>
        <div v-else class="tv-empty">No devices match this view yet. Nearby devices appear automatically as satellites hear them.</div>
      </section>

      <section v-if="selectedDevice" class="tm-form-card tvoice-presence-detail tpresence-detail">
        <header><div><span class="tv-eyebrow">Current location</span><h3>{{ deviceName(selectedDevice) }}</h3><p>{{ selectedDevice.owner || identitySummary(selectedDevice) }} · {{ selectedDevice.address || "private identity" }}</p></div><div class="tpresence-detail-actions"><button class="tv-button" type="button" @click="openEditor(selectedDevice)">{{ selectedDevice.configured ? "Edit tracker" : "Name & track" }}</button><button class="tv-button" type="button" @click="selectedId = ''">Close</button></div></header>
        <div class="tpresence-location-callout" :class="String(selectedDevice.home_state || 'nearby')">
          <span>{{ roomInitial(roomName(selectedDevice)) }}</span>
          <div><small>{{ selectedDevice.home_state === "away" ? "Last known location" : "Located in" }}</small><strong>{{ roomName(selectedDevice) }}</strong><p>{{ distanceLabel(selectedDevice.distance_m) }} from {{ selectedDevice.strongest_selector || "the nearest satellite" }}</p></div>
          <b>{{ selectedDevice.home_state === "away" ? "Away" : selectedDevice.confidence || "Live" }}</b>
        </div>
        <div class="tvoice-presence-detail-grid">
          <div><span>Home state</span><strong>{{ selectedDevice.home_state || "nearby" }}</strong><small>changed {{ ageLabel(selectedDevice.state_changed_ts) }}</small></div>
          <div><span>Estimated distance</span><strong>{{ distanceLabel(selectedDevice.distance_m) }}</strong><small>RSSI {{ selectedDevice.calibrated_rssi ?? selectedDevice.strongest_rssi }} dBm calibrated</small></div>
          <div><span>Last seen</span><strong>{{ ageLabel(selectedDevice.last_seen_ts) }}</strong><small>{{ selectedDevice.present ? "present now" : "not currently advertising" }}</small></div>
          <div><span>Identity</span><strong>{{ identitySummary(selectedDevice) }}</strong><small>{{ (selectedDevice.addresses || []).length || 1 }} observed address{{ (selectedDevice.addresses || []).length === 1 ? "" : "es" }}</small></div>
        </div>
        <div class="tvoice-presence-source-list"><article v-for="source in selectedDevice.sources || []" :key="String(source.selector)"><span>{{ roomInitial(source.room || 'Unknown') }}</span><div><strong>{{ source.room || source.device_name || "Unknown room" }}</strong><small>{{ distanceLabel(source.distance_m) }} · offset {{ Number(source.rssi_offset_db || 0) >= 0 ? "+" : "" }}{{ source.rssi_offset_db || 0 }} dB</small></div><b>{{ source.calibrated_rssi ?? source.rssi }} dBm</b></article></div>
      </section>
    </template>

    <template v-else-if="activeView === 'devices'">
      <section class="tm-form-card tpresence-device-registry">
        <header><div><span class="tv-eyebrow">Stable identity registry</span><h3>Tracked devices</h3><p>This registry stays alphabetized while live room and distance changes remain in Locations.</p></div><span>{{ trackedDevices.length }} tracking</span></header>
        <div v-if="registeredDevices.length" class="tpresence-registry-grid">
          <button v-for="device in registeredDevices" :key="deviceKey(device)" type="button" class="configured" :class="{ away: device.home_state === 'away' }" @click="openEditor(device)">
            <span class="tpresence-device-icon">{{ categoryIcon(device) }}</span>
            <span><strong>{{ deviceName(device) }}</strong><small>{{ identitySummary(device) }} · {{ device.owner || "No owner" }}</small></span>
            <b>{{ !device.tracked ? "Paused" : device.home_state === "away" ? "Away" : roomName(device) }}</b>
            <i>Edit</i>
          </button>
        </div>
        <div v-else class="tv-empty">No devices are tracked yet. Add one from the nearby-device list below.</div>

        <details class="tpresence-discovered">
          <summary><span><strong>Add a nearby device</strong><small>Unassigned devices stay in a stable identity order—not live distance order.</small></span><b>{{ availableDevices.length }} available</b></summary>
          <div class="tpresence-discovered-body">
            <label class="tpresence-registry-search"><span>Find a device</span><input v-model="registrySearch" type="search" placeholder="Name or BLE address" /></label>
            <div v-if="availableDevices.length" class="tpresence-registry-grid unassigned">
              <button v-for="device in availableDevices" :key="deviceKey(device)" type="button" @click="openEditor(device)">
                <span class="tpresence-device-icon">{{ categoryIcon(device) }}</span>
                <span><strong>{{ deviceName(device) }}</strong><small>{{ identitySummary(device) }}</small></span>
                <b>Track</b>
              </button>
            </div>
            <div v-else class="tv-empty">{{ registrySearch ? "No nearby devices match that search." : "No unassigned BLE devices are nearby." }}</div>
          </div>
        </details>
      </section>
    </template>

    <template v-else-if="activeView === 'history'">
      <section class="tm-form-card tpresence-history">
        <header><div><span class="tv-eyebrow">Persistent movement history</span><h3>Arrivals, departures, and room changes</h3><p>History stays in Tater for {{ settings.history_days || 30 }} days and is available to Cores, Verbas, and native automations.</p></div><button v-if="history.length" class="tv-button" type="button" :disabled="saving" @click="presenceAction('clear_history')">Clear history</button></header>
        <div v-if="history.length" class="tpresence-timeline">
          <article v-for="event in history" :key="String(event.id)"><span>{{ eventIcon(event) }}</span><div><strong>{{ event.name || event.device_id }}</strong><p>{{ eventText(event) }}</p><small><template v-if="event.distance_m">{{ distanceLabel(event.distance_m) }} · </template>{{ event.confidence || "recorded" }} confidence</small></div><time>{{ ageLabel(event.at) }}</time></article>
        </div>
        <div v-else class="tv-empty">Track a device to begin recording room movement and home or away changes.</div>
      </section>
    </template>

    <template v-else>
      <div class="tpresence-calibration-grid">
        <section class="tm-form-card tpresence-calibration">
          <header><div><span class="tv-eyebrow">Distance model</span><h3>Whole-home calibration</h3><p>These defaults convert steadied RSSI into an estimated distance. Device-specific values can override them.</p></div><span>Local only</span></header>
          <div class="tpresence-form-grid">
            <label><span>RSSI at one metre</span><input v-model="settingsDraft.reference_power" type="number" min="-100" max="-20" step="1" /><small>Place a reference beacon exactly 1 m from a satellite.</small></label>
            <label><span>Environmental attenuation</span><input v-model="settingsDraft.attenuation" type="number" min="1" max="6" step="0.1" /><small>Typical indoor values are 2.2–3.5.</small></label>
            <label><span>Room radius</span><input v-model="settingsDraft.max_room_radius_m" type="number" min="1" max="100" step="1" /><small>Signals beyond this show as Unknown.</small></label>
            <label><span>Home radius</span><input v-model="settingsDraft.max_home_radius_m" type="number" min="1" max="250" step="1" /><small>Maximum distance still considered home.</small></label>
            <label><span>Away timeout</span><input v-model="settingsDraft.home_timeout_s" type="number" min="15" max="3600" step="15" /><small>Seconds without a trustworthy observation.</small></label>
            <label><span>History retention</span><input v-model="settingsDraft.history_days" type="number" min="1" max="365" step="1" /><small>Days of arrival and movement events.</small></label>
          </div>
          <footer><span>Distance is an estimate; walls, people, and antenna orientation affect BLE RSSI.</span><button class="tv-button primary" type="button" :disabled="saving" @click="saveSettings">{{ saving ? "Saving…" : "Save model" }}</button></footer>
        </section>

        <section class="tm-form-card tpresence-scanner-calibration">
          <header><div><span class="tv-eyebrow">Receiver normalization</span><h3>Satellite RSSI offsets</h3><p>Use offsets when one satellite consistently appears stronger or weaker than the others at the same physical distance.</p></div><span>{{ diagnostics.calibrated_scanners || 0 }} calibrated</span></header>
          <div v-if="sources.length">
            <article v-for="source in sources" :key="String(source.selector)" :class="{ offline: ageSeconds(source.last_seen_ts) > 15 }"><i /><span><strong>{{ source.room || source.device_name || source.selector }}</strong><small>{{ source.board || source.selector }} · {{ ageLabel(source.last_seen_ts) }}</small></span><label><input v-model="scannerDrafts[String(source.selector)]" type="number" min="-40" max="40" step="0.5" /><small>dB</small></label><button class="tv-button" type="button" :disabled="saving" @click="saveScanner(String(source.selector))">Save</button></article>
          </div>
          <div v-else class="tv-empty">Satellite calibration appears after a native scanner reports in.</div>
        </section>
      </div>
    </template>

    <div class="tvoice-presence-lower tpresence-lower">
      <section class="tm-form-card tvoice-presence-api">
        <header><div><span class="tv-eyebrow">Tater-native API</span><h3>Use location anywhere in Tater</h3></div><span>REST + SSE</span></header>
        <p>Cores, Verbas, integrations, and automations can read current locations or listen continuously for movement events—without Home Assistant.</p>
        <button type="button" @click="copyEndpoint(snapshotEndpoint, 'Snapshot API')"><span>Locations</span><code>{{ snapshotEndpoint }}</code><b>Copy</b></button>
        <button type="button" @click="copyEndpoint(eventsEndpoint, 'Live stream')"><span>Live events</span><code>{{ eventsEndpoint }}</code><b>Copy</b></button>
      </section>

      <section class="tm-form-card tpresence-diagnostics">
        <header><div><span class="tv-eyebrow">System health</span><h3>Presence diagnostics</h3></div><span>{{ capabilities.irk ? "Full identity" : "BLE identity" }}</span></header>
        <div><article><span>Identity registry</span><strong>{{ diagnostics.identity_registry || 0 }}</strong><small>named devices</small></article><article><span>Private identities</span><strong>{{ diagnostics.irk_identities || 0 }}</strong><small>IRKs stored privately</small></article><article><span>iBeacons visible</span><strong>{{ diagnostics.ibeacons_visible || 0 }}</strong><small>stable beacon IDs</small></article><article><span>Observation buffer</span><strong>{{ diagnostics.bounded_observations || 0 }}/{{ diagnostics.observation_capacity || 1024 }}</strong><small>bounded server memory</small></article><article><span>Movement history</span><strong>{{ diagnostics.history_events || 0 }}/{{ diagnostics.history_capacity || 2000 }}</strong><small>persistent bounded events</small></article><article><span>Identity cache</span><strong>{{ diagnostics.identity_cache || 0 }}/{{ diagnostics.identity_cache_capacity || 2048 }}</strong><small>rotating-address lookups</small></article></div>
      </section>
    </div>

    <PopupTransition :open="Boolean(popupGroup)" backdrop-class="tv-modal-backdrop tset-modal" @close="roomPopup = ''">
      <section v-if="popupGroup" class="tv-modal tvoice-presence-room-modal" role="dialog" aria-modal="true">
        <header><div class="tvoice-presence-room-modal-title"><span>{{ roomInitial(popupGroup.room) }}</span><div><small class="tv-eyebrow">Live room devices</small><h2>{{ popupGroup.room }}</h2><p>{{ popupGroup.devices.length }} located · nearest first</p></div></div><button class="tv-button" type="button" @click="roomPopup = ''">Close</button></header>
        <div class="tvoice-presence-room-modal-body"><button v-for="device in popupGroup.devices" :key="deviceKey(device)" type="button" @click="selectDevice(device, true)"><span class="tpresence-device-icon">{{ categoryIcon(device) }}</span><span class="tvoice-presence-device-copy"><strong>{{ deviceName(device) }}</strong><small>{{ distanceLabel(device.distance_m) }} · {{ identitySummary(device) }}</small></span><b>{{ device.strongest_rssi }} dBm</b></button></div>
        <footer><span>Select a device to inspect its calibrated location.</span><button class="tv-button primary" type="button" @click="roomPopup = ''">Done</button></footer>
      </section>
    </PopupTransition>

    <PopupTransition :open="editorOpen" backdrop-class="tv-modal-backdrop tset-modal" @close="editorOpen = false">
      <section v-if="editorOpen" class="tv-modal tpresence-editor" role="dialog" aria-modal="true" aria-labelledby="tpresence-editor-title">
        <header><div><small class="tv-eyebrow">Tater identity registry</small><h2 id="tpresence-editor-title">{{ editor.name || "Track BLE device" }}</h2><p>Names and private identity keys stay on this Tater.</p></div><button class="tv-button" type="button" @click="editorOpen = false">Close</button></header>
        <div class="tpresence-editor-body">
          <div class="tpresence-form-grid">
            <label><span>Display name</span><input v-model="editor.name" placeholder="Alice's phone" /></label>
            <label><span>Owner or person</span><input v-model="editor.owner" placeholder="Alice" /></label>
            <label><span>Category</span><select v-model="editor.category"><option value="device">Device</option><option value="phone">Phone</option><option value="watch">Watch</option><option value="person">Person</option><option value="pet">Pet</option><option value="keys">Keys</option><option value="beacon">Beacon</option></select></label>
            <label class="tpresence-check"><input v-model="editor.track" type="checkbox" /><span><b>Track home and room state</b><small>Record arrivals, departures, and room changes.</small></span></label>
          </div>
          <section><header><div><strong>Stable identity</strong><small>Use the detected address, iBeacon ID, or a private IRK.</small></div></header><div class="tpresence-form-grid"><label><span>Detected address</span><input v-model="editor.address" readonly /></label><label><span>iBeacon UUID:major:minor</span><input v-model="editor.ibeaconId" placeholder="uuid:1:2" /></label><label class="wide"><span>Identity Resolving Key</span><input v-model="editor.irk" type="password" placeholder="Paste 32 hex characters or Base64" /><small>Leave blank to keep an existing IRK. It is never returned by the API.</small></label></div><div v-if="editor.identities.length" class="tpresence-identity-chips"><span v-for="identity in editor.identities" :key="String(identity.type) + String(identity.value || identity.fingerprint)">{{ identity.display || identity.type }}</span></div></section>
          <section><header><div><strong>Optional device calibration</strong><small>Leave blank to use the whole-home defaults.</small></div></header><div class="tpresence-form-grid"><label><span>RSSI at one metre</span><input v-model="editor.referencePower" type="number" min="-100" max="-20" placeholder="Global default" /></label><label><span>Room radius in metres</span><input v-model="editor.maxRadius" type="number" min="1" max="100" placeholder="Global default" /></label><label><span>Away timeout in seconds</span><input v-model="editor.homeTimeout" type="number" min="15" max="3600" placeholder="Global default" /></label></div></section>
        </div>
        <footer><button v-if="editor.configured" class="tv-button danger" type="button" :disabled="saving" @click="removeDevice">Stop tracking</button><span /><button class="tv-button" type="button" @click="editorOpen = false">Cancel</button><button class="tv-button primary" type="button" :disabled="saving || !editor.name.trim()" @click="saveDevice">{{ saving ? "Saving…" : "Save tracker" }}</button></footer>
      </section>
    </PopupTransition>
  </section>
</template>
