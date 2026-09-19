<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { getJson } from "../../../shared/api";
import PopupTransition from "../../../shared/PopupTransition.vue";
import type { JsonRow } from "../../types";

const props = defineProps<{
  snapshotEndpoint: string;
  eventsEndpoint: string;
}>();

const emit = defineEmits<{ notify: [message: string, tone?: string] }>();

type Movement = { address: string; name: string; from: string; to: string; at: number };
const ROOM_PREVIEW_LIMIT = 4;
const RANK_CHANGE_THRESHOLD_DBM = 8;

const snapshot = ref<JsonRow>({ devices: [], sources: [], rooms: {} });
const loading = ref(true);
const error = ref("");
const streamState = ref<"connecting" | "live" | "reconnecting" | "unsupported">("connecting");
const search = ref("");
const roomFilter = ref("all");
const selectedAddress = ref("");
const roomPopup = ref("");
const nowTs = ref(Date.now() / 1000);
const updatedAt = ref(0);
const movements = ref<Record<string, Movement>>({});
const lastRooms = new Map<string, string>();
const smoothedRssi = new Map<string, number>();
const rankedRssi = new Map<string, number>();
let eventSource: EventSource | null = null;
let fallbackTimer = 0;
let clockTimer = 0;

const devices = computed<JsonRow[]>(() => Array.isArray(snapshot.value.devices) ? snapshot.value.devices : []);
const sources = computed<JsonRow[]>(() => Array.isArray(snapshot.value.sources) ? snapshot.value.sources : []);
const roomOptions = computed(() => {
  const values = new Set(devices.value.map((device) => roomName(device)).filter(Boolean));
  return [...values].sort((left, right) => left === "Unknown" ? 1 : right === "Unknown" ? -1 : left.localeCompare(right));
});
const filteredDevices = computed(() => {
  const query = search.value.trim().toLowerCase();
  return devices.value.filter((device) => {
    const room = roomName(device);
    if (roomFilter.value !== "all" && room !== roomFilter.value) return false;
    if (!query) return true;
    return [device.display_name, device.advertised_name, device.address, device.manufacturer_id_hex, room]
      .some((value) => String(value || "").toLowerCase().includes(query));
  });
});
const roomGroups = computed(() => roomOptions.value
  .filter((room) => roomFilter.value === "all" || room === roomFilter.value)
  .map((room) => ({
    room,
    devices: filteredDevices.value
      .filter((device) => roomName(device) === room)
      .sort(compareByProximity),
  }))
  .filter((group) => group.devices.length || roomFilter.value !== "all"));
const popupGroup = computed(() => roomGroups.value.find((group) => group.room === roomPopup.value) || null);
const selectedDevice = computed(() => devices.value.find((device) => String(device.address || "") === selectedAddress.value) || null);
const activeCount = computed(() => devices.value.filter((device) => ageSeconds(device.last_seen_ts) <= 15).length);
const namedCount = computed(() => devices.value.filter((device) => String(device.advertised_name || "").trim()).length);
const onlineSources = computed(() => sources.value.filter((source) => ageSeconds(source.last_seen_ts) <= 15).length);
const recentMovements = computed(() => Object.values(movements.value)
  .filter((movement) => nowTs.value - movement.at <= 120)
  .sort((left, right) => right.at - left.at)
  .slice(0, 8));
const streamLabel = computed(() => ({
  connecting: "Connecting",
  live: "Live",
  reconnecting: "Reconnecting",
  unsupported: "Polling",
})[streamState.value]);

function roomName(device: JsonRow): string {
  return String(device.strongest_room || "Unknown").trim() || "Unknown";
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
  return `${Math.floor(age / 3600)}h ago`;
}

function signalBars(value: unknown): number {
  const rssi = Number(value ?? -127);
  if (rssi >= -55) return 4;
  if (rssi >= -67) return 3;
  if (rssi >= -78) return 2;
  return 1;
}

function proximityRssi(device: JsonRow): number {
  const address = String(device.address || "");
  return rankedRssi.get(address) ?? smoothedRssi.get(address) ?? Number(device.strongest_rssi ?? -127);
}

function proximityBand(device: JsonRow): number {
  // Six-dBm bands stop stationary devices from swapping places on every tiny
  // radio fluctuation while still letting a genuinely moving device re-rank.
  return Math.round(proximityRssi(device) / 6) * 6;
}

function proximityTier(device: JsonRow): number {
  const rssi = proximityRssi(device);
  if (rssi >= -55) return 3;
  if (rssi >= -67) return 2;
  if (rssi >= -78) return 1;
  return 0;
}

function proximityLabel(device: JsonRow): string {
  const tier = proximityTier(device);
  if (tier === 3) return "Very close";
  if (tier === 2) return "Close";
  if (tier === 1) return "Nearby";
  return "Farther away";
}

function compareByProximity(left: JsonRow, right: JsonRow): number {
  const tier = proximityTier(right) - proximityTier(left);
  if (tier) return tier;
  const distance = proximityBand(right) - proximityBand(left);
  if (distance) return distance;
  const byName = deviceName(left).localeCompare(deviceName(right));
  return byName || String(left.address || "").localeCompare(String(right.address || ""));
}

function roomInitial(room: string): string {
  return room === "Unknown" ? "?" : room.split(/\s+/).map((part) => part[0] || "").join("").slice(0, 2).toUpperCase();
}

function previewDevices(rows: JsonRow[]): JsonRow[] {
  return rows.slice(0, ROOM_PREVIEW_LIMIT);
}

function movedRecently(device: JsonRow): boolean {
  const movement = movements.value[String(device.address || "")];
  return Boolean(movement && nowTs.value - movement.at <= 4);
}

function selectDevice(device: JsonRow, closeRoom = false) {
  const address = String(device.address || "");
  selectedAddress.value = selectedAddress.value === address ? "" : address;
  if (closeRoom) roomPopup.value = "";
}

function applySnapshot(next: JsonRow) {
  const nextDevices = Array.isArray(next.devices) ? next.devices as JsonRow[] : [];
  const nextMovements = { ...movements.value };
  const activeAddresses = new Set<string>();
  nextDevices.forEach((device) => {
    const address = String(device.address || "");
    const room = roomName(device);
    const previous = lastRooms.get(address);
    const measuredRssi = Number(device.strongest_rssi ?? -127);
    if (address) {
      activeAddresses.add(address);
      const previousRssi = smoothedRssi.get(address);
      const nextRssi = previousRssi === undefined ? measuredRssi : (previousRssi * 0.75) + (measuredRssi * 0.25);
      smoothedRssi.set(address, nextRssi);
      const previousRank = rankedRssi.get(address);
      if (previousRank === undefined || previous !== room || Math.abs(nextRssi - previousRank) >= RANK_CHANGE_THRESHOLD_DBM) {
        rankedRssi.set(address, nextRssi);
      }
    }
    if (address && previous && previous !== room) {
      nextMovements[address] = { address, name: deviceName(device), from: previous, to: room, at: Date.now() / 1000 };
    }
    if (address) lastRooms.set(address, room);
  });
  for (const address of smoothedRssi.keys()) {
    if (!activeAddresses.has(address)) {
      smoothedRssi.delete(address);
      rankedRssi.delete(address);
    }
  }
  movements.value = nextMovements;
  snapshot.value = next;
  updatedAt.value = Date.now() / 1000;
  loading.value = false;
  error.value = "";
}

function endpointWithDefaults(endpoint: string): string {
  const url = new URL(endpoint, window.location.href);
  url.searchParams.set("limit", "500");
  url.searchParams.set("max_age_s", "60");
  url.searchParams.set("include_observations", "false");
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
  source.onerror = () => { streamState.value = "reconnecting"; };
}

async function copyEndpoint(value: string, label: string) {
  try {
    await navigator.clipboard.writeText(new URL(value, window.location.href).toString());
    emit("notify", `${label} endpoint copied.`, "success");
  } catch {
    emit("notify", `Could not copy the ${label.toLowerCase()} endpoint.`, "error");
  }
}

function handleVisibility() {
  if (document.visibilityState !== "visible") return;
  void refresh(true);
  if (!eventSource) startStream();
}

onMounted(() => {
  void refresh();
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
  <section class="tm-stack tvoice-presence">
    <section class="tm-form-card tvoice-presence-hero">
      <div class="tvoice-presence-radar" aria-hidden="true"><i /><i /><span /></div>
      <div class="tvoice-presence-copy">
        <span class="tv-eyebrow">Whole-home Bluetooth presence</span>
        <h3>See devices move through your rooms</h3>
        <p>Every native satellite contributes passive BLE observations. Tater compares signal and freshness, then updates the strongest room live.</p>
      </div>
      <div class="tvoice-presence-live" :class="`state-${streamState}`"><i /><span><strong>{{ streamLabel }}</strong><small>{{ updatedAt ? `Updated ${ageLabel(updatedAt)}` : "Waiting for data" }}</small></span></div>
    </section>

    <div class="tm-metrics tvoice-presence-metrics">
      <article><span>Present now</span><strong>{{ activeCount }}</strong><small>seen in 15 seconds</small></article>
      <article><span>Detected</span><strong>{{ devices.length }}</strong><small>within one minute</small></article>
      <article><span>Rooms</span><strong>{{ roomOptions.length }}</strong><small>reporting locations</small></article>
      <article><span>Scanners</span><strong>{{ onlineSources }}/{{ sources.length }}</strong><small>satellites online</small></article>
      <article><span>Named</span><strong>{{ namedCount }}</strong><small>advertised names</small></article>
    </div>

    <div v-if="error" class="tv-notice error" aria-live="polite">{{ error }}</div>
    <div v-if="loading" class="tv-notice">Listening for nearby Bluetooth devices…</div>

    <section v-else class="tm-form-card tvoice-presence-board">
      <header class="tvoice-presence-board-head">
        <div><span class="tv-eyebrow">Live room map</span><h3>Where devices are strongest right now</h3><p>Devices are ordered closest first using a steadied signal, so stationary devices stay put while movement remains visible.</p></div>
        <div class="tvoice-presence-filters">
          <label><span>Search</span><input v-model="search" type="search" placeholder="Name or BLE address" /></label>
          <label><span>Room</span><select v-model="roomFilter"><option value="all">All rooms</option><option v-for="room in roomOptions" :key="room" :value="room">{{ room }}</option></select></label>
        </div>
      </header>

      <div v-if="roomGroups.length" class="tvoice-presence-rooms">
        <article v-for="group in roomGroups" :key="group.room" class="tvoice-presence-room" :class="{ unknown: group.room === 'Unknown' }">
          <header><span>{{ roomInitial(group.room) }}</span><div><strong>{{ group.room }}</strong><small>{{ group.devices.length }} device{{ group.devices.length === 1 ? "" : "s" }} · closest first</small></div><i /></header>
          <div class="tvoice-presence-devices">
            <button v-for="device in previewDevices(group.devices)" :key="String(device.address)" type="button" :class="{ selected: selectedAddress === device.address, stale: !device.present, moved: movedRecently(device) }" @click="selectDevice(device)">
              <span class="tvoice-presence-device-mark"><i :class="`signal-${device.signal || 'weak'}`" /></span>
              <span class="tvoice-presence-device-copy"><strong>{{ deviceName(device) }}</strong><small>{{ proximityLabel(device) }} · {{ device.address }} · {{ ageLabel(device.last_seen_ts) }}</small></span>
              <span class="tvoice-presence-signal" :title="`${device.strongest_rssi} dBm`"><i v-for="bar in 4" :key="bar" :class="{ active: bar <= signalBars(device.strongest_rssi) }" /><b>{{ device.strongest_rssi }}</b></span>
            </button>
          </div>
          <button v-if="group.devices.length > ROOM_PREVIEW_LIMIT" class="tvoice-presence-room-more" type="button" @click="roomPopup = group.room"><span>View all {{ group.devices.length }} devices</span><small>+{{ group.devices.length - ROOM_PREVIEW_LIMIT }} more</small><b>→</b></button>
        </article>
      </div>
      <div v-else class="tv-empty">No devices match this view yet. Nearby devices appear automatically as satellites hear them.</div>
    </section>

    <PopupTransition :open="Boolean(popupGroup)" backdrop-class="tv-modal-backdrop tset-modal" @close="roomPopup = ''">
      <section v-if="popupGroup" class="tv-modal tvoice-presence-room-modal" role="dialog" aria-modal="true" :aria-labelledby="`presence-room-${roomInitial(popupGroup.room)}`">
        <header><div class="tvoice-presence-room-modal-title"><span>{{ roomInitial(popupGroup.room) }}</span><div><small class="tv-eyebrow">Live room devices</small><h2 :id="`presence-room-${roomInitial(popupGroup.room)}`">{{ popupGroup.room }}</h2><p>{{ popupGroup.devices.length }} device{{ popupGroup.devices.length === 1 ? "" : "s" }} · stable proximity order</p></div></div><button class="tv-button" type="button" @click="roomPopup = ''">Close</button></header>
        <div class="tvoice-presence-room-modal-body">
          <button v-for="device in popupGroup.devices" :key="String(device.address)" type="button" :class="{ stale: !device.present, moved: movedRecently(device) }" @click="selectDevice(device, true)">
            <span class="tvoice-presence-device-mark"><i :class="`signal-${device.signal || 'weak'}`" /></span>
            <span class="tvoice-presence-device-copy"><strong>{{ deviceName(device) }}</strong><small>{{ proximityLabel(device) }} · {{ device.address }} · {{ ageLabel(device.last_seen_ts) }}</small></span>
            <span class="tvoice-presence-signal" :title="`${device.strongest_rssi} dBm`"><i v-for="bar in 4" :key="bar" :class="{ active: bar <= signalBars(device.strongest_rssi) }" /><b>{{ device.strongest_rssi }}</b></span>
          </button>
        </div>
        <footer><span>Select a device to open its detailed satellite readings.</span><button class="tv-button primary" type="button" @click="roomPopup = ''">Done</button></footer>
      </section>
    </PopupTransition>

    <section v-if="selectedDevice" class="tm-form-card tvoice-presence-detail">
      <header><div><span class="tv-eyebrow">Device detail</span><h3>{{ deviceName(selectedDevice) }}</h3><p>{{ selectedDevice.address }}<template v-if="selectedDevice.manufacturer_id_hex"> · Manufacturer {{ selectedDevice.manufacturer_id_hex }}</template></p></div><button class="tv-button" type="button" @click="selectedAddress = ''">Close</button></header>
      <div class="tvoice-presence-detail-grid">
        <div><span>Current room</span><strong>{{ roomName(selectedDevice) }}</strong><small>{{ selectedDevice.confidence || "unknown" }} confidence</small></div>
        <div><span>Strongest signal</span><strong>{{ selectedDevice.strongest_rssi }} dBm</strong><small>{{ selectedDevice.signal || "unknown" }}</small></div>
        <div><span>Last seen</span><strong>{{ ageLabel(selectedDevice.last_seen_ts) }}</strong><small>{{ selectedDevice.present ? "present now" : "recently seen" }}</small></div>
        <div><span>Service UUIDs</span><strong>{{ (selectedDevice.service_uuids || []).length || "—" }}</strong><small>{{ (selectedDevice.service_uuids || []).join(", ") || "none advertised" }}</small></div>
      </div>
      <div class="tvoice-presence-source-list"><article v-for="source in selectedDevice.sources || []" :key="String(source.selector)"><span>{{ roomInitial(source.room || 'Unknown') }}</span><div><strong>{{ source.room || source.device_name || "Unknown room" }}</strong><small>{{ source.device_name || source.selector }} · {{ ageLabel(source.received_ts) }}</small></div><b>{{ source.rssi }} dBm</b></article></div>
    </section>

    <div class="tvoice-presence-lower">
      <section class="tm-form-card tvoice-presence-moves">
        <header><div><span class="tv-eyebrow">Movement feed</span><h3>Recent room changes</h3></div><span>{{ recentMovements.length }} live</span></header>
        <div v-if="recentMovements.length"><article v-for="movement in recentMovements" :key="`${movement.address}:${movement.at}`"><span>↗</span><div><strong>{{ movement.name }}</strong><small>{{ movement.from }} → {{ movement.to }}</small></div><time>{{ ageLabel(movement.at) }}</time></article></div>
        <div v-else class="tv-empty compact">Room changes will appear here as signal leadership moves between satellites.</div>
      </section>

      <section class="tm-form-card tvoice-presence-api">
        <header><div><span class="tv-eyebrow">Core + Verba API</span><h3>Use presence anywhere in Tater</h3></div><span>JSON + SSE</span></header>
        <p>The snapshot endpoint answers “where is this device?” on demand. The event stream pushes complete room state whenever observations change.</p>
        <button type="button" @click="copyEndpoint(snapshotEndpoint, 'Snapshot API')"><span>REST snapshot</span><code>{{ snapshotEndpoint }}</code><b>Copy</b></button>
        <button type="button" @click="copyEndpoint(eventsEndpoint, 'Live stream')"><span>Live stream</span><code>{{ eventsEndpoint }}</code><b>Copy</b></button>
        <small>Filter REST calls with <code>?address=aa:bb:cc:dd:ee:ff</code>, <code>?selector=native:…</code>, or <code>?max_age_s=30</code>. In-process cores and verbas can also call <code>tater_voice.native_ble.snapshot()</code>.</small>
      </section>
    </div>

    <section v-if="sources.length" class="tm-form-card tvoice-presence-scanners">
      <header><div><span class="tv-eyebrow">Scanner network</span><h3>Satellite coverage</h3></div><span>{{ onlineSources }} reporting now</span></header>
      <div><article v-for="source in sources" :key="String(source.selector)" :class="{ offline: ageSeconds(source.last_seen_ts) > 15 }"><i /><span><strong>{{ source.room || source.device_name || source.selector }}</strong><small>{{ source.device_name || source.board || source.selector }}</small></span><b>{{ ageLabel(source.last_seen_ts) }}</b></article></div>
    </section>
  </section>
</template>
