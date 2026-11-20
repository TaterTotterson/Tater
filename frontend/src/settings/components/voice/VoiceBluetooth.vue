<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { getJson, postJson } from "../../../shared/api";
import type { JsonRow } from "../../types";
const props = defineProps<{ endpoint: string }>();
const emit = defineEmits<{ notify: [message: string, tone?: string] }>();
const data = ref<JsonRow>({});
const busy = ref(false);
const error = ref("");
const selected = ref("");
const pins = ref<Record<string, string>>({});
let timer: number | undefined;
let disposed = false;
const scanners = computed<JsonRow[]>(() => data.value.scanners || []);
const devices = computed<JsonRow[]>(() => data.value.devices || []);
const paired = computed<JsonRow[]>(() => data.value.paired || []);
const available = computed(() => scanners.value.some(row => row.on_demand));
async function refresh() {
  try { const next = await getJson<JsonRow>(props.endpoint); if (!disposed) data.value = next; }
  catch (err) { if (!disposed) error.value = err instanceof Error ? err.message : "Bluetooth is unavailable."; }
}
async function action(name: string, body: JsonRow = {}) {
  if (busy.value) return;
  busy.value = true; error.value = "";
  try {
    await postJson(`${props.endpoint}/${name}`, body);
    if (name === "pair") { pins.value[String(body.address)] = ""; emit("notify", "Paired. The bond is saved on the satellite."); }
    await refresh();
  } catch (err) { error.value = err instanceof Error ? err.message : "Bluetooth request failed."; }
  finally { busy.value = false; }
}
onMounted(() => { void refresh(); timer = window.setInterval(() => { if (!busy.value && document.visibilityState === "visible") void refresh(); }, 2000); });
onBeforeUnmount(() => { disposed = true; window.clearInterval(timer); });
</script>
<template>
  <section class="tm-stack bluetooth-workspace">
    <section class="tm-form-card bluetooth-intro">
      <div><span class="tv-eyebrow">Satellite Bluetooth proxy</span><h2>Connect a device through your satellite</h2><p>Bluetooth sleeps when unused. Discovery stops automatically; devices such as Meshtastic stay connected while an app needs them.</p></div>
      <span class="bluetooth-state">{{ data.scanning ? "Discovering devices" : "Ready when needed" }}</span>
    </section>
    <div v-if="error" class="tv-notice error" role="alert">{{ error }}</div>
    <section class="tm-form-card">
      <div class="bluetooth-heading"><div><span class="tv-eyebrow">Discovery</span><h3>Find a nearby device</h3><p>Choose a satellite and scan for 15 seconds. Opening this page does not start Bluetooth.</p></div></div>
      <div class="bluetooth-actions">
        <label>Satellite<select v-model="selected"><option value="">All available satellites</option><option v-for="scanner in scanners" :key="scanner.selector" :value="scanner.selector" :disabled="!scanner.on_demand">{{ scanner.name }}{{ scanner.on_demand ? "" : " · firmware update needed" }}</option></select></label>
        <button class="tv-button primary" :disabled="busy || !available" @click="action('scan', { selector: selected, duration_s: 15 })">{{ data.scanning ? "Scan again" : "Scan for devices" }}</button>
        <button v-if="data.scanning" class="tv-button" :disabled="busy" @click="action('stop', { selector: selected })">Stop scan</button>
      </div>
      <p v-if="!available" class="tv-notice">Connect a satellite with updated Bluetooth proxy firmware to discover and pair devices.</p>
      <div class="bluetooth-proxies"><span v-for="scanner in scanners" :key="scanner.selector" class="bluetooth-chip"><b>{{ scanner.name }}</b> · {{ scanner.scanning ? `Scanning · ${scanner.remaining_s}s` : scanner.addrs?.length ? `${scanner.addrs.length} connected` : scanner.on_demand ? "Bluetooth idle" : "Update firmware" }}</span></div>
      <div v-if="devices.length" class="bluetooth-grid">
        <article v-for="device in devices" :key="device.address" class="bluetooth-device">
          <div><h3>{{ device.advertised_name || "Bluetooth device" }}</h3><small>{{ device.address }} · {{ device.rssi }} dBm</small><p>Via {{ device.device_name }}</p></div>
          <label>Six-digit PIN<input v-model="pins[device.address]" type="password" inputmode="numeric" maxlength="6" autocomplete="off" placeholder="Device PIN" /></label>
          <button class="tv-button" :disabled="busy || !/^\d{6}$/.test(pins[device.address] || '')" @click="action('pair', { selector: device.selector, address: device.address, pin: pins[device.address] })">Pair device</button>
        </article>
      </div>
      <p v-else class="bluetooth-empty">{{ data.scanning ? "Listening for nearby devices…" : "Start a scan with your device in pairing mode." }}</p>
    </section>
    <section class="tm-form-card"><span class="tv-eyebrow">Saved on the satellite</span><h3>Paired devices</h3><p>Pairing is remembered without keeping Bluetooth running. Apps reconnect when needed.</p>
      <div v-if="paired.length" class="bluetooth-grid"><article v-for="device in paired" :key="`${device.selector}:${device.address}`" class="bluetooth-device"><div><h3>{{ device.name }}</h3><small>{{ device.address }}</small><p>{{ device.satellite_name }}</p></div><button class="tv-button" :disabled="busy" @click="action('unpair', { selector: device.selector, address: device.address })">Unpair</button></article></div>
      <p v-else class="bluetooth-empty">No devices paired here yet. Existing connections managed by Cores keep their saved settings.</p>
    </section>
  </section>
</template>
<style scoped>
.bluetooth-workspace h2,.bluetooth-workspace h3{margin:6px 0 8px}.bluetooth-workspace p{color:var(--tv-muted,#929aa9);line-height:1.5;margin:5px 0 14px}.bluetooth-intro{display:flex;gap:24px;align-items:center;justify-content:space-between}.bluetooth-intro>div{max-width:720px}.bluetooth-state,.bluetooth-chip{border:1px solid var(--tv-border,#303642);border-radius:30px;padding:8px 12px;color:var(--tv-muted,#a9b1bf);font-size:12px}.bluetooth-state{white-space:nowrap}.bluetooth-actions{display:flex;align-items:end;gap:12px;flex-wrap:wrap}.bluetooth-actions label{flex:1;min-width:220px}.bluetooth-workspace label{display:grid;gap:7px;font-size:12px;color:var(--tv-muted,#929aa9)}.bluetooth-workspace input,.bluetooth-workspace select{background:var(--tv-bg,#151922);color:inherit;border:1px solid var(--tv-border,#343b49);border-radius:10px;padding:11px 12px;min-height:42px}.bluetooth-proxies{display:flex;gap:8px;flex-wrap:wrap;margin:18px 0}.bluetooth-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;margin-top:18px}.bluetooth-device{display:grid;gap:12px;border:1px solid var(--tv-border,#303642);border-radius:14px;padding:18px;background:var(--tv-bg,#151922)}.bluetooth-device small{color:var(--tv-muted,#929aa9)}.bluetooth-empty{padding:24px 0 8px}@media(max-width:700px){.bluetooth-intro{display:block}.bluetooth-state{display:inline-block;margin-top:8px}}
</style>
