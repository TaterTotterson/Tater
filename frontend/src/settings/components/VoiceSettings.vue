<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { getJson } from "../../shared/api";
import type { JsonRow } from "../types";
import VoiceFirmware from "./voice/VoiceFirmware.vue";
import VoiceAirPlay from "./voice/VoiceAirPlay.vue";
import VoicePlatform from "./voice/VoicePlatform.vue";
import VoicePresence from "./voice/VoicePresence.vue";
import VoiceSatellites from "./voice/VoiceSatellites.vue";
import VoiceStats from "./voice/VoiceStats.vue";
import VoiceStereo from "./voice/VoiceStereo.vue";

const props = defineProps<{
  runtimeEndpoint: string;
  actionEndpoint: string;
  presenceEndpoint: string;
  presenceEventsEndpoint: string;
  initialTab?: string;
  onTabChange?: (tab: string) => void;
}>();

const emit = defineEmits<{ notify: [message: string, tone?: string] }>();

const tabs = [
  { id: "satellites", label: "Satellites", short: "Pair & control", icon: "SAT", description: "Pair devices, manage rooms, volume, playback, and live satellite settings." },
  { id: "firmware", label: "Firmware", short: "Update & recover", icon: "FW", description: "Install official firmware over OTA or USB and follow progress live." },
  { id: "stereo", label: "Stereo Pairs", short: "Left & right", icon: "2X", description: "Create synchronized left and right playback destinations." },
  { id: "airplay", label: "AirPlay Input", short: "Stream to sats", icon: "AP", description: "Send audio from Apple devices into Tater satellites and speaker groups." },
  { id: "presence", label: "Presence", short: "Live room map", icon: "BLE", description: "Watch nearby Bluetooth devices move between the rooms covered by native satellites." },
  { id: "stats", label: "Stats", short: "Quality & latency", icon: "LIVE", description: "Live voice quality, latency, fallbacks, and per-satellite outcomes." },
  { id: "platform", label: "Settings", short: "Shared behavior", icon: "SET", description: "Shared satellite behavior and native voice-pipeline settings." },
] as const;
const tabIds = new Set(tabs.map((tab) => tab.id));
const normalize = (value: unknown) => {
  const token = String(value || "").trim().toLowerCase();
  return tabIds.has(token as typeof tabs[number]["id"]) ? token : "satellites";
};

const activeTab = ref(normalize(props.initialTab));
const payload = ref<JsonRow>({});
const loading = ref(false);
const refreshing = ref(false);
const error = ref("");
let refreshTimer: number | null = null;
let requestSequence = 0;

const activeSpec = computed(() => tabs.find((tab) => tab.id === activeTab.value) || tabs[0]);
const headerStats = computed<JsonRow[]>(() => Array.isArray(payload.value.header_stats) ? payload.value.header_stats : []);
const requestPanel = computed(() => ["platform", "presence"].includes(activeTab.value) ? "satellites" : activeTab.value);

async function refresh(silent = false) {
  const sequence = ++requestSequence;
  if (silent) refreshing.value = true;
  else loading.value = true;
  if (!silent) error.value = "";
  try {
    const result = await getJson<JsonRow>(`${props.runtimeEndpoint}?panel=${encodeURIComponent(requestPanel.value)}`);
    if (sequence !== requestSequence) return;
    payload.value = result.payload && typeof result.payload === "object" ? result.payload as JsonRow : result;
  } catch (refreshError) {
    if (sequence !== requestSequence) return;
    error.value = refreshError instanceof Error ? refreshError.message : "Voice runtime could not be loaded.";
  } finally {
    if (sequence === requestSequence) {
      loading.value = false;
      refreshing.value = false;
    }
  }
}

function select(tab: string) {
  activeTab.value = normalize(tab);
  error.value = "";
  payload.value = {};
  props.onTabChange?.(activeTab.value);
  void refresh();
}

function notify(message: string, tone = "success") {
  emit("notify", message, tone);
}

defineExpose({ select });

watch(() => props.initialTab, (next) => {
  const normalized = normalize(next);
  if (normalized !== activeTab.value) select(normalized);
});

onMounted(() => {
  props.onTabChange?.(activeTab.value);
  void refresh();
  refreshTimer = window.setInterval(() => {
    if (document.visibilityState === "visible" && !loading.value) void refresh(true);
  }, 10000);
});

onBeforeUnmount(() => {
  if (refreshTimer !== null) window.clearInterval(refreshTimer);
  requestSequence += 1;
});
</script>

<template>
  <section class="tset-resource tvoice-resource">
    <section class="tv-panel tvoice-hero">
      <div class="tvoice-hero-copy">
        <span class="tv-eyebrow">Tater satellite control center</span>
        <h2>Your voice hardware, in one friendly workspace</h2>
        <p>Pair satellites, keep firmware current, build stereo rooms, and tune Tater’s live voice pipeline without leaving Settings.</p>
      </div>
      <div class="tvoice-hero-signal" :class="{ warning: error }"><span class="tvoice-signal-orbit"><i /><i /><i /></span><div><strong>{{ refreshing ? "Refreshing devices" : `${activeSpec.label} ready` }}</strong><small>{{ error ? "Runtime needs attention" : "Live satellite state" }}</small></div></div>
    </section>

    <nav class="tv-tabs tvoice-tabs" aria-label="Satellite settings sections">
      <button v-for="tab in tabs" :key="tab.id" type="button" :class="{ active: activeTab === tab.id }" @click="select(tab.id)"><span class="tvoice-tab-mark">{{ tab.icon }}</span><span><b>{{ tab.label }}</b><small>{{ tab.short }}</small></span></button>
    </nav>

    <div class="tset-context"><span>{{ activeSpec.label }}</span><p>{{ activeSpec.description }}</p></div>
    <div v-if="error" class="tv-notice error" aria-live="polite">{{ error }}</div>
    <div v-if="loading" class="tv-notice">Loading {{ activeSpec.label }}…</div>

    <div v-if="headerStats.length && activeTab !== 'stats'" class="tm-metrics tvoice-metrics">
      <article v-for="metric in headerStats" :key="String(metric.label)"><span>{{ metric.label }}</span><strong>{{ metric.value }}</strong></article>
    </div>

    <VoiceSatellites v-if="activeTab === 'satellites' && !loading" :payload="payload" :action-endpoint="actionEndpoint" @refresh="refresh(true)" @notify="notify" />
    <VoicePresence v-else-if="activeTab === 'presence' && !loading" :snapshot-endpoint="presenceEndpoint" :events-endpoint="presenceEventsEndpoint" @notify="notify" />
    <VoiceFirmware v-else-if="activeTab === 'firmware' && !loading" :payload="payload" :action-endpoint="actionEndpoint" @refresh="refresh(true)" @notify="notify" />
    <VoiceStereo v-else-if="activeTab === 'stereo' && !loading" :payload="payload" :action-endpoint="actionEndpoint" @refresh="refresh(true)" @notify="notify" />
    <VoiceAirPlay v-else-if="activeTab === 'airplay' && !loading" :payload="payload" :action-endpoint="actionEndpoint" @refresh="refresh(true)" @notify="notify" />
    <VoiceStats v-else-if="activeTab === 'stats' && !loading" :payload="payload" :action-endpoint="actionEndpoint" @refresh="refresh(true)" @notify="notify" />
    <VoicePlatform v-else-if="activeTab === 'platform' && !loading" :payload="payload" :action-endpoint="actionEndpoint" @refresh="refresh(true)" @notify="notify" />
  </section>
</template>
