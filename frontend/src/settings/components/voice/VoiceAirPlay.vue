<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import type { JsonRow } from "../../types";
import { voiceAction } from "./runtime";

const props = defineProps<{ payload: JsonRow; actionEndpoint: string }>();
const emit = defineEmits<{ refresh: []; notify: [message: string, tone?: string] }>();

const draft = reactive<{ enabled: boolean; receiver_name: string; receiver_pin: string; targets: string[] }>({
  enabled: false,
  receiver_name: "Tater Audio",
  receiver_pin: "",
  targets: [],
});
const dirty = ref(false);
const busy = ref("");
const error = ref("");

const panel = computed<JsonRow>(() => props.payload.airplay_input && typeof props.payload.airplay_input === "object" ? props.payload.airplay_input as JsonRow : {});
const settings = computed<JsonRow>(() => panel.value.settings && typeof panel.value.settings === "object" ? panel.value.settings as JsonRow : {});
const status = computed<JsonRow>(() => panel.value.status && typeof panel.value.status === "object" ? panel.value.status as JsonRow : {});
const options = computed<JsonRow[]>(() => Array.isArray(panel.value.options) ? panel.value.options as JsonRow[] : []);
const statusToken = computed(() => String(status.value.status || (draft.enabled ? "starting" : "disabled")).toLowerCase());
const statusLabel = computed(() => ({
  disabled: "Off",
  starting: "Starting",
  ready: "Ready for audio",
  buffering: "Buffering",
  receiving: "Receiving audio",
  routing: "Connecting speakers",
  playing: "Playing",
  waiting_for_targets: "Choose speakers",
  dependency_missing: "Shairport Sync needed",
  runtime_unavailable: "Runtime unavailable",
  stopped: "Stopped",
  error: "Needs attention",
} as Record<string, string>)[statusToken.value] || statusToken.value.replaceAll("_", " "));
const statusTone = computed(() => ["ready", "buffering", "receiving", "routing", "playing"].includes(statusToken.value) ? "good" : ["error", "dependency_missing", "runtime_unavailable"].includes(statusToken.value) ? "bad" : "quiet");
const inputActive = computed(() => Boolean(status.value.input_active));
const receiverError = computed(() => String(status.value.route_error || status.value.receiver_error || ""));
const receiverDetail = computed(() => {
  if (receiverError.value) return receiverError.value;
  if (inputActive.value) return "Audio is arriving from an Apple device and being routed to the selected speakers.";
  if (draft.enabled && draft.targets.length) return "Open the AirPlay speaker picker on an Apple device and choose this receiver.";
  return "Enable the receiver and choose where incoming audio should play.";
});

function hydrate() {
  if (dirty.value) return;
  draft.enabled = Boolean(settings.value.enabled);
  draft.receiver_name = String(settings.value.receiver_name || "Tater Audio");
  draft.receiver_pin = String(settings.value.receiver_pin || "");
  draft.targets = Array.isArray(settings.value.targets) ? settings.value.targets.map(String) : [];
}

function markDirty() {
  dirty.value = true;
  error.value = "";
}

function toggleTarget(value: unknown) {
  const token = String(value || "");
  if (!token || busy.value) return;
  const next = new Set(draft.targets);
  if (next.has(token)) next.delete(token);
  else next.add(token);
  draft.targets = Array.from(next);
  markDirty();
}

function kindLabel(option: JsonRow) {
  return ({ satellite: "Satellite", stereo: "Stereo pair", sonos: "Sonos + AirPlay", airplay: "AirPlay speaker" } as Record<string, string>)[String(option.kind || "")] || "Speaker";
}

function kindMark(option: JsonRow) {
  return ({ satellite: "SAT", stereo: "2X", sonos: "SO", airplay: "AP" } as Record<string, string>)[String(option.kind || "")] || "SPK";
}

async function save() {
  if (busy.value) return;
  busy.value = "save";
  error.value = "";
  try {
    const result = await voiceAction(props.actionEndpoint, "voice_airplay_input_save", {
      values: {
        enabled: draft.enabled,
        receiver_name: draft.receiver_name,
        receiver_pin: draft.receiver_pin,
        targets: [...draft.targets],
      },
    });
    dirty.value = false;
    emit("notify", String(result.message || "AirPlay Input settings saved."), "success");
    emit("refresh");
  } catch (saveError) {
    error.value = saveError instanceof Error ? saveError.message : "AirPlay Input settings could not be saved.";
    emit("notify", error.value, "error");
  } finally {
    busy.value = "";
  }
}

async function stop() {
  if (busy.value) return;
  busy.value = "stop";
  error.value = "";
  try {
    const result = await voiceAction(props.actionEndpoint, "voice_airplay_input_stop");
    emit("notify", String(result.message || "AirPlay input stopped."), "success");
    emit("refresh");
  } catch (stopError) {
    error.value = stopError instanceof Error ? stopError.message : "AirPlay input could not be stopped.";
    emit("notify", error.value, "error");
  } finally {
    busy.value = "";
  }
}

watch(() => props.payload, hydrate, { deep: true, immediate: true });
</script>

<template>
  <section class="tm-stack tvoice-airplay">
    <div v-if="error" class="tv-notice error" aria-live="polite">{{ error }}</div>

    <section class="tm-form-card tvoice-airplay-hero">
      <div class="tvoice-airplay-flow" aria-hidden="true">
        <span class="source">AP</span><i /><span class="receiver">T</span><i /><span class="speaker">SAT</span>
      </div>
      <div class="tvoice-airplay-copy">
        <span class="tv-eyebrow">Built into Tater</span>
        <h3>AirPlay into your satellites</h3>
        <p>Send audio from an iPhone, iPad, or Mac to one Tater receiver, then play it across the satellites and speaker groups you choose.</p>
      </div>
      <span class="tvoice-airplay-status" :class="statusTone"><i />{{ statusLabel }}</span>
    </section>

    <section class="tvoice-airplay-summary">
      <article><span>Receiver</span><strong>{{ draft.receiver_name || "Tater Audio" }}</strong><small>Shown in Apple’s speaker picker</small></article>
      <article><span>Destinations</span><strong>{{ draft.targets.length }}</strong><small>{{ draft.targets.length === 1 ? "Speaker selected" : "Speakers selected" }}</small></article>
      <article><span>Input</span><strong>{{ inputActive ? "Live" : "Waiting" }}</strong><small>{{ inputActive ? "Audio is arriving now" : "No active stream" }}</small></article>
    </section>

    <section class="tm-form-card tvoice-airplay-control">
      <header><div><span class="tv-eyebrow">Receiver availability</span><h3>Make Tater discoverable</h3><p>{{ receiverDetail }}</p></div><span class="tvoice-section-mark">ON</span></header>
      <label class="tv-toggle tvoice-airplay-toggle">
        <input v-model="draft.enabled" class="tv-checkbox" type="checkbox" :disabled="Boolean(busy)" @change="markDirty">
        <span><strong>Enable AirPlay Input</strong><small>Advertise this Tater server as an AirPlay audio destination.</small></span>
      </label>
      <div v-if="receiverError" class="tv-notice error">{{ receiverError }}</div>
    </section>

    <section class="tm-form-card tvoice-airplay-identity">
      <header><div><span class="tv-eyebrow">How Apple devices see Tater</span><h3>Receiver identity</h3><p>Give the receiver a recognizable room or household name. A PIN is optional.</p></div><span class="tvoice-section-mark">ID</span></header>
      <div class="tvoice-airplay-fields">
        <label><span>Receiver name</span><input v-model="draft.receiver_name" type="text" maxlength="80" placeholder="Tater Audio" :disabled="Boolean(busy)" @input="markDirty"><small>For example: Tater Audio or Whole Home Tater.</small></label>
        <label><span>Pairing PIN <em>Optional</em></span><input v-model="draft.receiver_pin" type="password" maxlength="4" inputmode="numeric" pattern="[0-9]*" placeholder="Four digits" :disabled="Boolean(busy)" @input="markDirty"><small>Leave blank for normal AirPlay pairing.</small></label>
      </div>
    </section>

    <section class="tm-form-card tvoice-airplay-destinations">
      <header><div><span class="tv-eyebrow">Playback routing</span><h3>Choose where AirPlay plays</h3><p>Select one or more destinations. Tater handles the live stream and synchronized routing.</p></div><span class="tvoice-section-mark">{{ draft.targets.length }}</span></header>
      <div v-if="options.length" class="tvoice-airplay-grid">
        <button v-for="option in options" :key="String(option.value)" type="button" :class="{ selected: draft.targets.includes(String(option.value)), offline: option.available === false }" :disabled="Boolean(busy)" @click="toggleTarget(option.value)">
          <span class="tvoice-airplay-target-mark">{{ kindMark(option) }}</span>
          <span class="tvoice-airplay-target-copy"><small>{{ kindLabel(option) }}</small><strong>{{ option.label }}</strong><em>{{ option.description }}</em></span>
          <span class="tvoice-airplay-check">{{ draft.targets.includes(String(option.value)) ? "✓" : "+" }}</span>
        </button>
      </div>
      <div v-else class="tv-notice">No compatible destinations are available yet. Pair a satellite or create a stereo pair first.</div>
    </section>

    <footer class="tm-form-card tvoice-save-bar tvoice-airplay-save">
      <div><strong>{{ dirty ? "AirPlay changes are ready to save" : "AirPlay Input is up to date" }}</strong><small>Settings apply immediately and remain available without Music Core.</small></div>
      <div class="tm-inline-actions">
        <button v-if="inputActive" class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="stop">{{ busy === "stop" ? "Stopping…" : "Stop Current Input" }}</button>
        <button class="tv-button primary" type="button" :disabled="Boolean(busy) || !dirty" @click="save">{{ busy === "save" ? "Saving…" : "Save AirPlay Input" }}</button>
      </div>
    </footer>
  </section>
</template>
