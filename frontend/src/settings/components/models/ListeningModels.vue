<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { getJson } from "../../../shared/api";
import type { JsonRow } from "../../types";
import ModelFields from "./ModelFields.vue";

const props = defineProps<{
  draft: JsonRow;
  speechUi: JsonRow;
  voiceModelUi: JsonRow;
  warmupEndpoint: string;
}>();
const emit = defineEmits<{ dirty: [] }>();

const warmup = ref<JsonRow>({});
let warmupTimer: number | null = null;

const sttMeta: Record<string, { mark: string; short: string }> = {
  faster_whisper: { mark: "FW", short: "Fast, accurate local Whisper transcription." },
  parakeet_onnx: { mark: "PK", short: "Efficient local speech recognition with ONNX." },
  qwen3_asr_llama_cpp: { mark: "Q3", short: "Experimental local Qwen3-ASR through llama.cpp." },
  wyoming: { mark: "WY", short: "Use a Wyoming speech service on your network." },
  vosk: { mark: "VK", short: "Lightweight offline recognition for modest hardware." },
};

const voiceSections = computed<JsonRow[]>(() => Array.isArray(props.voiceModelUi.sections) ? props.voiceModelUi.sections : []);
const sttOptions = computed<JsonRow[]>(() => Array.isArray(props.speechUi.stt_backend_options) ? props.speechUi.stt_backend_options : []);
const accelerationOptions = computed<JsonRow[]>(() => Array.isArray(props.speechUi.acceleration_options) ? props.speechUi.acceleration_options : []);
const warmupItems = computed<JsonRow[]>(() => Array.isArray(warmup.value.items) ? warmup.value.items : []);
const warmupHasErrors = computed(() => warmupItems.value.some((item) => String(item.status || "").toLowerCase() === "error") || (Array.isArray(warmup.value.errors) && warmup.value.errors.length > 0));
const voiceValues = computed<JsonRow>(() => {
  if (!props.draft.esphome_settings || typeof props.draft.esphome_settings !== "object") props.draft.esphome_settings = {};
  return props.draft.esphome_settings as JsonRow;
});
const selectedStt = computed(() => String(props.draft.speech_stt_backend || "faster_whisper"));
const selectedSttLabel = computed(() => labelFor(sttOptions.value, selectedStt.value, "Speech recognition"));
const selectedAccelerationLabel = computed(() => labelFor(accelerationOptions.value, props.draft.speech_acceleration, "Auto acceleration"));
const localStt = computed(() => selectedStt.value !== "wyoming");
const visibleVoiceSections = computed(() => voiceSections.value.filter((section) => {
  const label = String(section.label || "").toLowerCase();
  return !label.includes("faster whisper") || selectedStt.value === "faster_whisper";
}));

function optionValue(option: JsonRow) { return String(option.value ?? option.id ?? ""); }
function optionLabel(option: JsonRow) { return String(option.label ?? option.name ?? option.value ?? option.id ?? ""); }
function labelFor(options: JsonRow[], value: unknown, fallback: string) { return optionLabel(options.find((option) => optionValue(option) === String(value || "")) || {}) || fallback; }
function sttMark(option: JsonRow) { return sttMeta[optionValue(option)]?.mark || optionLabel(option).slice(0, 2).toUpperCase(); }
function sttDescription(option: JsonRow) { return sttMeta[optionValue(option)]?.short || "Speech recognition provider."; }
function dirty() { emit("dirty"); }
function updateVoice(key: string, value: unknown) { voiceValues.value[key] = value; dirty(); }
function setStt(value: string) { props.draft.speech_stt_backend = value; dirty(); }
function setAcceleration(value: string) { props.draft.speech_acceleration = value; dirty(); }

async function refreshWarmup() {
  try {
    warmup.value = await getJson<JsonRow>(props.warmupEndpoint);
    if (warmup.value.running) scheduleWarmup();
  } catch { /* listening settings remain usable when warmup telemetry is unavailable */ }
}

function scheduleWarmup() {
  if (warmupTimer !== null) window.clearTimeout(warmupTimer);
  warmupTimer = window.setTimeout(refreshWarmup, 1500);
}

onMounted(() => { void refreshWarmup(); });
onBeforeUnmount(() => {
  if (warmupTimer !== null) window.clearTimeout(warmupTimer);
});

defineExpose({ refreshWarmup });
</script>

<template>
  <section class="tm-stack tm-speech-workspace tm-listening-workspace">
    <article class="tm-form-card tm-speech-hero">
      <header>
        <div><span class="tv-eyebrow">Hear clearly</span><h3>Listening and speech recognition</h3><p>Choose how Tater turns speech into text, then tune only the listening runtime that is actually in use.</p></div>
        <div class="tm-speech-hero-status"><span><i />{{ selectedSttLabel }}</span><b>{{ localStt ? selectedAccelerationLabel : "Network service" }}</b></div>
      </header>
    </article>

    <article v-if="warmup.running || warmupItems.length" class="tm-form-card tm-speech-warmup" :class="{ complete: !warmup.running && !warmupHasErrors, error: warmupHasErrors }">
      <header><div><span class="tv-eyebrow">Listening runtime</span><h3>{{ warmup.running ? "Preparing listening models" : warmupHasErrors ? "Listening model needs attention" : "Listening models are ready" }}</h3><p>{{ warmup.running ? "Tater is loading the selected local speech-recognition engine in the background." : warmupHasErrors ? "One or more listening models could not be prepared." : "The latest listening-model preparation finished successfully." }}</p></div><span class="tm-speech-status-chip">{{ warmup.running ? "Working" : warmupHasErrors ? "Check status" : "Ready" }}</span></header>
      <div v-if="warmup.running" class="tm-progress-list"><div v-for="item in warmupItems" :key="String(item.key || item.model)" class="tm-progress-row"><div><strong>{{ item.label || item.model || item.backend }}</strong><span>{{ item.message || item.status }}</span></div><progress :value="Number(item.progress || 0)" max="100" /></div></div>
      <div v-else class="tm-speech-ready"><i>{{ warmupHasErrors ? "!" : "✓" }}</i><span>{{ warmupItems.length }} listening {{ warmupItems.length === 1 ? "item" : "items" }} checked</span></div>
    </article>

    <article class="tm-form-card tm-speech-picker-card">
      <header><div><span class="tv-eyebrow">Step 1</span><h3>Choose how Tater listens</h3><p>Only settings for the selected speech-recognition engine appear below.</p></div><span class="tm-speech-status-chip">{{ selectedSttLabel }}</span></header>
      <div class="tm-speech-provider-grid" role="group" aria-label="Speech recognition backend">
        <button v-for="option in sttOptions" :key="optionValue(option)" type="button" :class="{ active: selectedStt === optionValue(option) }" :aria-pressed="selectedStt === optionValue(option)" @click="setStt(optionValue(option))"><i>{{ sttMark(option) }}</i><span><strong>{{ optionLabel(option) }}</strong><small>{{ sttDescription(option) }}</small></span><b aria-hidden="true">✓</b></button>
      </div>
    </article>

    <article class="tm-form-card tm-speech-runtime-card">
      <header><div><span class="tv-eyebrow">Step 2</span><h3>{{ localStt ? "Choose the processing hardware" : "Connect the Wyoming service" }}</h3><p>{{ localStt ? "Pick Auto unless you know which accelerator should run speech recognition." : "Enter the host and port of the Wyoming speech-to-text service." }}</p></div><span class="tm-speech-status-chip">{{ localStt ? selectedAccelerationLabel : "Network" }}</span></header>
      <div v-if="localStt" class="tm-speech-acceleration" role="group" aria-label="Speech acceleration">
        <button v-for="option in accelerationOptions" :key="optionValue(option)" type="button" :class="{ active: String(draft.speech_acceleration || 'auto') === optionValue(option) }" @click="setAcceleration(optionValue(option))"><i />{{ optionLabel(option) }}</button>
      </div>
      <div v-else class="tm-field-grid tm-speech-connection-fields">
        <label class="tm-field tm-field-wide"><span class="tm-field-label">Wyoming host</span><input v-model="draft.speech_wyoming_stt_host" type="text" placeholder="127.0.0.1" @input="dirty" /><small>Hostname or IP address of your Wyoming STT service.</small></label>
        <label class="tm-field"><span class="tm-field-label">Wyoming port</span><input v-model="draft.speech_wyoming_stt_port" type="number" min="1" max="65535" placeholder="10300" @input="dirty" /></label>
      </div>
    </article>

    <section v-if="visibleVoiceSections.length" class="tm-speech-runtime-settings"><div class="tm-speech-section-heading"><div><span class="tv-eyebrow">Fine tuning</span><h3>Listening controls</h3><p>These controls apply to the selected recognition path and shared listening runtime.</p></div><span>{{ visibleVoiceSections.length }} {{ visibleVoiceSections.length === 1 ? "section" : "sections" }}</span></div><ModelFields :sections="visibleVoiceSections" :values="voiceValues" @change="updateVoice" /></section>
  </section>
</template>
