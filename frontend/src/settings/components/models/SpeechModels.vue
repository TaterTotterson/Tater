<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { getJson, responseJson } from "../../../shared/api";
import type { JsonRow } from "../../types";
import TtsProfile from "./TtsProfile.vue";

const props = defineProps<{
  draft: JsonRow;
  speechUi: JsonRow;
  announcementUi: JsonRow;
  endpoints: {
    modelsSpeechPreview: string;
    modelsWyomingVoices: string;
    modelsOpenAiVoices: string;
    modelsOpenAiModels: string;
    modelsSpeechWarmup: string;
    modelsCloneAudio: string;
  };
}>();
const emit = defineEmits<{ dirty: []; notify: [message: string, tone?: string] }>();

const area = ref<"replies" | "announcements" | "playback">("replies");
const warmup = ref<JsonRow>({});
const previewText = ref("Hello from Tater. This is a voice preview.");
const previewScope = ref<"direct" | "announcement">("direct");
const previewBusy = ref(false);
const previewUrl = ref("");
const previewAudio = ref<HTMLAudioElement | null>(null);
const error = ref("");
let warmupTimer: number | null = null;

const warmupItems = computed<JsonRow[]>(() => Array.isArray(warmup.value.items) ? warmup.value.items : []);
const warmupHasErrors = computed(() => warmupItems.value.some((item) => String(item.status || "").toLowerCase() === "error") || (Array.isArray(warmup.value.errors) && warmup.value.errors.length > 0));
const directVoiceLabel = computed(() => labelFor(
  Array.isArray(props.speechUi.tts_backend_options) ? props.speechUi.tts_backend_options as JsonRow[] : [],
  props.draft.speech_tts_backend,
  "Reply voice",
));
const announcementVoiceLabel = computed(() => {
  const backend = String(props.draft.speech_announcement_tts_backend || "same_as_direct");
  if (["same_as_direct", "direct"].includes(backend)) return `Uses ${directVoiceLabel.value}`;
  return labelFor(
    Array.isArray(props.announcementUi.tts_backend_options) ? props.announcementUi.tts_backend_options as JsonRow[] : [],
    backend,
    "Announcement voice",
  );
});
const previewBackendLabel = computed(() => previewScope.value === "direct" ? directVoiceLabel.value : announcementVoiceLabel.value);
const areaIntro = computed(() => {
  if (area.value === "replies") return { eyebrow: "Tater's voice", title: "Conversational reply voice", description: "Choose the voice engine Tater uses when answering people directly.", status: directVoiceLabel.value, badge: "Direct replies" };
  if (area.value === "announcements") return { eyebrow: "Whole-home voice", title: "Announcements and proactive speech", description: "Reuse the reply voice or give announcements their own sound, then control satellite ducking.", status: announcementVoiceLabel.value, badge: "Announcements" };
  return { eyebrow: "Sound check", title: "Preview before you save", description: "Generate a sample from the settings currently on screen, including unsaved changes.", status: previewBackendLabel.value, badge: "Live preview" };
});

function optionValue(option: JsonRow) { return String(option.value ?? option.id ?? ""); }
function optionLabel(option: JsonRow) { return String(option.label ?? option.name ?? option.value ?? option.id ?? ""); }
function labelFor(options: JsonRow[], value: unknown, fallback: string) { return optionLabel(options.find((option) => optionValue(option) === String(value || "")) || {}) || fallback; }
function dirty() { error.value = ""; emit("dirty"); }

async function refreshWarmup() {
  try {
    warmup.value = await getJson<JsonRow>(props.endpoints.modelsSpeechWarmup);
    if (warmup.value.running) scheduleWarmup();
  } catch { /* settings remain usable when warmup telemetry is unavailable */ }
}

function scheduleWarmup() {
  if (warmupTimer !== null) window.clearTimeout(warmupTimer);
  warmupTimer = window.setTimeout(refreshWarmup, 1500);
}

function previewPayload(): JsonRow {
  const announcement = previewScope.value === "announcement";
  const requestedPrefix = announcement ? "speech_announcement_" : "speech_";
  let backend = String(props.draft[`${requestedPrefix}tts_backend`] || "");
  const inheritsReplyVoice = announcement && ["same_as_direct", "direct"].includes(backend);
  if (inheritsReplyVoice) backend = String(props.draft.speech_tts_backend || "wyoming");
  const prefix = inheritsReplyVoice ? "speech_" : requestedPrefix;
  const managed = backend === "qwen3_tts" ? "qwen_tts" : backend === "omnivoice" ? "omnivoice_tts" : "";
  return {
    text: previewText.value,
    backend,
    model: props.draft[`${prefix}tts_model`] || props.draft.speech_tts_model || "",
    voice: props.draft[`${prefix}tts_voice`] || props.draft.speech_tts_voice || "",
    acceleration: props.draft.speech_acceleration || "auto",
    wyoming_host: props.draft[`${prefix}wyoming_tts_host`] || props.draft.speech_wyoming_tts_host || "",
    wyoming_port: props.draft[`${prefix}wyoming_tts_port`] || props.draft.speech_wyoming_tts_port || "",
    wyoming_voice: props.draft[`${prefix}wyoming_tts_voice`] || props.draft.speech_wyoming_tts_voice || "",
    openai_base_url: props.draft[`${prefix}openai_tts_base_url`] || props.draft.speech_openai_tts_base_url || "",
    openai_api_key: props.draft[`${prefix}openai_tts_api_key`] || props.draft.speech_openai_tts_api_key || "",
    pocket_tts_output_gain: props.draft[`${prefix}pocket_tts_output_gain`] || props.draft.speech_pocket_tts_output_gain || null,
    clone_text: managed ? props.draft[`${prefix}tts_clone_text`] || "" : "",
    managed_language: managed ? props.draft[`${prefix}${managed}_language`] || "" : "",
    managed_instruct: managed ? props.draft[`${prefix}${managed}_instruct`] || "" : "",
  };
}

async function preview() {
  previewBusy.value = true;
  error.value = "";
  try {
    const response = await fetch(props.endpoints.modelsSpeechPreview, { method: "POST", credentials: "same-origin", headers: { Accept: "audio/wav", "Content-Type": "application/json" }, body: JSON.stringify(previewPayload()) });
    if (!response.ok) await responseJson(response);
    const blob = await response.blob();
    if (previewUrl.value) URL.revokeObjectURL(previewUrl.value);
    previewUrl.value = URL.createObjectURL(blob);
    await new Promise((resolve) => window.setTimeout(resolve, 0));
    await previewAudio.value?.play().catch(() => {});
  } catch (previewError) {
    error.value = previewError instanceof Error ? previewError.message : "Voice preview failed.";
    emit("notify", error.value, "error");
  } finally { previewBusy.value = false; }
}

onMounted(() => { void refreshWarmup(); });
onBeforeUnmount(() => {
  if (warmupTimer !== null) window.clearTimeout(warmupTimer);
  if (previewUrl.value) URL.revokeObjectURL(previewUrl.value);
});

defineExpose({ refreshWarmup });
</script>

<template>
  <section class="tm-stack tm-speech-workspace">
    <nav class="tv-tabs tm-inner-tabs tm-speech-tabs" aria-label="Speech settings areas">
      <button type="button" :class="{ active: area === 'replies' }" @click="area = 'replies'">Reply voice</button>
      <button type="button" :class="{ active: area === 'announcements' }" @click="area = 'announcements'">Announcements</button>
      <button type="button" :class="{ active: area === 'playback' }" @click="area = 'playback'">Playback & test</button>
    </nav>
    <div v-if="error" class="tv-notice error">{{ error }}</div>

    <article class="tm-form-card tm-speech-hero">
      <header>
        <div><span class="tv-eyebrow">{{ areaIntro.eyebrow }}</span><h3>{{ areaIntro.title }}</h3><p>{{ areaIntro.description }}</p></div>
        <div class="tm-speech-hero-status"><span><i />{{ areaIntro.status }}</span><b>{{ areaIntro.badge }}</b></div>
      </header>
    </article>

    <article v-if="warmup.running || warmupItems.length" class="tm-form-card tm-speech-warmup" :class="{ complete: !warmup.running && !warmupHasErrors, error: warmupHasErrors }">
      <header><div><span class="tv-eyebrow">Voice runtime</span><h3>{{ warmup.running ? "Preparing voice models" : warmupHasErrors ? "Voice model needs attention" : "Voice models are ready" }}</h3><p>{{ warmup.running ? "Tater is loading the selected local speech engines in the background." : warmupHasErrors ? "One or more voice models could not be prepared." : "The latest voice-model preparation finished successfully." }}</p></div><span class="tm-speech-status-chip">{{ warmup.running ? "Working" : warmupHasErrors ? "Check status" : "Ready" }}</span></header>
      <div v-if="warmup.running" class="tm-progress-list"><div v-for="item in warmupItems" :key="String(item.key || item.model)" class="tm-progress-row"><div><strong>{{ item.label || item.model || item.backend }}</strong><span>{{ item.message || item.status }}</span></div><progress :value="Number(item.progress || 0)" max="100" /></div></div>
      <div v-else class="tm-speech-ready"><i>{{ warmupHasErrors ? "!" : "✓" }}</i><span>{{ warmupItems.length }} voice {{ warmupItems.length === 1 ? "item" : "items" }} checked</span></div>
    </article>

    <TtsProfile v-if="area === 'replies'" scope="direct" :draft="draft" :ui="speechUi" :endpoints="endpoints" @dirty="dirty" @notify="(message, tone) => emit('notify', message, tone)" />

    <template v-else-if="area === 'announcements'">
      <TtsProfile scope="announcement" :draft="draft" :ui="announcementUi" :endpoints="endpoints" @dirty="dirty" @notify="(message, tone) => emit('notify', message, tone)" />
      <article class="tm-form-card tm-speech-duck-card"><header><div><span class="tv-eyebrow">Audio focus</span><h3>Lower other audio during announcements</h3><p>Fade satellite playback down before Tater speaks, then restore it smoothly.</p></div><span class="tm-speech-status-chip">{{ Number(draft.speech_satellite_ducking_target_percent || 0) }}% target</span></header><div class="tm-field-grid">
        <label class="tm-field tm-field-wide"><span class="tm-field-label">Background audio target</span><div class="tm-speech-range-field"><input v-model="draft.speech_satellite_ducking_target_percent" type="range" min="0" max="100" step="1" @input="dirty" /><input v-model="draft.speech_satellite_ducking_target_percent" type="number" min="0" max="100" @input="dirty" /></div><small>0% silences other audio; 100% leaves it unchanged.</small></label>
        <label class="tm-field"><span class="tm-field-label">Fade down time</span><input v-model="draft.speech_satellite_ducking_attack_ms" type="number" min="0" step="10" @input="dirty" /><small>Milliseconds before the announcement begins.</small></label>
        <label class="tm-field"><span class="tm-field-label">Restore time</span><input v-model="draft.speech_satellite_ducking_release_ms" type="number" min="0" step="10" @input="dirty" /><small>Milliseconds to return to the previous volume.</small></label>
      </div></article>
    </template>

    <article v-else class="tm-form-card tm-preview-card tm-speech-preview-card">
      <header><div><span class="tv-eyebrow">Try it now</span><h3>Voice preview</h3><p>The preview uses the values currently on screen, even before you save them.</p></div><span class="tm-speech-status-chip">{{ previewBackendLabel }}</span></header>
      <div class="tm-speech-profile-switch" role="group" aria-label="Voice profile to preview"><button type="button" :class="{ active: previewScope === 'direct' }" @click="previewScope = 'direct'"><i>↗</i><span><strong>Reply voice</strong><small>Normal conversation</small></span></button><button type="button" :class="{ active: previewScope === 'announcement' }" @click="previewScope = 'announcement'"><i>⌂</i><span><strong>Announcement voice</strong><small>Proactive and whole-home speech</small></span></button></div>
      <div class="tm-field-grid">
        <label class="tm-field tm-field-wide"><span class="tm-field-label">What should Tater say?</span><textarea v-model="previewText" placeholder="Type a short sentence to preview." /></label>
      </div>
      <div class="tm-speech-preview-actions"><button class="tv-button primary" type="button" :disabled="previewBusy || !previewText.trim()" @click="preview">{{ previewBusy ? "Generating voice…" : "Generate & play preview" }}</button><div v-if="previewUrl" class="tm-speech-audio-player"><span>Latest preview</span><audio ref="previewAudio" :src="previewUrl" controls /></div></div>
    </article>
  </section>
</template>
