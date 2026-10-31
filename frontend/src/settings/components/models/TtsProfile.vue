<script setup lang="ts">
import { computed, ref } from "vue";
import { postJson, responseJson } from "../../../shared/api";
import type { JsonRow } from "../../types";

const props = defineProps<{
  scope: "direct" | "announcement";
  draft: JsonRow;
  ui: JsonRow;
  endpoints: {
    modelsWyomingVoices: string;
    modelsOpenAiVoices: string;
    modelsOpenAiModels: string;
    modelsCloneAudio: string;
  };
}>();
const emit = defineEmits<{ dirty: []; notify: [message: string, tone?: string] }>();

const busy = ref("");
const error = ref("");
const discoveredVoices = ref<JsonRow[]>([]);
const discoveredModels = ref<JsonRow[]>([]);

const backendMeta: Record<string, { mark: string; short: string; kind: string }> = {
  same_as_direct: { mark: "↔", short: "Use the complete Reply Voice profile.", kind: "Shared" },
  wyoming: { mark: "WY", short: "Connect to a Wyoming voice service.", kind: "Network" },
  openai_compatible: { mark: "API", short: "Use an OpenAI-style speech endpoint.", kind: "API" },
  pocket_tts: { mark: "PT", short: "Lightweight CPU voice cloning from your shared sample.", kind: "Local clone" },
  qwen3_tts: { mark: "Q3", short: "Local multilingual speech and voice cloning.", kind: "Local clone" },
  omnivoice: { mark: "OM", short: "Local natural speech and voice cloning.", kind: "Local clone" },
};

const announcement = computed(() => props.scope === "announcement");
const keyPrefix = computed(() => announcement.value ? "speech_announcement_" : "speech_");
const backendKey = computed(() => `${keyPrefix.value}tts_backend`);
const modelKey = computed(() => `${keyPrefix.value}tts_model`);
const voiceKey = computed(() => `${keyPrefix.value}tts_voice`);
const backend = computed(() => String(props.draft[backendKey.value] || (announcement.value ? "same_as_direct" : "wyoming")));
const inheritsDirect = computed(() => announcement.value && ["same_as_direct", "direct"].includes(backend.value));
const effectiveBackend = computed(() => inheritsDirect.value ? String(props.draft.speech_tts_backend || "wyoming") : backend.value);
const modelOptions = computed<JsonRow[]>(() => {
  if (discoveredModels.value.length) return discoveredModels.value;
  const byBackend = props.ui.tts_model_options_by_backend && typeof props.ui.tts_model_options_by_backend === "object" ? props.ui.tts_model_options_by_backend as JsonRow : {};
  return Array.isArray(byBackend[effectiveBackend.value]) ? byBackend[effectiveBackend.value] : [];
});
const languageOptions = computed<JsonRow[]>(() => {
  const byBackend = props.ui.tts_language_options_by_backend && typeof props.ui.tts_language_options_by_backend === "object" ? props.ui.tts_language_options_by_backend as JsonRow : {};
  return Array.isArray(byBackend[effectiveBackend.value]) ? byBackend[effectiveBackend.value] : [];
});
const voiceOptions = computed<JsonRow[]>(() => {
  if (discoveredVoices.value.length) return discoveredVoices.value;
  const byModel = props.ui.tts_voice_options_by_model && typeof props.ui.tts_voice_options_by_model === "object" ? props.ui.tts_voice_options_by_model as JsonRow : {};
  return Array.isArray(byModel[String(props.draft[modelKey.value] || "")]) ? byModel[String(props.draft[modelKey.value] || "")] : [];
});
const backendOptions = computed<JsonRow[]>(() => Array.isArray(props.ui.tts_backend_options) ? props.ui.tts_backend_options : []);
const activeBackendMeta = computed(() => detailsFor(backend.value));
const directBackendLabel = computed(() => detailsFor(String(props.draft.speech_tts_backend || "wyoming")).label);
const canDiscover = computed(() => !inheritsDirect.value && ["wyoming", "openai_compatible"].includes(effectiveBackend.value));
const needsVoice = computed(() => voiceOptions.value.length > 0 || ["wyoming", "openai_compatible"].includes(effectiveBackend.value));
const cloneCapable = computed(() => ["pocket_tts", "qwen3_tts", "omnivoice"].includes(effectiveBackend.value));
const managed = computed(() => ["qwen3_tts", "omnivoice"].includes(effectiveBackend.value));
const showModelVoicePanel = computed(() => ["wyoming", "openai_compatible", "qwen3_tts"].includes(effectiveBackend.value));
const managedToken = computed(() => effectiveBackend.value === "qwen3_tts" ? "qwen_tts" : "omnivoice_tts");
const managedBase = computed(() => `${keyPrefix.value}${managedToken.value}`);
const cloneInfoKey = computed(() => `${keyPrefix.value}tts_clone_audio`);
const cloneAudioUrlKey = computed(() => `${keyPrefix.value}tts_clone_audio_url`);
const cloneTextKey = computed(() => `${keyPrefix.value}tts_clone_text`);
const languageKey = computed(() => `${managedBase.value}_language`);
const instructKey = computed(() => `${managedBase.value}_instruct`);
const cloneInfo = computed<JsonRow>(() => props.draft[cloneInfoKey.value] && typeof props.draft[cloneInfoKey.value] === "object" ? props.draft[cloneInfoKey.value] as JsonRow : {});
const cloneAudioUrl = computed(() => String(props.draft[cloneAudioUrlKey.value] || ""));

function key(suffix: string) { return `${keyPrefix.value}${suffix}`; }
function changed() { discoveredModels.value = []; discoveredVoices.value = []; error.value = ""; emit("dirty"); }
function optionValue(option: JsonRow) { return String(option.value ?? option.id ?? option.model ?? option.name ?? ""); }
function optionLabel(option: JsonRow) { return String(option.label ?? option.name ?? option.value ?? option.id ?? ""); }
function hasOption(options: JsonRow[], value: unknown) { return options.some((option) => optionValue(option) === String(value ?? "")); }
function detailsFor(value: string) {
  const token = String(value || "");
  const option = backendOptions.value.find((row) => optionValue(row) === token);
  const fallback = backendMeta[token] || { mark: token.slice(0, 2).toUpperCase(), short: "Speech synthesis provider.", kind: "Voice" };
  return { ...fallback, label: optionLabel(option || {}) || token || "Voice provider" };
}
function selectBackend(value: string) {
  if (backend.value === value) return;
  props.draft[backendKey.value] = value;
  if (["pocket_tts", "qwen3_tts", "omnivoice"].includes(value)) props.draft[voiceKey.value] = "";
  const byBackend = props.ui.tts_model_options_by_backend && typeof props.ui.tts_model_options_by_backend === "object" ? props.ui.tts_model_options_by_backend as JsonRow : {};
  const options = Array.isArray(byBackend[value]) ? byBackend[value] as JsonRow[] : [];
  if (options.length) props.draft[modelKey.value] = optionValue(options[0]);
  changed();
}

function readableSize(value: unknown) {
  const bytes = Number(value || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) return "";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

async function discover() {
  busy.value = "discover";
  error.value = "";
  try {
    let result: JsonRow = {};
    if (effectiveBackend.value === "wyoming") {
      result = await postJson<JsonRow>(props.endpoints.modelsWyomingVoices, { host: props.draft[key("wyoming_tts_host")], port: props.draft[key("wyoming_tts_port")], current_voice: props.draft[voiceKey.value] });
    } else if (effectiveBackend.value === "openai_compatible") {
      const body = { base_url: props.draft[key("openai_tts_base_url")], api_key: props.draft[key("openai_tts_api_key")] };
      const [models, voices] = await Promise.all([postJson<JsonRow>(props.endpoints.modelsOpenAiModels, body), postJson<JsonRow>(props.endpoints.modelsOpenAiVoices, body)]);
      result = voices;
      discoveredModels.value = normalizeOptions(models.models || models.options || []);
    }
    discoveredVoices.value = normalizeOptions(result.voices || result.options || []);
    emit("notify", `Loaded ${discoveredVoices.value.length} voice option(s).`, "success");
  } catch (discoverError) {
    error.value = discoverError instanceof Error ? discoverError.message : "Voice options could not be loaded.";
  } finally {
    busy.value = "";
  }
}

function normalizeOptions(value: unknown): JsonRow[] {
  return (Array.isArray(value) ? value : []).map((item) => typeof item === "object" && item ? item as JsonRow : { value: String(item), label: String(item) });
}

async function uploadClone(event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  if (!file || !cloneCapable.value) return;
  busy.value = "upload";
  error.value = "";
  try {
    const endpoint = `${props.endpoints.modelsCloneAudio}?scope=${props.scope}`;
    const result = await responseJson<JsonRow>(await fetch(endpoint, { method: "POST", credentials: "same-origin", headers: { Accept: "application/json", "X-Filename": file.name, "Content-Type": file.type || "application/octet-stream" }, body: file }));
    props.draft[cloneInfoKey.value] = result.audio || { configured: true, name: file.name, size: file.size };
    if (result.clone_text) props.draft[cloneTextKey.value] = result.clone_text;
    const sampleUrl = String(result.url || `${props.endpoints.modelsCloneAudio}?scope=${props.scope}`);
    props.draft[cloneAudioUrlKey.value] = `${sampleUrl}${sampleUrl.includes("?") ? "&" : "?"}v=${Date.now()}`;
    emit("dirty");
    emit("notify", "Reference audio uploaded and analyzed.", "success");
  } catch (uploadError) {
    error.value = uploadError instanceof Error ? uploadError.message : "Reference audio could not be uploaded.";
  } finally {
    busy.value = "";
    input.value = "";
  }
}

async function deleteClone() {
  if (!cloneCapable.value || !window.confirm("Remove this shared reference voice recording?")) return;
  busy.value = "delete";
  try {
    const endpoint = `${props.endpoints.modelsCloneAudio}?scope=${props.scope}`;
    const result = await responseJson<JsonRow>(await fetch(endpoint, { method: "DELETE", credentials: "same-origin", headers: { Accept: "application/json" } }));
    props.draft[cloneInfoKey.value] = result.audio || {};
    props.draft[cloneAudioUrlKey.value] = "";
    props.draft[cloneTextKey.value] = "";
    emit("dirty");
  } catch (deleteError) {
    error.value = deleteError instanceof Error ? deleteError.message : "Reference audio could not be removed.";
  } finally { busy.value = ""; }
}
</script>

<template>
  <section class="tm-stack tm-tts-profile">
    <article class="tm-form-card tm-tts-engine-card">
      <header><div><span class="tv-eyebrow">Voice engine</span><h3>{{ announcement ? "Choose the announcement voice" : "Choose Tater's reply voice" }}</h3><p>{{ announcement ? "Reuse the Reply Voice profile or select a dedicated engine for announcements." : "Select one engine. Only its matching model and connection settings appear." }}</p></div><span class="tm-speech-status-chip">{{ activeBackendMeta.label }}</span></header>
      <div v-if="error" class="tv-notice error">{{ error }}</div>
      <div class="tm-speech-provider-grid tm-tts-provider-grid" role="group" :aria-label="`${announcement ? 'Announcement' : 'Reply'} voice engine`">
        <button v-for="option in backendOptions" :key="optionValue(option)" type="button" :class="{ active: backend === optionValue(option) }" :aria-pressed="backend === optionValue(option)" @click="selectBackend(optionValue(option))"><i>{{ detailsFor(optionValue(option)).mark }}</i><span><strong>{{ optionLabel(option) }}</strong><small>{{ detailsFor(optionValue(option)).short }}</small></span><b aria-hidden="true">✓</b></button>
      </div>
    </article>

    <article v-if="inheritsDirect" class="tm-form-card tm-speech-inherit-card">
      <i>↔</i><div><span class="tv-eyebrow">Linked profile</span><h3>Announcements use the complete Reply Voice setup</h3><p>Model, voice, connection, cloning, and tuning changes from Reply Voice are applied automatically.</p></div><span>{{ directBackendLabel }}</span>
    </article>

    <article v-else class="tm-form-card tm-tts-config-card">
      <header><div><span class="tv-eyebrow">{{ activeBackendMeta.kind }} setup</span><h3>{{ activeBackendMeta.label }}</h3><p>{{ activeBackendMeta.short }}</p></div><span class="tm-speech-status-chip">{{ draft[voiceKey] || draft[modelKey] || "Choose voice" }}</span></header>

      <section v-if="['wyoming', 'openai_compatible'].includes(effectiveBackend)" class="tm-speech-connection-panel">
        <div class="tm-speech-section-heading compact"><div><h3>Connection</h3><p>Tell Tater where this voice service is running.</p></div><span>{{ activeBackendMeta.kind }}</span></div>
        <div class="tm-field-grid">
          <template v-if="effectiveBackend === 'wyoming'">
            <label class="tm-field tm-field-wide"><span class="tm-field-label">Wyoming host</span><input v-model="draft[key('wyoming_tts_host')]" type="text" placeholder="127.0.0.1" @input="emit('dirty')" /><small>Hostname or IP address of the Wyoming TTS service.</small></label>
            <label class="tm-field"><span class="tm-field-label">Wyoming port</span><input v-model="draft[key('wyoming_tts_port')]" type="number" min="1" max="65535" placeholder="10200" @input="emit('dirty')" /></label>
          </template>
          <template v-else-if="effectiveBackend === 'openai_compatible'">
            <label class="tm-field tm-field-wide"><span class="tm-field-label">Base URL</span><input v-model="draft[key('openai_tts_base_url')]" type="text" placeholder="http://127.0.0.1:8000" @input="emit('dirty')" /><small>The root URL for an OpenAI-compatible audio API.</small></label>
            <label class="tm-field tm-field-wide"><span class="tm-field-label">API key</span><input v-model="draft[key('openai_tts_api_key')]" type="password" autocomplete="off" placeholder="Optional" @input="emit('dirty')" /></label>
          </template>
        </div>
        <div v-if="canDiscover" class="tm-speech-discovery"><div><i>⌕</i><span><strong>Find available voices</strong><small>Ask the connected service for its current models and voices.</small></span></div><button class="tv-button" type="button" :disabled="Boolean(busy)" @click="discover">{{ busy === 'discover' ? "Discovering…" : "Discover models & voices" }}</button></div>
      </section>

      <section v-if="showModelVoicePanel" class="tm-speech-voice-panel">
        <div class="tm-speech-section-heading compact"><div><h3>Model and voice</h3><p>Choose what generates the audio and how Tater should sound.</p></div><span>{{ modelOptions.length || voiceOptions.length ? "Available choices" : "Manual entry" }}</span></div>
        <div class="tm-field-grid">
          <label v-if="modelOptions.length" class="tm-field"><span class="tm-field-label">Model</span><select v-model="draft[modelKey]" @change="changed"><option value="">Choose a model</option><option v-if="draft[modelKey] && !hasOption(modelOptions, draft[modelKey])" :value="draft[modelKey]">Current: {{ draft[modelKey] }}</option><option v-for="option in modelOptions" :key="optionValue(option)" :value="optionValue(option)">{{ optionLabel(option) }}</option></select></label>
          <label v-else-if="effectiveBackend !== 'wyoming'" class="tm-field"><span class="tm-field-label">Model</span><input v-model="draft[modelKey]" type="text" placeholder="Model id or alias" @input="emit('dirty')" /></label>
          <label v-if="needsVoice && voiceOptions.length" class="tm-field"><span class="tm-field-label">Voice</span><select v-model="draft[voiceKey]" @change="emit('dirty')"><option value="">Choose a voice</option><option v-if="draft[voiceKey] && !hasOption(voiceOptions, draft[voiceKey])" :value="draft[voiceKey]">Current: {{ draft[voiceKey] }}</option><option v-for="option in voiceOptions" :key="optionValue(option)" :value="optionValue(option)">{{ optionLabel(option) }}</option></select></label>
          <label v-else-if="needsVoice" class="tm-field"><span class="tm-field-label">Voice</span><input v-model="draft[voiceKey]" type="text" placeholder="Voice id or name" @input="emit('dirty')" /></label>
        </div>
      </section>

    </article>

    <article v-if="cloneCapable && !inheritsDirect" class="tm-form-card tm-speech-clone-card">
      <header><div><span class="tv-eyebrow">Shared personal voice</span><h3>Voice sample</h3><p>One recording is shared automatically by Pocket TTS, Qwen3-TTS, and OmniVoice when you switch engines.</p></div><span class="tm-speech-status-chip" :class="{ ready: cloneInfo.configured }">{{ cloneInfo.configured ? "Voice ready" : "Add a sample" }}</span></header>

      <section v-if="cloneInfo.configured" class="tm-voice-sample-player">
        <div class="tm-voice-sample-icon" aria-hidden="true">♫</div>
        <div class="tm-voice-sample-body">
          <div class="tm-voice-sample-name"><span><strong>{{ cloneInfo.name || "Voice sample" }}</strong><small>{{ readableSize(cloneInfo.size) }} · Shared across all local voice cloners</small></span><i>Ready</i></div>
          <audio v-if="cloneAudioUrl" :key="cloneAudioUrl" controls preload="metadata" :src="cloneAudioUrl">Your browser cannot play this audio sample.</audio>
          <div class="tm-voice-sample-actions">
            <label class="tv-button tm-voice-replace"><input type="file" accept="audio/*" :disabled="Boolean(busy)" @change="uploadClone" />{{ busy === 'upload' ? "Uploading…" : "Replace sample" }}</label>
            <button class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="deleteClone">{{ busy === 'delete' ? "Removing…" : "Remove sample" }}</button>
          </div>
        </div>
      </section>

      <label v-else class="tm-voice-sample-empty">
        <input type="file" accept="audio/*" :disabled="Boolean(busy)" @change="uploadClone" />
        <i aria-hidden="true">＋</i><span><strong>{{ busy === 'upload' ? "Uploading voice sample…" : "Choose a voice recording" }}</strong><small>Use a short, clean recording with little background noise. WAV, MP3, FLAC, M4A, OGG, Opus, and AAC are supported.</small></span>
      </label>

      <div class="tm-field-grid tm-voice-clone-tuning">
        <label v-if="managed" class="tm-field tm-field-wide"><span class="tm-field-label">Reference transcript</span><textarea v-model="draft[cloneTextKey]" placeholder="Enter exactly what is spoken in the reference recording." @input="emit('dirty')" /><small>Tater fills this automatically when STT is available. Correct it here if needed.</small></label>
        <label v-if="managed" class="tm-field"><span class="tm-field-label">Language</span><select v-model="draft[languageKey]" @change="emit('dirty')"><option v-if="draft[languageKey] && !hasOption(languageOptions, draft[languageKey])" :value="draft[languageKey]">Current: {{ draft[languageKey] }}</option><option v-for="option in languageOptions" :key="optionValue(option)" :value="optionValue(option)">{{ optionLabel(option) }}</option></select><small>{{ effectiveBackend === "omnivoice" ? "Auto works across OmniVoice's full language catalog. Open the list and type a language name to jump to it." : "Auto detects the language from the reply. Choose one explicitly only when detection needs help." }}</small></label>
        <label v-if="managed" class="tm-field"><span class="tm-field-label">Voice instruction</span><input v-model="draft[instructKey]" type="text" placeholder="Optional style or delivery guidance" @input="emit('dirty')" /></label>
        <label v-if="effectiveBackend === 'pocket_tts'" class="tm-field"><span class="tm-field-label">Output gain</span><input v-model="draft[key('pocket_tts_output_gain')]" type="number" step="0.05" min="0.1" max="4" @input="emit('dirty')" /><small>Adjust Pocket TTS volume without changing satellite volume.</small></label>
      </div>
    </article>
  </section>
</template>
