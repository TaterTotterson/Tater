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
    modelsChatterboxVoices: string;
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
  chatterbox: { mark: "CB", short: "Expressive speech with optional tuning.", kind: "Server" },
  kokoro: { mark: "KO", short: "Fast local neural speech synthesis.", kind: "Local" },
  pocket_tts: { mark: "PT", short: "Small local voice model with presets.", kind: "Local" },
  piper: { mark: "PI", short: "Reliable, lightweight offline voices.", kind: "Local" },
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
const voiceOptions = computed<JsonRow[]>(() => {
  if (discoveredVoices.value.length) return discoveredVoices.value;
  const byModel = props.ui.tts_voice_options_by_model && typeof props.ui.tts_voice_options_by_model === "object" ? props.ui.tts_voice_options_by_model as JsonRow : {};
  return Array.isArray(byModel[String(props.draft[modelKey.value] || "")]) ? byModel[String(props.draft[modelKey.value] || "")] : [];
});
const backendOptions = computed<JsonRow[]>(() => Array.isArray(props.ui.tts_backend_options) ? props.ui.tts_backend_options : []);
const activeBackendMeta = computed(() => detailsFor(backend.value));
const directBackendLabel = computed(() => detailsFor(String(props.draft.speech_tts_backend || "wyoming")).label);
const canDiscover = computed(() => !inheritsDirect.value && ["wyoming", "openai_compatible", "chatterbox"].includes(effectiveBackend.value));
const needsVoice = computed(() => voiceOptions.value.length > 0 || ["wyoming", "openai_compatible", "chatterbox", "kokoro", "pocket_tts"].includes(effectiveBackend.value));
const managed = computed(() => ["qwen3_tts", "omnivoice"].includes(effectiveBackend.value));
const managedToken = computed(() => effectiveBackend.value === "qwen3_tts" ? "qwen_tts" : "omnivoice_tts");
const managedBase = computed(() => `${keyPrefix.value}${managedToken.value}`);
const cloneInfoKey = computed(() => `${managedBase.value}_clone_audio`);
const cloneTextKey = computed(() => `${managedBase.value}_clone_text`);
const languageKey = computed(() => `${managedBase.value}_language`);
const instructKey = computed(() => `${managedBase.value}_instruct`);
const cloneInfo = computed<JsonRow>(() => props.draft[cloneInfoKey.value] && typeof props.draft[cloneInfoKey.value] === "object" ? props.draft[cloneInfoKey.value] as JsonRow : {});

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
  changed();
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
    } else if (effectiveBackend.value === "chatterbox") {
      result = await postJson<JsonRow>(props.endpoints.modelsChatterboxVoices, { base_url: props.draft[key("chatterbox_tts_base_url")], voice_mode: props.draft[key("chatterbox_tts_voice_mode")] });
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
  if (!file || !managed.value) return;
  busy.value = "upload";
  error.value = "";
  try {
    const endpoint = `${props.endpoints.modelsCloneAudio}/${effectiveBackend.value}?scope=${props.scope}`;
    const result = await responseJson<JsonRow>(await fetch(endpoint, { method: "POST", credentials: "same-origin", headers: { Accept: "application/json", "X-Filename": file.name, "Content-Type": file.type || "application/octet-stream" }, body: file }));
    props.draft[cloneInfoKey.value] = result.audio || { configured: true, name: file.name, size: file.size };
    if (result.clone_text) props.draft[cloneTextKey.value] = result.clone_text;
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
  if (!managed.value || !window.confirm("Remove this reference voice recording?")) return;
  busy.value = "delete";
  try {
    const endpoint = `${props.endpoints.modelsCloneAudio}/${effectiveBackend.value}?scope=${props.scope}`;
    const result = await responseJson<JsonRow>(await fetch(endpoint, { method: "DELETE", credentials: "same-origin", headers: { Accept: "application/json" } }));
    props.draft[cloneInfoKey.value] = result.audio || {};
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

      <section v-if="['wyoming', 'openai_compatible', 'chatterbox'].includes(effectiveBackend)" class="tm-speech-connection-panel">
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
          <template v-else>
            <label class="tm-field tm-field-wide"><span class="tm-field-label">Chatterbox URL</span><input v-model="draft[key('chatterbox_tts_base_url')]" type="text" placeholder="http://127.0.0.1:8004" @input="emit('dirty')" /><small>The Chatterbox server used for synthesis and voice discovery.</small></label>
            <label class="tm-field"><span class="tm-field-label">Voice source</span><select v-model="draft[key('chatterbox_tts_voice_mode')]" @change="changed"><option value="predefined">Predefined voice</option><option value="clone">Cloned voice</option></select></label>
          </template>
        </div>
        <div v-if="canDiscover" class="tm-speech-discovery"><div><i>⌕</i><span><strong>Find available voices</strong><small>Ask the connected service for its current models and voices.</small></span></div><button class="tv-button" type="button" :disabled="Boolean(busy)" @click="discover">{{ busy === 'discover' ? "Discovering…" : "Discover models & voices" }}</button></div>
      </section>

      <section class="tm-speech-voice-panel">
        <div class="tm-speech-section-heading compact"><div><h3>Model and voice</h3><p>Choose what generates the audio and how Tater should sound.</p></div><span>{{ modelOptions.length || voiceOptions.length ? "Available choices" : "Manual entry" }}</span></div>
        <div class="tm-field-grid">
          <label v-if="modelOptions.length" class="tm-field"><span class="tm-field-label">Model</span><select v-model="draft[modelKey]" @change="changed"><option value="">Choose a model</option><option v-if="draft[modelKey] && !hasOption(modelOptions, draft[modelKey])" :value="draft[modelKey]">Current: {{ draft[modelKey] }}</option><option v-for="option in modelOptions" :key="optionValue(option)" :value="optionValue(option)">{{ optionLabel(option) }}</option></select></label>
          <label v-else-if="!['wyoming', 'chatterbox'].includes(effectiveBackend)" class="tm-field"><span class="tm-field-label">Model</span><input v-model="draft[modelKey]" type="text" placeholder="Model id or alias" @input="emit('dirty')" /></label>
          <label v-if="needsVoice && voiceOptions.length" class="tm-field"><span class="tm-field-label">Voice</span><select v-model="draft[voiceKey]" @change="emit('dirty')"><option value="">Choose a voice</option><option v-if="draft[voiceKey] && !hasOption(voiceOptions, draft[voiceKey])" :value="draft[voiceKey]">Current: {{ draft[voiceKey] }}</option><option v-for="option in voiceOptions" :key="optionValue(option)" :value="optionValue(option)">{{ optionLabel(option) }}</option></select></label>
          <label v-else-if="needsVoice" class="tm-field"><span class="tm-field-label">Voice</span><input v-model="draft[voiceKey]" type="text" placeholder="Voice id or name" @input="emit('dirty')" /></label>
          <label v-if="effectiveBackend === 'kokoro'" class="tm-field"><span class="tm-field-label">Output gain</span><input v-model="draft[key('kokoro_output_gain')]" type="number" step="0.05" min="0" @input="emit('dirty')" /><small>Adjust Kokoro's output volume without changing satellite volume.</small></label>
          <label v-if="effectiveBackend === 'pocket_tts'" class="tm-field"><span class="tm-field-label">Output gain</span><input v-model="draft[key('pocket_tts_output_gain')]" type="number" step="0.05" min="0" @input="emit('dirty')" /><small>Adjust Pocket TTS output volume.</small></label>
        </div>
      </section>

      <details v-if="effectiveBackend === 'chatterbox'" class="tm-speech-advanced">
        <summary><span><strong>Fine-tune Chatterbox</strong><small>Optional expression, pacing, and generation controls</small></span><b>Advanced</b></summary>
        <div class="tm-field-grid">
          <label class="tm-field"><span class="tm-field-label">Language</span><input v-model="draft[key('chatterbox_tts_language')]" type="text" placeholder="Auto" @input="emit('dirty')" /></label>
          <label class="tm-field"><span class="tm-field-label">Chunk size</span><input v-model="draft[key('chatterbox_tts_chunk_size')]" type="number" min="1" @input="emit('dirty')" /></label>
          <label class="tm-field"><span class="tm-field-label">Temperature</span><input v-model="draft[key('chatterbox_tts_temperature')]" type="number" min="0" step="0.05" @input="emit('dirty')" /></label>
          <label class="tm-field"><span class="tm-field-label">Exaggeration</span><input v-model="draft[key('chatterbox_tts_exaggeration')]" type="number" min="0" step="0.05" @input="emit('dirty')" /></label>
          <label class="tm-field"><span class="tm-field-label">CFG weight</span><input v-model="draft[key('chatterbox_tts_cfg_weight')]" type="number" min="0" step="0.05" @input="emit('dirty')" /></label>
          <label class="tm-field"><span class="tm-field-label">Seed</span><input v-model="draft[key('chatterbox_tts_seed')]" type="number" @input="emit('dirty')" /></label>
          <label class="tm-field"><span class="tm-field-label">Speed</span><input v-model="draft[key('chatterbox_tts_speed_factor')]" type="number" min="0.1" step="0.05" @input="emit('dirty')" /></label>
          <label v-if="!announcement" class="tm-field tm-toggle-field tm-field-wide"><span><strong>Satellite streaming</strong><small>Start playback while the rest of the reply is still being generated.</small></span><input v-model="draft.speech_chatterbox_tts_streaming_enabled" type="checkbox" @change="emit('dirty')" /></label>
        </div>
      </details>
    </article>

    <article v-if="managed && !inheritsDirect" class="tm-form-card tm-speech-clone-card">
      <header><div><span class="tv-eyebrow">Personal voice</span><h3>Reference voice</h3><p>Upload a clean recording and transcript so {{ activeBackendMeta.label }} can reproduce that voice.</p></div><span class="tm-speech-status-chip" :class="{ ready: cloneInfo.configured }">{{ cloneInfo.configured ? "Reference ready" : "Not configured" }}</span></header>
      <div class="tm-field-grid">
        <label class="tm-field tm-field-wide"><span class="tm-field-label">Reference transcript</span><textarea v-model="draft[cloneTextKey]" placeholder="Enter exactly what is spoken in the reference recording." @input="emit('dirty')" /><small>A matching transcript produces a more accurate cloned voice.</small></label>
        <label class="tm-field"><span class="tm-field-label">Language</span><input v-model="draft[languageKey]" type="text" placeholder="English" @input="emit('dirty')" /></label>
        <label class="tm-field"><span class="tm-field-label">Voice instruction</span><input v-model="draft[instructKey]" type="text" placeholder="Optional style or delivery guidance" @input="emit('dirty')" /></label>
        <label class="tm-field tm-field-wide tm-speech-file-field"><span class="tm-field-label">Reference audio</span><input type="file" accept="audio/*" :disabled="Boolean(busy)" @change="uploadClone" /><small>{{ cloneInfo.configured ? `${cloneInfo.name} · ${Number(cloneInfo.size || 0).toLocaleString()} bytes` : "Use a short, clear recording with little background noise." }}</small></label>
      </div>
      <div v-if="cloneInfo.configured" class="tm-inline-actions tm-secondary-row"><button class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="deleteClone">{{ busy === 'delete' ? "Removing…" : "Remove reference audio" }}</button></div>
    </article>
  </section>
</template>
