<script setup lang="ts">
import { computed } from "vue";
import type { JsonRow } from "../../types";

const props = defineProps<{
  kind: "vision" | "audio" | "video";
  draft: JsonRow;
  localModels: JsonRow;
}>();
const emit = defineEmits<{ dirty: [] }>();

const label = computed(() => props.kind === "vision" ? "Image Understanding" : props.kind === "audio" ? "Audio Understanding" : "Video Understanding");
const prefix = computed(() => props.kind === "vision" ? "vision" : `${props.kind}_understanding`);
const modeKey = computed(() => `${prefix.value}_mode`);
const providerKey = computed(() => `${prefix.value}_provider`);
const modelKey = computed(() => `${prefix.value}_model`);
const apiBaseKey = computed(() => `${prefix.value}_api_base`);
const apiKeyKey = computed(() => `${prefix.value}_api_key`);
const maxSecondsKey = computed(() => `${prefix.value}_max_seconds`);
const mode = computed(() => String(props.draft[modeKey.value] || (props.kind === "vision" ? "api" : "base")));
const provider = computed(() => String(props.draft[providerKey.value] || (props.kind === "vision" ? "openai_compatible" : "llama_cpp")));
const allInstalled = computed<JsonRow[]>(() => Array.isArray(props.localModels?.models) ? props.localModels.models : []);
const installed = computed<JsonRow[]>(() => allInstalled.value.filter((row: JsonRow) => {
  if (String(row.provider || "") !== provider.value) return false;
  if (row.is_speculative_draft === true) return false;
  const capability = props.kind === "vision" ? "supports_vision" : props.kind === "audio" ? "supports_audio" : "supports_video";
  const hasObservedCapabilities = ["supports_vision", "supports_audio", "supports_video"].some((key) => Boolean(row[key]));
  return !hasObservedCapabilities || Boolean(row[capability]);
}));

const providerOptions = [
  { value: "openai_compatible", label: "OpenAI-Compatible API", mark: "API", short: "Use a hosted or LAN multimodal endpoint." },
  { value: "hf_transformers", label: "Hugging Face Transformers", mark: "HF", short: "Run a downloaded Transformers model locally." },
  { value: "llama_cpp", label: "llama.cpp Local", mark: "GGUF", short: "Run a downloaded quantized model in Tater." },
  { value: "llama_cpp_remote", label: "llama.cpp Remote", mark: "LAN", short: "Connect to a llama.cpp server on another machine." },
  { value: "mlx_lm", label: "MLX LM", mark: "MLX", short: "Use Apple Silicon optimized local inference." },
];

const modeOptions = computed(() => [
  ...(props.kind === "vision" ? [{ value: "api", label: "API", mark: "API", short: "Use a dedicated API model for every image." }] : []),
  { value: "base", label: "Same as Base", mark: "↔", short: "Reuse the main LLM when it supports this media." },
  { value: "auto", label: "Auto", mark: "A", short: "Let Tater choose the best available capable route." },
  { value: "dedicated", label: "Dedicated model", mark: "+", short: "Choose a separate model used only for this media." },
]);
const activeMode = computed(() => modeOptions.value.find((option) => option.value === mode.value) || modeOptions.value[0]);
const activeProvider = computed(() => providerOptions.find((option) => option.value === provider.value) || providerOptions[0]);
const kindMeta = computed(() => props.kind === "vision"
  ? { eyebrow: "Visual intelligence", title: "What should inspect images?", mark: "VIS", short: "Still images, camera frames, screenshots, and image-enabled tools." }
  : props.kind === "audio"
    ? { eyebrow: "Audio intelligence", title: "What should understand sound?", mark: "AUD", short: "Speech, music, ambience, and other bounded audio clips." }
    : { eyebrow: "Video intelligence", title: "What should inspect video?", mark: "VID", short: "Short clips analyzed as frames, actions, and scene changes." });

function changed() { emit("dirty"); }
function chooseMode(value: string) { props.draft[modeKey.value] = value; changed(); }
function chooseProvider(value: string) { props.draft[providerKey.value] = value; changed(); }
function local() { return ["hf_transformers", "llama_cpp", "mlx_lm"].includes(provider.value); }
function api() { return ["openai_compatible", "llama_cpp_remote"].includes(provider.value); }
function currentInstalled() { return allInstalled.value.some((model) => String(model.provider || "") === provider.value && String(model.model || "") === String(props.draft[modelKey.value] || "")); }
</script>

<template>
  <article class="tm-form-card tm-media-card" :class="`tm-media-${kind}`">
    <header class="tm-media-header"><div class="tm-media-title"><i>{{ kindMeta.mark }}</i><div><span class="tv-eyebrow">{{ kindMeta.eyebrow }}</span><h3>{{ label }}</h3><p>{{ kindMeta.short }}</p></div></div><span class="tm-speech-status-chip">{{ activeMode.label }}</span></header>

    <section class="tm-media-section">
      <div class="tm-speech-section-heading compact"><div><h3>{{ kindMeta.title }}</h3><p>Pick the routing behavior first. Detailed settings appear only when they are needed.</p></div><span>Route</span></div>
      <div class="tm-choice-grid tm-media-mode-grid" role="group" :aria-label="`${label} mode`">
        <button v-for="option in modeOptions" :key="option.value" type="button" :class="{ active: mode === option.value }" @click="chooseMode(option.value)"><i>{{ option.mark }}</i><span><strong>{{ option.label }}</strong><small>{{ option.short }}</small></span><b>✓</b></button>
      </div>
    </section>

    <section v-if="mode === 'dedicated' || mode === 'api'" class="tm-media-section tm-media-provider-section">
      <div class="tm-speech-section-heading compact"><div><h3>Choose a provider</h3><p>Only compatible installed models appear in the local model picker.</p></div><span>{{ activeProvider.label }}</span></div>
      <div class="tm-choice-grid tm-media-provider-grid" role="group" :aria-label="`${label} provider`">
        <button v-for="option in providerOptions" :key="option.value" type="button" :class="{ active: provider === option.value }" @click="chooseProvider(option.value)"><i>{{ option.mark }}</i><span><strong>{{ option.label }}</strong><small>{{ option.short }}</small></span><b>✓</b></button>
      </div>

      <div class="tm-media-config-panel">
        <div v-if="api()" class="tm-field-grid">
          <label class="tm-field"><span class="tm-field-label">API base URL</span><input v-model="draft[apiBaseKey]" type="url" placeholder="http://127.0.0.1:1234" @input="changed" /><small>Base URL for the compatible inference server.</small></label>
          <label class="tm-field"><span class="tm-field-label">API key</span><input v-model="draft[apiKeyKey]" type="password" autocomplete="off" placeholder="Optional" @input="changed" /><small>Stored locally and sent only to this endpoint.</small></label>
        </div>
        <label v-if="local()" class="tm-field tm-field-wide"><span class="tm-field-label">Downloaded {{ kind }} model</span><select v-model="draft[modelKey]" @change="changed"><option value="">Select a downloaded model</option><option v-if="draft[modelKey] && !currentInstalled()" :value="draft[modelKey]">Current: {{ draft[modelKey] }}</option><option v-for="model in installed" :key="String(model.model)" :value="model.model">{{ model.model }}</option></select><small v-if="!installed.length">No compatible downloaded models were detected. Add one from the Hugging Face tab.</small><small v-else>{{ installed.length }} compatible downloaded model{{ installed.length === 1 ? "" : "s" }} available.</small></label>
        <label v-else class="tm-field tm-field-wide"><span class="tm-field-label">Model name or alias</span><input v-model="draft[modelKey]" type="text" placeholder="Model id or server alias" @input="changed" /><small>Use the exact model name exposed by the selected server.</small></label>
      </div>
    </section>

    <div v-else class="tm-media-route-summary"><i>{{ activeMode.mark }}</i><span><strong>{{ activeMode.label }}</strong><small>{{ activeMode.short }} No separate provider configuration is required.</small></span></div>

    <section v-if="kind !== 'vision'" class="tm-media-limits">
      <div><strong>Maximum clip length</strong><small>Longer clips need more context and take longer to process.</small></div><label><input v-model="draft[maxSecondsKey]" type="range" min="1" max="3600" step="1" @input="changed" /><span><input v-model="draft[maxSecondsKey]" type="number" min="1" max="3600" step="1" @input="changed" /> sec</span></label>
    </section>

    <details v-if="kind === 'vision'" class="tm-speech-advanced tm-media-advanced"><summary><span><strong>llama.cpp performance</strong><small>Context allocation and optional dedicated slot</small></span><b>⌄</b></summary><div class="tm-field-grid"><label class="tm-field"><span class="tm-field-label">Multimodal context</span><input v-model="draft.hydra_llama_cpp_vision_context_tokens" type="number" min="256" max="262144" step="256" @input="changed" /><small>Maximum tokens reserved for image requests.</small></label><label class="tm-field"><span class="tm-field-label">Prompt cache slot</span><input v-model="draft.hydra_llama_cpp_vision_slot" type="number" min="0" max="31" placeholder="Auto" @input="changed" /><small>Leave blank for automatic slot selection.</small></label></div></details>
  </article>
</template>
