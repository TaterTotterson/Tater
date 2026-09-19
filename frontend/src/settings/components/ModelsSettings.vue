<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { getJson, postJson } from "../../shared/api";
import type { JsonRow, ModelsSettingsPayload, SettingsMountOptions } from "../types";
import HuggingFaceModels from "./models/HuggingFaceModels.vue";
import FaceModels from "./models/FaceModels.vue";
import IdentityModels from "./models/IdentityModels.vue";
import LlmModels from "./models/LlmModels.vue";
import LocalModelApplyProgress from "./models/LocalModelApplyProgress.vue";
import MediaModelCard from "./models/MediaModelCard.vue";
import SpeechModels from "./models/SpeechModels.vue";
import WakeWordModels from "./models/WakeWordModels.vue";

const props = defineProps<{
  settings: ModelsSettingsPayload;
  endpoints: SettingsMountOptions["endpoints"];
  initialTab?: string;
  onTabChange?: (tab: string) => void;
}>();

const emit = defineEmits<{
  changed: [settings: ModelsSettingsPayload];
  notify: [message: string, tone?: string];
}>();

const tabs = [
  { id: "huggingface", label: "Hugging Face", short: "Find, download, and remove local models." },
  { id: "routing", label: "LLM", short: "Base servers, Spudex, Beast roles, and local runtime tuning." },
  { id: "speech", label: "Speech", short: "Listening, reply voices, announcements, and playback." },
  { id: "wake", label: "Wake Word", short: "Live wake model, trainer, and STT verification settings." },
  { id: "vision", label: "Vision", short: "Still-image understanding and multimodal runtime." },
  { id: "audio-understanding", label: "Audio & Video", short: "Media understanding for clips and recordings." },
  { id: "speakerid", label: "Speaker ID", short: "Live voice identity runtime and enrollment." },
  { id: "emotionid", label: "Emotion ID", short: "Live voice-tone classification and prompt hints." },
  { id: "faceid", label: "Face ID", short: "Local face recognition model and processing status." },
] as const;

const tabIds = new Set(tabs.map((tab) => tab.id));
const activeTab = ref(normalizeTab(props.initialTab));
const draft = reactive<JsonRow>({});
const dirty = reactive<Record<string, boolean>>({});
const saving = ref(false);
const error = ref("");
const notice = ref("");
const wakePanel = ref<InstanceType<typeof WakeWordModels> | null>(null);
const speakerPanel = ref<InstanceType<typeof IdentityModels> | null>(null);
const emotionPanel = ref<InstanceType<typeof IdentityModels> | null>(null);
const speakerSubtab = ref<"people" | "settings">("people");
const speechPanel = ref<InstanceType<typeof SpeechModels> | null>(null);
const faceStatus = ref<JsonRow>({});
const modelApplyOpen = ref(false);
const modelWarmup = ref<JsonRow>({});
const modelWarmupPollError = ref("");
let faceTimer: number | null = null;
let modelWarmupTimer: number | null = null;

const activeSpec = computed(() => tabs.find((tab) => tab.id === activeTab.value) || tabs[0]);
const localModels = computed<JsonRow>(() => draft.local_llm_models && typeof draft.local_llm_models === "object" ? draft.local_llm_models as JsonRow : {});
const actionSpec = computed(() => {
  switch (activeTab.value) {
    case "routing": return { label: "Save & Apply LLM Settings", note: "Saves Base, Spudex, and Beast routes, then loads selected local models." };
    case "speech": return { label: "Save Speech Settings", note: "Saves listening, reply, announcement, playback, and native voice-model settings." };
    case "wake": return { label: "Apply To All Satellites", note: "Applies the wake model and verification mode together to every connected satellite." };
    case "vision": return { label: "Save & Apply Vision Settings", note: "Saves image understanding and loads the selected local model when needed." };
    case "audio-understanding": return { label: "Save & Apply Audio and Video", note: "Saves both media configurations and loads selected local models." };
    case "speakerid": return speakerSubtab.value === "settings" ? { label: "Save Speaker ID Settings", note: "Applies the runtime settings above; individual speaker actions remain on their cards." } : null;
    case "emotionid": return { label: "Save Emotion ID Settings", note: "Applies the voice-tone runtime settings above." };
    case "faceid": return { label: "Apply Face ID Settings", note: "Updates the recognition model and live runtime state." };
    default: return null;
  }
});
const faceSettings = computed<JsonRow>(() => draft.face_id && typeof draft.face_id === "object" ? draft.face_id as JsonRow : {});
const faceModels = computed<JsonRow[]>(() => Array.isArray(faceSettings.value.available_models) ? faceSettings.value.available_models : [
  { id: "facenet512", label: "FaceNet512" },
  { id: "adaface_ir50_webface4m", label: "AdaFace IR-50 · WebFace4M", experimental: true },
]);

function normalizeTab(value: unknown): string {
  const token = String(value || "").trim().toLowerCase();
  return tabIds.has(token as typeof tabs[number]["id"]) ? token : "huggingface";
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value ?? {})) as T;
}

function initVoiceModelValues(target: JsonRow) {
  if (!target.esphome_settings || typeof target.esphome_settings !== "object") target.esphome_settings = {};
  const values = target.esphome_settings as JsonRow;
  const ui = target.voice_model_ui && typeof target.voice_model_ui === "object" ? target.voice_model_ui as JsonRow : {};
  (Array.isArray(ui.sections) ? ui.sections : []).forEach((section: JsonRow) => {
    (Array.isArray(section.fields) ? section.fields : []).forEach((field: JsonRow) => {
      const key = String(field.key || "").trim();
      const type = String(field.type || "").trim().toLowerCase();
      if (key && !["table", "readonly", "section", "led_preview"].includes(type) && !field.disabled && !field.read_only && !field.readonly && !Object.prototype.hasOwnProperty.call(values, key)) values[key] = field.value ?? field.default ?? (type === "checkbox" ? false : "");
    });
  });
}

function sync(settings: ModelsSettingsPayload, preserveDirty = false) {
  const dirtySnapshot = { ...dirty };
  Object.keys(draft).forEach((key) => delete draft[key]);
  Object.assign(draft, clone(settings || {}));
  if (!Array.isArray(draft.hydra_base_servers) || !draft.hydra_base_servers.length) {
    draft.hydra_base_servers = [{
      provider: draft.hydra_llm_provider || "openai_compatible",
      host: draft.hydra_llm_host || "",
      port: draft.hydra_llm_port || "",
      model: draft.hydra_llm_model || "",
      api_key: draft.hydra_llm_api_key || "",
      llama_cpp_slot: draft.hydra_llama_cpp_base_slot || "",
    }];
  }
  if (!draft.face_id || typeof draft.face_id !== "object") draft.face_id = {};
  initVoiceModelValues(draft);
  Object.keys(dirty).forEach((key) => { dirty[key] = preserveDirty ? Boolean(dirtySnapshot[key]) : false; });
}

function markDirty() {
  dirty[activeTab.value] = true;
  error.value = "";
  notice.value = "";
}

function setSpeakerSubtab(tab: string) {
  speakerSubtab.value = tab === "settings" ? "settings" : "people";
}

function select(tab: string) {
  activeTab.value = normalizeTab(tab);
  if (activeTab.value === "speakerid") speakerSubtab.value = "people";
  error.value = "";
  notice.value = "";
  props.onTabChange?.(activeTab.value);
  if (activeTab.value === "faceid") void refreshFaceStatus();
}

function isLocalProvider(value: unknown) {
  return ["hf_transformers", "llama_cpp", "mlx_lm"].includes(String(value || ""));
}

function loadTarget(provider: unknown, model: unknown, extra: JsonRow = {}): JsonRow | null {
  const providerToken = String(provider || "");
  const modelToken = String(model || "").trim();
  return isLocalProvider(providerToken) && modelToken ? { provider: providerToken, model: modelToken, ...extra } : null;
}

function dedupeTargets(targets: Array<JsonRow | null>): JsonRow[] {
  const rows = new Map<string, JsonRow>();
  targets.filter(Boolean).forEach((target) => {
    const row = target as JsonRow;
    const key = `${row.provider}|${row.model}`;
    if (!rows.has(key)) rows.set(key, row);
    else {
      const current = rows.get(key) as JsonRow;
      ["roles", "media_kinds"].forEach((field) => {
        const merged = [...(Array.isArray(current[field]) ? current[field] : []), ...(Array.isArray(row[field]) ? row[field] : [])];
        if (merged.length) current[field] = Array.from(new Set(merged.map(String)));
      });
    }
  });
  return [...rows.values()];
}

function llmPayload(): JsonRow {
  const servers = (Array.isArray(draft.hydra_base_servers) ? draft.hydra_base_servers as JsonRow[] : []).map((row) => ({
    provider: String(row.provider || "openai_compatible"), host: String(row.host || "").trim(), port: String(row.port || "").trim(), model: String(row.model || "").trim(), api_key: String(row.api_key || "").trim(), llama_cpp_slot: String(row.llama_cpp_slot || "").trim(),
  }));
  const primary = servers[0] || {};
  const keys = [
    "hydra_hf_transformers_context_tokens", "hydra_hf_transformers_device", "hydra_hf_transformers_dtype", "hydra_hf_transformers_device_map", "hydra_hf_transformers_attn_implementation", "hydra_hf_transformers_trust_remote_code",
    "hydra_llama_cpp_context_tokens", "hydra_llama_cpp_mtp_enabled", "hydra_llama_cpp_speculative_method", "hydra_llama_cpp_mtp_draft_tokens", "hydra_llama_cpp_mtp_draft_model", "hydra_llama_cpp_n_batch", "hydra_llama_cpp_n_ubatch", "hydra_llama_cpp_flash_attn", "hydra_llama_cpp_offload_kqv", "hydra_llama_cpp_slot_count",
    "hydra_mlx_lm_context_tokens", "hydra_mlx_lm_trust_remote_code", "hydra_mlx_lm_lazy_load", "hydra_mlx_engine_prefill_step_size", "hydra_mlx_engine_kv_bits", "hydra_mlx_engine_kv_group_size", "hydra_mlx_engine_quantized_kv_start",
    "spudex_llm_provider", "spudex_llm_host", "spudex_llm_model", "hydra_beast_mode_enabled",
  ];
  const payload: JsonRow = Object.fromEntries(keys.map((key) => [key, draft[key]]));
  Object.assign(payload, {
    hydra_llm_provider: primary.provider || "openai_compatible", hydra_llm_host: primary.host || "", hydra_llm_port: primary.port || "", hydra_llm_model: primary.model || "", hydra_llm_api_key: primary.api_key || "", hydra_llama_cpp_base_slot: primary.llama_cpp_slot || "", hydra_base_servers: servers,
  });
  const targets: Array<JsonRow | null> = servers.map((row) => loadTarget(row.provider, row.model, { roles: ["Base"] }));
  if (draft.spudex_llm_provider) targets.push(loadTarget(draft.spudex_llm_provider, draft.spudex_llm_model));
  ["astraeus", "thanatos", "hermes"].forEach((role) => {
    ["provider", "host", "port", "model", "api_key", "llama_cpp_slot"].forEach((suffix) => { payload[`hydra_llm_${role}_${suffix}`] = draft[`hydra_llm_${role}_${suffix}`] ?? ""; });
    targets.push(loadTarget(draft[`hydra_llm_${role}_provider`], draft[`hydra_llm_${role}_model`], { roles: [role] }));
  });
  payload.hydra_local_model_load_targets = dedupeTargets(targets);
  return payload;
}

function validateLlm(): string {
  const servers = Array.isArray(draft.hydra_base_servers) ? draft.hydra_base_servers as JsonRow[] : [];
  const missingServer = servers.findIndex((row) => isLocalProvider(row.provider) && !String(row.model || "").trim());
  if (missingServer >= 0) return `Choose a downloaded model for ${missingServer === 0 ? "the primary Base server" : `fallback server ${missingServer}`}.`;
  if (draft.spudex_llm_provider && isLocalProvider(draft.spudex_llm_provider) && !String(draft.spudex_llm_model || "").trim()) return "Choose a downloaded model for Spudex.";
  if (draft.hydra_beast_mode_enabled) {
    for (const role of ["astraeus", "thanatos", "hermes"]) {
      if (isLocalProvider(draft[`hydra_llm_${role}_provider`]) && !String(draft[`hydra_llm_${role}_model`] || "").trim()) return `Choose a downloaded model for ${role.charAt(0).toUpperCase() + role.slice(1)}.`;
    }
  }
  return "";
}

const speechKeys = [
  "speech_stt_backend", "speech_acceleration", "speech_wyoming_stt_host", "speech_wyoming_stt_port", "speech_tts_backend", "speech_tts_model", "speech_tts_voice", "speech_kokoro_output_gain", "speech_pocket_tts_output_gain", "speech_qwen_tts_clone_text", "speech_qwen_tts_language", "speech_qwen_tts_instruct", "speech_omnivoice_tts_clone_text", "speech_omnivoice_tts_language", "speech_omnivoice_tts_instruct", "speech_wyoming_tts_host", "speech_wyoming_tts_port", "speech_wyoming_tts_voice", "speech_openai_tts_base_url", "speech_openai_tts_api_key", "speech_chatterbox_tts_base_url", "speech_chatterbox_tts_voice_mode", "speech_chatterbox_tts_chunk_size", "speech_chatterbox_tts_temperature", "speech_chatterbox_tts_exaggeration", "speech_chatterbox_tts_cfg_weight", "speech_chatterbox_tts_seed", "speech_chatterbox_tts_speed_factor", "speech_chatterbox_tts_language", "speech_chatterbox_tts_streaming_enabled", "speech_announcement_tts_backend", "speech_announcement_tts_model", "speech_announcement_tts_voice", "speech_announcement_kokoro_output_gain", "speech_announcement_pocket_tts_output_gain", "speech_announcement_qwen_tts_clone_text", "speech_announcement_qwen_tts_language", "speech_announcement_qwen_tts_instruct", "speech_announcement_omnivoice_tts_clone_text", "speech_announcement_omnivoice_tts_language", "speech_announcement_omnivoice_tts_instruct", "speech_satellite_ducking_target_percent", "speech_satellite_ducking_attack_ms", "speech_satellite_ducking_release_ms", "speech_announcement_wyoming_tts_host", "speech_announcement_wyoming_tts_port", "speech_announcement_wyoming_tts_voice", "speech_announcement_openai_tts_base_url", "speech_announcement_openai_tts_api_key", "speech_announcement_chatterbox_tts_base_url", "speech_announcement_chatterbox_tts_voice_mode", "speech_announcement_chatterbox_tts_chunk_size", "speech_announcement_chatterbox_tts_temperature", "speech_announcement_chatterbox_tts_exaggeration", "speech_announcement_chatterbox_tts_cfg_weight", "speech_announcement_chatterbox_tts_seed", "speech_announcement_chatterbox_tts_speed_factor", "speech_announcement_chatterbox_tts_language",
];

function speechPayload(): JsonRow {
  return { ...Object.fromEntries(speechKeys.map((key) => [key, draft[key]])), esphome_settings: clone(draft.esphome_settings || {}) };
}

function mediaPayload(kinds: Array<"vision" | "audio" | "video">): JsonRow {
  const payload: JsonRow = {};
  const targets: Array<JsonRow | null> = [];
  kinds.forEach((kind) => {
    const prefix = kind === "vision" ? "vision" : `${kind}_understanding`;
    ["mode", "provider", "api_base", "model", "api_key"].forEach((suffix) => { payload[`${prefix}_${suffix}`] = draft[`${prefix}_${suffix}`]; });
    if (kind !== "vision") payload[`${prefix}_max_seconds`] = Number(draft[`${prefix}_max_seconds`] || (kind === "audio" ? 60 : 15));
    if (String(draft[`${prefix}_mode`] || "") === "dedicated") targets.push(loadTarget(draft[`${prefix}_provider`], draft[`${prefix}_model`], { roles: [kind === "vision" ? "Image" : kind === "audio" ? "Audio" : "Video"], media_kinds: [kind] }));
  });
  if (kinds.includes("vision")) {
    payload.hydra_llama_cpp_vision_context_tokens = draft.hydra_llama_cpp_vision_context_tokens;
    payload.hydra_llama_cpp_vision_slot = draft.hydra_llama_cpp_vision_slot;
  }
  payload.hydra_local_model_load_targets = dedupeTargets(targets);
  return payload;
}

function validateMedia(kinds: Array<"vision" | "audio" | "video">): string {
  for (const kind of kinds) {
    const prefix = kind === "vision" ? "vision" : `${kind}_understanding`;
    if (String(draft[`${prefix}_mode`] || "") === "dedicated" && !String(draft[`${prefix}_model`] || "").trim()) return `Choose a dedicated ${kind === "vision" ? "image" : kind} model before saving.`;
  }
  return "";
}

async function saveStatic(payload: JsonRow, fallback: string): Promise<JsonRow> {
  const result = await postJson<JsonRow>(props.endpoints.models, payload);
  emit("changed", clone(draft) as ModelsSettingsPayload);
  const message = String(result.message || fallback);
  notice.value = message;
  emit("notify", message, "success");
  return result;
}

function clearModelWarmupTimer() {
  if (modelWarmupTimer !== null) window.clearTimeout(modelWarmupTimer);
  modelWarmupTimer = null;
}

function beginLocalModelApply() {
  clearModelWarmupTimer();
  modelWarmupPollError.value = "";
  modelWarmup.value = {
    running: true,
    ui_phase: "saving",
    progress: 2,
    items: [],
    errors: [],
    unload_before: [],
    unload_result: {},
    runtime_restart: {},
    load_models: true,
  };
  modelApplyOpen.value = true;
}

function acceptLocalModelWarmup(result: JsonRow) {
  const snapshot = result.hf_llm_warmup && typeof result.hf_llm_warmup === "object" ? result.hf_llm_warmup as JsonRow : {};
  modelWarmup.value = clone(snapshot);
  modelWarmupPollError.value = "";
  if (snapshot.running) scheduleModelWarmupPoll(450);
}

function failLocalModelApply(message: string) {
  clearModelWarmupTimer();
  modelWarmup.value = {
    ...modelWarmup.value,
    running: false,
    ui_phase: "",
    finished_ts: Date.now() / 1000,
    errors: [message],
  };
  modelWarmupPollError.value = "";
  modelApplyOpen.value = true;
}

async function pollModelWarmup() {
  modelWarmupTimer = null;
  try {
    const snapshot = await getJson<JsonRow>(props.endpoints.modelsHfWarmup);
    modelWarmup.value = snapshot;
    modelWarmupPollError.value = "";
    if (snapshot.running) scheduleModelWarmupPoll(700);
  } catch (pollError) {
    modelWarmupPollError.value = pollError instanceof Error ? pollError.message : "Live model progress is temporarily unavailable.";
    if (modelWarmup.value.running) scheduleModelWarmupPoll(1600);
  }
}

function scheduleModelWarmupPoll(delay = 700) {
  clearModelWarmupTimer();
  modelWarmupTimer = window.setTimeout(pollModelWarmup, delay);
}

async function reconnectModelWarmup() {
  try {
    const snapshot = await getJson<JsonRow>(props.endpoints.modelsHfWarmup);
    if (snapshot.running && String(snapshot.reason || "").startsWith("settings-save")) {
      modelWarmup.value = snapshot;
      modelWarmupPollError.value = "";
      modelApplyOpen.value = true;
      scheduleModelWarmupPoll(450);
    }
  } catch { /* an active save will surface polling errors after it begins */ }
}

async function runPrimaryAction() {
  if (!actionSpec.value || saving.value) return;
  saving.value = true;
  error.value = "";
  notice.value = "";
  let localApplyStarted = false;
  try {
    let result: JsonRow = {};
    if (activeTab.value === "routing") {
      const validation = validateLlm();
      if (validation) throw new Error(validation);
      beginLocalModelApply();
      localApplyStarted = true;
      result = await saveStatic(llmPayload(), "LLM settings saved and selected local models queued.");
      acceptLocalModelWarmup(result);
    }
    else if (activeTab.value === "speech") {
      result = await saveStatic(speechPayload(), "Speech settings saved.");
      await speechPanel.value?.refreshWarmup?.();
    } else if (activeTab.value === "vision") {
      const validation = validateMedia(["vision"]);
      if (validation) throw new Error(validation);
      beginLocalModelApply();
      localApplyStarted = true;
      result = await saveStatic(mediaPayload(["vision"]), "Vision settings saved.");
      acceptLocalModelWarmup(result);
    } else if (activeTab.value === "audio-understanding") {
      const validation = validateMedia(["audio", "video"]);
      if (validation) throw new Error(validation);
      beginLocalModelApply();
      localApplyStarted = true;
      result = await saveStatic(mediaPayload(["audio", "video"]), "Audio and video settings saved.");
      acceptLocalModelWarmup(result);
    }
    else if (activeTab.value === "faceid") {
      result = await saveStatic({ face_id_enabled: Boolean(faceSettings.value.enabled), face_id_model: String(faceSettings.value.model_id || "facenet512") }, "Face ID settings applied.");
      await refreshFaceStatus();
    } else if (activeTab.value === "wake") result = await wakePanel.value?.apply?.() || { ok: false, error: "Wake Word settings are not ready." };
    else if (activeTab.value === "speakerid") result = await speakerPanel.value?.apply?.() || { ok: false, error: "Speaker ID settings are not ready." };
    else if (activeTab.value === "emotionid") result = await emotionPanel.value?.apply?.() || { ok: false, error: "Emotion ID settings are not ready." };
    if (result && result.ok === false) throw new Error(String(result.error || "Settings could not be applied."));
    dirty[activeTab.value] = false;
  } catch (saveError) {
    error.value = saveError instanceof Error ? saveError.message : "Model settings could not be applied.";
    if (localApplyStarted) failLocalModelApply(error.value);
    emit("notify", error.value, "error");
  } finally { saving.value = false; }
}

function updateLocalModels(payload: JsonRow) {
  draft.local_llm_models = payload;
  const next = clone(draft) as ModelsSettingsPayload;
  emit("changed", next);
}

async function refreshFaceStatus() {
  try { faceStatus.value = await getJson<JsonRow>(props.endpoints.modelsFaceStatus); }
  catch (statusError) { error.value = statusError instanceof Error ? statusError.message : "Face ID status could not be loaded."; }
}

watch(() => props.settings, (next) => sync(next, true), { deep: true });
onMounted(() => {
  sync(props.settings);
  props.onTabChange?.(activeTab.value);
  if (activeTab.value === "faceid") void refreshFaceStatus();
  const initialWarmup = props.settings.hf_llm_warmup && typeof props.settings.hf_llm_warmup === "object" ? props.settings.hf_llm_warmup as JsonRow : {};
  if (initialWarmup.running && String(initialWarmup.reason || "").startsWith("settings-save")) {
    modelWarmup.value = clone(initialWarmup);
    modelApplyOpen.value = true;
    scheduleModelWarmupPoll(450);
  }
  void reconnectModelWarmup();
  faceTimer = window.setInterval(() => { if (activeTab.value === "faceid" && document.visibilityState === "visible") void refreshFaceStatus(); }, 5000);
});
onBeforeUnmount(() => {
  if (faceTimer !== null) window.clearInterval(faceTimer);
  clearModelWarmupTimer();
});
</script>

<template>
  <section class="tset-resource tmodels-resource tm-native-models">
    <section class="tv-panel tmodels-hero">
      <div><span class="tv-eyebrow">Model workspace</span><h2>Models, routing, and local runtimes</h2><p>Every Models area is reactive and has one clear save or apply action at its bottom. Downloads, tests, enrollment, and diagnostics remain beside the item they affect.</p></div>
      <span class="tv-live-pill"><i />{{ activeSpec.label }}</span>
    </section>

    <nav class="tv-tabs tmodels-tabs" aria-label="Model settings sections">
      <button v-for="tab in tabs" :key="tab.id" type="button" :class="{ active: activeTab === tab.id }" @click="select(tab.id)">{{ tab.label }}</button>
    </nav>

    <div class="tmodels-context"><strong>{{ activeSpec.label }}</strong><span>{{ activeSpec.short }}</span></div>
    <div v-if="error" class="tv-notice error" aria-live="polite">{{ error }}</div>
    <div v-if="notice" class="tv-notice" aria-live="polite">{{ notice }}</div>

    <HuggingFaceModels v-if="activeTab === 'huggingface'" :local-models="localModels" :endpoints="endpoints" @local-models="updateLocalModels" @notify="(message, tone) => emit('notify', message, tone)" />
    <LlmModels v-else-if="activeTab === 'routing'" :draft="draft" :local-models="localModels" :remote-models-endpoint="endpoints.modelsRemoteLlm" :context-estimate-endpoint="endpoints.modelsContextEstimate" @dirty="markDirty" />
    <SpeechModels v-else-if="activeTab === 'speech'" ref="speechPanel" :draft="draft" :speech-ui="draft.speech_ui || {}" :announcement-ui="draft.announcement_speech_ui || {}" :voice-model-ui="draft.voice_model_ui || {}" :endpoints="endpoints" @dirty="markDirty" @notify="(message, tone) => emit('notify', message, tone)" />
    <WakeWordModels v-else-if="activeTab === 'wake'" ref="wakePanel" :runtime-endpoint="endpoints.modelsVoiceRuntime" :action-endpoint="endpoints.modelsVoiceAction" @dirty="markDirty" @notify="(message, tone) => emit('notify', message, tone)" />
    <MediaModelCard v-else-if="activeTab === 'vision'" kind="vision" :draft="draft" :local-models="localModels" @dirty="markDirty" />
    <section v-else-if="activeTab === 'audio-understanding'" class="tm-stack"><MediaModelCard kind="audio" :draft="draft" :local-models="localModels" @dirty="markDirty" /><MediaModelCard kind="video" :draft="draft" :local-models="localModels" @dirty="markDirty" /></section>
    <IdentityModels v-else-if="activeTab === 'speakerid'" ref="speakerPanel" kind="speakerid" :runtime-endpoint="endpoints.modelsVoiceRuntime" :action-endpoint="endpoints.modelsVoiceAction" @dirty="markDirty" @subtab="setSpeakerSubtab" @notify="(message, tone) => emit('notify', message, tone)" />
    <IdentityModels v-else-if="activeTab === 'emotionid'" ref="emotionPanel" kind="emotionid" :runtime-endpoint="endpoints.modelsVoiceRuntime" :action-endpoint="endpoints.modelsVoiceAction" @dirty="markDirty" @notify="(message, tone) => emit('notify', message, tone)" />
    <FaceModels v-else-if="activeTab === 'faceid'" :settings="faceSettings" :status="faceStatus" :models="faceModels" :busy="saving" @dirty="markDirty" />

    <footer v-if="actionSpec" class="tset-save-bar tmodels-save-bar">
      <div><strong>{{ dirty[activeTab] ? "Unsaved changes" : `${activeSpec.label} settings are synchronized` }}</strong><span>{{ actionSpec.note }}</span></div>
      <button class="tv-button primary" type="button" :disabled="saving" @click="runPrimaryAction">{{ saving ? "Applying…" : actionSpec.label }}</button>
    </footer>

    <LocalModelApplyProgress :open="modelApplyOpen" :snapshot="modelWarmup" :poll-error="modelWarmupPollError" @close="modelApplyOpen = false" />
  </section>
</template>
