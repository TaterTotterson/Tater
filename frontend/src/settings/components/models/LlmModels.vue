<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from "vue";
import { getJson, postJson } from "../../../shared/api";
import type { JsonRow } from "../../types";

const props = defineProps<{
  draft: JsonRow;
  localModels: JsonRow;
  remoteModelsEndpoint: string;
  contextEstimateEndpoint: string;
}>();

const emit = defineEmits<{ dirty: [] }>();
const area = ref("base");
const runtimeProvider = ref("");
const remoteModels = reactive<Record<string, string[]>>({});
const remoteBusy = ref("");
const remoteError = ref("");
const contextEstimate = ref<JsonRow>({});
const contextLoading = ref(false);
const contextError = ref("");

const providerOptions = [
  { value: "openai_compatible", label: "OpenAI-Compatible", short: "Connect to an API", mark: "API", local: false },
  { value: "hf_transformers", label: "Transformers", short: "Run a local HF model", mark: "HF", local: true },
  { value: "llama_cpp", label: "llama.cpp", short: "Run a local GGUF", mark: "GG", local: true },
  { value: "llama_cpp_remote", label: "Remote llama.cpp", short: "Connect to a server", mark: "↗", local: false },
  { value: "mlx_lm", label: "MLX LM", short: "Optimized for Apple Silicon", mark: "MX", local: true },
  { value: "spud_link", label: "Paired Spud Hub", short: "Route through your hub", mark: "SP", local: false },
] as const;
const roles = [
  { id: "astraeus", label: "Astraeus", description: "Planning and orchestration." },
  { id: "thanatos", label: "Thanatos", description: "Critique, safety, and verification." },
  { id: "hermes", label: "Hermes", description: "Final response composition." },
];
const contextConfigs: Record<string, { key: string; min: number; fallback: number }> = {
  hf_transformers: { key: "hydra_hf_transformers_context_tokens", min: 256, fallback: 4096 },
  llama_cpp: { key: "hydra_llama_cpp_context_tokens", min: 256, fallback: 4096 },
  mlx_lm: { key: "hydra_mlx_lm_context_tokens", min: 128, fallback: 4096 },
};
const speculativeMethods: Record<string, { label: string; tokens: number; help: string; requiresDraft: boolean }> = {
  "draft-mtp": { label: "Multi-Token Prediction (MTP)", tokens: 3, help: "Uses prediction heads from a sidecar GGUF, or embedded heads when the main model includes them.", requiresDraft: false },
  "draft-dflash": { label: "DFlash", tokens: 15, help: "Uses llama.cpp's DFlash decoder and requires a matching DFlash draft GGUF.", requiresDraft: true },
  "draft-dspark": { label: "DSpark", tokens: 7, help: "Uses llama.cpp's DSpark decoder and requires a matching DSpark draft GGUF.", requiresDraft: true },
};

const installed = computed<JsonRow[]>(() => Array.isArray(props.localModels?.models) ? props.localModels.models : []);
const servers = computed<JsonRow[]>(() => {
  if (!Array.isArray(props.draft.hydra_base_servers) || !props.draft.hydra_base_servers.length) {
    props.draft.hydra_base_servers = [{
      provider: props.draft.hydra_llm_provider || "openai_compatible",
      host: props.draft.hydra_llm_host || "",
      port: props.draft.hydra_llm_port || "",
      model: props.draft.hydra_llm_model || "",
      api_key: props.draft.hydra_llm_api_key || "",
      llama_cpp_slot: props.draft.hydra_llama_cpp_base_slot || "",
    }];
  }
  return props.draft.hydra_base_servers as JsonRow[];
});
const selectedRuntimeProviders = computed(() => {
  const selected = new Set<string>();
  servers.value.forEach((row) => { if (local(row.provider)) selected.add(provider(row)); });
  if (local(props.draft.spudex_llm_provider)) selected.add(String(props.draft.spudex_llm_provider));
  if (props.draft.hydra_beast_mode_enabled) roles.forEach((role) => {
    const value = String(props.draft[roleKey(role.id, "provider")] || "");
    if (local(value)) selected.add(value);
  });
  return providerOptions.filter((option) => option.local && selected.has(option.value));
});
const activeRuntimeOption = computed(() => providerOptions.find((option) => option.value === runtimeProvider.value));
const activeContextConfig = computed(() => contextConfigs[runtimeProvider.value] || contextConfigs.llama_cpp);
const selectedRuntimeModel = computed(() => {
  const route = servers.value.find((row) => provider(row) === runtimeProvider.value && String(row.model || "").trim());
  if (route) return String(route.model || "").trim();
  if (String(props.draft.spudex_llm_provider || "") === runtimeProvider.value) return String(props.draft.spudex_llm_model || "").trim();
  for (const role of roles) {
    if (String(props.draft[roleKey(role.id, "provider")] || "") === runtimeProvider.value) return String(props.draft[roleKey(role.id, "model")] || "").trim();
  }
  return "";
});
const selectedRuntimeModelRow = computed<JsonRow | null>(() => modelsFor(runtimeProvider.value).find((model) => String(model.model || "") === selectedRuntimeModel.value) || null);
const activeContextMax = computed(() => Math.max(activeContextConfig.value.min, Number(selectedRuntimeModelRow.value?.max_context_tokens || 262144)));
const activeContextTokens = computed(() => {
  const raw = Number(props.draft[activeContextConfig.value.key] || activeContextConfig.value.fallback);
  return Math.max(activeContextConfig.value.min, Math.min(activeContextMax.value, Number.isFinite(raw) ? Math.round(raw) : activeContextConfig.value.fallback));
});
const contextWindow = computed<JsonRow>(() => contextEstimate.value.chat_context_window && typeof contextEstimate.value.chat_context_window === "object" ? contextEstimate.value.chat_context_window as JsonRow : {});
const contextMinimum = computed(() => Math.max(0, Number(contextWindow.value.minimum_context_window || 0)));
const contextRecommended = computed(() => Math.max(0, Number(contextWindow.value.recommended_context_window || 0)));
const contextPrompt = computed(() => Math.max(0, Number(contextWindow.value.prompt_tokens || 0)));
const contextReply = computed(() => Math.max(0, Number(contextWindow.value.completion_budget_tokens || 0)));
const contextReserve = computed(() => {
  const breakdown = contextWindow.value.breakdown && typeof contextWindow.value.breakdown === "object" ? contextWindow.value.breakdown as JsonRow : {};
  return Math.max(0, Number(contextWindow.value.capability_context_reserve_tokens ?? breakdown.capability_reserve_tokens ?? 0))
    + Math.max(0, Number(contextWindow.value.burst_context_reserve_tokens ?? breakdown.burst_reserve_tokens ?? 0));
});
const contextNeeded = computed(() => contextRecommended.value || contextPrompt.value + contextReply.value + contextReserve.value);
const contextTone = computed(() => {
  if (contextError.value || contextWindow.value.error) return "warn";
  if (!contextRecommended.value) return "idle";
  if (activeContextTokens.value < Math.max(1, contextMinimum.value)) return "danger";
  if (activeContextTokens.value < contextRecommended.value) return "warn";
  return "good";
});
const contextStatus = computed(() => contextLoading.value ? "Reading stats" : contextTone.value === "danger" ? "Too low" : contextTone.value === "warn" ? "Tight" : contextTone.value === "good" ? "Ready" : "Waiting");
const contextFitPercent = computed(() => activeContextTokens.value > 0 && contextNeeded.value > 0 ? Math.max(4, Math.min(100, (contextNeeded.value / activeContextTokens.value) * 100)) : 0);
const contextSelectedPercent = computed(() => activeContextMax.value > 0 ? Math.max(2, Math.min(100, (activeContextTokens.value / activeContextMax.value) * 100)) : 0);
const activeSpeculative = computed(() => speculativeMethods[String(props.draft.hydra_llama_cpp_speculative_method || "draft-mtp")] || speculativeMethods["draft-mtp"]);
const selectedLlamaBaseRow = computed<JsonRow | null>(() => {
  if (runtimeProvider.value !== "llama_cpp" || !selectedRuntimeModel.value) return null;
  return rawModelsFor("llama_cpp").find((model) => String(model.model || "") === selectedRuntimeModel.value) || { model: selectedRuntimeModel.value };
});
const llamaDraftModels = computed(() => {
  const base = selectedLlamaBaseRow.value;
  const method = normalizedSpeculativeMethod(props.draft.hydra_llama_cpp_speculative_method || "draft-mtp");
  if (!base || !method) return [];
  return rawModelsFor("llama_cpp").filter((model) => speculativeMethodForModel(model) === method && draftMatchesBase(model, base));
});
const selectedDraftCompatible = computed(() => {
  const selected = String(props.draft.hydra_llama_cpp_mtp_draft_model || "");
  return !selected || llamaDraftModels.value.some((model) => String(model.model || "") === selected);
});
const selectedLlamaBaseLabel = computed(() => {
  const row = selectedLlamaBaseRow.value;
  return row ? String(row.filename || row.model || "the selected base model").split("::").pop() || "the selected base model" : "";
});

watch(() => selectedRuntimeProviders.value.map((option) => option.value).join("|"), () => {
  if (!selectedRuntimeProviders.value.some((option) => option.value === runtimeProvider.value)) runtimeProvider.value = selectedRuntimeProviders.value[0]?.value || "";
}, { immediate: true });

function changed() { emit("dirty"); }
function provider(row: JsonRow): string { return String(row.provider || "openai_compatible"); }
function providerOption(value: unknown) { return providerOptions.find((option) => option.value === String(value || "")) || providerOptions[0]; }
function local(providerValue: unknown): boolean { return ["hf_transformers", "llama_cpp", "mlx_lm"].includes(String(providerValue || "")); }
function remote(providerValue: unknown): boolean { return String(providerValue || "") === "llama_cpp_remote"; }
function apiProvider(providerValue: unknown): boolean { return ["openai_compatible", "llama_cpp_remote"].includes(String(providerValue || "")); }
function llama(providerValue: unknown): boolean { return ["llama_cpp", "llama_cpp_remote"].includes(String(providerValue || "")); }
function roleKey(role: string, suffix: string) { return `hydra_llm_${role}_${suffix}`; }

function rawModelsFor(providerValue: unknown): JsonRow[] {
  return installed.value.filter((model) => String(model.provider || "") === String(providerValue || ""));
}
function normalizedSpeculativeMethod(value: unknown): string {
  const token = String(value || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
  if (["mtp", "draft-mtp", "multi-token-prediction", "multitoken-prediction"].includes(token)) return "draft-mtp";
  if (["dflash", "d-flash", "draft-dflash", "draft-d-flash"].includes(token)) return "draft-dflash";
  if (["dspark", "d-spark", "draft-dspark", "draft-d-spark"].includes(token)) return "draft-dspark";
  return "";
}
function modelIdentity(model: JsonRow): string {
  const modelValue = String(model.model || model.model_id || "");
  return String(model.filename || modelValue.split("::").pop() || model.model_path || "");
}
function speculativeMethodForModel(model: JsonRow): string {
  const explicit = normalizedSpeculativeMethod(model.speculative_method || model.speculative_draft_method || model.draft_method);
  if (explicit) return explicit;
  const identity = modelIdentity(model).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
  if (/(^|-)d-?flash(-|$)/.test(identity)) return "draft-dflash";
  if (/(^|-)d-?spark(-|$)/.test(identity)) return "draft-dspark";
  if (/(^|-)(mtp|multi-token-prediction)(-|$)/.test(identity)) return "draft-mtp";
  return "";
}
function isSpeculativeDraft(model: JsonRow): boolean {
  if (model.is_speculative_draft === true || speculativeMethodForModel(model)) return true;
  const identity = modelIdentity(model).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
  return /(^|-)(draft|sidecar|speculator|speculative)(-|$)/.test(identity);
}
function modelFamilyTokens(model: JsonRow): string[] {
  const noise = new Set(["gguf", "mtp", "dflash", "dspark", "draft", "sidecar", "speculator", "speculative", "nothink", "think", "imatrix", "ud", "k", "m", "s", "l", "xs", "xxs"]);
  return modelIdentity(model)
    .toLowerCase()
    .replace(/\.gguf$/i, "")
    .split(/[^a-z0-9]+/)
    .filter((token) => token && !noise.has(token) && !/^(?:q|iq)\d/.test(token) && !/^(?:f|fp|bf)\d+$/.test(token));
}
function draftMatchesBase(candidate: JsonRow, base: JsonRow): boolean {
  const candidateRepo = String(candidate.repo_id || "").trim().toLowerCase();
  const baseRepo = String(base.repo_id || "").trim().toLowerCase();
  const sameRepo = Boolean(candidateRepo && baseRepo && candidateRepo === baseRepo);
  const candidateTokens = modelFamilyTokens(candidate);
  const baseTokens = new Set(modelFamilyTokens(base));
  if (!candidateTokens.length || !baseTokens.size) return sameRepo;
  if (!baseTokens.has(candidateTokens[0])) return false;
  const shared = candidateTokens.filter((token) => baseTokens.has(token));
  return shared.length >= Math.min(3, candidateTokens.length) && shared.length / candidateTokens.length >= 0.75;
}
function modelsFor(providerValue: unknown): JsonRow[] {
  const rows = rawModelsFor(providerValue);
  return String(providerValue || "") === "llama_cpp" ? rows.filter((model) => !isSpeculativeDraft(model)) : rows;
}
function modelInstalled(providerValue: unknown, modelValue: unknown): boolean {
  return rawModelsFor(providerValue).some((model) => String(model.model || "") === String(modelValue || ""));
}
function clearIncompatibleDraft() {
  const selected = String(props.draft.hydra_llama_cpp_mtp_draft_model || "");
  if (selected && !llamaDraftModels.value.some((model) => String(model.model || "") === selected)) {
    props.draft.hydra_llama_cpp_mtp_draft_model = "";
  }
}
function routeModelChanged(row: JsonRow) {
  if (provider(row) === "llama_cpp") clearIncompatibleDraft();
  changed();
}
function setProvider(row: JsonRow, value: string) {
  row.provider = value;
  if (local(value)) runtimeProvider.value = value;
  changed();
}
function setDraftProvider(key: string, value: string) {
  props.draft[key] = value;
  if (local(value)) runtimeProvider.value = value;
  changed();
}
function addServer() {
  (props.draft.hydra_base_servers as JsonRow[]).push({ provider: "openai_compatible", host: "", port: "", model: "", api_key: "", llama_cpp_slot: "" });
  changed();
}
function removeServer(index: number) {
  if (servers.value.length <= 1) return;
  (props.draft.hydra_base_servers as JsonRow[]).splice(index, 1);
  changed();
}
function setSpudexMode(mode: string) {
  props.draft.spudex_llm_provider = mode === "base" ? "" : String(props.draft.spudex_llm_provider || "openai_compatible");
  changed();
}
function setContextTokens(event: Event) {
  const input = event.target as HTMLInputElement;
  const raw = Number(input.value || activeContextConfig.value.fallback);
  props.draft[activeContextConfig.value.key] = String(Math.max(activeContextConfig.value.min, Math.min(activeContextMax.value, Number.isFinite(raw) ? Math.round(raw) : activeContextConfig.value.fallback)));
  changed();
}
function setSpeculativeMethod(event: Event) {
  const method = String((event.target as HTMLSelectElement).value || "draft-mtp");
  props.draft.hydra_llama_cpp_speculative_method = method;
  props.draft.hydra_llama_cpp_mtp_draft_tokens = String(speculativeMethods[method]?.tokens || 3);
  clearIncompatibleDraft();
  changed();
}
function contextTokenLabel(value: unknown): string {
  const amount = Math.max(0, Number(value) || 0);
  if (!amount) return "unknown";
  if (amount >= 1024 && amount % 1024 === 0) return `${Math.round(amount / 1024)}k`;
  return Math.round(amount).toLocaleString();
}
function contextRecommendation(): string {
  if (contextError.value || contextWindow.value.error) return String(contextError.value || contextWindow.value.error);
  if (!contextRecommended.value) return "Tater is preparing a live estimate from the current prompt, tools, cores, and chat history.";
  if (activeContextTokens.value < contextMinimum.value) return `Set at least ${contextTokenLabel(contextMinimum.value)}. ${contextTokenLabel(contextRecommended.value)} gives Tater safer working room.`;
  if (activeContextTokens.value < contextRecommended.value) return `The current window is tight. Raise it toward ${contextTokenLabel(contextRecommended.value)} when memory allows.`;
  return `The selected ${contextTokenLabel(activeContextTokens.value)} window has room for the current prompt stack.`;
}

async function refreshContextEstimate() {
  if (!props.contextEstimateEndpoint || contextLoading.value) return;
  contextLoading.value = true;
  contextError.value = "";
  try { contextEstimate.value = await getJson<JsonRow>(props.contextEstimateEndpoint); }
  catch (error) { contextError.value = error instanceof Error ? error.message : "Context estimate could not be loaded."; }
  finally { contextLoading.value = false; }
}
async function discover(key: string, row: JsonRow) {
  remoteBusy.value = key;
  remoteError.value = "";
  try {
    const result = await postJson<JsonRow>(props.remoteModelsEndpoint, { host: row.host || "", port: row.port || "", api_key: row.api_key || "" });
    remoteModels[key] = (Array.isArray(result.models) ? result.models : [])
      .map((item: unknown) => typeof item === "object" && item ? String((item as JsonRow).id || (item as JsonRow).model || (item as JsonRow).name || "") : String(item || ""))
      .filter(Boolean);
  } catch (error) { remoteError.value = error instanceof Error ? error.message : "Remote llama.cpp models could not be loaded."; }
  finally { remoteBusy.value = ""; }
}

onMounted(() => { void refreshContextEstimate(); });
</script>

<template>
  <section class="tm-stack tm-llm-workspace">
    <nav class="tv-tabs tm-inner-tabs tm-llm-tabs" aria-label="LLM model areas">
      <button type="button" :class="{ active: area === 'base' }" @click="area = 'base'">Base model pool</button>
      <button type="button" :class="{ active: area === 'spudex' }" @click="area = 'spudex'">Spudex</button>
      <button type="button" :class="{ active: area === 'beast' }" @click="area = 'beast'">Beast mode</button>
    </nav>
    <div v-if="remoteError" class="tv-notice error">{{ remoteError }}</div>

    <template v-if="area === 'base'">
      <article class="tm-llm-route-note tm-llm-pool-note">
        <i>↻</i>
        <div><strong>Base routes share requests</strong><span>With multiple routes, normal Base calls rotate through the pool. Hydra roles stay assigned to a consistent pool member to keep their prompt caches warm. These routes are peers, not fallback-only backups.</span></div>
      </article>

      <article v-for="(row, index) in servers" :key="index" class="tm-form-card tm-llm-route-card">
        <header>
          <div><span class="tv-eyebrow">Base route {{ index + 1 }}</span><h3>{{ index === 0 ? "Choose a Base model" : "Additional Base model" }}</h3><p>{{ servers.length > 1 ? "This route shares normal Base requests with every other pool member." : "All normal Base requests use this route until another pool member is added." }}</p></div>
          <div class="tm-inline-actions"><span class="tm-llm-route-state"><i />Pool member {{ index + 1 }}</span><button v-if="index > 0" class="tv-button danger" type="button" @click="removeServer(index)">Remove</button></div>
        </header>

        <section class="tm-llm-provider-section">
          <div class="tm-llm-section-title"><div><strong>Provider</strong><span>Pick where this route runs. Only its matching settings appear below.</span></div><b>{{ providerOption(provider(row)).label }}</b></div>
          <div class="tm-llm-provider-picker" role="group" :aria-label="`Base route ${index + 1} provider`">
            <button v-for="option in providerOptions" :key="option.value" type="button" :class="{ active: provider(row) === option.value }" :aria-pressed="provider(row) === option.value" @click="setProvider(row, option.value)"><i>{{ option.mark }}</i><span><strong>{{ option.label }}</strong><small>{{ option.short }}</small></span><b aria-hidden="true">✓</b></button>
          </div>
        </section>

        <section class="tm-llm-connection-card">
          <header><div><span class="tv-eyebrow">Route settings</span><h4>{{ providerOption(provider(row)).label }}</h4></div><span :class="{ local: local(provider(row)) }">{{ local(provider(row)) ? "On this Tater" : provider(row) === 'spud_link' ? "Paired route" : "Network connection" }}</span></header>
          <div v-if="provider(row) === 'spud_link'" class="tm-llm-route-note"><i>SP</i><div><strong>No model address needed</strong><span>The paired Spud Hub chooses and runs the model for this request.</span></div></div>
          <div v-else class="tm-field-grid tm-llm-route-fields">
            <label v-if="apiProvider(provider(row))" class="tm-field tm-field-wide"><span class="tm-field-label">Host or base URL</span><input v-model="row.host" type="text" placeholder="http://127.0.0.1" @input="changed" /><small>Enter the server address; add the port separately below if needed.</small></label>
            <label v-if="apiProvider(provider(row))" class="tm-field"><span class="tm-field-label">Port</span><input v-model="row.port" type="number" min="1" max="65535" placeholder="1234" @input="changed" /></label>
            <label v-if="apiProvider(provider(row))" class="tm-field"><span class="tm-field-label">API key</span><input v-model="row.api_key" type="password" autocomplete="off" placeholder="Optional" @input="changed" /></label>
            <label v-if="local(provider(row))" class="tm-field tm-field-wide"><span class="tm-field-label">Downloaded model</span><select v-model="row.model" @change="routeModelChanged(row)"><option value="">Select a downloaded model</option><option v-if="row.model && !modelInstalled(provider(row), row.model)" :value="row.model">Current: {{ row.model }}</option><option v-for="model in modelsFor(provider(row))" :key="String(model.model)" :value="model.model">{{ model.model }}</option></select><small v-if="!modelsFor(provider(row)).length">No matching model is installed yet. Download one from Hugging Face first.</small></label>
            <label v-else class="tm-field tm-field-wide"><span class="tm-field-label">Model id or alias</span><input v-model="row.model" type="text" placeholder="Model id or server alias" @input="changed" /></label>
            <label v-if="llama(provider(row))" class="tm-field"><span class="tm-field-label">Prompt cache slot</span><input v-model="row.llama_cpp_slot" type="number" min="0" :max="Math.max(0, Number(draft.hydra_llama_cpp_slot_count || 1) - 1)" placeholder="Auto" @input="changed" /><small>Blank lets Tater assign stable slots automatically. A number pins this route to that exact slot.</small></label>
            <div v-if="remote(provider(row))" class="tm-inline-field tm-llm-discover"><button class="tv-button" type="button" :disabled="remoteBusy === `base-${index}`" @click="discover(`base-${index}`, row)">{{ remoteBusy === `base-${index}` ? "Looking…" : "Discover server models" }}</button><select v-if="remoteModels[`base-${index}`]?.length" v-model="row.model" @change="changed"><option v-for="model in remoteModels[`base-${index}`]" :key="model" :value="model">{{ model }}</option></select></div>
          </div>
        </section>
      </article>
      <button class="tv-button tm-add-row tm-llm-add-route" type="button" @click="addServer">＋ Add round-robin route</button>

      <article v-if="selectedRuntimeProviders.length" class="tm-form-card tm-llm-runtime-card">
        <header><div><span class="tv-eyebrow">Local runtime</span><h3>{{ activeRuntimeOption?.label }} settings</h3><p>Only settings for a local provider used above are shown. These apply whenever Tater loads that provider.</p></div><div v-if="selectedRuntimeProviders.length > 1" class="tm-llm-runtime-picker"><button v-for="option in selectedRuntimeProviders" :key="option.value" type="button" :class="{ active: runtimeProvider === option.value }" @click="runtimeProvider = option.value"><i>{{ option.mark }}</i>{{ option.label }}</button></div></header>

        <section class="tm-llm-context-card" :class="contextTone">
          <header><div><span class="tv-eyebrow">Context estimator</span><h4>How much working memory does this model need?</h4><p>{{ selectedRuntimeModel ? `Estimate for ${selectedRuntimeModel}` : "Choose a model above to include its detected context limit." }}</p></div><button class="tv-button" type="button" :disabled="contextLoading" @click="refreshContextEstimate">{{ contextLoading ? "Refreshing…" : "Refresh estimate" }}</button></header>
          <div class="tm-llm-context-layout">
            <div class="tm-llm-context-control"><div><strong>Context length</strong><b>{{ contextTokenLabel(activeContextTokens) }} tokens</b></div><input type="range" :min="activeContextConfig.min" :max="activeContextMax" step="256" :value="activeContextTokens" @input="setContextTokens" /><label><span>Exact token limit</span><input type="number" :min="activeContextConfig.min" :max="activeContextMax" step="256" :value="activeContextTokens" @input="setContextTokens" /></label><small>{{ selectedRuntimeModelRow?.max_context_tokens ? `Model maximum ${contextTokenLabel(activeContextMax)} from ${selectedRuntimeModelRow.context_source === 'gguf' ? 'GGUF metadata' : 'model config'}.` : `Model maximum is unknown; Tater uses a safe ${contextTokenLabel(activeContextMax)} slider cap.` }}</small></div>
            <div class="tm-llm-context-fit"><div><span><i />Context fit</span><strong>{{ contextStatus }}</strong></div><div class="tm-llm-context-meter" aria-hidden="true"><span :style="{ width: `${contextFitPercent}%` }" /><i :style="{ left: `${contextSelectedPercent}%` }" /></div><h4>{{ contextRecommended ? `Recommended: ${contextTokenLabel(contextRecommended)} tokens` : "Building a recommendation" }}</h4><p>{{ contextRecommendation() }}</p><div class="tm-llm-context-facts"><span>Prompt <b>{{ contextTokenLabel(contextPrompt) }}</b></span><span>Reply <b>{{ contextTokenLabel(contextReply) }}</b></span><span>Minimum <b>{{ contextTokenLabel(contextMinimum) }}</b></span><span>Reserve <b>{{ contextTokenLabel(contextReserve) }}</b></span></div></div>
          </div>
        </section>

        <template v-if="runtimeProvider === 'hf_transformers'">
          <section class="tm-llm-settings-section"><header><div><h4>Execution</h4><p>Choose how Transformers places and computes the model.</p></div><span class="tm-llm-section-chip">Transformers</span></header><div class="tm-field-grid">
            <label class="tm-field"><span class="tm-field-label">Device</span><select v-model="draft.hydra_hf_transformers_device" @change="changed"><option value="auto">Auto</option><option value="cuda">CUDA</option><option value="mps">Apple MPS</option><option value="cpu">CPU</option></select><small>Auto tries CUDA, Apple MPS, then CPU.</small></label>
            <label class="tm-field"><span class="tm-field-label">Precision</span><select v-model="draft.hydra_hf_transformers_dtype" @change="changed"><option value="auto">Auto</option><option value="float16">Float16</option><option value="bfloat16">BFloat16</option><option value="float32">Float32</option></select><small>Auto uses the model's recommended data type.</small></label>
            <label class="tm-field"><span class="tm-field-label">Device map</span><select v-model="draft.hydra_hf_transformers_device_map" @change="changed"><option value="default">Default</option><option value="auto">Auto</option><option value="balanced">Balanced</option><option value="disabled">Disabled</option></select><small>Controls how layers are distributed across devices.</small></label>
            <label class="tm-field"><span class="tm-field-label">Attention</span><select v-model="draft.hydra_hf_transformers_attn_implementation" @change="changed"><option value="auto">Auto</option><option value="sdpa">SDPA</option><option value="flash_attention_2">Flash Attention 2</option><option value="eager">Eager</option></select><small>Use Auto unless a model requires a specific implementation.</small></label>
          </div></section>
          <label class="tm-llm-switch"><input v-model="draft.hydra_hf_transformers_trust_remote_code" type="checkbox" @change="changed" /><span><strong>Trust remote model code</strong><small>Allow custom Python code from the selected repository. Enable only for models you trust.</small></span><b>{{ draft.hydra_hf_transformers_trust_remote_code ? "On" : "Off" }}</b></label>
        </template>

        <template v-else-if="runtimeProvider === 'llama_cpp'">
          <section class="tm-llm-settings-section"><header><div><h4>Performance</h4><p>Start with the defaults, then tune only when you need more throughput or concurrent slots.</p></div><span class="tm-llm-section-chip">GGUF</span></header><div class="tm-field-grid">
            <label class="tm-field"><span class="tm-field-label">Concurrent slots</span><input v-model="draft.hydra_llama_cpp_slot_count" type="number" min="1" max="32" @input="changed" /><small>How many llama.cpp requests can run at once.</small></label>
            <label class="tm-field"><span class="tm-field-label">Evaluation batch</span><input v-model="draft.hydra_llama_cpp_n_batch" type="number" min="32" max="8192" step="32" @input="changed" /><small>Higher can speed prompt processing when memory allows.</small></label>
            <label class="tm-field"><span class="tm-field-label">Micro-batch</span><input v-model="draft.hydra_llama_cpp_n_ubatch" type="number" min="0" max="8192" step="32" placeholder="Auto" @input="changed" /><small>0 lets llama.cpp choose, usually matching the evaluation batch.</small></label>
          </div><div class="tm-llm-switch-grid">
            <label class="tm-llm-switch"><input v-model="draft.hydra_llama_cpp_flash_attn" type="checkbox" @change="changed" /><span><strong>Flash attention</strong><small>Faster attention when supported by the model and backend.</small></span><b>{{ draft.hydra_llama_cpp_flash_attn ? "On" : "Off" }}</b></label>
            <label class="tm-llm-switch"><input v-model="draft.hydra_llama_cpp_offload_kqv" type="checkbox" @change="changed" /><span><strong>GPU KV offload</strong><small>Keep attention and KV-cache work on the GPU when supported.</small></span><b>{{ draft.hydra_llama_cpp_offload_kqv ? "On" : "Off" }}</b></label>
          </div></section>

          <section class="tm-llm-speculative" :class="{ enabled: draft.hydra_llama_cpp_mtp_enabled }">
            <header><div><span class="tv-eyebrow">Speed boost</span><h4>Speculative decoding</h4><p>A fast draft predicts tokens and the main model verifies them. It can improve generation speed with a compatible model pair.</p></div><label class="tm-llm-feature-toggle"><input v-model="draft.hydra_llama_cpp_mtp_enabled" type="checkbox" @change="changed" /><span><i /></span><b>{{ draft.hydra_llama_cpp_mtp_enabled ? "Enabled" : "Disabled" }}</b></label></header>
            <div v-if="draft.hydra_llama_cpp_mtp_enabled" class="tm-llm-speculative-settings">
              <label class="tm-field"><span class="tm-field-label">Draft method</span><select v-model="draft.hydra_llama_cpp_speculative_method" @change="setSpeculativeMethod"><option value="draft-mtp">Multi-Token Prediction (MTP)</option><option value="draft-dflash">DFlash</option><option value="draft-dspark">DSpark</option></select><small>{{ activeSpeculative.help }}</small></label>
              <label class="tm-field tm-llm-draft-model"><span class="tm-field-label">Draft model (GGUF)</span><select v-model="draft.hydra_llama_cpp_mtp_draft_model" @change="changed"><option value="">{{ activeSpeculative.requiresDraft ? "Choose a compatible draft model" : "Embedded heads / no sidecar" }}</option><option v-for="model in llamaDraftModels" :key="String(model.model)" :value="model.model">{{ model.filename || model.model }}</option></select><small v-if="!selectedLlamaBaseRow">Choose a llama.cpp base model above first.</small><small v-else-if="llamaDraftModels.length" class="tm-llm-draft-match good">{{ llamaDraftModels.length }} compatible {{ activeSpeculative.label }} {{ llamaDraftModels.length === 1 ? "model" : "models" }} for {{ selectedLlamaBaseLabel }}.</small><small v-else class="tm-llm-draft-match warn">No compatible {{ activeSpeculative.label }} sidecar is installed for {{ selectedLlamaBaseLabel }}.</small><small v-if="!selectedDraftCompatible" class="tm-llm-draft-match warn">The previous draft did not match this method or base model and will not be used.</small></label>
              <label class="tm-field tm-llm-draft-tokens"><span class="tm-field-label">Maximum draft tokens</span><div><input v-model="draft.hydra_llama_cpp_mtp_draft_tokens" type="range" min="1" max="16" step="1" @input="changed" /><input v-model="draft.hydra_llama_cpp_mtp_draft_tokens" type="number" min="1" max="16" step="1" @input="changed" /></div><small>{{ activeSpeculative.label }} recommends {{ activeSpeculative.tokens }}.</small></label>
            </div>
            <div v-else class="tm-llm-feature-off"><i>↗</i><span><strong>Optional advanced feature</strong><small>Leave this off for the simplest, most compatible llama.cpp setup.</small></span></div>
          </section>
        </template>

        <template v-else-if="runtimeProvider === 'mlx_lm'">
          <section class="tm-llm-settings-section"><header><div><h4>Memory and loading</h4><p>Tune MLX prefill and KV cache use for Apple Silicon.</p></div><span class="tm-llm-section-chip">Apple Silicon</span></header><div class="tm-field-grid">
            <label class="tm-field"><span class="tm-field-label">Prefill step size</span><input v-model="draft.hydra_mlx_engine_prefill_step_size" type="number" min="1" max="32768" placeholder="Auto" @input="changed" /><small>Blank lets Tater choose from available Mac memory.</small></label>
            <label class="tm-field"><span class="tm-field-label">Quantized KV bits</span><select v-model="draft.hydra_mlx_engine_kv_bits" @change="changed"><option value="">Auto</option><option v-for="bits in ['2','3','4','6','8']" :key="bits" :value="bits">{{ bits }}-bit</option></select><small>Lower values reduce memory use at a possible quality cost.</small></label>
            <label class="tm-field"><span class="tm-field-label">KV group size</span><select v-model="draft.hydra_mlx_engine_kv_group_size" @change="changed"><option value="">Auto</option><option value="32">32</option><option value="64">64</option><option value="128">128</option></select><small>Auto uses the MLX runtime default.</small></label>
            <label class="tm-field"><span class="tm-field-label">Quantized KV start</span><input v-model="draft.hydra_mlx_engine_quantized_kv_start" type="number" min="0" placeholder="Auto" @input="changed" /><small>The token index where quantized KV begins.</small></label>
          </div><div class="tm-llm-switch-grid">
            <label class="tm-llm-switch"><input v-model="draft.hydra_mlx_lm_lazy_load" type="checkbox" @change="changed" /><span><strong>Lazy loading</strong><small>Defer some weight materialization while loading the model.</small></span><b>{{ draft.hydra_mlx_lm_lazy_load ? "On" : "Off" }}</b></label>
            <label class="tm-llm-switch"><input v-model="draft.hydra_mlx_lm_trust_remote_code" type="checkbox" @change="changed" /><span><strong>Trust remote model code</strong><small>Allow custom tokenizer or config code from trusted repositories.</small></span><b>{{ draft.hydra_mlx_lm_trust_remote_code ? "On" : "Off" }}</b></label>
          </div></section>
        </template>
      </article>

      <article v-else class="tm-form-card tm-llm-no-runtime"><i>✓</i><div><h3>No local runtime to tune</h3><p>The selected routes run on an API, remote server, or paired Spud Hub. Their connection settings are already shown above.</p></div></article>
    </template>

    <template v-else-if="area === 'spudex'">
      <article class="tm-form-card tm-llm-special-card"><header><div><span class="tv-eyebrow">Spudex model</span><h3>Choose how Spudex thinks</h3><p>Share Tater's Base route for a simple setup, or give coding work a dedicated model.</p></div></header><div class="tm-llm-mode-picker"><button type="button" :class="{ active: !draft.spudex_llm_provider }" @click="setSpudexMode('base')"><i>1</i><span><strong>Use Base model</strong><small>Recommended for most setups</small></span></button><button type="button" :class="{ active: Boolean(draft.spudex_llm_provider) }" @click="setSpudexMode('dedicated')"><i>2</i><span><strong>Dedicated model</strong><small>Separate model just for Spudex</small></span></button></div></article>
      <article v-if="draft.spudex_llm_provider" class="tm-form-card tm-llm-special-card"><header><div><h3>Dedicated Spudex route</h3><p>Only settings for the selected provider are shown.</p></div></header><div class="tm-llm-provider-picker compact" role="group" aria-label="Spudex provider"><button v-for="option in providerOptions" :key="option.value" type="button" :class="{ active: draft.spudex_llm_provider === option.value }" @click="setDraftProvider('spudex_llm_provider', option.value)"><i>{{ option.mark }}</i><span><strong>{{ option.label }}</strong><small>{{ option.short }}</small></span><b aria-hidden="true">✓</b></button></div><div class="tm-field-grid">
        <label v-if="apiProvider(draft.spudex_llm_provider)" class="tm-field tm-field-wide"><span class="tm-field-label">Host or base URL</span><input v-model="draft.spudex_llm_host" type="text" placeholder="http://127.0.0.1:1234" @input="changed" /></label>
        <label v-if="local(draft.spudex_llm_provider)" class="tm-field tm-field-wide"><span class="tm-field-label">Downloaded model</span><select v-model="draft.spudex_llm_model" @change="changed"><option value="">Select a downloaded model</option><option v-if="draft.spudex_llm_model && !modelInstalled(draft.spudex_llm_provider, draft.spudex_llm_model)" :value="draft.spudex_llm_model">Current: {{ draft.spudex_llm_model }}</option><option v-for="model in modelsFor(draft.spudex_llm_provider)" :key="String(model.model)" :value="model.model">{{ model.model }}</option></select></label>
        <label v-else-if="draft.spudex_llm_provider !== 'spud_link'" class="tm-field tm-field-wide"><span class="tm-field-label">Model id or alias</span><input v-model="draft.spudex_llm_model" type="text" @input="changed" /></label>
      </div></article>
    </template>

    <template v-else>
      <article class="tm-form-card tm-llm-beast-hero"><header><div><span class="tv-eyebrow">Advanced routing</span><h3>Beast Mode</h3><p>Give planning, critique, and final responses their own models. Keep this disabled unless you intentionally run a multi-model setup.</p></div><label class="tm-llm-feature-toggle"><input v-model="draft.hydra_beast_mode_enabled" type="checkbox" @change="changed" /><span><i /></span><b>{{ draft.hydra_beast_mode_enabled ? "Enabled" : "Disabled" }}</b></label></header></article>
      <article v-for="role in roles" :key="role.id" class="tm-form-card tm-llm-role-card" :class="{ muted: !draft.hydra_beast_mode_enabled }"><header><div><span class="tv-eyebrow">Beast role</span><h3>{{ role.label }}</h3><p>{{ role.description }}</p></div><span class="tm-llm-section-chip">{{ providerOption(draft[roleKey(role.id, 'provider')]).label }}</span></header><div class="tm-llm-provider-picker compact" role="group" :aria-label="`${role.label} provider`"><button v-for="option in providerOptions" :key="option.value" type="button" :disabled="!draft.hydra_beast_mode_enabled" :class="{ active: draft[roleKey(role.id, 'provider')] === option.value }" @click="setDraftProvider(roleKey(role.id, 'provider'), option.value)"><i>{{ option.mark }}</i><span><strong>{{ option.label }}</strong><small>{{ option.short }}</small></span><b aria-hidden="true">✓</b></button></div><div class="tm-field-grid">
        <label v-if="apiProvider(draft[roleKey(role.id, 'provider')])" class="tm-field tm-field-wide"><span class="tm-field-label">Host or base URL</span><input v-model="draft[roleKey(role.id, 'host')]" type="text" @input="changed" /></label>
        <label v-if="apiProvider(draft[roleKey(role.id, 'provider')])" class="tm-field"><span class="tm-field-label">Port</span><input v-model="draft[roleKey(role.id, 'port')]" type="number" min="1" max="65535" @input="changed" /></label>
        <label v-if="apiProvider(draft[roleKey(role.id, 'provider')])" class="tm-field"><span class="tm-field-label">API key</span><input v-model="draft[roleKey(role.id, 'api_key')]" type="password" @input="changed" /></label>
        <label v-if="local(draft[roleKey(role.id, 'provider')])" class="tm-field tm-field-wide"><span class="tm-field-label">Downloaded model</span><select v-model="draft[roleKey(role.id, 'model')]" @change="changed"><option value="">Select a downloaded model</option><option v-if="draft[roleKey(role.id, 'model')] && !modelInstalled(draft[roleKey(role.id, 'provider')], draft[roleKey(role.id, 'model')])" :value="draft[roleKey(role.id, 'model')]">Current: {{ draft[roleKey(role.id, 'model')] }}</option><option v-for="model in modelsFor(draft[roleKey(role.id, 'provider')])" :key="String(model.model)" :value="model.model">{{ model.model }}</option></select></label>
        <label v-else-if="draft[roleKey(role.id, 'provider')] !== 'spud_link'" class="tm-field tm-field-wide"><span class="tm-field-label">Model id or alias</span><input v-model="draft[roleKey(role.id, 'model')]" type="text" @input="changed" /></label>
        <label v-if="llama(draft[roleKey(role.id, 'provider')])" class="tm-field"><span class="tm-field-label">Prompt cache slot</span><input v-model="draft[roleKey(role.id, 'llama_cpp_slot')]" type="number" min="0" :max="Math.max(0, Number(draft.hydra_llama_cpp_slot_count || 1) - 1)" placeholder="Auto" @input="changed" /><small>Blank gives this role a stable automatic slot. A number pins it to that exact slot.</small></label>
      </div></article>
    </template>
  </section>
</template>
