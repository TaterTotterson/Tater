<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import PopupTransition from "../../../shared/PopupTransition.vue";
import { getJson, postJson } from "../../../shared/api";
import type { JsonRow } from "../../types";

interface DownloadChoice {
  key: string;
  provider: string;
  providerLabel: string;
  repoId: string;
  modelId: string;
  filename: string;
  task: string;
  sizeLabel: string;
}

const props = defineProps<{
  localModels: JsonRow;
  endpoints: {
    modelsHuggingFace: string;
    modelsHuggingFaceDetail: string;
    modelsHuggingFaceDownload: string;
    modelsLocalLlm: string;
    modelsLocalLlmDelete: string;
    modelsHfWarmup: string;
    modelsHfWarmupCancel: string;
  };
}>();

const emit = defineEmits<{
  notify: [message: string, tone?: string];
  localModels: [payload: JsonRow];
}>();

const provider = ref("llama_cpp");
const view = ref("picks");
const task = ref("text-generation");
const query = ref("");
const loading = ref(false);
const detailLoading = ref(false);
const batchStarting = ref(false);
const addingRepo = ref("");
const error = ref("");
const results = ref<JsonRow[]>([]);
const selected = ref<JsonRow | null>(null);
const selectedProvider = ref("hf_transformers");
const selectedTask = ref("text-generation");
const selectedFiles = ref<string[]>([]);
const detailOpen = ref(false);
const nextCursor = ref("");
const integration = ref<JsonRow>({});
const downloadList = ref<DownloadChoice[]>([]);
const warmup = ref<JsonRow>({});
let warmupTimer: number | null = null;
let queuedStartTimer: number | null = null;
let searchSequence = 0;

const providers = [
  { value: "hf_transformers", label: "Transformers", help: "Full Hugging Face repositories for broad compatibility." },
  { value: "llama_cpp", label: "llama.cpp / GGUF", help: "Quantized GGUF files. Usually the best fit for local Tater models." },
  { value: "mlx_lm", label: "MLX LM", help: "Apple Silicon-optimized repositories for MLX." },
];
const tasks = [
  { value: "text-generation", label: "Text / chat" },
  { value: "image-text-to-text", label: "Vision / images" },
  { value: "audio-text-to-text", label: "Audio understanding" },
  { value: "video-text-to-text", label: "Video understanding" },
];
const views = [
  { value: "picks", label: "Tater Picks" },
  { value: "trending", label: "Trending" },
  { value: "downloads", label: "Most downloaded" },
  { value: "new", label: "New" },
];

const installed = computed<JsonRow[]>(() => Array.isArray(props.localModels?.models) ? props.localModels.models : []);
const files = computed<JsonRow[]>(() => selected.value && Array.isArray(selected.value.files) ? selected.value.files : []);
const detailModel = computed<JsonRow>(() => selected.value?.model && typeof selected.value.model === "object" ? selected.value.model as JsonRow : {});
const ggufFiles = computed(() => files.value.filter((file) => String(file.name || file.path || "").toLowerCase().endsWith(".gguf") && !String(file.name || file.path || "").toLowerCase().includes("mmproj")));
const mmprojFiles = computed(() => files.value.filter((file) => String(file.name || file.path || "").toLowerCase().includes("mmproj")));
const warmupItems = computed<JsonRow[]>(() => Array.isArray(warmup.value.items) ? warmup.value.items : []);
const selectedCount = computed(() => downloadList.value.length);
const completedCount = computed(() => warmupItems.value.filter((item) => ["downloaded", "loaded"].includes(String(item.status || "").toLowerCase())).length);
const activeDownload = computed(() => warmupItems.value.find((item) => Boolean(item.active)) || warmupItems.value.find((item) => !["downloaded", "loaded", "error", "cancelled", "canceled"].includes(String(item.status || "").toLowerCase())));
const detailQueueItems = computed(() => downloadList.value.filter((item) => item.provider === selectedProvider.value && item.repoId === modelRepo(detailModel.value)));
const detailQueued = computed(() => detailQueueItems.value.length > 0);
const detailRepositoryInstalled = computed(() => selectedProvider.value !== "llama_cpp" && isTargetInstalled(selectedProvider.value, modelRepo(detailModel.value)));

function params(extra: Record<string, string> = {}) {
  return new URLSearchParams({ provider: provider.value, view: view.value, query: query.value.trim(), task: task.value, limit: "24", ...extra }).toString();
}

function modelRepo(model: JsonRow): string {
  return String(model.repo_id || model.id || model.model_id || "").trim();
}

function modelName(model: JsonRow): string {
  return String(model.name || modelRepo(model) || "Unknown model");
}

function normalizeProvider(value: unknown): string {
  const token = String(value || "").trim().toLowerCase().replaceAll("-", "_").replaceAll(" ", "_");
  if (["llama", "llamacpp", "llama_cpp", "llama.cpp", "gguf"].includes(token) || token.includes("gguf") || token.includes("llama.cpp")) return "llama_cpp";
  if (["mlx", "mlx_lm", "mlxlm", "apple_mlx", "apple_silicon"].includes(token) || token.includes("mlx")) return "mlx_lm";
  if (["hf", "huggingface", "hugging_face", "transformers", "hf_transformers", "local_transformers"].includes(token) || token.includes("transformers")) return "hf_transformers";
  return token;
}

function modelProvider(model: JsonRow): string {
  return normalizeProvider(model.provider || provider.value || "hf_transformers");
}

function providerLabel(value: string): string {
  const providerValue = normalizeProvider(value);
  return providers.find((option) => option.value === providerValue)?.label || value;
}

function choiceKey(providerValue: string, modelId: string): string {
  return `${providerValue}|${modelId}`;
}

function modelIsSelected(model: JsonRow): boolean {
  const providerValue = modelProvider(model);
  const repoId = modelRepo(model);
  return downloadList.value.some((item) => item.provider === providerValue && item.repoId === repoId);
}

function installedProvider(model: JsonRow): string {
  return String(model.provider || "").trim();
}

function installedModelId(model: JsonRow): string {
  return String(model.model || "").trim();
}

function installedRepoId(model: JsonRow): string {
  const explicit = String(model.repo_id || "").trim();
  if (explicit) return explicit;
  return installedModelId(model).split("::", 1)[0];
}

function installedFilename(model: JsonRow): string {
  const explicit = String(model.filename || "").trim();
  if (explicit) return explicit;
  const modelId = installedModelId(model);
  return modelId.includes("::") ? modelId.slice(modelId.indexOf("::") + 2) : "";
}

function isTargetInstalled(providerValue: string, repoId: string, filename = ""): boolean {
  const modelId = providerValue === "llama_cpp" && filename ? `${repoId}::${filename}` : repoId;
  return installed.value.some((model) => {
    if (installedProvider(model) !== providerValue) return false;
    if (installedModelId(model) === modelId) return true;
    if (installedRepoId(model) !== repoId) return false;
    return providerValue !== "llama_cpp" || Boolean(filename && installedFilename(model) === filename);
  });
}

function installedCount(model: JsonRow): number {
  const providerValue = modelProvider(model);
  const repoId = modelRepo(model);
  return installed.value.filter((item) => installedProvider(item) === providerValue && installedRepoId(item) === repoId).length;
}

function installedLabel(model: JsonRow): string {
  const count = installedCount(model);
  if (!count) return "";
  return modelProvider(model) === "llama_cpp" ? `${count} file${count === 1 ? "" : "s"} installed` : "Installed";
}

function directFilename(model: JsonRow): string {
  return modelProvider(model) === "llama_cpp" ? String(model.preferred_gguf || "") : "";
}

function directTargetInstalled(model: JsonRow): boolean {
  const providerValue = modelProvider(model);
  const repoId = modelRepo(model);
  const filename = directFilename(model);
  return Boolean(repoId && (providerValue !== "llama_cpp" || filename) && isTargetInstalled(providerValue, repoId, filename));
}

function fileInstalled(file: JsonRow): boolean {
  return isTargetInstalled(selectedProvider.value, modelRepo(detailModel.value), String(file.name || file.path || ""));
}

function toggleSelectedFile(filename: string) {
  if (!filename || isTargetInstalled(selectedProvider.value, modelRepo(detailModel.value), filename)) return;
  selectedFiles.value = selectedFiles.value.includes(filename)
    ? selectedFiles.value.filter((value) => value !== filename)
    : [...selectedFiles.value, filename];
}

function formatCount(value: unknown): string {
  const count = Math.max(0, Number(value) || 0);
  if (count >= 1_000_000) return `${(count / 1_000_000).toFixed(1)}M`;
  if (count >= 1_000) return `${(count / 1_000).toFixed(1)}K`;
  return Math.round(count).toLocaleString();
}

function formatBytes(value: unknown): string {
  let bytes = Math.max(0, Number(value) || 0);
  if (!bytes) return "";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let index = 0;
  while (bytes >= 1024 && index < units.length - 1) { bytes /= 1024; index += 1; }
  return `${bytes.toFixed(index > 1 && bytes < 10 ? 1 : 0)} ${units[index]}`;
}

function formatDuration(value: unknown): string {
  const seconds = Math.max(0, Math.round(Number(value) || 0));
  if (!seconds) return "";
  if (seconds < 60) return `${seconds}s left`;
  if (seconds < 3600) return `${Math.ceil(seconds / 60)}m left`;
  return `${Math.floor(seconds / 3600)}h ${Math.ceil((seconds % 3600) / 60)}m left`;
}

function taskLabel(value: unknown): string {
  return tasks.find((option) => option.value === String(value || ""))?.label || "Model";
}

function hubUrl(repoId: string): string {
  const path = repoId.split("/").filter(Boolean).map((part) => encodeURIComponent(part)).join("/");
  return path ? `https://huggingface.co/${path}` : "https://huggingface.co/models";
}

function downloadMeta(item: JsonRow): string {
  const parts: string[] = [];
  const completed = formatBytes(item.download_bytes || item.current_bytes);
  const total = formatBytes(item.download_total_bytes || item.current_total_bytes);
  const speed = formatBytes(item.download_speed_bytes_per_sec || item.current_speed_bytes_per_sec);
  const eta = formatDuration(item.download_eta_seconds || item.current_eta_seconds);
  if (completed && total) parts.push(`${completed} of ${total}`);
  if (speed) parts.push(`${speed}/s`);
  if (eta) parts.push(eta);
  return parts.join(" · ") || String(item.message || item.status || "Waiting");
}

function downloadState(item: JsonRow): string {
  const status = String(item.status || "pending").toLowerCase();
  if (status === "downloaded" || status === "loaded") return "Downloaded";
  if (status === "error") return "Failed";
  if (status === "cancelled" || status === "canceled") return "Cancelled";
  if (status === "cancelling") return "Cancelling";
  if (status === "pending") return "Waiting";
  return item.active ? "Downloading" : status.charAt(0).toUpperCase() + status.slice(1);
}

function downloadTone(item: JsonRow): string {
  const status = String(item.status || "").toLowerCase();
  if (["downloaded", "loaded"].includes(status)) return "success";
  if (status === "error") return "error";
  if (["cancelled", "canceled", "cancelling"].includes(status)) return "cancelled";
  return item.active ? "active" : "pending";
}

async function search(append = false) {
  const sequence = ++searchSequence;
  loading.value = true;
  error.value = "";
  try {
    const extra: Record<string, string> = append && nextCursor.value ? { cursor: nextCursor.value } : {};
    const response = await getJson<JsonRow>(`${props.endpoints.modelsHuggingFace}?${params(extra)}`);
    if (sequence !== searchSequence) return;
    const rows = Array.isArray(response.models) ? response.models : [];
    results.value = append ? [...results.value, ...rows] : rows;
    nextCursor.value = String(response.next_cursor || "");
    integration.value = response.integration && typeof response.integration === "object" ? response.integration as JsonRow : {};
  } catch (searchError) {
    if (sequence === searchSequence) error.value = searchError instanceof Error ? searchError.message : "Hugging Face models could not be loaded.";
  } finally {
    if (sequence === searchSequence) loading.value = false;
  }
}

async function fetchModelDetail(model: JsonRow): Promise<JsonRow> {
  const repoId = modelRepo(model);
  const providerValue = modelProvider(model);
  if (!repoId) throw new Error("This model does not include a Hugging Face repository id.");
  return getJson<JsonRow>(`${props.endpoints.modelsHuggingFaceDetail}?${new URLSearchParams({ repo_id: repoId, provider: providerValue })}`);
}

async function openModel(model: JsonRow) {
  selected.value = null;
  selectedProvider.value = modelProvider(model);
  selectedTask.value = task.value;
  selectedFiles.value = [];
  detailOpen.value = true;
  detailLoading.value = true;
  error.value = "";
  try {
    selected.value = await fetchModelDetail(model);
    const repoId = modelRepo(model);
    const queuedFiles = downloadList.value
      .filter((item) => item.provider === selectedProvider.value && item.repoId === repoId && item.filename)
      .map((item) => item.filename);
    const preferred = String(selected.value.preferred_gguf || "");
    selectedFiles.value = queuedFiles.length
      ? queuedFiles
      : preferred && !isTargetInstalled(selectedProvider.value, repoId, preferred)
        ? [preferred]
        : [];
  } catch (detailError) {
    error.value = detailError instanceof Error ? detailError.message : "Model details could not be loaded.";
    detailOpen.value = false;
  } finally {
    detailLoading.value = false;
  }
}

function closeDetail() {
  detailOpen.value = false;
}

function addChoice(model: JsonRow, providerValue: string, taskValue: string, filename = "", detail: JsonRow | null = null) {
  const repoId = modelRepo(model);
  if (!repoId) return;
  const providerToken = normalizeProvider(providerValue);
  const modelId = providerToken === "llama_cpp" && filename ? `${repoId}::${filename}` : repoId;
  const choice: DownloadChoice = {
    key: choiceKey(providerToken, modelId),
    provider: providerToken,
    providerLabel: providerLabel(providerToken),
    repoId,
    modelId,
    filename,
    task: taskValue,
    sizeLabel: String(model.size_label || model.model_size || detail?.model_size || ""),
  };
  const index = downloadList.value.findIndex((item) => item.key === choice.key);
  if (index >= 0) downloadList.value.splice(index, 1, choice);
  else downloadList.value.push(choice);
  error.value = "";
  scheduleQueuedDownload();
}

function removeChoice(key: string) {
  downloadList.value = downloadList.value.filter((item) => item.key !== key);
  if (!downloadList.value.length && queuedStartTimer !== null) {
    window.clearTimeout(queuedStartTimer);
    queuedStartTimer = null;
  }
}

function removeRepoChoices(providerValue: string, repoId: string) {
  downloadList.value = downloadList.value.filter((item) => item.provider !== providerValue || item.repoId !== repoId);
}

function clearDownloadQueue() {
  downloadList.value = [];
  if (queuedStartTimer !== null) window.clearTimeout(queuedStartTimer);
  queuedStartTimer = null;
}

function scheduleQueuedDownload(delay = 180) {
  if (queuedStartTimer !== null) window.clearTimeout(queuedStartTimer);
  queuedStartTimer = window.setTimeout(() => {
    queuedStartTimer = null;
    void downloadSelected();
  }, delay);
}

async function toggleModel(model: JsonRow) {
  const providerValue = modelProvider(model);
  const repoId = modelRepo(model);
  if (modelIsSelected(model)) {
    removeRepoChoices(providerValue, repoId);
    return;
  }
  if (directTargetInstalled(model)) {
    await openModel(model);
    return;
  }
  if (providerValue !== "llama_cpp") {
    addChoice(model, providerValue, task.value);
    return;
  }

  const knownFile = String(model.preferred_gguf || "");
  if (knownFile) {
    addChoice(model, providerValue, task.value, knownFile);
    return;
  }

  addingRepo.value = repoId;
  error.value = "";
  try {
    const detail = await fetchModelDetail(model);
    const detailRow = detail.model && typeof detail.model === "object" ? detail.model as JsonRow : model;
    const filename = String(detail.preferred_gguf || "");
    if (!filename || isTargetInstalled(providerValue, repoId, filename)) {
      selected.value = detail;
      selectedProvider.value = providerValue;
      selectedTask.value = task.value;
      selectedFiles.value = [];
      detailOpen.value = true;
      return;
    }
    addChoice(detailRow, providerValue, task.value, filename, detail);
  } catch (selectionError) {
    error.value = selectionError instanceof Error ? selectionError.message : "The model could not be selected.";
  } finally {
    addingRepo.value = "";
  }
}

function queueFromDetail() {
  if (!selected.value) return;
  const repoId = modelRepo(detailModel.value);
  if (!repoId) return;
  if (selectedProvider.value === "llama_cpp" && !selectedFiles.value.length) {
    error.value = "Choose at least one GGUF file before adding this model.";
    return;
  }
  removeRepoChoices(selectedProvider.value, repoId);
  if (selectedProvider.value === "llama_cpp") {
    selectedFiles.value.forEach((filename) => {
      if (!isTargetInstalled(selectedProvider.value, repoId, filename)) addChoice(detailModel.value, selectedProvider.value, selectedTask.value, filename, selected.value);
    });
  } else if (!isTargetInstalled(selectedProvider.value, repoId)) {
    addChoice(detailModel.value, selectedProvider.value, selectedTask.value, "", selected.value);
  }
  closeDetail();
}

async function downloadSelected() {
  if (!downloadList.value.length || batchStarting.value || warmup.value.running) return;
  batchStarting.value = true;
  error.value = "";
  try {
    const queued = downloadList.value.slice(0, 32);
    const queuedKeys = new Set(queued.map((item) => item.key));
    const result = await postJson<JsonRow>(props.endpoints.modelsHuggingFaceDownload, {
      items: queued.map((item) => ({
        provider: normalizeProvider(item.provider),
        repo_id: item.repoId,
        model_id: item.modelId,
        filename: item.filename,
        task: item.task,
      })),
    });
    if (result.already_running) {
      warmup.value = result;
      scheduleWarmup();
      return;
    }
    if (!result.started) throw new Error("The download batch could not be started.");
    warmup.value = result;
    downloadList.value = downloadList.value.filter((item) => !queuedKeys.has(item.key));
    const count = Number(result.queued_count || queued.length);
    emit("notify", `${count} model file${count === 1 ? "" : "s"} started.`, "success");
    scheduleWarmup();
  } catch (downloadError) {
    error.value = downloadError instanceof Error ? downloadError.message : "The model downloads could not start.";
    emit("notify", error.value, "error");
  } finally {
    batchStarting.value = false;
  }
}

async function refreshLocalModels() {
  try {
    emit("localModels", await getJson<JsonRow>(props.endpoints.modelsLocalLlm));
  } catch { /* the active action already reports failures */ }
}

async function deleteModel(model: JsonRow) {
  const name = String(model.model || "this model");
  if (!window.confirm(`Delete ${name} from local model storage?`)) return;
  try {
    const result = await postJson<JsonRow>(props.endpoints.modelsLocalLlmDelete, { provider: model.provider, model: model.model });
    if (result.local_llm_models && typeof result.local_llm_models === "object") emit("localModels", result.local_llm_models as JsonRow);
    else await refreshLocalModels();
    emit("notify", `${name} deleted.`, "success");
  } catch (deleteError) {
    error.value = deleteError instanceof Error ? deleteError.message : "Local model could not be deleted.";
  }
}

async function pollWarmup() {
  try {
    warmup.value = await getJson<JsonRow>(props.endpoints.modelsHfWarmup);
    if (warmup.value.running) scheduleWarmup();
    else {
      await refreshLocalModels();
      warmupTimer = null;
      if (downloadList.value.length) scheduleQueuedDownload();
    }
  } catch { warmupTimer = null; }
}

function scheduleWarmup() {
  if (warmupTimer !== null) window.clearTimeout(warmupTimer);
  warmupTimer = window.setTimeout(pollWarmup, 1200);
}

async function cancelWarmup(item?: JsonRow) {
  try {
    warmup.value = await postJson<JsonRow>(props.endpoints.modelsHfWarmupCancel, item ? { key: item.key } : { cancel_all: true });
    if (warmup.value.running) scheduleWarmup();
  } catch (cancelError) {
    error.value = cancelError instanceof Error ? cancelError.message : "The download could not be canceled.";
  }
}

onMounted(async () => {
  await Promise.all([search(), pollWarmup()]);
});
onBeforeUnmount(() => {
  if (warmupTimer !== null) window.clearTimeout(warmupTimer);
  if (queuedStartTimer !== null) window.clearTimeout(queuedStartTimer);
});
</script>

<template>
  <section class="tm-stack tm-hf-browser">
    <article class="tm-form-card tm-hf-download-center" :class="{ quiet: !selectedCount && !warmupItems.length }">
      <header>
        <div><span class="tv-eyebrow">Download center</span><h3>{{ warmup.running ? `Downloading ${Math.min(completedCount + 1, warmupItems.length)} of ${warmupItems.length}` : batchStarting ? `Starting ${selectedCount} model file${selectedCount === 1 ? '' : 's'}…` : selectedCount ? `${selectedCount} model file${selectedCount === 1 ? '' : 's'} waiting` : warmupItems.length ? "Latest download batch" : "Ready for model downloads" }}</h3><p v-if="activeDownload">{{ activeDownload.model }} · {{ downloadMeta(activeDownload) }}<template v-if="selectedCount"> · {{ selectedCount }} queued next</template></p><p v-else-if="selectedCount">Starting automatically. Files download one at a time.</p><p v-else-if="warmupItems.length">{{ completedCount }} of {{ warmupItems.length }} completed.</p><p v-else>Choose a model below and Tater will start it automatically.</p></div>
        <div class="tm-hf-selection-actions"><span v-if="selectedCount" class="tm-hf-auto-pill">{{ batchStarting ? "Starting…" : "Auto-start on" }}</span><button v-if="selectedCount" class="tv-button" type="button" @click="clearDownloadQueue">Clear waiting</button><button v-if="error && selectedCount && !warmup.running" class="tv-button primary" type="button" :disabled="batchStarting" @click="downloadSelected">{{ batchStarting ? "Retrying…" : "Retry queue" }}</button><button v-if="warmup.running" class="tv-button danger" type="button" @click="cancelWarmup()">Cancel all</button></div>
      </header>
      <section v-if="selectedCount" class="tm-hf-waiting"><header><div><strong>Up next</strong><span>Starts automatically when the current file finishes.</span></div><b>{{ selectedCount }} waiting</b></header><div class="tm-hf-queue-list"><div v-for="item in downloadList" :key="item.key" class="tm-hf-queue-item"><div><strong>{{ item.repoId }}</strong><span>{{ item.providerLabel }}<template v-if="item.filename"> · {{ item.filename }}</template><template v-if="item.sizeLabel"> · {{ item.sizeLabel }}</template></span></div><button type="button" aria-label="Remove from download queue" @click="removeChoice(item.key)">×</button></div></div></section>
      <section v-if="warmup.running || warmupItems.length" class="tm-hf-activity">
        <div v-if="warmup.running && activeDownload" class="tm-hf-current-progress"><div><span>Current file</span><strong>{{ downloadState(activeDownload) }}</strong><b>{{ Math.round(Number(activeDownload.progress || 0)) }}%</b></div><progress class="tm-batch-progress" :value="Number(activeDownload.progress || 0)" max="100" /></div>
        <details class="tm-download-details" :open="Boolean(warmup.running)">
          <summary><span>Download activity</span><small>{{ warmupItems.length }} item{{ warmupItems.length === 1 ? '' : 's' }} · {{ completedCount }} complete</small></summary>
          <div class="tm-progress-list"><div v-for="item in warmupItems" :key="String(item.key || item.model)" class="tm-progress-row" :class="downloadTone(item)"><div class="tm-progress-copy"><div><strong>{{ item.model }}</strong><span class="tm-download-state">{{ downloadState(item) }}</span></div><span>{{ item.provider_label || item.provider }} · {{ downloadMeta(item) }}</span></div><progress :value="Number(item.progress || 0)" max="100" /><button v-if="item.cancelable" class="tv-button" type="button" @click="cancelWarmup(item)">Cancel</button></div></div>
        </details>
      </section>
    </article>

    <div v-if="integration.setup_needed" class="tv-notice">Public models can still be available, but gated or private models need the Hugging Face integration enabled with an access token.</div>
    <div v-if="error" class="tv-notice error" aria-live="polite">{{ error }}</div>

    <article class="tm-form-card tm-hf-guide">
      <header><div><span class="tv-eyebrow">Hugging Face model library</span><h3>Choose models and Tater handles the queue</h3></div><ol class="tm-hf-steps" aria-label="Download steps"><li><span>1</span><strong>Runtime</strong></li><li><span>2</span><strong>Choose</strong></li><li><span>3</span><strong>Auto-download</strong></li></ol></header>
      <div class="tm-hf-filters">
        <label class="tm-field"><span class="tm-field-label">Runtime</span><select v-model="provider" @change="search()"><option v-for="option in providers" :key="option.value" :value="option.value">{{ option.label }}</option></select></label>
        <label class="tm-field"><span class="tm-field-label">Capability</span><select v-model="task" @change="search()"><option v-for="option in tasks" :key="option.value" :value="option.value">{{ option.label }}</option></select></label>
        <form class="tm-hf-search" @submit.prevent="search()"><label class="tm-field"><span class="tm-field-label">Search Hugging Face</span><input v-model="query" type="search" placeholder="Model name or creator" /></label><button class="tv-button primary" type="submit" :disabled="loading">{{ loading ? "Searching…" : "Search" }}</button></form>
      </div>
      <div class="tm-hf-view-switch" role="group" aria-label="Model list"><button v-for="option in views" :key="option.value" type="button" :class="{ active: view === option.value }" :aria-pressed="view === option.value" @click="view = option.value; search()">{{ option.label }}</button></div>
    </article>

    <section class="tm-hf-results">
      <header class="tm-hf-results-head"><div><h3>{{ views.find((option) => option.value === view)?.label }} models</h3><p>{{ loading && !results.length ? "Finding compatible models…" : `${results.length} compatible ${taskLabel(task).toLowerCase()} model${results.length === 1 ? '' : 's'} shown` }}</p></div><span v-if="selectedCount" class="tm-hf-selected-pill">{{ selectedCount }} selected</span></header>
      <div class="tm-model-grid" :aria-busy="loading">
        <article v-for="model in results" :key="String(model.repo_id || model.id)" class="tm-model-card" :class="{ selected: modelIsSelected(model), pick: model.tater_pick, installed: installedCount(model) > 0, 'target-installed': directTargetInstalled(model) }">
          <div class="tm-model-card-head"><div class="tm-model-badges"><span class="tm-model-kind">{{ model.tater_pick_label || model.pipeline_tag || taskLabel(task) }}</span><span v-if="installedLabel(model)" class="tm-model-installed-badge">✓ {{ installedLabel(model) }}</span></div><button class="tm-model-select" :class="{ selected: modelIsSelected(model), installed: directTargetInstalled(model) }" type="button" :aria-pressed="modelIsSelected(model)" :disabled="Boolean(addingRepo) || directTargetInstalled(model)" @click="toggleModel(model)"><span aria-hidden="true">{{ directTargetInstalled(model) || modelIsSelected(model) ? "✓" : "↓" }}</span>{{ addingRepo === modelRepo(model) ? "Checking…" : directTargetInstalled(model) ? "Installed" : modelIsSelected(model) ? "Queued" : "Download" }}</button></div>
          <button class="tm-model-card-preview" type="button" @click="openModel(model)"><strong>{{ modelName(model) }}</strong><small>{{ model.author || modelRepo(model).split('/')[0] }} · {{ providerLabel(modelProvider(model)) }}</small><span v-if="model.tater_pick_note" class="tm-model-pick-note">{{ model.tater_pick_note }}</span><div class="tm-model-meta"><span v-if="model.size_label || model.model_size">{{ model.size_label || model.model_size }}</span><span v-if="model.supports_vision">Vision</span><span v-if="model.supports_audio">Audio</span><span v-if="model.supports_video">Video</span><span>{{ formatCount(model.downloads) }} downloads</span><span v-if="model.likes">{{ formatCount(model.likes) }} likes</span></div></button>
          <div class="tm-model-card-actions"><button class="tv-button" type="button" @click="openModel(model)">Details &amp; files</button><a class="tv-button" :href="hubUrl(modelRepo(model))" target="_blank" rel="noopener noreferrer">Open Hub</a></div>
        </article>
        <div v-if="!loading && !results.length" class="tv-notice">No compatible models matched this search. Try another capability, runtime, or search phrase.</div>
      </div>
      <button v-if="nextCursor" class="tv-button tm-load-more" type="button" :disabled="loading" @click="search(true)">{{ loading ? "Loading…" : "Load more models" }}</button>
    </section>

    <article class="tm-form-card"><header><div><h3>Installed local models</h3><p>These models are ready to select in Tater's LLM, Vision, Audio, and Video settings.</p></div><button class="tv-button" type="button" @click="refreshLocalModels">Refresh</button></header><div class="tm-installed-list"><div v-for="model in installed" :key="`${model.provider}:${model.model}`"><div><strong>{{ model.model }}</strong><span>{{ model.provider_label || model.provider }}<template v-if="model.supports_vision"> · Vision</template><template v-if="model.supports_audio"> · Audio</template><template v-if="model.supports_video"> · Video</template></span></div><button class="tv-button danger" type="button" @click="deleteModel(model)">Delete</button></div><div v-if="!installed.length" class="tv-notice">No local models are installed yet.</div></div></article>

    <PopupTransition :open="detailOpen" backdrop-class="tv-modal-backdrop tset-modal" @close="closeDetail">
      <section class="tv-modal tm-hf-detail-modal" role="dialog" aria-modal="true" aria-labelledby="tm-hf-detail-title">
        <header class="tv-modal-header"><div><span class="tv-eyebrow">Model details</span><h2 id="tm-hf-detail-title">{{ detailLoading ? "Loading model…" : modelName(detailModel) }}</h2><p v-if="!detailLoading">{{ providerLabel(selectedProvider) }} · {{ taskLabel(selectedTask) }}</p></div><button class="tv-button" type="button" @click="closeDetail">Close</button></header>
        <div v-if="detailLoading" class="tv-notice">Reading model files from Hugging Face…</div>
        <template v-else-if="selected">
          <div class="tm-hf-detail-summary"><p>{{ detailModel.description || "Review the available files and add this model to your download list." }}</p><a class="tv-button" :href="hubUrl(modelRepo(detailModel))" target="_blank" rel="noopener noreferrer">Open on Hugging Face</a></div>
          <section v-if="selectedProvider === 'llama_cpp'" class="tm-hf-file-section"><div><h3>Choose one or more GGUF files</h3><p>Select every quantization you want, then add them to the download center together. Files already on this Tater are disabled.</p></div><div v-if="!ggufFiles.length" class="tv-notice error">No downloadable GGUF files were found in this repository.</div><div v-else class="tm-hf-file-list"><button v-for="file in ggufFiles" :key="String(file.name || file.path)" type="button" class="tm-hf-file-choice" :class="{ selected: selectedFiles.includes(String(file.name || file.path)), installed: fileInstalled(file) }" :aria-pressed="selectedFiles.includes(String(file.name || file.path))" :disabled="fileInstalled(file)" @click="toggleSelectedFile(String(file.name || file.path))"><span>{{ file.name || file.path }}</span><small>{{ file.quant || "GGUF" }}<template v-if="file.size_label"> · {{ file.size_label }}</template><template v-if="fileInstalled(file)"> · Already installed</template></small><i aria-hidden="true">{{ fileInstalled(file) ? "✓" : selectedFiles.includes(String(file.name || file.path)) ? "✓" : "" }}</i></button></div><div v-if="mmprojFiles.length" class="tm-status-card"><div><strong>Vision projector included</strong><span>{{ selected.preferred_mmproj || "Tater will match it automatically." }}</span></div></div></section>
          <section v-else class="tm-hf-file-section"><div><h3>Repository download</h3><p>{{ selectedProvider === 'mlx_lm' ? "The full MLX repository, including tokenizer and config files, will be downloaded together." : "The full Transformers repository and its required files will be downloaded together." }}</p></div><div class="tm-hf-repo-files"><span v-for="file in files.slice(0, 14)" :key="String(file.name || file.path)">{{ file.name || file.path }}</span><small v-if="files.length > 14">+ {{ files.length - 14 }} more files</small></div></section>
          <footer class="tm-hf-detail-footer"><div><strong>{{ detailRepositoryInstalled ? "This model is already installed" : selectedProvider === 'llama_cpp' ? `${selectedFiles.length} file${selectedFiles.length === 1 ? '' : 's'} selected` : detailQueued ? "Already queued" : "Ready to download" }}</strong><span v-if="selectedProvider === 'llama_cpp'">{{ selectedFiles.length ? "Selected files will download one at a time." : "Choose one or more available GGUF files above." }}</span><span v-else>{{ detailRepositoryInstalled ? "Tater will not download it again." : "The complete repository will download automatically." }}</span></div><button class="tv-button primary" type="button" :disabled="detailRepositoryInstalled || (selectedProvider === 'llama_cpp' && !selectedFiles.length)" @click="queueFromDetail">{{ detailRepositoryInstalled ? "Installed" : detailQueued ? "Update queue" : selectedProvider === 'llama_cpp' && selectedFiles.length > 1 ? `Download ${selectedFiles.length} files` : "Download model" }}</button></footer>
        </template>
      </section>
    </PopupTransition>
  </section>
</template>
