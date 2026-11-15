<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import PopupTransition from "../../../shared/PopupTransition.vue";
import UiIcon from "../../../shared/UiIcon.vue";
import { modelPresentation } from "./modelPresentation";
import type { JsonRow } from "../../types";

const props = defineProps<{
  open: boolean;
  snapshot: JsonRow;
  pollError?: string;
  startup?: boolean;
}>();

const emit = defineEmits<{ close: []; reopen: [] }>();
const minimized = ref(false);
let completionTimer = 0;

const items = computed<JsonRow[]>(() => Array.isArray(props.snapshot.items) ? props.snapshot.items : []);
const errors = computed<string[]>(() => [
  ...(Array.isArray(props.snapshot.errors) ? props.snapshot.errors.map(String) : []),
  ...(props.pollError ? [props.pollError] : []),
].filter(Boolean));
const running = computed(() => Boolean(props.snapshot.running));
const startup = computed(() => Boolean(props.startup));
const startupPending = computed(() => startup.value && Boolean(props.snapshot.startup_bootstrap_pending));
const saving = computed(() => String(props.snapshot.ui_phase || "") === "saving");
const activeKey = computed(() => String(props.snapshot.active_key || ""));
const activeItem = computed<JsonRow | null>(() => items.value.find((item) => Boolean(item.active))
  || items.value.find((item) => ["preparing", "downloading", "loading", "working", "cancelling"].includes(status(item)))
  || null);
const loadDone = computed(() => items.value.length > 0 && items.value.every((item) => ["loaded", "downloaded", "error", "cancelled", "canceled"].includes(status(item))));
const restart = computed<JsonRow>(() => props.snapshot.runtime_restart && typeof props.snapshot.runtime_restart === "object" ? props.snapshot.runtime_restart as JsonRow : {});
const restartRunning = computed(() => Boolean(restart.value.running) || activeKey.value === "__runtime_restart__");
const restartFinished = computed(() => Number(restart.value.finished_ts || 0) > 0);
const complete = computed(() => !saving.value && !running.value && !startupPending.value);
const successful = computed(() => complete.value && !errors.value.length);
const progress = computed(() => {
  if (saving.value) return 6;
  if (activeKey.value === "__unload_previous__") return 14;
  if (restartRunning.value) return 94;
  if (complete.value) return errors.value.length ? Math.max(12, itemProgress.value) : 100;
  return Math.max(18, Math.min(91, 18 + itemProgress.value * 0.72));
});
const itemProgress = computed(() => {
  if (!items.value.length) return 0;
  return items.value.reduce((sum, item) => sum + clamp(item.progress), 0) / items.value.length;
});
const headline = computed(() => {
  if (errors.value.length && complete.value) return startup.value ? "Startup model needs attention" : "Model change needs attention";
  if (saving.value) return "Saving your model setup";
  if (activeKey.value === "__unload_previous__") return "Unloading the previous model";
  if (restartRunning.value) return "Refreshing Cores and Portals";
  if (activeItem.value) return `Loading ${String(activeItem.value.provider_label || providerLabel(activeItem.value.provider))}`;
  if (successful.value) return "Tater is ready";
  return startupPending.value ? "Starting Tater" : "Preparing local models";
});
const headlineDetail = computed(() => {
  if (errors.value.length && complete.value) return errors.value[0];
  if (saving.value) return "Writing the provider, routing, and runtime settings you selected.";
  if (activeKey.value === "__unload_previous__") return "Tater is releasing the old model cleanly before loading the new one.";
  if (restartRunning.value) return "The selected model is ready. Running surfaces are reconnecting to it now.";
  if (activeItem.value) return shortModel(activeItem.value);
  if (successful.value) return startup.value ? "Your local model and Tater runtime are ready." : "Models, Cores, and Portals are synchronized with your saved settings.";
  if (startupPending.value) return "The app is open while Tater prepares your saved local model and runtime.";
  return "Tater is preparing the selected local runtime.";
});
const statusText = computed(() => {
  if (saving.value) return "Saving settings…";
  if (activeKey.value === "__unload_previous__") return "Unloading previous local models…";
  if (restartRunning.value) return "Restarting running Cores and Portals…";
  if (activeItem.value) return String(activeItem.value.message || prettyStatus(activeItem.value.status));
  if (errors.value.length) return errors.value[0];
  if (successful.value) return "Model change complete";
  return startupPending.value ? "Preparing the startup model loader…" : "Waiting for the model loader…";
});
const stages = computed(() => {
  const unloadTargets = Array.isArray(props.snapshot.unload_before) ? props.snapshot.unload_before : [];
  const unloadResult = props.snapshot.unload_result && typeof props.snapshot.unload_result === "object" ? props.snapshot.unload_result as JsonRow : {};
  const unloadErrors = Array.isArray(unloadResult.errors) ? unloadResult.errors : [];
  const itemErrors = items.value.filter((item) => status(item) === "error");
  const loaded = items.value.filter((item) => ["loaded", "downloaded"].includes(status(item))).length;
  const restartFailures = restartFailureCount(restart.value);
  const hasRestart = restartRunning.value || restartFinished.value || Number(restart.value.started_ts || 0) > 0;
  if (startup.value) {
    return [
      {
        id: "open", label: "Open Tater",
        detail: "The control surface is available while startup continues.",
        state: "done",
      },
      {
        id: "load", label: "Load selected model",
        detail: items.value.length
          ? activeItem.value ? shortModel(activeItem.value) : `${loaded}/${items.value.length} ${items.value.length === 1 ? "model" : "models"} ready.`
          : startupPending.value ? "Reading the saved local model selection." : "No local model load was needed.",
        state: itemErrors.length ? "error" : activeItem.value ? "active" : loadDone.value ? "done" : startupPending.value ? "active" : "skipped",
      },
      {
        id: "ready", label: errors.value.length ? "Needs attention" : "Ready",
        detail: errors.value.length ? "Review the model message before trying again." : complete.value && !startupPending.value ? "Tater is ready to use." : "Finishes when the selected model is available.",
        state: errors.value.length && complete.value ? "error" : complete.value && !startupPending.value ? "done" : "pending",
      },
    ];
  }
  return [
    {
      id: "save", label: "Save settings",
      detail: saving.value ? "Writing model and runtime choices." : "Settings accepted.",
      state: saving.value ? "active" : "done",
    },
    {
      id: "unload", label: "Unload previous",
      detail: unloadTargets.length
        ? activeKey.value === "__unload_previous__" ? `${unloadTargets.length} previous ${unloadTargets.length === 1 ? "model" : "models"} releasing.` : `${Number(unloadResult.unloaded_count || unloadTargets.length)} previous ${unloadTargets.length === 1 ? "model" : "models"} handled.`
        : "No previous local model to release.",
      state: activeKey.value === "__unload_previous__" ? "active" : unloadErrors.length ? "error" : saving.value ? "pending" : unloadTargets.length ? "done" : "skipped",
    },
    {
      id: "load", label: "Load selected",
      detail: items.value.length
        ? activeItem.value ? shortModel(activeItem.value) : `${loaded}/${items.value.length} ${items.value.length === 1 ? "model" : "models"} ready.`
        : saving.value ? "Waiting for saved model choices." : "No local model load was needed.",
      state: itemErrors.length ? "error" : activeItem.value ? "active" : loadDone.value ? "done" : items.value.length ? "pending" : saving.value ? "pending" : "skipped",
    },
    {
      id: "restart", label: "Refresh platforms",
      detail: hasRestart ? restartDetail(restart.value) : items.value.length ? "Runs after the selected model is ready." : "No running platform refresh was needed.",
      state: restartFailures || restart.value.error ? "error" : restartRunning.value ? "active" : restartFinished.value ? "done" : complete.value ? "skipped" : "pending",
    },
    {
      id: "ready", label: errors.value.length ? "Needs attention" : "Ready",
      detail: errors.value.length ? "Review the message below before trying again." : "The saved model setup is ready to use.",
      state: errors.value.length && complete.value ? "error" : successful.value ? "done" : "pending",
    },
  ];
});
const restartVisible = computed(() => restartRunning.value || restartFinished.value || Boolean(restart.value.error));
const readyCount = computed(() => items.value.filter((item) => ["loaded", "downloaded"].includes(status(item))).length);
const title = computed(() => errors.value.length && complete.value ? "Model setup needs attention"
  : successful.value ? startup.value ? "Tater is ready" : "Model setup updated"
  : startup.value ? "Getting Tater ready" : "Updating model setup");
const activityLabel = computed(() => activeItem.value && activeKey.value !== "__unload_previous__" && !restartRunning.value
  ? `${status(activeItem.value) === "downloading" ? "Downloading" : "Loading"} ${displayModel(activeItem.value)}`
  : headline.value);
const stageLabels: Record<string, string> = { open: "Workspace", save: "Save", unload: "Unload", load: "Models", restart: "Refresh", ready: "Ready" };

function displayModel(item: JsonRow): string { return modelPresentation(shortModel(item), String(item.model || item.filename || "")).name; }
function roleLabel(item: JsonRow): string {
  const roles = Array.isArray(item.roles) ? item.roles : item.role ? [item.role] : [];
  const labels: Record<string, string> = { Base: "Base chat", WebUI: "Chat", "Decision Router": "Decision router", Audio: "Audio understanding", Video: "Video understanding", Image: "Image understanding" };
  return roles.map((role: unknown) => labels[String(role)] || String(role)).filter(Boolean).join(" · ") || "Local model";
}
function itemLabel(item: JsonRow): string {
  return ({ loaded: "Ready", pending: "Queued", error: "Needs attention" } as Record<string, string>)[status(item)] || prettyStatus(item.status);
}
function itemPercent(item: JsonRow): number { return status(item) === "loaded" ? 100 : clamp(item.progress); }
function itemActive(item: JsonRow): boolean {
  return Boolean(item.active) || ["preparing", "downloading", "loading", "working", "cancelling"].includes(status(item));
}
function modelIcon(item: JsonRow): string {
  if (["loaded", "downloaded"].includes(status(item))) return "check";
  if (status(item) === "error") return "alert";
  return /voice|speech|listening/i.test(roleLabel(item)) ? "mic" : /chat|base/i.test(roleLabel(item)) ? "chat" : "brain";
}
function clearCompletionTimer() { if (completionTimer) window.clearTimeout(completionTimer); completionTimer = 0; }
function closeProgress() { minimized.value = !complete.value; emit("close"); }
function reopenProgress() { minimized.value = false; emit("reopen"); }
watch(() => props.open, (open) => { if (open) { minimized.value = false; clearCompletionTimer(); } });
watch([complete, minimized], ([done, hidden]) => {
  clearCompletionTimer();
  if (done && hidden && !errors.value.length) completionTimer = window.setTimeout(() => { minimized.value = false; }, 3500);
});
onBeforeUnmount(clearCompletionTimer);

function clamp(value: unknown): number { return Math.max(0, Math.min(100, Number(value) || 0)); }
function status(item: JsonRow): string { return String(item.status || "pending").trim().toLowerCase(); }
function prettyStatus(value: unknown): string {
  const token = String(value || "pending").replaceAll("_", " ");
  return token.charAt(0).toUpperCase() + token.slice(1);
}
function providerLabel(value: unknown): string {
  return String(value || "") === "llama_cpp" ? "llama.cpp" : "local model";
}
function shortModel(item: JsonRow): string {
  const value = String(item.filename || item.model || "Selected local model");
  return value.split("::").pop() || value;
}
function countRows(value: unknown): number {
  if (Array.isArray(value)) return value.length;
  return value && typeof value === "object" ? Object.keys(value as JsonRow).length : 0;
}
function restartFailureCount(value: JsonRow): number {
  const stopped = value.stopped && typeof value.stopped === "object" ? value.stopped as JsonRow : {};
  const resumed = value.resumed && typeof value.resumed === "object" ? value.resumed as JsonRow : {};
  const group = (root: JsonRow, key: string): JsonRow => root[key] && typeof root[key] === "object" ? root[key] as JsonRow : {};
  return countRows(group(stopped, "cores").failed) + countRows(group(stopped, "portals").failed)
    + countRows(group(resumed, "cores").failed) + countRows(group(resumed, "portals").failed);
}
function restartDetail(value: JsonRow): string {
  if (value.error) return String(value.error);
  const active = value.active_before && typeof value.active_before === "object" ? value.active_before as JsonRow : {};
  const cores = countRows(active.cores);
  const portals = countRows(active.portals);
  const failures = restartFailureCount(value);
  if (failures) return `${failures} platform refresh ${failures === 1 ? "step needs" : "steps need"} attention.`;
  if (restartRunning.value) return `${cores} ${cores === 1 ? "Core" : "Cores"} and ${portals} ${portals === 1 ? "Portal" : "Portals"} reconnecting.`;
  return `${cores} ${cores === 1 ? "Core" : "Cores"} and ${portals} ${portals === 1 ? "Portal" : "Portals"} refreshed.`;
}
</script>

<template>
  <PopupTransition :open="open" backdrop-class="tv-modal-backdrop tset-modal tm-model-queue-backdrop" @close="closeProgress">
    <section class="tv-modal tm-model-queue" :class="{ running, complete, error: errors.length }" role="dialog" aria-modal="true" aria-labelledby="tm-model-apply-title">
      <header class="mq-top"><span class="mq-brand">Tater</span><span class="mq-context">{{ startup ? 'Startup' : 'Model settings' }}</span><button class="mq-icon-button" type="button" :aria-label="complete ? 'Close model progress' : 'Hide model progress'" @click="closeProgress"><span aria-hidden="true">{{ complete ? '×' : '−' }}</span></button></header>
      <div class="mq-body">
        <div class="mq-heading"><h2 id="tm-model-apply-title">{{ title }}</h2><p>{{ successful ? startup ? 'Your selected models are ready to use.' : 'Your saved model setup is ready to use.' : startup ? 'Preparing your saved models and services.' : 'Applying your saved model choices and reconnecting services.' }}</p></div>
        <ol class="mq-stages" aria-label="Model apply stages">
          <li v-for="stage in stages" :key="stage.id" :class="stage.state" :aria-label="stage.label + ': ' + stage.detail"><UiIcon v-if="stage.state === 'done'" name="check" /><UiIcon v-else-if="stage.state === 'error'" name="alert" /><i v-else class="mq-stage-dot" aria-hidden="true">{{ stage.state === 'skipped' ? '−' : '' }}</i><span>{{ stage.id === 'ready' && stage.state === 'error' ? 'Needs attention' : stageLabels[stage.id] || stage.label }}</span></li>
        </ol>
        <div v-if="items.length || saving || activeKey === '__unload_previous__' || restartVisible || complete" class="mq-overall">
          <div class="mq-progress-heading"><strong>{{ activityLabel }}</strong><span>{{ Math.round(progress) }}%</span></div>
          <div class="mq-track" role="progressbar" :aria-label="startup ? 'Startup progress' : 'Model change progress'" :aria-valuenow="Math.round(progress)" aria-valuemin="0" aria-valuemax="100"><span :style="{ width: progress + '%' }" /></div>
          <div v-if="items.length" class="mq-count"><span>{{ readyCount }} of {{ items.length }} {{ items.length === 1 ? 'model' : 'models' }} {{ snapshot.load_models === false ? 'downloaded' : 'ready' }}</span><span>Local models</span></div>
        </div>
        <div v-else class="mq-waiting"><UiIcon name="server" /><div><strong>{{ headline }}</strong><span>{{ headlineDetail }}</span></div></div>
        <div v-if="items.length" class="mq-models" aria-label="Model loading queue">
          <article v-for="item in items" :key="String(item.key || item.model)" class="mq-model" :class="[status(item), { active: itemActive(item) }]">
            <span class="mq-model-icon"><UiIcon :name="modelIcon(item)" /></span>
            <div class="mq-model-name"><strong>{{ displayModel(item) }}</strong><span>{{ roleLabel(item) }}</span></div>
            <div class="mq-model-state"><span>{{ itemLabel(item) }}</span><small v-if="itemActive(item)">{{ Math.round(itemPercent(item)) }}%</small></div>
            <div v-if="itemActive(item)" class="mq-track mq-row-track" role="progressbar" :aria-label="displayModel(item) + ' progress'" :aria-valuenow="Math.round(itemPercent(item))" aria-valuemin="0" aria-valuemax="100"><span :style="{ width: itemPercent(item) + '%' }" /></div>
          </article>
        </div>
        <div v-if="restartVisible" class="mq-runtime" :class="{ active: restartRunning, error: restart.error || restartFailureCount(restart) }" role="status"><UiIcon name="refresh" /><div><strong>{{ restartRunning ? 'Cores and Portals are restarting' : restart.error || restartFailureCount(restart) ? 'Platform refresh needs attention' : 'Cores and Portals refreshed' }}</strong><span>{{ restartDetail(restart) }}</span></div></div>
        <div v-if="errors.length" class="mq-errors" role="alert"><UiIcon name="alert" /><div><strong>What needs attention</strong><span v-for="message in errors" :key="message">{{ message }}</span></div></div>
        <details class="mq-details"><summary><UiIcon name="chevron" />Technical details</summary><div class="mq-technical">
          <p>{{ headlineDetail }}</p><p>{{ statusText }}</p><p v-if="running || saving || startupPending">This continues safely if you hide the window or change tabs.</p>
          <dl class="mq-stage-details"><template v-for="stage in stages" :key="stage.id"><dt>{{ stage.label }} · {{ prettyStatus(stage.state) }}</dt><dd>{{ stage.detail }}</dd></template></dl>
          <div v-for="item in items" :key="String(item.key || item.model)" class="mq-item-details"><strong>{{ roleLabel(item) }} · {{ item.provider_label || providerLabel(item.provider) }}</strong><code>{{ item.model || item.filename }}</code><code v-if="item.filename && item.filename !== item.model">{{ item.filename }}</code><span>{{ prettyStatus(item.status) }} · {{ Math.round(itemPercent(item)) }}%<template v-if="item.message"> · {{ item.message }}</template></span></div>
        </div></details>
      </div>
      <footer class="mq-footer"><span>{{ complete ? errors.length ? 'Your saved settings remain available.' : 'Your workspace and models are ready.' : 'Model loading continues in the background.' }}</span><button class="tv-button primary" type="button" @click="closeProgress">{{ complete ? startup ? 'Open Tater' : 'Done' : 'Continue in background' }}<span aria-hidden="true">→</span></button></footer>
    </section>
  </PopupTransition>
  <Teleport to="body"><aside v-if="minimized && !open" class="tm-model-loader-dock" :class="{ complete: successful, error: errors.length }" aria-label="Background model progress">
    <UiIcon :name="errors.length ? 'alert' : successful ? 'check' : 'brain'" /><div class="mq-dock-copy" role="status"><strong>{{ errors.length ? 'A model needs attention' : successful ? 'Your models are ready' : startupPending && !items.length ? 'Starting Tater' : 'Models are preparing' }}</strong><span>{{ activityLabel }}</span></div><button class="tv-button" type="button" @click="reopenProgress">View progress</button><button class="mq-icon-button" type="button" aria-label="Dismiss background model progress" @click="minimized = false"><UiIcon name="close" /></button>
  </aside></Teleport>
</template>
