<script setup lang="ts">
import { computed } from "vue";
import PopupTransition from "../../../shared/PopupTransition.vue";
import type { JsonRow } from "../../types";

const props = defineProps<{
  open: boolean;
  snapshot: JsonRow;
  pollError?: string;
}>();

const emit = defineEmits<{ close: [] }>();

const items = computed<JsonRow[]>(() => Array.isArray(props.snapshot.items) ? props.snapshot.items : []);
const errors = computed<string[]>(() => [
  ...(Array.isArray(props.snapshot.errors) ? props.snapshot.errors.map(String) : []),
  ...(props.pollError ? [props.pollError] : []),
].filter(Boolean));
const running = computed(() => Boolean(props.snapshot.running));
const saving = computed(() => String(props.snapshot.ui_phase || "") === "saving");
const activeKey = computed(() => String(props.snapshot.active_key || ""));
const activeItem = computed<JsonRow | null>(() => items.value.find((item) => Boolean(item.active))
  || items.value.find((item) => ["preparing", "downloading", "loading", "working", "cancelling"].includes(status(item)))
  || null);
const loadDone = computed(() => items.value.length > 0 && items.value.every((item) => ["loaded", "downloaded", "error", "cancelled", "canceled"].includes(status(item))));
const restart = computed<JsonRow>(() => props.snapshot.runtime_restart && typeof props.snapshot.runtime_restart === "object" ? props.snapshot.runtime_restart as JsonRow : {});
const restartRunning = computed(() => Boolean(restart.value.running) || activeKey.value === "__runtime_restart__");
const restartFinished = computed(() => Number(restart.value.finished_ts || 0) > 0);
const complete = computed(() => !saving.value && !running.value);
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
  if (errors.value.length && complete.value) return "Model change needs attention";
  if (saving.value) return "Saving your model setup";
  if (activeKey.value === "__unload_previous__") return "Unloading the previous model";
  if (restartRunning.value) return "Refreshing Cores and Portals";
  if (activeItem.value) return `Loading ${String(activeItem.value.provider_label || providerLabel(activeItem.value.provider))}`;
  if (successful.value) return "Tater is ready";
  return "Preparing local models";
});
const headlineDetail = computed(() => {
  if (errors.value.length && complete.value) return errors.value[0];
  if (saving.value) return "Writing the provider, routing, and runtime settings you selected.";
  if (activeKey.value === "__unload_previous__") return "Tater is releasing the old model cleanly before loading the new one.";
  if (restartRunning.value) return "The selected model is ready. Running surfaces are reconnecting to it now.";
  if (activeItem.value) return shortModel(activeItem.value);
  if (successful.value) return "Models, Cores, and Portals are synchronized with your saved settings.";
  return "Tater is preparing the selected local runtime.";
});
const statusText = computed(() => {
  if (saving.value) return "Saving settings…";
  if (activeKey.value === "__unload_previous__") return "Unloading previous local models…";
  if (restartRunning.value) return "Restarting running Cores and Portals…";
  if (activeItem.value) return String(activeItem.value.message || prettyStatus(activeItem.value.status));
  if (errors.value.length) return errors.value[0];
  if (successful.value) return "Model change complete";
  return "Waiting for the model loader…";
});
const stages = computed(() => {
  const unloadTargets = Array.isArray(props.snapshot.unload_before) ? props.snapshot.unload_before : [];
  const unloadResult = props.snapshot.unload_result && typeof props.snapshot.unload_result === "object" ? props.snapshot.unload_result as JsonRow : {};
  const unloadErrors = Array.isArray(unloadResult.errors) ? unloadResult.errors : [];
  const itemErrors = items.value.filter((item) => status(item) === "error");
  const loaded = items.value.filter((item) => ["loaded", "downloaded"].includes(status(item))).length;
  const restartFailures = restartFailureCount(restart.value);
  const hasRestart = restartRunning.value || restartFinished.value || Number(restart.value.started_ts || 0) > 0;
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

function clamp(value: unknown): number { return Math.max(0, Math.min(100, Number(value) || 0)); }
function status(item: JsonRow): string { return String(item.status || "pending").trim().toLowerCase(); }
function prettyStatus(value: unknown): string {
  const token = String(value || "pending").replaceAll("_", " ");
  return token.charAt(0).toUpperCase() + token.slice(1);
}
function providerLabel(value: unknown): string {
  return ({ llama_cpp: "llama.cpp", hf_transformers: "Transformers", mlx_lm: "MLX LM" } as Record<string, string>)[String(value || "")] || "local model";
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
  <PopupTransition :open="open" backdrop-class="tv-modal-backdrop tset-modal tm-model-apply-backdrop" @close="emit('close')">
    <section class="tv-modal tm-model-apply" :class="{ running, complete, error: errors.length }" role="dialog" aria-modal="true" aria-labelledby="tm-model-apply-title">
      <header>
        <div><span class="tv-eyebrow">Tater model loader</span><h2 id="tm-model-apply-title">Apply local model changes</h2><p>Live progress from the model runtime and every surface connected to it.</p></div>
        <button class="tv-button" type="button" @click="emit('close')">{{ running || saving ? "Hide" : "Close" }}</button>
      </header>

      <section class="tm-model-apply-hero">
        <div class="tm-model-spud" aria-hidden="true"><span class="body" /><span class="visor" /><span class="glow" /><i class="spark one" /><i class="spark two" /><i class="spark three" /></div>
        <div><span>{{ complete ? errors.length ? "Stopped" : "Complete" : "Working" }}</span><strong>{{ headline }}</strong><p>{{ headlineDetail }}</p></div>
      </section>

      <div class="tm-model-apply-overall">
        <div><strong>{{ statusText }}</strong><b>{{ Math.round(progress) }}%</b></div>
        <div class="tm-model-apply-track" role="progressbar" :aria-valuenow="Math.round(progress)" aria-valuemin="0" aria-valuemax="100"><span :style="{ width: `${progress}%` }" /></div>
        <small>{{ running || saving ? "This continues safely if you hide the window or change tabs." : errors.length ? "The saved settings remain available; fix the reported issue and apply again." : "All model work for this save has finished." }}</small>
      </div>

      <div class="tm-model-apply-stages" aria-label="Model apply stages">
        <article v-for="stage in stages" :key="stage.id" :class="stage.state">
          <i aria-hidden="true">{{ stage.state === "done" ? "✓" : stage.state === "error" ? "!" : stage.state === "skipped" ? "–" : "" }}</i>
          <div><strong>{{ stage.label }}</strong><span>{{ stage.detail }}</span></div>
        </article>
      </div>

      <section v-if="restartVisible" class="tm-model-restart-summary" :class="{ active: restartRunning, error: restart.error || restartFailureCount(restart) }">
        <i aria-hidden="true">↻</i><div><strong>{{ restartRunning ? "Cores and Portals are restarting" : restart.error || restartFailureCount(restart) ? "Platform refresh needs attention" : "Cores and Portals refreshed" }}</strong><span>{{ restartDetail(restart) }}</span></div>
      </section>

      <div v-if="items.length" class="tm-model-apply-items">
        <article v-for="item in items" :key="String(item.key || item.model)" :class="status(item)">
          <header><div><span>{{ item.provider_label || providerLabel(item.provider) }}</span><strong>{{ shortModel(item) }}</strong></div><b>{{ prettyStatus(item.status) }}</b></header>
          <div class="tm-model-item-track"><span :style="{ width: `${status(item) === 'loaded' ? 100 : clamp(item.progress)}%` }" /></div>
          <footer><span>{{ item.message || prettyStatus(item.status) }}</span><b>{{ Math.round(status(item) === "loaded" ? 100 : clamp(item.progress)) }}%</b></footer>
        </article>
      </div>

      <div v-if="errors.length" class="tm-model-apply-errors" role="alert"><strong>What needs attention</strong><span v-for="message in errors" :key="message">{{ message }}</span></div>

      <footer><span>{{ complete ? errors.length ? "Model changes finished with an issue." : "Your selected models are ready." : "Tater will keep working in the background." }}</span><button class="tv-button primary" type="button" @click="emit('close')">{{ complete ? "Done" : "Continue in background" }}</button></footer>
    </section>
  </PopupTransition>
</template>
