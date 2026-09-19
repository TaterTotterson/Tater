<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { getJson, postJson } from "../../shared/api";
import type { JsonRow } from "../types";
import SystemTaskCard from "./SystemTaskCard.vue";

const props = defineProps<{
  endpoint: string;
  coreRunEndpoint: string;
}>();

const emit = defineEmits<{
  notify: [message: string, tone?: string];
}>();

const payload = ref<JsonRow>({});
const loading = ref(false);
const loaded = ref(false);
const error = ref("");
const lastUpdatedAt = ref(0);
const busyKeys = ref<string[]>([]);
let active = false;
let pollTimer = 0;

const tasks = computed<JsonRow[]>(() => Array.isArray(payload.value.tasks) ? payload.value.tasks : []);
const coreGroups = computed<JsonRow[]>(() => Array.isArray(payload.value.core_tasks) ? payload.value.core_tasks : []);
const coreErrors = computed<JsonRow[]>(() => Array.isArray(payload.value.core_task_errors) ? payload.value.core_task_errors : []);
const coreTasks = computed<JsonRow[]>(() => coreGroups.value.flatMap((group) => Array.isArray(group.tasks) ? group.tasks : []));
const allTasks = computed<JsonRow[]>(() => [...tasks.value, ...coreTasks.value]);
const totalCount = computed(() => allTasks.value.length);
const runningCount = computed(() => allTasks.value.filter((task) => bool(task.running)).length);
const errorCount = computed(() => allTasks.value.filter((task) => String(task.status || "").toLowerCase() === "error").length + coreErrors.value.length);
const disabledCount = computed(() => allTasks.value.filter((task) => !bool(task.enabled, true)).length);
const readyCount = computed(() => allTasks.value.filter((task) => (
  bool(task.enabled, true)
  && !bool(task.running)
  && String(task.status || "").toLowerCase() !== "error"
)).length);
const healthyCount = computed(() => Math.max(0, totalCount.value - Math.min(totalCount.value, errorCount.value)));
const healthPercent = computed(() => totalCount.value ? Math.round((healthyCount.value / totalCount.value) * 100) : 0);
const healthStyle = computed(() => ({ "--system-health-angle": `${healthPercent.value * 3.6}deg` }));
const overallTone = computed(() => errorCount.value ? "attention" : runningCount.value ? "running" : "ready");
const overallLabel = computed(() => {
  if (errorCount.value) return "Needs attention";
  if (runningCount.value) return "Work in progress";
  if (totalCount.value) return "All processes ready";
  return "Waiting for task data";
});
const liveStateLabel = computed(() => error.value ? "Update issue" : loaded.value ? "Live" : "Connecting");
const lastUpdatedLabel = computed(() => lastUpdatedAt.value
  ? new Date(lastUpdatedAt.value).toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" })
  : "Not loaded");

function bool(value: unknown, fallback = false): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const token = value.trim().toLowerCase();
    if (["true", "1", "yes", "on", "enabled"].includes(token)) return true;
    if (["false", "0", "no", "off", "disabled"].includes(token)) return false;
  }
  return fallback;
}

function taskKey(taskId: string, coreKey = ""): string {
  return `${coreKey || "tater"}:${taskId}`;
}

function isBusy(taskId: unknown, coreKey = ""): boolean {
  return busyKeys.value.includes(taskKey(String(taskId || ""), coreKey));
}

function replaceBusy(key: string, value: boolean) {
  const next = new Set(busyKeys.value);
  if (value) next.add(key);
  else next.delete(key);
  busyKeys.value = [...next];
}

function clearPoll() {
  if (pollTimer) window.clearTimeout(pollTimer);
  pollTimer = 0;
}

function schedulePoll(delay = 2000) {
  clearPoll();
  if (!active) return;
  pollTimer = window.setTimeout(async () => {
    pollTimer = 0;
    await refresh(true);
    schedulePoll(2000);
  }, Math.max(500, delay));
}

async function refresh(quiet = false) {
  if (loading.value) return;
  loading.value = true;
  if (!quiet) error.value = "";
  try {
    payload.value = await getJson<JsonRow>(props.endpoint);
    loaded.value = true;
    error.value = "";
    lastUpdatedAt.value = Date.now();
  } catch (refreshError) {
    error.value = refreshError instanceof Error ? refreshError.message : "System tasks could not be loaded.";
  } finally {
    loading.value = false;
  }
}

async function runTask(taskId: string, coreKey = "") {
  const key = taskKey(taskId, coreKey);
  if (!taskId || busyKeys.value.includes(key)) return;
  replaceBusy(key, true);
  error.value = "";
  clearPoll();
  const endpoint = coreKey
    ? `${props.coreRunEndpoint}/${encodeURIComponent(coreKey)}/${encodeURIComponent(taskId)}/run`
    : `${props.endpoint}/${encodeURIComponent(taskId)}/run`;
  try {
    const next = await postJson<JsonRow>(endpoint);
    payload.value = next;
    loaded.value = true;
    lastUpdatedAt.value = Date.now();
    emit("notify", next.queued === false ? "That task is already running." : "System task started.", "success");
  } catch (runError) {
    error.value = runError instanceof Error ? runError.message : "System task could not be started.";
    emit("notify", error.value, "error");
  } finally {
    replaceBusy(key, false);
    schedulePoll(500);
  }
}

onMounted(async () => {
  active = true;
  await refresh();
  schedulePoll(2000);
});

onBeforeUnmount(() => {
  active = false;
  clearPoll();
});
</script>

<template>
  <section class="tset-resource tsystem-tasks">
    <div v-if="error" class="tv-notice error" aria-live="polite">{{ error }}</div>

    <section class="tv-panel tset-form-card">
      <header class="tsystem-head">
        <div>
          <span class="tv-eyebrow">Background work</span>
          <h2>System tasks</h2>
          <p>Live status for snapshots, maintenance jobs, and tasks provided by installed Cores.</p>
        </div>
        <div class="tsystem-live" :class="{ issue: Boolean(error) }" role="status" aria-live="polite">
          <span class="tsystem-live-state"><i />{{ liveStateLabel }}</span>
          <small>{{ loaded ? `Updated ${lastUpdatedLabel}` : "Loading process status…" }}</small>
        </div>
      </header>

      <div class="system-task-overview" :class="`tone-${overallTone}`" aria-live="polite">
        <div class="system-task-health">
          <div class="system-task-health-ring" :style="healthStyle">
            <span>{{ healthPercent }}%</span>
            <small>healthy</small>
          </div>
          <div>
            <span>Overall process health</span>
            <strong>{{ overallLabel }}</strong>
            <small>{{ tasks.length }} Tater · {{ coreTasks.length }} Core<span v-if="disabledCount"> · {{ disabledCount }} disabled</span></small>
          </div>
        </div>
        <div class="system-task-summary">
          <div><span>Processes</span><strong>{{ totalCount }}</strong></div>
          <div><span>Running</span><strong>{{ runningCount }}</strong></div>
          <div><span>Ready</span><strong>{{ readyCount }}</strong></div>
          <div :class="{ alert: errorCount }"><span>Issues</span><strong>{{ errorCount }}</strong></div>
        </div>
      </div>
    </section>

    <div v-if="loaded" class="system-task-sections">
      <section class="system-task-section">
        <header class="system-task-section-head">
          <div><span>Tater</span><h2>Scheduled tasks</h2></div>
          <strong>{{ tasks.length }} process{{ tasks.length === 1 ? "" : "es" }}</strong>
        </header>
        <div v-if="tasks.length" class="system-task-grid">
          <div class="system-task-list-head" aria-hidden="true">
            <span>Process</span><span>Schedule</span><span>Last activity</span><span>Next / trigger</span><span>Status</span>
          </div>
          <SystemTaskCard
            v-for="task in tasks"
            :key="task.id"
            :task="task"
            :busy="isBusy(task.id)"
            @run="runTask"
          />
        </div>
        <p v-else class="tv-empty">No Tater system tasks are registered.</p>
      </section>

      <section class="system-task-section">
        <header class="system-task-section-head">
          <div><span>Installed Cores</span><h2>Core tasks</h2></div>
          <strong>{{ coreTasks.length }} process{{ coreTasks.length === 1 ? "" : "es" }}</strong>
        </header>
        <div class="system-task-core-groups">
          <section v-for="group in coreGroups" :key="group.core_key" class="system-task-core-group">
            <header class="system-task-group-head">
              <div><span>Core tasks</span><h3>{{ group.label || group.core_key || "Core" }}</h3></div>
              <span class="system-task-core-state" :class="group.running ? 'running' : 'stopped'"><i />{{ group.running ? "Core running" : "Core stopped" }}</span>
            </header>
            <div v-if="Array.isArray(group.tasks) && group.tasks.length" class="system-task-grid">
              <div class="system-task-list-head" aria-hidden="true">
                <span>Process</span><span>Schedule</span><span>Last activity</span><span>Next / trigger</span><span>Status</span>
              </div>
              <SystemTaskCard
                v-for="task in group.tasks"
                :key="task.id"
                :task="task"
                :core-key="String(group.core_key || '')"
                :busy="isBusy(task.id, String(group.core_key || ''))"
                @run="runTask"
              />
            </div>
            <p v-else class="tv-empty">This Core does not expose background tasks.</p>
          </section>
          <p v-if="!coreGroups.length" class="tv-empty">No installed Cores expose background tasks yet.</p>
          <div v-for="row in coreErrors" :key="`${row.core_key}-${row.error}`" class="tv-notice error">
            {{ row.core_key || "Core" }}: {{ row.error || "Task status unavailable." }}
          </div>
        </div>
      </section>
    </div>
  </section>
</template>
