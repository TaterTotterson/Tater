<script setup lang="ts">
import { computed } from "vue";
import type { JsonRow } from "../types";

const props = withDefaults(defineProps<{
  task: JsonRow;
  coreKey?: string;
  busy?: boolean;
}>(), {
  coreKey: "",
  busy: false,
});

const emit = defineEmits<{
  run: [taskId: string, coreKey: string];
}>();

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

const taskId = computed(() => String(props.task.id || "").trim());
const label = computed(() => String(props.task.label || taskId.value || "System task").trim());
const description = computed(() => String(props.task.description || "").trim());
const status = computed(() => String(props.task.status || "idle").trim().toLowerCase());
const running = computed(() => bool(props.task.running));
const enabled = computed(() => bool(props.task.enabled, true));
const manual = computed(() => props.coreKey ? bool(props.task.manual, true) : true);
const canRun = computed(() => props.coreKey ? bool(props.task.can_run) : enabled.value);
const disabled = computed(() => props.busy || running.value || !canRun.value || !manual.value);
const schedule = computed(() => String(props.task.schedule_label || "").trim() || intervalLabel(props.task.interval_seconds));
const statusLabel = computed(() => {
  if (props.busy) return "Starting";
  if (running.value) return "Running";
  if (status.value === "error") return "Needs attention";
  if (status.value === "stopped") return "Core stopped";
  if (status.value === "waiting") return "Waiting";
  return enabled.value ? "Ready" : "Disabled";
});
const statusTone = computed(() => {
  if (props.busy || running.value) return "running";
  if (!enabled.value) return "disabled";
  return status.value || "idle";
});
const duration = computed(() => {
  const milliseconds = Math.max(0, Number(props.task.duration_ms || 0));
  if (!milliseconds) return "—";
  return milliseconds < 1000 ? `${milliseconds.toFixed(0)} ms` : `${(milliseconds / 1000).toFixed(1)} s`;
});
const nextRun = computed(() => {
  const custom = String(props.task.next_run_label || "").trim();
  if (custom) return custom;
  if (running.value) return "After this run";
  return enabled.value ? timeLabel(props.task.next_run_at, true) : "Off";
});

function timeLabel(value: unknown, future = false): string {
  const seconds = Number(value || 0);
  if (!Number.isFinite(seconds) || seconds <= 0) return future ? "Not scheduled" : "Not run yet";
  const deltaSeconds = Math.round(seconds - Date.now() / 1000);
  if (future && deltaSeconds > 0 && deltaSeconds < 90) return `in ${deltaSeconds}s`;
  try {
    return new Date(seconds * 1000).toLocaleString([], {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch {
    return future ? "Scheduled" : "Completed";
  }
}

function intervalLabel(value: unknown): string {
  const seconds = Math.max(0, Number(value || 0));
  if (!seconds) return "Off";
  if (seconds < 60) return `Every ${Math.round(seconds)}s`;
  if (seconds < 3600) return `Every ${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `Every ${Math.round(seconds / 3600)}h`;
  return `Every ${Math.round(seconds / 86400)}d`;
}
</script>

<template>
  <article class="system-task-card" :class="`tone-${statusTone}`">
    <div class="system-task-process">
      <div>
        <i class="system-task-process-dot" />
        <h3>{{ label }}</h3>
      </div>
      <p v-if="description">{{ description }}</p>
      <small>
        {{ Number(task.run_count || 0) ? `${Number(task.run_count)} completed run${Number(task.run_count) === 1 ? "" : "s"}` : "Waiting for its first run" }}
      </small>
    </div>

    <div class="system-task-cell">
      <span>Schedule</span>
      <strong>{{ schedule }}</strong>
    </div>
    <div class="system-task-cell">
      <span>Last activity</span>
      <strong>{{ timeLabel(task.finished_at) }}</strong>
      <small>{{ duration === "—" ? "No duration yet" : duration }}</small>
    </div>
    <div class="system-task-cell">
      <span>{{ task.next_run_label ? "Trigger" : "Next / trigger" }}</span>
      <strong>{{ nextRun }}</strong>
    </div>

    <div v-if="task.last_error" class="system-task-error">{{ task.last_error }}</div>

    <div class="system-task-command">
      <span class="system-task-status" :class="`tone-${statusTone}`"><i />{{ statusLabel }}</span>
      <button
        v-if="manual"
        class="tv-button"
        type="button"
        :disabled="disabled"
        @click="emit('run', taskId, coreKey)"
      >
        {{ busy ? "Starting…" : running ? "Running…" : "Run now" }}
      </button>
      <button v-else class="tv-button" type="button" disabled>Automatic</button>
    </div>
  </article>
</template>
