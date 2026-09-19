<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref } from "vue";
import { getJson } from "../../shared/api";
import type { JsonRow } from "../types";

interface LogEntry extends JsonRow {
  seq: number;
  ts: number;
  level: string;
  logger: string;
  module: string;
  function: string;
  line: number;
  message: string;
  display: string;
  exception: string;
}

const props = withDefaults(defineProps<{
  endpoint: string;
  initialAutoScroll?: boolean;
}>(), {
  initialAutoScroll: true,
});

const emit = defineEmits<{
  notify: [message: string, tone?: string];
  autoScrollChange: [enabled: boolean];
}>();

const entries = ref<LogEntry[]>([]);
const nextSeq = ref(0);
const loading = ref(false);
const paused = ref(false);
const autoScroll = ref(props.initialAutoScroll);
const level = ref("");
const loggerName = ref("");
const loggerOptions = ref<string[]>([]);
const levelCounts = ref<JsonRow>({});
const status = ref("Opening live tail…");
const error = ref("");
const logElement = ref<HTMLElement | null>(null);
let active = false;
let pollTimer = 0;
let requestId = 0;

const visibleEntries = computed(() => entries.value.slice(-500));
const warningCount = computed(() => Number(levelCounts.value.warning || levelCounts.value.warn || 0));
const errorCount = computed(() => Number(levelCounts.value.error || 0) + Number(levelCounts.value.critical || 0));

function normalize(row: JsonRow): LogEntry {
  return {
    ...row,
    seq: Math.max(0, Number(row.seq || 0)),
    ts: Number(row.ts || 0),
    level: String(row.level || "info").trim().toLowerCase() || "info",
    logger: String(row.logger || "root").trim() || "root",
    module: String(row.module || "").trim(),
    function: String(row.function || "").trim(),
    line: Math.max(0, Number(row.line || 0)),
    message: String(row.message || "").trim(),
    display: String(row.display || row.message || "").trim(),
    exception: String(row.exception || "").trim(),
  };
}

function timeLabel(value: unknown): string {
  const seconds = Number(value || 0);
  if (!Number.isFinite(seconds) || seconds <= 0) return "--:--:--";
  try {
    return new Date(seconds * 1000).toLocaleTimeString([], {
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch {
    return "--:--:--";
  }
}

function location(entry: LogEntry): string {
  const callable = [entry.module, entry.function].filter(Boolean).join(".");
  return `${callable}${entry.line ? `:${entry.line}` : ""}`;
}

function isNearBottom(): boolean {
  const element = logElement.value;
  if (!element) return true;
  return Math.abs(element.scrollHeight - element.clientHeight - element.scrollTop) < 72;
}

async function scrollToBottom() {
  await nextTick();
  if (logElement.value) logElement.value.scrollTop = logElement.value.scrollHeight;
}

function clearPoll() {
  if (pollTimer) window.clearTimeout(pollTimer);
  pollTimer = 0;
}

function schedulePoll(delay = 1000) {
  clearPoll();
  if (!active || paused.value) return;
  pollTimer = window.setTimeout(async () => {
    pollTimer = 0;
    await refresh(false);
    schedulePoll(1000);
  }, Math.max(300, delay));
}

async function refresh(reset = false) {
  const currentRequest = ++requestId;
  const shouldScroll = autoScroll.value && (reset || isNearBottom());
  loading.value = true;
  error.value = "";
  const params = new URLSearchParams({
    after_seq: String(reset ? 0 : nextSeq.value),
    limit: "500",
    level: level.value,
    logger_name: loggerName.value.trim(),
  });
  try {
    const payload = await getJson<JsonRow>(`${props.endpoint}?${params.toString()}`);
    if (!active || currentRequest !== requestId) return;
    const received = (Array.isArray(payload.entries) ? payload.entries : [])
      .map((row: JsonRow) => normalize(row))
      .filter((row: LogEntry) => row.seq > 0);
    if (reset) {
      entries.value = received;
    } else if (received.length) {
      const merged = new Map(entries.value.map((entry) => [entry.seq, entry]));
      received.forEach((entry: LogEntry) => merged.set(entry.seq, entry));
      entries.value = [...merged.values()].sort((left, right) => left.seq - right.seq).slice(-800);
    }
    nextSeq.value = Math.max(nextSeq.value, Number(payload.next_seq || 0));
    loggerOptions.value = Array.isArray(payload.loggers)
      ? payload.loggers.map((value: unknown) => String(value || "")).filter(Boolean)
      : [];
    levelCounts.value = payload.level_counts && typeof payload.level_counts === "object" ? payload.level_counts : {};
    status.value = `${visibleEntries.value.length} visible · ${warningCount.value} warnings · ${errorCount.value} errors`;
    if (shouldScroll) await scrollToBottom();
  } catch (refreshError) {
    if (currentRequest !== requestId) return;
    error.value = refreshError instanceof Error ? refreshError.message : "Logs could not be loaded.";
    status.value = `Live tail error: ${error.value}`;
  } finally {
    if (currentRequest === requestId) loading.value = false;
  }
}

async function resetTail() {
  clearPoll();
  nextSeq.value = 0;
  entries.value = [];
  await refresh(true);
  schedulePoll(1000);
}

async function togglePause() {
  paused.value = !paused.value;
  if (paused.value) {
    clearPoll();
    status.value = `Paused · ${visibleEntries.value.length} visible lines`;
  } else {
    await refresh(false);
    schedulePoll(250);
  }
}

function toggleAutoScroll() {
  autoScroll.value = !autoScroll.value;
  emit("autoScrollChange", autoScroll.value);
  if (autoScroll.value) void scrollToBottom();
}

function clearView() {
  entries.value = [];
  status.value = "Visible log view cleared. Live tail will continue.";
}

async function copyVisible() {
  const text = visibleEntries.value
    .map((entry) => `${timeLabel(entry.ts)} ${entry.level.toUpperCase().padEnd(8)} ${entry.logger} ${entry.message || entry.display}`)
    .join("\n");
  if (!text.trim()) {
    status.value = "No visible logs to copy.";
    return;
  }
  try {
    await navigator.clipboard.writeText(text);
    status.value = `Copied ${visibleEntries.value.length} visible log line${visibleEntries.value.length === 1 ? "" : "s"}.`;
    emit("notify", status.value, "success");
  } catch (copyError) {
    error.value = copyError instanceof Error ? copyError.message : "Clipboard access failed.";
    status.value = `Copy failed: ${error.value}`;
    emit("notify", status.value, "error");
  }
}

onMounted(async () => {
  active = true;
  await refresh(true);
  schedulePoll(1000);
});

onBeforeUnmount(() => {
  active = false;
  requestId += 1;
  clearPoll();
});
</script>

<template>
  <section class="tset-resource tlogs">
    <section class="tv-panel tlogs-console">
      <header class="tlogs-head">
        <div>
          <span class="tv-eyebrow">Live application stream</span>
          <h2>Runtime console</h2>
          <p>Backend messages, warnings, errors, and Core or Portal activity update automatically.</p>
        </div>
        <span class="tv-live-pill" :class="{ warning: paused || Boolean(error) }"><i />{{ error ? "Connection issue" : paused ? "Paused" : "Live" }}</span>
      </header>

      <div class="tv-metrics tlogs-summary">
        <div><span>Visible</span><strong>{{ visibleEntries.length }}</strong></div>
        <div><span>Warnings</span><strong>{{ warningCount }}</strong></div>
        <div><span>Errors</span><strong>{{ errorCount }}</strong></div>
        <div><span>Cursor</span><strong>{{ nextSeq }}</strong></div>
      </div>

      <div class="tlogs-actions">
        <button class="tv-button" type="button" :disabled="loading" @click="resetTail">{{ loading ? "Refreshing…" : "Refresh" }}</button>
        <button class="tv-button" :class="{ active: paused }" type="button" @click="togglePause">{{ paused ? "Resume" : "Pause" }}</button>
        <button class="tv-button" :class="{ active: autoScroll }" type="button" @click="toggleAutoScroll">Auto-scroll {{ autoScroll ? "on" : "off" }}</button>
        <button class="tv-button" type="button" @click="copyVisible">Copy visible</button>
        <button class="tv-button danger" type="button" @click="clearView">Clear view</button>
      </div>

      <div class="tlogs-filters">
        <label>Level
          <select v-model="level" @change="resetTail">
            <option value="">All levels</option>
            <option value="info">Info+</option>
            <option value="warning">Warning+</option>
            <option value="error">Error+</option>
            <option value="critical">Critical</option>
          </select>
        </label>
        <label>Logger
          <input v-model="loggerName" type="text" list="tlogs-logger-options" placeholder="Filter logger name" @change="resetTail" @keydown.enter.prevent="resetTail" />
          <datalist id="tlogs-logger-options"><option v-for="name in loggerOptions" :key="name" :value="name" /></datalist>
        </label>
        <span class="tlogs-status" :class="{ error: Boolean(error) }" aria-live="polite">{{ status }}</span>
      </div>

      <div ref="logElement" class="app-log-events tlogs-events" role="log" aria-live="polite">
        <article v-for="entry in visibleEntries" :key="entry.seq" class="app-log-line" :class="entry.level">
          <span class="app-log-time">{{ timeLabel(entry.ts) }}</span>
          <span class="app-log-level">{{ entry.level.toUpperCase() }}</span>
          <div class="app-log-body">
            <strong>{{ entry.logger }}</strong>
            <small v-if="location(entry)">{{ location(entry) }}</small>
            <pre>{{ entry.display || entry.message }}</pre>
            <pre v-if="entry.exception" class="app-log-exception">{{ entry.exception }}</pre>
          </div>
        </article>
        <div v-if="!visibleEntries.length" class="app-log-empty">{{ loading ? "Loading application logs…" : "No logs match these filters yet." }}</div>
      </div>
    </section>
  </section>
</template>
