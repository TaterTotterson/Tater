<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { getJson, postJson } from "../shared/api";
import ChatMessageView from "./components/ChatMessage.vue";
import type { ChatJobState, ChatMessage, ChatMountOptions, ChatProfile, ChatStatsPayload } from "./types";

const props = defineProps<{
  state: { profile: ChatProfile; messages: ChatMessage[]; stats: ChatStatsPayload };
  options: ChatMountOptions;
}>();

const feed = ref<HTMLElement | null>(null);
const composer = ref<HTMLTextAreaElement | null>(null);
const fileInput = ref<HTMLInputElement | null>(null);
const draft = ref("");
const pendingFiles = ref<File[]>([]);
const sending = ref(false);
const statusMessage = ref("");
const sessionId = ref(String(props.options.sessionId || ""));
const ephemeralMessages = ref<ChatMessage[]>([]);
const streams = ref<Record<string, string>>({});
const activeJobs = ref<Record<string, ChatJobState>>({ ...(props.options.initialJobs || {}) });
const stickToBottom = ref(true);
const sources: Record<string, EventSource> = {};
const pollTimers: Record<string, number> = {};

const profile = computed(() => props.state.profile || {});
const messages = computed(() => Array.isArray(props.state.messages) ? props.state.messages : []);
const taterName = computed(() => {
  const first = String(profile.value.tater_first_name || profile.value.tater_name || "Tater").trim() || "Tater";
  const last = String(profile.value.tater_last_name || "Totterson").trim();
  return String(profile.value.tater_full_name || [first, last].filter(Boolean).join(" ") || "Tater Totterson").trim();
});
const activeJobRows = computed(() => Object.entries(activeJobs.value).filter(([, row]) => Boolean(row)));
const activeCount = computed(() => activeJobRows.value.length);
const streamRows = computed(() => Object.entries(streams.value).filter(([, content]) => Boolean(content)));
const typingMessage = computed<ChatMessage>(() => ({ role: "assistant", content: { marker: "typing" } }));
const liveStatus = computed(() => {
  if (!activeCount.value) return statusMessage.value;
  const buckets = new Map<string, { label: string; count: number }>();
  activeJobRows.value.forEach(([, row]) => {
    const tool = String(row.current_tool || "").trim();
    if (!tool) return;
    const key = tool.toLowerCase();
    const bucket = buckets.get(key) || { label: tool, count: 0 };
    bucket.count += 1;
    buckets.set(key, bucket);
  });
  const tools = [...buckets.values()].sort((a, b) => b.count - a.count).slice(0, 3).map((row) => `${row.count} using ${row.label}`);
  return `${activeCount.value} ${activeCount.value === 1 ? "job" : "jobs"} running${tools.length ? ` • ${tools.join(" • ")}` : ""}`;
});
const speedLine = computed(() => {
  if (!props.state.stats?.enabled) return "";
  const stats = props.state.stats.stats;
  if (!stats || typeof stats !== "object") return "";
  const elapsed = Number(stats.elapsed || 0);
  const totalTokens = Number(stats.total_tokens || 0);
  const tpsTotal = Number(stats.tps_total || 0);
  const tpsPrompt = Number(stats.tps_prompt || 0);
  const tpsCompletion = Number(stats.tps_comp || 0);
  const mainTps = tpsCompletion > 0 ? tpsCompletion : tpsTotal;
  if (!(elapsed > 0 && totalTokens > 0 && mainTps > 0)) return "";
  const basis = String(stats.speed_basis || "");
  const mainLabel = ["llama_cpp_timing", "mlx_lm_timing"].includes(basis)
    ? "decode"
    : basis === "local_generate"
      ? "generated"
      : basis === "api_round_trip"
        ? "API completion"
        : "completion";
  const details: string[] = [];
  if (tpsTotal > 0 && Math.abs(tpsTotal - mainTps) >= 1) details.push(`total ${Math.round(tpsTotal)} tok/s`);
  if (tpsPrompt > 0) details.push(`prompt ${Math.round(tpsPrompt)} tok/s`);
  const promptTokens = Number(stats.prompt_tokens || 0);
  const completionTokens = Number(stats.completion_tokens || 0);
  return `${String(stats.model || "LLM")} — ${mainLabel}: ${Math.round(mainTps)} tok/s${details.length ? ` · ${details.join(" · ")}` : ""} • ${Math.round(totalTokens)} tok in ${elapsed.toFixed(2)}s (prompt ${Math.round(promptTokens)}, generated ${Math.round(completionTokens)})`;
});

function notify(message: string, tone = "success") {
  props.options.onToast?.(message, tone);
}

function reportError(error: unknown, fallback: string): string {
  const message = error instanceof Error ? error.message : fallback;
  statusMessage.value = message;
  props.options.onRequestError?.(message);
  return message;
}

function syncJobs() {
  props.options.onJobsChange?.({ ...activeJobs.value });
}

function closeSource(jobId: string) {
  sources[jobId]?.close();
  delete sources[jobId];
}

function clearPoll(jobId: string) {
  if (pollTimers[jobId]) window.clearTimeout(pollTimers[jobId]);
  delete pollTimers[jobId];
}

function updateJob(jobId: string, patch: ChatJobState) {
  const previous = activeJobs.value[jobId] || {};
  activeJobs.value = {
    ...activeJobs.value,
    [jobId]: {
      ...previous,
      ...patch,
      status: String(patch.status || previous.status || "running").toLowerCase(),
      updated_at: Date.now(),
    },
  };
  syncJobs();
}

function removeJob(jobId: string) {
  const next = { ...activeJobs.value };
  delete next[jobId];
  activeJobs.value = next;
  syncJobs();
}

function forceScrollToBottom() {
  stickToBottom.value = true;
  void nextTick(() => {
    if (feed.value) feed.value.scrollTop = feed.value.scrollHeight;
  });
}

function scrollIfFollowing() {
  if (!stickToBottom.value) return;
  forceScrollToBottom();
}

function handleFeedScroll() {
  const element = feed.value;
  if (!element) return;
  stickToBottom.value = element.scrollHeight - element.scrollTop - element.clientHeight < 120;
}

async function refreshHistory() {
  const payload = await getJson<{ messages?: ChatMessage[] }>(props.options.endpoints.history);
  props.state.messages = Array.isArray(payload.messages) ? payload.messages : [];
  ephemeralMessages.value = [];
}

async function refreshStats() {
  try {
    props.state.stats = await getJson<ChatStatsPayload>(props.options.endpoints.stats);
  } catch {
    props.state.stats = { enabled: false, stats: null };
  }
}

async function finalizeJob(jobId: string, message: string, responses: unknown[] = []) {
  if (!activeJobs.value[jobId]) return;
  closeSource(jobId);
  clearPoll(jobId);
  removeJob(jobId);
  const nextStreams = { ...streams.value };
  delete nextStreams[jobId];
  streams.value = nextStreams;
  try {
    await refreshHistory();
  } catch (error) {
    if (responses.length) {
      props.state.messages = [
        ...props.state.messages,
        ...responses.map((content) => ({ role: "assistant", username: "assistant", content: content as ChatMessage["content"] })),
      ];
    } else {
      reportError(error, "Chat history refresh failed.");
    }
  }
  await refreshStats();
  props.options.onHealthRefresh?.();
  statusMessage.value = message;
  forceScrollToBottom();
}

function handleSnapshot(jobId: string, snapshot: Record<string, any>) {
  const state = String(snapshot.status || "running").trim().toLowerCase();
  if (state === "done") {
    void finalizeJob(jobId, "Complete.", Array.isArray(snapshot.responses) ? snapshot.responses : []);
    return;
  }
  if (state === "error") {
    void finalizeJob(jobId, `Job failed: ${String(snapshot.error || "unknown error")}`);
    return;
  }
  updateJob(jobId, {
    status: state || "running",
    current_tool: String(snapshot.current_tool || "").trim(),
    task_name: String(snapshot.task_name || activeJobs.value[jobId]?.task_name || "").trim(),
  });
}

function schedulePoll(jobId: string, delay?: number) {
  clearPoll(jobId);
  if (!activeJobs.value[jobId]) return;
  pollTimers[jobId] = window.setTimeout(async () => {
    if (!activeJobs.value[jobId]) return;
    try {
      const snapshot = await getJson<Record<string, any>>(`${props.options.endpoints.jobs}/${encodeURIComponent(jobId)}`);
      handleSnapshot(jobId, snapshot);
    } catch (error) {
      props.options.onRequestError?.(error instanceof Error ? error.message : "Chat job polling failed.");
    }
    if (activeJobs.value[jobId]) schedulePoll(jobId, 1200);
  }, Math.max(250, delay ?? (props.options.isIngress ? 900 : 2000)));
}

function parseEvent(event: Event): Record<string, any> {
  try { return JSON.parse(String((event as MessageEvent).data || "{}")); }
  catch { return {}; }
}

function attachJob(jobId: string, initial: ChatJobState = {}) {
  if (!jobId) return;
  updateJob(jobId, { status: "queued", ...initial });
  closeSource(jobId);
  schedulePoll(jobId);
  if (typeof EventSource !== "function") return;
  const source = new EventSource(`${props.options.endpoints.jobs}/${encodeURIComponent(jobId)}/events`);
  sources[jobId] = source;
  source.addEventListener("status", (event) => handleSnapshot(jobId, parseEvent(event)));
  source.addEventListener("tool", (event) => {
    const payload = parseEvent(event);
    updateJob(jobId, {
      status: "running",
      current_tool: String(payload.current_tool || "tool"),
      task_name: String(payload.task_name || activeJobs.value[jobId]?.task_name || ""),
    });
  });
  source.addEventListener("waiting", (event) => {
    const text = String(parseEvent(event).wait_text || "").trim();
    if (!text) return;
    ephemeralMessages.value = [...ephemeralMessages.value, { role: "assistant", content: { marker: "plugin_wait", content: text } }];
    scrollIfFollowing();
  });
  source.addEventListener("response_chunk", (event) => {
    const chunk = String(parseEvent(event).chunk || "");
    if (!chunk) return;
    streams.value = { ...streams.value, [jobId]: String(streams.value[jobId] || "") + chunk };
    scrollIfFollowing();
  });
  source.addEventListener("done", (event) => {
    const payload = parseEvent(event);
    void finalizeJob(jobId, "Complete.", Array.isArray(payload.responses) ? payload.responses : []);
  });
  source.addEventListener("job_error", (event) => {
    void finalizeJob(jobId, `Job failed: ${String(parseEvent(event).error || "unknown error")}`);
  });
  source.onerror = () => closeSource(jobId);
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
}

function handleFiles(event: Event) {
  const input = event.target as HTMLInputElement;
  const candidates = Array.from(input.files || []);
  const maxEach = Number(profile.value.attach_max_mb_each || 0) * 1024 ** 2;
  const maxTotal = Number(profile.value.attach_max_mb_total || 0) * 1024 ** 2;
  const accepted: File[] = [];
  let total = 0;
  for (const file of candidates) {
    if (maxEach > 0 && file.size > maxEach) {
      notify(`${file.name} is larger than the ${profile.value.attach_max_mb_each} MB attachment limit.`, "error");
      continue;
    }
    if (maxTotal > 0 && total + file.size > maxTotal) {
      notify(`Attachments exceed the ${profile.value.attach_max_mb_total} MB total limit.`, "error");
      break;
    }
    accepted.push(file);
    total += file.size;
  }
  pendingFiles.value = accepted;
  input.value = "";
}

function removeFile(index: number) {
  pendingFiles.value = pendingFiles.value.filter((_file, fileIndex) => fileIndex !== index);
}

function clearFiles() {
  pendingFiles.value = [];
  if (fileInput.value) fileInput.value.value = "";
}

function fileDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error(`Could not read ${file.name}.`));
    reader.readAsDataURL(file);
  });
}

async function sendMessage() {
  if (sending.value) return;
  const message = draft.value.trim();
  const files = [...pendingFiles.value];
  if (!message && !files.length) {
    statusMessage.value = "Enter a message or attach files first.";
    return;
  }
  sending.value = true;
  draft.value = "";
  clearFiles();
  resizeComposer();
  statusMessage.value = files.length ? "Preparing attachments…" : "Queueing chat job…";
  forceScrollToBottom();
  try {
    const attachments = [];
    for (const file of files) {
      attachments.push({
        name: file.name || "attachment",
        mimetype: file.type || "application/octet-stream",
        data_url: await fileDataUrl(file),
      });
    }
    const response = await postJson<Record<string, any>>(props.options.endpoints.jobs, {
      message,
      session_id: sessionId.value,
      attachments,
    });
    const nextSessionId = String(response.session_id || "").trim();
    if (nextSessionId) {
      sessionId.value = nextSessionId;
      props.options.onSessionChange?.(nextSessionId);
    }
    const jobId = String(response.job_id || "").trim();
    if (!jobId) throw new Error("Backend did not return a job id.");
    await refreshHistory();
    attachJob(jobId, { status: "queued", task_name: String(response.task_name || "") });
    statusMessage.value = response.task_name ? `Job queued: ${response.task_name}` : "Job queued…";
    props.options.onHealthRefresh?.();
    forceScrollToBottom();
  } catch (error) {
    const messageText = reportError(error, "Chat failed.");
    notify(`Chat failed: ${messageText}`, "error");
  } finally {
    sending.value = false;
  }
}

function handleComposerKeydown(event: KeyboardEvent) {
  if (event.key === "Enter" && !event.shiftKey && !event.ctrlKey && !event.altKey && !event.metaKey && !event.isComposing) {
    event.preventDefault();
    void sendMessage();
  }
}

function resizeComposer() {
  void nextTick(() => {
    const element = composer.value;
    if (!element) return;
    element.style.height = "auto";
    element.style.height = `${Math.min(Math.max(element.scrollHeight, 44), 180)}px`;
  });
}

watch([() => messages.value.length, () => ephemeralMessages.value.length, () => streamRows.value.map(([id, value]) => `${id}:${value.length}`).join("|"), activeCount], scrollIfFollowing);
watch(draft, resizeComposer);

onMounted(() => {
  Object.entries(activeJobs.value).forEach(([jobId, row]) => attachJob(jobId, row));
  forceScrollToBottom();
});

onBeforeUnmount(() => {
  Object.keys(sources).forEach(closeSource);
  Object.keys(pollTimers).forEach(clearPoll);
});
</script>

<template>
  <div class="tater-vue-surface tc-chat">
    <section class="tv-panel chat-feed-card tc-feed-card">
      <div ref="feed" class="chat-log tc-chat-log" @scroll="handleFeedScroll">
        <div v-if="!messages.length && !ephemeralMessages.length && !streamRows.length" class="tc-empty-chat">
          <div class="tc-empty-avatar">{{ taterName.charAt(0) }}</div>
          <h2>Talk to {{ taterName }}</h2>
          <p>Ask a question, control your home, or attach something for Tater to inspect.</p>
        </div>
        <ChatMessageView
          v-for="(message, index) in messages"
          :key="message.id || `history-${index}`"
          :message="message"
          :profile="profile"
          :files-endpoint="options.endpoints.files"
          @media-ready="scrollIfFollowing"
        />
        <ChatMessageView
          v-for="(message, index) in ephemeralMessages"
          :key="`ephemeral-${index}`"
          :message="message"
          :profile="profile"
          :files-endpoint="options.endpoints.files"
        />
        <div v-for="([jobId, content]) in streamRows" :key="jobId" :data-chat-stream-job="jobId" aria-live="polite" aria-busy="true">
          <ChatMessageView :message="{ role: 'assistant', content }" :profile="profile" :files-endpoint="options.endpoints.files" />
        </div>
        <ChatMessageView v-if="activeCount" :message="typingMessage" :profile="profile" :files-endpoint="options.endpoints.files" />
      </div>

      <div v-if="pendingFiles.length" class="tc-attachment-tray">
        <div class="tc-attachment-list">
          <button v-for="(file, index) in pendingFiles" :key="`${file.name}-${file.size}-${index}`" type="button" class="tc-attachment-chip" :title="`Remove ${file.name}`" @click="removeFile(index)">
            <span>{{ file.name }}</span><small>{{ formatBytes(file.size) }}</small><b aria-hidden="true">×</b>
          </button>
        </div>
        <button type="button" class="tc-clear-files" @click="clearFiles">Clear all</button>
      </div>

      <div v-if="activeJobRows.length" class="tc-job-strip" aria-label="Active chat jobs">
        <span v-for="([jobId, job]) in activeJobRows" :key="jobId">
          <i />{{ job.task_name || 'Tater is working' }}<small v-if="job.current_tool">{{ job.current_tool }}</small>
        </span>
      </div>

      <div class="message-box chat-composer-card tc-composer-card">
        <div class="chat-composer" role="group" aria-label="Chat composer">
          <div class="chat-composer-bar">
            <label class="chat-composer-btn chat-composer-attach" title="Attach files" aria-label="Attach files">
              <input ref="fileInput" class="tc-file-input" type="file" multiple @change="handleFiles" />
              <span class="chat-composer-icon chat-composer-plus" aria-hidden="true">+</span>
            </label>
            <textarea ref="composer" v-model="draft" class="chat-composer-input" rows="1" :placeholder="`Message ${taterName}…`" @keydown="handleComposerKeydown" />
            <button type="button" class="chat-composer-send" :disabled="sending" :title="sending ? 'Preparing message' : 'Send message'" :aria-label="sending ? 'Preparing message' : 'Send message'" @click="sendMessage">
              <span class="chat-composer-icon chat-composer-send-arrow" aria-hidden="true">➤</span>
            </button>
          </div>
        </div>
      </div>
      <div v-if="speedLine" class="chat-speed-stats tc-speed-stats">{{ speedLine }}</div>
      <div class="chat-live-status tc-live-status" aria-live="polite">{{ liveStatus }}</div>
    </section>
  </div>
</template>
