<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, reactive, ref, watch } from "vue";
import ChatMessage from "../chat/components/ChatMessage.vue";
import type { ChatMessage as ChatMessageType } from "../chat/types";
import { getJson, postJson, responseJson } from "../shared/api";
import PopupTransition from "../shared/PopupTransition.vue";
import type { JsonRow, SpudexMountOptions, SpudexPayload } from "./types";

const props = defineProps<{ state: { payload: SpudexPayload }; options: SpudexMountOptions }>();
const tabs = [
  { id: "workbench", label: "Workbench" },
  { id: "manual", label: "Manual Session" },
  { id: "settings", label: "Settings" },
];
const policyRules = [
  ["require_approval", "Require Hydra approval", "Hydra-triggered actions pause for approval. Spudex Chat and manual commands remain direct."],
  ["require_file_approval", "Require file write approval", "Model-proposed file writes remain pending until approved or rejected."],
  ["allow_network", "Allow network commands", "Allows curl, wget, and Git network actions."],
  ["allow_installs", "Allow package and tool installs", "Allows pip, npm, uv, and similar environment installs."],
  ["allow_absolute_executables", "Allow absolute executable paths", "Allows commands such as /usr/bin/python3."],
  ["allow_shell_commands", "Allow shells", "Allows sh, bash, zsh, fish, cmd, and PowerShell."],
  ["allow_host_admin_commands", "Allow host and admin commands", "Allows sudo, chmod, chown, launchctl, osascript, and open."],
  ["allow_remote_control", "Allow remote control tools", "Allows ssh, scp, and sftp when network access is also enabled."],
  ["allow_containers", "Allow containers", "Allows Docker and Podman commands."],
  ["allow_host_package_managers", "Allow host package managers", "Allows brew, apt, yum, dnf, pacman, and apk."],
  ["allow_inline_eval", "Allow inline eval", "Allows python -c, node -e, ruby -e, and similar interpreter execution."],
] as const;

const activeTab = ref(normalizeTab(props.options.initialTab));
const selectedSessionId = ref(text(props.options.initialSessionId));
const manualSessionId = ref(text(props.options.initialManualSessionId));
const logs = ref<JsonRow[]>([]);
const logCursor = ref(0);
const manualLogs = ref<JsonRow[]>([]);
const manualLogCursor = ref(0);
const chatMessage = ref("");
const command = ref("");
const manualCwd = ref("agent_lab");
const manualCwdDisplay = ref("/");
const background = ref(false);
const busy = ref("");
const error = ref("");
const notice = ref("");
const detailsOpen = ref(false);
const settingsDirty = ref(false);
const settingsDraft = reactive<JsonRow>({});
let pollTimer = 0;
let pollInFlight = false;

const payload = computed(() => props.state.payload || {});
const sessions = computed(() => Array.isArray(payload.value.sessions) ? payload.value.sessions : []);
const manualSessions = computed(() => sessions.value.filter((session: JsonRow) => canonical(session.source) === "ui"));
const modelProcesses = computed(() => Array.isArray(payload.value.model_processes) ? payload.value.model_processes : []);
const selectedSession = computed(() => sessions.value.find((session: JsonRow) => text(session.id) === selectedSessionId.value) || null);
const selectedManualSession = computed(() => manualSessions.value.find((session: JsonRow) => text(session.id) === manualSessionId.value) || null);
const detailSession = computed(() => activeTab.value === "manual" ? selectedManualSession.value : selectedSession.value);
const activeCount = computed(() => Number(payload.value.active_count || sessions.value.filter(isActive).length));
const modelProcessCount = computed(() => Number(payload.value.model_process_count || modelProcesses.value.length));
const selectedActive = computed(() => isActive(selectedSession.value));
const manualActive = computed(() => isActive(selectedManualSession.value));
const currentChatSession = computed(() => canonical(selectedSession.value?.source) === "spudex_chat" ? selectedSession.value : null);
const chatBusy = computed(() => Boolean(busy.value === "chat" || isActive(currentChatSession.value)));
const platformOptions = computed(() => normalizePlatforms(payload.value.platform_options, settingsDraft.allowed_platforms));
const chatMessages = computed<ChatMessageType[]>(() => {
  const rows = logs.value.map((entry: JsonRow) => {
    const stream = canonical(entry.stream);
    const body = text(entry.text);
    if (!body) return null;
    if (stream === "user") return { role: "user", username: props.options.profile?.username, content: body };
    if (stream === "assistant") return { role: "assistant", content: body };
    return null;
  }).filter(Boolean) as ChatMessageType[];
  return chatBusy.value ? [...rows.slice(-20), { role: "assistant", content: { marker: "typing" } }] : rows.slice(-20);
});
const nonChatLogs = computed(() => logs.value.filter((entry: JsonRow) => !["user", "assistant"].includes(canonical(entry.stream))));
const liveStatus = computed(() => {
  const session = currentChatSession.value || selectedSession.value;
  if (busy.value === "chat") return "Starting Spudex chat…";
  if (!session) return activeCount.value ? `${activeCount.value} active Spudex process${activeCount.value === 1 ? "" : "es"}` : "Ready for a Spudex task.";
  const plan = Array.isArray(session.plan) ? session.plan : [];
  const current = plan.find((row: JsonRow) => canonical(row.status) === "in_progress");
  if (current?.step) return `Working: ${current.step}`;
  return `${statusLabel(session.status)}${text(session.label || session.command || session.goal) ? `: ${text(session.label || session.command || session.goal)}` : ""}`;
});

function text(value: unknown): string { return String(value ?? "").trim(); }
function canonical(value: unknown): string { return text(value).toLowerCase(); }
function encode(value: unknown): string { return encodeURIComponent(text(value)); }
function normalizeTab(value: unknown): string { const token = canonical(value); return token === "manual" || token === "settings" || token === "policy" ? (token === "policy" ? "settings" : token) : "workbench"; }
function isActive(session: JsonRow | null | undefined): boolean { const status = canonical(session?.status); return Boolean(session?.active) || status === "running" || status === "queued"; }
function statusLabel(value: unknown): string {
  const token = canonical(value) || "queued";
  const labels: Record<string, string> = { succeeded: "Done", completed: "Complete", failed: "Failed", running: "Running", blocked: "Blocked", timeout: "Timeout", stopped: "Stopped", incomplete: "Incomplete", queued: "Queued", draft: "Draft" };
  return labels[token] || token.replaceAll("_", " ").replace(/^./, (letter) => letter.toUpperCase());
}
function relativeTime(value: unknown): string {
  const raw = Number(value || 0);
  if (!raw) return "";
  const seconds = Math.max(0, Math.floor(Date.now() / 1000 - raw));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}
function mergeLogRows(current: JsonRow[], incoming: JsonRow[], reset: boolean): JsonRow[] {
  const rows = reset ? [] : [...current];
  const keyFor = (entry: JsonRow) => `${entry._session_id ?? ""}\u0000${text(entry.seq) || `${entry.ts ?? ""}\u0000${entry.stream ?? ""}\u0000${entry.text ?? ""}`}`;
  const seen = new Set(rows.map(keyFor));
  incoming.forEach((entry) => {
    const key = keyFor(entry);
    if (seen.has(key)) return;
    seen.add(key);
    rows.push(entry);
  });
  return rows.slice(-1000);
}
function normalizePlatforms(raw: unknown, allowedRaw: unknown): JsonRow[] {
  const allowed = new Set((Array.isArray(allowedRaw) ? allowedRaw : ["webui"]).map(canonical).filter(Boolean));
  const map = new Map<string, JsonRow>();
  (Array.isArray(raw) ? raw : []).forEach((row: JsonRow) => { const value = canonical(row.value); if (value && !map.has(value)) map.set(value, { ...row, value }); });
  allowed.forEach((value) => { if (!map.has(value)) map.set(value, { value, label: value === "all" ? "All platforms" : value.replaceAll("_", " "), description: "Saved platform, currently stopped", running: value === "all" }); });
  if (!map.size) map.set("webui", { value: "webui", label: "Web UI", description: "Tater browser UI", running: true });
  return [...map.values()];
}
function notify(message: string, tone = "success") { notice.value = message; error.value = tone === "error" ? message : ""; props.options.onToast?.(message, tone); }
function sessionEndpoint(sessionId: string, suffix = ""): string { return `${props.options.endpoints.sessions}/${encode(sessionId)}${suffix}`; }
async function requestDelete(endpoint: string): Promise<JsonRow> { return responseJson<JsonRow>(await fetch(endpoint, { method: "DELETE", credentials: "same-origin", headers: { Accept: "application/json" } })); }

function syncSettings(force = false) {
  if (settingsDirty.value && !force) return;
  const source = payload.value.settings || {};
  Object.assign(settingsDraft, {
    enabled: Boolean(source.enabled),
    policy_enabled: source.policy_enabled !== false,
    require_approval: Boolean(source.require_approval),
    require_file_approval: Boolean(source.require_file_approval),
    allow_absolute_executables: Boolean(source.allow_absolute_executables),
    allow_shell_commands: Boolean(source.allow_shell_commands),
    allow_host_admin_commands: Boolean(source.allow_host_admin_commands),
    allow_remote_control: Boolean(source.allow_remote_control),
    allow_containers: Boolean(source.allow_containers),
    allow_host_package_managers: Boolean(source.allow_host_package_managers),
    allow_inline_eval: Boolean(source.allow_inline_eval),
    allow_network: Boolean(source.allow_network),
    allow_installs: Boolean(source.allow_installs),
    allowed_platforms: Array.isArray(source.allowed_platforms) ? [...source.allowed_platforms] : ["webui"],
    default_cwd: text(source.default_cwd || "workspace"),
    max_task_steps: Number(source.max_task_steps || 6),
    command_timeout_sec: Number(source.command_timeout_sec || 45),
  });
  settingsDirty.value = false;
}
function ensureSelections() {
  if (!sessions.value.some((session: JsonRow) => text(session.id) === selectedSessionId.value)) selectSession(text(sessions.value[0]?.id), false);
  if (!manualSessions.value.some((session: JsonRow) => text(session.id) === manualSessionId.value)) selectManualSession(text(manualSessions.value[0]?.id), false);
}
function setTab(tab: string) { activeTab.value = normalizeTab(tab); detailsOpen.value = false; props.options.onTabChange?.(activeTab.value); }
function selectSession(id: string, load = true) {
  const next = text(id);
  if (next === selectedSessionId.value) return;
  selectedSessionId.value = next; logs.value = []; logCursor.value = 0; props.options.onSessionChange?.(next);
  if (load) void refreshLogs(true);
}
function selectManualSession(id: string, load = true, preserveLogs = false) {
  const next = text(id);
  if (next === manualSessionId.value) return;
  manualSessionId.value = next; if (!preserveLogs) manualLogs.value = []; manualLogCursor.value = 0; props.options.onManualSessionChange?.(next);
  if (next) { selectedSessionId.value = next; props.options.onSessionChange?.(next); }
  if (load) void refreshManualLogs(!preserveLogs);
}

async function refreshState(quiet = false) {
  if (!quiet) busy.value = "refresh";
  try {
    props.state.payload = await getJson<SpudexPayload>(props.options.endpoints.root);
    ensureSelections();
    syncSettings();
  } catch (requestError) { if (!quiet) notify(requestError instanceof Error ? requestError.message : "Spudex refresh failed.", "error"); }
  finally { if (!quiet && busy.value === "refresh") busy.value = ""; }
}
async function fetchLogs(sessionId: string, after: number): Promise<JsonRow> {
  return getJson<JsonRow>(`${sessionEndpoint(sessionId, "/logs")}?after_seq=${encode(after)}&limit=500`);
}
async function refreshLogs(reset = false) {
  const id = selectedSessionId.value;
  if (!id) { logs.value = []; logCursor.value = 0; return; }
  const result = await fetchLogs(id, reset ? 0 : logCursor.value);
  const rows = Array.isArray(result.entries) ? result.entries : [];
  logs.value = mergeLogRows(logs.value, rows, reset);
  logCursor.value = Number(result.last_seq || (reset ? 0 : logCursor.value));
  await nextTick();
  document.querySelectorAll(".tsx-chat-scroll, .tsx-terminal-body").forEach((panel) => {
    if (panel instanceof HTMLElement && (reset || panel.scrollHeight - panel.scrollTop - panel.clientHeight < 120)) panel.scrollTop = panel.scrollHeight;
  });
}
async function refreshManualLogs(reset = false) {
  const id = manualSessionId.value;
  if (!id) { manualLogs.value = []; manualLogCursor.value = 0; return; }
  const result = await fetchLogs(id, reset ? 0 : manualLogCursor.value);
  const rows = (Array.isArray(result.entries) ? result.entries : []).map((entry: JsonRow) => ({ ...entry, _session_id: id }));
  manualLogs.value = mergeLogRows(manualLogs.value, rows, reset);
  manualLogCursor.value = Number(result.last_seq || (reset ? 0 : manualLogCursor.value));
  await nextTick();
  const panel = document.querySelector(".tsx-manual-console-body");
  if (panel instanceof HTMLElement && (reset || panel.scrollHeight - panel.scrollTop - panel.clientHeight < 100)) panel.scrollTop = panel.scrollHeight;
}
async function refreshAll(quiet = false) {
  await refreshState(quiet);
  await Promise.all([refreshLogs(false), refreshManualLogs(false)]);
}
function schedulePoll() {
  if (pollTimer) window.clearTimeout(pollTimer);
  pollTimer = window.setTimeout(async () => {
    if (!pollInFlight) {
      pollInFlight = true;
      try { await refreshAll(true); } catch { /* Keep current live state. */ }
      finally { pollInFlight = false; }
    }
    schedulePoll();
  }, 2000);
}

async function sendChat() {
  const message = chatMessage.value.trim();
  if (!message) { notify("Enter a Spudex chat message first.", "error"); return; }
  if (chatBusy.value) { notify("Spudex is still working in this chat.", "error"); return; }
  busy.value = "chat";
  try {
    const sessionId = canonical(selectedSession.value?.source) === "spudex_chat" ? selectedSessionId.value : "";
    const result = await postJson<JsonRow>(props.options.endpoints.chat, { message, session_id: sessionId || null });
    const nextId = text(result.session?.id);
    if (nextId) selectSession(nextId, false);
    chatMessage.value = "";
    notify("Spudex task started.");
    await refreshState(true); await refreshLogs(true);
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Spudex chat failed.", "error"); }
  finally { busy.value = ""; }
}
async function newChat() {
  busy.value = "new-chat";
  try {
    const result = await postJson<JsonRow>(props.options.endpoints.chatSession, { label: "New Spudex chat" });
    selectSession(text(result.session?.id), false); chatMessage.value = ""; notify("New Spudex chat created.");
    await refreshState(true); await refreshLogs(true);
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "New Spudex chat failed.", "error"); }
  finally { busy.value = ""; }
}
async function runCommand() {
  const value = command.value.trim();
  if (!value) { notify("Enter a command first.", "error"); return; }
  busy.value = "run";
  try {
    const result = await postJson<JsonRow>(props.options.endpoints.run, { command: value, cwd: manualCwd.value, label: value.slice(0, 80), background: background.value });
    const id = text(result.session?.id);
    manualCwd.value = text(result.session?.cwd) || manualCwd.value;
    manualCwdDisplay.value = text(result.session?.cwd_display) || manualCwdDisplay.value;
    selectSession(id, false); selectManualSession(id, false, true); command.value = "";
    notify(result.builtin === "cd" ? `Working directory: ${manualCwdDisplay.value}` : "Spudex session started.");
    await refreshState(true); await Promise.all([refreshLogs(true), refreshManualLogs(false)]);
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Command failed.", "error"); }
  finally { busy.value = ""; }
}
async function stopSession(id: string, label = "Spudex session") {
  if (!id) return;
  busy.value = `stop-${id}`;
  try { await postJson<JsonRow>(sessionEndpoint(id, "/stop")); notify(`${label} stop requested.`); await refreshState(true); }
  catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Stop failed.", "error"); }
  finally { busy.value = ""; }
}
async function closeSession(session: JsonRow) {
  const id = text(session.id); if (!id) return;
  if (isActive(session) && !window.confirm("Close this running Spudex session? Its active command will be stopped.")) return;
  busy.value = `close-${id}`;
  try {
    await requestDelete(sessionEndpoint(id));
    if (id === selectedSessionId.value) selectSession("", false);
    if (id === manualSessionId.value) selectManualSession("", false);
    notify("Spudex session closed."); await refreshState(true);
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Close failed.", "error"); }
  finally { busy.value = ""; }
}
async function fileChange(sessionId: string, changeId: string, action: "approve" | "reject") {
  busy.value = `${action}-${changeId}`;
  try { await postJson<JsonRow>(sessionEndpoint(sessionId, `/file-changes/${action}`), { change_id: changeId }); notify(`File change ${action === "approve" ? "approved" : "rejected"}.`); await refreshState(true); }
  catch (requestError) { notify(requestError instanceof Error ? requestError.message : "File change update failed.", "error"); }
  finally { busy.value = ""; }
}
async function saveSettings() {
  busy.value = "settings";
  try {
    await postJson<JsonRow>(props.options.endpoints.settings, { values: { ...settingsDraft, allowed_platforms: settingsDraft.allowed_platforms?.length ? settingsDraft.allowed_platforms : ["webui"] } });
    settingsDirty.value = false; notify("Spudex settings saved."); await refreshState(true); syncSettings(true);
  } catch (requestError) { notify(requestError instanceof Error ? requestError.message : "Spudex settings failed.", "error"); }
  finally { busy.value = ""; }
}
function togglePlatform(value: string, checked: boolean) {
  const next = new Set((Array.isArray(settingsDraft.allowed_platforms) ? settingsDraft.allowed_platforms : []).map(canonical));
  if (checked) { if (value === "all") next.clear(); next.add(value); }
  else next.delete(value);
  if (value !== "all" && checked) next.delete("all");
  settingsDraft.allowed_platforms = [...next]; settingsDirty.value = true;
}
function chatKeydown(event: KeyboardEvent) { if (event.key === "Enter" && !event.shiftKey && !event.ctrlKey && !event.altKey && !event.metaKey && !event.isComposing) { event.preventDefault(); void sendChat(); } }
function selectSessionFromEvent(event: Event) { selectSession((event.target as HTMLSelectElement).value); }
function clearTerminal() { logs.value = logs.value.filter((entry: JsonRow) => ["user", "assistant"].includes(canonical(entry.stream))); }
function closeSelectedSession() { if (selectedSession.value) void closeSession(selectedSession.value); }
function handleEscape(event: KeyboardEvent) { if (event.key === "Escape") detailsOpen.value = false; }

watch(() => props.state.payload, () => { ensureSelections(); syncSettings(); }, { deep: false });
syncSettings(true); ensureSelections();
window.addEventListener("keydown", handleEscape);
void Promise.all([refreshLogs(true), refreshManualLogs(true)]).catch(() => {});
schedulePoll();
onBeforeUnmount(() => { if (pollTimer) window.clearTimeout(pollTimer); window.removeEventListener("keydown", handleEscape); });
defineExpose({ refresh: () => refreshAll(false) });
</script>

<template>
  <div class="tater-vue-surface tsx-spudex">
    <header class="tsx-spudex-header">
      <div class="tsx-brand"><span class="tsx-spud-mark" aria-hidden="true"><i /><i /><i /></span><div><span class="tv-eyebrow">Tater agent workspace</span><h1>Spudex</h1></div></div>
      <nav class="tv-tabs tsx-tabs" aria-label="Spudex sections"><button v-for="tab in tabs" :key="tab.id" type="button" :class="{ active: activeTab === tab.id }" @click="setTab(tab.id)">{{ tab.label }}<span v-if="tab.id === 'workbench' && activeCount">{{ activeCount }}</span></button></nav>
      <div class="tsx-header-actions"><span class="tv-live-pill" :class="{ busy: Boolean(busy) }"><i />{{ busy ? 'Working' : 'Live' }}</span><button class="tv-button tsx-icon-button" type="button" aria-label="Refresh Spudex" title="Refresh" @click="refreshAll(false)">↻</button></div>
    </header>
    <div v-if="notice || error" class="tv-notice" :class="{ error: Boolean(error) }">{{ error || notice }}</div>

    <section v-if="activeTab === 'workbench'" class="tsx-workbench">
      <div class="tsx-spud-bar">
        <div class="tsx-session-switcher">
          <span class="tsx-bar-label"><i :class="{ live: selectedActive }" />Session</span>
          <select :value="selectedSessionId" aria-label="Selected Spudex session" @change="selectSessionFromEvent">
            <option v-if="!sessions.length" value="">No sessions yet</option>
            <option v-for="session in sessions" :key="session.id" :value="String(session.id)">{{ session.label || session.command || 'Spudex session' }} · {{ statusLabel(session.status) }}</option>
          </select>
          <button class="tv-button primary tsx-new-chat" type="button" :disabled="busy === 'new-chat'" @click="newChat"><span aria-hidden="true">＋</span> New chat</button>
        </div>
        <div class="tsx-runtime-strip">
          <span class="tsx-bar-label"><i :class="{ live: modelProcesses.length }" />Runtime <b>{{ modelProcessCount }}</b></span>
          <div class="tsx-process-strip">
            <span v-if="!modelProcesses.length" class="tsx-runtime-quiet">No tracked processes</span>
            <article v-for="process in modelProcesses" :key="process.session_id" :title="[process.command, process.cwd].filter(Boolean).join(' · ')"><span>{{ process.label || process.command || 'Spudex process' }}</span><small>{{ process.pid ? `PID ${process.pid}` : 'Starting' }}</small><button type="button" aria-label="Stop model process" title="Kill process" @click="stopSession(String(process.session_id), 'Model process')">×</button></article>
          </div>
        </div>
        <div class="tsx-session-tools">
          <button class="tv-button" type="button" :disabled="!selectedSession" @click="detailsOpen = true">Details</button>
          <button class="tv-button danger" type="button" :disabled="!selectedActive" @click="stopSession(selectedSessionId)">Stop</button>
          <button class="tv-button tsx-icon-button" type="button" :disabled="!selectedSession" aria-label="Close selected session" title="Close session" @click="closeSelectedSession">×</button>
        </div>
      </div>

      <div class="tsx-workbench-grid">
        <section class="tv-panel tsx-chat-card">
          <header class="tsx-pane-head"><div><span class="tsx-pane-icon chat" aria-hidden="true">✦</span><div><strong>Chat with Tater</strong><small>{{ selectedSession?.label || selectedSession?.command || 'A fresh Spudex chat' }}</small></div></div><span class="tv-state" :class="{ good: selectedActive }">{{ selectedSession ? statusLabel(selectedSession.status) : 'Ready' }}</span></header>
          <div class="tsx-chat-scroll">
            <div v-if="currentChatSession && chatMessages.length" class="tsx-chat-feed"><ChatMessage v-for="(message, index) in chatMessages" :key="`${index}-${message.role}`" :message="message" :profile="options.profile || {}" :files-endpoint="options.endpoints.chatFiles" /></div>
            <div v-else-if="!chatBusy" class="tsx-chat-empty"><span class="tsx-spud-mark large" aria-hidden="true"><i /><i /><i /></span><h2>What are we building?</h2><p>Ask Tater to inspect, run, or fix something in the protected Spudex workspace.</p></div>
          </div>
          <form class="tsx-composer" @submit.prevent="sendChat"><textarea v-model="chatMessage" rows="1" placeholder="Message Tater through Spudex…" :disabled="chatBusy" @keydown="chatKeydown" /><button class="tv-button primary" type="submit" :disabled="chatBusy || !chatMessage.trim()">{{ chatBusy ? 'Working…' : 'Send' }}</button><small>{{ liveStatus }}</small></form>
        </section>

        <section class="tv-panel tsx-terminal-card">
          <header class="tsx-console-head"><div><span class="tsx-window-dots" aria-hidden="true"><i /><i /><i /></span><div><strong>Activity terminal</strong><small>tater@spudex:{{ selectedSession?.cwd_display || 'workspace' }}</small></div></div><div class="tsx-terminal-actions"><span>Read only</span><button class="tv-button" type="button" :disabled="!nonChatLogs.length" @click="clearTerminal">Clear</button></div></header>
          <div class="tsx-terminal-body" role="log" aria-label="Spudex command output" aria-live="polite">
            <div v-if="nonChatLogs.length" class="tsx-log-list"><article v-for="entry in nonChatLogs" :key="entry.seq || `${entry.ts}-${entry.text}`" :class="canonical(entry.stream)"><time>{{ relativeTime(entry.ts) }}</time><span>{{ entry.stream === 'command' ? '$' : entry.stream || 'log' }}</span><pre>{{ String(entry.text || '').replace(/^\$\s*/, '') }}</pre></article></div>
            <div v-else class="tsx-terminal-empty"><span>&gt;_</span><strong>Waiting for activity</strong><small>Commands, tool output, and system messages will appear here.</small></div>
          </div>
          <footer class="tsx-terminal-status"><span><i :class="{ live: selectedActive }" />{{ selectedActive ? 'Session active' : 'Standing by' }}</span><span>{{ selectedSession ? `#${String(selectedSession.id).slice(0, 8)}` : 'No session' }}</span></footer>
        </section>
      </div>
    </section>

    <section v-else-if="activeTab === 'manual'" class="tsx-manual">
      <section class="tsx-manual-terminal">
        <header class="tsx-manual-head">
          <div><span class="tsx-window-dots" aria-hidden="true"><i /><i /><i /></span><div><strong>Spudex Terminal</strong><small>tater@spudex:{{ manualCwdDisplay }}</small></div></div>
          <div class="tsx-manual-actions"><span class="tsx-manual-state"><i :class="{ live: manualActive || busy === 'run' }" />{{ busy === 'run' || manualActive ? 'Running' : selectedManualSession ? statusLabel(selectedManualSession.status) : 'Ready' }}</span><button class="tv-button" type="button" :disabled="!selectedManualSession" @click="detailsOpen = true">Details</button><button class="tv-button" type="button" :disabled="!manualLogs.length" @click="manualLogs = []">Clear</button><button class="tv-button danger" type="button" :disabled="!manualActive" @click="stopSession(manualSessionId, 'Manual session')">Stop</button></div>
        </header>
        <div class="tsx-manual-console-body" role="log" aria-label="Manual terminal output" aria-live="polite">
          <article v-for="entry in manualLogs" :key="`${entry._session_id || ''}-${entry.seq || entry.ts || ''}-${entry.text || ''}`" :class="canonical(entry.stream)"><span>{{ entry.stream === 'command' ? '$' : entry.stream || 'log' }}</span><pre>{{ String(entry.text || '').replace(/^\$\s*/, '') }}</pre></article>
          <div v-if="!manualLogs.length" class="tsx-manual-welcome"><span class="tsx-terminal-glyph">&gt;_</span><strong>Manual terminal ready.</strong><small>Browse Tater’s protected agent_lab with ls, pwd, and cd. Start at /.</small></div>
        </div>
        <form class="tsx-manual-prompt" @submit.prevent="runCommand">
          <label class="tsx-prompt-line"><span aria-hidden="true">$</span><input v-model="command" type="text" autocomplete="off" aria-label="Terminal command" placeholder="Type a command…" :disabled="busy === 'run'" /></label>
          <label class="tsx-terminal-check"><input v-model="background" class="tv-checkbox" type="checkbox" /><span>Keep running</span></label>
          <button class="tv-button primary tsx-terminal-run" type="submit" :disabled="busy === 'run' || !command.trim()"><span aria-hidden="true">↵</span>{{ busy === 'run' ? 'Running…' : 'Run' }}</button>
        </form>
        <footer class="tsx-manual-footer"><span>Policy checked</span><span>Current directory {{ manualCwdDisplay }}</span></footer>
      </section>
    </section>

    <section v-else class="tsx-settings">
      <div class="tv-panel tsx-access-card"><header><div><span class="tv-eyebrow">Hydra access</span><h2>Spudex availability</h2><p>Expose policy-controlled Spudex tools only on the Tater surfaces you choose.</p></div><label class="tsx-master-toggle"><span>{{ settingsDraft.enabled ? 'Enabled' : 'Off' }}</span><input v-model="settingsDraft.enabled" class="tv-checkbox" type="checkbox" @change="settingsDirty = true" /></label></header><div class="tsx-settings-grid"><label><span>Default working folder</span><input v-model="settingsDraft.default_cwd" type="text" @input="settingsDirty = true" /></label><label><span>Max task steps</span><input v-model.number="settingsDraft.max_task_steps" type="number" min="1" max="50" @input="settingsDirty = true" /></label><label><span>Command timeout (seconds)</span><input v-model.number="settingsDraft.command_timeout_sec" type="number" min="5" max="3600" @input="settingsDirty = true" /></label></div><div class="tsx-platforms"><div><strong>Platforms</strong><small>Select where Hydra can expose Spudex.</small></div><label v-for="option in platformOptions" :key="option.value" :class="{ running: option.running }"><span><strong>{{ option.label || option.value }}</strong><small>{{ option.value === 'all' ? 'Every platform' : option.running ? 'Running' : 'Stopped' }} · {{ option.description || 'Available platform' }}</small></span><input class="tv-checkbox" type="checkbox" :checked="settingsDraft.allowed_platforms?.includes(option.value)" @change="togglePlatform(String(option.value), ($event.target as HTMLInputElement).checked)" /></label></div></div>
      <div class="tv-panel tsx-policy-card"><header><div><span class="tv-eyebrow">Defense in depth</span><h2>Spudex policy</h2><p>Keep command safety on, then allow only the categories a workflow actually needs.</p></div><label class="tsx-master-toggle" :class="{ danger: !settingsDraft.policy_enabled }"><span>{{ settingsDraft.policy_enabled ? 'Policy on' : 'Policy off' }}</span><input v-model="settingsDraft.policy_enabled" class="tv-checkbox" type="checkbox" @change="settingsDirty = true" /></label></header><div class="tsx-policy-notice" :class="{ danger: !settingsDraft.policy_enabled }"><strong>{{ settingsDraft.policy_enabled ? 'Policy is active.' : 'Command safety policy is off.' }}</strong> {{ settingsDraft.policy_enabled ? 'Tater checks commands, paths, network use, installs, and the configurable categories below.' : 'Spudex can use shells, host paths, network commands, installs, and host-affecting tools.' }}</div><div class="tsx-guardrails"><span>Commands start inside <code>agent_lab</code>.</span><span>File writes stay inside <code>agent_lab</code>.</span><span>Model processes stay tracked and stoppable.</span></div><div class="tsx-policy-grid"><label v-for="rule in policyRules" :key="rule[0]"><span><strong>{{ rule[1] }}</strong><small>{{ rule[2] }}</small></span><input v-model="settingsDraft[rule[0]]" class="tv-checkbox" type="checkbox" @change="settingsDirty = true" /></label></div></div>
      <div class="tsx-settings-save"><span>Model routing remains in Settings → Models.</span><button class="tv-button primary" type="button" :disabled="busy === 'settings' || !settingsDirty" @click="saveSettings">{{ busy === 'settings' ? 'Saving…' : 'Save settings' }}</button></div>
    </section>
  </div>

  <PopupTransition :open="detailsOpen" @close="detailsOpen = false"><section class="tv-modal tsx-details" role="dialog" aria-modal="true" aria-label="Session details"><header><div><span class="tv-eyebrow">Session details</span><h2>{{ detailSession?.label || detailSession?.command || 'No session selected' }}</h2></div><button class="tv-button" type="button" @click="detailsOpen = false">Close</button></header><div v-if="detailSession" class="tsx-insights"><div v-if="detailSession.last_policy_block" class="tsx-policy-notice danger"><strong>{{ detailSession.last_policy_block.title || 'Command blocked' }}</strong> {{ detailSession.last_policy_block.reason || detailSession.last_policy_block.message }}<small v-if="detailSession.last_policy_block.toggle">Policy toggle: {{ detailSession.last_policy_block.toggle }}</small></div><article><h3>Plan</h3><ol v-if="detailSession.plan?.length" class="tsx-plan"><li v-for="item in detailSession.plan" :key="item.step" :class="canonical(item.status)"><span>{{ item.step || 'Step' }}</span><small>{{ String(item.status || 'pending').replaceAll('_', ' ') }}<template v-if="item.detail"> · {{ item.detail }}</template></small></li></ol><div v-else class="tv-empty compact">No task plan yet.</div></article><article><h3>Verification</h3><div v-if="detailSession.verification" class="tsx-verification" :class="canonical(detailSession.verification.status)"><strong>{{ detailSession.verification.status === 'passed' ? 'Verification passed' : detailSession.verification.status === 'failed' ? 'Verification failed' : 'Verification recorded' }}</strong><small>{{ detailSession.verification.command }}</small><pre v-if="detailSession.verification.summary">{{ detailSession.verification.summary }}</pre></div><div v-else class="tv-empty compact">No verification run yet.</div></article><article><h3>App previews</h3><div v-if="detailSession.previews?.length" class="tsx-preview-list"><a v-for="preview in detailSession.previews.slice(-6).reverse()" :key="preview.url" :href="preview.url" target="_blank" rel="noreferrer"><span>{{ preview.url }}</span><small>{{ preview.source || 'preview' }}</small></a></div><div v-else class="tv-empty compact">No app previews detected yet.</div></article><article><h3>Git</h3><div v-if="payload.git?.ok" class="tsx-git"><div><strong>{{ payload.git.branch || 'detached' }}</strong><small>{{ payload.git.repo }}</small></div><span class="tv-state" :class="{ good: !payload.git.dirty }">{{ payload.git.dirty ? `${payload.git.changed_count || payload.git.changed_files?.length || 0} changed` : 'Clean' }}</span><pre v-if="payload.git.changed_files?.length">{{ payload.git.changed_files.slice(0, 24).join('\n') }}</pre></div><div v-else class="tv-empty compact">No Git repository detected.</div></article><article class="wide"><h3>File changes</h3><div v-if="detailSession.file_changes?.length" class="tsx-file-list"><section v-for="change in detailSession.file_changes.slice(-6).reverse()" :key="change.id" :class="{ pending: change.pending, applied: change.applied }"><header><div><strong>{{ change.path_display || change.path || 'File change' }}</strong><small>{{ change.pending ? 'Pending' : change.applied ? 'Applied' : 'Rejected' }}<template v-if="change.bytes"> · {{ change.bytes }} bytes</template></small></div><div v-if="change.pending"><button class="tv-button" type="button" @click="fileChange(String(detailSession.id), String(change.id), 'approve')">Approve</button><button class="tv-button danger" type="button" @click="fileChange(String(detailSession.id), String(change.id), 'reject')">Reject</button></div></header><pre>{{ change.diff || 'No textual diff available.' }}</pre></section></div><div v-else class="tv-empty compact">No file changes yet.</div></article><article class="wide"><h3>Session memory</h3><p v-if="detailSession.memory_summary">{{ detailSession.memory_summary }}</p><div v-else class="tv-empty compact">No session memory yet.</div></article></div><div v-else class="tv-empty">Select a session to see its details.</div></section></PopupTransition>
</template>
