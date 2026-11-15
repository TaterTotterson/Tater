<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, useId } from "vue";
import { getJson, postJson } from "../shared/api";
import PopupTransition from "../shared/PopupTransition.vue";
import UiIcon from "../shared/UiIcon.vue";
import { modelPresentation } from "../settings/components/models/modelPresentation";
import type { JsonRow, RuntimeStatusMountOptions, RuntimeStatusState } from "./types";
import "./runtime.css";

type RuntimeTab = "overview" | "activity" | "models" | "context";
type StreamState = "connecting" | "live" | "reconnecting" | "paused" | "unsupported";

const props = defineProps<{ state: RuntimeStatusState; options: RuntimeStatusMountOptions; assistantName?: string; compact?: boolean }>();
const open = ref(false);
const loading = ref(false);
const error = ref("");
const notice = ref("");
const detailsUpdatedAt = ref("");
const telemetryUpdatedAt = ref("");
const breakdown = ref<JsonRow>({});
const telemetry = ref<JsonRow>({});
const streamState = ref<StreamState>("connecting");
const activeTab = ref<RuntimeTab>("overview");
const closeButton = ref<HTMLButtonElement | null>(null);
const triggerButton = ref<HTMLButtonElement | null>(null);
const monitorDialog = ref<HTMLElement | null>(null);
const tabNav = ref<HTMLElement | null>(null);
const monitorId = `runtime-monitor-${useId()}`;
const unloading = ref("");
let pollTimer = 0;
let eventSource: EventSource | null = null;

const assistantRuntimeLabel = computed(() => `${String(props.assistantName || "Tater").trim() || "Tater"} runtime`);

const health = computed(() => props.state.health || {});
const healthMemory = computed(() => row(health.value.loaded_models || health.value.loadedModels));
const streamedSystem = computed(() => row(telemetry.value.system));
const healthSystem = computed(() => Object.keys(streamedSystem.value).length ? streamedSystem.value : row(healthMemory.value.system));
const healthCpu = computed(() => row(healthSystem.value.cpu));
const healthRam = computed(() => row(healthSystem.value.ram));
const healthVram = computed(() => row(healthSystem.value.vram));
const loadedCount = computed(() => num(modelsPayload.value.loaded_count ?? healthMemory.value.loaded_count));
const baseText = computed(() => props.state.text || `${num(health.value.verbas_enabled)} verba enabled • ${num(health.value.portals_running)} portals running • ${num(health.value.cores_running)} cores running • ${num(health.value.hydra_jobs_active ?? health.value.chat_jobs_active)} hydra jobs • ${num(health.value.llm_calls_active)} llm calls • ${num(health.value.vision_calls_active ?? health.value.voice_calls_active)} vision calls`);
const pillSummary = computed(() => props.state.health
  ? `${num(health.value.verbas_enabled)} Verbas · ${num(health.value.portals_running)} Portals · ${num(health.value.cores_running)} Cores`
  : baseText.value);
const activeWork = computed(() => num(health.value.hydra_jobs_active ?? health.value.chat_jobs_active) + num(health.value.llm_calls_active) + num(health.value.vision_calls_active ?? health.value.voice_calls_active));
const streamStatusLabel = computed(() => ({ connecting: "Connecting", live: "Live", reconnecting: "Reconnecting", paused: "Paused", unsupported: "Polling fallback" }[streamState.value]));
const pillResources = computed(() => {
  const ramTotal = num(healthRam.value.total_bytes); const ramUsed = num(healthRam.value.used_bytes);
  const vramTotal = num(healthVram.value.total_bytes); const vramUsed = num(healthVram.value.used_bytes);
  const gpuPercent = percent(healthVram.value.utilization_percent);
  const unified = Boolean(healthSystem.value.unified_memory || healthVram.value.unified);
  const resources = [
    resource("CPU", healthCpu.value.percent, healthCpu.value.available === false),
    resource("GPU", gpuPercent, gpuPercent === null),
    resource(unified ? "MEM" : "RAM", ramTotal > 0 ? healthRam.value.percent ?? (ramUsed / ramTotal) * 100 : null, ramTotal <= 0),
  ];
  if (!unified) resources.push(resource("VRAM", vramTotal > 0 ? healthVram.value.percent ?? (vramUsed / vramTotal) * 100 : null, vramTotal <= 0));
  return resources;
});

const hydra = computed(() => row(breakdown.value.hydra_jobs || breakdown.value.chat_jobs));
const llm = computed(() => row(breakdown.value.llm_calls));
const vision = computed(() => row(breakdown.value.vision_calls || breakdown.value.voice_calls));
const context = computed(() => row(breakdown.value.chat_context_window));
const modelsPayload = computed(() => row(breakdown.value.loaded_models));
const modelSystem = computed(() => Object.keys(streamedSystem.value).length ? streamedSystem.value : row(modelsPayload.value.system));
const modelCpu = computed(() => row(modelSystem.value.cpu));
const modelRam = computed(() => row(modelSystem.value.ram));
const modelVram = computed(() => row(modelSystem.value.vram));
const models = computed<JsonRow[]>(() => rows(modelsPayload.value.models));
const gpuDevices = computed<JsonRow[]>(() => rows(modelVram.value.devices));
const hydraTurns = computed<JsonRow[]>(() => rows(hydra.value.active_turns));
const llmCalls = computed<JsonRow[]>(() => rows(llm.value.active_calls));
const visionCalls = computed<JsonRow[]>(() => rows(vision.value.active_calls));
const activityCount = computed(() => hydraTurns.value.length + num(llm.value.running_total ?? llm.value.active_total) + num(vision.value.active_total));
const modelTotals = computed(() => row(modelsPayload.value.totals));
const displayedActivityCount = computed(() => Object.keys(breakdown.value).length ? activityCount.value : activeWork.value);
const assistantLabel = computed(() => String(props.assistantName || "Tater").trim() || "Tater");
const runtimeTone = computed(() => props.state.tone !== "normal" ? props.state.tone : streamState.value === "reconnecting" || error.value ? "degraded" : "normal");
const runtimeStateLabel = computed(() => {
  if (props.state.tone === "offline") return "Backend offline";
  if (props.state.tone === "degraded") return "Runtime needs attention";
  if (error.value) return "Runtime details unavailable";
  if (streamState.value === "reconnecting") return `Reconnecting to ${assistantLabel.value}`;
  return `${assistantLabel.value} is ${displayedActivityCount.value || num(llm.value.queued_total) ? "working" : "ready"}`;
});
const runtimeDetailText = computed(() => {
  if (props.state.tone === "offline") return "The backend is unreachable. Checking the connection.";
  if (props.state.tone === "degraded") return props.state.text || "Some services need attention. Check the activity and model details.";
  if (error.value) return "Some details could not be refreshed. Showing the latest available readings.";
  if (streamState.value === "reconnecting") return "Showing the latest available readings while the connection recovers.";
  if (displayedActivityCount.value) return `${countLabel(displayedActivityCount.value, "active operation")}${num(llm.value.queued_total) ? ` · ${countLabel(llm.value.queued_total, "model call")} queued` : ""}.`;
  if (num(llm.value.queued_total)) return `${integer(llm.value.queued_total)} model call${num(llm.value.queued_total) === 1 ? "" : "s"} waiting to run.`;
  return "Your services are connected and ready to help.";
});
const runtimeTabs = computed(() => [
  { id: "overview" as RuntimeTab, icon: "dashboard", label: "Overview", meta: "" },
  { id: "activity" as RuntimeTab, icon: "activity", label: "Activity", meta: String(displayedActivityCount.value) },
  { id: "models" as RuntimeTab, icon: "cores", label: "Models", meta: String(loadedCount.value) },
  { id: "context" as RuntimeTab, icon: "chart", label: "Context", meta: "" },
]);
const previewModels = computed(() => [...models.value.filter(model => !model.managed), ...models.value.filter(model => model.managed)].slice(0, 2));
const activityHeadline = computed(() => {
  const turn = hydraTurns.value[0];
  if (turn) return text(turn.task_name) || "Hydra is working";
  const call = llmCalls.value[0] || visionCalls.value[0];
  if (call) return text(call.source_label || call.label) || "Processing a model call";
  if (num(llm.value.queued_total)) return "Model calls are waiting";
  return displayedActivityCount.value ? "Work is in progress" : "Ready for your next request";
});
const activityDescription = computed(() => displayedActivityCount.value || num(llm.value.queued_total)
  ? `${countLabel(hydraTurns.value.length, "Hydra task")} · ${countLabel(llm.value.running_total ?? llm.value.active_total, "model call")} · ${num(llm.value.queued_total)} queued · ${countLabel(vision.value.active_total, "vision call")}`
  : "No tasks or model calls are running right now.");
const modelSummary = computed(() => {
  const totals = modelTotals.value;
  return [
    `${num(modelsPayload.value.loaded_count ?? loadedCount.value)} loaded`,
    num(modelsPayload.value.local_llm_loaded_count) ? `${num(modelsPayload.value.local_llm_loaded_count)} LLM` : "",
    num(modelsPayload.value.managed_loaded_count) ? `${num(modelsPayload.value.managed_loaded_count)} managed` : "",
    num(totals.estimated_total_bytes) ? `est ${bytes(totals.estimated_total_bytes)}` : "",
    num(totals.estimated_vram_bytes) ? `VRAM est ${bytes(totals.estimated_vram_bytes)}` : "",
    num(totals.estimated_ram_bytes) ? `RAM est ${bytes(totals.estimated_ram_bytes)}` : "",
    num(totals.estimated_unified_bytes) ? `unified est ${bytes(totals.estimated_unified_bytes)}` : "",
  ].filter(Boolean).join(" • ") || "No loaded runtime models";
});
const contextRows = computed(() => {
  const values = row(context.value.breakdown);
  const historyMessages = num(context.value.history_messages);
  const maxHistory = num(context.value.max_history_messages) || historyMessages;
  const result = [
    ["System prompt", values.system_tokens],
    ["Runtime status", values.status_tokens],
    ["Core context + preamble", num(values.core_context_tokens) + num(values.platform_preamble_tokens)],
    [`Chat history (${historyMessages}/${maxHistory} msgs)`, values.history_tokens],
    ["Current user turn", values.user_tokens],
  ];
  const reserve = num(context.value.capability_context_reserve_tokens ?? values.capability_reserve_tokens);
  if (reserve) result.push(["Capability reserve", reserve]);
  return result.map(([label, tokens]) => ({ label: String(label), tokens: num(tokens) }));
});
const contextSummary = computed(() => [
  `Prompt ${integer(context.value.prompt_tokens)} tok`, `Reply budget ${integer(context.value.completion_budget_tokens)} tok`,
  num(context.value.capability_context_reserve_tokens) ? `Capability reserve ${integer(context.value.capability_context_reserve_tokens)} tok` : "",
  num(context.value.burst_context_reserve_tokens) ? `Burst reserve ${integer(context.value.burst_context_reserve_tokens)} tok` : "",
  `Min window ${integer(context.value.minimum_context_window)}`, `Recommended ${integer(context.value.recommended_context_window)}`,
].filter(Boolean).join(" • "));
const contextNotes = computed(() => {
  const values = row(context.value.breakdown); const examples = strings(values.high_context_verba_examples).slice(0, 4);
  return [
    num(context.value.burst_context_reserve_tokens) ? `Recommended window includes ${integer(context.value.burst_context_reserve_tokens)} tokens of burst reserve for heavy or multi-tool turns.` : "",
    num(values.high_context_verbas) || num(values.heavy_cores) ? `High-context signals: ${num(values.high_context_verbas)} high-context verbas • ${num(values.heavy_cores)} heavy cores${examples.length ? ` • e.g. ${examples.join(", ")}` : ""}` : "",
  ].filter(Boolean);
});

function row(value: unknown): JsonRow { return value && typeof value === "object" && !Array.isArray(value) ? value as JsonRow : {}; }
function rows(value: unknown): JsonRow[] { return Array.isArray(value) ? value.filter((item) => item && typeof item === "object") as JsonRow[] : []; }
function strings(value: unknown): string[] { return Array.isArray(value) ? value.map((item) => text(item)).filter(Boolean) : []; }
function numbers(value: unknown): number[] { return Array.isArray(value) ? value.map((item) => Number(item)).filter(Number.isFinite) : []; }
function text(value: unknown): string { return String(value ?? "").trim(); }
function num(value: unknown): number { const parsed = Number(value); return Number.isFinite(parsed) ? Math.max(0, parsed) : 0; }
function integer(value: unknown): string { return Math.round(num(value)).toLocaleString(); }
function countLabel(value: unknown, singular: string): string { return `${integer(value)} ${singular}${num(value) === 1 ? "" : "s"}`; }
function percent(value: unknown): number | null { if (value === null || value === undefined || text(value) === "") return null; const parsed = Number(value); return Number.isFinite(parsed) && parsed >= 0 ? Math.max(0, Math.min(100, parsed)) : null; }
function percentText(value: unknown): string { const parsed = percent(value); return parsed === null ? "n/a" : `${Math.round(parsed)}%`; }
function bytes(value: unknown): string { let amount = num(value); if (!amount) return "0 B"; const units = ["B", "KB", "MB", "GB", "TB"]; let index = 0; while (amount >= 1024 && index < units.length - 1) { amount /= 1024; index += 1; } return `${amount >= 10 || index === 0 ? amount.toFixed(0) : amount.toFixed(1)} ${units[index]}`; }
function resource(label: string, rawPercent: unknown, unavailable: boolean) { const value = percent(rawPercent); return { label, percent: value ?? 0, value: value === null ? "n/a" : `${Math.round(value)}%`, unavailable }; }
function age(value: unknown): string { const seconds = Math.round(num(value)); if (seconds < 60) return `${seconds}s`; const minutes = Math.floor(seconds / 60); return minutes < 60 ? `${minutes}m ${seconds % 60}s` : `${Math.floor(minutes / 60)}h ${minutes % 60}m`; }
function loadedAt(value: unknown): string { const stamp = num(value); return stamp ? `Loaded ${new Date(stamp * 1000).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}` : ""; }
function modelMeta(model: JsonRow): string { const estimate = model.remote ? "" : num(model.estimated_bytes) ? `${text(model.memory_kind || "ram").toUpperCase()} est ${bytes(model.estimated_bytes)}` : "Estimate unavailable"; return [text(model.kind_label || model.category), text(model.provider_label || model.provider || "Local"), text(model.device) ? `Device ${text(model.device)}` : "", estimate, loadedAt(model.loaded_ts)].filter(Boolean).join(" • "); }
function modelDetails(model: JsonRow): string[] { return [...strings(model.details), model.managed ? text(model.managed_by || "Managed by settings") : ""].filter(Boolean); }
function modelName(model: JsonRow): string {
  const raw = text(model.display_name || model.model) || "Model";
  if (model.display_name) return raw;
  if (model.managed && text(model.category) === "speaker_id") return "Speaker ID";
  if (model.managed && text(model.category) === "emotion_id") return "Emotion ID";
  if (model.managed && ["stt", "tts"].includes(text(model.category)) && text(model.provider_label).includes("•")) return text(model.provider_label).split("•").pop()!.trim().replace(/\s*\([^)]*\)\s*$/, "") || raw;
  if (model.managed && /^[a-f0-9]{8,}$/i.test(raw)) return text(model.provider_label || model.provider).split("•").pop()?.trim() || raw;
  return modelPresentation(raw, text(model.model)).name;
}
function modelIcon(model: JsonRow): string {
  const category = text(model.category).toLowerCase();
  return category === "llm" ? "brain" : category === "face_id" || category === "speaker_id" ? "users" : category === "emotion_id" ? "sparkles" : category === "stt" || category === "tts" ? "mic" : category === "vision" ? "eye" : "server";
}
function modelEstimate(model: JsonRow): string { return model.remote ? "Spud Hub" : num(model.estimated_bytes) ? `${bytes(model.estimated_bytes)} estimated` : "Estimate unavailable"; }
function contextRowPercent(tokens: unknown): number { const amount = num(tokens); const total = num(context.value.prompt_tokens); return amount > 0 && total > 0 ? Math.min(100, (amount / total) * 100) : 0; }
function gpuMeta(device: JsonRow): string {
  const power = Number(device.power_draw_w); const powerLimit = Number(device.power_limit_w);
  return [percent(device.utilization_percent) === null ? "GPU load n/a" : `GPU ${percentText(device.utilization_percent)}`, num(device.total_bytes) ? `${device.unified ? "GPU memory" : "VRAM"} ${bytes(device.used_bytes)} / ${bytes(device.total_bytes)}` : "", num(device.shared_memory_total_bytes) ? `Shared RAM ${bytes(device.shared_memory_used_bytes)} / ${bytes(device.shared_memory_total_bytes)}` : "", Number.isFinite(Number(device.temperature_c)) ? `${Number(device.temperature_c).toFixed(0)} C` : "", Number.isFinite(power) ? `${power.toFixed(0)} W${Number.isFinite(powerLimit) && powerLimit > 0 ? ` / ${powerLimit.toFixed(0)} W` : ""}` : "", text(device.detail)].filter(Boolean).join(" • ");
}
function callMeta(call: JsonRow, kind: "llm" | "vision"): string { return [`Model ${text(call.model || "model")}`, kind === "llm" ? text(call.host) : text(call.api_base), text(call.activity) ? `Activity ${text(call.activity)}` : text(call.function) ? `Fn ${text(call.function)}` : "", num(call.message_count) ? `${num(call.message_count)} msgs` : ""].filter(Boolean).join(" • "); }
function metric(label: string, used: unknown, total: unknown, rawPercent?: unknown, detail = "") { const totalValue = num(total); const usedValue = num(used); const explicit = percent(rawPercent); const value = explicit ?? (totalValue > 0 ? Math.max(0, Math.min(100, (usedValue / totalValue) * 100)) : null); return { label, percent: value ?? 0, value: rawPercent !== undefined ? percentText(rawPercent) : totalValue > 0 ? `${bytes(usedValue)} / ${bytes(totalValue)}` : "Unavailable", unavailable: value === null, detail }; }
const meters = computed(() => {
  const unified = Boolean(modelSystem.value.unified_memory || modelVram.value.unified);
  const values = [
    metric("CPU Usage", 0, 0, modelCpu.value.percent, [num(modelCpu.value.logical_count) ? `${num(modelCpu.value.logical_count)} logical cores` : "", num(modelCpu.value.physical_count) ? `${num(modelCpu.value.physical_count)} physical cores` : "", numbers(modelCpu.value.load_average).length ? `load ${numbers(modelCpu.value.load_average).map((item) => item.toFixed(2)).join(" / ")}` : ""].filter(Boolean).join(" • ") ),
    metric("GPU Usage", 0, 0, modelVram.value.utilization_percent, [text(modelVram.value.backend) ? `Backend ${text(modelVram.value.backend)}` : "", gpuDevices.value.length ? `${gpuDevices.value.length} device${gpuDevices.value.length === 1 ? "" : "s"}` : "", unified ? "shared/unified memory" : "", percent(modelVram.value.utilization_percent) === null ? "GPU load unavailable from this runtime" : ""].filter(Boolean).join(" • ") ),
    metric(unified ? "Unified Memory" : "System RAM", modelRam.value.used_bytes, modelRam.value.total_bytes),
  ];
  if (!unified) values.push(metric("System VRAM", modelVram.value.used_bytes, modelVram.value.total_bytes));
  return values;
});
function meterLabel(label: string): string { return ({ "CPU Usage": "CPU", "GPU Usage": "GPU", "Unified Memory": "Memory", "System RAM": "RAM", "System VRAM": "VRAM" } as Record<string, string>)[label] || label; }
function meterSummary(label: string): string {
  if (label === "CPU Usage") return [num(modelCpu.value.logical_count) ? `${num(modelCpu.value.logical_count)} cores` : "", numbers(modelCpu.value.load_average).length ? `Load ${numbers(modelCpu.value.load_average)[0].toFixed(2)}` : ""].filter(Boolean).join(" · ");
  if (label === "GPU Usage") return [text(gpuDevices.value[0]?.name), text(modelVram.value.backend)].filter(Boolean).join(" · ") || "GPU telemetry";
  return label === "Unified Memory" ? "Shared system memory" : label === "System VRAM" ? "Dedicated GPU memory" : "System memory";
}

async function selectTab(tab: RuntimeTab, focus = false) {
  activeTab.value = tab;
  if (focus) { await nextTick(); tabNav.value?.querySelector<HTMLButtonElement>(`[data-runtime-tab="${tab}"]`)?.focus(); }
}
function handleTabKey(event: KeyboardEvent) {
  if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
  event.preventDefault();
  const index = runtimeTabs.value.findIndex(tab => tab.id === activeTab.value);
  const next = event.key === "Home" ? 0 : event.key === "End" ? runtimeTabs.value.length - 1 : (index + (event.key === "ArrowRight" ? 1 : runtimeTabs.value.length - 1)) % runtimeTabs.value.length;
  void selectTab(runtimeTabs.value[next].id, true);
}

function applyTelemetryEvent(event: Event) {
  try {
    const payload = JSON.parse(String((event as MessageEvent<string>).data || "{}"));
    if (!payload || typeof payload !== "object") return;
    telemetry.value = payload; streamState.value = "live";
    const sampledAt = Number(payload.sampled_at || 0);
    telemetryUpdatedAt.value = new Date(sampledAt > 0 ? sampledAt * 1000 : Date.now()).toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" });
  } catch { streamState.value = "reconnecting"; }
}
function stopTelemetry(nextState: StreamState = "paused") { eventSource?.close(); eventSource = null; streamState.value = nextState; }
function startTelemetry() {
  if (document.visibilityState === "hidden") { stopTelemetry("paused"); return; }
  if (eventSource) return;
  if (typeof window.EventSource !== "function") { streamState.value = "unsupported"; return; }
  streamState.value = "connecting";
  const source = new EventSource(props.options.endpoints.telemetry); eventSource = source;
  source.addEventListener("telemetry", applyTelemetryEvent);
  source.onopen = () => { if (eventSource === source) streamState.value = "live"; };
  source.onerror = () => { if (eventSource === source) streamState.value = "reconnecting"; };
}
function handleVisibility() { if (document.visibilityState === "hidden") stopTelemetry("paused"); else startTelemetry(); }
async function refresh(silent = false) {
  if (loading.value) return;
  loading.value = true; error.value = ""; if (!silent && !Object.keys(breakdown.value).length) notice.value = "Loading runtime state…";
  try {
    const baseEndpoint = props.options.endpoints.breakdown; const endpoint = silent ? baseEndpoint : `${baseEndpoint}${baseEndpoint.includes("?") ? "&" : "?"}refresh=true`;
    const payload = await getJson<JsonRow>(endpoint); breakdown.value = payload || {}; detailsUpdatedAt.value = new Date().toLocaleTimeString([], { hour: "numeric", minute: "2-digit", second: "2-digit" }); notice.value = ""; props.options.onBreakdownChange?.(payload || {});
  } catch (requestError) { error.value = requestError instanceof Error ? requestError.message : "Runtime breakdown failed."; }
  finally { loading.value = false; }
}
function startPolling() { stopPolling(); pollTimer = window.setInterval(() => { if (open.value) void refresh(true); }, 5000); }
function stopPolling() { if (pollTimer) window.clearInterval(pollTimer); pollTimer = 0; }
async function openPopup() { activeTab.value = "overview"; open.value = true; await nextTick(); closeButton.value?.focus(); await refresh(false); startPolling(); }
function closePopup() { open.value = false; stopPolling(); triggerButton.value?.focus(); }
async function unloadModel(model: JsonRow) {
  const key = text(model.cache_key || model.model); if (!key || unloading.value) return; unloading.value = key;
  try { const result = await postJson<JsonRow>(props.options.endpoints.unloadModel, { provider: text(model.provider), model: text(model.model), cache_key: text(model.cache_key) }); const count = num(result.unloaded_count); const message = count ? `Unloaded ${count} local model${count === 1 ? "" : "s"}.` : "No loaded model matched."; props.options.onToast?.(message, "success"); await refresh(true); props.options.onHealthRefresh?.(); }
  catch (requestError) { const message = requestError instanceof Error ? requestError.message : "Model unload failed."; error.value = `Unload failed: ${message}`; props.options.onToast?.(error.value, "error"); }
  finally { unloading.value = ""; }
}
function handleKey(event: KeyboardEvent) {
  if (!open.value) return;
  if (event.key === "Escape") { event.preventDefault(); closePopup(); return; }
  if (event.key !== "Tab" || !monitorDialog.value) return;
  const controls = Array.from(monitorDialog.value.querySelectorAll<HTMLElement>('button:not([disabled]), summary, [href], [tabindex="0"]')).filter(element => element.tabIndex >= 0 && element.getClientRects().length);
  const first = controls[0], last = controls[controls.length - 1];
  if (!first || !last) return;
  const outside = !monitorDialog.value.contains(document.activeElement);
  if (event.shiftKey && (document.activeElement === first || outside)) { event.preventDefault(); last.focus(); }
  else if (!event.shiftKey && (document.activeElement === last || outside)) { event.preventDefault(); first.focus(); }
}
onMounted(() => { window.addEventListener("keydown", handleKey); document.addEventListener("visibilitychange", handleVisibility); startTelemetry(); });
onBeforeUnmount(() => { stopPolling(); stopTelemetry("paused"); window.removeEventListener("keydown", handleKey); document.removeEventListener("visibilitychange", handleVisibility); });
defineExpose({ open: openPopup });
</script>

<template>
  <button ref="triggerButton" class="tr-pill" :class="[state.tone, `stream-${streamState}`, { 'tr-compact': compact }]" type="button" :title="`${baseText}. Open the live runtime monitor.`" aria-haspopup="dialog" :aria-expanded="open" :aria-controls="monitorId" @click="openPopup">
    <span v-if="compact" class="workspace-runtime-label"><i />{{ state.tone === 'offline' ? 'Backend offline' : state.tone === 'degraded' ? 'Needs attention' : activeWork ? `${activeWork} active operations` : state.health ? 'System connected' : state.text || 'Checking system…' }}<span aria-hidden="true">›</span></span>
    <span class="tr-pill-brand"><span class="tr-pill-copy"><strong>{{ assistantRuntimeLabel }}</strong><small>{{ pillSummary }}</small></span></span>
    <span v-if="state.health" class="tr-pill-resources"><span v-for="item in pillResources" :key="item.label" class="tr-resource" :class="{ unavailable: item.unavailable }"><span><b>{{ item.label }}</b><em>{{ item.value }}</em></span><span class="tr-resource-track"><i :style="{ width: `${item.percent}%` }" /></span></span></span>
    <span class="tr-pill-end"><span><b>{{ loadedCount }}</b><small>models</small></span><i>›</i></span>
  </button>

  <PopupTransition :open="open" backdrop-class="tv-modal-backdrop tr-backdrop" @close="closePopup">
    <section :id="monitorId" ref="monitorDialog" class="tv-modal tr-modal tr-clear-tabs" role="dialog" aria-modal="true" :aria-labelledby="`${monitorId}-title`" :aria-describedby="`${monitorId}-description`">
      <header class="trc-header">
        <div><div class="trc-title"><UiIcon name="activity" /><h2 :id="`${monitorId}-title`">System monitor</h2></div><p :id="`${monitorId}-description`">A clear view of what {{ assistantLabel }} is doing.</p></div>
        <div class="trc-header-actions"><span class="trc-live" :class="`stream-${streamState}`"><i />{{ streamStatusLabel }}</span><button ref="closeButton" class="trc-icon-button" type="button" aria-label="Close system monitor" @click="closePopup"><UiIcon name="close" /></button></div>
      </header>
      <nav ref="tabNav" class="trc-tabs" role="tablist" aria-label="System monitor sections">
        <button v-for="tab in runtimeTabs" :id="`${monitorId}-${tab.id}`" :key="tab.id" :data-runtime-tab="tab.id" type="button" role="tab" :aria-selected="activeTab === tab.id" :aria-controls="`${monitorId}-panel`" :tabindex="activeTab === tab.id ? 0 : -1" @click="selectTab(tab.id)" @keydown="handleTabKey"><UiIcon :name="tab.icon" /><span>{{ tab.label }}</span><small v-if="tab.meta" class="trc-count">{{ tab.meta }}</small></button>
      </nav>
      <div :id="`${monitorId}-panel`" class="trc-content" role="tabpanel" :aria-labelledby="`${monitorId}-${activeTab}`">
        <p v-if="error" class="trc-notice error" role="alert">{{ error }}</p>
        <div v-if="!Object.keys(breakdown).length && loading" class="trc-empty" role="status">Loading runtime state…</div>

        <section v-else-if="activeTab === 'overview'" class="trc-overview">
          <article class="trc-ready" :class="runtimeTone"><span class="trc-ready-icon"><UiIcon :name="runtimeTone !== 'normal' ? 'alert' : displayedActivityCount || num(llm.queued_total) ? 'activity' : 'check'" /></span><div><h3>{{ runtimeStateLabel }}</h3><p>{{ runtimeDetailText }}</p></div><button class="trc-button trc-ready-action" type="button" @click="selectTab('activity', true)">View details</button></article>
          <div class="trc-resources">
            <article v-for="meter in meters" :key="meter.label" class="trc-meter" :class="{ unavailable: meter.unavailable }"><div class="trc-meter-heading"><strong>{{ meterLabel(meter.label) }}</strong><span>{{ meter.value }}</span></div><span class="trc-track" aria-hidden="true"><i :style="{ width: `${meter.percent}%` }" /></span><details v-if="meter.detail" class="trc-meter-details"><summary>{{ meterSummary(meter.label) }}</summary><p>{{ meter.detail }}</p></details><small v-else>{{ meterSummary(meter.label) }}</small></article>
          </div>
          <section><header class="trc-section-heading"><h3>Current activity</h3><button class="trc-link" type="button" @click="selectTab('activity', true)">View activity <UiIcon name="chevron" /></button></header><div class="trc-activity-summary"><UiIcon :name="displayedActivityCount || num(llm.queued_total) ? 'activity' : 'coffee'" /><div><strong>{{ activityHeadline }}</strong><p>{{ activityDescription }}</p></div></div></section>
          <section><header class="trc-section-heading"><h3>Loaded models <small class="trc-count">{{ loadedCount }}</small></h3><button class="trc-link" type="button" @click="selectTab('models', true)">View all <UiIcon name="chevron" /></button></header><div v-if="previewModels.length" class="trc-preview-models"><article v-for="model in previewModels" :key="model.cache_key || `${model.provider}:${model.model}`"><UiIcon :name="modelIcon(model)" /><div><strong>{{ modelName(model) }}</strong><small>{{ modelDetails(model)[0] || model.kind_label || model.category || 'Model' }}</small></div><span class="trc-tag">{{ model.remote ? 'Spud Hub' : num(model.estimated_bytes) ? bytes(model.estimated_bytes) : 'Loaded' }}</span></article></div><p v-else class="trc-help">{{ loadedCount ? 'Model details are not available yet.' : 'No runtime models are loaded right now.' }}</p></section>
          <section><header class="trc-section-heading"><h3>Connected services</h3><span class="trc-connection-state" :class="runtimeTone"><UiIcon :name="runtimeTone === 'normal' ? 'check' : 'alert'" />{{ runtimeTone === 'normal' ? 'All connected' : runtimeTone === 'offline' ? 'Backend offline' : 'Needs attention' }}</span></header><p class="trc-help">{{ num(health.verbas_enabled) }} Verba enabled · {{ num(health.cores_running) }} cores running · {{ num(health.portals_running) }} portals running</p></section>
        </section>

        <section v-else-if="activeTab === 'activity'" class="trc-activity-view">
          <header class="trc-view-heading"><h3>Current activity</h3><p>What {{ assistantLabel }} is working on right now.</p></header>
          <section class="trc-work-group"><header><span class="trc-work-icon"><UiIcon name="activity" /></span><div><h4>Hydra tasks</h4><p>{{ hydraTurns.length ? countLabel(hydraTurns.length, 'active task') : 'No active tasks' }}</p></div><span class="trc-tag" :class="{ good: !hydraTurns.length }">{{ hydraTurns.length ? 'Working' : 'Ready' }}</span></header>
            <div v-if="hydraTurns.length" class="trc-work-list"><article v-for="turn in hydraTurns" :key="turn.id"><div class="trc-call-heading"><strong>{{ turn.task_name || 'Hydra task' }}</strong><span class="trc-tag">Running {{ age(turn.age_seconds) }}</span></div><p>{{ turn.platform_label || turn.platform || 'Unknown' }}<span v-if="turn.source"> · {{ turn.source }}</span></p><p v-if="turn.current_tool">Current Verba/tool: {{ turn.current_tool }}</p><details><summary>Task details</summary><p v-if="turn.id">Turn {{ turn.id }}</p><p v-if="turn.scope">Scope: {{ turn.scope }}</p></details></article></div>
            <details class="trc-work-details"><summary>Runtime details</summary><p><b>{{ num(hydra.total) }}</b> total · <b>{{ num(hydra.webui_jobs) }}</b> Web UI queue · <b>{{ num(hydra.surface_running_turns) }}</b> surface turns</p></details>
          </section>
          <section class="trc-work-group"><header><span class="trc-work-icon"><UiIcon name="brain" /></span><div><h4>Language model calls</h4><p>{{ num(llm.running_total ?? llm.active_total) }} running · {{ num(llm.queued_total) }} queued</p></div><span class="trc-tag" :class="{ good: !num(llm.running_total ?? llm.active_total) && !num(llm.queued_total) }">{{ num(llm.running_total ?? llm.active_total) ? 'Working' : num(llm.queued_total) ? 'Queued' : 'Ready' }}</span></header>
            <div v-if="llmCalls.length" class="trc-work-list"><article v-for="(call, index) in llmCalls" :key="call.id || index"><div class="trc-call-heading"><strong>{{ call.source_label || call.label || 'Unknown source' }}</strong><span class="trc-tag">{{ call.state_label || (text(call.state) === 'queued' ? 'Queued' : 'Running') }} {{ age(call.state_age_seconds ?? call.age_seconds) }}</span></div><details><summary>Call details</summary><p>{{ callMeta(call, 'llm') }}</p></details></article></div>
            <details class="trc-work-details"><summary>Runtime details</summary><p><b>{{ num(llm.running_total ?? llm.active_total) }}</b> running · <b>{{ num(llm.queued_total) }}</b> queued · <b>{{ num(llm.totals?.completed) }}</b> completed</p></details>
          </section>
          <section class="trc-work-group"><header><span class="trc-work-icon"><UiIcon name="eye" /></span><div><h4>Vision calls</h4><p>{{ num(vision.active_total) ? countLabel(vision.active_total, 'active call') : 'No active vision calls' }}</p></div><span class="trc-tag" :class="{ good: !num(vision.active_total) }">{{ num(vision.active_total) ? 'Working' : 'Ready' }}</span></header>
            <div v-if="visionCalls.length" class="trc-work-list"><article v-for="(call, index) in visionCalls" :key="call.id || index"><div class="trc-call-heading"><strong>{{ call.source_label || call.label || 'Unknown source' }}</strong><span class="trc-tag">Running {{ age(call.age_seconds) }}</span></div><details><summary>Call details</summary><p>{{ callMeta(call, 'vision') }}</p></details></article></div>
            <details class="trc-work-details"><summary>Runtime details</summary><p><b>{{ num(vision.active_total) }}</b> active · <b>{{ num(vision.totals?.completed) }}</b> completed · <b>{{ num(vision.totals?.failed) }}</b> failed</p></details>
          </section>
        </section>

        <section v-else-if="activeTab === 'models'" class="trc-models-view">
          <header class="trc-view-heading"><h3>Loaded models <small class="trc-count">{{ loadedCount }}</small></h3><p v-if="Object.keys(modelsPayload).length">{{ countLabel(modelsPayload.local_llm_loaded_count, 'language model') }} · {{ countLabel(modelsPayload.managed_loaded_count, 'managed service') }}<span v-if="num(modelTotals.estimated_total_bytes)"> · Estimated memory {{ bytes(modelTotals.estimated_total_bytes) }}</span></p></header>
          <details class="trc-hardware-details"><summary>Compute &amp; memory details</summary><p>{{ modelSummary }}</p><section v-if="gpuDevices.length"><h4>GPU devices</h4><article v-for="(device, index) in gpuDevices" :key="device.index ?? index"><strong>{{ device.name || `GPU ${device.index ?? ''}` }}</strong><p>{{ gpuMeta(device) }}</p><span class="trc-track" aria-hidden="true"><i :style="{ width: `${percent(device.utilization_percent) ?? 0}%` }" /></span></article></section></details>
          <div v-if="models.length" class="trc-model-list"><article v-for="model in models" :key="model.cache_key || `${model.provider}:${model.model}`" class="trc-model-entry"><UiIcon :name="modelIcon(model)" /><div class="trc-model-copy"><strong>{{ modelName(model) }}</strong><p>{{ model.kind_label || model.category || 'Model' }} · {{ modelEstimate(model) }}</p><p v-if="model.warning" class="trc-warning">{{ model.warning }}</p><details><summary>Model details</summary><div class="trc-model-technical"><code>{{ model.model || 'model' }}</code><p>{{ modelMeta(model) }}</p><p v-for="(detail, index) in modelDetails(model)" :key="index">{{ detail }}</p></div></details></div><div class="trc-model-action"><button v-if="model.unloadable && !model.managed" class="trc-button" type="button" :disabled="Boolean(unloading)" :aria-label="`Unload ${modelName(model)}`" @click="unloadModel(model)">{{ unloading === text(model.cache_key || model.model) ? 'Unloading…' : 'Unload' }}</button><span v-else class="trc-tag">{{ model.remote ? 'Spud Hub' : model.managed ? 'Managed' : 'Loaded' }}</span></div></article></div><p v-else class="trc-empty">{{ loadedCount ? 'Model details are not available yet.' : 'No runtime models are loaded right now.' }}</p>
        </section>

        <section v-else class="trc-context-view">
          <header class="trc-view-heading"><h3>Chat context</h3><p v-if="context.error" class="trc-warning" role="alert">{{ context.error }}</p><p v-else-if="num(context.prompt_tokens) || num(context.minimum_context_window)">How much room your current chat needs.</p><p v-else>No estimate available yet. Send a chat message so Hydra can sample the active chat prompt stack.</p></header>
          <template v-if="!context.error && (num(context.prompt_tokens) || num(context.minimum_context_window))">
            <div class="trc-budget"><div><span>Current prompt</span><strong>{{ integer(context.prompt_tokens) }}</strong><small>tokens in use</small></div><div><span>Recommended window</span><strong>{{ integer(context.recommended_context_window) }}</strong><small>tokens for this workload</small></div></div>
            <header class="trc-section-heading"><h4>Prompt breakdown</h4><span>Estimated</span></header>
            <div class="trc-context-rows"><article v-for="item in contextRows.filter(item => item.label !== 'Capability reserve')" :key="item.label"><div><span>{{ item.label }}</span><b>{{ integer(item.tokens) }} tokens</b></div><span class="trc-track" aria-hidden="true"><i :style="{ width: `${contextRowPercent(item.tokens)}%` }" /></span></article></div>
            <details class="trc-context-details"><summary>Budget &amp; technical details</summary><dl><div><dt>Reply budget</dt><dd>{{ integer(context.completion_budget_tokens) }} tokens</dd></div><div><dt>Minimum window</dt><dd>{{ integer(context.minimum_context_window) }} tokens</dd></div><div v-if="contextRows.some(item => item.label === 'Capability reserve')"><dt>Capability reserve</dt><dd>{{ integer(contextRows.find(item => item.label === 'Capability reserve')?.tokens) }} tokens</dd></div><div><dt>Burst reserve</dt><dd>{{ integer(context.burst_context_reserve_tokens) }} tokens</dd></div></dl><p>{{ contextSummary }}</p><p>{{ num(context.enabled_verbas) }} Verba · {{ num(context.connected_portals) }} portals · {{ num(context.running_cores) }} cores</p><p v-for="note in contextNotes" :key="note">{{ note }}</p></details>
          </template>
        </section>
      </div>
      <footer class="trc-footer"><span><UiIcon name="activity" />{{ streamState === 'live' ? 'Live telemetry' : streamStatusLabel }}<span v-if="telemetryUpdatedAt || detailsUpdatedAt"> · Updated {{ telemetryUpdatedAt || detailsUpdatedAt }}</span></span><span>{{ assistantRuntimeLabel }}</span></footer>
    </section>
  </PopupTransition>
</template>
