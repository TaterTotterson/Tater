<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, ref } from "vue";
import { getJson, postJson } from "../shared/api";
import PopupTransition from "../shared/PopupTransition.vue";
import type { JsonRow, RuntimeStatusMountOptions, RuntimeStatusState } from "./types";

const props = defineProps<{ state: RuntimeStatusState; options: RuntimeStatusMountOptions }>();
const open = ref(false);
const loading = ref(false);
const error = ref("");
const notice = ref("");
const updatedAt = ref("");
const breakdown = ref<JsonRow>({});
const closeButton = ref<HTMLButtonElement | null>(null);
const unloading = ref("");
let pollTimer = 0;

const health = computed(() => props.state.health || {});
const healthMemory = computed(() => row(health.value.loaded_models || health.value.loadedModels));
const healthSystem = computed(() => row(healthMemory.value.system));
const healthCpu = computed(() => row(healthSystem.value.cpu));
const healthRam = computed(() => row(healthSystem.value.ram));
const healthVram = computed(() => row(healthSystem.value.vram));
const loadedCount = computed(() => num(healthMemory.value.loaded_count));
const baseText = computed(() => props.state.text || `${num(health.value.verbas_enabled)} verba enabled • ${num(health.value.portals_running)} portals running • ${num(health.value.cores_running)} cores running • ${num(health.value.hydra_jobs_active ?? health.value.chat_jobs_active)} hydra jobs • ${num(health.value.llm_calls_active)} llm calls • ${num(health.value.vision_calls_active ?? health.value.voice_calls_active)} vision calls`);
const loadedText = computed(() => {
  const estimated = num(row(healthMemory.value.totals).estimated_total_bytes);
  return `${loadedCount.value} model${loadedCount.value === 1 ? "" : "s"} loaded${estimated > 0 ? ` • est ${bytes(estimated)}` : ""}`;
});
const pillResources = computed(() => {
  const ramTotal = num(healthRam.value.total_bytes); const ramUsed = num(healthRam.value.used_bytes);
  const vramTotal = num(healthVram.value.total_bytes); const vramUsed = num(healthVram.value.used_bytes);
  const gpuPercent = percent(healthVram.value.utilization_percent);
  const unified = Boolean(healthSystem.value.unified_memory || healthVram.value.unified);
  const resources = [
    resource("CPU", healthCpu.value.percent, healthCpu.value.available === false),
    resource("GPU", gpuPercent, gpuPercent === null),
    resource(unified ? "Unified" : "RAM", ramTotal > 0 ? healthRam.value.percent ?? (ramUsed / ramTotal) * 100 : null, ramTotal <= 0),
  ];
  if (!unified) resources.push(resource("VRAM", vramTotal > 0 ? healthVram.value.percent ?? (vramUsed / vramTotal) * 100 : null, vramTotal <= 0));
  return resources;
});

const hydra = computed(() => row(breakdown.value.hydra_jobs || breakdown.value.chat_jobs));
const llm = computed(() => row(breakdown.value.llm_calls));
const vision = computed(() => row(breakdown.value.vision_calls || breakdown.value.voice_calls));
const context = computed(() => row(breakdown.value.chat_context_window));
const modelsPayload = computed(() => row(breakdown.value.loaded_models));
const modelSystem = computed(() => row(modelsPayload.value.system));
const modelCpu = computed(() => row(modelSystem.value.cpu));
const modelRam = computed(() => row(modelSystem.value.ram));
const modelVram = computed(() => row(modelSystem.value.vram));
const models = computed<JsonRow[]>(() => rows(modelsPayload.value.models));
const gpuDevices = computed<JsonRow[]>(() => rows(modelVram.value.devices));
const hydraTurns = computed<JsonRow[]>(() => rows(hydra.value.active_turns));
const llmCalls = computed<JsonRow[]>(() => rows(llm.value.active_calls));
const visionCalls = computed<JsonRow[]>(() => rows(vision.value.active_calls));
const modelSummary = computed(() => {
  const totals = row(modelsPayload.value.totals);
  return [
    `${num(modelsPayload.value.loaded_count ?? models.value.length)} loaded`,
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
  `Prompt ${integer(context.value.prompt_tokens)} tok`,
  `Reply budget ${integer(context.value.completion_budget_tokens)} tok`,
  num(context.value.capability_context_reserve_tokens) ? `Capability reserve ${integer(context.value.capability_context_reserve_tokens)} tok` : "",
  num(context.value.burst_context_reserve_tokens) ? `Burst reserve ${integer(context.value.burst_context_reserve_tokens)} tok` : "",
  `Min window ${integer(context.value.minimum_context_window)}`,
  `Recommended ${integer(context.value.recommended_context_window)}`,
].filter(Boolean).join(" • "));
const contextNotes = computed(() => {
  const values = row(context.value.breakdown);
  const examples = strings(values.high_context_verba_examples).slice(0, 4);
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
function percent(value: unknown): number | null { if (value === null || value === undefined || text(value) === "") return null; const parsed = Number(value); return Number.isFinite(parsed) && parsed >= 0 ? Math.max(0, Math.min(100, parsed)) : null; }
function percentText(value: unknown): string { const parsed = percent(value); return parsed === null ? "n/a" : `${Math.round(parsed)}%`; }
function bytes(value: unknown): string {
  let amount = num(value); if (!amount) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"]; let index = 0;
  while (amount >= 1024 && index < units.length - 1) { amount /= 1024; index += 1; }
  return `${amount >= 10 || index === 0 ? amount.toFixed(0) : amount.toFixed(1)} ${units[index]}`;
}
function resource(label: string, rawPercent: unknown, unavailable: boolean) {
  const value = percent(rawPercent); return { label, percent: value ?? 0, value: value === null ? "n/a" : `${Math.round(value)}%`, unavailable };
}
function age(value: unknown): string { const seconds = Math.round(num(value)); if (seconds < 60) return `${seconds}s`; const minutes = Math.floor(seconds / 60); return minutes < 60 ? `${minutes}m ${seconds % 60}s` : `${Math.floor(minutes / 60)}h ${minutes % 60}m`; }
function loadedAt(value: unknown): string { const stamp = num(value); return stamp ? `Loaded ${new Date(stamp * 1000).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })}` : ""; }
function modelMeta(model: JsonRow): string {
  const estimate = model.remote
    ? ""
    : num(model.estimated_bytes)
      ? `${text(model.memory_kind || "ram").toUpperCase()} est ${bytes(model.estimated_bytes)}`
      : "Estimate unavailable";
  return [text(model.kind_label || model.category), text(model.provider_label || model.provider || "Local"), text(model.device) ? `Device ${text(model.device)}` : "", estimate, loadedAt(model.loaded_ts)].filter(Boolean).join(" • ");
}
function modelDetails(model: JsonRow): string[] {
  return [...strings(model.details), model.managed ? text(model.managed_by || "Managed by settings") : ""].filter(Boolean);
}
function gpuMeta(device: JsonRow): string {
  const power = Number(device.power_draw_w); const powerLimit = Number(device.power_limit_w);
  return [
    percent(device.utilization_percent) === null ? "GPU load n/a" : `GPU ${percentText(device.utilization_percent)}`,
    num(device.total_bytes) ? `${device.unified ? "GPU memory" : "VRAM"} ${bytes(device.used_bytes)} / ${bytes(device.total_bytes)}` : "",
    num(device.shared_memory_total_bytes) ? `Shared RAM ${bytes(device.shared_memory_used_bytes)} / ${bytes(device.shared_memory_total_bytes)}` : "",
    Number.isFinite(Number(device.temperature_c)) ? `${Number(device.temperature_c).toFixed(0)} C` : "",
    Number.isFinite(power) ? `${power.toFixed(0)} W${Number.isFinite(powerLimit) && powerLimit > 0 ? ` / ${powerLimit.toFixed(0)} W` : ""}` : "",
    text(device.detail),
  ].filter(Boolean).join(" • ");
}
function callMeta(call: JsonRow, kind: "llm" | "vision"): string {
  return [`Model ${text(call.model || "model")}`, kind === "llm" ? text(call.host) : text(call.api_base), text(call.activity) ? `Activity ${text(call.activity)}` : text(call.function) ? `Fn ${text(call.function)}` : "", num(call.message_count) ? `${num(call.message_count)} msgs` : ""].filter(Boolean).join(" • ");
}
function metric(label: string, used: unknown, total: unknown, rawPercent?: unknown, detail = "") {
  const totalValue = num(total); const usedValue = num(used); const explicit = percent(rawPercent); const value = explicit ?? (totalValue > 0 ? Math.max(0, Math.min(100, (usedValue / totalValue) * 100)) : null);
  return { label, percent: value ?? 0, value: rawPercent !== undefined ? percentText(rawPercent) : totalValue > 0 ? `${bytes(usedValue)} / ${bytes(totalValue)}` : "Unavailable", unavailable: value === null, detail };
}
const meters = computed(() => {
  const unified = Boolean(modelSystem.value.unified_memory || modelVram.value.unified);
  const values = [
    metric("CPU Usage", 0, 0, modelCpu.value.percent, [num(modelCpu.value.logical_count) ? `${num(modelCpu.value.logical_count)} logical cores` : "", num(modelCpu.value.physical_count) ? `${num(modelCpu.value.physical_count)} physical cores` : "", numbers(modelCpu.value.load_average).length ? `load ${numbers(modelCpu.value.load_average).map((item) => item.toFixed(2)).join(" / ")}` : ""].filter(Boolean).join(" • ")),
    metric("GPU Usage", 0, 0, modelVram.value.utilization_percent, [text(modelVram.value.backend) ? `Backend ${text(modelVram.value.backend)}` : "", gpuDevices.value.length ? `${gpuDevices.value.length} device${gpuDevices.value.length === 1 ? "" : "s"}` : "", unified ? "shared/unified memory" : "", percent(modelVram.value.utilization_percent) === null ? "GPU load unavailable from this runtime" : ""].filter(Boolean).join(" • ")),
    metric(unified ? "Unified Memory" : "System RAM", modelRam.value.used_bytes, modelRam.value.total_bytes),
  ];
  if (!unified) values.push(metric("System VRAM", modelVram.value.used_bytes, modelVram.value.total_bytes));
  return values;
});

async function refresh(silent = false) {
  if (loading.value) return;
  loading.value = true; error.value = ""; if (!silent && !Object.keys(breakdown.value).length) notice.value = "Loading runtime state…";
  try {
    const baseEndpoint = props.options.endpoints.breakdown;
    const endpoint = silent ? baseEndpoint : `${baseEndpoint}${baseEndpoint.includes("?") ? "&" : "?"}refresh=true`;
    const payload = await getJson<JsonRow>(endpoint);
    breakdown.value = payload || {}; updatedAt.value = new Date().toLocaleTimeString(); notice.value = "";
    props.options.onBreakdownChange?.(payload || {});
  } catch (requestError) { error.value = requestError instanceof Error ? requestError.message : "Runtime breakdown failed."; }
  finally { loading.value = false; }
}
function startPolling() { stopPolling(); pollTimer = window.setInterval(() => { if (open.value) void refresh(true); }, 5000); }
function stopPolling() { if (pollTimer) window.clearInterval(pollTimer); pollTimer = 0; }
async function openPopup() { open.value = true; await nextTick(); closeButton.value?.focus(); await refresh(false); startPolling(); }
function closePopup() { open.value = false; stopPolling(); }
async function unloadModel(model: JsonRow) {
  const key = text(model.cache_key || model.model); if (!key || unloading.value) return; unloading.value = key;
  try {
    const result = await postJson<JsonRow>(props.options.endpoints.unloadModel, { provider: text(model.provider), model: text(model.model), cache_key: text(model.cache_key) });
    const count = num(result.unloaded_count); const message = count ? `Unloaded ${count} local model${count === 1 ? "" : "s"}.` : "No loaded model matched.";
    props.options.onToast?.(message, "success"); await refresh(true); props.options.onHealthRefresh?.();
  } catch (requestError) { const message = requestError instanceof Error ? requestError.message : "Model unload failed."; error.value = `Unload failed: ${message}`; props.options.onToast?.(error.value, "error"); }
  finally { unloading.value = ""; }
}
function handleKey(event: KeyboardEvent) { if (event.key === "Escape" && open.value) closePopup(); }
window.addEventListener("keydown", handleKey);
onBeforeUnmount(() => { stopPolling(); window.removeEventListener("keydown", handleKey); });
defineExpose({ open: openPopup });
</script>

<template>
  <button class="tr-pill" :class="state.tone" type="button" title="Open loaded models, CPU/GPU usage, memory, Hydra jobs, LLM calls, and vision calls" @click="openPopup">
    <span class="tr-pill-main"><i />{{ baseText }}</span>
    <span v-if="state.health" class="tr-pill-metrics"><span class="tr-pill-models">{{ loadedText }}</span><span class="tr-pill-resources"><span v-for="item in pillResources" :key="item.label" class="tr-resource" :class="{ unavailable: item.unavailable }"><b>{{ item.label }}</b><span><i :style="{ width: `${item.percent}%` }" /></span><em>{{ item.value }}</em></span></span></span>
  </button>

  <PopupTransition :open="open" backdrop-class="tv-modal-backdrop tr-backdrop" @close="closePopup">
      <section class="tv-modal tr-modal" role="dialog" aria-modal="true" aria-label="Runtime statistics">
        <header class="tr-modal-head"><span class="tr-modal-badge">RT</span><div><span class="tv-eyebrow">Runtime stats</span><h2>Live Activity</h2><p>Loaded models, compute and memory usage, Hydra turns, model calls, vision work, and context budget.</p></div><div class="tr-modal-actions"><span>{{ updatedAt ? `Updated ${updatedAt}` : "Live data" }}</span><button class="tv-button" type="button" :disabled="loading" @click="refresh(false)">{{ loading ? "Refreshing…" : "Refresh" }}</button><button ref="closeButton" class="tv-button" type="button" @click="closePopup">Close</button></div></header>
        <div v-if="error || notice" class="tv-notice" :class="{ error: Boolean(error) }">{{ error || notice }}</div>
        <div v-if="!Object.keys(breakdown).length && loading" class="tv-empty">Loading runtime state…</div>
        <div v-else class="tr-grid">
          <article class="tv-panel tr-card wide models"><header><div><span class="tv-eyebrow">Compute and memory</span><h2>Loaded Runtime Models</h2><p>{{ modelSummary }}</p></div></header><div class="tr-meter-grid"><div v-for="meter in meters" :key="meter.label" class="tr-meter" :class="{ unavailable: meter.unavailable }"><div><strong>{{ meter.label }}</strong><span>{{ meter.value }}</span></div><span class="tr-meter-track"><i :style="{ width: `${meter.percent}%` }" /></span><small v-if="meter.detail">{{ meter.detail }}</small></div></div><section v-if="gpuDevices.length" class="tr-block"><h3>GPU Devices</h3><div class="tr-list"><article v-for="(device, index) in gpuDevices" :key="device.index ?? index"><div><strong>{{ device.name || `GPU ${device.index ?? ''}` }}</strong><small>{{ gpuMeta(device) }}</small></div><span class="tv-state good">{{ percentText(device.utilization_percent) }}</span></article></div></section><section class="tr-block"><h3>Loaded Model Entries</h3><div v-if="models.length" class="tr-list"><article v-for="model in models" :key="model.cache_key || `${model.provider}:${model.model}`"><div><strong>{{ model.model || 'model' }}</strong><small>{{ modelMeta(model) }}</small><small v-if="modelDetails(model).length">{{ modelDetails(model).join(' • ') }}</small><small v-if="model.warning" class="danger">{{ model.warning }}</small></div><button v-if="model.unloadable && !model.managed" class="tv-button danger" type="button" :disabled="Boolean(unloading)" @click="unloadModel(model)">{{ unloading === text(model.cache_key || model.model) ? 'Unloading…' : 'Unload' }}</button><span v-else class="tv-state good">{{ model.remote ? 'Spud Hub' : model.managed ? 'Managed' : 'Loaded' }}</span></article></div><div v-else class="tv-empty compact">No runtime models are loaded right now.</div></section></article>

          <article class="tv-panel tr-card wide hydra"><header><div><span class="tv-eyebrow">Orchestration</span><h2>Hydra Jobs</h2><p>{{ num(hydra.total) }} total • Active turns {{ hydraTurns.length }} • WebUI queue {{ num(hydra.webui_jobs) }} • Surface turns {{ num(hydra.surface_running_turns) }}</p></div></header><section class="tr-block"><h3>Active Turns</h3><div v-if="hydraTurns.length" class="tr-turns"><article v-for="turn in hydraTurns" :key="turn.id"><header><strong>{{ turn.task_name || 'Hydra task' }}</strong><span class="tv-state good">Running {{ age(turn.age_seconds) }}</span></header><div><span>{{ turn.platform_label || turn.platform || 'Unknown' }}</span><span v-if="turn.source">{{ turn.source }}</span><span v-if="turn.id">Drop {{ text(turn.id).slice(0, 8) }}</span></div><small v-if="turn.current_tool">Current verba/tool: {{ turn.current_tool }}</small><small v-if="turn.scope">Scope: {{ turn.scope }}</small></article></div><div v-else class="tv-empty compact">No active Hydra turns right now.</div></section></article>

          <article class="tv-panel tr-card calls"><header><div><span class="tv-eyebrow">Language models</span><h2>LLM Calls</h2><p>{{ num(llm.active_total) }} active • Started {{ num(llm.totals?.started) }} • Completed {{ num(llm.totals?.completed) }} • Failed {{ num(llm.totals?.failed) }}</p></div></header><section class="tr-block"><h3>Active Calls</h3><div v-if="llmCalls.length" class="tr-list"><article v-for="(call, index) in llmCalls" :key="call.id || index"><div><strong>{{ call.source_label || call.label || 'Unknown source' }}</strong><small>{{ callMeta(call, 'llm') }}</small></div><span class="tv-state good">{{ age(call.age_seconds) }}</span></article></div><div v-else class="tv-empty compact">No active LLM calls right now.</div></section></article>

          <article class="tv-panel tr-card vision"><header><div><span class="tv-eyebrow">Vision</span><h2>Vision Calls</h2><p>{{ num(vision.active_total) }} active • Started {{ num(vision.totals?.started) }} • Completed {{ num(vision.totals?.completed) }} • Failed {{ num(vision.totals?.failed) }}</p></div></header><section class="tr-block"><h3>Active Calls</h3><div v-if="visionCalls.length" class="tr-list"><article v-for="(call, index) in visionCalls" :key="call.id || index"><div><strong>{{ call.source_label || call.label || 'Unknown source' }}</strong><small>{{ callMeta(call, 'vision') }}</small></div><span class="tv-state good">{{ age(call.age_seconds) }}</span></article></div><div v-else class="tv-empty compact">No active vision calls right now.</div></section></article>

          <article class="tv-panel tr-card wide context"><header><div><span class="tv-eyebrow">Prompt budget</span><h2>Estimated Chat Context Window</h2><p v-if="context.error">{{ context.error }}</p><p v-else-if="num(context.prompt_tokens) || num(context.minimum_context_window)">{{ contextSummary }}</p><p v-else>No estimate available yet. Send a chat message so Hydra can sample the active chat prompt stack.</p></div></header><section v-if="contextRows.length && !context.error" class="tr-block"><h3>Prompt Composition</h3><div class="tr-list dense"><article v-for="item in contextRows" :key="item.label"><strong>{{ item.label }}</strong><span class="tv-state">{{ integer(item.tokens) }}</span></article></div><small>Active stack: {{ num(context.enabled_verbas) }} verbas enabled • {{ num(context.connected_portals) }} portals connected • {{ num(context.running_cores) }} cores running</small><small v-for="note in contextNotes" :key="note">{{ note }}</small></section></article>
        </div>
      </section>
  </PopupTransition>
</template>
