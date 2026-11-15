<script setup lang="ts">
import { computed, markRaw, nextTick, onBeforeUnmount, onMounted, reactive, ref, shallowReactive } from "vue";
import ChatApp from "../chat/ChatApp.vue";
import AuthGate from "./AuthGate.vue";
import CoresApp from "../cores/CoresApp.vue";
import DashboardApp from "../dashboard/DashboardApp.vue";
import IntegrationsApp from "../integrations/IntegrationsApp.vue";
import PortalsApp from "../portals/PortalsApp.vue";
import RuntimeStatus from "../runtime/RuntimeStatus.vue";
import type { RuntimeStatusState } from "../runtime/types";
import { getJson } from "../shared/api";
import SettingsApp from "../settings/SettingsApp.vue";
import LocalModelApplyProgress from "../settings/components/models/LocalModelApplyProgress.vue";
import SpudexApp from "../spudex/SpudexApp.vue";
import VerbasApp from "../verbas/VerbasApp.vue";
import UiIcon from "../shared/UiIcon.vue";
import PopupToast from "../shared/PopupToast.vue";
import { normalizePopupEffect, prefersReducedMotion } from "../shared/popupMotion";
import type { AppShellAuthState, AppShellBranding, AppShellMountOptions, AppView, ShellJson, ShellViewDescriptor } from "./types";

const props = defineProps<{ options: AppShellMountOptions }>();

const viewSpecs: Array<{ id: AppView; label: string; subtitle: string }> = [
  { id: "dashboard", label: "Dashboard", subtitle: "Tater status, live signals, and generated briefs." },
  { id: "chat", label: "Chat", subtitle: "Talk to Tater Totterson" },
  { id: "verbas", label: "Verba", subtitle: "Enable tools and manage Verba settings + shop updates." },
  { id: "portals", label: "Portals", subtitle: "Portal runtime controls and full Portal Shop manager." },
  { id: "cores", label: "Cores", subtitle: "Core runtime controls and full Core Shop manager." },
  { id: "integrations", label: "Integrations", subtitle: "Service endpoints, credentials, devices, runtime, and integration updates." },
  { id: "spudex", label: "Terminal", subtitle: "Terminal sessions and coding work for Tater." },
  { id: "settings", label: "Settings", subtitle: "Global WebUI and Tater runtime configuration." },
];
const allowedViews = new Set(viewSpecs.map((view) => view.id));
const viewIcons: Record<AppView, string> = { dashboard: "dashboard", chat: "chat", verbas: "verbas", portals: "portals", cores: "cores", integrations: "plug", spudex: "terminal", settings: "sliders" };
const components = markRaw({
  dashboard: DashboardApp,
  chat: ChatApp,
  verbas: VerbasApp,
  portals: PortalsApp,
  cores: CoresApp,
  integrations: IntegrationsApp,
  spudex: SpudexApp,
  settings: SettingsApp,
});

const initialBranding = props.options.initialBranding || {};
const activeView = ref<AppView>(normalizeView(props.options.initialView));
const lastWorkspaceView = ref<AppView>(activeView.value === "settings" ? "dashboard" : activeView.value);
const loadedViews = shallowReactive<Partial<Record<AppView, ShellViewDescriptor>>>({});
const loadingView = ref<AppView | "">("");
const viewError = ref("");
const activeComponent = ref<any>(null);
const runtimeComponent = ref<InstanceType<typeof RuntimeStatus> | null>(null);
const branding = reactive<AppShellBranding>({
  firstName: String(initialBranding.firstName || "Tater"),
  fullName: String(initialBranding.fullName || "Tater Totterson"),
  version: String(initialBranding.version || ""),
  versionLabel: String(initialBranding.versionLabel || ""),
});
const runtimeState = reactive<RuntimeStatusState>({
  health: props.options.initialRuntimeState?.health || null,
  text: props.options.initialRuntimeState?.text || "Checking system…",
  tone: props.options.initialRuntimeState?.tone || "normal",
});
const authState = reactive<AppShellAuthState>({ ...props.options.initialAuthState });
const sidebarCollapsed = ref(Boolean(props.options.initialSidebarCollapsed));
const sidebarPhase = ref<"" | "collapsing" | "expanding">("");
let sidebarTimer = 0;
let requestSequence = 0;
let toastSequence = 0;
const toastTimers = new Map<number, number>();
const toasts = ref<Array<{ id: number; message: string; tone: "success" | "error"; closing: boolean }>>([]);
const startupModelOpen = ref(false);
const startupModelWarmup = ref<ShellJson>({});
const startupModelPollError = ref("");
let startupModelTimer = 0;
let startupModelCompleteTimer = 0;
let startupModelDismissed = false;
let startupModelObserved = false;
let startupModelRequest: AbortController | null = null;
let startupModelDisposed = false;

const activeSpec = computed(() => viewSpecs.find((view) => view.id === activeView.value) || viewSpecs[0]);
const activeDescriptor = computed(() => loadedViews[activeView.value] || null);
const activeViewComponent = computed(() => components[activeView.value]);
const activeState = computed<any>(() => activeDescriptor.value?.state || {});
const activeOptions = computed<any>(() => {
  const options = activeDescriptor.value?.options || {};
  return activeView.value === "settings" ? { ...options, onExitSettings: () => navigate(lastWorkspaceView.value) } : options;
});
const shellClasses = computed(() => ({
  "sidebar-collapsed": sidebarCollapsed.value,
  "sidebar-collapsing": sidebarPhase.value === "collapsing",
  "sidebar-expanding": sidebarPhase.value === "expanding",
  "settings-layout": activeView.value === "settings",
}));
const versionText = computed(() => {
  const assistantName = branding.firstName || "Tater";
  if (branding.versionLabel) return `${assistantName} ${branding.versionLabel}`;
  const normalized = String(branding.version || "").replace(/^v/i, "");
  return normalized ? `${assistantName} v${normalized}` : "";
});
const subtitle = computed(() => {
  if (activeView.value === "chat") return `Talk to ${branding.fullName || branding.firstName || "Tater"}`;
  if (activeView.value === "settings") return `Global WebUI and ${branding.firstName || "Tater"} runtime configuration.`;
  return activeSpec.value.subtitle;
});

function normalizeView(value: unknown): AppView {
  const key = String(value || "").trim().toLowerCase() as AppView;
  return allowedViews.has(key) ? key : "dashboard";
}
function viewFromLocation(): AppView | null {
  const match = String(window.location.hash || "").match(/^#\/?([a-z-]+)/i);
  if (match && allowedViews.has(match[1].toLowerCase() as AppView)) return match[1].toLowerCase() as AppView;
  const query = new URLSearchParams(window.location.search).get("view");
  return query && allowedViews.has(query.toLowerCase() as AppView) ? query.toLowerCase() as AppView : null;
}
function syncHistory(view: AppView, mode: "push" | "replace" | "none") {
  if (mode === "none") return;
  const url = new URL(window.location.href);
  url.hash = `/${view}`;
  const method = mode === "replace" ? "replaceState" : "pushState";
  window.history[method]({ ...(window.history.state || {}), taterView: view }, "", url);
}
function mergeDescriptor(view: AppView, descriptor: ShellViewDescriptor) {
  const existing = loadedViews[view];
  if (existing) {
    Object.keys(existing.state).forEach((key) => { if (!(key in descriptor.state)) delete existing.state[key]; });
    Object.assign(existing.state, descriptor.state || {});
    Object.assign(existing.options, descriptor.options || {});
    return;
  }
  loadedViews[view] = {
    state: reactive(descriptor.state || {}),
    options: markRaw(descriptor.options || {}),
  };
}
async function ensureView(view: AppView, refresh = false) {
  if (loadedViews[view] && !refresh) return;
  const sequence = ++requestSequence;
  loadingView.value = view;
  viewError.value = "";
  try {
    const descriptor = await props.options.loadView(view, { refresh });
    if (sequence !== requestSequence) return;
    mergeDescriptor(view, descriptor);
  } catch (error) {
    if (sequence !== requestSequence) return;
    viewError.value = error instanceof Error ? error.message : `Could not load ${view}.`;
  } finally {
    if (sequence === requestSequence) loadingView.value = "";
  }
}
async function navigate(viewInput: AppView, options: { history?: "push" | "replace" | "none"; refresh?: boolean } = {}) {
  const view = normalizeView(viewInput);
  const changed = activeView.value !== view;
  if (!changed && loadedViews[view] && !options.refresh) return;
  activeView.value = view;
  if (view !== "settings") lastWorkspaceView.value = view;
  document.body.dataset.view = view;
  props.options.onViewChange?.(view);
  syncHistory(view, changed ? options.history || "push" : options.history === "none" ? "none" : "replace");
  if (authState.required) return;
  await ensureView(view, Boolean(options.refresh));
  await nextTick();
}
async function refresh() {
  const exposed = activeComponent.value as { refresh?: () => Promise<void> } | null;
  if (typeof exposed?.refresh === "function") await exposed.refresh();
  else await ensureView(activeView.value, true);
}
async function refreshTab(key: string) {
  const exposed = activeComponent.value as { refreshTab?: (value: string) => Promise<void> } | null;
  if (typeof exposed?.refreshTab === "function") await exposed.refreshTab(key);
  else await refresh();
}
async function selectViewTab(viewInput: AppView, tab: string, childTab = "") {
  await navigate(viewInput);
  await nextTick();
  const exposed = activeComponent.value as { select?: (value: string, child?: string) => void | Promise<void> } | null;
  await exposed?.select?.(tab, childTab);
  await nextTick();
}
function selectSettings(tab: string) {
  const exposed = activeComponent.value as { select?: (value: string) => void } | null;
  exposed?.select?.(tab);
}
function updateView(viewInput: AppView, payload: ShellJson) {
  const view = normalizeView(viewInput);
  const descriptor = loadedViews[view];
  if (!descriptor) return;
  if (["dashboard", "verbas", "portals", "cores", "spudex"].includes(view)) descriptor.state.payload = payload;
  else if (view === "integrations") descriptor.state.settings = payload;
  else Object.assign(descriptor.state, payload || {});
}
function update(payload: ShellJson) { updateView(activeView.value, payload); }
function updateBranding(next: Partial<AppShellBranding>) {
  if (next.firstName !== undefined) branding.firstName = String(next.firstName || "Tater");
  if (next.fullName !== undefined) branding.fullName = String(next.fullName || branding.firstName || "Tater");
  if (next.version !== undefined) branding.version = String(next.version || "");
  if (next.versionLabel !== undefined) branding.versionLabel = String(next.versionLabel || "");
}
function updateAuth(next: Partial<AppShellAuthState>) {
  Object.assign(authState, next || {});
}
function requireAuth(next: Partial<AppShellAuthState>, message = "Session expired. Please log in again.") {
  requestSequence += 1;
  loadingView.value = "";
  Object.assign(authState, next || {}, { required: true, authenticated: false, message });
}
async function handleAuthenticated(next: AppShellAuthState) {
  Object.assign(authState, next, { required: false, authenticated: true, message: "" });
  await ensureView(activeView.value, !loadedViews[activeView.value]);
  await props.options.onAuthenticated?.(authState);
  startupModelDismissed = false;
  void pollStartupModelWarmup();
}
function setHealth(health: ShellJson, tone: RuntimeStatusState["tone"] = "normal") {
  runtimeState.health = health || {};
  runtimeState.text = "";
  runtimeState.tone = tone;
}
function setStatus(text: string, tone: RuntimeStatusState["tone"] = "normal") {
  runtimeState.health = null;
  runtimeState.text = String(text || "").trim();
  runtimeState.tone = tone;
}
async function openRuntime() { await runtimeComponent.value?.open?.(); }
function dismissToast(id: number) {
  const row = toasts.value.find((item) => item.id === id);
  if (!row || row.closing) return;
  row.closing = true;
  const prior = toastTimers.get(id);
  if (prior) window.clearTimeout(prior);
  toastTimers.delete(id);
}
function removeToast(id: number) {
  toasts.value = toasts.value.filter((item) => item.id !== id);
}
function toast(message: string, tone = "success", timeoutMs = 2600) {
  const value = String(message || "").trim();
  if (!value) return;
  const id = ++toastSequence;
  toasts.value.push({ id, message: value, tone: tone === "error" ? "error" : "success", closing: false });
  toastTimers.set(id, window.setTimeout(() => dismissToast(id), Math.max(1200, Number(timeoutMs) || 2600)));
}
function sidebarDuration() {
  return prefersReducedMotion() || normalizePopupEffect(document.body.dataset.popupEffect) === "disabled" ? 0 : 220;
}
function setSidebarCollapsed(collapsed: boolean) {
  const next = Boolean(collapsed);
  if (sidebarTimer) window.clearTimeout(sidebarTimer);
  const desktop = window.matchMedia?.("(min-width: 981px)").matches ?? window.innerWidth > 980;
  if (!desktop || sidebarDuration() === 0) {
    sidebarCollapsed.value = next;
    sidebarPhase.value = "";
    props.options.onSidebarChange?.(next);
    return;
  }
  sidebarPhase.value = next ? "collapsing" : "expanding";
  if (!next) sidebarCollapsed.value = false;
  sidebarTimer = window.setTimeout(() => {
    sidebarCollapsed.value = next;
    sidebarPhase.value = "";
    sidebarTimer = 0;
    props.options.onSidebarChange?.(next);
  }, sidebarDuration());
}
function handlePopState() {
  const view = viewFromLocation();
  if (view) void navigate(view, { history: "none" });
}

function clearStartupModelTimers() {
  if (startupModelTimer) window.clearTimeout(startupModelTimer);
  if (startupModelCompleteTimer) window.clearTimeout(startupModelCompleteTimer);
  startupModelTimer = 0;
  startupModelCompleteTimer = 0;
  startupModelRequest?.abort();
}

function scheduleStartupModelPoll(delay = 700) {
  if (startupModelDisposed || authState.required) return;
  if (startupModelTimer) window.clearTimeout(startupModelTimer);
  startupModelTimer = window.setTimeout(pollStartupModelWarmup, delay);
}

function closeStartupModelProgress() {
  startupModelDismissed = true;
  startupModelOpen.value = false;
}

async function pollStartupModelWarmup() {
  startupModelTimer = 0;
  const endpoint = String(props.options.modelWarmupEndpoint || "").trim();
  if (!endpoint || authState.required || startupModelDisposed || startupModelRequest) return;
  const request = new AbortController();
  startupModelRequest = request;
  const timeout = window.setTimeout(() => request.abort(), 4000);
  try {
    const snapshot = await getJson<ShellJson>(endpoint, { signal: request.signal });
    if (startupModelDisposed || authState.required) return;
    startupModelWarmup.value = snapshot;
    startupModelPollError.value = "";
    const bootstrapPending = Boolean(snapshot.startup_bootstrap_pending);
    const startupWarmup = String(snapshot.reason || "").trim().toLowerCase() === "startup";
    if (bootstrapPending || (startupWarmup && Boolean(snapshot.running))) {
      startupModelObserved = true;
      if (!startupModelDismissed) startupModelOpen.value = true;
      scheduleStartupModelPoll(snapshot.running ? 650 : 900);
      return;
    }
    if (!startupModelObserved) return;
    const errors = Array.isArray(snapshot.errors) ? snapshot.errors : [];
    const bootstrapError = String(snapshot.startup_bootstrap_error || "").trim();
    if (bootstrapError && !errors.length) startupModelWarmup.value = { ...snapshot, errors: [bootstrapError] };
    if (errors.length || bootstrapError) {
      if (!startupModelDismissed) startupModelOpen.value = true;
      return;
    }
    if (startupModelOpen.value && !startupModelCompleteTimer) {
      startupModelCompleteTimer = window.setTimeout(() => {
        startupModelOpen.value = false;
        startupModelCompleteTimer = 0;
      }, 1400);
    }
  } catch (error) {
    if (startupModelDisposed || authState.required) return;
    startupModelPollError.value = error instanceof Error ? error.message : "Live startup model progress is temporarily unavailable.";
    scheduleStartupModelPoll(1600);
  } finally {
    window.clearTimeout(timeout);
    startupModelRequest = null;
  }
}

defineExpose({ navigate, refresh, refreshTab, selectViewTab, selectSettings, select: selectSettings, updateView, update, setHealth, setStatus, openRuntime, toast, updateBranding, setSidebarCollapsed, updateAuth, requireAuth });

onMounted(() => {
  window.addEventListener("popstate", handlePopState);
  const requested = viewFromLocation() || activeView.value;
  void navigate(requested, { history: "replace" });
  void pollStartupModelWarmup();
});
onBeforeUnmount(() => {
  startupModelDisposed = true;
  window.removeEventListener("popstate", handlePopState);
  if (sidebarTimer) window.clearTimeout(sidebarTimer);
  clearStartupModelTimers();
  toastTimers.forEach((timer) => window.clearTimeout(timer));
  toastTimers.clear();
});
</script>

<template>
  <div class="bg-shape bg-shape-a" aria-hidden="true"></div>
  <div class="bg-shape bg-shape-b" aria-hidden="true"></div>
  <div id="app-shell" class="app-shell tater-vue-shell" :class="shellClasses">
    <header class="topbar workspace-topbar">
      <div class="workspace-brand"><h1 id="brand-name">{{ branding.firstName }}</h1><span aria-hidden="true">/</span><span class="workspace-current-view">{{ activeSpec.label }}</span></div>
      <div id="runtime-summary" class="runtime-summary runtime-summary-vue-host"><RuntimeStatus ref="runtimeComponent" :state="runtimeState" :options="options.runtimeOptions" :assistant-name="branding.firstName" compact /></div>
    </header>
    <aside id="app-sidebar" class="sidebar">
      <div class="sidebar-controls"><span id="brand-subtitle" class="workspace-nav-label">Workspace</span><button id="sidebar-collapse-btn" class="inline-btn sidebar-toggle-btn" type="button" :disabled="Boolean(sidebarPhase)" :aria-label="sidebarCollapsed ? 'Show menu' : 'Hide menu'" @click="setSidebarCollapsed(!sidebarCollapsed)"><UiIcon :name="sidebarCollapsed ? 'plus' : 'close'" /></button></div>
      <nav class="nav-stack" aria-label="Primary"><button v-for="view in viewSpecs" :key="view.id" class="nav-btn" :class="{ active: activeView === view.id }" :aria-current="activeView === view.id ? 'page' : undefined" :data-view="view.id" type="button" @click="navigate(view.id)"><UiIcon :name="viewIcons[view.id]" /><span>{{ view.label }}</span></button></nav>
      <div id="terminal-task-sidebar" v-show="activeView === 'spudex'" />
      <button type="button" class="workspace-services" :class="runtimeState.tone" @click="openRuntime"><UiIcon name="check" /><span>{{ runtimeState.tone === 'offline' ? 'Backend offline' : runtimeState.tone === 'degraded' ? 'Services need attention' : runtimeState.health ? 'Services connected' : 'Checking services…' }}</span></button>
      <div v-if="versionText" id="tater-build-version" class="sidebar-build-version" :aria-label="`Current ${branding.firstName || 'Tater'} version`" aria-live="polite">{{ versionText }}</div>
    </aside>

    <main class="main-pane">
      <header v-if="activeView === 'chat'" class="workspace-page-heading"><h2 id="view-title">{{ activeSpec.label }}</h2><p id="view-subtitle">{{ subtitle }}</p></header>
      <section id="view-root" class="view-root tater-shell-view-root" :data-view="activeView">
        <div v-if="viewError && !activeDescriptor" class="tv-notice error">Failed to load {{ activeSpec.label }}: {{ viewError }} <button class="inline-btn" type="button" @click="ensureView(activeView, true)">Try again</button></div>
        <div v-else-if="!activeDescriptor" class="tv-empty">Loading {{ activeSpec.label }}…</div>
        <KeepAlive :max="viewSpecs.length">
          <component v-if="activeDescriptor" :is="activeViewComponent" ref="activeComponent" :key="activeView" :state="activeState" :options="activeOptions" />
        </KeepAlive>
        <div v-if="loadingView && activeDescriptor" class="tater-shell-refresh-indicator" role="status">Refreshing {{ activeSpec.label }}…</div>
      </section>
    </main>
  </div>
  <button id="sidebar-expand-btn" class="sidebar-expand-fab" type="button" aria-label="Show menu" title="Show menu" :disabled="Boolean(sidebarPhase)" @click="setSidebarCollapsed(false)">{{ sidebarPhase ? '⋯' : '☰' }}</button>
  <div id="toast-root" class="toast-root" aria-live="polite" aria-atomic="false"><PopupToast v-for="item in toasts" :key="item.id" :open="!item.closing" :message="item.message" :tone="item.tone" @close="dismissToast(item.id)" @dismissed="removeToast(item.id)" /></div>
  <LocalModelApplyProgress v-if="!authState.required" :open="startupModelOpen" :snapshot="startupModelWarmup" :poll-error="startupModelPollError" startup @close="closeStartupModelProgress" @reopen="startupModelOpen = true" />
  <AuthGate :state="authState" :authenticate="options.authenticate" @authenticated="handleAuthenticated" />
</template>
