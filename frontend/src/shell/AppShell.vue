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
import SettingsApp from "../settings/SettingsApp.vue";
import SpudexApp from "../spudex/SpudexApp.vue";
import VerbasApp from "../verbas/VerbasApp.vue";
import type { AppShellAuthState, AppShellBranding, AppShellMountOptions, AppView, ShellJson, ShellViewDescriptor } from "./types";

const props = defineProps<{ options: AppShellMountOptions }>();

const viewSpecs: Array<{ id: AppView; label: string; subtitle: string }> = [
  { id: "dashboard", label: "Dashboard", subtitle: "Tater status, live signals, and generated briefs." },
  { id: "chat", label: "Chat", subtitle: "Talk to Tater Totterson" },
  { id: "verbas", label: "Verba", subtitle: "Enable tools and manage Verba settings + shop updates." },
  { id: "portals", label: "Portals", subtitle: "Portal runtime controls and full Portal Shop manager." },
  { id: "cores", label: "Cores", subtitle: "Core runtime controls and full Core Shop manager." },
  { id: "integrations", label: "Integrations", subtitle: "Service endpoints, credentials, devices, runtime, and integration updates." },
  { id: "spudex", label: "Spudex", subtitle: "Policy-controlled terminal sessions for Tater." },
  { id: "settings", label: "Settings", subtitle: "Global WebUI and Tater runtime configuration." },
];
const allowedViews = new Set(viewSpecs.map((view) => view.id));
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
const toasts = ref<Array<{ id: number; message: string; tone: "success" | "error"; visible: boolean; closing: boolean }>>([]);

const activeSpec = computed(() => viewSpecs.find((view) => view.id === activeView.value) || viewSpecs[0]);
const activeDescriptor = computed(() => loadedViews[activeView.value] || null);
const activeViewComponent = computed(() => components[activeView.value]);
const activeState = computed<any>(() => activeDescriptor.value?.state || {});
const activeOptions = computed<any>(() => activeDescriptor.value?.options || {});
const shellClasses = computed(() => ({
  "sidebar-collapsed": sidebarCollapsed.value,
  "sidebar-collapsing": sidebarPhase.value === "collapsing",
  "sidebar-expanding": sidebarPhase.value === "expanding",
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
  row.visible = false;
  row.closing = true;
  const prior = toastTimers.get(id);
  if (prior) window.clearTimeout(prior);
  toastTimers.set(id, window.setTimeout(() => {
    toasts.value = toasts.value.filter((item) => item.id !== id);
    toastTimers.delete(id);
  }, 420));
}
function toast(message: string, tone = "success", timeoutMs = 2600) {
  const value = String(message || "").trim();
  if (!value) return;
  const id = ++toastSequence;
  toasts.value.push({ id, message: value, tone: tone === "error" ? "error" : "success", visible: false, closing: false });
  requestAnimationFrame(() => { const row = toasts.value.find((item) => item.id === id); if (row) row.visible = true; });
  toastTimers.set(id, window.setTimeout(() => dismissToast(id), Math.max(1200, Number(timeoutMs) || 2600)));
}
function sidebarDuration(direction: "collapse" | "expand") {
  const style = String(document.body.dataset.popupEffect || "flame");
  const durations: Record<string, [number, number]> = { disabled: [120, 140], flame: [460, 480], dust: [500, 520], glitch: [340, 360], portal: [500, 520], melt: [480, 500] };
  return (durations[style] || durations.flame)[direction === "collapse" ? 0 : 1];
}
function setSidebarCollapsed(collapsed: boolean) {
  const next = Boolean(collapsed);
  if (sidebarTimer) window.clearTimeout(sidebarTimer);
  const desktop = window.matchMedia?.("(min-width: 981px)").matches ?? window.innerWidth > 980;
  if (!desktop) {
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
  }, sidebarDuration(next ? "collapse" : "expand"));
}
function handlePopState() {
  const view = viewFromLocation();
  if (view) void navigate(view, { history: "none" });
}

defineExpose({ navigate, refresh, refreshTab, selectViewTab, selectSettings, select: selectSettings, updateView, update, setHealth, setStatus, openRuntime, toast, updateBranding, setSidebarCollapsed, updateAuth, requireAuth });

onMounted(() => {
  window.addEventListener("popstate", handlePopState);
  const requested = viewFromLocation() || activeView.value;
  void navigate(requested, { history: "replace" });
});
onBeforeUnmount(() => {
  window.removeEventListener("popstate", handlePopState);
  if (sidebarTimer) window.clearTimeout(sidebarTimer);
  toastTimers.forEach((timer) => window.clearTimeout(timer));
  toastTimers.clear();
});
</script>

<template>
  <div class="bg-shape bg-shape-a" aria-hidden="true"></div>
  <div class="bg-shape bg-shape-b" aria-hidden="true"></div>
  <div id="app-shell" class="app-shell tater-vue-shell" :class="shellClasses">
    <aside id="app-sidebar" class="sidebar">
      <div class="sidebar-controls"><button id="sidebar-collapse-btn" class="inline-btn sidebar-toggle-btn" type="button" :disabled="Boolean(sidebarPhase)" :aria-label="sidebarCollapsed ? 'Show menu' : 'Hide menu'" @click="setSidebarCollapsed(!sidebarCollapsed)">{{ sidebarPhase ? '⋯' : sidebarCollapsed ? '☰' : '✕' }}</button></div>
      <div class="brand-wrap"><div><h1 id="brand-name">{{ branding.firstName }}</h1><p id="brand-subtitle">{{ branding.firstName }}OS Control Surface</p></div></div>
      <nav class="nav-stack" aria-label="Primary"><button v-for="view in viewSpecs" :key="view.id" class="nav-btn" :class="{ active: activeView === view.id }" :data-view="view.id" type="button" @click="navigate(view.id)">{{ view.label }}</button></nav>
      <div v-if="versionText" id="tater-build-version" class="sidebar-build-version" :aria-label="`Current ${branding.firstName || 'Tater'} version`" aria-live="polite">{{ versionText }}</div>
    </aside>

    <main class="main-pane">
      <header class="topbar"><div><h2 id="view-title">{{ activeSpec.label }}</h2><p id="view-subtitle">{{ subtitle }}</p></div><div id="runtime-summary" class="runtime-summary runtime-summary-vue-host"><RuntimeStatus ref="runtimeComponent" :state="runtimeState" :options="options.runtimeOptions" :assistant-name="branding.firstName" /></div></header>
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
  <div id="toast-root" class="toast-root" aria-live="polite" aria-atomic="true"><button v-for="item in toasts" :key="item.id" type="button" class="toast-item" :class="[item.tone, { show: item.visible, 'flame-out': item.closing }]" @click="dismissToast(item.id)">{{ item.message }}</button></div>
  <AuthGate :state="authState" :authenticate="options.authenticate" @authenticated="handleAuthenticated" />
</template>
