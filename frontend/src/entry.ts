import { createApp, reactive } from "vue";
import ChatApp from "./chat/ChatApp.vue";
import type {
  ChatController,
  ChatMessage,
  ChatMountOptions,
  ChatProfile,
  ChatStatsPayload,
} from "./chat/types";
import CoresApp from "./cores/CoresApp.vue";
import type { CoresController, CoresMountOptions, CoresPayload } from "./cores/types";
import DashboardApp from "./dashboard/DashboardApp.vue";
import type { DashboardController, DashboardMountOptions, DashboardPayload } from "./dashboard/types";
import IntegrationsApp from "./integrations/IntegrationsApp.vue";
import type {
  IntegrationSettingsPayload,
  IntegrationsController,
  IntegrationsMountOptions,
} from "./integrations/types";
import MusicCoreApp from "./music/MusicCoreApp.vue";
import type { CoreTabPayload, MusicCoreController, MusicCoreMountOptions } from "./music/types";
import PortalsApp from "./portals/PortalsApp.vue";
import type { PortalPayload, PortalsController, PortalsMountOptions } from "./portals/types";
import SpudexApp from "./spudex/SpudexApp.vue";
import type { SpudexController, SpudexMountOptions, SpudexPayload } from "./spudex/types";
import SettingsApp from "./settings/SettingsApp.vue";
import type { SettingsController, SettingsMountOptions, SettingsSummary } from "./settings/types";
import RuntimeStatus from "./runtime/RuntimeStatus.vue";
import type { RuntimeStatusController, RuntimeStatusMountOptions, RuntimeStatusState } from "./runtime/types";
import VerbasApp from "./verbas/VerbasApp.vue";
import type { VerbaPayload, VerbasController, VerbasMountOptions } from "./verbas/types";
import "./music/music-core.css";
import "./tater-ui.css";

export function mountChat(
  container: HTMLElement,
  options: ChatMountOptions,
): ChatController {
  const state = reactive<{
    profile: ChatProfile;
    messages: ChatMessage[];
    stats: ChatStatsPayload;
  }>({
    profile: options.initialProfile || {},
    messages: options.initialMessages || [],
    stats: options.initialStats || { enabled: false, stats: null },
  });
  const app = createApp(ChatApp, { state, options });
  app.mount(container);
  return {
    update(payload) {
      if (payload.profile) state.profile = payload.profile;
      if (payload.messages) state.messages = payload.messages;
      if (payload.stats) state.stats = payload.stats;
    },
    unmount() {
      app.unmount();
    },
  };
}

export function mountDashboard(
  container: HTMLElement,
  options: DashboardMountOptions,
): DashboardController {
  const state = reactive<{ payload: DashboardPayload }>({ payload: options.initialPayload });
  const app = createApp(DashboardApp, { state, options });
  app.mount(container);
  return {
    update(payload: DashboardPayload) {
      state.payload = payload;
    },
    unmount() {
      app.unmount();
    },
  };
}

export function mountIntegrations(
  container: HTMLElement,
  options: IntegrationsMountOptions,
): IntegrationsController {
  const state = reactive<{ settings: IntegrationSettingsPayload }>({ settings: options.initialSettings });
  const app = createApp(IntegrationsApp, { state, options });
  app.mount(container);
  return {
    update(payload: IntegrationSettingsPayload) {
      state.settings = payload;
    },
    unmount() {
      app.unmount();
    },
  };
}

export function mountVerbas(
  container: HTMLElement,
  options: VerbasMountOptions,
): VerbasController {
  const state = reactive<{ payload: VerbaPayload }>({ payload: options.initialPayload });
  const app = createApp(VerbasApp, { state, options });
  app.mount(container);
  return {
    update(payload: VerbaPayload) {
      state.payload = payload;
    },
    unmount() {
      app.unmount();
    },
  };
}

export function mountPortals(
  container: HTMLElement,
  options: PortalsMountOptions,
): PortalsController {
  const state = reactive<{ payload: PortalPayload }>({ payload: options.initialPayload });
  const app = createApp(PortalsApp, { state, options });
  app.mount(container);
  return {
    update(payload: PortalPayload) {
      state.payload = payload;
    },
    unmount() {
      app.unmount();
    },
  };
}

export function mountCores(
  container: HTMLElement,
  options: CoresMountOptions,
): CoresController {
  const state = reactive<{ payload: CoresPayload }>({ payload: options.initialPayload });
  const app = createApp(CoresApp, { state, options });
  const instance = app.mount(container) as unknown as {
    refresh?: () => Promise<void>;
    refreshTab?: (key: string) => Promise<void>;
  };
  return {
    update(payload: CoresPayload) {
      state.payload = payload;
    },
    refresh() {
      return instance.refresh?.() || Promise.resolve();
    },
    refreshTab(key: string) {
      return instance.refreshTab?.(key) || Promise.resolve();
    },
    unmount() {
      app.unmount();
    },
  };
}

export function mountSpudex(
  container: HTMLElement,
  options: SpudexMountOptions,
): SpudexController {
  const state = reactive<{ payload: SpudexPayload }>({ payload: options.initialPayload });
  const app = createApp(SpudexApp, { state, options });
  const instance = app.mount(container) as unknown as { refresh?: () => Promise<void> };
  return {
    update(payload: SpudexPayload) {
      state.payload = payload;
    },
    refresh() {
      return instance.refresh?.() || Promise.resolve();
    },
    unmount() {
      app.unmount();
    },
  };
}

export function mountSettings(
  container: HTMLElement,
  options: SettingsMountOptions,
): SettingsController {
  const state = reactive<{ summary: SettingsSummary }>({ summary: options.initialSummary || {} });
  const app = createApp(SettingsApp, { state, options });
  const instance = app.mount(container) as unknown as { select?: (tab: string) => void };
  return {
    update(summary: SettingsSummary) {
      state.summary = summary;
    },
    select(tab: string) {
      instance.select?.(tab);
    },
    unmount() {
      app.unmount();
    },
  };
}

export function mountRuntimeStatus(
  container: HTMLElement,
  options: RuntimeStatusMountOptions,
): RuntimeStatusController {
  const state = reactive<RuntimeStatusState>({
    health: options.initialState?.health || null,
    text: options.initialState?.text || "Checking system…",
    tone: options.initialState?.tone || "normal",
  });
  const app = createApp(RuntimeStatus, { state, options });
  const instance = app.mount(container) as unknown as { open?: () => Promise<void> };
  return {
    setHealth(health, tone = "normal") {
      state.health = health || {};
      state.text = "";
      state.tone = tone;
    },
    setStatus(text, tone = "normal") {
      state.health = null;
      state.text = String(text || "").trim();
      state.tone = tone;
    },
    open() {
      return instance.open?.() || Promise.resolve();
    },
    unmount() {
      app.unmount();
    },
  };
}

export function mountMusicCore(
  container: HTMLElement,
  options: MusicCoreMountOptions,
): MusicCoreController {
  const state = reactive<{ payload: CoreTabPayload }>({ payload: options.initialPayload });
  const app = createApp(MusicCoreApp, { state, options });
  app.mount(container);

  return {
    update(payload: CoreTabPayload) {
      state.payload = payload;
    },
    unmount() {
      app.unmount();
    },
  };
}

export type {
  ChatController,
  ChatMessage,
  ChatMountOptions,
  ChatProfile,
  ChatStatsPayload,
  CoresController,
  CoresMountOptions,
  CoresPayload,
  CoreTabPayload,
  DashboardController,
  DashboardMountOptions,
  DashboardPayload,
  IntegrationSettingsPayload,
  IntegrationsController,
  IntegrationsMountOptions,
  MusicCoreController,
  MusicCoreMountOptions,
  PortalPayload,
  PortalsController,
  PortalsMountOptions,
  SpudexController,
  SpudexMountOptions,
  SpudexPayload,
  SettingsController,
  SettingsMountOptions,
  SettingsSummary,
  RuntimeStatusController,
  RuntimeStatusMountOptions,
  RuntimeStatusState,
  VerbaPayload,
  VerbasController,
  VerbasMountOptions,
};
