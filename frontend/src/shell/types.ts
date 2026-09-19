import type { RuntimeStatusMountOptions, RuntimeStatusState } from "../runtime/types";

export type AppView = "dashboard" | "chat" | "verbas" | "portals" | "cores" | "integrations" | "spudex" | "settings";

export type ShellJson = Record<string, any>;

export interface ShellViewDescriptor {
  state: ShellJson;
  options: ShellJson;
}

export interface AppShellBranding {
  firstName: string;
  fullName?: string;
  version?: string;
  versionLabel?: string;
}

export interface AppShellAuthState {
  required: boolean;
  passwordSet: boolean;
  authenticated: boolean;
  mode: string;
  username: string;
  userAvatar: string;
  message?: string;
}

export interface AppShellAuthRequest {
  password: string;
  confirmPassword?: string;
  setup?: boolean;
}

export interface AppShellMountOptions {
  initialView?: AppView;
  initialSidebarCollapsed?: boolean;
  initialBranding?: Partial<AppShellBranding>;
  initialRuntimeState?: Partial<RuntimeStatusState>;
  initialAuthState: AppShellAuthState;
  authenticate: (request: AppShellAuthRequest) => Promise<AppShellAuthState>;
  runtimeOptions: RuntimeStatusMountOptions;
  loadView: (view: AppView, options?: { refresh?: boolean }) => Promise<ShellViewDescriptor>;
  onViewChange?: (view: AppView) => void;
  onSidebarChange?: (collapsed: boolean) => void;
  onAuthenticated?: (state: AppShellAuthState) => void | Promise<void>;
}

export interface AppShellController {
  navigate: (view: AppView, options?: { history?: "push" | "replace" | "none"; refresh?: boolean }) => Promise<void>;
  refresh: () => Promise<void>;
  refreshTab: (key: string) => Promise<void>;
  selectViewTab: (view: AppView, tab: string, childTab?: string) => Promise<void>;
  selectSettings: (tab: string) => void;
  select: (tab: string) => void;
  updateView: (view: AppView, payload: ShellJson) => void;
  update: (payload: ShellJson) => void;
  setHealth: (health: ShellJson, tone?: RuntimeStatusState["tone"]) => void;
  setStatus: (text: string, tone?: RuntimeStatusState["tone"]) => void;
  openRuntime: () => Promise<void>;
  toast: (message: string, tone?: string, timeoutMs?: number) => void;
  updateBranding: (branding: Partial<AppShellBranding>) => void;
  setSidebarCollapsed: (collapsed: boolean) => void;
  updateAuth: (state: Partial<AppShellAuthState>) => void;
  requireAuth: (state: Partial<AppShellAuthState>, message?: string) => void;
  unmount: () => void;
}
