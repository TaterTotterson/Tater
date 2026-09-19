function safeStorageGet(key, fallback = "") {
  try {
    const value = window.localStorage.getItem(String(key || ""));
    return value === null ? fallback : value;
  } catch {
    return fallback;
  }
}

function safeStorageSet(key, value) {
  try {
    window.localStorage.setItem(String(key || ""), String(value || ""));
  } catch {
    // Ignore storage failures (for example, restricted iframe storage).
  }
}

function createSessionId() {
  try {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return window.crypto.randomUUID();
    }
  } catch {
    // Ignore and fall back.
  }
  return `sess_${Date.now()}_${Math.random().toString(16).slice(2)}`;
}

function normalizeSpudexTab(value) {
  const token = String(value || "").trim().toLowerCase();
  if (token === "settings" || token === "policy") {
    return "settings";
  }
  return token === "manual" ? "manual" : "workbench";
}

function normalizeSpudLinkTab(value) {
  const token = String(value || "").trim().toLowerCase();
  if (token === "spudlet" || token === "upstream") {
    return "spudlet";
  }
  if (token === "settings" || token === "advanced") {
    return "settings";
  }
  return "pair";
}

function normalizeModelsTab(value) {
  const token = String(value || "").trim().toLowerCase();
  return ["huggingface", "routing", "speech", "wake", "vision", "audio-understanding", "speakerid", "emotionid", "faceid"].includes(token)
    ? token
    : "huggingface";
}

const state = {
  view: "dashboard",
  sessionId: safeStorageGet("tater_tateros_session_id", "") || createSessionId(),
  settingsTab: safeStorageGet("tater_tateros_settings_tab", "") || "general",
  integrationSubtab: safeStorageGet("tater_tateros_integration_tab", "") || "manager",
  dashboardPayload: null,
  dashboardShowMetrics: String(safeStorageGet("tater_dashboard_show_metrics", "true")).trim().toLowerCase() !== "false",
  dashboardShowMedia: String(safeStorageGet("tater_dashboard_show_media", "true")).trim().toLowerCase() !== "false",
  spudexSelectedSessionId: safeStorageGet("tater_spudex_selected_session_id", ""),
  spudexManualSessionId: safeStorageGet("tater_spudex_manual_session_id", ""),
  spudexTab: normalizeSpudexTab(safeStorageGet("tater_spudex_tab", "")),
  spudLinkTab: normalizeSpudLinkTab(safeStorageGet("tater_spud_link_tab", "pair")),
  modelsTab: normalizeModelsTab(safeStorageGet("tater_models_tab", "huggingface")),
  coreTopTab: safeStorageGet("tater_tateros_core_tab", "") || "manage",
  coreVueModulePromise: null,
  appShellController: null,
  sidebarCollapsed: String(safeStorageGet("tater_tateros_sidebar_collapsed", "false")).trim().toLowerCase() === "true",
  settingsLogAutoScroll: String(safeStorageGet("tater_settings_log_auto_scroll", "true")).trim().toLowerCase() !== "false",
  popupEffectStyle: String(safeStorageGet("tater_tateros_popup_effect_style", "flame")).trim().toLowerCase() || "flame",
  webuiTheme: String(safeStorageGet("tater_tateros_theme", "tater")).trim().toLowerCase() || "tater",
  activeChatJobs: {},
  redisStatus: {
    configured: false,
    connected: false,
    host: "",
    port: 6379,
    db: 0,
    username: "",
    use_tls: false,
    verify_tls: true,
    ca_cert_path: "",
    password_set: false,
    error: "",
  },
  auth: {
    checked: false,
    passwordSet: false,
    authenticated: false,
    mode: "ready",
    username: "User",
    userAvatar: "",
    appVersion: "",
    appVersionLabel: "",
  },
  chatProfile: {
    username: "User",
    userAvatar: "",
    taterAvatar: "",
    taterName: "Tater",
    taterFirstName: "Tater",
    taterLastName: "Totterson",
    taterFullName: "Tater Totterson",
    attachMaxMbEach: 0,
    attachMaxMbTotal: 0,
  },
};

safeStorageSet("tater_tateros_session_id", state.sessionId);

const APP_BASE_PATH = (() => {
  const rawPath = String(window.location.pathname || "/").trim();
  const normalized = rawPath.replace(/\/+$/, "");
  if (!normalized || normalized === "/") {
    return "";
  }
  return normalized;
})();
const IS_HA_INGRESS = APP_BASE_PATH.includes("/api/hassio_ingress/");

function withBasePath(path) {
  const raw = String(path || "").trim();
  if (!raw) {
    return APP_BASE_PATH || "/";
  }
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(raw) || raw.startsWith("//")) {
    return raw;
  }
  if (!APP_BASE_PATH) {
    return raw;
  }
  const normalized = raw.startsWith("/") ? raw : `/${raw}`;
  return `${APP_BASE_PATH}${normalized}`;
}

const SETTINGS_TAB_KEYS = ["general", "people", "models", "hydra", "esphome", "redis", "spudhub", "misc", "advanced", "system", "logs"];

const POPUP_EFFECT_STYLE_CHOICES = ["disabled", "flame", "dust", "glitch", "portal", "melt"];
const WEBUI_THEME_CHOICES = ["tater", "tater-light", "blueberry", "mint", "grape", "strawberry"];

function normalizeWebuiTheme(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return WEBUI_THEME_CHOICES.includes(normalized) ? normalized : "tater";
}

function applyWebuiTheme(value) {
  const normalized = normalizeWebuiTheme(value);
  state.webuiTheme = normalized;
  safeStorageSet("tater_tateros_theme", normalized);
  document.documentElement.setAttribute("data-theme", normalized);
  if (document.body) document.body.setAttribute("data-theme", normalized);
  return normalized;
}

function normalizePopupEffectStyle(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (POPUP_EFFECT_STYLE_CHOICES.includes(normalized)) {
    return normalized;
  }
  return "flame";
}

function applyPopupEffectStyle(value) {
  const normalized = normalizePopupEffectStyle(value);
  state.popupEffectStyle = normalized;
  safeStorageSet("tater_tateros_popup_effect_style", normalized);
  if (document.body) {
    document.body.setAttribute("data-popup-effect", normalized);
  }
  return normalized;
}

applyWebuiTheme(state.webuiTheme);
applyPopupEffectStyle(state.popupEffectStyle);

function showToast(message, type = "success", timeoutMs = 2600) {
  if (state.appShellController?.toast) {
    state.appShellController.toast(message, type, timeoutMs);
    return;
  }
  const text = String(message || "").trim();
  if (text) console[type === "error" ? "error" : "info"](text);
}

let healthRefreshTimer = 0;
let healthRefreshPromise = null;
let webuiAuthRecoveryInFlight = false;
let webuiAuthRecoveryLastAt = 0;

const REDIS_BOOTSTRAP_STATUS_TIMEOUT_MS = 900;
const REDIS_STATUS_TIMEOUT_MS = 2400;
const HEALTH_REQUEST_TIMEOUT_MS = 1200;
const HEALTH_POLL_RECOVERY_MS = 2200;
const HEALTH_POLL_CONNECTED_MS = 30000;

function _renderTaterBuildVersion(version, versionLabel) {
  state.appShellController?.updateBranding?.({ version, versionLabel });
}

function _setWebuiAuthStatus(raw) {
  const next = raw && typeof raw === "object" ? raw : {};
  if (Object.prototype.hasOwnProperty.call(next, "webui_theme")) applyWebuiTheme(next.webui_theme);
  const passwordSet = Boolean(next.password_set);
  const authenticated = Boolean(next.authenticated);
  const modeToken = String(next.mode || "").trim().toLowerCase();
  const mode = modeToken || (authenticated || !passwordSet ? "ready" : "login");
  const appVersion = String(next.app_version || "").trim().replace(/^v/i, "");
  const appVersionLabel = String(next.app_version_label || "").trim();
  state.auth = {
    checked: true,
    passwordSet,
    authenticated,
    mode,
    username: String(next.username || "User"),
    userAvatar: String(next.user_avatar || ""),
    appVersion,
    appVersionLabel,
  };
  _renderTaterBuildVersion(appVersion, appVersionLabel);
  state.appShellController?.updateAuth?.(appShellAuthState(state.auth));
  return state.auth;
}

function appShellAuthState(auth = state.auth, message = "") {
  const current = auth && typeof auth === "object" ? auth : {};
  return {
    required: Boolean(current.passwordSet && !current.authenticated),
    passwordSet: Boolean(current.passwordSet),
    authenticated: Boolean(current.authenticated),
    mode: String(current.mode || "ready"),
    username: String(current.username || "User"),
    userAvatar: String(current.userAvatar || ""),
    message: String(message || ""),
  };
}

async function fetchWebuiAuthStatus() {
  const raw = await api("/api/auth/status", {
    _skipRedisRecovery: true,
    _skipAuthRecovery: true,
    _timeoutMs: 2500,
  });
  return _setWebuiAuthStatus(raw);
}

function _isWebuiAuthApiPath(path) {
  const target = String(path || "").trim().toLowerCase();
  return target.startsWith("/api/auth/");
}

function _shouldTriggerWebuiAuthRecovery(path, statusCode) {
  const status = Number(statusCode || 0);
  return status === 401 && !_isWebuiAuthApiPath(path);
}

async function promptWebuiAuthRecovery(reason = "", { force = false } = {}) {
  const now = Date.now();
  if (!force) {
    if (webuiAuthRecoveryInFlight) {
      return;
    }
    if (now - webuiAuthRecoveryLastAt < 1200) {
      return;
    }
  }
  webuiAuthRecoveryInFlight = true;
  webuiAuthRecoveryLastAt = now;

  try {
    let auth = state.auth || {};
    try {
      auth = await fetchWebuiAuthStatus();
    } catch {
      // Fall back to current state if status refresh fails.
    }
    if (!auth.passwordSet || auth.authenticated) return;
    state.appShellController?.requireAuth?.(
      appShellAuthState(auth),
      String(reason || "Session expired. Please log in again.")
    );
  } finally {
    webuiAuthRecoveryInFlight = false;
  }
}

function _setRedisStatus(status) {
  const next = status && typeof status === "object" ? status : {};
  state.redisStatus = {
    mode: String(next.mode || (next.internal ? "internal" : "external") || "internal"),
    internal: Boolean(next.internal || String(next.mode || "").toLowerCase() === "internal"),
    configured: Boolean(next.configured),
    connected: Boolean(next.connected),
    host: String(next.host || ""),
    port: Number(next.port || 6379),
    db: Number(next.db || 0),
    username: String(next.username || ""),
    use_tls: Boolean(next.use_tls),
    verify_tls: Boolean(next.verify_tls),
    ca_cert_path: String(next.ca_cert_path || ""),
    password_set: Boolean(next.password_set),
    error: String(next.error || ""),
    source: String(next.source || ""),
    config_path: String(next.config_path || ""),
    data_path: String(next.data_path || ""),
    data_dir: String(next.data_dir || ""),
    socket_path: String(next.socket_path || ""),
    redis_pid: Number(next.redis_pid || 0),
    redis_managed: Boolean(next.redis_managed),
    redis_server_source: String(next.redis_server_source || ""),
  };
  return state.redisStatus;
}

function _scheduleHealthRefresh(delayMs = HEALTH_POLL_CONNECTED_MS) {
  if (healthRefreshTimer) {
    window.clearTimeout(healthRefreshTimer);
    healthRefreshTimer = 0;
  }
  healthRefreshTimer = window.setTimeout(() => {
    healthRefreshTimer = 0;
    void refreshHealth();
  }, Math.max(0, Number(delayMs) || 0));
}

async function ensureRedisSetup() {
  let status = state.redisStatus || {};
  try {
    status = _setRedisStatus(
      await api("/api/redis/status", {
        _skipRedisRecovery: true,
        _timeoutMs: REDIS_BOOTSTRAP_STATUS_TIMEOUT_MS,
      })
    );
  } catch (error) {
    const message = String(error?.message || "Failed to load Redis status.");
    status = _setRedisStatus({
      ...(status || {}),
      connected: false,
      error: message,
    });
  }
  return status;
}

function _isRedisSetupApiPath(path) {
  const target = String(path || "").trim().toLowerCase();
  return target.startsWith("/api/redis/");
}

function _isLikelyRedisFailureDetail(detail) {
  const message = String(detail || "").toLowerCase();
  if (!message) {
    return false;
  }
  return (
    message.includes("redis") ||
    message.includes("noauth") ||
    message.includes("wrongpass")
  );
}

function _shouldTriggerRedisRecovery(path, statusCode, detail) {
  const status = Number(statusCode || 0);
  if (_isRedisSetupApiPath(path) && status >= 500) {
    return true;
  }
  if (status === 503 && _isLikelyRedisFailureDetail(detail)) {
    return true;
  }
  return false;
}

async function promptRedisSetupRecovery(reason = "", { force = false } = {}) {
  void reason;
  void force;
  _scheduleHealthRefresh(180);
}

async function api(path, options = {}) {
  const requestOptions = options && typeof options === "object" ? options : {};
  const skipRedisRecovery = Boolean(requestOptions._skipRedisRecovery);
  const skipAuthRecovery = Boolean(requestOptions._skipAuthRecovery);
  const timeoutMs = Math.max(0, Number(requestOptions._timeoutMs || 0));
  const { _skipRedisRecovery, _skipAuthRecovery, _timeoutMs, ...fetchOptions } = requestOptions;

  let timeoutId = 0;
  let timeoutController = null;
  let requestSignal = fetchOptions.signal;
  if (!requestSignal && timeoutMs > 0 && typeof AbortController !== "undefined") {
    timeoutController = new AbortController();
    requestSignal = timeoutController.signal;
    timeoutId = window.setTimeout(() => {
      try {
        timeoutController.abort();
      } catch {
        // ignore
      }
    }, timeoutMs);
  }

  let response;
  try {
    response = await fetch(withBasePath(path), {
      headers: { "Content-Type": "application/json", ...(fetchOptions.headers || {}) },
      ...fetchOptions,
      ...(requestSignal ? { signal: requestSignal } : {}),
    });
  } catch (error) {
    if (timeoutId) {
      window.clearTimeout(timeoutId);
    }
    if (timeoutController && error?.name === "AbortError") {
      throw new Error(`Request timed out after ${timeoutMs}ms`);
    }
    throw error;
  }
  if (timeoutId) {
    window.clearTimeout(timeoutId);
  }

  if (!response.ok) {
    let detail = "Request failed";
    let detailPayload = null;
    try {
      const body = await response.json();
      detailPayload = body.detail || detail;
      if (detailPayload && typeof detailPayload === "object") {
        detail = String(detailPayload.message || detailPayload.detail || detail);
      } else {
        detail = String(detailPayload || detail);
      }
    } catch {
      detail = response.statusText || detail;
    }
    if (!skipAuthRecovery && _shouldTriggerWebuiAuthRecovery(path, response.status)) {
      void promptWebuiAuthRecovery(detail);
    }
    if (!skipRedisRecovery && _shouldTriggerRedisRecovery(path, response.status, detail)) {
      void promptRedisSetupRecovery(detail);
    }
    const error = new Error(detail);
    error.status = response.status;
    error.detail = detailPayload;
    if (detailPayload && typeof detailPayload === "object" && detailPayload.code) {
      error.code = String(detailPayload.code || "");
    }
    throw error;
  }

  if (response.status === 204) {
    return {};
  }

  return response.json();
}

function normalizeSettingsTab(value) {
  const token = String(value || "").trim().toLowerCase();
  return SETTINGS_TAB_KEYS.includes(token) ? token : "general";
}

function setPreferredSettingsTab(tabKey) {
  const normalized = normalizeSettingsTab(tabKey);
  state.settingsTab = normalized;
  safeStorageSet("tater_tateros_settings_tab", normalized);
  return normalized;
}

function setPreferredIntegrationTab(tabKey) {
  const token = String(tabKey || "").trim().toLowerCase();
  const normalized = ["manager", "devices", "rooms", "runtime"].includes(token) ? token : "manager";
  state.integrationSubtab = normalized;
  safeStorageSet("tater_tateros_integration_tab", normalized);
  return normalized;
}

function _composeName(firstRaw, lastRaw, fallback = "Tater Totterson") {
  const first = String(firstRaw || "").trim();
  const last = String(lastRaw || "").trim();
  const joined = [first, last].filter(Boolean).join(" ").trim();
  return joined || String(fallback || "Tater Totterson").trim() || "Tater Totterson";
}

function getTaterFirstName() {
  return String(state.chatProfile.taterFirstName || state.chatProfile.taterName || "Tater").trim() || "Tater";
}

function getTaterFullName() {
  return (
    String(state.chatProfile.taterFullName || "").trim() ||
    _composeName(state.chatProfile.taterFirstName, state.chatProfile.taterLastName, "Tater Totterson")
  );
}

function applyBranding(firstNameRaw) {
  const firstName = String(firstNameRaw || "").trim() || "Tater";
  state.appShellController?.updateBranding?.({ firstName, fullName: getTaterFullName() });
}

async function refreshBranding() {
  try {
    const profile = await api("/api/chat/profile");
    applyVueChatProfile(profile || {});
    if (Object.prototype.hasOwnProperty.call(profile || {}, "webui_theme")) applyWebuiTheme(profile.webui_theme);
    if (Object.prototype.hasOwnProperty.call(profile || {}, "popup_effect_style")) applyPopupEffectStyle(profile.popup_effect_style);
  } catch {
    applyBranding(getTaterFirstName());
  }
}

function persistCoreTopTab(tabName) {
  const normalized = String(tabName || "manage").trim() || "manage";
  state.coreTopTab = normalized;
  safeStorageSet("tater_tateros_core_tab", normalized);
}

function setRuntimeSummaryText(text, tone = "normal") {
  state.appShellController?.setStatus?.(text, tone);
}

function setRuntimeSummaryHealth(health, tone = "normal") {
  state.appShellController?.setHealth?.(health || {}, tone);
}

async function refreshHealth() {
  if (healthRefreshPromise) {
    return healthRefreshPromise;
  }

  healthRefreshPromise = (async () => {
    let redisConnected = false;
    try {
      const health = await api("/api/health", { _timeoutMs: HEALTH_REQUEST_TIMEOUT_MS });
      if (health?.redis_status && typeof health.redis_status === "object") {
        _setRedisStatus(health.redis_status);
      }
      redisConnected = Boolean(health?.redis_status?.connected ?? health?.redis);
      if (!redisConnected) {
        setRuntimeSummaryText("Starting internal Redis", "offline");
        return health;
      }
      setRuntimeSummaryHealth(health, "normal");
      return health;
    } catch {
      setRuntimeSummaryText("Backend offline", "offline");
      return null;
    } finally {
      const nextDelay = redisConnected ? HEALTH_POLL_CONNECTED_MS : HEALTH_POLL_RECOVERY_MS;
      _scheduleHealthRefresh(nextDelay);
      healthRefreshPromise = null;
    }
  })();

  return healthRefreshPromise;
}

function taterVueAssetVersion() {
  const entryScript = Array.from(document.scripts || []).find((script) =>
    String(script.src || "").includes("/static/app.js")
  );
  if (entryScript?.src) {
    try {
      const buildVersion = new URL(entryScript.src, window.location.href).searchParams.get("v");
      if (buildVersion) return buildVersion;
    } catch (_error) {
      // Fall through to the released Tater version for older entry pages.
    }
  }
  return String(state.auth?.appVersion || state.auth?.appVersionLabel || "").trim().replace(/^v/i, "");
}

function taterVueAssetUrl(path) {
  const base = withBasePath(path);
  const version = taterVueAssetVersion();
  return version ? `${base}${base.includes("?") ? "&" : "?"}v=${encodeURIComponent(version)}` : base;
}

function ensureTaterVueStylesheet() {
  const href = taterVueAssetUrl("/static/ui/tater-ui.css");
  let link = document.getElementById("tater-vue-stylesheet");
  if (link instanceof HTMLLinkElement && link.href.endsWith(href)) {
    return;
  }
  if (!(link instanceof HTMLLinkElement)) {
    link = document.createElement("link");
    link.id = "tater-vue-stylesheet";
    link.rel = "stylesheet";
    document.head.appendChild(link);
  }
  link.href = href;
}

async function loadCoreVueModule() {
  ensureTaterVueStylesheet();
  if (!state.coreVueModulePromise) {
    state.coreVueModulePromise = import(taterVueAssetUrl("/static/ui/tater-ui.js")).catch((error) => {
      state.coreVueModulePromise = null;
      throw error;
    });
  }
  return state.coreVueModulePromise;
}

function createRuntimeVueOptions() {
  return {
    initialState: { text: "Checking system…", tone: "normal" },
    endpoints: {
      breakdown: withBasePath("/api/runtime/breakdown"),
      telemetry: withBasePath("/api/runtime/telemetry"),
      unloadModel: withBasePath("/api/runtime/local-llm/unload"),
    },
    onHealthRefresh: () => void refreshHealth(),
    onToast: (message, tone) => showToast(message, tone === "error" ? "error" : "success", tone === "error" ? 4200 : 2800),
  };
}

function createDashboardVueOptions(payload) {
  return {
    initialPayload: payload || {},
    dashboardEndpoint: withBasePath("/api/dashboard"),
    refreshBriefsEndpoint: withBasePath("/api/dashboard/briefs/refresh"),
    settingsEndpoint: withBasePath("/api/dashboard/settings"),
    initialPreferences: {
      showMetrics: state.dashboardShowMetrics,
      showMedia: state.dashboardShowMedia,
    },
    onPreferencesChange: (preferences) => {
      state.dashboardShowMetrics = Boolean(preferences?.showMetrics);
      state.dashboardShowMedia = Boolean(preferences?.showMedia);
      safeStorageSet("tater_dashboard_show_metrics", state.dashboardShowMetrics ? "true" : "false");
      safeStorageSet("tater_dashboard_show_media", state.dashboardShowMedia ? "true" : "false");
    },
    onPayloadChange: (nextPayload) => {
      state.dashboardPayload = nextPayload;
    },
    onNavigate: (target) => void openDashboardUpdateTarget(target),
    onToast: (message, tone) => showToast(message, tone === "error" ? "error" : "success", tone === "error" ? 4200 : 2800),
  };
}

function applyVueChatProfile(profile = {}) {
  const firstName = String(profile.tater_first_name || profile.tater_name || "").trim() || "Tater";
  const lastName = String(profile.tater_last_name || "").trim() || "Totterson";
  const fullName = String(profile.tater_full_name || "").trim() || _composeName(firstName, lastName, "Tater Totterson");
  state.chatProfile = {
    username: String(profile.username || "User"),
    userAvatar: String(profile.user_avatar || ""),
    taterAvatar: String(profile.tater_avatar || ""),
    taterName: firstName,
    taterFirstName: firstName,
    taterLastName: lastName,
    taterFullName: fullName,
    attachMaxMbEach: Number(profile.attach_max_mb_each ?? state.chatProfile.attachMaxMbEach ?? 0),
    attachMaxMbTotal: Number(profile.attach_max_mb_total ?? state.chatProfile.attachMaxMbTotal ?? 0),
  };
  applyBranding(firstName);
}

function createChatVueDescriptor({ profile, messages, stats }) {
  const payload = { profile: profile || {}, messages: Array.isArray(messages) ? messages : [], stats: stats || {} };
  return {
    state: payload,
    options: {
      initialProfile: payload.profile,
      initialMessages: payload.messages,
      initialStats: payload.stats,
      initialJobs: state.activeChatJobs,
      sessionId: state.sessionId,
      isIngress: IS_HA_INGRESS,
      endpoints: {
        history: withBasePath("/api/chat/history"),
        profile: withBasePath("/api/chat/profile"),
        stats: withBasePath("/api/chat/stats"),
        jobs: withBasePath("/api/chat/jobs"),
        files: withBasePath("/api/chat/files"),
      },
      onSessionChange: (sessionId) => {
        state.sessionId = String(sessionId || "").trim() || state.sessionId;
        safeStorageSet("tater_tateros_session_id", state.sessionId);
      },
      onProfileChange: (nextProfile) => applyVueChatProfile(nextProfile || {}),
      onJobsChange: (jobs) => {
        state.activeChatJobs = jobs && typeof jobs === "object" ? jobs : {};
      },
      onToast: (message, tone) => showToast(message, tone === "error" ? "error" : "success", tone === "error" ? 4200 : 2800),
      onRequestError: (message) => {
        if (_isLikelyRedisFailureDetail(message || "")) {
          void promptRedisSetupRecovery(String(message || "Redis connection lost."));
        } else {
          _scheduleHealthRefresh(220);
        }
      },
      onHealthRefresh: () => void refreshHealth(),
    },
  };
}

function createIntegrationsVueDescriptor(settings) {
  return {
    state: { settings: settings || {} },
    options: {
      initialSettings: settings || {},
      initialTab: state.integrationSubtab || "manager",
      endpoints: {
        settings: withBasePath("/api/settings"),
        shop: withBasePath("/api/shop/integrations"),
        integrationSettings: withBasePath("/api/settings/integrations"),
        integrationActions: withBasePath("/api/settings/integrations"),
        deviceRegistry: withBasePath("/api/settings/integrations/device-registry"),
        rooms: withBasePath("/api/settings/integrations/rooms"),
        runtime: withBasePath("/api/settings/integrations/runtime"),
        runtimeStates: withBasePath("/api/settings/integrations/runtime/states"),
        runtimeEvents: withBasePath("/api/settings/integrations/runtime/events"),
        systemTasks: withBasePath("/api/settings/system-tasks"),
      },
      onTabChange: (tab) => setPreferredIntegrationTab(tab),
      onToast: (message, tone) => showToast(message, tone === "error" ? "error" : "success", tone === "error" ? 4200 : 2800),
    },
  };
}

function createVerbasVueDescriptor(runtimeData, shopData) {
  const payload = { runtime: runtimeData || {}, shop: shopData || {} };
  return {
    state: { payload },
    options: {
      initialPayload: payload,
      endpoints: {
        runtime: withBasePath("/api/verbas"),
        shop: withBasePath("/api/shop/verbas"),
      },
      onToast: (message, tone) => showToast(message, tone === "error" ? "error" : "success", tone === "error" ? 4200 : 2800),
      onHealthRefresh: () => void refreshHealth(),
    },
  };
}

function createPortalsVueDescriptor(runtimeData, shopData) {
  const payload = { runtime: runtimeData || {}, shop: shopData || {} };
  return {
    state: { payload },
    options: {
      initialPayload: payload,
      endpoints: {
        runtime: withBasePath("/api/portals"),
        shop: withBasePath("/api/shop/portals"),
      },
      onToast: (message, tone) => showToast(message, tone === "error" ? "error" : "success", tone === "error" ? 4200 : 2800),
      onHealthRefresh: () => void refreshHealth(),
    },
  };
}

function createCoresVueDescriptor(runtimeData, shopData, tabsData) {
  const payload = { runtime: runtimeData || {}, shop: shopData || {}, tabs: tabsData || { tabs: [] } };
  return {
    state: { payload },
    options: {
      initialPayload: payload,
      initialTab: state.coreTopTab || "manage",
      endpoints: {
        runtime: withBasePath("/api/cores"),
        shop: withBasePath("/api/shop/cores"),
        tabs: withBasePath("/api/cores/tabs"),
      },
      onTabChange: (tab) => persistCoreTopTab(tab),
      onToast: (message, tone) => showToast(message, tone === "error" ? "error" : "success", tone === "error" ? 4200 : 2800),
      onHealthRefresh: () => void refreshHealth(),
    },
  };
}

function createSpudexVueDescriptor(payload) {
  const initialPayload = payload || {};
  return {
    state: { payload: initialPayload },
    options: {
      initialPayload,
      initialTab: state.spudexTab || "workbench",
      initialSessionId: state.spudexSelectedSessionId || "",
      initialManualSessionId: state.spudexManualSessionId || "",
      profile: {
        username: state.chatProfile.username || "User",
        user_avatar: state.chatProfile.userAvatar || "",
        tater_avatar: state.chatProfile.taterAvatar || "",
        tater_name: state.chatProfile.taterName || "Tater",
        tater_first_name: state.chatProfile.taterFirstName || state.chatProfile.taterName || "Tater",
        tater_last_name: state.chatProfile.taterLastName || "Totterson",
        tater_full_name: state.chatProfile.taterFullName || getTaterFullName(),
      },
      endpoints: {
        root: withBasePath("/api/spudex"),
        settings: withBasePath("/api/spudex/settings"),
        run: withBasePath("/api/spudex/run"),
        chat: withBasePath("/api/spudex/chat"),
        chatSession: withBasePath("/api/spudex/chat/session"),
        sessions: withBasePath("/api/spudex/sessions"),
        chatFiles: withBasePath("/api/chat/files"),
      },
      onTabChange: (tab) => {
        state.spudexTab = normalizeSpudexTab(tab);
        safeStorageSet("tater_spudex_tab", state.spudexTab);
      },
      onSessionChange: (sessionId) => {
        state.spudexSelectedSessionId = String(sessionId || "").trim();
        safeStorageSet("tater_spudex_selected_session_id", state.spudexSelectedSessionId);
      },
      onManualSessionChange: (sessionId) => {
        state.spudexManualSessionId = String(sessionId || "").trim();
        safeStorageSet("tater_spudex_manual_session_id", state.spudexManualSessionId);
      },
      onToast: (message, tone) => showToast(message, tone === "error" ? "error" : "success", tone === "error" ? 4200 : 2800),
    },
  };
}

function createSettingsVueDescriptor(settings, redisStatus, redisEncryptionStatus, hooks = {}) {
  const summary = {
    redisConnected: Boolean(redisStatus?.connected),
    adminGateCount: Array.isArray(settings?.admin_only_plugins) ? settings.admin_only_plugins.length : 0,
    integrationCount: Array.isArray(settings?.integrations) ? settings.integrations.length : 0,
  };
  const viewOptions = {
    initialTab: !redisStatus?.connected ? "redis" : normalizeSettingsTab(state.settingsTab || "general"),
    initialSummary: summary,
    initialGeneral: {
      username: settings?.username || "User",
      user_avatar: settings?.user_avatar || "",
      tater_avatar: settings?.tater_avatar || "",
      show_speed_stats: Boolean(settings?.show_speed_stats),
      webui_theme: normalizeWebuiTheme(settings?.webui_theme || state.webuiTheme),
      tater_first_name: settings?.tater_first_name || "Tater",
      tater_last_name: settings?.tater_last_name || "Totterson",
      tater_personality: settings?.tater_personality || "",
      webui_password_set: Boolean(settings?.webui_password_set),
    },
    initialHydra: {
      max_display: Number(settings?.max_display ?? 8),
      max_store: Number(settings?.max_store ?? 20),
      max_llm: Number(settings?.max_llm ?? 8),
      hydra_max_ledger_items: Number(settings?.hydra_max_ledger_items ?? 1500),
      hydra_astraeus_plan_review_enabled: Boolean(settings?.hydra_astraeus_plan_review_enabled),
      hydra_auto_continue_incomplete_final_enabled: Boolean(settings?.hydra_auto_continue_incomplete_final_enabled),
      defaults: {
        max_display: Number(settings?.hydra_defaults?.max_display ?? 8),
        max_store: Number(settings?.hydra_defaults?.max_store ?? 20),
        max_llm: Number(settings?.hydra_defaults?.max_llm ?? 8),
        hydra_max_ledger_items: Number(settings?.hydra_defaults?.hydra_max_ledger_items ?? 1500),
        hydra_astraeus_plan_review_enabled: settings?.hydra_defaults?.hydra_astraeus_plan_review_enabled !== false,
        hydra_auto_continue_incomplete_final_enabled: Boolean(settings?.hydra_defaults?.hydra_auto_continue_incomplete_final_enabled),
      },
    },
    initialPeople: settings?.people && typeof settings.people === "object" ? settings.people : {},
    initialPeopleTab: safeStorageGet("tater_people_tab", "people"),
    initialPeopleSort: safeStorageGet("tater_people_sort", "recent"),
    initialSpudLink: settings?.spud_link && typeof settings.spud_link === "object" ? settings.spud_link : {},
    initialModels: settings && typeof settings === "object" ? settings : {},
    initialSpudLinkTab: normalizeSpudLinkTab(state.spudLinkTab || "pair"),
    initialModelsTab: normalizeModelsTab(state.modelsTab || "huggingface"),
    initialVoiceTab: safeStorageGet("tater_voice_tab", "satellites"),
    initialRedisStatus: redisStatus && typeof redisStatus === "object" ? redisStatus : {},
    initialRedisEncryptionStatus:
      redisEncryptionStatus && typeof redisEncryptionStatus === "object" ? redisEncryptionStatus : {},
    initialMisc: {
      popup_effect_style: settings?.popup_effect_style || "flame",
      emoji_enable_on_reaction_add: settings?.emoji_enable_on_reaction_add !== false,
      emoji_enable_auto_reaction_on_reply: settings?.emoji_enable_auto_reaction_on_reply !== false,
      emoji_reaction_chain_chance_percent: Number(settings?.emoji_reaction_chain_chance_percent ?? 100),
      emoji_reply_reaction_chance_percent: Number(settings?.emoji_reply_reaction_chance_percent ?? 12),
      emoji_reaction_chain_cooldown_seconds: Number(settings?.emoji_reaction_chain_cooldown_seconds ?? 30),
      emoji_reply_reaction_cooldown_seconds: Number(settings?.emoji_reply_reaction_cooldown_seconds ?? 120),
      emoji_min_message_length: Number(settings?.emoji_min_message_length ?? 4),
    },
    initialAdvanced: {
      admin_plugin_options: Array.isArray(settings?.admin_plugin_options) ? settings.admin_plugin_options : [],
      admin_only_plugins: Array.isArray(settings?.admin_only_plugins) ? settings.admin_only_plugins : [],
      admin_only_plugins_defaults: Array.isArray(settings?.admin_only_plugins_defaults) ? settings.admin_only_plugins_defaults : [],
      tater_api_enabled: Boolean(settings?.tater_api_enabled),
      tater_api_key_set: Boolean(settings?.tater_api_key_set),
      tater_api_mode: settings?.tater_api_mode || "direct",
      tater_api_hydra_tools_enabled: Boolean(settings?.tater_api_hydra_tools_enabled),
    },
    initialLogAutoScroll: state.settingsLogAutoScroll,
    endpoints: {
      general: withBasePath("/api/settings/general"),
      misc: withBasePath("/api/settings/misc"),
      advanced: withBasePath("/api/settings/advanced"),
      hydra: withBasePath("/api/settings/hydra"),
      hydraMetrics: withBasePath("/api/settings/hydra/metrics"),
      hydraData: withBasePath("/api/settings/hydra/data"),
      hydraDataClear: withBasePath("/api/settings/hydra/data/clear"),
      systemTasks: withBasePath("/api/settings/system-tasks"),
      coreTaskRun: withBasePath("/api/settings/core-tasks"),
      logs: withBasePath("/api/settings/logs"),
      people: withBasePath("/api/settings/people"),
      peopleAction: withBasePath("/api/settings/people/action"),
      spudLink: withBasePath("/api/settings/spud-link"),
      spudLinkStatus: withBasePath("/api/spudlink/status"),
      spudLinkPairingCode: withBasePath("/api/spudlink/pairing-code"),
      spudLinkConnect: withBasePath("/api/spudlink/connect"),
      spudLinkRevoke: withBasePath("/api/spudlink/revoke-node"),
      models: withBasePath("/api/settings"),
      modelsVoiceRuntime: withBasePath("/api/settings/voice/runtime"),
      modelsVoiceAction: withBasePath("/api/settings/voice/runtime/action"),
      modelsLocalLlm: withBasePath("/api/settings/local-llm/models"),
      modelsLocalLlmDelete: withBasePath("/api/settings/local-llm/models/delete"),
      modelsHuggingFace: withBasePath("/api/settings/huggingface/models"),
      modelsHuggingFaceDetail: withBasePath("/api/settings/huggingface/model"),
      modelsHuggingFaceDownload: withBasePath("/api/settings/huggingface/download"),
      modelsHfWarmup: withBasePath("/api/settings/hf-llm/warmup"),
      modelsHfWarmupCancel: withBasePath("/api/settings/hf-llm/warmup/cancel"),
      modelsRemoteLlm: withBasePath("/api/settings/llama-cpp/remote-models"),
      modelsContextEstimate: withBasePath("/api/runtime/context-estimate"),
      modelsFaceStatus: withBasePath("/api/settings/face-id/status"),
      modelsSpeechPreview: withBasePath("/api/settings/speech/tts-preview"),
      modelsWyomingVoices: withBasePath("/api/settings/speech/wyoming-tts-voices"),
      modelsOpenAiVoices: withBasePath("/api/settings/speech/openai-compatible-tts-voices"),
      modelsOpenAiModels: withBasePath("/api/settings/speech/openai-compatible-tts-models"),
      modelsChatterboxVoices: withBasePath("/api/settings/speech/chatterbox-tts-voices"),
      modelsSpeechWarmup: withBasePath("/api/settings/speech/warmup"),
      modelsCloneAudio: withBasePath("/api/settings/speech/clone-audio"),
      voiceRuntime: withBasePath("/api/settings/voice/runtime"),
      voiceAction: withBasePath("/api/settings/voice/runtime/action"),
      voicePresence: withBasePath("/api/tater/satellite/v1/presence"),
      voicePresenceEvents: withBasePath("/api/tater/satellite/v1/presence/events"),
      redisStatus: withBasePath("/api/redis/status"),
      redisConfigure: withBasePath("/api/redis/configure"),
      redisMigrateInternal: withBasePath("/api/redis/migrate/internal"),
      redisEncryptionStatus: withBasePath("/api/redis/encryption/status"),
      redisEncrypt: withBasePath("/api/redis/encryption/encrypt"),
      redisDecrypt: withBasePath("/api/redis/encryption/decrypt"),
      clearChat: withBasePath("/api/chat/clear"),
    },
    publicEndpoints: {
      chat: `${window.location.origin.replace(/\/$/, "")}/v1/chat/completions`,
      models: `${window.location.origin.replace(/\/$/, "")}/v1/models`,
      spudLinkLlm: `${window.location.origin.replace(/\/$/, "")}${withBasePath("/api/spudlink/v1/tater/llm")}`,
      spudLinkModels: `${window.location.origin.replace(/\/$/, "")}${withBasePath("/api/spudlink/v1/models/capabilities")}`,
      spudLinkPair: `${window.location.origin.replace(/\/$/, "")}${withBasePath("/api/spudlink/pair")}`,
    },
    onTabChange: (tab) => {
      const normalized = normalizeSettingsTab(tab);
      setPreferredSettingsTab(normalized);
    },
    onGeneralChange: (nextSettings) => hooks.onGeneralChange?.(nextSettings || {}),
    onThemePreview: (theme) => applyWebuiTheme(theme),
    onHydraChange: (nextSettings) => hooks.onHydraChange?.(nextSettings || {}),
    onMiscChange: (nextSettings) => hooks.onMiscChange?.(nextSettings || {}),
    onAdvancedChange: (nextSettings) => hooks.onAdvancedChange?.(nextSettings || {}),
    onPeopleChange: (payload) => {
      settings.people = payload && typeof payload === "object" ? payload : {};
    },
    onPeopleTabChange: (tab) => safeStorageSet("tater_people_tab", String(tab || "people")),
    onPeopleSortChange: (sort) => safeStorageSet("tater_people_sort", String(sort || "recent")),
    onSpudLinkChange: (nextSettings) => {
      const next = nextSettings && typeof nextSettings === "object" ? nextSettings : {};
      const routes = next?.model_routes && typeof next.model_routes === "object"
        ? next.model_routes
        : next?.model_routing?.routes && typeof next.model_routing.routes === "object"
          ? next.model_routing.routes
          : {};
      settings.spud_link = next;
      settings.spud_link_mode = String(next.mode || "disabled");
      settings.spud_link_node_name = String(next.node_name || "");
      settings.spud_link_home_url = String(next.home_url || "");
      settings.spud_link_public_url = String(next.public_url || "");
      settings.spud_link_pairing_enabled = Boolean(next.pairing_enabled);
      settings.spud_link_allow_spudlets = next.allow_spudlets !== false;
      settings.spud_link_allow_little_spuds = next.allow_little_spuds !== false;
      settings.spud_link_little_spud_tools_enabled = next.little_spud_tools_enabled !== false;
      settings.spud_link_telemetry_enabled = next.telemetry_enabled !== false;
      settings.spud_link_request_previews_enabled = Boolean(next.request_previews_enabled);
      settings.spud_link_model_routing_enabled = Boolean(next.model_routing_enabled ?? next?.model_routing?.enabled);
      settings.spud_link_hub_url = String(next.hub_url || "");
      settings.spud_link_node_token_set = Boolean(next.node_token_set);
      ["llm", "vad", "stt", "tts", "vision", "audio", "video", "speaker_id", "emotion_id", "face_id"].forEach((kind) => {
        const route = kind === "llm" ? "hub" : String(routes[kind] || "auto");
        settings[`spud_link_model_route_${kind}`] = route;
      });
    },
    onSpudLinkTabChange: (tab) => {
      state.spudLinkTab = normalizeSpudLinkTab(tab);
      safeStorageSet("tater_spud_link_tab", state.spudLinkTab);
    },
    onModelsTabChange: (tab) => hooks.onModelsTabChange?.(normalizeModelsTab(tab)),
    onVoiceTabChange: (tab) => safeStorageSet("tater_voice_tab", String(tab || "satellites")),
    onRedisStatusChange: (nextStatus) => {
      _setRedisStatus(nextStatus || {});
    },
    onModelsChange: (nextSettings) => {
      if (nextSettings && typeof nextSettings === "object") Object.assign(settings, nextSettings);
    },
    onLogAutoScrollChange: (enabled) => {
      state.settingsLogAutoScroll = Boolean(enabled);
      safeStorageSet("tater_settings_log_auto_scroll", state.settingsLogAutoScroll ? "true" : "false");
    },
    onToast: (message, tone) => showToast(message, tone === "error" ? "error" : "success", tone === "error" ? 4200 : 2800),
  };
  return {
    state: {
      summary,
      general: viewOptions.initialGeneral,
      hydra: viewOptions.initialHydra,
      misc: viewOptions.initialMisc,
      people: viewOptions.initialPeople,
      spudLink: viewOptions.initialSpudLink,
      models: viewOptions.initialModels,
      advanced: viewOptions.initialAdvanced,
    },
    options: viewOptions,
  };
}

function createSettingsShellHooks(settings) {
  return {
    onModelsTabChange: (tab) => {
      const normalized = normalizeModelsTab(tab);
      state.modelsTab = normalized;
      safeStorageSet("tater_models_tab", normalized);
    },
    onGeneralChange: async (nextSettings) => {
      Object.assign(settings, nextSettings || {});
      applyWebuiTheme(nextSettings?.webui_theme || "tater");
      await Promise.all([
        refreshBranding(),
        fetchWebuiAuthStatus().catch(() => null),
      ]);
    },
    onMiscChange: (nextSettings) => {
      Object.assign(settings, nextSettings || {});
      applyPopupEffectStyle(nextSettings?.popup_effect_style || "flame");
    },
    onHydraChange: (nextSettings) => Object.assign(settings, nextSettings || {}),
    onAdvancedChange: (nextSettings) => Object.assign(settings, nextSettings || {}),
  };
}

async function loadVueShellView(viewName) {
  const view = String(viewName || "dashboard").trim().toLowerCase();
  if (view === "dashboard") {
    const payload = await api("/api/dashboard", { _timeoutMs: 12000 });
    state.dashboardPayload = payload;
    return { state: { payload: payload || {} }, options: createDashboardVueOptions(payload) };
  }
  if (view === "chat") {
    const [profile, history, stats] = await Promise.all([
      api("/api/chat/profile"),
      api("/api/chat/history"),
      api("/api/chat/stats").catch(() => ({ enabled: false, stats: null })),
    ]);
    applyVueChatProfile(profile || {});
    return createChatVueDescriptor({
      profile: profile || {},
      messages: Array.isArray(history?.messages) ? history.messages : [],
      stats: stats || { enabled: false, stats: null },
    });
  }
  if (view === "verbas") {
    const [runtimeData, shopData] = await Promise.all([api("/api/verbas"), api("/api/shop/verbas")]);
    return createVerbasVueDescriptor(runtimeData, shopData);
  }
  if (view === "portals") {
    const [runtimeData, shopData] = await Promise.all([api("/api/portals"), api("/api/shop/portals")]);
    return createPortalsVueDescriptor(runtimeData, shopData);
  }
  if (view === "cores") {
    const [runtimeData, shopData, tabsData] = await Promise.all([api("/api/cores"), api("/api/shop/cores"), api("/api/cores/tabs")]);
    return createCoresVueDescriptor(runtimeData, shopData, tabsData);
  }
  if (view === "integrations") {
    return createIntegrationsVueDescriptor(await api("/api/settings"));
  }
  if (view === "spudex") {
    const payload = await api("/api/spudex");
    return createSpudexVueDescriptor(payload);
  }
  if (view === "settings") {
    const [redisStatusPayload, redisEncryptionPayload, settings] = await Promise.all([
      api("/api/redis/status", { _skipRedisRecovery: true, _timeoutMs: REDIS_STATUS_TIMEOUT_MS }),
      api("/api/redis/encryption/status", { _skipRedisRecovery: true, _timeoutMs: HEALTH_REQUEST_TIMEOUT_MS }),
      api("/api/settings"),
    ]);
    const redisStatus = _setRedisStatus(redisStatusPayload);
    const redisEncryptionStatus = normalizeRedisEncryptionStatusPayload(redisEncryptionPayload);
    return createSettingsVueDescriptor(
      settings,
      redisStatus,
      redisEncryptionStatus,
      createSettingsShellHooks(settings)
    );
  }
  throw new Error(`Unknown view: ${view}`);
}

function handleVueShellViewChange(viewName) {
  const view = String(viewName || "dashboard").trim().toLowerCase();
  state.view = view;
  document.body.dataset.view = view;
}

async function mountVueAppShell() {
  const root = document.getElementById("tater-app-root");
  if (!(root instanceof HTMLElement)) return false;
  const module = await loadCoreVueModule();
  if (typeof module?.mountAppShell !== "function") return false;
  const controller = module.mountAppShell(root, {
    initialView: state.view || "dashboard",
    initialSidebarCollapsed: state.sidebarCollapsed,
    initialBranding: {
      firstName: getTaterFirstName(),
      fullName: getTaterFullName(),
      version: state.auth.appVersion,
      versionLabel: state.auth.appVersionLabel,
    },
    initialRuntimeState: {
      text: state.auth.passwordSet && !state.auth.authenticated ? "Login required" : "Checking system…",
      tone: state.auth.passwordSet && !state.auth.authenticated ? "offline" : "normal",
    },
    initialAuthState: appShellAuthState(state.auth),
    authenticate: authenticateWebui,
    runtimeOptions: createRuntimeVueOptions(),
    loadView: (view, options) => loadVueShellView(view, options),
    onViewChange: (view) => handleVueShellViewChange(view),
    onSidebarChange: (collapsed) => {
      state.sidebarCollapsed = Boolean(collapsed);
      safeStorageSet("tater_tateros_sidebar_collapsed", state.sidebarCollapsed ? "true" : "false");
    },
    onAuthenticated: () => finishAuthenticatedBootstrap(),
  });
  state.appShellController = controller;
  return true;
}

async function authenticateWebui(request = {}) {
  const setup = Boolean(request.setup);
  const payload = setup
    ? { password: String(request.password || ""), confirm_password: String(request.confirmPassword || "") }
    : { password: String(request.password || "") };
  const status = await api(setup ? "/api/auth/setup" : "/api/auth/login", {
    method: "POST",
    body: JSON.stringify(payload),
    _skipRedisRecovery: true,
    _skipAuthRecovery: true,
  });
  return appShellAuthState(_setWebuiAuthStatus(status));
}

async function finishAuthenticatedBootstrap() {
  await refreshBranding();
  await refreshHealth();
  _scheduleHealthRefresh(HEALTH_POLL_CONNECTED_MS);
}

async function openDashboardUpdateTarget(rawTarget) {
  const target = String(rawTarget || "").trim().toLowerCase();
  const shell = state.appShellController;
  if (!shell?.selectViewTab) throw new Error("The Vue application shell is not mounted.");
  if (target === "firmware") {
    setPreferredSettingsTab("esphome");
    safeStorageSet("tater_voice_tab", "firmware");
    await shell.selectViewTab("settings", "esphome", "firmware");
    return;
  }
  if (target === "integrations") {
    setPreferredIntegrationTab("manager");
    await shell.selectViewTab("integrations", "manager");
    return;
  }
  if (["cores", "portals", "verbas"].includes(target)) {
    await shell.selectViewTab(target, "manage");
  }
}

function normalizeRedisEncryptionStatusPayload(raw) {
  const next = raw && typeof raw === "object" ? raw : {};
  const liveEnabled = next.live_encryption_enabled;
  return {
    encryption_available: Boolean(next.encryption_available !== false),
    key_exists: Boolean(next.key_exists),
    key_path: String(next.key_path || ""),
    key_fingerprint: String(next.key_fingerprint || ""),
    live_encryption_enabled: liveEnabled === undefined ? Boolean(next.snapshot_exists) : Boolean(liveEnabled),
    live_encryption_state_path: String(next.live_encryption_state_path || next.snapshot_path || ""),
    live_encryption_updated: String(next.live_encryption_updated || next.snapshot_modified || ""),
    snapshot_exists: Boolean(next.snapshot_exists),
    snapshot_path: String(next.snapshot_path || ""),
    snapshot_size_bytes: Number(next.snapshot_size_bytes || 0),
    snapshot_modified: String(next.snapshot_modified || ""),
    error: String(next.error || ""),
  };
}

async function init() {
  state.settingsTab = normalizeSettingsTab(state.settingsTab || "general");
  await ensureRedisSetup();
  await fetchWebuiAuthStatus();
  if (!(await mountVueAppShell())) {
    throw new Error("The Vue application bundle did not provide the application shell.");
  }
  if (!state.auth.passwordSet || state.auth.authenticated) await finishAuthenticatedBootstrap();
}

window.addEventListener("beforeunload", () => {
  if (healthRefreshTimer) {
    window.clearTimeout(healthRefreshTimer);
    healthRefreshTimer = 0;
  }
  try {
    state.appShellController?.unmount?.();
  } catch (_error) {
    // The page is already leaving; ignore teardown failures.
  }
  state.appShellController = null;
});

init().catch((error) => {
  const root = document.getElementById("tater-app-root");
  if (root) {
    root.className = "tater-startup-error";
    root.textContent = `Failed to initialize Tater UI: ${error?.message || "unknown error"}`;
  }
});
