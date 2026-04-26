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

const state = {
  view: "chat",
  sessionId: safeStorageGet("tater_tateros_session_id", "") || createSessionId(),
  settingsTab: safeStorageGet("tater_tateros_settings_tab", "") || "general",
  coreTopTab: safeStorageGet("tater_tateros_core_tab", "") || "manage",
  coreTabSpecs: {},
  coreTabPayloadCache: {},
  coreTabLoadPromises: {},
  esphomeRuntimeLoadPromise: null,
  esphomeRuntimeRequestSeq: 0,
  esphomeFirmwarePayload: null,
  esphomeSpeakerIdPayload: null,
  esphomeFirmwareSelection: {
    templateKey: "",
    selector: "",
  },
  esphomeFirmwareDrafts: {},
  sidebarCollapsed: String(safeStorageGet("tater_tateros_sidebar_collapsed", "false")).trim().toLowerCase() === "true",
  sidebarCollapseTimer: 0,
  runtimeBreakdownPollTimer: 0,
  runtimeBreakdownPayload: null,
  runtimeHistoryWindow: "24h",
  runtimeSettingsSaveHandler: null,
  runtimeSettingsOpenHandler: null,
  runtimeSettingsCloseHandler: null,
  runtimeSettingsCatalog: {
    verbas: {},
    portals: {},
    cores: {},
  },
  popupEffectStyle: String(safeStorageGet("tater_tateros_popup_effect_style", "flame")).trim().toLowerCase() || "flame",
  sending: false,
  chatSendInFlight: 0,
  activeChatJobs: {},
  chatEventSources: {},
  chatPollMeta: {},
  notice: "",
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
const REDIS_PASSWORD_MASK = "********";

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

const VIEW_META = {
  chat: { title: "Chat", subtitle: "Talk to Tater Totterson" },
  verbas: { title: "Verba", subtitle: "Enable tools and manage Verba settings + shop updates." },
  portals: { title: "Portals", subtitle: "Portal runtime controls and full Portal Shop manager." },
  cores: { title: "Cores", subtitle: "Core runtime controls and full Core Shop manager." },
  settings: { title: "Settings", subtitle: "Global WebUI and Tater runtime configuration." },
};

const SETTINGS_TAB_KEYS = ["general", "models", "hydra", "integrations", "esphome", "redis", "misc", "advanced"];

const POPUP_EFFECT_STYLE_CHOICES = ["disabled", "flame", "dust", "glitch", "portal", "melt"];
const POPUP_EFFECT_CLOSE_MS = {
  disabled: 120,
  flame: 280,
  dust: 320,
  glitch: 260,
  portal: 300,
  melt: 300,
};
const SIDEBAR_EFFECT_MS = {
  disabled: { collapse: 120, expand: 140 },
  flame: { collapse: 460, expand: 480 },
  dust: { collapse: 500, expand: 520 },
  glitch: { collapse: 340, expand: 360 },
  portal: { collapse: 500, expand: 520 },
  melt: { collapse: 480, expand: 500 },
};
const SIDEBAR_EFFECT_LABELS = {
  disabled: { collapse: "Closing...", expand: "Opening..." },
  flame: { collapse: "Burning...", expand: "Igniting..." },
  dust: { collapse: "Crumbling...", expand: "Gathering..." },
  glitch: { collapse: "Glitching...", expand: "Re-syncing..." },
  portal: { collapse: "Warping...", expand: "Returning..." },
  melt: { collapse: "Melting...", expand: "Reforming..." },
};
const SIDEBAR_SYMBOLS = {
  menu: "☰",
  hide: "✕",
  busy: "⋯",
};

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
  applySidebarState();
  return normalized;
}

function getPopupEffectCloseMs(style = state.popupEffectStyle) {
  const normalized = normalizePopupEffectStyle(style);
  const mapped = Number(POPUP_EFFECT_CLOSE_MS[normalized]);
  return Number.isFinite(mapped) && mapped > 0 ? mapped : 280;
}

function getSidebarEffectMs(direction = "collapse", style = state.popupEffectStyle) {
  const normalized = normalizePopupEffectStyle(style);
  const profile = SIDEBAR_EFFECT_MS[normalized] || SIDEBAR_EFFECT_MS.flame;
  const key = direction === "expand" ? "expand" : "collapse";
  const fallback = key === "expand" ? 480 : 460;
  const mapped = Number(profile?.[key]);
  return Number.isFinite(mapped) && mapped > 0 ? mapped : fallback;
}

function getSidebarEffectLabel(direction = "collapse", style = state.popupEffectStyle) {
  const normalized = normalizePopupEffectStyle(style);
  const profile = SIDEBAR_EFFECT_LABELS[normalized] || SIDEBAR_EFFECT_LABELS.flame;
  return direction === "expand" ? profile.expand : profile.collapse;
}

applyPopupEffectStyle(state.popupEffectStyle);

function applySidebarState() {
  const shell = document.getElementById("app-shell");
  const collapseBtn = document.getElementById("sidebar-collapse-btn");
  const expandBtn = document.getElementById("sidebar-expand-btn");
  if (!shell) {
    return;
  }
  const isCollapsing = shell.classList.contains("sidebar-collapsing");
  const isExpanding = shell.classList.contains("sidebar-expanding");
  const isAnimating = isCollapsing || isExpanding;
  shell.classList.toggle("sidebar-collapsed", Boolean(state.sidebarCollapsed));
  if (collapseBtn) {
    collapseBtn.disabled = isAnimating;
    collapseBtn.setAttribute("aria-label", state.sidebarCollapsed ? "Show menu" : "Hide menu");
    collapseBtn.title = state.sidebarCollapsed ? "Show menu" : "Hide menu";
    const collapseLabel = getSidebarEffectLabel("collapse");
    const expandLabel = getSidebarEffectLabel("expand");
    collapseBtn.textContent = isCollapsing || isExpanding ? SIDEBAR_SYMBOLS.busy : state.sidebarCollapsed ? SIDEBAR_SYMBOLS.menu : SIDEBAR_SYMBOLS.hide;
    collapseBtn.dataset.statusText = isCollapsing ? collapseLabel : isExpanding ? expandLabel : "";
  }
  if (expandBtn) {
    expandBtn.disabled = isAnimating;
    expandBtn.setAttribute("aria-label", state.sidebarCollapsed ? "Show menu" : "Hide menu");
    expandBtn.title = state.sidebarCollapsed ? "Show menu" : "Hide menu";
    expandBtn.textContent = isAnimating ? SIDEBAR_SYMBOLS.busy : SIDEBAR_SYMBOLS.menu;
  }
}

function setSidebarCollapsed(nextValue) {
  const shell = document.getElementById("app-shell");
  const nextCollapsed = Boolean(nextValue);
  const isDesktop = window.matchMedia ? window.matchMedia("(min-width: 981px)").matches : window.innerWidth > 980;

  if (state.sidebarCollapseTimer) {
    window.clearTimeout(state.sidebarCollapseTimer);
    state.sidebarCollapseTimer = 0;
  }

  if (!shell) {
    state.sidebarCollapsed = nextCollapsed;
    safeStorageSet("tater_tateros_sidebar_collapsed", state.sidebarCollapsed ? "true" : "false");
    applySidebarState();
    return;
  }

  if (nextCollapsed && !state.sidebarCollapsed && isDesktop) {
    const collapseMs = getSidebarEffectMs("collapse");
    shell.classList.remove("sidebar-expanding");
    shell.classList.remove("sidebar-collapsed");
    shell.classList.add("sidebar-collapsing");
    applySidebarState();
    state.sidebarCollapseTimer = window.setTimeout(() => {
      shell.classList.remove("sidebar-collapsing");
      state.sidebarCollapsed = true;
      safeStorageSet("tater_tateros_sidebar_collapsed", "true");
      state.sidebarCollapseTimer = 0;
      applySidebarState();
    }, collapseMs);
    return;
  }

  if (!nextCollapsed && state.sidebarCollapsed && isDesktop) {
    const expandMs = getSidebarEffectMs("expand");
    shell.classList.remove("sidebar-collapsing");
    shell.classList.remove("sidebar-collapsed");
    shell.classList.add("sidebar-expanding");
    state.sidebarCollapsed = false;
    safeStorageSet("tater_tateros_sidebar_collapsed", "false");
    applySidebarState();
    state.sidebarCollapseTimer = window.setTimeout(() => {
      shell.classList.remove("sidebar-expanding");
      state.sidebarCollapseTimer = 0;
      applySidebarState();
    }, expandMs);
    return;
  }

  shell.classList.remove("sidebar-collapsing");
  shell.classList.remove("sidebar-expanding");
  state.sidebarCollapsed = nextCollapsed;
  safeStorageSet("tater_tateros_sidebar_collapsed", state.sidebarCollapsed ? "true" : "false");
  applySidebarState();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function showToast(message, type = "success", timeoutMs = 2600) {
  const root = document.getElementById("toast-root");
  const text = String(message || "").trim();
  if (!root || !text) {
    return;
  }

  const kind = String(type || "success").trim().toLowerCase();
  const item = document.createElement("div");
  item.className = `toast-item ${kind === "error" ? "error" : "success"}`;
  item.textContent = text;
  root.appendChild(item);

  requestAnimationFrame(() => {
    item.classList.add("show");
  });

  let closed = false;
  const closeToast = () => {
    if (closed) {
      return;
    }
    closed = true;
    item.classList.remove("show");
    item.classList.add("flame-out");
    window.setTimeout(() => {
      item.remove();
    }, getPopupEffectCloseMs());
  };

  const ttl = Math.max(1200, Number(timeoutMs) || 2600);
  const timer = window.setTimeout(closeToast, ttl);
  item.addEventListener("click", () => {
    window.clearTimeout(timer);
    closeToast();
  });
}

function waitMs(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, Math.max(0, Number(ms) || 0));
  });
}

function syncPopupBodyScrollLock() {
  const hasVisibleModal = Boolean(document.querySelector(".cerb-modal.active, .cerb-modal.closing"));
  if (document.body) {
    document.body.classList.toggle("modal-open", hasVisibleModal);
  }
}

function openPopupModal(modal) {
  if (!modal) {
    return;
  }
  const existingTimer = Number(modal.dataset.closeTimer || "0");
  if (existingTimer) {
    window.clearTimeout(existingTimer);
    modal.dataset.closeTimer = "0";
  }
  modal.classList.remove("closing");
  modal.classList.add("active");
  modal.setAttribute("aria-hidden", "false");
  syncPopupBodyScrollLock();
}

function closePopupModal(modal) {
  if (!modal) {
    return;
  }
  const existingTimer = Number(modal.dataset.closeTimer || "0");
  if (existingTimer) {
    window.clearTimeout(existingTimer);
    modal.dataset.closeTimer = "0";
  }
  if (!modal.classList.contains("active") && !modal.classList.contains("closing")) {
    modal.setAttribute("aria-hidden", "true");
    return;
  }
  modal.classList.remove("active");
  modal.classList.add("closing");
  modal.setAttribute("aria-hidden", "true");
  const timer = window.setTimeout(() => {
    modal.classList.remove("closing");
    modal.dataset.closeTimer = "0";
    syncPopupBodyScrollLock();
  }, getPopupEffectCloseMs());
  modal.dataset.closeTimer = String(timer);
  syncPopupBodyScrollLock();
}

let redisRecoveryPromptInFlight = false;
let redisRecoveryPromptLastAt = 0;
let healthRefreshTimer = 0;
let healthRefreshPromise = null;
let webuiAuthPromise = null;
let webuiAuthResolve = null;
let webuiAuthRecoveryInFlight = false;
let webuiAuthRecoveryLastAt = 0;

const REDIS_BOOTSTRAP_STATUS_TIMEOUT_MS = 900;
const REDIS_STATUS_TIMEOUT_MS = 2400;
const HEALTH_REQUEST_TIMEOUT_MS = 1200;
const HEALTH_POLL_RECOVERY_MS = 2200;
const HEALTH_POLL_CONNECTED_MS = 8000;
const REDIS_RECOVERY_PROMPT_COOLDOWN_MS = 1200;

function _setWebuiAuthStatus(raw) {
  const next = raw && typeof raw === "object" ? raw : {};
  const passwordSet = Boolean(next.password_set);
  const authenticated = Boolean(next.authenticated);
  const modeToken = String(next.mode || "").trim().toLowerCase();
  const mode = modeToken || (authenticated || !passwordSet ? "ready" : "login");
  state.auth = {
    checked: true,
    passwordSet,
    authenticated,
    mode,
    username: String(next.username || "User"),
    userAvatar: String(next.user_avatar || ""),
  };
  return state.auth;
}

function _setWebuiAuthLock(locked) {
  const isLocked = Boolean(locked);
  document.body?.classList.toggle("webui-auth-locked", isLocked);
  if (isLocked) {
    document.body?.classList.remove("modal-open");
  }
}

function _hideWebuiAuthModal() {
  const modal = document.getElementById("webui-auth-modal");
  if (!modal) {
    return;
  }
  modal.classList.remove("active");
  modal.setAttribute("aria-hidden", "true");
  _setWebuiAuthLock(false);
}

function ensureWebuiAuthModal() {
  let modal = document.getElementById("webui-auth-modal");
  if (modal) {
    return modal;
  }

  document.body.insertAdjacentHTML(
    "beforeend",
    `
      <div id="webui-auth-modal" class="webui-auth-overlay" aria-hidden="true">
        <div class="card webui-auth-card" role="dialog" aria-modal="true" aria-label="WebUI Login">
          <div id="webui-auth-avatar" class="webui-auth-avatar-wrap"></div>
          <h3 id="webui-auth-title" class="card-title">WebUI Login</h3>
          <div id="webui-auth-subtitle" class="small"></div>
          <form id="webui-auth-form" class="form-grid webui-auth-form">
            <label>Password
              <input id="webui-auth-password" type="password" autocomplete="current-password" />
            </label>
            <label id="webui-auth-confirm-row">Repeat Password
              <input id="webui-auth-confirm" type="password" autocomplete="new-password" />
            </label>
            <button type="submit" id="webui-auth-submit" class="action-btn">Unlock</button>
          </form>
          <div id="webui-auth-status" class="small"></div>
        </div>
      </div>
    `
  );

  modal = document.getElementById("webui-auth-modal");
  const form = document.getElementById("webui-auth-form");
  const submitBtn = document.getElementById("webui-auth-submit");
  const statusEl = document.getElementById("webui-auth-status");
  const passwordEl = document.getElementById("webui-auth-password");
  const confirmEl = document.getElementById("webui-auth-confirm");
  const confirmRowEl = document.getElementById("webui-auth-confirm-row");
  const titleEl = document.getElementById("webui-auth-title");
  const subtitleEl = document.getElementById("webui-auth-subtitle");
  const avatarWrapEl = document.getElementById("webui-auth-avatar");

  const setBusy = (busy) => {
    const disabled = Boolean(busy);
    if (submitBtn) {
      submitBtn.disabled = disabled;
    }
    if (passwordEl) {
      passwordEl.disabled = disabled;
    }
    if (confirmEl) {
      confirmEl.disabled = disabled;
    }
  };

  const applyState = (auth, options = {}) => {
    const profile = auth && typeof auth === "object" ? auth : state.auth || {};
    const mode = String(profile.mode || (profile.passwordSet ? "login" : "setup")).trim().toLowerCase();
    const passwordSet = Boolean(profile.passwordSet);
    const userName = String(profile.username || "User").trim() || "User";
    const avatarUrl = String(profile.userAvatar || "").trim();
    const message = String(options.message || "").trim();
    const tone = String(options.tone || "").trim().toLowerCase();
    const preserveInputs = Boolean(options.preserveInputs);

    if (titleEl) {
      titleEl.textContent = passwordSet ? "WebUI Login" : "Create WebUI Password";
    }
    if (subtitleEl) {
      subtitleEl.textContent = passwordSet
        ? `${userName}, enter your password to unlock TaterOS.`
        : `${userName}, set a password to protect this WebUI.`;
    }
    if (avatarWrapEl) {
      if (avatarUrl) {
        avatarWrapEl.innerHTML = `<img class="webui-auth-avatar-img" src="${escapeHtml(avatarUrl)}" alt="User avatar" />`;
      } else {
        avatarWrapEl.innerHTML = `<div class="webui-auth-avatar-fallback">${escapeHtml(_avatarInitial(userName, "U"))}</div>`;
      }
    }

    const setupMode = !passwordSet || mode === "setup";
    if (confirmRowEl) {
      confirmRowEl.classList.toggle("hidden", !setupMode);
    }
    if (passwordEl) {
      passwordEl.setAttribute("autocomplete", setupMode ? "new-password" : "current-password");
      if (!preserveInputs) {
        passwordEl.value = "";
      }
    }
    if (confirmEl) {
      confirmEl.setAttribute("autocomplete", setupMode ? "new-password" : "off");
      if (!preserveInputs) {
        confirmEl.value = "";
      }
    }
    if (submitBtn) {
      submitBtn.textContent = setupMode ? "Save Password" : "Login";
    }
    if (statusEl) {
      if (!message) {
        statusEl.textContent = setupMode
          ? "Create a password and repeat it to save."
          : "Enter your password to continue.";
        statusEl.classList.remove("error");
        statusEl.classList.remove("success");
      } else {
        statusEl.textContent = message;
        statusEl.classList.toggle("error", tone === "error");
        statusEl.classList.toggle("success", tone === "success");
      }
    }
  };

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const auth = state.auth || {};
    const setupMode = !Boolean(auth.passwordSet);
    const password = String(passwordEl?.value || "");
    const confirmPassword = String(confirmEl?.value || "");

    if (!password) {
      applyState(auth, { message: "Password is required.", tone: "error", preserveInputs: true });
      return;
    }

    if (setupMode) {
      if (password.length < 4) {
        applyState(auth, { message: "Password must be at least 4 characters.", tone: "error", preserveInputs: true });
        return;
      }
      if (password !== confirmPassword) {
        applyState(auth, { message: "Passwords do not match.", tone: "error", preserveInputs: true });
        return;
      }
    }

    setBusy(true);
    try {
      const route = setupMode ? "/api/auth/setup" : "/api/auth/login";
      const body = setupMode
        ? { password, confirm_password: confirmPassword }
        : { password };
      const status = await api(route, {
        method: "POST",
        body: JSON.stringify(body),
        _skipRedisRecovery: true,
        _skipAuthRecovery: true,
      });
      const next = _setWebuiAuthStatus(status);
      applyState(next, { message: "Login successful.", tone: "success" });
      if (next.authenticated) {
        _hideWebuiAuthModal();
        if (typeof webuiAuthResolve === "function") {
          webuiAuthResolve(next);
        }
        webuiAuthPromise = null;
        webuiAuthResolve = null;
      }
    } catch (error) {
      applyState(auth, {
        message: String(error?.message || "Authentication failed."),
        tone: "error",
        preserveInputs: true,
      });
    } finally {
      setBusy(false);
    }
  });

  modal._applyAuthState = applyState;
  return modal;
}

function _showWebuiAuthModal(authState, options = {}) {
  const modal = ensureWebuiAuthModal();
  if (typeof modal._applyAuthState === "function") {
    modal._applyAuthState(authState || state.auth || {}, options);
  }
  modal.classList.add("active");
  modal.setAttribute("aria-hidden", "false");
  _setWebuiAuthLock(true);
  return modal;
}

async function fetchWebuiAuthStatus() {
  const raw = await api("/api/auth/status", {
    _skipRedisRecovery: true,
    _skipAuthRecovery: true,
    _timeoutMs: 2500,
  });
  return _setWebuiAuthStatus(raw);
}

async function ensureWebuiAuth() {
  let auth = state.auth || {};
  try {
    auth = await fetchWebuiAuthStatus();
  } catch (error) {
    if (_isLikelyRedisFailureDetail(error?.message || "")) {
      await ensureRedisSetup();
      const redisError = new Error("Redis setup required.");
      redisError.code = "REDIS_SETUP_REQUIRED";
      throw redisError;
    } else {
      throw error;
    }
  }

  if (!auth.passwordSet || auth.authenticated) {
    _hideWebuiAuthModal();
    if (typeof webuiAuthResolve === "function") {
      webuiAuthResolve(auth);
    }
    webuiAuthPromise = null;
    webuiAuthResolve = null;
    return auth;
  }

  _showWebuiAuthModal(auth);
  if (!webuiAuthPromise) {
    webuiAuthPromise = new Promise((resolve) => {
      webuiAuthResolve = resolve;
    });
  }
  return webuiAuthPromise;
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
    if (!auth.passwordSet || auth.authenticated) {
      _hideWebuiAuthModal();
      if (typeof webuiAuthResolve === "function") {
        webuiAuthResolve(auth);
      }
      webuiAuthPromise = null;
      webuiAuthResolve = null;
      return;
    }
    _showWebuiAuthModal(auth, {
      message: String(reason || "Session expired. Please log in again."),
      tone: "error",
      preserveInputs: false,
    });
    if (!webuiAuthPromise) {
      webuiAuthPromise = new Promise((resolve) => {
        webuiAuthResolve = resolve;
      });
    }
  } finally {
    webuiAuthRecoveryInFlight = false;
  }
}

function _setRedisStatus(status) {
  const next = status && typeof status === "object" ? status : {};
  state.redisStatus = {
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
  };
  return state.redisStatus;
}

function _redisSetupMessage(status) {
  if (!status?.configured) {
    return "Redis is not configured yet. Enter the server details and save to continue.";
  }
  if (!status?.connected) {
    const reason = String(status?.error || "").trim();
    return reason ? `Redis is configured but unavailable: ${reason}` : "Redis is configured but currently unavailable.";
  }
  return "Redis is connected.";
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
  if (!status.connected) {
    state.notice = _redisRecoveryNotice(status?.error || "");
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
    message.includes("authentication required") ||
    message.includes("noauth") ||
    message.includes("wrongpass")
  );
}

function _redisRecoveryNotice(reason = "") {
  const detail = String(reason || "").trim();
  return detail
    ? `Redis is unavailable. Reconnect it to continue. ${detail}`
    : "Redis is unavailable. Reconnect it to continue.";
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
  const now = Date.now();
  if (!force) {
    if (redisRecoveryPromptInFlight) {
      return;
    }
    if (now - redisRecoveryPromptLastAt < REDIS_RECOVERY_PROMPT_COOLDOWN_MS) {
      return;
    }
  }
  redisRecoveryPromptInFlight = true;
  redisRecoveryPromptLastAt = now;

  try {
    let status = state.redisStatus || {};
    const fallbackError = String(reason || status?.error || "Redis is unavailable.").trim() || "Redis is unavailable.";
    status = _setRedisStatus({
      ...(status || {}),
      connected: false,
      error: fallbackError,
    });
    state.notice = _redisRecoveryNotice(fallbackError);
    _scheduleHealthRefresh(180);
  } finally {
    redisRecoveryPromptInFlight = false;
  }
}

function ensureActionProgressModal() {
  let modal = document.getElementById("action-progress-modal");
  if (modal) {
    return modal;
  }

  document.body.insertAdjacentHTML(
    "beforeend",
    `
      <div id="action-progress-modal" class="cerb-modal" aria-hidden="true">
        <div class="cerb-modal-dialog card action-progress-dialog" role="dialog" aria-modal="true" aria-label="Action Progress">
          <div class="card-head">
            <h3 id="action-progress-title" class="card-title">Working...</h3>
            <button type="button" class="inline-btn" id="action-progress-close">Close</button>
          </div>
          <div id="action-progress-detail" class="small"></div>
          <div class="action-progress-track" aria-hidden="true">
            <div id="action-progress-fill" class="action-progress-fill"></div>
          </div>
          <div id="action-progress-status" class="small action-progress-status">Starting...</div>
        </div>
      </div>
    `
  );

  modal = document.getElementById("action-progress-modal");
  const closeBtn = document.getElementById("action-progress-close");
  const closeModal = () => {
    closePopupModal(modal);
  };

  closeBtn?.addEventListener("click", () => {
    if (closeBtn.disabled) {
      return;
    }
    closeModal();
  });
  modal.addEventListener("click", (event) => {
    if (event.target === modal && !closeBtn?.disabled) {
      closeModal();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && modal.classList.contains("active") && !closeBtn?.disabled) {
      closeModal();
    }
  });
  return modal;
}

function openActionProgressModal(title, detail = "") {
  const modal = ensureActionProgressModal();
  const titleEl = document.getElementById("action-progress-title");
  const detailEl = document.getElementById("action-progress-detail");
  const fillEl = document.getElementById("action-progress-fill");
  const statusEl = document.getElementById("action-progress-status");
  const closeBtn = document.getElementById("action-progress-close");

  if (titleEl) {
    titleEl.textContent = String(title || "Working...").trim() || "Working...";
  }
  if (detailEl) {
    detailEl.textContent = String(detail || "").trim();
  }
  if (fillEl) {
    fillEl.style.width = "8%";
    fillEl.classList.remove("success", "error");
  }
  if (statusEl) {
    statusEl.textContent = "Starting...";
    statusEl.classList.remove("success", "error");
  }
  if (closeBtn) {
    closeBtn.disabled = true;
    closeBtn.textContent = "Working...";
  }
  openPopupModal(modal);
  return modal;
}

function setActionProgress(percent, statusText, tone = "running") {
  const safePercent = Math.max(0, Math.min(100, Number(percent) || 0));
  const fillEl = document.getElementById("action-progress-fill");
  const statusEl = document.getElementById("action-progress-status");
  if (fillEl) {
    fillEl.style.width = `${safePercent}%`;
    fillEl.classList.toggle("success", tone === "success");
    fillEl.classList.toggle("error", tone === "error");
  }
  if (statusEl) {
    statusEl.textContent = String(statusText || "").trim() || "Working...";
    statusEl.classList.toggle("success", tone === "success");
    statusEl.classList.toggle("error", tone === "error");
  }
}

function finishActionProgress(tone = "success", statusText = "Completed.") {
  const closeBtn = document.getElementById("action-progress-close");
  if (closeBtn) {
    closeBtn.disabled = false;
    closeBtn.textContent = "Close";
  }
  setActionProgress(100, statusText, tone);
}

function closeActionProgressModal() {
  const modal = document.getElementById("action-progress-modal");
  if (!modal) {
    return;
  }
  closePopupModal(modal);
}

async function runActionWithProgress(meta, actionFn) {
  const title = String(meta?.title || "Working...").trim() || "Working...";
  const detail = String(meta?.detail || "").trim();
  const workingText = String(meta?.workingText || "Working...").trim() || "Working...";
  const successText = String(meta?.successText || "Completed.").trim() || "Completed.";
  const errorPrefix = String(meta?.errorPrefix || "Action failed").trim() || "Action failed";

  openActionProgressModal(title, detail);
  let progress = 8;
  setActionProgress(progress, workingText, "running");

  const timer = window.setInterval(() => {
    if (progress >= 92) {
      return;
    }
    const delta = Math.max(1, Math.round((95 - progress) * 0.12));
    progress = Math.min(92, progress + delta);
    setActionProgress(progress, workingText, "running");
  }, 170);

  try {
    const result = await actionFn();
    window.clearInterval(timer);
    finishActionProgress("success", successText);
    await waitMs(520);
    closeActionProgressModal();
    return result;
  } catch (error) {
    window.clearInterval(timer);
    finishActionProgress("error", `${errorPrefix}: ${error.message}`);
    throw error;
  }
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
    try {
      const body = await response.json();
      detail = body.detail || detail;
    } catch {
      detail = response.statusText || detail;
    }
    if (!skipAuthRecovery && _shouldTriggerWebuiAuthRecovery(path, response.status)) {
      void promptWebuiAuthRecovery(detail);
    }
    if (!skipRedisRecovery && _shouldTriggerRedisRecovery(path, response.status, detail)) {
      void promptRedisSetupRecovery(detail);
    }
    throw new Error(detail);
  }

  if (response.status === 204) {
    return {};
  }

  return response.json();
}

function safeJsonParse(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("Failed to read image file."));
    reader.readAsDataURL(file);
  });
}

function boolFromAny(value, fallback = false) {
  if (typeof value === "boolean") {
    return value;
  }
  if (value === null || value === undefined) {
    return Boolean(fallback);
  }
  const text = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on", "enabled"].includes(text)) {
    return true;
  }
  if (["0", "false", "no", "n", "off", "disabled"].includes(text)) {
    return false;
  }
  return Boolean(fallback);
}

function renderNotice(text) {
  return `<div class="notice">${escapeHtml(text)}</div>`;
}

function consumeNoticeHtml() {
  const text = String(state.notice || "").trim();
  state.notice = "";
  if (!text) {
    return "";
  }
  return renderNotice(text);
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

function parseSettingValue(raw, type) {
  if (type === "number") {
    const parsed = Number(raw);
    return Number.isNaN(parsed) ? 0 : parsed;
  }
  if (type === "multiselect") {
    if (Array.isArray(raw)) {
      return raw
        .map((item) => String(item || "").trim())
        .filter(Boolean);
    }
    return [];
  }
  if (type === "checkbox") {
    return Boolean(raw);
  }
  return raw;
}

function renderToggleRow(inputHtml, statusText = "Enabled") {
  return `<div class="inline-row toggle-row">${inputHtml}<span class="small">${escapeHtml(statusText)}</span></div>`;
}

function renderSimpleDataTable(columns, rows, emptyMessage = "No rows.") {
  const cols = Array.isArray(columns) ? columns : [];
  const list = Array.isArray(rows) ? rows : [];
  if (!cols.length) {
    return `<div class="small">${escapeHtml(emptyMessage)}</div>`;
  }
  const head = cols.map((col) => `<th>${escapeHtml(col.label || col.key || "")}</th>`).join("");
  const body = list.length
    ? list
        .map((row) => {
          const cells = cols
            .map((col) => {
              const value = row && typeof row === "object" ? row[col.key] : "";
              return `<td>${escapeHtml(value ?? "")}</td>`;
            })
            .join("");
          return `<tr>${cells}</tr>`;
        })
        .join("")
    : `<tr><td colspan="${cols.length}" class="small">${escapeHtml(emptyMessage)}</td></tr>`;

  return `
    <div class="core-data-table-wrap">
      <table class="core-data-table">
        <thead><tr>${head}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

function renderSimpleBarChart(points, emptyMessage = "No chart data.") {
  const rows = Array.isArray(points) ? points : [];
  const cleaned = rows
    .map((point) => {
      if (point && typeof point === "object") {
        const label = String(point.label ?? point.key ?? point.name ?? "").trim();
        const value = Number(point.value ?? point.count ?? 0);
        return {
          label: label || "item",
          value: Number.isFinite(value) ? value : 0,
        };
      }
      return {
        label: String(point ?? "").trim() || "item",
        value: 0,
      };
    })
    .filter((row) => row.label);
  if (!cleaned.length) {
    return `<div class="small">${escapeHtml(emptyMessage)}</div>`;
  }
  const maxValue = cleaned.reduce((acc, row) => Math.max(acc, row.value), 0);
  return `
    <div class="core-bar-chart">
      ${cleaned
        .map((row) => {
          const widthPct = maxValue > 0 ? Math.max(0, Math.min(100, (row.value / maxValue) * 100)) : 0;
          return `
            <div class="core-bar-row">
              <div class="core-bar-label">${escapeHtml(row.label)}</div>
              <div class="core-bar-track"><div class="core-bar-fill" style="width:${widthPct}%;"></div></div>
              <div class="core-bar-value">${escapeHtml(String(row.value))}</div>
            </div>
          `;
        })
        .join("")}
    </div>
  `;
}

function hydraPlatformLabel(platform) {
  const token = String(platform || "").trim().toLowerCase();
  const labels = {
    all: "All",
    webui: "WebUI",
    homeassistant: "Home Assistant",
    homekit: "HomeKit",
    xbmc: "XBMC",
    automation: "Automations",
  };
  return labels[token] || token || "unknown";
}

function runtimeSettingKeySupportsGenerator(field) {
  const key = String(field?.key || "").trim().toUpperCase();
  if (!key) {
    return false;
  }
  return key === "API_AUTH_KEY" || key === "AUTH_TOKEN";
}

function generateRuntimeApiKey() {
  const bytes = new Uint8Array(24);
  const cryptoObj = window?.crypto;
  if (cryptoObj && typeof cryptoObj.getRandomValues === "function") {
    cryptoObj.getRandomValues(bytes);
  } else {
    for (let i = 0; i < bytes.length; i += 1) {
      bytes[i] = Math.floor(Math.random() * 256);
    }
  }
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
}

async function copyTextToClipboard(value) {
  const source = String(value || "");
  if (!source) {
    return false;
  }
  if (navigator?.clipboard && typeof navigator.clipboard.writeText === "function") {
    try {
      await navigator.clipboard.writeText(source);
      return true;
    } catch {
      // Fall through to legacy copy path.
    }
  }
  try {
    const temp = document.createElement("textarea");
    temp.value = source;
    temp.setAttribute("readonly", "readonly");
    temp.style.position = "fixed";
    temp.style.opacity = "0";
    temp.style.pointerEvents = "none";
    document.body.appendChild(temp);
    temp.focus();
    temp.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(temp);
    return Boolean(ok);
  } catch {
    return false;
  }
}

function buildSettingInput(field, inputId) {
  const safeLabel = escapeHtml(field.label || field.key);
  const safeKey = escapeHtml(field.key || "");
  const safeDesc = field.description ? `<div class="small">${escapeHtml(field.description)}</div>` : "";
  const type = String(field.type || "text").toLowerCase();

  if (type === "select") {
    const options = Array.isArray(field.options) ? field.options : [];
    const optionRows = options
      .map((optRaw) => {
        if (typeof optRaw === "object" && optRaw !== null) {
          const value = String(optRaw.value ?? optRaw.id ?? optRaw.key ?? optRaw.label ?? "");
          const label = String(optRaw.label ?? value);
          const selected = String(field.value ?? "") === value ? "selected" : "";
          return `<option value="${escapeHtml(value)}" ${selected}>${escapeHtml(label)}</option>`;
        }
        const value = String(optRaw ?? "");
        const selected = String(field.value ?? "") === value ? "selected" : "";
        return `<option value="${escapeHtml(value)}" ${selected}>${escapeHtml(value)}</option>`;
      })
      .join("");

    return `<label>${safeLabel}<select id="${inputId}" data-setting-type="select" data-setting-key="${safeKey}">${optionRows}</select>${safeDesc}</label>`;
  }

  if (type === "multiselect") {
    const options = Array.isArray(field.options) ? field.options : [];
    const selectedValues = (() => {
      if (Array.isArray(field.value)) {
        return new Set(field.value.map((item) => String(item ?? "")));
      }
      if (typeof field.value === "string") {
        const text = field.value.trim();
        if (text.startsWith("[") && text.endsWith("]")) {
          const parsed = safeJsonParse(text);
          if (Array.isArray(parsed)) {
            return new Set(parsed.map((item) => String(item ?? "")));
          }
        }
        if (text) {
          return new Set(
            text
              .split(",")
              .map((item) => item.trim())
              .filter(Boolean)
          );
        }
      }
      return new Set();
    })();

    const size = Math.max(4, Math.min(14, options.length || 8));
    const optionRows = options
      .map((optRaw) => {
        if (typeof optRaw === "object" && optRaw !== null) {
          const value = String(optRaw.value ?? optRaw.id ?? optRaw.key ?? optRaw.label ?? "");
          const label = String(optRaw.label ?? value);
          const selected = selectedValues.has(value) ? "selected" : "";
          return `<option value="${escapeHtml(value)}" ${selected}>${escapeHtml(label)}</option>`;
        }
        const value = String(optRaw ?? "");
        const selected = selectedValues.has(value) ? "selected" : "";
        return `<option value="${escapeHtml(value)}" ${selected}>${escapeHtml(value)}</option>`;
      })
      .join("");

    return `<label>${safeLabel}<select id="${inputId}" class="settings-multiselect" multiple size="${size}" data-setting-type="multiselect" data-setting-key="${safeKey}">${optionRows}</select>${safeDesc}</label>`;
  }

  if (type === "checkbox") {
    const checked = boolFromAny(field?.value, false) ? "checked" : "";
    const toggleInput = `<input id="${inputId}" class="toggle-input" type="checkbox" data-setting-type="checkbox" data-setting-key="${safeKey}" ${checked} />`;
    return `<label>${safeLabel}${renderToggleRow(toggleInput)}${safeDesc}</label>`;
  }

  if (type === "textarea" || type === "multiline") {
    return `<label>${safeLabel}<textarea id="${inputId}" data-setting-type="textarea" data-setting-key="${safeKey}">${escapeHtml(field.value ?? "")}</textarea>${safeDesc}</label>`;
  }

  const htmlType = type === "password" ? "password" : type === "number" ? "number" : "text";
  const numberAttrs =
    type === "number"
      ? ` step="${escapeHtml(field?.step ?? "any")}"${
          field?.min !== undefined ? ` min="${escapeHtml(field.min)}"` : ""
        }${field?.max !== undefined ? ` max="${escapeHtml(field.max)}"` : ""}`
      : "";
  const inputHtml = `<input id="${inputId}" type="${htmlType}"${numberAttrs} value="${escapeHtml(
    field.value ?? ""
  )}" data-setting-type="${escapeHtml(type)}" data-setting-key="${safeKey}" />`;
  if (runtimeSettingKeySupportsGenerator(field) && (htmlType === "password" || htmlType === "text")) {
    return `<label>${safeLabel}<div class="runtime-setting-input-action">${inputHtml}<button type="button" class="inline-btn runtime-copy-key-btn" data-target-input="${escapeHtml(
      inputId
    )}">Copy Key</button><button type="button" class="inline-btn runtime-generate-key-btn" data-target-input="${escapeHtml(
      inputId
    )}">Generate Key</button></div>${safeDesc}</label>`;
  }
  return `<label>${safeLabel}${inputHtml}${safeDesc}</label>`;
}

function getInputValue(input) {
  const type = input.dataset.settingType || input.type;
  if (type === "multiselect") {
    return Array.from(input.selectedOptions || [])
      .map((option) => String(option?.value || "").trim())
      .filter(Boolean);
  }
  if (type === "checkbox") {
    return input.checked;
  }
  return input.value;
}

function collectFormValues(formElement) {
  const values = {};
  formElement.querySelectorAll("[data-setting-key]").forEach((input) => {
    const key = String(input.dataset.settingKey || "").trim();
    if (!key) {
      return;
    }
    values[key] = parseSettingValue(getInputValue(input), input.dataset.settingType || input.type);
  });
  return values;
}

function _normalizeRuntimeSettingsKind(kind) {
  const token = String(kind || "").trim().toLowerCase();
  if (token === "verbas" || token === "portals" || token === "cores") {
    return token;
  }
  return "";
}

function resetRuntimeSettingsCatalog(kind) {
  const token = _normalizeRuntimeSettingsKind(kind);
  if (!token) {
    return;
  }
  state.runtimeSettingsCatalog[token] = {};
}

function registerRuntimeSettings(kind, key, payload) {
  const token = _normalizeRuntimeSettingsKind(kind);
  const entryKey = String(key || "").trim();
  if (!token || !entryKey || !payload || typeof payload !== "object") {
    return;
  }
  state.runtimeSettingsCatalog[token][entryKey] = {
    key: entryKey,
    label: String(payload.label || entryKey).trim() || entryKey,
    kind: token,
    endpoint: String(payload.endpoint || "").trim(),
    settings: Array.isArray(payload.settings) ? payload.settings : [],
  };
}

function getRuntimeSettingsEntry(kind, key) {
  const token = _normalizeRuntimeSettingsKind(kind);
  const entryKey = String(key || "").trim();
  if (!token || !entryKey) {
    return null;
  }
  const group = state.runtimeSettingsCatalog[token];
  if (!group || typeof group !== "object") {
    return null;
  }
  return group[entryKey] || null;
}

function formatRuntimeSettingsTitle(label) {
  const base = String(label || "").trim() || "Settings";
  return /settings$/i.test(base) ? base : `${base} Settings`;
}

function ensureRuntimeSettingsModal() {
  let modal = document.getElementById("runtime-settings-modal");
  if (modal) {
    return modal;
  }

  document.body.insertAdjacentHTML(
    "beforeend",
    `
      <div id="runtime-settings-modal" class="cerb-modal" aria-hidden="true">
        <div class="cerb-modal-dialog card runtime-settings-dialog" role="dialog" aria-modal="true" aria-label="Runtime Settings">
          <div class="card-head">
            <h3 id="runtime-settings-title" class="card-title">Settings</h3>
            <div class="inline-row">
              <button type="button" class="inline-btn" id="runtime-settings-close">Close</button>
            </div>
          </div>
          <div id="runtime-settings-meta" class="small"></div>
          <form id="runtime-settings-form" class="form-grid runtime-settings-form">
            <div id="runtime-settings-fields" class="form-grid runtime-settings-fields"></div>
            <div class="inline-row">
              <button type="submit" id="runtime-settings-save" class="action-btn">Save Settings</button>
            </div>
          </form>
          <div id="runtime-settings-status" class="small"></div>
        </div>
      </div>
    `
  );

  modal = document.getElementById("runtime-settings-modal");
  const form = document.getElementById("runtime-settings-form");
  const closeBtn = document.getElementById("runtime-settings-close");
  const saveBtn = document.getElementById("runtime-settings-save");
  const statusEl = document.getElementById("runtime-settings-status");

  const closeModal = () => {
    if (typeof state.runtimeSettingsCloseHandler === "function") {
      try {
        state.runtimeSettingsCloseHandler({ modal, form, statusEl, saveBtn, closeBtn });
      } catch (_error) {
        // Ignore modal cleanup errors.
      }
    }
    state.runtimeSettingsSaveHandler = null;
    state.runtimeSettingsOpenHandler = null;
    state.runtimeSettingsCloseHandler = null;
    closePopupModal(modal);
  };

  closeBtn?.addEventListener("click", closeModal);
  modal.addEventListener("click", (event) => {
    if (event.target === modal) {
      closeModal();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && modal.classList.contains("active")) {
      closeModal();
    }
  });
  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (typeof state.runtimeSettingsSaveHandler !== "function") {
      return;
    }
    const values = collectFormValues(form);
    if (statusEl) {
      statusEl.textContent = "Saving...";
    }
    if (saveBtn) {
      saveBtn.disabled = true;
    }
    if (closeBtn) {
      closeBtn.disabled = true;
    }
    try {
      const result = await state.runtimeSettingsSaveHandler(values);
      const successText = String(result?.message || "Settings saved.").trim() || "Settings saved.";
      if (statusEl) {
        statusEl.textContent = successText;
      }
      showToast(successText);
      closeModal();
    } catch (error) {
      const msg = String(error?.message || "unknown error");
      if (statusEl) {
        statusEl.textContent = `Save failed: ${msg}`;
      }
      showToast(`Save failed: ${msg}`, "error", 3600);
    } finally {
      if (saveBtn) {
        saveBtn.disabled = false;
      }
      if (closeBtn) {
        closeBtn.disabled = false;
      }
    }
  });
  return modal;
}

function openRuntimeSettingsModal({ title, meta, fields, onSave, onOpen, onClose }) {
  const modal = ensureRuntimeSettingsModal();
  const titleEl = document.getElementById("runtime-settings-title");
  const metaEl = document.getElementById("runtime-settings-meta");
  const fieldsEl = document.getElementById("runtime-settings-fields");
  const statusEl = document.getElementById("runtime-settings-status");
  const saveBtn = document.getElementById("runtime-settings-save");
  const normalizedFields = Array.isArray(fields) ? fields : [];

  if (typeof state.runtimeSettingsCloseHandler === "function") {
    try {
      state.runtimeSettingsCloseHandler({ modal, fieldsEl, statusEl, saveBtn });
    } catch (_error) {
      // Ignore cleanup errors while swapping modal content.
    }
  }
  state.runtimeSettingsSaveHandler = typeof onSave === "function" ? onSave : null;
  state.runtimeSettingsOpenHandler = typeof onOpen === "function" ? onOpen : null;
  state.runtimeSettingsCloseHandler = typeof onClose === "function" ? onClose : null;

  if (titleEl) {
    titleEl.textContent = String(title || "Settings").trim() || "Settings";
  }
  if (metaEl) {
    metaEl.textContent = String(meta || "").trim();
  }
  if (statusEl) {
    statusEl.textContent = "";
  }
  if (saveBtn) {
    saveBtn.disabled = false;
    saveBtn.style.display = typeof state.runtimeSettingsSaveHandler === "function" ? "" : "none";
  }

  if (fieldsEl) {
    if (!normalizedFields.length) {
      fieldsEl.innerHTML = `<div class="small">No configurable settings.</div>`;
    } else {
      fieldsEl.innerHTML = normalizedFields
        .map((field, idx) => {
          const inputId = `runtime_settings_${idx}_${String(field?.key || "").replace(/[^a-zA-Z0-9_]/g, "_")}`;
          return buildSettingInput(field, inputId);
        })
        .join("");
      fieldsEl.querySelectorAll(".runtime-generate-key-btn").forEach((button) => {
        button.addEventListener("click", () => {
          const targetId = String(button.getAttribute("data-target-input") || "").trim();
          if (!targetId) {
            return;
          }
          const input = document.getElementById(targetId);
          if (!input || typeof input.value !== "string") {
            return;
          }
          input.value = generateRuntimeApiKey();
          input.dispatchEvent(new Event("input", { bubbles: true }));
          input.dispatchEvent(new Event("change", { bubbles: true }));
        });
      });
      fieldsEl.querySelectorAll(".runtime-copy-key-btn").forEach((button) => {
        button.addEventListener("click", async () => {
          const targetId = String(button.getAttribute("data-target-input") || "").trim();
          if (!targetId) {
            return;
          }
          const input = document.getElementById(targetId);
          const source = String(input?.value || "");
          if (!source) {
            showToast("No key to copy.", "error", 2000);
            return;
          }
          const original = String(button.textContent || "Copy Key");
          button.disabled = true;
          try {
            const copied = await copyTextToClipboard(source);
            if (!copied) {
              throw new Error("clipboard unavailable");
            }
            button.textContent = "Copied";
            showToast("API key copied.");
          } catch (error) {
            button.textContent = "Copy failed";
            showToast(`Copy failed: ${String(error?.message || "clipboard unavailable")}`, "error", 2600);
          } finally {
            window.setTimeout(() => {
              button.textContent = original;
              button.disabled = false;
            }, 1200);
          }
        });
      });
    }
  }

  openPopupModal(modal);
  if (typeof state.runtimeSettingsOpenHandler === "function") {
    try {
      state.runtimeSettingsOpenHandler({ modal, fieldsEl, statusEl, saveBtn, titleEl, metaEl });
    } catch (_error) {
      // Ignore modal setup errors.
    }
  }
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

function syncChatCopy() {
  if (state.view === "chat") {
    updateHeader();
  }
  const input = document.getElementById("chat-input");
  if (input) {
    input.placeholder = `Message ${getTaterFullName()}...`;
  }
}

function updateHeader() {
  const meta = VIEW_META[state.view];
  const subtitle = state.view === "chat" ? `Talk to ${getTaterFullName()}` : meta.subtitle;
  document.getElementById("view-title").textContent = meta.title;
  document.getElementById("view-subtitle").textContent = subtitle;
}

function applyBranding(firstNameRaw) {
  const firstName = String(firstNameRaw || "").trim() || "Tater";
  const brandNameEl = document.getElementById("brand-name");
  const brandSubtitleEl = document.getElementById("brand-subtitle");
  if (brandNameEl) {
    brandNameEl.textContent = firstName;
  }
  if (brandSubtitleEl) {
    brandSubtitleEl.textContent = `${firstName}OS Control Surface`;
  }
}

async function refreshBranding() {
  try {
    const profile = await api("/api/chat/profile");
    const firstName = String(profile.tater_first_name || profile.tater_name || "").trim() || "Tater";
    const lastName = String(profile.tater_last_name || "").trim() || "Totterson";
    const fullName = String(profile.tater_full_name || "").trim() || _composeName(firstName, lastName, "Tater Totterson");

    state.chatProfile.taterName = firstName;
    state.chatProfile.taterFirstName = firstName;
    state.chatProfile.taterLastName = lastName;
    state.chatProfile.taterFullName = fullName;
    state.chatProfile.attachMaxMbEach = Number(profile.attach_max_mb_each ?? state.chatProfile.attachMaxMbEach ?? 0);
    state.chatProfile.attachMaxMbTotal = Number(profile.attach_max_mb_total ?? state.chatProfile.attachMaxMbTotal ?? 0);
    if (profile && Object.prototype.hasOwnProperty.call(profile, "popup_effect_style")) {
      applyPopupEffectStyle(profile.popup_effect_style);
    }

    applyBranding(firstName);
    syncChatCopy();
  } catch {
    applyBranding(getTaterFirstName());
    syncChatCopy();
  }
}

function persistCoreTopTab(tabName) {
  const normalized = String(tabName || "manage").trim() || "manage";
  state.coreTopTab = normalized;
  safeStorageSet("tater_tateros_core_tab", normalized);
}

function setActiveNav(viewName) {
  document.querySelectorAll(".nav-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.view === viewName);
  });
}

function formatRuntimeSummary(health) {
  const verbasEnabled = Number(health?.verbas_enabled ?? 0);
  const portalsRunning = Number(health?.portals_running ?? 0);
  const coresRunning = Number(health?.cores_running ?? 0);
  const hydraJobsActive = Number(health?.hydra_jobs_active ?? health?.chat_jobs_active ?? 0);
  const llmCallsActive = Number(health?.llm_calls_active ?? 0);
  const visionCallsActive = Number(health?.vision_calls_active ?? health?.voice_calls_active ?? 0);
  return `${verbasEnabled} verba enabled • ${portalsRunning} portals running • ${coresRunning} cores running • ${hydraJobsActive} hydra jobs • ${llmCallsActive} llm calls • ${visionCallsActive} vision calls`;
}

function setRuntimeSummaryText(text, tone = "normal") {
  const summary = document.getElementById("runtime-summary");
  if (!summary) {
    return;
  }
  summary.textContent = String(text || "").trim();
  summary.classList.toggle("degraded", tone === "degraded");
  summary.classList.toggle("offline", tone === "offline");
}

function _runtimeAgeLabel(secondsRaw) {
  const seconds = Math.max(0, Number(secondsRaw) || 0);
  if (seconds < 60) {
    return `${Math.round(seconds)}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remSeconds = Math.floor(seconds % 60);
  if (minutes < 60) {
    return `${minutes}m ${remSeconds}s`;
  }
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  return `${hours}h ${remMinutes}m`;
}

function _runtimeStartedLabel(epochSecondsRaw) {
  const epoch = Number(epochSecondsRaw);
  if (!Number.isFinite(epoch) || epoch <= 0) {
    return "";
  }
  try {
    return new Date(epoch * 1000).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  } catch {
    return "";
  }
}

function _runtimeHistoryWindowKey(valueRaw, fallback = "24h") {
  const value = String(valueRaw || "").trim().toLowerCase();
  if (value === "24h" || value === "7d" || value === "30d") {
    return value;
  }
  return String(fallback || "").trim().toLowerCase();
}

function _runtimeHistoryWindowLabel(row) {
  const key = _runtimeHistoryWindowKey(row?.key, "");
  if (key) {
    return key;
  }
  return String(row?.label || "window").trim() || "window";
}

function _renderRuntimeHistoryWindowTabs(historyWindows, selectedKey, ariaLabel = "History window") {
  const rows = Array.isArray(historyWindows) ? historyWindows.filter((row) => row && typeof row === "object") : [];
  if (rows.length <= 1) {
    return "";
  }
  return `
    <div class="runtime-history-tabs" role="tablist" aria-label="${escapeHtml(ariaLabel)}">
      ${rows
        .map((row) => {
          const key = _runtimeHistoryWindowKey(row?.key, "");
          if (!key) {
            return "";
          }
          const active = key === _runtimeHistoryWindowKey(selectedKey, "24h");
          return `<button type="button" class="runtime-history-tab-btn${active ? " active" : ""}" data-runtime-history-window="${escapeHtml(
            key
          )}">${escapeHtml(_runtimeHistoryWindowLabel(row))}</button>`;
        })
        .join("")}
    </div>
  `;
}

function _renderRuntimeHydraJobRows(hydraJobs) {
  const activeTurns = Array.isArray(hydraJobs?.active_turns) ? hydraJobs.active_turns : [];

  const activeTurnsHtml = activeTurns.length
    ? `
        <div class="runtime-breakdown-list runtime-hydra-turn-list">
          ${activeTurns
            .map((row) => {
              const turnId = String(row?.id || "").trim();
              const shortId = turnId ? turnId.slice(0, 8) : "";
              const platformLabel = String(row?.platform_label || row?.platform || "Unknown");
              const taskName = String(row?.task_name || "").trim() || "Hydra task";
              const currentTool = String(row?.current_tool || "").trim();
              const source = String(row?.source || "").trim();
              const scope = String(row?.scope || "").trim();
              const age = _runtimeAgeLabel(row?.age_seconds);
              const started = _runtimeStartedLabel(row?.started_at);
              const metaParts = [platformLabel, source, shortId ? `Drop ${shortId}` : "", started ? `Started ${started}` : ""].filter(Boolean);
              return `
                <div class="runtime-hydra-turn-card">
                  <div class="runtime-hydra-turn-head">
                    <div class="runtime-hydra-turn-name">${escapeHtml(taskName)}</div>
                    <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(`Running ${age}`)}</span></div>
                  </div>
                  <div class="runtime-hydra-turn-meta">
                    ${metaParts.map((part) => `<span class="runtime-hydra-turn-pill">${escapeHtml(part)}</span>`).join("")}
                  </div>
                  ${currentTool ? `<div class="small muted runtime-hydra-turn-scope">Current verba/tool: ${escapeHtml(currentTool)}</div>` : ""}
                  ${scope ? `<div class="small muted runtime-hydra-turn-scope">Scope: ${escapeHtml(scope)}</div>` : ""}
                </div>
              `;
            })
            .join("")}
        </div>
      `
    : `<div class="small muted">No active Hydra turns right now.</div>`;

  return `
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle">Active Turns</div>
      ${activeTurnsHtml}
    </div>
  `;
}

function _renderRuntimeLlmCallRows(llmCalls) {
  const activeCalls = Array.isArray(llmCalls?.active_calls) ? llmCalls.active_calls : [];

  const activeCallsHtml = activeCalls.length
    ? `
        <div class="runtime-breakdown-list">
          ${activeCalls
            .map((row) => {
              const sourceLabel = String(row?.source_label || row?.label || "Unknown source");
              const model = String(row?.model || "model");
              const host = String(row?.host || "").trim();
              const functionName = String(row?.function || "").trim();
              const activity = String(row?.activity || "").trim();
              const messageCount = Number(row?.message_count ?? 0);
              const detailLineParts = [`Model ${model}`];
              if (host) {
                detailLineParts.push(host);
              }
              const extraLineParts = [];
              if (activity) {
                extraLineParts.push(`Activity ${activity}`);
              } else if (functionName) {
                extraLineParts.push(`Fn ${functionName}`);
              }
              if (messageCount > 0) {
                extraLineParts.push(`${messageCount} msgs`);
              }
              const age = _runtimeAgeLabel(row?.age_seconds);
              return `
                <div class="runtime-breakdown-row">
                  <div class="runtime-breakdown-main">
                    <div class="runtime-breakdown-name">${escapeHtml(sourceLabel)}</div>
                    <div class="small muted">${escapeHtml(detailLineParts.join(" • "))}</div>
                    ${extraLineParts.length ? `<div class="small muted">${escapeHtml(extraLineParts.join(" • "))}</div>` : ""}
                  </div>
                  <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(age)}</span></div>
                </div>
              `;
            })
            .join("")}
        </div>
      `
    : `<div class="small muted">No active LLM calls right now.</div>`;

  return `
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle">Active Calls</div>
      ${activeCallsHtml}
    </div>
  `;
}

function _renderRuntimeVisionCallRows(visionCalls) {
  const activeCalls = Array.isArray(visionCalls?.active_calls) ? visionCalls.active_calls : [];

  const activeCallsHtml = activeCalls.length
    ? `
        <div class="runtime-breakdown-list">
          ${activeCalls
            .map((row) => {
              const sourceLabel = String(row?.source_label || row?.label || "Unknown source");
              const model = String(row?.model || "model");
              const apiBase = String(row?.api_base || "").trim();
              const functionName = String(row?.function || "").trim();
              const detailLineParts = [`Model ${model}`];
              if (apiBase) {
                detailLineParts.push(apiBase);
              }
              const extraLineParts = [];
              if (functionName) {
                extraLineParts.push(`Fn ${functionName}`);
              }
              const age = _runtimeAgeLabel(row?.age_seconds);
              return `
                <div class="runtime-breakdown-row">
                  <div class="runtime-breakdown-main">
                    <div class="runtime-breakdown-name">${escapeHtml(sourceLabel)}</div>
                    <div class="small muted">${escapeHtml(detailLineParts.join(" • "))}</div>
                    ${extraLineParts.length ? `<div class="small muted">${escapeHtml(extraLineParts.join(" • "))}</div>` : ""}
                  </div>
                  <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(age)}</span></div>
                </div>
              `;
            })
            .join("")}
        </div>
      `
    : `<div class="small muted">No active vision calls right now.</div>`;

  return `
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle">Active Calls</div>
      ${activeCallsHtml}
    </div>
  `;
}

function _runtimeInt(value, fallback = 0) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return Number(fallback || 0);
  }
  return Math.max(0, Math.round(parsed));
}

function _runtimeFmtInt(value) {
  return _runtimeInt(value, 0).toLocaleString();
}

function _renderRuntimeContextWindowCard(estimate) {
  const payload = estimate && typeof estimate === "object" ? estimate : {};
  const error = String(payload?.error || "").trim();
  if (error) {
    return `
      <section class="runtime-breakdown-card runtime-breakdown-card-wide">
        <div class="runtime-breakdown-head">
          <h4 class="runtime-breakdown-title">Estimated Chat Context Window</h4>
          <div class="small muted">Estimator unavailable</div>
        </div>
        <div class="runtime-breakdown-block">
          <div class="small muted">${escapeHtml(error)}</div>
        </div>
      </section>
    `;
  }

  const promptTokens = _runtimeInt(payload?.prompt_tokens);
  const completionBudget = _runtimeInt(payload?.completion_budget_tokens);
  const minimumWindow = _runtimeInt(payload?.minimum_context_window);
  const recommendedWindow = _runtimeInt(payload?.recommended_context_window);
  const historyMessages = _runtimeInt(payload?.history_messages);
  const maxHistoryMessages = _runtimeInt(payload?.max_history_messages);
  const enabledVerbas = _runtimeInt(payload?.enabled_verbas);
  const connectedPortals = _runtimeInt(payload?.connected_portals);
  const runningCores = _runtimeInt(payload?.running_cores);
  const breakdown = payload?.breakdown && typeof payload.breakdown === "object" ? payload.breakdown : {};
  const systemTokens = _runtimeInt(breakdown?.system_tokens);
  const statusTokens = _runtimeInt(breakdown?.status_tokens);
  const coreTokens = _runtimeInt(breakdown?.core_context_tokens);
  const preambleTokens = _runtimeInt(breakdown?.platform_preamble_tokens);
  const historyTokens = _runtimeInt(breakdown?.history_tokens);
  const userTokens = _runtimeInt(breakdown?.user_tokens);
  const capabilityReserveTokens = _runtimeInt(payload?.capability_context_reserve_tokens ?? breakdown?.capability_reserve_tokens);
  const burstReserveTokens = _runtimeInt(payload?.burst_context_reserve_tokens ?? breakdown?.burst_reserve_tokens);
  const highContextVerbas = _runtimeInt(breakdown?.high_context_verbas);
  const heavyCores = _runtimeInt(breakdown?.heavy_cores);
  const reserveExamples = Array.isArray(breakdown?.high_context_verba_examples)
    ? breakdown.high_context_verba_examples.map((item) => String(item || "").trim()).filter(Boolean).slice(0, 4)
    : [];

  if (promptTokens <= 0 && minimumWindow <= 0 && recommendedWindow <= 0) {
    return `
      <section class="runtime-breakdown-card runtime-breakdown-card-wide">
        <div class="runtime-breakdown-head">
          <h4 class="runtime-breakdown-title">Estimated Chat Context Window</h4>
          <div class="small muted">No estimate available yet</div>
        </div>
        <div class="runtime-breakdown-block">
          <div class="small muted">Send a chat message so Hydra can sample the active chat prompt stack.</div>
        </div>
      </section>
    `;
  }

  const summaryParts = [
    `Prompt ${_runtimeFmtInt(promptTokens)} tok`,
    `Reply budget ${_runtimeFmtInt(completionBudget)} tok`,
    capabilityReserveTokens > 0 ? `Capability reserve ${_runtimeFmtInt(capabilityReserveTokens)} tok` : "",
    burstReserveTokens > 0 ? `Burst reserve ${_runtimeFmtInt(burstReserveTokens)} tok` : "",
    `Min window ${_runtimeFmtInt(minimumWindow)}`,
    `Recommended ${_runtimeFmtInt(recommendedWindow)}`,
  ].filter(Boolean);
  const historyLine = `${historyMessages}/${maxHistoryMessages || historyMessages} msgs`;
  const promptCompositionRows = [
    { label: "System prompt", tokens: systemTokens },
    { label: "Runtime status", tokens: statusTokens },
    { label: "Core context + preamble", tokens: coreTokens + preambleTokens },
    { label: `Chat history (${historyLine})`, tokens: historyTokens },
    { label: "Current user turn", tokens: userTokens },
  ];
  if (capabilityReserveTokens > 0) {
    promptCompositionRows.push({ label: "Capability reserve", tokens: capabilityReserveTokens });
  }
  const promptCompositionHtml = promptCompositionRows
    .map(
      (row) => `
          <div class="runtime-breakdown-row runtime-breakdown-row-dense">
            <div class="runtime-breakdown-main">
              <div class="runtime-breakdown-name">${escapeHtml(String(row.label || ""))}</div>
            </div>
            <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(
              _runtimeFmtInt(row.tokens)
            )}</span></div>
          </div>
        `
    )
    .join("");

  return `
    <section class="runtime-breakdown-card runtime-breakdown-card-wide">
      <div class="runtime-breakdown-head">
        <h4 class="runtime-breakdown-title">Estimated Chat Context Window</h4>
        <div class="small muted">${escapeHtml(summaryParts.join(" • "))}</div>
      </div>
      <div class="runtime-breakdown-block">
        <div class="runtime-breakdown-subtitle">Prompt Composition</div>
        <div class="runtime-breakdown-list runtime-breakdown-list-static runtime-breakdown-list-dense">
          ${promptCompositionHtml}
        </div>
      </div>
      <div class="runtime-breakdown-block">
        <div class="small muted">
          Active stack: ${escapeHtml(`${enabledVerbas} verbas enabled • ${connectedPortals} portals connected • ${runningCores} cores running`)}
        </div>
        ${
          burstReserveTokens > 0
            ? `<div class="small muted">Recommended window includes burst reserve: ${escapeHtml(_runtimeFmtInt(burstReserveTokens))} tok for heavy/multi-tool turns.</div>`
            : ""
        }
        ${
          highContextVerbas > 0 || heavyCores > 0
            ? `<div class="small muted">High-context signals: ${escapeHtml(`${highContextVerbas} high-context verbas • ${heavyCores} heavy cores`)}${reserveExamples.length ? ` • e.g. ${escapeHtml(reserveExamples.join(", "))}` : ""}</div>`
            : ""
        }
      </div>
    </section>
  `;
}

function renderRuntimeBreakdown(payload) {
  const hydraJobs =
    payload?.hydra_jobs && typeof payload.hydra_jobs === "object"
      ? payload.hydra_jobs
      : payload?.chat_jobs && typeof payload.chat_jobs === "object"
        ? payload.chat_jobs
        : {};
  const llmCalls = payload?.llm_calls && typeof payload.llm_calls === "object" ? payload.llm_calls : {};
  const visionCalls =
    payload?.vision_calls && typeof payload.vision_calls === "object"
      ? payload.vision_calls
      : payload?.voice_calls && typeof payload.voice_calls === "object"
        ? payload.voice_calls
        : {};
  const contextEstimate = payload?.chat_context_window && typeof payload.chat_context_window === "object"
    ? payload.chat_context_window
    : {};
  const activeTurnCount = Array.isArray(hydraJobs?.active_turns) ? hydraJobs.active_turns.length : Number(hydraJobs.surface_running_turns ?? 0);
  const hydraSummary = `${Number(hydraJobs.total ?? 0)} total • Active turns ${activeTurnCount} • WebUI queue ${Number(
    hydraJobs.webui_jobs ?? 0
  )} • Surface turns ${Number(hydraJobs.surface_running_turns ?? 0)}`;
  const llmSummary = `${Number(llmCalls.active_total ?? 0)} active • Started ${Number(
    llmCalls?.totals?.started ?? 0
  )} • Completed ${Number(llmCalls?.totals?.completed ?? 0)} • Failed ${Number(llmCalls?.totals?.failed ?? 0)}`;
  const visionSummary = `${Number(visionCalls.active_total ?? 0)} active • Started ${Number(
    visionCalls?.totals?.started ?? 0
  )} • Completed ${Number(visionCalls?.totals?.completed ?? 0)} • Failed ${Number(visionCalls?.totals?.failed ?? 0)}`;
  return `
    <div class="runtime-breakdown-grid">
      <section class="runtime-breakdown-card runtime-breakdown-card-wide">
        <div class="runtime-breakdown-head">
          <h4 class="runtime-breakdown-title">Hydra Jobs</h4>
          <div class="small muted">${escapeHtml(hydraSummary)}</div>
        </div>
        ${_renderRuntimeHydraJobRows(hydraJobs)}
      </section>
      <section class="runtime-breakdown-card">
        <div class="runtime-breakdown-head">
          <h4 class="runtime-breakdown-title">LLM Calls</h4>
          <div class="small muted">${escapeHtml(llmSummary)}</div>
        </div>
        ${_renderRuntimeLlmCallRows(llmCalls)}
      </section>
      <section class="runtime-breakdown-card">
        <div class="runtime-breakdown-head">
          <h4 class="runtime-breakdown-title">Vision Calls</h4>
          <div class="small muted">${escapeHtml(visionSummary)}</div>
        </div>
        ${_renderRuntimeVisionCallRows(visionCalls)}
      </section>
      ${_renderRuntimeContextWindowCard(contextEstimate)}
    </div>
  `;
}

function ensureRuntimeBreakdownModal() {
  let modal = document.getElementById("runtime-breakdown-modal");
  if (modal) {
    return modal;
  }

  document.body.insertAdjacentHTML(
    "beforeend",
    `
      <div id="runtime-breakdown-modal" class="cerb-modal" aria-hidden="true">
        <div class="cerb-modal-dialog card runtime-breakdown-dialog" role="dialog" aria-modal="true" aria-label="Hydra Jobs, LLM Calls, and Vision Calls">
          <div class="card-head">
            <h3 class="card-title">Live Hydra Jobs + LLM Calls + Vision Calls</h3>
            <div class="inline-row">
              <span id="runtime-breakdown-updated" class="small"></span>
              <button type="button" class="inline-btn" id="runtime-breakdown-refresh">Refresh</button>
              <button type="button" class="inline-btn" id="runtime-breakdown-close">Close</button>
            </div>
          </div>
          <div id="runtime-breakdown-status" class="small"></div>
          <div id="runtime-breakdown-content" class="cerb-modal-body runtime-breakdown-content"></div>
        </div>
      </div>
    `
  );

  modal = document.getElementById("runtime-breakdown-modal");
  const closeBtn = document.getElementById("runtime-breakdown-close");
  const refreshBtn = document.getElementById("runtime-breakdown-refresh");

  const closeModal = () => {
    stopRuntimeBreakdownPolling();
    closePopupModal(modal);
  };

  closeBtn?.addEventListener("click", closeModal);
  refreshBtn?.addEventListener("click", async () => {
    await loadRuntimeBreakdown({ force: true });
  });
  const contentEl = document.getElementById("runtime-breakdown-content");
  contentEl?.addEventListener("click", (event) => {
    const button = event.target instanceof Element ? event.target.closest("[data-runtime-history-window]") : null;
    if (!button) {
      return;
    }
    const nextKey = _runtimeHistoryWindowKey(button.getAttribute("data-runtime-history-window"), "");
    if (!nextKey) {
      return;
    }
    if (_runtimeHistoryWindowKey(state.runtimeHistoryWindow, "24h") === nextKey) {
      return;
    }
    state.runtimeHistoryWindow = nextKey;
    const cachedPayload = state.runtimeBreakdownPayload;
    if (cachedPayload && typeof cachedPayload === "object") {
      contentEl.innerHTML = renderRuntimeBreakdown(cachedPayload);
    } else {
      void loadRuntimeBreakdown({ force: true, silent: true });
    }
  });
  modal.addEventListener("click", (event) => {
    if (event.target === modal) {
      closeModal();
    }
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && modal.classList.contains("active")) {
      closeModal();
    }
  });
  return modal;
}

function stopRuntimeBreakdownPolling() {
  const timer = Number(state.runtimeBreakdownPollTimer || 0);
  if (timer) {
    window.clearInterval(timer);
    state.runtimeBreakdownPollTimer = 0;
  }
}

function startRuntimeBreakdownPolling() {
  stopRuntimeBreakdownPolling();
  state.runtimeBreakdownPollTimer = window.setInterval(() => {
    const modal = document.getElementById("runtime-breakdown-modal");
    if (!modal || !modal.classList.contains("active")) {
      stopRuntimeBreakdownPolling();
      return;
    }
    loadRuntimeBreakdown({ silent: true });
  }, 5000);
}

async function loadRuntimeBreakdown({ force = false, silent = false } = {}) {
  const modal = ensureRuntimeBreakdownModal();
  if (!modal) {
    return;
  }
  if (!force && modal.dataset.loading === "1") {
    return;
  }
  const statusEl = document.getElementById("runtime-breakdown-status");
  const updatedEl = document.getElementById("runtime-breakdown-updated");
  const contentEl = document.getElementById("runtime-breakdown-content");
  modal.dataset.loading = "1";

  if (statusEl && !silent) {
    statusEl.textContent = "Loading runtime state...";
  }

  try {
    const payload = await api("/api/runtime/breakdown");
    state.runtimeBreakdownPayload = payload;
    if (contentEl) {
      contentEl.innerHTML = renderRuntimeBreakdown(payload);
    }
    if (statusEl) {
      statusEl.textContent = "";
    }
    if (updatedEl) {
      updatedEl.textContent = `Updated ${new Date().toLocaleTimeString()}`;
    }
  } catch (error) {
    if (statusEl) {
      statusEl.textContent = `Runtime breakdown failed: ${error?.message || "unknown error"}`;
    }
    if (contentEl && !silent) {
      contentEl.innerHTML = "";
    }
  } finally {
    modal.dataset.loading = "0";
  }
}

async function openRuntimeBreakdownModal() {
  const modal = ensureRuntimeBreakdownModal();
  openPopupModal(modal);
  await loadRuntimeBreakdown({ force: true });
  startRuntimeBreakdownPolling();
}

function bindRuntimeSummary() {
  const summary = document.getElementById("runtime-summary");
  if (!summary || summary.dataset.bound === "1") {
    return;
  }
  summary.dataset.bound = "1";
  summary.setAttribute("role", "button");
  summary.setAttribute("tabindex", "0");
  summary.title = "Open live Hydra jobs, LLM calls, and vision calls";
  summary.classList.add("interactive");
  summary.addEventListener("click", () => {
    openRuntimeBreakdownModal();
  });
  summary.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      openRuntimeBreakdownModal();
    }
  });
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
        setRuntimeSummaryText("Redis setup required", "offline");
        void promptRedisSetupRecovery(String(health?.redis_status?.error || state.redisStatus?.error || "Redis connection lost."));
        return health;
      }
      setRuntimeSummaryText(formatRuntimeSummary(health), "normal");
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

function closeChatEventSource(jobId = "") {
  const targetId = String(jobId || "").trim();
  const sources = state.chatEventSources && typeof state.chatEventSources === "object" ? state.chatEventSources : {};

  if (targetId) {
    const source = sources[targetId];
    if (source && typeof source.close === "function") {
      source.close();
    }
    delete sources[targetId];
    state.chatEventSources = sources;
    return;
  }

  Object.keys(sources).forEach((id) => {
    const source = sources[id];
    if (source && typeof source.close === "function") {
      source.close();
    }
  });
  state.chatEventSources = {};
}

function stopAllChatJobPolling() {
  const pollMeta = state.chatPollMeta && typeof state.chatPollMeta === "object" ? state.chatPollMeta : {};
  Object.keys(pollMeta).forEach((id) => {
    const row = pollMeta[id] && typeof pollMeta[id] === "object" ? pollMeta[id] : {};
    row.token = Number(row.token || 0) + 1;
    if (row.timer) {
      window.clearTimeout(row.timer);
    }
  });
  state.chatPollMeta = {};
}

function _avatarInitial(label, fallback = "?") {
  const text = String(label || "").trim();
  if (!text) {
    return String(fallback || "?").slice(0, 1).toUpperCase();
  }
  const match = text.match(/[A-Za-z0-9]/);
  return (match ? match[0] : text[0]).toUpperCase();
}

function _chatAvatarMarkup({ src, displayName, role }) {
  if (src) {
    return `<img class="chat-avatar-img" src="${escapeHtml(src)}" alt="${escapeHtml(displayName)} avatar" />`;
  }
  return `<div class="chat-avatar-fallback ${escapeHtml(role)}">${escapeHtml(
    _avatarInitial(displayName, role === "user" ? "U" : "T")
  )}</div>`;
}

function _formatBytes(raw) {
  const bytes = Number(raw);
  if (!Number.isFinite(bytes) || bytes <= 0) {
    return "";
  }
  if (bytes < 1024) {
    return `${Math.round(bytes)} B`;
  }
  if (bytes < 1024 * 1024) {
    return `${(bytes / 1024).toFixed(1)} KB`;
  }
  if (bytes < 1024 * 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function _chatFileUrl(fileId, mimetype = "") {
  const token = String(fileId || "").trim();
  if (!token) {
    return "";
  }
  const base = `/api/chat/files/${encodeURIComponent(token)}`;
  const mm = String(mimetype || "").trim();
  const withQuery = mm ? `${base}?mimetype=${encodeURIComponent(mm)}` : base;
  return withBasePath(withQuery);
}

function _sanitizeMarkdownHref(rawHref) {
  const href = String(rawHref || "").trim();
  if (!href) {
    return "";
  }
  const lowered = href.toLowerCase();
  if (
    lowered.startsWith("http://") ||
    lowered.startsWith("https://") ||
    lowered.startsWith("mailto:") ||
    lowered.startsWith("tel:") ||
    lowered.startsWith("/") ||
    lowered.startsWith("#")
  ) {
    return href;
  }
  return "";
}

function _renderMarkdownInline(rawText) {
  const source = String(rawText ?? "");
  if (!source) {
    return "";
  }

  const placeholders = [];
  let working = source;

  // Protect inline code first so emphasis/link transforms do not affect it.
  working = working.replace(/`([^`\n]+)`/g, (_match, codeText) => {
    const token = `@@MD_TOKEN_${placeholders.length}@@`;
    placeholders.push(`<code>${escapeHtml(codeText)}</code>`);
    return token;
  });

  // Convert markdown links to safe anchors.
  working = working.replace(/\[([^\]\n]+)\]\(([^)\s]+)\)/g, (_match, label, href) => {
    const safeHref = _sanitizeMarkdownHref(href);
    const token = `@@MD_TOKEN_${placeholders.length}@@`;
    if (!safeHref) {
      placeholders.push(`${escapeHtml(label)} (${escapeHtml(href)})`);
      return token;
    }
    placeholders.push(
      `<a href="${escapeHtml(safeHref)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`
    );
    return token;
  });

  working = escapeHtml(working);
  working = working.replace(/\*\*([^*\n][^*\n]*?)\*\*/g, "<strong>$1</strong>");
  working = working.replace(/\*([^*\n][^*\n]*?)\*/g, "<em>$1</em>");

  // Restore protected inline code and links.
  placeholders.forEach((html, index) => {
    const token = `@@MD_TOKEN_${index}@@`;
    working = working.replaceAll(token, html);
  });

  return working;
}

function _renderAssistantMarkdownBubble(rawText) {
  const source = String(rawText ?? "").replace(/\r\n?/g, "\n");
  const lines = source.split("\n");
  const chunks = [];
  let paragraphLines = [];
  let listType = "";
  let listItems = [];
  let inCodeBlock = false;
  let codeLines = [];
  let codeLang = "";

  const flushParagraph = () => {
    if (!paragraphLines.length) {
      return;
    }
    const rendered = paragraphLines
      .map((line) => _renderMarkdownInline(line.trim()))
      .join("<br />");
    chunks.push(`<p>${rendered}</p>`);
    paragraphLines = [];
  };

  const flushList = () => {
    if (!listType || !listItems.length) {
      listType = "";
      listItems = [];
      return;
    }
    const itemsHtml = listItems.map((item) => `<li>${_renderMarkdownInline(item)}</li>`).join("");
    chunks.push(`<${listType}>${itemsHtml}</${listType}>`);
    listType = "";
    listItems = [];
  };

  const flushCodeBlock = () => {
    if (!inCodeBlock) {
      return;
    }
    const langClass = codeLang ? ` class="language-${escapeHtml(codeLang)}"` : "";
    chunks.push(`<pre><code${langClass}>${escapeHtml(codeLines.join("\n"))}</code></pre>`);
    inCodeBlock = false;
    codeLines = [];
    codeLang = "";
  };

  for (const rawLine of lines) {
    const line = String(rawLine ?? "");
    const fenceMatch = line.match(/^```(?:\s*([A-Za-z0-9_+\-]+))?\s*$/);
    if (fenceMatch) {
      flushParagraph();
      flushList();
      if (inCodeBlock) {
        flushCodeBlock();
      } else {
        inCodeBlock = true;
        codeLang = String(fenceMatch[1] || "").trim();
        codeLines = [];
      }
      continue;
    }

    if (inCodeBlock) {
      codeLines.push(line);
      continue;
    }

    const trimmed = line.trim();
    if (!trimmed) {
      flushParagraph();
      flushList();
      continue;
    }

    const headingMatch = trimmed.match(/^(#{1,6})\s+(.*)$/);
    if (headingMatch) {
      flushParagraph();
      flushList();
      const level = Math.min(6, Math.max(1, headingMatch[1].length));
      chunks.push(`<h${level}>${_renderMarkdownInline(headingMatch[2].trim())}</h${level}>`);
      continue;
    }

    const orderedMatch = trimmed.match(/^\d+\.\s+(.*)$/);
    if (orderedMatch) {
      flushParagraph();
      if (listType && listType !== "ol") {
        flushList();
      }
      listType = "ol";
      listItems.push(String(orderedMatch[1] || "").trim());
      continue;
    }

    const bulletMatch = trimmed.match(/^[-*+]\s+(.*)$/);
    if (bulletMatch) {
      flushParagraph();
      if (listType && listType !== "ul") {
        flushList();
      }
      listType = "ul";
      listItems.push(String(bulletMatch[1] || "").trim());
      continue;
    }

    if (listType) {
      flushList();
    }
    paragraphLines.push(trimmed);
  }

  flushParagraph();
  flushList();
  flushCodeBlock();

  if (!chunks.length) {
    return `<div class="bubble-body markdown">${_renderMarkdownInline(source)}</div>`;
  }
  return `<div class="bubble-body markdown">${chunks.join("")}</div>`;
}

function renderChatMessage(message) {
  const role = String(message.role || "assistant").toLowerCase();
  const roleClass = role === "user" ? "user" : "assistant";
  const displayName =
    roleClass === "user"
      ? String(message.username || state.chatProfile.username || "User")
      : getTaterFullName();
  const avatarSrc = roleClass === "user" ? state.chatProfile.userAvatar : state.chatProfile.taterAvatar;
  const content = message.content;
  let bodyHtml = "";
  let rowExtraClass = "";

  if (typeof content === "string") {
    bodyHtml =
      roleClass === "assistant"
        ? _renderAssistantMarkdownBubble(content)
        : `<div class="bubble-body">${escapeHtml(content)}</div>`;
  } else if (content && typeof content === "object") {
    const marker = String(content.marker || "").trim().toLowerCase();
    if (marker === "plugin_wait") {
      const waitText = String(content.content || "").trim() || "Working on it...";
      bodyHtml = `<div class="bubble-body">${escapeHtml(waitText)}</div>`;
    } else if (marker === "typing") {
      rowExtraClass = " typing-indicator";
      bodyHtml = `
        <div class="bubble-body chat-typing-body" aria-label="${escapeHtml(`${displayName} is typing`)}">
          <div class="chat-typing-label">${escapeHtml(`${displayName} is typing`)}</div>
          <div class="chat-typing-dots" aria-hidden="true">
            <span></span>
            <span></span>
            <span></span>
          </div>
        </div>
      `;
    } else {
      const contentType = String(content.type || "").toLowerCase();
      const fileId = String(content.id || "").trim();
      const fileName = String(content.name || "attachment").trim() || "attachment";
      const mimetype = String(content.mimetype || "").trim() || "application/octet-stream";
      const fileUrl = _chatFileUrl(fileId, mimetype);
      const hasDataB64 = typeof content.data_b64 === "string" && content.data_b64.trim().length > 0;
      const hasFileUrl = Boolean(fileUrl);
      if (contentType === "image" && hasDataB64) {
        const imageUrl = `data:${escapeHtml(content.mimetype || "image/png")};base64,${content.data_b64}`;
        bodyHtml = `<img class="chat-media-image" src="${imageUrl}" alt="${escapeHtml(content.name || "image")}" />`;
      } else if (contentType === "image" && hasFileUrl) {
        bodyHtml = `<img class="chat-media-image" src="${fileUrl}" alt="${escapeHtml(fileName)}" />`;
      } else if (contentType === "audio" && hasDataB64) {
        const mimetype = String(content.mimetype || "audio/mpeg");
        const audioUrl = `data:${escapeHtml(mimetype)};base64,${content.data_b64}`;
        bodyHtml = `
          <div class="chat-media-wrap">
            <audio controls preload="metadata" src="${audioUrl}"></audio>
            <div class="chat-file-meta">${escapeHtml(fileName)}</div>
            <a class="inline-btn" href="${audioUrl}" download="${escapeHtml(fileName)}">Download Audio</a>
          </div>
        `;
      } else if (contentType === "audio" && hasFileUrl) {
        bodyHtml = `
          <div class="chat-media-wrap">
            <audio controls preload="metadata" src="${fileUrl}"></audio>
            <div class="chat-file-meta">${escapeHtml(fileName)}</div>
            <a class="inline-btn" href="${fileUrl}" download="${escapeHtml(fileName)}">Download Audio</a>
          </div>
        `;
      } else if (contentType === "video" && hasDataB64) {
        const mimetype = String(content.mimetype || "video/mp4");
        const videoUrl = `data:${escapeHtml(mimetype)};base64,${content.data_b64}`;
        bodyHtml = `
          <div class="chat-media-wrap">
            <video controls preload="metadata" src="${videoUrl}" class="chat-media-video"></video>
            <div class="chat-file-meta">${escapeHtml(fileName)}</div>
            <a class="inline-btn" href="${videoUrl}" download="${escapeHtml(fileName)}">Download Video</a>
          </div>
        `;
      } else if (contentType === "video" && hasFileUrl) {
        bodyHtml = `
          <div class="chat-media-wrap">
            <video controls preload="metadata" src="${fileUrl}" class="chat-media-video"></video>
            <div class="chat-file-meta">${escapeHtml(fileName)}</div>
            <a class="inline-btn" href="${fileUrl}" download="${escapeHtml(fileName)}">Download Video</a>
          </div>
        `;
      } else if (contentType === "file" && hasDataB64) {
        const sizeLabel = _formatBytes(content.size);
        bodyHtml = `
          <div class="chat-file-card">
            <div class="chat-file-meta">${escapeHtml(fileName)}${sizeLabel ? ` (${escapeHtml(sizeLabel)})` : ""}</div>
            <a class="inline-btn" href="data:${escapeHtml(mimetype)};base64,${content.data_b64}" download="${escapeHtml(
              fileName
            )}">Download File</a>
          </div>
        `;
      } else if (contentType === "file" && hasFileUrl) {
        const sizeLabel = _formatBytes(content.size);
        bodyHtml = `
          <div class="chat-file-card">
            <div class="chat-file-meta">${escapeHtml(fileName)}${sizeLabel ? ` (${escapeHtml(sizeLabel)})` : ""}</div>
            <a class="inline-btn" href="${fileUrl}" download="${escapeHtml(fileName)}">Download File</a>
          </div>
        `;
      } else {
        bodyHtml = `<pre>${escapeHtml(JSON.stringify(content, null, 2))}</pre>`;
      }
    }
  } else {
    bodyHtml = `<div class="bubble-body">${escapeHtml(String(content ?? ""))}</div>`;
  }

  const avatarHtml = `<div class="chat-avatar">${_chatAvatarMarkup({ src: avatarSrc, displayName, role: roleClass })}</div>`;
  const bubbleHtml = `
    <div class="bubble ${escapeHtml(roleClass)}">
      <div class="role">${escapeHtml(displayName)}</div>
      ${bodyHtml}
    </div>
  `;

  return `
    <article class="chat-row ${escapeHtml(roleClass)}${rowExtraClass}">
      ${roleClass === "user" ? `${bubbleHtml}${avatarHtml}` : `${avatarHtml}${bubbleHtml}`}
    </article>
  `;
}

function reposToText(repos) {
  const rows = Array.isArray(repos) ? repos : [];
  return rows
    .map((repo) => {
      const name = String(repo?.name || "").trim();
      const url = String(repo?.url || "").trim();
      if (!url) {
        return "";
      }
      return name ? `${name}|${url}` : url;
    })
    .filter(Boolean)
    .join("\n");
}

function textToRepos(text) {
  const rows = [];
  String(text || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .forEach((line) => {
      if (line.includes("|")) {
        const [namePart, ...urlParts] = line.split("|");
        const name = String(namePart || "").trim();
        const url = String(urlParts.join("|") || "").trim();
        if (url) {
          rows.push({ name, url });
        }
      } else {
        rows.push({ name: "", url: line });
      }
    });
  return rows;
}

function renderShopRepoListItem(kind, repo) {
  const name = String(repo?.name || "").trim();
  const url = String(repo?.url || "").trim();
  if (!url) {
    return "";
  }
  const title = name || "Unnamed Repo";
  return `
    <article class="shop-repo-item" data-kind="${escapeHtml(kind)}" data-repo-name="${escapeHtml(name)}" data-repo-url="${escapeHtml(url)}">
      <div class="shop-repo-body">
        <div class="shop-repo-name">${escapeHtml(title)}</div>
        <div class="shop-repo-url"><code>${escapeHtml(url)}</code></div>
      </div>
      <button class="inline-btn danger shop-remove-repo" type="button" data-kind="${escapeHtml(kind)}">Remove</button>
    </article>
  `;
}

function collectShopReposFromList(kind) {
  const list = document.getElementById(`shop-repo-list-${kind}`);
  if (!list) {
    return [];
  }
  return Array.from(list.querySelectorAll(".shop-repo-item"))
    .map((item) => ({
      name: String(item?.dataset?.repoName || "").trim(),
      url: String(item?.dataset?.repoUrl || "").trim(),
    }))
    .filter((repo) => repo.url);
}

function ensureShopRepoListEmptyState(kind) {
  const list = document.getElementById(`shop-repo-list-${kind}`);
  if (!list) {
    return;
  }
  const hasRows = Boolean(list.querySelector(".shop-repo-item"));
  const emptyNode = list.querySelector(".shop-repo-empty");
  if (hasRows && emptyNode) {
    emptyNode.remove();
  }
  if (!hasRows && !emptyNode) {
    list.insertAdjacentHTML("beforeend", `<div class="small shop-repo-empty">No additional repos configured.</div>`);
  }
}

function shopEndpoint(kind) {
  if (kind === "verbas") {
    return "/api/shop/verbas";
  }
  if (kind === "cores") {
    return "/api/shop/cores";
  }
  return "/api/shop/portals";
}

function shopLabel(kind) {
  if (kind === "verbas") {
    return "Verba";
  }
  if (kind === "cores") {
    return "Core";
  }
  return "Portal";
}

function setShopStatus(kind, text) {
  const statusEl = document.getElementById(`shop-status-${kind}`);
  if (statusEl) {
    statusEl.textContent = String(text || "").trim();
  }
}

function getActiveShopTab(kind) {
  const active = document.querySelector(`.shop-tab-btn[data-kind='${kind}'].active`);
  return String(active?.dataset?.tab || "").trim();
}

function activateShopTab(kind, tabName) {
  const requested = String(tabName || "").trim();
  const buttons = Array.from(document.querySelectorAll(`.shop-tab-btn[data-kind='${kind}']`));
  const panels = Array.from(document.querySelectorAll(`.shop-tab-panel[data-kind='${kind}']`));
  if (!buttons.length || !panels.length) {
    return;
  }
  const available = new Set(buttons.map((button) => String(button.dataset.tab || "").trim()).filter(Boolean));
  const fallback = String(buttons[0]?.dataset?.tab || "").trim();
  const activeTab = available.has(requested) ? requested : fallback;
  if (!activeTab) {
    return;
  }
  buttons.forEach((button) => {
    button.classList.toggle("active", button.dataset.tab === activeTab);
  });
  panels.forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.tabPanel === activeTab);
  });
}

function renderShopManager(kind, data) {
  const noun = shopLabel(kind);
  const repos = data?.repos || {};
  const defaultRepo = repos.default || { name: "", url: "" };
  const additionalRepos = Array.isArray(repos.additional) ? repos.additional : [];
  const installed = Array.isArray(data?.installed) ? data.installed : [];
  const catalog = Array.isArray(data?.catalog) ? data.catalog : [];
  const available = catalog.filter((item) => !item.installed);
  const errors = Array.isArray(data?.errors) ? data.errors : [];
  const updatesAvailable = Number(data?.updates_available || 0);

  const warningsHtml = errors.map((entry) => renderNotice(`${noun} shop: ${entry}`)).join("");

  const installedHtml = installed.length
    ? installed
        .map((entry) => {
          const enabledText = kind === "verbas" ? ` • enabled: ${entry.enabled ? "yes" : "no"}` : "";
          const runtimeText = kind !== "verbas" ? ` • running: ${entry.running ? "yes" : "no"}` : "";
          const platformText = entry.platforms_str ? `<div class="small">Portals: ${escapeHtml(entry.platforms_str)}</div>` : "";

          return `
            <article class="card shop-installed-card" data-kind="${kind}" data-id="${escapeHtml(entry.id)}">
              <div class="card-head">
                <h3 class="card-title">${escapeHtml(entry.name || entry.id)}</h3>
                <span class="small">${escapeHtml(entry.id)}</span>
              </div>
              <div class="small">installed: ${escapeHtml(entry.installed_ver || "0.0.0")} • store: ${escapeHtml(entry.store_ver || "-")} • source: ${escapeHtml(entry.source_label || "local")}${enabledText}${runtimeText}</div>
              <div class="muted">${escapeHtml(entry.description || "")}</div>
              ${platformText}
              <div class="small" style="margin-top:8px;">Use <strong>Manage</strong> tab for update/remove actions.</div>
            </article>
          `;
        })
        .join("")
    : renderNotice(`No installed ${noun.toLowerCase()} modules found.`);

  const catalogHtml = available.length
    ? available
        .map((item) => {
          const platforms = Array.isArray(item.platforms) && item.platforms.length ? ` • portals: ${item.platforms.join(", ")}` : "";
          return `
            <article class="card">
              <div class="card-head">
                <h3 class="card-title">${escapeHtml(item.name || item.id)}</h3>
                <span class="small">${escapeHtml(item.id)}</span>
              </div>
              <div class="small">version: ${escapeHtml(item.version || "-")} • source: ${escapeHtml(item.source_label || "")}${escapeHtml(platforms)}</div>
              <div class="muted">${escapeHtml(item.description || "")}</div>
              <div class="inline-row" style="margin-top:8px;">
                <button class="action-btn shop-action" data-kind="${kind}" data-action="install" data-id="${escapeHtml(item.id)}">Install</button>
              </div>
            </article>
          `;
        })
        .join("")
    : renderNotice(`No additional ${noun.toLowerCase()} modules available in configured repos.`);

  return `
    <div class="card">
      <div class="card-head">
        <h3 class="card-title">${noun} Shop Summary</h3>
        <div class="inline-row">
          <span class="small">updates: ${updatesAvailable}</span>
          <button class="action-btn shop-update-all" data-kind="${kind}" ${updatesAvailable ? "" : "disabled"}>Update All</button>
        </div>
      </div>
      <div class="small">Installed: ${installed.length} • Catalog: ${catalog.length} • Available: ${available.length}</div>
      <div id="shop-status-${kind}" class="small"></div>
    </div>
    ${warningsHtml}
    <div class="card">
      <div class="card-head">
        <h3 class="card-title">${noun} Repos</h3>
      </div>
      <div class="small">Default repo: ${escapeHtml(defaultRepo.name || "Default")} • ${escapeHtml(defaultRepo.url || "")}</div>
      <label style="margin-top:10px;">Additional repos (one per line; optional name as <code>name|url</code>)
        <textarea id="shop-repos-${kind}" style="min-height:110px;">${escapeHtml(reposToText(additionalRepos))}</textarea>
      </label>
      <div class="inline-row" style="margin-top:8px;">
        <button class="inline-btn shop-save-repos" data-kind="${kind}">Save Repos</button>
      </div>
    </div>
    <div class="card">
      <div class="card-head">
        <h3 class="card-title">Installed ${noun}s</h3>
      </div>
      <div class="form-grid shop-grid">${installedHtml}</div>
    </div>
    <div class="card">
      <div class="card-head">
        <h3 class="card-title">${noun} Catalog</h3>
      </div>
      <div class="form-grid shop-grid">${catalogHtml}</div>
    </div>
  `;
}

function renderShopTabbedManager(kind, data, options = {}) {
  const noun = shopLabel(kind);
  const repos = data?.repos || {};
  const defaultRepo = repos.default || { name: "", url: "" };
  const additionalRepos = Array.isArray(repos.additional) ? repos.additional : [];
  const installed = Array.isArray(data?.installed) ? data.installed : [];
  const catalog = Array.isArray(data?.catalog) ? data.catalog : [];
  const available = catalog.filter((item) => !item.installed);
  const updates = installed.filter((item) => item.update_available);
  const errors = Array.isArray(data?.errors) ? data.errors : [];
  const updatesAvailable = Number(data?.updates_available || updates.length || 0);
  const runtimeHtml = String(options.runtimeHtml || "").trim();
  const runtimeTitle = String(options.runtimeTitle || `${noun} Runtime`).trim();
  const runtimeCard = options.runtimeCard !== false;

  const warningsHtml = errors.map((entry) => renderNotice(`${noun} shop: ${entry}`)).join("");

  const installedHtml = installed.length
    ? installed
        .map((entry) => {
          const enabledText = kind === "verbas" ? ` • enabled: ${entry.enabled ? "yes" : "no"}` : "";
          const runtimeText = kind !== "verbas" ? ` • running: ${entry.running ? "yes" : "no"}` : "";
          const platformText = entry.platforms_str ? `<div class="small">Portals: ${escapeHtml(entry.platforms_str)}</div>` : "";

          return `
            <article class="card shop-installed-card" data-kind="${kind}" data-id="${escapeHtml(entry.id)}">
              <div class="card-head">
                <h3 class="card-title">${escapeHtml(entry.name || entry.id)}</h3>
                <span class="small">${escapeHtml(entry.id)}</span>
              </div>
              <div class="small">installed: ${escapeHtml(entry.installed_ver || "0.0.0")} • store: ${escapeHtml(entry.store_ver || "-")} • source: ${escapeHtml(entry.source_label || "local")}${enabledText}${runtimeText}</div>
              <div class="muted">${escapeHtml(entry.description || "")}</div>
              ${platformText}
            </article>
          `;
        })
        .join("")
    : renderNotice(`No installed ${noun.toLowerCase()} modules found.`);

  const storeHtml = available.length
    ? available
        .map((item) => {
          const platforms = Array.isArray(item.platforms) && item.platforms.length ? ` • portals: ${item.platforms.join(", ")}` : "";
          return `
            <article class="card">
              <div class="card-head">
                <h3 class="card-title">${escapeHtml(item.name || item.id)}</h3>
                <span class="small">${escapeHtml(item.id)}</span>
              </div>
              <div class="small">version: ${escapeHtml(item.version || "-")} • source: ${escapeHtml(item.source_label || "")}${escapeHtml(platforms)}</div>
              <div class="muted">${escapeHtml(item.description || "")}</div>
              <div class="inline-row" style="margin-top:8px;">
                <button class="action-btn shop-action" data-kind="${kind}" data-action="install" data-id="${escapeHtml(item.id)}">Install</button>
              </div>
            </article>
          `;
        })
        .join("")
    : renderNotice(`No additional ${noun.toLowerCase()} modules available in configured repos.`);

  const manageHtml = installed.length
    ? installed
        .map((entry) => {
          const updateButton = entry.update_available
            ? `<button class="action-btn shop-action" data-kind="${kind}" data-action="update" data-id="${escapeHtml(entry.id)}">Update</button>`
            : `<button class="inline-btn" disabled>Up to date</button>`;
          const enabledText = kind === "verbas" ? ` • enabled: ${entry.enabled ? "yes" : "no"}` : "";
          const runtimeText = kind !== "verbas" ? ` • running: ${entry.running ? "yes" : "no"}` : "";
          return `
            <article class="card">
              <div class="card-head">
                <h3 class="card-title">${escapeHtml(entry.name || entry.id)}</h3>
                <span class="small">${escapeHtml(entry.id)}</span>
              </div>
              <div class="small">installed: ${escapeHtml(entry.installed_ver || "0.0.0")} • store: ${escapeHtml(entry.store_ver || "-")} • source: ${escapeHtml(entry.source_label || "local")}${enabledText}${runtimeText}</div>
              <div class="muted">${escapeHtml(entry.description || "")}</div>
              <div class="inline-row" style="margin-top:8px;">
                ${updateButton}
                <label class="small inline-row">
                  <input type="checkbox" class="shop-purge" />
                  <span>Delete Data?</span>
                </label>
                <button class="inline-btn shop-action" data-kind="${kind}" data-action="remove" data-id="${escapeHtml(entry.id)}">Remove</button>
              </div>
            </article>
          `;
        })
        .join("")
    : renderNotice(`No installed ${noun.toLowerCase()} modules found.`);

  const runtimeSection = runtimeHtml
    ? runtimeCard
      ? `
      <div class="card">
        <div class="card-head"><h3 class="card-title">${escapeHtml(runtimeTitle)}</h3></div>
        <div class="form-grid shop-grid">${runtimeHtml}</div>
      </div>
    `
      : `<div class="form-grid shop-grid">${runtimeHtml}</div>`
    : "";
  const installedPanelHtml = runtimeSection
    ? runtimeSection
    : `<div class="form-grid shop-grid">${installedHtml}</div>`;
  const defaultRepoName = String(defaultRepo.name || "Default").trim() || "Default";
  const defaultRepoUrl = String(defaultRepo.url || "").trim();
  const additionalRepoRows = additionalRepos.map((repo) => renderShopRepoListItem(kind, repo)).filter(Boolean).join("");

  return `
    <section class="shop-manager-root" data-kind="${kind}">
      <div class="card">
      <div class="card-head">
        <h3 class="card-title">${noun} Manager</h3>
        <div class="inline-row">
          <span class="small">Installed: ${installed.length}</span>
          <span class="small">Store: ${catalog.length}</span>
          <span class="small">Updates: ${updatesAvailable}</span>
        </div>
      </div>
      <div id="shop-status-${kind}" class="small"></div>
      ${warningsHtml}
      <div class="shop-tabs" data-kind="${kind}">
        <button class="shop-tab-btn active" data-kind="${kind}" data-tab="installed">Installed</button>
        <button class="shop-tab-btn" data-kind="${kind}" data-tab="store">Store</button>
        <button class="shop-tab-btn" data-kind="${kind}" data-tab="manage">Manage</button>
        <button class="shop-tab-btn" data-kind="${kind}" data-tab="settings">Settings</button>
      </div>

      <section class="shop-tab-panel active" data-kind="${kind}" data-tab-panel="installed">
        ${installedPanelHtml}
      </section>

      <section class="shop-tab-panel" data-kind="${kind}" data-tab-panel="store">
        <div class="form-grid shop-grid">${storeHtml}</div>
      </section>

      <section class="shop-tab-panel" data-kind="${kind}" data-tab-panel="manage">
        <div class="inline-row" style="margin: 8px 0 12px;">
          <button class="action-btn shop-update-all" data-kind="${kind}" ${updatesAvailable ? "" : "disabled"}>Update All</button>
          <span class="small">${updatesAvailable} update(s) available.</span>
        </div>
        <div class="form-grid shop-grid">${manageHtml}</div>
      </section>

      <section class="shop-tab-panel" data-kind="${kind}" data-tab-panel="settings">
        <div class="shop-repo-section">
          <div class="small">Default repo (read-only)</div>
          <article class="shop-repo-item default">
            <div class="shop-repo-body">
              <div class="shop-repo-name">${escapeHtml(defaultRepoName)}</div>
              <div class="shop-repo-url"><code>${escapeHtml(defaultRepoUrl || "(not set)")}</code></div>
            </div>
            <span class="small">Built-in</span>
          </article>
        </div>
        <div class="shop-repo-section">
          <div class="small">Additional repos</div>
          <div class="form-grid two-col shop-repo-inputs">
            <label>Repo Name (optional)
              <input id="shop-repo-name-${kind}" type="text" placeholder="My ${escapeHtml(noun)} Repo" />
            </label>
            <label>Repo URL
              <input id="shop-repo-url-${kind}" type="text" placeholder="https://example.com/${kind}-manifest.json" />
            </label>
          </div>
          <div class="inline-row" style="margin-top:8px;">
            <button class="inline-btn shop-add-repo" type="button" data-kind="${kind}">Add Repo</button>
            <button class="action-btn shop-save-repos" type="button" data-kind="${kind}">Save Repos</button>
          </div>
          <div id="shop-repo-list-${kind}" class="shop-repo-list" style="margin-top:10px;">
            ${additionalRepoRows || `<div class="small shop-repo-empty">No additional repos configured.</div>`}
          </div>
        </div>
      </section>
      </div>
    </section>
  `;
}

function bindShopTabs(kind) {
  const buttons = Array.from(document.querySelectorAll(`.shop-tab-btn[data-kind='${kind}']`));
  const panels = Array.from(document.querySelectorAll(`.shop-tab-panel[data-kind='${kind}']`));
  if (!buttons.length || !panels.length) {
    return;
  }

  buttons.forEach((button) => {
    button.addEventListener("click", () => activateShopTab(kind, button.dataset.tab));
  });
  activateShopTab(kind, getActiveShopTab(kind) || String(buttons[0]?.dataset?.tab || "installed"));
}

function buildVerbaRuntimeHtml(runtimeData, shopData) {
  const items = Array.isArray(runtimeData?.items) ? runtimeData.items : [];
  const installedShopItems = Array.isArray(shopData?.installed) ? shopData.installed : [];
  const shopById = new Map(
    installedShopItems
      .map((entry) => [String(entry?.id || "").trim(), entry])
      .filter(([id]) => Boolean(id))
  );

  if (!items.length) {
    resetRuntimeSettingsCatalog("verbas");
    return renderNotice("No verba found in verba registry.");
  }

  resetRuntimeSettingsCatalog("verbas");

  return items
    .map((item) => {
      const shopEntry = shopById.get(String(item.id || "").trim()) || {};
      const settings = Array.isArray(item.settings) ? item.settings : [];
      const pluginId = String(item.id || "").trim();
      registerRuntimeSettings("verbas", pluginId, {
        label: String(item.name || pluginId).trim() || pluginId,
        endpoint: "/api/verbas",
        settings,
      });
      const settingsBlock = settings.length
        ? `
            <div class="inline-row" style="margin-top:8px;">
              <button class="inline-btn open-runtime-settings" data-runtime-kind="verbas" data-runtime-key="${escapeHtml(pluginId)}">Settings</button>
            </div>
          `
        : `<div class="small">No configurable settings.</div>`;
      const platformValues = Array.isArray(item.platforms) && item.platforms.length
        ? item.platforms
        : Array.isArray(shopEntry.platforms)
          ? shopEntry.platforms
          : [];
      const platforms = platformValues.length ? platformValues.join(", ") : "all";
      const installedVer = String(shopEntry.installed_ver || "0.0.0").trim() || "0.0.0";
      const storeVer = String(shopEntry.store_ver || "-").trim() || "-";
      const sourceLabel = String(shopEntry.source_label || "local").trim() || "local";
      const description = String(shopEntry.description || item.description || "No description").trim() || "No description";
      return `
        <article class="card" data-plugin-id="${escapeHtml(item.id)}">
          <div class="card-head">
            <h3 class="card-title">${escapeHtml(item.name)}</h3>
            <div class="inline-row">
              <span class="small">${escapeHtml(item.id)}</span>
              <button class="inline-btn verba-toggle">${item.enabled ? "Disable" : "Enable"}</button>
            </div>
          </div>
          <div class="small">installed: ${escapeHtml(installedVer)} • store: ${escapeHtml(storeVer)} • source: ${escapeHtml(sourceLabel)} • enabled: ${item.enabled ? "yes" : "no"}</div>
          <div class="muted">${escapeHtml(description)}</div>
          <div class="small">Portals: ${escapeHtml(platforms)}</div>
          ${settingsBlock}
        </article>
      `;
    })
    .join("");
}

function buildSurfaceRuntimeHtml(kind, runtimeData, shopData) {
  const items = Array.isArray(runtimeData?.items) ? runtimeData.items : [];
  const installedShopItems = Array.isArray(shopData?.installed) ? shopData.installed : [];
  const endpoint = kind === "cores" ? "/api/cores" : "/api/portals";
  const moduleSuffix = kind === "cores" ? "_core" : "_portal";
  const shopByModuleKey = new Map();
  const shopById = new Map();
  installedShopItems.forEach((entry) => {
    const moduleKey = String(entry?.module_key || "").trim();
    const entryId = String(entry?.id || "").trim();
    if (moduleKey) {
      shopByModuleKey.set(moduleKey, entry);
    }
    if (entryId) {
      shopById.set(entryId, entry);
    }
  });
  const resolveShopEntry = (runtimeKey) => {
    const key = String(runtimeKey || "").trim();
    if (!key) {
      return null;
    }
    const byModule = shopByModuleKey.get(key);
    if (byModule) {
      return byModule;
    }
    const normalizedId = key.endsWith(moduleSuffix) ? key.slice(0, -moduleSuffix.length) : key;
    return shopById.get(normalizedId) || shopById.get(key) || null;
  };

  if (!items.length) {
    resetRuntimeSettingsCatalog(kind);
    return renderNotice(`No ${kind} found.`);
  }

  resetRuntimeSettingsCatalog(kind);

  return items
    .map((item) => {
      const shopEntry = resolveShopEntry(item.key) || {};
      const settings = Array.isArray(item.settings) ? item.settings : [];
      const running = Boolean(item.running);
      const desired = Boolean(item.desired_running);
      const statusClass = running ? "running" : "stopped";
      const statusText = running ? "Running" : desired ? "Pending start" : "Stopped";
      const actionLabel = running ? "Stop" : "Start";
      const installedVer = String(shopEntry.installed_ver || "0.0.0").trim() || "0.0.0";
      const storeVer = String(shopEntry.store_ver || "-").trim() || "-";
      const sourceLabel = String(shopEntry.source_label || "local").trim() || "local";
      const description = String(shopEntry.description || "").trim();
      const surfaceKey = String(item.key || "").trim();
      registerRuntimeSettings(kind, surfaceKey, {
        label: String(item.label || surfaceKey).trim() || surfaceKey,
        endpoint,
        settings,
      });
      const settingsBlock = settings.length
        ? `
            <div class="inline-row" style="margin-top:8px;">
              <button class="inline-btn open-runtime-settings" data-runtime-kind="${escapeHtml(kind)}" data-runtime-key="${escapeHtml(surfaceKey)}">Settings</button>
            </div>
          `
        : `<div class="small">No settings in manifest.</div>`;
      return `
        <article class="card" data-surface-key="${escapeHtml(item.key)}">
          <div class="card-head">
            <h3 class="card-title">${escapeHtml(item.label || item.key)}</h3>
            <div class="inline-row">
              <span class="status-chip ${statusClass}">${statusText}</span>
              <button class="inline-btn surface-toggle">${actionLabel}</button>
            </div>
          </div>
          <div class="small">installed: ${escapeHtml(installedVer)} • store: ${escapeHtml(storeVer)} • source: ${escapeHtml(sourceLabel)} • running: ${running ? "yes" : "no"}</div>
          ${description ? `<div class="muted">${escapeHtml(description)}</div>` : ""}
          ${settingsBlock}
        </article>
      `;
    })
    .join("");
}

function replaceShopManagerInPlace(kind, managerHtml) {
  const existing = document.querySelector(`.shop-manager-root[data-kind='${kind}']`);
  if (!existing) {
    return false;
  }
  const template = document.createElement("template");
  template.innerHTML = String(managerHtml || "").trim();
  const replacement = template.content.firstElementChild;
  if (!replacement) {
    return false;
  }
  existing.replaceWith(replacement);
  return true;
}

async function refreshShopManagerInPlace(kind, preferredTab = "") {
  const targetTab = String(preferredTab || getActiveShopTab(kind) || "installed").trim() || "installed";

  if (kind === "verbas") {
    const [runtimeData, shopData] = await Promise.all([api("/api/verbas"), api("/api/shop/verbas")]);
    const runtimeHtml = buildVerbaRuntimeHtml(runtimeData, shopData);
    const managerHtml = renderShopTabbedManager("verbas", shopData, {
      runtimeHtml,
      runtimeTitle: "Verba Runtime",
      runtimeCard: false,
    });
    if (!replaceShopManagerInPlace("verbas", managerHtml)) {
      return false;
    }
    bindVerbaRuntimeActions(document.getElementById("view-root"));
    bindShopTabs("verbas");
    bindShopActions("verbas");
    activateShopTab("verbas", targetTab);
    return true;
  }

  if (kind === "portals" || kind === "cores") {
    const endpoint = kind === "cores" ? "/api/cores" : "/api/portals";
    const shopApi = kind === "cores" ? "/api/shop/cores" : "/api/shop/portals";
    const [runtimeData, shopData] = await Promise.all([api(endpoint), api(shopApi)]);
    const runtimeHtml = buildSurfaceRuntimeHtml(kind, runtimeData, shopData);
    const managerHtml = renderShopTabbedManager(kind, shopData, {
      runtimeHtml,
      runtimeTitle: kind === "cores" ? "Core Runtime" : "Portal Runtime",
      runtimeCard: false,
    });
    if (!replaceShopManagerInPlace(kind, managerHtml)) {
      return false;
    }
    bindSurfaceRuntimeActions(kind, endpoint, document.getElementById("view-root"));
    bindShopTabs(kind);
    bindShopActions(kind);
    activateShopTab(kind, targetTab);
    return true;
  }

  return false;
}

function encodeCoreManagerId(value) {
  return encodeURIComponent(String(value ?? ""));
}

function decodeCoreManagerId(value) {
  try {
    return decodeURIComponent(String(value || ""));
  } catch {
    return String(value || "");
  }
}

function renderCoreManagerField(field) {
  const key = String(field?.key || "").trim();
  if (!key) {
    return "";
  }
  const label = String(field?.label || key).trim() || key;
  const type = String(field?.type || "text").toLowerCase();
  const description = String(field?.description || "").trim();
  const descHtml = description ? `<div class="small">${escapeHtml(description)}</div>` : "";
  const placeholder = String(field?.placeholder || "").trim();
  const placeholderAttr = placeholder ? `placeholder="${escapeHtml(placeholder)}"` : "";
  const readOnly = boolFromAny(field?.read_only, false);
  const hideLabel = boolFromAny(field?.hide_label, false);
  const readOnlyAttr = readOnly ? " readonly" : "";
  const showWhen = field?.show_when && typeof field.show_when === "object" ? field.show_when : {};
  const showWhenAll = Array.isArray(field?.show_when_all)
    ? field.show_when_all.filter((item) => item && typeof item === "object")
    : [];
  const normalizeShowWhen = (condition) => {
    const sourceKey = String(condition?.source_key ?? condition?.key ?? "").trim();
    const values = [];
    const appendShowValue = (raw) => {
      const token = String(raw ?? "").trim();
      if (token && !values.includes(token)) {
        values.push(token);
      }
    };
    if (Array.isArray(condition?.any_of)) {
      condition.any_of.forEach(appendShowValue);
    }
    if (Array.isArray(condition?.values)) {
      condition.values.forEach(appendShowValue);
    }
    if (condition?.equals !== undefined) {
      appendShowValue(condition.equals);
    }
    if (condition?.eq !== undefined) {
      appendShowValue(condition.eq);
    }
    if (condition?.value !== undefined) {
      appendShowValue(condition.value);
    }
    return { sourceKey, values };
  };
  const wrapWithCondition = (innerHtml, condition) => {
    const { sourceKey, values } = normalizeShowWhen(condition || {});
    if (!sourceKey || !values.length) {
      return innerHtml;
    }
    let encodedValues = "";
    try {
      encodedValues = encodeURIComponent(JSON.stringify(values));
    } catch (_error) {
      encodedValues = "";
    }
    return `<div data-core-show-source-key="${escapeHtml(sourceKey)}" data-core-show-values="${escapeHtml(encodedValues)}">${innerHtml}</div>`;
  };
  const wrapField = (innerHtml) => {
    if (showWhenAll.length) {
      return showWhenAll.reduce((html, condition) => wrapWithCondition(html, condition), innerHtml);
    }
    return wrapWithCondition(innerHtml, showWhen);
  };

  if (type === "table") {
    const rawColumns = Array.isArray(field?.columns) ? field.columns : [];
    const columns = rawColumns
      .map((col, index) => {
        if (col && typeof col === "object") {
          const keyName = String(col.key ?? col.id ?? col.field ?? `col_${index}`).trim() || `col_${index}`;
          const labelName = String(col.label ?? keyName).trim() || keyName;
          return { key: keyName, label: labelName };
        }
        const labelName = String(col ?? "").trim();
        const keyName = labelName || `col_${index}`;
        return { key: keyName, label: labelName || keyName };
      })
      .filter((col) => col.key);
    const rows = Array.isArray(field?.rows) ? field.rows : [];

    if (!columns.length) {
      return wrapField(`
        <label>${escapeHtml(label)}
          <div class="small">No table columns configured.</div>
          ${descHtml}
        </label>
      `);
    }

    const headHtml = columns.map((col) => `<th>${escapeHtml(col.label)}</th>`).join("");
    const bodyHtml = rows.length
      ? rows
          .map((rawRow) => {
            const rowObj = rawRow && typeof rawRow === "object" ? rawRow : {};
            const cells = columns
              .map((col, colIndex) => {
                let value = "";
                if (Array.isArray(rawRow)) {
                  value = rawRow[colIndex] ?? "";
                } else {
                  value = rowObj[col.key] ?? "";
                }
                return `<td>${escapeHtml(value)}</td>`;
              })
              .join("");
            return `<tr>${cells}</tr>`;
          })
          .join("")
      : `<tr><td colspan="${columns.length}" class="small">No rows.</td></tr>`;

    return wrapField(`
      <label>${escapeHtml(label)}
        <div class="core-data-table-wrap">
          <table class="core-data-table">
            <thead><tr>${headHtml}</tr></thead>
            <tbody>${bodyHtml}</tbody>
          </table>
        </div>
        ${descHtml}
      </label>
    `);
  }

  if (type === "bar_chart" || type === "bars") {
    const points = Array.isArray(field?.points) ? field.points : [];
    const normalized = points
      .map((point) => {
        if (point && typeof point === "object") {
          const pointLabel = String(point.label ?? point.key ?? point.name ?? "").trim();
          const pointValue = Number(point.value ?? point.count ?? 0);
          return {
            label: pointLabel || "item",
            value: Number.isFinite(pointValue) ? pointValue : 0,
          };
        }
        return {
          label: String(point ?? "").trim() || "item",
          value: 0,
        };
      })
      .filter((row) => row.label);
    const maxValue = normalized.reduce((acc, row) => Math.max(acc, row.value), 0);

    const chartHtml = normalized.length
      ? normalized
          .map((row) => {
            const widthPct = maxValue > 0 ? Math.max(0, Math.min(100, (row.value / maxValue) * 100)) : 0;
            return `
              <div class="core-bar-row">
                <div class="core-bar-label">${escapeHtml(row.label)}</div>
                <div class="core-bar-track"><div class="core-bar-fill" style="width:${widthPct}%;"></div></div>
                <div class="core-bar-value">${escapeHtml(String(row.value))}</div>
              </div>
            `;
          })
          .join("")
      : `<div class="small">No chart data.</div>`;

    return wrapField(`
      <label>${escapeHtml(label)}
        <div class="core-bar-chart">${chartHtml}</div>
        ${descHtml}
      </label>
    `);
  }

  if (type === "image") {
    const src = String(field?.src ?? field?.url ?? "").trim();
    if (!src) {
      if (hideLabel) {
        return wrapField(`
          <div class="small">No image available.</div>
          ${descHtml}
        `);
      }
      return wrapField(`
        <label>${escapeHtml(label)}
          <div class="small">No image available.</div>
          ${descHtml}
        </label>
      `);
    }
    const alt = String(field?.alt || label || "Event image").trim() || "Event image";
    const imgCaption = String(field?.caption || "").trim();
    if (hideLabel) {
      return wrapField(`
        <div style="margin-top:6px;">
          <img src="${escapeHtml(src)}" alt="${escapeHtml(alt)}" loading="lazy" style="width:100%; max-height:320px; object-fit:contain; border:1px solid rgba(255,255,255,0.12); border-radius:10px; background:rgba(0,0,0,0.2);" />
        </div>
        ${imgCaption ? `<div class="small" style="margin-top:6px;">${escapeHtml(imgCaption)}</div>` : ""}
        ${descHtml}
      `);
    }
    return wrapField(`
      <label>${escapeHtml(label)}
        <div style="margin-top:6px;">
          <img src="${escapeHtml(src)}" alt="${escapeHtml(alt)}" loading="lazy" style="width:100%; max-height:320px; object-fit:contain; border:1px solid rgba(255,255,255,0.12); border-radius:10px; background:rgba(0,0,0,0.2);" />
        </div>
        ${imgCaption ? `<div class="small" style="margin-top:6px;">${escapeHtml(imgCaption)}</div>` : ""}
        ${descHtml}
      </label>
    `);
  }

  if (type === "checkbox") {
    const checked = boolFromAny(field?.value, false) ? "checked" : "";
    return wrapField(`
      <label>
        ${escapeHtml(label)}
        ${renderToggleRow(
          `<input class="toggle-input" type="checkbox" data-core-field-key="${escapeHtml(
            key
          )}" data-core-field-type="checkbox" ${checked} />`
        )}
        ${descHtml}
      </label>
    `);
  }

  if (type === "multiselect") {
    const options = Array.isArray(field?.options) ? field.options : [];
    const selectedValues = new Set(
      (Array.isArray(field?.value) ? field.value : [field?.value])
        .map((item) => String(item ?? "").trim())
        .filter(Boolean)
    );
    const dependent = field?.dependent_options && typeof field.dependent_options === "object" ? field.dependent_options : {};
    const filterSourceKey = String(dependent?.source_key || "").trim();
    let filterOptionsMap = "";
    let filterDefaultOptions = "";
    let filterPreferredValues = "";
    if (filterSourceKey) {
      try {
        filterOptionsMap = encodeURIComponent(JSON.stringify(dependent?.options_by_source || {}));
      } catch (_error) {
        filterOptionsMap = "";
      }
      try {
        const fallbackOptions = Array.isArray(dependent?.default_options) ? dependent.default_options : options;
        filterDefaultOptions = encodeURIComponent(JSON.stringify(fallbackOptions));
      } catch (_error) {
        filterDefaultOptions = "";
      }
      try {
        filterPreferredValues = encodeURIComponent(JSON.stringify(Array.from(selectedValues)));
      } catch (_error) {
        filterPreferredValues = "";
      }
    }
    const dependentAttrs = filterSourceKey
      ? ` data-core-filter-source-key="${escapeHtml(filterSourceKey)}"
          data-core-filter-options-map="${escapeHtml(filterOptionsMap)}"
          data-core-filter-default-options="${escapeHtml(filterDefaultOptions)}"
          data-core-filter-preferred-values="${escapeHtml(filterPreferredValues)}"`
      : "";
    const size = Math.max(4, Math.min(10, options.length || 6));
    const optionsHtml = options
      .map((raw) => {
        if (raw && typeof raw === "object") {
          const optionValue = String(raw.value ?? raw.id ?? raw.key ?? raw.label ?? "");
          const optionLabel = String(raw.label ?? optionValue);
          const isSelected = selectedValues.has(optionValue) ? "selected" : "";
          return `<option value="${escapeHtml(optionValue)}" ${isSelected}>${escapeHtml(optionLabel)}</option>`;
        }
        const optionValue = String(raw ?? "");
        const isSelected = selectedValues.has(optionValue) ? "selected" : "";
        return `<option value="${escapeHtml(optionValue)}" ${isSelected}>${escapeHtml(optionValue)}</option>`;
      })
      .join("");
    return wrapField(`
      <label>${escapeHtml(label)}
        <select multiple size="${size}" data-core-field-key="${escapeHtml(key)}" data-core-field-type="multiselect"${dependentAttrs}>${optionsHtml}</select>
        ${descHtml}
      </label>
    `);
  }

  if (type === "select") {
    const options = Array.isArray(field?.options) ? field.options : [];
    const selected = String(field?.value ?? "");
    const dependent = field?.dependent_options && typeof field.dependent_options === "object" ? field.dependent_options : {};
    const filterSourceKey = String(dependent?.source_key || "").trim();
    let filterOptionsMap = "";
    let filterDefaultOptions = "";
    if (filterSourceKey) {
      try {
        filterOptionsMap = encodeURIComponent(JSON.stringify(dependent?.options_by_source || {}));
      } catch (_error) {
        filterOptionsMap = "";
      }
      try {
        const fallbackOptions = Array.isArray(dependent?.default_options) ? dependent.default_options : options;
        filterDefaultOptions = encodeURIComponent(JSON.stringify(fallbackOptions));
      } catch (_error) {
        filterDefaultOptions = "";
      }
    }
    const dependentAttrs = filterSourceKey
      ? ` data-core-filter-source-key="${escapeHtml(filterSourceKey)}"
          data-core-filter-options-map="${escapeHtml(filterOptionsMap)}"
          data-core-filter-default-options="${escapeHtml(filterDefaultOptions)}"
          data-core-filter-preferred-value="${escapeHtml(selected)}"`
      : "";
    const renderSelectOptionRows = (rows) =>
      rows
        .map((raw) => {
          if (raw && typeof raw === "object" && Array.isArray(raw.options) && !("value" in raw) && !("id" in raw) && !("key" in raw)) {
            const groupLabel = String(raw.label ?? raw.title ?? "Options").trim() || "Options";
            const groupRows = renderSelectOptionRows(raw.options);
            return groupRows ? `<optgroup label="${escapeHtml(groupLabel)}">${groupRows}</optgroup>` : "";
          }
          if (raw && typeof raw === "object") {
            const optionValue = String(raw.value ?? raw.id ?? raw.key ?? raw.label ?? "");
            const optionLabel = String(raw.label ?? optionValue);
            const isSelected = optionValue === selected ? "selected" : "";
            return `<option value="${escapeHtml(optionValue)}" ${isSelected}>${escapeHtml(optionLabel)}</option>`;
          }
          const optionValue = String(raw ?? "");
          const isSelected = optionValue === selected ? "selected" : "";
          return `<option value="${escapeHtml(optionValue)}" ${isSelected}>${escapeHtml(optionValue)}</option>`;
        })
        .join("");
    const optionsHtml = renderSelectOptionRows(options);
    return wrapField(`
      <label>${escapeHtml(label)}
        <select data-core-field-key="${escapeHtml(key)}" data-core-field-type="select"${dependentAttrs}>${optionsHtml}</select>
        ${descHtml}
      </label>
    `);
  }

  if (type === "textarea" || type === "multiline") {
    if (readOnly) {
      const readonlyText = escapeHtml(String(field?.value ?? "")).replace(/\n/g, "<br>");
      if (hideLabel) {
        return wrapField(`
          <div class="core-readonly-text">${readonlyText}</div>
          ${descHtml}
        `);
      }
      return wrapField(`
        <label>${escapeHtml(label)}
          <div class="core-readonly-text">${readonlyText}</div>
          ${descHtml}
        </label>
      `);
    }
    return wrapField(`
      <label>${escapeHtml(label)}
        <textarea data-core-field-key="${escapeHtml(key)}" data-core-field-type="textarea"${readOnlyAttr} ${placeholderAttr}>${escapeHtml(
          field?.value ?? ""
        )}</textarea>
        ${descHtml}
      </label>
    `);
  }

  const htmlType = type === "password" ? "password" : type === "number" ? "number" : "text";
  const numberAttrs =
    type === "number"
      ? ` step="${escapeHtml(field?.step ?? "any")}"${
          field?.min !== undefined ? ` min="${escapeHtml(field.min)}"` : ""
        }${field?.max !== undefined ? ` max="${escapeHtml(field.max)}"` : ""}`
      : "";
  return wrapField(`
    <label>${escapeHtml(label)}
      <input type="${htmlType}"${numberAttrs}${readOnlyAttr} value="${escapeHtml(field?.value ?? "")}" ${placeholderAttr} data-core-field-key="${escapeHtml(
    key
  )}" data-core-field-type="${escapeHtml(type)}" />
      ${descHtml}
    </label>
  `);
}

function renderCoreSettingsManager(body, tabSpec) {
  const safeTabLabel = escapeHtml(tabSpec?.label || tabSpec?.core_key || "Core");
  const safeCoreKey = escapeHtml(tabSpec?.core_key || "");
  const summary = String(body.summary || "").trim();
  const stats = Array.isArray(body.stats) ? body.stats : [];
  const ui = body?.ui && typeof body.ui === "object" ? body.ui : {};
  const useTabs = boolFromAny(ui?.use_tabs, false);
  const createTabLabel = String(ui?.create_tab_label || "Create").trim() || "Create";
  const itemsTabLabel = String(ui?.items_tab_label || "Current").trim() || "Current";
  const itemFieldsDropdown = boolFromAny(ui?.item_fields_dropdown, false);
  const itemFieldsDropdownLabel = String(ui?.item_fields_dropdown_label || "Settings").trim() || "Settings";
  const itemFieldsPopup = boolFromAny(ui?.item_fields_popup, false);
  const itemFieldsPopupLabel = String(ui?.item_fields_popup_label || itemFieldsDropdownLabel || "Settings").trim() || "Settings";
  const itemSectionsInDropdown = boolFromAny(ui?.item_sections_in_dropdown, false);
  const addForm = ui?.add_form && typeof ui.add_form === "object" ? ui.add_form : {};
  const addFields = Array.isArray(addForm?.fields) ? addForm.fields : [];
  const itemForms = Array.isArray(ui?.item_forms) ? ui.item_forms : [];
  const addAction = String(addForm?.action || "").trim();
  const addSubmitLabel = String(addForm?.submit_label || "Add").trim() || "Add";
  const managerTitle = String(ui?.title || "Manager").trim() || "Manager";
  const emptyMessage = String(ui?.empty_message || body?.empty_message || "No entries found.").trim();
  const managerTabsRaw = Array.isArray(ui?.manager_tabs) ? ui.manager_tabs : [];
  const defaultManagerTab = String(ui?.default_tab || "").trim();
  const statsRefreshButton = boolFromAny(ui?.stats_refresh_button, false);
  const statsRefreshLabel = String(ui?.stats_refresh_label || "Refresh").trim() || "Refresh";
  const statsControls = Array.isArray(ui?.stats_controls)
    ? ui.stats_controls.filter((item) => item && typeof item === "object")
    : [];
  const statsControlsAction = String(ui?.stats_controls_action || "").trim();
  const statsControlsAutoSave = boolFromAny(ui?.stats_controls_auto_save, true);

  function renderCoreStatsControlField(field) {
    const key = String(field?.key || "").trim();
    if (!key) {
      return "";
    }
    const label = String(field?.label || key).trim() || key;
    const type = String(field?.type || "checkbox").trim().toLowerCase();
    if (type === "checkbox") {
      const checked = boolFromAny(field?.value, false) ? " checked" : "";
      return `
        <label class="small core-stats-control-toggle">
          <input type="checkbox"${checked} data-core-field-key="${escapeHtml(key)}" data-core-field-type="checkbox" />
          ${escapeHtml(label)}
        </label>
      `;
    }
    const value = String(field?.value ?? "");
    return `
      <label class="small core-stats-control-input">
        ${escapeHtml(label)}
        <input type="text" value="${escapeHtml(value)}" data-core-field-key="${escapeHtml(key)}" data-core-field-type="text" />
      </label>
    `;
  }

  const statsControlsHtml =
    statsControlsAction && statsControls.length
      ? `
        <form class="inline-row core-stats-controls-form"
          data-core-key="${safeCoreKey}"
          data-core-action="${escapeHtml(statsControlsAction)}"
          data-core-auto-save="${statsControlsAutoSave ? "1" : "0"}"
          style="margin-left:auto; gap:10px; align-items:center;">
          ${statsControls.map((field) => renderCoreStatsControlField(field)).join("")}
          <span class="small core-manager-status"></span>
        </form>
      `
      : "";

  const statsHtml = stats.length || statsRefreshButton || statsControlsHtml
    ? `
      <div class="core-metric-row">
        ${stats
          .map((entry) => {
            const label = String(entry?.label || "").trim();
            const value = entry?.value;
            if (!label) {
              return "";
            }
            const valueText = String(value ?? "-").trim() || "-";
            return `
              <div class="core-metric-pill">
                <div class="small">${escapeHtml(label)}</div>
                <div>${escapeHtml(valueText)}</div>
              </div>
            `;
          })
          .join("")}
        ${statsControlsHtml}
        ${
          statsRefreshButton
            ? `<button type="button" class="action-btn core-tab-refresh-btn" data-core-tab-refresh="1">${escapeHtml(
                statsRefreshLabel
              )}</button>`
            : ""
        }
      </div>
    `
    : "";

  function renderCoreManagerItemCard(item, renderOptions = {}) {
    const itemId = String(item?.id || "").trim();
    const encodedId = escapeHtml(encodeCoreManagerId(itemId));
    const title = String(item?.title || itemId || "(item)").trim() || "(item)";
    const subtitle = String(item?.subtitle || "").trim();
    const detail = String(item?.detail || "").trim();
    const itemGroup = String(item?.group || "").trim().toLowerCase();
    const itemGroupToken = itemGroup.replace(/[^a-z0-9_-]/g, "");
    const itemGroupClass = itemGroupToken ? ` core-manager-item-${itemGroupToken}` : "";
    const itemFields = Array.isArray(item?.fields) ? item.fields : [];
    const sections = Array.isArray(item?.sections) ? item.sections : [];
    const explicitPopupFields = Array.isArray(item?.popup_fields) ? item.popup_fields : [];
    const itemFieldsPopupEnabled = itemFieldsPopup && boolFromAny(item?.fields_popup, true);
    const itemFieldsDropdownEnabled = boolFromAny(item?.fields_dropdown, itemFieldsDropdown);
    const itemFieldsDropdownLabelLocal = String(item?.fields_dropdown_label || itemFieldsDropdownLabel).trim() || "Settings";
    const itemSectionsInDropdownEnabled = boolFromAny(item?.sections_in_dropdown, itemSectionsInDropdown);
    const saveAction = String(item?.save_action || "").trim();
    const removeAction = String(item?.remove_action || "").trim();
    const runAction = String(item?.run_action || "").trim();
    const runConfirm = String(item?.run_confirm || "").trim();
    const removeConfirm = String(item?.remove_confirm || "Remove this item?").trim();
    const itemFieldContent = itemFields.map((field) => renderCoreManagerField(field)).join("");
    const popupFields = [];
    explicitPopupFields.forEach((field) => {
      if (field && typeof field === "object") {
        popupFields.push({ ...field });
      }
    });
    if (itemFieldsPopupEnabled) {
      itemFields.forEach((field) => {
        if (field && typeof field === "object") {
          popupFields.push({ ...field });
        }
      });
      sections.forEach((section) => {
        const sectionLabel = String(section?.label || "Section").trim() || "Section";
        const fields = Array.isArray(section?.fields) ? section.fields : [];
        fields.forEach((field) => {
          if (!field || typeof field !== "object") {
            return;
          }
          const nextField = { ...field };
          const rawLabel = String(nextField.label || nextField.key || "Field").trim() || "Field";
          nextField.label = `${sectionLabel} • ${rawLabel}`;
          popupFields.push(nextField);
        });
      });
    }
    let popupFieldsEncoded = "";
    try {
      popupFieldsEncoded = popupFields.length ? encodeURIComponent(JSON.stringify(popupFields)) : "";
    } catch (_error) {
      popupFieldsEncoded = "";
    }
    const popupMode = String(item?.popup_mode || "").trim();
    let popupConfigEncoded = "";
    try {
      popupConfigEncoded = item?.popup_config ? encodeURIComponent(JSON.stringify(item.popup_config)) : "";
    } catch (_error) {
      popupConfigEncoded = "";
    }
    const popupTitle = String(item?.settings_title || `${title} Settings`).trim() || `${title} Settings`;
    const pageIndexRaw = Number(renderOptions?.page_index ?? renderOptions?.pageIndex ?? 0);
    const pageIndex = Number.isFinite(pageIndexRaw) ? Math.max(0, Math.floor(pageIndexRaw)) : 0;
    const pageAttr = pageIndex > 0 ? ` data-core-page-index="${pageIndex}"` : "";
    const pageStyle = pageIndex > 1 ? ` style="display:none;"` : "";
    const heroImageSrc = String(item?.hero_image_src || "").trim();
    const heroImageAlt = String(item?.hero_image_alt || title).trim() || title;
    const heroBadges = Array.isArray(item?.hero_badges) ? item.hero_badges : [];
    const summaryRows = Array.isArray(item?.summary_rows) ? item.summary_rows : [];
    const sensorRows = Array.isArray(item?.sensor_rows) ? item.sensor_rows : [];
    const sensorTitle = String(item?.sensor_title || "Sensors").trim() || "Sensors";
    const hasSatelliteSummary = Boolean(
      heroImageSrc || heroBadges.length || summaryRows.length || sensorRows.length || detail
    );

          const sectionHtml = sections
            .map((section) => {
              const sectionLabel = String(section?.label || "Section").trim() || "Section";
              const fields = Array.isArray(section?.fields) ? section.fields : [];
              const inline = itemSectionsInDropdownEnabled || boolFromAny(section?.inline, false);
              const tone = String(section?.tone || "").trim().toLowerCase();
              const toneClass = tone ? ` ${escapeHtml(`tone-${tone}`)}` : "";
              if (inline) {
                return `
                  <section class="core-inline-section${toneClass}">
                    <div class="small core-inline-section-title">${escapeHtml(sectionLabel)}</div>
                    <div class="form-grid">
                      ${fields.map((field) => renderCoreManagerField(field)).join("")}
                    </div>
                  </section>
                `;
              }
              return `
                  <details class="settings-dropdown">
                    <summary class="settings-summary">${escapeHtml(sectionLabel)}</summary>
                    <div class="form-grid">
                      ${fields.map((field) => renderCoreManagerField(field)).join("")}
                    </div>
                  </details>
                `;
            })
            .join("");

    const dropdownContentParts = [];
    if (itemFieldContent) {
      dropdownContentParts.push(`<div class="form-grid">${itemFieldContent}</div>`);
    }
    if (itemSectionsInDropdownEnabled && sectionHtml) {
      dropdownContentParts.push(sectionHtml);
    }
    const dropdownContentHtml = dropdownContentParts.join("");

    const itemFieldsHtml = itemFieldsPopupEnabled
      ? ""
      : itemFieldsDropdownEnabled
      ? dropdownContentHtml
        ? `
          <details class="settings-dropdown">
            <summary class="settings-summary">${escapeHtml(itemFieldsDropdownLabelLocal)}</summary>
            ${dropdownContentHtml}
          </details>
        `
        : ""
      : itemFieldContent
        ? `
          <div class="form-grid">
            ${itemFieldContent}
          </div>
        `
        : "";

    const hasPopupSettingsBtn = Boolean(popupFieldsEncoded) && (itemFieldsPopupEnabled || explicitPopupFields.length > 0);
    const hasSaveBtn = saveAction && (!itemFieldsPopupEnabled || !popupFieldsEncoded);
    const hasAnyAction = Boolean(hasPopupSettingsBtn || hasSaveBtn || removeAction || runAction);
    const actionRowHtml = hasAnyAction
      ? `
        <div class="inline-row" style="margin-top:10px;">
          ${
            hasSaveBtn
              ? `<button type="button" class="action-btn core-manager-save">${escapeHtml(
                  String(item?.save_label || "Save")
                )}</button>`
              : ""
          }
          ${
            removeAction
              ? `<button type="button" class="inline-btn danger core-manager-remove">${escapeHtml(
                  String(item?.remove_label || "Remove")
                )}</button>`
              : ""
          }
          ${
            hasPopupSettingsBtn
              ? `<button type="button" class="action-btn core-manager-settings">${escapeHtml(
                  String(item?.settings_label || itemFieldsPopupLabel)
                )}</button>`
              : ""
          }
          ${
            runAction
              ? `<button type="button" class="action-btn core-manager-run" style="margin-left:auto;">${escapeHtml(
                  String(item?.run_label || "Run Now")
                )}</button>`
              : ""
          }
          <span class="small core-manager-status"></span>
        </div>
      `
      : "";

    const heroBadgesHtml = heroBadges.length
      ? `
        <div class="core-satellite-badges">
          ${heroBadges
            .map((badge) => {
              const label = String(badge?.label || "").trim();
              if (!label) {
                return "";
              }
              const tone = String(badge?.tone || "muted").trim().toLowerCase().replace(/[^a-z0-9_-]/g, "");
              return `<span class="core-satellite-badge tone-${escapeHtml(tone || "muted")}">${escapeHtml(label)}</span>`;
            })
            .join("")}
        </div>
      `
      : "";

    const summaryRowsHtml = summaryRows.length
      ? `
        <div class="core-satellite-facts">
          ${summaryRows
            .map((row) => {
              const label = String(row?.label || "").trim();
              const value = String(row?.value ?? "").trim();
              if (!label || !value) {
                return "";
              }
              return `
                <div class="core-satellite-fact">
                  <div class="small core-satellite-fact-label">${escapeHtml(label)}</div>
                  <div class="core-satellite-fact-value">${escapeHtml(value)}</div>
                </div>
              `;
            })
            .join("")}
        </div>
      `
      : "";

    const sensorRowsHtml = sensorRows.length
      ? `
        <div class="core-satellite-sensors">
          <div class="small core-satellite-sensors-title">${escapeHtml(sensorTitle)}</div>
          <div class="core-satellite-sensor-grid">
            ${sensorRows
              .map((row) => {
                const label = String(row?.label || "").trim();
                const value = String(row?.value ?? "").trim();
                const meta = String(row?.meta || row?.kind || "").trim();
                if (!label || !value) {
                  return "";
                }
                return `
                  <div class="core-satellite-sensor-pill">
                    <span class="core-satellite-sensor-label">${escapeHtml(label)}</span>
                    <span class="core-satellite-sensor-value">${escapeHtml(value)}</span>
                    ${meta ? `<span class="core-satellite-sensor-meta">${escapeHtml(meta)}</span>` : ""}
                  </div>
                `;
              })
              .join("")}
          </div>
        </div>
      `
      : "";

    const summaryBlockHtml = hasSatelliteSummary
      ? `
        <div class="core-satellite-summary">
          ${
            heroImageSrc
              ? `<div class="core-satellite-image-wrap"><img class="core-satellite-image" src="${escapeHtml(heroImageSrc)}" alt="${escapeHtml(
                  heroImageAlt
                )}"></div>`
              : ""
          }
          <div class="core-satellite-summary-main">
            ${subtitle ? `<div class="small core-satellite-subtitle">${escapeHtml(subtitle)}</div>` : ""}
            ${heroBadgesHtml}
            ${detail ? `<div class="small core-satellite-detail">${escapeHtml(detail)}</div>` : ""}
            ${summaryRowsHtml}
            ${sensorRowsHtml}
          </div>
        </div>
      `
      : `${subtitle ? `<div class="small">${escapeHtml(subtitle)}</div>` : ""}`;

    return `
      <article class="card core-manager-item${itemGroupClass}"
        data-core-key="${safeCoreKey}"
        data-core-item-id="${encodedId}"
        data-core-item-group="${escapeHtml(itemGroup)}"
        data-core-save-action="${escapeHtml(saveAction)}"
        data-core-remove-action="${escapeHtml(removeAction)}"
        data-core-run-action="${escapeHtml(runAction)}"
        data-core-run-confirm="${escapeHtml(runConfirm)}"
        data-core-remove-confirm="${escapeHtml(removeConfirm)}"
        data-core-item-popup-fields="${escapeHtml(popupFieldsEncoded)}"
        data-core-item-popup-mode="${escapeHtml(popupMode)}"
        data-core-item-popup-config="${escapeHtml(popupConfigEncoded)}"
        data-core-item-popup-title="${escapeHtml(popupTitle)}"${pageAttr}${pageStyle}>
        <div class="card-head">
          <h3 class="card-title">${escapeHtml(title)}</h3>
          <span class="small">${safeCoreKey}</span>
        </div>
        ${summaryBlockHtml}
        ${itemFieldsHtml}
        ${itemFieldsPopupEnabled ? "" : itemSectionsInDropdownEnabled ? "" : sectionHtml}
        ${actionRowHtml}
      </article>
    `;
  }

  function filterCoreManagerItemsByGroup(groupKey) {
    const wanted = String(groupKey || "").trim().toLowerCase();
    if (!wanted) {
      return itemForms;
    }
    return itemForms.filter((item) => String(item?.group || "").trim().toLowerCase() === wanted);
  }

  function renderCoreManagerItemsContent(items, options = {}) {
    const rows = Array.isArray(items) ? items : [];
    const selector = boolFromAny(options?.selector, false);
    const selectorLabel = String(options?.selector_label || "Select Item").trim() || "Select Item";
    const sectionEmptyMessage = String(options?.empty_message || emptyMessage).trim() || emptyMessage;
    const itemGroup = String(options?.item_group || "").trim().toLowerCase();
    const groupToken = itemGroup.replace(/[^a-z0-9_-]/g, "");
    const groupClass = groupToken ? ` core-tab-items-group-${groupToken}` : "";
    const pageSizeRaw = Number(options?.page_size ?? 0);
    const pageSize = Number.isFinite(pageSizeRaw) ? Math.max(0, Math.floor(pageSizeRaw)) : 0;

    if (!rows.length) {
      return renderNotice(sectionEmptyMessage);
    }

    if (!selector && pageSize > 0 && rows.length > pageSize) {
      const pageCount = Math.max(1, Math.ceil(rows.length / pageSize));
      const cardsHtml = rows
        .map((item, index) =>
          renderCoreManagerItemCard(item, {
            pageIndex: Math.floor(index / pageSize) + 1,
          })
        )
        .join("");
      return `
        <div class="core-manager-pagination" data-core-pagination data-core-page-count="${pageCount}" data-core-current-page="1">
          <div class="core-tab-items${groupClass}" style="margin-top:10px;">${cardsHtml}</div>
          <div class="inline-row" style="margin-top:10px;">
            <button type="button" class="action-btn" data-core-page-prev disabled>Previous</button>
            <span class="small" data-core-page-label>Page 1 of ${pageCount}</span>
            <button type="button" class="action-btn" data-core-page-next>Next</button>
          </div>
        </div>
      `;
    }

    if (!selector || rows.length <= 1) {
      return `<div class="core-tab-items${groupClass}" style="margin-top:10px;">${rows.map((item) => renderCoreManagerItemCard(item)).join("")}</div>`;
    }

    const rowsWithToken = rows.map((item, index) => {
      const rawId = String(item?.id || "").trim() || `idx_${index}`;
      return {
        token: encodeCoreManagerId(rawId),
        label: String(item?.title || rawId || `Item ${index + 1}`).trim() || `Item ${index + 1}`,
        html: renderCoreManagerItemCard(item),
      };
    });

    const optionsHtml = rowsWithToken
      .map((row) => `<option value="${escapeHtml(row.token)}">${escapeHtml(row.label)}</option>`)
      .join("");
    const cardsHtml = rowsWithToken
      .map(
        (row, index) => `
          <div class="core-manager-select-card ${index === 0 ? "active" : ""}" data-core-manager-select-card="${escapeHtml(row.token)}">
            ${row.html}
          </div>
        `
      )
      .join("");

    return `
      <div class="core-manager-selector-wrap" data-core-manager-selector-block>
        <label>${escapeHtml(selectorLabel)}
          <select class="core-manager-selector" data-core-manager-selector>
            ${optionsHtml}
          </select>
        </label>
        <div class="core-manager-selector-cards" style="margin-top:10px;">
          ${cardsHtml}
        </div>
      </div>
    `;
  }

  function renderCoreManagerGroupedItems(groupsRaw, groupPanelEmptyMessage) {
    const groups = (Array.isArray(groupsRaw) ? groupsRaw : [])
      .map((group, index) => {
        const key = String(group?.key || `group_${index + 1}`).trim();
        if (!key) {
          return null;
        }
        return {
          key,
          label: String(group?.label || key).trim() || key,
          itemGroup: String(group?.item_group || key).trim(),
          selector: boolFromAny(group?.selector, true),
          selectorLabel: String(group?.selector_label || "Select Item").trim() || "Select Item",
          emptyMessage: String(group?.empty_message || groupPanelEmptyMessage || emptyMessage).trim() || emptyMessage,
        };
      })
      .filter(Boolean);

    if (!groups.length) {
      return renderNotice(groupPanelEmptyMessage || "No grouped items defined.");
    }

    const defaultSubTab = groups[0]?.key || "";
    const subButtonsHtml = groups
      .map(
        (group, index) => `
          <button type="button" class="core-manager-subtab-btn ${index === 0 ? "active" : ""}" data-core-manager-subtab="${escapeHtml(
            group.key
          )}">
            ${escapeHtml(group.label)}
          </button>
        `
      )
      .join("");

    const subPanelsHtml = groups
      .map((group, index) => {
        const groupItems = filterCoreManagerItemsByGroup(group.itemGroup);
        const content = renderCoreManagerItemsContent(groupItems, {
          selector: group.selector,
          selector_label: group.selectorLabel,
          item_group: group.itemGroup,
          empty_message: group.emptyMessage,
        });
        return `
          <section class="core-manager-subtab-panel ${index === 0 ? "active" : ""}" data-core-manager-subtab-panel="${escapeHtml(
            group.key
          )}">
            ${content}
          </section>
        `;
      })
      .join("");

    return `
      <div class="core-manager-subtabs-wrap">
        <div class="core-manager-subtabs" data-core-manager-default-subtab="${escapeHtml(defaultSubTab)}">
          ${subButtonsHtml}
        </div>
        ${subPanelsHtml}
      </div>
    `;
  }

  const addFormHtml = addAction
    ? `
      <form class="form-grid core-manager-add-form" data-core-key="${safeCoreKey}" data-core-action="${escapeHtml(addAction)}">
        ${addFields.map((field) => renderCoreManagerField(field)).join("")}
        <div class="inline-row">
          <button type="submit" class="action-btn">${escapeHtml(addSubmitLabel)}</button>
          <span class="small core-manager-status"></span>
        </div>
      </form>
    `
    : "";

  const itemCardsHtml = renderCoreManagerItemsContent(itemForms, { selector: false, empty_message: emptyMessage });

  const managerTabs = managerTabsRaw
    .map((raw, index) => {
      const key = String(raw?.key || `tab_${index + 1}`).trim();
      if (!key) {
        return null;
      }
      const label = String(raw?.label || key).trim() || key;
      const source = String(raw?.source || "items").trim().toLowerCase();
      return {
        key,
        label,
        source,
        itemGroup: String(raw?.item_group || "").trim(),
        selector: boolFromAny(raw?.selector, false),
        selectorLabel: String(raw?.selector_label || "Select Item").trim() || "Select Item",
        groups: Array.isArray(raw?.groups) ? raw.groups : [],
        pageSize: (() => {
          const parsed = Number(raw?.page_size ?? 0);
          return Number.isFinite(parsed) ? Math.max(0, Math.floor(parsed)) : 0;
        })(),
        emptyMessage: String(raw?.empty_message || "").trim(),
      };
    })
    .filter(Boolean);

  function renderManagerTabContent(tab) {
    if (!tab || typeof tab !== "object") {
      return renderNotice("Invalid tab configuration.");
    }
    if (tab.source === "add_form") {
      return addFormHtml || `<div class="small">No tool form defined.</div>`;
    }
    if (tab.source === "grouped_items") {
      return renderCoreManagerGroupedItems(tab.groups, tab.emptyMessage || emptyMessage);
    }
    const rows = tab.itemGroup ? filterCoreManagerItemsByGroup(tab.itemGroup) : itemForms;
    return renderCoreManagerItemsContent(rows, {
      selector: tab.selector,
      selector_label: tab.selectorLabel,
      item_group: tab.itemGroup,
      page_size: tab.pageSize,
      empty_message: tab.emptyMessage || emptyMessage,
    });
  }

  let managerInnerHtml = "";
  if (managerTabs.length) {
    const tabKeys = new Set(managerTabs.map((tab) => tab.key));
    const firstKey = managerTabs[0]?.key || "";
    const activeKey = tabKeys.has(defaultManagerTab) ? defaultManagerTab : firstKey;
    const tabButtonsHtml = managerTabs
      .map(
        (tab) => `
          <button type="button" class="core-manager-tab-btn ${tab.key === activeKey ? "active" : ""}" data-core-manager-tab="${escapeHtml(
            tab.key
          )}">
            ${escapeHtml(tab.label)}
          </button>
        `
      )
      .join("");
    const tabPanelsHtml = managerTabs
      .map(
        (tab) => `
          <section class="core-manager-tab-panel ${tab.key === activeKey ? "active" : ""}" data-core-manager-tab-panel="${escapeHtml(
              tab.key
            )}">
            ${renderManagerTabContent(tab)}
          </section>
        `
      )
      .join("");
    managerInnerHtml = `
      <div class="core-manager-tabs" data-core-manager-default-tab="${escapeHtml(activeKey)}">
        ${tabButtonsHtml}
      </div>
      ${tabPanelsHtml}
    `;
  } else if (useTabs) {
    const tabKeys = new Set(["create", "items"]);
    const activeTab = tabKeys.has(defaultManagerTab) ? defaultManagerTab : "create";
    managerInnerHtml = `
      <div class="core-manager-tabs" data-core-manager-default-tab="${escapeHtml(activeTab)}">
        <button type="button" class="core-manager-tab-btn ${activeTab === "create" ? "active" : ""}" data-core-manager-tab="create">${escapeHtml(
          createTabLabel
        )}</button>
        <button type="button" class="core-manager-tab-btn ${activeTab === "items" ? "active" : ""}" data-core-manager-tab="items">${escapeHtml(
          itemsTabLabel
        )}</button>
      </div>
      <section class="core-manager-tab-panel ${activeTab === "create" ? "active" : ""}" data-core-manager-tab-panel="create">
        ${addFormHtml || `<div class="small">No create form defined.</div>`}
      </section>
      <section class="core-manager-tab-panel ${activeTab === "items" ? "active" : ""}" data-core-manager-tab-panel="items">
        ${itemCardsHtml}
      </section>
    `;
  } else {
    managerInnerHtml = `
      ${addFormHtml || `<div class="small">No add form defined.</div>`}
      ${itemCardsHtml}
    `;
  }

  return `
    <div class="card">
      <div class="card-head">
        <h3 class="card-title">${safeTabLabel}</h3>
        <span class="small">${safeCoreKey}</span>
      </div>
      ${summary ? `<div class="small">${escapeHtml(summary)}</div>` : ""}
      ${statsHtml}
      <div class="card" style="margin-top:10px;">
        <div class="card-head">
          <h3 class="card-title">${escapeHtml(managerTitle)}</h3>
        </div>
        ${managerInnerHtml}
      </div>
    </div>
  `;
}

function renderCoreTabPayload(payload, tabSpec) {
  const safeTabLabel = escapeHtml(tabSpec?.label || tabSpec?.core_key || "Core");
  const safeCoreKey = escapeHtml(tabSpec?.core_key || "");
  const body = payload && typeof payload === "object" ? payload : {};
  const errorText = String(body.error || "").trim();
  if (errorText) {
    return `
      <div class="card">
        <div class="card-head">
          <h3 class="card-title">${safeTabLabel}</h3>
          <span class="small">${safeCoreKey}</span>
        </div>
        ${renderNotice(errorText)}
      </div>
    `;
  }

  if (body?.ui && typeof body.ui === "object" && String(body.ui.kind || "").trim() === "settings_manager") {
    return renderCoreSettingsManager(body, tabSpec);
  }

  const summary = String(body.summary || "").trim();
  const stats = Array.isArray(body.stats) ? body.stats : [];
  const items = Array.isArray(body.items) ? body.items : [];
  const emptyMessage = String(body.empty_message || "No data available for this tab.").trim();

  const statsHtml = stats.length
    ? `
      <div class="core-metric-row">
        ${stats
          .map((entry) => {
            const label = String(entry?.label || "").trim();
            const value = entry?.value;
            if (!label) {
              return "";
            }
            const valueText = String(value ?? "-").trim() || "-";
            return `
              <div class="core-metric-pill">
                <div class="small">${escapeHtml(label)}</div>
                <div>${escapeHtml(valueText)}</div>
              </div>
            `;
          })
          .join("")}
      </div>
    `
    : "";

  const itemsHtml = items.length
    ? items
        .map((entry) => {
          const title = String(entry?.title || "").trim() || "(untitled)";
          const subtitle = String(entry?.subtitle || "").trim();
          const detail = String(entry?.detail || "").trim();
          return `
            <article class="core-tab-item">
              <div class="card-head">
                <h3 class="card-title">${escapeHtml(title)}</h3>
              </div>
              ${subtitle ? `<div class="small">${escapeHtml(subtitle)}</div>` : ""}
              ${detail ? `<div class="muted">${escapeHtml(detail)}</div>` : ""}
            </article>
          `;
        })
        .join("")
    : renderNotice(emptyMessage);

  return `
    <div class="card">
      <div class="card-head">
        <h3 class="card-title">${safeTabLabel}</h3>
        <span class="small">${safeCoreKey}</span>
      </div>
      ${summary ? `<div class="small">${escapeHtml(summary)}</div>` : ""}
      ${statsHtml}
      <div class="core-tab-items">${itemsHtml}</div>
    </div>
  `;
}

function _encodeCoreManagerJson(value) {
  try {
    return value ? encodeURIComponent(JSON.stringify(value)) : "";
  } catch (_error) {
    return "";
  }
}

function renderEspHomeRuntimeStats(stats) {
  const rows = Array.isArray(stats) ? stats : [];
  if (!rows.length) {
    return "";
  }
  return `
    <div class="core-metric-row">
      ${rows
        .map((entry) => {
          const label = String(entry?.label || "").trim();
          const valueText = String(entry?.value ?? "-").trim() || "-";
          if (!label) {
            return "";
          }
          return `
            <div class="core-metric-pill">
              <div class="small">${escapeHtml(label)}</div>
              <div>${escapeHtml(valueText)}</div>
            </div>
          `;
        })
        .join("")}
      <button type="button" class="action-btn core-tab-refresh-btn" data-core-tab-refresh="1">Refresh</button>
    </div>
  `;
}

function renderEspHomeStatsPanel(sections, tables) {
  const metricSections = Array.isArray(sections) ? sections : [];
  const dataTables = Array.isArray(tables) ? tables : [];
  const sectionHtml = metricSections
    .map((section) => {
      const title = String(section?.title || "Stats").trim() || "Stats";
      const metrics = Array.isArray(section?.metrics) ? section.metrics : [];
      const pills = metrics
        .map((entry) => {
          const label = String(entry?.label || "").trim();
          const valueText = String(entry?.value ?? "-").trim() || "-";
          if (!label) {
            return "";
          }
          return `
            <div class="core-metric-pill">
              <div class="small">${escapeHtml(label)}</div>
              <div>${escapeHtml(valueText)}</div>
            </div>
          `;
        })
        .join("");
      if (!pills) {
        return "";
      }
      return `
        <section class="core-inline-section">
          <div class="small core-inline-section-title">${escapeHtml(title)}</div>
          <div class="core-metric-row">${pills}</div>
        </section>
      `;
    })
    .join("");
  const tableHtml = dataTables
    .map((table) => {
      const title = String(table?.title || "Table").trim() || "Table";
      const columns = Array.isArray(table?.columns) ? table.columns : [];
      const rows = Array.isArray(table?.rows) ? table.rows : [];
      const emptyMessage = String(table?.empty_message || "No rows.").trim() || "No rows.";
      return `
        <section class="core-inline-section">
          <div class="small core-inline-section-title">${escapeHtml(title)}</div>
          ${renderSimpleDataTable(columns, rows, emptyMessage)}
        </section>
      `;
    })
    .join("");
  if (!sectionHtml && !tableHtml) {
    return renderNotice("No ESPHome stats yet.");
  }
  return `${sectionHtml}${tableHtml}`;
}

async function runEspHomeRefreshAction() {
  return api("/api/settings/esphome/runtime/action", {
    method: "POST",
    body: JSON.stringify({
      action: "voice_refresh",
      payload: {},
    }),
  });
}

function renderEspHomeSummaryRows(summaryRows) {
  const rows = Array.isArray(summaryRows) ? summaryRows : [];
  if (!rows.length) {
    return "";
  }
  return `
    <div class="core-satellite-facts">
      ${rows
        .map((row) => {
          const label = String(row?.label || "").trim();
          const value = String(row?.value ?? "").trim();
          if (!label || !value) {
            return "";
          }
          return `
            <div class="core-satellite-fact">
              <div class="small core-satellite-fact-label">${escapeHtml(label)}</div>
              <div class="core-satellite-fact-value">${escapeHtml(value)}</div>
            </div>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderEspHomeEntityControl(row, connected = true) {
  const control = row?.control && typeof row.control === "object" ? row.control : null;
  if (!control) {
    return "";
  }
  const entityKey = String(row?.key || "").trim();
  const label = String(row?.label || "").trim() || "Entity";
  if (!entityKey) {
    return "";
  }
  const disabledAttr = connected ? "" : " disabled";
  const controlType = String(control.type || "").trim().toLowerCase();
  const command = String(control.command || "").trim();
  if (!command) {
    return "";
  }

  if (controlType === "toggle" || controlType === "light") {
    const checked = boolFromAny(control.checked, false) ? " checked" : "";
    const toggleHtml = `
      <label class="toggle-row core-satellite-entity-toggle-row">
        <input
          type="checkbox"
          class="toggle-input esphome-entity-toggle"
          data-esphome-entity-key="${escapeHtml(entityKey)}"
          data-esphome-entity-command="${escapeHtml(command)}"
          data-esphome-entity-label="${escapeHtml(label)}"${checked}${disabledAttr}
        />
        <span class="small">${boolFromAny(control.checked, false) ? "On" : "Off"}</span>
      </label>
    `;
    const colorEnabled = controlType === "light" && boolFromAny(control.supports_color, false);
    const colorValue = String(control.color || "#ff9b45").trim() || "#ff9b45";
    const colorHtml = colorEnabled
      ? `
        <label class="core-satellite-entity-color-wrap" title="Set light color">
          <span class="small">Color</span>
          <input
            type="color"
            class="esphome-entity-color"
            value="${escapeHtml(colorValue)}"
            data-esphome-entity-key="${escapeHtml(entityKey)}"
            data-esphome-entity-command="${escapeHtml(command)}"
            data-esphome-entity-label="${escapeHtml(label)}"${disabledAttr}
          />
        </label>
      `
      : "";
    return `<div class="core-satellite-sensor-controls">${toggleHtml}${colorHtml}</div>`;
  }

  if (controlType === "button") {
    return `
      <div class="core-satellite-sensor-controls">
        <button
          type="button"
          class="inline-btn esphome-entity-button"
          data-esphome-entity-key="${escapeHtml(entityKey)}"
          data-esphome-entity-command="${escapeHtml(command)}"
          data-esphome-entity-label="${escapeHtml(label)}"${disabledAttr}
        >${escapeHtml(String(control.label || "Run"))}</button>
      </div>
    `;
  }

  if (controlType === "select") {
    const options = Array.isArray(control.options) ? control.options : [];
    if (!options.length) {
      return "";
    }
    const currentValue = String(control.value || "").trim();
    return `
      <div class="core-satellite-sensor-controls">
        <select
          class="esphome-entity-select"
          data-esphome-entity-key="${escapeHtml(entityKey)}"
          data-esphome-entity-command="${escapeHtml(command)}"
          data-esphome-entity-label="${escapeHtml(label)}"${disabledAttr}>
          ${options
            .map((option) => {
              const value = String(option || "").trim();
              if (!value) {
                return "";
              }
              const selected = value === currentValue ? " selected" : "";
              return `<option value="${escapeHtml(value)}"${selected}>${escapeHtml(value)}</option>`;
            })
            .join("")}
        </select>
      </div>
    `;
  }

  if (controlType === "number") {
    const value = String(control.value ?? "").trim();
    const min = control.min ?? "";
    const max = control.max ?? "";
    const step = control.step ?? "";
    return `
      <div class="core-satellite-sensor-controls">
        <div class="core-satellite-inline-set">
          <input
            type="number"
            class="esphome-entity-number"
            value="${escapeHtml(value)}"
            ${min !== "" ? `min="${escapeHtml(String(min))}"` : ""}
            ${max !== "" ? `max="${escapeHtml(String(max))}"` : ""}
            ${step !== "" ? `step="${escapeHtml(String(step))}"` : ""}
            data-esphome-entity-key="${escapeHtml(entityKey)}"
            data-esphome-entity-command="${escapeHtml(command)}"
            data-esphome-entity-label="${escapeHtml(label)}"${disabledAttr}
          />
          <button
            type="button"
            class="inline-btn esphome-entity-number-set"
            data-esphome-entity-key="${escapeHtml(entityKey)}"
            data-esphome-entity-command="${escapeHtml(command)}"
            data-esphome-entity-label="${escapeHtml(label)}"${disabledAttr}
          >Set</button>
        </div>
      </div>
    `;
  }

  if (controlType === "text") {
    const value = String(control.value ?? "").trim();
    const maxLength = Number(control.max_length ?? 0);
    const maxLengthAttr = Number.isFinite(maxLength) && maxLength > 0 ? ` maxlength="${escapeHtml(String(maxLength))}"` : "";
    return `
      <div class="core-satellite-sensor-controls">
        <div class="core-satellite-inline-set">
          <input
            type="text"
            class="esphome-entity-text"
            value="${escapeHtml(value)}"${maxLengthAttr}
            data-esphome-entity-key="${escapeHtml(entityKey)}"
            data-esphome-entity-command="${escapeHtml(command)}"
            data-esphome-entity-label="${escapeHtml(label)}"${disabledAttr}
          />
          <button
            type="button"
            class="inline-btn esphome-entity-text-set"
            data-esphome-entity-key="${escapeHtml(entityKey)}"
            data-esphome-entity-command="${escapeHtml(command)}"
            data-esphome-entity-label="${escapeHtml(label)}"${disabledAttr}
          >Set</button>
        </div>
      </div>
    `;
  }

  return "";
}

function renderEspHomeSensorRows(sensorRows, sensorTitle = "Live Entities", connected = true) {
  const rows = Array.isArray(sensorRows) ? sensorRows : [];
  if (!rows.length) {
    return "";
  }
  return `
    <div class="core-satellite-sensors">
      <div class="small core-satellite-sensors-title">${escapeHtml(String(sensorTitle || "Live Entities"))}</div>
      <div class="core-satellite-sensor-grid">
        ${rows
          .map((row) => {
            const label = String(row?.label || "").trim();
            const value = String(row?.value ?? "").trim();
            const meta = String(row?.meta || row?.kind || "").trim();
            const controlHtml = renderEspHomeEntityControl(row, connected);
            if (!label || !value) {
              return "";
            }
            return `
              <div class="core-satellite-sensor-pill${controlHtml ? " is-controllable" : ""}">
                <span class="core-satellite-sensor-label">${escapeHtml(label)}</span>
                <span class="core-satellite-sensor-value">${escapeHtml(value)}</span>
                ${meta ? `<span class="core-satellite-sensor-meta">${escapeHtml(meta)}</span>` : ""}
                ${controlHtml}
              </div>
            `;
          })
          .join("")}
      </div>
    </div>
  `;
}

function renderEspHomeSatelliteCard(item, coreKey = "esphome") {
  const itemId = String(item?.id || "").trim();
  const encodedId = escapeHtml(encodeCoreManagerId(itemId));
  const title = String(item?.title || itemId || "Satellite").trim() || "Satellite";
  const subtitle = String(item?.subtitle || "").trim();
  const detail = String(item?.detail || "").trim();
  const saveAction = String(item?.save_action || "").trim();
  const removeAction = String(item?.remove_action || "").trim();
  const runAction = String(item?.run_action || "").trim();
  const runConfirm = String(item?.run_confirm || "").trim();
  const removeConfirm = String(item?.remove_confirm || "Forget this satellite?").trim();
  const popupFields = Array.isArray(item?.popup_fields) ? item.popup_fields : [];
  const popupFieldsEncoded = _encodeCoreManagerJson(popupFields);
  const popupConfigEncoded = _encodeCoreManagerJson(item?.popup_config || {});
  const popupMode = String(item?.popup_mode || "").trim();
  const popupTitle = String(item?.settings_title || `${title} Live Log`).trim() || `${title} Live Log`;
  const settingsLabel = String(item?.settings_label || "Live Log").trim() || "Live Log";
  const fields = Array.isArray(item?.fields) ? item.fields : [];
  const heroImageSrc = String(item?.hero_image_src || "").trim();
  const heroImageAlt = String(item?.hero_image_alt || title).trim() || title;
  const heroBadges = Array.isArray(item?.hero_badges) ? item.hero_badges : [];
  const summaryRows = Array.isArray(item?.summary_rows) ? item.summary_rows : [];
  const sensorRows = Array.isArray(item?.sensor_rows) ? item.sensor_rows : [];
  const sensorTitle = String(item?.sensor_title || "Live Entities").trim() || "Live Entities";
  const connected = boolFromAny(item?.connected, false);

  const heroBadgesHtml = heroBadges.length
    ? `
      <div class="core-satellite-badges">
        ${heroBadges
          .map((badge) => {
            const label = String(badge?.label || "").trim();
            if (!label) {
              return "";
            }
            const tone = String(badge?.tone || "muted").trim().toLowerCase().replace(/[^a-z0-9_-]/g, "");
            return `<span class="core-satellite-badge tone-${escapeHtml(tone || "muted")}">${escapeHtml(label)}</span>`;
          })
          .join("")}
      </div>
    `
    : "";

  const summaryBlockHtml = `
    <div class="core-satellite-summary">
      ${
        heroImageSrc
          ? `<div class="core-satellite-image-wrap"><img class="core-satellite-image" src="${escapeHtml(heroImageSrc)}" alt="${escapeHtml(
              heroImageAlt
            )}"></div>`
          : ""
      }
      <div class="core-satellite-summary-main">
        ${subtitle ? `<div class="small core-satellite-subtitle">${escapeHtml(subtitle)}</div>` : ""}
        ${heroBadgesHtml}
        ${detail ? `<div class="small core-satellite-detail">${escapeHtml(detail)}</div>` : ""}
        ${renderEspHomeSummaryRows(summaryRows)}
        ${renderEspHomeSensorRows(sensorRows, sensorTitle, connected)}
      </div>
    </div>
  `;

  return `
    <article class="card core-manager-item esphome-satellite-card"
      data-core-key="${escapeHtml(coreKey)}"
      data-core-item-id="${encodedId}"
      data-core-item-group="satellite"
      data-core-save-action="${escapeHtml(saveAction)}"
      data-core-remove-action="${escapeHtml(removeAction)}"
      data-core-run-action="${escapeHtml(runAction)}"
      data-core-run-confirm="${escapeHtml(runConfirm)}"
      data-core-remove-confirm="${escapeHtml(removeConfirm)}"
      data-core-item-popup-fields="${escapeHtml(popupFieldsEncoded)}"
      data-core-item-popup-mode="${escapeHtml(popupMode)}"
      data-core-item-popup-config="${escapeHtml(popupConfigEncoded)}"
      data-core-item-popup-title="${escapeHtml(popupTitle)}">
      <div class="card-head">
        <h3 class="card-title">${escapeHtml(title)}</h3>
        <span class="small">${escapeHtml(coreKey)}</span>
      </div>
      ${summaryBlockHtml}
      ${fields.length ? `<div class="form-grid">${fields.map((field) => renderCoreManagerField(field)).join("")}</div>` : ""}
      <div class="inline-row" style="margin-top:10px;">
        ${saveAction ? `<button type="button" class="action-btn core-manager-save">Save</button>` : ""}
        ${popupFields.length ? `<button type="button" class="action-btn core-manager-settings">${escapeHtml(settingsLabel)}</button>` : ""}
        ${removeAction ? `<button type="button" class="inline-btn danger core-manager-remove">Forget</button>` : ""}
        ${runAction ? `<button type="button" class="action-btn core-manager-run" style="margin-left:auto;">${escapeHtml(String(item?.run_label || "Run"))}</button>` : ""}
        <span class="small core-manager-status"></span>
      </div>
    </article>
  `;
}

function renderEspHomeSettingsCard(item, coreKey = "esphome") {
  const title = String(item?.title || "Voice Pipeline Settings").trim() || "Voice Pipeline Settings";
  const subtitle = String(item?.subtitle || "").trim();
  const saveAction = String(item?.save_action || "").trim();
  const sections = Array.isArray(item?.sections) ? item.sections : [];
  return `
    <article class="card core-manager-item"
      data-core-key="${escapeHtml(coreKey)}"
      data-core-item-id="${escapeHtml(encodeCoreManagerId(String(item?.id || "voice_settings")))}"
      data-core-item-group="settings"
      data-core-save-action="${escapeHtml(saveAction)}">
      <div class="card-head">
        <h3 class="card-title">${escapeHtml(title)}</h3>
        <span class="small">${escapeHtml(coreKey)}</span>
      </div>
      ${subtitle ? `<div class="small">${escapeHtml(subtitle)}</div>` : ""}
      ${sections
        .map((section) => {
          const sectionLabel = String(section?.label || "Section").trim() || "Section";
          const fields = Array.isArray(section?.fields) ? section.fields : [];
          return `
            <section class="core-inline-section" style="margin-top:12px;">
              <div class="small core-inline-section-title">${escapeHtml(sectionLabel)}</div>
              <div class="form-grid">
                ${fields.map((field) => renderCoreManagerField(field)).join("")}
              </div>
            </section>
          `;
        })
        .join("")}
      <div class="inline-row" style="margin-top:12px;">
        ${saveAction ? `<button type="button" class="action-btn core-manager-save">${escapeHtml(String(item?.save_label || "Save Settings"))}</button>` : ""}
        <span class="small core-manager-status"></span>
      </div>
    </article>
  `;
}

function renderEspHomeActionCard(item, coreKey = "esphome") {
  const title = String(item?.title || "Action").trim() || "Action";
  const subtitle = String(item?.subtitle || "").trim();
  const runAction = String(item?.run_action || "").trim();
  const runConfirm = String(item?.run_confirm || "").trim();
  return `
    <article class="card core-manager-item"
      data-core-key="${escapeHtml(coreKey)}"
      data-core-item-id="${escapeHtml(encodeCoreManagerId(String(item?.id || title)))}"
      data-core-item-group="action"
      data-core-run-action="${escapeHtml(runAction)}"
      data-core-run-confirm="${escapeHtml(runConfirm)}">
      <div class="card-head">
        <h3 class="card-title">${escapeHtml(title)}</h3>
        <span class="small">${escapeHtml(coreKey)}</span>
      </div>
      ${subtitle ? `<div class="small">${escapeHtml(subtitle)}</div>` : ""}
      <div class="inline-row" style="margin-top:12px;">
        ${runAction ? `<button type="button" class="action-btn core-manager-run">${escapeHtml(String(item?.run_label || "Run"))}</button>` : ""}
        <span class="small core-manager-status"></span>
      </div>
    </article>
  `;
}

function renderEspHomeAddPanel(addForm, coreKey = "esphome") {
  const action = String(addForm?.action || "").trim();
  const submitLabel = String(addForm?.submit_label || "Add Satellite").trim() || "Add Satellite";
  const fields = Array.isArray(addForm?.fields) ? addForm.fields : [];
  return `
    <form class="card core-manager-add-form"
      data-core-key="${escapeHtml(coreKey)}"
      data-core-action="${escapeHtml(action)}">
      <div class="card-head">
        <h3 class="card-title">Add Satellite</h3>
        <span class="small">${escapeHtml(coreKey)}</span>
      </div>
      <div class="small">Add a satellite manually when mDNS discovery has not found it yet.</div>
      <div class="form-grid" style="margin-top:12px;">
        ${fields.map((field) => renderCoreManagerField(field)).join("")}
      </div>
      <div class="inline-row" style="margin-top:12px;">
        <button type="submit" class="action-btn">${escapeHtml(submitLabel)}</button>
        <span class="small core-manager-status"></span>
      </div>
    </form>
  `;
}

function renderEspHomeFirmwareSections(sections) {
  const rows = Array.isArray(sections) ? sections : [];
  if (!rows.length) {
    return renderNotice("No firmware substitutions are available for this device.");
  }
  return rows
    .map((section) => {
      const title = String(section?.title || "Section").trim() || "Section";
      const fields = Array.isArray(section?.fields) ? section.fields : [];
      if (!fields.length) {
        return "";
      }
      return `
        <section class="core-inline-section" style="margin-top:12px;">
          <div class="small core-inline-section-title">${escapeHtml(title)}</div>
          <div class="form-grid two-col">
            ${fields.map((field) => renderCoreManagerField(field)).join("")}
          </div>
        </section>
      `;
    })
    .join("");
}

function normalizeEspHomeFirmwareSelection(firmware) {
  const body = firmware && typeof firmware === "object" ? firmware : {};
  const templates = Array.isArray(body?.templates) ? body.templates : [];
  const devices = Array.isArray(body?.devices) ? body.devices : [];
  const templateValues = templates.map((row) => String(row?.value || "").trim()).filter(Boolean);
  const deviceValues = devices.map((row) => String(row?.value || "").trim()).filter(Boolean);
  const requestedTemplate = String(
    state.esphomeFirmwareSelection?.templateKey || body?.active_template_key || templateValues[0] || ""
  ).trim();
  const requestedSelector = String(
    state.esphomeFirmwareSelection?.selector || body?.active_selector || deviceValues[0] || ""
  ).trim();
  return {
    templateKey: templateValues.includes(requestedTemplate) ? requestedTemplate : templateValues[0] || "",
    selector: deviceValues.includes(requestedSelector) ? requestedSelector : deviceValues[0] || "",
  };
}

function resolveEspHomeFirmwareVariant(firmware, templateKey = "", selector = "") {
  const body = firmware && typeof firmware === "object" ? firmware : {};
  const variants = body?.variants && typeof body.variants === "object" ? body.variants : {};
  const templateMap =
    templateKey && variants[templateKey] && typeof variants[templateKey] === "object" ? variants[templateKey] : {};
  return selector && templateMap[selector] && typeof templateMap[selector] === "object" ? templateMap[selector] : null;
}

function applyEspHomeFirmwareDraftToVariant(variant, templateKey = "") {
  const base = variant && typeof variant === "object" ? variant : null;
  if (!base) {
    return null;
  }
  const token = String(templateKey || "").trim();
  const draft =
    token && state.esphomeFirmwareDrafts && typeof state.esphomeFirmwareDrafts === "object"
      ? state.esphomeFirmwareDrafts[token]
      : null;
  if (!draft || typeof draft !== "object") {
    return base;
  }
  const sections = Array.isArray(base?.sections) ? base.sections : [];
  return {
    ...base,
    sections: sections.map((section) => {
      const fields = Array.isArray(section?.fields) ? section.fields : [];
      return {
        ...section,
        fields: fields.map((field) => {
          const key = String(field?.key || "").trim();
          if (!key || boolFromAny(field?.read_only, false) || !Object.prototype.hasOwnProperty.call(draft, key)) {
            return field;
          }
          return {
            ...field,
            value: draft[key],
          };
        }),
      };
    }),
  };
}

function captureEspHomeFirmwareDraft(card) {
  if (!(card instanceof HTMLElement)) {
    return;
  }
  const values = collectCoreManagerValues(card);
  const templateKey = String(card.dataset?.firmwareTemplateKey || values?.template_key || "").trim();
  if (!templateKey) {
    return;
  }
  const draft = {};
  Object.entries(values || {}).forEach(([key, value]) => {
    const token = String(key || "").trim();
    if (!token || token === "template_key" || token === "selector") {
      return;
    }
    draft[token] = value;
  });
  state.esphomeFirmwareDrafts[templateKey] = draft;
}

function deriveWakeWordSlugFromUrl(value) {
  const token = String(value || "").trim();
  if (!token) {
    return "";
  }
  const clean = token.split("?")[0].trim();
  const segments = clean.split("/").filter(Boolean);
  const filename = String(segments[segments.length - 1] || "").trim();
  if (!filename) {
    return "";
  }
  return filename.replace(/\.json$/i, "").replace(/[^A-Za-z0-9._-]+/g, "_").replace(/^_+|_+$/g, "").toLowerCase();
}

function syncEspHomeFirmwareWakeWordCatalog(card, { fromPicker = false } = {}) {
  if (!(card instanceof HTMLElement)) {
    return;
  }
  const picker = card.querySelector('select[data-core-field-key="wake_word_catalog"]');
  const urlInput = card.querySelector('input[data-core-field-key="wake_word_model_url"]');
  const nameInput = card.querySelector('input[data-core-field-key="wake_word_name"]');
  if (!(picker instanceof HTMLSelectElement) || !(urlInput instanceof HTMLInputElement)) {
    return;
  }

  if (fromPicker) {
    const selectedUrl = String(picker.value || "").trim();
    if (selectedUrl && selectedUrl !== "__custom__") {
      urlInput.value = selectedUrl;
      if (nameInput instanceof HTMLInputElement && !nameInput.readOnly) {
        const slug = deriveWakeWordSlugFromUrl(selectedUrl);
        if (slug) {
          nameInput.value = slug;
        }
      }
    }
    captureEspHomeFirmwareDraft(card);
    return;
  }

  const currentUrl = String(urlInput.value || "").trim();
  const optionValues = Array.from(picker.options).map((option) => String(option.value || "").trim());
  if (currentUrl && optionValues.includes(currentUrl)) {
    picker.value = currentUrl;
  } else if (optionValues.includes("__custom__")) {
    picker.value = "__custom__";
  }
  captureEspHomeFirmwareDraft(card);
}

function renderEspHomeFirmwareCard(firmware, coreKey = "esphome") {
  const body = firmware && typeof firmware === "object" ? firmware : {};
  const templates = Array.isArray(body?.templates) ? body.templates : [];
  const devices = Array.isArray(body?.devices) ? body.devices : [];
  const cli = body?.cli && typeof body.cli === "object" ? body.cli : {};
  const selection = normalizeEspHomeFirmwareSelection(body);
  const selectedTemplate =
    templates.find((row) => String(row?.value || "").trim() === selection.templateKey) || {};
  const selectedDevice =
    devices.find((row) => String(row?.value || "").trim() === selection.selector) || {};
  const variant = applyEspHomeFirmwareDraftToVariant(
    resolveEspHomeFirmwareVariant(body, selection.templateKey, selection.selector),
    selection.templateKey
  );
  const variantAvailable = Boolean(variant && typeof variant === "object");
  const title =
    String(variant?.title || selectedDevice?.title || selection.selector || "Firmware Target").trim() || "Firmware Target";
  const subtitle = String(variant?.subtitle || "").trim();
  const detail = String(variant?.detail || selectedDevice?.detail || "").trim();
  const templateLabel =
    String(variant?.template_label || selectedTemplate?.label || selection.templateKey || "Firmware").trim() || "Firmware";
  const cliAvailable = boolFromAny(variant?.cli_available ?? cli?.available, false);
  const cliReason = String(variant?.cli_reason || cli?.detail || "").trim();
  const links = Array.isArray(variant?.links) ? variant.links : [];
  const linksHtml = links.length
    ? `
      <div class="small" style="margin-top:10px;">
        ${links
          .map((link) => {
            const href = String(link?.href || "").trim();
            const label = String(link?.label || href || "Link").trim() || "Link";
            if (!href) {
              return "";
            }
            return `<a href="${escapeHtml(href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`;
          })
          .filter(Boolean)
          .join(" • ")}
      </div>
    `
    : "";
  const saveDisabledAttr = variantAvailable ? "" : " disabled";
  const flashDisabledAttr = cliAvailable && variantAvailable ? "" : " disabled";
  const itemId = escapeHtml(encodeCoreManagerId(selection.selector));
  const controlsHtml = `
    <section class="core-inline-section" style="margin-top:12px;">
      <div class="small core-inline-section-title">Target</div>
      <div class="form-grid two-col">
        ${renderCoreManagerField({
          key: "template_key",
          label: "Firmware Template",
          type: "select",
          value: selection.templateKey,
          options: templates,
          description: "Pick the firmware YAML to use for this build.",
        })}
        ${renderCoreManagerField({
          key: "selector",
          label: "Connected Device",
          type: "select",
          value: selection.selector,
          options: devices,
          description: "Choose which currently connected ESPHome device should receive the build + flash action.",
        })}
      </div>
    </section>
  `;
  const sectionsHtml = variant
    ? renderEspHomeFirmwareSections(Array.isArray(variant?.sections) ? variant.sections : [])
    : renderNotice("No firmware form is available for the current template/device selection.");

  return `
    <article class="card core-manager-item esphome-firmware-card"
      data-core-key="${escapeHtml(coreKey)}"
      data-core-item-id="${itemId}"
      data-firmware-selector="${escapeHtml(selection.selector)}"
      data-firmware-template-key="${escapeHtml(selection.templateKey)}">
      <div class="card-head">
        <h3 class="card-title">${escapeHtml(title)}</h3>
        <span class="small">${escapeHtml(templateLabel)}</span>
      </div>
      ${subtitle ? `<div class="small">${escapeHtml(subtitle)}</div>` : ""}
      ${detail ? `<div class="small" style="margin-top:6px;">${escapeHtml(detail)}</div>` : ""}
      ${
        !cliAvailable && cliReason
          ? `<div class="small" style="margin-top:8px;">ESPHome CLI unavailable: ${escapeHtml(cliReason)}</div>`
          : ""
      }
      ${linksHtml}
      ${controlsHtml}
      ${sectionsHtml}
      <div class="inline-row" style="margin-top:12px;">
        <button
          type="button"
          class="action-btn esphome-firmware-action"
          data-firmware-action="voice_firmware_save"
          data-firmware-title="Saving Firmware Settings"
          data-firmware-working="Saving firmware substitutions..."
          data-firmware-success="Firmware substitutions saved."
          data-firmware-error="Firmware save failed"${saveDisabledAttr}
        >Save</button>
        <button
          type="button"
          class="action-btn esphome-firmware-action"
          data-firmware-action="voice_firmware_flash_start"
          data-firmware-title="Flashing Firmware"
          data-firmware-working="Building and flashing firmware..."
          data-firmware-success="Firmware flash finished."
          data-firmware-error="Firmware flash failed"${flashDisabledAttr}
        >Build + Flash</button>
        <button
          type="button"
          class="action-btn esphome-firmware-action"
          data-firmware-action="voice_firmware_clean"
          data-firmware-title="Cleaning Build Files"
          data-firmware-working="Cleaning firmware build files..."
          data-firmware-success="Firmware build files cleaned."
          data-firmware-error="Firmware cleanup failed"
        >Clean Build Files</button>
        <span class="small core-manager-status"></span>
      </div>
    </article>
  `;
}

function renderEspHomeFirmwarePanel(firmware, coreKey = "esphome") {
  const body = firmware && typeof firmware === "object" ? firmware : {};
  const devices = Array.isArray(body?.devices) ? body.devices : [];
  const warnings = Array.isArray(body?.warnings) ? body.warnings : [];
  const cli = body?.cli && typeof body.cli === "object" ? body.cli : {};
  const cliAvailable = boolFromAny(cli?.available, false);
  const cliLabel = String(cli?.label || "Unavailable").trim() || "Unavailable";
  const cliDetail = String(cli?.detail || "").trim();
  const wifiNote = String(body?.wifi_note || "").trim();
  const emptyMessage =
    String(body?.empty_message || "No connected ESPHome devices are available for firmware actions.").trim() ||
    "No connected ESPHome devices are available for firmware actions.";

  return `
    <div class="card">
      <div class="card-head">
        <h3 class="card-title">Firmware Builder</h3>
        <span class="small">${escapeHtml(coreKey)}</span>
      </div>
      <div class="small">
        Pick a firmware template, choose one connected ESPHome device, then build and flash directly from Tater.
      </div>
      <div class="small" style="margin-top:8px;">
        ESPHome CLI: ${escapeHtml(cliLabel)}${cliDetail ? ` • ${escapeHtml(cliDetail)}` : ""}
      </div>
      ${wifiNote ? `<div class="small" style="margin-top:8px;">${escapeHtml(wifiNote)}</div>` : ""}
      ${
        !cliAvailable
          ? `<div class="small" style="margin-top:8px;">Build and flash actions stay disabled until ESPHome is runnable from this machine.</div>`
          : ""
      }
      ${
        warnings.length
          ? `<div class="small" style="margin-top:8px;">${warnings.map((warning) => escapeHtml(String(warning || "").trim())).filter(Boolean).join("<br>")}</div>`
          : ""
      }
    </div>
    ${devices.length ? renderEspHomeFirmwareCard(body, coreKey) : renderNotice(emptyMessage)}
  `;
}

function renderEspHomeSpeakerIdSummaryMetrics(metrics) {
  const rows = Array.isArray(metrics) ? metrics : [];
  if (!rows.length) {
    return "";
  }
  return `
    <div class="core-metric-row">
      ${rows
        .map((row) => {
          const label = String(row?.label || "").trim();
          const value = String(row?.value ?? "").trim();
          if (!label) {
            return "";
          }
          return `
            <div class="core-metric-pill">
              <div class="small">${escapeHtml(label)}</div>
              <div>${escapeHtml(value || "-")}</div>
            </div>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderEspHomeSpeakerIdPanel(payload, coreKey = "esphome") {
  const body = payload && typeof payload === "object" ? payload : {};
  const availability = body?.availability && typeof body.availability === "object" ? body.availability : {};
  const settingsSections = Array.isArray(body?.settings_sections) ? body.settings_sections : [];
  const createFields = Array.isArray(body?.create_fields) ? body.create_fields : [];
  const speakers = Array.isArray(body?.speakers) ? body.speakers : [];
  const pending = body?.pending && typeof body.pending === "object" ? body.pending : null;
  const available = boolFromAny(availability?.available, false);
  const availabilityLabel = String(availability?.label || (available ? "available" : "unavailable")).trim() || "unavailable";
  const availabilityDetail = String(availability?.detail || "").trim();
  const modelSource = String(availability?.model_source || "").trim();

  const settingsHtml = settingsSections.length
    ? settingsSections
        .map((section) => {
          const label = String(section?.label || "Settings").trim() || "Settings";
          const fields = Array.isArray(section?.fields) ? section.fields : [];
          if (!fields.length) {
            return "";
          }
          return `
            <section class="core-inline-section">
              <div class="small core-inline-section-title">${escapeHtml(label)}</div>
              <div class="form-grid two-col">
                ${fields.map((field) => renderCoreManagerField(field)).join("")}
              </div>
            </section>
          `;
        })
        .join("")
    : renderNotice("Speaker ID settings are unavailable right now.");

  const pendingHtml = pending
    ? `
        <article class="card core-manager-item esphome-speaker-id-pending-card">
          <div class="card-head">
            <h3 class="card-title">Pending Capture</h3>
          </div>
          <div class="small">
            The next voice turn from <strong>${escapeHtml(String(pending?.selector_label || "Any satellite"))}</strong> will be saved for
            <strong>${escapeHtml(String(pending?.speaker_name || "speaker"))}</strong>.
          </div>
          <div class="small" style="margin-top:8px;">
            Armed: ${escapeHtml(String(pending?.armed_at || "-"))} • Expires: ${escapeHtml(String(pending?.expires_at || "-"))}
          </div>
          <div class="inline-row" style="margin-top:12px;">
            <button type="button" class="inline-btn danger esphome-speaker-id-cancel">Cancel Pending Capture</button>
            <span class="small core-manager-status"></span>
          </div>
        </article>
      `
    : "";

  const speakersHtml = speakers.length
    ? speakers
        .map((speaker) => {
          const fields = Array.isArray(speaker?.fields) ? speaker.fields : [];
          const sampleCount = Number(speaker?.sample_count || 0);
          const name = String(speaker?.name || "Speaker").trim() || "Speaker";
          const updatedAt = String(speaker?.updated_at || "-").trim() || "-";
          const speakerId = String(speaker?.speaker_id || "").trim();
          return `
            <article class="card core-manager-item esphome-speaker-id-speaker-card" data-speaker-id="${escapeHtml(speakerId)}">
              <div class="card-head">
                <h3 class="card-title">${escapeHtml(name)}</h3>
              </div>
              ${renderEspHomeSpeakerIdSummaryMetrics([
                { label: "Samples", value: sampleCount },
                { label: "Last Updated", value: updatedAt },
              ])}
              <div class="form-grid" style="margin-top:12px;">
                ${fields.map((field) => renderCoreManagerField(field)).join("")}
              </div>
              <div class="inline-row" style="margin-top:12px;">
                <button type="button" class="action-btn esphome-speaker-id-save">Save</button>
                <button type="button" class="action-btn esphome-speaker-id-capture"${available ? "" : " disabled"}>Capture Sample</button>
                <button type="button" class="inline-btn danger esphome-speaker-id-delete">Delete</button>
                <span class="small core-manager-status"></span>
              </div>
            </article>
          `;
        })
        .join("")
    : renderNotice("No enrolled speakers yet. Create one below, then capture one or more voice samples.");

  return `
    <section class="core-inline-section">
      <div class="small core-inline-section-title">Speaker ID</div>
      <div class="small">
        Enroll local voiceprints for people who use your satellites, then let Tater attach the matched speaker to the voice turn before Hydra runs.
      </div>
      <div class="small" style="margin-top:8px;">
        Status: ${escapeHtml(availabilityLabel)}${modelSource ? ` • Model: ${escapeHtml(modelSource)}` : ""}${availabilityDetail ? ` • ${escapeHtml(availabilityDetail)}` : ""}
      </div>
      <div class="small" style="margin-top:8px;">
        Capture flow: click <strong>Capture Sample</strong>, say the wake word, then speak one clear sentence for a few seconds on the selected satellite.
      </div>
      <div style="margin-top:12px;">
        ${renderEspHomeSpeakerIdSummaryMetrics(Array.isArray(body?.summary_metrics) ? body.summary_metrics : [])}
      </div>
    </section>

    <article class="card core-manager-item esphome-speaker-id-settings-card" data-core-key="${escapeHtml(coreKey)}">
      <div class="card-head">
        <h3 class="card-title">Runtime Settings</h3>
      </div>
      ${settingsHtml}
      <div class="inline-row" style="margin-top:12px;">
        <button type="button" class="action-btn esphome-speaker-id-settings-save">Save Settings</button>
        <span class="small core-manager-status"></span>
      </div>
    </article>

    <article class="card core-manager-item esphome-speaker-id-create-card" data-core-key="${escapeHtml(coreKey)}">
      <div class="card-head">
        <h3 class="card-title">Create Speaker</h3>
      </div>
      <div class="form-grid">
        ${createFields.map((field) => renderCoreManagerField(field)).join("")}
      </div>
      <div class="inline-row" style="margin-top:12px;">
        <button type="button" class="action-btn esphome-speaker-id-create">Create Speaker</button>
        <span class="small core-manager-status"></span>
      </div>
    </article>

    <div class="core-tab-items">
      ${speakersHtml}
    </div>

    ${pendingHtml}
  `;
}

function renderEspHomeRuntimeHeader({ title = "Tater Voice", summary = "", stats = [], coreKey = "esphome" } = {}) {
  return `
    <div class="card esphome-runtime-shell">
      <div class="card-head">
        <h3 class="card-title">${escapeHtml(String(title || "Tater Voice"))}</h3>
        <span class="small">${escapeHtml(String(coreKey || "esphome"))}</span>
      </div>
      ${summary ? `<div class="small">${escapeHtml(summary)}</div>` : ""}
      ${renderEspHomeRuntimeStats(Array.isArray(stats) ? stats : [])}
    </div>
  `;
}

function bindEspHomeSettingsTabs(root = document) {
  const host =
    root instanceof HTMLElement
      ? root.querySelector("#settings-esphome-shell")
      : document.getElementById("settings-esphome-shell");
  if (!(host instanceof HTMLElement)) {
    return;
  }
  const buttons = Array.from(host.querySelectorAll(".settings-subtab-btn[data-esphome-tab]"));
  const panels = Array.from(host.querySelectorAll(".settings-subpanel[data-esphome-panel]"));
  if (!buttons.length || !panels.length) {
    return;
  }
  const activate = (tabKey, { load = false } = {}) => {
    host.dataset.esphomeActiveTab = String(tabKey || "satellites").trim() || "satellites";
    buttons.forEach((button) => {
      button.classList.toggle("active", button.dataset.esphomeTab === tabKey);
    });
    panels.forEach((panel) => {
      panel.classList.toggle("active", panel.dataset.esphomePanel === tabKey);
    });
    if (load) {
      void ensureEspHomeRuntimeLoaded({ force: true, panel: tabKey });
    }
  };
  buttons.forEach((button) => {
    if (button.dataset.esphomeTabBound === "1") {
      return;
    }
    button.dataset.esphomeTabBound = "1";
    button.addEventListener("click", () => activate(String(button.dataset.esphomeTab || "").trim(), { load: true }));
  });
  const initial = String(host.dataset.esphomeActiveTab || "satellites").trim() || "satellites";
  activate(initial);
}

function renderCoreTabPending(tabSpec, message = "Open this tab to load data.") {
  const safeTabLabel = escapeHtml(tabSpec?.label || tabSpec?.core_key || "Core");
  const safeCoreKey = escapeHtml(tabSpec?.core_key || "");
  const text = String(message || "").trim() || "Open this tab to load data.";
  return `
    <div class="card">
      <div class="card-head">
        <h3 class="card-title">${safeTabLabel}</h3>
        <span class="small">${safeCoreKey}</span>
      </div>
      ${renderNotice(text)}
    </div>
  `;
}

function renderCoreTopTabs(dynamicTabs, manageHtml, manageLabel = "Manage") {
  const tabs = Array.isArray(dynamicTabs) ? dynamicTabs : [];
  const safeManageLabel = escapeHtml(String(manageLabel || "Manage"));
  const dynamicButtons = tabs
    .map(
      (tab) => `
        <button class="core-top-tab-btn" data-core-tab="${escapeHtml(tab.core_key || "")}">
          ${escapeHtml(tab.label || tab.core_key || "Core")}
        </button>
      `
    )
    .join("");

  const dynamicPanels = tabs
    .map(
      (tab) => `
        <section
          class="core-top-tab-panel"
          data-core-tab-panel="${escapeHtml(tab.core_key || "")}"
          data-core-tab-loaded="0"
        >
          ${renderCoreTabPending(tab)}
        </section>
      `
    )
    .join("");

  return `
    <div class="core-top-tabs">
      ${dynamicButtons}
      <button class="core-top-tab-btn active" data-core-tab="manage">${safeManageLabel}</button>
    </div>
    ${dynamicPanels}
    <section class="core-top-tab-panel active" data-core-tab-panel="manage">
      ${manageHtml}
    </section>
  `;
}

function getActiveCoreTopTab() {
  const active = document.querySelector(".core-top-tab-btn.active[data-core-tab]");
  return String(active?.dataset?.coreTab || "").trim();
}

function activateCoreTopTab(tabName, options = {}) {
  const forceReload = Boolean(options && options.forceReload);
  const buttons = Array.from(document.querySelectorAll(".core-top-tab-btn[data-core-tab]"));
  const panels = Array.from(document.querySelectorAll(".core-top-tab-panel[data-core-tab-panel]"));
  if (!buttons.length || !panels.length) {
    return;
  }
  const requested = String(tabName || "").trim();
  const available = new Set(buttons.map((button) => String(button.dataset.coreTab || "").trim()).filter(Boolean));
  const activeTab = available.has(requested) ? requested : available.has("manage") ? "manage" : String(buttons[0]?.dataset?.coreTab || "");
  if (!activeTab) {
    return;
  }

  persistCoreTopTab(activeTab);
  buttons.forEach((button) => {
    button.classList.toggle("active", button.dataset.coreTab === activeTab);
  });
  panels.forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.coreTabPanel === activeTab);
  });
  if (activeTab && activeTab !== "manage") {
    void ensureCoreTopTabLoaded(activeTab, { force: forceReload });
  }
}

function bindCoreTopTabs() {
  const buttons = Array.from(document.querySelectorAll(".core-top-tab-btn[data-core-tab]"));
  const panels = Array.from(document.querySelectorAll(".core-top-tab-panel[data-core-tab-panel]"));
  if (!buttons.length || !panels.length) {
    return;
  }

  buttons.forEach((button) => {
    if (button.dataset.coreTopBound === "1") {
      return;
    }
    button.dataset.coreTopBound = "1";
    button.addEventListener("click", () => activateCoreTopTab(button.dataset.coreTab, { forceReload: true }));
  });

  const available = new Set(buttons.map((button) => button.dataset.coreTab));
  activateCoreTopTab(available.has(state.coreTopTab) ? state.coreTopTab : "manage");
}

function bindCoreManagerTabs() {
  document.querySelectorAll(".core-manager-tabs").forEach((tabsRoot) => {
    if (tabsRoot.dataset.coreManagerTabsBound === "1") {
      return;
    }
    tabsRoot.dataset.coreManagerTabsBound = "1";
    const buttons = Array.from(tabsRoot.querySelectorAll(".core-manager-tab-btn[data-core-manager-tab]"));
    if (!buttons.length) {
      return;
    }
    const card = tabsRoot.closest(".card");
    const panels = Array.from(card?.querySelectorAll(".core-manager-tab-panel[data-core-manager-tab-panel]") || []);
    if (!panels.length) {
      return;
    }

    const activate = (tabName) => {
      buttons.forEach((button) => {
        button.classList.toggle("active", button.dataset.coreManagerTab === tabName);
      });
      panels.forEach((panel) => {
        panel.classList.toggle("active", panel.dataset.coreManagerTabPanel === tabName);
      });
    };

    buttons.forEach((button) => {
      button.addEventListener("click", () => activate(button.dataset.coreManagerTab));
    });

    const available = new Set(buttons.map((button) => String(button.dataset.coreManagerTab || "").trim()).filter(Boolean));
    const requestedDefault = String(tabsRoot.dataset.coreManagerDefaultTab || "").trim();
    const firstTab = String(buttons[0]?.dataset?.coreManagerTab || "").trim();
    const fallbackTab = available.has("create") ? "create" : firstTab;
    const initialTab = available.has(requestedDefault) ? requestedDefault : fallbackTab;
    if (initialTab) {
      activate(initialTab);
    }
  });
}

function bindCoreManagerSubtabs() {
  document.querySelectorAll(".core-manager-subtabs").forEach((tabsRoot) => {
    if (tabsRoot.dataset.coreManagerSubtabsBound === "1") {
      return;
    }
    tabsRoot.dataset.coreManagerSubtabsBound = "1";
    const buttons = Array.from(tabsRoot.querySelectorAll(".core-manager-subtab-btn[data-core-manager-subtab]"));
    if (!buttons.length) {
      return;
    }
    const wrap = tabsRoot.closest(".core-manager-subtabs-wrap");
    const panels = Array.from(wrap?.querySelectorAll(".core-manager-subtab-panel[data-core-manager-subtab-panel]") || []);
    if (!panels.length) {
      return;
    }

    const activate = (tabName) => {
      buttons.forEach((button) => {
        button.classList.toggle("active", button.dataset.coreManagerSubtab === tabName);
      });
      panels.forEach((panel) => {
        panel.classList.toggle("active", panel.dataset.coreManagerSubtabPanel === tabName);
      });
    };

    buttons.forEach((button) => {
      button.addEventListener("click", () => activate(button.dataset.coreManagerSubtab));
    });

    const available = new Set(buttons.map((button) => String(button.dataset.coreManagerSubtab || "").trim()).filter(Boolean));
    const requestedDefault = String(tabsRoot.dataset.coreManagerDefaultSubtab || "").trim();
    const firstTab = String(buttons[0]?.dataset?.coreManagerSubtab || "").trim();
    const initialTab = available.has(requestedDefault) ? requestedDefault : firstTab;
    if (initialTab) {
      activate(initialTab);
    }
  });
}

function bindCoreManagerSelectors() {
  document.querySelectorAll(".core-manager-selector[data-core-manager-selector]").forEach((select) => {
    if (select.dataset.coreManagerSelectorBound === "1") {
      return;
    }
    select.dataset.coreManagerSelectorBound = "1";
    const block = select.closest("[data-core-manager-selector-block]");
    if (!block) {
      return;
    }
    const cards = Array.from(block.querySelectorAll(".core-manager-select-card[data-core-manager-select-card]"));
    if (!cards.length) {
      return;
    }

    const activate = (token) => {
      const selected = String(token || "").trim();
      cards.forEach((card) => {
        card.classList.toggle("active", card.dataset.coreManagerSelectCard === selected);
      });
    };

    select.addEventListener("change", () => activate(select.value));
    const initial = String(select.value || cards[0]?.dataset?.coreManagerSelectCard || "").trim();
    if (initial) {
      activate(initial);
    }
  });
}

function bindCoreManagerPagination() {
  document.querySelectorAll("[data-core-pagination]").forEach((block) => {
    if (!(block instanceof HTMLElement)) {
      return;
    }
    if (block.dataset.corePaginationBound === "1") {
      return;
    }
    block.dataset.corePaginationBound = "1";

    const cards = Array.from(block.querySelectorAll(".core-manager-item[data-core-page-index]"));
    if (!cards.length) {
      return;
    }

    const pageCountRaw = Number(block.dataset.corePageCount || 1);
    const pageCount = Number.isFinite(pageCountRaw) ? Math.max(1, Math.floor(pageCountRaw)) : 1;
    const currentRaw = Number(block.dataset.coreCurrentPage || 1);
    let currentPage = Number.isFinite(currentRaw) ? Math.max(1, Math.floor(currentRaw)) : 1;
    currentPage = Math.min(currentPage, pageCount);

    const prevBtn = block.querySelector("[data-core-page-prev]");
    const nextBtn = block.querySelector("[data-core-page-next]");
    const label = block.querySelector("[data-core-page-label]");

    const paint = () => {
      cards.forEach((card) => {
        const pageRaw = Number((card instanceof HTMLElement ? card.dataset.corePageIndex : "") || 1);
        const pageIndex = Number.isFinite(pageRaw) ? Math.max(1, Math.floor(pageRaw)) : 1;
        card.style.display = pageIndex === currentPage ? "" : "none";
      });
      if (label instanceof HTMLElement) {
        label.textContent = `Page ${currentPage} of ${pageCount}`;
      }
      if (prevBtn instanceof HTMLButtonElement) {
        prevBtn.disabled = currentPage <= 1;
      }
      if (nextBtn instanceof HTMLButtonElement) {
        nextBtn.disabled = currentPage >= pageCount;
      }
      block.dataset.coreCurrentPage = String(currentPage);
    };

    if (prevBtn instanceof HTMLButtonElement) {
      prevBtn.addEventListener("click", () => {
        if (currentPage <= 1) {
          return;
        }
        currentPage -= 1;
        paint();
      });
    }
    if (nextBtn instanceof HTMLButtonElement) {
      nextBtn.addEventListener("click", () => {
        if (currentPage >= pageCount) {
          return;
        }
        currentPage += 1;
        paint();
      });
    }

    paint();
  });
}

function _coreFieldByKey(host, key) {
  const wanted = String(key || "").trim();
  if (!wanted || !host) {
    return null;
  }
  const candidates = Array.from(host.querySelectorAll("[data-core-field-key]"));
  return candidates.find((node) => String(node?.dataset?.coreFieldKey || "").trim() === wanted) || null;
}

function _coreDecodeDependentJson(raw, fallback) {
  const token = String(raw || "").trim();
  if (!token) {
    return fallback;
  }
  try {
    return JSON.parse(decodeURIComponent(token));
  } catch (_error) {
    return fallback;
  }
}

function _coreNormalizeOptionRows(raw) {
  if (!Array.isArray(raw)) {
    return [];
  }
  return raw
    .map((item) => {
      if (item && typeof item === "object") {
        const value = String(item.value ?? item.id ?? item.key ?? item.label ?? "");
        const label = String(item.label ?? value);
        return { value, label };
      }
      const value = String(item ?? "");
      return { value, label: value };
    })
    .filter((row) => row.value || row.label);
}

function _coreRenderSelectOptions(selectEl, options, preferredValue = "", preferredValues = null) {
  if (!(selectEl instanceof HTMLSelectElement)) {
    return;
  }
  const preferred = String(preferredValue || "").trim();
  const preferredMany = Array.isArray(preferredValues)
    ? preferredValues.map((item) => String(item ?? "").trim()).filter(Boolean)
    : [];
  const current = String(selectEl.value || "").trim();
  const currentMany = selectEl.multiple
    ? Array.from(selectEl.selectedOptions || [])
        .map((option) => String(option?.value || "").trim())
        .filter(Boolean)
    : [];
  const rows = Array.isArray(options) ? options : [];
  if (!rows.length) {
    selectEl.innerHTML = "";
    return;
  }
  const html = rows
    .map((row) => {
      const value = String(row?.value ?? "");
      const label = String(row?.label ?? value);
      return `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`;
    })
    .join("");
  selectEl.innerHTML = html;
  if (selectEl.multiple) {
    const selected = new Set([...currentMany, ...preferredMany]);
    if (!selected.size && preferred) {
      selected.add(preferred);
    }
    Array.from(selectEl.options || []).forEach((option) => {
      option.selected = selected.has(String(option.value || "").trim());
    });
    const nextMany = Array.from(selectEl.selectedOptions || [])
      .map((option) => String(option?.value || "").trim())
      .filter(Boolean);
    const previousToken = currentMany.join("\u0000");
    const nextToken = nextMany.join("\u0000");
    if (previousToken !== nextToken) {
      selectEl.dispatchEvent(new Event("change", { bubbles: true }));
      selectEl.dispatchEvent(new Event("input", { bubbles: true }));
    }
    return;
  }
  const hasCurrent = rows.some((row) => String(row?.value ?? "") === current);
  const hasPreferred = rows.some((row) => String(row?.value ?? "") === preferred);
  if (hasCurrent) {
    selectEl.value = current;
  } else if (hasPreferred) {
    selectEl.value = preferred;
  } else {
    selectEl.selectedIndex = 0;
  }
  const nextValue = String(selectEl.value || "").trim();
  if (nextValue !== current) {
    selectEl.dispatchEvent(new Event("change", { bubbles: true }));
    selectEl.dispatchEvent(new Event("input", { bubbles: true }));
  }
}

function bindCoreManagerConditionalFields() {
  document.querySelectorAll("[data-core-show-source-key]").forEach((container) => {
    if (!(container instanceof HTMLElement)) {
      return;
    }
    if (container.dataset.coreShowBound === "1") {
      return;
    }
    const sourceKey = String(container.dataset.coreShowSourceKey || "").trim();
    if (!sourceKey) {
      return;
    }
    const host =
      container.closest(".core-manager-add-form, .core-manager-item, .core-manager-tab-panel, .card") || document;
    const sourceInput = _coreFieldByKey(host, sourceKey);
    if (!(sourceInput instanceof HTMLInputElement || sourceInput instanceof HTMLSelectElement || sourceInput instanceof HTMLTextAreaElement)) {
      return;
    }
    const allowedRaw = _coreDecodeDependentJson(container.dataset.coreShowValues, []);
    const allowedValues = Array.isArray(allowedRaw)
      ? allowedRaw.map((item) => String(item ?? "").trim()).filter(Boolean)
      : [];
    if (!allowedValues.length) {
      return;
    }
    const refresh = () => {
      const sourceValue = String(sourceInput.value || "").trim();
      const visible = allowedValues.includes(sourceValue);
      container.style.display = visible ? "" : "none";
      container.setAttribute("aria-hidden", visible ? "false" : "true");
      container.querySelectorAll("[data-core-field-key]").forEach((input) => {
        if (input instanceof HTMLInputElement || input instanceof HTMLSelectElement || input instanceof HTMLTextAreaElement) {
          input.disabled = !visible;
        }
      });
    };

    sourceInput.addEventListener("change", refresh);
    sourceInput.addEventListener("input", refresh);
    container.dataset.coreShowBound = "1";
    refresh();
  });
}

function bindCoreManagerDependentSelects() {
  document.querySelectorAll("select[data-core-filter-source-key]").forEach((targetSelect) => {
    if (!(targetSelect instanceof HTMLSelectElement)) {
      return;
    }
    if (targetSelect.dataset.coreDependentBound === "1") {
      return;
    }
    const sourceKey = String(targetSelect.dataset.coreFilterSourceKey || "").trim();
    if (!sourceKey) {
      return;
    }
    const host =
      targetSelect.closest(".core-manager-add-form, .core-manager-item, .core-manager-tab-panel, .card") || document;
    const sourceInput = _coreFieldByKey(host, sourceKey);
    if (!(sourceInput instanceof HTMLInputElement || sourceInput instanceof HTMLSelectElement || sourceInput instanceof HTMLTextAreaElement)) {
      return;
    }

    const optionsBySource = _coreDecodeDependentJson(targetSelect.dataset.coreFilterOptionsMap, {});
    const defaultOptions = _coreNormalizeOptionRows(
      _coreDecodeDependentJson(targetSelect.dataset.coreFilterDefaultOptions, [])
    );
    const preferredValuesRaw = _coreDecodeDependentJson(targetSelect.dataset.coreFilterPreferredValues, []);
    const preferredValues = Array.isArray(preferredValuesRaw)
      ? preferredValuesRaw.map((item) => String(item ?? "").trim()).filter(Boolean)
      : [];
    const preferredValue = String(targetSelect.dataset.coreFilterPreferredValue || targetSelect.value || "").trim();
    const isMulti = targetSelect.multiple || String(targetSelect.dataset.coreFieldType || "").toLowerCase() === "multiselect";

    const refresh = () => {
      const sourceValue = String(sourceInput.value || "").trim();
      const sourceRows = optionsBySource && typeof optionsBySource === "object" ? optionsBySource[sourceValue] : [];
      const narrowed = _coreNormalizeOptionRows(sourceRows);
      let nextRows = defaultOptions;
      if (sourceValue) {
        if (narrowed.length) {
          nextRows = narrowed;
        } else if (isMulti) {
          nextRows = [];
        } else {
          nextRows = defaultOptions.filter((row) => String(row?.value ?? "").trim() === "");
          if (!nextRows.length && defaultOptions.length) {
            nextRows = [defaultOptions[0]];
          }
        }
      } else if (narrowed.length && !isMulti) {
        nextRows = narrowed;
      }
      _coreRenderSelectOptions(targetSelect, nextRows, preferredValue, preferredValues);
    };

    sourceInput.addEventListener("change", refresh);
    sourceInput.addEventListener("input", refresh);
    targetSelect.dataset.coreDependentBound = "1";
    refresh();
  });
}

function setCoreManagerStatus(host, text) {
  const statusEl = host?.querySelector(".core-manager-status");
  if (statusEl) {
    statusEl.textContent = String(text || "");
  }
}

function collectCoreManagerValues(host) {
  const values = {};
  host.querySelectorAll("[data-core-field-key]").forEach((input) => {
    if (input.disabled) {
      return;
    }
    const key = String(input.dataset.coreFieldKey || "").trim();
    if (!key) {
      return;
    }
    const type = String(input.dataset.coreFieldType || input.type || "").toLowerCase();
    if (type === "checkbox") {
      values[key] = Boolean(input.checked);
      return;
    }
    if (type === "number") {
      const parsed = Number(input.value);
      values[key] = Number.isNaN(parsed) ? 0 : parsed;
      return;
    }
    if (type === "multiselect" && input instanceof HTMLSelectElement) {
      values[key] = Array.from(input.selectedOptions || [])
        .map((option) => String(option?.value || "").trim())
        .filter(Boolean);
      return;
    }
    values[key] = input.value;
  });
  return values;
}

async function runCoreTabAction(coreKey, action, payload = {}) {
  return api(`/api/cores/${encodeURIComponent(coreKey)}/tab-action`, {
    method: "POST",
    body: JSON.stringify({
      action,
      payload,
    }),
  });
}

function resolveCoreActionEndpoint(node, coreKey = "") {
  const host = node && typeof node.closest === "function" ? node.closest("[data-core-action-endpoint]") : null;
  const endpoint = String(host?.dataset?.coreActionEndpoint || "").trim();
  if (endpoint) {
    return endpoint;
  }
  const key = String(coreKey || host?.dataset?.coreKey || "").trim();
  if (!key) {
    throw new Error("Missing core key.");
  }
  return `/api/cores/${encodeURIComponent(key)}/tab-action`;
}

async function runCoreManagerAction(node, coreKey, action, payload = {}) {
  const endpoint = resolveCoreActionEndpoint(node, coreKey);
  return api(endpoint, {
    method: "POST",
    body: JSON.stringify({
      action,
      payload,
    }),
  });
}

function resolveCoreRefreshScope(node) {
  const host = node && typeof node.closest === "function" ? node.closest("[data-core-refresh-scope]") : null;
  return String(host?.dataset?.coreRefreshScope || "").trim();
}

function normalizeEspHomeRuntimePanel(panel = "") {
  const token = String(panel || "").trim().toLowerCase();
  return ["satellites", "firmware", "platform", "speakerid", "stats"].includes(token) ? token : "satellites";
}

function getActiveEspHomeRuntimePanel(defaultPanel = "satellites") {
  const shell = document.getElementById("settings-esphome-shell");
  const activeButton = shell?.querySelector?.(".settings-subtab-btn.active[data-esphome-tab]");
  return normalizeEspHomeRuntimePanel(
    String(activeButton?.dataset?.esphomeTab || shell?.dataset?.esphomeActiveTab || defaultPanel).trim() || defaultPanel
  );
}

async function fetchEspHomeRuntimePayload(panel = "") {
  const targetPanel = normalizeEspHomeRuntimePanel(panel);
  return api(`/api/settings/esphome/runtime?panel=${encodeURIComponent(targetPanel)}`);
}

async function ensureEspHomeRuntimeLoaded({ force = false, panel = "" } = {}) {
  const shell = document.getElementById("settings-esphome-shell");
  const head = document.getElementById("settings-esphome-runtime-head");
  const satellitesHost = document.getElementById("settings-esphome-runtime-satellites");
  const addHost = document.getElementById("settings-esphome-runtime-add");
  const firmwareHost = document.getElementById("settings-esphome-runtime-firmware");
  const speakerIdHost = document.getElementById("settings-esphome-runtime-speakerid");
  const statsHost = document.getElementById("settings-esphome-runtime-stats");
  if (
    !(shell instanceof HTMLElement) ||
    !(head instanceof HTMLElement) ||
    !(satellitesHost instanceof HTMLElement) ||
    !(addHost instanceof HTMLElement) ||
    !(firmwareHost instanceof HTMLElement) ||
    !(speakerIdHost instanceof HTMLElement)
  ) {
      return;
  }
  const targetPanel = normalizeEspHomeRuntimePanel(panel || shell.dataset.esphomeActiveTab || "satellites");
  const alreadyLoaded =
    String(shell.dataset.runtimeLoaded || "").trim() === "1" &&
    String(shell.dataset.runtimePanel || "").trim() === targetPanel;
  if (!force && alreadyLoaded) {
    return;
  }
  if (!force && state.esphomeRuntimeLoadPromise) {
    return state.esphomeRuntimeLoadPromise;
  }

  shell.dataset.coreActionEndpoint = "/api/settings/esphome/runtime/action";
  shell.dataset.coreRefreshScope = "esphome-runtime";
  shell.dataset.coreKey = "esphome";
  shell.dataset.runtimeLoaded = "loading";
  shell.dataset.runtimePanel = targetPanel;
  head.innerHTML = renderNotice(force ? "Refreshing ESPHome runtime..." : "Loading ESPHome runtime...");
  if (targetPanel === "satellites") {
    satellitesHost.innerHTML = renderNotice("Loading satellites...");
    addHost.innerHTML = renderNotice("Loading add form...");
  } else if (targetPanel === "firmware") {
    firmwareHost.innerHTML = renderNotice("Loading firmware builder...");
  } else if (targetPanel === "speakerid") {
    speakerIdHost.innerHTML = renderNotice("Loading Speaker ID...");
  } else if (targetPanel === "stats" && statsHost instanceof HTMLElement) {
    statsHost.innerHTML = renderNotice("Loading stats...");
  }
  const requestSeq = ++state.esphomeRuntimeRequestSeq;

  const request = fetchEspHomeRuntimePayload(targetPanel)
    .then((result) => {
      if (requestSeq !== state.esphomeRuntimeRequestSeq) {
        return;
      }
      const tabSpec =
        result?.tab && typeof result.tab === "object"
          ? result.tab
          : { core_key: "esphome", label: "ESPHome", surface_kind: "esphome" };
      const payload = result?.payload && typeof result.payload === "object" ? result.payload : result;
      const body = payload && typeof payload === "object" ? payload : {};
      const ui = body?.ui && typeof body.ui === "object" ? body.ui : {};
      const itemForms = Array.isArray(ui?.item_forms) ? ui.item_forms : [];
      const satelliteItems = itemForms.filter((item) => String(item?.group || "").trim().toLowerCase() === "satellite");
      const addForm = ui?.add_form && typeof ui.add_form === "object" ? ui.add_form : {};
      const summary = String(body.summary || "").trim();
      const headerStats = Array.isArray(body.header_stats) ? body.header_stats : [];
      const statsSections = Array.isArray(body.stats_sections) ? body.stats_sections : [];
      const statsTables = Array.isArray(body.stats_tables) ? body.stats_tables : [];
      const firmware = body?.firmware && typeof body.firmware === "object" ? body.firmware : {};
      const speakerId = body?.speaker_id && typeof body.speaker_id === "object" ? body.speaker_id : {};
      const emptyMessage = String(body.empty_message || "No satellites discovered yet.").trim();
      const coreKey = String(tabSpec?.core_key || "esphome").trim() || "esphome";

      head.innerHTML = renderEspHomeRuntimeHeader({
        title: String(ui?.title || tabSpec?.label || "Tater Voice"),
        summary,
        stats: headerStats,
        coreKey,
      });
      if (targetPanel === "satellites") {
        satellitesHost.innerHTML = satelliteItems.length
          ? `<div class="core-tab-items core-tab-items-group-satellite">${satelliteItems
              .map((item) => renderEspHomeSatelliteCard(item, coreKey))
              .join("")}</div>`
          : renderNotice(emptyMessage);
        addHost.innerHTML = renderEspHomeAddPanel(addForm, coreKey);
      }
      if (targetPanel === "firmware") {
        state.esphomeFirmwarePayload = firmware;
        state.esphomeFirmwareSelection = normalizeEspHomeFirmwareSelection(firmware);
        firmwareHost.innerHTML = renderEspHomeFirmwarePanel(firmware, coreKey);
      }
      if (targetPanel === "speakerid" && speakerIdHost instanceof HTMLElement) {
        state.esphomeSpeakerIdPayload = speakerId;
        speakerIdHost.innerHTML = renderEspHomeSpeakerIdPanel(speakerId, coreKey);
      }
      if (targetPanel === "stats" && statsHost instanceof HTMLElement) {
        statsHost.innerHTML = renderEspHomeStatsPanel(statsSections, statsTables);
      }
      shell.dataset.runtimeLoaded = "1";
      bindCoreTabManagers();
    })
    .catch((error) => {
      if (requestSeq !== state.esphomeRuntimeRequestSeq) {
        return;
      }
      const message = error instanceof Error ? error.message : String(error || "Failed to load ESPHome runtime.");
      head.innerHTML = renderNotice(message);
      if (targetPanel === "satellites") {
        satellitesHost.innerHTML = renderNotice(message);
        addHost.innerHTML = renderNotice(message);
      } else if (targetPanel === "firmware") {
        firmwareHost.innerHTML = renderNotice(message);
      } else if (targetPanel === "speakerid" && speakerIdHost instanceof HTMLElement) {
        speakerIdHost.innerHTML = renderNotice(message);
      } else if (targetPanel === "stats" && statsHost instanceof HTMLElement) {
        statsHost.innerHTML = renderNotice(message);
      }
      if (targetPanel === "firmware") {
        state.esphomeFirmwarePayload = null;
      } else if (targetPanel === "speakerid") {
        state.esphomeSpeakerIdPayload = null;
      }
      shell.dataset.runtimeLoaded = "error";
    })
    .finally(() => {
      if (state.esphomeRuntimeLoadPromise === request) {
        state.esphomeRuntimeLoadPromise = null;
      }
    });

  state.esphomeRuntimeLoadPromise = request;
  return request;
}

async function refreshEspHomeRuntimeInPlace() {
  await runEspHomeRefreshAction();
  await ensureEspHomeRuntimeLoaded({ force: true, panel: getActiveEspHomeRuntimePanel() });
}

async function reloadEspHomeRuntimePayloadOnly() {
  await ensureEspHomeRuntimeLoaded({ force: true, panel: getActiveEspHomeRuntimePanel() });
}

function getCoreTabSpec(tabName = "") {
  const key = String(tabName || "").trim();
  if (!key) {
    return null;
  }
  const catalog = state.coreTabSpecs && typeof state.coreTabSpecs === "object" ? state.coreTabSpecs : {};
  return catalog[key] || null;
}

async function fetchCoreTabPayload(coreKey, { force = false } = {}) {
  const key = String(coreKey || "").trim();
  if (!key) {
    throw new Error("Missing core tab key.");
  }
  if (!force && state.coreTabPayloadCache && Object.prototype.hasOwnProperty.call(state.coreTabPayloadCache, key)) {
    return state.coreTabPayloadCache[key];
  }
  if (state.coreTabLoadPromises?.[key]) {
    return state.coreTabLoadPromises[key];
  }
  if (force && state.coreTabPayloadCache) {
    delete state.coreTabPayloadCache[key];
  }
  const request = api(`/api/cores/${encodeURIComponent(key)}/tab`)
    .then((payload) => {
      state.coreTabPayloadCache[key] = payload;
      return payload;
    })
    .finally(() => {
      if (state.coreTabLoadPromises?.[key] === request) {
        delete state.coreTabLoadPromises[key];
      }
    });
  state.coreTabLoadPromises[key] = request;
  return request;
}

async function ensureCoreTopTabLoaded(tabName = "", { force = false } = {}) {
  const targetTab = String(tabName || "").trim();
  if (!targetTab || targetTab === "manage") {
    return;
  }
  const panel = Array.from(document.querySelectorAll(".core-top-tab-panel[data-core-tab-panel]")).find(
    (entry) => String(entry?.dataset?.coreTabPanel || "").trim() === targetTab
  );
  if (!(panel instanceof HTMLElement)) {
    return;
  }
  const tabSpec = getCoreTabSpec(targetTab) || { core_key: targetTab, label: targetTab };
  const alreadyLoaded = String(panel.dataset.coreTabLoaded || "").trim() === "1";
  if (!force && alreadyLoaded && Object.prototype.hasOwnProperty.call(state.coreTabPayloadCache || {}, targetTab)) {
    return;
  }

  panel.dataset.coreTabLoaded = "loading";
  panel.innerHTML = renderCoreTabPending(tabSpec, force ? "Refreshing..." : "Loading...");

  try {
    const payload = await fetchCoreTabPayload(targetTab, { force });
    panel.innerHTML = renderCoreTabPayload(payload, tabSpec);
    panel.dataset.coreTabLoaded = "1";
    bindCoreTabManagers();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error || "Failed to load this core tab.");
    panel.innerHTML = renderCoreTabPayload({ error: message }, tabSpec);
    panel.dataset.coreTabLoaded = "error";
    bindCoreTabManagers();
  }
}

function persistCoreTabFromNode(node) {
  const panel = node?.closest("[data-core-tab-panel]");
  const tabName = String(panel?.dataset?.coreTabPanel || "").trim();
  if (tabName) {
    persistCoreTopTab(tabName);
  }
  return tabName;
}

async function refreshCoreTabInPlace(tabName = "") {
  const targetTab = String(tabName || getActiveCoreTopTab() || "manage").trim() || "manage";
  if (targetTab === "manage") {
    await refreshShopManagerInPlace("cores", getActiveShopTab("cores") || "installed");
    activateCoreTopTab("manage");
    return;
  }
  await ensureCoreTopTabLoaded(targetTab, { force: true });
  if (getActiveCoreTopTab() !== targetTab) {
    activateCoreTopTab(targetTab);
  }
}

async function refreshCoreManagerInPlace(node, fallbackTab = "") {
  const scope = resolveCoreRefreshScope(node);
  if (scope === "esphome-runtime") {
    await refreshEspHomeRuntimeInPlace();
    return "esphome-runtime";
  }
  const targetTab = String(fallbackTab || persistCoreTabFromNode(node) || "").trim();
  await refreshCoreTabInPlace(targetTab);
  return targetTab || getActiveCoreTopTab() || "manage";
}

function updateEspHomeSatelliteSensorRows(card, entityRows) {
  if (!(card instanceof HTMLElement)) {
    return;
  }
  const summaryMain = card.querySelector(".core-satellite-summary-main");
  if (!(summaryMain instanceof HTMLElement)) {
    return;
  }
  const rows = Array.isArray(entityRows) ? entityRows : [];
  const title = rows.length ? "Live Entities" : "No Entities";
  const html = renderEspHomeSensorRows(rows, title, true);
  const current = summaryMain.querySelector(".core-satellite-sensors");
  if (html) {
    if (current instanceof HTMLElement) {
      current.outerHTML = html;
    } else {
      summaryMain.insertAdjacentHTML("beforeend", html);
    }
  } else if (current instanceof HTMLElement) {
    current.remove();
  }
  bindEspHomeEntityControls(card);
}

function bindEspHomeEntityControls(root = document) {
  const executeEntityAction = async (controlEl, payload, { revert } = {}) => {
    const card = controlEl?.closest?.(".esphome-satellite-card");
    const coreKey = String(card?.dataset?.coreKey || "esphome").trim();
    const selector = decodeCoreManagerId(card?.dataset?.coreItemId || "");
    if (!(card instanceof HTMLElement) || !coreKey || !selector) {
      throw new Error("Missing satellite context.");
    }
    const result = await runCoreManagerAction(card, coreKey, "voice_entity_command", {
      id: selector,
      selector,
      ...payload,
    });
    updateEspHomeSatelliteSensorRows(card, result?.entity_rows);
    setCoreManagerStatus(card, String(result?.message || "Updated."));
    return result;
  };

  root.querySelectorAll(".esphome-entity-toggle").forEach((input) => {
    if (!(input instanceof HTMLInputElement) || input.dataset.esphomeBound === "1") {
      return;
    }
    input.dataset.esphomeBound = "1";
    input.addEventListener("change", async () => {
      const previous = !input.checked;
      input.disabled = true;
      try {
        await executeEntityAction(input, {
          entity_key: String(input.dataset.esphomeEntityKey || "").trim(),
          command: String(input.dataset.esphomeEntityCommand || "").trim(),
          value: input.checked,
          entity_label: String(input.dataset.esphomeEntityLabel || "").trim(),
        });
      } catch (error) {
        input.checked = previous;
        const message = String(error?.message || "Failed to update entity.");
        setCoreManagerStatus(input.closest(".esphome-satellite-card"), `Failed: ${message}`);
        showToast(`Failed: ${message}`, "error", 3200);
      } finally {
        if (document.body.contains(input)) {
          input.disabled = false;
        }
      }
    });
  });

  root.querySelectorAll(".esphome-entity-color").forEach((input) => {
    if (!(input instanceof HTMLInputElement) || input.dataset.esphomeBound === "1") {
      return;
    }
    input.dataset.esphomeBound = "1";
    input.dataset.esphomeLastValue = input.value;
    input.addEventListener("change", async () => {
      const previous = String(input.dataset.esphomeLastValue || input.value || "#ff9b45").trim() || "#ff9b45";
      input.disabled = true;
      try {
        await executeEntityAction(input, {
          entity_key: String(input.dataset.esphomeEntityKey || "").trim(),
          command: String(input.dataset.esphomeEntityCommand || "").trim(),
          value: true,
          state: true,
          color: input.value,
          entity_label: String(input.dataset.esphomeEntityLabel || "").trim(),
        });
      } catch (error) {
        input.value = previous;
        const message = String(error?.message || "Failed to update light color.");
        setCoreManagerStatus(input.closest(".esphome-satellite-card"), `Failed: ${message}`);
        showToast(`Failed: ${message}`, "error", 3200);
      } finally {
        input.dataset.esphomeLastValue = input.value;
        if (document.body.contains(input)) {
          input.disabled = false;
        }
      }
    });
  });

  root.querySelectorAll(".esphome-entity-button").forEach((button) => {
    if (!(button instanceof HTMLButtonElement) || button.dataset.esphomeBound === "1") {
      return;
    }
    button.dataset.esphomeBound = "1";
    button.addEventListener("click", async () => {
      button.disabled = true;
      try {
        await executeEntityAction(button, {
          entity_key: String(button.dataset.esphomeEntityKey || "").trim(),
          command: String(button.dataset.esphomeEntityCommand || "").trim(),
          entity_label: String(button.dataset.esphomeEntityLabel || "").trim(),
        });
      } catch (error) {
        const message = String(error?.message || "Failed to run entity action.");
        setCoreManagerStatus(button.closest(".esphome-satellite-card"), `Failed: ${message}`);
        showToast(`Failed: ${message}`, "error", 3200);
      } finally {
        if (document.body.contains(button)) {
          button.disabled = false;
        }
      }
    });
  });

  root.querySelectorAll(".esphome-entity-select").forEach((select) => {
    if (!(select instanceof HTMLSelectElement) || select.dataset.esphomeBound === "1") {
      return;
    }
    select.dataset.esphomeBound = "1";
    select.dataset.esphomeLastValue = select.value;
    select.addEventListener("change", async () => {
      const previous = String(select.dataset.esphomeLastValue || "").trim();
      select.disabled = true;
      try {
        await executeEntityAction(select, {
          entity_key: String(select.dataset.esphomeEntityKey || "").trim(),
          command: String(select.dataset.esphomeEntityCommand || "").trim(),
          value: select.value,
          entity_label: String(select.dataset.esphomeEntityLabel || "").trim(),
        });
      } catch (error) {
        select.value = previous;
        const message = String(error?.message || "Failed to update entity.");
        setCoreManagerStatus(select.closest(".esphome-satellite-card"), `Failed: ${message}`);
        showToast(`Failed: ${message}`, "error", 3200);
      } finally {
        select.dataset.esphomeLastValue = select.value;
        if (document.body.contains(select)) {
          select.disabled = false;
        }
      }
    });
  });

  root.querySelectorAll(".esphome-entity-number-set").forEach((button) => {
    if (!(button instanceof HTMLButtonElement) || button.dataset.esphomeBound === "1") {
      return;
    }
    button.dataset.esphomeBound = "1";
    button.addEventListener("click", async () => {
      const wrap = button.closest(".core-satellite-inline-set");
      const input = wrap?.querySelector?.(".esphome-entity-number");
      if (!(input instanceof HTMLInputElement)) {
        return;
      }
      const previous = input.dataset.esphomeLastValue || input.value;
      button.disabled = true;
      input.disabled = true;
      try {
        await executeEntityAction(button, {
          entity_key: String(button.dataset.esphomeEntityKey || "").trim(),
          command: String(button.dataset.esphomeEntityCommand || "").trim(),
          value: input.value,
          entity_label: String(button.dataset.esphomeEntityLabel || "").trim(),
        });
      } catch (error) {
        input.value = String(previous || "");
        const message = String(error?.message || "Failed to update entity.");
        setCoreManagerStatus(button.closest(".esphome-satellite-card"), `Failed: ${message}`);
        showToast(`Failed: ${message}`, "error", 3200);
      } finally {
        input.dataset.esphomeLastValue = input.value;
        if (document.body.contains(button)) {
          button.disabled = false;
        }
        if (document.body.contains(input)) {
          input.disabled = false;
        }
      }
    });
  });

  root.querySelectorAll(".esphome-entity-number").forEach((input) => {
    if (!(input instanceof HTMLInputElement)) {
      return;
    }
    input.dataset.esphomeLastValue = input.value;
  });

  root.querySelectorAll(".esphome-entity-text-set").forEach((button) => {
    if (!(button instanceof HTMLButtonElement) || button.dataset.esphomeBound === "1") {
      return;
    }
    button.dataset.esphomeBound = "1";
    button.addEventListener("click", async () => {
      const wrap = button.closest(".core-satellite-inline-set");
      const input = wrap?.querySelector?.(".esphome-entity-text");
      if (!(input instanceof HTMLInputElement)) {
        return;
      }
      const previous = input.dataset.esphomeLastValue || input.value;
      button.disabled = true;
      input.disabled = true;
      try {
        await executeEntityAction(button, {
          entity_key: String(button.dataset.esphomeEntityKey || "").trim(),
          command: String(button.dataset.esphomeEntityCommand || "").trim(),
          value: input.value,
          entity_label: String(button.dataset.esphomeEntityLabel || "").trim(),
        });
      } catch (error) {
        input.value = String(previous || "");
        const message = String(error?.message || "Failed to update entity.");
        setCoreManagerStatus(button.closest(".esphome-satellite-card"), `Failed: ${message}`);
        showToast(`Failed: ${message}`, "error", 3200);
      } finally {
        input.dataset.esphomeLastValue = input.value;
        if (document.body.contains(button)) {
          button.disabled = false;
        }
        if (document.body.contains(input)) {
          input.disabled = false;
        }
      }
    });
  });

  root.querySelectorAll(".esphome-entity-text").forEach((input) => {
    if (!(input instanceof HTMLInputElement) || input.dataset.esphomeBound === "1") {
      return;
    }
    input.dataset.esphomeBound = "1";
    input.dataset.esphomeLastValue = input.value;
    input.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") {
        return;
      }
      event.preventDefault();
      const wrap = input.closest(".core-satellite-inline-set");
      const button = wrap?.querySelector?.(".esphome-entity-text-set");
      if (button instanceof HTMLButtonElement && !button.disabled) {
        button.click();
      }
    });
  });
}

function rerenderEspHomeFirmwarePanel(root = document) {
  const host =
    root instanceof HTMLElement
      ? root.querySelector("#settings-esphome-runtime-firmware")
      : document.getElementById("settings-esphome-runtime-firmware");
  if (!(host instanceof HTMLElement)) {
    return;
  }
  const firmware = state.esphomeFirmwarePayload && typeof state.esphomeFirmwarePayload === "object" ? state.esphomeFirmwarePayload : null;
  if (!firmware) {
    return;
  }
  state.esphomeFirmwareSelection = normalizeEspHomeFirmwareSelection(firmware);
  const shell = document.getElementById("settings-esphome-shell");
  const coreKey = String(shell?.dataset?.coreKey || "esphome").trim() || "esphome";
  host.innerHTML = renderEspHomeFirmwarePanel(firmware, coreKey);
  bindCoreTabManagers();
}

function bindEspHomeFirmwareSelectors(root = document) {
  root.querySelectorAll(".esphome-firmware-card select[data-core-field-key]").forEach((input) => {
    if (!(input instanceof HTMLSelectElement)) {
      return;
    }
    const key = String(input.dataset.coreFieldKey || "").trim();
    if (!["template_key", "selector", "wake_word_catalog"].includes(key)) {
      return;
    }
    if (input.dataset.esphomeFirmwareSelectBound === "1") {
      return;
    }
    input.dataset.esphomeFirmwareSelectBound = "1";
    input.addEventListener("change", () => {
      const card = input.closest(".esphome-firmware-card");
      if (key === "wake_word_catalog") {
        syncEspHomeFirmwareWakeWordCatalog(card, { fromPicker: true });
        return;
      }
      captureEspHomeFirmwareDraft(card);
      const templateInput = card?.querySelector?.('select[data-core-field-key="template_key"]');
      const selectorInput = card?.querySelector?.('select[data-core-field-key="selector"]');
      state.esphomeFirmwareSelection = {
        templateKey: String(templateInput?.value || "").trim(),
        selector: String(selectorInput?.value || "").trim(),
      };
      rerenderEspHomeFirmwarePanel(document);
    });
  });

  root.querySelectorAll('.esphome-firmware-card input[data-core-field-key="wake_word_model_url"]').forEach((input) => {
    if (!(input instanceof HTMLInputElement) || input.dataset.esphomeFirmwareWakeWordBound === "1") {
      return;
    }
    input.dataset.esphomeFirmwareWakeWordBound = "1";
    const sync = () => {
      const card = input.closest(".esphome-firmware-card");
      syncEspHomeFirmwareWakeWordCatalog(card, { fromPicker: false });
    };
    input.addEventListener("input", sync);
    input.addEventListener("change", sync);
    sync();
  });
}

function setEspHomeFirmwareCardBusy(card, busy) {
  if (!(card instanceof HTMLElement)) {
    return;
  }
  card.querySelectorAll(".esphome-firmware-action").forEach((button) => {
    if (!(button instanceof HTMLButtonElement)) {
      return;
    }
    if (busy) {
      button.dataset.esphomeFirmwareDisabledBefore = button.disabled ? "1" : "0";
      button.disabled = true;
      return;
    }
    const wasDisabled = String(button.dataset.esphomeFirmwareDisabledBefore || "").trim() === "1";
    button.disabled = wasDisabled;
    delete button.dataset.esphomeFirmwareDisabledBefore;
  });
}

function openEspHomeFirmwareFlashViewer(card, coreKey) {
  if (!(card instanceof HTMLElement)) {
    return;
  }
  const selector = String(card.dataset?.firmwareSelector || decodeCoreManagerId(card.dataset?.coreItemId || "")).trim();
  const templateKey = String(card.dataset?.firmwareTemplateKey || "").trim();
  const templateSelect = card.querySelector('select[data-core-field-key="template_key"]');
  const selectorSelect = card.querySelector('select[data-core-field-key="selector"]');
  const templateLabel =
    String(templateSelect?.selectedOptions?.[0]?.textContent || templateKey || "Firmware").trim() || "Firmware";
  const deviceLabel =
    String(selectorSelect?.selectedOptions?.[0]?.textContent || selector || "ESPHome Device").trim() || "ESPHome Device";
  const values = collectCoreManagerValues(card);
  if (!selector || !templateKey || !coreKey) {
    showToast("Pick a firmware template and connected device before flashing.", "error", 3200);
    return;
  }

  captureEspHomeFirmwareDraft(card);
  setEspHomeFirmwareCardBusy(card, true);
  setCoreManagerStatus(card, "Opening firmware flash log...");

  let stopped = false;
  let sessionId = "";
  let cursor = 0;
  let pollTimer = 0;
  let logConsole = null;
  let statusNode = null;
  let modalDialog = null;

  const deriveLogTone = (entry) => {
    const explicitLevel = String(entry?.level || "").trim().toLowerCase();
    if (explicitLevel) {
      if (["error", "err", "danger"].includes(explicitLevel)) {
        return "error";
      }
      if (["warn", "warning"].includes(explicitLevel)) {
        return "warn";
      }
      if (["debug", "trace", "verbose", "very_verbose"].includes(explicitLevel)) {
        return "debug";
      }
      if (["config", "info"].includes(explicitLevel)) {
        return "info";
      }
    }
    const text = String(entry?.message || entry?.display || "").trim();
    const bracketMatch = text.match(/^\[[^\]]+\]\[([A-Z])\]/);
    const token = bracketMatch?.[1] || "";
    if (token === "E") {
      return "error";
    }
    if (token === "W") {
      return "warn";
    }
    if (token === "D" || token === "V") {
      return "debug";
    }
    return "info";
  };

  const renderConsoleLine = (entry) => {
    if (!(logConsole instanceof HTMLElement)) {
      return;
    }
    const lineEl = document.createElement("div");
    const tone = deriveLogTone(entry);
    lineEl.className = `voice-log-line tone-${tone}`;
    const timeText = String(entry?.time || "").trim();
    if (timeText) {
      const timeEl = document.createElement("span");
      timeEl.className = "voice-log-time";
      timeEl.textContent = timeText;
      lineEl.appendChild(timeEl);
    }
    const levelToken = (() => {
      const explicitLevel = String(entry?.level || "").trim().toLowerCase();
      if (explicitLevel) {
        return explicitLevel.replace(/_/g, " ").toUpperCase();
      }
      return tone.toUpperCase();
    })();
    const levelEl = document.createElement("span");
    levelEl.className = `voice-log-level tone-${tone}`;
    levelEl.textContent = levelToken;
    lineEl.appendChild(levelEl);

    const messageEl = document.createElement("span");
    messageEl.className = "voice-log-message";
    messageEl.textContent = String(entry?.display || entry?.message || "").trim();
    lineEl.appendChild(messageEl);
    logConsole.appendChild(lineEl);
  };

  const renderEntries = (entries, reset = false) => {
    if (!(logConsole instanceof HTMLElement)) {
      return;
    }
    const rows = Array.isArray(entries)
      ? entries.filter((entry) => String(entry?.display || entry?.message || "").trim())
      : [];
    const shouldStick =
      reset ||
      logConsole.scrollHeight - logConsole.scrollTop - logConsole.clientHeight < 28;
    if (reset) {
      logConsole.innerHTML = "";
    }
    if (!rows.length && reset) {
      const emptyEl = document.createElement("div");
      emptyEl.className = "voice-log-empty";
      emptyEl.textContent = "Waiting for ESPHome build output...";
      logConsole.appendChild(emptyEl);
    } else if (rows.length) {
      rows.forEach((entry) => renderConsoleLine(entry));
    }
    if (shouldStick) {
      logConsole.scrollTop = logConsole.scrollHeight;
    }
  };

  const refreshBehindModal = async () => {
    try {
      await reloadEspHomeRuntimePayloadOnly();
    } catch (_error) {
      // Ignore background refresh failures while leaving the log viewer.
    }
  };

  const stopViewer = async () => {
    if (stopped) {
      return;
    }
    stopped = true;
    if (pollTimer) {
      window.clearTimeout(pollTimer);
      pollTimer = 0;
    }
    try {
      if (sessionId) {
        await runCoreManagerAction(card, coreKey, "voice_firmware_flash_stop", {
          session_id: sessionId,
          id: sessionId,
        });
      }
    } catch (_error) {
      // Ignore cleanup failures while closing the firmware log viewer.
    } finally {
      setEspHomeFirmwareCardBusy(card, false);
      void refreshBehindModal();
    }
  };

  const schedulePoll = (delayMs = 1200) => {
    if (stopped || !sessionId) {
      return;
    }
    pollTimer = window.setTimeout(async () => {
      if (stopped || !sessionId) {
        return;
      }
      try {
        const result = await runCoreManagerAction(card, coreKey, "voice_firmware_flash_poll", {
          session_id: sessionId,
          id: sessionId,
          after_seq: cursor,
        });
        const entries = Array.isArray(result?.entries) ? result.entries : [];
        if (entries.length) {
          renderEntries(entries, false);
        }
        cursor = Number(result?.cursor || cursor || 0);
        if (statusNode instanceof HTMLElement) {
          statusNode.textContent =
            String(result?.status_text || result?.message || "").trim() || `Streaming firmware logs for ${deviceLabel}.`;
        }
        if (!boolFromAny(result?.active, true)) {
          const message = String(result?.message || result?.status_text || "").trim();
          setCoreManagerStatus(card, message || "Firmware session finished.");
          return;
        }
        const phase = String(result?.phase || "").trim();
        schedulePoll(phase === "awaiting_device_logs" ? 1500 : 1100);
      } catch (error) {
        if (statusNode instanceof HTMLElement) {
          statusNode.textContent = `Firmware log error: ${String(error?.message || "unknown error")}`;
        }
        schedulePoll(2500);
      }
    }, delayMs);
  };

  openRuntimeSettingsModal({
    title: `${templateLabel} Build + Flash`,
    meta: [deviceLabel, selector].filter(Boolean).join(" • "),
    fields: [
      {
        key: "live_log_feed",
        label: "Firmware Log",
        type: "textarea",
        value: "",
        description: "ESPHome build output, upload progress, and live device logs stay in this window.",
      },
    ],
    onOpen: async ({ modal, fieldsEl, statusEl }) => {
      statusNode = statusEl instanceof HTMLElement ? statusEl : null;
      modalDialog = modal?.querySelector(".runtime-settings-dialog") || null;
      modal?.classList.add("voice-log-modal");
      modalDialog?.classList.add("runtime-settings-dialog-log");
      fieldsEl?.classList.add("runtime-settings-fields-log");
      const logArea = fieldsEl?.querySelector('[data-setting-key="live_log_feed"]') || null;
      if (logArea instanceof HTMLTextAreaElement) {
        logArea.readOnly = true;
        logArea.spellcheck = false;
        logArea.classList.add("voice-log-source-textarea");
        const label = logArea.closest("label");
        const consoleEl = document.createElement("div");
        consoleEl.className = "voice-log-console";
        consoleEl.setAttribute("role", "log");
        consoleEl.setAttribute("aria-live", "polite");
        consoleEl.setAttribute("aria-label", `${deviceLabel} firmware log`);
        logConsole = consoleEl;
        if (label instanceof HTMLElement) {
          label.classList.add("voice-log-field");
          label.appendChild(consoleEl);
        }
      }
      if (statusNode instanceof HTMLElement) {
        statusNode.textContent = `Starting firmware flash for ${deviceLabel}...`;
        statusNode.classList.add("voice-log-status");
      }
      try {
        const result = await runCoreManagerAction(card, coreKey, "voice_firmware_flash_start", {
          id: selector,
          selector,
          template_key: templateKey,
          values,
        });
        sessionId = String(result?.session_id || "").trim();
        cursor = Number(result?.cursor || 0);
        delete state.esphomeFirmwareDrafts[templateKey];
        renderEntries(Array.isArray(result?.entries) ? result.entries : [], true);
        if (statusNode instanceof HTMLElement) {
          statusNode.textContent =
            String(result?.status_text || result?.message || "").trim() || `Streaming firmware logs for ${deviceLabel}.`;
        }
        setCoreManagerStatus(card, "Firmware flash in progress...");
        schedulePoll(900);
      } catch (error) {
        const message = String(error?.message || "unknown error");
        renderEntries([{ display: `Failed to start firmware flash: ${message}`, level: "error" }], true);
        if (statusNode instanceof HTMLElement) {
          statusNode.textContent = `Firmware flash failed to start: ${message}`;
        }
        setEspHomeFirmwareCardBusy(card, false);
        setCoreManagerStatus(card, `Failed: ${message}`);
        showToast(`Failed: ${message}`, "error", 3600);
      }
    },
    onClose: ({ modal, fieldsEl, statusEl }) => {
      modal?.classList.remove("voice-log-modal");
      modalDialog?.classList.remove("runtime-settings-dialog-log");
      fieldsEl?.classList.remove("runtime-settings-fields-log");
      if (statusEl instanceof HTMLElement) {
        statusEl.classList.remove("voice-log-status");
      }
      void stopViewer();
    },
  });
}

function bindEspHomeFirmwareActions(root = document) {
  root.querySelectorAll(".esphome-firmware-action").forEach((button) => {
    if (!(button instanceof HTMLButtonElement) || button.dataset.esphomeFirmwareBound === "1") {
      return;
    }
    button.dataset.esphomeFirmwareBound = "1";
    button.addEventListener("click", async (event) => {
      const actionButton = event.currentTarget;
      const card = actionButton?.closest?.(".esphome-firmware-card");
      const coreKey = String(card?.dataset?.coreKey || "esphome").trim();
      const selector = String(card?.dataset?.firmwareSelector || decodeCoreManagerId(card?.dataset?.coreItemId || "")).trim();
      const templateKey = String(card?.dataset?.firmwareTemplateKey || "").trim();
      const action = String(actionButton?.dataset?.firmwareAction || "").trim();
      const title = String(actionButton?.dataset?.firmwareTitle || "Working...").trim() || "Working...";
      const workingText = String(actionButton?.dataset?.firmwareWorking || "Working...").trim() || "Working...";
      const successText = String(actionButton?.dataset?.firmwareSuccess || "Completed.").trim() || "Completed.";
      const errorPrefix = String(actionButton?.dataset?.firmwareError || "Firmware action failed").trim() || "Firmware action failed";
      if (!(card instanceof HTMLElement) || !coreKey || !selector || !action) {
        return;
      }

      if (action === "voice_firmware_flash_start") {
        openEspHomeFirmwareFlashViewer(card, coreKey);
        return;
      }

      const values = collectCoreManagerValues(card);
      captureEspHomeFirmwareDraft(card);
      setCoreManagerStatus(card, workingText);
      actionButton.disabled = true;
      try {
        const result = await runActionWithProgress(
          {
            title,
            detail: selector,
            workingText,
            successText,
            errorPrefix,
          },
          () =>
            runCoreManagerAction(card, coreKey, action, {
              id: selector,
              selector,
              template_key: templateKey,
              values,
            })
        );
        if (action !== "voice_firmware_clean") {
          delete state.esphomeFirmwareDrafts[templateKey];
        }
        await reloadEspHomeRuntimePayloadOnly();
        const message = String(result?.message || successText).trim() || successText;
        showToast(message);
      } catch (error) {
        const message = String(error?.message || "Firmware action failed.");
        setCoreManagerStatus(card, `Failed: ${message}`);
        showToast(`Failed: ${message}`, "error", 3600);
      } finally {
        if (document.body.contains(actionButton)) {
          actionButton.disabled = false;
        }
      }
    });
  });
}

function rerenderEspHomeSpeakerIdPanel(root = document) {
  const host =
    root instanceof HTMLElement
      ? root.querySelector("#settings-esphome-runtime-speakerid")
      : document.getElementById("settings-esphome-runtime-speakerid");
  if (!(host instanceof HTMLElement)) {
    return;
  }
  const shell = document.getElementById("settings-esphome-shell");
  const coreKey = String(host.dataset.coreKey || shell?.dataset?.coreKey || "esphome").trim() || "esphome";
  const payload =
    state.esphomeSpeakerIdPayload && typeof state.esphomeSpeakerIdPayload === "object" ? state.esphomeSpeakerIdPayload : {};
  host.dataset.coreKey = coreKey;
  host.innerHTML = renderEspHomeSpeakerIdPanel(payload, coreKey);
  bindEspHomeSpeakerIdActions(document);
}

function bindEspHomeSpeakerIdActions(root = document) {
  const runAction = async (host, action, payload, successText) => {
    const statusHost = host instanceof HTMLElement ? host : null;
    setCoreManagerStatus(statusHost, "Working...");
    try {
      const result = await api("/api/settings/esphome/runtime/action", {
        method: "POST",
        body: JSON.stringify({ action, payload }),
      });
      if (result?.speaker_id && typeof result.speaker_id === "object") {
        state.esphomeSpeakerIdPayload = result.speaker_id;
      }
      rerenderEspHomeSpeakerIdPanel(document);
      const message = String(result?.message || successText).trim() || successText;
      showToast(message);
      return result;
    } catch (error) {
      const message = String(error?.message || "Speaker ID action failed.");
      setCoreManagerStatus(statusHost, `Failed: ${message}`);
      showToast(`Failed: ${message}`, "error", 3600);
      throw error;
    }
  };

  root.querySelectorAll(".esphome-speaker-id-settings-save").forEach((button) => {
    if (!(button instanceof HTMLButtonElement) || button.dataset.esphomeSpeakerIdBound === "1") {
      return;
    }
    button.dataset.esphomeSpeakerIdBound = "1";
    button.addEventListener("click", async (event) => {
      const card = event.currentTarget?.closest?.(".esphome-speaker-id-settings-card");
      const values = collectCoreManagerValues(card);
      await runAction(card, "speaker_id_settings_save", { values }, "Speaker ID settings saved.");
    });
  });

  root.querySelectorAll(".esphome-speaker-id-cancel").forEach((button) => {
    if (!(button instanceof HTMLButtonElement) || button.dataset.esphomeSpeakerIdBound === "1") {
      return;
    }
    button.dataset.esphomeSpeakerIdBound = "1";
    button.addEventListener("click", async (event) => {
      const card = event.currentTarget?.closest?.(".esphome-speaker-id-pending-card");
      await runAction(card, "speaker_id_pending_cancel", {}, "Pending speaker capture canceled.");
    });
  });

  root.querySelectorAll(".esphome-speaker-id-create").forEach((button) => {
    if (!(button instanceof HTMLButtonElement) || button.dataset.esphomeSpeakerIdBound === "1") {
      return;
    }
    button.dataset.esphomeSpeakerIdBound = "1";
    button.addEventListener("click", async (event) => {
      const card = event.currentTarget?.closest?.(".esphome-speaker-id-create-card");
      const values = collectCoreManagerValues(card);
      await runAction(card, "speaker_id_speaker_create", { values }, "Speaker created.");
    });
  });

  root.querySelectorAll(".esphome-speaker-id-save").forEach((button) => {
    if (!(button instanceof HTMLButtonElement) || button.dataset.esphomeSpeakerIdBound === "1") {
      return;
    }
    button.dataset.esphomeSpeakerIdBound = "1";
    button.addEventListener("click", async (event) => {
      const card = event.currentTarget?.closest?.(".esphome-speaker-id-speaker-card");
      const speakerId = String(card?.dataset?.speakerId || "").trim();
      if (!speakerId) {
        return;
      }
      const values = collectCoreManagerValues(card);
      await runAction(card, "speaker_id_speaker_save", { speaker_id: speakerId, values }, "Speaker saved.");
    });
  });

  root.querySelectorAll(".esphome-speaker-id-capture").forEach((button) => {
    if (!(button instanceof HTMLButtonElement) || button.dataset.esphomeSpeakerIdBound === "1") {
      return;
    }
    button.dataset.esphomeSpeakerIdBound = "1";
    button.addEventListener("click", async (event) => {
      const card = event.currentTarget?.closest?.(".esphome-speaker-id-speaker-card");
      const speakerId = String(card?.dataset?.speakerId || "").trim();
      if (!speakerId) {
        return;
      }
      const values = collectCoreManagerValues(card);
      await runAction(
        card,
        "speaker_id_enrollment_arm",
        { speaker_id: speakerId, values },
        "Speaker capture armed."
      );
    });
  });

  root.querySelectorAll(".esphome-speaker-id-delete").forEach((button) => {
    if (!(button instanceof HTMLButtonElement) || button.dataset.esphomeSpeakerIdBound === "1") {
      return;
    }
    button.dataset.esphomeSpeakerIdBound = "1";
    button.addEventListener("click", async (event) => {
      const card = event.currentTarget?.closest?.(".esphome-speaker-id-speaker-card");
      const speakerId = String(card?.dataset?.speakerId || "").trim();
      const title = String(card?.querySelector?.(".card-title")?.textContent || "this speaker").trim() || "this speaker";
      if (!speakerId || !window.confirm(`Delete ${title}?`)) {
        return;
      }
      await runAction(card, "speaker_id_speaker_delete", { speaker_id: speakerId }, "Speaker deleted.");
    });
  });
}

function bindCoreTabManagers() {
  bindCoreManagerTabs();
  bindCoreManagerSubtabs();
  bindCoreManagerSelectors();
  bindCoreManagerPagination();
  bindCoreManagerConditionalFields();
  bindCoreManagerDependentSelects();
  bindEspHomeEntityControls();
  bindEspHomeFirmwareSelectors();
  bindEspHomeFirmwareActions();
  bindEspHomeSpeakerIdActions();
  document.querySelectorAll(".core-tab-refresh-btn[data-core-tab-refresh]").forEach((button) => {
    if (!(button instanceof HTMLButtonElement)) {
      return;
    }
    if (button.dataset.coreTabRefreshBound === "1") {
      return;
    }
    button.dataset.coreTabRefreshBound = "1";
    button.addEventListener("click", async () => {
      const refreshScope = resolveCoreRefreshScope(button);
      const targetTab = String(getActiveCoreTopTab() || "").trim();
      if ((!refreshScope && (!targetTab || targetTab === "manage")) || button.disabled) {
        return;
      }
      const originalText = String(button.textContent || "Refresh");
      button.disabled = true;
      button.textContent = "Refreshing...";
      try {
        if (refreshScope === "esphome-runtime") {
          await refreshEspHomeRuntimeInPlace();
        } else {
          await refreshCoreTabInPlace(targetTab);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error || "Refresh failed");
        state.notice = `Refresh failed: ${message}`;
        renderNoticeBar();
        button.disabled = false;
        button.textContent = originalText;
      }
    });
  });
  document.querySelectorAll(".core-stats-controls-form[data-core-key][data-core-action]").forEach((form) => {
    if (!(form instanceof HTMLFormElement)) {
      return;
    }
    if (form.dataset.coreStatsControlsBound === "1") {
      return;
    }
    form.dataset.coreStatsControlsBound = "1";
    const coreKey = String(form.dataset.coreKey || "").trim();
    const action = String(form.dataset.coreAction || "").trim();
    if (!coreKey || !action) {
      return;
    }
    let saving = false;

    const saveNow = async () => {
      if (saving) {
        return;
      }
      saving = true;
      setCoreManagerStatus(form, "Saving...");
      try {
        const activeTab = persistCoreTabFromNode(form);
        const values = collectCoreManagerValues(form);
        await runCoreManagerAction(form, coreKey, action, { ...values, values });
        await refreshCoreManagerInPlace(form, activeTab);
      } catch (error) {
        setCoreManagerStatus(form, `Failed: ${error.message}`);
      } finally {
        saving = false;
      }
    };

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      await saveNow();
    });

    if (String(form.dataset.coreAutoSave || "1") !== "0") {
      form.querySelectorAll("[data-core-field-key]").forEach((input) => {
        if (!(input instanceof HTMLInputElement || input instanceof HTMLSelectElement || input instanceof HTMLTextAreaElement)) {
          return;
        }
        input.addEventListener("change", () => {
          void saveNow();
        });
      });
    }
  });
  const decodeCoreManagerPopupFields = (card) => {
    const encoded = String(card?.dataset?.coreItemPopupFields || "").trim();
    if (!encoded) {
      return [];
    }
    try {
      const decoded = decodeURIComponent(encoded);
      const parsed = JSON.parse(decoded);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_error) {
      return [];
    }
  };

  const decodeCoreManagerPopupConfig = (card) => {
    const encoded = String(card?.dataset?.coreItemPopupConfig || "").trim();
    if (!encoded) {
      return {};
    }
    try {
      const decoded = decodeURIComponent(encoded);
      const parsed = JSON.parse(decoded);
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch (_error) {
      return {};
    }
  };

  const openVoiceSatelliteLogViewer = (card, coreKey, popupTitle, popupFields, popupConfig) => {
    const selector = String(popupConfig?.selector || decodeCoreManagerId(card?.dataset?.coreItemId || "")).trim();
    const host = String(popupConfig?.host || "").trim();
    const name = String(popupConfig?.name || "").trim();
    if (!selector) {
      showToast("This satellite does not have a valid selector for log streaming.", "error", 3200);
      return;
    }

    let stopped = false;
    let cursor = 0;
    let pollTimer = 0;
    let logArea = null;
    let logConsole = null;
    let statusNode = null;
    let modalDialog = null;

    const deriveLogTone = (entry) => {
      const explicitLevel = String(entry?.level || "").trim().toLowerCase();
      if (explicitLevel) {
        if (["error", "err", "danger"].includes(explicitLevel)) {
          return "error";
        }
        if (["warn", "warning"].includes(explicitLevel)) {
          return "warn";
        }
        if (["debug", "trace", "verbose", "very_verbose"].includes(explicitLevel)) {
          return "debug";
        }
        if (["config", "info"].includes(explicitLevel)) {
          return "info";
        }
      }
      const text = String(entry?.message || entry?.display || "").trim();
      const bracketMatch = text.match(/^\[[^\]]+\]\[([A-Z])\]/);
      const token = bracketMatch?.[1] || "";
      if (token === "E") {
        return "error";
      }
      if (token === "W") {
        return "warn";
      }
      if (token === "D" || token === "V") {
        return "debug";
      }
      return "info";
    };

    const renderConsoleLine = (entry) => {
      if (!(logConsole instanceof HTMLElement)) {
        return;
      }
      const lineEl = document.createElement("div");
      const tone = deriveLogTone(entry);
      lineEl.className = `voice-log-line tone-${tone}`;
      const timeText = String(entry?.time || "").trim();
      if (timeText) {
        const timeEl = document.createElement("span");
        timeEl.className = "voice-log-time";
        timeEl.textContent = timeText;
        lineEl.appendChild(timeEl);
      }
      const levelToken = (() => {
        const explicitLevel = String(entry?.level || "").trim().toLowerCase();
        if (explicitLevel) {
          return explicitLevel.replace(/_/g, " ").toUpperCase();
        }
        return tone.toUpperCase();
      })();
      const levelEl = document.createElement("span");
      levelEl.className = `voice-log-level tone-${tone}`;
      levelEl.textContent = levelToken;
      lineEl.appendChild(levelEl);

      const messageEl = document.createElement("span");
      messageEl.className = "voice-log-message";
      messageEl.textContent = String(entry?.message || entry?.display || "").trim();
      lineEl.appendChild(messageEl);
      logConsole.appendChild(lineEl);
    };

    const renderEntries = (entries, reset = false) => {
      if (!(logConsole instanceof HTMLElement)) {
        return;
      }
      const rows = Array.isArray(entries)
        ? entries.filter((entry) => String(entry?.display || entry?.message || "").trim())
        : [];
      const shouldStick =
        reset ||
        logConsole.scrollHeight - logConsole.scrollTop - logConsole.clientHeight < 28;
      if (reset) {
        logConsole.innerHTML = "";
      }
      if (!rows.length && reset) {
        const emptyEl = document.createElement("div");
        emptyEl.className = "voice-log-empty";
        emptyEl.textContent = "Waiting for live ESPHome logs...";
        logConsole.appendChild(emptyEl);
      } else if (rows.length) {
        rows.forEach((entry) => renderConsoleLine(entry));
      }
      if (shouldStick) {
        logConsole.scrollTop = logConsole.scrollHeight;
      }
    };

    const stopViewer = async () => {
      stopped = true;
      if (pollTimer) {
        window.clearTimeout(pollTimer);
        pollTimer = 0;
      }
      try {
        await runCoreManagerAction(card, coreKey, "voice_logs_stop", { id: selector, selector });
      } catch (_error) {
        // Ignore cleanup failures while closing the log viewer.
      }
    };

    const schedulePoll = (delayMs = 1200) => {
      if (stopped) {
        return;
      }
      pollTimer = window.setTimeout(async () => {
        if (stopped) {
          return;
        }
        try {
          const result = await runCoreManagerAction(card, coreKey, "voice_logs_poll", {
            id: selector,
            selector,
            after_seq: cursor,
          });
          const entries = Array.isArray(result?.entries) ? result.entries : [];
          if (entries.length) {
            renderEntries(entries, false);
          }
          cursor = Number(result?.cursor || cursor || 0);
          if (statusNode) {
            const errorText = String(result?.error || "").trim();
            statusNode.textContent = errorText
              ? `Log feed warning: ${errorText}`
              : `Streaming live logs from ${name || selector}.`;
          }
          schedulePoll(1200);
        } catch (error) {
          if (statusNode) {
            statusNode.textContent = `Log feed error: ${String(error?.message || "unknown error")}`;
          }
          schedulePoll(2500);
        }
      }, delayMs);
    };

    openRuntimeSettingsModal({
      title: popupTitle || `${name || selector} Live Log`,
      meta: [name || selector, host || selector].filter(Boolean).join(" • "),
      fields: popupFields,
      onOpen: async ({ modal, fieldsEl, statusEl }) => {
        statusNode = statusEl instanceof HTMLElement ? statusEl : null;
        logArea = fieldsEl?.querySelector('[data-setting-key="live_log_feed"]') || null;
        modalDialog = modal?.querySelector(".runtime-settings-dialog") || null;
        modal?.classList.add("voice-log-modal");
        modalDialog?.classList.add("runtime-settings-dialog-log");
        fieldsEl?.classList.add("runtime-settings-fields-log");
        if (logArea instanceof HTMLTextAreaElement) {
          logArea.readOnly = true;
          logArea.spellcheck = false;
          logArea.classList.add("voice-log-source-textarea");
          const label = logArea.closest("label");
          const consoleEl = document.createElement("div");
          consoleEl.className = "voice-log-console";
          consoleEl.setAttribute("role", "log");
          consoleEl.setAttribute("aria-live", "polite");
          consoleEl.setAttribute("aria-label", `${name || selector} live device log`);
          logConsole = consoleEl;
          if (label instanceof HTMLElement) {
            label.classList.add("voice-log-field");
            label.appendChild(consoleEl);
          }
        }
        if (statusNode) {
          statusNode.textContent = "Opening live log feed...";
          statusNode.classList.add("voice-log-status");
        }
        try {
          const result = await runCoreManagerAction(card, coreKey, "voice_logs_start", { id: selector, selector });
          const entries = Array.isArray(result?.entries) ? result.entries : [];
          cursor = Number(result?.cursor || 0);
          renderEntries(entries, true);
          if (statusNode) {
            statusNode.textContent = `Streaming live logs from ${name || selector}.`;
          }
          schedulePoll(1000);
        } catch (error) {
          renderEntries(
            [
              {
                display: `Failed to open live log feed: ${String(error?.message || "unknown error")}`,
              },
            ],
            true
          );
          if (statusNode) {
            statusNode.textContent = `Log feed failed: ${String(error?.message || "unknown error")}`;
          }
        }
      },
      onClose: ({ modal, fieldsEl, statusEl }) => {
        modal?.classList.remove("voice-log-modal");
        modalDialog?.classList.remove("runtime-settings-dialog-log");
        fieldsEl?.classList.remove("runtime-settings-fields-log");
        if (statusEl instanceof HTMLElement) {
          statusEl.classList.remove("voice-log-status");
        }
        void stopViewer();
      },
    });
  };

  document.querySelectorAll(".core-manager-add-form[data-core-key][data-core-action]").forEach((form) => {
    if (form.dataset.coreManagerActionBound === "1") {
      return;
    }
    form.dataset.coreManagerActionBound = "1";
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const coreKey = String(form.dataset.coreKey || "").trim();
      const action = String(form.dataset.coreAction || "").trim();
      if (!coreKey || !action) {
        return;
      }
      const values = collectCoreManagerValues(form);
      setCoreManagerStatus(form, "Saving...");
      try {
        const activeTab = persistCoreTabFromNode(form);
        const result = await runActionWithProgress(
          {
            title: "Saving core item",
            detail: coreKey,
            workingText: "Saving changes...",
            successText: "Saved.",
            errorPrefix: "Core manager save failed",
          },
          () => runCoreManagerAction(form, coreKey, action, { ...values, values })
        );
        await refreshCoreManagerInPlace(form, activeTab);
        state.notice = String(result?.message || "Saved.");
        setCoreManagerStatus(form, state.notice);
        showToast(state.notice);
      } catch (error) {
        setCoreManagerStatus(form, `Failed: ${error.message}`);
        showToast(`Failed: ${error.message}`, "error", 3600);
      }
    });
  });

  document.querySelectorAll(".core-manager-settings").forEach((button) => {
    if (button.dataset.coreManagerActionBound === "1") {
      return;
    }
    button.dataset.coreManagerActionBound = "1";
    button.addEventListener("click", (event) => {
      const card = event.currentTarget.closest(".core-manager-item");
      const coreKey = String(card?.dataset?.coreKey || "").trim();
      const action = String(card?.dataset?.coreSaveAction || "").trim();
      const itemId = decodeCoreManagerId(card?.dataset?.coreItemId || "");
      if (!card || !coreKey) {
        return;
      }
      const modalFields = decodeCoreManagerPopupFields(card);
      if (!modalFields.length) {
        showToast("No configurable settings found for this item.", "error", 2600);
        return;
      }
      const popupTitle = String(card?.dataset?.coreItemPopupTitle || `${itemId || coreKey} Settings`).trim();
      const popupMode = String(card?.dataset?.coreItemPopupMode || "").trim();
      const popupConfig = decodeCoreManagerPopupConfig(card);
      if (popupMode === "voice-satellite-log") {
        openVoiceSatelliteLogViewer(card, coreKey, popupTitle, modalFields, popupConfig);
        return;
      }
      openRuntimeSettingsModal({
        title: popupTitle || "Settings",
        meta: coreKey,
        fields: modalFields,
        onSave: action
          ? async (values) => {
              setCoreManagerStatus(card, "Saving...");
              const activeTab = persistCoreTabFromNode(card);
              const result = await runActionWithProgress(
                {
                  title: "Saving core item",
                  detail: itemId || coreKey,
                  workingText: "Saving changes...",
                  successText: "Saved.",
                  errorPrefix: "Core manager save failed",
                },
                () => runCoreManagerAction(card, coreKey, action, { id: itemId, values })
              );
              await refreshCoreManagerInPlace(card, activeTab);
              state.notice = String(result?.message || "Saved.");
              return result;
            }
          : undefined,
      });
    });
  });

  document.querySelectorAll(".core-manager-save").forEach((button) => {
    if (button.dataset.coreManagerActionBound === "1") {
      return;
    }
    button.dataset.coreManagerActionBound = "1";
    button.addEventListener("click", async (event) => {
      const card = event.currentTarget.closest(".core-manager-item");
      const coreKey = String(card?.dataset?.coreKey || "").trim();
      const action = String(card?.dataset?.coreSaveAction || "").trim();
      const itemId = decodeCoreManagerId(card?.dataset?.coreItemId || "");
      if (!card || !coreKey || !action) {
        return;
      }
      const values = collectCoreManagerValues(card);
      setCoreManagerStatus(card, "Saving...");
      try {
        const activeTab = persistCoreTabFromNode(card);
        const result = await runActionWithProgress(
          {
            title: "Saving core item",
            detail: itemId || coreKey,
            workingText: "Saving changes...",
            successText: "Saved.",
            errorPrefix: "Core manager save failed",
          },
          () => runCoreManagerAction(card, coreKey, action, { id: itemId, values })
        );
        await refreshCoreManagerInPlace(card, activeTab);
        state.notice = String(result?.message || "Saved.");
        setCoreManagerStatus(card, state.notice);
        showToast(state.notice);
      } catch (error) {
        setCoreManagerStatus(card, `Failed: ${error.message}`);
        showToast(`Failed: ${error.message}`, "error", 3600);
      }
    });
  });

  document.querySelectorAll(".core-manager-remove").forEach((button) => {
    if (button.dataset.coreManagerActionBound === "1") {
      return;
    }
    button.dataset.coreManagerActionBound = "1";
    button.addEventListener("click", async (event) => {
      const card = event.currentTarget.closest(".core-manager-item");
      const coreKey = String(card?.dataset?.coreKey || "").trim();
      const action = String(card?.dataset?.coreRemoveAction || "").trim();
      const itemId = decodeCoreManagerId(card?.dataset?.coreItemId || "");
      const confirmText = String(card?.dataset?.coreRemoveConfirm || "Remove this item?").trim();
      if (!card || !coreKey || !action) {
        return;
      }
      if (!window.confirm(confirmText)) {
        return;
      }
      setCoreManagerStatus(card, "Removing...");
      try {
        const activeTab = persistCoreTabFromNode(card);
        const result = await runActionWithProgress(
          {
            title: "Removing core item",
            detail: itemId || coreKey,
            workingText: "Removing item...",
            successText: "Removed.",
            errorPrefix: "Core manager remove failed",
          },
          () => runCoreManagerAction(card, coreKey, action, { id: itemId })
        );
        await refreshCoreManagerInPlace(card, activeTab);
        state.notice = String(result?.message || "Removed.");
        setCoreManagerStatus(card, state.notice);
        showToast(state.notice);
      } catch (error) {
        setCoreManagerStatus(card, `Failed: ${error.message}`);
        showToast(`Failed: ${error.message}`, "error", 3600);
      }
    });
  });

  document.querySelectorAll(".core-manager-run").forEach((button) => {
    if (button.dataset.coreManagerActionBound === "1") {
      return;
    }
    button.dataset.coreManagerActionBound = "1";
    button.addEventListener("click", async (event) => {
      const card = event.currentTarget.closest(".core-manager-item");
      const coreKey = String(card?.dataset?.coreKey || "").trim();
      const action = String(card?.dataset?.coreRunAction || "").trim();
      const itemId = decodeCoreManagerId(card?.dataset?.coreItemId || "");
      const confirmText = String(card?.dataset?.coreRunConfirm || "").trim();
      if (!card || !coreKey || !action) {
        return;
      }
      if (confirmText && !window.confirm(confirmText)) {
        return;
      }
      setCoreManagerStatus(card, "Queueing...");
      try {
        const activeTab = persistCoreTabFromNode(card);
        const values = collectCoreManagerValues(card);
        const result = await runActionWithProgress(
          {
            title: "Running core item",
            detail: itemId || coreKey,
            workingText: "Queueing run now...",
            successText: "Queued.",
            errorPrefix: "Core manager run failed",
          },
          () => runCoreManagerAction(card, coreKey, action, { id: itemId, values })
        );
        const sampleUrl = String(result?.sample_url || "").trim();
        if (sampleUrl) {
          try {
            const audio = new Audio(sampleUrl);
            await audio.play();
          } catch (playError) {
            showToast(`Sample ready but playback failed: ${playError.message}`, "error", 3600);
          }
        }
        await refreshCoreManagerInPlace(card, activeTab);
        state.notice = String(result?.message || "Queued.");
        setCoreManagerStatus(card, state.notice);
        showToast(state.notice);
      } catch (error) {
        setCoreManagerStatus(card, `Failed: ${error.message}`);
        showToast(`Failed: ${error.message}`, "error", 3600);
      }
    });
  });
}

async function runShopAction(kind, action, payload) {
  const endpoint = `${shopEndpoint(kind)}/${action}`;
  return api(endpoint, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

function bindShopActions(kind) {
  const setStatus = (text) => setShopStatus(kind, text);

  const repoNameInput = document.getElementById(`shop-repo-name-${kind}`);
  const repoUrlInput = document.getElementById(`shop-repo-url-${kind}`);
  const addRepoButton = document.querySelector(`.shop-add-repo[data-kind='${kind}']`);
  const repoList = document.getElementById(`shop-repo-list-${kind}`);

  ensureShopRepoListEmptyState(kind);

  const addRepo = () => {
    const name = String(repoNameInput?.value || "").trim();
    const url = String(repoUrlInput?.value || "").trim();
    if (!repoList) {
      setStatus("Repo list is unavailable.");
      return;
    }
    if (!url) {
      setStatus("Repo URL is required.");
      repoUrlInput?.focus();
      return;
    }
    const duplicate = collectShopReposFromList(kind).some((repo) => String(repo.url || "").toLowerCase() === url.toLowerCase());
    if (duplicate) {
      setStatus("That repo URL is already added.");
      repoUrlInput?.focus();
      return;
    }
    repoList.insertAdjacentHTML("beforeend", renderShopRepoListItem(kind, { name, url }));
    ensureShopRepoListEmptyState(kind);
    if (repoNameInput) {
      repoNameInput.value = "";
    }
    if (repoUrlInput) {
      repoUrlInput.value = "";
      repoUrlInput.focus();
    }
    setStatus("Repo added. Click Save Repos to persist.");
  };

  if (addRepoButton) {
    addRepoButton.addEventListener("click", addRepo);
  }

  [repoNameInput, repoUrlInput].forEach((input) => {
    if (!input) {
      return;
    }
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        addRepo();
      }
    });
  });

  if (repoList) {
    repoList.addEventListener("click", (event) => {
      const button = event.target.closest(`.shop-remove-repo[data-kind='${kind}']`);
      if (!button) {
        return;
      }
      const row = button.closest(".shop-repo-item");
      if (row) {
        row.remove();
      }
      ensureShopRepoListEmptyState(kind);
      setStatus("Repo removed. Click Save Repos to persist.");
    });
  }

  const saveButton = document.querySelector(`.shop-save-repos[data-kind='${kind}']`);
  if (saveButton) {
    saveButton.addEventListener("click", async () => {
      const repos = collectShopReposFromList(kind);
      try {
        await runActionWithProgress(
          {
            title: `Saving ${shopLabel(kind)} repos`,
            detail: `${repos.length} additional repo(s)`,
            workingText: "Saving repositories...",
            successText: "Repo settings saved.",
            errorPrefix: "Repo save failed",
          },
          () => runShopAction(kind, "repos", { repos })
        );
        await refreshShopManagerInPlace(kind, "settings");
        state.notice = `${shopLabel(kind)} repos saved.`;
        showToast(state.notice);
      } catch (error) {
        setStatus(`Repo save failed: ${error.message}`);
        showToast(`Repo save failed: ${error.message}`, "error", 3600);
      }
    });
  }

  const updateAllButton = document.querySelector(`.shop-update-all[data-kind='${kind}']`);
  if (updateAllButton) {
    updateAllButton.addEventListener("click", async () => {
      const activeTab = getActiveShopTab(kind);
      setStatus("Running update all...");
      try {
        const result = await runActionWithProgress(
          {
            title: `Updating all ${shopLabel(kind)} items`,
            detail: "Applying all available updates",
            workingText: "Running updates...",
            successText: "Update-all finished.",
            errorPrefix: "Update all failed",
          },
          () => runShopAction(kind, "update-all", {})
        );
        await refreshShopManagerInPlace(kind, activeTab || "manage");
        const updated = Array.isArray(result.updated) ? result.updated.length : 0;
        const failed = Array.isArray(result.failed) ? result.failed.length : 0;
        state.notice = `${shopLabel(kind)} update-all completed. Updated ${updated}, failed ${failed}.`;
        showToast(state.notice, failed ? "error" : "success", 3400);
        setStatus(state.notice);
      } catch (error) {
        setStatus(`Update all failed: ${error.message}`);
        showToast(`Update all failed: ${error.message}`, "error", 3600);
      }
    });
  }

  document.querySelectorAll(`.shop-action[data-kind='${kind}']`).forEach((button) => {
    button.addEventListener("click", async (event) => {
      const action = String(event.currentTarget.dataset.action || "").trim();
      const id = String(event.currentTarget.dataset.id || "").trim();
      if (!action || !id) {
        return;
      }

      const payload = { id };
      if (action === "remove") {
        const card = event.currentTarget.closest(".shop-installed-card, .card");
        const purge = Boolean(card?.querySelector(".shop-purge")?.checked);
        payload.purge_redis = purge;
      }

      setStatus(`${action} ${id}...`);
      try {
        const activeTab = getActiveShopTab(kind);
        const verb = action === "install" ? "Installing" : action === "update" ? "Updating" : action === "remove" ? "Removing" : "Running";
        const result = await runActionWithProgress(
          {
            title: `${verb} ${shopLabel(kind).toLowerCase()}`,
            detail: id,
            workingText: `${verb.toLowerCase()} ${id}...`,
            successText: `${shopLabel(kind)} action completed.`,
            errorPrefix: `${action} ${id} failed`,
          },
          () => runShopAction(kind, action, payload)
        );
        await refreshShopManagerInPlace(kind, activeTab || (action === "install" ? "installed" : "manage"));
        const message = result.message || `${shopLabel(kind)} action completed.`;
        state.notice = message;
        setStatus(message);
        showToast(message);
      } catch (error) {
        setStatus(`${action} ${id} failed: ${error.message}`);
        showToast(`${action} ${id} failed: ${error.message}`, "error", 3600);
      }
    });
  });
}

function bindVerbaRuntimeActions(root = document) {
  root.querySelectorAll(".verba-toggle").forEach((button) => {
    button.addEventListener("click", async (event) => {
      const card = event.currentTarget.closest("[data-plugin-id]");
      const pluginId = String(card?.dataset?.pluginId || "").trim();
      const nextEnabled = String(event.currentTarget.textContent || "").trim().toLowerCase() === "enable";
      if (!pluginId) {
        return;
      }

      setShopStatus("verbas", `${nextEnabled ? "Enabling" : "Disabling"} ${pluginId}...`);
      try {
        await runActionWithProgress(
          {
            title: `${nextEnabled ? "Enabling" : "Disabling"} verba`,
            detail: pluginId,
            workingText: `${nextEnabled ? "Enabling" : "Disabling"} ${pluginId}...`,
            successText: `${pluginId} ${nextEnabled ? "enabled" : "disabled"}.`,
            errorPrefix: "Verba toggle failed",
          },
          () =>
            api(`/api/verbas/${pluginId}/enabled`, {
              method: "POST",
              body: JSON.stringify({ enabled: nextEnabled }),
            })
        );

        await refreshShopManagerInPlace("verbas", getActiveShopTab("verbas") || "installed");
        await refreshHealth();
        showToast(`${pluginId} ${nextEnabled ? "enabled" : "disabled"}.`);
      } catch (error) {
        setShopStatus("verbas", `Toggle failed: ${error.message}`);
        showToast(`Toggle failed: ${error.message}`, "error", 3600);
      }
    });
  });

  root.querySelectorAll(".open-runtime-settings[data-runtime-kind='verbas']").forEach((button) => {
    if (button.dataset.runtimeSettingsBound === "1") {
      return;
    }
    button.dataset.runtimeSettingsBound = "1";
    button.addEventListener("click", async (event) => {
      const trigger = event.currentTarget;
      const pluginId = String(trigger?.dataset?.runtimeKey || "").trim();
      const entry = getRuntimeSettingsEntry("verbas", pluginId);
      if (!pluginId || !entry) {
        return;
      }
      openRuntimeSettingsModal({
        title: formatRuntimeSettingsTitle(entry.label),
        meta: pluginId,
        fields: entry.settings,
        onSave: async (values) => {
          await api(`/api/verbas/${pluginId}/settings`, {
            method: "POST",
            body: JSON.stringify({ values }),
          });
          const message = `Saved settings for ${pluginId}.`;
          setShopStatus("verbas", message);
          state.notice = message;
          await refreshShopManagerInPlace("verbas", getActiveShopTab("verbas") || "installed");
          return { message };
        },
      });
    });
  });
}

function bindSurfaceRuntimeActions(kind, endpoint, root = document) {
  root.querySelectorAll(".surface-toggle").forEach((button) => {
    button.addEventListener("click", async (event) => {
      const card = event.currentTarget.closest("[data-surface-key]");
      const surfaceKey = String(card?.dataset?.surfaceKey || "").trim();
      if (!surfaceKey) {
        return;
      }
      const action = String(event.currentTarget.textContent || "").trim().toLowerCase() === "start" ? "start" : "stop";
      const noun = shopLabel(kind).toLowerCase();
      setShopStatus(kind, `${action} ${surfaceKey}...`);

      try {
        await runActionWithProgress(
          {
            title: `${action === "start" ? "Starting" : "Stopping"} ${noun}`,
            detail: surfaceKey,
            workingText: `${action} ${surfaceKey}...`,
            successText: `${surfaceKey} ${action === "start" ? "started" : "stopped"}.`,
            errorPrefix: `${noun} ${action} failed`,
          },
          () => api(`${endpoint}/${surfaceKey}/${action}`, { method: "POST" })
        );

        await refreshShopManagerInPlace(kind, getActiveShopTab(kind) || "installed");
        await refreshHealth();
        showToast(`${surfaceKey} ${action === "start" ? "started" : "stopped"}.`);
      } catch (error) {
        setShopStatus(kind, `${action} failed: ${error.message}`);
        showToast(`${action} failed: ${error.message}`, "error", 3600);
      }
    });
  });

  root.querySelectorAll(`.open-runtime-settings[data-runtime-kind='${kind}']`).forEach((button) => {
    if (button.dataset.runtimeSettingsBound === "1") {
      return;
    }
    button.dataset.runtimeSettingsBound = "1";
    button.addEventListener("click", (event) => {
      const trigger = event.currentTarget;
      const surfaceKey = String(trigger?.dataset?.runtimeKey || "").trim();
      const entry = getRuntimeSettingsEntry(kind, surfaceKey);
      if (!surfaceKey || !entry) {
        return;
      }
      openRuntimeSettingsModal({
        title: formatRuntimeSettingsTitle(entry.label),
        meta: surfaceKey,
        fields: entry.settings,
        onSave: async (values) => {
          await api(`${endpoint}/${surfaceKey}/settings`, {
            method: "POST",
            body: JSON.stringify({ values }),
          });
          const message = `Saved settings for ${surfaceKey}.`;
          setShopStatus(kind, message);
          state.notice = message;
          await refreshShopManagerInPlace(kind, getActiveShopTab(kind) || "installed");
          return { message };
        },
      });
    });
  });
}

async function loadChatView() {
  const root = document.getElementById("view-root");
  root.innerHTML = `${consumeNoticeHtml()}
    <div class="card chat-feed-card">
      <div id="chat-log" class="chat-log"></div>
      <div id="chat-speed-stats" class="small chat-speed-stats" style="display:none;"></div>
      <div id="chat-status" class="small chat-live-status" aria-live="polite"></div>
      <div class="message-box chat-composer-card">
        <div class="chat-composer" role="group" aria-label="Chat composer">
          <div class="chat-composer-bar">
            <input id="chat-files" class="chat-file-input" type="file" multiple />
            <label for="chat-files" class="chat-composer-btn chat-composer-attach" title="Attach files" aria-label="Attach files">
              <span class="chat-composer-icon chat-composer-plus" aria-hidden="true">+</span>
            </label>
            <textarea
              id="chat-input"
              class="chat-composer-input"
              rows="1"
              placeholder="${escapeHtml(`Message ${getTaterFullName()}...`)}"
            ></textarea>
            <button type="button" id="send-chat" class="chat-composer-send" title="Send message" aria-label="Send message">
              <span class="chat-composer-icon chat-composer-send-arrow" aria-hidden="true">➤</span>
            </button>
          </div>
          <div id="chat-files-row" class="chat-files-row" style="display:none;">
            <div id="chat-files-meta" class="small chat-files-meta"></div>
            <button
              type="button"
              id="clear-chat-files"
              class="chat-files-clear-btn"
              title="Clear attached files"
              aria-label="Clear attached files"
            >
              Clear
            </button>
          </div>
        </div>
      </div>
    </div>
  `;

  const chatLog = document.getElementById("chat-log");
  const speedStatsEl = document.getElementById("chat-speed-stats");
  const status = document.getElementById("chat-status");
  const chatFilesEl = document.getElementById("chat-files");
  const chatFilesRowEl = document.getElementById("chat-files-row");
  const chatFilesMetaEl = document.getElementById("chat-files-meta");
  const clearChatFilesBtn = document.getElementById("clear-chat-files");
  let pendingFiles = [];

  const scrollChatToBottom = () => {
    if (!chatLog) {
      return;
    }
    chatLog.scrollTop = chatLog.scrollHeight;
  };

  const stickChatToBottom = () => {
    scrollChatToBottom();
    requestAnimationFrame(scrollChatToBottom);
    setTimeout(scrollChatToBottom, 0);
    setTimeout(scrollChatToBottom, 140);
  };

  const bindChatMediaAutoScroll = () => {
    const mediaNodes = chatLog.querySelectorAll("img.chat-media-image, audio, video");
    mediaNodes.forEach((node) => {
      if (!(node instanceof HTMLElement)) {
        return;
      }
      if (node.dataset.bottomBound === "1") {
        return;
      }
      node.dataset.bottomBound = "1";

      const bump = () => {
        stickChatToBottom();
      };

      if (node instanceof HTMLImageElement) {
        if (node.complete) {
          bump();
        } else {
          node.addEventListener("load", bump, { once: true });
          node.addEventListener("error", bump, { once: true });
        }
        return;
      }

      if (node instanceof HTMLMediaElement) {
        if (node.readyState > 0) {
          bump();
        } else {
          node.addEventListener("loadedmetadata", bump, { once: true });
          node.addEventListener("loadeddata", bump, { once: true });
          node.addEventListener("error", bump, { once: true });
        }
      }
    });
  };

  const removeChatTypingIndicator = () => {
    if (!chatLog) {
      return;
    }
    chatLog.querySelectorAll(".chat-row.typing-indicator").forEach((node) => node.remove());
  };

  const syncChatTypingIndicator = (scroll = true) => {
    removeChatTypingIndicator();
    if (_activeChatJobCount() <= 0) {
      return;
    }
    chatLog.insertAdjacentHTML(
      "beforeend",
      renderChatMessage({
        role: "assistant",
        content: { marker: "typing" },
      })
    );
    if (scroll) {
      stickChatToBottom();
    }
  };

  const updatePendingFilesUi = () => {
    if (!chatFilesMetaEl) {
      return;
    }
    if (!pendingFiles.length) {
      chatFilesMetaEl.textContent = "";
      if (chatFilesRowEl) {
        chatFilesRowEl.style.display = "none";
      }
      return;
    }
    const names = pendingFiles
      .slice(0, 3)
      .map((file) => String(file?.name || "").trim())
      .filter(Boolean);
    const extra = pendingFiles.length > names.length ? ` +${pendingFiles.length - names.length} more` : "";
    chatFilesMetaEl.textContent = `${pendingFiles.length} file${pendingFiles.length === 1 ? "" : "s"} attached: ${names.join(", ")}${extra}`;
    if (chatFilesRowEl) {
      chatFilesRowEl.style.display = "flex";
    }
  };

  async function refreshChatProfile() {
    try {
      const profile = await api("/api/chat/profile");
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
      syncChatCopy();
      updatePendingFilesUi();
    } catch {
      state.chatProfile = {
        username: "User",
        userAvatar: "",
        taterAvatar: "",
        taterName: "Tater",
        taterFirstName: "Tater",
        taterLastName: "Totterson",
        taterFullName: "Tater Totterson",
        attachMaxMbEach: Number(state.chatProfile.attachMaxMbEach ?? 0),
        attachMaxMbTotal: Number(state.chatProfile.attachMaxMbTotal ?? 0),
      };
      syncChatCopy();
      updatePendingFilesUi();
    }
  }

  async function refreshChatHistory() {
    const history = await api("/api/chat/history");
    const messages = Array.isArray(history.messages) ? history.messages : [];
    chatLog.innerHTML = messages.map(renderChatMessage).join("") || renderNotice("No messages yet.");
    syncChatTypingIndicator(false);
    stickChatToBottom();
    bindChatMediaAutoScroll();
    await refreshChatSpeedStats();
  }

  const _normalizeAssistantMessages = (responses) => {
    if (!Array.isArray(responses)) {
      return [];
    }
    return responses
      .map((item) => ({
        role: "assistant",
        username: "assistant",
        content: item,
      }))
      .filter((item) => {
        if (!item || typeof item !== "object") {
          return false;
        }
        const content = item.content;
        if (typeof content === "string") {
          return Boolean(content.trim());
        }
        if (content && typeof content === "object") {
          return true;
        }
        return false;
      });
  };

  const appendAssistantResponses = (responses) => {
    const messages = _normalizeAssistantMessages(responses);
    if (!messages.length) {
      return false;
    }
    removeChatTypingIndicator();
    const existingLog = String(chatLog.innerHTML || "").trim();
    if (!existingLog || existingLog.includes('class="notice"')) {
      chatLog.innerHTML = "";
    }
    const html = messages.map(renderChatMessage).join("");
    chatLog.insertAdjacentHTML("beforeend", html);
    syncChatTypingIndicator(false);
    stickChatToBottom();
    bindChatMediaAutoScroll();
    return true;
  };

  const appendAssistantWaitLine = (waitText) => {
    const text = String(waitText || "").trim();
    if (!text) {
      return false;
    }
    return appendAssistantResponses([
      {
        marker: "plugin_wait",
        content: text,
      },
    ]);
  };

  async function refreshChatSpeedStats() {
    if (!speedStatsEl) {
      return;
    }

    speedStatsEl.textContent = "";
    speedStatsEl.style.display = "none";

    try {
      const payload = await api("/api/chat/stats");
      if (!payload || !boolFromAny(payload.enabled, false)) {
        return;
      }
      const stats = payload.stats && typeof payload.stats === "object" ? payload.stats : null;
      if (!stats) {
        return;
      }

      const model = String(stats.model || "LLM").trim() || "LLM";
      const elapsed = Number(stats.elapsed || 0);
      const promptTokens = Number(stats.prompt_tokens || 0);
      const completionTokens = Number(stats.completion_tokens || 0);
      const totalTokens = Number(stats.total_tokens || 0);
      const tpsTotal = Number(stats.tps_total || 0);
      const tpsComp = Number(stats.tps_comp || 0);

      if (!Number.isFinite(elapsed) || elapsed <= 0 || !Number.isFinite(totalTokens) || totalTokens <= 0) {
        return;
      }

      const compPart = Number.isFinite(tpsComp) && tpsComp > 0 ? ` | completion: ${Math.round(tpsComp)} tok/s` : "";
      speedStatsEl.textContent = `${model} - ${Math.round(tpsTotal)} tok/s${compPart} • ${Math.round(
        totalTokens
      )} tok in ${elapsed.toFixed(2)}s (prompt ${Math.round(promptTokens)}, completion ${Math.round(completionTokens)})`;
      speedStatsEl.style.display = "block";
    } catch {
      speedStatsEl.textContent = "";
      speedStatsEl.style.display = "none";
    }
  }

  const _getActiveChatJobs = () => {
    if (!state.activeChatJobs || typeof state.activeChatJobs !== "object") {
      state.activeChatJobs = {};
    }
    return state.activeChatJobs;
  };

  const _getChatPollMeta = () => {
    if (!state.chatPollMeta || typeof state.chatPollMeta !== "object") {
      state.chatPollMeta = {};
    }
    return state.chatPollMeta;
  };

  const _activeChatJobCount = () => {
    const jobs = _getActiveChatJobs();
    return Object.keys(jobs).filter((jobId) => Boolean(jobs[jobId])).length;
  };

  const _chatJobEntry = (jobId, createIfMissing = true) => {
    const id = String(jobId || "").trim();
    if (!id) {
      return null;
    }
    const jobs = _getActiveChatJobs();
    const existing = jobs[id];
    if (existing && typeof existing === "object") {
      return existing;
    }
    if (!createIfMissing) {
      return null;
    }
    const next = {
      status: "queued",
      current_tool: "",
      task_name: "",
      updated_at: Date.now(),
    };
    jobs[id] = next;
    state.activeChatJobs = jobs;
    return next;
  };

  const _updateChatJobEntry = (jobId, patch = null) => {
    const row = _chatJobEntry(jobId, true);
    if (!row) {
      return null;
    }
    const payload = patch && typeof patch === "object" ? patch : {};
    if (Object.prototype.hasOwnProperty.call(payload, "status")) {
      row.status = String(payload.status || "").trim().toLowerCase() || row.status || "queued";
    }
    if (Object.prototype.hasOwnProperty.call(payload, "current_tool")) {
      row.current_tool = String(payload.current_tool || "").trim();
    }
    if (Object.prototype.hasOwnProperty.call(payload, "task_name")) {
      const maybeTask = String(payload.task_name || "").trim();
      if (maybeTask) {
        row.task_name = maybeTask;
      }
    }
    row.updated_at = Date.now();
    return row;
  };

  const _normalizedToolBucket = (toolRaw) => {
    const tool = String(toolRaw || "").trim();
    if (!tool) {
      return "";
    }
    return tool.toLowerCase();
  };

  const _aggregateChatToolUsageLine = () => {
    const jobs = _getActiveChatJobs();
    const entries = Object.values(jobs).filter((row) => row && typeof row === "object");
    const toolBuckets = {};
    entries.forEach((row) => {
      const tool = String(row.current_tool || "").trim();
      const bucket = _normalizedToolBucket(tool);
      if (!bucket) {
        return;
      }
      if (!toolBuckets[bucket]) {
        toolBuckets[bucket] = { count: 0, display: tool };
      }
      toolBuckets[bucket].count += 1;
      if (!toolBuckets[bucket].display && tool) {
        toolBuckets[bucket].display = tool;
      }
    });
    const parts = Object.values(toolBuckets)
      .sort((a, b) => {
        const diff = Number(b.count || 0) - Number(a.count || 0);
        if (diff !== 0) {
          return diff;
        }
        return String(a.display || "").localeCompare(String(b.display || ""));
      })
      .slice(0, 3)
      .map((row) => `${Number(row.count || 0)} using ${String(row.display || "tool")}`);
    return parts.join(" • ");
  };

  const _setChatLiveStatus = (text = "") => {
    const activeCount = _activeChatJobCount();
    const normalized = String(text || "").trim();
    syncChatTypingIndicator(activeCount > 0);
    if (activeCount > 0) {
      const toolLine = _aggregateChatToolUsageLine();
      const jobWord = activeCount === 1 ? "job" : "jobs";
      status.textContent = toolLine ? `${activeCount} ${jobWord} running • ${toolLine}` : `${activeCount} ${jobWord} running`;
      return;
    }
    status.textContent = normalized;
  };

  const stopChatJobPolling = (jobId = "") => {
    const targetId = String(jobId || "").trim();
    const pollMeta = _getChatPollMeta();

    if (targetId) {
      const row = pollMeta[targetId] && typeof pollMeta[targetId] === "object" ? pollMeta[targetId] : {};
      row.token = Number(row.token || 0) + 1;
      if (row.timer) {
        window.clearTimeout(row.timer);
      }
      row.timer = 0;
      pollMeta[targetId] = row;
      state.chatPollMeta = pollMeta;
      return;
    }

    Object.keys(pollMeta).forEach((id) => {
      const row = pollMeta[id] && typeof pollMeta[id] === "object" ? pollMeta[id] : {};
      row.token = Number(row.token || 0) + 1;
      if (row.timer) {
        window.clearTimeout(row.timer);
      }
      row.timer = 0;
      pollMeta[id] = row;
    });
    state.chatPollMeta = pollMeta;
  };

  const finalizeChatJob = async ({ jobId, statusText, responses = [] }) => {
    const id = String(jobId || "").trim();
    if (!id) {
      return;
    }
    const jobs = _getActiveChatJobs();
    if (!jobs[id]) {
      return;
    }
    delete jobs[id];
    state.activeChatJobs = jobs;
    stopChatJobPolling(id);
    closeChatEventSource(id);
    const pollMeta = _getChatPollMeta();
    delete pollMeta[id];
    state.chatPollMeta = pollMeta;

    const inlineRendered = appendAssistantResponses(responses);
    if (inlineRendered) {
      await refreshChatSpeedStats();
    } else {
      await refreshChatHistory();
    }
    await refreshHealth();

    const remaining = _activeChatJobCount();
    if (remaining > 0) {
      _setChatLiveStatus(String(statusText || "Complete."));
      return;
    }
    _setChatLiveStatus(String(statusText || ""));
  };

  const scheduleChatJobPoll = (jobId, delayMs = 1200) => {
    const id = String(jobId || "").trim();
    if (!id) {
      return;
    }
    const jobs = _getActiveChatJobs();
    if (!jobs[id]) {
      return;
    }
    const pollMeta = _getChatPollMeta();
    const row = pollMeta[id] && typeof pollMeta[id] === "object" ? pollMeta[id] : {};
    row.token = Number(row.token || 0) + 1;
    const token = row.token;
    if (row.timer) {
      window.clearTimeout(row.timer);
    }
    row.timer = window.setTimeout(async () => {
      const jobsNow = _getActiveChatJobs();
      if (!jobsNow[id]) {
        return;
      }
      const pollNow = _getChatPollMeta();
      const current = pollNow[id] && typeof pollNow[id] === "object" ? pollNow[id] : {};
      if (Number(current.token || 0) !== token) {
        return;
      }

      try {
        const snapshot = await api(`/api/chat/jobs/${encodeURIComponent(id)}`);
        const snapshotStatus = String(snapshot.status || "").trim().toLowerCase();
        if (snapshotStatus === "done") {
          await finalizeChatJob({
            jobId: id,
            statusText: "Complete.",
            responses: Array.isArray(snapshot.responses) ? snapshot.responses : [],
          });
          return;
        }
        if (snapshotStatus === "error") {
          await finalizeChatJob({
            jobId: id,
            statusText: `Job failed: ${snapshot.error || "unknown error"}`,
          });
          return;
        }

        const tool = String(snapshot.current_tool || "").trim();
        const taskName = String(snapshot.task_name || "").trim();
        _updateChatJobEntry(id, {
          status: snapshotStatus || "running",
          current_tool: tool,
          task_name: taskName,
        });
        _setChatLiveStatus();
      } catch (error) {
        _setChatLiveStatus();
        if (_isLikelyRedisFailureDetail(error?.message || "")) {
          void promptRedisSetupRecovery(String(error?.message || "Redis connection lost."));
        } else {
          _scheduleHealthRefresh(220);
        }
      }

      scheduleChatJobPoll(id, 1200);
    }, Math.max(250, Number(delayMs) || 1200));
    pollMeta[id] = row;
    state.chatPollMeta = pollMeta;
  };

  function attachJobStream(jobId, initialMeta = null) {
    const id = String(jobId || "").trim();
    if (!id) {
      return;
    }
    _updateChatJobEntry(id, initialMeta && typeof initialMeta === "object" ? initialMeta : { status: "queued" });
    _setChatLiveStatus();

    closeChatEventSource(id);
    stopChatJobPolling(id);
    scheduleChatJobPoll(id, IS_HA_INGRESS ? 900 : 2000);

    if (typeof EventSource !== "function") {
      return;
    }

    const eventSource = new EventSource(withBasePath(`/api/chat/jobs/${encodeURIComponent(id)}/events`));
    if (!state.chatEventSources || typeof state.chatEventSources !== "object") {
      state.chatEventSources = {};
    }
    state.chatEventSources[id] = eventSource;

    eventSource.addEventListener("status", (event) => {
      const payload = safeJsonParse(event.data) || {};
      const st = String(payload.status || "running");
      const tool = String(payload.current_tool || "").trim();
      const taskName = String(payload.task_name || "").trim();
      _updateChatJobEntry(id, {
        status: st,
        current_tool: tool,
        task_name: taskName,
      });
      _setChatLiveStatus();
    });

    eventSource.addEventListener("tool", (event) => {
      const payload = safeJsonParse(event.data) || {};
      const tool = String(payload.current_tool || "").trim() || "tool";
      const taskName = String(payload.task_name || "").trim();
      _updateChatJobEntry(id, {
        status: "running",
        current_tool: tool,
        task_name: taskName,
      });
      _setChatLiveStatus();
    });

    eventSource.addEventListener("waiting", async (event) => {
      const payload = safeJsonParse(event.data) || {};
      const waitText = String(payload.wait_text || "").trim();
      if (!waitText) {
        return;
      }
      appendAssistantWaitLine(waitText);
    });

    eventSource.addEventListener("done", async (event) => {
      const payload = safeJsonParse(event.data) || {};
      await finalizeChatJob({
        jobId: id,
        statusText: "Complete.",
        responses: Array.isArray(payload.responses) ? payload.responses : [],
      });
    });

    eventSource.addEventListener("job_error", async (event) => {
      const payload = safeJsonParse(event.data) || {};
      await finalizeChatJob({
        jobId: id,
        statusText: `Job failed: ${payload.error || "unknown error"}`,
      });
    });

    eventSource.onerror = () => {
      const jobsNow = _getActiveChatJobs();
      if (!jobsNow[id]) {
        return;
      }
      // In some HA ingress/proxy setups SSE is unstable. Keep polling as the source of truth.
      closeChatEventSource(id);
      _setChatLiveStatus();
    };
  }

  await refreshChatProfile();
  await refreshChatHistory();

  const chatInputEl = document.getElementById("chat-input");
  const sendChatBtn = document.getElementById("send-chat");
  const autoSizeChatInput = () => {
    if (!chatInputEl) {
      return;
    }
    chatInputEl.style.height = "auto";
    const nextHeight = Math.min(Math.max(chatInputEl.scrollHeight, 44), 180);
    chatInputEl.style.height = `${nextHeight}px`;
  };
  autoSizeChatInput();

  if (chatFilesEl) {
    chatFilesEl.addEventListener("change", () => {
      pendingFiles = Array.from(chatFilesEl.files || []);
      updatePendingFilesUi();
    });
  }
  if (clearChatFilesBtn) {
    clearChatFilesBtn.addEventListener("click", () => {
      pendingFiles = [];
      if (chatFilesEl) {
        chatFilesEl.value = "";
      }
      updatePendingFilesUi();
    });
  }

  const sendChatMessage = async () => {
    const message = String(chatInputEl.value || "").trim();
    const filesForSend = Array.from(pendingFiles || []);
    if (!message && !filesForSend.length) {
      status.textContent = "Enter a message or attach files first.";
      return;
    }

    pendingFiles = [];
    if (chatFilesEl) {
      chatFilesEl.value = "";
    }
    updatePendingFilesUi();
    chatInputEl.value = "";
    autoSizeChatInput();

    state.chatSendInFlight = Math.max(0, Number(state.chatSendInFlight) || 0) + 1;
    state.sending = state.chatSendInFlight > 0;
    _setChatLiveStatus(filesForSend.length ? "Preparing attachments..." : "Queueing chat job...");

    try {
      const attachments = [];
      for (const file of filesForSend) {
        attachments.push({
          name: String(file?.name || "attachment").trim() || "attachment",
          mimetype: String(file?.type || "application/octet-stream").trim() || "application/octet-stream",
          data_url: await readFileAsDataUrl(file),
        });
      }
      const response = await api("/api/chat/jobs", {
        method: "POST",
        body: JSON.stringify({ message, session_id: state.sessionId, attachments }),
      });

      const sessionId = String(response.session_id || "").trim();
      if (sessionId) {
        state.sessionId = sessionId;
        safeStorageSet("tater_tateros_session_id", state.sessionId);
      }

      const jobId = String(response.job_id || "").trim();
      const taskName = String(response.task_name || "").trim();
      if (!jobId) {
        throw new Error("Backend did not return a job id.");
      }

      await refreshChatHistory();
      attachJobStream(jobId, {
        status: "queued",
        current_tool: "",
        task_name: taskName,
      });
      _setChatLiveStatus(taskName ? `Job queued: ${taskName}` : "Job queued...");
      await refreshHealth();
    } catch (error) {
      _setChatLiveStatus(`Chat failed: ${error.message}`);
      if (_isLikelyRedisFailureDetail(error?.message || "")) {
        void promptRedisSetupRecovery(String(error?.message || "Redis connection lost."));
      } else {
        _scheduleHealthRefresh(220);
      }
    } finally {
      state.chatSendInFlight = Math.max(0, Number(state.chatSendInFlight) - 1);
      state.sending = state.chatSendInFlight > 0;
    }
  };

  sendChatBtn.addEventListener("click", sendChatMessage);

  chatInputEl.addEventListener("keydown", (event) => {
    if (
      event.key === "Enter" &&
      !event.shiftKey &&
      !event.ctrlKey &&
      !event.altKey &&
      !event.metaKey &&
      !event.isComposing
    ) {
      event.preventDefault();
      sendChatMessage();
    }
  });
  chatInputEl.addEventListener("input", autoSizeChatInput);
}

async function loadVerbasView() {
  const root = document.getElementById("view-root");

  const [runtimeData, shopData] = await Promise.all([
    api("/api/verbas"),
    api("/api/shop/verbas"),
  ]);
  const runtimeHtml = buildVerbaRuntimeHtml(runtimeData, shopData);

  root.innerHTML = `${consumeNoticeHtml()}
    ${renderShopTabbedManager("verbas", shopData, {
      runtimeHtml,
      runtimeTitle: "Verba Runtime",
      runtimeCard: false,
    })}
  `;
  bindVerbaRuntimeActions(root);
  bindShopTabs("verbas");
  bindShopActions("verbas");
}

async function loadSurfaceView(kind) {
  const endpoint = kind === "cores" ? "/api/cores" : "/api/portals";
  const shopApi = kind === "cores" ? "/api/shop/cores" : "/api/shop/portals";
  let runtimeData;
  let shopData;
  let coreTabsData = { tabs: [] };
  if (kind === "cores") {
    [runtimeData, shopData, coreTabsData] = await Promise.all([api(endpoint), api(shopApi), api("/api/cores/tabs")]);
  } else {
    [runtimeData, shopData] = await Promise.all([api(endpoint), api(shopApi)]);
  }

  const root = document.getElementById("view-root");
  const runtimeHtml = buildSurfaceRuntimeHtml(kind, runtimeData, shopData);

  if (kind === "portals") {
    root.innerHTML = `${consumeNoticeHtml()}
      ${renderShopTabbedManager("portals", shopData, {
        runtimeHtml,
        runtimeTitle: "Portal Runtime",
        runtimeCard: false,
      })}
    `;
  } else {
    const manageHtml = renderShopTabbedManager("cores", shopData, {
      runtimeHtml,
      runtimeTitle: "Core Runtime",
      runtimeCard: false,
    });
    const dynamicTabs = Array.isArray(coreTabsData?.tabs) ? coreTabsData.tabs : [];
    state.coreTabSpecs = Object.fromEntries(
      dynamicTabs
        .map((tab) => {
          const key = String(tab?.core_key || "").trim();
          if (!key) {
            return null;
          }
          return [key, tab];
        })
        .filter(Boolean)
    );
    state.coreTabPayloadCache = {};
    state.coreTabLoadPromises = {};
    const manageLabel = String(coreTabsData?.manage_label || "Manage");
    root.innerHTML = `${consumeNoticeHtml()}
      ${renderCoreTopTabs(dynamicTabs, manageHtml, manageLabel)}
    `;
  }

  bindSurfaceRuntimeActions(kind, endpoint, root);

  if (kind === "portals") {
    bindShopTabs("portals");
  } else if (kind === "cores") {
    bindCoreTopTabs();
    bindCoreTabManagers();
    bindShopTabs("cores");
  }
  bindShopActions(kind);
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

function summarizeRedisEncryptionStatusPayload(entry) {
  const status = normalizeRedisEncryptionStatusPayload(entry);
  if (!status.encryption_available) {
    return {
      text: status.error || "Redis encryption tools are unavailable.",
      tone: "error",
    };
  }
  if (status.error) {
    return {
      text: status.error,
      tone: "error",
    };
  }
  if (status.live_encryption_enabled) {
    const fingerprint = status.key_fingerprint ? ` Key: ${status.key_fingerprint}.` : "";
    const updated = status.live_encryption_updated ? ` Updated: ${status.live_encryption_updated}.` : "";
    return {
      text: `Live Redis encryption is enabled.${fingerprint}${updated}`.trim(),
      tone: "success",
    };
  }
  if (status.key_exists) {
    return {
      text: status.key_fingerprint
        ? `Encryption key ready (${status.key_fingerprint}). Live Redis encryption is currently disabled.`
        : "Encryption key ready. Live Redis encryption is currently disabled.",
      tone: "normal",
    };
  }
  return {
    text: "No Redis encryption key found yet. Encrypting live Redis will create one automatically.",
    tone: "normal",
  };
}

function renderRedisEncryptionStatusSummaryHtml(entry) {
  const summary = summarizeRedisEncryptionStatusPayload(entry);
  const cssClass =
    summary.tone === "success" ? "redis-live-state-enabled" : summary.tone === "error" ? "redis-live-state-disabled" : "";
  const text = escapeHtml(summary.text);
  if (cssClass) {
    return `<span class="${cssClass}">${text}</span>`;
  }
  return text;
}

function renderSettingsRedisSectionHtml(redisStatus, redisEncryptionStatus, { includeEncryption = true } = {}) {
  return `
    <section class="settings-tab-panel active" data-settings-panel="redis">
      <div class="form-grid">
        <section class="core-inline-section">
          <div class="small core-inline-section-title">Redis Connection</div>
          <div class="form-grid two-col">
            <label>Host
              <input id="set_redis_host" type="text" value="${escapeHtml(redisStatus.host || "")}" />
            </label>
            <label>Port
              <input id="set_redis_port" type="number" min="1" max="65535" value="${escapeHtml(redisStatus.port || 6379)}" />
            </label>
            <label>DB
              <input id="set_redis_db" type="number" min="0" value="${escapeHtml(redisStatus.db || 0)}" />
            </label>
            <label>Username (optional)
              <input id="set_redis_username" type="text" value="${escapeHtml(redisStatus.username || "")}" />
            </label>
            <label>Password (optional)
              <input id="set_redis_password" type="password" autocomplete="new-password" />
            </label>
            <label>CA Cert Path (optional)
              <input id="set_redis_ca_cert_path" type="text" value="${escapeHtml(redisStatus.ca_cert_path || "")}" />
            </label>
            <label class="toggle-row">Use TLS
              <input id="set_redis_use_tls" type="checkbox" ${redisStatus.use_tls ? "checked" : ""} />
            </label>
            <label class="toggle-row">Verify TLS Cert
              <input id="set_redis_verify_tls" type="checkbox" ${redisStatus.verify_tls ? "checked" : ""} />
            </label>
            <div class="inline-row" style="grid-column: 1 / -1;">
              <button type="button" id="settings-redis-refresh" class="inline-btn">Refresh</button>
              <button type="button" id="settings-redis-test" class="inline-btn">Test Connection</button>
              <button type="button" id="settings-redis-save" class="action-btn">Save Redis</button>
            </div>
            <div id="settings-redis-status" class="small" style="grid-column: 1 / -1;">
              ${escapeHtml(_redisSetupMessage(redisStatus))}
            </div>
          </div>
        </section>

        ${
          includeEncryption
            ? `<section class="core-inline-section">
                <div class="small core-inline-section-title">Redis Encryption</div>
                <div class="form-grid two-col">
                  <label>Key File
                    <input id="set_redis_encryption_key_path" type="text" value="${escapeHtml(
                      redisEncryptionStatus.key_path || ""
                    )}" readonly />
                  </label>
                  <label>Mode State File
                    <input id="set_redis_encryption_snapshot_path" type="text" value="${escapeHtml(
                      redisEncryptionStatus.live_encryption_state_path || ""
                    )}" readonly />
                  </label>
                  <div class="inline-row" style="grid-column: 1 / -1;">
                    <button type="button" id="settings-redis-encrypt" class="action-btn">Encrypt Live Redis</button>
                    <button type="button" id="settings-redis-decrypt" class="inline-btn danger">Decrypt Live Redis</button>
                  </div>
                  <div class="small" style="grid-column: 1 / -1;">
                    Encrypt auto-creates a key if needed, encrypts existing Redis values in place, and keeps future writes encrypted. Decrypt reverses values in place and returns to plaintext writes.
                  </div>
                  <div id="settings-redis-encryption-status" class="small" style="grid-column: 1 / -1;">
                    ${renderRedisEncryptionStatusSummaryHtml(redisEncryptionStatus)}
                  </div>
                </div>
              </section>`
            : ""
        }
      </div>
    </section>
  `;
}

function bindSettingsRedisSection({
  statusTargetEl,
  initialRedisStatus,
  initialRedisEncryptionStatus,
  includeEncryption = true,
  onConnected = null,
} = {}) {
  let redisEncryptionState = normalizeRedisEncryptionStatusPayload(initialRedisEncryptionStatus);
  const redisHostEl = document.getElementById("set_redis_host");
  const redisPortEl = document.getElementById("set_redis_port");
  const redisDbEl = document.getElementById("set_redis_db");
  const redisUsernameEl = document.getElementById("set_redis_username");
  const redisPasswordEl = document.getElementById("set_redis_password");
  const redisUseTlsEl = document.getElementById("set_redis_use_tls");
  const redisVerifyTlsEl = document.getElementById("set_redis_verify_tls");
  const redisCaCertPathEl = document.getElementById("set_redis_ca_cert_path");
  const redisStatusLineEl = document.getElementById("settings-redis-status");
  const redisRefreshBtnEl = document.getElementById("settings-redis-refresh");
  const redisTestBtnEl = document.getElementById("settings-redis-test");
  const redisSaveBtnEl = document.getElementById("settings-redis-save");
  const redisEncryptionStatusEl = document.getElementById("settings-redis-encryption-status");
  const redisEncryptionKeyPathEl = document.getElementById("set_redis_encryption_key_path");
  const redisEncryptionSnapshotPathEl = document.getElementById("set_redis_encryption_snapshot_path");
  const redisEncryptionEncryptBtnEl = document.getElementById("settings-redis-encrypt");
  const redisEncryptionDecryptBtnEl = document.getElementById("settings-redis-decrypt");

  const setRedisStatusMessage = (message) => {
    if (statusTargetEl) {
      statusTargetEl.textContent = String(message || "").trim();
    }
  };

  const maybeReloadFullSettings = async (status) => {
    if (!status?.connected || typeof onConnected !== "function") {
      return;
    }
    await onConnected(status);
  };

  const setRedisBusy = (busy) => {
    const disabled = Boolean(busy);
    if (disabled) {
      [redisRefreshBtnEl, redisTestBtnEl, redisSaveBtnEl, redisEncryptionEncryptBtnEl, redisEncryptionDecryptBtnEl]
        .filter(Boolean)
        .forEach((button) => {
          button.disabled = true;
        });
      return;
    }
    [redisRefreshBtnEl, redisTestBtnEl, redisSaveBtnEl]
      .filter(Boolean)
      .forEach((button) => {
        button.disabled = false;
      });
    applyRedisEncryptionStatus(redisEncryptionState);
  };

  const syncRedisTlsUi = () => {
    const enabled = Boolean(redisUseTlsEl?.checked);
    if (redisVerifyTlsEl) {
      redisVerifyTlsEl.disabled = !enabled;
    }
    if (redisCaCertPathEl) {
      redisCaCertPathEl.disabled = !enabled;
      if (!enabled) {
        redisCaCertPathEl.value = "";
      }
    }
  };

  const redisFormPayload = (testOnly) => {
    const maskedPassword = String(redisPasswordEl?.dataset?.masked || "") === "1";
    const passwordRaw = maskedPassword ? "" : String(redisPasswordEl?.value || "");
    const keepExistingPassword = (maskedPassword || !passwordRaw) && Boolean(state.redisStatus?.password_set);
    return {
      host: String(redisHostEl?.value || "").trim(),
      port: Number(redisPortEl?.value || 6379),
      db: Number(redisDbEl?.value || 0),
      username: String(redisUsernameEl?.value || "").trim(),
      password: passwordRaw,
      use_tls: Boolean(redisUseTlsEl?.checked),
      verify_tls: Boolean(redisVerifyTlsEl?.checked),
      ca_cert_path: String(redisCaCertPathEl?.value || "").trim(),
      keep_existing_password: keepExistingPassword,
      test_only: Boolean(testOnly),
    };
  };

  const applyRedisConnectionStatus = (raw) => {
    const status = _setRedisStatus(raw);
    if (redisHostEl) {
      redisHostEl.value = String(status.host || "");
    }
    if (redisPortEl) {
      redisPortEl.value = String(status.port || 6379);
    }
    if (redisDbEl) {
      redisDbEl.value = String(status.db || 0);
    }
    if (redisUsernameEl) {
      redisUsernameEl.value = String(status.username || "");
    }
    if (redisUseTlsEl) {
      redisUseTlsEl.checked = Boolean(status.use_tls);
    }
    if (redisVerifyTlsEl) {
      redisVerifyTlsEl.checked = Boolean(status.verify_tls);
    }
    if (redisCaCertPathEl) {
      redisCaCertPathEl.value = String(status.ca_cert_path || "");
    }
    if (redisPasswordEl) {
      if (status.password_set) {
        redisPasswordEl.value = REDIS_PASSWORD_MASK;
        redisPasswordEl.dataset.masked = "1";
      } else {
        redisPasswordEl.value = "";
        delete redisPasswordEl.dataset.masked;
      }
      redisPasswordEl.placeholder = status.password_set ? "Leave blank to keep saved password" : "";
    }
    syncRedisTlsUi();
    if (redisStatusLineEl) {
      redisStatusLineEl.textContent = _redisSetupMessage(status);
      redisStatusLineEl.classList.toggle("error", !status.connected);
      redisStatusLineEl.classList.toggle("success", Boolean(status.connected));
    }
    return status;
  };

  const applyRedisEncryptionStatus = (raw) => {
    redisEncryptionState = normalizeRedisEncryptionStatusPayload(raw);
    if (redisEncryptionKeyPathEl) {
      redisEncryptionKeyPathEl.value = String(redisEncryptionState.key_path || "");
    }
    if (redisEncryptionSnapshotPathEl) {
      redisEncryptionSnapshotPathEl.value = String(redisEncryptionState.live_encryption_state_path || "");
    }
    if (redisEncryptionStatusEl) {
      redisEncryptionStatusEl.innerHTML = renderRedisEncryptionStatusSummaryHtml(redisEncryptionState);
      const hasError = Boolean(redisEncryptionState.error) || !Boolean(redisEncryptionState.encryption_available);
      redisEncryptionStatusEl.classList.toggle("error", hasError);
      redisEncryptionStatusEl.classList.toggle("success", !hasError);
    }
    if (redisEncryptionEncryptBtnEl) {
      const encryptDisabled =
        !redisEncryptionState.encryption_available || Boolean(redisEncryptionState.live_encryption_enabled);
      redisEncryptionEncryptBtnEl.disabled = encryptDisabled;
      redisEncryptionEncryptBtnEl.title = redisEncryptionState.live_encryption_enabled
        ? "Live encryption is already enabled."
        : "";
    }
    if (redisEncryptionDecryptBtnEl) {
      redisEncryptionDecryptBtnEl.disabled = !redisEncryptionState.encryption_available || !redisEncryptionState.live_encryption_enabled;
    }
    return redisEncryptionState;
  };

  const refreshRedisSection = async () => {
    const nextStatus = await api("/api/redis/status", {
      _skipRedisRecovery: true,
      _timeoutMs: REDIS_STATUS_TIMEOUT_MS,
    });
    const appliedStatus = applyRedisConnectionStatus(nextStatus);
    if (includeEncryption) {
      applyRedisEncryptionStatus(
        await api("/api/redis/encryption/status", {
          _skipRedisRecovery: true,
          _timeoutMs: HEALTH_REQUEST_TIMEOUT_MS,
        })
      );
    }
    return appliedStatus;
  };

  applyRedisConnectionStatus(initialRedisStatus);
  applyRedisEncryptionStatus(redisEncryptionState);
  redisUseTlsEl?.addEventListener("change", syncRedisTlsUi);
  redisPasswordEl?.addEventListener("focus", () => {
    if (String(redisPasswordEl?.dataset?.masked || "") === "1") {
      redisPasswordEl.value = "";
      delete redisPasswordEl.dataset.masked;
    }
  });
  redisPasswordEl?.addEventListener("input", () => {
    if (String(redisPasswordEl?.dataset?.masked || "") === "1" && String(redisPasswordEl.value || "") !== REDIS_PASSWORD_MASK) {
      delete redisPasswordEl.dataset.masked;
    }
  });
  redisRefreshBtnEl?.addEventListener("click", async () => {
    setRedisBusy(true);
    setRedisStatusMessage("Refreshing Redis status...");
    try {
      const nextStatus = await refreshRedisSection();
      setRedisStatusMessage("Redis status refreshed.");
      await maybeReloadFullSettings(nextStatus);
    } catch (error) {
      setRedisStatusMessage(`Redis refresh failed: ${error.message}`);
    } finally {
      setRedisBusy(false);
    }
  });
  redisTestBtnEl?.addEventListener("click", async () => {
    setRedisBusy(true);
    setRedisStatusMessage("Testing Redis connection...");
    try {
      const result = await api("/api/redis/configure", {
        method: "POST",
        body: JSON.stringify(redisFormPayload(true)),
        _skipRedisRecovery: true,
      });
      applyRedisConnectionStatus(result);
      setRedisStatusMessage("Redis connection test succeeded.");
    } catch (error) {
      setRedisStatusMessage(`Redis test failed: ${error.message}`);
    } finally {
      setRedisBusy(false);
    }
  });
  redisSaveBtnEl?.addEventListener("click", async () => {
    setRedisBusy(true);
    setRedisStatusMessage("Saving Redis settings...");
    try {
      const result = await api("/api/redis/configure", {
        method: "POST",
        body: JSON.stringify(redisFormPayload(false)),
        _skipRedisRecovery: true,
      });
      const nextStatus = applyRedisConnectionStatus(result);
      const replay = result?.bootstrap_replay && typeof result.bootstrap_replay === "object" ? result.bootstrap_replay : null;
      if (replay && replay.ok === false) {
        setRedisStatusMessage(`Redis saved, but startup replay failed: ${String(replay.error || "unknown error")}`);
      } else if (replay) {
        setRedisStatusMessage(
          `Redis saved. Startup replay complete (restore: ${replay.ran_restore ? "ran" : "skipped"}, autostart: ${
            replay.ran_autostart ? "ran" : "skipped"
          }).`
        );
      } else {
        setRedisStatusMessage("Redis settings saved.");
      }
      await refreshHealth();
      await maybeReloadFullSettings(nextStatus);
    } catch (error) {
      setRedisStatusMessage(`Redis save failed: ${error.message}`);
    } finally {
      setRedisBusy(false);
    }
  });
  redisEncryptionEncryptBtnEl?.addEventListener("click", async () => {
    setRedisBusy(true);
    setRedisStatusMessage("Encrypting live Redis values (auto-creating key if needed)...");
    try {
      const result = await runActionWithProgress(
        {
          title: "Encrypting Redis",
          detail: "Pausing active runtimes and encrypting Redis values in place",
          workingText: "Encrypting live Redis values...",
          successText: "Redis encryption complete.",
          errorPrefix: "Redis encryption failed",
        },
        () => api("/api/redis/encryption/encrypt", { method: "POST", _skipRedisRecovery: true })
      );
      applyRedisEncryptionStatus(result?.encryption_status || (await api("/api/redis/encryption/status", { _skipRedisRecovery: true })));
      const keySuffix = result?.key_created ? " New encryption key generated." : "";
      setRedisStatusMessage(`Encrypted ${Number(result?.keys_encrypted || 0)} Redis value(s); live encryption enabled.${keySuffix}`);
    } catch (error) {
      setRedisStatusMessage(`Redis encryption failed: ${error.message}`);
    } finally {
      setRedisBusy(false);
    }
  });
  redisEncryptionDecryptBtnEl?.addEventListener("click", async () => {
    if (!window.confirm("Decrypt live Redis values now? This switches future writes back to plaintext.")) {
      return;
    }
    setRedisBusy(true);
    setRedisStatusMessage("Decrypting live Redis values...");
    try {
      const result = await runActionWithProgress(
        {
          title: "Decrypting Redis",
          detail: "Pausing active runtimes and decrypting Redis values in place",
          workingText: "Decrypting live Redis values...",
          successText: "Redis decryption complete.",
          errorPrefix: "Redis decrypt failed",
        },
        () => api("/api/redis/encryption/decrypt", { method: "POST", _skipRedisRecovery: true })
      );
      applyRedisEncryptionStatus(
        result?.encryption_status || (await api("/api/redis/encryption/status", { _skipRedisRecovery: true }))
      );
      await refreshRedisSection();
      await refreshHealth();
      const replay = result?.bootstrap_replay && typeof result.bootstrap_replay === "object" ? result.bootstrap_replay : null;
      if (replay && replay.ok === false) {
        setRedisStatusMessage(
          `Decrypted ${Number(result?.restored_keys || 0)} Redis value(s), but startup replay failed: ${replay.error || "unknown error"}`
        );
      } else {
        setRedisStatusMessage(`Decrypted ${Number(result?.restored_keys || 0)} Redis value(s); live encryption disabled.`);
      }
    } catch (error) {
      setRedisStatusMessage(`Redis decrypt failed: ${error.message}`);
    } finally {
      setRedisBusy(false);
    }
  });
}

function setRedisBootstrapMode(enabled) {
  document.body?.classList.toggle("redis-bootstrap-mode", Boolean(enabled));
}

function renderRedisBootstrapView(root, redisStatus, redisEncryptionStatus) {
  setRedisBootstrapMode(true);
  document.body.dataset.view = "redis-bootstrap";
  root.dataset.view = "redis-bootstrap";
  root.innerHTML = `
    <div class="redis-bootstrap-wrap">
      <section class="card redis-bootstrap-card">
        <div class="redis-bootstrap-brand">
          <div class="brand-dot" aria-hidden="true"></div>
          <div>
            <div class="redis-bootstrap-kicker">TaterOS Startup</div>
            <h1 class="redis-bootstrap-title">Connect Redis</h1>
            <p class="redis-bootstrap-copy">
              Tater couldn’t reach Redis during startup, so the normal WebUI is waiting. Update the Redis connection below and once it connects, Tater will reload normally.
            </p>
          </div>
        </div>
        <div id="settings-status" class="small redis-bootstrap-status"></div>
        ${renderSettingsRedisSectionHtml(redisStatus, redisEncryptionStatus, { includeEncryption: false })}
      </section>
    </div>
  `;

  const statusEl = document.getElementById("settings-status");
  if (statusEl) {
    statusEl.textContent = _redisRecoveryNotice(redisStatus?.error || "");
    statusEl.classList.add("error");
  }
  bindSettingsRedisSection({
    statusTargetEl: statusEl,
    initialRedisStatus: redisStatus,
    initialRedisEncryptionStatus: redisEncryptionStatus,
    includeEncryption: false,
    onConnected: async () => {
      window.location.reload();
    },
  });
}

async function loadSettingsView() {
  const root = document.getElementById("view-root");
  setRedisBootstrapMode(false);
  const [redisStatusPayload, redisEncryptionPayload] = await Promise.all([
    api("/api/redis/status", { _skipRedisRecovery: true, _timeoutMs: REDIS_STATUS_TIMEOUT_MS }),
    api("/api/redis/encryption/status", { _skipRedisRecovery: true, _timeoutMs: HEALTH_REQUEST_TIMEOUT_MS }),
  ]);
  const redisStatus = _setRedisStatus(redisStatusPayload);
  const redisEncryptionStatus = normalizeRedisEncryptionStatusPayload(redisEncryptionPayload);
  const settings = await api("/api/settings");
  let webuiPasswordIsSet = Boolean(settings?.webui_password_set);
  const adminOptions = Array.isArray(settings.admin_plugin_options)
    ? settings.admin_plugin_options.map((value) => String(value || "").trim()).filter(Boolean)
    : [];
  const adminSelected = new Set(
    (Array.isArray(settings.admin_only_plugins) ? settings.admin_only_plugins : [])
      .map((value) => String(value || "").trim())
      .filter(Boolean)
  );
  const adminDefaults = new Set(
    (Array.isArray(settings.admin_only_plugins_defaults) ? settings.admin_only_plugins_defaults : [])
      .map((value) => String(value || "").trim())
      .filter(Boolean)
  );
  const hydraDefaults =
    settings?.hydra_defaults && typeof settings.hydra_defaults === "object" ? settings.hydra_defaults : {};
  const normalizeHydraBaseRow = (row) => ({
    host: String(row?.host || "").trim(),
    port: String(row?.port || "").trim(),
    model: String(row?.model || "").trim(),
  });
  const configuredHydraBaseRows = Array.isArray(settings?.hydra_base_servers)
    ? settings.hydra_base_servers.map((row) => normalizeHydraBaseRow(row))
    : [];
  const normalizedHydraBaseRows = [];
  const normalizedHydraBaseSeen = new Set();
  const appendHydraBaseRow = (row) => {
    const normalized = normalizeHydraBaseRow(row);
    if (!normalized.host || !normalized.model) {
      return;
    }
    const signature = `${normalized.host}|${normalized.port}|${normalized.model}`;
    if (normalizedHydraBaseSeen.has(signature)) {
      return;
    }
    normalizedHydraBaseSeen.add(signature);
    normalizedHydraBaseRows.push(normalized);
  };
  if (configuredHydraBaseRows.length) {
    configuredHydraBaseRows.forEach((row) => appendHydraBaseRow(row));
  } else {
    appendHydraBaseRow({
      host: settings.hydra_llm_host || "",
      port: settings.hydra_llm_port || "",
      model: settings.hydra_llm_model || "",
    });
  }
  if (!normalizedHydraBaseRows.length) {
    normalizedHydraBaseRows.push(
      normalizeHydraBaseRow({
        host: settings.hydra_llm_host || "",
        port: settings.hydra_llm_port || "",
        model: settings.hydra_llm_model || "",
      })
    );
  }
  const hydraPrimaryBaseRow = normalizedHydraBaseRows[0] || normalizeHydraBaseRow({});
  const hydraAdditionalBaseRows = normalizedHydraBaseRows.slice(1);
  const hydraAdditionalBaseRowsHtml = hydraAdditionalBaseRows.length
    ? hydraAdditionalBaseRows
        .map(
          (row, index) => `
            <div class="hydra-base-server-row" data-hydra-base-index="${index}">
              <label>Host / IP
                <input type="text" data-hydra-base-field="host" value="${escapeHtml(row.host)}" />
              </label>
              <label>Port
                <input type="number" min="1" max="65535" data-hydra-base-field="port" value="${escapeHtml(row.port)}" />
              </label>
              <label>Model
                <input type="text" data-hydra-base-field="model" value="${escapeHtml(row.model)}" />
              </label>
              <div class="hydra-base-server-actions">
                <button type="button" class="inline-btn danger" data-hydra-base-remove="${index}">Remove</button>
              </div>
            </div>
          `
        )
        .join("")
    : `<div class="small hydra-base-server-empty">No additional base servers configured.</div>`;
  const popupEffectStyle = normalizePopupEffectStyle(settings?.popup_effect_style || state.popupEffectStyle);
  applyPopupEffectStyle(popupEffectStyle);
  const hydraPlatforms = ["webui", "discord", "irc", "telegram", "matrix", "homeassistant", "homekit", "xbmc", "automation"];
  const hydraPlatformOptionsHtml = hydraPlatforms
    .map((platform) => `<option value="${escapeHtml(platform)}">${escapeHtml(hydraPlatformLabel(platform))}</option>`)
    .join("");

  const adminOptionHtml = adminOptions.length
    ? adminOptions
        .map((pluginId) => {
          const selected = adminSelected.has(pluginId) ? "selected" : "";
          return `<option value="${escapeHtml(pluginId)}" ${selected}>${escapeHtml(pluginId)}</option>`;
        })
        .join("")
    : `<option value="" disabled>(No plugin ids available)</option>`;
  const speechUi = settings?.speech_ui && typeof settings.speech_ui === "object" ? settings.speech_ui : {};
  const announcementSpeechUi =
    settings?.announcement_speech_ui && typeof settings.announcement_speech_ui === "object"
      ? settings.announcement_speech_ui
      : {};
  const speechSttBackendOptions = Array.isArray(speechUi.stt_backend_options) ? speechUi.stt_backend_options : [];
  const speechTtsBackendOptions = Array.isArray(speechUi.tts_backend_options) ? speechUi.tts_backend_options : [];
  const speechTtsModelOptionsByBackend =
    speechUi.tts_model_options_by_backend && typeof speechUi.tts_model_options_by_backend === "object"
      ? speechUi.tts_model_options_by_backend
      : {};
  const speechTtsVoiceOptionsByModel =
    speechUi.tts_voice_options_by_model && typeof speechUi.tts_voice_options_by_model === "object"
      ? speechUi.tts_voice_options_by_model
      : {};
  const announcementTtsBackendOptions = Array.isArray(announcementSpeechUi.tts_backend_options)
    ? announcementSpeechUi.tts_backend_options
    : [];
  const announcementTtsModelOptionsByBackend =
    announcementSpeechUi.tts_model_options_by_backend && typeof announcementSpeechUi.tts_model_options_by_backend === "object"
      ? announcementSpeechUi.tts_model_options_by_backend
      : {};
  const announcementTtsVoiceOptionsByModel =
    announcementSpeechUi.tts_voice_options_by_model && typeof announcementSpeechUi.tts_voice_options_by_model === "object"
      ? announcementSpeechUi.tts_voice_options_by_model
      : {};
  const renderSettingsSelectOptions = (options, currentValue, { blankLabel = null } = {}) => {
    const current = String(currentValue || "").trim();
    const normalized = Array.isArray(options)
      ? options
          .map((row) => ({
            value: String(row?.value || "").trim(),
            label: String(row?.label || row?.value || "").trim(),
          }))
          .filter((row) => row.value)
      : [];
    if (blankLabel !== null && !normalized.some((row) => row.value === "")) {
      normalized.unshift({ value: "", label: String(blankLabel || "").trim() || "Default" });
    }
    if (current && !normalized.some((row) => row.value === current)) {
      normalized.push({ value: current, label: current });
    }
    return normalized
      .map((row) => {
        const selected = row.value === current ? "selected" : "";
        return `<option value="${escapeHtml(row.value)}" ${selected}>${escapeHtml(row.label || row.value)}</option>`;
      })
      .join("");
  };
  const currentSpeechTtsBackend = String(settings.speech_tts_backend || "wyoming").trim() || "wyoming";
  const initialSpeechTtsModelOptions = Array.isArray(speechTtsModelOptionsByBackend[currentSpeechTtsBackend])
    ? speechTtsModelOptionsByBackend[currentSpeechTtsBackend]
    : [];
  const currentSpeechTtsModel = String(settings.speech_tts_model || "").trim();
  const initialSpeechTtsVoiceOptions = Array.isArray(speechTtsVoiceOptionsByModel[currentSpeechTtsModel])
    ? speechTtsVoiceOptionsByModel[currentSpeechTtsModel]
    : [];
  const currentAnnouncementTtsBackend =
    String(settings.speech_announcement_tts_backend || settings.speech_tts_backend || "wyoming").trim() || "wyoming";
  const initialAnnouncementTtsModelOptions = Array.isArray(announcementTtsModelOptionsByBackend[currentAnnouncementTtsBackend])
    ? announcementTtsModelOptionsByBackend[currentAnnouncementTtsBackend]
    : [];
  const currentAnnouncementTtsModel = String(settings.speech_announcement_tts_model || "").trim();
  const initialAnnouncementTtsVoiceOptions = Array.isArray(announcementTtsVoiceOptionsByModel[currentAnnouncementTtsModel])
    ? announcementTtsVoiceOptionsByModel[currentAnnouncementTtsModel]
    : [];
  const esphomeUi = settings?.esphome_ui && typeof settings.esphome_ui === "object" ? settings.esphome_ui : {};
  const esphomeFields = Array.isArray(esphomeUi.fields) ? esphomeUi.fields : [];
  const esphomeFieldsHtml = esphomeFields.length
    ? esphomeFields.map((field) => renderCoreManagerField(field)).join("")
    : renderNotice("ESPHome settings are unavailable right now.");
  const esphomeSections = Array.isArray(esphomeUi.sections) ? esphomeUi.sections : [];
  const esphomeExperimentalSection = esphomeSections.find(
    (section) => String(section?.label || "").trim().toLowerCase() === "experimental"
  ) || null;
  const esphomeExperimentalFields = Array.isArray(esphomeExperimentalSection?.fields) ? esphomeExperimentalSection.fields : [];
  const esphomeExperimentalFieldsHtml = esphomeExperimentalFields.length
    ? esphomeExperimentalFields.map((field) => renderCoreManagerField(field)).join("")
    : "";
  const esphomePipelineSections = esphomeSections.filter(
    (section) => String(section?.label || "").trim().toLowerCase() !== "experimental"
  );
  const esphomePipelineFields = esphomePipelineSections.flatMap((section) =>
    Array.isArray(section?.fields) ? section.fields : []
  );
  const esphomePipelineFieldsHtml = esphomePipelineFields.length
    ? esphomePipelineFields.map((field) => renderCoreManagerField(field)).join("")
    : esphomeFieldsHtml;
  const esphomeRunning = boolFromAny(esphomeUi.running, false);

  root.innerHTML = `${consumeNoticeHtml()}
    <div class="card">
      <div class="card-head">
        <h3 class="card-title">Settings</h3>
      </div>
      <div class="small">Categories: General, Models, Hydra, Integrations, ESPHome, Redis, Misc, Advanced.</div>
      <div id="settings-status" class="small" style="margin-top: 6px;"></div>

      <div class="settings-tabs">
        <button type="button" class="settings-tab-btn active" data-settings-tab="general">General</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="models">Models</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="hydra">Hydra</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="integrations">Integrations</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="esphome">ESPHome</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="redis">Redis</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="misc">Misc</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="advanced">Advanced</button>
      </div>

      <form id="settings-form">
        <section class="settings-tab-panel active" data-settings-panel="general">
          <div class="form-grid">
            <section class="core-inline-section">
              <div class="small core-inline-section-title">General</div>
              <div class="form-grid two-col">
                <label>WebUI Username
                  <input id="set_username" type="text" value="${escapeHtml(settings.username || "User")}" />
                </label>
                <label>Show tokens/sec stats
                  ${renderToggleRow(`<input id="set_show_speed_stats" class="toggle-input" type="checkbox" ${settings.show_speed_stats ? "checked" : ""} />`)}
                </label>
                <label>Tater First Name
                  <input id="set_tater_first_name" type="text" value="${escapeHtml(settings.tater_first_name || "Tater")}" />
                </label>
                <label>Tater Last Name
                  <input id="set_tater_last_name" type="text" value="${escapeHtml(settings.tater_last_name || "Totterson")}" />
                </label>
                <label style="grid-column: 1 / -1;">Personality / Style
                  <textarea id="set_tater_personality">${escapeHtml(settings.tater_personality || "")}</textarea>
                </label>
              </div>
            </section>

            <section class="core-inline-section">
              <div class="small core-inline-section-title">WebUI Login</div>
              <div class="form-grid two-col">
                <label>WebUI Password
                  <input
                    id="set_webui_password"
                    type="password"
                    autocomplete="new-password"
                    placeholder="Leave blank to keep current password"
                  />
                </label>
                <label>Repeat WebUI Password
                  <input
                    id="set_webui_password_confirm"
                    type="password"
                    autocomplete="new-password"
                    placeholder="Repeat new password"
                  />
                </label>
                <div class="inline-row" style="grid-column: 1 / -1;">
                  <button
                    type="button"
                    id="settings-webui-password-clear"
                    class="inline-btn danger"
                    ${webuiPasswordIsSet ? "" : "disabled"}
                  >
                    Remove WebUI Password
                  </button>
                  <span id="settings-webui-password-status" class="small">${
                    webuiPasswordIsSet
                      ? "WebUI password is enabled. Login is required."
                      : "No WebUI password set. Login is not required."
                  }</span>
                </div>
              </div>
            </section>

            <section class="core-inline-section">
              <div class="small core-inline-section-title">Chat Avatars</div>
              <div class="settings-avatar-grid">
                <div class="settings-avatar-card">
                  <div class="small">WebUI User Avatar</div>
                  ${
                    settings.user_avatar
                      ? `<img id="set_user_avatar_preview" class="settings-avatar-preview" src="${escapeHtml(
                          settings.user_avatar
                        )}" alt="User avatar preview" />`
                      : `<div id="set_user_avatar_preview" class="settings-avatar-preview fallback">${escapeHtml(
                          _avatarInitial(settings.username || "User", "U")
                        )}</div>`
                  }
                  <div class="settings-avatar-controls">
                    <input id="set_user_avatar_file" type="file" accept="image/png,image/jpeg,image/gif,image/webp" />
                    <button type="button" id="set_user_avatar_clear" class="inline-btn danger">Clear User Avatar</button>
                  </div>
                </div>
                <div class="settings-avatar-card">
                  <div class="small">Tater Avatar</div>
                  ${
                    settings.tater_avatar
                      ? `<img id="set_tater_avatar_preview" class="settings-avatar-preview" src="${escapeHtml(
                          settings.tater_avatar
                        )}" alt="Tater avatar preview" />`
                      : `<div id="set_tater_avatar_preview" class="settings-avatar-preview fallback">${escapeHtml(
                          _avatarInitial(settings.tater_first_name || "Tater", "T")
                        )}</div>`
                  }
                  <div class="settings-avatar-controls">
                    <input id="set_tater_avatar_file" type="file" accept="image/png,image/jpeg,image/gif,image/webp" />
                    <button type="button" id="set_tater_avatar_clear" class="inline-btn danger">Clear Tater Avatar</button>
                  </div>
                </div>
              </div>
            </section>

            <div class="inline-row" style="grid-column: 1 / -1;">
              <button type="button" id="settings-save-general" class="action-btn">Save Settings</button>
              <span class="small">Saves General and non-model settings.</span>
            </div>
          </div>
        </section>

        <section class="settings-tab-panel" data-settings-panel="models">
          <div class="form-grid two-col">
            <div class="settings-section-title">Models</div>
            <div class="small" style="grid-column: 1 / -1;">
              Shared model routing for Tater, including base LLM routing, vision, STT, and TTS.
            </div>
            <div class="hydra-model-mode" style="grid-column: 1 / -1;">
              <div class="small hydra-model-mode-label">Beast Mode Routing</div>
              ${renderToggleRow(
                `<input id="set_hydra_beast_mode_enabled" class="toggle-input" type="checkbox" ${
                  settings.hydra_beast_mode_enabled ? "checked" : ""
                } />`
              )}
              <div class="small">Off: regular calls use Base model servers. On: dedicated models per head, while AI Calls still uses Base model servers.</div>
            </div>
            <div id="settings-hydra-model-stack" class="hydra-model-stack">
              <div id="settings-hydra-base-fields" class="hydra-model-panel is-active">
                <div class="hydra-model-panel-title">Base Model</div>
                <div class="small hydra-model-panel-note">Used for regular AI calls. Multiple base servers rotate in round-robin order.</div>
                <label>Base Host / IP
                  <input id="set_hydra_llm_host" type="text" value="${escapeHtml(
                    hydraPrimaryBaseRow.host || ""
                  )}" />
                </label>
                <label>Base Port
                  <input id="set_hydra_llm_port" type="number" min="1" max="65535" value="${escapeHtml(
                    hydraPrimaryBaseRow.port || ""
                  )}" />
                </label>
                <label style="grid-column: 1 / -1;">Base Model
                  <input id="set_hydra_llm_model" type="text" value="${escapeHtml(
                    hydraPrimaryBaseRow.model || ""
                  )}" />
                </label>
                <div class="hydra-base-server-block">
                  <div class="hydra-role-title">Additional Base Servers</div>
                  <div class="small hydra-model-panel-note">Add more base servers to alternate regular AI calls across them.</div>
                  <div id="settings-hydra-base-servers" class="hydra-base-server-list">${hydraAdditionalBaseRowsHtml}</div>
                  <div class="inline-row">
                    <button type="button" id="settings-hydra-base-server-add" class="inline-btn">Add Server</button>
                  </div>
                </div>
              </div>

              <div class="hydra-model-panel is-active">
                <div class="hydra-model-panel-title">Vision Model</div>
                <div class="small hydra-model-panel-note">Used for image tools and vision-enabled requests.</div>
                <label>Vision API Base URL
                  <input id="set_vision_api_base" type="text" value="${escapeHtml(
                    settings.vision_api_base || "http://127.0.0.1:1234"
                  )}" />
                </label>
                <label>Vision Model
                  <input id="set_vision_model" type="text" value="${escapeHtml(
                    settings.vision_model || "qwen2.5-vl-7b-instruct"
                  )}" />
                </label>
                <label style="grid-column: 1 / -1;">Vision API Key (optional)
                  <input id="set_vision_api_key" type="password" value="${escapeHtml(settings.vision_api_key || "")}" />
                </label>
              </div>

              <div class="hydra-model-panel is-active">
                <div class="hydra-model-panel-title">STT</div>
                <div class="small hydra-model-panel-note">
                  Shared globally across Tater, cores, and verbas.
                </div>
                <label>STT Backend
                  <select id="set_speech_stt_backend">
                    ${renderSettingsSelectOptions(speechSttBackendOptions, settings.speech_stt_backend || "faster_whisper")}
                  </select>
                </label>
                <label id="speech-wyoming-stt-host-wrap">Wyoming STT Host
                  <input id="set_speech_wyoming_stt_host" type="text" value="${escapeHtml(
                    settings.speech_wyoming_stt_host || "127.0.0.1"
                  )}" />
                </label>
                <label id="speech-wyoming-stt-port-wrap">Wyoming STT Port
                  <input id="set_speech_wyoming_stt_port" type="number" min="1" max="65535" value="${escapeHtml(
                    settings.speech_wyoming_stt_port || "10300"
                  )}" />
                </label>
              </div>

              <div class="hydra-model-panel is-active">
                <div class="hydra-model-panel-title">TTS</div>
                <div class="small hydra-model-panel-note">
                  Shared globally across Tater, cores, and verbas.
                </div>
                <div class="small core-inline-section-title" style="grid-column: 1 / -1;">Direct TTS</div>
                <label>TTS Backend
                  <select id="set_speech_tts_backend">
                    ${renderSettingsSelectOptions(speechTtsBackendOptions, currentSpeechTtsBackend)}
                  </select>
                </label>
                <label id="speech-tts-model-wrap">TTS Model
                  <select id="set_speech_tts_model">
                    ${renderSettingsSelectOptions(initialSpeechTtsModelOptions, settings.speech_tts_model || "")}
                  </select>
                </label>
                <label id="speech-tts-voice-wrap">TTS Voice
                  <select id="set_speech_tts_voice">
                    ${renderSettingsSelectOptions(initialSpeechTtsVoiceOptions, settings.speech_tts_voice || "")}
                  </select>
                </label>
                <label id="speech-wyoming-tts-host-wrap">Wyoming TTS Host
                  <input id="set_speech_wyoming_tts_host" type="text" value="${escapeHtml(
                    settings.speech_wyoming_tts_host || "127.0.0.1"
                  )}" />
                </label>
                <label id="speech-wyoming-tts-port-wrap">Wyoming TTS Port
                  <input id="set_speech_wyoming_tts_port" type="number" min="1" max="65535" value="${escapeHtml(
                    settings.speech_wyoming_tts_port || "10200"
                  )}" />
                </label>
                <label id="speech-wyoming-tts-voice-wrap" style="grid-column: 1 / -1;">Wyoming TTS Voice (optional)
                  <select id="set_speech_wyoming_tts_voice">
                    <option value="">Default</option>
                    ${
                      settings.speech_wyoming_tts_voice
                        ? `<option value="${escapeHtml(settings.speech_wyoming_tts_voice)}" selected>${escapeHtml(
                            `${settings.speech_wyoming_tts_voice} (saved)`
                          )}</option>`
                        : ""
                    }
                  </select>
                  <div id="speech-wyoming-tts-voice-status" class="small"></div>
                </label>

                <div class="small core-inline-section-title" style="grid-column: 1 / -1; margin-top: 8px;">Announcement TTS</div>
                <div class="small hydra-model-panel-note" style="grid-column: 1 / -1;">
                  Default for Voice Core announcement flows used by cores and verbas.
                </div>
                <label>Announcement Backend
                  <select id="set_speech_announcement_tts_backend">
                    ${renderSettingsSelectOptions(announcementTtsBackendOptions, currentAnnouncementTtsBackend)}
                  </select>
                </label>
                <label id="speech-announcement-tts-model-wrap">Announcement Model
                  <select id="set_speech_announcement_tts_model">
                    ${renderSettingsSelectOptions(initialAnnouncementTtsModelOptions, settings.speech_announcement_tts_model || "")}
                  </select>
                </label>
                <label id="speech-announcement-tts-voice-wrap">Announcement Voice
                  <select id="set_speech_announcement_tts_voice">
                    ${renderSettingsSelectOptions(initialAnnouncementTtsVoiceOptions, settings.speech_announcement_tts_voice || "")}
                  </select>
                </label>
                <label style="grid-column: 1 / -1;">TTS Sample Text
                  <textarea id="set_speech_tts_sample_text" rows="3">Hello from Tater. This is a voice preview.</textarea>
                </label>
                <div class="inline-row" style="grid-column: 1 / -1;">
                  <button type="button" id="settings-speech-tts-preview" class="inline-btn">Test Voice</button>
                  <button type="button" id="settings-speech-tts-download" class="inline-btn">Download Sample</button>
                  <span id="settings-speech-tts-preview-status" class="small"></span>
                </div>
              </div>
              <div id="settings-hydra-beast-fields" class="hydra-model-panel ${
                settings.hydra_beast_mode_enabled ? "is-active" : ""
              }">
                <div class="hydra-model-panel-title">Beast Head Models</div>
                <div class="small hydra-model-panel-note">Used only in Beast Mode. AI Calls still uses Base model keys.</div>

                <div class="hydra-role-title">Chat (normal conversation replies)</div>
                <label>Chat Host / IP
                  <input id="set_hydra_llm_chat_host" type="text" value="${escapeHtml(
                    settings.hydra_llm_chat_host || ""
                  )}" />
                </label>
                <label>Chat Port
                  <input id="set_hydra_llm_chat_port" type="number" min="1" max="65535" value="${escapeHtml(
                    settings.hydra_llm_chat_port || ""
                  )}" />
                </label>
                <label style="grid-column: 1 / -1;">Chat Model
                  <input id="set_hydra_llm_chat_model" type="text" value="${escapeHtml(
                    settings.hydra_llm_chat_model || ""
                  )}" />
                </label>

                <div class="hydra-role-title">Astraeus (planning)</div>
                <label>Astraeus Host / IP
                  <input id="set_hydra_llm_astraeus_host" type="text" value="${escapeHtml(
                    settings.hydra_llm_astraeus_host || ""
                  )}" />
                </label>
                <label>Astraeus Port
                  <input id="set_hydra_llm_astraeus_port" type="number" min="1" max="65535" value="${escapeHtml(
                    settings.hydra_llm_astraeus_port || ""
                  )}" />
                </label>
                <label style="grid-column: 1 / -1;">Astraeus Model
                  <input id="set_hydra_llm_astraeus_model" type="text" value="${escapeHtml(
                    settings.hydra_llm_astraeus_model || ""
                  )}" />
                </label>

                <div class="hydra-role-title">Thanatos (execution)</div>
                <label>Thanatos Host / IP
                  <input id="set_hydra_llm_thanatos_host" type="text" value="${escapeHtml(
                    settings.hydra_llm_thanatos_host || ""
                  )}" />
                </label>
                <label>Thanatos Port
                  <input id="set_hydra_llm_thanatos_port" type="number" min="1" max="65535" value="${escapeHtml(
                    settings.hydra_llm_thanatos_port || ""
                  )}" />
                </label>
                <label style="grid-column: 1 / -1;">Thanatos Model
                  <input id="set_hydra_llm_thanatos_model" type="text" value="${escapeHtml(
                    settings.hydra_llm_thanatos_model || ""
                  )}" />
                </label>

                <div class="hydra-role-title">Minos (judging)</div>
                <label>Minos Host / IP
                  <input id="set_hydra_llm_minos_host" type="text" value="${escapeHtml(
                    settings.hydra_llm_minos_host || ""
                  )}" />
                </label>
                <label>Minos Port
                  <input id="set_hydra_llm_minos_port" type="number" min="1" max="65535" value="${escapeHtml(
                    settings.hydra_llm_minos_port || ""
                  )}" />
                </label>
                <label style="grid-column: 1 / -1;">Minos Model
                  <input id="set_hydra_llm_minos_model" type="text" value="${escapeHtml(
                    settings.hydra_llm_minos_model || ""
                  )}" />
                </label>

                <div class="hydra-role-title">Hermes (final response)</div>
                <label>Hermes Host / IP
                  <input id="set_hydra_llm_hermes_host" type="text" value="${escapeHtml(
                    settings.hydra_llm_hermes_host || ""
                  )}" />
                </label>
                <label>Hermes Port
                  <input id="set_hydra_llm_hermes_port" type="number" min="1" max="65535" value="${escapeHtml(
                    settings.hydra_llm_hermes_port || ""
                  )}" />
                </label>
                <label style="grid-column: 1 / -1;">Hermes Model
                  <input id="set_hydra_llm_hermes_model" type="text" value="${escapeHtml(
                    settings.hydra_llm_hermes_model || ""
                  )}" />
                </label>
              </div>
            </div>
            <div class="inline-row" style="grid-column: 1 / -1;">
              <button type="button" id="settings-hydra-model-save" class="action-btn">Save Models</button>
              <span class="small">Saves shared LLM, vision, STT, and TTS routing settings.</span>
            </div>
          </div>
        </section>

        <section class="settings-tab-panel" data-settings-panel="integrations">
          <div class="form-grid">
            <section class="core-inline-section">
              <div class="small core-inline-section-title">Web Search</div>
              <div class="form-grid two-col">
                <label>Google API Key
                  <input id="set_web_search_google_api_key" type="password" value="${escapeHtml(settings.web_search_google_api_key || "")}" />
                </label>
                <label>Google Search CX
                  <input id="set_web_search_google_cx" type="text" value="${escapeHtml(settings.web_search_google_cx || "")}" />
                </label>
              </div>
            </section>

            <section class="core-inline-section">
              <div class="small core-inline-section-title">Home Assistant</div>
              <div class="form-grid two-col">
                <label>Base URL
                  <input id="set_homeassistant_base_url" type="text" value="${escapeHtml(settings.homeassistant_base_url || "http://homeassistant.local:8123")}" />
                </label>
                <label>Long-Lived Access Token
                  <input id="set_homeassistant_token" type="password" value="${escapeHtml(settings.homeassistant_token || "")}" />
                </label>
              </div>
            </section>

            <section class="core-inline-section">
              <div class="small core-inline-section-title">UniFi Network</div>
              <div class="form-grid two-col">
                <label>Console Base URL
                  <input id="set_unifi_network_base_url" type="text" value="${escapeHtml(settings.unifi_network_base_url || "https://10.4.20.1")}" />
                </label>
                <label>API Key
                  <input id="set_unifi_network_api_key" type="password" value="${escapeHtml(settings.unifi_network_api_key || "")}" />
                </label>
              </div>
            </section>

            <section class="core-inline-section">
              <div class="small core-inline-section-title">UniFi Protect</div>
              <div class="form-grid two-col">
                <label>Console Base URL
                  <input id="set_unifi_protect_base_url" type="text" value="${escapeHtml(settings.unifi_protect_base_url || "https://10.4.20.127")}" />
                </label>
                <label>API Key
                  <input id="set_unifi_protect_api_key" type="password" value="${escapeHtml(settings.unifi_protect_api_key || "")}" />
                </label>
              </div>
            </section>

            <div class="inline-row" style="grid-column: 1 / -1;">
              <button type="button" id="settings-save-integrations" class="action-btn">Save Settings</button>
              <span class="small">Saves Integrations and non-model settings.</span>
            </div>
          </div>
        </section>

        <section class="settings-tab-panel" data-settings-panel="esphome">
          <div
            id="settings-esphome-shell"
            data-core-action-endpoint="/api/settings/esphome/runtime/action"
            data-core-refresh-scope="esphome-runtime"
            data-core-key="esphome"
          >
            <div class="settings-subtabs">
              <button type="button" class="settings-subtab-btn active" data-esphome-tab="satellites">Satellites</button>
              <button type="button" class="settings-subtab-btn" data-esphome-tab="firmware">Firmware</button>
              <button type="button" class="settings-subtab-btn" data-esphome-tab="speakerid">Speaker ID</button>
              <button type="button" class="settings-subtab-btn" data-esphome-tab="stats">Stats</button>
              <button type="button" class="settings-subtab-btn" data-esphome-tab="platform">Settings</button>
            </div>

            <div id="settings-esphome-runtime-head">
              ${renderNotice("Open the ESPHome tab to load the built-in ESPHome runtime.")}
            </div>

            <div class="settings-subpanel active" data-esphome-panel="satellites">
              <div class="small" style="margin:10px 0 14px;">
                Live ESPHome satellites connected directly to Tater, including room context, live entities, and device logs.
              </div>
              <div id="settings-esphome-runtime-satellites">
                ${renderNotice("Open the ESPHome tab to load satellites.")}
              </div>
              <div style="margin-top:16px;">
                <div class="settings-section-title">Add Satellite</div>
                <div class="small" style="margin-bottom:10px;">
                  Manually add a satellite by host or IP when discovery has not found it yet.
                </div>
                <div id="settings-esphome-runtime-add">
                  ${renderNotice("Open the ESPHome tab to load the add form.")}
                </div>
              </div>
            </div>

            <div class="settings-subpanel" data-esphome-panel="platform">
              <div id="settings-esphome-form">
                <section class="core-inline-section">
                  <div class="small core-inline-section-title">ESPHome Platform</div>
                  <div class="small">
                    ${escapeHtml(
                      String(esphomeUi.description || "Built-in ESPHome services for Tater.")
                    )}
                  </div>
                  <div class="small" style="margin-top:8px;">
                    ${escapeHtml(
                      String(esphomeUi.runtime_tab_hint || "Live voice satellites, entities, and logs are managed directly in this ESPHome settings area.")
                    )}
                  </div>
                </section>

                <section class="core-inline-section">
                  <div class="small core-inline-section-title">Runtime</div>
                  <div class="small">
                    Built-in service status: ${escapeHtml(esphomeRunning ? "running" : "starting")} • always-on with Tater
                  </div>
                </section>

                ${
                  esphomeExperimentalFieldsHtml
                    ? `<section class="core-inline-section">
                        <div class="small core-inline-section-title">Experimental</div>
                        <div class="form-grid two-col">
                          ${esphomeExperimentalFieldsHtml}
                        </div>
                      </section>`
                    : ""
                }

                <section class="core-inline-section">
                  <div class="small core-inline-section-title">Voice Pipeline Settings</div>
                  <div class="form-grid two-col">
                    ${esphomePipelineFieldsHtml}
                  </div>
                </section>

                <div class="inline-row" style="margin-top:12px;">
                  <button type="button" id="settings-save-esphome" class="action-btn">Save Settings</button>
                  <span class="small">Saves built-in ESPHome device settings for Tater.</span>
                </div>
              </div>
            </div>

            <div class="settings-subpanel" data-esphome-panel="firmware">
              <div class="small" style="margin:10px 0 14px;">
                Build or flash Tater firmware for connected VoicePE and Satellite1 devices after reviewing the template substitutions.
              </div>
              <div id="settings-esphome-runtime-firmware">
                ${renderNotice("Open the ESPHome tab to load the firmware builder.")}
              </div>
            </div>

            <div class="settings-subpanel" data-esphome-panel="speakerid">
              <div class="small" style="margin:10px 0 14px;">
                Enroll voiceprints for people who use your satellites, then let Tater tag the speaker before Hydra runs.
              </div>
              <div id="settings-esphome-runtime-speakerid">
                ${renderNotice("Open the ESPHome tab to load Speaker ID.")}
              </div>
            </div>

            <div class="settings-subpanel" data-esphome-panel="stats">
              <div class="small" style="margin:10px 0 14px;">
                Voice pipeline quality, latency, fallback, and discovery metrics for tuning ESPHome devices in Tater.
              </div>
              <div id="settings-esphome-runtime-stats">
                ${renderNotice("Open the ESPHome tab to load stats.")}
              </div>
            </div>

          </div>
        </section>

        ${renderSettingsRedisSectionHtml(redisStatus, redisEncryptionStatus)}

        <section class="settings-tab-panel" data-settings-panel="misc">
          <div class="form-grid">
            <section class="core-inline-section">
              <div class="small core-inline-section-title">Compotato Popup Effects</div>
              <div class="form-grid two-col">
                <label>Popup Animation Style
                  <select id="set_popup_effect_style">
                    <option value="disabled" ${popupEffectStyle === "disabled" ? "selected" : ""}>Disabled</option>
                    <option value="flame" ${popupEffectStyle === "flame" ? "selected" : ""}>Flame</option>
                    <option value="dust" ${popupEffectStyle === "dust" ? "selected" : ""}>Crumble To Dust</option>
                    <option value="glitch" ${popupEffectStyle === "glitch" ? "selected" : ""}>Glitch Out</option>
                    <option value="portal" ${popupEffectStyle === "portal" ? "selected" : ""}>Portal Swirl Shut</option>
                    <option value="melt" ${popupEffectStyle === "melt" ? "selected" : ""}>Melt Downward</option>
                  </select>
                </label>
                <div class="small" style="grid-column: 1 / -1;">
                  Applies to modal popups and toast popups when they appear and close.
                </div>
              </div>
            </section>

            <section class="core-inline-section">
              <div class="small core-inline-section-title">Emoji</div>
              <div class="form-grid two-col">
                <label>Enable reaction-chain mode (Discord)
                  ${renderToggleRow(
                    `<input id="set_emoji_enable_on_reaction_add" class="toggle-input" type="checkbox" ${
                      settings.emoji_enable_on_reaction_add ? "checked" : ""
                    } />`
                  )}
                </label>
                <label>Enable auto reactions on replies
                  ${renderToggleRow(
                    `<input id="set_emoji_enable_auto_reaction_on_reply" class="toggle-input" type="checkbox" ${
                      settings.emoji_enable_auto_reaction_on_reply ? "checked" : ""
                    } />`
                  )}
                </label>
                <label>Reaction-chain chance (%)
                  <input id="set_emoji_reaction_chain_chance_percent" type="number" min="0" max="100" value="${escapeHtml(
                    settings.emoji_reaction_chain_chance_percent ?? 100
                  )}" />
                </label>
                <label>Reply reaction chance (%)
                  <input id="set_emoji_reply_reaction_chance_percent" type="number" min="0" max="100" value="${escapeHtml(
                    settings.emoji_reply_reaction_chance_percent ?? 12
                  )}" />
                </label>
                <label>Reaction-chain cooldown (seconds)
                  <input id="set_emoji_reaction_chain_cooldown_seconds" type="number" min="0" max="86400" value="${escapeHtml(
                    settings.emoji_reaction_chain_cooldown_seconds ?? 30
                  )}" />
                </label>
                <label>Reply reaction cooldown (seconds)
                  <input id="set_emoji_reply_reaction_cooldown_seconds" type="number" min="0" max="86400" value="${escapeHtml(
                    settings.emoji_reply_reaction_cooldown_seconds ?? 120
                  )}" />
                </label>
                <label>Minimum message length
                  <input id="set_emoji_min_message_length" type="number" min="0" max="200" value="${escapeHtml(
                    settings.emoji_min_message_length ?? 4
                  )}" />
                </label>
              </div>
            </section>

            <div class="inline-row" style="grid-column: 1 / -1;">
              <button type="button" id="settings-save-misc" class="action-btn">Save Settings</button>
              <span class="small">Saves Misc and non-model settings.</span>
            </div>
          </div>
        </section>

        <section class="settings-tab-panel" data-settings-panel="hydra">
          <div class="settings-subtabs">
            <button type="button" class="settings-subtab-btn active" data-hydra-tab="settings">Hydra</button>
            <button type="button" class="settings-subtab-btn" data-hydra-tab="metrics">Hydra Metrics</button>
            <button type="button" class="settings-subtab-btn" data-hydra-tab="data">Hydra Data</button>
          </div>

          <div class="settings-subpanel active" data-hydra-panel="settings">
            <div class="form-grid two-col">
              <div class="settings-section-title">Hydra General</div>
              <label>Messages Shown in WebUI
                <input id="set_max_display" type="number" min="1" value="${escapeHtml(settings.max_display || 8)}" />
              </label>
              <label>Max Stored Messages <span class="small">(0 = infinite)</span>
                <input id="set_max_store" type="number" min="0" value="${escapeHtml(settings.max_store || 20)}" />
              </label>
              <label>Messages Sent To LLM
                <input id="set_max_llm" type="number" min="1" value="${escapeHtml(settings.max_llm || 8)}" />
              </label>
              <label>Max Ledger Items
                <input id="set_hydra_max_ledger_items" type="number" min="1" value="${escapeHtml(
                  settings.hydra_max_ledger_items ?? 1500
                )}" />
              </label>
              <label>Retry Depth (Step Retry Limit)
                <input id="set_hydra_step_retry_limit" type="number" min="1" max="10" value="${escapeHtml(
                  settings.hydra_step_retry_limit ?? 1
                )}" />
                <div class="small">Default: 1. Max retry attempts per plan step before Hydra stops and asks/fails.</div>
              </label>
              <label>Astraeus Second Plan Check
                ${renderToggleRow(
                  `<input id="set_hydra_astraeus_plan_review_enabled" class="toggle-input" type="checkbox" ${
                    settings.hydra_astraeus_plan_review_enabled ? "checked" : ""
                  } />`
                )}
                <div class="small">May improve planning quality, but slower.</div>
              </label>
              <div class="inline-row" style="grid-column: 1 / -1;">
                <button type="button" id="settings-hydra-defaults" class="inline-btn">Set Default Values</button>
                <span class="small">Applies default Hydra values to the fields above.</span>
              </div>
              <div class="inline-row" style="grid-column: 1 / -1;">
                <button type="button" id="settings-save" class="action-btn">Save Settings</button>
                <span class="small">Saves Hydra-only behavior settings. Use the Models tab for model routing settings.</span>
              </div>
            </div>
          </div>

          <div class="settings-subpanel" data-hydra-panel="metrics">
            <div class="form-grid two-col">
              <label>Portal
                <select id="set_cerb_metrics_platform">
                  <option value="all">All</option>
                  ${hydraPlatformOptionsHtml}
                </select>
              </label>
              <label>Ledger entries
                <input id="set_cerb_metrics_limit" type="number" min="10" max="300" step="10" value="50" />
              </label>
              <label>Outcome Filter
                <select id="set_cerb_metrics_outcome">
                  <option value="all">all</option>
                  <option value="done">done</option>
                  <option value="blocked">blocked</option>
                  <option value="failed">failed</option>
                </select>
              </label>
              <label>Tool Filter
                <select id="set_cerb_metrics_tool">
                  <option value="all">all</option>
                </select>
              </label>
              <label>Show only tool turns
                ${renderToggleRow('<input id="set_cerb_metrics_tools_only" class="toggle-input" type="checkbox" />')}
              </label>
              <div class="inline-row" style="grid-column: 1 / -1;">
                <button type="button" id="settings-cerb-metrics-refresh" class="inline-btn">Refresh Metrics</button>
                <span id="settings-cerb-metrics-status" class="small"></span>
              </div>
            </div>
            <div id="settings-cerb-metrics-content" class="form-grid"></div>
          </div>

          <div class="settings-subpanel" data-hydra-panel="data">
            <div class="form-grid two-col">
              <div class="settings-section-title">All Portals</div>
              <div class="inline-row" style="grid-column: 1 / -1;">
                <button type="button" id="settings-cerb-clear-all" class="inline-btn danger">Clear All Hydra Data</button>
              </div>

              <div class="settings-section-title">Per-Portal Data</div>
              <label>Portal
                <select id="set_cerb_data_platform">
                  ${hydraPlatformOptionsHtml}
                </select>
              </label>
              <div></div>
              <div class="inline-row" style="grid-column: 1 / -1;">
                <button type="button" id="settings-cerb-clear-platform-all" class="inline-btn danger">Clear Portal Data</button>
                <button type="button" id="settings-cerb-clear-platform-metrics" class="inline-btn danger">Reset Metrics Only</button>
                <button type="button" id="settings-cerb-clear-platform-ledger" class="inline-btn danger">Clear Ledger Only</button>
              </div>
            </div>
            <div id="settings-cerb-data-status" class="small" style="margin-top: 8px;"></div>
            <div id="settings-cerb-data-content" class="form-grid"></div>
          </div>
        </section>

        <section class="settings-tab-panel" data-settings-panel="advanced">
          <div class="form-grid">
            <section class="core-inline-section">
              <div class="small core-inline-section-title">Admin Tool Gating</div>
              <div class="form-grid">
                <label>
                  Admin-only plugin ids
                  <select id="set_admin_only_plugins" class="settings-multiselect" multiple size="14">
                    ${adminOptionHtml}
                  </select>
                  <div class="small">Selected plugins can only run for portal admin users.</div>
                </label>
                <div class="inline-row">
                  <button type="button" id="settings-admin-defaults" class="inline-btn">Reset To Defaults</button>
                  <span class="small">Loads the default admin-only plugin list.</span>
                </div>
              </div>
            </section>

            <div class="inline-row">
              <button type="button" id="settings-save-advanced" class="action-btn">Save Settings</button>
              <span class="small">Saves Advanced and non-model settings.</span>
            </div>

            <section class="core-inline-section tone-danger">
              <div class="small core-inline-section-title">Clear Chat History</div>
              <div class="inline-row">
                <button type="button" id="settings-clear-chat" class="inline-btn danger">Clear Chat History</button>
                <span class="small">Deletes all messages in the WebUI chat history.</span>
              </div>
            </section>
          </div>
        </section>

      </form>
    </div>
  `;

  const statusEl = document.getElementById("settings-status");
  const webuiPasswordEl = document.getElementById("set_webui_password");
  const webuiPasswordConfirmEl = document.getElementById("set_webui_password_confirm");
  const webuiPasswordClearBtnEl = document.getElementById("settings-webui-password-clear");
  const webuiPasswordStatusEl = document.getElementById("settings-webui-password-status");
  let clearWebuiPasswordRequested = false;

  const refreshWebuiPasswordUi = () => {
    const hasPasswordInput = Boolean(String(webuiPasswordEl?.value || "").trim());
    const hasConfirmInput = Boolean(String(webuiPasswordConfirmEl?.value || "").trim());
    if (webuiPasswordClearBtnEl) {
      webuiPasswordClearBtnEl.disabled = !webuiPasswordIsSet || hasPasswordInput || hasConfirmInput;
    }
    if (!webuiPasswordStatusEl) {
      return;
    }
    if (clearWebuiPasswordRequested) {
      webuiPasswordStatusEl.textContent = "WebUI password will be removed on save. Login will not be required.";
      return;
    }
    if (hasPasswordInput || hasConfirmInput) {
      webuiPasswordStatusEl.textContent = "New WebUI password will be saved when you click Save Settings.";
      return;
    }
    webuiPasswordStatusEl.textContent = webuiPasswordIsSet
      ? "WebUI password is enabled. Login is required."
      : "No WebUI password set. Login is not required.";
  };

  const onWebuiPasswordInput = () => {
    if (String(webuiPasswordEl?.value || "").trim() || String(webuiPasswordConfirmEl?.value || "").trim()) {
      clearWebuiPasswordRequested = false;
    }
    refreshWebuiPasswordUi();
  };

  webuiPasswordEl?.addEventListener("input", onWebuiPasswordInput);
  webuiPasswordConfirmEl?.addEventListener("input", onWebuiPasswordInput);
  webuiPasswordClearBtnEl?.addEventListener("click", () => {
    clearWebuiPasswordRequested = true;
    if (webuiPasswordEl) {
      webuiPasswordEl.value = "";
    }
    if (webuiPasswordConfirmEl) {
      webuiPasswordConfirmEl.value = "";
    }
    refreshWebuiPasswordUi();
    statusEl.textContent = "WebUI password removal queued. Click Save Settings to apply.";
  });
  refreshWebuiPasswordUi();

  const tabButtons = Array.from(root.querySelectorAll(".settings-tab-btn"));
  const tabPanels = Array.from(root.querySelectorAll(".settings-tab-panel"));
  const initialSettingsTab = !redisStatus.connected ? "redis" : normalizeSettingsTab(state.settingsTab || "general");

  const activateTab = (tabKey) => {
    const normalizedTab = normalizeSettingsTab(tabKey);
    tabButtons.forEach((button) => {
      button.classList.toggle("active", button.dataset.settingsTab === normalizedTab);
    });
    tabPanels.forEach((panel) => {
      panel.classList.toggle("active", panel.dataset.settingsPanel === normalizedTab);
    });
    setPreferredSettingsTab(normalizedTab);
    if (normalizedTab === "esphome") {
      void ensureEspHomeRuntimeLoaded({ force: true, panel: getActiveEspHomeRuntimePanel() });
    }
  };

  tabButtons.forEach((button) => {
    button.addEventListener("click", () => activateTab(button.dataset.settingsTab));
  });
  activateTab(initialSettingsTab);

  bindSettingsRedisSection({
    statusTargetEl: statusEl,
    initialRedisStatus: redisStatus,
    initialRedisEncryptionStatus: redisEncryptionStatus,
  });

  let clearUserAvatarRequested = false;
  let clearTaterAvatarRequested = false;

  const userAvatarFileEl = document.getElementById("set_user_avatar_file");
  const taterAvatarFileEl = document.getElementById("set_tater_avatar_file");
  const usernameEl = document.getElementById("set_username");
  const taterFirstNameEl = document.getElementById("set_tater_first_name");

  const setAvatarPreview = (kind, src) => {
    const previewId = kind === "user" ? "set_user_avatar_preview" : "set_tater_avatar_preview";
    const current = document.getElementById(previewId);
    if (!current) {
      return;
    }

    const fallbackLabel =
      kind === "user" ? String(usernameEl?.value || "User") : String(taterFirstNameEl?.value || "Tater");
    let nextNode;
    if (src) {
      nextNode = document.createElement("img");
      nextNode.id = previewId;
      nextNode.className = "settings-avatar-preview";
      nextNode.alt = kind === "user" ? "User avatar preview" : "Tater avatar preview";
      nextNode.src = String(src);
    } else {
      nextNode = document.createElement("div");
      nextNode.id = previewId;
      nextNode.className = "settings-avatar-preview fallback";
      nextNode.textContent = _avatarInitial(fallbackLabel, kind === "user" ? "U" : "T");
    }
    current.replaceWith(nextNode);
  };

  if (userAvatarFileEl) {
    userAvatarFileEl.addEventListener("change", async () => {
      const file = userAvatarFileEl.files && userAvatarFileEl.files[0];
      if (!file) {
        return;
      }
      try {
        const dataUrl = await readFileAsDataUrl(file);
        clearUserAvatarRequested = false;
        setAvatarPreview("user", dataUrl);
      } catch (error) {
        statusEl.textContent = `User avatar preview failed: ${error.message}`;
      }
    });
  }

  if (taterAvatarFileEl) {
    taterAvatarFileEl.addEventListener("change", async () => {
      const file = taterAvatarFileEl.files && taterAvatarFileEl.files[0];
      if (!file) {
        return;
      }
      try {
        const dataUrl = await readFileAsDataUrl(file);
        clearTaterAvatarRequested = false;
        setAvatarPreview("tater", dataUrl);
      } catch (error) {
        statusEl.textContent = `Tater avatar preview failed: ${error.message}`;
      }
    });
  }

  document.getElementById("set_user_avatar_clear").addEventListener("click", () => {
    clearUserAvatarRequested = true;
    if (userAvatarFileEl) {
      userAvatarFileEl.value = "";
    }
    setAvatarPreview("user", "");
    statusEl.textContent = "User avatar will be cleared on save.";
  });

  document.getElementById("set_tater_avatar_clear").addEventListener("click", () => {
    clearTaterAvatarRequested = true;
    if (taterAvatarFileEl) {
      taterAvatarFileEl.value = "";
    }
    setAvatarPreview("tater", "");
    statusEl.textContent = "Tater avatar will be cleared on save.";
  });

  document.getElementById("settings-clear-chat").addEventListener("click", async () => {
    if (!window.confirm("Clear chat history now?")) {
      return;
    }
    statusEl.textContent = "Clearing chat history...";
    try {
      await api("/api/chat/clear", { method: "POST" });
      statusEl.textContent = "Chat history cleared.";
    } catch (error) {
      statusEl.textContent = `Clear failed: ${error.message}`;
    }
  });

  document.getElementById("settings-hydra-defaults").addEventListener("click", () => {
    const map = [
      ["set_hydra_max_ledger_items", "hydra_max_ledger_items"],
      ["set_hydra_step_retry_limit", "hydra_step_retry_limit"],
      ["set_hydra_astraeus_plan_review_enabled", "hydra_astraeus_plan_review_enabled"],
    ];
    map.forEach(([inputId, primaryKey]) => {
      const input = document.getElementById(inputId);
      if (!input) {
        return;
      }
      const hasPrimary = Object.prototype.hasOwnProperty.call(hydraDefaults, primaryKey);
      if (!hasPrimary) {
        return;
      }
      const rawValue = hydraDefaults[primaryKey];
      if (input.type === "checkbox") {
        input.checked = boolFromAny(rawValue, false);
      } else {
        input.value = String(rawValue);
      }
    });
    statusEl.textContent = "Hydra defaults loaded into form (model settings unchanged). Click Save Settings to apply.";
  });

  const hydraBeastToggleEl = document.getElementById("set_hydra_beast_mode_enabled");
  const hydraBaseFieldsEl = document.getElementById("settings-hydra-base-fields");
  const hydraBeastFieldsEl = document.getElementById("settings-hydra-beast-fields");
  const applyHydraBeastVisibility = () => {
    if (!hydraBaseFieldsEl || !hydraBeastFieldsEl || !hydraBeastToggleEl) {
      return;
    }
    const beastEnabled = Boolean(hydraBeastToggleEl.checked);
    hydraBaseFieldsEl.classList.add("is-active");
    hydraBeastFieldsEl.classList.toggle("is-active", beastEnabled);
  };
  if (hydraBeastToggleEl) {
    hydraBeastToggleEl.addEventListener("change", applyHydraBeastVisibility);
    applyHydraBeastVisibility();
  }

  const hydraBaseServersEl = document.getElementById("settings-hydra-base-servers");
  const hydraBaseServerAddEl = document.getElementById("settings-hydra-base-server-add");
  const normalizeHydraBaseRowInput = (row) => ({
    host: String(row?.host || "").trim(),
    port: String(row?.port || "").trim(),
    model: String(row?.model || "").trim(),
  });
  const readHydraAdditionalBaseRows = () => {
    if (!hydraBaseServersEl) {
      return [];
    }
    return Array.from(hydraBaseServersEl.querySelectorAll(".hydra-base-server-row")).map((rowEl) => {
      const hostEl = rowEl.querySelector('[data-hydra-base-field="host"]');
      const portEl = rowEl.querySelector('[data-hydra-base-field="port"]');
      const modelEl = rowEl.querySelector('[data-hydra-base-field="model"]');
      return normalizeHydraBaseRowInput({
        host: hostEl ? hostEl.value : "",
        port: portEl ? portEl.value : "",
        model: modelEl ? modelEl.value : "",
      });
    });
  };
  const renderHydraAdditionalBaseRows = (rows) => {
    if (!hydraBaseServersEl) {
      return;
    }
    const safeRows = Array.isArray(rows) ? rows.map((row) => normalizeHydraBaseRowInput(row)) : [];
    if (!safeRows.length) {
      hydraBaseServersEl.innerHTML = `<div class="small hydra-base-server-empty">No additional base servers configured.</div>`;
      return;
    }
    hydraBaseServersEl.innerHTML = safeRows
      .map(
        (row, index) => `
          <div class="hydra-base-server-row" data-hydra-base-index="${index}">
            <label>Host / IP
              <input type="text" data-hydra-base-field="host" value="${escapeHtml(row.host)}" />
            </label>
            <label>Port
              <input type="number" min="1" max="65535" data-hydra-base-field="port" value="${escapeHtml(row.port)}" />
            </label>
            <label>Model
              <input type="text" data-hydra-base-field="model" value="${escapeHtml(row.model)}" />
            </label>
            <div class="hydra-base-server-actions">
              <button type="button" class="inline-btn danger" data-hydra-base-remove="${index}">Remove</button>
            </div>
          </div>
        `
      )
      .join("");
  };
  let hydraAdditionalBaseRowsState = Array.isArray(hydraAdditionalBaseRows)
    ? hydraAdditionalBaseRows.map((row) => normalizeHydraBaseRowInput(row))
    : [];
  renderHydraAdditionalBaseRows(hydraAdditionalBaseRowsState);
  hydraBaseServerAddEl?.addEventListener("click", () => {
    hydraAdditionalBaseRowsState = readHydraAdditionalBaseRows();
    hydraAdditionalBaseRowsState.push(normalizeHydraBaseRowInput({ host: "", port: "", model: "" }));
    renderHydraAdditionalBaseRows(hydraAdditionalBaseRowsState);
  });
  hydraBaseServersEl?.addEventListener("click", (event) => {
    const button = event.target instanceof Element ? event.target.closest("[data-hydra-base-remove]") : null;
    if (!button) {
      return;
    }
    const removeIndex = Number(button.getAttribute("data-hydra-base-remove"));
    if (!Number.isFinite(removeIndex) || removeIndex < 0) {
      return;
    }
    hydraAdditionalBaseRowsState = readHydraAdditionalBaseRows();
    hydraAdditionalBaseRowsState.splice(removeIndex, 1);
    renderHydraAdditionalBaseRows(hydraAdditionalBaseRowsState);
  });

  const speechSttBackendEl = document.getElementById("set_speech_stt_backend");
  const speechWyomingSttHostWrapEl = document.getElementById("speech-wyoming-stt-host-wrap");
  const speechWyomingSttPortWrapEl = document.getElementById("speech-wyoming-stt-port-wrap");
  const speechTtsBackendEl = document.getElementById("set_speech_tts_backend");
  const speechTtsModelWrapEl = document.getElementById("speech-tts-model-wrap");
  const speechTtsModelEl = document.getElementById("set_speech_tts_model");
  const speechTtsVoiceWrapEl = document.getElementById("speech-tts-voice-wrap");
  const speechTtsVoiceEl = document.getElementById("set_speech_tts_voice");
  const announcementTtsBackendEl = document.getElementById("set_speech_announcement_tts_backend");
  const announcementTtsModelWrapEl = document.getElementById("speech-announcement-tts-model-wrap");
  const announcementTtsModelEl = document.getElementById("set_speech_announcement_tts_model");
  const announcementTtsVoiceWrapEl = document.getElementById("speech-announcement-tts-voice-wrap");
  const announcementTtsVoiceEl = document.getElementById("set_speech_announcement_tts_voice");
  const speechTtsSampleTextEl = document.getElementById("set_speech_tts_sample_text");
  const speechTtsPreviewBtnEl = document.getElementById("settings-speech-tts-preview");
  const speechTtsDownloadBtnEl = document.getElementById("settings-speech-tts-download");
  const speechTtsPreviewStatusEl = document.getElementById("settings-speech-tts-preview-status");
  const speechWyomingTtsHostWrapEl = document.getElementById("speech-wyoming-tts-host-wrap");
  const speechWyomingTtsPortWrapEl = document.getElementById("speech-wyoming-tts-port-wrap");
  const speechWyomingTtsVoiceWrapEl = document.getElementById("speech-wyoming-tts-voice-wrap");
  const speechWyomingTtsVoiceEl = document.getElementById("set_speech_wyoming_tts_voice");
  const speechWyomingTtsVoiceStatusEl = document.getElementById("speech-wyoming-tts-voice-status");
  let speechTtsPreviewUrl = "";
  let speechTtsPreviewBlob = null;
  let speechWyomingTtsRefreshSeq = 0;
  let speechWyomingTtsRefreshTimer = 0;

  const setElementVisible = (element, visible) => {
    if (!element) {
      return;
    }
    element.style.display = visible ? "" : "none";
  };

  const syncSpeechTtsModelOptions = ({ forceReset = false } = {}) => {
    if (!speechTtsBackendEl || !speechTtsModelEl) {
      return [];
    }
    const backend = String(speechTtsBackendEl.value || "").trim();
    const modelOptions = Array.isArray(speechTtsModelOptionsByBackend[backend]) ? speechTtsModelOptionsByBackend[backend] : [];
    const currentModel = String(speechTtsModelEl.value || "").trim();
    const allowedModels = new Set(modelOptions.map((row) => String(row?.value || "").trim()).filter(Boolean));
    const nextModel = !forceReset && allowedModels.has(currentModel)
      ? currentModel
      : String(modelOptions[0]?.value || "").trim();
    speechTtsModelEl.innerHTML = renderSettingsSelectOptions(modelOptions, nextModel);
    speechTtsModelEl.value = nextModel;
    return modelOptions;
  };

  const syncSpeechTtsVoiceOptions = ({ forceReset = false } = {}) => {
    if (!speechTtsModelEl || !speechTtsVoiceEl) {
      return;
    }
    const model = String(speechTtsModelEl.value || "").trim();
    const voiceOptions = Array.isArray(speechTtsVoiceOptionsByModel[model]) ? speechTtsVoiceOptionsByModel[model] : [];
    const currentVoice = String(speechTtsVoiceEl.value || "").trim();
    const allowedVoices = new Set(voiceOptions.map((row) => String(row?.value || "").trim()).filter(Boolean));
    const nextVoice = !forceReset && allowedVoices.has(currentVoice)
      ? currentVoice
      : String(voiceOptions[0]?.value || "").trim();
    speechTtsVoiceEl.innerHTML = renderSettingsSelectOptions(voiceOptions, nextVoice);
    speechTtsVoiceEl.value = nextVoice;
  };

  const syncAnnouncementTtsModelOptions = ({ forceReset = false } = {}) => {
    if (!announcementTtsBackendEl || !announcementTtsModelEl) {
      return [];
    }
    const backend = String(announcementTtsBackendEl.value || "").trim();
    const modelOptions = Array.isArray(announcementTtsModelOptionsByBackend[backend])
      ? announcementTtsModelOptionsByBackend[backend]
      : [];
    const currentModel = String(announcementTtsModelEl.value || "").trim();
    const allowedModels = new Set(modelOptions.map((row) => String(row?.value || "").trim()).filter(Boolean));
    const nextModel = !forceReset && allowedModels.has(currentModel)
      ? currentModel
      : String(modelOptions[0]?.value || "").trim();
    announcementTtsModelEl.innerHTML = renderSettingsSelectOptions(modelOptions, nextModel);
    announcementTtsModelEl.value = nextModel;
    return modelOptions;
  };

  const syncAnnouncementTtsVoiceOptions = ({ forceReset = false } = {}) => {
    if (!announcementTtsModelEl || !announcementTtsVoiceEl) {
      return;
    }
    const model = String(announcementTtsModelEl.value || "").trim();
    const voiceOptions = Array.isArray(announcementTtsVoiceOptionsByModel[model])
      ? announcementTtsVoiceOptionsByModel[model]
      : [];
    const currentVoice = String(announcementTtsVoiceEl.value || "").trim();
    const allowedVoices = new Set(voiceOptions.map((row) => String(row?.value || "").trim()).filter(Boolean));
    const nextVoice = !forceReset && allowedVoices.has(currentVoice)
      ? currentVoice
      : String(voiceOptions[0]?.value || "").trim();
    announcementTtsVoiceEl.innerHTML = renderSettingsSelectOptions(voiceOptions, nextVoice);
    announcementTtsVoiceEl.value = nextVoice;
  };

  const applySpeechSettingsVisibility = ({ resetTtsSelection = false } = {}) => {
    const sttBackend = String(speechSttBackendEl?.value || "").trim();
    const ttsBackend = String(speechTtsBackendEl?.value || "").trim();
    const announcementTtsBackend = String(announcementTtsBackendEl?.value || "").trim();
    setElementVisible(speechWyomingSttHostWrapEl, sttBackend === "wyoming");
    setElementVisible(speechWyomingSttPortWrapEl, sttBackend === "wyoming");

    const showsLocalModel = ["kokoro", "pocket_tts", "piper"].includes(ttsBackend);
    const showsVoiceSelect = ["kokoro", "pocket_tts"].includes(ttsBackend);
    const showsWyoming = ttsBackend === "wyoming";

    syncSpeechTtsModelOptions({ forceReset: resetTtsSelection });
    syncSpeechTtsVoiceOptions({ forceReset: resetTtsSelection });

    setElementVisible(speechTtsModelWrapEl, showsLocalModel);
    setElementVisible(speechTtsVoiceWrapEl, showsVoiceSelect);
    setElementVisible(speechWyomingTtsHostWrapEl, showsWyoming);
    setElementVisible(speechWyomingTtsPortWrapEl, showsWyoming);
    setElementVisible(speechWyomingTtsVoiceWrapEl, showsWyoming);
    if (showsWyoming) {
      queueRefreshSpeechWyomingTtsVoices();
    } else if (speechWyomingTtsVoiceStatusEl) {
      speechWyomingTtsVoiceStatusEl.textContent = "";
    }

    const showsAnnouncementLocalModel = ["kokoro", "pocket_tts", "piper"].includes(announcementTtsBackend);
    const showsAnnouncementVoiceSelect = ["kokoro", "pocket_tts"].includes(announcementTtsBackend);

    syncAnnouncementTtsModelOptions({ forceReset: resetTtsSelection });
    syncAnnouncementTtsVoiceOptions({ forceReset: resetTtsSelection });

    setElementVisible(announcementTtsModelWrapEl, showsAnnouncementLocalModel);
    setElementVisible(announcementTtsVoiceWrapEl, showsAnnouncementVoiceSelect);
  };

  const setSpeechTtsDownloadEnabled = (enabled) => {
    if (!speechTtsDownloadBtnEl) {
      return;
    }
    speechTtsDownloadBtnEl.disabled = !enabled;
  };

  const renderSpeechWyomingTtsVoiceOptions = (rows, currentValue = "") => {
    if (!speechWyomingTtsVoiceEl) {
      return;
    }
    const options = [{ value: "", label: "Default" }];
    const seen = new Set([""]);
    const inputRows = Array.isArray(rows) ? rows : [];
    inputRows.forEach((row) => {
      const value = String(row?.value || "").trim();
      const label = String(row?.label || value).trim();
      if (!value || seen.has(value)) {
        return;
      }
      seen.add(value);
      options.push({ value, label });
    });
    const current = String(currentValue || "").trim();
    if (current && !seen.has(current)) {
      options.push({ value: current, label: `${current} (saved)` });
    }
    speechWyomingTtsVoiceEl.innerHTML = renderSettingsSelectOptions(options, current);
    speechWyomingTtsVoiceEl.value = current;
  };

  const refreshSpeechWyomingTtsVoices = async () => {
    if (!speechWyomingTtsVoiceEl || String(speechTtsBackendEl?.value || "").trim() !== "wyoming") {
      return;
    }
    const host = String(document.getElementById("set_speech_wyoming_tts_host")?.value || "").trim();
    const port = String(document.getElementById("set_speech_wyoming_tts_port")?.value || "").trim();
    const currentVoice = String(speechWyomingTtsVoiceEl.value || "").trim();
    if (!host || !port) {
      renderSpeechWyomingTtsVoiceOptions([], currentVoice);
      if (speechWyomingTtsVoiceStatusEl) {
        speechWyomingTtsVoiceStatusEl.textContent = "Enter Wyoming host and port to load voices.";
      }
      return;
    }

    const requestId = ++speechWyomingTtsRefreshSeq;
    if (speechWyomingTtsVoiceStatusEl) {
      speechWyomingTtsVoiceStatusEl.textContent = "Loading Wyoming voices...";
    }
    try {
      const result = await api("/api/settings/speech/wyoming-tts-voices", {
        method: "POST",
        body: JSON.stringify({
          host,
          port,
          current_voice: currentVoice,
        }),
        _timeoutMs: 15000,
      });
      if (requestId !== speechWyomingTtsRefreshSeq) {
        return;
      }
      const voices = Array.isArray(result?.voices) ? result.voices : [];
      renderSpeechWyomingTtsVoiceOptions(voices, currentVoice);
      if (speechWyomingTtsVoiceStatusEl) {
        const count = Math.max(0, Number(result?.count || 0));
        speechWyomingTtsVoiceStatusEl.textContent = count
          ? `Loaded ${count} Wyoming voice${count === 1 ? "" : "s"}.`
          : "No Wyoming voices reported by server.";
      }
    } catch (error) {
      if (requestId !== speechWyomingTtsRefreshSeq) {
        return;
      }
      renderSpeechWyomingTtsVoiceOptions([], currentVoice);
      if (speechWyomingTtsVoiceStatusEl) {
        speechWyomingTtsVoiceStatusEl.textContent = error?.message || "Failed to load Wyoming voices.";
      }
    }
  };

  const queueRefreshSpeechWyomingTtsVoices = () => {
    if (speechWyomingTtsRefreshTimer) {
      window.clearTimeout(speechWyomingTtsRefreshTimer);
    }
    speechWyomingTtsRefreshTimer = window.setTimeout(() => {
      speechWyomingTtsRefreshTimer = 0;
      refreshSpeechWyomingTtsVoices();
    }, 250);
  };

  const stopSpeechTtsPreviewPlayback = () => {
    if (speechTtsPreviewUrl) {
      try {
        URL.revokeObjectURL(speechTtsPreviewUrl);
      } catch {
        // ignore preview cleanup failures
      }
      speechTtsPreviewUrl = "";
    }
  };

  const clearSpeechTtsPreviewCache = () => {
    stopSpeechTtsPreviewPlayback();
    speechTtsPreviewBlob = null;
    setSpeechTtsDownloadEnabled(false);
  };

  const buildSpeechTtsPreviewPayload = () => ({
    backend: String(speechTtsBackendEl?.value || "").trim(),
    model: String(speechTtsModelEl?.value || "").trim(),
    voice: String(speechTtsVoiceEl?.value || "").trim(),
    wyoming_host: String(document.getElementById("set_speech_wyoming_tts_host")?.value || "").trim(),
    wyoming_port: String(document.getElementById("set_speech_wyoming_tts_port")?.value || "").trim(),
    wyoming_voice: String(speechWyomingTtsVoiceEl?.value || "").trim(),
    text: String(speechTtsSampleTextEl?.value || "").trim() || "Hello from Tater. This is a voice preview.",
  });

  const requestSpeechTtsPreviewBlob = async () => {
    const response = await fetch(withBasePath("/api/settings/speech/tts-preview"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildSpeechTtsPreviewPayload()),
    });
    if (!response.ok) {
      let detail = "TTS preview failed";
      try {
        const body = await response.json();
        detail = String(body?.detail || detail);
      } catch {
        // ignore non-JSON errors
      }
      throw new Error(detail);
    }
    return response.blob();
  };

  const sanitizeFilenamePart = (value, fallback) => {
    const token = String(value || "").trim().replace(/[^a-z0-9._-]+/gi, "-").replace(/^-+|-+$/g, "");
    return token || fallback;
  };

  const buildSpeechTtsSampleFilename = () => {
    const backend = sanitizeFilenamePart(speechTtsBackendEl?.value, "tts");
    const model = sanitizeFilenamePart(speechTtsModelEl?.value, "default-model");
    const voice = sanitizeFilenamePart(speechTtsVoiceEl?.value || speechWyomingTtsVoiceEl?.value, "default-voice");
    return `tater-tts-sample-${backend}-${model}-${voice}.wav`;
  };

  const previewSpeechTts = async () => {
    if (!speechTtsPreviewBtnEl) {
      return;
    }
    speechTtsPreviewBtnEl.disabled = true;
    setSpeechTtsDownloadEnabled(false);
    if (speechTtsPreviewStatusEl) {
      speechTtsPreviewStatusEl.textContent = "Generating sample...";
    }
    clearSpeechTtsPreviewCache();
    try {
      speechTtsPreviewBlob = await requestSpeechTtsPreviewBlob();
      speechTtsPreviewUrl = URL.createObjectURL(speechTtsPreviewBlob);
      const audio = new Audio(speechTtsPreviewUrl);
      audio.addEventListener(
        "ended",
        () => {
          stopSpeechTtsPreviewPlayback();
          if (speechTtsPreviewStatusEl) {
            speechTtsPreviewStatusEl.textContent = "Sample ready. You can download it too.";
          }
        },
        { once: true }
      );
      await audio.play();
      setSpeechTtsDownloadEnabled(true);
      if (speechTtsPreviewStatusEl) {
        speechTtsPreviewStatusEl.textContent = "Playing sample...";
      }
    } catch (error) {
      clearSpeechTtsPreviewCache();
      const message = error?.message || "TTS preview failed";
      if (speechTtsPreviewStatusEl) {
        speechTtsPreviewStatusEl.textContent = message;
      }
      showToast(message, "error", 3600);
    } finally {
      speechTtsPreviewBtnEl.disabled = false;
    }
  };

  const downloadSpeechTtsSample = async () => {
    if (!speechTtsDownloadBtnEl) {
      return;
    }
    speechTtsDownloadBtnEl.disabled = true;
    if (speechTtsPreviewStatusEl) {
      speechTtsPreviewStatusEl.textContent = speechTtsPreviewBlob ? "Preparing download..." : "Generating sample for download...";
    }
    try {
      if (!speechTtsPreviewBlob) {
        speechTtsPreviewBlob = await requestSpeechTtsPreviewBlob();
      }
      const downloadUrl = URL.createObjectURL(speechTtsPreviewBlob);
      const link = document.createElement("a");
      link.href = downloadUrl;
      link.download = buildSpeechTtsSampleFilename();
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.setTimeout(() => {
        try {
          URL.revokeObjectURL(downloadUrl);
        } catch {
          // ignore download cleanup failures
        }
      }, 1000);
      if (speechTtsPreviewStatusEl) {
        speechTtsPreviewStatusEl.textContent = "Sample downloaded.";
      }
      setSpeechTtsDownloadEnabled(true);
    } catch (error) {
      const message = error?.message || "TTS sample download failed";
      if (speechTtsPreviewStatusEl) {
        speechTtsPreviewStatusEl.textContent = message;
      }
      showToast(message, "error", 3600);
    } finally {
      speechTtsDownloadBtnEl.disabled = false;
    }
  };

  speechSttBackendEl?.addEventListener("change", applySpeechSettingsVisibility);
  speechTtsBackendEl?.addEventListener("change", () => {
    clearSpeechTtsPreviewCache();
    applySpeechSettingsVisibility({ resetTtsSelection: true });
  });
  speechTtsModelEl?.addEventListener("change", () => {
    clearSpeechTtsPreviewCache();
    syncSpeechTtsVoiceOptions({ forceReset: true });
  });
  speechTtsVoiceEl?.addEventListener("change", clearSpeechTtsPreviewCache);
  announcementTtsBackendEl?.addEventListener("change", () => {
    applySpeechSettingsVisibility({ resetTtsSelection: true });
  });
  announcementTtsModelEl?.addEventListener("change", () => {
    syncAnnouncementTtsVoiceOptions({ forceReset: true });
  });
  speechTtsSampleTextEl?.addEventListener("input", clearSpeechTtsPreviewCache);
  document.getElementById("set_speech_wyoming_tts_host")?.addEventListener("input", () => {
    clearSpeechTtsPreviewCache();
    queueRefreshSpeechWyomingTtsVoices();
  });
  document.getElementById("set_speech_wyoming_tts_port")?.addEventListener("input", () => {
    clearSpeechTtsPreviewCache();
    queueRefreshSpeechWyomingTtsVoices();
  });
  speechWyomingTtsVoiceEl?.addEventListener("change", clearSpeechTtsPreviewCache);
  speechTtsPreviewBtnEl?.addEventListener("click", previewSpeechTts);
  speechTtsDownloadBtnEl?.addEventListener("click", downloadSpeechTtsSample);
  setSpeechTtsDownloadEnabled(false);
  applySpeechSettingsVisibility();

  document.getElementById("settings-hydra-model-save").addEventListener("click", async () => {
    const baseHost = String(document.getElementById("set_hydra_llm_host")?.value || "").trim();
    const basePort = String(document.getElementById("set_hydra_llm_port")?.value || "").trim();
    const baseModel = String(document.getElementById("set_hydra_llm_model")?.value || "").trim();
    const visionApiBase = String(document.getElementById("set_vision_api_base")?.value || "").trim();
    const visionModel = String(document.getElementById("set_vision_model")?.value || "").trim();
    const visionApiKey = String(document.getElementById("set_vision_api_key")?.value || "").trim();
    const speechSttBackend = String(document.getElementById("set_speech_stt_backend")?.value || "").trim();
    const speechWyomingSttHost = String(document.getElementById("set_speech_wyoming_stt_host")?.value || "").trim();
    const speechWyomingSttPort = String(document.getElementById("set_speech_wyoming_stt_port")?.value || "").trim();
    const speechTtsBackend = String(document.getElementById("set_speech_tts_backend")?.value || "").trim();
    const speechTtsModel = String(document.getElementById("set_speech_tts_model")?.value || "").trim();
    const speechTtsVoice = String(document.getElementById("set_speech_tts_voice")?.value || "").trim();
    const speechWyomingTtsHost = String(document.getElementById("set_speech_wyoming_tts_host")?.value || "").trim();
    const speechWyomingTtsPort = String(document.getElementById("set_speech_wyoming_tts_port")?.value || "").trim();
    const speechWyomingTtsVoice = String(speechWyomingTtsVoiceEl?.value || "").trim();
    const speechAnnouncementTtsBackend = String(document.getElementById("set_speech_announcement_tts_backend")?.value || "").trim();
    const speechAnnouncementTtsModel = String(document.getElementById("set_speech_announcement_tts_model")?.value || "").trim();
    const speechAnnouncementTtsVoice = String(document.getElementById("set_speech_announcement_tts_voice")?.value || "").trim();
    const additionalBaseRows = readHydraAdditionalBaseRows();
    const hydraBaseServersPayload = [normalizeHydraBaseRowInput({ host: baseHost, port: basePort, model: baseModel })];
    additionalBaseRows.forEach((row) => hydraBaseServersPayload.push(normalizeHydraBaseRowInput(row)));
    const payload = {
      hydra_llm_host: baseHost,
      hydra_llm_port: basePort,
      hydra_llm_model: baseModel,
      hydra_base_servers: hydraBaseServersPayload,
      hydra_beast_mode_enabled: Boolean(document.getElementById("set_hydra_beast_mode_enabled")?.checked),
      vision_api_base: visionApiBase,
      vision_model: visionModel,
      vision_api_key: visionApiKey,
      speech_stt_backend: speechSttBackend,
      speech_wyoming_stt_host: speechWyomingSttHost,
      speech_wyoming_stt_port: speechWyomingSttPort,
      speech_tts_backend: speechTtsBackend,
      speech_tts_model: speechTtsModel,
      speech_tts_voice: speechTtsVoice,
      speech_wyoming_tts_host: speechWyomingTtsHost,
      speech_wyoming_tts_port: speechWyomingTtsPort,
      speech_wyoming_tts_voice: speechWyomingTtsVoice,
      speech_announcement_tts_backend: speechAnnouncementTtsBackend,
      speech_announcement_tts_model: speechAnnouncementTtsModel,
      speech_announcement_tts_voice: speechAnnouncementTtsVoice,
    };
    const hydraRoleIds = ["chat", "astraeus", "thanatos", "minos", "hermes"];
    hydraRoleIds.forEach((role) => {
      const hostEl = document.getElementById(`set_hydra_llm_${role}_host`);
      const portEl = document.getElementById(`set_hydra_llm_${role}_port`);
      const modelEl = document.getElementById(`set_hydra_llm_${role}_model`);
      payload[`hydra_llm_${role}_host`] = hostEl ? String(hostEl.value || "").trim() : "";
      payload[`hydra_llm_${role}_port`] = portEl ? String(portEl.value || "").trim() : "";
      payload[`hydra_llm_${role}_model`] = modelEl ? String(modelEl.value || "").trim() : "";
    });
    statusEl.textContent = "Saving shared model settings...";
    try {
      await api("/api/settings", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      statusEl.textContent = "Shared model settings saved.";
      showToast("Shared model settings saved.");
    } catch (error) {
      statusEl.textContent = `Model save failed: ${error.message}`;
    }
  });

  const hydraSubtabButtons = Array.from(root.querySelectorAll(".settings-subtab-btn[data-hydra-tab]"));
  const hydraSubPanels = Array.from(root.querySelectorAll(".settings-subpanel[data-hydra-panel]"));
  const activateHydraSubtab = (tabKey) => {
    hydraSubtabButtons.forEach((button) => {
      button.classList.toggle("active", button.dataset.hydraTab === tabKey);
    });
    hydraSubPanels.forEach((panel) => {
      panel.classList.toggle("active", panel.dataset.hydraPanel === tabKey);
    });
  };
  hydraSubtabButtons.forEach((button) => {
    button.addEventListener("click", () => activateHydraSubtab(button.dataset.hydraTab));
  });
  bindEspHomeSettingsTabs(root);

  const metricsStatusEl = document.getElementById("settings-cerb-metrics-status");
  const metricsContentEl = document.getElementById("settings-cerb-metrics-content");
  const metricsPlatformEl = document.getElementById("set_cerb_metrics_platform");
  const metricsLimitEl = document.getElementById("set_cerb_metrics_limit");
  const metricsOutcomeEl = document.getElementById("set_cerb_metrics_outcome");
  const metricsToolEl = document.getElementById("set_cerb_metrics_tool");
  const metricsToolsOnlyEl = document.getElementById("set_cerb_metrics_tools_only");
  let hydraLedgerRows = [];

  const dataStatusEl = document.getElementById("settings-cerb-data-status");
  const dataContentEl = document.getElementById("settings-cerb-data-content");
  const dataPlatformEl = document.getElementById("set_cerb_data_platform");

  const ensureHydraLedgerModal = () => {
    let modal = document.getElementById("cerb-ledger-modal");
    if (modal) {
      return modal;
    }
    document.body.insertAdjacentHTML(
      "beforeend",
      `
        <div id="cerb-ledger-modal" class="cerb-modal" aria-hidden="true">
          <div class="cerb-modal-dialog card" role="dialog" aria-modal="true" aria-label="Hydra Ledger Entry">
            <div class="card-head">
              <h3 class="card-title">Hydra Ledger Entry</h3>
              <div class="inline-row">
                <button type="button" class="inline-btn" id="cerb-ledger-copy">Copy JSON</button>
                <button type="button" class="inline-btn" id="cerb-ledger-close">Close</button>
              </div>
            </div>
            <div id="cerb-ledger-meta" class="small"></div>
            <div class="cerb-modal-body">
              <pre id="cerb-ledger-pre" class="cerb-modal-pre"></pre>
            </div>
          </div>
        </div>
      `
    );

    modal = document.getElementById("cerb-ledger-modal");
    const closeBtn = document.getElementById("cerb-ledger-close");
    const copyBtn = document.getElementById("cerb-ledger-copy");

    const closeModal = () => {
      closePopupModal(modal);
    };

    closeBtn?.addEventListener("click", closeModal);
    modal.addEventListener("click", (event) => {
      if (event.target === modal) {
        closeModal();
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && modal.classList.contains("active")) {
        closeModal();
      }
    });
    copyBtn?.addEventListener("click", async () => {
      const source = String(modal?.dataset?.ledgerJson || "");
      if (!source) {
        return;
      }
      const original = copyBtn.textContent;
      try {
        await navigator.clipboard.writeText(source);
        copyBtn.textContent = "Copied";
      } catch {
        copyBtn.textContent = "Copy failed";
      }
      window.setTimeout(() => {
        copyBtn.textContent = original;
      }, 1200);
    });

    return modal;
  };

  const openHydraLedgerModal = (row) => {
    const modal = ensureHydraLedgerModal();
    const preEl = document.getElementById("cerb-ledger-pre");
    const metaEl = document.getElementById("cerb-ledger-meta");
    const rawRow = row?.raw && typeof row.raw === "object" ? row.raw : row;
    const ledgerJson = JSON.stringify(rawRow || {}, null, 2);
    modal.dataset.ledgerJson = ledgerJson;
    if (preEl) {
      preEl.textContent = ledgerJson;
    }
    if (metaEl) {
      const time = String(row?.time || "").trim();
      const platform = String(row?.platform || "").trim();
      const scope = String(row?.scope || "").trim();
      const outcome = String(row?.outcome || "").trim();
      const parts = [time, platform, scope, outcome].filter(Boolean);
      metaEl.textContent = parts.join(" • ");
    }
    openPopupModal(modal);
  };

  const toRateText = (value) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed.toFixed(4) : "0.0000";
  };
  const metricLabel = (value) => String(value || "").replaceAll("_", " ").trim();
  const renderMetricPills = (title, metrics, metricNames) => {
    const pills = (Array.isArray(metricNames) ? metricNames : []).map((name) => {
      return `
        <div class="core-metric-pill">
          <div class="small">${escapeHtml(metricLabel(name) || name)}</div>
          <div>${escapeHtml(String((metrics || {})[name] ?? 0))}</div>
        </div>
      `;
    });
    return `
      <section class="core-inline-section">
        <div class="small core-inline-section-title">${escapeHtml(title)}</div>
        <div class="core-metric-row">${pills.join("")}</div>
      </section>
    `;
  };

  const renderHydraLedgerTable = (rows, emptyMessage = "No ledger rows for this filter.") => {
    const list = Array.isArray(rows) ? rows : [];
    if (!list.length) {
      return `<div class="small">${escapeHtml(emptyMessage)}</div>`;
    }

    const bodyHtml = list
      .map((row, index) => {
        return `
          <tr>
            <td>${escapeHtml(String(row?.["#"] ?? index + 1))}</td>
            <td>${escapeHtml(String(row?.time ?? ""))}</td>
            <td>${escapeHtml(String(row?.platform ?? ""))}</td>
            <td>${escapeHtml(String(row?.scope ?? ""))}</td>
            <td>${escapeHtml(String(row?.astraeus_thanatos_kind ?? row?.planner_kind ?? ""))}</td>
            <td>${escapeHtml(String(row?.outcome ?? ""))}</td>
            <td>${escapeHtml(String(row?.planned_tool ?? ""))}</td>
            <td>${escapeHtml(String(row?.validation_status ?? ""))}</td>
            <td>${escapeHtml(String(row?.total_ms ?? ""))}</td>
            <td>
              <button type="button" class="inline-btn cerb-ledger-open" data-ledger-index="${index}">Open</button>
            </td>
          </tr>
        `;
      })
      .join("");

    return `
      <div class="core-data-table-wrap cerb-ledger-wrap">
        <table class="core-data-table cerb-ledger-table">
          <thead>
            <tr>
              <th>#</th>
              <th>Time</th>
              <th>Platform</th>
              <th>Scope</th>
              <th>Astraeus/Thanatos</th>
              <th>Outcome</th>
              <th>Tool</th>
              <th>Validation</th>
              <th>Total ms</th>
              <th>Ledger</th>
            </tr>
          </thead>
          <tbody>${bodyHtml}</tbody>
        </table>
      </div>
    `;
  };

  const renderHydraMetricsPayload = (payload) => {
    const metricNames = Array.isArray(payload?.metric_names) ? payload.metric_names : [];
    const globalRates = (Array.isArray(payload?.global_rates) ? payload.global_rates : []).map((row) => ({
      metric: metricLabel(row?.metric || ""),
      value: toRateText(row?.value),
    }));
    const platformRates = (Array.isArray(payload?.platform_rates) ? payload.platform_rates : []).map((row) => ({
      metric: metricLabel(row?.metric || ""),
      value: toRateText(row?.value),
    }));
    const platformRows = (Array.isArray(payload?.platform_rows) ? payload.platform_rows : []).map((row) => ({
      platform: row?.platform_label || hydraPlatformLabel(row?.platform || ""),
      total_turns: row?.total_turns ?? 0,
      total_tools_called: row?.total_tools_called ?? 0,
      total_repairs: row?.total_repairs ?? 0,
      validation_failures: row?.validation_failures ?? 0,
      tool_failures: row?.tool_failures ?? 0,
      tool_call_rate: toRateText(row?.tool_call_rate),
      repair_rate: toRateText(row?.repair_rate),
      validation_failure_rate: toRateText(row?.validation_failure_rate),
      tool_failure_rate: toRateText(row?.tool_failure_rate),
    }));
    const ledgerRows = Array.isArray(payload?.summary_rows) ? payload.summary_rows : [];

    const html = `
      ${renderMetricPills("Global Counters", payload?.global_metrics || {}, metricNames)}
      <section class="core-inline-section">
        <div class="small core-inline-section-title">Global Rates</div>
        ${renderSimpleDataTable(
          [
            { key: "metric", label: "Metric" },
            { key: "value", label: "Value" },
          ],
          globalRates,
          "No global rates."
        )}
      </section>

      ${renderMetricPills(
        `Selected Portal Counters (${payload?.selected_platform_label || hydraPlatformLabel(payload?.selected_platform)})`,
        payload?.platform_metrics || {},
        metricNames
      )}
      <section class="core-inline-section">
        <div class="small core-inline-section-title">Selected Portal Rates</div>
        ${renderSimpleDataTable(
          [
            { key: "metric", label: "Metric" },
            { key: "value", label: "Value" },
          ],
          platformRates,
          "No selected-portal rates."
        )}
      </section>

      <section class="core-inline-section">
        <div class="small core-inline-section-title">Per-Portal Totals</div>
        ${renderSimpleDataTable(
          [
            { key: "platform", label: "Platform" },
            { key: "total_turns", label: "Turns" },
            { key: "total_tools_called", label: "Tools" },
            { key: "total_repairs", label: "Repairs" },
            { key: "validation_failures", label: "Validation Failures" },
            { key: "tool_failures", label: "Tool Failures" },
            { key: "tool_call_rate", label: "Tool Rate" },
            { key: "repair_rate", label: "Repair Rate" },
            { key: "validation_failure_rate", label: "Validation Fail Rate" },
            { key: "tool_failure_rate", label: "Tool Fail Rate" },
          ],
          platformRows,
          "No per-portal totals."
        )}
      </section>

      <section class="core-inline-section">
        <div class="small core-inline-section-title">Ledger Rows</div>
        ${renderHydraLedgerTable(ledgerRows, "No ledger rows for this filter.")}
      </section>

      <section class="core-inline-section">
        <div class="small core-inline-section-title">Top Tools (Filtered)</div>
        ${renderSimpleBarChart(payload?.top_tools || [], "No tool usage in filtered rows.")}
      </section>
      <section class="core-inline-section">
        <div class="small core-inline-section-title">Top Failure Reasons (Filtered)</div>
        ${renderSimpleBarChart(payload?.top_reasons || [], "No failure reasons in filtered rows.")}
      </section>
    `;
    hydraLedgerRows = ledgerRows;
    metricsContentEl.innerHTML = html;
    metricsContentEl.querySelectorAll(".cerb-ledger-open").forEach((button) => {
      button.addEventListener("click", () => {
        const idx = Number(button.dataset.ledgerIndex || "-1");
        if (!Number.isFinite(idx) || idx < 0 || idx >= hydraLedgerRows.length) {
          return;
        }
        openHydraLedgerModal(hydraLedgerRows[idx]);
      });
    });
  };

  const fetchHydraMetrics = async (toolValue) => {
    const params = new URLSearchParams();
    params.set("platform", String(metricsPlatformEl.value || "webui"));
    params.set("limit", String(Math.max(10, Math.min(300, Number(metricsLimitEl.value || 50)))));
    params.set("outcome", String(metricsOutcomeEl.value || "all"));
    params.set("tool", String(toolValue || "all"));
    params.set("show_only_tool_turns", metricsToolsOnlyEl.checked ? "true" : "false");
    return api(`/api/settings/hydra/metrics?${params.toString()}`);
  };

  const refreshHydraMetrics = async () => {
    metricsStatusEl.textContent = "Loading metrics...";
    try {
      const currentTool = String(metricsToolEl.value || "all");
      let payload = await fetchHydraMetrics(currentTool);
      const toolOptions = ["all", ...(Array.isArray(payload?.tool_options) ? payload.tool_options : [])];

      metricsToolEl.innerHTML = toolOptions
        .map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`)
        .join("");

      let effectiveTool = currentTool;
      if (!toolOptions.includes(effectiveTool)) {
        effectiveTool = "all";
      }
      metricsToolEl.value = effectiveTool;
      if (effectiveTool !== currentTool) {
        payload = await fetchHydraMetrics(effectiveTool);
      }

      renderHydraMetricsPayload(payload);
      const filtered = Number(payload?.ledger_filtered ?? 0);
      const total = Number(payload?.ledger_total ?? 0);
      metricsStatusEl.textContent = `Loaded ${filtered} of ${total} ledger rows.`;
    } catch (error) {
      metricsStatusEl.textContent = `Metrics load failed: ${error.message}`;
      metricsContentEl.innerHTML = renderNotice(`Failed to load Hydra metrics: ${error.message}`);
    }
  };

  const refreshHydraData = async () => {
    dataStatusEl.textContent = "Loading data...";
    try {
      const payload = await api("/api/settings/hydra/data");
      const summary = payload?.summary && typeof payload.summary === "object" ? payload.summary : {};
      const platformRows = Array.isArray(payload?.platform_rows) ? payload.platform_rows : [];
      const ledgerRows = Array.isArray(payload?.ledger_rows) ? payload.ledger_rows : [];

      dataContentEl.innerHTML = `
        <section class="core-inline-section">
          <div class="small core-inline-section-title">Data Summary</div>
          <div class="core-metric-row">
            <div class="core-metric-pill">
              <div class="small">Metric Keys</div>
              <div>${escapeHtml(String(summary.metric_keys ?? 0))}</div>
            </div>
            <div class="core-metric-pill">
              <div class="small">Ledger Lists</div>
              <div>${escapeHtml(String(summary.ledger_lists ?? 0))}</div>
            </div>
            <div class="core-metric-pill">
              <div class="small">Ledger Entries</div>
              <div>${escapeHtml(String(summary.ledger_entries_total ?? 0))}</div>
            </div>
          </div>
        </section>

        <section class="core-inline-section">
          <div class="small core-inline-section-title">Turns By Platform</div>
          ${renderSimpleBarChart(payload?.turns_chart || [], "No turn counters yet.")}
        </section>
        <section class="core-inline-section">
          <div class="small core-inline-section-title">Ledger Entries By Key</div>
          ${renderSimpleBarChart(payload?.ledger_chart || [], "No ledger entries yet.")}
        </section>

        <section class="core-inline-section">
          <div class="small core-inline-section-title">Per-Portal Counters</div>
          ${renderSimpleDataTable(
            [
              { key: "platform_label", label: "Platform" },
              { key: "total_turns", label: "Turns" },
              { key: "total_tools_called", label: "Tools" },
              { key: "total_repairs", label: "Repairs" },
              { key: "validation_failures", label: "Validation Failures" },
              { key: "tool_failures", label: "Tool Failures" },
              { key: "ledger_entries", label: "Ledger Entries" },
            ],
            platformRows,
            "No per-portal counter rows."
          )}
        </section>

        <section class="core-inline-section">
          <div class="small core-inline-section-title">Ledger Keys</div>
          ${renderSimpleDataTable(
            [
              { key: "platform_label", label: "Platform" },
              { key: "ledger_key", label: "Ledger Key" },
              { key: "entries", label: "Entries" },
            ],
            ledgerRows,
            "No ledger keys found."
          )}
        </section>
      `;

      dataStatusEl.textContent = "Hydra data loaded.";
    } catch (error) {
      dataStatusEl.textContent = `Data load failed: ${error.message}`;
      dataContentEl.innerHTML = renderNotice(`Failed to load Hydra data: ${error.message}`);
    }
  };

  const clearHydraData = async ({ mode, platform, label }) => {
    if (!window.confirm(`Confirm ${label}?`)) {
      return;
    }
    dataStatusEl.textContent = "Running clear action...";
    try {
      const result = await api("/api/settings/hydra/data/clear", {
        method: "POST",
        body: JSON.stringify({ mode, platform }),
      });
      dataStatusEl.textContent = `Cleared. Metrics removed: ${result.metrics_removed}. Ledger lists removed: ${result.ledger_removed}.`;
      await Promise.all([refreshHydraData(), refreshHydraMetrics()]);
    } catch (error) {
      dataStatusEl.textContent = `Clear failed: ${error.message}`;
    }
  };

  document.getElementById("settings-cerb-metrics-refresh").addEventListener("click", () => {
    refreshHydraMetrics();
  });
  metricsPlatformEl.addEventListener("change", () => refreshHydraMetrics());
  metricsOutcomeEl.addEventListener("change", () => refreshHydraMetrics());
  metricsToolEl.addEventListener("change", () => refreshHydraMetrics());
  metricsToolsOnlyEl.addEventListener("change", () => refreshHydraMetrics());
  metricsLimitEl.addEventListener("change", () => refreshHydraMetrics());

  document.getElementById("settings-cerb-clear-all").addEventListener("click", () => {
    clearHydraData({
      mode: "all",
      platform: "all",
      label: "clearing all Hydra metrics and ledger data across all portals",
    });
  });
  document.getElementById("settings-cerb-clear-platform-all").addEventListener("click", () => {
    clearHydraData({
      mode: "all",
      platform: String(dataPlatformEl.value || "webui"),
      label: `clearing all Hydra data for ${hydraPlatformLabel(dataPlatformEl.value)}`,
    });
  });
  document.getElementById("settings-cerb-clear-platform-metrics").addEventListener("click", () => {
    clearHydraData({
      mode: "metrics",
      platform: String(dataPlatformEl.value || "webui"),
      label: `resetting Hydra metrics for ${hydraPlatformLabel(dataPlatformEl.value)}`,
    });
  });
  document.getElementById("settings-cerb-clear-platform-ledger").addEventListener("click", () => {
    clearHydraData({
      mode: "ledger",
      platform: String(dataPlatformEl.value || "webui"),
      label: `clearing Hydra ledger for ${hydraPlatformLabel(dataPlatformEl.value)}`,
    });
  });

  await Promise.all([refreshHydraMetrics(), refreshHydraData()]);

  document.getElementById("settings-admin-defaults").addEventListener("click", () => {
    const select = document.getElementById("set_admin_only_plugins");
    const values = adminDefaults;
    Array.from(select.options).forEach((option) => {
      option.selected = values.has(String(option.value || "").trim());
    });
    statusEl.textContent = "Default admin tool list loaded. Click Save Settings to apply.";
  });

  document.getElementById("settings-form").addEventListener("submit", (event) => {
    event.preventDefault();
  });

  const runSettingsSave = async () => {
    const adminSelect = document.getElementById("set_admin_only_plugins");
    const adminOnlyPlugins = Array.from(adminSelect.selectedOptions)
      .map((option) => String(option.value || "").trim())
      .filter(Boolean);
    const webuiPassword = String(webuiPasswordEl?.value || "");
    const webuiPasswordConfirm = String(webuiPasswordConfirmEl?.value || "");
    const hasWebuiPasswordInput = Boolean(webuiPassword || webuiPasswordConfirm);

    if (clearWebuiPasswordRequested && hasWebuiPasswordInput) {
      statusEl.textContent = "Choose either a new WebUI password or remove the current one.";
      showToast("Choose either a new WebUI password or remove the current one.", "error", 3600);
      return;
    }

    if (hasWebuiPasswordInput) {
      if (!webuiPassword) {
        statusEl.textContent = "Enter a WebUI password.";
        showToast("Enter a WebUI password.", "error", 3600);
        return;
      }
      if (webuiPassword.length < 4) {
        statusEl.textContent = "WebUI password must be at least 4 characters.";
        showToast("WebUI password must be at least 4 characters.", "error", 3600);
        return;
      }
      if (webuiPassword !== webuiPasswordConfirm) {
        statusEl.textContent = "WebUI passwords do not match.";
        showToast("WebUI passwords do not match.", "error", 3600);
        return;
      }
    }

    const esphomeFormEl = document.getElementById("settings-esphome-form");
    const payload = {
      username: document.getElementById("set_username").value,
      max_display: Number(document.getElementById("set_max_display").value || 8),
      max_store: Number(document.getElementById("set_max_store").value || 20),
      max_llm: Number(document.getElementById("set_max_llm").value || 8),
      tater_first_name: document.getElementById("set_tater_first_name").value,
      tater_last_name: document.getElementById("set_tater_last_name").value,
      tater_personality: document.getElementById("set_tater_personality").value,
      show_speed_stats: document.getElementById("set_show_speed_stats").checked,
      web_search_google_api_key: document.getElementById("set_web_search_google_api_key").value,
      web_search_google_cx: document.getElementById("set_web_search_google_cx").value,
      homeassistant_base_url: document.getElementById("set_homeassistant_base_url").value,
      homeassistant_token: document.getElementById("set_homeassistant_token").value,
      unifi_network_base_url: document.getElementById("set_unifi_network_base_url").value,
      unifi_network_api_key: document.getElementById("set_unifi_network_api_key").value,
      unifi_protect_base_url: document.getElementById("set_unifi_protect_base_url").value,
      unifi_protect_api_key: document.getElementById("set_unifi_protect_api_key").value,
      vision_api_base: document.getElementById("set_vision_api_base").value,
      vision_model: document.getElementById("set_vision_model").value,
      vision_api_key: document.getElementById("set_vision_api_key").value,
      emoji_enable_on_reaction_add: document.getElementById("set_emoji_enable_on_reaction_add").checked,
      emoji_enable_auto_reaction_on_reply: document.getElementById("set_emoji_enable_auto_reaction_on_reply").checked,
      emoji_reaction_chain_chance_percent: Number(
        document.getElementById("set_emoji_reaction_chain_chance_percent").value || 100
      ),
      emoji_reply_reaction_chance_percent: Number(
        document.getElementById("set_emoji_reply_reaction_chance_percent").value || 12
      ),
      emoji_reaction_chain_cooldown_seconds: Number(
        document.getElementById("set_emoji_reaction_chain_cooldown_seconds").value || 30
      ),
      emoji_reply_reaction_cooldown_seconds: Number(
        document.getElementById("set_emoji_reply_reaction_cooldown_seconds").value || 120
      ),
      emoji_min_message_length: Number(document.getElementById("set_emoji_min_message_length").value || 4),
      hydra_max_ledger_items: Number(document.getElementById("set_hydra_max_ledger_items").value || 1500),
      hydra_step_retry_limit: Number(document.getElementById("set_hydra_step_retry_limit").value || 1),
      hydra_astraeus_plan_review_enabled: document.getElementById(
        "set_hydra_astraeus_plan_review_enabled"
      ).checked,
      popup_effect_style: normalizePopupEffectStyle(document.getElementById("set_popup_effect_style")?.value || "flame"),
      admin_only_plugins: adminOnlyPlugins,
      esphome_settings: esphomeFormEl ? collectCoreManagerValues(esphomeFormEl) : {},
    };
    if (clearWebuiPasswordRequested) {
      payload.clear_webui_password = true;
    } else if (hasWebuiPasswordInput) {
      payload.webui_password = webuiPassword;
      payload.webui_password_confirm = webuiPasswordConfirm;
    }

    statusEl.textContent = "Saving...";
    try {
      const userAvatarFile = userAvatarFileEl?.files && userAvatarFileEl.files[0] ? userAvatarFileEl.files[0] : null;
      const taterAvatarFile = taterAvatarFileEl?.files && taterAvatarFileEl.files[0] ? taterAvatarFileEl.files[0] : null;

      if (userAvatarFile) {
        payload.user_avatar = await readFileAsDataUrl(userAvatarFile);
      } else if (clearUserAvatarRequested) {
        payload.clear_user_avatar = true;
      }

      if (taterAvatarFile) {
        payload.tater_avatar = await readFileAsDataUrl(taterAvatarFile);
      } else if (clearTaterAvatarRequested) {
        payload.clear_tater_avatar = true;
      }

      const saveResult = await api("/api/settings", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      applyPopupEffectStyle(payload.popup_effect_style);
      if (webuiPasswordEl) {
        webuiPasswordEl.value = "";
      }
      if (webuiPasswordConfirmEl) {
        webuiPasswordConfirmEl.value = "";
      }
      clearWebuiPasswordRequested = false;
      if (payload.clear_webui_password) {
        webuiPasswordIsSet = false;
      } else if (payload.webui_password) {
        webuiPasswordIsSet = true;
      }
      try {
        const authStatus = await fetchWebuiAuthStatus();
        webuiPasswordIsSet = Boolean(authStatus?.passwordSet);
      } catch {
        // Keep local status if auth refresh fails.
      }
      refreshWebuiPasswordUi();
      await refreshBranding();
      clearUserAvatarRequested = false;
      clearTaterAvatarRequested = false;
      if (userAvatarFileEl) {
        userAvatarFileEl.value = "";
      }
      if (taterAvatarFileEl) {
        taterAvatarFileEl.value = "";
      }
      statusEl.textContent = "Saved.";
      showToast("Settings saved.");
    } catch (error) {
      statusEl.textContent = `Save failed: ${error.message}`;
      showToast(`Save failed: ${error.message}`, "error", 3600);
    }
  };

  [
    "settings-save",
    "settings-save-general",
    "settings-save-integrations",
    "settings-save-esphome",
    "settings-save-misc",
    "settings-save-advanced",
  ].forEach((buttonId) => {
    document.getElementById(buttonId)?.addEventListener("click", runSettingsSave);
  });
}

async function loadView(viewName) {
  setRedisBootstrapMode(false);
  state.view = viewName;
  document.body.dataset.view = String(viewName || "").trim().toLowerCase();
  setActiveNav(viewName);
  updateHeader();

  const root = document.getElementById("view-root");
  root.dataset.view = String(viewName || "").trim().toLowerCase();
  root.innerHTML = renderNotice("Loading...");

  try {
    if (viewName === "chat") {
      await loadChatView();
      return;
    }
    if (viewName === "verbas") {
      await loadVerbasView();
      return;
    }
    if (viewName === "portals" || viewName === "cores") {
      await loadSurfaceView(viewName);
      return;
    }
    if (viewName === "settings") {
      await loadSettingsView();
      return;
    }

    root.innerHTML = renderNotice(`Unknown view: ${viewName}`);
  } catch (error) {
    root.innerHTML = renderNotice(`Failed to load ${viewName}: ${error.message}`);
  }
}

function bindNav() {
  document.querySelectorAll(".nav-btn").forEach((button) => {
    button.addEventListener("click", () => loadView(button.dataset.view));
  });
}

function bindSidebarControls() {
  const collapseBtn = document.getElementById("sidebar-collapse-btn");
  const expandBtn = document.getElementById("sidebar-expand-btn");

  collapseBtn?.addEventListener("click", () => {
    setSidebarCollapsed(!state.sidebarCollapsed);
  });
  expandBtn?.addEventListener("click", () => {
    setSidebarCollapsed(false);
  });

  applySidebarState();
}

async function init() {
  bindSidebarControls();
  bindNav();
  bindRuntimeSummary();
  state.settingsTab = normalizeSettingsTab(state.settingsTab || "general");
  const redisStatus = await ensureRedisSetup();
  if (!redisStatus?.connected) {
    const root = document.getElementById("view-root");
    renderRedisBootstrapView(root, redisStatus, normalizeRedisEncryptionStatusPayload({}));
    return;
  }
  try {
    await ensureWebuiAuth();
  } catch (error) {
    if (error?.code === "REDIS_SETUP_REQUIRED" || _isLikelyRedisFailureDetail(error?.message || "")) {
      const root = document.getElementById("view-root");
      renderRedisBootstrapView(root, state.redisStatus || redisStatus, normalizeRedisEncryptionStatusPayload({}));
      return;
    }
    throw error;
  }
  await refreshBranding();
  await refreshHealth();
  await loadView(state.view);
  _scheduleHealthRefresh(HEALTH_POLL_CONNECTED_MS);
}

window.addEventListener("beforeunload", () => {
  if (healthRefreshTimer) {
    window.clearTimeout(healthRefreshTimer);
    healthRefreshTimer = 0;
  }
  closeChatEventSource();
  stopAllChatJobPolling();
  stopRuntimeBreakdownPolling();
});

init().catch((error) => {
  const root = document.getElementById("view-root");
  if (root) {
    root.innerHTML = renderNotice(`Failed to initialize UI: ${error?.message || "unknown error"}`);
  }
});
