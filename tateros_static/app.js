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
  coreTopTab: safeStorageGet("tater_tateros_core_tab", "") || "manage",
  sidebarCollapsed: String(safeStorageGet("tater_tateros_sidebar_collapsed", "false")).trim().toLowerCase() === "true",
  sidebarCollapseTimer: 0,
  runtimeBreakdownPollTimer: 0,
  runtimeBreakdownPayload: null,
  runtimeHistoryWindow: "24h",
  runtimeSettingsSaveHandler: null,
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

let redisSetupPromise = null;
let redisSetupResolve = null;
let redisRecoveryPromptInFlight = false;
let redisRecoveryPromptLastAt = 0;
let webuiAuthPromise = null;
let webuiAuthResolve = null;
let webuiAuthRecoveryInFlight = false;
let webuiAuthRecoveryLastAt = 0;

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
      auth = await fetchWebuiAuthStatus();
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

function _showRedisSetupModal(status) {
  const modal = ensureRedisSetupModal();
  if (typeof modal._applyRedisStatus === "function") {
    modal._applyRedisStatus(status || state.redisStatus || {});
  }
  openPopupModal(modal);
  // Defensive hard-open guard in case a stale closing class lingers.
  modal.classList.remove("closing");
  modal.classList.add("active");
  modal.setAttribute("aria-hidden", "false");
  syncPopupBodyScrollLock();
  if (!redisSetupPromise) {
    redisSetupPromise = new Promise((resolve) => {
      redisSetupResolve = resolve;
    });
  }
  return modal;
}

function ensureRedisSetupModal() {
  let modal = document.getElementById("redis-setup-modal");
  if (modal) {
    return modal;
  }

  document.body.insertAdjacentHTML(
    "beforeend",
    `
      <div id="redis-setup-modal" class="cerb-modal redis-setup-modal" aria-hidden="true">
        <div class="cerb-modal-dialog card redis-setup-dialog" role="dialog" aria-modal="true" aria-label="Redis Setup Required">
          <div class="card-head">
            <h3 class="card-title">Redis Setup Required</h3>
          </div>
          <div id="redis-setup-status" class="small"></div>
          <form id="redis-setup-form" class="form-grid two-col redis-setup-form">
            <label>Host
              <input id="redis_host" type="text" placeholder="127.0.0.1" />
            </label>
            <label>Port
              <input id="redis_port" type="number" min="1" max="65535" value="6379" />
            </label>
            <label>DB
              <input id="redis_db" type="number" min="0" value="0" />
            </label>
            <label>Username (optional)
              <input id="redis_username" type="text" />
            </label>
            <label>Password (optional)
              <input id="redis_password" type="password" autocomplete="new-password" />
            </label>
            <label>CA Cert Path (optional)
              <input id="redis_ca_cert_path" type="text" placeholder="/path/to/ca.pem" />
            </label>
            <label class="toggle-row">Use TLS
              <input id="redis_use_tls" type="checkbox" />
            </label>
            <label class="toggle-row">Verify TLS Cert
              <input id="redis_verify_tls" type="checkbox" checked />
            </label>
            <div class="inline-actions" style="grid-column: 1 / -1;">
              <button type="button" id="redis_test_btn" class="inline-btn">Test Connection</button>
              <button type="submit" id="redis_save_btn" class="action-btn">Save and Connect</button>
              <button type="button" id="redis_refresh_btn" class="inline-btn">Refresh Status</button>
            </div>
          </form>
          <div id="redis-setup-extra" class="small"></div>
        </div>
      </div>
    `
  );

  modal = document.getElementById("redis-setup-modal");
  const form = document.getElementById("redis-setup-form");
  const statusEl = document.getElementById("redis-setup-status");
  const extraEl = document.getElementById("redis-setup-extra");
  const saveBtn = document.getElementById("redis_save_btn");
  const testBtn = document.getElementById("redis_test_btn");
  const refreshBtn = document.getElementById("redis_refresh_btn");
  const useTlsEl = document.getElementById("redis_use_tls");
  const verifyTlsEl = document.getElementById("redis_verify_tls");
  const caCertEl = document.getElementById("redis_ca_cert_path");
  const passwordEl = document.getElementById("redis_password");

  const setBusy = (busy) => {
    const disabled = Boolean(busy);
    if (saveBtn) {
      saveBtn.disabled = disabled;
      saveBtn.textContent = disabled ? "Saving..." : "Save and Connect";
    }
    if (testBtn) {
      testBtn.disabled = disabled;
    }
    if (refreshBtn) {
      refreshBtn.disabled = disabled;
    }
  };

  const syncTlsUi = () => {
    const enabled = Boolean(useTlsEl?.checked);
    if (verifyTlsEl) {
      verifyTlsEl.disabled = !enabled;
    }
    if (caCertEl) {
      caCertEl.disabled = !enabled;
      if (!enabled) {
        caCertEl.value = "";
      }
    }
  };

  const formPayload = (status, testOnly) => {
    const maskedPassword = String(passwordEl?.dataset?.masked || "") === "1";
    const passwordRaw = maskedPassword ? "" : String(passwordEl?.value || "");
    const keepExistingPassword = (maskedPassword || !passwordRaw) && Boolean(status?.password_set);
    return {
      host: String(document.getElementById("redis_host")?.value || "").trim(),
      port: Number(document.getElementById("redis_port")?.value || 6379),
      db: Number(document.getElementById("redis_db")?.value || 0),
      username: String(document.getElementById("redis_username")?.value || "").trim(),
      password: passwordRaw,
      use_tls: Boolean(useTlsEl?.checked),
      verify_tls: Boolean(verifyTlsEl?.checked),
      ca_cert_path: String(caCertEl?.value || "").trim(),
      keep_existing_password: keepExistingPassword,
      test_only: Boolean(testOnly),
    };
  };

  const applyStatus = (raw, options = {}) => {
    const preserveInputs = Boolean(options?.preserveInputs);
    const currentInputs = preserveInputs
      ? {
          host: String(document.getElementById("redis_host")?.value || ""),
          port: String(document.getElementById("redis_port")?.value || "6379"),
          db: String(document.getElementById("redis_db")?.value || "0"),
          username: String(document.getElementById("redis_username")?.value || ""),
          password: String(passwordEl?.value || ""),
          password_masked: String(passwordEl?.dataset?.masked || "") === "1",
          use_tls: Boolean(useTlsEl?.checked),
          verify_tls: Boolean(verifyTlsEl?.checked),
          ca_cert_path: String(caCertEl?.value || ""),
        }
      : null;
    const status = _setRedisStatus(raw);
    if (document.getElementById("redis_host")) {
      document.getElementById("redis_host").value = preserveInputs ? String(currentInputs?.host || "") : String(status.host || "");
    }
    if (document.getElementById("redis_port")) {
      document.getElementById("redis_port").value = preserveInputs ? String(currentInputs?.port || "6379") : String(status.port || 6379);
    }
    if (document.getElementById("redis_db")) {
      document.getElementById("redis_db").value = preserveInputs ? String(currentInputs?.db || "0") : String(status.db || 0);
    }
    if (document.getElementById("redis_username")) {
      document.getElementById("redis_username").value = preserveInputs
        ? String(currentInputs?.username || "")
        : String(status.username || "");
    }
    if (useTlsEl) {
      useTlsEl.checked = preserveInputs ? Boolean(currentInputs?.use_tls) : Boolean(status.use_tls);
    }
    if (verifyTlsEl) {
      verifyTlsEl.checked = preserveInputs ? Boolean(currentInputs?.verify_tls) : Boolean(status.verify_tls);
    }
    if (caCertEl) {
      caCertEl.value = preserveInputs ? String(currentInputs?.ca_cert_path || "") : String(status.ca_cert_path || "");
    }
    if (passwordEl) {
      if (preserveInputs) {
        passwordEl.value = String(currentInputs?.password || "");
        if (currentInputs?.password_masked) {
          passwordEl.dataset.masked = "1";
        } else {
          delete passwordEl.dataset.masked;
        }
      } else if (status.password_set) {
        passwordEl.value = REDIS_PASSWORD_MASK;
        passwordEl.dataset.masked = "1";
      } else {
        passwordEl.value = "";
        delete passwordEl.dataset.masked;
      }
      passwordEl.placeholder = status.password_set ? "Leave blank to keep saved password" : "";
    }
    syncTlsUi();
    if (statusEl) {
      statusEl.textContent = _redisSetupMessage(status);
      statusEl.classList.toggle("error", !status.connected);
      statusEl.classList.toggle("success", Boolean(status.connected));
    }
    if (extraEl) {
      const source = status.source ? `Source: ${status.source}` : "";
      const path = status.config_path ? `Config: ${status.config_path}` : "";
      extraEl.textContent = [source, path].filter(Boolean).join(" • ");
    }
  };

  useTlsEl?.addEventListener("change", syncTlsUi);
  passwordEl?.addEventListener("focus", () => {
    if (String(passwordEl?.dataset?.masked || "") === "1") {
      passwordEl.value = "";
      delete passwordEl.dataset.masked;
    }
  });
  passwordEl?.addEventListener("input", () => {
    if (String(passwordEl?.dataset?.masked || "") === "1" && String(passwordEl.value || "") !== REDIS_PASSWORD_MASK) {
      delete passwordEl.dataset.masked;
    }
  });

  refreshBtn?.addEventListener("click", async () => {
    setBusy(true);
    try {
      const status = await api("/api/redis/status");
      applyStatus(status);
    } catch (error) {
      if (statusEl) {
        statusEl.textContent = `Failed to refresh Redis status: ${error.message}`;
        statusEl.classList.add("error");
      }
    } finally {
      setBusy(false);
    }
  });

  testBtn?.addEventListener("click", async () => {
    const current = state.redisStatus || {};
    const payload = formPayload(current, true);
    setBusy(true);
    try {
      const status = await api("/api/redis/configure", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      applyStatus(status, { preserveInputs: true });
      if (statusEl) {
        statusEl.textContent = "Redis connection test succeeded.";
        statusEl.classList.remove("error");
        statusEl.classList.add("success");
      }
    } catch (error) {
      if (statusEl) {
        statusEl.textContent = String(error?.message || "Redis connection test failed.");
        statusEl.classList.remove("success");
        statusEl.classList.add("error");
      }
    } finally {
      setBusy(false);
    }
  });

  form?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const current = state.redisStatus || {};
    const shouldReloadAfterSave = !Boolean(current?.configured);
    const payload = formPayload(current, false);
    setBusy(true);
    try {
      const status = await api("/api/redis/configure", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      applyStatus(status);
      if (!status.connected) {
        throw new Error("Redis settings were saved but the connection is still unavailable.");
      }
      if (shouldReloadAfterSave) {
        window.location.reload();
        return;
      }
      closePopupModal(modal);
      if (typeof redisSetupResolve === "function") {
        redisSetupResolve(status);
      }
      redisSetupPromise = null;
      redisSetupResolve = null;
    } catch (error) {
      if (statusEl) {
        statusEl.textContent = String(error?.message || "Redis setup failed.");
        statusEl.classList.remove("success");
        statusEl.classList.add("error");
      }
    } finally {
      setBusy(false);
    }
  });

  modal._applyRedisStatus = applyStatus;
  return modal;
}

async function ensureRedisSetup() {
  let status = state.redisStatus || {};
  try {
    status = _setRedisStatus(await api("/api/redis/status", { _skipRedisRecovery: true, _timeoutMs: 2500 }));
  } catch (error) {
    const message = String(error?.message || "Failed to load Redis status.");
    status = _setRedisStatus({
      ...(status || {}),
      connected: false,
      error: message,
    });
  }
  if (status.connected) {
    return status;
  }

  _showRedisSetupModal(status);
  return redisSetupPromise;
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
  const existingModal = document.getElementById("redis-setup-modal");
  if (!force && existingModal?.classList.contains("active")) {
    return;
  }
  if (!force) {
    if (redisRecoveryPromptInFlight) {
      return;
    }
    if (now - redisRecoveryPromptLastAt < 2500) {
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
    _showRedisSetupModal(status);

    try {
      status = _setRedisStatus(await api("/api/redis/status", { _skipRedisRecovery: true, _timeoutMs: 2500 }));
    } catch (error) {
      const nextError = String(reason || error?.message || status?.error || "Redis is unavailable.");
      status = _setRedisStatus({
        ...(status || {}),
        connected: false,
        error: nextError,
      });
    }

    if (status.connected) {
      const modal = document.getElementById("redis-setup-modal");
      if (modal?.classList?.contains("active")) {
        closePopupModal(modal);
      }
      return;
    }

    _showRedisSetupModal(status);
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
    state.runtimeSettingsSaveHandler = null;
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

function openRuntimeSettingsModal({ title, meta, fields, onSave }) {
  const modal = ensureRuntimeSettingsModal();
  const titleEl = document.getElementById("runtime-settings-title");
  const metaEl = document.getElementById("runtime-settings-meta");
  const fieldsEl = document.getElementById("runtime-settings-fields");
  const statusEl = document.getElementById("runtime-settings-status");
  const saveBtn = document.getElementById("runtime-settings-save");
  const normalizedFields = Array.isArray(fields) ? fields : [];

  state.runtimeSettingsSaveHandler = typeof onSave === "function" ? onSave : null;

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
  return `${verbasEnabled} verba enabled • ${portalsRunning} portals running • ${coresRunning} cores running • ${hydraJobsActive} hydra jobs • ${llmCallsActive} llm calls`;
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
  const byPlatform = Array.isArray(hydraJobs?.by_platform) ? hydraJobs.by_platform : [];
  const activeTurns = Array.isArray(hydraJobs?.active_turns) ? hydraJobs.active_turns : [];
  const history = hydraJobs?.history && typeof hydraJobs.history === "object" ? hydraJobs.history : {};
  const historyWindows = Array.isArray(history?.windows) ? history.windows : [];
  const platformRowsHtml = byPlatform.length
    ? `
        <div class="runtime-breakdown-list">
          ${byPlatform
            .map((row) => {
              const count = Number(row?.running_turns ?? 0);
              return `
                <div class="runtime-breakdown-row compact">
                  <div class="runtime-breakdown-main">
                    <div class="runtime-breakdown-name">${escapeHtml(String(row?.label || row?.platform || "Unknown"))}</div>
                  </div>
                  <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(`${count} running`)}</span></div>
                </div>
              `;
            })
            .join("")}
        </div>
      `
    : `<div class="small muted">No active running turns.</div>`;

  const activeTurnsHtml = activeTurns.length
    ? `
        <div class="runtime-breakdown-list runtime-hydra-turn-list">
          ${activeTurns
            .map((row) => {
              const turnId = String(row?.id || "").trim();
              const shortId = turnId ? turnId.slice(0, 8) : "";
              const platformLabel = String(row?.platform_label || row?.platform || "Unknown");
              const taskName = String(row?.task_name || "").trim() || "Hydra task";
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
                  ${scope ? `<div class="small muted runtime-hydra-turn-scope">Scope: ${escapeHtml(scope)}</div>` : ""}
                </div>
              `;
            })
            .join("")}
        </div>
      `
    : `<div class="small muted">No active Hydra turns right now.</div>`;

  const historyWindowKey = _runtimeHistoryWindowKey(state.runtimeHistoryWindow, "24h");
  const selectedHistoryWindow =
    historyWindows.find((row) => _runtimeHistoryWindowKey(row?.key, "") === historyWindowKey) || historyWindows[0] || null;
  const selectedHistoryKey = _runtimeHistoryWindowKey(selectedHistoryWindow?.key, historyWindowKey);
  const historyTabsHtml = _renderRuntimeHistoryWindowTabs(historyWindows, selectedHistoryKey, "Hydra history window");
  const historyHtml = selectedHistoryWindow
    ? (() => {
        const jobs = Number(selectedHistoryWindow?.jobs ?? 0);
        const done = Number(selectedHistoryWindow?.done ?? 0);
        const blocked = Number(selectedHistoryWindow?.blocked ?? 0);
        const failed = Number(selectedHistoryWindow?.failed ?? 0);
        const topPlatforms = Array.isArray(selectedHistoryWindow?.top_platforms) ? selectedHistoryWindow.top_platforms : [];
        const platformLine = topPlatforms.length
          ? topPlatforms.map((row) => `${String(row?.label || row?.platform || "Unknown")}: ${Number(row?.jobs ?? 0)}`).join(" • ")
          : "No jobs in this period.";
        return `
          <div class="runtime-breakdown-list runtime-breakdown-list-static">
            <div class="runtime-breakdown-row">
              <div class="runtime-breakdown-main">
                <div class="runtime-breakdown-name">${escapeHtml(String(selectedHistoryWindow?.label || "Window"))}</div>
                <div class="small muted">Done ${done} • Blocked ${blocked} • Failed ${failed}</div>
                <div class="small muted">${escapeHtml(platformLine)}</div>
              </div>
              <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(`${jobs} jobs`)}</span></div>
            </div>
          </div>
        `;
      })()
    : `<div class="small muted">No history available yet.</div>`;

  return `
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle">Active Turns</div>
      ${activeTurnsHtml}
    </div>
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle">By Platform</div>
      ${platformRowsHtml}
    </div>
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle-row">
        <div class="runtime-breakdown-subtitle">History</div>
        ${historyTabsHtml}
      </div>
      ${historyHtml}
      <div class="small muted">Sample size: ${escapeHtml(String(Number(history?.sample_size ?? 0)))} ledger rows</div>
    </div>
  `;
}

function _renderRuntimeLlmCallRows(llmCalls) {
  const totals = llmCalls?.totals && typeof llmCalls.totals === "object" ? llmCalls.totals : {};
  const byKind = Array.isArray(llmCalls?.active_by_kind) ? llmCalls.active_by_kind : [];
  const bySource = Array.isArray(llmCalls?.active_by_source) ? llmCalls.active_by_source : [];
  const activeCalls = Array.isArray(llmCalls?.active_calls) ? llmCalls.active_calls : [];
  const history = llmCalls?.history && typeof llmCalls.history === "object" ? llmCalls.history : {};
  const historyWindows = Array.isArray(history?.windows) ? history.windows : [];

  const byKindHtml = byKind.length
    ? `
        <div class="runtime-breakdown-list">
          ${byKind
            .map((row) => {
              const calls = Number(row?.calls ?? 0);
              return `
                <div class="runtime-breakdown-row compact">
                  <div class="runtime-breakdown-main">
                    <div class="runtime-breakdown-name">${escapeHtml(String(row?.label || row?.kind || "Unknown"))}</div>
                  </div>
                  <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(`${calls} active`)}</span></div>
                </div>
              `;
            })
            .join("")}
        </div>
      `
    : `<div class="small muted">No active LLM calls.</div>`;

  const bySourceHtml = bySource.length
    ? `
        <div class="runtime-breakdown-list">
          ${bySource
            .map((row) => {
              const calls = Number(row?.calls ?? 0);
              return `
                <div class="runtime-breakdown-row compact">
                  <div class="runtime-breakdown-main">
                    <div class="runtime-breakdown-name">${escapeHtml(String(row?.label || "Unknown source"))}</div>
                  </div>
                  <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(`${calls} active`)}</span></div>
                </div>
              `;
            })
            .join("")}
        </div>
      `
    : `<div class="small muted">No active source rows.</div>`;

  const activeCallsHtml = activeCalls.length
    ? `
        <div class="runtime-breakdown-list">
          ${activeCalls
            .map((row) => {
              const sourceLabel = String(row?.source_label || row?.label || "Unknown source");
              const model = String(row?.model || "model");
              const host = String(row?.host || "").trim();
              const functionName = String(row?.function || "").trim();
              const messageCount = Number(row?.message_count ?? 0);
              const detailLineParts = [`Model ${model}`];
              if (host) {
                detailLineParts.push(host);
              }
              const extraLineParts = [];
              if (functionName) {
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

  const historyWindowKey = _runtimeHistoryWindowKey(state.runtimeHistoryWindow, "24h");
  const selectedHistoryWindow =
    historyWindows.find((row) => _runtimeHistoryWindowKey(row?.key, "") === historyWindowKey) || historyWindows[0] || null;
  const selectedHistoryKey = _runtimeHistoryWindowKey(selectedHistoryWindow?.key, historyWindowKey);
  const historyTabsHtml = _renderRuntimeHistoryWindowTabs(historyWindows, selectedHistoryKey, "LLM call history window");
  const historyHtml = selectedHistoryWindow
    ? (() => {
        const calls = Number(selectedHistoryWindow?.calls ?? 0);
        const completed = Number(selectedHistoryWindow?.completed ?? 0);
        const failed = Number(selectedHistoryWindow?.failed ?? 0);
        const avgMs = Number(selectedHistoryWindow?.avg_ms ?? 0);
        const topSources = Array.isArray(selectedHistoryWindow?.top_sources) ? selectedHistoryWindow.top_sources : [];
        const sourceLine = topSources.length
          ? topSources.map((row) => `${String(row?.label || row?.source || "Unknown")}: ${Number(row?.calls ?? 0)}`).join(" • ")
          : "No calls in this period.";
        return `
          <div class="runtime-breakdown-list runtime-breakdown-list-static">
            <div class="runtime-breakdown-row">
              <div class="runtime-breakdown-main">
                <div class="runtime-breakdown-name">${escapeHtml(String(selectedHistoryWindow?.label || "Window"))}</div>
                <div class="small muted">Done ${completed} • Failed ${failed} • Avg ${avgMs.toFixed(1)} ms</div>
                <div class="small muted">${escapeHtml(sourceLine)}</div>
              </div>
              <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(`${calls} calls`)}</span></div>
            </div>
          </div>
        `;
      })()
    : `<div class="small muted">No LLM call history available yet.</div>`;

  return `
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle">By Type</div>
      ${byKindHtml}
    </div>
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle">By Source</div>
      ${bySourceHtml}
    </div>
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle">Active Calls</div>
      ${activeCallsHtml}
    </div>
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle-row">
        <div class="runtime-breakdown-subtitle">History</div>
        ${historyTabsHtml}
      </div>
      ${historyHtml}
      <div class="small muted">Sample size: ${escapeHtml(String(Number(history?.sample_size ?? 0)))} completed calls</div>
    </div>
    <div class="runtime-breakdown-block">
      <div class="small muted">
        Totals since boot: Started ${escapeHtml(String(Number(totals?.started ?? 0)))} • Completed ${escapeHtml(
          String(Number(totals?.completed ?? 0))
        )} • Failed ${escapeHtml(String(Number(totals?.failed ?? 0)))}
      </div>
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
    `Min window ${_runtimeFmtInt(minimumWindow)}`,
    `Recommended ${_runtimeFmtInt(recommendedWindow)}`,
  ];

  return `
    <section class="runtime-breakdown-card runtime-breakdown-card-wide">
      <div class="runtime-breakdown-head">
        <h4 class="runtime-breakdown-title">Estimated Chat Context Window</h4>
        <div class="small muted">${escapeHtml(summaryParts.join(" • "))}</div>
      </div>
      <div class="runtime-breakdown-block">
        <div class="runtime-breakdown-subtitle">Prompt Composition</div>
        <div class="runtime-breakdown-list">
          <div class="runtime-breakdown-row compact">
            <div class="runtime-breakdown-main">
              <div class="runtime-breakdown-name">System prompt</div>
              <div class="small muted">Chat fallback instructions</div>
            </div>
            <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(_runtimeFmtInt(systemTokens))}</span></div>
          </div>
          <div class="runtime-breakdown-row compact">
            <div class="runtime-breakdown-main">
              <div class="runtime-breakdown-name">Runtime status block</div>
              <div class="small muted">Enabled verbas, portals, and cores</div>
            </div>
            <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(_runtimeFmtInt(statusTokens))}</span></div>
          </div>
          <div class="runtime-breakdown-row compact">
            <div class="runtime-breakdown-main">
              <div class="runtime-breakdown-name">Core context blocks</div>
              <div class="small muted">Memory/core-injected chat context</div>
            </div>
            <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(_runtimeFmtInt(coreTokens + preambleTokens))}</span></div>
          </div>
          <div class="runtime-breakdown-row compact">
            <div class="runtime-breakdown-main">
              <div class="runtime-breakdown-name">Chat history</div>
              <div class="small muted">${escapeHtml(`${historyMessages}/${maxHistoryMessages || historyMessages} messages sent to LLM`)}</div>
            </div>
            <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(_runtimeFmtInt(historyTokens))}</span></div>
          </div>
          <div class="runtime-breakdown-row compact">
            <div class="runtime-breakdown-main">
              <div class="runtime-breakdown-name">Current user turn</div>
              <div class="small muted">Estimated next user input</div>
            </div>
            <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(_runtimeFmtInt(userTokens))}</span></div>
          </div>
        </div>
      </div>
      <div class="runtime-breakdown-block">
        <div class="small muted">
          Active stack: ${escapeHtml(`${enabledVerbas} verbas enabled • ${connectedPortals} portals connected • ${runningCores} cores running`)}
        </div>
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
  return `
    <div class="runtime-breakdown-grid">
      <section class="runtime-breakdown-card">
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
        <div class="cerb-modal-dialog card runtime-breakdown-dialog" role="dialog" aria-modal="true" aria-label="Hydra Jobs and LLM Calls">
          <div class="card-head">
            <h3 class="card-title">Live Hydra Jobs + LLM Calls</h3>
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
  summary.title = "Open live Hydra jobs and LLM calls";
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
  try {
    const health = await api("/api/health");
    if (health?.redis_status && typeof health.redis_status === "object") {
      _setRedisStatus(health.redis_status);
    }
    const redisConnected = Boolean(health?.redis_status?.connected ?? health?.redis);
    if (!redisConnected) {
      setRuntimeSummaryText("Redis setup required", "offline");
      void promptRedisSetupRecovery(String(health?.redis_status?.error || "Redis connection lost."));
      return;
    }
    setRuntimeSummaryText(formatRuntimeSummary(health), "normal");
  } catch {
    setRuntimeSummaryText("Backend offline", "offline");
  }
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
    <article class="chat-row ${escapeHtml(roleClass)}">
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
    const optionsHtml = options
      .map((raw) => {
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

  const statsHtml = stats.length || statsRefreshButton
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
    const popupTitle = String(item?.settings_title || `${title} Settings`).trim() || `${title} Settings`;
    const pageIndexRaw = Number(renderOptions?.page_index ?? renderOptions?.pageIndex ?? 0);
    const pageIndex = Number.isFinite(pageIndexRaw) ? Math.max(0, Math.floor(pageIndexRaw)) : 0;
    const pageAttr = pageIndex > 0 ? ` data-core-page-index="${pageIndex}"` : "";
    const pageStyle = pageIndex > 1 ? ` style="display:none;"` : "";

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
            hasPopupSettingsBtn
              ? `<button type="button" class="action-btn core-manager-settings">${escapeHtml(
                  String(item?.settings_label || itemFieldsPopupLabel)
                )}</button>`
              : ""
          }
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
        data-core-item-popup-title="${escapeHtml(popupTitle)}"${pageAttr}${pageStyle}>
        <div class="card-head">
          <h3 class="card-title">${escapeHtml(title)}</h3>
          <span class="small">${safeCoreKey}</span>
        </div>
        ${subtitle ? `<div class="small">${escapeHtml(subtitle)}</div>` : ""}
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
        <section class="core-top-tab-panel" data-core-tab-panel="${escapeHtml(tab.core_key || "")}">
          ${renderCoreTabPayload(tab.payload, tab)}
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

function activateCoreTopTab(tabName) {
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
    button.addEventListener("click", () => activateCoreTopTab(button.dataset.coreTab));
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

  const tabsData = await api("/api/cores/tabs");
  const tabs = Array.isArray(tabsData?.tabs) ? tabsData.tabs : [];
  const spec = tabs.find((entry) => String(entry?.core_key || "").trim() === targetTab);
  const panel = Array.from(document.querySelectorAll(".core-top-tab-panel[data-core-tab-panel]")).find(
    (entry) => String(entry?.dataset?.coreTabPanel || "").trim() === targetTab
  );
  if (!panel) {
    return;
  }
  if (!spec) {
    panel.innerHTML = renderNotice("This core tab is no longer available.");
    return;
  }

  panel.innerHTML = renderCoreTabPayload(spec.payload, spec);
  bindCoreTabManagers();
  activateCoreTopTab(targetTab);
}

function bindCoreTabManagers() {
  bindCoreManagerTabs();
  bindCoreManagerSubtabs();
  bindCoreManagerSelectors();
  bindCoreManagerPagination();
  bindCoreManagerConditionalFields();
  bindCoreManagerDependentSelects();
  document.querySelectorAll(".core-tab-refresh-btn[data-core-tab-refresh]").forEach((button) => {
    if (!(button instanceof HTMLButtonElement)) {
      return;
    }
    if (button.dataset.coreTabRefreshBound === "1") {
      return;
    }
    button.dataset.coreTabRefreshBound = "1";
    button.addEventListener("click", async () => {
      const targetTab = String(getActiveCoreTopTab() || "").trim();
      if (!targetTab || targetTab === "manage" || button.disabled) {
        return;
      }
      const originalText = String(button.textContent || "Refresh");
      button.disabled = true;
      button.textContent = "Refreshing...";
      try {
        await refreshCoreTabInPlace(targetTab);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error || "Refresh failed");
        state.notice = `Refresh failed: ${message}`;
        renderNoticeBar();
        button.disabled = false;
        button.textContent = originalText;
      }
    });
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
          () => runCoreTabAction(coreKey, action, { ...values, values })
        );
        await refreshCoreTabInPlace(activeTab);
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
                () => runCoreTabAction(coreKey, action, { id: itemId, values })
              );
              await refreshCoreTabInPlace(activeTab);
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
          () => runCoreTabAction(coreKey, action, { id: itemId, values })
        );
        await refreshCoreTabInPlace(activeTab);
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
          () => runCoreTabAction(coreKey, action, { id: itemId })
        );
        await refreshCoreTabInPlace(activeTab);
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
        const result = await runActionWithProgress(
          {
            title: "Running core item",
            detail: itemId || coreKey,
            workingText: "Queueing run now...",
            successText: "Queued.",
            errorPrefix: "Core manager run failed",
          },
          () => runCoreTabAction(coreKey, action, { id: itemId })
        );
        await refreshCoreTabInPlace(activeTab);
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
              <span class="chat-composer-icon" aria-hidden="true">📎</span>
            </label>
            <textarea
              id="chat-input"
              class="chat-composer-input"
              rows="1"
              placeholder="${escapeHtml(`Message ${getTaterFullName()}...`)}"
            ></textarea>
            <button
              type="button"
              id="clear-chat-files"
              class="chat-composer-btn chat-composer-clear"
              style="display:none;"
              title="Clear attached files"
              aria-label="Clear attached files"
            >
              <span class="chat-composer-icon chat-composer-clear-plus" aria-hidden="true">＋</span>
            </button>
            <button type="button" id="send-chat" class="chat-composer-send" title="Send message" aria-label="Send message">
              <span class="chat-composer-icon chat-composer-send-arrow" aria-hidden="true">➤</span>
            </button>
          </div>
          <div id="chat-files-meta" class="small chat-files-meta" style="display:none;"></div>
        </div>
      </div>
    </div>
  `;

  const chatLog = document.getElementById("chat-log");
  const speedStatsEl = document.getElementById("chat-speed-stats");
  const status = document.getElementById("chat-status");
  const chatFilesEl = document.getElementById("chat-files");
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

  const updatePendingFilesUi = () => {
    if (!chatFilesMetaEl) {
      return;
    }
    if (!pendingFiles.length) {
      chatFilesMetaEl.textContent = "";
      chatFilesMetaEl.style.display = "none";
      if (clearChatFilesBtn) {
        clearChatFilesBtn.style.display = "none";
      }
      return;
    }
    const names = pendingFiles
      .slice(0, 3)
      .map((file) => String(file?.name || "").trim())
      .filter(Boolean);
    const extra = pendingFiles.length > names.length ? ` +${pendingFiles.length - names.length} more` : "";
    chatFilesMetaEl.textContent = `${pendingFiles.length} file(s): ${names.join(", ")}${extra}`;
    chatFilesMetaEl.style.display = "block";
    if (clearChatFilesBtn) {
      clearChatFilesBtn.style.display = "inline-flex";
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
    const existingLog = String(chatLog.innerHTML || "").trim();
    if (!existingLog || existingLog.includes('class="notice"')) {
      chatLog.innerHTML = "";
    }
    const html = messages.map(renderChatMessage).join("");
    chatLog.insertAdjacentHTML("beforeend", html);
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
      } catch {
        _setChatLiveStatus();
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

async function loadSettingsView() {
  const root = document.getElementById("view-root");
  const [settings, redisStatusPayload, redisEncryptionPayload] = await Promise.all([
    api("/api/settings"),
    api("/api/redis/status"),
    api("/api/redis/encryption/status"),
  ]);
  const redisStatus = _setRedisStatus(redisStatusPayload);
  const normalizeRedisEncryptionStatus = (raw) => {
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
      // Legacy snapshot fields (for older backends / compatibility).
      snapshot_exists: Boolean(next.snapshot_exists),
      snapshot_path: String(next.snapshot_path || ""),
      snapshot_size_bytes: Number(next.snapshot_size_bytes || 0),
      snapshot_modified: String(next.snapshot_modified || ""),
      error: String(next.error || ""),
    };
  };
  const redisEncryptionStatus = normalizeRedisEncryptionStatus(redisEncryptionPayload);
  let webuiPasswordIsSet = Boolean(settings?.webui_password_set);
  const summarizeRedisEncryptionStatus = (entry) => {
    const status = normalizeRedisEncryptionStatus(entry);
    if (!status.encryption_available) {
      return {
        status,
        parts: [
          {
            text: status.error ? `Encryption unavailable: ${status.error}` : "Encryption unavailable.",
            tone: "neutral",
          },
        ],
      };
    }
    const parts = [];
    parts.push({ text: status.key_exists ? "Key ready" : "Key not initialized", tone: "neutral" });
    parts.push({
      text: status.live_encryption_enabled ? "Live encryption: enabled" : "Live encryption: disabled",
      tone: status.live_encryption_enabled ? "live-enabled" : "live-disabled",
    });
    if (status.key_fingerprint) {
      parts.push({ text: `Key fingerprint: ${status.key_fingerprint}`, tone: "neutral" });
    }
    if (status.live_encryption_updated) {
      parts.push({ text: `Mode updated: ${status.live_encryption_updated}`, tone: "neutral" });
    }
    if (status.error) {
      parts.push({ text: `Warning: ${status.error}`, tone: "neutral" });
    }
    return { status, parts };
  };
  const renderRedisEncryptionStatusHtml = (entry) => {
    const summary = summarizeRedisEncryptionStatus(entry);
    const parts = Array.isArray(summary?.parts) ? summary.parts : [];
    if (!parts.length) {
      return "";
    }
    return parts
      .map((part) => {
        const text = escapeHtml(String(part?.text || ""));
        if (part?.tone === "live-enabled") {
          return `<span class="redis-live-state-enabled">${text}</span>`;
        }
        if (part?.tone === "live-disabled") {
          return `<span class="redis-live-state-disabled">${text}</span>`;
        }
        return text;
      })
      .join(" • ");
  };
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

  root.innerHTML = `${consumeNoticeHtml()}
    <div class="card">
      <div class="card-head">
        <h3 class="card-title">Settings</h3>
      </div>
      <div class="small">Categories: General, Hydra, Integrations, Emoji, Compotato, Redis, Advanced.</div>
      <div id="settings-status" class="small" style="margin-top: 6px;"></div>

      <div class="settings-tabs">
        <button type="button" class="settings-tab-btn active" data-settings-tab="general">General</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="hydra">Hydra</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="integrations">Integrations</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="emoji">Emoji</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="compozr">Compotato</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="redis">Redis</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="advanced">Advanced</button>
      </div>

      <form id="settings-form">
        <section class="settings-tab-panel active" data-settings-panel="general">
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

            <div class="settings-section-title">WebUI Login</div>
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

            <div class="settings-section-title">Chat Avatars</div>
            <div class="settings-avatar-grid" style="grid-column: 1 / -1;">
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
            <div class="inline-row" style="grid-column: 1 / -1;">
              <button type="button" id="settings-save-general" class="action-btn">Save Settings</button>
              <span class="small">Saves General and non-model settings.</span>
            </div>
          </div>
        </section>

        <section class="settings-tab-panel" data-settings-panel="integrations">
          <div class="form-grid two-col">
            <div class="settings-section-title">Web Search</div>
            <label>Google API Key
              <input id="set_web_search_google_api_key" type="password" value="${escapeHtml(settings.web_search_google_api_key || "")}" />
            </label>
            <label>Google Search CX
              <input id="set_web_search_google_cx" type="text" value="${escapeHtml(settings.web_search_google_cx || "")}" />
            </label>

            <div class="settings-section-title">Home Assistant</div>
            <label>Base URL
              <input id="set_homeassistant_base_url" type="text" value="${escapeHtml(settings.homeassistant_base_url || "http://homeassistant.local:8123")}" />
            </label>
            <label>Long-Lived Access Token
              <input id="set_homeassistant_token" type="password" value="${escapeHtml(settings.homeassistant_token || "")}" />
            </label>

            <div class="settings-section-title">UniFi Network</div>
            <label>Console Base URL
              <input id="set_unifi_network_base_url" type="text" value="${escapeHtml(settings.unifi_network_base_url || "https://10.4.20.1")}" />
            </label>
            <label>API Key
              <input id="set_unifi_network_api_key" type="password" value="${escapeHtml(settings.unifi_network_api_key || "")}" />
            </label>

            <div class="settings-section-title">UniFi Protect</div>
            <label>Console Base URL
              <input id="set_unifi_protect_base_url" type="text" value="${escapeHtml(settings.unifi_protect_base_url || "https://10.4.20.127")}" />
            </label>
            <label>API Key
              <input id="set_unifi_protect_api_key" type="password" value="${escapeHtml(settings.unifi_protect_api_key || "")}" />
            </label>

            <div class="settings-section-title">Vision</div>
            <label>Vision API Base URL
              <input id="set_vision_api_base" type="text" value="${escapeHtml(settings.vision_api_base || "http://127.0.0.1:1234")}" />
            </label>
            <label>Vision Model
              <input id="set_vision_model" type="text" value="${escapeHtml(settings.vision_model || "qwen2.5-vl-7b-instruct")}" />
            </label>
            <label style="grid-column: 1 / -1;">Vision API Key (optional)
              <input id="set_vision_api_key" type="password" value="${escapeHtml(settings.vision_api_key || "")}" />
            </label>
            <div class="inline-row" style="grid-column: 1 / -1;">
              <button type="button" id="settings-save-integrations" class="action-btn">Save Settings</button>
              <span class="small">Saves Integrations and non-model settings.</span>
            </div>
          </div>
        </section>

        <section class="settings-tab-panel" data-settings-panel="redis">
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

            <section class="core-inline-section">
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
                  ${renderRedisEncryptionStatusHtml(redisEncryptionStatus)}
                </div>
              </div>
            </section>
          </div>
        </section>

        <section class="settings-tab-panel" data-settings-panel="emoji">
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
            <div class="inline-row" style="grid-column: 1 / -1;">
              <button type="button" id="settings-save-emoji" class="action-btn">Save Settings</button>
              <span class="small">Saves Emoji and non-model settings.</span>
            </div>
          </div>
        </section>

        <section class="settings-tab-panel" data-settings-panel="hydra">
          <div class="settings-subtabs">
            <button type="button" class="settings-subtab-btn active" data-hydra-tab="settings">Hydra</button>
            <button type="button" class="settings-subtab-btn" data-hydra-tab="models">Hydra Models</button>
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
              <label>Agent State TTL Seconds (0 = no TTL)
                <input id="set_hydra_agent_state_ttl_seconds" type="number" min="0" value="${escapeHtml(
                  settings.hydra_agent_state_ttl_seconds ?? 1200
                )}" />
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
                <span class="small">Saves non-model settings. Use the Hydra Models tab for model routing settings.</span>
              </div>
            </div>
          </div>

          <div class="settings-subpanel" data-hydra-panel="models">
            <div class="form-grid two-col">
              <div class="settings-section-title">Hydra Models</div>
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
                  <div id="settings-hydra-base-title" class="hydra-model-panel-title">${
                    settings.hydra_beast_mode_enabled ? "AI Calls" : "Base Model"
                  }</div>
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
                <button type="button" id="settings-hydra-model-save" class="action-btn">Save Model</button>
              <span class="small">Saves Hydra model settings only (base model + Beast Mode role models).</span>
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

        <section class="settings-tab-panel" data-settings-panel="compozr">
          <div class="form-grid two-col">
            <div class="settings-section-title">Compotato Popup Effects</div>
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
            <div class="small">Applies to modal popups and toast popups when they appear and close.</div>
            <div class="inline-row" style="grid-column: 1 / -1;">
              <button type="button" id="settings-save-compozr" class="action-btn">Save Settings</button>
              <span class="small">Saves Compotato and non-model settings.</span>
            </div>
          </div>
        </section>

        <section class="settings-tab-panel" data-settings-panel="advanced">
          <div class="form-grid">
            <div class="settings-section-title">Admin Tool Gating</div>
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

            <div class="core-inline-section tone-danger">
              <div class="core-inline-section-title">Chat Data</div>
              <div class="inline-row">
                <button type="button" id="settings-clear-chat" class="inline-btn danger">Clear Chat History</button>
                <span class="small">Deletes all messages in the WebUI chat history.</span>
              </div>
            </div>
            <div class="inline-row">
              <button type="button" id="settings-save-advanced" class="action-btn">Save Settings</button>
              <span class="small">Saves Advanced and non-model settings.</span>
            </div>
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

  const activateTab = (tabKey) => {
    tabButtons.forEach((button) => {
      button.classList.toggle("active", button.dataset.settingsTab === tabKey);
    });
    tabPanels.forEach((panel) => {
      panel.classList.toggle("active", panel.dataset.settingsPanel === tabKey);
    });
  };

  tabButtons.forEach((button) => {
    button.addEventListener("click", () => activateTab(button.dataset.settingsTab));
  });

  let redisEncryptionState = normalizeRedisEncryptionStatus(redisEncryptionStatus);
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
    redisEncryptionState = normalizeRedisEncryptionStatus(raw);
    if (redisEncryptionKeyPathEl) {
      redisEncryptionKeyPathEl.value = String(redisEncryptionState.key_path || "");
    }
    if (redisEncryptionSnapshotPathEl) {
      redisEncryptionSnapshotPathEl.value = String(redisEncryptionState.live_encryption_state_path || "");
    }
    if (redisEncryptionStatusEl) {
      redisEncryptionStatusEl.innerHTML = renderRedisEncryptionStatusHtml(redisEncryptionState);
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
    const [nextStatus, nextEncryption] = await Promise.all([api("/api/redis/status"), api("/api/redis/encryption/status")]);
    applyRedisConnectionStatus(nextStatus);
    applyRedisEncryptionStatus(nextEncryption);
  };

  applyRedisConnectionStatus(redisStatus);
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
    statusEl.textContent = "Refreshing Redis status...";
    try {
      await refreshRedisSection();
      statusEl.textContent = "Redis status refreshed.";
    } catch (error) {
      statusEl.textContent = `Redis refresh failed: ${error.message}`;
    } finally {
      setRedisBusy(false);
    }
  });
  redisTestBtnEl?.addEventListener("click", async () => {
    setRedisBusy(true);
    statusEl.textContent = "Testing Redis connection...";
    try {
      const result = await api("/api/redis/configure", {
        method: "POST",
        body: JSON.stringify(redisFormPayload(true)),
      });
      applyRedisConnectionStatus(result);
      statusEl.textContent = "Redis connection test succeeded.";
    } catch (error) {
      statusEl.textContent = `Redis test failed: ${error.message}`;
    } finally {
      setRedisBusy(false);
    }
  });
  redisSaveBtnEl?.addEventListener("click", async () => {
    setRedisBusy(true);
    statusEl.textContent = "Saving Redis settings...";
    try {
      const result = await api("/api/redis/configure", {
        method: "POST",
        body: JSON.stringify(redisFormPayload(false)),
      });
      applyRedisConnectionStatus(result);
      const replay = result?.bootstrap_replay && typeof result.bootstrap_replay === "object" ? result.bootstrap_replay : null;
      if (replay && replay.ok === false) {
        statusEl.textContent = `Redis saved, but startup replay failed: ${String(replay.error || "unknown error")}`;
      } else if (replay) {
        statusEl.textContent = `Redis saved. Startup replay complete (restore: ${
          replay.ran_restore ? "ran" : "skipped"
        }, autostart: ${replay.ran_autostart ? "ran" : "skipped"}).`;
      } else {
        statusEl.textContent = "Redis settings saved.";
      }
      await refreshHealth();
    } catch (error) {
      statusEl.textContent = `Redis save failed: ${error.message}`;
    } finally {
      setRedisBusy(false);
    }
  });
  redisEncryptionEncryptBtnEl?.addEventListener("click", async () => {
    setRedisBusy(true);
    statusEl.textContent = "Encrypting live Redis values (auto-creating key if needed)...";
    try {
      const result = await runActionWithProgress(
        {
          title: "Encrypting Redis",
          detail: "Pausing active runtimes and encrypting Redis values in place",
          workingText: "Encrypting live Redis values...",
          successText: "Redis encryption complete.",
          errorPrefix: "Redis encryption failed",
        },
        () => api("/api/redis/encryption/encrypt", { method: "POST" })
      );
      applyRedisEncryptionStatus(result?.encryption_status || (await api("/api/redis/encryption/status")));
      const keySuffix = result?.key_created ? " New encryption key generated." : "";
      statusEl.textContent = `Encrypted ${Number(result?.keys_encrypted || 0)} Redis value(s); live encryption enabled.${keySuffix}`;
    } catch (error) {
      statusEl.textContent = `Redis encryption failed: ${error.message}`;
    } finally {
      setRedisBusy(false);
    }
  });
  redisEncryptionDecryptBtnEl?.addEventListener("click", async () => {
    if (!window.confirm("Decrypt live Redis values now? This switches future writes back to plaintext.")) {
      return;
    }
    setRedisBusy(true);
    statusEl.textContent = "Decrypting live Redis values...";
    try {
      const result = await runActionWithProgress(
        {
          title: "Decrypting Redis",
          detail: "Pausing active runtimes and decrypting Redis values in place",
          workingText: "Decrypting live Redis values...",
          successText: "Redis decryption complete.",
          errorPrefix: "Redis decrypt failed",
        },
        () => api("/api/redis/encryption/decrypt", { method: "POST" })
      );
      applyRedisEncryptionStatus(result?.encryption_status || (await api("/api/redis/encryption/status")));
      await refreshRedisSection();
      await refreshHealth();
      const replay = result?.bootstrap_replay && typeof result.bootstrap_replay === "object" ? result.bootstrap_replay : null;
      if (replay && replay.ok === false) {
        statusEl.textContent = `Decrypted ${Number(result?.restored_keys || 0)} Redis value(s), but startup replay failed: ${
          replay.error || "unknown error"
        }`;
      } else {
        statusEl.textContent = `Decrypted ${Number(result?.restored_keys || 0)} Redis value(s); live encryption disabled.`;
      }
    } catch (error) {
      statusEl.textContent = `Redis decrypt failed: ${error.message}`;
    } finally {
      setRedisBusy(false);
    }
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
      ["set_hydra_agent_state_ttl_seconds", "hydra_agent_state_ttl_seconds"],
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
  const hydraBaseTitleEl = document.getElementById("settings-hydra-base-title");
  const hydraBeastFieldsEl = document.getElementById("settings-hydra-beast-fields");
  const applyHydraBeastVisibility = () => {
    if (!hydraBaseFieldsEl || !hydraBeastFieldsEl || !hydraBeastToggleEl) {
      return;
    }
    const beastEnabled = Boolean(hydraBeastToggleEl.checked);
    hydraBaseFieldsEl.classList.add("is-active");
    hydraBeastFieldsEl.classList.toggle("is-active", beastEnabled);
    if (hydraBaseTitleEl) {
      hydraBaseTitleEl.textContent = beastEnabled ? "AI Calls" : "Base Model";
    }
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

  document.getElementById("settings-hydra-model-save").addEventListener("click", async () => {
    const baseHost = String(document.getElementById("set_hydra_llm_host")?.value || "").trim();
    const basePort = String(document.getElementById("set_hydra_llm_port")?.value || "").trim();
    const baseModel = String(document.getElementById("set_hydra_llm_model")?.value || "").trim();
    const additionalBaseRows = readHydraAdditionalBaseRows();
    const hydraBaseServersPayload = [normalizeHydraBaseRowInput({ host: baseHost, port: basePort, model: baseModel })];
    additionalBaseRows.forEach((row) => hydraBaseServersPayload.push(normalizeHydraBaseRowInput(row)));
    const payload = {
      hydra_llm_host: baseHost,
      hydra_llm_port: basePort,
      hydra_llm_model: baseModel,
      hydra_base_servers: hydraBaseServersPayload,
      hydra_beast_mode_enabled: Boolean(document.getElementById("set_hydra_beast_mode_enabled")?.checked),
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
    statusEl.textContent = "Saving Hydra model settings...";
    try {
      await api("/api/settings", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      statusEl.textContent = "Hydra model settings saved.";
      showToast("Hydra model settings saved.");
    } catch (error) {
      statusEl.textContent = `Model save failed: ${error.message}`;
    }
  });

  const hydraSubtabButtons = Array.from(root.querySelectorAll(".settings-subtab-btn"));
  const hydraSubPanels = Array.from(root.querySelectorAll(".settings-subpanel"));
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
      hydra_agent_state_ttl_seconds: Number(
        document.getElementById("set_hydra_agent_state_ttl_seconds").value || 1200
      ),
      hydra_max_ledger_items: Number(document.getElementById("set_hydra_max_ledger_items").value || 1500),
      hydra_step_retry_limit: Number(document.getElementById("set_hydra_step_retry_limit").value || 1),
      hydra_astraeus_plan_review_enabled: document.getElementById(
        "set_hydra_astraeus_plan_review_enabled"
      ).checked,
      popup_effect_style: normalizePopupEffectStyle(document.getElementById("set_popup_effect_style")?.value || "flame"),
      admin_only_plugins: adminOnlyPlugins,
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

      await api("/api/settings", {
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
    "settings-save-emoji",
    "settings-save-compozr",
    "settings-save-advanced",
  ].forEach((buttonId) => {
    document.getElementById(buttonId)?.addEventListener("click", runSettingsSave);
  });
}

async function loadView(viewName) {
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
  await ensureWebuiAuth();
  await ensureRedisSetup();
  await refreshBranding();
  await refreshHealth();
  await loadView(state.view);

  setInterval(refreshHealth, 8000);
}

window.addEventListener("beforeunload", () => {
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
