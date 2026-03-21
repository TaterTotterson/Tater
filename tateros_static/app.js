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
  runtimeSettingsSaveHandler: null,
  runtimeSettingsCatalog: {
    verbas: {},
    portals: {},
    cores: {},
  },
  popupEffectStyle: String(safeStorageGet("tater_tateros_popup_effect_style", "flame")).trim().toLowerCase() || "flame",
  sending: false,
  activeChatJobId: "",
  chatEventSource: null,
  notice: "",
  chatProfile: {
    username: "User",
    userAvatar: "",
    taterAvatar: "",
    taterName: "Tater",
    taterFirstName: "Tater",
    taterLastName: "Totterson",
    taterFullName: "Tater Totterson",
    attachMaxMbEach: 25,
    attachMaxMbTotal: 50,
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
  const response = await fetch(withBasePath(path), {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });

  if (!response.ok) {
    let detail = "Request failed";
    try {
      const body = await response.json();
      detail = body.detail || detail;
    } catch {
      detail = response.statusText || detail;
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

function cerberusPlatformLabel(platform) {
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
  return `<label>${safeLabel}<input id="${inputId}" type="${htmlType}"${numberAttrs} value="${escapeHtml(field.value ?? "")}" data-setting-type="${escapeHtml(type)}" data-setting-key="${safeKey}" />${safeDesc}</label>`;
}

function getInputValue(input) {
  const type = input.dataset.settingType || input.type;
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
    state.chatProfile.attachMaxMbEach = Number(profile.attach_max_mb_each || state.chatProfile.attachMaxMbEach || 25);
    state.chatProfile.attachMaxMbTotal = Number(profile.attach_max_mb_total || state.chatProfile.attachMaxMbTotal || 50);
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
  const chatJobsActive = Number(health?.chat_jobs_active ?? 0);
  return `${verbasEnabled} verba enabled • ${portalsRunning} portals running • ${coresRunning} cores running • ${chatJobsActive} chat jobs`;
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

function _renderRuntimeChatJobRows(chatJobs) {
  const byPlatform = Array.isArray(chatJobs?.by_platform) ? chatJobs.by_platform : [];
  const activeTurns = Array.isArray(chatJobs?.active_turns) ? chatJobs.active_turns : [];
  const history = chatJobs?.history && typeof chatJobs.history === "object" ? chatJobs.history : {};
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
        <div class="runtime-breakdown-list">
          ${activeTurns
            .map((row) => {
              const platformLabel = String(row?.platform_label || row?.platform || "Unknown");
              const source = String(row?.source || "").trim();
              const scope = String(row?.scope || "").trim();
              const age = _runtimeAgeLabel(row?.age_seconds);
              const detailParts = [source, scope].filter(Boolean);
              return `
                <div class="runtime-breakdown-row compact">
                  <div class="runtime-breakdown-main">
                    <div class="runtime-breakdown-name">${escapeHtml(platformLabel)}</div>
                    <div class="small muted">${escapeHtml(detailParts.join(" • "))}</div>
                  </div>
                  <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(age)}</span></div>
                </div>
              `;
            })
            .join("")}
        </div>
      `
    : `<div class="small muted">No active chat turns right now.</div>`;

  const historyHtml = historyWindows.length
    ? `
        <div class="runtime-breakdown-list">
          ${historyWindows
            .map((windowRow) => {
              const jobs = Number(windowRow?.jobs ?? 0);
              const done = Number(windowRow?.done ?? 0);
              const blocked = Number(windowRow?.blocked ?? 0);
              const failed = Number(windowRow?.failed ?? 0);
              const topPlatforms = Array.isArray(windowRow?.top_platforms) ? windowRow.top_platforms : [];
              const platformLine = topPlatforms.length
                ? topPlatforms
                    .map((row) => `${String(row?.label || row?.platform || "Unknown")}: ${Number(row?.jobs ?? 0)}`)
                    .join(" • ")
                : "No jobs in this period.";
              return `
                <div class="runtime-breakdown-row">
                  <div class="runtime-breakdown-main">
                    <div class="runtime-breakdown-name">${escapeHtml(String(windowRow?.label || "Window"))}</div>
                    <div class="small muted">Done ${done} • Blocked ${blocked} • Failed ${failed}</div>
                    <div class="small muted">${escapeHtml(platformLine)}</div>
                  </div>
                  <div class="runtime-breakdown-status"><span class="status-chip running">${escapeHtml(`${jobs} jobs`)}</span></div>
                </div>
              `;
            })
            .join("")}
        </div>
      `
    : `<div class="small muted">No history available yet.</div>`;

  return `
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle">By Platform</div>
      ${platformRowsHtml}
    </div>
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle">Active Turns</div>
      ${activeTurnsHtml}
    </div>
    <div class="runtime-breakdown-block">
      <div class="runtime-breakdown-subtitle">History</div>
      ${historyHtml}
      <div class="small muted">Sample size: ${escapeHtml(String(Number(history?.sample_size ?? 0)))} ledger rows</div>
    </div>
  `;
}

function renderRuntimeBreakdown(payload) {
  const chatJobs = payload?.chat_jobs && typeof payload.chat_jobs === "object" ? payload.chat_jobs : {};
  const summary = `${Number(chatJobs.total ?? 0)} total • WebUI queue ${Number(chatJobs.webui_jobs ?? 0)} • Surface turns ${Number(chatJobs.surface_running_turns ?? 0)}`;
  return `
    <section class="runtime-breakdown-card">
      <div class="runtime-breakdown-head">
        <h4 class="runtime-breakdown-title">Chat Jobs</h4>
        <div class="small muted">${escapeHtml(summary)}</div>
      </div>
      ${_renderRuntimeChatJobRows(chatJobs)}
    </section>
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
        <div class="cerb-modal-dialog card runtime-breakdown-dialog" role="dialog" aria-modal="true" aria-label="Chat Jobs">
          <div class="card-head">
            <h3 class="card-title">Live Chat Jobs</h3>
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
  summary.title = "Open live chat jobs";
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
    if (health.ok === false) {
      setRuntimeSummaryText("Backend degraded", "degraded");
      return;
    }
    setRuntimeSummaryText(formatRuntimeSummary(health), "normal");
  } catch {
    setRuntimeSummaryText("Backend offline", "offline");
  }
}

function closeChatEventSource() {
  if (state.chatEventSource) {
    state.chatEventSource.close();
    state.chatEventSource = null;
  }
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
    ? `
      <div class="card">
        <div class="card-head"><h3 class="card-title">${escapeHtml(runtimeTitle)}</h3></div>
        <div class="form-grid shop-grid">${runtimeHtml}</div>
      </div>
    `
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
      return `
        <label>${escapeHtml(label)}
          <div class="small">No table columns configured.</div>
          ${descHtml}
        </label>
      `;
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

    return `
      <label>${escapeHtml(label)}
        <div class="core-data-table-wrap">
          <table class="core-data-table">
            <thead><tr>${headHtml}</tr></thead>
            <tbody>${bodyHtml}</tbody>
          </table>
        </div>
        ${descHtml}
      </label>
    `;
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

    return `
      <label>${escapeHtml(label)}
        <div class="core-bar-chart">${chartHtml}</div>
        ${descHtml}
      </label>
    `;
  }

  if (type === "checkbox") {
    const checked = boolFromAny(field?.value, false) ? "checked" : "";
    return `
      <label>
        ${escapeHtml(label)}
        ${renderToggleRow(
          `<input class="toggle-input" type="checkbox" data-core-field-key="${escapeHtml(
            key
          )}" data-core-field-type="checkbox" ${checked} />`
        )}
        ${descHtml}
      </label>
    `;
  }

  if (type === "select") {
    const options = Array.isArray(field?.options) ? field.options : [];
    const selected = String(field?.value ?? "");
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
    return `
      <label>${escapeHtml(label)}
        <select data-core-field-key="${escapeHtml(key)}" data-core-field-type="select">${optionsHtml}</select>
        ${descHtml}
      </label>
    `;
  }

  if (type === "textarea" || type === "multiline") {
    return `
      <label>${escapeHtml(label)}
        <textarea data-core-field-key="${escapeHtml(key)}" data-core-field-type="textarea" ${placeholderAttr}>${escapeHtml(
          field?.value ?? ""
        )}</textarea>
        ${descHtml}
      </label>
    `;
  }

  const htmlType = type === "password" ? "password" : type === "number" ? "number" : "text";
  const numberAttrs =
    type === "number"
      ? ` step="${escapeHtml(field?.step ?? "any")}"${
          field?.min !== undefined ? ` min="${escapeHtml(field.min)}"` : ""
        }${field?.max !== undefined ? ` max="${escapeHtml(field.max)}"` : ""}`
      : "";
  return `
    <label>${escapeHtml(label)}
      <input type="${htmlType}"${numberAttrs} value="${escapeHtml(field?.value ?? "")}" ${placeholderAttr} data-core-field-key="${escapeHtml(
    key
  )}" data-core-field-type="${escapeHtml(type)}" />
      ${descHtml}
    </label>
  `;
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

  function renderCoreManagerItemCard(item) {
    const itemId = String(item?.id || "").trim();
    const encodedId = escapeHtml(encodeCoreManagerId(itemId));
    const title = String(item?.title || itemId || "(item)").trim() || "(item)";
    const subtitle = String(item?.subtitle || "").trim();
    const itemGroup = String(item?.group || "").trim().toLowerCase();
    const itemFields = Array.isArray(item?.fields) ? item.fields : [];
    const sections = Array.isArray(item?.sections) ? item.sections : [];
    const saveAction = String(item?.save_action || "").trim();
    const removeAction = String(item?.remove_action || "").trim();
    const removeConfirm = String(item?.remove_confirm || "Remove this item?").trim();
    const itemFieldContent = itemFields.map((field) => renderCoreManagerField(field)).join("");

          const sectionHtml = sections
            .map((section) => {
              const sectionLabel = String(section?.label || "Section").trim() || "Section";
              const fields = Array.isArray(section?.fields) ? section.fields : [];
              const inline = itemSectionsInDropdown || boolFromAny(section?.inline, false);
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
    if (itemSectionsInDropdown && sectionHtml) {
      dropdownContentParts.push(sectionHtml);
    }
    const dropdownContentHtml = dropdownContentParts.join("");

    const itemFieldsHtml = itemFieldsDropdown
      ? dropdownContentHtml
        ? `
          <details class="settings-dropdown">
            <summary class="settings-summary">${escapeHtml(itemFieldsDropdownLabel)}</summary>
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

    return `
      <article class="card core-manager-item"
        data-core-key="${safeCoreKey}"
        data-core-item-id="${encodedId}"
        data-core-item-group="${escapeHtml(itemGroup)}"
        data-core-save-action="${escapeHtml(saveAction)}"
        data-core-remove-action="${escapeHtml(removeAction)}"
        data-core-remove-confirm="${escapeHtml(removeConfirm)}">
        <div class="card-head">
          <h3 class="card-title">${escapeHtml(title)}</h3>
          <span class="small">${safeCoreKey}</span>
        </div>
        ${subtitle ? `<div class="small">${escapeHtml(subtitle)}</div>` : ""}
        ${itemFieldsHtml}
        ${itemSectionsInDropdown ? "" : sectionHtml}
        <div class="inline-row" style="margin-top:10px;">
          ${
            saveAction
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
          <span class="small core-manager-status"></span>
        </div>
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

    if (!rows.length) {
      return renderNotice(sectionEmptyMessage);
    }

    if (!selector || rows.length <= 1) {
      return `<div class="core-tab-items" style="margin-top:10px;">${rows.map((item) => renderCoreManagerItemCard(item)).join("")}</div>`;
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
      <button class="core-top-tab-btn active" data-core-tab="manage">${safeManageLabel}</button>
      ${dynamicButtons}
    </div>
    <section class="core-top-tab-panel active" data-core-tab-panel="manage">
      ${manageHtml}
    </section>
    ${dynamicPanels}
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

function setCoreManagerStatus(host, text) {
  const statusEl = host?.querySelector(".core-manager-status");
  if (statusEl) {
    statusEl.textContent = String(text || "");
  }
}

function collectCoreManagerValues(host) {
  const values = {};
  host.querySelectorAll("[data-core-field-key]").forEach((input) => {
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
        title: `${entry.label} Settings`,
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
        title: `${entry.label} Settings`,
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
          <div id="chat-files-meta" class="small chat-files-meta">No files selected.</div>
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
      const eachMb = Number(state.chatProfile.attachMaxMbEach || 25);
      const totalMb = Number(state.chatProfile.attachMaxMbTotal || 50);
      chatFilesMetaEl.textContent = `No files selected. Limits: ${eachMb}MB each, ${totalMb}MB total.`;
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
        attachMaxMbEach: Number(profile.attach_max_mb_each || state.chatProfile.attachMaxMbEach || 25),
        attachMaxMbTotal: Number(profile.attach_max_mb_total || state.chatProfile.attachMaxMbTotal || 50),
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
        attachMaxMbEach: Number(state.chatProfile.attachMaxMbEach || 25),
        attachMaxMbTotal: Number(state.chatProfile.attachMaxMbTotal || 50),
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

  let chatPollToken = 0;
  let chatPollTimer = 0;

  const stopChatJobPolling = () => {
    chatPollToken += 1;
    if (chatPollTimer) {
      window.clearTimeout(chatPollTimer);
      chatPollTimer = 0;
    }
  };

  const finalizeChatJob = async ({ jobId, statusText, responses = [] }) => {
    if (state.activeChatJobId !== jobId) {
      return;
    }
    status.textContent = String(statusText || "");
    state.sending = false;
    state.activeChatJobId = "";
    stopChatJobPolling();
    closeChatEventSource();
    const inlineRendered = appendAssistantResponses(responses);
    if (inlineRendered) {
      await refreshChatSpeedStats();
    } else {
      await refreshChatHistory();
    }
    await refreshHealth();
  };

  const scheduleChatJobPoll = (jobId, delayMs = 1200) => {
    const token = chatPollToken;
    chatPollTimer = window.setTimeout(async () => {
      if (token !== chatPollToken) {
        return;
      }
      if (state.view !== "chat" || state.activeChatJobId !== jobId) {
        return;
      }

      try {
        const snapshot = await api(`/api/chat/jobs/${encodeURIComponent(jobId)}`);
        const snapshotStatus = String(snapshot.status || "").trim().toLowerCase();
        if (snapshotStatus === "done") {
          await finalizeChatJob({
            jobId,
            statusText: "Complete.",
            responses: Array.isArray(snapshot.responses) ? snapshot.responses : [],
          });
          return;
        }
        if (snapshotStatus === "error") {
          await finalizeChatJob({
            jobId,
            statusText: `Job failed: ${snapshot.error || "unknown error"}`,
          });
          return;
        }

        const tool = String(snapshot.current_tool || "").trim();
        if (tool) {
          status.textContent = `Using ${tool}...`;
        } else if (snapshotStatus) {
          status.textContent = `Running (${snapshotStatus})...`;
        } else {
          status.textContent = "Waiting for response...";
        }
      } catch {
        status.textContent = "Waiting for response...";
      }

      scheduleChatJobPoll(jobId, 1200);
    }, Math.max(250, Number(delayMs) || 1200));
  };

  function attachJobStream(jobId) {
    closeChatEventSource();
    stopChatJobPolling();
    state.activeChatJobId = jobId;
    scheduleChatJobPoll(jobId, IS_HA_INGRESS ? 900 : 2000);

    if (typeof EventSource !== "function") {
      return;
    }

    const eventSource = new EventSource(withBasePath(`/api/chat/jobs/${encodeURIComponent(jobId)}/events`));
    state.chatEventSource = eventSource;

    eventSource.addEventListener("status", (event) => {
      const payload = safeJsonParse(event.data) || {};
      const st = String(payload.status || "running");
      const tool = String(payload.current_tool || "").trim();
      if (tool) {
        status.textContent = `Running (${st}) • ${tool}`;
      } else {
        status.textContent = `Running (${st})...`;
      }
    });

    eventSource.addEventListener("tool", (event) => {
      const payload = safeJsonParse(event.data) || {};
      const tool = String(payload.current_tool || "").trim() || "tool";
      status.textContent = `Using ${tool}...`;
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
        jobId,
        statusText: "Complete.",
        responses: Array.isArray(payload.responses) ? payload.responses : [],
      });
    });

    eventSource.addEventListener("job_error", async (event) => {
      const payload = safeJsonParse(event.data) || {};
      await finalizeChatJob({
        jobId,
        statusText: `Job failed: ${payload.error || "unknown error"}`,
      });
    });

    eventSource.onerror = () => {
      if (state.activeChatJobId !== jobId) {
        return;
      }
      // In some HA ingress/proxy setups SSE is unstable. Keep polling as the source of truth.
      closeChatEventSource();
      status.textContent = "Waiting for response...";
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
    if (state.sending) {
      return;
    }

    const message = String(chatInputEl.value || "").trim();
    if (!message && !pendingFiles.length) {
      status.textContent = "Enter a message or attach files first.";
      return;
    }

    state.sending = true;
    status.textContent = pendingFiles.length ? "Preparing attachments..." : "Queueing chat job...";

    try {
      const attachments = [];
      for (const file of pendingFiles) {
        attachments.push({
          name: String(file?.name || "attachment").trim() || "attachment",
          mimetype: String(file?.type || "application/octet-stream").trim() || "application/octet-stream",
          data_url: await readFileAsDataUrl(file),
        });
      }
      chatInputEl.value = "";
      autoSizeChatInput();
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
      if (!jobId) {
        throw new Error("Backend did not return a job id.");
      }

      pendingFiles = [];
      if (chatFilesEl) {
        chatFilesEl.value = "";
      }
      updatePendingFilesUi();

      await refreshChatHistory();
      status.textContent = "Job queued...";
      attachJobStream(jobId);
      await refreshHealth();
    } catch (error) {
      state.sending = false;
      status.textContent = `Chat failed: ${error.message}`;
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
      })}
    `;
  } else {
    const manageHtml = renderShopTabbedManager("cores", shopData, {
      runtimeHtml,
      runtimeTitle: "Core Runtime",
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
  const settings = await api("/api/settings");
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
  const cerberusDefaults =
    settings?.cerberus_defaults && typeof settings.cerberus_defaults === "object" ? settings.cerberus_defaults : {};
  const popupEffectStyle = normalizePopupEffectStyle(settings?.popup_effect_style || state.popupEffectStyle);
  applyPopupEffectStyle(popupEffectStyle);
  const cerberusPlatforms = ["webui", "discord", "irc", "telegram", "matrix", "homeassistant", "homekit", "xbmc", "automation"];
  const cerberusPlatformOptionsHtml = cerberusPlatforms
    .map((platform) => `<option value="${escapeHtml(platform)}">${escapeHtml(cerberusPlatformLabel(platform))}</option>`)
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
      <div class="small">Categories: General, Cerberus, Integrations, Emoji, Compotato, Advanced.</div>
      <div id="settings-status" class="small" style="margin-top: 6px;"></div>

      <div class="settings-tabs">
        <button type="button" class="settings-tab-btn active" data-settings-tab="general">General</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="cerberus">Cerberus</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="integrations">Integrations</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="emoji">Emoji</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="compozr">Compotato</button>
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
          </div>
        </section>

        <section class="settings-tab-panel" data-settings-panel="cerberus">
          <div class="settings-subtabs">
            <button type="button" class="settings-subtab-btn active" data-cerberus-tab="settings">Cerberus</button>
            <button type="button" class="settings-subtab-btn" data-cerberus-tab="metrics">Cerberus Metrics</button>
            <button type="button" class="settings-subtab-btn" data-cerberus-tab="data">Cerberus Data</button>
          </div>

          <div class="settings-subpanel active" data-cerberus-panel="settings">
            <div class="form-grid two-col">
              <label>Cerberus LLM Host / IP
                <input id="set_cerberus_llm_host" type="text" value="${escapeHtml(
                  settings.cerberus_llm_host || ""
                )}" />
              </label>
              <label>Cerberus LLM Port
                <input id="set_cerberus_llm_port" type="number" min="1" max="65535" value="${escapeHtml(
                  settings.cerberus_llm_port || ""
                )}" />
              </label>
              <label>Cerberus LLM Model
                <input id="set_cerberus_llm_model" type="text" value="${escapeHtml(
                  settings.cerberus_llm_model || ""
                )}" />
              </label>
              <div class="settings-section-title">Cerberus General</div>
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
                <input id="set_cerberus_agent_state_ttl_seconds" type="number" min="0" value="${escapeHtml(
                  settings.cerberus_agent_state_ttl_seconds ?? 604800
                )}" />
              </label>
              <label>Max Ledger Items
                <input id="set_cerberus_max_ledger_items" type="number" min="1" value="${escapeHtml(
                  settings.cerberus_max_ledger_items ?? 1500
                )}" />
              </label>
              <label>Step Retry Limit
                <input id="set_cerberus_step_retry_limit" type="number" min="1" max="10" value="${escapeHtml(
                  settings.cerberus_step_retry_limit ?? 1
                )}" />
              </label>
              <div class="inline-row" style="grid-column: 1 / -1;">
                <button type="button" id="settings-cerberus-defaults" class="inline-btn">Set Default Values</button>
                <span class="small">Applies default Cerberus values to the fields above.</span>
              </div>
            </div>
          </div>

          <div class="settings-subpanel" data-cerberus-panel="metrics">
            <div class="form-grid two-col">
              <label>Portal
                <select id="set_cerb_metrics_platform">
                  <option value="all">All</option>
                  ${cerberusPlatformOptionsHtml}
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

          <div class="settings-subpanel" data-cerberus-panel="data">
            <div class="form-grid two-col">
              <div class="settings-section-title">All Portals</div>
              <div class="inline-row" style="grid-column: 1 / -1;">
                <button type="button" id="settings-cerb-clear-all" class="inline-btn danger">Clear All Cerberus Data</button>
              </div>

              <div class="settings-section-title">Per-Portal Data</div>
              <label>Portal
                <select id="set_cerb_data_platform">
                  ${cerberusPlatformOptionsHtml}
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
          </div>
        </section>

        <div class="inline-row settings-actions">
          <button class="action-btn" type="submit">Save Settings</button>
          <span class="small">All tab values are saved together.</span>
        </div>
      </form>
    </div>
  `;

  const statusEl = document.getElementById("settings-status");
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

  document.getElementById("settings-cerberus-defaults").addEventListener("click", () => {
    const map = [
      ["set_cerberus_llm_host", "cerberus_llm_host"],
      ["set_cerberus_llm_port", "cerberus_llm_port"],
      ["set_cerberus_llm_model", "cerberus_llm_model"],
      ["set_cerberus_agent_state_ttl_seconds", "cerberus_agent_state_ttl_seconds"],
      ["set_cerberus_max_ledger_items", "cerberus_max_ledger_items"],
      ["set_cerberus_step_retry_limit", "cerberus_step_retry_limit"],
    ];
    map.forEach(([inputId, primaryKey]) => {
      const input = document.getElementById(inputId);
      if (!input) {
        return;
      }
      const hasPrimary = Object.prototype.hasOwnProperty.call(cerberusDefaults, primaryKey);
      if (!hasPrimary) {
        return;
      }
      const rawValue = cerberusDefaults[primaryKey];
      input.value = String(rawValue);
    });
    statusEl.textContent = "Cerberus defaults loaded into form. Click Save Settings to apply.";
  });

  const cerberusSubtabButtons = Array.from(root.querySelectorAll(".settings-subtab-btn"));
  const cerberusSubPanels = Array.from(root.querySelectorAll(".settings-subpanel"));
  const activateCerberusSubtab = (tabKey) => {
    cerberusSubtabButtons.forEach((button) => {
      button.classList.toggle("active", button.dataset.cerberusTab === tabKey);
    });
    cerberusSubPanels.forEach((panel) => {
      panel.classList.toggle("active", panel.dataset.cerberusPanel === tabKey);
    });
  };
  cerberusSubtabButtons.forEach((button) => {
    button.addEventListener("click", () => activateCerberusSubtab(button.dataset.cerberusTab));
  });

  const metricsStatusEl = document.getElementById("settings-cerb-metrics-status");
  const metricsContentEl = document.getElementById("settings-cerb-metrics-content");
  const metricsPlatformEl = document.getElementById("set_cerb_metrics_platform");
  const metricsLimitEl = document.getElementById("set_cerb_metrics_limit");
  const metricsOutcomeEl = document.getElementById("set_cerb_metrics_outcome");
  const metricsToolEl = document.getElementById("set_cerb_metrics_tool");
  const metricsToolsOnlyEl = document.getElementById("set_cerb_metrics_tools_only");
  let cerberusLedgerRows = [];

  const dataStatusEl = document.getElementById("settings-cerb-data-status");
  const dataContentEl = document.getElementById("settings-cerb-data-content");
  const dataPlatformEl = document.getElementById("set_cerb_data_platform");

  const ensureCerberusLedgerModal = () => {
    let modal = document.getElementById("cerb-ledger-modal");
    if (modal) {
      return modal;
    }
    document.body.insertAdjacentHTML(
      "beforeend",
      `
        <div id="cerb-ledger-modal" class="cerb-modal" aria-hidden="true">
          <div class="cerb-modal-dialog card" role="dialog" aria-modal="true" aria-label="Cerberus Ledger Entry">
            <div class="card-head">
              <h3 class="card-title">Cerberus Ledger Entry</h3>
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

  const openCerberusLedgerModal = (row) => {
    const modal = ensureCerberusLedgerModal();
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

  const renderCerberusLedgerTable = (rows, emptyMessage = "No ledger rows for this filter.") => {
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

  const renderCerberusMetricsPayload = (payload) => {
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
      platform: row?.platform_label || cerberusPlatformLabel(row?.platform || ""),
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
        `Selected Portal Counters (${payload?.selected_platform_label || cerberusPlatformLabel(payload?.selected_platform)})`,
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
        ${renderCerberusLedgerTable(ledgerRows, "No ledger rows for this filter.")}
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
    cerberusLedgerRows = ledgerRows;
    metricsContentEl.innerHTML = html;
    metricsContentEl.querySelectorAll(".cerb-ledger-open").forEach((button) => {
      button.addEventListener("click", () => {
        const idx = Number(button.dataset.ledgerIndex || "-1");
        if (!Number.isFinite(idx) || idx < 0 || idx >= cerberusLedgerRows.length) {
          return;
        }
        openCerberusLedgerModal(cerberusLedgerRows[idx]);
      });
    });
  };

  const fetchCerberusMetrics = async (toolValue) => {
    const params = new URLSearchParams();
    params.set("platform", String(metricsPlatformEl.value || "webui"));
    params.set("limit", String(Math.max(10, Math.min(300, Number(metricsLimitEl.value || 50)))));
    params.set("outcome", String(metricsOutcomeEl.value || "all"));
    params.set("tool", String(toolValue || "all"));
    params.set("show_only_tool_turns", metricsToolsOnlyEl.checked ? "true" : "false");
    return api(`/api/settings/cerberus/metrics?${params.toString()}`);
  };

  const refreshCerberusMetrics = async () => {
    metricsStatusEl.textContent = "Loading metrics...";
    try {
      const currentTool = String(metricsToolEl.value || "all");
      let payload = await fetchCerberusMetrics(currentTool);
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
        payload = await fetchCerberusMetrics(effectiveTool);
      }

      renderCerberusMetricsPayload(payload);
      const filtered = Number(payload?.ledger_filtered ?? 0);
      const total = Number(payload?.ledger_total ?? 0);
      metricsStatusEl.textContent = `Loaded ${filtered} of ${total} ledger rows.`;
    } catch (error) {
      metricsStatusEl.textContent = `Metrics load failed: ${error.message}`;
      metricsContentEl.innerHTML = renderNotice(`Failed to load Cerberus metrics: ${error.message}`);
    }
  };

  const refreshCerberusData = async () => {
    dataStatusEl.textContent = "Loading data...";
    try {
      const payload = await api("/api/settings/cerberus/data");
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

      dataStatusEl.textContent = "Cerberus data loaded.";
    } catch (error) {
      dataStatusEl.textContent = `Data load failed: ${error.message}`;
      dataContentEl.innerHTML = renderNotice(`Failed to load Cerberus data: ${error.message}`);
    }
  };

  const clearCerberusData = async ({ mode, platform, label }) => {
    if (!window.confirm(`Confirm ${label}?`)) {
      return;
    }
    dataStatusEl.textContent = "Running clear action...";
    try {
      const result = await api("/api/settings/cerberus/data/clear", {
        method: "POST",
        body: JSON.stringify({ mode, platform }),
      });
      dataStatusEl.textContent = `Cleared. Metrics removed: ${result.metrics_removed}. Ledger lists removed: ${result.ledger_removed}.`;
      await Promise.all([refreshCerberusData(), refreshCerberusMetrics()]);
    } catch (error) {
      dataStatusEl.textContent = `Clear failed: ${error.message}`;
    }
  };

  document.getElementById("settings-cerb-metrics-refresh").addEventListener("click", () => {
    refreshCerberusMetrics();
  });
  metricsPlatformEl.addEventListener("change", () => refreshCerberusMetrics());
  metricsOutcomeEl.addEventListener("change", () => refreshCerberusMetrics());
  metricsToolEl.addEventListener("change", () => refreshCerberusMetrics());
  metricsToolsOnlyEl.addEventListener("change", () => refreshCerberusMetrics());
  metricsLimitEl.addEventListener("change", () => refreshCerberusMetrics());

  document.getElementById("settings-cerb-clear-all").addEventListener("click", () => {
    clearCerberusData({
      mode: "all",
      platform: "all",
      label: "clearing all Cerberus metrics and ledger data across all portals",
    });
  });
  document.getElementById("settings-cerb-clear-platform-all").addEventListener("click", () => {
    clearCerberusData({
      mode: "all",
      platform: String(dataPlatformEl.value || "webui"),
      label: `clearing all Cerberus data for ${cerberusPlatformLabel(dataPlatformEl.value)}`,
    });
  });
  document.getElementById("settings-cerb-clear-platform-metrics").addEventListener("click", () => {
    clearCerberusData({
      mode: "metrics",
      platform: String(dataPlatformEl.value || "webui"),
      label: `resetting Cerberus metrics for ${cerberusPlatformLabel(dataPlatformEl.value)}`,
    });
  });
  document.getElementById("settings-cerb-clear-platform-ledger").addEventListener("click", () => {
    clearCerberusData({
      mode: "ledger",
      platform: String(dataPlatformEl.value || "webui"),
      label: `clearing Cerberus ledger for ${cerberusPlatformLabel(dataPlatformEl.value)}`,
    });
  });

  await Promise.all([refreshCerberusMetrics(), refreshCerberusData()]);

  document.getElementById("settings-admin-defaults").addEventListener("click", () => {
    const select = document.getElementById("set_admin_only_plugins");
    const values = adminDefaults;
    Array.from(select.options).forEach((option) => {
      option.selected = values.has(String(option.value || "").trim());
    });
    statusEl.textContent = "Default admin tool list loaded. Click Save Settings to apply.";
  });

  document.getElementById("settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const adminSelect = document.getElementById("set_admin_only_plugins");
    const adminOnlyPlugins = Array.from(adminSelect.selectedOptions)
      .map((option) => String(option.value || "").trim())
      .filter(Boolean);

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
      cerberus_llm_host: document.getElementById("set_cerberus_llm_host").value,
      cerberus_llm_port: document.getElementById("set_cerberus_llm_port").value,
      cerberus_llm_model: document.getElementById("set_cerberus_llm_model").value,
      cerberus_agent_state_ttl_seconds: Number(
        document.getElementById("set_cerberus_agent_state_ttl_seconds").value || 604800
      ),
      cerberus_max_ledger_items: Number(document.getElementById("set_cerberus_max_ledger_items").value || 1500),
      cerberus_step_retry_limit: Number(document.getElementById("set_cerberus_step_retry_limit").value || 1),
      popup_effect_style: normalizePopupEffectStyle(document.getElementById("set_popup_effect_style")?.value || "flame"),
      admin_only_plugins: adminOnlyPlugins,
    };

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
  });
}

async function loadView(viewName) {
  state.view = viewName;
  setActiveNav(viewName);
  updateHeader();

  const root = document.getElementById("view-root");
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
  await refreshBranding();
  await refreshHealth();
  await loadView(state.view);

  setInterval(refreshHealth, 8000);
}

window.addEventListener("beforeunload", () => {
  closeChatEventSource();
  stopRuntimeBreakdownPolling();
});

init().catch((error) => {
  const root = document.getElementById("view-root");
  if (root) {
    root.innerHTML = renderNotice(`Failed to initialize UI: ${error?.message || "unknown error"}`);
  }
});
