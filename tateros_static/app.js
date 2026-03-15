const state = {
  view: "chat",
  sessionId: localStorage.getItem("tater_tateros_session_id") || crypto.randomUUID(),
  coreTopTab: localStorage.getItem("tater_tateros_core_tab") || "manage",
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

localStorage.setItem("tater_tateros_session_id", state.sessionId);

const VIEW_META = {
  chat: { title: "Chat", subtitle: "Talk to Tater Totterson" },
  verbas: { title: "Verbas", subtitle: "Enable tools and manage Verba settings + shop updates." },
  portals: { title: "Portals", subtitle: "Portal runtime controls and full Portal Shop manager." },
  cores: { title: "Cores", subtitle: "Core runtime controls and full Core Shop manager." },
  settings: { title: "Settings", subtitle: "Global WebUI and Tater runtime configuration." },
};

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
    window.setTimeout(() => {
      item.remove();
    }, 180);
  };

  const ttl = Math.max(1200, Number(timeoutMs) || 2600);
  const timer = window.setTimeout(closeToast, ttl);
  item.addEventListener("click", () => {
    window.clearTimeout(timer);
    closeToast();
  });
}

async function api(path, options = {}) {
  const response = await fetch(path, {
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
  return `<label>${safeLabel}<input id="${inputId}" type="${htmlType}" value="${escapeHtml(field.value ?? "")}" data-setting-type="${escapeHtml(type)}" data-setting-key="${safeKey}" />${safeDesc}</label>`;
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
    input.placeholder = `Type your message to ${getTaterFullName()}...`;
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
  localStorage.setItem("tater_tateros_core_tab", normalized);
}

function setActiveNav(viewName) {
  document.querySelectorAll(".nav-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.view === viewName);
  });
}

async function refreshHealth() {
  const summary = document.getElementById("runtime-summary");
  try {
    const health = await api("/api/health");
    if (!summary) {
      return;
    }
    if (health.ok === false) {
      summary.textContent = "Backend degraded";
      return;
    }
    summary.textContent = `${health.verbas_enabled ?? 0} verbas enabled • ${health.portals_running ?? 0} portals running • ${health.cores_running ?? 0} cores running • ${health.chat_jobs_active ?? 0} chat jobs`;
  } catch {
    if (summary) {
      summary.textContent = "Backend offline";
    }
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
  return mm ? `${base}?mimetype=${encodeURIComponent(mm)}` : base;
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
    bodyHtml = `<div class="bubble-body">${escapeHtml(content)}</div>`;
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
  `;
}

function bindShopTabs(kind) {
  const buttons = Array.from(document.querySelectorAll(`.shop-tab-btn[data-kind='${kind}']`));
  const panels = Array.from(document.querySelectorAll(`.shop-tab-panel[data-kind='${kind}']`));
  if (!buttons.length || !panels.length) {
    return;
  }

  const activate = (tabName) => {
    buttons.forEach((button) => {
      button.classList.toggle("active", button.dataset.tab === tabName);
    });
    panels.forEach((panel) => {
      panel.classList.toggle("active", panel.dataset.tabPanel === tabName);
    });
  };

  buttons.forEach((button) => {
    button.addEventListener("click", () => activate(button.dataset.tab));
  });
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
  return `
    <label>${escapeHtml(label)}
      <input type="${htmlType}" value="${escapeHtml(field?.value ?? "")}" ${placeholderAttr} data-core-field-key="${escapeHtml(
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

function bindCoreTopTabs() {
  const buttons = Array.from(document.querySelectorAll(".core-top-tab-btn[data-core-tab]"));
  const panels = Array.from(document.querySelectorAll(".core-top-tab-panel[data-core-tab-panel]"));
  if (!buttons.length || !panels.length) {
    return;
  }

  const activate = (tabName) => {
    persistCoreTopTab(tabName);
    buttons.forEach((button) => {
      button.classList.toggle("active", button.dataset.coreTab === tabName);
    });
    panels.forEach((panel) => {
      panel.classList.toggle("active", panel.dataset.coreTabPanel === tabName);
    });
  };

  buttons.forEach((button) => {
    button.addEventListener("click", () => activate(button.dataset.coreTab));
  });

  const available = new Set(buttons.map((button) => button.dataset.coreTab));
  activate(available.has(state.coreTopTab) ? state.coreTopTab : "manage");
}

function bindCoreManagerTabs() {
  document.querySelectorAll(".core-manager-tabs").forEach((tabsRoot) => {
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
}

function bindCoreTabManagers() {
  bindCoreManagerTabs();
  bindCoreManagerSubtabs();
  bindCoreManagerSelectors();

  document.querySelectorAll(".core-manager-add-form[data-core-key][data-core-action]").forEach((form) => {
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
        persistCoreTabFromNode(form);
        const result = await runCoreTabAction(coreKey, action, { ...values, values });
        state.notice = String(result?.message || "Saved.");
        showToast(state.notice);
        await loadView("cores");
      } catch (error) {
        setCoreManagerStatus(form, `Failed: ${error.message}`);
      }
    });
  });

  document.querySelectorAll(".core-manager-save").forEach((button) => {
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
        persistCoreTabFromNode(card);
        const result = await runCoreTabAction(coreKey, action, { id: itemId, values });
        state.notice = String(result?.message || "Saved.");
        showToast(state.notice);
        await loadView("cores");
      } catch (error) {
        setCoreManagerStatus(card, `Failed: ${error.message}`);
      }
    });
  });

  document.querySelectorAll(".core-manager-remove").forEach((button) => {
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
        persistCoreTabFromNode(card);
        const result = await runCoreTabAction(coreKey, action, { id: itemId });
        state.notice = String(result?.message || "Removed.");
        await loadView("cores");
      } catch (error) {
        setCoreManagerStatus(card, `Failed: ${error.message}`);
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
  const statusEl = document.getElementById(`shop-status-${kind}`);

  function setStatus(text) {
    if (statusEl) {
      statusEl.textContent = text;
    }
  }

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
        await runShopAction(kind, "repos", { repos });
        state.notice = `${shopLabel(kind)} repos saved.`;
        showToast(state.notice);
        await loadView(state.view);
      } catch (error) {
        setStatus(`Repo save failed: ${error.message}`);
        showToast(`Repo save failed: ${error.message}`, "error", 3600);
      }
    });
  }

  const updateAllButton = document.querySelector(`.shop-update-all[data-kind='${kind}']`);
  if (updateAllButton) {
    updateAllButton.addEventListener("click", async () => {
      setStatus("Running update all...");
      try {
        const result = await runShopAction(kind, "update-all", {});
        const updated = Array.isArray(result.updated) ? result.updated.length : 0;
        const failed = Array.isArray(result.failed) ? result.failed.length : 0;
        state.notice = `${shopLabel(kind)} update-all completed. Updated ${updated}, failed ${failed}.`;
        await loadView(state.view);
      } catch (error) {
        setStatus(`Update all failed: ${error.message}`);
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
        const card = event.currentTarget.closest(".shop-installed-card");
        const purge = Boolean(card?.querySelector(".shop-purge")?.checked);
        payload.purge_redis = purge;
      }

      setStatus(`${action} ${id}...`);
      try {
        const result = await runShopAction(kind, action, payload);
        const message = result.message || `${shopLabel(kind)} action completed.`;
        state.notice = message;
        await loadView(state.view);
      } catch (error) {
        setStatus(`${action} ${id} failed: ${error.message}`);
      }
    });
  });
}

async function loadChatView() {
  const root = document.getElementById("view-root");
  root.innerHTML = `${consumeNoticeHtml()}
    <div class="card">
      <div class="card-head">
        <h3 class="card-title">Conversation</h3>
      </div>
      <div id="chat-log" class="chat-log"></div>
      <div id="chat-speed-stats" class="small chat-speed-stats" style="display:none;"></div>
      <div id="chat-status" class="small chat-live-status" aria-live="polite"></div>
    </div>
    <div class="card message-box">
      <label>Message
        <textarea id="chat-input" rows="2" placeholder="${escapeHtml(`Type your message to ${getTaterFullName()}...`)}"></textarea>
      </label>
      <div class="inline-row">
        <label for="chat-files" class="inline-btn">Attach Files</label>
        <button type="button" id="clear-chat-files" class="inline-btn" style="display:none;">Clear</button>
        <input id="chat-files" class="chat-file-input" type="file" multiple />
        <button class="action-btn" id="send-chat">Send</button>
        <span class="small">Session: <code>${escapeHtml(state.sessionId.slice(0, 8))}</code></span>
        <span id="chat-files-meta" class="small">No files selected.</span>
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

  function attachJobStream(jobId) {
    closeChatEventSource();
    state.activeChatJobId = jobId;

    const eventSource = new EventSource(`/api/chat/jobs/${encodeURIComponent(jobId)}/events`);
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
      status.textContent = waitText;
      await refreshChatHistory();
    });

    eventSource.addEventListener("done", async () => {
      status.textContent = "Complete.";
      state.sending = false;
      closeChatEventSource();
      await refreshChatHistory();
      await refreshHealth();
    });

    eventSource.addEventListener("job_error", async (event) => {
      const payload = safeJsonParse(event.data) || {};
      status.textContent = `Job failed: ${payload.error || "unknown error"}`;
      state.sending = false;
      closeChatEventSource();
      await refreshChatHistory();
      await refreshHealth();
    });

    eventSource.onerror = async () => {
      try {
        const snapshot = await api(`/api/chat/jobs/${encodeURIComponent(jobId)}`);
        const snapshotStatus = String(snapshot.status || "");
        if (snapshotStatus === "done") {
          status.textContent = "Complete.";
          state.sending = false;
          closeChatEventSource();
          await refreshChatHistory();
          await refreshHealth();
        } else if (snapshotStatus === "error") {
          status.textContent = `Job failed: ${snapshot.error || "unknown error"}`;
          state.sending = false;
          closeChatEventSource();
          await refreshChatHistory();
          await refreshHealth();
        }
      } catch {
        status.textContent = "Stream disconnected while waiting for job updates.";
      }
    };
  }

  await refreshChatProfile();
  await refreshChatHistory();

  const chatInputEl = document.getElementById("chat-input");
  const sendChatBtn = document.getElementById("send-chat");

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
      const response = await api("/api/chat/jobs", {
        method: "POST",
        body: JSON.stringify({ message, session_id: state.sessionId, attachments }),
      });

      const sessionId = String(response.session_id || "").trim();
      if (sessionId) {
        state.sessionId = sessionId;
        localStorage.setItem("tater_tateros_session_id", state.sessionId);
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
}

async function loadVerbasView() {
  const root = document.getElementById("view-root");

  const [runtimeData, shopData] = await Promise.all([
    api("/api/verbas"),
    api("/api/shop/verbas"),
  ]);

  const items = Array.isArray(runtimeData.items) ? runtimeData.items : [];
  const installedShopItems = Array.isArray(shopData?.installed) ? shopData.installed : [];
  const shopById = new Map(
    installedShopItems
      .map((entry) => [String(entry?.id || "").trim(), entry])
      .filter(([id]) => Boolean(id))
  );
  const runtimeHtml = items.length
    ? items
        .map((item) => {
          const shopEntry = shopById.get(String(item.id || "").trim()) || {};
          const settings = Array.isArray(item.settings) ? item.settings : [];
          const settingsHtml = settings
            .map((field) => buildSettingInput(field, `verba_${item.id}_${field.key}`))
            .join("");
          const settingsBlock = settingsHtml
            ? `<details class="settings-dropdown"><summary class="settings-summary">Settings</summary><form class="form-grid verba-settings">${settingsHtml}<button type="submit" class="action-btn">Save Settings</button></form></details>`
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
        .join("")
    : renderNotice("No verbas found in plugin registry.");

  root.innerHTML = `${consumeNoticeHtml()}
    ${renderShopTabbedManager("verbas", shopData, {
      runtimeHtml,
      runtimeTitle: "Verba Runtime",
    })}
  `;

  root.querySelectorAll(".verba-toggle").forEach((button) => {
    button.addEventListener("click", async (event) => {
      const card = event.target.closest("[data-plugin-id]");
      const pluginId = card.dataset.pluginId;
      const nextEnabled = event.target.textContent.trim() === "Enable";
      await api(`/api/verbas/${pluginId}/enabled`, {
        method: "POST",
        body: JSON.stringify({ enabled: nextEnabled }),
      });
      await loadView("verbas");
      await refreshHealth();
    });
  });

  root.querySelectorAll("form.verba-settings").forEach((form) => {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const card = event.target.closest("[data-plugin-id]");
      const pluginId = card.dataset.pluginId;
      const values = collectFormValues(form);
      await api(`/api/verbas/${pluginId}/settings`, {
        method: "POST",
        body: JSON.stringify({ values }),
      });
      state.notice = `Saved settings for ${pluginId}.`;
      showToast(state.notice);
      await loadView("verbas");
    });
  });

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

  const items = Array.isArray(runtimeData.items) ? runtimeData.items : [];
  const root = document.getElementById("view-root");
  const installedShopItems = Array.isArray(shopData?.installed) ? shopData.installed : [];
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

  const runtimeHtml = items.length
    ? items
        .map((item) => {
          const shopEntry = resolveShopEntry(item.key) || {};
          const settings = Array.isArray(item.settings) ? item.settings : [];
          const settingsHtml = settings
            .map((field) => buildSettingInput(field, `${kind}_${item.key}_${field.key}`))
            .join("");

          const running = Boolean(item.running);
          const desired = Boolean(item.desired_running);
          const statusClass = running ? "running" : "stopped";
          const statusText = running ? "Running" : desired ? "Pending start" : "Stopped";
          const actionLabel = running ? "Stop" : "Start";
          const installedVer = String(shopEntry.installed_ver || "0.0.0").trim() || "0.0.0";
          const storeVer = String(shopEntry.store_ver || "-").trim() || "-";
          const sourceLabel = String(shopEntry.source_label || "local").trim() || "local";
          const description = String(shopEntry.description || "").trim();
          const settingsBlock = settingsHtml
            ? `<details class="settings-dropdown"><summary class="settings-summary">Settings</summary><form class="form-grid surface-settings">${settingsHtml}<button type="submit" class="action-btn">Save Settings</button></form></details>`
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
        .join("")
    : renderNotice(`No ${kind} found.`);

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

  root.querySelectorAll(".surface-toggle").forEach((button) => {
    button.addEventListener("click", async (event) => {
      const card = event.target.closest("[data-surface-key]");
      const surfaceKey = card.dataset.surfaceKey;
      const action = event.target.textContent.trim().toLowerCase() === "start" ? "start" : "stop";
      await api(`${endpoint}/${surfaceKey}/${action}`, { method: "POST" });
      await loadView(kind);
      await refreshHealth();
    });
  });

  root.querySelectorAll("form.surface-settings").forEach((form) => {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const card = event.target.closest("[data-surface-key]");
      const surfaceKey = card.dataset.surfaceKey;
      const values = collectFormValues(form);

      await api(`${endpoint}/${surfaceKey}/settings`, {
        method: "POST",
        body: JSON.stringify({ values }),
      });

      state.notice = `Saved settings for ${surfaceKey}.`;
      showToast(state.notice);
      await loadView(kind);
    });
  });

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
      <div class="small">Categories: General, Integrations, Emoji, Cerberus, Advanced.</div>
      <div id="settings-status" class="small" style="margin-top: 6px;"></div>

      <div class="settings-tabs">
        <button type="button" class="settings-tab-btn active" data-settings-tab="general">General</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="integrations">Integrations</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="emoji">Emoji</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="cerberus">Cerberus</button>
        <button type="button" class="settings-tab-btn" data-settings-tab="advanced">Advanced</button>
      </div>

      <form id="settings-form">
        <section class="settings-tab-panel active" data-settings-panel="general">
          <div class="form-grid two-col">
            <label>WebUI Username
              <input id="set_username" type="text" value="${escapeHtml(settings.username || "User")}" />
            </label>
            <label>Messages Shown in WebUI
              <input id="set_max_display" type="number" min="1" value="${escapeHtml(settings.max_display || 8)}" />
            </label>
            <label>Show tokens/sec stats
              ${renderToggleRow(`<input id="set_show_speed_stats" class="toggle-input" type="checkbox" ${settings.show_speed_stats ? "checked" : ""} />`)}
            </label>
            <div></div>
            <label>Max Stored Messages <span class="small">(0 = infinite)</span>
              <input id="set_max_store" type="number" min="0" value="${escapeHtml(settings.max_store || 20)}" />
            </label>
            <label>Messages Sent To LLM
              <input id="set_max_llm" type="number" min="1" value="${escapeHtml(settings.max_llm || 8)}" />
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
              <label>Agent Max Rounds (0 = unlimited)
                <input id="set_cerberus_max_rounds" type="number" min="0" value="${escapeHtml(
                  settings.cerberus_max_rounds ?? 18
                )}" />
              </label>
              <label>Agent Max Tool Calls (0 = unlimited)
                <input id="set_cerberus_max_tool_calls" type="number" min="0" value="${escapeHtml(
                  settings.cerberus_max_tool_calls ?? 18
                )}" />
              </label>
              <label>Agent State TTL Seconds (0 = no TTL)
                <input id="set_cerberus_agent_state_ttl_seconds" type="number" min="0" value="${escapeHtml(
                  settings.cerberus_agent_state_ttl_seconds ?? 604800
                )}" />
              </label>
              <label>Planner Max Tokens
                <input id="set_cerberus_planner_max_tokens" type="number" min="1" value="${escapeHtml(
                  settings.cerberus_planner_max_tokens ?? 3300
                )}" />
              </label>
              <label>Checker Max Tokens
                <input id="set_cerberus_checker_max_tokens" type="number" min="1" value="${escapeHtml(
                  settings.cerberus_checker_max_tokens ?? 2550
                )}" />
              </label>
              <label>Doer Max Tokens
                <input id="set_cerberus_doer_max_tokens" type="number" min="1" value="${escapeHtml(
                  settings.cerberus_doer_max_tokens ?? 2700
                )}" />
              </label>
              <label>Tool-Repair Max Tokens
                <input id="set_cerberus_tool_repair_max_tokens" type="number" min="1" value="${escapeHtml(
                  settings.cerberus_tool_repair_max_tokens ?? 2250
                )}" />
              </label>
              <label>Recovery Max Tokens
                <input id="set_cerberus_recovery_max_tokens" type="number" min="1" value="${escapeHtml(
                  settings.cerberus_recovery_max_tokens ?? 1050
                )}" />
              </label>
              <label>Max Ledger Items
                <input id="set_cerberus_max_ledger_items" type="number" min="1" value="${escapeHtml(
                  settings.cerberus_max_ledger_items ?? 1500
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
      ["set_cerberus_max_rounds", "cerberus_max_rounds"],
      ["set_cerberus_max_tool_calls", "cerberus_max_tool_calls"],
      ["set_cerberus_agent_state_ttl_seconds", "cerberus_agent_state_ttl_seconds"],
      ["set_cerberus_planner_max_tokens", "cerberus_planner_max_tokens"],
      ["set_cerberus_checker_max_tokens", "cerberus_checker_max_tokens"],
      ["set_cerberus_doer_max_tokens", "cerberus_doer_max_tokens"],
      ["set_cerberus_tool_repair_max_tokens", "cerberus_tool_repair_max_tokens"],
      ["set_cerberus_recovery_max_tokens", "cerberus_recovery_max_tokens"],
      ["set_cerberus_max_ledger_items", "cerberus_max_ledger_items"],
    ];
    map.forEach(([inputId, key]) => {
      const input = document.getElementById(inputId);
      if (!input || !(key in cerberusDefaults)) {
        return;
      }
      input.value = String(cerberusDefaults[key]);
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
      modal.classList.remove("active");
      modal.setAttribute("aria-hidden", "true");
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
    modal.classList.add("active");
    modal.setAttribute("aria-hidden", "false");
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
            <td>${escapeHtml(String(row?.planner_kind ?? ""))}</td>
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
              <th>Planner</th>
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
      cerberus_max_rounds: Number(document.getElementById("set_cerberus_max_rounds").value || 18),
      cerberus_max_tool_calls: Number(document.getElementById("set_cerberus_max_tool_calls").value || 18),
      cerberus_agent_state_ttl_seconds: Number(
        document.getElementById("set_cerberus_agent_state_ttl_seconds").value || 604800
      ),
      cerberus_planner_max_tokens: Number(document.getElementById("set_cerberus_planner_max_tokens").value || 3300),
      cerberus_checker_max_tokens: Number(document.getElementById("set_cerberus_checker_max_tokens").value || 2550),
      cerberus_doer_max_tokens: Number(document.getElementById("set_cerberus_doer_max_tokens").value || 2700),
      cerberus_tool_repair_max_tokens: Number(
        document.getElementById("set_cerberus_tool_repair_max_tokens").value || 2250
      ),
      cerberus_recovery_max_tokens: Number(document.getElementById("set_cerberus_recovery_max_tokens").value || 1050),
      cerberus_max_ledger_items: Number(document.getElementById("set_cerberus_max_ledger_items").value || 1500),
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

async function init() {
  bindNav();
  await refreshBranding();
  await refreshHealth();
  await loadView(state.view);

  setInterval(refreshHealth, 8000);
}

window.addEventListener("beforeunload", () => {
  closeChatEventSource();
});

init();
