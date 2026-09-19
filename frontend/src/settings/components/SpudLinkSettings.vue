<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { getJson, postJson } from "../../shared/api";
import type { JsonRow, SpudLinkSettings } from "../types";

const props = defineProps<{
  settings: SpudLinkSettings;
  endpoint: string;
  statusEndpoint: string;
  pairingCodeEndpoint: string;
  connectEndpoint: string;
  revokeEndpoint: string;
  llmApiUrl: string;
  modelsApiUrl: string;
  pairApiUrl: string;
  initialTab?: string;
}>();

const emit = defineEmits<{
  changed: [settings: SpudLinkSettings];
  tabChange: [tab: string];
  notify: [message: string, tone?: string];
}>();

const routeSpecs = [
  { id: "llm", label: "LLM", note: "Hydra planning, tools, chat, and final answers" },
  { id: "vad", label: "Speech-End Detection (VAD)", note: "The Hub detects when speech ends; this Tater keeps a local fallback" },
  { id: "stt", label: "Speech to Text", note: "Transcription runs on the Hub" },
  { id: "tts", label: "Text to Speech", note: "The Hub returns ready-to-play speech audio" },
  { id: "vision", label: "Vision", note: "Image descriptions and camera snapshots" },
  { id: "audio", label: "Audio Understanding", note: "Music and general audio analysis" },
  { id: "video", label: "Video Understanding", note: "Camera clips and attached video analysis" },
  { id: "speaker_id", label: "Speaker ID", note: "Voice embeddings run on the Hub; profiles stay here" },
  { id: "emotion_id", label: "Emotion ID", note: "Voice tone analysis" },
  { id: "face_id", label: "Face ID", note: "Face embeddings run on the Hub; People and the face library stay here" },
] as const;

const validTabs = new Set(["pair", "spudlet", "settings"]);
const normalizeTab = (value: unknown) => validTabs.has(String(value || "").trim()) ? String(value).trim() : "pair";
const activeTab = ref(normalizeTab(props.initialTab));
const saving = ref(false);
const connecting = ref(false);
const refreshing = ref(false);
const dirty = ref(false);
const error = ref("");
const notice = ref("");
const pairingCode = ref("");
const revokingNode = ref("");

const draft = reactive({
  mode: "disabled",
  node_name: "Tater",
  home_url: "",
  public_url: "",
  pairing_enabled: false,
  allow_spudlets: true,
  allow_little_spuds: true,
  little_spud_tools_enabled: true,
  telemetry_enabled: true,
  request_previews_enabled: false,
  model_routing_enabled: false,
  hub_url: "",
  routes: {} as Record<string, string>,
});

const pairing = reactive({
  open: false,
  role: "little_spud",
  phase: "idle" as "idle" | "loading" | "waiting" | "success" | "error",
  message: "",
  code: "",
  qr: "",
  expiresAt: 0,
  startedAt: 0,
  connectedName: "",
});

let statusTimer = 0;
let pairingTimer = 0;
let disposed = false;

const nodes = computed(() => Array.isArray(props.settings.linked_nodes) ? props.settings.linked_nodes : []);
const pairedHub = computed<JsonRow>(() => props.settings.paired_hub && typeof props.settings.paired_hub === "object" ? props.settings.paired_hub : {});
const hubConnected = computed(() => Boolean(pairedHub.value.connected));
const pairingBusy = computed(() => pairing.phase === "loading" || pairing.phase === "waiting");
const modeLabel = computed(() => draft.mode === "hub" ? "Spud Hub" : draft.mode === "spudlet" ? "Spudlet" : "Disabled");
const modeDescription = computed(() => {
  if (draft.mode === "hub") return "This Tater accepts Little Spuds and Spudlets and shares its models, Hydra, tools, and memory.";
  if (draft.mode === "spudlet") return "This Tater uses an upstream Spud Hub while keeping its own app and local data.";
  return "Spud Link is off. Linked clients cannot pair or make remote requests until it is enabled.";
});

function routeValue(kind: string) {
  if (kind === "llm" || draft.model_routing_enabled) return "hub";
  const value = String(draft.routes[kind] || "auto").toLowerCase();
  return ["auto", "hub", "local"].includes(value) ? value : "auto";
}

function routeUsesHub(kind: string) {
  if (draft.mode !== "spudlet" || !hubConnected.value) return false;
  const route = routeValue(kind);
  return route !== "local" && (kind === "llm" || route === "hub" || draft.model_routing_enabled);
}

function syncFromSettings(settings: SpudLinkSettings) {
  const routing = settings.model_routing && typeof settings.model_routing === "object" ? settings.model_routing : {};
  const routes = settings.model_routes && typeof settings.model_routes === "object"
    ? settings.model_routes
    : routing.routes && typeof routing.routes === "object" ? routing.routes : {};
  Object.assign(draft, {
    mode: ["hub", "spudlet"].includes(String(settings.mode || "")) ? String(settings.mode) : "disabled",
    node_name: String(settings.node_name || "Tater"),
    home_url: String(settings.home_url || window.location.origin || ""),
    public_url: String(settings.public_url || ""),
    pairing_enabled: Boolean(settings.pairing_enabled),
    allow_spudlets: settings.allow_spudlets !== false,
    allow_little_spuds: settings.allow_little_spuds !== false,
    little_spud_tools_enabled: settings.little_spud_tools_enabled !== false,
    telemetry_enabled: settings.telemetry_enabled !== false,
    request_previews_enabled: Boolean(settings.request_previews_enabled),
    model_routing_enabled: Boolean(settings.model_routing_enabled ?? routing.enabled),
    hub_url: String(settings.hub_url || ""),
  });
  draft.routes = Object.fromEntries(routeSpecs.map((spec) => [spec.id, spec.id === "llm" ? "hub" : String(routes[spec.id] || "auto")]));
  dirty.value = false;
}

function markDirty() {
  dirty.value = true;
  error.value = "";
  notice.value = "";
}

function selectTab(tab: string) {
  activeTab.value = normalizeTab(tab);
  emit("tabChange", activeTab.value);
}

function settingsPayload(overrides: Record<string, unknown> = {}) {
  return {
    spud_link_mode: draft.mode,
    spud_link_node_name: draft.node_name.trim(),
    spud_link_home_url: draft.home_url.trim().replace(/\/+$/, ""),
    spud_link_public_url: draft.public_url.trim().replace(/\/+$/, ""),
    spud_link_pairing_enabled: draft.pairing_enabled,
    spud_link_allow_spudlets: draft.allow_spudlets,
    spud_link_allow_little_spuds: draft.allow_little_spuds,
    spud_link_little_spud_tools_enabled: draft.little_spud_tools_enabled,
    spud_link_telemetry_enabled: draft.telemetry_enabled,
    spud_link_request_previews_enabled: draft.request_previews_enabled,
    spud_link_model_routing_enabled: draft.model_routing_enabled,
    ...Object.fromEntries(routeSpecs.map((spec) => [`spud_link_model_route_${spec.id}`, routeValue(spec.id)])),
    spud_link_hub_url: draft.hub_url.trim().replace(/\/+$/, ""),
    ...overrides,
  };
}

async function persistSettings(overrides: Record<string, unknown> = {}) {
  const next = await postJson<SpudLinkSettings>(props.endpoint, settingsPayload(overrides));
  emit("changed", next);
  syncFromSettings(next);
  return next;
}

async function save() {
  saving.value = true;
  error.value = "";
  notice.value = "";
  try {
    await persistSettings();
    notice.value = "Spud Link settings saved and synchronized.";
    emit("notify", notice.value, "success");
  } catch (saveError) {
    error.value = saveError instanceof Error ? saveError.message : "Spud Link settings could not be saved.";
    emit("notify", error.value, "error");
  } finally {
    saving.value = false;
  }
}

async function refreshStatus(silent = false) {
  if (refreshing.value || disposed) return;
  refreshing.value = true;
  try {
    const result = await getJson<JsonRow>(props.statusEndpoint);
    const next = result.spud_link && typeof result.spud_link === "object" ? result.spud_link as SpudLinkSettings : null;
    if (next) {
      emit("changed", next);
      if (!dirty.value) syncFromSettings(next);
    }
    if (!silent) notice.value = "Spud Link status refreshed.";
  } catch (refreshError) {
    if (!silent) {
      error.value = refreshError instanceof Error ? refreshError.message : "Spud Link status could not be refreshed.";
      emit("notify", error.value, "error");
    }
  } finally {
    refreshing.value = false;
  }
}

function scheduleStatusPoll() {
  window.clearTimeout(statusTimer);
  if (disposed) return;
  statusTimer = window.setTimeout(async () => {
    await refreshStatus(true);
    scheduleStatusPoll();
  }, 5000);
}

function validUrl(value: string, label: string, required = false) {
  const normalized = value.trim().replace(/\/+$/, "");
  if (!normalized) {
    if (required) throw new Error(`Enter the ${label} before creating the QR code.`);
    return "";
  }
  let parsed: URL;
  try {
    parsed = new URL(normalized);
  } catch {
    throw new Error(`${label} must be a complete http:// or https:// address.`);
  }
  if (!["http:", "https:"].includes(parsed.protocol) || !parsed.hostname) throw new Error(`${label} must be a complete http:// or https:// address.`);
  return normalized;
}

function closePairing() {
  window.clearTimeout(pairingTimer);
  pairingTimer = 0;
  pairing.open = false;
  pairing.phase = "idle";
}

async function pollPairing() {
  if (!pairing.open || pairing.phase !== "waiting" || disposed) return;
  try {
    const result = await getJson<JsonRow>(props.statusEndpoint);
    const next = result.spud_link && typeof result.spud_link === "object" ? result.spud_link as SpudLinkSettings : {};
    emit("changed", next);
    if (!dirty.value) syncFromSettings(next);
    const linked = Array.isArray(next.linked_nodes) ? next.linked_nodes.find((node) => {
      if (String(node.role || "").toLowerCase() !== pairing.role) return false;
      return Math.max(Number(node.created_at || 0), Number(node.last_seen_at || 0)) >= pairing.startedAt - 1;
    }) : null;
    if (linked && !next.pairing_code_active) {
      pairing.phase = "success";
      pairing.connectedName = String(linked.name || (pairing.role === "spudlet" ? "Spudlet" : "Little Spud"));
      pairing.message = `${pairing.connectedName} is connected and ready.`;
      emit("notify", pairing.role === "spudlet" ? "Spudlet connected." : "Little Spud connected.", "success");
      return;
    }
  } catch {
    // A brief status failure should not cancel a still-valid invitation.
  }
  pairingTimer = window.setTimeout(pollPairing, 1100);
}

async function startPairing(role: "little_spud" | "spudlet") {
  error.value = "";
  let homeUrl = "";
  let publicUrl = "";
  try {
    if (role === "little_spud") {
      homeUrl = validUrl(draft.home_url, "Home / LAN URL", true);
      publicUrl = validUrl(draft.public_url, "Away / Tater Tunnel URL");
    }
  } catch (urlError) {
    error.value = urlError instanceof Error ? urlError.message : "Enter valid connection addresses.";
    emit("notify", error.value, "error");
    return;
  }

  pairing.open = true;
  pairing.role = role;
  pairing.phase = "loading";
  pairing.message = role === "spudlet" ? "Creating a short code for the other Tater…" : "Creating a private QR code for Little Spud…";
  pairing.code = "";
  pairing.qr = "";
  pairing.connectedName = "";
  pairing.startedAt = Date.now() / 1000;
  try {
    const readyMode = draft.mode === "disabled" ? "hub" : draft.mode;
    await persistSettings({
      spud_link_mode: readyMode,
      spud_link_pairing_enabled: true,
      ...(role === "spudlet" ? { spud_link_allow_spudlets: true } : { spud_link_allow_little_spuds: true }),
      ...(role === "little_spud" ? { spud_link_home_url: homeUrl, spud_link_public_url: publicUrl } : {}),
    });
    const result = await postJson<JsonRow>(props.pairingCodeEndpoint, {
      role,
      ...(role === "little_spud" ? { home_url: homeUrl, public_url: publicUrl } : {}),
    });
    pairing.code = String(result.manual_code || result.pairing_code || "");
    pairing.qr = String(result.pairing_qr_svg || "");
    pairing.expiresAt = Number(result.expires_at || 0);
    if (role === "little_spud" && !pairing.qr) throw new Error("QR generation is unavailable on this Tater install.");
    pairing.phase = "waiting";
    pairing.message = role === "spudlet" ? "Paste this code into the other Tater." : "Open Little Spud and scan this QR code.";
    void pollPairing();
  } catch (pairError) {
    pairing.phase = "error";
    pairing.message = pairError instanceof Error ? pairError.message : "The pairing invitation could not be created.";
    emit("notify", `Pairing failed: ${pairing.message}`, "error");
  }
}

async function copyPairingCode() {
  if (!pairing.code) return;
  try {
    await navigator.clipboard.writeText(pairing.code);
    notice.value = "Spudlet pairing code copied.";
  } catch (copyError) {
    error.value = copyError instanceof Error ? copyError.message : "The pairing code could not be copied.";
  }
}

async function connect() {
  const hubUrl = draft.hub_url.trim();
  const code = pairingCode.value.trim();
  if (!hubUrl || !code) {
    error.value = "Spud Hub URL and pairing code are required.";
    emit("notify", error.value, "error");
    return;
  }
  connecting.value = true;
  error.value = "";
  notice.value = "";
  try {
    await postJson<JsonRow>(props.connectEndpoint, {
      hub_url: hubUrl,
      pairing_code: code,
      role: "spudlet",
      node_name: draft.node_name.trim(),
      public_url: draft.public_url.trim(),
    });
    pairingCode.value = "";
    const next = await getJson<SpudLinkSettings>(props.endpoint);
    emit("changed", next);
    syncFromSettings(next);
    notice.value = "Connected to the Spud Hub. Model routes are live now.";
    emit("notify", notice.value, "success");
  } catch (connectError) {
    error.value = connectError instanceof Error ? connectError.message : "The Spud Hub connection failed.";
    emit("notify", `Connect failed: ${error.value}`, "error");
  } finally {
    connecting.value = false;
  }
}

async function disconnect() {
  if (!window.confirm("Disconnect this Tater from its Spud Hub? It will need a new pairing code to reconnect.")) return;
  saving.value = true;
  try {
    await persistSettings({ clear_spud_link_node_token: true, spud_link_mode: "disabled" });
    notice.value = "This Tater is disconnected from its Spud Hub.";
    emit("notify", notice.value, "success");
  } catch (disconnectError) {
    error.value = disconnectError instanceof Error ? disconnectError.message : "The Hub connection could not be removed.";
    emit("notify", error.value, "error");
  } finally {
    saving.value = false;
  }
}

async function revoke(node: JsonRow) {
  const nodeId = String(node.id || "").trim();
  const name = String(node.name || nodeId || "this linked Spud");
  if (!nodeId || !window.confirm(`Revoke ${name}? This device will need to pair again.`)) return;
  revokingNode.value = nodeId;
  error.value = "";
  try {
    await postJson<JsonRow>(props.revokeEndpoint, { node_id: nodeId });
    await refreshStatus(true);
    notice.value = `${name} revoked.`;
    emit("notify", notice.value, "success");
  } catch (revokeError) {
    error.value = revokeError instanceof Error ? revokeError.message : "The linked Spud could not be revoked.";
    emit("notify", `Revoke failed: ${error.value}`, "error");
  } finally {
    revokingNode.value = "";
  }
}

function roleLabel(role: unknown) {
  return String(role || "") === "little_spud" ? "Little Spud" : String(role || "") === "spudlet" ? "Spudlet" : "Linked Spud";
}

function formatTime(value: unknown) {
  const timestamp = Number(value || 0);
  return timestamp > 0 ? new Date(timestamp * 1000).toLocaleString() : "Never";
}

watch(
  () => props.settings,
  (settings) => {
    if (!dirty.value) syncFromSettings(settings || {});
  },
  { immediate: true },
);

onMounted(async () => {
  await refreshStatus(true);
  scheduleStatusPoll();
});

onBeforeUnmount(() => {
  disposed = true;
  window.clearTimeout(statusTimer);
  window.clearTimeout(pairingTimer);
});
</script>

<template>
  <section class="tset-resource tlink-resource">
    <div v-if="notice || error" class="tv-notice" :class="{ error: Boolean(error) }" aria-live="polite">
      {{ error || notice }}
    </div>

    <section class="tv-panel tlink-hero">
      <div class="tlink-orbit" aria-hidden="true"><i class="hub" /><i class="spudlet" /><i class="little" /></div>
      <div>
        <span class="tv-eyebrow">One Tater, three ways to connect</span>
        <h2>Spud Link</h2>
        <p>Share models and tools from a Hub, borrow them as a Spudlet, or pair a lightweight Little Spud by QR.</p>
      </div>
      <div class="tlink-live">
        <span :class="{ connected: hubConnected || nodes.length > 0 }" />
        {{ hubConnected ? "Hub connected" : nodes.length ? `${nodes.length} linked` : modeLabel }}
      </div>
    </section>

    <nav class="tv-tabs tlink-tabs" aria-label="Spud Link sections">
      <button type="button" :class="{ active: activeTab === 'pair' }" @click="selectTab('pair')">Pair devices</button>
      <button type="button" :class="{ active: activeTab === 'spudlet' }" @click="selectTab('spudlet')">Use a Spud Hub</button>
      <button type="button" :class="{ active: activeTab === 'settings' }" @click="selectTab('settings')">Settings</button>
    </nav>

    <template v-if="activeTab === 'pair'">
      <div class="tlink-pair-grid">
        <section class="tv-panel tlink-pair-card">
          <header><span class="tv-eyebrow">Little Spud</span><h2>Pair by QR</h2><p>Create one private code containing the addresses this companion should use.</p></header>
          <label>Home / LAN URL<input v-model="draft.home_url" type="url" placeholder="http://tater.local:8501" @input="markDirty" /><small>Required. Used while the Little Spud is on your home network.</small></label>
          <label>Away / Tater Tunnel URL<input v-model="draft.public_url" type="url" placeholder="https://your-tater-tunnel.example" @input="markDirty" /><small>Optional fallback for use away from home.</small></label>
          <button class="tv-button primary" type="button" :disabled="pairingBusy" @click="startPairing('little_spud')">Show Little Spud QR</button>
        </section>

        <section class="tv-panel tlink-pair-card">
          <header><span class="tv-eyebrow">Spudlet</span><h2>Link another Tater</h2><p>Create a short, single-use code to paste into another full Tater.</p></header>
          <div class="tlink-pair-steps"><strong>1</strong><span>Open Spud Link on the other Tater.</span><strong>2</strong><span>Paste the Hub URL and code there.</span><strong>3</strong><span>The permanent token is saved automatically.</span></div>
          <button class="tv-button primary" type="button" :disabled="pairingBusy" @click="startPairing('spudlet')">Create Spudlet code</button>
        </section>
      </div>

      <section class="tv-panel tlink-nodes">
        <header class="tv-panel-head"><div><span class="tv-eyebrow">Linked Spuds</span><h2>Devices connected to this Tater</h2><p>Live status refreshes every five seconds.</p></div><div class="tlink-head-actions"><span class="tv-live-pill"><i />{{ nodes.length }} linked</span><button class="tv-button" type="button" :disabled="refreshing" @click="refreshStatus()">{{ refreshing ? "Refreshing…" : "Refresh" }}</button></div></header>
        <div v-if="nodes.length" class="tlink-node-list">
          <article v-for="node in nodes" :key="String(node.id || node.name)" class="tlink-node-row">
            <span class="tlink-node-dot" />
            <div><strong>{{ node.name || node.id || "Linked Spud" }}</strong><small>{{ roleLabel(node.role) }} · last seen {{ formatTime(node.last_seen_at) }}</small><div class="tlink-facts"><span v-if="node.last_remote_addr">IP {{ node.last_remote_addr }}</span><span v-if="node.version">v{{ node.version }}</span><span v-if="node.remote_mode">{{ node.remote_mode }}</span></div></div>
            <button class="tv-button danger" type="button" :disabled="revokingNode === String(node.id || '')" @click="revoke(node)">{{ revokingNode === String(node.id || '') ? "Revoking…" : "Revoke" }}</button>
          </article>
        </div>
        <div v-else class="tv-empty">No linked Spuds yet. Create a QR or Spudlet code above to connect one.</div>
      </section>
    </template>

    <template v-else-if="activeTab === 'spudlet'">
      <section class="tv-panel tset-form-card tlink-connect">
        <header class="tv-panel-head"><div><span class="tv-eyebrow">Upstream Hub</span><h2>Connect this Tater to a Spud Hub</h2><p>Create a Spudlet code on the main Tater, then paste its address and code here.</p></div><span class="tlink-mode-chip">Spudlet</span></header>
        <div class="tv-form-grid">
          <label class="full">Spud Hub URL<input v-model="draft.hub_url" type="text" placeholder="http://spud-hub.local:8501" @input="markDirty" /><small>Use its LAN address at home or its public Tater Tunnel address.</small></label>
          <label class="full">Spudlet pairing code<input v-model="pairingCode" type="text" autocomplete="off" placeholder="SPUD-XXXXXX-XXXXXX" /></label>
        </div>
        <div class="tlink-connection" :class="{ connected: hubConnected }"><span class="tlink-node-dot" /><div><strong>{{ hubConnected ? `Connected to ${pairedHub.hub_name || 'Spud Hub'}` : "Not connected to a Spud Hub" }}</strong><small>{{ hubConnected ? `${pairedHub.hub_url || draft.hub_url} · paired ${formatTime(pairedHub.connected_at)}` : "Connecting changes this Tater to Spudlet mode and saves its private token." }}</small></div></div>
        <div class="tlink-actions"><button class="tv-button primary" type="button" :disabled="connecting" @click="connect">{{ connecting ? "Connecting…" : "Connect to Spud Hub" }}</button><button v-if="hubConnected" class="tv-button danger" type="button" :disabled="saving" @click="disconnect">Disconnect</button></div>
      </section>

      <section class="tv-panel tlink-routing">
        <header class="tv-panel-head"><div><span class="tv-eyebrow">Model routing</span><h2>Choose what runs on the Hub</h2><p>Wake word detection stays on this device. Other model work can run here or on the paired Hub.</p></div><label class="tv-toggle compact"><input v-model="draft.model_routing_enabled" class="tv-checkbox" type="checkbox" @change="markDirty" /><span><strong>Use Hub for all models</strong><small>Recommended on low-power installs</small></span></label></header>
        <div class="tlink-route-grid">
          <label v-for="spec in routeSpecs" :key="spec.id" class="tlink-route" :class="{ hub: routeUsesHub(spec.id) }">
            <span><strong>{{ spec.label }}</strong><small>{{ spec.note }}</small></span>
            <span>
              <em v-if="spec.id === 'llm'">Spud Hub</em>
              <select v-else v-model="draft.routes[spec.id]" :disabled="draft.model_routing_enabled" @change="markDirty"><option value="auto">Auto</option><option value="hub">Spud Hub</option><option value="local">This Tater</option></select>
              <small>{{ routeUsesHub(spec.id) ? "Loaded on Spud Hub" : "Runs on this Tater" }}</small>
            </span>
          </label>
        </div>
      </section>
    </template>

    <template v-else>
      <div class="tlink-settings-grid">
        <section class="tv-panel tset-form-card">
          <header><span class="tv-eyebrow">Role</span><h2>How this Tater uses Spud Link</h2></header>
          <div class="tv-form-grid">
            <label>Spud Link mode<select v-model="draft.mode" @change="markDirty"><option value="disabled">Disabled</option><option value="hub">Spud Hub</option><option value="spudlet">Spudlet</option></select></label>
            <label>Display name<input v-model="draft.node_name" type="text" @input="markDirty" /><small>Shown to linked devices and the upstream Hub.</small></label>
          </div>
          <div class="tlink-mode-help"><strong>{{ modeLabel }}</strong><span>{{ modeDescription }}</span></div>
        </section>

        <section class="tv-panel tset-form-card">
          <header><span class="tv-eyebrow">Pairing & privacy</span><h2>Connection policy</h2></header>
          <div class="tlink-toggle-grid">
            <label class="tv-toggle"><input v-model="draft.pairing_enabled" class="tv-checkbox" type="checkbox" @change="markDirty" /><span><strong>Allow new pairing invites</strong><small>Pair buttons enable this automatically.</small></span></label>
            <label class="tv-toggle"><input v-model="draft.allow_spudlets" class="tv-checkbox" type="checkbox" @change="markDirty" /><span><strong>Allow Spudlets</strong><small>Full Tater clients may connect.</small></span></label>
            <label class="tv-toggle"><input v-model="draft.allow_little_spuds" class="tv-checkbox" type="checkbox" @change="markDirty" /><span><strong>Allow Little Spuds</strong><small>Lightweight companions may connect.</small></span></label>
            <label class="tv-toggle"><input v-model="draft.little_spud_tools_enabled" class="tv-checkbox" type="checkbox" @change="markDirty" /><span><strong>Little Spud tool use</strong><small>Allow paired companions to use Hydra tools.</small></span></label>
            <label class="tv-toggle"><input v-model="draft.telemetry_enabled" class="tv-checkbox" type="checkbox" @change="markDirty" /><span><strong>Telemetry</strong><small>Keep linked-device health and activity metadata.</small></span></label>
            <label class="tv-toggle"><input v-model="draft.request_previews_enabled" class="tv-checkbox" type="checkbox" @change="markDirty" /><span><strong>Request previews</strong><small>Off by default so activity remains metadata-first.</small></span></label>
          </div>
        </section>
      </div>

      <details class="tv-panel tlink-endpoints"><summary>Technical endpoints</summary><div><span>LLM route</span><code>{{ llmApiUrl }}</code><span>Model gateway</span><code>{{ modelsApiUrl }}</code><span>Pair</span><code>{{ pairApiUrl }}</code></div></details>
    </template>

    <footer v-if="activeTab !== 'pair'" class="tset-save-bar">
      <div><strong>{{ dirty ? "Unsaved Spud Link changes" : "Spud Link is synchronized" }}</strong><span>Live connection status continues to refresh while this tab is open.</span></div>
      <button class="tv-button primary" type="button" :disabled="saving || !dirty" @click="save">{{ saving ? "Saving…" : "Save Spud Link settings" }}</button>
    </footer>

    <Teleport to="body">
      <div v-if="pairing.open" class="tlink-modal-backdrop" role="presentation" @click.self="closePairing">
        <section class="tlink-modal" role="dialog" aria-modal="true" aria-labelledby="tlink-pair-title">
          <header><div><span class="tv-eyebrow">Spud Link</span><h2 id="tlink-pair-title">{{ pairing.role === 'spudlet' ? "Link a Spudlet" : "Pair a Little Spud" }}</h2></div><button class="tv-button" type="button" @click="closePairing">Close</button></header>
          <div v-if="pairing.phase === 'loading'" class="tlink-modal-state"><span class="tlink-loader" /><strong>Creating a secure invitation…</strong><small>{{ pairing.message }}</small></div>
          <div v-else-if="pairing.phase === 'success'" class="tlink-modal-state success"><span class="tlink-check">✓</span><strong>Connected!</strong><small>{{ pairing.message }}</small></div>
          <div v-else-if="pairing.phase === 'error'" class="tlink-modal-state error"><span class="tlink-check">!</span><strong>Pairing failed</strong><small>{{ pairing.message }}</small></div>
          <div v-else class="tlink-modal-state">
            <img v-if="pairing.role === 'little_spud'" class="tlink-qr" :src="pairing.qr" alt="Little Spud pairing QR code" />
            <div v-else class="tlink-code"><code>{{ pairing.code }}</code><button class="tv-button" type="button" @click="copyPairingCode">Copy</button></div>
            <strong>{{ pairing.message }}</strong>
            <small>Waiting for connection<span v-if="pairing.expiresAt"> · expires {{ new Date(pairing.expiresAt * 1000).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }) }}</span></small>
          </div>
        </section>
      </div>
    </Teleport>
  </section>
</template>
