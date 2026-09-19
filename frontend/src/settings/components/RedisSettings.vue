<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref, watch } from "vue";
import { getJson, postJson } from "../../shared/api";
import type { RedisConnectionStatus, RedisEncryptionStatus } from "../types";

const props = defineProps<{
  initialStatus?: RedisConnectionStatus;
  initialEncryptionStatus?: RedisEncryptionStatus;
  statusEndpoint: string;
  configureEndpoint: string;
  migrateEndpoint: string;
  encryptionStatusEndpoint: string;
  encryptEndpoint: string;
  decryptEndpoint: string;
}>();

const emit = defineEmits<{
  status: [status: RedisConnectionStatus];
  notify: [message: string, tone?: string];
}>();

const draft = reactive({
  mode: "internal",
  data_path: "",
  host: "",
  port: 6379,
  db: 0,
  username: "",
  password: "",
  use_tls: false,
  verify_tls: true,
  ca_cert_path: "",
});
const status = ref<RedisConnectionStatus>({});
const encryption = ref<RedisEncryptionStatus>({});
const dirty = ref(false);
const busyAction = ref("");
const refreshing = ref(false);
const error = ref("");
const notice = ref("");
let refreshTimer: number | null = null;

function text(value: unknown): string {
  return String(value ?? "").trim();
}

function normalizeStatus(raw: RedisConnectionStatus | undefined): RedisConnectionStatus {
  const next = raw && typeof raw === "object" ? raw : {};
  const mode = text(next.mode).toLowerCase() === "external" && !next.internal ? "external" : "internal";
  return {
    ...next,
    mode,
    internal: mode === "internal",
    configured: Boolean(next.configured),
    connected: Boolean(next.connected),
    host: text(next.host),
    port: Number(next.port || 6379),
    db: Number(next.db || 0),
    username: text(next.username),
    use_tls: Boolean(next.use_tls),
    verify_tls: next.verify_tls !== false,
    ca_cert_path: text(next.ca_cert_path),
    password_set: Boolean(next.password_set),
    error: text(next.error),
    fallback_reason: text(next.fallback_reason),
    source: text(next.source),
    config_path: text(next.config_path),
    data_path: text(next.data_path),
    data_dir: text(next.data_dir),
    socket_path: text(next.socket_path),
    redis_pid: Number(next.redis_pid || 0),
    redis_managed: Boolean(next.redis_managed),
    redis_server_source: text(next.redis_server_source),
  };
}

function normalizeEncryption(raw: RedisEncryptionStatus | undefined): RedisEncryptionStatus {
  const next = raw && typeof raw === "object" ? raw : {};
  const liveEnabled = next.live_encryption_enabled;
  return {
    ...next,
    encryption_available: next.encryption_available !== false,
    key_exists: Boolean(next.key_exists),
    key_path: text(next.key_path),
    key_fingerprint: text(next.key_fingerprint),
    live_encryption_enabled: liveEnabled === undefined ? Boolean(next.snapshot_exists) : Boolean(liveEnabled),
    live_encryption_state_path: text(next.live_encryption_state_path || next.snapshot_path),
    live_encryption_updated: text(next.live_encryption_updated || next.snapshot_modified),
    error: text(next.error),
  };
}

function hydrateDraft(next: RedisConnectionStatus) {
  Object.assign(draft, {
    mode: next.internal || next.mode !== "external" ? "internal" : "external",
    data_path: text(next.data_path),
    host: text(next.host),
    port: Number(next.port || 6379),
    db: Number(next.db || 0),
    username: text(next.username),
    password: "",
    use_tls: Boolean(next.use_tls),
    verify_tls: next.verify_tls !== false,
    ca_cert_path: text(next.ca_cert_path),
  });
}

function applyStatus(raw: RedisConnectionStatus, hydrate = !dirty.value) {
  const next = normalizeStatus(raw);
  status.value = next;
  if (hydrate) hydrateDraft(next);
  emit("status", next);
}

function markDirty() {
  dirty.value = true;
  notice.value = "";
  error.value = "";
}

function payload(testOnly: boolean) {
  return {
    mode: draft.mode,
    host: text(draft.host),
    port: Number(draft.port || 6379),
    db: Number(draft.db || 0),
    username: text(draft.username),
    password: draft.password,
    use_tls: Boolean(draft.use_tls),
    verify_tls: Boolean(draft.verify_tls),
    ca_cert_path: text(draft.ca_cert_path),
    data_path: text(draft.data_path),
    keep_existing_password: !draft.password && Boolean(status.value.password_set),
    test_only: testOnly,
  };
}

function validate(): string {
  if (draft.mode === "external" && !text(draft.host)) return "Enter the external Redis host.";
  if (!Number.isInteger(Number(draft.port)) || Number(draft.port) < 1 || Number(draft.port) > 65535) {
    return "Redis port must be between 1 and 65535.";
  }
  if (!Number.isInteger(Number(draft.db)) || Number(draft.db) < 0) return "Redis DB must be zero or greater.";
  return "";
}

function setResult(message: string, tone: "success" | "error" = "success") {
  if (tone === "error") {
    error.value = message;
    notice.value = "";
  } else {
    notice.value = message;
    error.value = "";
  }
  emit("notify", message, tone);
}

function replayMessage(raw: unknown): string {
  if (!raw || typeof raw !== "object") return "";
  const replay = raw as Record<string, unknown>;
  if (replay.ok === false) return ` Startup replay failed: ${text(replay.error) || "unknown error"}.`;
  return ` Startup replay complete (restore ${replay.ran_restore ? "ran" : "skipped"}; autostart ${replay.ran_autostart ? "ran" : "skipped"}).`;
}

async function refresh(silent = false, force = false) {
  if (busyAction.value && !force) return;
  refreshing.value = true;
  let refreshError = "";
  try {
    const next = await getJson<RedisConnectionStatus>(props.statusEndpoint);
    applyStatus(next, !dirty.value);
  } catch (refreshFailure) {
    refreshError = refreshFailure instanceof Error ? refreshFailure.message : "Redis connection status could not be loaded.";
  }
  try {
    encryption.value = normalizeEncryption(await getJson<RedisEncryptionStatus>(props.encryptionStatusEndpoint));
  } catch (refreshFailure) {
    const message = refreshFailure instanceof Error ? refreshFailure.message : "Redis encryption status could not be loaded.";
    refreshError = refreshError ? `${refreshError} ${message}` : message;
  } finally {
    refreshing.value = false;
  }
  if (refreshError) {
    error.value = refreshError;
    if (!silent) emit("notify", refreshError, "error");
  } else if (!silent) {
    notice.value = "Redis status refreshed.";
    error.value = "";
  }
}

async function testConnection() {
  const validationError = validate();
  if (validationError) {
    setResult(validationError, "error");
    return;
  }
  busyAction.value = "test";
  error.value = "";
  notice.value = "Testing Redis connection…";
  try {
    await postJson<RedisConnectionStatus>(props.configureEndpoint, payload(true));
    setResult("Redis connection test succeeded.");
  } catch (testError) {
    setResult(testError instanceof Error ? `Redis test failed: ${testError.message}` : "Redis connection test failed.", "error");
  } finally {
    busyAction.value = "";
  }
}

async function save() {
  const validationError = validate();
  if (validationError) {
    setResult(validationError, "error");
    return;
  }
  busyAction.value = "save";
  error.value = "";
  notice.value = "Saving Redis settings…";
  try {
    const result = await postJson<RedisConnectionStatus>(props.configureEndpoint, payload(false));
    dirty.value = false;
    applyStatus(result, true);
    setResult(`Redis settings saved.${replayMessage(result.bootstrap_replay)}`);
    await refresh(true, true);
  } catch (saveError) {
    setResult(saveError instanceof Error ? `Redis save failed: ${saveError.message}` : "Redis settings could not be saved.", "error");
  } finally {
    busyAction.value = "";
  }
}

async function migrateToInternal() {
  if (!canMigrate.value) return;
  const destination = text(draft.data_path) || "the default internal Redis store";
  if (!window.confirm(`Migrate the connected external Redis database into ${destination} and switch Tater to internal Redis? The target internal DB will be replaced.`)) return;
  busyAction.value = "migrate";
  error.value = "";
  notice.value = "Pausing active runtimes and migrating Redis data…";
  try {
    const result = await postJson<RedisConnectionStatus>(props.migrateEndpoint, {
      data_path: text(draft.data_path),
      flush_internal: true,
    });
    dirty.value = false;
    applyStatus((result.redis_status as RedisConnectionStatus | undefined) || result, true);
    if (result.encryption_status) encryption.value = normalizeEncryption(result.encryption_status as RedisEncryptionStatus);
    const migration = result.migration && typeof result.migration === "object" ? result.migration as Record<string, unknown> : {};
    setResult(`Migrated ${Number(migration.keys_restored || 0)} Redis key(s) and switched to internal Redis.${replayMessage(result.bootstrap_replay)}`);
    await refresh(true, true);
  } catch (migrationError) {
    setResult(migrationError instanceof Error ? `Redis migration failed: ${migrationError.message}` : "Redis migration failed.", "error");
  } finally {
    busyAction.value = "";
  }
}

async function encryptLive() {
  busyAction.value = "encrypt";
  error.value = "";
  notice.value = "Pausing active runtimes and encrypting Redis values…";
  try {
    const result = await postJson<Record<string, unknown>>(props.encryptEndpoint);
    encryption.value = normalizeEncryption(result.encryption_status as RedisEncryptionStatus | undefined);
    const keyNote = result.key_created ? " A new encryption key was generated." : "";
    setResult(`Encrypted ${Number(result.keys_encrypted || 0)} Redis value(s); live encryption is enabled.${keyNote}`);
    await refresh(true, true);
  } catch (encryptError) {
    setResult(encryptError instanceof Error ? `Redis encryption failed: ${encryptError.message}` : "Redis encryption failed.", "error");
  } finally {
    busyAction.value = "";
  }
}

async function decryptLive() {
  if (!window.confirm("Decrypt live Redis values now? Future writes will return to plaintext.")) return;
  busyAction.value = "decrypt";
  error.value = "";
  notice.value = "Pausing active runtimes and decrypting Redis values…";
  try {
    const result = await postJson<Record<string, unknown>>(props.decryptEndpoint);
    encryption.value = normalizeEncryption(result.encryption_status as RedisEncryptionStatus | undefined);
    setResult(`Decrypted ${Number(result.restored_keys || 0)} Redis value(s); live encryption is disabled.${replayMessage(result.bootstrap_replay)}`);
    await refresh(true, true);
  } catch (decryptError) {
    setResult(decryptError instanceof Error ? `Redis decrypt failed: ${decryptError.message}` : "Redis decryption failed.", "error");
  } finally {
    busyAction.value = "";
  }
}

const internalMode = computed(() => draft.mode === "internal");
const connected = computed(() => Boolean(status.value.connected));
const canMigrate = computed(() => connected.value && !status.value.internal && status.value.mode === "external" && !busyAction.value);
const encryptionEnabled = computed(() => Boolean(encryption.value.live_encryption_enabled));
const encryptionAvailable = computed(() => encryption.value.encryption_available !== false && !encryption.value.error);
const connectionMessage = computed(() => {
  if (status.value.internal) {
    if (connected.value) return status.value.data_path ? `Internal Redis is connected at ${status.value.data_path}.` : "Internal Redis is connected.";
    return status.value.error ? `Internal Redis could not start: ${status.value.error}` : "Internal Redis is not running.";
  }
  if (!status.value.configured) return "External Redis is not configured yet.";
  if (!connected.value) return status.value.error ? `External Redis is unavailable: ${status.value.error}` : "External Redis is unavailable.";
  return `External Redis is connected at ${status.value.host}:${status.value.port}.`;
});
const encryptionMessage = computed(() => {
  if (!encryptionAvailable.value) return text(encryption.value.error) || "Redis encryption tools are unavailable.";
  if (encryptionEnabled.value) {
    const fingerprint = encryption.value.key_fingerprint ? ` Key ${encryption.value.key_fingerprint}.` : "";
    return `Live Redis encryption is enabled.${fingerprint}`;
  }
  if (encryption.value.key_exists) return encryption.value.key_fingerprint
    ? `Encryption key ready (${encryption.value.key_fingerprint}). Live encryption is disabled.`
    : "Encryption key ready. Live encryption is disabled.";
  return "No encryption key exists yet. Encrypting live Redis will create one automatically.";
});
const storageLabel = computed(() => status.value.internal ? "Internal" : text(status.value.host) || "External");

watch(() => props.initialStatus, (next) => applyStatus(next || {}, !dirty.value), { immediate: true });
watch(() => props.initialEncryptionStatus, (next) => { encryption.value = normalizeEncryption(next); }, { immediate: true });

onMounted(() => {
  void refresh(true);
  refreshTimer = window.setInterval(() => {
    if (document.visibilityState === "visible" && !busyAction.value) void refresh(true);
  }, 10000);
});

onBeforeUnmount(() => {
  if (refreshTimer !== null) window.clearInterval(refreshTimer);
});
</script>

<template>
  <section class="tset-resource tredis-resource">
    <section class="tv-panel tredis-hero">
      <div>
        <span class="tv-eyebrow">Live data service</span>
        <h2>Redis connection and encryption</h2>
        <p>Connection health refreshes automatically while unsaved form edits stay untouched.</p>
      </div>
      <div class="tredis-hero-actions">
        <span class="tv-live-pill" :class="{ warning: !connected }"><i />{{ refreshing ? "Updating" : connected ? "Connected" : "Needs attention" }}</span>
        <button class="tv-button" type="button" :disabled="Boolean(busyAction) || refreshing" @click="refresh(false)">Refresh</button>
      </div>
    </section>

    <div class="tm-metrics tredis-metrics">
      <article><span>Mode</span><strong>{{ internalMode ? "Internal" : "External" }}</strong></article>
      <article><span>Connection</span><strong>{{ connected ? "Healthy" : "Unavailable" }}</strong></article>
      <article><span>Store</span><strong>{{ storageLabel }}</strong></article>
      <article><span>Encryption</span><strong>{{ encryptionEnabled ? "Enabled" : "Disabled" }}</strong></article>
    </div>

    <div v-if="notice || error" class="tv-notice" :class="{ error: Boolean(error) }" aria-live="polite">{{ error || notice }}</div>
    <div v-if="status.fallback_reason" class="tv-notice error" aria-live="polite">
      Redis recovered to the internal store: {{ status.fallback_reason }}
    </div>

    <div class="tredis-grid">
      <section class="tv-panel tset-form-card tredis-connection-card">
        <header class="tv-panel-head">
          <div>
            <span class="tv-eyebrow">Connection</span>
            <h2>Redis server</h2>
            <p>{{ connectionMessage }}</p>
          </div>
          <span class="tredis-state" :class="{ connected }"><i />{{ connected ? "Live" : "Offline" }}</span>
        </header>

        <div class="tv-form-grid">
          <label>
            Mode
            <select v-model="draft.mode" :disabled="Boolean(busyAction)" @change="markDirty">
              <option value="internal">Internal</option>
              <option value="external">External</option>
            </select>
          </label>
          <label v-if="internalMode">
            Internal data file
            <input v-model="draft.data_path" type="text" :disabled="Boolean(busyAction)" @input="markDirty" />
          </label>
          <template v-else>
            <label>
              Host
              <input v-model="draft.host" type="text" :disabled="Boolean(busyAction)" @input="markDirty" />
            </label>
            <label>
              Port
              <input v-model.number="draft.port" type="number" min="1" max="65535" :disabled="Boolean(busyAction)" @input="markDirty" />
            </label>
            <label>
              Username <small>Optional</small>
              <input v-model="draft.username" type="text" autocomplete="username" :disabled="Boolean(busyAction)" @input="markDirty" />
            </label>
            <label>
              Password <small>{{ status.password_set ? "Leave blank to keep the saved password" : "Optional" }}</small>
              <input v-model="draft.password" type="password" autocomplete="new-password" :placeholder="status.password_set ? 'Saved password will be kept' : ''" :disabled="Boolean(busyAction)" @input="markDirty" />
            </label>
          </template>
          <label>
            Database
            <input v-model.number="draft.db" type="number" min="0" :disabled="Boolean(busyAction)" @input="markDirty" />
          </label>
          <template v-if="!internalMode">
            <label class="tv-toggle tredis-toggle">
              <input v-model="draft.use_tls" class="tv-checkbox" type="checkbox" :disabled="Boolean(busyAction)" @change="markDirty" />
              <span><strong>Use TLS</strong><small>Encrypt the Redis network connection.</small></span>
            </label>
            <label class="tv-toggle tredis-toggle">
              <input v-model="draft.verify_tls" class="tv-checkbox" type="checkbox" :disabled="Boolean(busyAction) || !draft.use_tls" @change="markDirty" />
              <span><strong>Verify TLS certificate</strong><small>Recommended for external Redis.</small></span>
            </label>
            <label v-if="draft.use_tls" class="full">
              CA certificate path <small>Optional</small>
              <input v-model="draft.ca_cert_path" type="text" :disabled="Boolean(busyAction)" @input="markDirty" />
            </label>
          </template>
        </div>

        <div class="tredis-card-actions">
          <button class="tv-button" type="button" :disabled="Boolean(busyAction)" @click="testConnection">
            {{ busyAction === "test" ? "Testing…" : "Test Connection" }}
          </button>
          <button class="tv-button" type="button" :disabled="!canMigrate" :title="status.internal ? 'Redis is already internal.' : !connected ? 'Connect external Redis before migrating.' : ''" @click="migrateToInternal">
            {{ busyAction === "migrate" ? "Migrating…" : "Migrate to Internal" }}
          </button>
        </div>
      </section>

      <section class="tv-panel tset-form-card tredis-encryption-card">
        <header class="tv-panel-head">
          <div>
            <span class="tv-eyebrow">At-rest protection</span>
            <h2>Live value encryption</h2>
            <p>{{ encryptionMessage }}</p>
          </div>
          <span class="tredis-state" :class="{ connected: encryptionEnabled }"><i />{{ encryptionEnabled ? "Encrypted" : "Plaintext" }}</span>
        </header>

        <dl class="tredis-details">
          <div><dt>Key file</dt><dd>{{ encryption.key_path || "Not available" }}</dd></div>
          <div><dt>Mode state file</dt><dd>{{ encryption.live_encryption_state_path || "Not available" }}</dd></div>
          <div><dt>Last updated</dt><dd>{{ encryption.live_encryption_updated || "Not recorded" }}</dd></div>
          <div><dt>Key fingerprint</dt><dd>{{ encryption.key_fingerprint || "Not created" }}</dd></div>
        </dl>

        <p class="tredis-help">Encrypt transforms existing values in place, creates a key when needed, and encrypts future writes. Decrypt reverses the values and returns future writes to plaintext.</p>
        <div class="tredis-card-actions">
          <button class="tv-button" type="button" :disabled="Boolean(busyAction) || !connected || !encryptionAvailable || encryptionEnabled" @click="encryptLive">
            {{ busyAction === "encrypt" ? "Encrypting…" : "Encrypt Live Redis" }}
          </button>
          <button class="tv-button danger" type="button" :disabled="Boolean(busyAction) || !connected || !encryptionAvailable || !encryptionEnabled" @click="decryptLive">
            {{ busyAction === "decrypt" ? "Decrypting…" : "Decrypt Live Redis" }}
          </button>
        </div>
      </section>

      <section class="tv-panel tredis-runtime-card">
        <header>
          <span class="tv-eyebrow">Runtime details</span>
          <h2>Storage health</h2>
          <p>Useful paths and process details for recovery and diagnostics.</p>
        </header>
        <dl class="tredis-details compact">
          <div><dt>Configuration source</dt><dd>{{ status.source || "Unknown" }}</dd></div>
          <div><dt>Configuration file</dt><dd>{{ status.config_path || "Not reported" }}</dd></div>
          <div><dt>Data directory</dt><dd>{{ status.data_dir || "External service" }}</dd></div>
          <div><dt>Socket</dt><dd>{{ status.socket_path || "TCP connection" }}</dd></div>
          <div><dt>Managed process</dt><dd>{{ status.redis_managed ? "Yes" : "No" }}</dd></div>
          <div><dt>Process ID</dt><dd>{{ status.redis_pid || "Not reported" }}</dd></div>
        </dl>
      </section>
    </div>

    <section class="tset-save-bar tredis-save-bar">
      <div>
        <strong>{{ dirty ? "Unsaved Redis changes" : "Redis settings are synchronized" }}</strong>
        <span>Live health continues to refresh without replacing edits in progress.</span>
      </div>
      <button class="tv-button primary" type="button" :disabled="Boolean(busyAction) || !dirty" @click="save">
        {{ busyAction === "save" ? "Saving…" : "Save Redis Settings" }}
      </button>
    </section>
  </section>
</template>
