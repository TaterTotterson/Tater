<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import { postJson } from "../../shared/api";
import type { AdvancedSettings, JsonRow } from "../types";

const props = defineProps<{
  settings: AdvancedSettings;
  endpoint: string;
  clearChatEndpoint: string;
  chatApiUrl: string;
  modelsApiUrl: string;
}>();

const emit = defineEmits<{
  saved: [settings: AdvancedSettings];
  notify: [message: string, tone?: string];
}>();

const draft = reactive({
  tater_api_enabled: false,
  tater_api_mode: "direct",
  tater_api_hydra_tools_enabled: false,
});
const apiKey = ref("");
const apiKeyVisible = ref(false);
const selectedAdmins = ref<string[]>([]);
const saving = ref(false);
const clearingChat = ref(false);
const dirty = ref(false);
const error = ref("");
const notice = ref("");

const adminOptions = computed(() => {
  const values = new Set([
    ...(Array.isArray(props.settings.admin_plugin_options) ? props.settings.admin_plugin_options : []),
    ...(Array.isArray(props.settings.admin_only_plugins) ? props.settings.admin_only_plugins : []),
    ...(Array.isArray(props.settings.admin_only_plugins_defaults) ? props.settings.admin_only_plugins_defaults : []),
  ].map((value) => String(value || "").trim()).filter(Boolean));
  return [...values].sort((left, right) => left.localeCompare(right));
});

const apiKeyStatus = computed(() => {
  if (apiKey.value) return "A replacement key is ready. Save to activate it.";
  return props.settings.tater_api_key_set
    ? "A key is saved. Tater will never show it back here."
    : "No key is saved yet.";
});

function syncFromSettings(settings: AdvancedSettings) {
  Object.assign(draft, {
    tater_api_enabled: Boolean(settings.tater_api_enabled),
    tater_api_mode: String(settings.tater_api_mode || "direct"),
    tater_api_hydra_tools_enabled: Boolean(settings.tater_api_hydra_tools_enabled),
  });
  selectedAdmins.value = Array.isArray(settings.admin_only_plugins)
    ? settings.admin_only_plugins.map((value) => String(value || "").trim()).filter(Boolean)
    : [];
  apiKey.value = "";
  apiKeyVisible.value = false;
  dirty.value = false;
}

function markDirty() {
  dirty.value = true;
  error.value = "";
  notice.value = "";
}

function generateKey() {
  const bytes = new Uint8Array(32);
  if (window.crypto?.getRandomValues) window.crypto.getRandomValues(bytes);
  else bytes.forEach((_, index) => { bytes[index] = Math.floor(Math.random() * 256); });
  apiKey.value = `tater-${Array.from(bytes).map((value) => value.toString(16).padStart(2, "0")).join("")}`;
  apiKeyVisible.value = true;
  markDirty();
}

function useAdminDefaults() {
  selectedAdmins.value = Array.isArray(props.settings.admin_only_plugins_defaults)
    ? [...props.settings.admin_only_plugins_defaults]
    : [];
  markDirty();
}

async function save() {
  saving.value = true;
  error.value = "";
  notice.value = "";
  const payload: Record<string, unknown> = {
    tater_api_enabled: Boolean(draft.tater_api_enabled),
    tater_api_mode: draft.tater_api_mode,
    tater_api_hydra_tools_enabled: Boolean(draft.tater_api_hydra_tools_enabled),
    admin_only_plugins: [...selectedAdmins.value],
  };
  if (apiKey.value.trim()) payload.tater_api_key = apiKey.value.trim();
  try {
    const next = await postJson<AdvancedSettings>(props.endpoint, payload);
    emit("saved", next);
    syncFromSettings(next);
    notice.value = "Advanced settings saved and synchronized.";
    emit("notify", notice.value, "success");
  } catch (saveError) {
    error.value = saveError instanceof Error ? saveError.message : "Advanced settings could not be saved.";
    emit("notify", error.value, "error");
  } finally {
    saving.value = false;
  }
}

async function clearChat() {
  if (!window.confirm("Clear chat history and uploaded chat attachments now?")) return;
  clearingChat.value = true;
  error.value = "";
  try {
    await postJson<JsonRow>(props.clearChatEndpoint);
    notice.value = "Chat history and uploaded attachments cleared.";
    emit("notify", notice.value, "success");
  } catch (clearError) {
    error.value = clearError instanceof Error ? clearError.message : "Chat history could not be cleared.";
    emit("notify", error.value, "error");
  } finally {
    clearingChat.value = false;
  }
}

watch(
  () => props.settings,
  (settings) => {
    if (!dirty.value) syncFromSettings(settings || {});
  },
  { immediate: true },
);
</script>

<template>
  <section class="tset-resource">
    <div v-if="notice || error" class="tv-notice" :class="{ error: Boolean(error) }" aria-live="polite">
      {{ error || notice }}
    </div>

    <div class="tset-resource-grid">
      <section class="tv-panel tset-form-card">
        <header>
          <span class="tv-eyebrow">OpenAI-compatible API</span>
          <h2>Local application access</h2>
          <p>Allow other applications to call Tater’s Base model directly or through Hydra.</p>
        </header>
        <div class="tadvanced-api-guide">
          <div class="tadvanced-api-guide-title">
            <span>Quick setup</span>
            <strong>The request’s model chooses the route</strong>
            <small>Send the saved key as <code>Authorization: Bearer …</code></small>
          </div>
          <div>
            <code>tater/base</code>
            <span><strong>Direct</strong><small>Uses the Base LLM without Hydra.</small></span>
          </div>
          <div>
            <code>tater/hydra</code>
            <span><strong>Hydra</strong><small>Uses Hydra; tool access follows the setting below.</small></span>
          </div>
          <p><code>GET /v1/models</code> lists available IDs. Other model IDs use <strong>Default mode</strong>.</p>
        </div>
        <div class="tv-form-grid">
          <label class="tv-toggle">
            <input v-model="draft.tater_api_enabled" class="tv-checkbox" type="checkbox" @change="markDirty" />
            <span><strong>Enable API</strong><small>When disabled, the v1 endpoints reject requests.</small></span>
          </label>
          <label>
            Default mode
            <select v-model="draft.tater_api_mode" @change="markDirty">
              <option value="direct">Direct Base LLM</option>
              <option value="hydra">Hydra</option>
            </select>
          </label>
          <label class="full">
            API key
            <div class="tset-input-action">
              <input v-model="apiKey" :type="apiKeyVisible ? 'text' : 'password'" autocomplete="new-password" :placeholder="settings.tater_api_key_set ? 'Enter a new key to replace the saved key' : 'Generate a key before enabling clients'" @input="markDirty" />
              <button class="tv-button" type="button" @click="generateKey">Generate</button>
            </div>
            <small>{{ apiKeyStatus }}</small>
          </label>
          <label class="tv-toggle">
            <input v-model="draft.tater_api_hydra_tools_enabled" class="tv-checkbox" type="checkbox" @change="markDirty" />
            <span><strong>Hydra tool use</strong><small>Only applies to requests running in Hydra mode.</small></span>
          </label>
          <div class="tset-endpoints">
            <span>Chat</span><code>{{ chatApiUrl }}</code>
            <span>Models</span><code>{{ modelsApiUrl }}</code>
          </div>
        </div>
      </section>

      <section class="tv-panel tset-form-card">
        <header>
          <span class="tv-eyebrow">Authorization</span>
          <h2>Admin tool gating</h2>
          <p>Selected Verbas are limited to linked People marked as administrators.</p>
        </header>
        <label class="tset-admin-select">
          Admin-only plugin IDs
          <select v-model="selectedAdmins" multiple size="14" @change="markDirty">
            <option v-for="plugin in adminOptions" :key="plugin" :value="plugin">{{ plugin }}</option>
          </select>
          <small>Kernel tools remain admin-only whenever at least one Person is marked as an administrator.</small>
        </label>
        <button class="tv-button" type="button" @click="useAdminDefaults">Reset to defaults</button>
      </section>

      <section class="tv-panel tset-form-card tset-danger-card">
        <header>
          <span class="tv-eyebrow">Destructive maintenance</span>
          <h2>Clear chat history</h2>
          <p>Deletes stored WebUI messages and uploaded chat attachments.</p>
        </header>
        <button class="tv-button danger" type="button" :disabled="clearingChat" @click="clearChat">{{ clearingChat ? "Clearing…" : "Clear chat history" }}</button>
      </section>
    </div>

    <footer class="tset-save-bar">
      <div><strong>{{ dirty ? "Unsaved changes" : "Advanced settings are synchronized" }}</strong><span>Secrets are write-only and are never returned by the server.</span></div>
      <button class="tv-button primary" type="button" :disabled="saving || !dirty" @click="save">{{ saving ? "Saving…" : "Save advanced settings" }}</button>
    </footer>
  </section>
</template>
