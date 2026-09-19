<script setup lang="ts">
import { computed, nextTick, ref } from "vue";
import AdvancedSettingsPanel from "./components/AdvancedSettings.vue";
import GeneralSettingsPanel from "./components/GeneralSettings.vue";
import HydraSettingsPanel from "./components/HydraSettings.vue";
import LogsSettingsPanel from "./components/LogsSettings.vue";
import MiscSettingsPanel from "./components/MiscSettings.vue";
import ModelsSettingsPanel from "./components/ModelsSettings.vue";
import PeopleSettingsPanel from "./components/PeopleSettings.vue";
import RedisSettingsPanel from "./components/RedisSettings.vue";
import SpudLinkSettingsPanel from "./components/SpudLinkSettings.vue";
import SystemTasksSettingsPanel from "./components/SystemTasksSettings.vue";
import VoiceSettingsPanel from "./components/VoiceSettings.vue";
import type { AdvancedSettings, GeneralSettings, HydraSettings, MiscSettings, ModelsSettingsPayload, PeopleSettingsPayload, RedisConnectionStatus, SettingsMountOptions, SettingsSummary, SpudLinkSettings } from "./types";

const props = defineProps<{
  state: {
    summary: SettingsSummary;
    general: GeneralSettings;
    hydra: HydraSettings;
    misc: MiscSettings;
    people: PeopleSettingsPayload;
    spudLink: SpudLinkSettings;
    models: ModelsSettingsPayload;
    advanced: AdvancedSettings;
  };
  options: SettingsMountOptions;
}>();

const tabs = [
  { id: "general", label: "General", description: "Appearance, identity, login, avatars, and everyday WebUI behavior." },
  { id: "people", label: "People", description: "Recognized people, user records, and identity management." },
  { id: "models", label: "Models", description: "LLM, vision, speech, wake word, speaker, and emotion models." },
  { id: "hydra", label: "Hydra", description: "Model routing, role assignments, fallback behavior, and live metrics." },
  { id: "esphome", label: "Satellites", description: "Pair satellites, install firmware, build stereo pairs, and tune the live voice runtime." },
  { id: "redis", label: "Redis", description: "Data service connection, encryption, recovery, and storage health." },
  { id: "spudhub", label: "Spud Link", description: "Hub, Spudlet, and Little Spud pairing and linked-node management." },
  { id: "misc", label: "Misc", description: "Chat history, attachments, uploads, and other supporting behavior." },
  { id: "advanced", label: "Advanced", description: "Admin-gated tools, limits, security controls, and expert options." },
  { id: "system", label: "System Tasks", description: "Live background snapshots, scheduled maintenance, process health, and run history." },
  { id: "logs", label: "Logs", description: "Live application logs with filters, pause, copy, and tail controls." },
] as const;

const tabIds = new Set(tabs.map((tab) => tab.id));
const normalize = (value: unknown) => {
  const key = String(value || "").trim().toLowerCase();
  return tabIds.has(key as typeof tabs[number]["id"]) ? key : "general";
};
const activeTab = ref(normalize(props.options.initialTab));
const voicePanel = ref<{ select?: (tab: string) => void | Promise<void> } | null>(null);
const activeSpec = computed(() => tabs.find((tab) => tab.id === activeTab.value) || tabs[0]);
const summary = computed(() => props.state.summary || {});

async function select(tab: string, notify = false, childTab = "") {
  const next = normalize(tab);
  activeTab.value = next;
  if (notify) props.options.onTabChange?.(next);
  if (next === "esphome" && childTab) {
    await nextTick();
    await voicePanel.value?.select?.(childTab);
  }
}

function applyGeneral(settings: GeneralSettings) {
  props.state.general = settings;
  props.options.onGeneralChange?.(settings);
}

function applyHydra(settings: HydraSettings) {
  props.state.hydra = settings;
  props.options.onHydraChange?.(settings);
}

function applyMisc(settings: MiscSettings) {
  props.state.misc = settings;
  props.options.onMiscChange?.(settings);
}

function applyPeople(payload: PeopleSettingsPayload) {
  props.state.people = payload;
  props.options.onPeopleChange?.(payload);
}

function applySpudLink(settings: SpudLinkSettings) {
  props.state.spudLink = settings;
  props.options.onSpudLinkChange?.(settings);
}

function applyModels(settings: ModelsSettingsPayload) {
  props.state.models = settings;
  props.options.onModelsChange?.(settings);
}

function applyRedisStatus(status: RedisConnectionStatus) {
  props.state.summary = { ...props.state.summary, redisConnected: Boolean(status.connected) };
  props.options.onRedisStatusChange?.(status);
}

function applyAdvanced(settings: AdvancedSettings) {
  props.state.advanced = settings;
  props.state.summary = {
    ...props.state.summary,
    adminGateCount: Array.isArray(settings.admin_only_plugins) ? settings.admin_only_plugins.length : 0,
  };
  props.options.onAdvancedChange?.(settings);
}

function notify(message: string, tone = "success") {
  props.options.onToast?.(message, tone);
}

defineExpose({ select: (tab: string, childTab = "") => select(tab, false, childTab) });
</script>

<template>
  <div class="tater-vue-surface tset-settings">
    <header class="tv-page-heading">
      <div>
        <span class="tv-eyebrow">Tater configuration</span>
        <h1>Settings</h1>
        <p>Configure identity, intelligence, voice, storage, security, and diagnostics from one workspace.</p>
      </div>
      <div class="tv-heading-actions">
        <span class="tv-live-pill" :class="{ warning: !summary.redisConnected }">
          <i />{{ summary.redisConnected ? "Services connected" : "Redis needs attention" }}
        </span>
      </div>
    </header>

    <div class="tv-metrics tset-metrics">
      <div><span>Redis</span><strong>{{ summary.redisConnected ? "Connected" : "Setup needed" }}</strong></div>
      <div><span>Admin gated</span><strong>{{ Number(summary.adminGateCount || 0) }}</strong></div>
      <div><span>Integrations</span><strong>{{ Number(summary.integrationCount || 0) }}</strong></div>
    </div>

    <nav class="tv-tabs tset-tabs" aria-label="Settings sections">
      <button
        v-for="tab in tabs"
        :key="tab.id"
        type="button"
        :class="{ active: activeTab === tab.id }"
        :data-settings-vue-tab="tab.id"
        @click="select(tab.id, true)"
      >
        {{ tab.label }}
      </button>
    </nav>

    <div class="tset-context" aria-live="polite">
      <span>{{ activeSpec.label }}</span>
      <p>{{ activeSpec.description }}</p>
    </div>

    <GeneralSettingsPanel
      v-if="activeTab === 'general'"
      :settings="state.general"
      :endpoint="options.endpoints.general"
      :on-theme-preview="options.onThemePreview"
      @saved="applyGeneral"
      @notify="notify"
    />

    <HydraSettingsPanel
      v-else-if="activeTab === 'hydra'"
      :settings="state.hydra"
      :endpoint="options.endpoints.hydra"
      :metrics-endpoint="options.endpoints.hydraMetrics"
      :data-endpoint="options.endpoints.hydraData"
      :clear-data-endpoint="options.endpoints.hydraDataClear"
      @saved="applyHydra"
      @notify="notify"
    />

    <ModelsSettingsPanel
      v-else-if="activeTab === 'models'"
      :settings="state.models"
      :endpoints="options.endpoints"
      :initial-tab="options.initialModelsTab"
      :on-tab-change="options.onModelsTabChange"
      @changed="applyModels"
      @notify="notify"
    />

    <PeopleSettingsPanel
      v-else-if="activeTab === 'people'"
      :payload="state.people"
      :endpoint="options.endpoints.people"
      :action-endpoint="options.endpoints.peopleAction"
      :initial-tab="options.initialPeopleTab"
      :initial-sort="options.initialPeopleSort"
      @changed="applyPeople"
      @tab-change="options.onPeopleTabChange?.($event)"
      @sort-change="options.onPeopleSortChange?.($event)"
      @notify="notify"
    />

    <SpudLinkSettingsPanel
      v-else-if="activeTab === 'spudhub'"
      :settings="state.spudLink"
      :endpoint="options.endpoints.spudLink"
      :status-endpoint="options.endpoints.spudLinkStatus"
      :pairing-code-endpoint="options.endpoints.spudLinkPairingCode"
      :connect-endpoint="options.endpoints.spudLinkConnect"
      :revoke-endpoint="options.endpoints.spudLinkRevoke"
      :llm-api-url="options.publicEndpoints.spudLinkLlm"
      :models-api-url="options.publicEndpoints.spudLinkModels"
      :pair-api-url="options.publicEndpoints.spudLinkPair"
      :initial-tab="options.initialSpudLinkTab"
      @changed="applySpudLink"
      @tab-change="options.onSpudLinkTabChange?.($event)"
      @notify="notify"
    />

    <VoiceSettingsPanel
      v-else-if="activeTab === 'esphome'"
      ref="voicePanel"
      :runtime-endpoint="options.endpoints.voiceRuntime"
      :action-endpoint="options.endpoints.voiceAction"
      :presence-endpoint="options.endpoints.voicePresence"
      :presence-events-endpoint="options.endpoints.voicePresenceEvents"
      :initial-tab="options.initialVoiceTab"
      :on-tab-change="options.onVoiceTabChange"
      @notify="notify"
    />

    <RedisSettingsPanel
      v-else-if="activeTab === 'redis'"
      :initial-status="options.initialRedisStatus"
      :initial-encryption-status="options.initialRedisEncryptionStatus"
      :status-endpoint="options.endpoints.redisStatus"
      :configure-endpoint="options.endpoints.redisConfigure"
      :migrate-endpoint="options.endpoints.redisMigrateInternal"
      :encryption-status-endpoint="options.endpoints.redisEncryptionStatus"
      :encrypt-endpoint="options.endpoints.redisEncrypt"
      :decrypt-endpoint="options.endpoints.redisDecrypt"
      @status="applyRedisStatus"
      @notify="notify"
    />

    <MiscSettingsPanel
      v-else-if="activeTab === 'misc'"
      :settings="state.misc"
      :endpoint="options.endpoints.misc"
      @saved="applyMisc"
      @notify="notify"
    />

    <AdvancedSettingsPanel
      v-else-if="activeTab === 'advanced'"
      :settings="state.advanced"
      :endpoint="options.endpoints.advanced"
      :clear-chat-endpoint="options.endpoints.clearChat"
      :chat-api-url="options.publicEndpoints.chat"
      :models-api-url="options.publicEndpoints.models"
      @saved="applyAdvanced"
      @notify="notify"
    />

    <SystemTasksSettingsPanel
      v-else-if="activeTab === 'system'"
      :endpoint="options.endpoints.systemTasks"
      :core-run-endpoint="options.endpoints.coreTaskRun"
      @notify="notify"
    />

    <LogsSettingsPanel
      v-else-if="activeTab === 'logs'"
      :endpoint="options.endpoints.logs"
      :initial-auto-scroll="options.initialLogAutoScroll !== false"
      @auto-scroll-change="options.onLogAutoScrollChange?.($event)"
      @notify="notify"
    />
  </div>
</template>
