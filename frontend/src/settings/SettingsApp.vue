<script setup lang="ts">
import { computed, nextTick, ref } from "vue";
import { settingsCategories, settingsPages, settingsPageForLegacy, searchSettings } from "./navigation";
import type { SettingsCategory, SettingsPage } from "./navigation";
import SettingsIcon from "./components/SettingsIcon.vue";
import { provideSettingsWorkspace } from "./focusedWorkspace";
import PopupTransition from "../shared/PopupTransition.vue";
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

function legacyPage(tab: unknown, childTab = "") {
  const key = String(tab || "").trim().toLowerCase();
  const child = childTab || (key === "models" ? props.options.initialModelsTab : key === "esphome" ? props.options.initialVoiceTab : "");
  return settingsPageForLegacy(key, child);
}

const activePage = ref<SettingsPage>(settingsPages.find(page => page.id === props.options.initialPage) || legacyPage(props.options.initialTab));
const activeTab = computed(() => activePage.value.panel);
const activeCategory = computed(() => settingsCategories.find(category => category.id === activePage.value.category)!);
const categoryPages = computed(() => settingsPages.filter(page => page.category === activeCategory.value.id));
const sections = computed(() => categoryPages.value.filter((page, index, pages) => pages.findIndex(item => item.section === page.section) === index));
const sectionPages = computed(() => categoryPages.value.filter(page => page.section === activePage.value.section));
const visited = new Map<SettingsCategory, SettingsPage>();
const searchQuery = ref("");
const searchResults = computed(() => searchSettings(searchQuery.value));
const summary = computed(() => props.state.summary || {});
const activePanel = ref<{ hasUnsavedChanges?: () => boolean } | null>(null);
const panelRevision = ref(0);
const pendingNavigation = ref<{ page: SettingsPage; notify: boolean } | null>(null);
const focusedWorkspace = provideSettingsWorkspace();
const { target: workspaceTarget, active: activeWorkspace } = focusedWorkspace;

function selectPage(page: SettingsPage, notify = true) {
  if (page.id !== activePage.value.id && activePanel.value?.hasUnsavedChanges?.()) {
    pendingNavigation.value = { page, notify };
    return;
  }
  commitPage(page, notify);
}

function commitPage(page: SettingsPage, notify: boolean) {
  focusedWorkspace.close.value?.();
  visited.set(activePage.value.category, activePage.value);
  activePage.value = page;
  searchQuery.value = "";
  props.options.onPageChange?.(page.id);
  if (notify) props.options.onTabChange?.(page.panel);
  if (props.options.onExitSettings) void nextTick(() => window.scrollTo({ top: 0, behavior: "auto" }));
}

function discardAndNavigate() {
  const pending = pendingNavigation.value;
  if (!pending) return;
  pendingNavigation.value = null;
  panelRevision.value += 1;
  commitPage(pending.page, pending.notify);
}

function selectPageFromInput(event: Event) {
  const input = event.target as HTMLSelectElement;
  const page = sectionPages.value.find(item => item.id === input.value);
  if (page) selectPage(page);
  input.value = activePage.value.id;
}

function select(tab: string, notify = false, childTab = "") {
  const directPage = settingsPages.find(page => page.id === tab && page.panel !== tab);
  selectPage(directPage || legacyPage(tab, childTab), notify);
}

function selectCategory(category: SettingsCategory) {
  selectPage(visited.get(category) || settingsPages.find(page => page.category === category)!);
}

function modelsTabChanged(child: string) {
  props.options.onModelsTabChange?.(child);
  if (activeTab.value === "models") activePage.value = settingsPageForLegacy("models", child);
}

function voiceTabChanged(child: string) {
  props.options.onVoiceTabChange?.(child);
  if (activeTab.value === "esphome") activePage.value = settingsPageForLegacy("esphome", child);
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
    <aside class="tset-sidebar">
      <button v-if="options.onExitSettings" class="tset-back" type="button" @click="options.onExitSettings()"><SettingsIcon name="back" />Back to {{ state.general.tater_first_name || 'Tater' }}</button>
      <div class="tset-sidebar-label">Your {{ state.general.tater_first_name || 'Tater' }}</div>
      <nav class="tset-category-nav" aria-label="Settings categories">
        <button v-for="category in settingsCategories" :key="category.id" type="button" :aria-current="activeCategory.id === category.id ? 'page' : undefined" @click="selectCategory(category.id)"><SettingsIcon :name="category.icon" /><span>{{ category.label }}</span></button>
      </nav>
      <div class="tset-service-summary">
        <button type="button" :class="{ warning: !summary.redisConnected }" @click="selectPage(settingsPageForLegacy('redis'))"><SettingsIcon name="check" />{{ summary.redisConnected ? 'Services connected' : 'Redis needs attention' }}</button>
        <span>Diagnostics live in System</span>
      </div>
    </aside>
    <section class="tset-main" aria-label="Settings content">
      <div v-show="!activeWorkspace" :inert="Boolean(activeWorkspace)" class="tset-workspace-origin">
      <label class="tset-search"><SettingsIcon name="search" /><input v-model="searchQuery" type="search" aria-label="Search all settings" placeholder="Search settings… try “wake word”" autocomplete="off" /><button v-if="searchQuery" type="button" aria-label="Clear settings search" @click="searchQuery = ''">×</button></label>
      <section v-if="searchQuery.trim()" class="tset-search-results" aria-label="Settings search results">
        <header class="tset-page-heading"><h1>Search settings</h1><p role="status">{{ searchResults.length }} {{ searchResults.length === 1 ? 'result' : 'results' }} for “{{ searchQuery.trim() }}”</p></header>
        <div v-if="searchResults.length" class="tset-result-list">
          <button v-for="page in searchResults" :key="page.id" type="button" @click="selectPage(page)"><span><strong>{{ page.label }}</strong><small>{{ settingsCategories.find(category => category.id === page.category)?.label }} · {{ page.description }}</small></span><SettingsIcon name="chevron" /></button>
        </div>
        <p v-else class="tv-empty">Try a setting name such as “wake word,” “password,” “Hydra,” or “Redis.”</p>
      </section>
      <div v-show="!searchQuery.trim()" class="tset-page-content">
        <header class="tset-page-heading"><h1>{{ activeCategory.label }}</h1><p>{{ activeCategory.description }}</p></header>
        <nav v-if="sections.length > 1" class="tset-section-nav" :aria-label="`${activeCategory.label} sections`">
          <button v-for="section in sections" :key="section.section" type="button" :aria-current="activePage.section === section.section ? 'page' : undefined" :data-settings-vue-tab="section.panel" @click="selectPage(section)">{{ section.sectionLabel }}</button>
        </nav>
        <nav v-if="sectionPages.length > 1" class="tset-subsection-nav" :aria-label="`${activePage.sectionLabel} settings pages`">
          <button v-for="page in sectionPages" :key="page.id" type="button" :aria-current="activePage.id === page.id ? 'page' : undefined" @click="selectPage(page)">{{ page.label }}</button>
        </nav>
        <div :key="panelRevision" class="tset-active-panel">
            <GeneralSettingsPanel
              v-if="activeTab === 'general'"
              ref="activePanel"
              :settings="state.general"
              :section="activePage.id === 'login' ? 'login' : 'general'"
              :endpoint="options.endpoints.general"
              :on-theme-preview="options.onThemePreview"
              @saved="applyGeneral"
              @notify="notify"
            />

            <HydraSettingsPanel
              v-else-if="activeTab === 'hydra'"
              ref="activePanel"
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
              ref="activePanel"
              embedded
              :settings="state.models"
              :endpoints="options.endpoints"
              :initial-tab="activePage.child"
              :on-tab-change="modelsTabChanged"
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
              ref="activePanel"
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
              ref="activePanel"
              embedded
              :runtime-endpoint="options.endpoints.voiceRuntime"
              :action-endpoint="options.endpoints.voiceAction"
              :presence-endpoint="options.endpoints.voicePresence"
              :presence-events-endpoint="options.endpoints.voicePresenceEvents"
              :initial-tab="activePage.child"
              :on-tab-change="voiceTabChanged"
              @notify="notify"
            />

            <RedisSettingsPanel
              v-else-if="activeTab === 'redis'"
              ref="activePanel"
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
              ref="activePanel"
              :settings="state.misc"
              :endpoint="options.endpoints.misc"
              @saved="applyMisc"
              @notify="notify"
            />

            <AdvancedSettingsPanel
              v-else-if="activeTab === 'advanced'"
              ref="activePanel"
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
      </div>
      </div>
      <div ref="workspaceTarget" v-show="Boolean(activeWorkspace)" class="tset-workspace-host" />
    </section>
    <PopupTransition :open="Boolean(pendingNavigation)" backdrop-class="tv-modal-backdrop tset-modal" @close="pendingNavigation = null">
      <section class="tv-modal tset-unsaved-dialog" role="dialog" aria-modal="true" aria-labelledby="tset-unsaved-title">
        <header><h2 id="tset-unsaved-title">Unsaved settings</h2><p>Save your changes before moving to {{ pendingNavigation?.page.label }}. You can also discard them and continue.</p></header>
        <footer><button class="tv-button" type="button" @click="pendingNavigation = null">Keep editing</button><button class="tv-button danger" type="button" @click="discardAndNavigate">Discard and continue</button></footer>
      </section>
    </PopupTransition>
  </div>
</template>
