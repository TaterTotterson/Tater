<script setup lang="ts">
import { computed, ref } from "vue";
import type { SettingsMountOptions, SettingsSummary } from "./types";

const props = defineProps<{
  state: { summary: SettingsSummary };
  options: SettingsMountOptions;
}>();

const tabs = [
  { id: "general", label: "General", description: "Identity, login, avatars, and everyday WebUI behavior." },
  { id: "people", label: "People", description: "Recognized people, user records, and identity management." },
  { id: "models", label: "Models", description: "LLM, vision, speech, wake word, speaker, and emotion models." },
  { id: "hydra", label: "Hydra", description: "Model routing, role assignments, fallback behavior, and live metrics." },
  { id: "esphome", label: "Voice", description: "Satellites, firmware, stereo pairs, voice processing, and live controls." },
  { id: "redis", label: "Redis", description: "Data service connection, encryption, recovery, and storage health." },
  { id: "spudhub", label: "Spud Link", description: "Hub, Spudlet, and Little Spud pairing and linked-node management." },
  { id: "misc", label: "Misc", description: "Chat history, attachments, uploads, and other supporting behavior." },
  { id: "advanced", label: "Advanced", description: "Admin-gated tools, limits, security controls, and expert options." },
  { id: "system", label: "System Tasks", description: "Background snapshots, scheduled maintenance, run history, and manual refresh controls." },
  { id: "logs", label: "Logs", description: "Live application logs with filters, pause, copy, and tail controls." },
] as const;

const tabIds = new Set(tabs.map((tab) => tab.id));
const normalize = (value: unknown) => {
  const key = String(value || "").trim().toLowerCase();
  return tabIds.has(key as typeof tabs[number]["id"]) ? key : "general";
};
const activeTab = ref(normalize(props.options.initialTab));
const activeSpec = computed(() => tabs.find((tab) => tab.id === activeTab.value) || tabs[0]);
const summary = computed(() => props.state.summary || {});

function select(tab: string, notify = false) {
  const next = normalize(tab);
  activeTab.value = next;
  if (notify) props.options.onTabChange?.(next);
}

defineExpose({ select: (tab: string) => select(tab, false) });
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
  </div>
</template>
