<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { fetchMusicState, runMusicAction } from "./api";
import LibraryBrowser from "./components/LibraryBrowser.vue";
import MusicPlayer from "./components/MusicPlayer.vue";
import RecommendationsBrowser from "./components/RecommendationsBrowser.vue";
import SettingsCard from "./components/SettingsCard.vue";
import type { CoreTabPayload, MusicCoreMountOptions, MusicItem } from "./types";

const props = defineProps<{
  state: { payload: CoreTabPayload };
  options: MusicCoreMountOptions;
}>();

const selectedTab = ref("");
const busyKeys = ref(new Set<string>());
const errorMessage = ref("");
const eventStatus = ref<"connecting" | "live" | "offline">("connecting");
let eventSource: EventSource | null = null;
let reconnectTimer = 0;

const payload = computed(() => props.state.payload || {});
const ui = computed(() => payload.value.ui || {});
const items = computed(() => ui.value.item_forms || []);
const player = computed(() => items.value.find((item) => item.group === "player"));
const managerTabs = computed(() => ui.value.manager_tabs || []);
const activeManagerTab = computed(() =>
  managerTabs.value.find((tab) => tab.key === selectedTab.value) || managerTabs.value[0],
);
const activeLibraryGroups = computed(() =>
  activeManagerTab.value?.source === "grouped_items" ? activeManagerTab.value.groups || [] : [],
);
const selectedLibraryGroup = ref("");
const activeItems = computed(() => {
  const tab = activeManagerTab.value;
  if (!tab || tab.source === "grouped_items") return [];
  return items.value.filter((item) => !tab.item_group || item.group === tab.item_group);
});

watch(
  managerTabs,
  (tabs) => {
    if (!tabs.some((tab) => tab.key === selectedTab.value)) {
      const preferred = String(ui.value.default_tab || "");
      selectedTab.value = tabs.some((tab) => tab.key === preferred) ? preferred : tabs[0]?.key || "";
    }
  },
  { immediate: true, deep: true },
);

watch(
  activeLibraryGroups,
  (groups) => {
    if (!groups.some((group) => group.key === selectedLibraryGroup.value)) {
      selectedLibraryGroup.value = groups[0]?.key || "";
    }
  },
  { immediate: true, deep: true },
);

function isBusy(key: string): boolean {
  return busyKeys.value.has(key);
}

function setBusy(key: string, active: boolean): void {
  const next = new Set(busyKeys.value);
  if (active) next.add(key);
  else next.delete(key);
  busyKeys.value = next;
}

async function refreshState(): Promise<void> {
  props.state.payload = await fetchMusicState(props.options.tabEndpoint);
}

async function run(
  action: string,
  actionPayload: Record<string, unknown>,
  busyKey = action,
): Promise<boolean> {
  if (!action || isBusy(busyKey)) return false;
  errorMessage.value = "";
  setBusy(busyKey, true);
  try {
    await runMusicAction(props.options.actionEndpoint, action, actionPayload);
    await refreshState();
    return true;
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : String(error || "Music action failed.");
    return false;
  } finally {
    setBusy(busyKey, false);
  }
}

function connectEvents(): void {
  if (eventSource) eventSource.close();
  if (reconnectTimer) window.clearTimeout(reconnectTimer);
  eventStatus.value = "connecting";
  eventSource = new EventSource(props.options.eventsEndpoint);
  eventSource.addEventListener("core-tab", (event) => {
    try {
      props.state.payload = JSON.parse((event as MessageEvent<string>).data) as CoreTabPayload;
      eventStatus.value = "live";
    } catch {
      // Ignore one malformed event and keep the current player state.
    }
  });
  eventSource.addEventListener("open", () => {
    eventStatus.value = "live";
  });
  eventSource.addEventListener("error", () => {
    eventStatus.value = "offline";
    eventSource?.close();
    eventSource = null;
    reconnectTimer = window.setTimeout(connectEvents, 3000);
  });
}

onMounted(connectEvents);
onBeforeUnmount(() => {
  if (reconnectTimer) window.clearTimeout(reconnectTimer);
  eventSource?.close();
  eventSource = null;
});
</script>

<template>
  <main class="tater-music-core">
    <div v-if="payload.error" class="tm-error">{{ payload.error }}</div>
    <template v-else>
      <header class="tm-page-heading">
        <div>
          <div class="tm-eyebrow">Tater Music</div>
          <h1>{{ ui.title || 'Music Core' }}</h1>
          <p>{{ payload.summary }}</p>
        </div>
        <div class="tm-live-state" :class="eventStatus" :title="`Music updates: ${eventStatus}`">
          <span></span>{{ eventStatus === 'live' ? 'Live' : eventStatus === 'connecting' ? 'Connecting' : 'Reconnecting' }}
        </div>
      </header>

      <section v-if="payload.stats?.length" class="tm-stats" aria-label="Music library status">
        <div v-for="stat in payload.stats" :key="stat.label">
          <span>{{ stat.label }}</span>
          <strong>{{ stat.value ?? '—' }}</strong>
        </div>
      </section>

      <section class="tm-playback-dock" :class="{ 'has-player': player }" aria-label="Playback and navigation">
        <MusicPlayer v-if="player" :item="player" :busy="isBusy" :run="run" />

        <nav class="tm-tabs" aria-label="Music Core sections">
          <button
            v-for="tab in managerTabs"
            :key="tab.key"
            type="button"
            :class="{ active: selectedTab === tab.key }"
            @click="selectedTab = tab.key"
          >
            {{ tab.label || tab.key }}
          </button>
        </nav>

        <nav v-if="activeLibraryGroups.length" class="tm-subtabs tm-dock-subtabs" aria-label="Browse music library">
          <button
            v-for="group in activeLibraryGroups"
            :key="group.key"
            type="button"
            :class="{ active: selectedLibraryGroup === group.key }"
            @click="selectedLibraryGroup = group.key"
          >
            {{ group.label || group.key }}
          </button>
        </nav>
      </section>

      <LibraryBrowser
        v-if="activeManagerTab?.source === 'grouped_items'"
        :groups="activeManagerTab.groups || []"
        :items="items"
        :busy="isBusy"
        :run="run"
        :selected-group="selectedLibraryGroup"
        :show-navigation="false"
        @update:selected-group="selectedLibraryGroup = $event"
      />
      <RecommendationsBrowser
        v-else-if="activeManagerTab?.key === 'recommendations'"
        :items="activeItems"
        :busy="isBusy"
        :run="run"
      />
      <section v-else class="tm-settings-grid" :class="`group-${activeManagerTab?.item_group || 'all'}`">
        <SettingsCard v-for="item in activeItems" :key="item.id" :item="item" :busy="isBusy" :run="run" />
        <div v-if="!activeItems.length" class="tm-empty">
          {{ activeManagerTab?.empty_message || payload.empty_message || 'Nothing is available here yet.' }}
        </div>
      </section>

      <div v-if="errorMessage" class="tm-error-toast" role="alert">
        <span>{{ errorMessage }}</span>
        <button type="button" aria-label="Dismiss" @click="errorMessage = ''">×</button>
      </div>
    </template>
  </main>
</template>
