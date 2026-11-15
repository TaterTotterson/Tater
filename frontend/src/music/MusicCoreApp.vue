<script setup lang="ts">
import { computed, onActivated, onBeforeUnmount, onDeactivated, onMounted, ref, watch } from "vue";
import { fetchMusicState, runMusicAction } from "./api";
import {coreWorkspace,workspaceTabs,initialWorkspaceTab} from "../cores/workspace";
import {provideCoreDrafts} from "../cores/drafts";
import LibraryBrowser from "./components/LibraryBrowser.vue";
import MusicPlayer from "./components/MusicPlayer.vue";
import RecommendationsBrowser from "./components/RecommendationsBrowser.vue";
import SettingsCard from "./components/SettingsCard.vue";
import TrackList from "./components/TrackList.vue";
import type { CoreTabPayload, MusicCoreMountOptions, MusicItem } from "./types";

const props = defineProps<{
  state: { payload: CoreTabPayload };
  options: MusicCoreMountOptions;
}>();

provideCoreDrafts();
const metadata=computed(()=>coreWorkspace(props.options.coreKey,ui.value.workspace));
const selectedTab = ref("");
const busyKeys = ref(new Set<string>());
const errorMessage = ref("");
const successMessage = ref("");
const eventStatus = ref<"connecting" | "live" | "offline">("connecting");
let eventSource: EventSource | null = null;
let reconnectTimer = 0;
let suspended=false;let disposed=false;

const payload = computed(() => props.state.payload || {});
const ui = computed(() => payload.value.ui || {});
const items = computed(() => ui.value.item_forms || []);
const player = computed(() => items.value.find((item) => item.group === "player"));
const upNext = computed(() => {
  const tracks = player.value?.track_list || [];
  const current = tracks.findIndex(track => track.active);
  return tracks.slice(current + 1, current + 6);
});
const managerTabs = computed(()=>workspaceTabs(ui.value.manager_tabs,metadata.value));
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
      selectedTab.value=initialWorkspaceTab(tabs,metadata.value,ui.value.default_tab);
    }
  },
  { immediate: true, deep: true },
);

watch(
  activeLibraryGroups,
  (groups) => {
    if (!groups.some((group: {key:string}) => group.key === selectedLibraryGroup.value)) {
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
  const result=await fetchMusicState(props.options.tabEndpoint);if(!disposed)props.state.payload=result;
}

async function run(
  action: string,
  actionPayload: Record<string, unknown>,
  busyKey = action,
): Promise<boolean> {
  if (!action || isBusy(busyKey)) return false;
  errorMessage.value = "";
  successMessage.value = "";
  setBusy(busyKey, true);
  try {
    const result = await runMusicAction(props.options.actionEndpoint, action, actionPayload);
    if (result.ok === false) {
      throw new Error(String(result.message || result.detail || "Music action failed."));
    }
    await refreshState();
    const message = typeof result.message === "string" ? result.message.trim() : "";
    if (message) {
      if (props.options.onToast) props.options.onToast(message, "success");
      else successMessage.value = message;
    }
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
  if(suspended||disposed)return;
  eventStatus.value = "connecting";
  const source=new EventSource(props.options.eventsEndpoint);eventSource=source;
  source.addEventListener("core-tab", (event) => {
    if(suspended||disposed||eventSource!==source)return;
    try {
      props.state.payload = JSON.parse((event as MessageEvent<string>).data) as CoreTabPayload;
      eventStatus.value = "live";
    } catch {
      // Ignore one malformed event and keep the current player state.
    }
  });
  source.addEventListener("open", () => {
    if(eventSource===source)eventStatus.value = "live";
  });
  source.addEventListener("error", () => {
    if(eventSource!==source)return;
    eventStatus.value = "offline";
    eventSource?.close();
    eventSource = null;
    reconnectTimer = window.setTimeout(connectEvents, 3000);
  });
}

function disconnectEvents(){if(reconnectTimer)window.clearTimeout(reconnectTimer);reconnectTimer=0;eventSource?.close();eventSource=null;}
onDeactivated(()=>{suspended=true;disconnectEvents();});
onActivated(()=>{if(!suspended)return;suspended=false;void refreshState().catch(()=>{});connectEvents();});
onMounted(connectEvents);
onBeforeUnmount(() => {
  disposed=true;
  if (reconnectTimer) window.clearTimeout(reconnectTimer);
  eventSource?.close();
  eventSource = null;
});
</script>

<template>
  <main class="tater-music-core" :class="{'cw-music-embedded':options.embedded}">
    <div v-if="payload.error" class="tm-error">{{ payload.error }}</div>
    <template v-else>
      <header class="tm-page-heading">
        <div v-if="!options.embedded">
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

      <section class="workspace-music-navigation" aria-label="Music navigation">

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

      <div class="cw-music-workspace" data-workspace-layout="music"><div class="cw-music-main">
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
      <TrackList
        v-else-if="activeManagerTab?.source === 'player_queue' && player"
        :item="player"
        :busy="isBusy"
        :run="run"
      />
      <section v-else class="tm-settings-grid" :class="`group-${activeManagerTab?.item_group || 'all'}`">
        <SettingsCard
          v-for="item in activeItems"
          :key="item.id"
          :item="item"
          :busy="isBusy"
          :run="run"
          :fields-popup="Boolean(ui.item_fields_popup) && item.fields_popup !== false"
          :fields-dropdown="Boolean(item.fields_dropdown ?? ui.item_fields_dropdown)"
          :dropdown-label="item.fields_dropdown_label || ui.item_fields_dropdown_label || 'Connection settings'"
          :popup-label="item.settings_label || ui.item_fields_popup_label || 'Settings'"
        />
        <div v-if="!activeItems.length" class="tm-empty">
          {{ activeManagerTab?.empty_message || payload.empty_message || 'Nothing is available here yet.' }}
        </div>
      </section>

      </div><aside v-if="player" class="cw-purpose-context cw-music-context"><h3>Up next</h3><p>{{ player.title }}</p><small>{{ player.subtitle||player.detail }}</small><ol><li v-for="(track,index) in upNext" :key="track.id||index"><strong>{{ track.title||'Untitled' }}</strong><span>{{ [track.artist,track.album].filter(Boolean).join(' · ') }}</span></li></ol><button v-if="managerTabs.some(tab=>tab.key==='playlist')" class="cw-text-button" type="button" @click="selectedTab='playlist'">View playlist →</button></aside></div>

      <section v-if="player" class="tm-playback-dock workspace-music-player" aria-label="Playback"><MusicPlayer :item="player" :busy="isBusy" :run="run" /></section>

      <div v-if="errorMessage" class="tm-error-toast" role="alert">
        <span>{{ errorMessage }}</span>
        <button type="button" aria-label="Dismiss" @click="errorMessage = ''">×</button>
      </div>
      <div v-if="successMessage" class="tm-success-toast" role="status">
        <span>{{ successMessage }}</span>
        <button type="button" aria-label="Dismiss" @click="successMessage = ''">×</button>
      </div>
    </template>
  </main>
</template>
