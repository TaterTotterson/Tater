<script setup lang="ts">
import {
  computed,
  onActivated,
  onBeforeUnmount,
  onDeactivated,
  onMounted,
  ref,
  watch,
} from "vue";
import { fetchMusicState, runMusicAction } from "./api";
import { provideCoreDrafts } from "../cores/drafts";
import LibraryBrowser from "./components/LibraryBrowser.vue";
import MusicPlayer from "./components/MusicPlayer.vue";
import SpeakersWorkspace from "./components/SpeakersWorkspace.vue";
import RecommendationsBrowser from "./components/RecommendationsBrowser.vue";
import SettingsCard from "./components/SettingsCard.vue";
import TrackList from "./components/TrackList.vue";
import MusicIcon from "./components/MusicIcon.vue";
import { playerFriendlyName } from "./playerDisplay";
import type {
  CoreTabPayload,
  MusicCoreMountOptions,
  MusicTrack,
  MusicBrowseResult,
  MusicItem,
} from "./types";
const props = defineProps<{
  state: { payload: CoreTabPayload };
  options: MusicCoreMountOptions;
}>();
provideCoreDrafts();
const libraryBrowser = ref<InstanceType<typeof LibraryBrowser> | null>(null);
const selectedTab = ref("listen"),
  selectedLibraryGroup = ref("albums"),
  homeQuery = ref("");
const busyKeys = ref(new Set<string>()),
  errorMessage = ref(""),
  successMessage = ref(""),
  eventStatus = ref<"connecting" | "live" | "offline">("connecting");
let eventSource: EventSource | null = null,
  reconnectTimer = 0,
  suspended = false,
  disposed = false;
const payload = computed(() => props.state.payload || {});
const ui = computed(() => payload.value.ui || {});
const items = computed(() => ui.value.item_forms || []);
const player = computed(() =>
  items.value.find((item) => item.group === "player"),
);
const playbackError = computed(
  () =>
    player.value?.playback_error ||
    (player.value?.timeline?.status === "error" ||
    player.value?.hero_badges?.some((b) => b.label === "ERROR")
      ? player.value?.detail || "Music playback failed."
      : ""),
);
const libraryGroups = computed(
  () =>
    ui.value.manager_tabs?.find((tab) => tab.key === "library")?.groups || [
      { key: "albums", label: "Albums" },
      { key: "artists", label: "Artists" },
      { key: "genres", label: "Genres" },
    ],
);
const recommendations = computed(() =>
  items.value.filter((item) => item.group === "recommendations"),
);
const playlists = computed(() =>
  recommendations.value.filter(
    (item) => item.card_variant === "recommendation_playlist",
  ),
);
const overview = computed(() =>
  recommendations.value.find(
    (item) => item.card_variant === "recommendations_intro",
  ),
);
const recommendationsLabel = computed(() =>
  overview.value?.assistant_name
    ? overview.value.assistant_name.endsWith("s")
      ? overview.value.assistant_name + "' picks"
      : overview.value.assistant_name + "'s picks"
    : "Tater’s picks",
);
const managerTabs = computed(() => [
  { key: "listen", label: "Listen" },
  { key: "library", label: "Library" },
  { key: "recommendations", label: recommendationsLabel.value },
  { key: "playlist", label: "Queue" },
  { key: "speakers", label: "Speakers" },
  { key: "settings", label: "Settings" },
]);
const upNext = computed(() => {
  const tracks = player.value?.track_list || [];
  const index = tracks.findIndex((track) => track.active);
  return tracks.slice(index + 1, index + 4);
});
const activeDestinations = computed(() =>
  (player.value?.player_rows || []).filter((row) => row.selected),
);
const destinationName = computed(() =>
  activeDestinations.value.length === 1
    ? playerFriendlyName(
        activeDestinations.value[0].label,
        activeDestinations.value[0].target,
      )
    : activeDestinations.value.length
      ? activeDestinations.value.length + " destinations"
      : "Choose speakers",
);
const physicalSpeakers = computed(() =>
  activeDestinations.value.reduce(
    (n, row) =>
      n + Number(row.speaker_count || (row.target.includes("stereo:") ? 2 : 1)),
    0,
  ),
);
const connected = computed(
  () =>
    ui.value.provider_connected ??
    items.value.some(
      (item) =>
        item.group === "providers" &&
        item.hero_badges?.some((b) => b.label === "CONNECTED"),
    ),
);
const featured = computed(() =>
  playlists.value.length
    ? playlists.value.slice(0, 3)
    : items.value.filter((item) => item.group === "albums").slice(0, 3),
);
const hero = computed(() => featured.value[0]);
const settingsItems = computed(() =>
  items.value.filter(
    (item) => item.group === "settings" || item.group === "providers",
  ),
);
const settingId = ref("");
watch(
  settingsItems,
  (rows) => {
    if (!rows.some((item) => item.id === settingId.value))
      settingId.value =
        rows.find((item) => item.id === "settings:music")?.id ||
        rows[0]?.id ||
        "";
  },
  { immediate: true },
);
function settingLabel(item: MusicItem) {
  return item.group === "providers"
    ? "Tater Tube"
    : item.id === "settings:music"
      ? "Playback"
      : item.id === "settings:library_sync"
        ? "Library & sync"
        : item.id === "settings:personalization"
          ? "Personalization"
          : item.title || "More settings";
}
async function browse(
  values: Record<string, unknown>,
): Promise<MusicBrowseResult> {
  if (!ui.value.browse_action)
    throw Error("Update Music Core to browse songs without starting playback.");
  const result = await runMusicAction(
    props.options.actionEndpoint,
    ui.value.browse_action,
    { values },
  );
  if (result.ok === false)
    throw Error(
      String(result.message || result.detail || "Music browsing failed."),
    );
  return result as unknown as MusicBrowseResult;
}
async function playLibraryTrack(track: MusicTrack) {
  if (ui.value.track_play_action && track.id)
    await run(
      ui.value.track_play_action,
      { id: track.id, values: { track_id: track.id } },
      "library-track:" + track.id,
    );
}
async function playQueueTrack(track: MusicTrack) {
  if (player.value?.track_list_action)
    await run(
      player.value.track_list_action,
      { id: track.id, values: {} },
      "track:" + track.id,
    );
}
async function playItem(item: MusicItem) {
  if (item.browse_values && ui.value.selection_play_action) {
    await run(
      ui.value.selection_play_action,
      { values: item.browse_values },
      "item:" + item.id,
    );
    return;
  }
  if (item.run_action)
    await run(item.run_action, { id: item.id, values: {} }, "item:" + item.id);
}
function search() {
  selectedTab.value = "library";
  void libraryBrowser.value?.searchQuery(homeQuery.value.trim());
}
function openConnection() {
  settingId.value =
    settingsItems.value.find((item) => item.group === "providers")?.id ||
    settingId.value;
  selectedTab.value = "settings";
}
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
  const result = await fetchMusicState(props.options.tabEndpoint);
  if (!disposed) props.state.payload = result;
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
    const result = await runMusicAction(
      props.options.actionEndpoint,
      action,
      actionPayload,
    );
    if (result.ok === false) {
      throw new Error(
        String(result.message || result.detail || "Music action failed."),
      );
    }
    await refreshState();
    const message =
      typeof result.message === "string" ? result.message.trim() : "";
    if (message) {
      if (props.options.onToast) props.options.onToast(message, "success");
      else successMessage.value = message;
    }
    return true;
  } catch (error) {
    errorMessage.value =
      error instanceof Error
        ? error.message
        : String(error || "Music action failed.");
    return false;
  } finally {
    setBusy(busyKey, false);
  }
}

function connectEvents(): void {
  if (eventSource) eventSource.close();
  if (reconnectTimer) window.clearTimeout(reconnectTimer);
  if (suspended || disposed) return;
  eventStatus.value = "connecting";
  const source = new EventSource(props.options.eventsEndpoint);
  eventSource = source;
  source.addEventListener("core-tab", (event) => {
    if (suspended || disposed || eventSource !== source) return;
    try {
      props.state.payload = JSON.parse(
        (event as MessageEvent<string>).data,
      ) as CoreTabPayload;
      eventStatus.value = "live";
    } catch {
      // Ignore one malformed event and keep the current player state.
    }
  });
  source.addEventListener("open", () => {
    if (eventSource === source) eventStatus.value = "live";
  });
  source.addEventListener("error", () => {
    if (eventSource !== source) return;
    eventStatus.value = "offline";
    eventSource?.close();
    eventSource = null;
    reconnectTimer = window.setTimeout(connectEvents, 3000);
  });
}

function disconnectEvents() {
  if (reconnectTimer) window.clearTimeout(reconnectTimer);
  reconnectTimer = 0;
  eventSource?.close();
  eventSource = null;
}
onDeactivated(() => {
  suspended = true;
  disconnectEvents();
});
onActivated(() => {
  if (!suspended) return;
  suspended = false;
  void refreshState().catch(() => {});
  connectEvents();
});
onMounted(connectEvents);
onBeforeUnmount(() => {
  disposed = true;
  if (reconnectTimer) window.clearTimeout(reconnectTimer);
  eventSource?.close();
  eventSource = null;
});
</script>
<template>
  <main
    class="tater-music-core tm-music-workspace"
    data-workspace-layout="music"
    :class="{ 'cw-music-embedded': options.embedded }"
  >
    <p v-if="payload.error" class="tm-error" role="alert">
      {{ payload.error }}
    </p>
    <template v-else>
      <header class="tm-workspace-heading">
        <div v-if="!options.embedded">
          <h1>Music</h1>
          <p>Your library. Your rooms. One place to listen.</p>
        </div>
        <button
          type="button"
          class="tm-source-state"
          :class="{ connected }"
          @click="openConnection"
        >
          <span />{{ connected ? "Tater Tube connected" : "Connect Tater Tube"
          }}<span v-if="eventStatus === 'offline'" class="tm-secondary">
            · Reconnecting</span
          >
        </button>
      </header>
      <nav class="tm-workspace-tabs" aria-label="Music navigation">
        <button
          v-for="tab in managerTabs"
          :key="tab.key"
          type="button"
          :aria-current="selectedTab === tab.key ? 'page' : undefined"
          @click="selectedTab = tab.key"
        >
          {{ tab.label }}
        </button>
      </nav>
      <div
        v-if="playbackError || player?.warnings?.length"
        class="tm-playback-warning"
        :role="playbackError ? 'alert' : 'status'"
      >
        <MusicIcon name="alert" /><span>{{
          playbackError || player?.warnings?.join(" ")
        }}</span>
      </div>
      <section v-if="selectedTab === 'listen'" class="tm-listening-workspace">
        <div class="tm-listening-main">
          <form
            class="tm-workspace-search"
            role="search"
            @submit.prevent="search"
          >
            <MusicIcon name="search" /><input
              v-model="homeQuery"
              type="search"
              aria-label="Search artists, albums, or songs"
              placeholder="Search artists, albums, or songs"
            /><button v-if="homeQuery" type="submit">Search</button>
          </form>
          <section class="tm-listen-hero">
            <img
              v-if="hero?.hero_image_src"
              :src="hero.hero_image_src"
              :alt="hero.hero_image_alt || ''"
            /><span v-else class="tm-hero-placeholder"
              ><MusicIcon name="music"
            /></span>
            <div>
              <div class="tm-eyebrow">
                {{
                  playlists.length
                    ? "Made for your ears"
                    : "From your own library"
                }}
              </div>
              <h2>
                {{
                  hero?.title ||
                  (connected
                    ? "Find your next favorite."
                    : "Make room for music.")
                }}
              </h2>
              <p>
                {{
                  hero?.subtitle ||
                  (connected
                    ? "Browse your albums, find a favorite, and choose where to listen."
                    : "Connect Tater Tube Server to bring your music library into Tater.")
                }}
              </p>
              <button
                v-if="hero?.run_action"
                type="button"
                class="tm-button primary"
                :disabled="isBusy('item:' + hero.id)"
                @click="playItem(hero)"
              >
                <MusicIcon name="play" />{{
                  isBusy("item:" + hero.id)
                    ? "Starting…"
                    : playlists.length
                      ? "Play this mix"
                      : "Play album"
                }}</button
              ><button
                v-else
                type="button"
                class="tm-button primary"
                @click="
                  connected ? (selectedTab = 'library') : openConnection()
                "
              >
                {{ connected ? "Browse library" : "Connect your library" }}
              </button>
            </div>
          </section>
          <header class="tm-section-heading">
            <h3>
              {{
                playlists.length ? recommendationsLabel : "From your library"
              }}
            </h3>
            <button
              class="tm-text-button"
              type="button"
              @click="
                selectedTab = playlists.length ? 'recommendations' : 'library'
              "
            >
              See all <MusicIcon name="external" />
            </button>
          </header>
          <div v-if="featured.length" class="tm-featured-grid">
            <article v-for="item in featured" :key="item.id">
              <button
                type="button"
                class="tm-featured-music"
                :disabled="isBusy('item:' + item.id)"
                @click="playItem(item)"
              >
                <img
                  v-if="item.hero_image_src"
                  :src="item.hero_image_src"
                  :alt="item.hero_image_alt || ''"
                  loading="lazy"
                /><span v-else class="tm-art-fallback"
                  ><MusicIcon name="music" /></span
                ><strong>{{ item.title }}</strong
                ><small>{{ item.subtitle }}</small>
              </button>
            </article>
          </div>
          <p v-else class="tm-secondary">
            {{
              overview?.detail ||
              "Your albums and personalized music picks will appear here."
            }}
          </p>
          <template v-if="ui.recent_tracks?.length"
            ><header class="tm-section-heading">
              <h3>Pick up where you left off</h3>
            </header>
            <button
              v-for="track in ui.recent_tracks.slice(0, 2)"
              :key="track.id"
              type="button"
              class="tm-home-track"
              :disabled="isBusy('library-track:' + track.id)"
              @click="playLibraryTrack(track)"
            >
              <img
                v-if="track.image_src"
                :src="track.image_src"
                :alt="track.image_alt || ''"
              /><span
                ><strong>{{ track.title }}</strong
                ><small>{{ track.artist }}</small></span
              ><MusicIcon name="play" /></button
          ></template>
        </div>
        <aside class="tm-listening-context">
          <div>
            <header class="tm-section-heading">
              <h3>Up next</h3>
              <MusicIcon name="queue" />
            </header>
            <button
              v-for="track in upNext"
              :key="track.id"
              type="button"
              class="tm-home-track"
              :disabled="isBusy('track:' + track.id)"
              @click="playQueueTrack(track)"
            >
              <img
                v-if="track.image_src"
                :src="track.image_src"
                :alt="track.image_alt || ''"
              /><span
                ><strong>{{ track.title }}</strong
                ><small>{{ track.artist }}</small></span
              ><small>{{ track.duration }}</small>
            </button>
            <p v-if="!upNext.length" class="tm-secondary">
              Play some music to start your queue.
            </p>
            <button
              type="button"
              class="tm-text-button"
              @click="selectedTab = 'playlist'"
            >
              View queue <MusicIcon name="chevron" />
            </button>
          </div>
          <div class="tm-playing-on">
            <div class="tm-eyebrow">Playing on</div>
            <button
              type="button"
              class="tm-output-card"
              @click="selectedTab = 'speakers'"
            >
              <MusicIcon name="speaker" /><span
                ><strong>{{ destinationName }}</strong
                ><small
                  >{{ activeDestinations.length }} destination{{
                    activeDestinations.length === 1 ? "" : "s"
                  }}
                  · {{ physicalSpeakers }} speakers</small
                ></span
              ></button
            ><span
              v-if="
                activeDestinations.some((row) => row.target.includes('stereo:'))
              "
              class="tm-secondary"
              >Left + right channels paired</span
            ><button
              type="button"
              class="tm-text-button"
              @click="selectedTab = 'speakers'"
            >
              Change speakers
            </button>
          </div>
          <div v-if="player?.continuous_radio" class="tm-radio-note">
            <MusicIcon name="radio" /><span
              >{{
                player.continuation_pending
                  ? "Finding your next songs"
                  : "Continuous radio"
              }}<small>Similar music keeps playing</small></span
            >
          </div>
        </aside>
      </section>
      <LibraryBrowser
        ref="libraryBrowser"
        v-show="selectedTab === 'library'"
        :groups="libraryGroups"
        :items="items"
        :busy="isBusy"
        :run="run"
        :browse="ui.browse_action ? browse : undefined"
        :play-track="playLibraryTrack"
        :selection-play-action="ui.selection_play_action"
        :selected-group="selectedLibraryGroup"
        @update:selected-group="selectedLibraryGroup = $event"
      />
      <RecommendationsBrowser
        v-show="selectedTab === 'recommendations'"
        :items="recommendations"
        :busy="isBusy"
        :run="run"
      />
      <TrackList
        v-if="player"
        v-show="selectedTab === 'playlist'"
        :item="player"
        :busy="isBusy"
        :run="run"
      />
      <SpeakersWorkspace
        v-if="player"
        v-show="selectedTab === 'speakers'"
        :item="player"
        :busy="isBusy"
        :run="run"
      />
      <section
        v-show="selectedTab === 'settings'"
        class="tm-settings-workspace"
        aria-label="Music settings"
      >
        <nav class="tm-settings-sections" aria-label="Music settings sections">
          <button
            v-for="item in settingsItems"
            :key="item.id"
            type="button"
            :aria-current="settingId === item.id ? 'page' : undefined"
            @click="settingId = item.id"
          >
            {{ settingLabel(item) }}
          </button>
        </nav>
        <div class="tm-settings-content">
          <div
            v-for="item in settingsItems"
            v-show="settingId === item.id"
            :key="item.id"
          >
            <SettingsCard
              :item="item"
              :busy="isBusy"
              :run="run"
              :fields-popup="false"
              :fields-dropdown="false"
            />
          </div>
          <div v-if="!settingsItems.length" class="tm-empty">
            Music settings are unavailable.
          </div>
          <details class="tm-library-status">
            <summary>Library status</summary>
            <dl>
              <div v-for="stat in payload.stats || []" :key="stat.label">
                <dt>{{ stat.label }}</dt>
                <dd>{{ stat.value }}</dd>
              </div>
            </dl>
            <button
              type="button"
              class="tm-button secondary"
              :disabled="isBusy('library:refresh')"
              @click="run('music_sync_now', {}, 'library:refresh')"
            >
              <MusicIcon name="refresh" />Refresh library
            </button>
          </details>
        </div>
      </section>
      <section
        v-if="player"
        class="tm-playback-dock workspace-music-player"
        aria-label="Playback"
      >
        <MusicPlayer
          :item="player"
          :busy="isBusy"
          :run="run"
          @speakers="selectedTab = 'speakers'"
        />
      </section>
      <div v-if="errorMessage" class="tm-error-toast" role="alert">
        <span>{{ errorMessage }}</span
        ><button
          type="button"
          aria-label="Dismiss error"
          @click="errorMessage = ''"
        >
          ×
        </button>
      </div>
      <div v-if="successMessage" class="tm-success-toast" role="status">
        <span>{{ successMessage }}</span
        ><button
          type="button"
          aria-label="Dismiss message"
          @click="successMessage = ''"
        >
          ×
        </button>
      </div>
    </template>
  </main>
</template>
