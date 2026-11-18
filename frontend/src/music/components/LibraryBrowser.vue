<script setup lang="ts">
import { computed, ref, watch } from "vue";
import MusicIcon from "./MusicIcon.vue";
import type {
  ManagerGroup,
  MusicItem,
  MusicTrack,
  MusicBrowseResult,
} from "../types";
const props = withDefaults(
  defineProps<{
    groups: ManagerGroup[];
    items: MusicItem[];
    busy: (key: string) => boolean;
    run: (
      action: string,
      payload: Record<string, unknown>,
      busyKey?: string,
    ) => Promise<boolean>;
    browse?: (values: Record<string, unknown>) => Promise<MusicBrowseResult>;
    playTrack?: (track: MusicTrack) => Promise<void>;
    selectionPlayAction?: string;
    selectedGroup?: string;
    showNavigation?: boolean;
    initialQuery?: string;
  }>(),
  { selectedGroup: "albums", showNavigation: true, initialQuery: "" },
);
const emit = defineEmits<{ "update:selectedGroup": [value: string] }>();
const activeGroupKey = ref(props.selectedGroup),
  pages = ref<Record<string, number>>({}),
  query = ref(props.initialQuery),
  result = ref<MusicBrowseResult | null>(null),
  resultValues = ref<Record<string, unknown>>({}),
  selection = ref<MusicItem | null>(null),
  loading = ref(false),
  error = ref("");
let request = 0;
const groups = computed(() =>
  props.groups
    .filter((g) => g.key !== "search")
    .sort(
      (a, b) =>
        ["albums", "artists", "genres"].indexOf(a.key) -
        ["albums", "artists", "genres"].indexOf(b.key),
    ),
);
const activeGroup = computed(
  () =>
    groups.value.find((g) => g.key === activeGroupKey.value) || groups.value[0],
);
const groupItems = computed(() =>
  props.items
    .filter(
      (i) =>
        i.group === (activeGroup.value?.item_group || activeGroup.value?.key),
    )
    .filter(
      (i) =>
        !query.value ||
        [i.title, i.subtitle]
          .join(" ")
          .toLocaleLowerCase()
          .includes(query.value.toLocaleLowerCase()),
    ),
);
const pageSize = computed(() =>
  Math.max(1, Number(activeGroup.value?.page_size || 36)),
);
const page = computed(() =>
  Math.min(pages.value[activeGroupKey.value] || 1, pageCount.value),
);
const pageCount = computed(() =>
  Math.max(1, Math.ceil(groupItems.value.length / pageSize.value)),
);
const visibleItems = computed(() =>
  groupItems.value.slice(
    (page.value - 1) * pageSize.value,
    page.value * pageSize.value,
  ),
);
watch(
  () => props.initialQuery,
  (q) => {
    query.value = q;
    if (q) void search();
  },
  { immediate: true },
);
watch(
  () => props.selectedGroup,
  (group) => {
    if (group) activeGroupKey.value = group;
  },
);
function selectGroup(group: string) {
  request++;
  result.value = null;
  selection.value = null;
  error.value = "";
  loading.value = false;
  activeGroupKey.value = group;
  emit("update:selectedGroup", group);
}
function back() {
  request++;
  result.value = null;
  selection.value = null;
  query.value = "";
  error.value = "";
  loading.value = false;
}
async function load(
  values: Record<string, unknown>,
  item: MusicItem | null = null,
) {
  if (!props.browse) return;
  const ticket = ++request;
  loading.value = true;
  result.value = null;
  error.value = "";
  selection.value = item;
  try {
    const data = await props.browse(values);
    if (ticket === request) {
      result.value = data;
      resultValues.value = Object.fromEntries(
        Object.entries(values).filter(
          ([key]) => key !== "offset" && key !== "limit",
        ),
      );
    }
  } catch (e) {
    if (ticket === request)
      error.value = e instanceof Error ? e.message : String(e);
  } finally {
    if (ticket === request) loading.value = false;
  }
}
async function open(item: MusicItem) {
  if (props.browse) await load(item.browse_values || facetValues(item), item);
  else if (item.run_action)
    await props.run(
      item.run_action,
      { id: item.id, values: {} },
      "item:" + item.id,
    );
}
function facetValues(item: MusicItem) {
  const i = item.id.indexOf(":");
  return { [item.id.slice(0, i)]: item.id.slice(i + 1) };
}
async function search() {
  if (!query.value.trim()) {
    back();
    return;
  }
  if (!props.browse) {
    await props.run(
      "music_ui_play",
      { values: { query: query.value.trim() } },
      "library-play",
    );
    return;
  }
  await load({ query: query.value.trim() });
}
async function playCategory(item: MusicItem) {
  if (props.selectionPlayAction)
    await props.run(
      props.selectionPlayAction,
      { values: item.browse_values || facetValues(item) },
      "item:" + item.id,
    );
  else if (item.run_action)
    await props.run(
      item.run_action,
      { id: item.id, values: {} },
      "item:" + item.id,
    );
}
async function playSelection() {
  const item = selection.value;
  if (props.selectionPlayAction) {
    await props.run(
      props.selectionPlayAction,
      {
        values: resultValues.value,
      },
      "library-play",
    );
    return;
  }
  if (item?.run_action)
    await props.run(
      item.run_action,
      { id: item.id, values: {} },
      "library-play",
    );
  else
    await props.run(
      "music_ui_play",
      { values: { query: query.value } },
      "library-play",
    );
}
async function moveResults(direction: number) {
  if (!result.value) return;
  await load(
    {
      ...resultValues.value,
      offset: result.value.offset + direction * result.value.limit,
      limit: result.value.limit,
    },
    selection.value,
  );
}
defineExpose({
  searchQuery(value: string) {
    query.value = value;
    return search();
  },
});
</script>
<template>
  <section class="tm-library">
    <form class="tm-workspace-search" role="search" @submit.prevent="search">
      <MusicIcon name="search" /><input
        v-model="query"
        type="search"
        aria-label="Search music"
        placeholder="Search artists, albums, or songs"
      /><button v-if="query" type="submit" :disabled="loading">Search</button>
    </form>
    <template v-if="result || selection || loading || error">
      <button
        type="button"
        class="tm-text-button tm-library-back"
        @click="back"
      >
        <MusicIcon name="back" />Back to library
      </button>
      <header class="tm-browse-heading">
        <img
          v-if="selection?.hero_image_src"
          :src="selection.hero_image_src"
          :alt="selection.hero_image_alt || ''"
        />
        <div>
          <div class="tm-eyebrow">
            {{
              selection?.group === "albums"
                ? "Album"
                : selection?.group === "artists"
                  ? "Artist"
                  : selection?.group === "genres"
                    ? "Genre"
                    : "Search results"
            }}
          </div>
          <h2>{{ selection?.title || result?.title || "Searching…" }}</h2>
          <p>
            {{ selection?.subtitle
            }}<span v-if="result"> · {{ result.total }} songs</span>
          </p>
          <button
            v-if="result?.total && !loading"
            type="button"
            class="tm-button primary"
            :disabled="busy('library-play')"
            @click="playSelection"
          >
            <MusicIcon name="play" />Play
            {{
              selection?.group === "albums"
                ? "album"
                : selection
                  ? "selection"
                  : "results"
            }}
          </button>
        </div>
      </header>
      <p v-if="error" role="alert" class="tm-inline-error">{{ error }}</p>
      <p v-if="loading" class="tm-secondary" role="status">Loading music…</p>
      <div v-else-if="result?.tracks.length" class="tm-library-tracks">
        <button
          v-for="track in result.tracks"
          :key="track.id"
          class="tm-track"
          type="button"
          :disabled="busy('library-track:' + track.id)"
          @click="playTrack?.(track)"
        >
          <span class="tm-track-position"><MusicIcon name="play" /></span
          ><img
            v-if="track.image_src"
            class="tm-track-art"
            :src="track.image_src"
            :alt="track.image_alt || ''"
            loading="lazy"
          /><span v-else class="tm-track-art placeholder"
            ><MusicIcon name="music" /></span
          ><span class="tm-track-copy"
            ><strong>{{ track.title }}</strong
            ><small>{{
              [track.artist, track.album].filter(Boolean).join(" · ")
            }}</small></span
          ><span class="tm-track-duration">{{ track.duration }}</span>
        </button>
      </div>
      <p v-else-if="result && !loading" class="tm-empty">
        No songs match this search. Try an artist, album, or song title.
      </p>
      <nav
        v-if="result && result.total > result.limit"
        class="tm-pagination"
        aria-label="Song results"
      >
        <button
          type="button"
          :disabled="loading || result.offset === 0"
          @click="moveResults(-1)"
        >
          Previous</button
        ><span
          >{{ result.offset + 1 }}–{{
            Math.min(result.offset + result.limit, result.total)
          }}
          of {{ result.total }}</span
        ><button
          type="button"
          :disabled="loading || result.offset + result.limit >= result.total"
          @click="moveResults(1)"
        >
          Next
        </button>
      </nav>
    </template>
    <template v-else>
      <nav
        v-if="showNavigation"
        class="tm-library-filters"
        aria-label="Browse music library"
      >
        <button
          v-for="group in groups"
          :key="group.key"
          type="button"
          :aria-pressed="activeGroup?.key === group.key"
          @click="selectGroup(group.key)"
        >
          {{ group.label || group.key }}
        </button>
      </nav>
      <header class="tm-section-heading">
        <div>
          <h2>Your {{ activeGroup?.label?.toLowerCase() || "library" }}</h2>
        </div>
        <span class="tm-secondary"
          >{{ groupItems.length }} {{ activeGroup?.label?.toLowerCase() }}</span
        >
      </header>
      <div v-if="visibleItems.length" class="tm-library-grid">
        <article
          v-for="item in visibleItems"
          :key="item.id"
          class="tm-library-card"
        >
          <button
            type="button"
            class="tm-library-open"
            :aria-label="'Browse ' + item.title"
            @click="open(item)"
          >
            <span class="tm-library-art"
              ><img
                v-if="item.hero_image_src"
                :src="item.hero_image_src"
                :alt="item.hero_image_alt || ''"
                loading="lazy" /><MusicIcon v-else name="music" /></span
            ><span class="tm-library-copy"
              ><strong>{{ item.title || "Untitled" }}</strong
              ><small>{{ item.subtitle }}</small></span
            ></button
          ><button
            v-if="item.run_action"
            type="button"
            class="tm-album-play"
            :disabled="busy('item:' + item.id)"
            :aria-label="'Play ' + item.title"
            @click="playCategory(item)"
          >
            <MusicIcon name="play" />
          </button>
        </article>
      </div>
      <p v-else class="tm-empty">
        {{
          query
            ? "No categories match. Press Search to find songs."
            : activeGroup?.empty_message ||
              "Connect Tater Tube Server to load your library."
        }}
      </p>
      <nav
        v-if="pageCount > 1"
        class="tm-pagination"
        aria-label="Library pages"
      >
        <button
          type="button"
          :disabled="page <= 1"
          @click="pages[activeGroupKey] = page - 1"
        >
          Previous</button
        ><span>Page {{ page }} of {{ pageCount }}</span
        ><button
          type="button"
          :disabled="page >= pageCount"
          @click="pages[activeGroupKey] = page + 1"
        >
          Next
        </button>
      </nav>
    </template>
  </section>
</template>
