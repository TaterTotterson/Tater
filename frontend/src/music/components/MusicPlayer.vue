<script setup lang="ts">
import {
  computed,
  onMounted,
  onBeforeUnmount,
  onActivated,
  onDeactivated,
  ref,
  watch,
} from "vue";
import MusicIcon from "./MusicIcon.vue";
import type { MusicItem } from "../types";
import { playerFriendlyName } from "../playerDisplay";
const props = defineProps<{
  item: MusicItem;
  busy: (key: string) => boolean;
  run: (
    action: string,
    payload: Record<string, unknown>,
    busyKey?: string,
  ) => Promise<boolean>;
}>();
const emit = defineEmits<{ speakers: [] }>();
const volume = ref(75),
  volumeEditing = ref(false),
  seeking = ref(false),
  seekPosition = ref(0),
  now = ref(Date.now() / 1000);
let timer = 0;
let seekTrackId = "";
const volumeField = computed(() =>
  props.item.fields?.find((field) => field.key === "volume_percent"),
);
watch(
  volumeField,
  (field) => {
    if (field && !volumeEditing.value) volume.value = Number(field.value ?? 75);
  },
  { immediate: true },
);
const selected = computed(() =>
  (props.item.player_rows || []).filter((row) => row.selected),
);
const destinations = computed(() =>
  selected.value.length === 1
    ? playerFriendlyName(selected.value[0].label, selected.value[0].target)
    : selected.value.length
      ? String(selected.value.length) + " destinations"
      : "Choose speakers",
);
const timeline = computed(() => props.item.timeline);
const duration = computed(() =>
  Math.max(0, Number(timeline.value?.duration_seconds || 0)),
);
const position = computed(() => {
  if (seeking.value) return seekPosition.value;
  const t = timeline.value;
  const offset = Number(t?.position_offset_seconds || 0);
  const elapsed =
    t?.status === "playing" && t.started_at
      ? Math.max(0, now.value - t.started_at)
      : 0;
  return Math.min(duration.value || Infinity, Math.max(0, offset + elapsed));
});
const seekable = computed(() =>
  Boolean(timeline.value?.seekable && props.item.seek_action),
);
const transport = computed(() =>
  [...(props.item.actions || [])].sort(
    (a, b) => rank(a.action) - rank(b.action),
  ),
);
function rank(action: string) {
  return action.endsWith("_previous")
    ? 0
    : action.endsWith("_play") || action.endsWith("_pause")
      ? 1
      : action.endsWith("_next")
        ? 2
        : 3;
}
function glyph(action: string) {
  return action.endsWith("_previous")
    ? "previous"
    : action.endsWith("_next")
      ? "next"
      : action.endsWith("_pause")
        ? "pause"
        : action.endsWith("_play")
          ? "play"
          : "stop";
}
function time(seconds: number) {
  const n = Math.max(0, Math.floor(seconds));
  return Math.floor(n / 60) + ":" + String(n % 60).padStart(2, "0");
}
function startClock() {
  window.clearInterval(timer);
  now.value = Date.now() / 1000;
  timer = window.setInterval(() => (now.value = Date.now() / 1000), 1000);
}
onMounted(startClock);
onActivated(startClock);
onDeactivated(() => window.clearInterval(timer));
onBeforeUnmount(() => window.clearInterval(timer));
async function control(action: string) {
  await props.run(
    action,
    { id: props.item.id, values: { volume_percent: volume.value } },
    "transport",
  );
}
function updateVolumeFromEvent(event: Event) {
  volumeEditing.value = true;
  volume.value = Number((event.target as HTMLInputElement).value);
}
async function setVolume() {
  if (volumeField.value?.action)
    await props.run(
      volumeField.value.action,
      { id: props.item.id, values: { volume_percent: volume.value } },
      "volume",
    );
  volumeEditing.value = false;
  volume.value = Number(volumeField.value?.value ?? volume.value);
}
function stageSeek(event: Event) {
  if (!seeking.value) seekTrackId = timeline.value?.track_id || "";
  seeking.value = true;
  seekPosition.value = Number((event.target as HTMLInputElement).value);
}
async function commitSeek() {
  if (!seekable.value || seekTrackId !== (timeline.value?.track_id || "")) {
    seeking.value = false;
    return;
  }
  await props.run(
    props.item.seek_action!,
    { id: props.item.id, values: { position_seconds: seekPosition.value } },
    "seek",
  );
  seeking.value = false;
  now.value = Date.now() / 1000;
}
async function shuffle() {
  if (props.item.track_list_shuffle_action)
    await props.run(
      props.item.track_list_shuffle_action,
      {
        id: props.item.id,
        values: { shuffle: !props.item.track_list_shuffle },
      },
      "shuffle",
    );
}
async function repeat() {
  if (props.item.repeat_action)
    await props.run(
      props.item.repeat_action,
      {
        id: props.item.id,
        values: {
          repeat:
            props.item.repeat === "off"
              ? "all"
              : props.item.repeat === "all"
                ? "one"
                : "off",
        },
      },
      "repeat",
    );
}
</script>
<template>
  <section class="tm-player tm-workspace-dock" aria-label="Music player">
    <div class="tm-dock-track">
      <img
        v-if="item.hero_image_src"
        :src="item.hero_image_src"
        :alt="item.hero_image_alt || ''"
      />
      <span v-else class="tm-art-fallback" aria-hidden="true"
        ><MusicIcon name="music"
      /></span>
      <span
        ><strong>{{
          item.now_playing?.title || item.title || "Nothing playing"
        }}</strong
        ><small>{{ item.now_playing?.artist || item.subtitle }}</small></span
      >
    </div>
    <div class="tm-dock-center">
      <div class="tm-dock-transport" aria-label="Playback controls">
        <button
          v-if="item.track_list_shuffle_action"
          type="button"
          aria-label="Shuffle"
          :aria-pressed="Boolean(item.track_list_shuffle)"
          :disabled="busy('shuffle')"
          @click="shuffle"
        >
          <MusicIcon name="shuffle" />
        </button>
        <button
          v-for="entry in transport"
          :key="entry.action"
          type="button"
          :class="{
            'tm-dock-play': ['_play', '_pause'].some((suffix) =>
              entry.action.endsWith(suffix),
            ),
          }"
          :disabled="busy('transport')"
          :aria-label="entry.aria_label || entry.label"
          :title="entry.tooltip || entry.label"
          @click="control(entry.action)"
        >
          <MusicIcon :name="glyph(entry.action)" />
        </button>
        <button
          v-if="item.repeat_action"
          type="button"
          :aria-label="'Repeat: ' + (item.repeat || 'off')"
          :title="'Repeat: ' + (item.repeat || 'off')"
          :aria-pressed="item.repeat !== 'off'"
          :disabled="busy('repeat')"
          @click="repeat"
        >
          <MusicIcon :name="item.repeat === 'one' ? 'repeat-one' : 'repeat'" />
        </button>
      </div>
      <label v-if="timeline" class="tm-timeline"
        ><span>{{ time(position) }}</span
        ><input
          type="range"
          min="0"
          :max="duration || 1"
          step="1"
          :value="position"
          :disabled="!seekable || busy('seek')"
          aria-label="Track position"
          @input="stageSeek"
          @change="commitSeek"
        /><span>{{ time(duration) }}</span></label
      >
    </div>
    <div class="tm-dock-utility">
      <label
        v-if="volumeField"
        class="tm-player-volume"
        :style="{ '--tm-volume-percent': volume + '%' }"
        ><MusicIcon name="speaker" /><input
          type="range"
          min="0"
          max="100"
          step="1"
          :value="volume"
          :disabled="busy('volume')"
          aria-label="Music volume"
          @input="updateVolumeFromEvent"
          @change="setVolume"
        /><output>{{ volume }}%</output></label
      >
      <button
        type="button"
        class="tm-dock-speakers"
        :aria-label="item.settings_aria_label || 'Choose speakers and players'"
        @click="emit('speakers')"
      >
        <MusicIcon name="speaker" /><span>{{ destinations }}</span
        ><MusicIcon name="chevron" />
      </button>
    </div>
  </section>
</template>
