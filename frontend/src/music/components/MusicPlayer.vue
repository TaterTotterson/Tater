<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import DynamicField from "./DynamicField.vue";
import TrackList from "./TrackList.vue";
import PopupTransition from "../../shared/PopupTransition.vue";
import type { MusicField, MusicItem } from "../types";

const props = defineProps<{
  item: MusicItem;
  busy: (key: string) => boolean;
  run: (action: string, payload: Record<string, unknown>, busyKey?: string) => Promise<boolean>;
}>();

const speakersOpen = ref(false);
const volume = ref(75);
const speakerValues = ref<Record<string, unknown>>({});
const position = ref(0);
const seeking = ref(false);
const collapsed = ref(false);
const speakersDirty = ref(false);
const volumeEditing = ref(false);
let progressTimer: number | undefined;

const volumeField = computed(() => props.item.fields?.find((field) => field.key === "volume_percent"));
const popupFields = computed(() => props.item.popup_fields || []);
const playback = computed(() => props.item.playback || {});
const duration = computed(() => Math.max(0, Number(playback.value.duration_seconds || 0)));
const canSeek = computed(() => Boolean(playback.value.seekable && duration.value > 0));

function copyFieldValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((entry) => (entry && typeof entry === "object" ? { ...entry } : entry));
  if (value && typeof value === "object") return { ...(value as Record<string, unknown>) };
  return value;
}

watch(
  volumeField,
  (field) => {
    if (field && !volumeEditing.value) volume.value = Number(field.value ?? 75);
  },
  { immediate: true },
);

function syncedPosition(): number {
  const state = playback.value;
  let next = Math.max(0, Number(state.position_seconds || 0));
  const updatedAt = Number(state.position_updated_at || 0);
  if (String(state.status || "").toLowerCase() === "playing" && updatedAt > 0) {
    next += Math.max(0, Date.now() / 1000 - updatedAt);
  }
  return duration.value > 0 ? Math.min(duration.value, next) : next;
}

function refreshProgress(): void {
  if (!seeking.value) position.value = syncedPosition();
}

watch(playback, refreshProgress, { immediate: true, deep: true });

onMounted(() => {
  progressTimer = window.setInterval(refreshProgress, 250);
});

onBeforeUnmount(() => {
  if (progressTimer !== undefined) window.clearInterval(progressTimer);
});

watch(
  popupFields,
  (fields) => {
    if (!speakersOpen.value || !speakersDirty.value) syncSpeakerValues(fields);
  },
  { immediate: true },
);

function syncSpeakerValues(fields = popupFields.value): void {
  speakerValues.value = Object.fromEntries(fields.map((field) => [field.key, copyFieldValue(field.value)]));
}

function openSpeakers(): void {
  syncSpeakerValues();
  speakersDirty.value = false;
  speakersOpen.value = true;
}

function closeSpeakers(): void {
  speakersOpen.value = false;
  speakersDirty.value = false;
  syncSpeakerValues();
}

function controlClass(action: string): string {
  if (action.endsWith("_play")) return "primary";
  if (action.endsWith("_stop")) return "stop";
  return "";
}

function actionGlyph(action: string, fallback: string): string {
  if (action.endsWith("_previous")) return "⏮";
  if (action.endsWith("_play")) return "▶";
  if (action.endsWith("_stop")) return "■";
  if (action.endsWith("_next")) return "⏭";
  return fallback;
}

async function runTransport(action: string): Promise<void> {
  await props.run(action, { id: props.item.id, values: { volume_percent: volume.value } }, "transport");
}

async function setVolume(): Promise<void> {
  const field = volumeField.value;
  if (!field?.action) {
    volumeEditing.value = false;
    return;
  }
  const saved = await props.run(
    field.action,
    { id: props.item.id, values: { volume_percent: volume.value } },
    "volume",
  );
  volumeEditing.value = false;
  if (!saved) volume.value = Number(volumeField.value?.value ?? volume.value);
}

function updateVolume(value: unknown): void {
  volume.value = Number(value);
  volumeEditing.value = true;
}

function formatTime(value: number): string {
  const seconds = Math.max(0, Math.round(Number(value) || 0));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainder = seconds % 60;
  return hours > 0
    ? `${hours}:${String(minutes).padStart(2, "0")}:${String(remainder).padStart(2, "0")}`
    : `${minutes}:${String(remainder).padStart(2, "0")}`;
}

function updateSeekPreview(event: Event): void {
  seeking.value = true;
  position.value = Number((event.target as HTMLInputElement).value || 0);
}

async function commitSeek(event: Event): Promise<void> {
  position.value = Number((event.target as HTMLInputElement).value || 0);
  const action = playback.value.seek_action;
  if (!action) {
    seeking.value = false;
    return;
  }
  const moved = await props.run(
    action,
    { id: props.item.id, values: { position_seconds: position.value } },
    "seek",
  );
  seeking.value = false;
  if (!moved) refreshProgress();
}

async function seekRelative(direction: number): Promise<void> {
  if (!canSeek.value) return;
  const step = Math.max(1, Number(playback.value.seek_step_seconds || 15));
  const delta = direction * step;
  position.value = Math.max(0, Math.min(duration.value, position.value + delta));
  const action = playback.value.seek_relative_action;
  if (!action) return;
  const moved = await props.run(
    action,
    { id: props.item.id, values: { delta_seconds: delta } },
    "seek",
  );
  if (!moved) refreshProgress();
}

async function saveSpeakers(): Promise<void> {
  if (!props.item.save_action) return;
  const saved = await props.run(
    props.item.save_action,
    { id: props.item.id, values: speakerValues.value },
    "speakers",
  );
  if (saved) {
    speakersDirty.value = false;
    syncSpeakerValues();
    speakersOpen.value = false;
  }
}

function setSpeakerValue(field: MusicField, value: unknown): void {
  speakerValues.value = { ...speakerValues.value, [field.key]: value };
  speakersDirty.value = true;
}
</script>

<template>
  <section class="tm-player" :class="{ 'is-collapsed': collapsed }" aria-label="Music player">
    <button
      type="button"
      class="tm-player-size-toggle"
      :aria-label="collapsed ? 'Expand music player' : 'Switch to mini player'"
      :title="collapsed ? 'Expand music player' : 'Switch to mini player'"
      :aria-expanded="!collapsed"
      aria-controls="tm-player-details"
      @click="collapsed = !collapsed"
    >
      <svg
        class="tm-player-size-icon"
        :class="{ 'is-up': !collapsed }"
        viewBox="0 0 16 16"
        aria-hidden="true"
      >
        <path d="m3.5 6 4.5 4 4.5-4" />
      </svg>
    </button>

    <div id="tm-player-details" class="tm-player-main">
      <div class="tm-art-wrap">
        <img v-if="item.hero_image_src" class="tm-art" :src="item.hero_image_src" :alt="item.hero_image_alt || ''" />
        <div v-else class="tm-art tm-art-placeholder" aria-hidden="true">♫</div>
      </div>

      <div class="tm-now-playing">
        <div class="tm-eyebrow">Now playing</div>
        <h2>{{ item.title || 'Music Player' }}</h2>
        <p>{{ item.subtitle || item.detail }}</p>
        <div class="tm-progress" :class="{ disabled: !canSeek }">
          <input
            type="range"
            min="0"
            :max="duration || 0"
            step="1"
            :value="position"
            :disabled="!canSeek || busy('seek')"
            aria-label="Track position"
            @input="updateSeekPreview"
            @change="commitSeek"
          />
          <div class="tm-progress-times" aria-live="off">
            <span>{{ formatTime(position) }}</span>
            <span>{{ formatTime(duration) }}</span>
          </div>
        </div>
        <div v-if="item.hero_badges?.length" class="tm-badges">
          <span v-for="badge in item.hero_badges" :key="badge.label" :class="`tone-${badge.tone || 'muted'}`">
            {{ badge.label }}
          </span>
        </div>
      </div>

      <div class="tm-player-controls">
        <div class="tm-seek-controls" aria-label="Seek controls">
          <button
            type="button"
            :disabled="!canSeek || busy('seek')"
            aria-label="Rewind 15 seconds"
            title="Rewind 15 seconds"
            @click="seekRelative(-1)"
          >
            ↶ <span>15</span>
          </button>
          <button
            type="button"
            :disabled="!canSeek || busy('seek')"
            aria-label="Forward 15 seconds"
            title="Forward 15 seconds"
            @click="seekRelative(1)"
          >
            ↷ <span>15</span>
          </button>
        </div>
        <div class="tm-transport" aria-label="Playback controls">
          <button
            v-for="entry in item.actions || []"
            :key="entry.action"
            type="button"
            :class="controlClass(entry.action)"
            :disabled="busy('transport')"
            :aria-label="entry.aria_label || entry.label"
            :title="entry.tooltip || entry.label"
            @click="runTransport(entry.action)"
          >
            {{ actionGlyph(entry.action, entry.label || 'Run') }}
          </button>
        </div>

        <div class="tm-volume-speakers">
          <DynamicField
            v-if="volumeField"
            :field="volumeField"
            :model-value="volume"
            compact
            @update:model-value="updateVolume"
            @change="setVolume"
          />
          <button
            type="button"
            class="tm-speaker-button"
            :aria-label="item.settings_aria_label || 'Choose speakers and players'"
            title="Choose speakers and players"
            @click="openSpeakers"
          >
            <span aria-hidden="true">🔊</span>
            <span class="tm-speaker-label">Players</span>
          </button>
        </div>
      </div>
    </div>

    <div v-if="!collapsed && item.summary_rows?.length" class="tm-player-facts">
      <div v-for="row in item.summary_rows" :key="row.label">
        <span>{{ row.label }}</span>
        <strong>{{ row.value || '—' }}</strong>
      </div>
    </div>

    <TrackList v-if="!collapsed" :item="item" :busy="busy" :run="run" />

    <PopupTransition :open="speakersOpen" backdrop-class="tm-modal-backdrop" @close="closeSpeakers">
        <section class="tm-modal" role="dialog" aria-modal="true" aria-labelledby="tm-speaker-title">
          <header>
            <div>
              <div class="tm-eyebrow">Playback destination</div>
              <h3 id="tm-speaker-title">{{ item.settings_title || 'Choose Speakers & Players' }}</h3>
            </div>
            <button type="button" class="tm-close" aria-label="Close" @click="closeSpeakers">×</button>
          </header>
          <div class="tm-modal-body">
            <DynamicField
              v-for="field in popupFields"
              :key="field.key"
              :field="field"
              :model-value="speakerValues[field.key]"
              @update:model-value="setSpeakerValue(field, $event)"
            />
          </div>
          <footer>
            <button type="button" class="tm-button secondary" @click="closeSpeakers">Cancel</button>
            <button type="button" class="tm-button primary" :disabled="busy('speakers')" @click="saveSpeakers">
              Set players
            </button>
          </footer>
        </section>
    </PopupTransition>
  </section>
</template>
