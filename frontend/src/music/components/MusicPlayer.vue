<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import DynamicField from "./DynamicField.vue";
import TrackList from "./TrackList.vue";
import PopupTransition from "../../shared/PopupTransition.vue";
import type { MusicField, MusicItem, MusicPlayerRow } from "../types";

type PlayerSetting = {
  volume_percent: number;
  sync_offset_ms: number;
  transport_mode: "auto" | "native" | "airplay";
};

const props = defineProps<{
  item: MusicItem;
  busy: (key: string) => boolean;
  run: (action: string, payload: Record<string, unknown>, busyKey?: string) => Promise<boolean>;
}>();

const speakersOpen = ref(false);
const volume = ref(75);
const speakerValues = ref<Record<string, unknown>>({});
const playerSettings = ref<Record<string, PlayerSetting>>({});
const position = ref(0);
const seeking = ref(false);
const collapsed = ref(false);
const speakersDirty = ref(false);
const volumeEditing = ref(false);
let progressTimer: number | undefined;

const volumeField = computed(() => props.item.fields?.find((field) => field.key === "volume_percent"));
const popupFields = computed(() => props.item.popup_fields || []);
const playerRows = computed(() => props.item.player_rows || []);
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
  [popupFields, playerRows],
  ([fields]) => {
    if (!speakersOpen.value || !speakersDirty.value) syncSpeakerValues(fields);
  },
  { immediate: true },
);

function syncSpeakerValues(fields = popupFields.value): void {
  speakerValues.value = Object.fromEntries(fields.map((field) => [field.key, copyFieldValue(field.value)]));
  playerSettings.value = Object.fromEntries(
    playerRows.value.map((row) => [
      row.target,
      {
        volume_percent: clampNumber(row.volume_percent, 75, 0, 100),
        sync_offset_ms: clampNumber(row.sync_offset_ms, 0, -1000, 1000),
        transport_mode: transportMode(row.transport_mode),
      },
    ]),
  );
}

function clampNumber(value: unknown, fallback: number, minimum: number, maximum: number): number {
  const parsed = Number(value);
  return Math.max(minimum, Math.min(maximum, Number.isFinite(parsed) ? parsed : fallback));
}

function transportMode(value: unknown): "auto" | "native" | "airplay" {
  const mode = String(value || "").toLowerCase();
  return mode === "native" || mode === "airplay" ? mode : "auto";
}

function selectedTargets(): string[] {
  const raw = speakerValues.value.targets;
  if (Array.isArray(raw)) return raw.map(String).filter(Boolean);
  return typeof raw === "string" && raw ? [raw] : [];
}

function isPlayerSelected(target: string): boolean {
  return selectedTargets().includes(target);
}

function setPlayerSelected(target: string, selected: boolean): void {
  const targets = selectedTargets();
  speakerValues.value = {
    ...speakerValues.value,
    targets: selected
      ? Array.from(new Set([...targets, target]))
      : targets.filter((entry) => entry !== target),
  };
  speakersDirty.value = true;
}

function playerSetting(target: string): PlayerSetting {
  return playerSettings.value[target] || {
    volume_percent: 75,
    sync_offset_ms: 0,
    transport_mode: "auto",
  };
}

function updateTransportMode(target: string, event: Event): void {
  const current = playerSetting(target);
  playerSettings.value = {
    ...playerSettings.value,
    [target]: {
      ...current,
      transport_mode: transportMode((event.target as HTMLSelectElement).value),
    },
  };
  speakersDirty.value = true;
}

function updatePlayerSetting(
  target: string,
  key: "volume_percent" | "sync_offset_ms",
  value: unknown,
): void {
  const current = playerSetting(target);
  playerSettings.value = {
    ...playerSettings.value,
    [target]: {
      ...current,
      [key]: key === "volume_percent"
        ? clampNumber(value, current.volume_percent, 0, 100)
        : clampNumber(value, current.sync_offset_ms, -1000, 1000),
    },
  };
  speakersDirty.value = true;
}

function updatePlayerSettingFromEvent(
  target: string,
  key: "volume_percent" | "sync_offset_ms",
  event: Event,
): void {
  updatePlayerSetting(target, key, (event.target as HTMLInputElement).value);
}

function nudgePlayer(target: string, delta: number): void {
  updatePlayerSetting(target, "sync_offset_ms", playerSetting(target).sync_offset_ms + delta);
}

function offsetLabel(value: unknown): string {
  const offset = clampNumber(value, 0, -1000, 1000);
  if (offset === 0) return "In sync";
  return `${Math.abs(offset)} ms ${offset < 0 ? "earlier" : "later"}`;
}

function syncQualityLabel(row: MusicPlayerRow): string {
  if (row.sync_quality === "precise") return "Precise sync";
  if (row.sync_quality === "bridge") return "AirPlay bridge";
  if (row.sync_quality === "automatic") {
    const mode = playerSetting(row.target).transport_mode;
    if (mode === "native") return "Native Sonos";
    if (mode === "airplay") return "AirPlay bridge";
    return "Auto sync";
  }
  return "Best effort";
}

function syncQualityTitle(row: MusicPlayerRow): string {
  if (row.sync_quality === "precise") return "Clock-scheduled Tater playback";
  if (row.sync_quality === "bridge") return "Wall-clock scheduled through Tater AirPlay Bridge";
  if (row.sync_quality === "automatic") {
    return "Automatic uses AirPlay Bridge with Tater sats and native Sonos otherwise";
  }
  return "Timing depends on the external player";
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
  if (action.endsWith("_play") || action.endsWith("_pause")) return "primary";
  if (action.endsWith("_stop")) return "stop";
  return "";
}

function actionGlyph(action: string, fallback: string): string {
  if (action.endsWith("_previous")) return "⏮";
  if (action.endsWith("_play")) return "▶";
  if (action.endsWith("_pause")) return "⏸";
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
    {
      id: props.item.id,
      values: { ...speakerValues.value, player_settings: playerSettings.value },
    },
    "speakers",
  );
  if (saved) {
    speakersDirty.value = false;
    syncSpeakerValues();
    speakersOpen.value = false;
  }
}

async function testSync(): Promise<void> {
  if (!props.item.test_sync_action || selectedTargets().length === 0) return;
  await props.run(
    props.item.test_sync_action,
    {
      id: props.item.id,
      values: { ...speakerValues.value, player_settings: playerSettings.value },
    },
    "sync-test",
  );
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
            <div v-if="playerRows.length" class="tm-player-rows">
              <p class="tm-player-calibration-help">
                Select players, set each volume, then move Audio sync toward Earlier or Later until the test clicks line up.
              </p>
              <article
                v-for="row in playerRows"
                :key="row.target"
                class="tm-player-row"
                :class="{ 'is-selected': isPlayerSelected(row.target) }"
              >
                <header>
                  <label class="tm-player-row-select">
                    <input
                      type="checkbox"
                      :checked="isPlayerSelected(row.target)"
                      @change="setPlayerSelected(row.target, ($event.target as HTMLInputElement).checked)"
                    />
                    <span>
                      <strong>{{ row.label || row.target }}</strong>
                      <small v-if="row.meta">{{ row.meta }}</small>
                    </span>
                  </label>
                  <span
                    class="tm-sync-quality"
                    :class="`is-${row.sync_quality || 'best_effort'}`"
                    :title="syncQualityTitle(row)"
                  >
                    {{ syncQualityLabel(row) }}
                  </span>
                </header>

                <div
                  class="tm-player-row-controls"
                  :class="{
                    disabled: !isPlayerSelected(row.target),
                    'has-transport': Boolean(row.transport_options?.length),
                  }"
                >
                  <label v-if="row.transport_options?.length" class="tm-player-row-control tm-transport-mode-control">
                    <span>
                      <strong>Playback route</strong>
                      <output>{{ playerSetting(row.target).transport_mode === 'auto' ? 'Context aware' : 'Fixed' }}</output>
                    </span>
                    <select
                      :value="playerSetting(row.target).transport_mode"
                      :disabled="!isPlayerSelected(row.target)"
                      :aria-label="`${row.label || row.target} playback route`"
                      @change="updateTransportMode(row.target, $event)"
                    >
                      <option
                        v-for="option in row.transport_options"
                        :key="option.value"
                        :value="option.value"
                      >{{ option.label }}</option>
                    </select>
                  </label>

                  <label class="tm-player-row-control">
                    <span><strong>Volume</strong><output>{{ playerSetting(row.target).volume_percent }}%</output></span>
                    <input
                      type="range"
                      min="0"
                      max="100"
                      step="1"
                      :value="playerSetting(row.target).volume_percent"
                      :disabled="!isPlayerSelected(row.target)"
                      :aria-label="`${row.label || row.target} volume`"
                      @input="updatePlayerSettingFromEvent(row.target, 'volume_percent', $event)"
                    />
                  </label>

                  <div class="tm-player-row-control tm-sync-control">
                    <span>
                      <strong>Audio sync</strong>
                      <output>{{ offsetLabel(playerSetting(row.target).sync_offset_ms) }}</output>
                    </span>
                    <input
                      type="range"
                      min="-1000"
                      max="1000"
                      step="10"
                      :value="playerSetting(row.target).sync_offset_ms"
                      :disabled="!isPlayerSelected(row.target)"
                      :aria-label="`${row.label || row.target} audio sync offset`"
                      @input="updatePlayerSettingFromEvent(row.target, 'sync_offset_ms', $event)"
                    />
                    <div class="tm-sync-nudges">
                      <button
                        type="button"
                        :disabled="!isPlayerSelected(row.target)"
                        :aria-label="`Move ${row.label || row.target} 10 milliseconds earlier`"
                        @click="nudgePlayer(row.target, -10)"
                      >−10 ms</button>
                      <input
                        type="number"
                        min="-1000"
                        max="1000"
                        step="10"
                        :value="playerSetting(row.target).sync_offset_ms"
                        :disabled="!isPlayerSelected(row.target)"
                        :aria-label="`${row.label || row.target} offset in milliseconds`"
                        @input="updatePlayerSettingFromEvent(row.target, 'sync_offset_ms', $event)"
                      />
                      <button
                        type="button"
                        :disabled="!isPlayerSelected(row.target)"
                        :aria-label="`Move ${row.label || row.target} 10 milliseconds later`"
                        @click="nudgePlayer(row.target, 10)"
                      >+10 ms</button>
                      <button
                        type="button"
                        class="tm-sync-reset"
                        :disabled="!isPlayerSelected(row.target) || playerSetting(row.target).sync_offset_ms === 0"
                        @click="updatePlayerSetting(row.target, 'sync_offset_ms', 0)"
                      >Reset</button>
                    </div>
                  </div>
                </div>
              </article>
            </div>
            <template v-else>
              <DynamicField
                v-for="field in popupFields"
                :key="field.key"
                :field="field"
                :model-value="speakerValues[field.key]"
                @update:model-value="setSpeakerValue(field, $event)"
              />
            </template>
          </div>
          <footer class="tm-player-modal-footer">
            <button
              v-if="item.test_sync_action && playerRows.length"
              type="button"
              class="tm-button secondary tm-sync-test"
              :disabled="busy('sync-test') || selectedTargets().length === 0"
              title="Stops current music and plays a short click track"
              @click="testSync"
            >
              {{ busy('sync-test') ? 'Starting test…' : 'Test sync' }}
            </button>
            <span class="tm-modal-footer-spacer" />
            <button type="button" class="tm-button secondary" @click="closeSpeakers">Cancel</button>
            <button
              type="button"
              class="tm-button primary"
              :disabled="busy('speakers') || selectedTargets().length === 0"
              @click="saveSpeakers"
            >
              Set players
            </button>
          </footer>
        </section>
    </PopupTransition>
  </section>
</template>
