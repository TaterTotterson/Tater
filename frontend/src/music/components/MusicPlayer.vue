<script setup lang="ts">
import { computed, ref, watch } from "vue";
import PopupTransition from "../../shared/PopupTransition.vue";
import type { MusicField, MusicItem, MusicPlayerRow } from "../types";
import {
  groupPlayerTargets,
  playerFriendlyName,
  playerSecondaryText,
  playerTargetKind,
} from "../playerDisplay";

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
const speakersDirty = ref(false);
const volumeEditing = ref(false);

const volumeField = computed(() => props.item.fields?.find((field) => field.key === "volume_percent"));
const popupFields = computed(() => props.item.popup_fields || []);
const playerRows = computed(() => props.item.player_rows || []);
const playerSections = computed(() => groupPlayerTargets(playerRows.value, (row) => row.target));
const volumeStyle = computed<Record<string, string>>(() => ({
  "--tm-volume-percent": `${Math.max(0, Math.min(100, volume.value))}%`,
}));
const selectedPlayerCount = computed(() => selectedTargets().length);

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

function updatePlayerVolumeFromEvent(target: string, event: Event): void {
  const current = playerSetting(target);
  playerSettings.value = {
    ...playerSettings.value,
    [target]: {
      ...current,
      volume_percent: clampNumber(
        (event.target as HTMLInputElement).value,
        current.volume_percent,
        0,
        100,
      ),
    },
  };
  speakersDirty.value = true;
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

function playerKind(row: MusicPlayerRow): string {
  return playerTargetKind(row.target);
}

function playerDisplayName(row: MusicPlayerRow): string {
  return playerFriendlyName(row.label, row.target);
}

function playerDisplayMeta(row: MusicPlayerRow): string {
  return playerSecondaryText(row.label, row.meta, row.target);
}

function playerGlyph(row: MusicPlayerRow): string {
  const kind = playerKind(row);
  if (kind === "stereo") return "T²";
  if (kind === "satellite") return "T";
  if (kind === "airplay") return "△";
  if (kind === "sonos") return "S";
  if (kind === "home") return "H";
  return "♪";
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

function updateVolumeFromEvent(event: Event): void {
  updateVolume((event.target as HTMLInputElement).value);
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
  <section class="tm-player" aria-label="Music player">
    <div id="tm-player-details" class="tm-player-main">
      <div class="tm-art-wrap">
        <img v-if="item.hero_image_src" class="tm-art" :src="item.hero_image_src" :alt="item.hero_image_alt || ''" />
        <div v-else class="tm-art tm-art-placeholder" aria-hidden="true">♫</div>
      </div>

      <div class="tm-now-playing">
        <h2>{{ item.title || 'Music Player' }}</h2>
        <p>{{ item.subtitle || item.detail }}</p>
      </div>

      <div class="tm-transport" aria-label="Playback controls">
        <button
          v-for="entry in item.actions || []"
          :key="entry.action"
          type="button"
          :class="[controlClass(entry.action), { 'is-play': entry.action.endsWith('_play') }]"
          :disabled="busy('transport')"
          :aria-label="entry.aria_label || entry.label"
          :title="entry.tooltip || entry.label"
          @click="runTransport(entry.action)"
        >
          <svg
            v-if="entry.action.endsWith('_play')"
            class="tm-transport-play-icon"
            viewBox="0 0 24 24"
            focusable="false"
            aria-hidden="true"
          >
            <path d="M10 6.5 22 13.5 10 20.5Z" />
          </svg>
          <span v-else class="tm-transport-glyph" aria-hidden="true">
            {{ actionGlyph(entry.action, entry.label || 'Run') }}
          </span>
        </button>
      </div>

      <div class="tm-player-utility">
        <label v-if="volumeField" class="tm-player-volume" :style="volumeStyle">
          <span aria-hidden="true">♪</span>
          <input
            type="range"
            min="0"
            max="100"
            step="1"
            :value="volume"
            :disabled="busy('volume')"
            aria-label="Music volume"
            @input="updateVolumeFromEvent"
            @change="setVolume"
          />
          <output>{{ volume }}%</output>
        </label>
        <button
          type="button"
          class="tm-speaker-button"
          :aria-label="item.settings_aria_label || 'Choose speakers and players'"
          title="Choose speakers and players"
          @click="openSpeakers"
        >
          <span aria-hidden="true">🔊</span>
          <span class="tm-speaker-label">
            {{ selectedPlayerCount ? `${selectedPlayerCount} Player${selectedPlayerCount === 1 ? '' : 's'}` : 'Players' }}
          </span>
        </button>
      </div>

    </div>

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
              <div class="tm-player-picker-intro">
                <p class="tm-player-calibration-help">
                  Pick any combination of Tater sats and external speakers. Selected players expand for playback route and volume.
                </p>
                <strong>{{ selectedTargets().length }} selected</strong>
              </div>
              <section v-for="section in playerSections" :key="section.key" class="tm-player-section">
                <h4>{{ section.label }}</h4>
                <div class="tm-player-section-list">
                  <article
                    v-for="row in section.items"
                    :key="row.target"
                    class="tm-player-row"
                    :class="[
                      `kind-${playerKind(row)}`,
                      { 'is-selected': isPlayerSelected(row.target) },
                    ]"
                  >
                  <header>
                  <label class="tm-player-row-select">
                    <input
                      type="checkbox"
                      :checked="isPlayerSelected(row.target)"
                      @change="setPlayerSelected(row.target, ($event.target as HTMLInputElement).checked)"
                    />
                    <span class="tm-player-row-icon" aria-hidden="true">{{ playerGlyph(row) }}</span>
                    <span class="tm-player-row-copy">
                      <strong>{{ playerDisplayName(row) }}</strong>
                      <small v-if="playerDisplayMeta(row)">{{ playerDisplayMeta(row) }}</small>
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
                  v-if="isPlayerSelected(row.target)"
                  class="tm-player-row-controls"
                  :class="{
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
                      @input="updatePlayerVolumeFromEvent(row.target, $event)"
                    />
                  </label>
                </div>
                  </article>
                </div>
              </section>
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
