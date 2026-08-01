<script setup lang="ts">
import { computed, ref, watch } from "vue";
import DynamicField from "./DynamicField.vue";
import TrackList from "./TrackList.vue";
import type { MusicField, MusicItem } from "../types";

const props = defineProps<{
  item: MusicItem;
  busy: (key: string) => boolean;
  run: (action: string, payload: Record<string, unknown>, busyKey?: string) => Promise<boolean>;
}>();

const speakersOpen = ref(false);
const volume = ref(75);
const speakerValues = ref<Record<string, unknown>>({});

const volumeField = computed(() => props.item.fields?.find((field) => field.key === "volume_percent"));
const popupFields = computed(() => props.item.popup_fields || []);

function copyFieldValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map((entry) => (entry && typeof entry === "object" ? { ...entry } : entry));
  if (value && typeof value === "object") return { ...(value as Record<string, unknown>) };
  return value;
}

watch(
  volumeField,
  (field) => {
    if (field) volume.value = Number(field.value ?? 75);
  },
  { immediate: true },
);

watch(
  popupFields,
  (fields) => {
    speakerValues.value = Object.fromEntries(fields.map((field) => [field.key, copyFieldValue(field.value)]));
  },
  { immediate: true },
);

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
  if (!field?.action) return;
  await props.run(
    field.action,
    { id: props.item.id, values: { volume_percent: volume.value } },
    "volume",
  );
}

async function saveSpeakers(): Promise<void> {
  if (!props.item.save_action) return;
  const saved = await props.run(
    props.item.save_action,
    { id: props.item.id, values: speakerValues.value },
    "speakers",
  );
  if (saved) speakersOpen.value = false;
}

function setSpeakerValue(field: MusicField, value: unknown): void {
  speakerValues.value = { ...speakerValues.value, [field.key]: value };
}
</script>

<template>
  <section class="tm-player" aria-label="Music player">
    <div class="tm-player-main">
      <div class="tm-art-wrap">
        <img v-if="item.hero_image_src" class="tm-art" :src="item.hero_image_src" :alt="item.hero_image_alt || ''" />
        <div v-else class="tm-art tm-art-placeholder" aria-hidden="true">♫</div>
      </div>

      <div class="tm-now-playing">
        <div class="tm-eyebrow">Now playing</div>
        <h2>{{ item.title || 'Music Player' }}</h2>
        <p>{{ item.subtitle || item.detail }}</p>
        <div v-if="item.hero_badges?.length" class="tm-badges">
          <span v-for="badge in item.hero_badges" :key="badge.label" :class="`tone-${badge.tone || 'muted'}`">
            {{ badge.label }}
          </span>
        </div>
      </div>

      <div class="tm-player-controls">
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
            @update:model-value="volume = Number($event)"
            @change="setVolume"
          />
          <button
            type="button"
            class="tm-speaker-button"
            :aria-label="item.settings_aria_label || 'Choose speakers and players'"
            title="Choose speakers and players"
            @click="speakersOpen = true"
          >
            <span aria-hidden="true">🔊</span>
            <span class="tm-speaker-label">Players</span>
          </button>
        </div>
      </div>
    </div>

    <div v-if="item.summary_rows?.length" class="tm-player-facts">
      <div v-for="row in item.summary_rows" :key="row.label">
        <span>{{ row.label }}</span>
        <strong>{{ row.value || '—' }}</strong>
      </div>
    </div>

    <TrackList :item="item" :busy="busy" :run="run" />

    <Teleport to="body">
      <div v-if="speakersOpen" class="tm-modal-backdrop" @click.self="speakersOpen = false">
        <section class="tm-modal" role="dialog" aria-modal="true" aria-labelledby="tm-speaker-title">
          <header>
            <div>
              <div class="tm-eyebrow">Playback destination</div>
              <h3 id="tm-speaker-title">{{ item.settings_title || 'Choose Speakers & Players' }}</h3>
            </div>
            <button type="button" class="tm-close" aria-label="Close" @click="speakersOpen = false">×</button>
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
            <button type="button" class="tm-button secondary" @click="speakersOpen = false">Cancel</button>
            <button type="button" class="tm-button primary" :disabled="busy('speakers')" @click="saveSpeakers">
              Set players
            </button>
          </footer>
        </section>
      </div>
    </Teleport>
  </section>
</template>
