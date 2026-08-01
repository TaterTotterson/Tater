<script setup lang="ts">
import type { MusicItem, MusicTrack } from "../types";

const props = defineProps<{
  item: MusicItem;
  busy: (key: string) => boolean;
  run: (action: string, payload: Record<string, unknown>, busyKey?: string) => Promise<boolean>;
}>();

async function playTrack(track: MusicTrack): Promise<void> {
  if (!props.item.track_list_action || !track.id) return;
  await props.run(props.item.track_list_action, { id: track.id, values: {} }, `track:${track.id}`);
}

async function updateShuffle(event: Event): Promise<void> {
  const input = event.target as HTMLInputElement;
  if (!props.item.track_list_shuffle_action) return;
  const saved = await props.run(
    props.item.track_list_shuffle_action,
    { id: props.item.id, values: { shuffle: input.checked } },
    "shuffle",
  );
  if (!saved) {
    input.checked = !input.checked;
  }
}
</script>

<template>
  <details class="tm-queue" open>
    <summary>
      <span>
        <strong>{{ item.track_list_label || 'Current Track List' }}</strong>
        <small>{{ item.track_list?.length || 0 }} tracks</small>
      </span>
      <label class="tm-shuffle" @click.stop>
        <input
          type="checkbox"
          :checked="Boolean(item.track_list_shuffle)"
          :disabled="busy('shuffle')"
          @change="updateShuffle"
        />
        Shuffle
      </label>
    </summary>
    <div v-if="item.track_list?.length" class="tm-track-scroll" role="listbox" aria-label="Current track list">
      <button
        v-for="track in item.track_list"
        :key="track.id || track.position"
        type="button"
        class="tm-track"
        :class="{ active: track.active, pending: busy(`track:${track.id}`) }"
        :disabled="busy(`track:${track.id}`)"
        :aria-current="track.active ? 'true' : undefined"
        :title="`Double-click to play ${track.title || 'this track'}`"
        @dblclick="playTrack(track)"
      >
        <span class="tm-track-position">{{ track.active ? '▶' : track.position }}</span>
        <span class="tm-track-copy">
          <strong>{{ track.title || 'Untitled' }}</strong>
          <small>{{ [track.artist, track.album].filter(Boolean).join(' · ') || 'Unknown artist' }}</small>
        </span>
        <span class="tm-track-duration">{{ track.duration || '' }}</span>
      </button>
    </div>
    <div v-else class="tm-empty compact">Play an album, artist, genre, or search to create a track list.</div>
  </details>
</template>
