<script setup lang="ts">
import { computed } from "vue";
import type { MusicItem } from "../types";

const props = defineProps<{
  items: MusicItem[];
  busy: (key: string) => boolean;
  run: (action: string, payload: Record<string, unknown>, busyKey?: string) => Promise<boolean>;
}>();

const overview = computed(() => props.items.find((item) => item.card_variant === "recommendations_intro"));
const playlists = computed(() => props.items.filter((item) => item.card_variant === "recommendation_playlist"));

async function refreshRecommendations(): Promise<void> {
  const item = overview.value;
  if (!item?.run_action || !item.refresh_available) return;
  await props.run(item.run_action, { id: item.id, values: {} }, "recommendations:refresh");
}

async function playPlaylist(item: MusicItem): Promise<void> {
  if (!item.run_action) return;
  await props.run(item.run_action, { id: item.id, values: {} }, `recommendations:${item.id}`);
}
</script>

<template>
  <section class="tm-recommendations" aria-label="Tater Recommendations">
    <header class="tm-recommendations-heading">
      <div>
        <div class="tm-eyebrow">Made for your ears</div>
        <h2>{{ overview?.title || 'Tater Recommendations' }}</h2>
        <p>{{ overview?.subtitle || 'Named playlists shaped by what you listen to.' }}</p>
        <small v-if="overview?.detail">{{ overview.detail }}</small>
      </div>
      <button
        type="button"
        class="tm-button primary"
        :disabled="!overview?.refresh_available || busy('recommendations:refresh') || overview?.refresh_running"
        @click="refreshRecommendations"
      >
        {{ busy('recommendations:refresh') || overview?.refresh_running ? 'Tater is mixing…' : overview?.run_label || 'Refresh Recommendations' }}
      </button>
    </header>

    <div v-if="playlists.length" class="tm-recommendation-grid">
      <article v-for="playlist in playlists" :key="playlist.id" class="tm-recommendation-card">
        <div class="tm-recommendation-hero">
          <img
            v-if="playlist.hero_image_src"
            :src="playlist.hero_image_src"
            :alt="playlist.hero_image_alt || ''"
            loading="lazy"
          />
          <div v-else class="tm-recommendation-placeholder" aria-hidden="true">♫</div>
          <div v-if="playlist.hero_badges?.length" class="tm-badges">
            <span
              v-for="badge in playlist.hero_badges"
              :key="badge.label"
              :class="`tone-${badge.tone || 'muted'}`"
            >
              {{ badge.label }}
            </span>
          </div>
        </div>

        <div class="tm-recommendation-copy">
          <div class="tm-eyebrow">Tater mix</div>
          <h3>{{ playlist.title || 'Tater Mix' }}</h3>
          <p>{{ playlist.subtitle }}</p>
        </div>

        <div class="tm-recommendation-items">
          <div v-for="entry in playlist.recommendation_items || []" :key="entry.id" class="tm-recommendation-entry">
            <img v-if="entry.image_src" :src="entry.image_src" :alt="entry.image_alt || ''" loading="lazy" />
            <span v-else class="tm-recommendation-entry-art" aria-hidden="true">♫</span>
            <div>
              <small>{{ entry.type === 'album' ? `Album · ${entry.track_count || 0} tracks` : 'Song' }}</small>
              <strong>{{ entry.title || 'Untitled' }}</strong>
              <span>{{ [entry.artist, entry.type === 'song' ? entry.album : ''].filter(Boolean).join(' · ') }}</span>
              <p v-if="entry.reason">{{ entry.reason }}</p>
            </div>
          </div>
        </div>

        <footer>
          <button
            type="button"
            class="tm-button primary"
            :disabled="busy(`recommendations:${playlist.id}`)"
            @click="playPlaylist(playlist)"
          >
            {{ busy(`recommendations:${playlist.id}`) ? 'Starting…' : `▶ ${playlist.run_label || 'Play Playlist'}` }}
          </button>
          <small>Plays on the destinations selected above.</small>
        </footer>
      </article>
    </div>

    <div v-else class="tm-empty tm-recommendations-empty">
      <strong>No mixes yet</strong>
      <span>{{ overview?.detail || 'Play some music and Tater will start learning your taste.' }}</span>
    </div>
  </section>
</template>
