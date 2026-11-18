<script setup lang="ts">
import UiIcon from "../../shared/UiIcon.vue";
defineProps<{ name: string }>();
const transport = new Set([
  "play",
  "pause",
  "previous",
  "next",
  "stop",
  "shuffle",
  "repeat",
  "repeat-one",
  "speaker",
  "queue",
]);
</script>
<template>
  <svg
    v-if="transport.has(name)"
    class="tm-music-icon"
    viewBox="0 0 24 24"
    width="18"
    height="18"
    fill="none"
    stroke="currentColor"
    stroke-width="1.6"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
  >
    <path v-if="name === 'play'" d="m8 4 13 8-13 8Z" />
    <path v-else-if="name === 'pause'" d="M8 5v14M16 5v14" />
    <path v-else-if="name === 'previous'" d="M5 5v14m14-14L7 12l12 7Z" />
    <path v-else-if="name === 'next'" d="M19 5v14M5 5l12 7-12 7Z" />
    <rect
      v-else-if="name === 'stop'"
      x="5"
      y="5"
      width="14"
      height="14"
      rx="1"
    />
    <path
      v-else-if="name === 'shuffle'"
      d="m17 3 4 4-4 4M3 17h3l9-10h6M3 7h3l3 3m4 4 3 3h5m-4-4 4 4-4 4"
    />
    <template v-else-if="name === 'repeat' || name === 'repeat-one'">
      <path
        d="m17 2 4 4-4 4M3 11V8a2 2 0 0 1 2-2h16M7 22l-4-4 4-4m14-1v3a2 2 0 0 1-2 2H3"
      />
      <path v-if="name === 'repeat-one'" d="m10 10 2-1v6m-2 0h4" />
    </template>
    <path
      v-else-if="name === 'speaker'"
      d="M4 9h4l5-4v14l-5-4H4Zm13-1a6 6 0 0 1 0 8m3-11a10 10 0 0 1 0 14"
    />
    <path
      v-else-if="name === 'queue'"
      d="M3 6h18M3 12h12M3 18h8m7-4v7m-3-3h6"
    />
  </svg>
  <UiIcon v-else :name="name" />
</template>
