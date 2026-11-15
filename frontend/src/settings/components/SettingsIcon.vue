<script setup lang="ts">
defineProps<{ name: string }>();
</script>

<template>
  <svg class="tset-icon" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
    <template v-if="name === 'sliders'"><path d="M4 6h6m4 0h6M4 12h10m4 0h2M4 18h2m4 0h10M10 3v6m8 0v6M6 15v6" /></template>
    <template v-else-if="name === 'users'"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2m20 0v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75" /><circle cx="9" cy="7" r="4" /></template>
    <template v-else-if="name === 'brain'"><path d="M12 18V5a3 3 0 0 0-5.8-1A4 4 0 0 0 3 10a4 4 0 0 0 1 7.5A4 4 0 0 0 12 18Zm0 0a4 4 0 0 0 8-.5A4 4 0 0 0 21 10a4 4 0 0 0-3.2-6A3 3 0 0 0 12 5M8 8a3 3 0 0 0-1 5m10-5a3 3 0 0 1 0 5" /></template>
    <template v-else-if="name === 'mic'"><rect x="9" y="2" width="6" height="12" rx="3" /><path d="M5 10v2a7 7 0 0 0 14 0v-2m-7 9v3m-4 0h8" /></template>
    <template v-else-if="name === 'plug'"><path d="M9 3v5m6-5v5M7 8h10v4a5 5 0 0 1-10 0V8Zm5 9v5" /></template>
    <template v-else-if="name === 'shield'"><path d="m12 3 8 3v6c0 5-8 9-8 9s-8-4-8-9V6l8-3Z" /><path d="m8 12 3 3 5-6" /></template>
    <template v-else-if="name === 'server'"><rect x="3" y="3" width="18" height="7" rx="2" /><rect x="3" y="14" width="18" height="7" rx="2" /><path d="M7 6.5h.01M7 17.5h.01" /></template>
    <template v-else-if="name === 'search'"><circle cx="10.5" cy="10.5" r="6.5" /><path d="m16 16 5 5" /></template>
    <path v-else-if="name === 'back'" d="m12 5-7 7 7 7M5 12h16" />
    <path v-else-if="name === 'chevron'" d="m9 5 7 7-7 7" />
    <template v-else-if="name === 'check'"><circle cx="12" cy="12" r="9" /><path d="m8 12 3 3 5-6" /></template>
    <template v-else-if="name === 'alert'"><circle cx="12" cy="12" r="9" /><path d="M12 7v6m0 4h.01" /></template>
  </svg>
</template>
