<script setup lang="ts">
import SettingsIcon from "../settings/components/SettingsIcon.vue";

defineProps<{ name: string }>();
const workspaceIcons = new Set(["dashboard", "chat", "verbas", "portals", "cores", "terminal", "plus", "arrow-up", "close", "file", "refresh", "sparkles", "activity", "download", "sun", "eye", "calendar", "coffee", "chart", "folder", "git", "circle", "external", "workflow", "weather", "radio", "music", "rss", "film", "robot", "sliders"]);
</script>

<template>
  <svg v-if="workspaceIcons.has(name)" class="workspace-icon" viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
    <template v-if="name === 'dashboard'"><rect x="3" y="3" width="7" height="9" rx="1" /><rect x="14" y="3" width="7" height="5" rx="1" /><rect x="3" y="16" width="7" height="5" rx="1" /><rect x="14" y="12" width="7" height="9" rx="1" /></template>
    <path v-else-if="name === 'chat'" d="M21 11.5a8.5 8.5 0 0 1-8.5 8.5 9 9 0 0 1-4-.9L3 21l1.9-5.5a9 9 0 0 1-.9-4A8.5 8.5 0 0 1 12.5 3 8.5 8.5 0 0 1 21 11.5Z" />
    <template v-else-if="name === 'verbas'"><path d="m15 4 5 5L7 22l-5-5L15 4ZM11 8l5 5M5 2v4M3 4h4m13 10v4m-2-2h4" /></template>
    <template v-else-if="name === 'portals'"><rect x="3" y="3" width="18" height="18" rx="2" /><path d="M3 9h18M9 9v12" /></template>
    <template v-else-if="name === 'cores'"><path d="m12 2 5 3v6l-5 3-5-3V5l5-3Zm0 12v7M7 5l5 3 5-3M12 8v6M7 10l-5 3v6l5 3 5-3m5-9 5 3v6l-5 3-5-3M2 13l5 3 5-3m0 0 5 3 5-3M7 16v6m10-6v6" /></template>
    <path v-else-if="name === 'terminal'" d="m4 5 6 6-6 6m9 2h7" />
    <path v-else-if="name === 'plus'" d="M12 5v14M5 12h14" />
    <path v-else-if="name === 'arrow-up'" d="M12 20V4m-7 7 7-7 7 7" />
    <path v-else-if="name === 'close'" d="m6 6 12 12M6 18 18 6" />
    <template v-else-if="name === 'file'"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8l-6-6Z" /><path d="M14 2v6h6M8 13h8M8 17h5" /></template>
    <path v-else-if="name === 'refresh'" d="M20 7v5h-5M4 17v-5h5m-4-5a8 8 0 0 1 13-3l2 3M4 17l2 3a8 8 0 0 0 13-3" />
    <path v-else-if="name === 'sparkles'" d="m12 3 2.4 6.6L21 12l-6.6 2.4L12 21l-2.4-6.6L3 12l6.6-2.4L12 3Zm7-1v4m-2-2h4M4 18v4m-2-2h4" />
    <path v-else-if="name === 'activity'" d="M2 12h4l3-9 6 18 3-9h4" />
    <path v-else-if="name === 'download'" d="M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5" />
    <template v-else-if="name === 'sun'"><circle cx="12" cy="12" r="4" /><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5" /></template>
    <template v-else-if="name === 'eye'"><path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z" /><circle cx="12" cy="12" r="3" /></template>
    <template v-else-if="name === 'calendar'"><rect x="3" y="5" width="18" height="16" rx="2" /><path d="M7 2v6m10-6v6M3 11h18M7 15h2m3 0h2m3 0h.01M7 18h2m3 0h2" /></template>
    <template v-else-if="name === 'coffee'"><path d="M3 9h14v8a4 4 0 0 1-4 4H7a4 4 0 0 1-4-4V9Zm14 0h2a3 3 0 1 1 0 6h-2M7 2v3m4-3v3M1 21h20" /></template>
    <path v-else-if="name === 'folder'" d="M3 7V5a2 2 0 0 1 2-2h5l2 3h7a2 2 0 0 1 2 2v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V7Z" />
    <template v-else-if="name === 'git'"><circle cx="6" cy="5" r="2" /><circle cx="18" cy="5" r="2" /><circle cx="6" cy="19" r="2" /><path d="M6 7v10m12-10v3a4 4 0 0 1-4 4H6" /></template>
    <circle v-else-if="name === 'circle'" cx="12" cy="12" r="8" />
    <path v-else-if="name === 'external'" d="M14 3h7v7m0-7-11 11M9 3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-4" />
    <template v-else-if="name==='workflow'"><rect x="3" y="3" width="6" height="6" rx="1"/><rect x="15" y="15" width="6" height="6" rx="1"/><path d="M9 6h5a4 4 0 0 1 4 4v5M6 9v9h9"/></template>
    <template v-else-if="name==='weather'"><path d="M7 19h11a4 4 0 1 0-1-7.9 6 6 0 0 0-11.5 2A3 3 0 0 0 7 19Z"/><path d="M12 2v2M3 6l2 2M2 12h2M20 6l-2 2"/></template>
    <template v-else-if="name==='radio'"><circle cx="12" cy="12" r="2"/><path d="M6.3 6.3a8 8 0 0 0 0 11.4m11.4-11.4a8 8 0 0 1 0 11.4M3.5 3.5a12 12 0 0 0 0 17m17-17a12 12 0 0 1 0 17"/></template>
    <template v-else-if="name==='music'"><path d="M9 18V5l12-2v13M9 9l12-2"/><ellipse cx="6" cy="18" rx="3" ry="3"/><ellipse cx="18" cy="16" rx="3" ry="3"/></template>
    <template v-else-if="name==='rss'"><circle cx="5" cy="19" r="1"/><path d="M4 11a9 9 0 0 1 9 9M4 4a16 16 0 0 1 16 16"/></template>
    <template v-else-if="name==='film'"><rect x="3" y="3" width="18" height="18" rx="2"/><path d="M7 3v18M17 3v18M3 8h4m-4 8h4M17 8h4m-4 8h4"/></template>
    <template v-else-if="name==='robot'"><rect x="4" y="7" width="16" height="14" rx="3"/><path d="M12 3v4M2 11v6m20-6v6M8 16h8"/><circle cx="8" cy="12" r=".8"/><circle cx="16" cy="12" r=".8"/></template>
    <path v-else-if="name==='sliders'" d="M3 6h6m5 0h7M3 12h12m4 0h2M3 18h3m5 0h10M9 3v6m6 0v6M6 15v6"/>
    <path v-else-if="name === 'chart'" d="M3 3v18h18M8 16v-4m5 4V8m5 8V5" />
  </svg>
  <SettingsIcon v-else :name="name" />
</template>
