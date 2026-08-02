<script setup lang="ts">
import { computed } from "vue";
import type { ChatMessage, ChatProfile } from "../types";
import { renderMarkdown, safeHref } from "../markdown";

const props = defineProps<{
  message: ChatMessage;
  profile: ChatProfile;
  filesEndpoint: string;
}>();

const emit = defineEmits<{ mediaReady: [] }>();

const role = computed(() => (String(props.message.role || "assistant").toLowerCase() === "user" ? "user" : "assistant"));
const isUser = computed(() => role.value === "user");
const fullTaterName = computed(() => {
  const first = String(props.profile.tater_first_name || props.profile.tater_name || "Tater").trim() || "Tater";
  const last = String(props.profile.tater_last_name || "Totterson").trim();
  return String(props.profile.tater_full_name || [first, last].filter(Boolean).join(" ") || "Tater Totterson").trim();
});
const displayName = computed(() => isUser.value ? String(props.message.username || props.profile.username || "User") : fullTaterName.value);
const avatar = computed(() => String(isUser.value ? props.profile.user_avatar || "" : props.profile.tater_avatar || ""));
const initial = computed(() => {
  const match = displayName.value.match(/[A-Za-z0-9]/);
  return (match?.[0] || (isUser.value ? "U" : "T")).toUpperCase();
});
const contentObject = computed<Record<string, any> | null>(() => {
  const content = props.message.content;
  return content && typeof content === "object" ? content : null;
});
const marker = computed(() => String(contentObject.value?.marker || "").trim().toLowerCase());
const contentType = computed(() => String(contentObject.value?.type || "").trim().toLowerCase());
const fileName = computed(() => String(contentObject.value?.name || "attachment").trim() || "attachment");
const mimeType = computed(() => {
  const raw = String(contentObject.value?.mimetype || "application/octet-stream").trim();
  return /^[A-Za-z0-9.+-]+\/[A-Za-z0-9.+-]+$/.test(raw) ? raw : "application/octet-stream";
});
const sizeLabel = computed(() => {
  const bytes = Number(contentObject.value?.size || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) return "";
  if (bytes < 1024) return `${Math.round(bytes)} B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  return `${(bytes / 1024 ** 3).toFixed(1)} GB`;
});
const mediaUrl = computed(() => {
  const row = contentObject.value;
  if (!row) return "";
  const data = String(row.data_b64 || "").trim();
  if (data) return `data:${mimeType.value};base64,${data}`;
  const direct = String(row.url || row.src || row.href || "").trim();
  if (direct) {
    const safe = safeHref(direct);
    if (!safe || /^(mailto:|tel:|#)/i.test(safe)) return "";
    if (safe.startsWith("/")) {
      const markerIndex = props.filesEndpoint.indexOf("/api/chat/files");
      const prefix = markerIndex >= 0 ? props.filesEndpoint.slice(0, markerIndex) : "";
      return `${prefix}${safe}`;
    }
    return safe;
  }
  const fileId = String(row.id || row.file_id || "").trim();
  if (!fileId) return "";
  return `${props.filesEndpoint}/${encodeURIComponent(fileId)}?mimetype=${encodeURIComponent(mimeType.value)}`;
});
const plainContent = computed(() => typeof props.message.content === "string" ? props.message.content : "");
const waitText = computed(() => String(contentObject.value?.content || "Working on it…"));
const jsonText = computed(() => {
  try { return JSON.stringify(contentObject.value, null, 2); }
  catch { return String(contentObject.value ?? ""); }
});
</script>

<template>
  <article class="chat-row" :class="[role, { 'typing-indicator': marker === 'typing' }]">
    <div v-if="!isUser" class="chat-avatar">
      <img v-if="avatar" class="chat-avatar-img" :src="avatar" :alt="`${displayName} avatar`" />
      <div v-else class="chat-avatar-fallback assistant">{{ initial }}</div>
    </div>
    <div class="bubble" :class="role">
      <div class="role">{{ displayName }}</div>
      <div v-if="marker === 'typing'" class="bubble-body chat-typing-body" :aria-label="`${displayName} is typing`">
        <span class="chat-typing-label">{{ displayName }} is typing</span>
        <span class="chat-typing-dots" aria-hidden="true"><span /><span /><span /></span>
      </div>
      <div v-else-if="marker === 'plugin_wait'" class="bubble-body">{{ waitText }}</div>
      <div v-else-if="typeof message.content === 'string' && !isUser" class="bubble-body markdown" v-html="renderMarkdown(plainContent)" />
      <div v-else-if="typeof message.content === 'string'" class="bubble-body">{{ plainContent }}</div>
      <img v-else-if="contentType === 'image' && mediaUrl" class="chat-media-image" :src="mediaUrl" :alt="fileName" @load="emit('mediaReady')" @error="emit('mediaReady')" />
      <div v-else-if="contentType === 'audio' && mediaUrl" class="chat-media-wrap">
        <audio controls preload="metadata" :src="mediaUrl" @loadedmetadata="emit('mediaReady')" @error="emit('mediaReady')" />
        <div class="chat-file-meta">{{ fileName }}</div>
        <a class="tv-button tc-download" :href="mediaUrl" :download="fileName">Download audio</a>
      </div>
      <div v-else-if="contentType === 'video' && mediaUrl" class="chat-media-wrap">
        <video controls preload="metadata" :src="mediaUrl" class="chat-media-video" @loadedmetadata="emit('mediaReady')" @error="emit('mediaReady')" />
        <div class="chat-file-meta">{{ fileName }}</div>
        <a class="tv-button tc-download" :href="mediaUrl" :download="fileName">Download video</a>
      </div>
      <div v-else-if="contentType === 'file' && mediaUrl" class="chat-file-card">
        <div class="chat-file-meta">{{ fileName }}<template v-if="sizeLabel"> ({{ sizeLabel }})</template></div>
        <a class="tv-button tc-download" :href="mediaUrl" :download="fileName">Download file</a>
      </div>
      <pre v-else>{{ jsonText }}</pre>
    </div>
    <div v-if="isUser" class="chat-avatar">
      <img v-if="avatar" class="chat-avatar-img" :src="avatar" :alt="`${displayName} avatar`" />
      <div v-else class="chat-avatar-fallback user">{{ initial }}</div>
    </div>
  </article>
</template>
