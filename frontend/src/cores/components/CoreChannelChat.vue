<script setup lang="ts">
import { computed, nextTick, onMounted, reactive, ref, watch } from "vue";
import { postJson } from "../../shared/api";
import type { CoreTabSpec, JsonRow } from "../types";
import CoreManagerItems from "./CoreManagerItems.vue";

const props = defineProps<{
  payload: JsonRow;
  tab: CoreTabSpec;
  actionEndpoint: string;
  refresh: () => Promise<void>;
  notify?: (message: string, tone?: string) => void;
}>();

const activeTab = ref("chat");
const activeGroup = ref("");
const selectedChannel = ref("");
const drafts = reactive<Record<string, string>>({});
const busyKeys = ref<string[]>([]);
const statusText = ref("");
const error = ref("");
const feed = ref<HTMLElement | null>(null);
const keepAtBottom = ref(true);

const body = computed<JsonRow>(() => props.payload && typeof props.payload === "object" ? props.payload : {});
const ui = computed<JsonRow>(() => body.value.ui && typeof body.value.ui === "object" ? body.value.ui : {});
const connection = computed<JsonRow>(() => ui.value.status && typeof ui.value.status === "object" ? ui.value.status : {});
const channels = computed<JsonRow[]>(() => (Array.isArray(ui.value.channels) ? ui.value.channels : []).filter((row: JsonRow) => text(row.id)));
const messages = computed<JsonRow[]>(() => Array.isArray(ui.value.messages) ? ui.value.messages : []);
const composer = computed<JsonRow>(() => ui.value.composer && typeof ui.value.composer === "object" ? ui.value.composer : {});
const managerTabs = computed<JsonRow[]>(() => (Array.isArray(ui.value.manager_tabs) ? ui.value.manager_tabs : []).filter((row: JsonRow) => text(row.key)));
const topTabs = computed<JsonRow[]>(() => [{ key: "chat", label: ui.value.chat_label || "Chat" }, ...managerTabs.value.filter((row) => text(row.key) !== "chat")]);
const currentManager = computed<JsonRow | null>(() => managerTabs.value.find((row) => text(row.key) === activeTab.value) || null);
const groupTabs = computed<JsonRow[]>(() => currentManager.value && Array.isArray(currentManager.value.groups) ? currentManager.value.groups.filter((row: JsonRow) => text(row.key)) : []);
const currentGroup = computed<JsonRow | null>(() => groupTabs.value.find((row) => text(row.key) === activeGroup.value) || groupTabs.value[0] || null);
const itemForms = computed<JsonRow[]>(() => Array.isArray(ui.value.item_forms) ? ui.value.item_forms : []);
const featuredItems = computed<JsonRow[]>(() => {
  const group = text(currentManager.value?.featured_item_group);
  return group ? itemsFor(group) : [];
});
const currentChannel = computed<JsonRow | null>(() => channels.value.find((row) => text(row.id) === selectedChannel.value) || channels.value[0] || null);
const filteredMessages = computed<JsonRow[]>(() => messages.value.filter((row) => (text(row.channel) || "0") === selectedChannel.value));
const draft = computed({
  get: () => drafts[selectedChannel.value] || "",
  set: (value: string) => { drafts[selectedChannel.value] = value; },
});
const maxLength = computed(() => Math.max(1, Number(composer.value.max_length || 200)));

function text(value: unknown): string { return String(value ?? "").trim(); }
function isBusy(key: string): boolean { return busyKeys.value.includes(key); }
function setBusy(key: string, active: boolean) {
  const next = new Set(busyKeys.value);
  if (active) next.add(key); else next.delete(key);
  busyKeys.value = [...next];
}
function decorate(items: JsonRow[]): JsonRow[] { return items.map((item) => ({ ...item, core_key: props.tab.core_key })); }
function itemsFor(group: unknown): JsonRow[] {
  const wanted = text(group).toLowerCase();
  return decorate(wanted ? itemForms.value.filter((item) => text(item.group).toLowerCase() === wanted) : itemForms.value);
}
function listOptions(source: JsonRow | null): JsonRow {
  if (!source) return {};
  return {
    selector: Boolean(source.selector),
    selector_label: source.selector_label,
    item_group: source.item_group,
    page_size: source.page_size,
    empty_message: source.empty_message || body.value.empty_message,
  };
}
async function run(action: string, payload: JsonRow, busyKey = action, successText = "Done."): Promise<JsonRow | null> {
  const name = text(action);
  if (!name || isBusy(busyKey)) return null;
  setBusy(busyKey, true);
  error.value = "";
  statusText.value = "Working…";
  try {
    const result = await postJson<JsonRow>(props.actionEndpoint, { action: name, payload: payload || {} });
    const message = text(result?.message) || successText;
    statusText.value = message;
    await props.refresh();
    props.notify?.(message, "success");
    return result || {};
  } catch (requestError) {
    const message = requestError instanceof Error ? requestError.message : "Core action failed.";
    error.value = message;
    statusText.value = "";
    props.notify?.(message, "error");
    return null;
  } finally { setBusy(busyKey, false); }
}
async function sendMessage() {
  const message = draft.value.trim();
  const action = text(composer.value.action) || "send_message";
  if (!message || isBusy("chat:send")) return;
  const result = await run(
    action,
    { values: { text: message, channel: selectedChannel.value || "0", destination: text(composer.value.destination) || "broadcast" } },
    "chat:send",
    "Message queued for Meshtastic.",
  );
  if (result) {
    draft.value = "";
    keepAtBottom.value = true;
    await scrollToBottom(true);
  }
}
function handleComposerKey(event: KeyboardEvent) {
  if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
    event.preventDefault();
    void sendMessage();
  }
}
function selectChannel(channel: JsonRow) {
  selectedChannel.value = text(channel.id) || "0";
  keepAtBottom.value = true;
}
function updateScrollIntent() {
  const element = feed.value;
  if (!element) return;
  keepAtBottom.value = element.scrollHeight - element.scrollTop - element.clientHeight < 72;
}
async function scrollToBottom(force = false) {
  await nextTick();
  const element = feed.value;
  if (element && (force || keepAtBottom.value)) element.scrollTop = element.scrollHeight;
}
function initials(message: JsonRow): string {
  const source = text(message.sender_name || message.sender_id || "Mesh");
  const parts = source.split(/\s+/).filter(Boolean);
  return (parts.length > 1 ? `${parts[0][0]}${parts[parts.length - 1][0]}` : source.slice(0, 2)).toUpperCase();
}
function formatTime(value: unknown): string {
  const raw = text(value);
  if (!raw) return "";
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return raw;
  const today = new Date();
  const sameDay = date.toDateString() === today.toDateString();
  return new Intl.DateTimeFormat(undefined, sameDay
    ? { hour: "numeric", minute: "2-digit" }
    : { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(date);
}

watch(channels, (rows) => {
  const available = new Set(rows.map((row) => text(row.id)));
  if (!available.has(selectedChannel.value)) {
    const saved = (() => { try { return window.localStorage.getItem(`tater-channel-chat:${props.tab.core_key}`) || ""; } catch { return ""; } })();
    const requested = text(ui.value.default_channel);
    selectedChannel.value = available.has(saved) ? saved : available.has(requested) ? requested : text(rows[0]?.id) || "0";
  }
}, { immediate: true });
watch(selectedChannel, (value) => {
  try { window.localStorage.setItem(`tater-channel-chat:${props.tab.core_key}`, value); } catch { /* Storage is optional. */ }
  void scrollToBottom(true);
});
watch(groupTabs, (rows) => {
  const available = new Set(rows.map((row) => text(row.key)));
  if (!available.has(activeGroup.value)) activeGroup.value = text(rows[0]?.key);
}, { immediate: true });
watch(() => filteredMessages.value.length, () => void scrollToBottom());
onMounted(() => void scrollToBottom(true));
</script>

<template>
  <div class="card core-channel-chat" data-core-renderer="vue-channel-chat" :data-core-live-updates="ui.live_updates ? '1' : '0'">
    <header class="core-chat-heading">
      <div><span class="tv-eyebrow">Live radio workspace</span><h3>{{ ui.title || tab.label || tab.core_key }}</h3><p>{{ body.summary }}</p></div>
      <div class="core-chat-connection" :class="{ connected: connection.connected }">
        <span><i />{{ connection.label || (connection.connected ? 'Connected' : 'Disconnected') }}</span>
        <small>{{ connection.radio_name || connection.transport || tab.core_key }}</small>
        <button class="tv-button" type="button" :disabled="isBusy('chat:refresh')" @click="run(connection.refresh_action || 'refresh', {}, 'chat:refresh', 'Meshtastic refreshed.')">Refresh</button>
      </div>
    </header>

    <div v-if="error || statusText || connection.error" class="tv-notice compact" :class="{ error: Boolean(error || connection.error) }">{{ error || connection.error || statusText }}</div>

    <nav class="core-manager-tabs core-channel-top-tabs" aria-label="Meshtastic sections">
      <button v-for="entry in topTabs" :key="entry.key" type="button" class="core-manager-tab-btn" :class="{ active: activeTab === entry.key }" @click="activeTab = entry.key">{{ entry.label || entry.key }}</button>
    </nav>

    <section v-if="activeTab === 'chat'" class="core-channel-chat-shell">
      <aside class="core-channel-rail" aria-label="Mesh channels">
        <div class="core-channel-rail-title"><span>Channels</span><small>{{ channels.length }}</small></div>
        <button v-for="channel in channels" :key="channel.id" type="button" :class="{ active: selectedChannel === String(channel.id) }" @click="selectChannel(channel)">
          <span class="core-channel-hash">#</span>
          <span><strong>{{ channel.label || `Channel ${channel.id}` }}</strong><small>{{ channel.subtitle || `Channel ${channel.id}` }}</small></span>
          <em>{{ channel.message_count || 0 }}</em>
        </button>
      </aside>

      <div class="core-channel-conversation">
        <header>
          <div><span class="core-channel-hash">#</span><div><h4>{{ currentChannel?.label || 'Channel' }}</h4><small>{{ currentChannel?.subtitle || 'Meshtastic broadcast channel' }}</small></div></div>
          <span>{{ filteredMessages.length }} message{{ filteredMessages.length === 1 ? '' : 's' }}</span>
        </header>

        <div ref="feed" class="core-channel-feed" role="log" aria-live="polite" @scroll="updateScrollIntent">
          <div v-if="!filteredMessages.length" class="core-channel-empty"><span>#</span><h4>No messages here yet</h4><p>Messages received by this core will be saved and appear live in this channel.</p></div>
          <article v-for="message in filteredMessages" :key="`${message.id}:${message.direction}`" class="core-channel-message" :class="{ outbound: message.direction === 'outbound' }">
            <div v-if="message.direction !== 'outbound'" class="core-channel-avatar" aria-hidden="true">{{ initials(message) }}</div>
            <div class="core-channel-message-body">
              <div class="core-channel-message-author"><strong>{{ message.direction === 'outbound' ? 'You' : message.sender_name || message.sender_id || 'Mesh' }}</strong><span>{{ formatTime(message.timestamp) }}</span></div>
              <div class="core-channel-bubble">{{ message.text || '(empty message)' }}</div>
              <small v-if="message.direction !== 'outbound' && message.sender_id && message.sender_id !== message.sender_name">{{ message.sender_id }}</small>
            </div>
          </article>
        </div>

        <form class="core-channel-composer" @submit.prevent="sendMessage">
          <textarea v-model="draft" :maxlength="maxLength" :placeholder="composer.placeholder || `Message #${currentChannel?.label || 'channel'}…`" rows="2" :disabled="!connection.connected || isBusy('chat:send')" @keydown="handleComposerKey" />
          <div><small>Enter to send · Shift+Enter for a new line</small><span>{{ draft.length }}/{{ maxLength }}</span><button class="tv-button primary" type="submit" :disabled="!connection.connected || !draft.trim() || isBusy('chat:send')">{{ isBusy('chat:send') ? 'Sending…' : composer.send_label || 'Send' }}</button></div>
        </form>
      </div>
    </section>

    <section v-else-if="currentManager" class="card core-channel-manager">
      <header class="card-head"><div><span class="tv-eyebrow">Meshtastic</span><h3 class="card-title">{{ currentManager.label || currentManager.key }}</h3></div></header>
      <div v-if="featuredItems.length" class="core-channel-manager-featured">
        <div class="core-channel-manager-featured-label"><span>{{ currentManager.featured_label || 'Current connection' }}</span><i /></div>
        <CoreManagerItems :items="featuredItems" :ui="ui" :options="listOptions({ item_group: currentManager.featured_item_group })" :run="run" :busy="isBusy" />
      </div>
      <template v-if="currentManager.source === 'grouped_items' && groupTabs.length">
        <nav class="core-manager-subtabs" :aria-label="`${currentManager.label || currentManager.key} groups`"><button v-for="group in groupTabs" :key="group.key" type="button" class="core-manager-subtab-btn" :class="{ active: activeGroup === group.key }" @click="activeGroup = group.key">{{ group.label || group.key }}</button></nav>
        <CoreManagerItems v-if="currentGroup" :items="itemsFor(currentGroup.item_group || currentGroup.key)" :ui="ui" :options="listOptions(currentGroup)" :run="run" :busy="isBusy" />
      </template>
      <CoreManagerItems v-else :items="itemsFor(currentManager.item_group)" :ui="ui" :options="listOptions(currentManager)" :run="run" :busy="isBusy" />
    </section>
  </div>
</template>
