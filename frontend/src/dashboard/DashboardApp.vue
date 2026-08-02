<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { getJson, postJson } from "../shared/api";
import PopupTransition from "../shared/PopupTransition.vue";
import type { DashboardMountOptions, DashboardPayload, JsonRow } from "./types";

const props = defineProps<{
  state: { payload: DashboardPayload };
  options: DashboardMountOptions;
}>();

const controlsOpen = ref(false);
const busy = ref("");
const error = ref("");
const showMetrics = ref(props.options.initialPreferences?.showMetrics !== false);
const showMedia = ref(props.options.initialPreferences?.showMedia !== false);
let refreshTimer = 0;
let sectionLayoutFrame = 0;
let sectionResizeObserver: ResizeObserver | null = null;
const sectionGrid = ref<HTMLElement | null>(null);

const payload = computed(() => props.state.payload || {});
const sections = computed(() => (Array.isArray(payload.value.sections) ? payload.value.sections : []));
const sortedSections = computed(() =>
  [...sections.value].sort((left, right) =>
    text(left?.title || left?.id).localeCompare(text(right?.title || right?.id), undefined, { sensitivity: "base" })
  )
);
const briefRows = computed(() => (Array.isArray(payload.value.briefs) ? payload.value.briefs : []));
const briefById = computed(() => new Map(briefRows.value.map((row) => [String(row?.id || ""), row])));
const updates = computed(() => (payload.value.updates && typeof payload.value.updates === "object" ? payload.value.updates : {}));
const updateGroups = computed(() => (Array.isArray(updates.value.groups) ? updates.value.groups : []));
const personal = computed(() => payload.value.settings?.personal || {});
const refreshSettings = computed(() => payload.value.settings?.refresh || {});
const personalId = ref("");
const refreshSeconds = ref(300);
const briefRefreshSeconds = ref(3600);

const overviewBrief = computed(() => briefById.value.get("overview"));
const systemBrief = computed(() => briefById.value.get("system"));

function text(value: unknown): string {
  return String(value ?? "").trim();
}

function timeLabel(epoch: unknown): string {
  const value = Number(epoch || 0);
  if (!Number.isFinite(value) || value <= 0) return "";
  return new Intl.DateTimeFormat(undefined, { hour: "numeric", minute: "2-digit" }).format(new Date(value * 1000));
}

function briefFor(section: JsonRow): JsonRow | undefined {
  return briefById.value.get(text(section?.id));
}

function sectionItems(section: JsonRow): JsonRow[] {
  const keys = ["outlook_items", "snapshots", "visuals", "recent_events", "events", "devices"];
  for (const key of keys) {
    const rows = section?.[key];
    if (Array.isArray(rows) && rows.length) return rows.slice(0, 8);
  }
  return [];
}

function itemImage(row: JsonRow): string {
  return text(row?.image_src || row?.hero_image_src || row?.image);
}

function navigate(target: unknown) {
  const value = text(target);
  if (value) props.options.onNavigate?.(value);
}

function notify(message: string, toneValue = "success") {
  props.options.onToast?.(message, toneValue);
}

function applyPayload(next: DashboardPayload) {
  props.state.payload = next;
  props.options.onPayloadChange?.(next);
}

async function refresh(options: { snapshot?: boolean; quiet?: boolean } = {}) {
  if (busy.value && options.quiet) return;
  if (!options.quiet) busy.value = options.snapshot ? "Refreshing live snapshot…" : "Refreshing dashboard…";
  error.value = "";
  try {
    const separator = props.options.dashboardEndpoint.includes("?") ? "&" : "?";
    const endpoint = options.snapshot
      ? `${props.options.dashboardEndpoint}${separator}refresh_snapshot=true`
      : props.options.dashboardEndpoint;
    applyPayload(await getJson<DashboardPayload>(endpoint));
    if (!options.quiet) notify("Dashboard refreshed.");
  } catch (requestError) {
    error.value = requestError instanceof Error ? requestError.message : "Dashboard refresh failed.";
    if (!options.quiet) notify(error.value, "error");
  } finally {
    if (!options.quiet) busy.value = "";
  }
}

async function refreshBriefs() {
  busy.value = "Generating fresh briefs…";
  error.value = "";
  try {
    applyPayload(await postJson<DashboardPayload>(props.options.refreshBriefsEndpoint, { brief_id: null }));
    notify("Dashboard brief refresh queued.");
  } catch (requestError) {
    error.value = requestError instanceof Error ? requestError.message : "Brief refresh failed.";
    notify(error.value, "error");
  } finally {
    busy.value = "";
  }
}

async function saveSettings(changes: Record<string, unknown>, message: string) {
  busy.value = "Saving dashboard controls…";
  error.value = "";
  try {
    applyPayload(await postJson<DashboardPayload>(props.options.settingsEndpoint, changes));
    notify(message);
  } catch (requestError) {
    error.value = requestError instanceof Error ? requestError.message : "Dashboard settings failed to save.";
    notify(error.value, "error");
  } finally {
    busy.value = "";
  }
}

function savePreferences() {
  props.options.onPreferencesChange?.({
    showMetrics: showMetrics.value,
    showMedia: showMedia.value,
  });
}

function scheduleRefresh() {
  window.clearInterval(refreshTimer);
  const seconds = Number(refreshSettings.value.refresh_interval_seconds || refreshSeconds.value || 0);
  if (seconds > 0) {
    refreshTimer = window.setInterval(() => void refresh({ quiet: true }), Math.max(15, seconds) * 1000);
  }
}

function layoutSectionCards() {
  sectionLayoutFrame = 0;
  const grid = sectionGrid.value;
  if (!grid) return;
  const gridStyle = window.getComputedStyle(grid);
  const rowHeight = Number.parseFloat(gridStyle.gridAutoRows) || 8;
  const rowGap = Number.parseFloat(gridStyle.rowGap) || 12;
  const cards = Array.from(grid.children).filter((row): row is HTMLElement => row instanceof HTMLElement);
  cards.forEach((card) => { card.style.gridRowEnd = "auto"; });
  cards.forEach((card) => {
    const height = card.getBoundingClientRect().height;
    const span = Math.max(1, Math.ceil((height + rowGap) / (rowHeight + rowGap)));
    card.style.gridRowEnd = `span ${span}`;
  });
}

function scheduleSectionLayout() {
  window.cancelAnimationFrame(sectionLayoutFrame);
  sectionLayoutFrame = window.requestAnimationFrame(layoutSectionCards);
}

function observeSectionCards() {
  sectionResizeObserver?.disconnect();
  const grid = sectionGrid.value;
  if (!grid) return;
  sectionResizeObserver = new ResizeObserver(scheduleSectionLayout);
  sectionResizeObserver.observe(grid);
  Array.from(grid.children).forEach((card) => sectionResizeObserver?.observe(card));
  scheduleSectionLayout();
}

watch([showMetrics, showMedia], savePreferences);
watch(
  () => props.state.payload,
  () => {
    personalId.value = text(personal.value.person_id);
    refreshSeconds.value = Number(refreshSettings.value.refresh_interval_seconds ?? 300);
    briefRefreshSeconds.value = Number(refreshSettings.value.brief_refresh_interval_seconds ?? 3600);
    scheduleRefresh();
    void nextTick().then(observeSectionCards);
  },
  { immediate: true },
);

onMounted(() => {
  void refresh({ quiet: true });
  void nextTick().then(observeSectionCards);
});
onBeforeUnmount(() => {
  window.clearInterval(refreshTimer);
  window.cancelAnimationFrame(sectionLayoutFrame);
  sectionResizeObserver?.disconnect();
});

const refreshOptions = [
  [0, "Off"], [30, "30 seconds"], [60, "1 minute"], [300, "5 minutes"], [900, "15 minutes"], [1800, "30 minutes"], [3600, "1 hour"], [7200, "2 hours"], [14400, "4 hours"],
];
const briefOptions = [
  [0, "Off"], [300, "5 minutes"], [900, "15 minutes"], [1800, "30 minutes"], [3600, "1 hour"], [7200, "2 hours"], [14400, "4 hours"], [21600, "6 hours"], [43200, "12 hours"],
];
</script>

<template>
  <div class="tater-vue-surface td-dashboard">
    <header class="tv-page-heading">
      <div>
        <span class="tv-eyebrow">Home at a glance</span>
        <h1>Dashboard</h1>
        <p>
          <span v-if="payload.generated_at">Updated {{ timeLabel(payload.generated_at) }}</span>
          <span v-else>Live status, signals, and Tater summaries.</span>
        </p>
      </div>
      <div class="tv-heading-actions">
        <span class="tv-live-pill" :class="{ busy: Boolean(busy) }"><i />{{ busy || "Live" }}</span>
        <button type="button" class="tv-button" @click="controlsOpen = true">Controls</button>
      </div>
    </header>

    <div v-if="error" class="tv-notice error">{{ error }}</div>

    <section v-if="overviewBrief?.text" class="td-overview tv-panel">
      <div>
        <span class="tv-eyebrow">Today</span>
        <h2>{{ overviewBrief.title || "Home Brief" }}</h2>
      </div>
      <p>{{ overviewBrief.text }}</p>
    </section>

    <section v-if="updateGroups.length" class="tv-panel td-updates">
      <div class="tv-panel-head">
        <div><span class="tv-eyebrow">Update watch</span><h2>{{ Number(updates.total || 0) ? `${updates.total} available` : "Everything current" }}</h2></div>
        <span>{{ updates.summary || "Firmware and Tater Shop surfaces checked." }}</span>
      </div>
      <div class="td-update-grid">
        <button v-for="group in updateGroups" :key="text(group.kind)" type="button" @click="navigate(group.kind)">
          <span>{{ group.label || group.kind }}</span>
          <strong>{{ group.error ? "Needs check" : Number(group.count || 0) ? `${group.count} available` : "Current" }}</strong>
          <small v-if="group.items?.length">{{ group.items.slice(0, 3).map((row: JsonRow) => row.name || row.id).join(" • ") }}</small>
          <small v-else>{{ group.error || "No pending updates" }}</small>
        </button>
      </div>
    </section>

    <section v-if="systemBrief?.text" class="tv-panel td-system-brief">
      <span class="tv-eyebrow">Tater</span>
      <h2>{{ systemBrief.title || "System summary" }}</h2>
      <p>{{ systemBrief.text }}</p>
    </section>

    <section ref="sectionGrid" class="td-section-grid">
      <article v-for="section in sortedSections" :key="text(section.id)" class="tv-panel td-section" :class="`section-${text(section.id)}`">
        <header class="tv-panel-head">
          <div><span class="tv-eyebrow">{{ section.id }}</span><h2>{{ section.title || section.id }}</h2><p>{{ section.subtitle }}</p></div>
          <span v-if="briefFor(section)?.updated_at">{{ timeLabel(briefFor(section)?.updated_at) }}</span>
        </header>
        <p v-if="briefFor(section)?.text" class="td-brief">{{ briefFor(section)?.text }}</p>
        <div v-if="showMetrics && section.stats?.length" class="tv-metrics">
          <div v-for="stat in section.stats" :key="text(stat.label)"><span>{{ stat.label }}</span><strong>{{ stat.value ?? "-" }}</strong></div>
        </div>
        <div v-if="sectionItems(section).length" class="td-items">
          <article v-for="(item, index) in sectionItems(section)" :key="text(item.id || item.title || index)" class="td-item">
            <img v-if="showMedia && itemImage(item)" :src="itemImage(item)" :alt="text(item.image_alt || item.title || 'Dashboard image')" loading="lazy" />
            <div><strong>{{ item.title || item.name || item.label || "Signal" }}</strong><span>{{ item.subtitle || item.when || item.state || item.detail }}</span></div>
          </article>
        </div>
      </article>
    </section>

    <PopupTransition :open="controlsOpen" @close="controlsOpen = false">
        <section class="tv-modal td-dashboard-controls" role="dialog" aria-modal="true" aria-label="Dashboard controls">
          <header><div><span class="tv-eyebrow">Dashboard</span><h2>Controls</h2></div><button class="tv-button" type="button" @click="controlsOpen = false">Close</button></header>
          <div class="tv-control-list">
            <label class="tv-toggle"><input v-model="showMetrics" class="tv-checkbox" type="checkbox" /><span><strong>Metric pills</strong><small>Show compact live readings inside each area.</small></span></label>
            <label class="tv-toggle"><input v-model="showMedia" class="tv-checkbox" type="checkbox" /><span><strong>Media</strong><small>Show snapshots and satellite images when available.</small></span></label>
            <label><span>Dashboard refresh</span><select v-model.number="refreshSeconds" @change="saveSettings({ refresh_interval_seconds: refreshSeconds, brief_refresh_interval_seconds: briefRefreshSeconds }, 'Dashboard refresh updated.')"><option v-for="row in refreshOptions" :key="row[0]" :value="row[0]">{{ row[1] }}</option></select></label>
            <label><span>Brief refresh</span><select v-model.number="briefRefreshSeconds" @change="saveSettings({ refresh_interval_seconds: refreshSeconds, brief_refresh_interval_seconds: briefRefreshSeconds }, 'Brief refresh updated.')"><option v-for="row in briefOptions" :key="row[0]" :value="row[0]">{{ row[1] }}</option></select></label>
            <label><span>Personal profile</span><select v-model="personalId" @change="saveSettings({ personal_person_id: personalId || null }, 'Personal dashboard profile updated.')"><option value="">All people</option><option v-for="row in personal.people_options || []" :key="text(row.value)" :value="text(row.value)">{{ row.label || row.value }}</option></select></label>
          </div>
          <footer><span>{{ busy || error }}</span><div><button class="tv-button" type="button" @click="refresh({ snapshot: true })">Refresh snapshot</button><button class="tv-button primary" type="button" @click="refreshBriefs">Generate briefs</button></div></footer>
        </section>
    </PopupTransition>
  </div>
</template>
