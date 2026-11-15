<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { getJson, postJson } from "../shared/api";
import PopupTransition from "../shared/PopupTransition.vue";
import UiIcon from "../shared/UiIcon.vue";
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
const activeSectionId = ref("overview");
const updatesOpen = ref(false);

const payload = computed(() => props.state.payload || {});
const sections = computed(() => (Array.isArray(payload.value.sections) ? payload.value.sections : []));
const sectionOrder = ["environment", "awareness", "personal", "voice"];
const sortedSections = computed(() =>
  [...sections.value].sort((left, right) => {
    const rank = (section: JsonRow) => {
      const index = sectionOrder.indexOf(sectionId(section));
      return index < 0 ? sectionOrder.length : index;
    };
    return rank(left) - rank(right) || text(left?.title || left?.id).localeCompare(text(right?.title || right?.id), undefined, { sensitivity: "base" });
  })
);
const selectedSection = computed(() => sortedSections.value.find((section) => sectionId(section) === activeSectionId.value));
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
const systemCards = computed(() => (Array.isArray(payload.value.cards) ? payload.value.cards : []).filter((card) => ["tater", "hydra", "surfaces"].includes(text(card.id))));
const updateHeadline = computed(() => {
  const total = Number(updates.value.total || 0);
  if (total > 0) return `${total} ${total === 1 ? "update" : "updates"} available`;
  return updateGroups.value.some((group) => group.error) ? "Updates need a check" : "Everything current";
});

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

function sectionId(section: JsonRow): string {
  return text(section.id || section.title);
}

function sectionTabId(section: JsonRow): string {
  return `td-tab-${encodeURIComponent(sectionId(section))}`;
}

function sectionIcon(section: JsonRow): string {
  return ({ environment: "sun", awareness: "eye", personal: "calendar", voice: "mic", guardian: "shield" } as Record<string, string>)[sectionId(section)] || "dashboard";
}

function sectionSummary(section: JsonRow): string {
  const brief = text(briefFor(section)?.text);
  if (brief) return brief;
  const card = (payload.value.cards || []).find((row) => text(row.id) === sectionId(section));
  return [text(card?.value), text(card?.detail)].filter(Boolean).join(" · ") || text(section.subtitle) || "View this section's latest readings.";
}

function moveSectionFocus(event: KeyboardEvent) {
  if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
  const tabs = Array.from((event.currentTarget as HTMLElement).querySelectorAll<HTMLButtonElement>('[role="tab"]'));
  const current = tabs.indexOf(event.target as HTMLButtonElement);
  if (current < 0) return;
  event.preventDefault();
  const next = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : (current + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length;
  tabs[next]?.click();
  tabs[next]?.focus();
}

watch(sortedSections, () => {
  if (activeSectionId.value !== "overview" && !selectedSection.value) activeSectionId.value = "overview";
});

watch([showMetrics, showMedia], savePreferences);
watch(
  () => props.state.payload,
  () => {
    personalId.value = text(personal.value.person_id);
    refreshSeconds.value = Number(refreshSettings.value.refresh_interval_seconds ?? 300);
    briefRefreshSeconds.value = Number(refreshSettings.value.brief_refresh_interval_seconds ?? 3600);
    scheduleRefresh();
  },
  { immediate: true },
);

onMounted(() => {
  void refresh({ quiet: true });
});
onBeforeUnmount(() => {
  window.clearInterval(refreshTimer);
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
    <header class="tv-page-heading td-page-heading">
      <div>
        <h1>Dashboard</h1>
        <p class="td-page-status">
          <span class="tv-live-pill" :class="{ busy: Boolean(busy) }"><i />{{ busy || "Live" }}</span>
          <span v-if="payload.generated_at">· Updated {{ timeLabel(payload.generated_at) }}</span>
        </p>
      </div>
      <button type="button" class="tv-button" :aria-expanded="controlsOpen" @click="controlsOpen = true"><UiIcon name="sliders" />Controls</button>
    </header>

    <div v-if="error" class="tv-notice error" role="alert">{{ error }}</div>

    <section class="td-overview tv-panel">
      <header class="td-card-heading"><UiIcon name="sparkles" /><h2>{{ overviewBrief?.title || "Home Brief" }}</h2><span>Today</span></header>
      <p v-if="overviewBrief?.text">{{ overviewBrief.text }}</p>
      <p v-else class="td-pending-brief">Your home brief will appear here when it is ready.</p>
      <footer class="workspace-brief-footer"><span>{{ overviewBrief?.updated_at ? `Generated ${timeLabel(overviewBrief.updated_at)}` : "Home summary" }}</span><button type="button" class="workspace-text-button" :disabled="Boolean(busy)" @click="refreshBriefs"><UiIcon name="refresh" />Refresh brief</button></footer>
    </section>

    <nav class="td-section-tabs" role="tablist" aria-label="Dashboard sections" @keydown="moveSectionFocus">
      <button id="td-tab-overview" type="button" role="tab" aria-controls="td-section-content" :aria-selected="activeSectionId === 'overview'" :tabindex="activeSectionId === 'overview' ? 0 : -1" @click="activeSectionId = 'overview'"><UiIcon name="dashboard" />Overview</button>
      <button v-for="section in sortedSections" :id="sectionTabId(section)" :key="sectionId(section)" type="button" role="tab" aria-controls="td-section-content" :aria-selected="activeSectionId === sectionId(section)" :tabindex="activeSectionId === sectionId(section) ? 0 : -1" @click="activeSectionId = sectionId(section)"><UiIcon :name="sectionIcon(section)" />{{ section.title || section.id }}</button>
    </nav>

    <div class="td-navigator-layout">
      <div id="td-section-content" class="td-section-content" role="tabpanel" :aria-labelledby="selectedSection ? sectionTabId(selectedSection) : 'td-tab-overview'">
        <section v-if="activeSectionId === 'overview'" class="tv-panel td-section-overview" aria-label="Section overview">
          <article v-for="section in sortedSections" :key="sectionId(section)" class="td-overview-row">
            <UiIcon :name="sectionIcon(section)" />
            <div><h2>{{ section.title || section.id }}</h2><p>{{ sectionSummary(section) }}</p></div>
            <button type="button" class="workspace-text-button" :aria-label="`View ${text(section.title || section.id)}`" @click="activeSectionId = sectionId(section)"><span>View</span><UiIcon name="chevron" /></button>
          </article>
          <p v-if="!sortedSections.length" class="td-empty-copy">Live sections will appear as your connected cores report in.</p>
        </section>

        <article v-else-if="selectedSection" class="tv-panel td-section" :class="`section-${sectionId(selectedSection)}`">
          <header class="tv-panel-head">
            <div><div class="td-card-heading"><UiIcon :name="sectionIcon(selectedSection)" /><h2>{{ selectedSection.title || selectedSection.id }}</h2></div><p>{{ selectedSection.subtitle }}</p></div>
            <span v-if="briefFor(selectedSection)?.updated_at">{{ timeLabel(briefFor(selectedSection)?.updated_at) }}</span>
          </header>
          <p v-if="briefFor(selectedSection)?.text" class="td-brief">{{ briefFor(selectedSection)?.text }}</p>
          <div v-if="showMetrics && selectedSection.stats?.length" class="td-metric-pills">
            <span v-for="stat in selectedSection.stats" :key="text(stat.label)"><strong>{{ stat.value ?? "-" }}</strong>{{ stat.label }}</span>
          </div>
          <div v-if="sectionItems(selectedSection).length" class="td-items">
            <article v-for="(item, index) in sectionItems(selectedSection)" :key="text(item.id || item.title || index)" class="td-item">
              <img v-if="showMedia && itemImage(item)" :src="itemImage(item)" :alt="text(item.image_alt || item.title || 'Dashboard image')" loading="lazy" />
              <UiIcon v-else :name="sectionIcon(selectedSection)" />
              <div><strong>{{ item.title || item.name || item.label || "Signal" }}</strong><span>{{ item.subtitle || item.when || item.state || item.detail }}</span></div>
            </article>
          </div>
          <p v-if="!briefFor(selectedSection)?.text && !selectedSection.stats?.length && !sectionItems(selectedSection).length" class="td-empty-copy">No recent readings for this section yet.</p>
        </article>
      </div>

      <aside class="td-status-rail" aria-label="Tater status and updates">
        <section v-if="systemBrief?.text || systemCards.length" class="tv-panel td-system-brief">
          <header class="td-card-heading"><UiIcon name="activity" /><h2>Tater status</h2><span>Live</span></header>
          <h3 v-if="systemBrief?.title && systemBrief.title !== 'Tater status'">{{ systemBrief.title }}</h3>
          <p v-if="systemBrief?.text">{{ systemBrief.text }}</p>
          <div v-if="systemCards.length" class="td-system-lines">
            <div v-for="card in systemCards" :key="text(card.id)" class="td-system-line"><span>{{ card.label || card.id }}</span><div><strong :class="{ good: card.tone === 'good', warning: card.tone === 'warning' }">{{ card.value }}</strong><small v-if="card.detail">{{ card.detail }}</small></div></div>
          </div>
        </section>

        <section v-if="updateGroups.length" class="td-update-watch">
          <button type="button" class="td-update-summary" :aria-expanded="updatesOpen" aria-controls="td-update-groups" @click="updatesOpen = !updatesOpen">
            <UiIcon name="download" />
            <span><strong>{{ updateHeadline }}</strong><small>{{ updates.summary || "Firmware and Tater Shop surfaces checked." }}</small><span>{{ updatesOpen ? "Hide updates" : "View updates" }}</span></span>
            <UiIcon name="chevron" />
          </button>
          <div v-if="updatesOpen" id="td-update-groups" class="td-update-groups">
            <button v-for="group in updateGroups" :key="text(group.kind)" type="button" @click="navigate(group.kind)">
              <div><strong>{{ group.label || group.kind }}</strong><span :class="{ available: group.error || Number(group.count || 0) > 0 }">{{ group.error ? "Needs check" : Number(group.count || 0) ? `${group.count} available` : "Current" }}</span></div>
              <small v-if="group.items?.length">{{ group.items.slice(0, 3).map((row: JsonRow) => row.name || row.id).join(" • ") }}</small>
              <small v-else>{{ group.error || "No pending updates" }}</small>
              <small v-if="group.error && group.items?.length">{{ group.error }}</small>
            </button>
          </div>
        </section>
      </aside>
    </div>

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
          <footer><span>{{ busy || error }}</span><div><button class="tv-button" type="button" :disabled="Boolean(busy)" @click="refresh({ snapshot: true })">Refresh snapshot</button><button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="refreshBriefs">Generate briefs</button></div></footer>
        </section>
    </PopupTransition>
  </div>
</template>
