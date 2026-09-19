<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from "vue";
import { getJson, postJson } from "../../shared/api";
import type { HydraSettings, JsonRow } from "../types";

const props = defineProps<{
  settings: HydraSettings;
  endpoint: string;
  metricsEndpoint: string;
  dataEndpoint: string;
  clearDataEndpoint: string;
}>();

const emit = defineEmits<{
  saved: [settings: HydraSettings];
  notify: [message: string, tone?: string];
}>();

const activeTab = ref<"settings" | "metrics" | "data">("settings");
const saving = ref(false);
const dirty = ref(false);
const notice = ref("");
const error = ref("");
const metrics = ref<JsonRow | null>(null);
const data = ref<JsonRow | null>(null);
const metricsLoading = ref(false);
const dataLoading = ref(false);
const clearing = ref(false);

const draft = reactive({
  max_display: 8,
  max_store: 20,
  max_llm: 8,
  hydra_max_ledger_items: 1500,
  hydra_astraeus_plan_review_enabled: true,
  hydra_auto_continue_incomplete_final_enabled: false,
});

const filters = reactive({
  platform: "all",
  limit: 50,
  outcome: "all",
  tool: "all",
  toolsOnly: false,
});
const dataPlatform = ref("webui");

const metricNames = computed<string[]>(() => Array.isArray(metrics.value?.metric_names) ? metrics.value?.metric_names : []);
const toolOptions = computed<string[]>(() => {
  const values = Array.isArray(metrics.value?.tool_options) ? metrics.value?.tool_options : [];
  return ["all", ...values.map((value: unknown) => String(value || "")).filter(Boolean)];
});
const platformOptions = computed<string[]>(() => {
  const metricValues = Array.isArray(metrics.value?.platform_options) ? metrics.value?.platform_options : [];
  const dataValues = Array.isArray(data.value?.platform_options) ? data.value?.platform_options : [];
  const values = new Set([...metricValues, ...dataValues].map((value: unknown) => String(value || "")).filter(Boolean));
  values.delete("all");
  return [...values];
});
const ledgerRows = computed<JsonRow[]>(() => Array.isArray(metrics.value?.summary_rows) ? metrics.value?.summary_rows : []);
const dataSummary = computed<JsonRow>(() => data.value?.summary && typeof data.value.summary === "object" ? data.value.summary : {});

function bounded(value: unknown, fallback: number, minimum: number, maximum?: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  const rounded = Math.round(parsed);
  return Math.min(maximum ?? rounded, Math.max(minimum, rounded));
}

function syncFromSettings(settings: HydraSettings) {
  Object.assign(draft, {
    max_display: bounded(settings.max_display, 8, 1),
    max_store: bounded(settings.max_store, 20, 0),
    max_llm: bounded(settings.max_llm, 8, 1),
    hydra_max_ledger_items: bounded(settings.hydra_max_ledger_items, 1500, 1),
    hydra_astraeus_plan_review_enabled: Boolean(settings.hydra_astraeus_plan_review_enabled),
    hydra_auto_continue_incomplete_final_enabled: Boolean(settings.hydra_auto_continue_incomplete_final_enabled),
  });
  dirty.value = false;
}

function markDirty() {
  dirty.value = true;
  notice.value = "";
  error.value = "";
}

function applyDefaults() {
  const defaults = props.settings.defaults || {};
  Object.assign(draft, {
    max_display: bounded(defaults.max_display, 8, 1),
    max_store: bounded(defaults.max_store, 20, 0),
    max_llm: bounded(defaults.max_llm, 8, 1),
    hydra_max_ledger_items: bounded(defaults.hydra_max_ledger_items, 1500, 1),
    hydra_astraeus_plan_review_enabled: defaults.hydra_astraeus_plan_review_enabled !== false,
    hydra_auto_continue_incomplete_final_enabled: Boolean(defaults.hydra_auto_continue_incomplete_final_enabled),
  });
  markDirty();
  notice.value = "Default behavior values loaded. Save to apply them.";
}

async function save() {
  saving.value = true;
  error.value = "";
  notice.value = "";
  const payload = {
    max_display: bounded(draft.max_display, 8, 1),
    max_store: bounded(draft.max_store, 20, 0),
    max_llm: bounded(draft.max_llm, 8, 1),
    hydra_max_ledger_items: bounded(draft.hydra_max_ledger_items, 1500, 1),
    hydra_astraeus_plan_review_enabled: Boolean(draft.hydra_astraeus_plan_review_enabled),
    hydra_auto_continue_incomplete_final_enabled: Boolean(draft.hydra_auto_continue_incomplete_final_enabled),
  };
  try {
    const next = await postJson<HydraSettings>(props.endpoint, payload);
    emit("saved", next);
    syncFromSettings(next);
    notice.value = "Hydra behavior saved and synchronized.";
    emit("notify", notice.value, "success");
  } catch (saveError) {
    error.value = saveError instanceof Error ? saveError.message : "Hydra behavior could not be saved.";
    emit("notify", error.value, "error");
  } finally {
    saving.value = false;
  }
}

function metricLabel(value: unknown): string {
  return String(value || "").replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function platformLabel(value: unknown): string {
  const token = String(value || "").trim();
  if (!token) return "Unknown";
  if (token === "all") return "All portals";
  return metricLabel(token);
}

function rate(value: unknown): string {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed.toFixed(4) : "0.0000";
}

function rows(value: unknown): JsonRow[] {
  return Array.isArray(value) ? value : [];
}

function barWidth(items: unknown, value: unknown): string {
  const list = rows(items);
  const maximum = Math.max(1, ...list.map((item) => Number(item.value || 0)));
  return `${Math.max(2, (Number(value || 0) / maximum) * 100)}%`;
}

async function loadMetrics() {
  metricsLoading.value = true;
  error.value = "";
  const params = new URLSearchParams({
    platform: filters.platform,
    limit: String(bounded(filters.limit, 50, 10, 300)),
    outcome: filters.outcome,
    tool: filters.tool,
    show_only_tool_turns: filters.toolsOnly ? "true" : "false",
  });
  try {
    const payload = await getJson<JsonRow>(`${props.metricsEndpoint}?${params.toString()}`);
    metrics.value = payload;
    if (!toolOptions.value.includes(filters.tool)) filters.tool = "all";
  } catch (loadError) {
    error.value = loadError instanceof Error ? loadError.message : "Hydra metrics could not be loaded.";
  } finally {
    metricsLoading.value = false;
  }
}

async function loadData() {
  dataLoading.value = true;
  error.value = "";
  try {
    data.value = await getJson<JsonRow>(props.dataEndpoint);
    if (!platformOptions.value.includes(dataPlatform.value) && platformOptions.value.length) {
      dataPlatform.value = platformOptions.value[0];
    }
  } catch (loadError) {
    error.value = loadError instanceof Error ? loadError.message : "Hydra data could not be loaded.";
  } finally {
    dataLoading.value = false;
  }
}

async function clearData(mode: "all" | "metrics" | "ledger", platform: string) {
  const target = platform === "all" ? "every portal" : platformLabel(platform);
  const content = mode === "all" ? "metrics and ledger data" : mode;
  if (!window.confirm(`Clear Hydra ${content} for ${target}? This cannot be undone.`)) return;
  clearing.value = true;
  error.value = "";
  try {
    const result = await postJson<JsonRow>(props.clearDataEndpoint, { mode, platform });
    notice.value = `Cleared ${Number(result.metrics_removed || 0)} metric keys and ${Number(result.ledger_removed || 0)} ledger lists.`;
    emit("notify", notice.value, "success");
    await Promise.all([loadData(), loadMetrics()]);
  } catch (clearError) {
    error.value = clearError instanceof Error ? clearError.message : "Hydra data could not be cleared.";
    emit("notify", error.value, "error");
  } finally {
    clearing.value = false;
  }
}

function selectTab(tab: "settings" | "metrics" | "data") {
  activeTab.value = tab;
  if (tab === "metrics" && !metrics.value) void loadMetrics();
  if (tab === "data" && !data.value) void loadData();
}

watch(
  () => props.settings,
  (settings) => {
    if (!dirty.value) syncFromSettings(settings || {});
  },
  { immediate: true },
);

onMounted(() => {
  void Promise.allSettled([loadMetrics(), loadData()]);
});
</script>

<template>
  <section class="tset-resource thydra">
    <div v-if="notice || error" class="tv-notice" :class="{ error: Boolean(error) }" aria-live="polite">
      {{ error || notice }}
    </div>

    <nav class="tv-tabs thydra-tabs" aria-label="Hydra settings sections">
      <button type="button" :class="{ active: activeTab === 'settings' }" @click="selectTab('settings')">Behavior</button>
      <button type="button" :class="{ active: activeTab === 'metrics' }" @click="selectTab('metrics')">Metrics</button>
      <button type="button" :class="{ active: activeTab === 'data' }" @click="selectTab('data')">Stored data</button>
    </nav>

    <template v-if="activeTab === 'settings'">
      <div class="tset-resource-grid">
        <section class="tv-panel tset-form-card">
          <header>
            <span class="tv-eyebrow">Conversation windows</span>
            <h2>History and context</h2>
            <p>Control how much conversation state is displayed, retained, and sent to the active model.</p>
          </header>
          <div class="tv-form-grid">
            <label>Messages shown in WebUI<input v-model.number="draft.max_display" type="number" min="1" @input="markDirty" /></label>
            <label>Maximum stored messages<input v-model.number="draft.max_store" type="number" min="0" @input="markDirty" /><small>Use 0 for unlimited storage.</small></label>
            <label>Messages sent to LLM<input v-model.number="draft.max_llm" type="number" min="1" @input="markDirty" /></label>
            <label>Maximum ledger items<input v-model.number="draft.hydra_max_ledger_items" type="number" min="1" @input="markDirty" /></label>
          </div>
        </section>

        <section class="tv-panel tset-form-card">
          <header>
            <span class="tv-eyebrow">Planning</span>
            <h2>Failure handling</h2>
            <p>Choose whether Hydra performs extra planning and recovery passes.</p>
          </header>
          <div class="tv-form-grid">
            <label class="tv-toggle">
              <input v-model="draft.hydra_astraeus_plan_review_enabled" class="tv-checkbox" type="checkbox" @change="markDirty" />
              <span><strong>Astraeus second plan check</strong><small>May improve planning quality at the cost of latency.</small></span>
            </label>
            <label class="tv-toggle">
              <input v-model="draft.hydra_auto_continue_incomplete_final_enabled" class="tv-checkbox" type="checkbox" @change="markDirty" />
              <span><strong>Continue incomplete final answers</strong><small>Automatically request a continuation when a final response appears truncated.</small></span>
            </label>
          </div>
          <button class="tv-button" type="button" @click="applyDefaults">Load defaults</button>
        </section>
      </div>

      <footer class="tset-save-bar">
        <div><strong>{{ dirty ? "Unsaved changes" : "Hydra behavior is synchronized" }}</strong><span>Model routing remains under the Models tab.</span></div>
        <button class="tv-button primary" type="button" :disabled="saving || !dirty" @click="save">{{ saving ? "Saving…" : "Save Hydra behavior" }}</button>
      </footer>
    </template>

    <template v-else-if="activeTab === 'metrics'">
      <section class="tv-panel tset-form-card">
        <header class="thydra-panel-head">
          <div><span class="tv-eyebrow">Live telemetry</span><h2>Hydra metrics</h2><p>Filter recent planning, tool, and validation activity.</p></div>
          <button class="tv-button" type="button" :disabled="metricsLoading" @click="loadMetrics">{{ metricsLoading ? "Refreshing…" : "Refresh" }}</button>
        </header>
        <div class="tv-form-grid thydra-filters">
          <label>Portal<select v-model="filters.platform" @change="loadMetrics"><option value="all">All portals</option><option v-for="platform in platformOptions" :key="platform" :value="platform">{{ platformLabel(platform) }}</option></select></label>
          <label>Ledger entries<input v-model.number="filters.limit" type="number" min="10" max="300" step="10" @change="loadMetrics" /></label>
          <label>Outcome<select v-model="filters.outcome" @change="loadMetrics"><option value="all">All</option><option value="done">Done</option><option value="blocked">Blocked</option><option value="failed">Failed</option></select></label>
          <label>Tool<select v-model="filters.tool" @change="loadMetrics"><option v-for="tool in toolOptions" :key="tool" :value="tool">{{ tool }}</option></select></label>
          <label class="tv-toggle"><input v-model="filters.toolsOnly" class="tv-checkbox" type="checkbox" @change="loadMetrics" /><span><strong>Tool turns only</strong><small>Hide ledger entries without a planned tool.</small></span></label>
        </div>
      </section>

      <div v-if="metrics" class="thydra-stack">
        <section class="tv-panel tset-form-card">
          <header><span class="tv-eyebrow">Counters</span><h2>Global and selected portal</h2><p>Showing {{ Number(metrics.ledger_filtered || 0) }} of {{ Number(metrics.ledger_total || 0) }} recent ledger rows.</p></header>
          <div class="thydra-counter-groups">
            <div><h3>Global</h3><div class="tv-metrics thydra-metrics"><div v-for="name in metricNames" :key="`global-${name}`"><span>{{ metricLabel(name) }}</span><strong>{{ Number(metrics.global_metrics?.[name] || 0) }}</strong></div></div></div>
            <div><h3>{{ metrics.selected_platform_label || platformLabel(filters.platform) }}</h3><div class="tv-metrics thydra-metrics"><div v-for="name in metricNames" :key="`portal-${name}`"><span>{{ metricLabel(name) }}</span><strong>{{ Number(metrics.platform_metrics?.[name] || 0) }}</strong></div></div></div>
          </div>
        </section>

        <section class="tv-panel tset-form-card">
          <header><span class="tv-eyebrow">Rates</span><h2>Execution quality</h2></header>
          <div class="thydra-rate-grid">
            <div><h3>Global</h3><dl><template v-for="row in rows(metrics.global_rates)" :key="row.metric"><dt>{{ metricLabel(row.metric) }}</dt><dd>{{ rate(row.value) }}</dd></template></dl></div>
            <div><h3>Selected portal</h3><dl><template v-for="row in rows(metrics.platform_rates)" :key="row.metric"><dt>{{ metricLabel(row.metric) }}</dt><dd>{{ rate(row.value) }}</dd></template></dl></div>
          </div>
        </section>

        <section class="tv-panel tset-form-card">
          <header><span class="tv-eyebrow">Portal comparison</span><h2>Per-portal totals</h2></header>
          <div class="thydra-table-wrap"><table><thead><tr><th>Portal</th><th>Turns</th><th>Tools</th><th>Repairs</th><th>Validation failures</th><th>Tool failures</th><th>Tool rate</th><th>Repair rate</th></tr></thead><tbody><tr v-for="row in rows(metrics.platform_rows)" :key="row.platform"><td>{{ row.platform_label || platformLabel(row.platform) }}</td><td>{{ row.total_turns || 0 }}</td><td>{{ row.total_tools_called || 0 }}</td><td>{{ row.total_repairs || 0 }}</td><td>{{ row.validation_failures || 0 }}</td><td>{{ row.tool_failures || 0 }}</td><td>{{ rate(row.tool_call_rate) }}</td><td>{{ rate(row.repair_rate) }}</td></tr></tbody></table></div>
        </section>

        <section class="tv-panel tset-form-card">
          <header><span class="tv-eyebrow">Recent activity</span><h2>Ledger</h2></header>
          <div v-if="ledgerRows.length" class="thydra-ledger">
            <details v-for="(row, index) in ledgerRows" :key="`${row.time}-${index}`">
              <summary><span>{{ row.time || "Recent turn" }}</span><strong>{{ row.outcome || "unknown" }}</strong><span>{{ row.platform || "" }}</span><span>{{ row.planned_tool || "No tool" }}</span><span>{{ row.total_ms || 0 }} ms</span></summary>
              <dl><dt>Scope</dt><dd>{{ row.scope || "—" }}</dd><dt>Planner</dt><dd>{{ row.planner_kind || "—" }}</dd><dt>Validation</dt><dd>{{ row.validation_status || "—" }} {{ row.validation_reason || "" }}</dd><dt>Result</dt><dd>{{ row.tool_result_summary || row.outcome_reason || "—" }}</dd></dl>
              <pre>{{ JSON.stringify(row.raw || row, null, 2) }}</pre>
            </details>
          </div>
          <p v-else class="tv-empty">No ledger rows match these filters.</p>
        </section>

        <div class="tset-resource-grid">
          <section v-for="chart in [{ title: 'Top tools', items: metrics.top_tools }, { title: 'Top failure reasons', items: metrics.top_reasons }]" :key="chart.title" class="tv-panel tset-form-card">
            <header><span class="tv-eyebrow">Filtered ledger</span><h2>{{ chart.title }}</h2></header>
            <div v-if="rows(chart.items).length" class="thydra-bars"><div v-for="item in rows(chart.items)" :key="item.label"><span>{{ item.label }}</span><i><b :style="{ width: barWidth(chart.items, item.value) }" /></i><strong>{{ item.value }}</strong></div></div>
            <p v-else class="tv-empty">No matching activity.</p>
          </section>
        </div>
      </div>
    </template>

    <template v-else>
      <section class="tv-panel tset-form-card">
        <header class="thydra-panel-head"><div><span class="tv-eyebrow">Storage</span><h2>Hydra data</h2><p>Inspect or clear metric counters and execution ledgers.</p></div><button class="tv-button" type="button" :disabled="dataLoading" @click="loadData">{{ dataLoading ? "Refreshing…" : "Refresh" }}</button></header>
        <div class="tv-metrics thydra-data-summary"><div><span>Metric keys</span><strong>{{ Number(dataSummary.metric_keys || 0) }}</strong></div><div><span>Ledger lists</span><strong>{{ Number(dataSummary.ledger_lists || 0) }}</strong></div><div><span>Ledger entries</span><strong>{{ Number(dataSummary.ledger_entries_total || 0) }}</strong></div></div>
      </section>

      <div v-if="data" class="thydra-stack">
        <div class="tset-resource-grid">
          <section v-for="chart in [{ title: 'Turns by portal', items: data.turns_chart }, { title: 'Ledger entries by key', items: data.ledger_chart }]" :key="chart.title" class="tv-panel tset-form-card">
            <header><span class="tv-eyebrow">Distribution</span><h2>{{ chart.title }}</h2></header>
            <div v-if="rows(chart.items).length" class="thydra-bars"><div v-for="item in rows(chart.items)" :key="item.label"><span>{{ item.label }}</span><i><b :style="{ width: barWidth(chart.items, item.value) }" /></i><strong>{{ item.value }}</strong></div></div>
            <p v-else class="tv-empty">No stored values yet.</p>
          </section>
        </div>

        <section class="tv-panel tset-form-card">
          <header><span class="tv-eyebrow">Portal storage</span><h2>Stored counters</h2></header>
          <div class="thydra-table-wrap"><table><thead><tr><th>Portal</th><th>Turns</th><th>Tools</th><th>Repairs</th><th>Validation failures</th><th>Tool failures</th><th>Ledger entries</th></tr></thead><tbody><tr v-for="row in rows(data.platform_rows)" :key="row.platform"><td>{{ row.platform_label || platformLabel(row.platform) }}</td><td>{{ row.total_turns || 0 }}</td><td>{{ row.total_tools_called || 0 }}</td><td>{{ row.total_repairs || 0 }}</td><td>{{ row.validation_failures || 0 }}</td><td>{{ row.tool_failures || 0 }}</td><td>{{ row.ledger_entries || 0 }}</td></tr></tbody></table></div>
        </section>

        <section class="tv-panel tset-form-card tset-danger-card thydra-clear-card">
          <header><span class="tv-eyebrow">Destructive maintenance</span><h2>Clear Hydra data</h2><p>Choose a portal and remove its counters, ledger, or both.</p></header>
          <label>Portal<select v-model="dataPlatform"><option v-for="platform in platformOptions" :key="platform" :value="platform">{{ platformLabel(platform) }}</option></select></label>
          <div class="thydra-clear-actions"><button class="tv-button danger" type="button" :disabled="clearing" @click="clearData('metrics', dataPlatform)">Reset metrics</button><button class="tv-button danger" type="button" :disabled="clearing" @click="clearData('ledger', dataPlatform)">Clear ledger</button><button class="tv-button danger" type="button" :disabled="clearing" @click="clearData('all', dataPlatform)">Clear portal data</button><button class="tv-button danger" type="button" :disabled="clearing" @click="clearData('all', 'all')">Clear everything</button></div>
        </section>
      </div>
    </template>
  </section>
</template>
