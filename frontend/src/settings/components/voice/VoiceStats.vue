<script setup lang="ts">
import { computed, ref } from "vue";
import type { JsonRow } from "../../types";
import { voiceAction } from "./runtime";

const props = defineProps<{ payload: JsonRow; actionEndpoint: string }>();
const emit = defineEmits<{ refresh: []; notify: [message: string, tone?: string] }>();

const busy = ref(false);
const error = ref("");
const sections = computed<JsonRow[]>(() => Array.isArray(props.payload.stats_sections) ? props.payload.stats_sections : []);
const tables = computed<JsonRow[]>(() => Array.isArray(props.payload.stats_tables) ? props.payload.stats_tables : []);
const controls = computed<JsonRow>(() => props.payload.stats_controls && typeof props.payload.stats_controls === "object" ? props.payload.stats_controls as JsonRow : {});
const metricCount = computed(() => sections.value.reduce((total, section) => total + (Array.isArray(section.metrics) ? section.metrics.length : 0), 0));
const resultCount = computed(() => tables.value.reduce((total, table) => total + (Array.isArray(table.rows) ? table.rows.length : 0), 0));

async function reset() {
  if (busy.value || !controls.value.reset_action) return;
  if (!window.confirm(String(controls.value.reset_confirm || "Reset all stored voice statistics?"))) return;
  busy.value = true;
  error.value = "";
  try {
    const result = await voiceAction(props.actionEndpoint, String(controls.value.reset_action || "voice_statistics_reset"), { id: controls.value.id });
    emit("notify", String(result.message || "Voice statistics reset."), "success");
    emit("refresh");
  } catch (resetError) {
    error.value = resetError instanceof Error ? resetError.message : "Voice statistics could not be reset.";
    emit("notify", error.value, "error");
  } finally { busy.value = false; }
}
</script>

<template>
  <section class="tm-stack tvoice-stats">
    <div v-if="error" class="tv-notice error">{{ error }}</div>
    <section class="tm-form-card tvoice-subhero tvoice-stats-hero"><div><span class="tv-eyebrow">Live voice health</span><h3>See how every conversation performs</h3><p>Quality, latency, backend fallbacks, and device outcomes update automatically while this tab is open.</p></div><div class="tvoice-subhero-metrics"><span><b>{{ metricCount }}</b>Live signals</span><span><b>{{ resultCount }}</b>Device rows</span></div></section>
    <div class="tm-card-grid tvoice-stat-sections">
      <article v-for="(section, sectionIndex) in sections" :key="String(section.title)" class="tm-form-card tvoice-stat-card">
        <header><div class="tvoice-card-identity"><span class="tvoice-card-mark">{{ sectionIndex + 1 }}</span><div><span class="tv-eyebrow">Live measurements</span><h3>{{ section.title }}</h3></div></div><span>{{ (section.metrics || []).length }} signals</span></header>
        <div class="tm-metrics tvoice-metric-grid"><article v-for="metric in section.metrics || []" :key="String(metric.label)"><span>{{ metric.label }}</span><strong>{{ metric.value }}</strong></article></div>
      </article>
    </div>

    <article v-for="table in tables" :key="String(table.title)" class="tm-form-card tvoice-stats-table">
      <header><div><span class="tv-eyebrow">Per-satellite detail</span><h3>{{ table.title }}</h3></div><span>{{ (table.rows || []).length }} rows</span></header>
      <div class="tm-table-wrap"><table><thead><tr><th v-for="column in table.columns || []" :key="String(column.key)">{{ column.label || column.key }}</th></tr></thead><tbody>
        <tr v-for="(row, index) in table.rows || []" :key="index"><td v-for="column in table.columns || []" :key="String(column.key)">{{ row[String(column.key)] ?? "—" }}</td></tr>
        <tr v-if="!(table.rows || []).length"><td :colspan="Math.max(1, (table.columns || []).length)">{{ table.empty_message || "No measurements yet." }}</td></tr>
      </tbody></table></div>
    </article>

    <div v-if="!sections.length && !tables.length" class="tv-notice">No voice statistics are available yet.</div>

    <footer v-if="controls.reset_action" class="tset-save-bar tvoice-save-bar">
      <div><strong>Statistics update automatically</strong><span>{{ controls.description }}</span></div>
      <button class="tv-button danger" type="button" :disabled="busy" @click="reset">{{ busy ? "Resetting…" : controls.reset_label || "Reset Voice Statistics" }}</button>
    </footer>
  </section>
</template>
