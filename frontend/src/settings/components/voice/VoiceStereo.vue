<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import type { JsonRow } from "../../types";
import ModelFields from "../models/ModelFields.vue";
import { fieldsToValues, itemForms, voiceAction } from "./runtime";

const props = defineProps<{ payload: JsonRow; actionEndpoint: string }>();
const emit = defineEmits<{ refresh: []; notify: [message: string, tone?: string] }>();

const drafts = reactive<Record<string, JsonRow>>({});
const dirty = reactive<Record<string, boolean>>({});
const busy = ref("");
const error = ref("");

const pairs = computed(() => itemForms(props.payload).filter((form) => ["stereo_pair", "stereo_pair_create"].includes(String(form.group || ""))));
const savedPairs = computed(() => pairs.value.filter((item) => String(item.group || "") === "stereo_pair"));
const readyPairs = computed(() => savedPairs.value.filter((item) => Boolean(item.connected)).length);

function key(item: JsonRow) { return String(item.id || item.title || "pair"); }
function sections(item: JsonRow): JsonRow[] { return [{ label: item.group === "stereo_pair_create" ? "New pair" : "Pair configuration", fields: Array.isArray(item.fields) ? item.fields : [] }]; }

function hydrate() {
  const active = new Set<string>();
  pairs.value.forEach((item) => {
    const token = key(item);
    active.add(token);
    if (!dirty[token]) drafts[token] = fieldsToValues(Array.isArray(item.fields) ? item.fields : []);
  });
  Object.keys(drafts).forEach((token) => { if (!active.has(token)) { delete drafts[token]; delete dirty[token]; } });
}

function update(item: JsonRow, field: string, value: unknown) {
  const token = key(item);
  if (!drafts[token]) drafts[token] = {};
  drafts[token][field] = value;
  dirty[token] = true;
  error.value = "";
}

async function save(item: JsonRow) {
  const token = key(item);
  if (busy.value) return;
  busy.value = token;
  error.value = "";
  try {
    const selector = item.group === "stereo_pair_create" ? "" : String(item.id || "");
    const result = await voiceAction(props.actionEndpoint, String(item.save_action || "voice_stereo_pair_save"), { id: selector, selector, values: { ...(drafts[token] || {}) } });
    dirty[token] = false;
    emit("notify", String(result.message || (selector ? "Stereo pair saved." : "Stereo pair created.")), "success");
    emit("refresh");
  } catch (saveError) {
    error.value = saveError instanceof Error ? saveError.message : "Stereo pair could not be saved.";
    emit("notify", error.value, "error");
  } finally { busy.value = ""; }
}

async function remove(item: JsonRow) {
  if (!item.remove_action || !window.confirm(String(item.remove_confirm || `Delete ${item.title || "this stereo pair"}?`))) return;
  const token = key(item);
  busy.value = token;
  try {
    const result = await voiceAction(props.actionEndpoint, String(item.remove_action), { id: item.id, selector: item.id });
    emit("notify", String(result.message || "Stereo pair deleted."), "success");
    emit("refresh");
  } catch (removeError) {
    error.value = removeError instanceof Error ? removeError.message : "Stereo pair could not be deleted.";
    emit("notify", error.value, "error");
  } finally { busy.value = ""; }
}

watch(() => props.payload, hydrate, { deep: true, immediate: true });
</script>

<template>
  <section class="tm-stack tvoice-stereo">
    <div v-if="error" class="tv-notice error">{{ error }}</div>
    <section class="tm-form-card tvoice-subhero tvoice-stereo-hero"><div><span class="tv-eyebrow">Room-filling playback</span><h3>Build synchronized stereo rooms</h3><p>Pair two compatible satellites as left and right speakers while keeping each device available for normal voice turns.</p></div><div class="tvoice-subhero-metrics"><span><b>{{ savedPairs.length }}</b>Saved pairs</span><span><b>{{ readyPairs }}</b>Ready now</span></div></section>
    <article v-for="item in pairs" :key="key(item)" class="tm-form-card tvoice-pair-card" :class="{ 'is-new': item.group === 'stereo_pair_create' }">
      <header class="tvoice-pair-head">
        <div class="tvoice-card-identity"><span class="tvoice-card-mark">{{ item.group === "stereo_pair_create" ? "+" : "2X" }}</span><div><span class="tv-eyebrow">{{ item.group === "stereo_pair_create" ? "Create a room" : "Stereo destination" }}</span><h3>{{ item.title || "Stereo Pair" }}</h3><p>{{ item.subtitle }}<template v-if="item.detail"> · {{ item.detail }}</template></p></div></div>
        <span class="tv-live-pill" :class="{ warning: item.group !== 'stereo_pair_create' && !item.connected }"><i />{{ item.group === "stereo_pair_create" ? "New pair" : item.connected ? "Ready" : "Unavailable" }}</span>
      </header>
      <dl v-if="Array.isArray(item.summary_rows) && item.summary_rows.length" class="tm-detail-list tvoice-pair-summary"><template v-for="row in item.summary_rows" :key="String(row.label)"><dt>{{ row.label }}</dt><dd>{{ row.value || "—" }}</dd></template></dl>
      <ModelFields :sections="sections(item)" :values="drafts[key(item)] || {}" :disabled="Boolean(busy)" @change="(field, value) => update(item, field, value)" />
      <div class="tm-inline-actions tvoice-item-actions">
        <button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="save(item)">{{ busy === key(item) ? "Saving…" : item.save_label || "Save Pair" }}</button>
        <button v-if="item.remove_action" class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="remove(item)">{{ item.remove_label || "Delete Pair" }}</button>
        <span v-if="dirty[key(item)]" class="tm-unsaved-label">Unsaved changes</span>
      </div>
    </article>
    <div v-if="!pairs.length" class="tv-notice">No stereo-pair configuration is available.</div>
  </section>
</template>
