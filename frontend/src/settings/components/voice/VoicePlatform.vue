<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import type { JsonRow } from "../../types";
import ModelFields from "../models/ModelFields.vue";
import { formSections, itemForms, sectionsToValues, voiceAction } from "./runtime";

const props = defineProps<{ payload: JsonRow; actionEndpoint: string }>();
const emit = defineEmits<{ refresh: []; notify: [message: string, tone?: string] }>();

const sharedValues = reactive<JsonRow>({});
const pipelineValues = reactive<JsonRow>({});
const dirty = ref(false);
const busy = ref("");
const error = ref("");
const notice = ref("");

const sharedForm = computed(() => itemForms(props.payload, "global_satellite_settings")[0] || null);
const pipelineForm = computed(() => itemForms(props.payload, "settings")[0] || null);
const sharedSections = computed(() => formSections(sharedForm.value));
const pipelineSections = computed(() => formSections(pipelineForm.value));
const settingCount = computed(() => [...sharedSections.value, ...pipelineSections.value].reduce((total, section) => total + (Array.isArray(section.fields) ? section.fields.length : 0), 0));

function replace(target: JsonRow, values: JsonRow) {
  Object.keys(target).forEach((key) => delete target[key]);
  Object.assign(target, values);
}

function hydrate(force = false) {
  if (dirty.value && !force) return;
  replace(sharedValues, sectionsToValues(formSections(sharedForm.value)));
  replace(pipelineValues, sectionsToValues(formSections(pipelineForm.value)));
}

function update(target: JsonRow, key: string, value: unknown) {
  target[key] = value;
  dirty.value = true;
  error.value = "";
  notice.value = "";
}

async function save() {
  if (busy.value) return;
  busy.value = "save";
  error.value = "";
  notice.value = "";
  try {
    const messages: string[] = [];
    if (sharedForm.value) {
      const result = await voiceAction(props.actionEndpoint, String(sharedForm.value.save_action || "voice_global_satellite_settings_save"), { id: sharedForm.value.id, values: { ...sharedValues } });
      if (result.message) messages.push(String(result.message));
    }
    if (pipelineForm.value) {
      const result = await voiceAction(props.actionEndpoint, String(pipelineForm.value.save_action || "voice_settings_save"), { id: pipelineForm.value.id, values: { ...pipelineValues } });
      if (result.message) messages.push(String(result.message));
    }
    dirty.value = false;
    notice.value = messages.join(" ") || "Voice settings saved and applied.";
    emit("notify", notice.value, "success");
    emit("refresh");
  } catch (saveError) {
    error.value = saveError instanceof Error ? saveError.message : "Voice settings could not be saved.";
    emit("notify", error.value, "error");
  } finally { busy.value = ""; }
}

async function resetDefaults() {
  if (!pipelineForm.value || !window.confirm(String(pipelineForm.value.reset_confirm || "Restore native voice settings to defaults?"))) return;
  busy.value = "reset";
  error.value = "";
  try {
    const result = await voiceAction(props.actionEndpoint, String(pipelineForm.value.reset_action || "voice_settings_reset_defaults"));
    dirty.value = false;
    emit("notify", String(result.message || "Voice settings restored to defaults."), "success");
    emit("refresh");
  } catch (resetError) {
    error.value = resetError instanceof Error ? resetError.message : "Voice defaults could not be restored.";
    emit("notify", error.value, "error");
  } finally { busy.value = ""; }
}

watch(() => props.payload, () => hydrate(false), { deep: true, immediate: true });
</script>

<template>
  <section class="tm-stack tvoice-platform">
    <div v-if="error" class="tv-notice error">{{ error }}</div>
    <div v-if="notice" class="tv-notice">{{ notice }}</div>

    <section class="tm-form-card tvoice-subhero tvoice-platform-hero"><div><span class="tv-eyebrow">Shared satellite behavior</span><h3>Tune the whole voice network</h3><p>Every control is grouped by purpose. Save once at the bottom to apply shared behavior and runtime tuning together.</p></div><div class="tvoice-subhero-metrics"><span><b>{{ settingCount }}</b>Controls</span><span><b>{{ sharedSections.length + pipelineSections.length }}</b>Groups</span></div></section>

    <section v-if="sharedForm" class="tm-stack tvoice-platform-group">
      <div class="tvoice-section-heading"><div class="tvoice-card-identity"><span class="tvoice-card-mark">ALL</span><div><span class="tv-eyebrow">Every satellite</span><h3>{{ sharedForm.title || "Shared satellite behavior" }}</h3><p>{{ sharedForm.subtitle }}</p></div></div><span class="tvoice-group-count">{{ sharedSections.length }} groups</span></div>
      <ModelFields :sections="sharedSections" :values="sharedValues" :disabled="Boolean(busy)" @change="(key, value) => update(sharedValues, key, value)" />
    </section>

    <section v-if="pipelineForm" class="tm-stack tvoice-platform-group">
      <div class="tvoice-section-heading"><div class="tvoice-card-identity"><span class="tvoice-card-mark">SYS</span><div><span class="tv-eyebrow">Tater runtime</span><h3>{{ pipelineForm.title || "Voice pipeline" }}</h3><p>{{ pipelineForm.subtitle }}</p></div></div><span class="tvoice-group-count">{{ pipelineSections.length }} groups</span></div>
      <ModelFields :sections="pipelineSections" :values="pipelineValues" :disabled="Boolean(busy)" @change="(key, value) => update(pipelineValues, key, value)" />
    </section>

    <div v-if="!sharedForm && !pipelineForm" class="tv-notice">Satellite runtime settings are unavailable.</div>

    <footer v-if="sharedForm || pipelineForm" class="tset-save-bar tvoice-save-bar">
      <div><strong>{{ dirty ? "Unsaved satellite changes" : "Satellite settings are synchronized" }}</strong><span>Saves runtime tuning and immediately applies shared behavior to connected satellites.</span></div>
      <div class="tm-inline-actions">
        <button v-if="pipelineForm?.reset_action" class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="resetDefaults">Restore defaults</button>
        <button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="save">{{ busy === "save" ? "Applying…" : "Save & Apply Satellite Settings" }}</button>
      </div>
    </footer>
  </section>
</template>
