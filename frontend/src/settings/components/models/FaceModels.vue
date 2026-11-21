<script setup lang="ts">
import { computed } from "vue";
import type { JsonRow } from "../../types";

const props = withDefaults(defineProps<{
  settings: JsonRow;
  status: JsonRow;
  busy?: boolean;
}>(), {
  busy: false,
});

const emit = defineEmits<{ dirty: [] }>();

const enabled = computed(() => Boolean(props.settings.enabled));
const modelSwitch = computed<JsonRow>(() => props.status.model_switch && typeof props.status.model_switch === "object" ? props.status.model_switch as JsonRow : {});
const switchState = computed(() => String(modelSwitch.value.state || ""));
const rebuilding = computed(() => ["queued", "embedding", "rebuilding"].includes(switchState.value));
const progress = computed(() => Math.max(0, Math.min(100, Number(modelSwitch.value.progress || props.status.progress || 0))));
const stateLabel = computed(() => {
  const state = String(props.status.state || (enabled.value ? "idle" : "disabled"));
  if (rebuilding.value) return "Rebuilding face embeddings";
  if (state === "loading") return "Loading model";
  if (state === "error") return "Runtime error";
  return state === "disabled" ? "Disabled" : "Ready";
});
const accelerator = computed(() => String(props.status.accelerator || props.status.accelerator_target || "CPU"));
const knownFaces = computed(() => props.status.identity_count ?? props.status.face_count ?? "—");

function toggleEnabled(event: Event) {
  props.settings.enabled = (event.target as HTMLInputElement).checked;
  emit("dirty");
}
</script>

<template>
  <section class="tm-stack tm-face-workspace">
    <article class="tm-form-card tm-model-area-hero tm-face-hero">
      <div class="tm-face-scan-mark"><i /><i /><i /><i /><span>☺</span></div>
      <div class="tm-model-area-hero-copy"><span class="tv-eyebrow">Visual identity</span><h3>Recognize familiar faces locally</h3><p>Face images and embeddings stay in Tater data. Awareness can attach a known identity without sending the camera image to a recognition service.</p></div>
      <div class="tm-model-area-status"><span><i :class="{ ready: enabled && stateLabel !== 'Runtime error' }" />{{ stateLabel }}</span><span>{{ accelerator }}</span><span>{{ knownFaces }} known</span></div>
    </article>

    <article class="tm-form-card tm-face-runtime-card">
      <header><div><span class="tv-eyebrow">Runtime</span><h3>Face recognition</h3><p>Recognize saved people with YuNet + SFace. Tater manages the model automatically.</p></div><span class="tm-speech-status-chip" :class="{ ready: enabled }">{{ enabled ? "Enabled" : "Off" }}</span></header>
      <label class="tm-identity-master-toggle tm-face-master-toggle"><span><i>☺</i><span><strong>Enable Face ID</strong><small>Load the model and recognize faces from Awareness camera bursts.</small></span></span><input type="checkbox" :checked="enabled" :disabled="busy" @change="toggleEnabled" /></label>

      <div v-if="enabled" class="tm-media-route-summary"><i>SF</i><span><strong>YuNet + SFace</strong><small>The same recognition model is used for every saved person.</small></span></div>
      <div v-else class="tm-media-route-summary"><i>OFF</i><span><strong>Recognition is disabled</strong><small>Existing people and face images remain saved and can be used again when enabled.</small></span></div>
    </article>

    <article v-if="rebuilding" class="tm-form-card tm-face-rebuild-card">
      <header><div><span class="tv-eyebrow">Background task</span><h3>Rebuilding face embeddings</h3><p>Tater is updating saved face references to SFace. Recognition resumes automatically when this finishes.</p></div><strong>{{ Math.round(progress) }}%</strong></header>
      <div class="tm-face-progress"><i :style="{ width: `${progress}%` }" /></div>
    </article>

    <article v-if="['partial', 'error'].includes(switchState)" class="tm-status-card error"><div><strong>Some saved faces need attention</strong><span>{{ modelSwitch.message || modelSwitch.error }}</span></div></article>

    <article v-if="status.message || status.error" class="tm-status-card" :class="{ error: status.error }"><div><strong>{{ status.error ? "Face ID needs attention" : stateLabel }}</strong><span>{{ status.error || status.message }}</span></div></article>
  </section>
</template>
