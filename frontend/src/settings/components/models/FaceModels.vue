<script setup lang="ts">
import { computed } from "vue";
import type { JsonRow } from "../../types";

const props = withDefaults(defineProps<{
  settings: JsonRow;
  status: JsonRow;
  models?: JsonRow[];
  busy?: boolean;
}>(), {
  models: () => [],
  busy: false,
});

const emit = defineEmits<{ dirty: [] }>();

const enabled = computed(() => Boolean(props.settings.enabled));
const selectedModel = computed(() => String(props.settings.model_id || "facenet512"));
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

const modelDetails: Record<string, { mark: string; short: string }> = {
  facenet512: { mark: "FN", short: "Reliable default with broad compatibility and strong local recognition." },
  adaface_ir50_webface4m: { mark: "ADA", short: "Experimental newer embedding model for varied faces and lighting." },
};

function chooseModel(id: string) {
  props.settings.model_id = id;
  emit("dirty");
}

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
      <header><div><span class="tv-eyebrow">Runtime</span><h3>Face recognition</h3><p>Enable recognition, then choose the embedding model used for every saved person.</p></div><span class="tm-speech-status-chip" :class="{ ready: enabled }">{{ enabled ? "Enabled" : "Off" }}</span></header>
      <label class="tm-identity-master-toggle tm-face-master-toggle"><span><i>☺</i><span><strong>Enable Face ID</strong><small>Load the model and recognize faces from Awareness camera bursts.</small></span></span><input type="checkbox" :checked="enabled" :disabled="busy" @change="toggleEnabled" /></label>

      <section v-if="enabled" class="tm-face-model-section">
        <div class="tm-speech-section-heading compact"><div><h3>Recognition model</h3><p>Changing models rebuilds every saved face embedding in the background.</p></div><span>{{ status.model || selectedModel }}</span></div>
        <div class="tm-choice-grid tm-face-model-grid" role="group" aria-label="Face recognition model">
          <button v-for="model in models" :key="String(model.id)" type="button" :class="{ active: selectedModel === String(model.id) }" :disabled="busy" @click="chooseModel(String(model.id))"><i>{{ modelDetails[String(model.id)]?.mark || "FACE" }}</i><span><strong>{{ model.label || model.id }}</strong><small>{{ modelDetails[String(model.id)]?.short || "Local face embedding model." }}</small><em v-if="model.experimental">Experimental</em></span><b>✓</b></button>
        </div>
      </section>
      <div v-else class="tm-media-route-summary"><i>OFF</i><span><strong>Recognition is disabled</strong><small>Existing people and face images remain saved and can be used again when enabled.</small></span></div>
    </article>

    <article v-if="rebuilding" class="tm-form-card tm-face-rebuild-card">
      <header><div><span class="tv-eyebrow">Background task</span><h3>Rebuilding face embeddings</h3><p>Tater is converting saved face images for the selected model. Recognition resumes automatically when this finishes.</p></div><strong>{{ Math.round(progress) }}%</strong></header>
      <div class="tm-face-progress"><i :style="{ width: `${progress}%` }" /></div>
    </article>

    <article v-if="status.message || status.error" class="tm-status-card" :class="{ error: status.error }"><div><strong>{{ status.error ? "Face ID needs attention" : stateLabel }}</strong><span>{{ status.error || status.message }}</span></div></article>
  </section>
</template>
