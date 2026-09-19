<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { getJson, postJson } from "../../../shared/api";
import type { JsonRow } from "../../types";

const props = defineProps<{
  kind: "speakerid" | "emotionid";
  runtimeEndpoint: string;
  actionEndpoint: string;
}>();

const emit = defineEmits<{
  notify: [message: string, tone?: string];
  dirty: [];
  subtab: [tab: "people" | "settings"];
}>();

const data = ref<JsonRow>({});
const values = reactive<JsonRow>({});
const speakerDrafts = reactive<Record<string, JsonRow>>({});
const createDraft = reactive<JsonRow>({ speaker_name: "" });
const loading = ref(false);
const busy = ref("");
const error = ref("");
const localDirty = ref(false);
const editingSpeaker = ref(false);
const speakerTab = ref<"people" | "settings">("people");
let timer: number | null = null;

const isSpeaker = computed(() => props.kind === "speakerid");
const sections = computed<JsonRow[]>(() => Array.isArray(data.value.settings_sections) ? data.value.settings_sections : []);
const metrics = computed<JsonRow[]>(() => Array.isArray(data.value.summary_metrics) ? data.value.summary_metrics : []);
const speakers = computed<JsonRow[]>(() => Array.isArray(data.value.speakers) ? data.value.speakers : []);
const pending = computed<JsonRow | null>(() => {
  const candidate = data.value.pending;
  if (!candidate || typeof candidate !== "object") return null;
  return String((candidate as JsonRow).speaker_id || "").trim() ? candidate as JsonRow : null;
});
const availability = computed<JsonRow>(() => data.value.availability && typeof data.value.availability === "object" ? data.value.availability as JsonRow : {});
const lastResult = computed<JsonRow>(() => data.value.last_result && typeof data.value.last_result === "object" ? data.value.last_result as JsonRow : {});
const runtimeFields = computed<Record<string, JsonRow>>(() => Object.fromEntries(sections.value.flatMap((section) => Array.isArray(section.fields) ? section.fields as JsonRow[] : []).map((field) => [String(field.key || ""), field]).filter(([key]) => key)));
const enabledKey = computed(() => isSpeaker.value ? "VOICE_SPEAKER_ID_ENABLED" : "VOICE_EMOTION_ID_ENABLED");
const enabled = computed(() => Boolean(values[enabledKey.value]));
const bestMatch = computed(() => Boolean(values.VOICE_SPEAKER_ID_BEST_MATCH));
const promptHints = computed(() => Boolean(values.VOICE_EMOTION_ID_PROMPT_HINT_ENABLED));
const scorePercent = computed(() => Math.max(0, Math.min(100, Math.round(Number(lastResult.value.score || 0) * 100))));
const heroMetrics = computed(() => {
  const wanted = isSpeaker.value ? ["Enrolled Speakers", "Last Speaker", "Last Score", "Model"] : ["Prompt Hint", "Last Tone", "Last Score", "Model"];
  return wanted.map((label) => metrics.value.find((metric) => String(metric.label || "") === label)).filter(Boolean) as JsonRow[];
});

function field(key: string): JsonRow { return runtimeFields.value[key] || {}; }
function fieldDisabled(key: string): boolean { return Boolean(busy.value || field(key).disabled || field(key).read_only || field(key).readonly); }
function setSetting(key: string, value: unknown) { updateSetting(key, value); }
function targetValue(event: Event): string { return (event.target as HTMLInputElement).value; }
function targetChecked(event: Event): boolean { return (event.target as HTMLInputElement).checked; }

function selectSpeakerTab(tab: "people" | "settings") {
  speakerTab.value = tab;
  emit("subtab", tab);
}

function hydrateSettings(force = false) {
  if (localDirty.value && !force) return;
  Object.keys(values).forEach((key) => delete values[key]);
  sections.value.forEach((section) => {
    (Array.isArray(section.fields) ? section.fields : []).forEach((field: JsonRow) => {
      const key = String(field.key || "").trim();
      const type = String(field.type || "").trim().toLowerCase();
      if (key && !["table", "readonly", "section", "led_preview"].includes(type) && !field.disabled && !field.read_only && !field.readonly) values[key] = field.value ?? field.default ?? (type === "checkbox" ? false : "");
    });
  });
}

function hydrateSpeakers(force = false) {
  if (editingSpeaker.value && !force) return;
  const active = new Set<string>();
  speakers.value.forEach((speaker) => {
    const id = String(speaker.speaker_id || "");
    if (!id) return;
    active.add(id);
    if (!speakerDrafts[id]) speakerDrafts[id] = {};
    (Array.isArray(speaker.fields) ? speaker.fields : []).forEach((field: JsonRow) => {
      speakerDrafts[id][String(field.key || "")] = field.value ?? "";
    });
  });
  Object.keys(speakerDrafts).forEach((id) => { if (!active.has(id)) delete speakerDrafts[id]; });
}

function applyRuntime(result: JsonRow, force = false) {
  const body = result.payload && typeof result.payload === "object" ? result.payload as JsonRow : result;
  const next = isSpeaker.value ? body.speaker_id : body.emotion_id;
  data.value = next && typeof next === "object" ? next as JsonRow : body;
  hydrateSettings(force);
  if (isSpeaker.value) hydrateSpeakers(force);
}

async function refresh(silent = false) {
  if (!silent) loading.value = true;
  error.value = "";
  try {
    applyRuntime(await getJson<JsonRow>(`${props.runtimeEndpoint}?panel=${props.kind}`), !silent);
  } catch (refreshError) {
    error.value = refreshError instanceof Error ? refreshError.message : "Identity runtime could not be loaded.";
  } finally {
    loading.value = false;
  }
}

function updateSetting(key: string, value: unknown) {
  values[key] = value;
  localDirty.value = true;
  emit("dirty");
}

async function runAction(action: string, payload: JsonRow = {}, success = "Settings updated."): Promise<JsonRow> {
  if (busy.value) throw new Error("Another identity action is already running.");
  busy.value = action;
  error.value = "";
  try {
    const result = await postJson<JsonRow>(props.actionEndpoint, { action, payload });
    const next = isSpeaker.value ? result.speaker_id : result.emotion_id;
    if (next && typeof next === "object") {
      data.value = next as JsonRow;
      const settingsSaved = action.endsWith("_settings_save");
      const speakerSaved = ["speaker_id_speaker_save", "speaker_id_speaker_create", "speaker_id_speaker_delete"].includes(action);
      if (settingsSaved) localDirty.value = false;
      if (speakerSaved) editingSpeaker.value = false;
      hydrateSettings(settingsSaved);
      if (isSpeaker.value) hydrateSpeakers(speakerSaved);
    }
    const message = String(result.message || success);
    emit("notify", message, "success");
    return result;
  } catch (actionError) {
    error.value = actionError instanceof Error ? actionError.message : "Identity action failed.";
    emit("notify", error.value, "error");
    throw actionError;
  } finally {
    busy.value = "";
  }
}

async function apply(): Promise<JsonRow> {
  try {
    const action = isSpeaker.value ? "speaker_id_settings_save" : "emotion_id_settings_save";
    return await runAction(action, { values: { ...values } }, `${isSpeaker.value ? "Speaker" : "Emotion"} ID settings saved.`);
  } catch {
    return { ok: false, error: error.value };
  }
}

async function createSpeaker() {
  if (!String(createDraft.speaker_name || "").trim()) {
    error.value = "Enter a speaker name first.";
    return;
  }
  try {
    await runAction("speaker_id_speaker_create", { values: { speaker_name: createDraft.speaker_name } }, "Speaker created.");
    createDraft.speaker_name = "";
  } catch { /* surfaced above */ }
}

async function saveSpeaker(speaker: JsonRow) {
  const id = String(speaker.speaker_id || "");
  try {
    await runAction("speaker_id_speaker_save", { speaker_id: id, values: { ...(speakerDrafts[id] || {}) } }, "Speaker saved.");
  } catch { /* surfaced above */ }
}

function markSpeakerEditing() {
  editingSpeaker.value = true;
}

async function captureSpeaker(speaker: JsonRow) {
  const id = String(speaker.speaker_id || "");
  try {
    await runAction("speaker_id_enrollment_arm", { speaker_id: id, values: { preferred_selector: speakerDrafts[id]?.preferred_selector || "" } }, "The next voice turn will be captured.");
  } catch { /* surfaced above */ }
}

async function deleteSpeaker(speaker: JsonRow) {
  const name = String(speaker.name || "this speaker");
  if (!window.confirm(`Delete ${name} and all saved voice samples?`)) return;
  try {
    await runAction("speaker_id_speaker_delete", { speaker_id: String(speaker.speaker_id || "") }, "Speaker deleted.");
  } catch { /* surfaced above */ }
}

async function cancelCapture() {
  try {
    await runAction("speaker_id_pending_cancel", {}, "Pending speaker capture canceled.");
  } catch { /* surfaced above */ }
}

onMounted(() => {
  if (isSpeaker.value) emit("subtab", speakerTab.value);
  void refresh();
  timer = window.setInterval(() => { if (!busy.value && document.visibilityState === "visible") void refresh(true); }, 12000);
});

onBeforeUnmount(() => { if (timer !== null) window.clearInterval(timer); });

defineExpose({ apply, refresh });
</script>

<template>
  <section class="tm-stack tm-identity-workspace" :class="isSpeaker ? 'tm-identity-speaker' : 'tm-identity-emotion'">
    <div v-if="loading" class="tv-notice">Loading live {{ isSpeaker ? "Speaker" : "Emotion" }} ID settings…</div>
    <div v-if="error" class="tv-notice error">{{ error }}</div>

    <article class="tm-form-card tm-model-area-hero tm-identity-hero">
      <div class="tm-identity-hero-mark">{{ isSpeaker ? "ID" : "♪" }}</div>
      <div class="tm-model-area-hero-copy"><span class="tv-eyebrow">{{ isSpeaker ? "Voice identity" : "Voice tone" }}</span><h3>{{ isSpeaker ? "Know who is speaking" : "Understand how it was said" }}</h3><p>{{ isSpeaker ? "Match each voice turn against enrolled local profiles before Hydra responds." : "Classify vocal emotion and optionally add a subtle tone hint to Hydra's prompt." }}</p></div>
      <div class="tm-model-area-status"><span><i :class="{ ready: enabled }" />{{ enabled ? "Enabled" : "Disabled" }}</span><span>{{ availability.available === false ? "Needs attention" : "Runtime ready" }}</span></div>
    </article>

    <div v-if="heroMetrics.length" class="tm-metrics tm-identity-metrics"><article v-for="metric in heroMetrics" :key="String(metric.label)"><span>{{ metric.label }}</span><strong>{{ metric.value }}</strong></article></div>

    <article v-if="availability.detail || availability.error" class="tm-status-card" :class="{ error: availability.error || availability.available === false }">
      <strong>{{ availability.available === false ? "Runtime needs attention" : "Runtime ready" }}</strong>
      <span>{{ availability.detail || availability.error }}</span>
    </article>

    <nav v-if="isSpeaker" class="tv-tabs tm-inner-tabs tm-identity-tabs" aria-label="Speaker ID sections">
      <button type="button" :class="{ active: speakerTab === 'people' }" @click="selectSpeakerTab('people')">People</button>
      <button type="button" :class="{ active: speakerTab === 'settings' }" @click="selectSpeakerTab('settings')">Settings</button>
    </nav>

    <article v-if="!isSpeaker || speakerTab === 'settings'" class="tm-form-card tm-identity-runtime-card">
      <header><div><span class="tv-eyebrow">Runtime</span><h3>{{ isSpeaker ? "Speaker matching" : "Emotion classification" }}</h3><p>{{ isSpeaker ? "Choose how strict matching should be. Enrollment controls remain with each person below." : "Choose when tone context is useful and how confident the model must be." }}</p></div><span class="tm-speech-status-chip">{{ enabled ? "Active" : "Off" }}</span></header>

      <label class="tm-identity-master-toggle"><span><i>{{ isSpeaker ? "ID" : "♪" }}</i><span><strong>Enable {{ isSpeaker ? "Speaker" : "Emotion" }} ID</strong><small>{{ field(enabledKey).description }}</small></span></span><input type="checkbox" :checked="enabled" :disabled="fieldDisabled(enabledKey)" @change="setSetting(enabledKey, targetChecked($event))" /></label>

      <template v-if="enabled && isSpeaker">
        <section class="tm-identity-section">
          <div class="tm-speech-section-heading compact"><div><h3>Matching strategy</h3><p>Threshold mode is safer; Best Match is more aggressive with known households.</p></div><span>{{ bestMatch ? "Best match" : "Threshold" }}</span></div>
          <label class="tm-wake-toggle-card tm-identity-option-card"><span><strong>Best Match Mode</strong><small>{{ field('VOICE_SPEAKER_ID_BEST_MATCH').description }}</small></span><input type="checkbox" :checked="bestMatch" :disabled="fieldDisabled('VOICE_SPEAKER_ID_BEST_MATCH')" @change="setSetting('VOICE_SPEAKER_ID_BEST_MATCH', targetChecked($event))" /></label>
          <div v-if="!bestMatch" class="tm-field-grid">
            <label class="tm-field"><span class="tm-field-label">Match threshold</span><input type="number" :value="values.VOICE_SPEAKER_ID_MATCH_THRESHOLD" min="0" max="1" step="0.01" :disabled="fieldDisabled('VOICE_SPEAKER_ID_MATCH_THRESHOLD')" @input="setSetting('VOICE_SPEAKER_ID_MATCH_THRESHOLD', targetValue($event))" /><small>{{ field('VOICE_SPEAKER_ID_MATCH_THRESHOLD').description }}</small></label>
            <label class="tm-field"><span class="tm-field-label">Runner-up margin</span><input type="number" :value="values.VOICE_SPEAKER_ID_MATCH_MARGIN" min="0" max="1" step="0.01" :disabled="fieldDisabled('VOICE_SPEAKER_ID_MATCH_MARGIN')" @input="setSetting('VOICE_SPEAKER_ID_MATCH_MARGIN', targetValue($event))" /><small>{{ field('VOICE_SPEAKER_ID_MATCH_MARGIN').description }}</small></label>
          </div>
          <div v-else class="tm-identity-warning"><i>!</i><span><strong>Best Match always picks someone</strong><small>An unknown voice can be assigned to the closest enrolled speaker.</small></span></div>
        </section>
        <section class="tm-identity-section"><div class="tm-speech-section-heading compact"><div><h3>Voice length</h3><p>Skip clips that are too short for a reliable embedding.</p></div><span>Seconds</span></div><div class="tm-field-grid"><label class="tm-field"><span class="tm-field-label">Minimum for matching</span><input type="number" :value="values.VOICE_SPEAKER_ID_MIN_SPEECH_S" min="0.4" max="15" step="0.05" :disabled="fieldDisabled('VOICE_SPEAKER_ID_MIN_SPEECH_S')" @input="setSetting('VOICE_SPEAKER_ID_MIN_SPEECH_S', targetValue($event))" /><small>{{ field('VOICE_SPEAKER_ID_MIN_SPEECH_S').description }}</small></label><label class="tm-field"><span class="tm-field-label">Minimum for enrollment</span><input type="number" :value="values.VOICE_SPEAKER_ID_ENROLL_MIN_SPEECH_S" min="1" max="20" step="0.05" :disabled="fieldDisabled('VOICE_SPEAKER_ID_ENROLL_MIN_SPEECH_S')" @input="setSetting('VOICE_SPEAKER_ID_ENROLL_MIN_SPEECH_S', targetValue($event))" /><small>{{ field('VOICE_SPEAKER_ID_ENROLL_MIN_SPEECH_S').description }}</small></label></div></section>
      </template>

      <template v-else-if="enabled">
        <section class="tm-identity-section">
          <div class="tm-speech-section-heading compact"><div><h3>Prompt behavior</h3><p>Classification can run quietly or provide tone context to Hydra.</p></div><span>{{ promptHints ? "Prompt hints on" : "Classify only" }}</span></div>
          <div class="tm-wake-feedback-grid"><label class="tm-wake-toggle-card"><span><strong>Add To Prompt</strong><small>{{ field('VOICE_EMOTION_ID_PROMPT_HINT_ENABLED').description }}</small></span><input type="checkbox" :checked="promptHints" :disabled="fieldDisabled('VOICE_EMOTION_ID_PROMPT_HINT_ENABLED')" @change="setSetting('VOICE_EMOTION_ID_PROMPT_HINT_ENABLED', targetChecked($event))" /></label><label v-if="promptHints" class="tm-wake-toggle-card"><span><strong>Use Neutral Context</strong><small>{{ field('VOICE_EMOTION_ID_INCLUDE_NEUTRAL').description }}</small></span><input type="checkbox" :checked="Boolean(values.VOICE_EMOTION_ID_INCLUDE_NEUTRAL)" :disabled="fieldDisabled('VOICE_EMOTION_ID_INCLUDE_NEUTRAL')" @change="setSetting('VOICE_EMOTION_ID_INCLUDE_NEUTRAL', targetChecked($event))" /></label></div>
          <div class="tm-field-grid"><label v-if="promptHints" class="tm-field"><span class="tm-field-label">Prompt confidence threshold</span><input type="number" :value="values.VOICE_EMOTION_ID_CONFIDENCE_THRESHOLD" min="0" max="1" step="0.01" :disabled="fieldDisabled('VOICE_EMOTION_ID_CONFIDENCE_THRESHOLD')" @input="setSetting('VOICE_EMOTION_ID_CONFIDENCE_THRESHOLD', targetValue($event))" /><small>{{ field('VOICE_EMOTION_ID_CONFIDENCE_THRESHOLD').description }}</small></label><label class="tm-field"><span class="tm-field-label">Minimum speech length</span><input type="number" :value="values.VOICE_EMOTION_ID_MIN_SPEECH_S" min="0.4" max="15" step="0.05" :disabled="fieldDisabled('VOICE_EMOTION_ID_MIN_SPEECH_S')" @input="setSetting('VOICE_EMOTION_ID_MIN_SPEECH_S', targetValue($event))" /><small>{{ field('VOICE_EMOTION_ID_MIN_SPEECH_S').description }}</small></label></div>
        </section>
      </template>

      <div v-else class="tm-media-route-summary"><i>OFF</i><span><strong>Runtime disabled</strong><small>Turn it on to reveal its matching and tuning controls.</small></span></div>
    </article>

    <template v-if="isSpeaker && speakerTab === 'people'">
      <article v-if="pending" class="tm-status-card live tm-identity-capture-live">
        <div><i class="tm-identity-pulse" /><span><strong>Listening for {{ pending.speaker_name || "speaker" }}</strong><small>Speak one clear sentence from {{ pending.selector_label || "a satellite" }}. The next complete voice turn becomes a sample.</small></span></div>
        <button class="tv-button" type="button" :disabled="Boolean(busy)" @click="cancelCapture">Cancel capture</button>
      </article>

      <article class="tm-form-card tm-identity-add-card">
        <header><div><span class="tv-eyebrow">New profile</span><h3>Add a speaker</h3><p>Create the person first, then capture one or more clear voice samples.</p></div><i class="tm-identity-add-mark">+</i></header>
        <label class="tm-field tm-field-wide"><span class="tm-field-label">Speaker name</span><input v-model="createDraft.speaker_name" type="text" placeholder="Name" /></label>
        <div class="tm-inline-actions"><button class="tv-button" type="button" :disabled="Boolean(busy)" @click="createSpeaker">Add speaker</button></div>
      </article>

      <section class="tm-card-grid tm-identity-speaker-grid">
        <article v-for="speaker in speakers" :key="String(speaker.speaker_id)" class="tm-form-card tm-speaker-card">
          <header><div class="tm-speaker-heading"><i>{{ String(speaker.name || "?").charAt(0).toUpperCase() }}</i><div><h3>{{ speaker.name || "Speaker" }}</h3><p>{{ speaker.sample_count || 0 }} voice sample{{ Number(speaker.sample_count || 0) === 1 ? "" : "s" }} · Updated {{ speaker.updated_at || "—" }}</p></div></div><span class="tm-speech-status-chip" :class="{ ready: Number(speaker.sample_count || 0) > 0 }">{{ Number(speaker.sample_count || 0) > 0 ? "Ready" : "Needs sample" }}</span></header>
          <div class="tm-field-grid">
            <label class="tm-field"><span class="tm-field-label">Speaker name</span><input v-model="speakerDrafts[String(speaker.speaker_id)].speaker_name" type="text" @input="markSpeakerEditing" /></label>
            <label class="tm-field"><span class="tm-field-label">Capture from satellite</span><select v-model="speakerDrafts[String(speaker.speaker_id)].preferred_selector" @change="markSpeakerEditing"><option v-for="option in data.selector_options || []" :key="String(option.value)" :value="option.value">{{ option.label }}</option></select><small>Used for the next voice sample. Leave on Any satellite to capture from whichever device hears the speaker.</small></label>
          </div>
          <div class="tm-inline-actions">
            <button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="captureSpeaker(speaker)">Capture voice sample</button>
            <button class="tv-button" type="button" :disabled="Boolean(busy)" @click="saveSpeaker(speaker)">Save profile</button>
            <button class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="deleteSpeaker(speaker)">Delete</button>
          </div>
        </article>
        <div v-if="!speakers.length" class="tm-identity-empty"><i>ID</i><strong>No speakers enrolled yet</strong><span>Add a profile above, then capture a clear sentence.</span></div>
      </section>
    </template>

    <article v-if="!isSpeaker" class="tm-form-card tm-emotion-result-card">
      <header><div><span class="tv-eyebrow">Live result</span><h3>Latest voice tone</h3><p>Updates automatically after a long enough voice turn is classified.</p></div><span class="tm-speech-status-chip">{{ lastResult.updated_at || "Waiting" }}</span></header>
      <div class="tm-emotion-result"><div class="tm-emotion-score" :style="{ '--tm-score': `${scorePercent * 3.6}deg` }"><span><strong>{{ scorePercent }}%</strong><small>confidence</small></span></div><div class="tm-emotion-copy"><span>Detected tone</span><strong>{{ lastResult.emotion || "No result yet" }}</strong><p>{{ lastResult.prompt_hint || "No prompt hint was added for the latest turn." }}</p></div></div>
    </article>
  </section>
</template>
