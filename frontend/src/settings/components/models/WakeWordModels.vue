<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { getJson, postJson } from "../../../shared/api";
import type { JsonRow } from "../../types";

const props = defineProps<{
  runtimeEndpoint: string;
  actionEndpoint: string;
}>();

const emit = defineEmits<{
  notify: [message: string, tone?: string];
  dirty: [];
}>();

const loading = ref(false);
const busy = ref("");
const error = ref("");
const payload = ref<JsonRow>({});
const wakeForm = ref<JsonRow | null>(null);
const verifierForm = ref<JsonRow | null>(null);
const trainer = ref<JsonRow | null>(null);
const wakeValues = reactive<JsonRow>({});
const verifierValues = reactive<JsonRow>({});
const pairing = ref<JsonRow | null>(null);
const localDirty = ref(false);
let refreshTimer: number | null = null;
let pairingTimer: number | null = null;

const headerStats = computed<JsonRow[]>(() => Array.isArray(payload.value.header_stats) ? payload.value.header_stats : []);
const wakeFields = computed(() => fieldMap(wakeForm.value));
const verifierFields = computed(() => fieldMap(verifierForm.value));
const wakeEngine = computed(() => String(wakeValues.wake_engine || "micro_wake_word"));
const wakeSource = computed(() => String(wakeValues.wake_word || "hey_tater"));
const verifierModeField = computed<JsonRow>(() => Object.values(verifierFields.value).find((field) => String(field.type || "") === "select") || {});
const verifierModeKey = computed(() => String(verifierModeField.value.key || "wake_verifier_mode"));
const verifierMode = computed(() => String(verifierValues[verifierModeKey.value] || "off"));
const verifierResults = computed<JsonRow>(() => Object.values(verifierFields.value).find((field) => String(field.type || "") === "table") || {});
const verifierSummary = computed(() => String(Object.values(verifierFields.value).find((field) => String(field.key || "").includes("summary"))?.value || "No checks recorded yet"));
const verifierStt = computed(() => String(Object.values(verifierFields.value).find((field) => String(field.key || "").includes("stt_engine"))?.value || stat("STT Backend") || "Unavailable"));
const connected = computed(() => stat("Connected") || "0");
const activeWakeLabel = computed(() => optionLabel(optionsFor(wakeFields.value.wake_engine).find((option) => optionValue(option) === wakeEngine.value)) || "microWakeWord");
const activeWakeSourceLabel = computed(() => optionLabel(optionsFor(wakeFields.value.wake_word).find((option) => optionValue(option) === wakeSource.value)) || "Built-in Hey Tater");

const wakeEngineDetails: Record<string, { mark: string; short: string }> = {
  micro_wake_word: { mark: "MW", short: "Private, fast wake detection directly on each satellite." },
  button: { mark: "BTN", short: "Start listening only from the satellite's physical control." },
  server: { mark: "NET", short: "Stream audio to Tater and detect the wake phrase centrally." },
  off: { mark: "OFF", short: "Disable automatic wake detection on every satellite." },
};

const wakeSourceDetails: Record<string, { mark: string; short: string }> = {
  hey_tater: { mark: "T", short: "Tater's bundled, ready-to-use wake model." },
  catalog: { mark: "CAT", short: "Choose a versioned model from the official catalog." },
  custom_url: { mark: "URL", short: "Load a compatible microWakeWord JSON package by URL." },
};

const verifierDetails: Record<string, { label: string; mark: string; short: string }> = {
  off: { label: "Disabled", mark: "OFF", short: "Trust the on-device wake model without a second check." },
  observe: { label: "Observe", mark: "OBS", short: "Record STT decisions without blocking any wake." },
  enforce: { label: "Enabled", mark: "ON", short: "Reject clear transcript mismatches; fail open on errors." },
};

function allFields(form: JsonRow | null): JsonRow[] {
  return (form && Array.isArray(form.sections) ? form.sections : []).flatMap((section: JsonRow) => Array.isArray(section.fields) ? section.fields as JsonRow[] : []);
}

function fieldMap(form: JsonRow | null): Record<string, JsonRow> {
  return Object.fromEntries(allFields(form).map((field) => [String(field.key || ""), field]).filter(([key]) => key));
}

function optionsFor(field: JsonRow | undefined): JsonRow[] {
  return field && Array.isArray(field.options) ? field.options as JsonRow[] : [];
}

function optionValue(option: unknown): string {
  return option && typeof option === "object" ? String((option as JsonRow).value ?? (option as JsonRow).id ?? "") : String(option ?? "");
}

function optionLabel(option: unknown): string {
  return option && typeof option === "object" ? String((option as JsonRow).label ?? (option as JsonRow).name ?? optionValue(option)) : String(option ?? "");
}

function stat(label: string): string {
  return String(headerStats.value.find((row) => String(row.label || "").toLowerCase() === label.toLowerCase())?.value ?? "");
}

function chooseWakeEngine(value: string) { update(wakeValues, "wake_engine", value); }
function chooseWakeSource(value: string) { update(wakeValues, "wake_word", value); }
function chooseVerifierMode(value: string) { update(verifierValues, verifierModeKey.value, value); }

function hydrate(form: JsonRow | null, target: JsonRow) {
  Object.keys(target).forEach((key) => delete target[key]);
  const sections = form && Array.isArray(form.sections) ? form.sections : [];
  sections.forEach((section: JsonRow) => {
    (Array.isArray(section.fields) ? section.fields : []).forEach((field: JsonRow) => {
      const key = String(field.key || "").trim();
      const type = String(field.type || "").trim().toLowerCase();
      if (key && !["table", "readonly", "section", "led_preview"].includes(type) && !field.disabled && !field.read_only && !field.readonly) target[key] = field.value ?? field.default ?? "";
    });
  });
}

function applyPayload(result: JsonRow, preserveValues = false) {
  const body = result.payload && typeof result.payload === "object" ? result.payload as JsonRow : result;
  payload.value = body;
  const ui = body.ui && typeof body.ui === "object" ? body.ui as JsonRow : {};
  const forms = Array.isArray(ui.item_forms) ? ui.item_forms as JsonRow[] : [];
  wakeForm.value = forms.find((form) => String(form.group || "") === "global_satellite_model_settings") || null;
  verifierForm.value = forms.find((form) => String(form.group || "") === "wake_verifier") || null;
  trainer.value = forms.find((form) => String(form.group || "") === "wake_trainer_link") || null;
  if (!preserveValues) {
    hydrate(wakeForm.value, wakeValues);
    hydrate(verifierForm.value, verifierValues);
  }
}

async function refresh(silent = false) {
  if (!silent) loading.value = true;
  error.value = "";
  try {
    applyPayload(await getJson<JsonRow>(`${props.runtimeEndpoint}?panel=satellites`), silent && localDirty.value);
  } catch (refreshError) {
    error.value = refreshError instanceof Error ? refreshError.message : "Wake Word settings could not be loaded.";
  } finally {
    loading.value = false;
  }
}

function update(target: JsonRow, key: string, value: unknown) {
  target[key] = value;
  localDirty.value = true;
  error.value = "";
  emit("dirty");
}

async function action(actionName: string, body: JsonRow = {}): Promise<JsonRow> {
  return postJson<JsonRow>(props.actionEndpoint, { action: actionName, payload: body });
}

async function apply(): Promise<JsonRow> {
  if (busy.value) return { ok: false, error: "Wake Word settings are busy." };
  if (!wakeForm.value || !verifierForm.value) return { ok: false, error: "Wake Word settings are still loading." };
  busy.value = "apply";
  error.value = "";
  try {
    const wakeAction = String(wakeForm.value.save_action || "voice_global_satellite_settings_save");
    const verifierAction = String(verifierForm.value.save_action || "voice_wake_verifier_save");
    const first = await action(wakeAction, { id: wakeForm.value.id, values: { ...wakeValues } });
    const second = await action(verifierAction, { id: verifierForm.value.id, values: { ...verifierValues } });
    localDirty.value = false;
    await refresh(true);
    const message = String(second.message || first.message || "Wake Word settings applied to all connected satellites.");
    emit("notify", message, "success");
    return { ok: true, message };
  } catch (actionError) {
    error.value = actionError instanceof Error ? actionError.message : "Wake Word settings could not be applied.";
    emit("notify", error.value, "error");
    return { ok: false, error: error.value };
  } finally {
    busy.value = "";
  }
}

async function resetStats() {
  if (!window.confirm(String(verifierForm.value?.reset_confirm || "Reset wake-verification statistics?"))) return;
  busy.value = "reset";
  try {
    const result = await action(String(verifierForm.value?.reset_action || "voice_wake_verifier_stats_reset"));
    emit("notify", String(result.message || "Wake-verification statistics reset."), "success");
    await refresh(true);
  } catch (actionError) {
    error.value = actionError instanceof Error ? actionError.message : "Statistics could not be reset.";
  } finally {
    busy.value = "";
  }
}

async function startPairing() {
  if (!trainer.value) return;
  busy.value = "pair";
  try {
    pairing.value = await action(String(trainer.value.start_action || "voice_wake_trainer_link_pairing_start"));
    schedulePairingPoll();
  } catch (actionError) {
    error.value = actionError instanceof Error ? actionError.message : "Trainer pairing could not start.";
  } finally {
    busy.value = "";
  }
}

async function checkPairing() {
  const pairingId = String(pairing.value?.pairing_id || "");
  if (!pairingId || !trainer.value) return;
  try {
    const result = await action(String(trainer.value.status_action || "voice_wake_trainer_link_pairing_status"), { values: { pairing_id: pairingId } });
    pairing.value = result;
    if (result.wake_trainer_link && typeof result.wake_trainer_link === "object") trainer.value = result.wake_trainer_link as JsonRow;
    if (Boolean((result.wake_trainer_link as JsonRow | undefined)?.linked) || String(result.status || "") === "linked") {
      stopPairingPoll();
      emit("notify", "Wake Word Trainer linked.", "success");
      await refresh(true);
    } else {
      schedulePairingPoll();
    }
  } catch (actionError) {
    stopPairingPoll();
    error.value = actionError instanceof Error ? actionError.message : "Trainer pairing status could not be checked.";
  }
}

function schedulePairingPoll() {
  stopPairingPoll();
  pairingTimer = window.setTimeout(checkPairing, 2000);
}

function stopPairingPoll() {
  if (pairingTimer !== null) window.clearTimeout(pairingTimer);
  pairingTimer = null;
}

async function unlinkTrainer() {
  if (!trainer.value || !window.confirm("Unlink this Wake Word Trainer?")) return;
  busy.value = "unlink";
  try {
    const result = await action(String(trainer.value.unlink_action || "voice_wake_trainer_link_unlink"));
    trainer.value = result.wake_trainer_link && typeof result.wake_trainer_link === "object" ? result.wake_trainer_link as JsonRow : trainer.value;
    pairing.value = null;
    emit("notify", "Wake Word Trainer unlinked.", "success");
  } catch (actionError) {
    error.value = actionError instanceof Error ? actionError.message : "Trainer could not be unlinked.";
  } finally {
    busy.value = "";
  }
}

onMounted(() => {
  void refresh();
  refreshTimer = window.setInterval(() => { if (!busy.value && document.visibilityState === "visible") void refresh(true); }, 12000);
});

onBeforeUnmount(() => {
  if (refreshTimer !== null) window.clearInterval(refreshTimer);
  stopPairingPoll();
});

defineExpose({ apply, refresh });
</script>

<template>
  <section class="tm-stack tm-wake-workspace">
    <div v-if="loading" class="tv-notice">Loading live Wake Word settings…</div>
    <div v-if="error" class="tv-notice error">{{ error }}</div>

    <article class="tm-form-card tm-model-area-hero tm-wake-hero">
      <div class="tm-model-area-hero-copy"><span class="tv-eyebrow">Wake pipeline</span><h3>Say it once. Wake every room.</h3><p>Choose how satellites detect a wake, where their model comes from, and whether Tater performs a fast STT verification before opening the microphone.</p></div>
      <div class="tm-model-area-status"><span><i class="ready" />{{ connected }} connected</span><span>{{ activeWakeLabel }}</span><span>{{ verifierDetails[verifierMode]?.label || "Disabled" }} verification</span></div>
    </article>

    <article v-if="wakeForm" class="tm-form-card tm-wake-engine-card">
      <header><div><span class="tv-eyebrow">Step 1 · Detection</span><h3>How should Tater wake?</h3><p>Only the settings used by the selected engine are shown.</p></div><span class="tm-speech-status-chip">{{ activeWakeLabel }}</span></header>
      <div class="tm-choice-grid tm-wake-choice-grid" role="group" aria-label="Wake engine">
        <button v-for="option in optionsFor(wakeFields.wake_engine)" :key="optionValue(option)" type="button" :class="{ active: wakeEngine === optionValue(option) }" :disabled="Boolean(busy)" @click="chooseWakeEngine(optionValue(option))"><i>{{ wakeEngineDetails[optionValue(option)]?.mark || "WAKE" }}</i><span><strong>{{ optionLabel(option) }}</strong><small>{{ wakeEngineDetails[optionValue(option)]?.short }}</small></span><b>✓</b></button>
      </div>

      <section v-if="wakeEngine === 'micro_wake_word'" class="tm-wake-source-panel">
        <div class="tm-speech-section-heading compact"><div><h3>Wake model source</h3><p>Choose Tater's model, a catalog release, or your own package.</p></div><span>{{ activeWakeSourceLabel }}</span></div>
        <div class="tm-choice-grid tm-wake-source-grid" role="group" aria-label="Wake model source">
          <button v-for="option in optionsFor(wakeFields.wake_word)" :key="optionValue(option)" type="button" :class="{ active: wakeSource === optionValue(option) }" :disabled="Boolean(busy)" @click="chooseWakeSource(optionValue(option))"><i>{{ wakeSourceDetails[optionValue(option)]?.mark || "SRC" }}</i><span><strong>{{ optionLabel(option) }}</strong><small>{{ wakeSourceDetails[optionValue(option)]?.short }}</small></span><b>✓</b></button>
        </div>
        <label v-if="wakeSource === 'catalog'" class="tm-field tm-field-wide"><span class="tm-field-label">Wake Word Catalog</span><select v-model="wakeValues.wake_word_catalog_url" :disabled="Boolean(busy)" @change="update(wakeValues, 'wake_word_catalog_url', wakeValues.wake_word_catalog_url)"><option v-for="option in optionsFor(wakeFields.wake_word_catalog_url)" :key="optionValue(option)" :value="optionValue(option)">{{ optionLabel(option) }}</option></select><small>{{ wakeFields.wake_word_catalog_url?.description }}</small></label>
        <label v-if="wakeSource === 'custom_url'" class="tm-field tm-field-wide"><span class="tm-field-label">Wake Word JSON URL</span><input v-model="wakeValues.wake_word_url" type="url" :placeholder="String(wakeFields.wake_word_url?.placeholder || 'https://example.local/wake_word.json')" :disabled="Boolean(busy)" @input="update(wakeValues, 'wake_word_url', wakeValues.wake_word_url)" /><small>{{ wakeFields.wake_word_url?.description }}</small></label>
      </section>

      <div v-else class="tm-wake-engine-note"><strong>{{ activeWakeLabel }} selected</strong><span>{{ wakeEngineDetails[wakeEngine]?.short }} Wake-model selection is not needed for this mode.</span></div>
    </article>

    <article v-if="wakeForm" class="tm-form-card tm-wake-training-card">
      <header><div><span class="tv-eyebrow">Step 2 · Improve</span><h3>Wake Word Trainer</h3><p>Optionally send useful wake clips to your securely linked trainer.</p></div><span v-if="trainer" class="tv-live-pill" :class="{ warning: !trainer.linked }"><i />{{ trainer.status_label }}</span></header>
      <div class="tm-wake-feedback-grid">
        <label class="tm-wake-toggle-card"><span><strong>Good wakes</strong><small>Send confirmed wakes to improve recognition.</small></span><input v-model="wakeValues.capture_wake_audio" type="checkbox" :disabled="Boolean(busy)" @change="update(wakeValues, 'capture_wake_audio', wakeValues.capture_wake_audio)" /></label>
        <label class="tm-wake-toggle-card"><span><strong>Close misses</strong><small>Send near-wakes that can improve model tuning.</small></span><input v-model="wakeValues.capture_close_misses" type="checkbox" :disabled="Boolean(busy)" @change="update(wakeValues, 'capture_close_misses', wakeValues.capture_close_misses)" /></label>
      </div>
      <label class="tm-field tm-field-wide"><span class="tm-field-label">Trainer App URL</span><input v-model="wakeValues.trainer_app_url" type="url" :placeholder="String(wakeFields.trainer_app_url?.placeholder || 'http://trainer.local:8789')" :disabled="Boolean(busy)" @input="update(wakeValues, 'trainer_app_url', wakeValues.trainer_app_url)" /><small>The destination used by satellites when wake-clip sharing is enabled.</small></label>

      <div v-if="trainer?.linked" class="tm-wake-trainer-linked"><div><i>✓</i><span><strong>{{ trainer.trainer_name }}</strong><small>Last model: {{ trainer.last_wake_word || "No model published yet" }} · {{ trainer.last_publish_at || "Waiting for first publish" }}</small></span></div><button class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="unlinkTrainer">Unlink</button></div>
      <div v-else class="tm-wake-trainer-link"><div><i>↗</i><span><strong>Link the trainer securely</strong><small>A short pairing code ensures only your trainer can publish wake models.</small></span></div><div class="tm-inline-actions"><button class="tv-button" type="button" :disabled="Boolean(busy)" @click="startPairing">{{ pairing ? "Restart pairing" : "Link trainer" }}</button><button v-if="pairing" class="tv-button" type="button" :disabled="Boolean(busy)" @click="checkPairing">Check link</button></div></div>
      <div v-if="pairing && !trainer?.linked" class="tm-pairing-box"><span>Pairing code</span><strong>{{ pairing.pairing_code || pairing.code || "Waiting…" }}</strong><a v-if="pairing.pairing_url || pairing.url" :href="String(pairing.pairing_url || pairing.url)" target="_blank" rel="noreferrer">Open trainer pairing</a></div>
    </article>

    <article v-if="verifierForm" class="tm-form-card tm-wake-verifier-card">
      <header><div><span class="tv-eyebrow">Step 3 · Verify</span><h3>STT wake verification</h3><p>Use the selected Speech STT engine for a fast second opinion after an on-device wake.</p></div><span class="tm-speech-status-chip">{{ verifierStt }}</span></header>
      <div class="tm-choice-grid tm-wake-verifier-grid" role="group" aria-label="Wake verification mode">
        <button v-for="option in optionsFor(verifierModeField)" :key="optionValue(option)" type="button" :class="{ active: verifierMode === optionValue(option) }" :disabled="Boolean(busy)" @click="chooseVerifierMode(optionValue(option))"><i>{{ verifierDetails[optionValue(option)]?.mark }}</i><span><strong>{{ verifierDetails[optionValue(option)]?.label || optionLabel(option) }}</strong><small>{{ verifierDetails[optionValue(option)]?.short }}</small></span><b>✓</b></button>
      </div>
      <div class="tm-wake-verifier-summary"><div><span>Current results</span><strong>{{ verifierSummary }}</strong></div><div><span>Configured STT</span><strong>{{ verifierStt }}</strong></div></div>

      <details v-if="Array.isArray(verifierResults.rows)" class="tm-wake-results" :open="Boolean(verifierResults.rows.length)"><summary><span><strong>Results by satellite</strong><small>{{ verifierResults.rows.length }} satellite{{ verifierResults.rows.length === 1 ? "" : "s" }} with verification data</small></span><b>⌄</b></summary><div class="tm-table-wrap"><table><thead><tr><th v-for="column in verifierResults.columns || []" :key="String(column.key)">{{ column.label || column.key }}</th></tr></thead><tbody><tr v-for="(row, rowIndex) in verifierResults.rows" :key="rowIndex"><td v-for="column in verifierResults.columns || []" :key="String(column.key)">{{ row[String(column.key)] ?? "—" }}</td></tr><tr v-if="!verifierResults.rows.length"><td :colspan="Math.max(1, (verifierResults.columns || []).length)">No verifier results yet. Observe mode is a good way to collect data safely.</td></tr></tbody></table></div></details>
      <div class="tm-inline-actions tm-secondary-row"><button class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="resetStats">Reset Verification Stats</button></div>
    </article>
  </section>
</template>
