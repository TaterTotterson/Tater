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
const mwwForm = ref<JsonRow | null>(null);
const wakeForm = ref<JsonRow | null>(null);
const trainerSettingsForm = ref<JsonRow | null>(null);
const verifierForm = ref<JsonRow | null>(null);
const trainer = ref<JsonRow | null>(null);
const mwwValues = reactive<JsonRow>({});
const wakeValues = reactive<JsonRow>({});
const trainerSettingsValues = reactive<JsonRow>({});
const verifierValues = reactive<JsonRow>({});
const pairing = ref<JsonRow | null>(null);
const localDirty = ref(false);
let refreshTimer: number | null = null;
let pairingTimer: number | null = null;

const headerStats = computed<JsonRow[]>(() => Array.isArray(payload.value.header_stats) ? payload.value.header_stats : []);
const mwwFields = computed(() => fieldMap(mwwForm.value));
const wakeFields = computed(() => fieldMap(wakeForm.value));
const trainerSettingsFields = computed(() => fieldMap(trainerSettingsForm.value));
const verifierFields = computed(() => fieldMap(verifierForm.value));
const wakeEngine = computed(() => String(wakeValues.wake_engine || "micro_wake_word"));
const mwwWakeEngine = computed(() => String(mwwValues.wake_engine || "micro_wake_word"));
const mwwWakeSource = computed(() => String(mwwValues.wake_word || "hey_tater"));
const wakeSource = computed(() => String(wakeValues.wake_word || "hey_tater"));
const detectorMode = computed(() => String(wakeValues.wake_detector_mode || "dual"));
const mwwEnabled = computed(() => detectorMode.value === "mww" || detectorMode.value === "dual");
const owwEnabled = computed(() => detectorMode.value === "oww" || detectorMode.value === "dual");
const owwSource = computed(() => String(wakeValues.oww_wake_word || "hey_tater"));
const verifierModeField = computed<JsonRow>(() => Object.values(verifierFields.value).find((field) => String(field.type || "") === "select") || {});
const verifierModeKey = computed(() => String(verifierModeField.value.key || "wake_verifier_mode"));
const verifierMode = computed(() => String(verifierValues[verifierModeKey.value] || "off"));
const verifierResults = computed<JsonRow>(() => Object.values(verifierFields.value).find((field) => String(field.type || "") === "table") || {});
const verifierSummary = computed(() => String(Object.values(verifierFields.value).find((field) => String(field.key || "").includes("summary"))?.value || "No checks recorded yet"));
const verifierStt = computed(() => String(Object.values(verifierFields.value).find((field) => String(field.key || "").includes("stt_engine"))?.value || stat("STT Backend") || "Unavailable"));
const connected = computed(() => stat("Connected") || "0");
const activeWakeLabel = computed(() => {
  if (wakeEngine.value !== "micro_wake_word") return optionLabel(optionsFor(wakeFields.value.wake_engine).find((option) => optionValue(option) === wakeEngine.value)) || "Wake disabled";
  if (detectorMode.value === "dual") return "Dual Wake Word";
  return detectorMode.value === "oww" ? "openWakeWord" : "microWakeWord";
});
const activeMwwWakeLabel = computed(() => mwwWakeEngine.value === "micro_wake_word" ? "microWakeWord" : optionLabel(optionsFor(mwwFields.value.wake_engine).find((option) => optionValue(option) === mwwWakeEngine.value)) || "Wake disabled");
const activeMwwSourceLabel = computed(() => optionLabel(optionsFor(mwwFields.value.wake_word).find((option) => optionValue(option) === mwwWakeSource.value)) || "Built-in Hey Tater");
const activeWakeSourceLabel = computed(() => optionLabel(optionsFor(wakeFields.value.wake_word).find((option) => optionValue(option) === wakeSource.value)) || "Built-in Hey Tater");
const activeOwwSourceLabel = computed(() => owwSourceOptionLabel(optionsFor(wakeFields.value.oww_wake_word).find((option) => optionValue(option) === owwSource.value)) || "Built-in Hey Tater");
const pairingCode = computed(() => String(pairing.value?.display_code || pairing.value?.pairing_code || pairing.value?.code || "").trim());
const pairingState = computed(() => String(pairing.value?.state || pairing.value?.status || "waiting").trim().toLowerCase());
const pairingRemaining = computed(() => Math.max(0, Math.floor(Number(pairing.value?.expires_in_s || 0))));

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

const detectorModeDetails: Record<string, { mark: string; short: string }> = {
  mww: { mark: "MW", short: "Use microWakeWord by itself for the original fast Echo wake path." },
  oww: { mark: "OW", short: "Use openWakeWord by itself as the Echo's primary detector." },
  dual: { mark: "2X", short: "Run both models together and require agreement before opening the mic." },
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

function owwSourceOptionLabel(option: unknown): string {
  const value = optionValue(option);
  if (value === "catalog") return mwwEnabled.value ? "Dual Wake Word Catalog" : "openWakeWord Catalog";
  if (value === "custom_url") return mwwEnabled.value ? "Custom Matched Bundle" : "Custom openWakeWord Package";
  return optionLabel(option);
}

function owwSourceOptionDescription(option: unknown): string {
  const value = optionValue(option);
  if (value === "hey_tater") return mwwEnabled.value ? "Use the matching built-in Hey Tater MWW + OWW pair." : "Use the built-in Hey Tater OWW model.";
  if (value === "catalog") return mwwEnabled.value ? "Choose a verified matching pair from the official catalog." : "Choose an OWW model from the official catalog.";
  return mwwEnabled.value ? "Load both matching models from one trainer-produced bundle." : "Load only the OWW model from a trainer-produced package.";
}

function formatPairingTime(value: number): string {
  const total = Math.max(0, Math.floor(Number(value) || 0));
  return `${Math.floor(total / 60)}:${String(total % 60).padStart(2, "0")}`;
}

function stat(label: string): string {
  return String(headerStats.value.find((row) => String(row.label || "").toLowerCase() === label.toLowerCase())?.value ?? "");
}

function chooseWakeEngine(value: string) { update(wakeValues, "wake_engine", value); }
function chooseMwwWakeEngine(value: string) { update(mwwValues, "wake_engine", value); }
function chooseMwwWakeSource(value: string) { update(mwwValues, "wake_word", value); }
function chooseWakeSource(value: string) { update(wakeValues, "wake_word", value); }
function chooseOwwSource(value: string) { update(wakeValues, "oww_wake_word", value); }
function chooseDetectorMode(value: string) { update(wakeValues, "wake_detector_mode", value); }
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
  mwwForm.value = forms.find((form) => String(form.group || "") === "mww_satellite_model_settings") || null;
  wakeForm.value = forms.find((form) => String(form.group || "") === "echo_satellite_model_settings") || null;
  trainerSettingsForm.value = forms.find((form) => String(form.group || "") === "global_wake_trainer_settings") || null;
  verifierForm.value = forms.find((form) => String(form.group || "") === "wake_verifier") || null;
  trainer.value = forms.find((form) => String(form.group || "") === "wake_trainer_link") || null;
  if (!preserveValues) {
    hydrate(mwwForm.value, mwwValues);
    hydrate(wakeForm.value, wakeValues);
    hydrate(trainerSettingsForm.value, trainerSettingsValues);
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
  if (!mwwForm.value || !wakeForm.value || !trainerSettingsForm.value || !verifierForm.value) return { ok: false, error: "Wake Word settings are still loading." };
  busy.value = "apply";
  error.value = "";
  try {
    const mwwAction = String(mwwForm.value.save_action || "voice_global_satellite_settings_save");
    const wakeAction = String(wakeForm.value.save_action || "voice_global_satellite_settings_save");
    const trainerSettingsAction = String(trainerSettingsForm.value.save_action || "voice_global_satellite_settings_save");
    const verifierAction = String(verifierForm.value.save_action || "voice_wake_verifier_save");
    const first = await action(mwwAction, { id: mwwForm.value.id, profile: mwwForm.value.profile || "mww", defer_push: true, values: { ...mwwValues } });
    const second = await action(wakeAction, { id: wakeForm.value.id, profile: wakeForm.value.profile || "echo", defer_push: true, values: { ...wakeValues } });
    const third = await action(trainerSettingsAction, { id: trainerSettingsForm.value.id, values: { ...trainerSettingsValues } });
    const fourth = await action(verifierAction, { id: verifierForm.value.id, values: { ...verifierValues } });
    localDirty.value = false;
    await refresh(true);
    const message = String(fourth.message || third.message || second.message || first.message || "Wake Word settings applied to all connected satellites.");
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
  error.value = "";
  stopPairingPoll();
  try {
    const result = await action(String(trainer.value.start_action || "voice_wake_trainer_link_pairing_start"));
    const pairingId = String(result.pairing_id || "").trim();
    const displayCode = String(result.display_code || result.pairing_code || result.code || "").trim();
    if (!pairingId || !displayCode) throw new Error("Tater did not create a pairing code.");
    pairing.value = { ...result, display_code: displayCode };
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
    const previousPairing = pairing.value || {};
    pairing.value = {
      ...previousPairing,
      ...result,
      display_code: String(result.display_code || previousPairing.display_code || previousPairing.pairing_code || previousPairing.code || "").trim(),
    };
    if (result.wake_trainer_link && typeof result.wake_trainer_link === "object") trainer.value = result.wake_trainer_link as JsonRow;
    const state = String(result.state || result.status || "").trim().toLowerCase();
    if (Boolean(result.linked) || Boolean((result.wake_trainer_link as JsonRow | undefined)?.linked) || state === "linked") {
      stopPairingPoll();
      emit("notify", "Wake Word Trainer linked.", "success");
      await refresh(true);
    } else if (state === "expired" || Boolean(result.expired)) {
      stopPairingPoll();
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
      <div class="tm-model-area-hero-copy"><span class="tv-eyebrow">Wake pipeline</span><h3>Say it once. Wake every room.</h3><p>Set the microWakeWord profile used by ESP-style satellites separately from the MWW, OWW, or Dual Wake mode used by Echo firmware.</p></div>
      <div class="tm-model-area-status"><span><i class="ready" />{{ connected }} connected</span><span>ESP: {{ activeMwwWakeLabel }}</span><span>Echo: {{ activeWakeLabel }}</span><span>{{ verifierDetails[verifierMode]?.label || "Disabled" }} verification</span></div>
    </article>

    <article v-if="mwwForm" class="tm-form-card tm-wake-engine-card">
      <header><div><span class="tv-eyebrow">Step 1 · ESP satellites</span><h3>How should ESP firmware wake?</h3><p>This profile is sent only to ESP and other satellites that do not advertise openWakeWord support.</p></div><span class="tm-speech-status-chip">{{ activeMwwWakeLabel }}</span></header>
      <div class="tm-choice-grid tm-wake-choice-grid" role="group" aria-label="microWakeWord satellite wake engine">
        <button v-for="option in optionsFor(mwwFields.wake_engine)" :key="optionValue(option)" type="button" :class="{ active: mwwWakeEngine === optionValue(option) }" :disabled="Boolean(busy)" @click="chooseMwwWakeEngine(optionValue(option))"><i>{{ wakeEngineDetails[optionValue(option)]?.mark || "WAKE" }}</i><span><strong>{{ optionLabel(option) }}</strong><small>{{ wakeEngineDetails[optionValue(option)]?.short }}</small></span><b>✓</b></button>
      </div>
      <section v-if="mwwWakeEngine === 'micro_wake_word'" class="tm-wake-source-panel">
        <div class="tm-speech-section-heading compact"><div><h3>microWakeWord model</h3><p>Choose Tater's built-in model, a catalog release, or your own package.</p></div><span>{{ activeMwwSourceLabel }}</span></div>
        <div class="tm-choice-grid tm-wake-source-grid" role="group" aria-label="microWakeWord satellite model source">
          <button v-for="option in optionsFor(mwwFields.wake_word)" :key="optionValue(option)" type="button" :class="{ active: mwwWakeSource === optionValue(option) }" :disabled="Boolean(busy)" @click="chooseMwwWakeSource(optionValue(option))"><i>{{ wakeSourceDetails[optionValue(option)]?.mark || "SRC" }}</i><span><strong>{{ optionLabel(option) }}</strong><small>{{ wakeSourceDetails[optionValue(option)]?.short }}</small></span><b>✓</b></button>
        </div>
        <label v-if="mwwWakeSource === 'catalog'" class="tm-field tm-field-wide"><span class="tm-field-label">Wake Word Catalog</span><select v-model="mwwValues.wake_word_catalog_url" :disabled="Boolean(busy)" @change="update(mwwValues, 'wake_word_catalog_url', mwwValues.wake_word_catalog_url)"><option v-for="option in optionsFor(mwwFields.wake_word_catalog_url)" :key="optionValue(option)" :value="optionValue(option)">{{ optionLabel(option) }}</option></select><small>{{ mwwFields.wake_word_catalog_url?.description }}</small></label>
        <label v-if="mwwWakeSource === 'custom_url'" class="tm-field tm-field-wide"><span class="tm-field-label">microWakeWord JSON URL</span><input v-model="mwwValues.wake_word_url" type="url" :placeholder="String(mwwFields.wake_word_url?.placeholder || 'https://example.local/wake_word.json')" :disabled="Boolean(busy)" @input="update(mwwValues, 'wake_word_url', mwwValues.wake_word_url)" /><small>{{ mwwFields.wake_word_url?.description }}</small></label>
      </section>
      <div v-else class="tm-wake-engine-note"><strong>{{ activeMwwWakeLabel }} selected</strong><span>{{ wakeEngineDetails[mwwWakeEngine]?.short }} Wake-model selection is not needed for this mode.</span></div>
    </article>

    <article v-if="wakeForm" class="tm-form-card tm-wake-engine-card">
      <header><div><span class="tv-eyebrow">Step 1 · Echo satellites</span><h3>How should Echo firmware wake?</h3><p>Only OWW-capable Echo satellites receive this profile.</p></div><span class="tm-speech-status-chip">{{ activeWakeLabel }}</span></header>
      <div class="tm-choice-grid tm-wake-choice-grid" role="group" aria-label="Wake engine">
        <button v-for="option in optionsFor(wakeFields.wake_engine)" :key="optionValue(option)" type="button" :class="{ active: wakeEngine === optionValue(option) }" :disabled="Boolean(busy)" @click="chooseWakeEngine(optionValue(option))"><i>{{ wakeEngineDetails[optionValue(option)]?.mark || "WAKE" }}</i><span><strong>{{ optionLabel(option) }}</strong><small>{{ wakeEngineDetails[optionValue(option)]?.short }}</small></span><b>✓</b></button>
      </div>

      <section v-if="wakeEngine === 'micro_wake_word'" class="tm-wake-source-panel">
        <div class="tm-speech-section-heading compact"><div><h3>Wake detection mode</h3><p>Choose one detector or Dual Wake Word mode. There is no ambiguous combination of checkboxes.</p></div><span>{{ activeWakeLabel }}</span></div>
        <div class="tm-choice-grid tm-wake-source-grid" role="group" aria-label="Echo wake detection mode">
          <button v-for="option in optionsFor(wakeFields.wake_detector_mode)" :key="optionValue(option)" type="button" :class="{ active: detectorMode === optionValue(option) }" :disabled="Boolean(busy)" @click="chooseDetectorMode(optionValue(option))"><i>{{ detectorModeDetails[optionValue(option)]?.mark || 'WAKE' }}</i><span><strong>{{ optionLabel(option) }}</strong><small>{{ detectorModeDetails[optionValue(option)]?.short }}</small></span><b>✓</b></button>
        </div>

        <div v-if="mwwEnabled && !owwEnabled" class="tm-wake-model-block">
          <div class="tm-speech-section-heading compact"><div><h3>microWakeWord model</h3><p>Choose Tater's model, a catalog release, or your own package.</p></div><span>{{ activeWakeSourceLabel }}</span></div>
          <div class="tm-choice-grid tm-wake-source-grid" role="group" aria-label="microWakeWord model source">
            <button v-for="option in optionsFor(wakeFields.wake_word)" :key="optionValue(option)" type="button" :class="{ active: wakeSource === optionValue(option) }" :disabled="Boolean(busy)" @click="chooseWakeSource(optionValue(option))"><i>{{ wakeSourceDetails[optionValue(option)]?.mark || "SRC" }}</i><span><strong>{{ optionLabel(option) }}</strong><small>{{ wakeSourceDetails[optionValue(option)]?.short }}</small></span><b>✓</b></button>
          </div>
          <label v-if="wakeSource === 'catalog'" class="tm-field tm-field-wide"><span class="tm-field-label">Wake Word Catalog</span><select v-model="wakeValues.wake_word_catalog_url" :disabled="Boolean(busy)" @change="update(wakeValues, 'wake_word_catalog_url', wakeValues.wake_word_catalog_url)"><option v-for="option in optionsFor(wakeFields.wake_word_catalog_url)" :key="optionValue(option)" :value="optionValue(option)">{{ optionLabel(option) }}</option></select><small>{{ wakeFields.wake_word_catalog_url?.description }}</small></label>
          <label v-if="wakeSource === 'custom_url'" class="tm-field tm-field-wide"><span class="tm-field-label">microWakeWord JSON URL</span><input v-model="wakeValues.wake_word_url" type="url" :placeholder="String(wakeFields.wake_word_url?.placeholder || 'https://example.local/wake_word.json')" :disabled="Boolean(busy)" @input="update(wakeValues, 'wake_word_url', wakeValues.wake_word_url)" /><small>{{ wakeFields.wake_word_url?.description }}</small></label>
        </div>

        <div v-if="owwEnabled" class="tm-wake-model-block">
          <div class="tm-speech-section-heading compact"><div><h3>{{ mwwEnabled ? "Paired wake bundle" : "openWakeWord model" }}</h3><p>{{ mwwEnabled ? "One versioned bundle supplies both matching models, so MWW and OWW can never listen for different phrases." : "Select the built-in model, an official catalog release, or a package published by either Tater trainer." }}</p></div><span>{{ activeOwwSourceLabel }}</span></div>
          <div class="tm-choice-grid tm-wake-source-grid tm-wake-source-grid-two" role="group" :aria-label="mwwEnabled ? 'paired wake bundle source' : 'openWakeWord model source'">
            <button v-for="option in optionsFor(wakeFields.oww_wake_word)" :key="optionValue(option)" type="button" :class="{ active: owwSource === optionValue(option) }" :disabled="Boolean(busy) || (optionValue(option) === 'catalog' && !optionsFor(wakeFields.oww_wake_word_catalog_url).length)" @click="chooseOwwSource(optionValue(option))"><i>{{ optionValue(option) === 'hey_tater' ? 'T' : (optionValue(option) === 'catalog' ? 'CAT' : 'URL') }}</i><span><strong>{{ owwSourceOptionLabel(option) }}</strong><small>{{ owwSourceOptionDescription(option) }}</small></span><b>✓</b></button>
          </div>
          <label v-if="owwSource === 'catalog'" class="tm-field tm-field-wide"><span class="tm-field-label">{{ mwwEnabled ? "Dual Wake Word Catalog" : "openWakeWord Catalog" }}</span><select v-model="wakeValues.oww_wake_word_catalog_url" :disabled="Boolean(busy) || !optionsFor(wakeFields.oww_wake_word_catalog_url).length" @change="update(wakeValues, 'oww_wake_word_catalog_url', wakeValues.oww_wake_word_catalog_url)"><option v-if="!optionsFor(wakeFields.oww_wake_word_catalog_url).length" value="" disabled>Awaiting the first verified V7 wake word</option><option v-for="option in optionsFor(wakeFields.oww_wake_word_catalog_url)" :key="optionValue(option)" :value="optionValue(option)">{{ optionLabel(option) }}</option></select><small>{{ wakeFields.oww_wake_word_catalog_url?.description }}</small></label>
          <label v-if="owwSource === 'custom_url'" class="tm-field tm-field-wide"><span class="tm-field-label">{{ wakeFields.oww_wake_word_url?.label }}</span><input v-model="wakeValues.oww_wake_word_url" type="url" :placeholder="String(wakeFields.oww_wake_word_url?.placeholder || 'https://example.local/hey_tater.wake-bundle.json')" :disabled="Boolean(busy)" @input="update(wakeValues, 'oww_wake_word_url', wakeValues.oww_wake_word_url)" /><small>{{ wakeFields.oww_wake_word_url?.description }}</small></label>
          <div v-if="owwSource === 'hey_tater'" class="tm-wake-model-note">{{ mwwEnabled ? "The built-in selection always keeps both Hey Tater detectors paired." : "The built-in selection uses the Hey Tater openWakeWord model included with current Echo firmware." }}</div>
        </div>
      </section>

      <div v-else class="tm-wake-engine-note"><strong>{{ activeWakeLabel }} selected</strong><span>{{ wakeEngineDetails[wakeEngine]?.short }} Wake-model selection is not needed for this mode.</span></div>
    </article>

    <article v-if="trainerSettingsForm" class="tm-form-card tm-wake-training-card">
      <header><div><span class="tv-eyebrow">Step 2 · Improve</span><h3>Wake Word Trainer</h3><p>Optionally send useful wake clips to your securely linked trainer.</p></div><span v-if="trainer" class="tv-live-pill" :class="{ warning: !trainer.linked }"><i />{{ trainer.status_label }}</span></header>
      <div class="tm-wake-feedback-grid">
        <label class="tm-wake-toggle-card"><span><strong>Good wakes</strong><small>Send confirmed wakes to improve recognition.</small></span><input v-model="trainerSettingsValues.capture_wake_audio" type="checkbox" :disabled="Boolean(busy)" @change="update(trainerSettingsValues, 'capture_wake_audio', trainerSettingsValues.capture_wake_audio)" /></label>
        <label class="tm-wake-toggle-card"><span><strong>Close misses</strong><small>Send near-wakes that can improve model tuning.</small></span><input v-model="trainerSettingsValues.capture_close_misses" type="checkbox" :disabled="Boolean(busy)" @change="update(trainerSettingsValues, 'capture_close_misses', trainerSettingsValues.capture_close_misses)" /></label>
      </div>
      <label class="tm-field tm-field-wide"><span class="tm-field-label">Trainer App URL</span><input v-model="trainerSettingsValues.trainer_app_url" type="url" :placeholder="String(trainerSettingsFields.trainer_app_url?.placeholder || 'http://trainer.local:8789')" :disabled="Boolean(busy)" @input="update(trainerSettingsValues, 'trainer_app_url', trainerSettingsValues.trainer_app_url)" /><small>The destination used by satellites when wake-clip sharing is enabled.</small></label>

      <div v-if="trainer?.linked" class="tm-wake-trainer-linked"><div><i>✓</i><span><strong>{{ trainer.trainer_name }}</strong><small>Last model: {{ trainer.last_wake_word || "No model published yet" }} · {{ trainer.last_publish_at || "Waiting for first publish" }}</small></span></div><button class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="unlinkTrainer">Unlink</button></div>
      <div v-else class="tm-wake-trainer-link"><div><i>↗</i><span><strong>Link the trainer securely</strong><small>A short pairing code ensures only your trainer can publish wake models.</small></span></div><div class="tm-inline-actions"><button class="tv-button" type="button" :disabled="Boolean(busy)" @click="startPairing">{{ pairing ? "Restart pairing" : "Link trainer" }}</button><button v-if="pairing && pairingState === 'waiting'" class="tv-button" type="button" :disabled="Boolean(busy)" @click="checkPairing">Check link</button></div></div>
      <div v-if="pairing && !trainer?.linked" class="tm-pairing-box" :class="{ expired: pairingState === 'expired' }"><span>Pairing code</span><strong>{{ pairingState === "expired" ? "Expired" : pairingCode || "Creating…" }}</strong><small v-if="pairingState === 'waiting'">Expires in {{ formatPairingTime(pairingRemaining) }}</small><small v-else-if="pairingState === 'expired'">Restart pairing to create a new code.</small><a v-if="pairing.pairing_url || pairing.url" :href="String(pairing.pairing_url || pairing.url)" target="_blank" rel="noreferrer">Open trainer pairing</a></div>
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
