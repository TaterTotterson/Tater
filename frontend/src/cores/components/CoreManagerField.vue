<script setup lang="ts">
import { computed, onBeforeUnmount, ref } from "vue";
import type { JsonRow } from "../types";

const props = defineProps<{
  field: JsonRow;
  modelValue: unknown;
  allValues: JsonRow;
}>();

const emit = defineEmits<{
  "update:modelValue": [value: unknown];
  commit: [value: unknown];
  error: [message: string];
}>();

const cameraOpen = ref(false);
const cameraVideo = ref<HTMLVideoElement | null>(null);
const mediaVideo = ref<HTMLVideoElement | null>(null);
const videoPosterVisible = ref(false);
let cameraStream: MediaStream | null = null;
let videoHasPlayed = false;
let resettingVideo = false;

const type = computed(() => text(props.field.type || "text").toLowerCase());
const presentation = computed(() => text(props.field.presentation || props.field.display).toLowerCase());
const label = computed(() => text(props.field.label || props.field.key || "Field"));
const stringValue = computed(() => String(props.modelValue ?? ""));
const selectedValues = computed(() => new Set(normalizeList(props.modelValue)));
const visible = computed(() => conditionsMatch(props.field.show_when_all, props.field.show_when, true));
const conditionallyDisabled = computed(() => conditionsMatch(props.field.disable_when_all, props.field.disable_when, false));
const disabled = computed(() => Boolean(props.field.disabled || conditionallyDisabled.value));
const readOnly = computed(() => Boolean(props.field.read_only || props.field.readonly || ["readonly", "read_only"].includes(type.value)));
const fullWidth = computed(() => Boolean(props.field.full_width) || ["heading", "section_heading", "table", "bar_chart", "bars", "image_checklist", "image", "video", "file"].includes(type.value));
const options = computed(() => {
  const dependent = props.field.dependent_options && typeof props.field.dependent_options === "object"
    ? props.field.dependent_options
    : null;
  if (!dependent) return Array.isArray(props.field.options) ? props.field.options : [];
  const sourceKey = text(dependent.source_key);
  const sourceValue = conditionValue(props.allValues[sourceKey]);
  const narrowed = dependent.options_by_source && typeof dependent.options_by_source === "object"
    ? dependent.options_by_source[sourceValue]
    : null;
  if (Array.isArray(narrowed) && narrowed.length) return narrowed;
  return Array.isArray(dependent.default_options)
    ? dependent.default_options
    : Array.isArray(props.field.options) ? props.field.options : [];
});
const choiceCards = computed(() => ["choice_cards", "multi_choice_cards"].includes(type.value)
  || (presentation.value === "cards" && ["select", "multiselect"].includes(type.value)));
const multipleChoices = computed(() => ["multi_choice_cards", "multiselect", "image_checklist"].includes(type.value));
const tableColumns = computed(() => (Array.isArray(props.field.columns) ? props.field.columns : []).map((column: unknown, index: number) => {
  if (column && typeof column === "object") {
    const row = column as JsonRow;
    const key = text(row.key ?? row.id ?? row.field ?? `col_${index}`) || `col_${index}`;
    return { key, label: text(row.label || key) || key };
  }
  const key = text(column) || `col_${index}`;
  return { key, label: key };
}));
const chartPoints = computed(() => {
  const rows = Array.isArray(props.field.points) ? props.field.points : [];
  const normalized = rows.map((point: unknown) => {
    if (point && typeof point === "object") {
      const row = point as JsonRow;
      return { label: text(row.label ?? row.key ?? row.name) || "item", value: finite(row.value ?? row.count) };
    }
    return { label: text(point) || "item", value: 0 };
  });
  const max = normalized.reduce((value: number, row: { value: number }) => Math.max(value, row.value), 0);
  return normalized.map((row: { label: string; value: number }) => ({ ...row, width: max > 0 ? Math.max(0, Math.min(100, row.value / max * 100)) : 0 }));
});
const imageRows = computed(() => (Array.isArray(props.field.options) ? props.field.options : []).map((entry: unknown, index: number) => {
  const row = entry && typeof entry === "object" ? entry as JsonRow : {};
  const value = text(row.value ?? row.id);
  return {
    value,
    src: text(row.src ?? row.url),
    alt: text(row.alt) || `${label.value} image ${index + 1}`,
    caption: text(row.caption),
    meta: text(row.meta ?? row.description),
    selectable: row.selectable === undefined ? Boolean(value) : Boolean(row.selectable && value),
  };
}).filter((row: JsonRow) => row.src));

function text(value: unknown): string { return String(value ?? "").trim(); }
function finite(value: unknown): number { const parsed = Number(value); return Number.isFinite(parsed) ? parsed : 0; }
function conditionValue(value: unknown): string { return typeof value === "boolean" ? value ? "true" : "false" : text(value); }
function normalizeList(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((entry) => String(entry ?? "")).filter(Boolean);
  const raw = text(value);
  if (!raw) return [];
  try { const parsed = JSON.parse(raw); if (Array.isArray(parsed)) return parsed.map((entry) => String(entry ?? "")).filter(Boolean); }
  catch { /* Use comma-separated values. */ }
  return raw.split(",").map((entry) => entry.trim()).filter(Boolean);
}
function conditionRows(many: unknown, one: unknown): JsonRow[] {
  if (Array.isArray(many)) return many.filter((row) => row && typeof row === "object") as JsonRow[];
  return one && typeof one === "object" ? [one as JsonRow] : [];
}
function conditionsMatch(many: unknown, one: unknown, emptyResult: boolean): boolean {
  const rows = conditionRows(many, one);
  if (!rows.length) return emptyResult;
  return rows.every((condition) => {
    const source = text(condition.source_key ?? condition.key);
    if (!source) return emptyResult;
    const allowed = [
      ...(Array.isArray(condition.any_of) ? condition.any_of : []),
      ...(Array.isArray(condition.values) ? condition.values : []),
      ...(condition.equals !== undefined ? [condition.equals] : []),
      ...(condition.eq !== undefined ? [condition.eq] : []),
      ...(condition.value !== undefined ? [condition.value] : []),
    ].map(conditionValue);
    return allowed.length ? allowed.includes(conditionValue(props.allValues[source])) : emptyResult;
  });
}
function optionValue(option: unknown): string {
  if (option && typeof option === "object") {
    const row = option as JsonRow;
    return String(row.value ?? row.id ?? row.key ?? row.label ?? "");
  }
  return String(option ?? "");
}
function optionLabel(option: unknown): string {
  if (option && typeof option === "object") {
    const row = option as JsonRow;
    return text(row.label ?? row.title ?? row.name ?? optionValue(option)) || optionValue(option);
  }
  return String(option ?? "");
}
function optionDetail(option: unknown, key: "description" | "meta" | "icon"): string {
  if (!option || typeof option !== "object") return "";
  const row = option as JsonRow;
  if (key === "description") return text(row.description ?? row.subtitle);
  if (key === "meta") return text(row.meta ?? row.detail);
  return text(row.icon);
}
function fieldValue(event: Event): unknown {
  const input = event.target as HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement;
  if (type.value === "checkbox") return (input as HTMLInputElement).checked;
  if (["number", "range"].includes(type.value)) return input.value === "" ? "" : Number(input.value);
  if (multipleChoices.value && input instanceof HTMLSelectElement) return [...input.selectedOptions].map((option) => option.value).filter(Boolean);
  return input.value;
}
function update(event: Event) { emit("update:modelValue", fieldValue(event)); }
function commit(event: Event) { const value = fieldValue(event); emit("update:modelValue", value); emit("commit", value); }
function toggleChoice(value: string) {
  if (disabled.value || readOnly.value) return;
  if (!multipleChoices.value) { emit("update:modelValue", value); return; }
  const next = new Set(selectedValues.value);
  if (next.has(value)) next.delete(value); else next.add(value);
  emit("update:modelValue", [...next]);
}
function tableCell(row: unknown, key: string, index: number): unknown {
  if (Array.isArray(row)) return row[index] ?? "";
  return row && typeof row === "object" ? (row as JsonRow)[key] ?? "" : "";
}
function dataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error(`Could not read ${file.name}.`));
    reader.readAsDataURL(file);
  });
}
async function setFile(file: File) {
  const maxBytes = Number(props.field.max_bytes || 0);
  if (maxBytes > 0 && file.size > maxBytes) {
    emit("error", `${file.name} is larger than ${Math.max(1, Math.floor(maxBytes / 1024 / 1024))} MB.`);
    return;
  }
  try {
    if (text(props.field.file_encoding || props.field.encoding).toLowerCase() === "base64") {
      const encoded = await dataUrl(file);
      emit("update:modelValue", { filename: file.name || "upload.bin", content_type: file.type || "application/octet-stream", size: file.size, data_b64: encoded.slice(encoded.indexOf(",") + 1) });
    } else emit("update:modelValue", await file.text());
  } catch (error) { emit("error", error instanceof Error ? error.message : `Could not read ${file.name}.`); }
}
async function handleFile(event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  if (file) await setFile(file);
  input.value = "";
}
async function startCamera() {
  if (!navigator.mediaDevices?.getUserMedia) { emit("error", "Camera access is unavailable in this browser."); return; }
  try {
    cameraStream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: text(props.field.camera_facing_mode) === "environment" ? "environment" : "user" }, audio: false });
    cameraOpen.value = true;
    requestAnimationFrame(() => { if (cameraVideo.value) cameraVideo.value.srcObject = cameraStream; });
  } catch (error) { emit("error", error instanceof Error ? error.message : "Could not open the camera."); }
}
function stopCamera() {
  cameraStream?.getTracks().forEach((track) => track.stop());
  cameraStream = null;
  cameraOpen.value = false;
}
async function takePhoto() {
  const video = cameraVideo.value;
  if (!video || !video.videoWidth || !video.videoHeight) return;
  const canvas = document.createElement("canvas");
  canvas.width = video.videoWidth; canvas.height = video.videoHeight;
  canvas.getContext("2d")?.drawImage(video, 0, 0);
  const blob = await new Promise<Blob | null>((resolve) => canvas.toBlob(resolve, "image/jpeg", .9));
  if (blob) await setFile(new File([blob], `camera-${Date.now()}.jpg`, { type: "image/jpeg" }));
  stopCamera();
}
function canResetVideo(): boolean {
  return Boolean(props.field.reset_to_poster && (props.field.poster || props.field.poster_src));
}
function showVideoPoster() {
  const video = mediaVideo.value;
  if (!video || !canResetVideo() || !videoHasPlayed) return;
  resettingVideo = true;
  videoHasPlayed = false;
  if (!video.paused) video.pause();
  try { video.currentTime = 0; } catch { /* WebKit can reject seeks while metadata changes. */ }
  videoPosterVisible.value = true;
  resettingVideo = false;
}
function handleVideoPlay() {
  videoHasPlayed = true;
  videoPosterVisible.value = false;
}
function handleVideoPause(event: Event) {
  const video = event.target as HTMLVideoElement;
  if (!resettingVideo && !video.ended) showVideoPoster();
}
async function playVideoFromPoster() {
  const video = mediaVideo.value;
  if (!video) return;
  videoPosterVisible.value = false;
  try {
    video.currentTime = 0;
    await video.play();
  } catch {
    videoPosterVisible.value = true;
  }
}

onBeforeUnmount(stopCamera);
</script>

<template>
  <div v-if="visible" class="tcx-native-field" :class="[{ full: fullWidth, muted: conditionallyDisabled }, `type-${type}`]">
    <section v-if="type === 'heading' || type === 'section_heading'" class="core-builder-step-heading">
      <div class="core-builder-step-title">{{ label }}</div><div v-if="field.description" class="small">{{ field.description }}</div>
    </section>

    <input v-else-if="type === 'hidden'" type="hidden" :value="stringValue" />

    <label v-else-if="type === 'table'">
      <span>{{ label }}</span><div class="core-data-table-wrap"><table class="core-data-table"><thead><tr><th v-for="column in tableColumns" :key="column.key">{{ column.label }}</th></tr></thead><tbody><tr v-for="(row, rowIndex) in field.rows || []" :key="rowIndex"><td v-for="(column, columnIndex) in tableColumns" :key="column.key">{{ tableCell(row, column.key, columnIndex) }}</td></tr><tr v-if="!(field.rows || []).length"><td :colspan="Math.max(1, tableColumns.length)" class="small">No rows.</td></tr></tbody></table></div><small v-if="field.description">{{ field.description }}</small>
    </label>

    <label v-else-if="type === 'bar_chart' || type === 'bars'">
      <span>{{ label }}</span><div class="core-bar-chart"><div v-for="point in chartPoints" :key="point.label" class="core-bar-row"><div class="core-bar-label">{{ point.label }}</div><div class="core-bar-track"><div class="core-bar-fill" :style="{ width: `${point.width}%` }" /></div><div class="core-bar-value">{{ point.value }}</div></div><div v-if="!chartPoints.length" class="small">No chart data.</div></div><small v-if="field.description">{{ field.description }}</small>
    </label>

    <div v-else-if="type === 'image_checklist'" class="core-image-checklist">
      <div class="core-image-checklist-head"><strong>{{ label }}</strong><span class="small">{{ imageRows.length }} image{{ imageRows.length === 1 ? '' : 's' }}</span></div><div class="core-image-check-grid"><button v-for="row in imageRows" :key="row.src" type="button" class="core-image-check-card" :class="{ selected: selectedValues.has(row.value), 'read-only': !row.selectable }" :disabled="disabled || readOnly || !row.selectable" @click="toggleChoice(row.value)"><img :src="row.src" :alt="row.alt" loading="lazy" /><span class="core-image-check-copy"><strong v-if="row.caption">{{ row.caption }}</strong><small v-if="row.meta">{{ row.meta }}</small></span></button></div><small v-if="field.description">{{ field.description }}</small>
    </div>

    <div v-else-if="type === 'image'">
      <strong v-if="!field.hide_label">{{ label }}</strong><img v-if="field.src || field.url" class="tcx-native-media" :src="field.src || field.url" :alt="field.alt || label" loading="lazy" /><div v-else class="small">No image available.</div><small v-if="field.caption">{{ field.caption }}</small><small v-if="field.description">{{ field.description }}</small>
    </div>

    <div v-else-if="type === 'video'">
      <strong v-if="!field.hide_label">{{ label }}</strong><div v-if="field.src || field.url" class="tcx-native-video-shell"><video ref="mediaVideo" v-show="!videoPosterVisible" class="tcx-native-media" :src="field.src || field.url" :poster="field.poster || field.poster_src" :controls="field.controls !== false" :preload="field.preload || 'metadata'" playsinline @play="handleVideoPlay" @pause="handleVideoPause" @ended="showVideoPoster" /><button v-if="canResetVideo()" v-show="videoPosterVisible" type="button" class="tcx-native-video-poster" :aria-label="`Play ${label || 'event clip'}`" @click="playVideoFromPoster"><img :src="field.poster || field.poster_src" :alt="label || 'Event clip snapshot'" /><span aria-hidden="true">▶</span></button></div><div v-else class="small">No video available.</div><small v-if="field.caption">{{ field.caption }}</small><small v-if="field.description">{{ field.description }}</small>
    </div>

    <label v-else-if="type === 'checkbox'" class="tcx-native-toggle" :class="{ compact: presentation === 'compact' || presentation === 'compact_toggle' }">
      <input class="toggle-input" type="checkbox" :checked="Boolean(modelValue)" :disabled="disabled" @change="update" /><span><strong>{{ label }}</strong><small v-if="field.description">{{ field.description }}</small></span>
    </label>

    <label v-else-if="type === 'range'" class="core-range-field">
      <span class="core-range-field-head"><span>{{ label }}</span><output>{{ modelValue ?? 0 }}{{ field.suffix || '' }}</output></span><input type="range" :value="Number(modelValue ?? 0)" :min="field.min ?? 0" :max="field.max ?? 100" :step="field.step ?? 1" :disabled="disabled || readOnly" @input="update" @change="commit" /><small v-if="field.description">{{ field.description }}</small>
    </label>

    <div v-else-if="choiceCards" class="core-choice-card-field">
      <div class="core-choice-card-field-label">{{ label }}</div><div class="core-choice-card-grid" :class="{ 'is-multiple': multipleChoices }"><button v-for="option in options" :key="optionValue(option)" type="button" class="core-choice-card" :class="{ selected: selectedValues.has(optionValue(option)) }" :aria-pressed="selectedValues.has(optionValue(option))" :disabled="disabled || readOnly" @click="toggleChoice(optionValue(option))"><span v-if="optionDetail(option, 'icon')" class="core-choice-card-icon" aria-hidden="true">{{ optionDetail(option, 'icon') }}</span><span class="core-choice-card-copy"><strong>{{ optionLabel(option) }}</strong><span v-if="optionDetail(option, 'description')">{{ optionDetail(option, 'description') }}</span><small v-if="optionDetail(option, 'meta')">{{ optionDetail(option, 'meta') }}</small></span></button></div><small v-if="field.description">{{ field.description }}</small>
    </div>

    <label v-else-if="type === 'multiselect'">
      <span>{{ label }}</span><select multiple :size="Math.max(4, Math.min(10, options.length || 6))" :disabled="disabled || readOnly" @change="update"><option v-for="option in options" :key="optionValue(option)" :value="optionValue(option)" :selected="selectedValues.has(optionValue(option))">{{ optionLabel(option) }}</option></select><small v-if="field.description">{{ field.description }}</small>
    </label>

    <label v-else-if="type === 'select'">
      <span>{{ label }}</span><select :value="stringValue" :disabled="disabled || readOnly" @change="update"><template v-for="(option, index) in options" :key="optionValue(option) || index"><optgroup v-if="option && typeof option === 'object' && Array.isArray(option.options) && option.value === undefined && option.id === undefined && option.key === undefined" :label="option.label || option.title || 'Options'"><option v-for="child in option.options" :key="optionValue(child)" :value="optionValue(child)">{{ optionLabel(child) }}</option></optgroup><option v-else :value="optionValue(option)">{{ optionLabel(option) }}</option></template></select><small v-if="field.description">{{ field.description }}</small>
    </label>

    <label v-else-if="type === 'textarea' || type === 'multiline'">
      <span v-if="!field.hide_label">{{ label }}</span><div v-if="readOnly" class="core-readonly-text">{{ stringValue }}</div><textarea v-else :value="stringValue" :rows="field.rows || 4" :placeholder="field.placeholder" :disabled="disabled" @input="update" /><small v-if="field.description">{{ field.description }}</small>
    </label>

    <label v-else-if="type === 'file'">
      <span>{{ label }}</span><input type="file" :accept="field.accept" :capture="field.camera_capture ? (field.camera_facing_mode === 'environment' ? 'environment' : 'user') : undefined" :disabled="disabled || readOnly" @change="handleFile" /><div v-if="field.camera_capture" class="inline-row"><button v-if="!cameraOpen" class="inline-btn" type="button" @click="startCamera">Use camera</button><template v-else><button class="action-btn" type="button" @click="takePhoto">Take photo</button><button class="inline-btn" type="button" @click="stopCamera">Cancel</button></template></div><video v-if="cameraOpen" ref="cameraVideo" class="tcx-native-camera" autoplay muted playsinline /><small>{{ modelValue ? 'A value is ready. Choose another file to replace it.' : 'No file selected.' }}</small><small v-if="field.description">{{ field.description }}</small>
    </label>

    <label v-else>
      <span v-if="!field.hide_label">{{ label }}</span><div v-if="readOnly" class="core-readonly-text">{{ stringValue }}</div><input v-else :type="type === 'password' ? 'password' : type === 'number' ? 'number' : 'text'" :value="modelValue as string | number | undefined" :min="field.min" :max="field.max" :step="field.step" :placeholder="field.placeholder" :disabled="disabled" @input="update" /><small v-if="conditionallyDisabled && field.disabled_note">{{ field.disabled_note }}</small><small v-if="field.description">{{ field.description }}</small>
    </label>
  </div>
</template>
