<script setup lang="ts">
import { computed } from "vue";

type JsonRow = Record<string, any>;

const props = defineProps<{
  field: JsonRow;
  modelValue: unknown;
  allValues: JsonRow;
}>();

const emit = defineEmits<{
  "update:modelValue": [value: unknown];
  error: [message: string];
  notify: [message: string, tone?: string];
}>();

const type = computed(() => text(props.field.type || "text").toLowerCase());
const label = computed(() => text(props.field.label || props.field.key || "Setting"));
const stringValue = computed(() => String(props.modelValue ?? ""));
const selectedValues = computed(() => new Set(normalizeList(props.modelValue)));
const visible = computed(() => {
  const conditions = Array.isArray(props.field.show_when_all)
    ? props.field.show_when_all
    : props.field.show_when && typeof props.field.show_when === "object"
      ? [props.field.show_when]
      : [];
  return conditions.every((condition: JsonRow) => {
    const source = text(condition.source_key ?? condition.key);
    if (!source) return true;
    const allowed = [
      ...(Array.isArray(condition.any_of) ? condition.any_of : []),
      ...(Array.isArray(condition.values) ? condition.values : []),
      ...(condition.equals !== undefined ? [condition.equals] : []),
      ...(condition.value !== undefined ? [condition.value] : []),
    ].map((value) => String(value ?? "").trim());
    if (!allowed.length) return true;
    const current = typeof props.allValues[source] === "boolean"
      ? props.allValues[source] ? "true" : "false"
      : String(props.allValues[source] ?? "").trim();
    return allowed.includes(current);
  });
});
const keyTools = computed(() => ["API_AUTH_KEY", "AUTH_TOKEN"].includes(text(props.field.key).toUpperCase()));

function text(value: unknown): string { return String(value ?? "").trim(); }
function normalizeList(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((row) => String(row ?? "")).filter(Boolean);
  const raw = text(value);
  if (!raw) return [];
  if (raw.startsWith("[") && raw.endsWith("]")) {
    try { const parsed = JSON.parse(raw); if (Array.isArray(parsed)) return parsed.map((row) => String(row ?? "")).filter(Boolean); }
    catch { /* Fall back to comma-separated values. */ }
  }
  return raw.split(",").map((row) => row.trim()).filter(Boolean);
}
function optionValue(option: unknown): string {
  if (option && typeof option === "object") {
    const row = option as JsonRow;
    return text(row.value ?? row.id ?? row.key ?? row.label);
  }
  return text(option);
}
function optionLabel(option: unknown): string {
  if (option && typeof option === "object") {
    const row = option as JsonRow;
    return text(row.label ?? row.name ?? row.title ?? optionValue(row));
  }
  return text(option);
}
function update(event: Event) {
  const input = event.target as HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement;
  if (type.value === "checkbox") emit("update:modelValue", (input as HTMLInputElement).checked);
  else if (type.value === "number" || type.value === "range") emit("update:modelValue", input.value === "" ? "" : Number(input.value));
  else emit("update:modelValue", input.value);
}
function toggleOption(value: string, checked: boolean) {
  const next = new Set(selectedValues.value);
  if (checked) next.add(value); else next.delete(value);
  emit("update:modelValue", [...next]);
}
function dataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error(`Could not read ${file.name}.`));
    reader.readAsDataURL(file);
  });
}
async function handleFile(event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  if (!file) return;
  const maxBytes = Number(props.field.max_bytes || 0);
  if (maxBytes > 0 && file.size > maxBytes) {
    emit("error", `${file.name} is larger than ${Math.max(1, Math.floor(maxBytes / 1024 / 1024))} MB.`);
    input.value = "";
    return;
  }
  try {
    if (text(props.field.file_encoding || props.field.encoding).toLowerCase() === "base64") {
      const encoded = await dataUrl(file);
      emit("update:modelValue", {
        filename: file.name || "upload.bin",
        content_type: file.type || "application/octet-stream",
        size: file.size,
        data_b64: encoded.slice(encoded.indexOf(",") + 1),
      });
    } else {
      const content = await file.text();
      if (text(props.field.accept).toLowerCase().includes("json") || file.name.toLowerCase().endsWith(".json")) JSON.parse(content);
      emit("update:modelValue", content);
    }
  } catch (error) {
    emit("error", error instanceof Error ? error.message : `Could not read ${file.name}.`);
  } finally { input.value = ""; }
}
function generateKey() {
  const bytes = new Uint8Array(24);
  crypto.getRandomValues(bytes);
  emit("update:modelValue", [...bytes].map((value) => value.toString(16).padStart(2, "0")).join(""));
}
async function copyKey() {
  if (!stringValue.value) { emit("error", "No key to copy."); return; }
  try { await navigator.clipboard.writeText(stringValue.value); emit("notify", "Key copied."); }
  catch { emit("error", "Clipboard is unavailable."); }
}
</script>

<template>
  <template v-if="visible">
    <input v-if="type === 'hidden'" type="hidden" :value="stringValue" />
    <section v-else-if="type === 'section' || type === 'header'" class="tvf-section">
      <h3>{{ label }}</h3><p v-if="field.description">{{ field.description }}</p>
    </section>
    <label v-else-if="type === 'readonly' || type === 'read_only'" class="tvf-field full">
      <span>{{ label }}</span><output>{{ stringValue }}</output><small v-if="field.description">{{ field.description }}</small>
    </label>
    <label v-else-if="type === 'checkbox'" class="tv-toggle tvf-toggle full">
      <input class="tv-checkbox" type="checkbox" :checked="Boolean(modelValue)" @change="update" />
      <span><strong>{{ label }}</strong><small v-if="field.description">{{ field.description }}</small></span>
    </label>
    <fieldset v-else-if="type === 'multiselect'" class="tvf-field tvf-multiselect full">
      <legend>{{ label }}</legend>
      <div><label v-for="option in field.options || []" :key="optionValue(option)"><input class="tv-checkbox" type="checkbox" :checked="selectedValues.has(optionValue(option))" @change="toggleOption(optionValue(option), ($event.target as HTMLInputElement).checked)" /><span>{{ optionLabel(option) }}</span></label></div>
      <small v-if="field.description">{{ field.description }}</small>
    </fieldset>
    <label v-else-if="type === 'select'" class="tvf-field">
      <span>{{ label }}</span><select :value="stringValue" @change="update"><option v-for="option in field.options || []" :key="optionValue(option)" :value="optionValue(option)">{{ optionLabel(option) }}</option></select><small v-if="field.description">{{ field.description }}</small>
    </label>
    <label v-else-if="type === 'textarea' || type === 'multiline'" class="tvf-field full">
      <span>{{ label }}</span><textarea :value="stringValue" :placeholder="field.placeholder" :rows="field.rows || 4" @input="update" /><small v-if="field.description">{{ field.description }}</small>
    </label>
    <label v-else-if="type === 'file'" class="tvf-field full">
      <span>{{ label }}</span><input type="file" :accept="field.accept" @change="handleFile" /><small>{{ modelValue ? 'A saved value is present. Choose a file to replace it.' : 'No file saved.' }}</small><small v-if="field.description">{{ field.description }}</small>
    </label>
    <label v-else-if="type === 'range'" class="tvf-field">
      <span>{{ label }}</span><div class="tvf-range"><input type="range" :value="Number(modelValue ?? field.default ?? 0)" :min="field.min ?? 0" :max="field.max ?? 100" :step="field.step ?? 1" @input="update" /><output>{{ modelValue }}{{ field.suffix || '' }}</output></div><small v-if="field.description">{{ field.description }}</small>
    </label>
    <label v-else class="tvf-field" :class="{ full: field.full_width }">
      <span>{{ label }}</span>
      <div :class="{ 'tvf-input-actions': keyTools }"><input :type="['password','number','color','time','email','url'].includes(type) ? type : 'text'" :value="modelValue as string | number | undefined" :min="field.min" :max="field.max" :step="field.step" :placeholder="field.placeholder" @input="update" /><template v-if="keyTools"><button class="tv-button" type="button" @click="copyKey">Copy</button><button class="tv-button" type="button" @click="generateKey">Generate</button></template></div>
      <small v-if="field.description">{{ field.description }}</small>
    </label>
  </template>
</template>
