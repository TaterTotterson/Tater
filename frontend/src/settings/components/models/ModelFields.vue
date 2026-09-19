<script setup lang="ts">
import { computed } from "vue";
import type { JsonRow } from "../../types";

const props = withDefaults(defineProps<{
  sections?: JsonRow[];
  values: JsonRow;
  disabled?: boolean;
}>(), {
  sections: () => [],
  disabled: false,
});

const emit = defineEmits<{
  change: [key: string, value: unknown];
}>();

const rows = computed(() => (Array.isArray(props.sections) ? props.sections : []));

function fields(section: JsonRow): JsonRow[] {
  return Array.isArray(section.fields) ? section.fields : [];
}

function keyOf(field: JsonRow): string {
  return String(field.key || field.id || "").trim();
}

function typeOf(field: JsonRow): string {
  const token = String(field.type || "text").trim().toLowerCase();
  if (token === "toggle") return "checkbox";
  if (token === "readonly") return "text";
  return token;
}

function valueOf(field: JsonRow): unknown {
  const key = keyOf(field);
  if (Object.prototype.hasOwnProperty.call(props.values, key)) return props.values[key];
  return field.value ?? field.default ?? (typeOf(field) === "checkbox" ? false : "");
}

function optionsOf(field: JsonRow): JsonRow[] {
  const dependent = field.dependent_options && typeof field.dependent_options === "object"
    ? field.dependent_options as JsonRow
    : null;
  if (dependent) {
    const sourceKey = String(dependent.source_key || "");
    const sourceValue = String(props.values[sourceKey] ?? "");
    const bySource = dependent.options_by_source && typeof dependent.options_by_source === "object"
      ? dependent.options_by_source as JsonRow
      : {};
    const selected = bySource[sourceValue];
    if (Array.isArray(selected)) return selected;
    if (Array.isArray(dependent.default_options)) return dependent.default_options;
  }
  return Array.isArray(field.options) ? field.options : [];
}

function optionValue(option: unknown): string {
  if (option && typeof option === "object") {
    const row = option as JsonRow;
    return String(row.value ?? row.id ?? row.key ?? "");
  }
  return String(option ?? "");
}

function optionLabel(option: unknown): string {
  if (option && typeof option === "object") {
    const row = option as JsonRow;
    return String(row.label ?? row.name ?? row.value ?? row.id ?? "");
  }
  return String(option ?? "");
}

function groupedOptions(option: unknown): JsonRow[] {
  if (!option || typeof option !== "object") return [];
  const rows = (option as JsonRow).options;
  return Array.isArray(rows) ? rows : [];
}

function visible(field: JsonRow): boolean {
  const condition = field.show_when && typeof field.show_when === "object" ? field.show_when as JsonRow : null;
  if (!condition) return true;
  const actual = props.values[String(condition.source_key || "")];
  if (Array.isArray(condition.any_of)) return condition.any_of.map(String).includes(String(actual ?? ""));
  if (Object.prototype.hasOwnProperty.call(condition, "equals")) return String(actual ?? "") === String(condition.equals ?? "");
  return true;
}

function readonly(field: JsonRow): boolean {
  const rawType = String(field.type || "").trim().toLowerCase();
  return props.disabled || Boolean(field.disabled || field.read_only || field.readonly || ["table", "readonly", "section", "led_preview"].includes(rawType));
}

function setValue(field: JsonRow, event: Event) {
  const target = event.target as HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement;
  const value = typeOf(field) === "checkbox" && target instanceof HTMLInputElement ? target.checked : target.value;
  emit("change", keyOf(field), value);
}

function columns(field: JsonRow): JsonRow[] {
  return Array.isArray(field.columns) ? field.columns : [];
}

function tableRows(field: JsonRow): JsonRow[] {
  return Array.isArray(field.rows) ? field.rows : [];
}

function ledAnimation(state: JsonRow): string {
  const raw = String(props.values[String(state.animation_key || "")] || "pulse").trim().toLowerCase();
  return raw.replace(/[^a-z0-9_-]+/g, "_") || "pulse";
}

function ledAnimationLabel(state: JsonRow): string {
  return ledAnimation(state).replace(/[_-]+/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function ledPreviewStyle(): Record<string, string> {
  const brightness = Math.max(0, Math.min(100, Number(props.values.led_brightness ?? 100)));
  return {
    "--tm-led-color": String(props.values.led_color || "#ff9b45"),
    "--tm-led-strength": String(.42 + (brightness / 100) * .58),
  };
}
</script>

<template>
  <section class="tm-field-sections">
    <article v-for="(section, sectionIndex) in rows" :key="String(section.label || sectionIndex)" class="tm-form-card">
      <header v-if="section.label || section.description">
        <h3>{{ section.label || "Settings" }}</h3>
        <p v-if="section.description">{{ section.description }}</p>
      </header>
      <div class="tm-field-grid">
        <template v-for="field in fields(section)" :key="keyOf(field)">
          <div v-if="visible(field) && typeOf(field) === 'section'" class="tm-field-section-break">
            <strong>{{ field.label || "Settings" }}</strong><small v-if="field.description">{{ field.description }}</small>
          </div>

          <div v-else-if="visible(field) && typeOf(field) === 'led_preview'" class="tm-field tm-field-wide tm-led-preview">
            <span class="tm-field-label">{{ field.label || "LED Preview" }} <small>Live examples</small></span>
            <div class="tm-led-preview-grid">
              <article v-for="state in field.states || []" :key="String(state.label)" class="tm-led-preview-card" :style="ledPreviewStyle()">
                <div class="tm-led-stage" :class="`animation-${ledAnimation(state)}`" aria-hidden="true">
                  <span class="tm-led-halo" />
                  <i v-for="dotIndex in 12" :key="dotIndex" class="tm-led-dot" :style="`--tm-led-i:${dotIndex - 1}`" />
                  <span class="tm-led-core" />
                </div>
                <div><strong>{{ state.label }}</strong><small>{{ ledAnimationLabel(state) }}</small></div>
              </article>
            </div>
          </div>

          <div v-else-if="visible(field) && typeOf(field) === 'table'" class="tm-field tm-field-wide tm-runtime-table-field">
            <span class="tm-field-label">{{ field.label }}</span>
            <div class="tm-table-wrap">
              <table>
                <thead><tr><th v-for="column in columns(field)" :key="String(column.key)">{{ column.label || column.key }}</th></tr></thead>
                <tbody>
                  <tr v-for="(row, rowIndex) in tableRows(field)" :key="rowIndex">
                    <td v-for="column in columns(field)" :key="String(column.key)">{{ row[String(column.key)] ?? "—" }}</td>
                  </tr>
                  <tr v-if="!tableRows(field).length"><td :colspan="Math.max(1, columns(field).length)">No results yet.</td></tr>
                </tbody>
              </table>
            </div>
            <small v-if="field.description">{{ field.description }}</small>
          </div>

          <label v-else-if="visible(field) && typeOf(field) === 'checkbox'" class="tm-field tm-toggle-field" :class="{ 'tm-field-wide': field.full_width }">
            <span><strong>{{ field.label || keyOf(field) }}</strong><small v-if="field.description">{{ field.description }}</small></span>
            <input type="checkbox" :checked="Boolean(valueOf(field))" :disabled="readonly(field)" @change="setValue(field, $event)" />
          </label>

          <label v-else-if="visible(field)" class="tm-field" :class="{ 'tm-field-wide': field.full_width || typeOf(field) === 'textarea' }">
            <span class="tm-field-label">{{ field.label || keyOf(field) }}</span>
            <select v-if="typeOf(field) === 'select'" :value="String(valueOf(field) ?? '')" :disabled="readonly(field)" @change="setValue(field, $event)">
              <template v-for="(option, optionIndex) in optionsOf(field)" :key="optionValue(option) || `${optionLabel(option)}:${optionIndex}`">
                <optgroup v-if="groupedOptions(option).length" :label="optionLabel(option)">
                  <option v-for="child in groupedOptions(option)" :key="optionValue(child)" :value="optionValue(child)">{{ optionLabel(child) }}</option>
                </optgroup>
                <option v-else :value="optionValue(option)">{{ optionLabel(option) }}</option>
              </template>
            </select>
            <textarea v-else-if="typeOf(field) === 'textarea'" :value="String(valueOf(field) ?? '')" :placeholder="String(field.placeholder || '')" :readonly="readonly(field)" @input="setValue(field, $event)" />
            <input
              v-else
              :type="typeOf(field) === 'number' ? 'number' : typeOf(field) === 'password' ? 'password' : typeOf(field) === 'time' ? 'time' : typeOf(field) === 'color' ? 'color' : 'text'"
              :value="String(valueOf(field) ?? '')"
              :placeholder="String(field.placeholder || '')"
              :min="field.min"
              :max="field.max"
              :step="field.step"
              :readonly="readonly(field)"
              @input="setValue(field, $event)"
            />
            <small v-if="field.description">{{ field.description }}</small>
          </label>
        </template>
      </div>
    </article>
  </section>
</template>
