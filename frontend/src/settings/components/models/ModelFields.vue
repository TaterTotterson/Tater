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
  return props.disabled || Boolean(field.disabled || field.read_only || field.readonly || ["table", "readonly", "section", "led_preview", "display_theme_preview"].includes(rawType));
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

function equalizerBands(field: JsonRow): JsonRow[] {
  const configured = Array.isArray(field.bands) ? field.bands as JsonRow[] : [];
  if (configured.length) return configured;
  return ["125 Hz", "250 Hz", "500 Hz", "1 kHz", "2 kHz", "3.5 kHz", "5.5 kHz", "8 kHz"]
    .map((label, index) => ({ label, index }));
}

function equalizerValues(field: JsonRow): number[] {
  const raw = valueOf(field);
  const values = Array.isArray(raw) ? raw : [];
  return equalizerBands(field).map((_band, index) => {
    const value = Number(values[index] ?? 0);
    return Number.isFinite(value) ? value : 0;
  });
}

function equalizerDb(value: number): string {
  if (value === 0) return "0 dB";
  return `${value > 0 ? "+" : ""}${value.toFixed(Number.isInteger(value) ? 0 : 1)} dB`;
}

function setEqualizerBand(field: JsonRow, index: number, event: Event) {
  const target = event.target as HTMLInputElement;
  const values = equalizerValues(field);
  values[index] = Number(target.value);
  emit("change", keyOf(field), values);
}

function resetEqualizer(field: JsonRow) {
  emit("change", keyOf(field), equalizerBands(field).map(() => 0));
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

function displayThemes(field: JsonRow): JsonRow[] {
  return Array.isArray(field.themes) ? field.themes : [];
}

function selectedDisplayTheme(field: JsonRow): JsonRow {
  const selected = String(props.values.display_theme || "tater");
  return displayThemes(field).find((row) => String(row.value || "") === selected)
    || displayThemes(field)[0]
    || {};
}

function displayThemeColors(field: JsonRow): string[] {
  const colors = selectedDisplayTheme(field).colors;
  return Array.isArray(colors) ? colors.map(String).slice(0, 4) : ["#ff8430", "#34e2b7", "#9a70ff", "#ffd05c"];
}

function displayThemeStyle(field: JsonRow): Record<string, string> {
  const colors = displayThemeColors(field);
  return {
    "--tm-display-primary": colors[0] || "#ff8430",
    "--tm-display-listening": colors[1] || colors[0] || "#34e2b7",
    "--tm-display-thinking": colors[2] || colors[0] || "#9a70ff",
    "--tm-display-highlight": colors[3] || colors[0] || "#ffd05c",
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
                <div class="tm-led-stage" :class="[`animation-${ledAnimation(state)}`, { 'tm-led-stage-single': field.single_light }]" aria-hidden="true">
                  <span class="tm-led-halo" />
                  <i v-for="dotIndex in 12" :key="dotIndex" class="tm-led-dot" :style="`--tm-led-i:${dotIndex - 1}`" />
                  <span class="tm-led-core" />
                </div>
                <div><strong>{{ state.label }}</strong><small>{{ ledAnimationLabel(state) }}</small></div>
              </article>
            </div>
          </div>

          <div v-else-if="visible(field) && typeOf(field) === 'display_theme_preview'" class="tm-field tm-field-wide tm-display-theme-preview">
            <span class="tm-field-label">
              {{ field.label || "Theme Preview" }}
              <small>{{ selectedDisplayTheme(field).label || "Tater Harvest" }}</small>
            </span>
            <div class="tm-display-theme-stage" :style="displayThemeStyle(field)" aria-hidden="true">
              <span class="tm-display-theme-orb" />
              <span class="tm-display-theme-clock">10:42</span>
              <span class="tm-display-theme-greeting">GOOD MORNING</span>
              <span class="tm-display-theme-weather">72°</span>
              <span class="tm-display-theme-condition">CLEAR SKIES</span>
              <span class="tm-display-theme-reply">Ready when you are</span>
            </div>
            <div class="tm-display-theme-swatches">
              <i v-for="color in displayThemeColors(field)" :key="color" :style="{ background: color }" />
              <span>{{ selectedDisplayTheme(field).description }}</span>
            </div>
          </div>

          <div v-else-if="visible(field) && typeOf(field) === 'equalizer'" class="tm-field tm-field-wide tm-equalizer">
            <div class="tm-equalizer-head">
              <span class="tm-field-label">{{ field.label || "Equalizer" }} <small>Per-device</small></span>
              <button class="tv-button" type="button" :disabled="readonly(field)" @click="resetEqualizer(field)">Reset flat</button>
            </div>
            <div class="tm-equalizer-bands" role="group" :aria-label="String(field.label || 'Equalizer')">
              <label v-for="(band, bandIndex) in equalizerBands(field)" :key="String(band.label || bandIndex)">
                <output>{{ equalizerDb(equalizerValues(field)[bandIndex]) }}</output>
                <input type="range" :aria-label="`${band.label || `Band ${bandIndex + 1}`} gain`" :min="field.min ?? -12" :max="field.max ?? 12" :step="field.step ?? 0.5" :value="equalizerValues(field)[bandIndex]" :disabled="readonly(field)" @input="setEqualizerBand(field, bandIndex, $event)" />
                <span>{{ band.label || `Band ${bandIndex + 1}` }}</span>
              </label>
            </div>
            <small v-if="field.description">{{ field.description }}</small>
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
