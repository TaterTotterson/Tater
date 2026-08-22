<script setup lang="ts">
import { computed } from "vue";
import type { MusicField, Primitive, SelectOption } from "../types";
import {
  groupPlayerTargets,
  playerFriendlyName,
  playerSecondaryText,
  playerTargetKind,
} from "../playerDisplay";

const props = defineProps<{
  field: MusicField;
  modelValue: unknown;
  compact?: boolean;
}>();

const emit = defineEmits<{
  "update:modelValue": [value: unknown];
  change: [value: unknown];
}>();

const fieldType = computed(() => String(props.field.type || "text").toLowerCase());
const targetMultiselect = computed(() => fieldType.value === "player_multiselect");
const cardPresentation = computed(() => String(props.field.presentation || "").toLowerCase() === "cards");
const disabled = computed(() => Boolean(props.field.disabled || props.field.read_only));
const stringValue = computed(() => String(props.modelValue ?? ""));
const numberValue = computed(() => Number(props.modelValue ?? 0));
const selectedValues = computed(() =>
  new Set(
    (Array.isArray(props.modelValue) ? props.modelValue : [props.modelValue])
      .map((value) => String(value ?? ""))
      .filter(Boolean),
  ),
);
const optionSections = computed(() =>
  groupPlayerTargets(props.field.options || [], (option) => optionValue(option)),
);

function optionValue(option: SelectOption | Primitive): string {
  if (option && typeof option === "object") {
    return String(option.value ?? option.id ?? option.key ?? option.label ?? "");
  }
  return String(option ?? "");
}

function optionLabel(option: SelectOption | Primitive): string {
  if (option && typeof option === "object") {
    const candidates = [
      option.label,
      option.title,
      option.name,
      option.friendly_name,
      option.description,
      option.meta,
      optionValue(option),
    ];
    return candidates.map((value) => String(value ?? "").trim()).find(Boolean) || "Unnamed player";
  }
  return String(option ?? "").trim() || "Unnamed player";
}

function optionDisplayName(option: SelectOption | Primitive): string {
  return playerFriendlyName(optionLabel(option), optionValue(option));
}

function optionMeta(option: SelectOption | Primitive): string {
  if (!option || typeof option !== "object") return "";
  const label = optionLabel(option);
  const candidates = [option.description, option.meta, option.room, option.area];
  return candidates
    .map((value) => String(value ?? "").trim())
    .find((value) => Boolean(value) && value !== label) || "";
}

function optionDisplayMeta(option: SelectOption | Primitive): string {
  return playerSecondaryText(optionLabel(option), optionMeta(option), optionValue(option));
}

function optionKind(option: SelectOption | Primitive): string {
  return playerTargetKind(optionValue(option));
}

function optionGlyph(option: SelectOption | Primitive): string {
  if (option && typeof option === "object" && String(option.icon || "").trim()) {
    return String(option.icon).trim();
  }
  const kind = optionKind(option);
  if (kind === "stereo") return "T²";
  if (kind === "satellite") return "T";
  if (kind === "airplay") return "△";
  if (kind === "sonos") return "S";
  if (kind === "home") return "H";
  return "♪";
}

function updateFromInput(event: Event): void {
  const input = event.target as HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement;
  let value: unknown;
  if (fieldType.value === "checkbox") {
    value = (input as HTMLInputElement).checked;
  } else if (fieldType.value === "number" || fieldType.value === "range") {
    value = Number(input.value);
  } else {
    value = input.value;
  }
  emit("update:modelValue", value);
}

function commitInput(event: Event): void {
  const input = event.target as HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement;
  const value =
    fieldType.value === "checkbox"
      ? (input as HTMLInputElement).checked
      : fieldType.value === "number" || fieldType.value === "range"
        ? Number(input.value)
        : input.value;
  emit("update:modelValue", value);
  emit("change", value);
}

function toggleOption(value: string, checked: boolean): void {
  const next = new Set(selectedValues.value);
  if (checked) {
    next.add(value);
  } else {
    next.delete(value);
  }
  emit("update:modelValue", Array.from(next));
}
</script>

<template>
  <label
    v-if="fieldType === 'checkbox'"
    class="tm-field tm-checkbox"
    :class="{ compact }"
  >
    <input
      type="checkbox"
      :checked="Boolean(modelValue)"
      :disabled="disabled"
      @change="updateFromInput"
    />
    <span>
      <strong>{{ field.label || field.key }}</strong>
      <small v-if="field.description">{{ field.description }}</small>
    </span>
  </label>

  <fieldset
    v-else-if="fieldType === 'multiselect' || fieldType === 'player_multiselect'"
    class="tm-field tm-multiselect"
    :class="{
      compact,
      'full-width': Boolean(field.full_width),
      'tm-target-multiselect': targetMultiselect,
      'tm-choice-card-multiselect': cardPresentation,
    }"
  >
    <legend>{{ field.label || field.key }}</legend>
    <div v-if="cardPresentation" class="tm-choice-card-grid" role="group">
      <button
        v-for="option in field.options || []"
        :key="optionValue(option)"
        type="button"
        class="tm-choice-card"
        :class="{ selected: selectedValues.has(optionValue(option)) }"
        :aria-pressed="selectedValues.has(optionValue(option))"
        :disabled="disabled || !optionValue(option)"
        @click="toggleOption(optionValue(option), !selectedValues.has(optionValue(option)))"
      >
        <span v-if="optionGlyph(option)" class="tm-choice-card-icon" aria-hidden="true">{{ optionGlyph(option) }}</span>
        <span class="tm-choice-card-copy">
          <strong>{{ optionLabel(option) }}</strong>
          <small v-if="optionMeta(option)">{{ optionMeta(option) }}</small>
        </span>
      </button>
    </div>
    <div v-else-if="targetMultiselect" class="tm-option-sections">
      <section v-for="section in optionSections" :key="section.key" class="tm-option-section">
        <h4>{{ section.label }}</h4>
        <div class="tm-option-grid">
          <label
            v-for="option in section.items"
            :key="optionValue(option)"
            class="tm-option"
            :class="[
              `kind-${optionKind(option)}`,
              { 'is-selected': selectedValues.has(optionValue(option)) },
            ]"
          >
            <input
              type="checkbox"
              :checked="selectedValues.has(optionValue(option))"
              :disabled="disabled || !optionValue(option)"
              @change="toggleOption(optionValue(option), ($event.target as HTMLInputElement).checked)"
            />
            <span class="tm-option-icon" aria-hidden="true">{{ optionGlyph(option) }}</span>
            <span class="tm-option-copy">
              <strong>{{ optionDisplayName(option) }}</strong>
              <small v-if="optionDisplayMeta(option)">{{ optionDisplayMeta(option) }}</small>
            </span>
          </label>
        </div>
      </section>
    </div>
    <div v-else class="tm-option-grid">
      <label
        v-for="option in field.options || []"
        :key="optionValue(option)"
        class="tm-option"
        :class="[
          `kind-${optionKind(option)}`,
          { 'is-selected': selectedValues.has(optionValue(option)) },
        ]"
      >
        <input
          type="checkbox"
          :checked="selectedValues.has(optionValue(option))"
          :disabled="disabled || !optionValue(option)"
          @change="toggleOption(optionValue(option), ($event.target as HTMLInputElement).checked)"
        />
        <span class="tm-option-copy">
          <strong>{{ optionLabel(option) }}</strong>
          <small v-if="optionMeta(option)">{{ optionMeta(option) }}</small>
        </span>
      </label>
    </div>
    <small v-if="field.description">{{ field.description }}</small>
  </fieldset>

  <label v-else-if="fieldType === 'select'" class="tm-field" :class="{ compact }">
    <span>{{ field.label || field.key }}</span>
    <select :value="stringValue" :disabled="disabled" @change="updateFromInput">
      <option v-for="option in field.options || []" :key="optionValue(option)" :value="optionValue(option)">
        {{ optionLabel(option) }}
      </option>
    </select>
    <small v-if="field.description">{{ field.description }}</small>
  </label>

  <label v-else-if="fieldType === 'range'" class="tm-field tm-range" :class="{ compact }">
    <span>{{ field.label || field.key }}</span>
    <div class="tm-range-row">
      <input
        type="range"
        :value="numberValue"
        :min="field.min ?? 0"
        :max="field.max ?? 100"
        :step="field.step ?? 1"
        :disabled="disabled"
        @input="updateFromInput"
        @change="commitInput"
      />
      <output>{{ numberValue }}{{ field.suffix || '' }}</output>
    </div>
  </label>

  <label v-else class="tm-field" :class="{ compact }">
    <span>{{ field.label || field.key }}</span>
    <textarea
      v-if="fieldType === 'textarea' || fieldType === 'multiline'"
      :value="stringValue"
      :placeholder="field.placeholder"
      :required="field.required"
      :disabled="disabled"
      @input="updateFromInput"
    />
    <input
      v-else
      :type="fieldType === 'password' ? 'password' : fieldType === 'number' ? 'number' : 'text'"
      :value="modelValue as string | number | undefined"
      :placeholder="field.placeholder"
      :required="field.required"
      :disabled="disabled"
      :min="field.min"
      :max="field.max"
      :step="field.step"
      @input="updateFromInput"
    />
    <small v-if="field.description">{{ field.description }}</small>
  </label>
</template>
