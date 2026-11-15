<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, onUpdated, ref, shallowRef, useId } from "vue";
import UiIcon from "../../../shared/UiIcon.vue";
import FocusedWorkspace from "../FocusedWorkspace.vue";
import { modelPresentation } from "./modelPresentation";

const props = withDefaults(defineProps<{ label: string; actionLabel?: string }>(), { actionLabel: "Choose model" });
type Choice = { value: string; label: string; disabled: boolean; group: string };
const nativeRoot = ref<HTMLElement | null>(null);
const trigger = ref<HTMLButtonElement | null>(null);
const searchInput = ref<HTMLInputElement | null>(null);
const choices = shallowRef<Choice[]>([]);
const selected = ref("");
const disabled = ref(false);
const ready = ref(false);
const open = ref(false);
const query = ref("");
const candidate = ref("");
const showIdentifiers = ref(false);
const showCurrentDetails = ref(false);
const id = useId();
let observer: MutationObserver | null = null;
let signature = "";
const current = computed(() => choices.value.find(option => option.value === selected.value));
const presentation = computed(() => modelPresentation(current.value?.label || selected.value, selected.value));
const filtered = computed(() => {
  const terms = query.value.toLocaleLowerCase().trim().split(/\s+/).filter(Boolean);
  return choices.value.filter(option => terms.every(term => `${option.label} ${option.value} ${option.group}`.toLocaleLowerCase().includes(term)));
});
const canApply = computed(() => !disabled.value && choices.value.some(option => option.value === candidate.value && !option.disabled));

function nativeSelect() { return nativeRoot.value?.querySelector("select") || null; }
function sync() {
  const select = nativeSelect();
  if (!select) return;
  const options = Array.from(select.options).map(option => ({
    value: option.value, label: option.textContent?.trim() || option.value,
    disabled: option.disabled || Boolean(option.parentElement instanceof HTMLOptGroupElement && option.parentElement.disabled),
    group: option.parentElement instanceof HTMLOptGroupElement ? option.parentElement.label : "",
  }));
  const snapshot = JSON.stringify([select.value, select.disabled, options]);
  if (signature === snapshot) return;
  signature = snapshot;
  choices.value = options;
  selected.value = select.value;
  disabled.value = select.disabled;
  if (!open.value) candidate.value = select.value;
  ready.value = true;
}
async function show() {
  sync();
  candidate.value = selected.value;
  query.value = "";
  showIdentifiers.value = false;
  open.value = true;
  await nextTick();
  searchInput.value?.focus();
}
async function close() {
  open.value = false;
  await nextTick();
  trigger.value?.focus();
}
function apply() {
  const select = nativeSelect();
  if (!select || !canApply.value) return;
  if (select.value !== candidate.value) {
    // Keep the original select as the source of truth. Its existing Vue model
    // binding and change handler run together, exactly as with the old picker.
    select.value = candidate.value;
    select.dispatchEvent(new Event("change", { bubbles: true }));
  }
  sync();
  void close();
}
onMounted(() => {
  sync();
  observer = new MutationObserver(sync);
  if (nativeRoot.value) observer.observe(nativeRoot.value, { childList: true, subtree: true, attributes: true, characterData: true });
});
onUpdated(sync);
onBeforeUnmount(() => observer?.disconnect());
</script>

<template>
  <div class="tm-native-model-picker" @keydown.esc.stop="close">
    <div v-show="!ready" ref="nativeRoot" class="tm-model-select-native"><slot /></div>
    <template v-if="ready">
      <div class="tm-model-selection">
        <div class="tm-model-selection-copy"><strong>{{ presentation.name }}</strong><small v-if="presentation.detail">{{ presentation.detail }}</small><small v-else>{{ label }}</small></div>
        <button ref="trigger" class="tv-button" type="button" :disabled="disabled" :aria-controls="`${id}-choices`" :aria-label="`${actionLabel}: ${label}`" @click="show">{{ actionLabel }}<UiIcon name="chevron" /></button>
      </div>
      <FocusedWorkspace :open="open" title="Choose a model" :eyebrow="label" description="Select an option, then confirm your choice. Save and apply the model settings when you are ready." back-label="Back to model settings" @close="close">
        <section :id="`${id}-choices`" class="tm-model-chooser tm-model-choice-workspace" :aria-label="`Choose ${label}`">
        <header><div><h2>{{ label }}</h2><p>Current: {{ presentation.name }}</p></div><span class="tm-model-current">{{ choices.length }} {{ choices.length === 1 ? 'option' : 'options' }}</span></header>
        <label class="tm-model-search"><UiIcon name="search" /><input ref="searchInput" v-model="query" type="search" :aria-label="`Search ${label}`" placeholder="Search by name or model ID…" /></label>
        <div class="tm-model-options">
          <label v-for="(option, index) in filtered" :key="`${option.value}:${index}`" class="tm-model-option" :class="{ disabled: option.disabled }">
            <input v-model="candidate" type="radio" :name="`${id}-choice`" :value="option.value" :disabled="disabled || option.disabled" />
            <span><strong>{{ modelPresentation(option.label, option.value).name }}</strong><small v-if="option.group || modelPresentation(option.label, option.value).detail">{{ [option.group, modelPresentation(option.label, option.value).detail].filter(Boolean).join(' · ') }}</small><code v-if="showIdentifiers">{{ option.value || option.label }}</code><small v-if="showIdentifiers && option.value !== option.label">{{ option.label }}</small></span>
            <small v-if="option.value === selected" class="tm-model-current">Current</small>
          </label>
          <p v-if="!filtered.length" class="tv-empty" role="status">No matching options. Try another name or clear the search.</p>
        </div>
        <footer><span role="status">{{ filtered.length }} {{ filtered.length === 1 ? 'option' : 'options' }} shown</span><button class="tv-button" type="button" :aria-pressed="showIdentifiers" @click="showIdentifiers = !showIdentifiers">{{ showIdentifiers ? 'Hide model details' : 'Show model details' }}</button></footer>
      </section>
      <template #footer><span>Your choice updates the form. It is applied when you save the model settings.</span><div class="tm-inline-actions"><button class="tv-button" type="button" @click="close">Cancel</button><button class="tv-button primary" type="button" :disabled="!canApply" @click="apply">Use this model</button></div></template>
      </FocusedWorkspace>
      <div v-if="presentation.identifier && presentation.name !== presentation.original" class="tm-model-identifier"><button class="tm-link-button" type="button" :aria-expanded="showCurrentDetails" @click="showCurrentDetails = !showCurrentDetails">{{ showCurrentDetails ? 'Hide model identifier' : 'Full model identifier' }}</button><template v-if="showCurrentDetails"><code>{{ presentation.identifier }}</code><small v-if="presentation.original !== presentation.identifier">{{ presentation.original }}</small></template></div>
    </template>
  </div>
</template>
