<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from "vue";
import { postJson } from "../../shared/api";
import PopupTransition from "../../shared/PopupTransition.vue";
import PopupToast from "../../shared/PopupToast.vue";
import { normalizePopupEffect, popupEffects } from "../../shared/popupMotion";
import type { MiscSettings } from "../types";

const props = defineProps<{
  settings: MiscSettings;
  endpoint: string;
}>();

const emit = defineEmits<{
  saved: [settings: MiscSettings];
  notify: [message: string, tone?: string];
}>();

const draft = reactive({
  popup_effect_style: "tilt",
  emoji_enable_on_reaction_add: true,
  emoji_enable_auto_reaction_on_reply: true,
  emoji_reaction_chain_chance_percent: 100,
  emoji_reply_reaction_chance_percent: 12,
  emoji_reaction_chain_cooldown_seconds: 30,
  emoji_reply_reaction_cooldown_seconds: 120,
  emoji_min_message_length: 4,
});
const saving = ref(false);
const dirty = ref(false);
const error = ref("");
const notice = ref("");
const previewOpen = ref(false);

const previewToastOpen = ref(false);
const previewToastEffect = ref("tilt");
const previewToastMessage = ref("");
let previewToastTimer = 0;
const selectedEffect = computed(() => popupEffects.find(effect => effect.id === draft.popup_effect_style) || popupEffects[0]);

function bounded(value: unknown, fallback: number, minimum: number, maximum: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(maximum, Math.max(minimum, Math.round(parsed)));
}

function syncFromSettings(settings: MiscSettings) {
  Object.assign(draft, {
    popup_effect_style: normalizePopupEffect(settings.popup_effect_style),
    emoji_enable_on_reaction_add: settings.emoji_enable_on_reaction_add !== false,
    emoji_enable_auto_reaction_on_reply: settings.emoji_enable_auto_reaction_on_reply !== false,
    emoji_reaction_chain_chance_percent: bounded(settings.emoji_reaction_chain_chance_percent, 100, 0, 100),
    emoji_reply_reaction_chance_percent: bounded(settings.emoji_reply_reaction_chance_percent, 12, 0, 100),
    emoji_reaction_chain_cooldown_seconds: bounded(settings.emoji_reaction_chain_cooldown_seconds, 30, 0, 86400),
    emoji_reply_reaction_cooldown_seconds: bounded(settings.emoji_reply_reaction_cooldown_seconds, 120, 0, 86400),
    emoji_min_message_length: bounded(settings.emoji_min_message_length, 4, 0, 200),
  });
  dirty.value = false;
}

function markDirty() {
  dirty.value = true;
  error.value = "";
  notice.value = "";
}

function preview() { previewOpen.value = true; }
function closePreview() { previewOpen.value = false; }
function previewToast() {
  if (previewToastTimer) window.clearTimeout(previewToastTimer);
  previewToastEffect.value = draft.popup_effect_style;
  previewToastMessage.value = `${selectedEffect.value.label} preview. Click to dismiss, or watch it close.`;
  previewToastOpen.value = true;
  previewToastTimer = window.setTimeout(() => { previewToastOpen.value = false; }, 2200);
}

async function save() {
  saving.value = true;
  error.value = "";
  notice.value = "";
  const payload = {
    popup_effect_style: draft.popup_effect_style,
    emoji_enable_on_reaction_add: Boolean(draft.emoji_enable_on_reaction_add),
    emoji_enable_auto_reaction_on_reply: Boolean(draft.emoji_enable_auto_reaction_on_reply),
    emoji_reaction_chain_chance_percent: bounded(draft.emoji_reaction_chain_chance_percent, 100, 0, 100),
    emoji_reply_reaction_chance_percent: bounded(draft.emoji_reply_reaction_chance_percent, 12, 0, 100),
    emoji_reaction_chain_cooldown_seconds: bounded(draft.emoji_reaction_chain_cooldown_seconds, 30, 0, 86400),
    emoji_reply_reaction_cooldown_seconds: bounded(draft.emoji_reply_reaction_cooldown_seconds, 120, 0, 86400),
    emoji_min_message_length: bounded(draft.emoji_min_message_length, 4, 0, 200),
  };
  try {
    const next = await postJson<MiscSettings>(props.endpoint, payload);
    emit("saved", next);
    syncFromSettings(next);
    notice.value = "Misc settings saved and synchronized.";
    emit("notify", notice.value, "success");
  } catch (saveError) {
    error.value = saveError instanceof Error ? saveError.message : "Misc settings could not be saved.";
    emit("notify", error.value, "error");
  } finally {
    saving.value = false;
  }
}

watch(
  () => props.settings,
  (settings) => {
    if (!dirty.value) syncFromSettings(settings || {});
  },
  { immediate: true },
);

onBeforeUnmount(() => { if (previewToastTimer) window.clearTimeout(previewToastTimer); });
defineExpose({ hasUnsavedChanges: () => dirty.value });
</script>

<template>
  <section class="tset-resource">
    <div v-if="notice || error" class="tv-notice" :class="{ error: Boolean(error) }" aria-live="polite">
      {{ error || notice }}
    </div>

    <div class="tset-resource-grid">
      <section class="tv-panel tset-form-card popup-motion-card">
        <header>
          <div>
            <span class="tv-eyebrow">Motion</span>
          <h2>Compotato popup effects</h2>
          <p>A little Compiz spirit. Choose how dialogs and notifications open and close.</p>
          </div>
          <div class="popup-motion-preview-actions">
          <button class="tv-button" type="button" @click="preview">Preview dialog</button>
          <button class="tv-button" type="button" :disabled="previewToastOpen" @click="previewToast">Preview notification</button>
        </div>
        </header>
        <fieldset class="popup-motion-options">
          <legend>Popup animation style</legend>
          <label v-for="effect in popupEffects" :key="effect.id" class="popup-motion-choice" :class="{ selected: draft.popup_effect_style === effect.id }">
            <input v-model="draft.popup_effect_style" type="radio" name="popup-motion" :value="effect.id" @change="markDirty" />
            <span class="popup-motion-choice-copy"><span><strong>{{ effect.label }}</strong><small>{{ effect.hint }}</small></span><span>{{ effect.description }}</span></span>
          </label>
        </fieldset>
        <p class="popup-motion-note">Previews use your selection without saving. Reduced Motion in your system settings turns animations off automatically.</p>
      </section>

      <section class="tv-panel tset-form-card">
        <header>
          <span class="tv-eyebrow">Emoji</span>
          <h2>Reaction behavior</h2>
          <p>Control Discord reaction chains and automatic reactions to replies.</p>
        </header>
        <div class="tv-form-grid">
          <label class="tv-toggle">
            <input v-model="draft.emoji_enable_on_reaction_add" class="tv-checkbox" type="checkbox" @change="markDirty" />
            <span><strong>Reaction-chain mode</strong><small>React when another Discord reaction is added.</small></span>
          </label>
          <label class="tv-toggle">
            <input v-model="draft.emoji_enable_auto_reaction_on_reply" class="tv-checkbox" type="checkbox" @change="markDirty" />
            <span><strong>Automatic reply reactions</strong><small>Allow a reaction after replying.</small></span>
          </label>
          <label>
            Reaction-chain chance (%)
            <input v-model.number="draft.emoji_reaction_chain_chance_percent" type="number" min="0" max="100" @input="markDirty" />
          </label>
          <label>
            Reply reaction chance (%)
            <input v-model.number="draft.emoji_reply_reaction_chance_percent" type="number" min="0" max="100" @input="markDirty" />
          </label>
          <label>
            Reaction-chain cooldown (seconds)
            <input v-model.number="draft.emoji_reaction_chain_cooldown_seconds" type="number" min="0" max="86400" @input="markDirty" />
          </label>
          <label>
            Reply reaction cooldown (seconds)
            <input v-model.number="draft.emoji_reply_reaction_cooldown_seconds" type="number" min="0" max="86400" @input="markDirty" />
          </label>
          <label>
            Minimum message length
            <input v-model.number="draft.emoji_min_message_length" type="number" min="0" max="200" @input="markDirty" />
          </label>
        </div>
      </section>
    </div>

    <footer class="tset-save-bar">
      <div><strong>{{ dirty ? "Unsaved changes" : "Misc settings are synchronized" }}</strong><span>The server response becomes the active UI state.</span></div>
      <button class="tv-button primary" type="button" :disabled="saving || !dirty" @click="save">{{ saving ? "Saving…" : "Save misc settings" }}</button>
    </footer>

    <PopupTransition :open="previewOpen" :effect="draft.popup_effect_style" backdrop-class="tv-modal-backdrop tset-modal" @close="closePreview">
      <section class="tv-modal popup-effect-preview-dialog" role="dialog" aria-modal="true" aria-label="Compotato popup effect preview">
        <header>
          <div><span class="tv-eyebrow">Compotato preview</span><h2>{{ selectedEffect.label }} popup effect</h2><p>{{ selectedEffect.description }} Close this dialog to see the exit animation.</p></div>
          <button class="tv-button" type="button" @click="closePreview">Close preview</button>
        </header>
      </section>
    </PopupTransition>
    <div class="toast-root" aria-live="polite">
      <PopupToast :open="previewToastOpen" :effect="previewToastEffect" :message="previewToastMessage" @close="previewToastOpen = false" />
    </div>
  </section>
</template>
