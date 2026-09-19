<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref, watch } from "vue";
import { postJson } from "../../shared/api";
import type { GeneralSettings } from "../types";

const props = defineProps<{
  settings: GeneralSettings;
  endpoint: string;
  onThemePreview?: (theme: string) => void;
}>();

const emit = defineEmits<{
  saved: [settings: GeneralSettings];
  notify: [message: string, tone?: string];
}>();

const draft = reactive({
  username: "User",
  show_speed_stats: false,
  webui_theme: "tater",
  tater_first_name: "Tater",
  tater_last_name: "Totterson",
  tater_personality: "",
});
const password = ref("");
const passwordConfirm = ref("");
const clearPassword = ref(false);
const userAvatar = ref("");
const taterAvatar = ref("");
const userAvatarUpload = ref("");
const taterAvatarUpload = ref("");
const clearUserAvatar = ref(false);
const clearTaterAvatar = ref(false);
const saving = ref(false);
const dirty = ref(false);
const error = ref("");
const notice = ref("");
const userAvatarInput = ref<HTMLInputElement | null>(null);
const taterAvatarInput = ref<HTMLInputElement | null>(null);
const themes = [
  { id: "tater", name: "Tater", description: "The original roasted orange Tater look.", colors: ["#d65a1f", "#f08345", "#202225"] },
  { id: "tater-light", name: "Tater Light", description: "Warm orange on cream and soft-white surfaces.", colors: ["#c8531d", "#fffaf3", "#eadbca"] },
  { id: "blueberry", name: "Blueberry", description: "Clear blue accents with a cool navy surface.", colors: ["#4285f4", "#70b7ff", "#1b2635"] },
  { id: "mint", name: "Mint", description: "Fresh teal-green accents and forest shadows.", colors: ["#32b58d", "#67d6b1", "#182923"] },
  { id: "grape", name: "Grape", description: "Rich violet accents with deep plum panels.", colors: ["#9b6cf4", "#c18aff", "#292036"] },
  { id: "strawberry", name: "Strawberry", description: "Warm rose accents with berry-toned surfaces.", colors: ["#e75f91", "#ff8fb6", "#30202a"] },
] as const;

const passwordSet = computed(() => Boolean(props.settings.webui_password_set));
const passwordStatus = computed(() => {
  if (clearPassword.value) return "WebUI password will be removed when you save.";
  if (password.value || passwordConfirm.value) return "The new WebUI password will be applied when you save.";
  return passwordSet.value
    ? "WebUI password is enabled. Login is required."
    : "No WebUI password is set. Login is not required.";
});

function text(value: unknown, fallback = ""): string {
  return String(value ?? "").trim() || fallback;
}

function avatarInitial(value: unknown, fallback: string): string {
  return text(value, fallback).charAt(0).toUpperCase() || fallback;
}

function syncFromSettings(settings: GeneralSettings) {
  Object.assign(draft, {
    username: text(settings.username, "User"),
    show_speed_stats: Boolean(settings.show_speed_stats),
    webui_theme: text(settings.webui_theme, "tater").toLowerCase(),
    tater_first_name: text(settings.tater_first_name, "Tater"),
    tater_last_name: text(settings.tater_last_name, "Totterson"),
    tater_personality: String(settings.tater_personality ?? ""),
  });
  userAvatar.value = text(settings.user_avatar);
  taterAvatar.value = text(settings.tater_avatar);
  userAvatarUpload.value = "";
  taterAvatarUpload.value = "";
  clearUserAvatar.value = false;
  clearTaterAvatar.value = false;
  clearPassword.value = false;
  password.value = "";
  passwordConfirm.value = "";
  if (userAvatarInput.value) userAvatarInput.value.value = "";
  if (taterAvatarInput.value) taterAvatarInput.value.value = "";
  dirty.value = false;
}

function selectTheme(theme: string) {
  draft.webui_theme = theme;
  props.onThemePreview?.(theme);
  markDirty();
}

function markDirty() {
  dirty.value = true;
  notice.value = "";
  error.value = "";
}

function onPasswordInput() {
  if (password.value || passwordConfirm.value) clearPassword.value = false;
  markDirty();
}

function queuePasswordRemoval() {
  password.value = "";
  passwordConfirm.value = "";
  clearPassword.value = true;
  markDirty();
}

function readFile(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("The selected image could not be read."));
    reader.readAsDataURL(file);
  });
}

async function selectAvatar(kind: "user" | "tater", event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  if (!file) return;
  try {
    const dataUrl = await readFile(file);
    if (kind === "user") {
      userAvatar.value = dataUrl;
      userAvatarUpload.value = dataUrl;
      clearUserAvatar.value = false;
    } else {
      taterAvatar.value = dataUrl;
      taterAvatarUpload.value = dataUrl;
      clearTaterAvatar.value = false;
    }
    markDirty();
  } catch (readError) {
    error.value = readError instanceof Error ? readError.message : "The selected image could not be read.";
  }
}

function clearAvatar(kind: "user" | "tater") {
  if (kind === "user") {
    userAvatar.value = "";
    userAvatarUpload.value = "";
    clearUserAvatar.value = true;
    if (userAvatarInput.value) userAvatarInput.value.value = "";
  } else {
    taterAvatar.value = "";
    taterAvatarUpload.value = "";
    clearTaterAvatar.value = true;
    if (taterAvatarInput.value) taterAvatarInput.value.value = "";
  }
  markDirty();
}

async function save() {
  error.value = "";
  notice.value = "";
  if (password.value || passwordConfirm.value) {
    if (password.value.length < 4) {
      error.value = "WebUI password must be at least 4 characters.";
      return;
    }
    if (password.value !== passwordConfirm.value) {
      error.value = "WebUI password confirmation does not match.";
      return;
    }
  }

  const payload: Record<string, unknown> = {
    username: text(draft.username, "User"),
    show_speed_stats: Boolean(draft.show_speed_stats),
    webui_theme: draft.webui_theme,
    tater_first_name: text(draft.tater_first_name, "Tater"),
    tater_last_name: text(draft.tater_last_name, "Totterson"),
    tater_personality: draft.tater_personality,
  };
  if (clearPassword.value) payload.clear_webui_password = true;
  else if (password.value) {
    payload.webui_password = password.value;
    payload.webui_password_confirm = passwordConfirm.value;
  }
  if (clearUserAvatar.value) payload.clear_user_avatar = true;
  else if (userAvatarUpload.value) payload.user_avatar = userAvatarUpload.value;
  if (clearTaterAvatar.value) payload.clear_tater_avatar = true;
  else if (taterAvatarUpload.value) payload.tater_avatar = taterAvatarUpload.value;

  saving.value = true;
  try {
    const next = await postJson<GeneralSettings>(props.endpoint, payload);
    emit("saved", next);
    syncFromSettings(next);
    notice.value = "General settings saved and synchronized.";
    emit("notify", notice.value, "success");
  } catch (saveError) {
    error.value = saveError instanceof Error ? saveError.message : "General settings could not be saved.";
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

onBeforeUnmount(() => {
  if (dirty.value) props.onThemePreview?.(text(props.settings.webui_theme, "tater").toLowerCase());
});
</script>

<template>
  <section class="tset-general">
    <div v-if="notice || error" class="tv-notice" :class="{ error: Boolean(error) }" aria-live="polite">
      {{ error || notice }}
    </div>

    <div class="tset-general-grid">
      <section class="tv-panel tset-form-card tset-theme-section">
        <header>
          <span class="tv-eyebrow">Appearance</span>
          <h2>Color theme</h2>
          <p>Choose a palette for the whole Tater WebUI. Changes preview immediately and are kept when you save.</p>
        </header>
        <div class="tset-theme-grid" role="radiogroup" aria-label="Tater color theme">
          <button
            v-for="theme in themes"
            :key="theme.id"
            class="tset-theme-card"
            :class="{ active: draft.webui_theme === theme.id }"
            type="button"
            role="radio"
            :aria-checked="draft.webui_theme === theme.id"
            @click="selectTheme(theme.id)"
          >
            <span class="tset-theme-swatches" aria-hidden="true">
              <i v-for="color in theme.colors" :key="color" :style="{ background: color }" />
            </span>
            <span class="tset-theme-copy"><strong>{{ theme.name }}</strong><small>{{ theme.description }}</small></span>
            <span class="tset-theme-check" aria-hidden="true">{{ draft.webui_theme === theme.id ? '✓' : '' }}</span>
          </button>
        </div>
      </section>

      <section class="tv-panel tset-form-card">
        <header>
          <span class="tv-eyebrow">Identity</span>
          <h2>Names and personality</h2>
          <p>These values are shared by Chat and every surface that refers to you or Tater.</p>
        </header>
        <div class="tv-form-grid">
          <label>
            WebUI username
            <input v-model="draft.username" type="text" autocomplete="username" @input="markDirty" />
          </label>
          <label class="tv-toggle tset-inline-toggle">
            <input v-model="draft.show_speed_stats" class="tv-checkbox" type="checkbox" @change="markDirty" />
            <span><strong>Show tokens/sec stats</strong><small>Display generation speed in Chat.</small></span>
          </label>
          <label>
            Tater first name
            <input v-model="draft.tater_first_name" type="text" @input="markDirty" />
          </label>
          <label>
            Tater last name
            <input v-model="draft.tater_last_name" type="text" @input="markDirty" />
          </label>
          <label class="full">
            Personality / style
            <textarea v-model="draft.tater_personality" rows="5" @input="markDirty" />
          </label>
        </div>
      </section>

      <section class="tv-panel tset-form-card">
        <header>
          <span class="tv-eyebrow">Access</span>
          <h2>WebUI login</h2>
          <p>Leave both password fields blank to keep the current login setting.</p>
        </header>
        <div class="tv-form-grid">
          <label>
            New password
            <input v-model="password" type="password" autocomplete="new-password" @input="onPasswordInput" />
          </label>
          <label>
            Repeat password
            <input v-model="passwordConfirm" type="password" autocomplete="new-password" @input="onPasswordInput" />
          </label>
        </div>
        <div class="tset-password-row">
          <button class="tv-button danger" type="button" :disabled="!passwordSet || Boolean(password || passwordConfirm)" @click="queuePasswordRemoval">
            Remove password
          </button>
          <span>{{ passwordStatus }}</span>
        </div>
      </section>

      <section class="tv-panel tset-form-card tset-avatar-section">
        <header>
          <span class="tv-eyebrow">Appearance</span>
          <h2>Chat avatars</h2>
          <p>Previews update immediately; the new images become authoritative when saved.</p>
        </header>
        <div class="tset-avatar-grid">
          <article>
            <span>WebUI user</span>
            <img v-if="userAvatar" :src="userAvatar" alt="WebUI user avatar preview" />
            <div v-else class="tset-avatar-fallback">{{ avatarInitial(draft.username, "U") }}</div>
            <input ref="userAvatarInput" type="file" accept="image/png,image/jpeg,image/gif,image/webp" @change="selectAvatar('user', $event)" />
            <button class="tv-button danger" type="button" @click="clearAvatar('user')">Clear avatar</button>
          </article>
          <article>
            <span>Tater</span>
            <img v-if="taterAvatar" :src="taterAvatar" alt="Tater avatar preview" />
            <div v-else class="tset-avatar-fallback">{{ avatarInitial(draft.tater_first_name, "T") }}</div>
            <input ref="taterAvatarInput" type="file" accept="image/png,image/jpeg,image/gif,image/webp" @change="selectAvatar('tater', $event)" />
            <button class="tv-button danger" type="button" @click="clearAvatar('tater')">Clear avatar</button>
          </article>
        </div>
      </section>
    </div>

    <footer class="tset-save-bar">
      <div>
        <strong>{{ dirty ? "Unsaved changes" : "General settings are synchronized" }}</strong>
        <span>Saving updates every Vue surface from the canonical server response.</span>
      </div>
      <button class="tv-button primary" type="button" :disabled="saving || !dirty" @click="save">
        {{ saving ? "Saving…" : "Save general settings" }}
      </button>
    </footer>
  </section>
</template>
