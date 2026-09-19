<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import type { AppShellAuthRequest, AppShellAuthState } from "./types";

const props = defineProps<{
  state: AppShellAuthState;
  authenticate: (request: AppShellAuthRequest) => Promise<AppShellAuthState>;
}>();
const emit = defineEmits<{ authenticated: [state: AppShellAuthState] }>();

const password = ref("");
const confirmPassword = ref("");
const busy = ref(false);
const error = ref("");
const setupMode = computed(() => !props.state.passwordSet || props.state.mode === "setup");
const userName = computed(() => String(props.state.username || "User").trim() || "User");
const avatarInitial = computed(() => userName.value.match(/[A-Za-z0-9]/)?.[0]?.toUpperCase() || "U");

async function submit() {
  error.value = "";
  if (!password.value) { error.value = "Password is required."; return; }
  if (setupMode.value && password.value.length < 4) { error.value = "Password must be at least 4 characters."; return; }
  if (setupMode.value && password.value !== confirmPassword.value) { error.value = "Passwords do not match."; return; }
  busy.value = true;
  try {
    const next = await props.authenticate({
      password: password.value,
      confirmPassword: confirmPassword.value,
      setup: setupMode.value,
    });
    if (next.required) {
      error.value = next.message || "Authentication failed.";
      return;
    }
    password.value = "";
    confirmPassword.value = "";
    emit("authenticated", next);
  } catch (submitError) {
    error.value = submitError instanceof Error ? submitError.message : "Authentication failed.";
  } finally {
    busy.value = false;
  }
}

watch(() => props.state.required, (locked) => {
  document.body.classList.toggle("webui-auth-locked", Boolean(locked));
  if (locked) document.body.classList.remove("modal-open");
  else error.value = "";
}, { immediate: true });

onBeforeUnmount(() => document.body.classList.remove("webui-auth-locked"));
</script>

<template>
  <Teleport to="body">
    <Transition name="tater-popup" appear>
      <div v-if="state.required" id="webui-auth-modal" class="webui-auth-overlay active" aria-hidden="false">
        <div class="card webui-auth-card" role="dialog" aria-modal="true" aria-label="WebUI Login">
          <div class="webui-auth-avatar-wrap">
            <img v-if="state.userAvatar" class="webui-auth-avatar-img" :src="state.userAvatar" alt="User avatar" />
            <div v-else class="webui-auth-avatar-fallback">{{ avatarInitial }}</div>
          </div>
          <h3 class="card-title">{{ setupMode ? "Create WebUI Password" : "WebUI Login" }}</h3>
          <div class="small">{{ setupMode ? `${userName}, set a password to protect this WebUI.` : `${userName}, enter your password to unlock TaterOS.` }}</div>
          <form class="form-grid webui-auth-form" @submit.prevent="submit">
            <label>Password<input v-model="password" type="password" :autocomplete="setupMode ? 'new-password' : 'current-password'" :disabled="busy" autofocus /></label>
            <label v-if="setupMode">Repeat Password<input v-model="confirmPassword" type="password" autocomplete="new-password" :disabled="busy" /></label>
            <button type="submit" class="action-btn" :disabled="busy">{{ busy ? "Checking…" : setupMode ? "Save Password" : "Login" }}</button>
          </form>
          <div class="small" :class="{ error: Boolean(error) }">{{ error || state.message || (setupMode ? "Create a password and repeat it to save." : "Enter your password to continue.") }}</div>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>
