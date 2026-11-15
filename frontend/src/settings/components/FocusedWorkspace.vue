<script setup lang="ts">
import { inject, nextTick, onBeforeUnmount, ref, useId, watch } from "vue";
import SettingsIcon from "./SettingsIcon.vue";
import { settingsWorkspaceKey } from "../focusedWorkspace";

const props = defineProps<{ open: boolean; title: string; eyebrow?: string; description?: string; backLabel: string }>();
const emit = defineEmits<{ close: [] }>();
const host = inject(settingsWorkspaceKey, null);
const id = useId();
const heading = ref<HTMLElement | null>(null);
const surface = ref<HTMLElement | null>(null);
let returnFocus: HTMLElement | null = null;
let scrollPositions: { element: Element; top: number; left: number }[] = [];

function release() {
  if (host?.active.value === id) { host.active.value = ""; host.close.value = null; }
}

watch(() => props.open, async (open) => {
  if (open) {
    returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    scrollPositions = [];
    for (let element: Element | null = returnFocus; element; element = element.parentElement) {
      if (element.scrollHeight > element.clientHeight || element.scrollWidth > element.clientWidth) {
        scrollPositions.push({ element, top: element.scrollTop, left: element.scrollLeft });
      }
    }
    if (host) { host.active.value = id; host.close.value = () => emit("close"); }
    await nextTick();
    if (!props.open) return;
    surface.value?.scrollIntoView({ block: "start", behavior: "instant" });
    heading.value?.focus({ preventScroll: true });
  } else {
    release();
    await nextTick();
    if (props.open) return;
    scrollPositions.forEach(({ element, top, left }) => element.scrollTo({ top, left, behavior: "instant" }));
    if (returnFocus?.isConnected) returnFocus.focus({ preventScroll: true });
  }
}, { flush: "post", immediate: true });

onBeforeUnmount(release);
</script>

<template>
  <Teleport :to="host?.target.value || 'body'" :disabled="!host?.target.value">
    <section v-if="open" ref="surface" class="tset-focused-workspace" :aria-labelledby="`${id}-title`" @keydown.esc.stop.prevent="emit('close')">
      <button class="tset-workspace-back" type="button" @click="emit('close')"><SettingsIcon name="back" />{{ backLabel }}</button>
      <header class="tset-workspace-heading">
        <div><span v-if="eyebrow" class="tv-eyebrow">{{ eyebrow }}</span><h1 :id="`${id}-title`" ref="heading" tabindex="-1">{{ title }}</h1><p v-if="description">{{ description }}</p></div>
        <slot name="status" />
      </header>
      <slot />
      <footer v-if="$slots.footer" class="tset-workspace-footer"><slot name="footer" /></footer>
    </section>
  </Teleport>
</template>
