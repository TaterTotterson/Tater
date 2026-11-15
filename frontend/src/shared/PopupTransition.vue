<script setup lang="ts">
import { onBeforeUnmount, ref, watch } from "vue";
import { animatePopup, cancelPopupMotion } from "./popupMotion";
defineOptions({ inheritAttrs: false });

const props = withDefaults(defineProps<{
  open: boolean;
  backdropClass?: string;
  effect?: string;
}>(), {
  backdropClass: "tv-modal-backdrop",
});

const emit = defineEmits<{ close: [] }>();
const backdrop = ref<HTMLElement | null>(null);
const enter = (element: Element, done: () => void) => animatePopup(element, "enter", done, props.effect);
const leave = (element: Element, done: () => void) => animatePopup(element, "leave", done);

function syncBodyLock() {
  window.requestAnimationFrame(() => {
    const visible = Boolean(document.querySelector(".tater-popup-effect-backdrop"));
    document.body.classList.toggle("modal-open", visible);
  });
}

watch(() => props.open, (open) => {
  if (open) document.body.classList.add("modal-open");
}, { immediate: true });

onBeforeUnmount(() => {
  if (backdrop.value) cancelPopupMotion(backdrop.value);
  syncBodyLock();
});
</script>

<template>
  <Teleport to="body">
    <Transition :css="false" appear @enter="enter" @leave="leave" @enter-cancelled="cancelPopupMotion" @leave-cancelled="cancelPopupMotion" @before-enter="syncBodyLock" @after-leave="syncBodyLock">
      <div
        v-if="open"
        ref="backdrop"
        class="tater-popup-effect-backdrop"
        :class="backdropClass"
        v-bind="$attrs"
        @click.self="emit('close')"
      >
        <slot />
      </div>
    </Transition>
  </Teleport>
</template>
