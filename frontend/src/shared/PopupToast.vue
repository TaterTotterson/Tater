<script setup lang="ts">
import { onBeforeUnmount, ref } from "vue";
import { animatePopup, cancelPopupMotion } from "./popupMotion";

const props = withDefaults(defineProps<{ open: boolean; message: string; tone?: string; effect?: string }>(), { tone: "success" });
const emit = defineEmits<{ close: []; dismissed: [] }>();
const element = ref<HTMLElement | null>(null);
const enter = (el: Element, done: () => void) => animatePopup(el, "enter", done, props.effect, "toast");
const leave = (el: Element, done: () => void) => animatePopup(el, "leave", done, undefined, "toast");
onBeforeUnmount(() => { if (element.value) cancelPopupMotion(element.value); });
</script>

<template>
  <Transition :css="false" appear @enter="enter" @leave="leave" @enter-cancelled="cancelPopupMotion" @leave-cancelled="cancelPopupMotion" @after-leave="emit('dismissed')">
    <button v-if="open" ref="element" type="button" class="toast-item" :class="tone" :aria-label="`${message} (dismiss notification)`" @click="emit('close')">{{ message }}</button>
  </Transition>
</template>
