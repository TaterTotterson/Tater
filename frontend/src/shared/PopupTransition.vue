<script setup lang="ts">
import { onBeforeUnmount, watch } from "vue";

const props = withDefaults(defineProps<{
  open: boolean;
  backdropClass?: string;
}>(), {
  backdropClass: "tv-modal-backdrop",
});

const emit = defineEmits<{ close: [] }>();

function syncBodyLock() {
  window.requestAnimationFrame(() => {
    const visible = Boolean(document.querySelector(".cerb-modal.active, .cerb-modal.closing, .tater-popup-effect-backdrop"));
    document.body.classList.toggle("modal-open", visible);
  });
}

watch(() => props.open, (open) => {
  if (open) document.body.classList.add("modal-open");
}, { immediate: true });

onBeforeUnmount(syncBodyLock);
</script>

<template>
  <Teleport to="body">
    <Transition name="tater-popup" appear @before-enter="syncBodyLock" @after-leave="syncBodyLock">
      <div
        v-if="open"
        class="tater-popup-effect-backdrop"
        :class="backdropClass"
        @click.self="emit('close')"
      >
        <span class="tater-popup-effect-field" aria-hidden="true" />
        <span class="tater-popup-effect-burst" aria-hidden="true" />
        <slot />
      </div>
    </Transition>
  </Teleport>
</template>
