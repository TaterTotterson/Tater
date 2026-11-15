<script setup lang="ts">
import UiIcon from '../shared/UiIcon.vue';
import { statusLabel, isActive } from './workspace';
import type { JsonRow } from './types';
defineProps<{ sessions: JsonRow[]; selectedId: string; all: boolean; projectName: string }>();
const emit=defineEmits<{select:[id:string];toggle:[];close:[session:JsonRow]}>();
</script>
<template>
  <section class="txw-task-nav" aria-label="Terminal tasks">
    <header><span>Recent tasks</span><button type="button" :aria-pressed="all" @click="emit('toggle')">{{ all ? 'This project' : 'All sessions' }}</button></header>
    <p v-if="!sessions.length" class="txw-muted">Start a task in {{ projectName }}.</p>
    <div v-for="session in sessions" :key="session.id" class="txw-task-row" :class="{selected:selectedId===String(session.id)}">
      <button type="button" class="txw-task-select" :aria-current="selectedId===String(session.id) ? 'page' : undefined" @click="emit('select',String(session.id))"><span>{{ session.label || session.goal || session.command || 'Terminal session' }}</span><small><i v-if="isActive(session)" class="txw-live-dot" />{{ statusLabel(session.status) }}<template v-if="all && session.project?.name"> · {{ session.project.name }}</template></small></button>
      <button type="button" class="txw-task-close" :aria-label="`Close ${session.label || 'session'}`" @click="emit('close',session)"><UiIcon name="close" /></button>
    </div>
  </section>
</template>
