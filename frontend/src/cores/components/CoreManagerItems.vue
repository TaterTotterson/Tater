<script setup lang="ts">
import { computed, inject, ref, watch } from "vue";
import type { JsonRow } from "../types";
import CoreManagerItem from "./CoreManagerItem.vue";
import {workspaceKey} from '../drafts';
import {itemSummary,itemImage} from '../workspace';

type ActionRunner = (action: string, payload: JsonRow, busyKey?: string, successText?: string) => Promise<JsonRow | null>;

const props = defineProps<{
  items: JsonRow[];
  ui: JsonRow;
  options?: JsonRow;
  run: ActionRunner;
  busy: (key: string) => boolean;
}>();

const metadata=inject(workspaceKey,ref<JsonRow>({}));
const presentation=computed(()=>String(props.options?.presentation||'stack'));
const isList=computed(()=>Boolean(props.options?.selector)||['list','timeline'].includes(presentation.value));
const editorExpanded=ref(false);
const search=ref('');
const page = ref(1);
const selectedItem = ref("");
const selectedIds = ref<string[]>([]);
const rows = computed(() => Array.isArray(props.items) ? props.items : []);
const pageSize = computed(() => Math.max(0, Math.floor(Number(props.options?.page_size || 0))));
const pageCount = computed(() => pageSize.value > 0 ? Math.max(1, Math.ceil(rows.value.length / pageSize.value)) : 1);
const serverPagination = computed<JsonRow | null>(() => props.options?.server_pagination && typeof props.options.server_pagination === "object" && props.options.server_pagination.enabled ? props.options.server_pagination : null);
const currentPage = computed(() => serverPagination.value ? Math.max(1, Number(serverPagination.value.page || 1)) : page.value);
const currentPageCount = computed(() => serverPagination.value ? Math.max(1, Number(serverPagination.value.page_count || 1)) : pageCount.value);
const pagedRows=computed(()=>{if(serverPagination.value||pageSize.value<=0)return rows.value;return rows.value.slice((page.value-1)*pageSize.value,page.value*pageSize.value);});
const listRows=computed(()=>pagedRows.value.filter(item=>`${item.title||item.id} ${itemSummary(item)}`.toLowerCase().includes(search.value.toLowerCase())));
const visibleRows=computed(()=>isList.value?rows.value.filter(item=>itemId(item)===(selectedItem.value||itemId(rows.value[0]))).slice(0,1):pagedRows.value);
const selectableIds = computed(() => rows.value.filter((item) => item.selectable).map(itemId).filter(Boolean));
const allSelected = computed(() => Boolean(selectableIds.value.length) && selectableIds.value.every((id) => selectedIds.value.includes(id)));
const bulkActions = computed<JsonRow[]>(() => Array.isArray(props.options?.bulk_actions) ? props.options!.bulk_actions.filter((entry: JsonRow) => entry.action) : []);

function text(value: unknown): string { return String(value ?? "").trim(); }
function itemId(item: JsonRow | undefined): string { return text(item?.id); }
function setSelected(id: string, selected: boolean) {
  const next = new Set(selectedIds.value);
  if (selected) next.add(id); else next.delete(id);
  selectedIds.value = [...next];
}
function selectAll(selected: boolean) {
  selectedIds.value = selected ? [...selectableIds.value] : [];
}
async function bulkAction(entry: JsonRow) {
  const minimum = Math.max(1, Number(entry.minimum_selected || 1));
  if (selectedIds.value.length < minimum) return;
  const confirmation = text(entry.confirm);
  if (confirmation && !window.confirm(confirmation)) return;
  const result = await props.run(text(entry.action), { ids: [...selectedIds.value], values: { identity_ids: [...selectedIds.value] } }, `bulk:${entry.action}`, text(entry.success_text) || "Done.");
  if (result) selectedIds.value = [];
}
async function movePage(direction: number) {
  const target = Math.min(currentPageCount.value, Math.max(1, currentPage.value + direction));
  if (target === currentPage.value) return;
  if (serverPagination.value) {
    const action = text(serverPagination.value.action);
    if (!action) return;
    await props.run(action, { page: target, page_size: Math.max(1, Number(serverPagination.value.page_size || pageSize.value || 1)) }, `page:${action}`, `Loaded page ${target}.`);
  } else {page.value = target;if(isList.value)selectedItem.value=itemId(pagedRows.value[0]);}
}

watch(rows, (items) => {
  const ids = new Set(items.map(itemId));
  selectedIds.value = selectedIds.value.filter((id) => ids.has(id));
  if (!ids.has(selectedItem.value)) selectedItem.value = itemId(items[0]);
  if (page.value > pageCount.value) page.value = pageCount.value;
}, { immediate: true });
</script>

<template>
  <div v-if="rows.length" class="tcx-native-items-wrap">
    <div v-if="bulkActions.length" class="core-manager-bulk-toolbar">
      <label class="core-manager-bulk-select-all"><input class="toggle-input" type="checkbox" :checked="allSelected" @change="selectAll(($event.target as HTMLInputElement).checked)" /><span>Select all</span></label><span class="small">{{ selectedIds.length }} selected</span><div class="core-manager-bulk-buttons"><button v-for="entry in bulkActions" :key="entry.action" type="button" :class="entry.tone === 'danger' ? 'inline-btn danger' : 'action-btn'" :disabled="selectedIds.length < Math.max(1, Number(entry.minimum_selected || 1)) || busy(`bulk:${entry.action}`)" @click="bulkAction(entry)">{{ entry.label || 'Apply to selected' }}</button></div>
    </div>

    <div :class="['cw-item-workspace',`cw-view-${presentation}`,{'cw-has-list':isList,'cw-editor-expanded':editorExpanded}]">
      <aside v-if="isList&&!editorExpanded" class="cw-item-list" :aria-label="options?.selector_label||'Workspace items'">
        <label class="cw-list-search"><span>Find an item</span><input v-model="search" type="search" placeholder="Search this page…" /></label>
        <button v-for="item in listRows" :key="itemId(item)" type="button" class="cw-item-list-row" :aria-current="selectedItem===itemId(item)?'true':undefined" @click="selectedItem=itemId(item)"><img v-if="itemImage(item)" :src="itemImage(item)" alt="" loading="lazy" /><span><strong>{{ item.title||item.id }}</strong><small>{{ itemSummary(item) }}</small><span v-if="item.hero_badges?.length" class="cw-list-badges"><span v-for="badge in item.hero_badges" :key="badge.label">{{ badge.label }}</span></span></span><span aria-hidden="true">›</span></button>
        <p v-if="!listRows.length" class="tv-empty compact">No matching entries on this page.</p>
      </aside>
      <div class="cw-item-inspector"><div v-if="isList" class="cw-inspector-toolbar"><span>{{ editorExpanded?'Editing workspace':'Details & settings' }}</span><button class="cw-text-button" type="button" @click="editorExpanded=!editorExpanded">{{ editorExpanded?'Back to list':'Expand editor' }}</button></div><div class="core-tab-items" :class="options?.item_group ? `core-tab-items-group-${String(options.item_group).toLowerCase().replace(/[^a-z0-9_-]/g, '')}` : ''"><CoreManagerItem v-for="item in visibleRows" :key="`${item.group||''}:${itemId(item)}`" :item="item" :ui="ui" :run="run" :busy="busy" :selected="selectedIds.includes(itemId(item))" @select="setSelected(itemId(item),$event)" /></div></div>
    </div>

    <div v-if="currentPageCount > 1" class="inline-row tcx-native-pagination"><button class="action-btn" type="button" :disabled="currentPage <= 1 || busy(`page:${serverPagination?.action || ''}`)" @click="movePage(-1)">Previous</button><span class="small">Page {{ currentPage }} of {{ currentPageCount }}</span><button class="action-btn" type="button" :disabled="currentPage >= currentPageCount || busy(`page:${serverPagination?.action || ''}`)" @click="movePage(1)">Next</button></div>
  </div>
  <div v-else class="tv-empty compact">{{ options?.empty_message || ui.empty_message || 'No entries found.' }}</div>
</template>
