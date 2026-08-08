<script setup lang="ts">
import { computed, ref, watch } from "vue";
import DynamicField from "./DynamicField.vue";
import type { ManagerGroup, MusicField, MusicItem } from "../types";

const props = withDefaults(defineProps<{
  groups: ManagerGroup[];
  items: MusicItem[];
  busy: (key: string) => boolean;
  run: (action: string, payload: Record<string, unknown>, busyKey?: string) => Promise<boolean>;
  selectedGroup?: string;
  showNavigation?: boolean;
}>(), {
  selectedGroup: "",
  showNavigation: true,
});

const emit = defineEmits<{
  "update:selectedGroup": [value: string];
}>();

const activeGroupKey = ref(props.selectedGroup || props.groups[0]?.key || "search");
const pages = ref<Record<string, number>>({});
const searchValues = ref<Record<string, unknown>>({});

watch(
  () => props.groups,
  (groups) => {
    if (!groups.some((group) => group.key === selectedGroup.value)) {
      selectGroup(groups[0]?.key || "search");
    }
  },
  { deep: true },
);

watch(
  () => props.selectedGroup,
  (group) => {
    if (group && group !== activeGroupKey.value) activeGroupKey.value = group;
  },
);

const selectedGroup = computed(() => activeGroupKey.value);
const activeGroup = computed(() => props.groups.find((group) => group.key === selectedGroup.value));
const groupItems = computed(() => {
  const itemGroup = activeGroup.value?.item_group || activeGroup.value?.key;
  return props.items.filter((item) => item.group === itemGroup);
});
const searchItem = computed(() => groupItems.value[0]);
const pageSize = computed(() => Math.max(0, Number(activeGroup.value?.page_size || 0)));
const page = computed(() => Math.max(1, pages.value[selectedGroup.value] || 1));
const pageCount = computed(() =>
  pageSize.value ? Math.max(1, Math.ceil(groupItems.value.length / pageSize.value)) : 1,
);
const visibleItems = computed(() => {
  if (!pageSize.value) return groupItems.value;
  const boundedPage = Math.min(page.value, pageCount.value);
  const start = (boundedPage - 1) * pageSize.value;
  return groupItems.value.slice(start, start + pageSize.value);
});

watch(
  searchItem,
  (item) => {
    if (!item) return;
    const next = { ...searchValues.value };
    for (const field of item.fields || []) {
      if (!(field.key in next)) next[field.key] = field.value;
    }
    searchValues.value = next;
  },
  { immediate: true },
);

function setPage(next: number): void {
  pages.value = {
    ...pages.value,
    [selectedGroup.value]: Math.max(1, Math.min(next, pageCount.value)),
  };
}

function selectGroup(group: string): void {
  activeGroupKey.value = group;
  emit("update:selectedGroup", group);
}

function setSearchValue(field: MusicField, value: unknown): void {
  searchValues.value = { ...searchValues.value, [field.key]: value };
}

async function runSearch(): Promise<void> {
  const item = searchItem.value;
  if (!item?.run_action) return;
  await props.run(item.run_action, { id: item.id, values: searchValues.value }, `item:${item.id}`);
}

async function runItem(item: MusicItem): Promise<void> {
  if (!item.run_action) return;
  await props.run(item.run_action, { id: item.id, values: {} }, `item:${item.id}`);
}
</script>

<template>
  <section class="tm-library">
    <nav v-if="showNavigation" class="tm-subtabs" aria-label="Browse music library">
      <button
        v-for="group in groups"
        :key="group.key"
        type="button"
        :class="{ active: selectedGroup === group.key }"
        @click="selectGroup(group.key)"
      >
        {{ group.label || group.key }}
      </button>
    </nav>

    <form v-if="activeGroup?.key === 'search' && searchItem" class="tm-search" @submit.prevent="runSearch">
      <div>
        <div class="tm-eyebrow">Search across your connected library</div>
        <h3>{{ searchItem.title || 'Find music' }}</h3>
        <p>{{ searchItem.subtitle }}</p>
      </div>
      <div class="tm-search-controls">
        <DynamicField
          v-for="field in searchItem.fields || []"
          :key="field.key"
          :field="field"
          :model-value="searchValues[field.key]"
          compact
          @update:model-value="setSearchValue(field, $event)"
        />
        <button type="submit" class="tm-button primary" :disabled="busy(`item:${searchItem.id}`)">
          {{ searchItem.run_label || 'Play Search' }}
        </button>
      </div>
    </form>

    <template v-else>
      <div v-if="visibleItems.length" class="tm-library-grid">
        <article v-for="item in visibleItems" :key="item.id" class="tm-library-card">
          <div class="tm-library-art">
            <img v-if="item.hero_image_src" :src="item.hero_image_src" :alt="item.hero_image_alt || ''" loading="lazy" />
            <span v-else aria-hidden="true">♫</span>
            <button
              v-if="item.run_action"
              type="button"
              :disabled="busy(`item:${item.id}`)"
              :aria-label="`${item.run_label || 'Play'} ${item.title || ''}`"
              @click="runItem(item)"
            >
              ▶
            </button>
          </div>
          <div class="tm-library-copy">
            <strong :title="item.title">{{ item.title || 'Untitled' }}</strong>
            <small>{{ item.subtitle }}</small>
          </div>
        </article>
      </div>
      <div v-else class="tm-empty">{{ activeGroup?.empty_message || 'Nothing is available here yet.' }}</div>

      <div v-if="pageCount > 1" class="tm-pagination" aria-label="Library pages">
        <button type="button" :disabled="page <= 1" @click="setPage(page - 1)">Previous</button>
        <span>Page {{ Math.min(page, pageCount) }} of {{ pageCount }}</span>
        <button type="button" :disabled="page >= pageCount" @click="setPage(page + 1)">Next</button>
      </div>
    </template>
  </section>
</template>
