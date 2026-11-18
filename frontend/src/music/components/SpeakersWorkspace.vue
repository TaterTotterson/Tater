<script setup lang="ts">
import { computed, watch } from "vue";
import {
  useCoreDraft,
  cloneFieldValue,
  acknowledgeDraft,
} from "../../cores/drafts";
import DynamicField from "./DynamicField.vue";
import MusicIcon from "./MusicIcon.vue";
import {
  groupPlayerTargets,
  playerFriendlyName,
  playerSecondaryText,
  playerTargetKind,
} from "../playerDisplay";
import type { MusicItem, MusicField, MusicPlayerRow } from "../types";
const props = defineProps<{
  item: MusicItem;
  busy: (key: string) => boolean;
  run: (
    action: string,
    payload: Record<string, unknown>,
    busyKey?: string,
  ) => Promise<boolean>;
}>();
const draft = useCoreDraft("music-speakers:" + props.item.id),
  values = draft.values,
  dirty = draft.dirty;
const rows = computed(() => props.item.player_rows || []);
const sections = computed(() =>
  groupPlayerTargets(rows.value, (row) => row.target),
);
const targets = computed<string[]>(() =>
  Array.isArray(values.targets) ? values.targets.map(String) : [],
);
const selected = computed(() =>
  rows.value.filter((row) => targets.value.includes(row.target)),
);
const speakerCount = computed(() =>
  selected.value.reduce(
    (n, row) =>
      n +
      Number(
        row.speaker_count ||
          (playerTargetKind(row.target) === "stereo" ? 2 : 1),
      ),
    0,
  ),
);
function hydrate() {
  for (const field of props.item.popup_fields || [])
    if (!dirty.has(field.key)) values[field.key] = cloneFieldValue(field.value);
  if (!dirty.has("player_settings"))
    values.player_settings = Object.fromEntries(
      rows.value.map((row) => [
        row.target,
        {
          volume_percent: Number(row.volume_percent ?? 75),
          sync_offset_ms: Number(row.sync_offset_ms ?? 0),
          transport_mode: row.transport_mode || "auto",
        },
      ]),
    );
}
watch([() => props.item.popup_fields, () => props.item.player_rows], hydrate, {
  immediate: true,
});
function setting(target: string) {
  return (
    values.player_settings?.[target] || {
      volume_percent: 75,
      sync_offset_ms: 0,
      transport_mode: "auto",
    }
  );
}
function select(target: string, event: Event) {
  const checked = (event.target as HTMLInputElement).checked;
  values.targets = checked
    ? Array.from(new Set([...targets.value, target]))
    : targets.value.filter((t) => t !== target);
  dirty.add("targets");
}
function update(target: string, key: string, value: unknown) {
  values.player_settings = {
    ...values.player_settings,
    [target]: { ...setting(target), [key]: value },
  };
  dirty.add("player_settings");
}
function updateField(field: MusicField, value: unknown) {
  values[field.key] = value;
  dirty.add(field.key);
}
function name(row: MusicPlayerRow) {
  return playerFriendlyName(row.label, row.target);
}
function meta(row: MusicPlayerRow) {
  return playerSecondaryText(row.label, row.meta, row.target);
}
async function save() {
  if (!props.item.save_action || !targets.value.length) return;
  const submitted = cloneFieldValue(values);
  if (
    await props.run(
      props.item.save_action,
      { id: props.item.id, values: submitted },
      "speakers",
    )
  ) {
    acknowledgeDraft(values, dirty, submitted);
    hydrate();
  }
}
async function testSync() {
  if (props.item.test_sync_action && targets.value.length)
    await props.run(
      props.item.test_sync_action,
      { id: props.item.id, values: cloneFieldValue(values) },
      "sync-test",
    );
}
function reset() {
  dirty.clear();
  hydrate();
}
</script>
<template>
  <section class="tm-speakers-workspace" aria-label="Speakers and players">
    <header class="tm-section-heading">
      <div>
        <h2>Where should music play?</h2>
        <p>
          Select a stereo pair, individual speaker, or several destinations.
        </p>
      </div>
      <span class="tm-secondary">{{ targets.length }} selected</span>
    </header>
    <div v-if="!rows.length" class="tm-empty">
      No speakers are available. Connect a satellite or media player, then
      refresh this workspace.
    </div>
    <section
      v-for="section in sections"
      :key="section.key"
      class="tm-speaker-section"
    >
      <h3>{{ section.label }}</h3>
      <div class="tm-speaker-grid">
        <article
          v-for="row in section.items"
          :key="row.target"
          class="tm-speaker-card"
          :class="{ 'is-selected': targets.includes(row.target) }"
        >
          <label class="tm-speaker-choice"
            ><input
              type="checkbox"
              :checked="targets.includes(row.target)"
              :disabled="busy('speakers')"
              @change="select(row.target, $event)"
            /><MusicIcon
              :name="
                playerTargetKind(row.target) === 'stereo' ? 'music' : 'speaker'
              "
            /><span
              ><strong>{{ name(row) }}</strong
              ><small
                >{{
                  playerTargetKind(row.target) === "stereo"
                    ? "Stereo pair"
                    : ""
                }}{{
                  meta(row)
                    ? (playerTargetKind(row.target) === "stereo" ? " · " : "") +
                      meta(row)
                    : ""
                }}</small
              ></span
            ></label
          >
          <div v-if="row.members?.length" class="tm-pair-members">
            <span v-for="member in row.members" :key="member.selector"
              ><b>{{ member.channel === "left" ? "L" : "R" }}</b
              >{{ member.label }}</span
            >
          </div>
          <div
            v-else-if="playerTargetKind(row.target) === 'stereo'"
            class="tm-pair-members"
          >
            <span><b>L</b>Left speaker</span><span><b>R</b>Right speaker</span>
          </div>
          <div v-if="targets.includes(row.target)" class="tm-speaker-options">
            <label class="tm-speaker-volume"
              ><span>Volume</span
              ><output>{{ setting(row.target).volume_percent }}%</output
              ><input
                type="range"
                min="0"
                max="100"
                :value="setting(row.target).volume_percent"
                :disabled="busy('speakers')"
                :aria-label="name(row) + ' volume'"
                @input="
                  update(
                    row.target,
                    'volume_percent',
                    Number(($event.target as HTMLInputElement).value),
                  )
                "
            /></label>
            <details class="tm-speaker-tuning">
              <summary>Fine-tune synchronization</summary>
              <label class="tm-speaker-offset"
                >Timing offset
                <span
                  ><input
                    type="number"
                    min="-1000"
                    max="1000"
                    step="25"
                    :value="setting(row.target).sync_offset_ms"
                    :disabled="busy('speakers')"
                    :aria-label="name(row) + ' timing offset in milliseconds'"
                    @input="
                      update(
                        row.target,
                        'sync_offset_ms',
                        Number(($event.target as HTMLInputElement).value),
                      )
                    "
                  />
                  ms</span
                ></label
              >
              <fieldset
                v-if="row.transport_options?.length"
                class="tm-route-options"
              >
                <legend>Playback route</legend>
                <button
                  v-for="option in row.transport_options"
                  :key="option.value"
                  type="button"
                  :aria-pressed="
                    setting(row.target).transport_mode === option.value
                  "
                  :disabled="busy('speakers')"
                  @click="update(row.target, 'transport_mode', option.value)"
                >
                  {{ option.label }}
                </button>
              </fieldset>
              <p class="tm-secondary">
                {{
                  row.sync_quality === "precise"
                    ? "Clock-synchronized Tater playback"
                    : row.sync_quality === "automatic"
                      ? "Auto uses AirPlay with Tater satellites and native Sonos otherwise."
                      : "Timing depends on the external player."
                }}
              </p>
              <p
                v-if="playerTargetKind(row.target) === 'stereo'"
                class="tm-secondary"
              >
                Channel balance and left/right delays are available in Voice &
                devices → Stereo pairs.
              </p>
            </details>
          </div>
        </article>
      </div>
    </section>
    <div class="tm-speaker-selection" aria-live="polite">
      <MusicIcon name="speaker" /><span
        ><strong>{{
          selected.length === 1
            ? name(selected[0])
            : targets.length + " destinations"
        }}</strong
        ><small
          >{{ targets.length }} destination{{
            targets.length === 1 ? "" : "s"
          }}
          · {{ speakerCount }} physical speakers</small
        ></span
      ><button
        type="button"
        class="tm-button primary"
        :disabled="!targets.length || busy('speakers')"
        @click="save"
      >
        {{ busy("speakers") ? "Applying…" : "Apply to playback" }}</button
      ><button
        v-if="dirty.size"
        type="button"
        class="tm-text-button"
        :disabled="busy('speakers')"
        @click="reset"
      >
        Reset changes
      </button>
    </div>
    <div class="tm-speaker-test">
      <button
        v-if="item.test_sync_action"
        type="button"
        class="tm-button secondary"
        :disabled="!targets.length || busy('sync-test') || busy('speakers')"
        @click="testSync"
      >
        {{ busy("sync-test") ? "Starting test…" : "Test speaker sync" }}</button
      ><span class="tm-secondary"
        >Stops the current music and plays a short click track.</span
      >
    </div>
    <DynamicField
      v-for="field in (item.popup_fields || []).filter(
        (field) => field.key !== 'targets',
      )"
      :key="field.key"
      :field="field"
      :model-value="values[field.key]"
      @update:model-value="updateField(field, $event)"
    />
  </section>
</template>
