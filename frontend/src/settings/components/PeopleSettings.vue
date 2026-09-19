<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, reactive, ref, watch } from "vue";
import { getJson, postJson } from "../../shared/api";
import PopupTransition from "../../shared/PopupTransition.vue";
import type { JsonRow, PeopleSettingsPayload } from "../types";

interface FaceUpload {
  filename: string;
  content_type: string;
  data_b64: string;
}

const props = withDefaults(defineProps<{
  payload: PeopleSettingsPayload;
  endpoint: string;
  actionEndpoint: string;
  initialTab?: string;
  initialSort?: string;
}>(), {
  initialTab: "people",
  initialSort: "recent",
});

const emit = defineEmits<{
  changed: [payload: PeopleSettingsPayload];
  notify: [message: string, tone?: string];
  tabChange: [tab: string];
  sortChange: [sort: string];
}>();

const data = ref<PeopleSettingsPayload>(props.payload || {});
const activeTab = ref(normalizeTab(props.initialTab));
const sortMode = ref(normalizeSort(props.initialSort));
const loading = ref(false);
const busy = ref("");
const error = ref("");
const notice = ref("");
const createName = ref("");
const selectedPersonId = ref("");
const selectedFaceId = ref("");
const selectedObservations = ref<string[]>([]);
const galleryTarget = ref("");
const enrollmentOpen = ref(false);
const enrollmentPersonId = ref("");
const enrollmentUpload = ref<FaceUpload | null>(null);
const enrollmentPreview = ref("");
const cameraActive = ref(false);
const cameraVideo = ref<HTMLVideoElement | null>(null);
let cameraStream: MediaStream | null = null;

const personDraft = reactive({ display_name: "", is_admin: false, instructions: "" });
const faceDrafts = reactive<Record<string, { person_id: string; name: string; merge_target: string }>>({});
const identitySelections = reactive<Record<string, string>>({});

const people = computed<JsonRow[]>(() => Array.isArray(data.value.people) ? data.value.people : []);
const identities = computed<JsonRow[]>(() => Array.isArray(data.value.identities) ? data.value.identities : []);
const faces = computed<JsonRow[]>(() => Array.isArray(data.value.faces) ? data.value.faces : []);
const summaryMetrics = computed<JsonRow[]>(() => Array.isArray(data.value.summary_metrics) ? data.value.summary_metrics : []);
const faceStatus = computed<JsonRow>(() => data.value.face_id && typeof data.value.face_id === "object" ? data.value.face_id : {});
const selectedPerson = computed(() => people.value.find((person) => String(person.id || "") === selectedPersonId.value) || null);
const selectedFace = computed(() => faces.value.find((face) => String(face.id || "") === selectedFaceId.value) || null);
const sortedPeople = computed(() => [...people.value].sort((left, right) => {
  const nameOrder = String(left.display_name || "").localeCompare(String(right.display_name || ""), undefined, { sensitivity: "base" });
  const leftFace = faceContext(left);
  const rightFace = faceContext(right);
  const seenOrder = timeRank(rightFace.last_seen) - timeRank(leftFace.last_seen);
  const faceOrder = Number(Boolean(rightFace.linked)) - Number(Boolean(leftFace.linked));
  if (sortMode.value === "name") return nameOrder;
  if (sortMode.value === "face") return faceOrder || seenOrder || nameOrder;
  return seenOrder || nameOrder;
}));
const editableFaces = computed(() => faces.value.map((face) => ({ face, draft: faceDraft(face) })));

function normalizeTab(value: unknown): "people" | "faces" | "identities" {
  const token = String(value || "").trim();
  return token === "faces" || token === "identities" ? token : "people";
}

function normalizeSort(value: unknown): "recent" | "name" | "face" {
  const token = String(value || "").trim();
  return token === "name" || token === "face" ? token : "recent";
}

function setTab(value: unknown) {
  activeTab.value = normalizeTab(value);
  emit("tabChange", activeTab.value);
}

function setSort() {
  sortMode.value = normalizeSort(sortMode.value);
  emit("sortChange", sortMode.value);
}

function identityKey(identity: JsonRow): string {
  return `${String(identity.platform || "unknown")}:${String(identity.external_id || "")}`;
}

function faceContext(person: JsonRow): JsonRow {
  return person.face_id && typeof person.face_id === "object" ? person.face_id : {};
}

function aliases(person: JsonRow | null): JsonRow[] {
  return person && Array.isArray(person.aliases) ? person.aliases : [];
}

function gallery(face: JsonRow | null): JsonRow[] {
  return face && Array.isArray(face.gallery) ? face.gallery : [];
}

function initial(value: unknown, fallback = "P"): string {
  return String(value || "").trim().charAt(0).toUpperCase() || fallback;
}

function platformLabel(value: unknown): string {
  const token = String(value || "").trim();
  const labels: Record<string, string> = {
    webui: "WebUI",
    little_spud: "Little Spud",
    homekit: "HomeKit",
    macos: "macOS",
    xbmc: "Kodi",
  };
  return labels[token] || token.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase()) || "Identity";
}

function timeRank(value: unknown): number {
  if (typeof value === "number") return Number.isFinite(value) ? value * (value < 1e12 ? 1000 : 1) : 0;
  const numeric = Number(value);
  if (String(value || "").trim() && Number.isFinite(numeric)) return numeric * (numeric < 1e12 ? 1000 : 1);
  const parsed = Date.parse(String(value || ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function timeLabel(value: unknown): string {
  const timestamp = timeRank(value);
  if (!timestamp) return "Not seen yet";
  const date = new Date(timestamp);
  const now = new Date();
  const dayOffset = Math.round((new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime() - new Date(date.getFullYear(), date.getMonth(), date.getDate()).getTime()) / 86400000);
  const time = date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  if (dayOffset === 0) return `Today at ${time}`;
  if (dayOffset === 1) return `Yesterday at ${time}`;
  return date.toLocaleString([], { month: "short", day: "numeric", year: date.getFullYear() === now.getFullYear() ? undefined : "numeric", hour: "numeric", minute: "2-digit" });
}

function syncDrafts() {
  const faceIds = new Set<string>();
  faces.value.forEach((face) => {
    const id = String(face.id || "");
    if (!id) return;
    faceIds.add(id);
    faceDrafts[id] = {
      person_id: String(face.person_id || ""),
      name: String(face.local_name || ""),
      merge_target: "",
    };
  });
  Object.keys(faceDrafts).forEach((id) => { if (!faceIds.has(id)) delete faceDrafts[id]; });

  const identityIds = new Set<string>();
  identities.value.forEach((identity) => {
    const key = identityKey(identity);
    identityIds.add(key);
    identitySelections[key] = String(identity.person_id || "");
  });
  Object.keys(identitySelections).forEach((key) => { if (!identityIds.has(key)) delete identitySelections[key]; });

  if (selectedPersonId.value && !people.value.some((person) => String(person.id || "") === selectedPersonId.value)) closePerson();
  if (selectedFaceId.value && !faces.value.some((face) => String(face.id || "") === selectedFaceId.value)) closeGallery();
}

function faceDraft(face: JsonRow) {
  const id = String(face.id || "");
  if (!faceDrafts[id]) {
    faceDrafts[id] = { person_id: String(face.person_id || ""), name: String(face.local_name || ""), merge_target: "" };
  }
  return faceDrafts[id];
}

function applyPayload(next: PeopleSettingsPayload) {
  data.value = next || {};
  syncDrafts();
  emit("changed", data.value);
}

async function refresh() {
  loading.value = true;
  error.value = "";
  try {
    applyPayload(await getJson<PeopleSettingsPayload>(props.endpoint));
  } catch (refreshError) {
    error.value = refreshError instanceof Error ? refreshError.message : "People could not be loaded.";
  } finally {
    loading.value = false;
  }
}

async function runAction(action: string, payload: JsonRow, fallback: string): Promise<boolean> {
  if (busy.value) return false;
  busy.value = action;
  error.value = "";
  notice.value = "";
  try {
    const result = await postJson<JsonRow>(props.actionEndpoint, { action, payload });
    if (result.people && typeof result.people === "object") applyPayload(result.people as PeopleSettingsPayload);
    notice.value = String(result.message || fallback).trim() || fallback;
    emit("notify", notice.value, "success");
    return true;
  } catch (actionError) {
    error.value = actionError instanceof Error ? actionError.message : "People action failed.";
    emit("notify", error.value, "error");
    return false;
  } finally {
    busy.value = "";
  }
}

async function createPerson() {
  const name = createName.value.trim();
  if (!name) {
    error.value = "Enter a display name.";
    return;
  }
  if (await runAction("people_create", { values: { display_name: name } }, "Person created.")) createName.value = "";
}

function openPerson(person: JsonRow) {
  selectedPersonId.value = String(person.id || "");
  Object.assign(personDraft, {
    display_name: String(person.display_name || "Person"),
    is_admin: Boolean(person.is_admin),
    instructions: String(person.instructions || ""),
  });
}

function closePerson() {
  selectedPersonId.value = "";
}

async function savePerson() {
  if (!selectedPersonId.value) return;
  const saved = await runAction("people_save", {
    person_id: selectedPersonId.value,
    values: { ...personDraft },
  }, "Person saved.");
  if (saved && selectedPerson.value) openPerson(selectedPerson.value);
}

async function deletePerson() {
  const person = selectedPerson.value;
  if (!person || !window.confirm(`Delete ${String(person.display_name || "this person")}? Identity links will be removed.`)) return;
  if (await runAction("people_delete", { person_id: String(person.id || "") }, "Person deleted.")) closePerson();
}

async function detachAlias(alias: JsonRow) {
  if (!selectedPersonId.value) return;
  await runAction("people_alias_detach", {
    person_id: selectedPersonId.value,
    platform: String(alias.platform || ""),
    external_id: String(alias.external_id || ""),
  }, "Identity unlinked.");
}

async function linkIdentity(identity: JsonRow) {
  const personId = identitySelections[identityKey(identity)] || "";
  if (!personId) {
    error.value = "Choose a person for this identity.";
    return;
  }
  await runAction("people_alias_attach", {
    person_id: personId,
    platform: String(identity.platform || ""),
    external_id: String(identity.external_id || ""),
    label: String(identity.label || ""),
    kind: String(identity.kind || "user"),
  }, "Identity linked.");
}

async function forgetIdentity(identity: JsonRow) {
  const label = String(identity.label || identity.external_id || "this identity");
  if (!window.confirm(`Forget ${platformLabel(identity.platform)}: ${label}? Matching discovered chat and memory will be removed.`)) return;
  await runAction("people_identity_forget", {
    platform: String(identity.platform || ""),
    external_id: String(identity.external_id || ""),
  }, "Identity forgotten.");
}

async function saveFace(face: JsonRow, draft: { person_id: string; name: string }) {
  await runAction("people_face_save", {
    identity_id: String(face.id || ""),
    values: { person_id: draft.person_id, name: draft.name },
  }, "Face identity saved.");
}

async function mergeFace(face: JsonRow, draft: { merge_target: string }) {
  if (!draft.merge_target || !window.confirm("Merge this entire face profile into the selected profile?")) return;
  await runAction("people_face_merge", {
    identity_id: String(face.id || ""),
    values: { target_identity_id: draft.merge_target },
  }, "Face profiles merged.");
}

async function deleteFace(face: JsonRow) {
  if (!window.confirm("Remove this Face ID profile and all of its saved captures?")) return;
  await runAction("people_face_delete", { identity_id: String(face.id || "") }, "Face identity removed.");
}

function openGallery(face: JsonRow) {
  selectedFaceId.value = String(face.id || "");
  selectedObservations.value = [];
  galleryTarget.value = "";
}

function closeGallery() {
  selectedFaceId.value = "";
  selectedObservations.value = [];
  galleryTarget.value = "";
}

function toggleObservation(id: unknown) {
  const token = String(id || "");
  const next = new Set(selectedObservations.value);
  if (next.has(token)) next.delete(token);
  else next.add(token);
  selectedObservations.value = [...next];
}

async function moveObservations() {
  if (!selectedFace.value || !selectedObservations.value.length || !galleryTarget.value) {
    error.value = "Select at least one image and choose a destination.";
    return;
  }
  const completed = await runAction("people_face_move_images", {
    identity_id: String(selectedFace.value.id || ""),
    values: { target_identity_id: galleryTarget.value, observation_ids: selectedObservations.value },
  }, "Face images moved.");
  if (completed) selectedObservations.value = [];
}

async function removeObservations() {
  if (!selectedFace.value || !selectedObservations.value.length) return;
  if (!window.confirm(`Permanently delete ${selectedObservations.value.length} selected face image${selectedObservations.value.length === 1 ? "" : "s"}?`)) return;
  const completed = await runAction("people_face_remove_images", {
    identity_id: String(selectedFace.value.id || ""),
    values: { observation_ids: selectedObservations.value },
  }, "Face images permanently deleted.");
  if (completed) selectedObservations.value = [];
}

function openEnrollment() {
  enrollmentOpen.value = true;
  enrollmentPersonId.value = "";
  enrollmentUpload.value = null;
  enrollmentPreview.value = "";
  error.value = "";
}

function stopCamera() {
  cameraStream?.getTracks().forEach((track) => track.stop());
  cameraStream = null;
  cameraActive.value = false;
  if (cameraVideo.value) cameraVideo.value.srcObject = null;
}

function closeEnrollment() {
  stopCamera();
  enrollmentOpen.value = false;
}

async function fileToUpload(file: File): Promise<FaceUpload> {
  if (file.size > 8 * 1024 * 1024) throw new Error("The face photo must be 8 MB or smaller.");
  const dataUrl = await new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("The selected face photo could not be read."));
    reader.readAsDataURL(file);
  });
  const comma = dataUrl.indexOf(",");
  if (comma < 0) throw new Error("The selected face photo could not be read.");
  enrollmentPreview.value = dataUrl;
  return { filename: file.name || "face-photo.jpg", content_type: file.type || "image/jpeg", data_b64: dataUrl.slice(comma + 1) };
}

async function chooseEnrollmentFile(event: Event) {
  const file = (event.target as HTMLInputElement).files?.[0];
  if (!file) return;
  try {
    enrollmentUpload.value = await fileToUpload(file);
  } catch (fileError) {
    error.value = fileError instanceof Error ? fileError.message : "The face photo could not be read.";
  }
}

async function startCamera() {
  if (!navigator.mediaDevices?.getUserMedia) {
    error.value = "Camera capture requires HTTPS or localhost and a supported browser.";
    return;
  }
  try {
    stopCamera();
    cameraStream = await navigator.mediaDevices.getUserMedia({ video: { facingMode: "user" }, audio: false });
    cameraActive.value = true;
    await nextTick();
    if (cameraVideo.value) {
      cameraVideo.value.srcObject = cameraStream;
      await cameraVideo.value.play();
    }
  } catch (cameraError) {
    error.value = cameraError instanceof Error ? cameraError.message : "Camera access failed.";
    stopCamera();
  }
}

function captureCamera() {
  const video = cameraVideo.value;
  if (!video || !video.videoWidth || !video.videoHeight) {
    error.value = "Wait for the camera preview before taking the photo.";
    return;
  }
  const canvas = document.createElement("canvas");
  canvas.width = video.videoWidth;
  canvas.height = video.videoHeight;
  canvas.getContext("2d")?.drawImage(video, 0, 0);
  const dataUrl = canvas.toDataURL("image/jpeg", 0.9);
  enrollmentPreview.value = dataUrl;
  enrollmentUpload.value = { filename: "camera-face.jpg", content_type: "image/jpeg", data_b64: dataUrl.split(",", 2)[1] || "" };
  stopCamera();
}

async function enrollFace() {
  if (!enrollmentPersonId.value) {
    error.value = "Choose a person for this face.";
    return;
  }
  if (!enrollmentUpload.value) {
    error.value = "Choose a face photo or take one with the camera first.";
    return;
  }
  const completed = await runAction("people_face_enroll", {
    values: { person_id: enrollmentPersonId.value, face_image: enrollmentUpload.value },
  }, "Face added to person.");
  if (completed) closeEnrollment();
}

watch(() => props.payload, (payload) => {
  data.value = payload || {};
  syncDrafts();
}, { immediate: true });

onBeforeUnmount(stopCamera);
</script>

<template>
  <section class="tset-resource tpeople">
    <div v-if="notice || error" class="tv-notice" :class="{ error: Boolean(error) }" aria-live="polite">{{ error || notice }}</div>

    <section class="tv-panel tpeople-overview">
      <header>
        <div><span class="tv-eyebrow">Identity directory</span><h2>Everyone Tater knows</h2><p>Bring faces, portal users, and voice speakers together as the same person.</p></div>
        <button class="tv-button" type="button" :disabled="loading" @click="refresh">{{ loading ? "Refreshing…" : "Refresh" }}</button>
      </header>
      <div class="tv-metrics tpeople-metrics"><div v-for="metric in summaryMetrics" :key="metric.label"><span>{{ metric.label }}</span><strong>{{ Number(metric.value || 0) }}</strong></div></div>
    </section>

    <nav class="tv-tabs tpeople-tabs" aria-label="People sections">
      <button type="button" :class="{ active: activeTab === 'people' }" @click="setTab('people')">People</button>
      <button type="button" :class="{ active: activeTab === 'faces' }" @click="setTab('faces')">Faces</button>
      <button type="button" :class="{ active: activeTab === 'identities' }" @click="setTab('identities')">Identities</button>
    </nav>

    <template v-if="activeTab === 'people'">
      <section class="tv-panel tset-form-card">
        <header class="tpeople-section-head"><div><span class="tv-eyebrow">Directory</span><h2>People</h2><p>Open a card to manage access, instructions, and linked identities.</p></div><label>Sort<select v-model="sortMode" @change="setSort"><option value="recent">Recently seen</option><option value="name">Name</option><option value="face">Face ID linked</option></select></label></header>
        <details class="people-create-panel"><summary>Add a person</summary><div class="people-create-body"><label class="people-field">Display name<input v-model="createName" type="text" placeholder="Fred" @keydown.enter.prevent="createPerson" /></label><button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="createPerson">Create person</button></div></details>
      </section>

      <div v-if="sortedPeople.length" class="people-directory-grid">
        <article v-for="person in sortedPeople" :key="person.id" class="card people-person-card">
          <div class="people-person-card-main tpeople-card-button" role="button" tabindex="0" @click="openPerson(person)" @keydown.enter.prevent="openPerson(person)" @keydown.space.prevent="openPerson(person)">
            <div class="people-person-avatar" :class="{ 'has-image': Boolean(faceContext(person).image_src) }"><img v-if="faceContext(person).image_src" :src="faceContext(person).image_src" :alt="`${person.display_name} Face ID profile`" loading="lazy" /><span v-else>{{ initial(person.display_name) }}</span></div>
            <div class="people-person-card-copy"><div class="card-head people-person-card-head"><div><h3 class="card-title">{{ person.display_name || "Person" }}</h3><div class="people-person-badges"><span class="people-badge" :class="faceContext(person).linked ? 'face' : 'muted'">{{ faceContext(person).linked ? "Face ID" : "No Face ID" }}</span><span v-if="person.is_admin" class="people-badge admin">Admin</span></div></div></div><div class="people-last-seen" :class="{ 'is-seen': Boolean(faceContext(person).last_seen) }"><span class="people-presence-dot" /><span>{{ faceContext(person).last_seen_camera ? `Last seen at ${faceContext(person).last_seen_camera} · ` : "" }}{{ timeLabel(faceContext(person).last_seen) }}</span></div><div class="people-person-stats"><span><strong>{{ aliases(person).length }}</strong> linked identities</span><span><strong>{{ Number(faceContext(person).identity_count || 0) }}</strong> face profiles</span><span><strong>{{ Number(faceContext(person).capture_count || 0) }}</strong> captures</span></div></div>
          </div>
          <div class="people-person-card-footer"><button class="people-person-open" type="button" @click="openPerson(person)">Manage person</button></div>
        </article>
      </div>
      <p v-else class="tv-empty">No people yet. Create a person, then link discovered identities.</p>
    </template>

    <template v-else-if="activeTab === 'identities'">
      <section class="tv-panel tset-form-card"><header><span class="tv-eyebrow">Discovered accounts</span><h2>Identities</h2><p>Link portal, WebUI, voice, and memory identities to the correct person.</p></header>
        <div v-if="identities.length" class="people-identity-list"><article v-for="identity in identities" :key="identityKey(identity)" class="people-identity-card"><span class="people-platform-mark">{{ initial(platformLabel(identity.platform), 'I') }}</span><div class="people-identity-copy"><div class="people-identity-title-row"><strong>{{ identity.label || identity.external_id }}</strong><span class="people-badge" :class="identity.person_id ? 'linked' : 'muted'">{{ identity.person_id ? `Linked to ${identity.person_name}` : "Unlinked" }}</span></div><span>{{ [platformLabel(identity.platform), String(identity.kind || 'user').replaceAll('_', ' '), identity.source, Number(identity.fact_count || 0) ? `${Number(identity.fact_count)} facts` : ''].filter(Boolean).join(' · ') }}</span><small>{{ identity.external_id }}</small></div><div class="people-identity-actions"><select v-model="identitySelections[identityKey(identity)]"><option value="">Choose person…</option><option v-for="person in people" :key="person.id" :value="person.id">{{ person.display_name }}</option></select><button class="tv-button" type="button" :disabled="Boolean(busy) || !identitySelections[identityKey(identity)]" @click="linkIdentity(identity)">{{ identity.person_id ? "Update link" : "Link" }}</button><button v-if="identity.forgettable && !identity.person_id" class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="forgetIdentity(identity)">Forget</button></div></article></div>
        <p v-else class="tv-empty">No portal or voice identities have been discovered yet.</p>
      </section>
    </template>

    <template v-else>
      <section class="tv-panel tset-form-card"><header class="tpeople-section-head"><div><span class="tv-eyebrow">Face ID</span><h2>Known faces</h2><p>Link faces to people, review captures, and merge duplicates.</p></div><div class="tpeople-head-actions"><span class="people-badge" :class="faceStatus.loaded ? 'linked' : 'muted'">{{ faceStatus.loaded ? "Model ready" : "Model not ready" }}</span><button class="tv-button primary" type="button" :disabled="!people.length" @click="openEnrollment">Add a face</button></div></header></section>
      <div v-if="editableFaces.length" class="people-face-grid"><article v-for="entry in editableFaces" :key="entry.face.id" class="people-face-card"><div class="people-face-summary"><div class="people-face-avatar"><img v-if="entry.face.image_src" :src="entry.face.image_src" :alt="entry.face.name || 'Unknown face'" loading="lazy" /><span v-else>{{ initial(entry.face.name, '?') }}</span></div><div class="people-face-copy"><div class="people-identity-title-row"><strong>{{ entry.face.name || `Unknown face · ${String(entry.face.id).slice(-6)}` }}</strong><span class="people-badge" :class="entry.face.person_id ? 'linked' : 'muted'">{{ entry.face.person_id ? `Linked to ${entry.face.person_name}` : "Not linked" }}</span></div><span>{{ Number(entry.face.capture_count || 0) }} captures · {{ Number(entry.face.event_count || 0) }} events</span><small>{{ timeLabel(entry.face.last_seen) }}</small></div></div><div class="people-face-fields"><label class="people-field">Person<select v-model="entry.draft.person_id"><option value="">Not linked</option><option v-for="person in people" :key="person.id" :value="person.id">{{ person.display_name }}</option></select></label><label class="people-field">Face name<input v-model="entry.draft.name" type="text" maxlength="80" placeholder="Used when not linked" /></label><button class="tv-button" type="button" :disabled="Boolean(busy)" @click="saveFace(entry.face, entry.draft)">Save</button></div><button v-if="gallery(entry.face).length" class="people-face-review-trigger" type="button" @click="openGallery(entry.face)"><span class="people-face-review-trigger-copy"><small>Saved face images</small><strong>Review {{ gallery(entry.face).length }} images</strong></span><span class="people-face-review-trigger-action">Open gallery →</span></button><div class="people-face-footer"><select v-model="entry.draft.merge_target"><option value="">Merge profile into…</option><option v-for="candidate in faces.filter((candidate) => candidate.id !== entry.face.id)" :key="candidate.id" :value="candidate.id">{{ candidate.name || `Unknown face · ${String(candidate.id).slice(-6)}` }}</option></select><button class="tv-button" type="button" :disabled="Boolean(busy) || !entry.draft.merge_target" @click="mergeFace(entry.face, entry.draft)">Merge</button><button class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="deleteFace(entry.face)">Remove profile</button></div></article></div>
      <p v-else class="tv-empty">{{ faceStatus.enabled ? "No faces have been added yet." : "Face ID is disabled. Enable it under Models › Face ID." }}</p>
    </template>

    <PopupTransition :open="Boolean(selectedPerson)" backdrop-class="tv-modal-backdrop tpeople tset-modal" @close="closePerson">
      <section v-if="selectedPerson" class="tv-modal people-person-dialog" role="dialog" aria-modal="true" aria-labelledby="people-person-dialog-title"><header><div><span class="tv-eyebrow">Tater person</span><h2 id="people-person-dialog-title">{{ selectedPerson.display_name }}</h2></div><button class="tv-button" type="button" @click="closePerson">Close</button></header><div class="people-person-manage-body"><div class="people-edit-grid"><label class="people-field">Display name<input v-model="personDraft.display_name" type="text" /></label><label class="tv-toggle people-admin-toggle"><input v-model="personDraft.is_admin" class="tv-checkbox" type="checkbox" /><span><strong>Admin access</strong><small>Allow admin-only tools from linked identities.</small></span></label></div><label class="people-field people-instructions-field">Response instructions<textarea v-model="personDraft.instructions" rows="4" placeholder="Always call this person sir." /><small>Used only when Tater resolves the current user to this person.</small></label><div class="people-person-actions"><button class="tv-button primary" type="button" :disabled="Boolean(busy)" @click="savePerson">Save person</button><button class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="deletePerson">Delete</button></div><section class="people-linked-section"><div class="people-section-label">Linked identities</div><div v-if="aliases(selectedPerson).length" class="people-alias-list"><div v-for="alias in aliases(selectedPerson)" :key="identityKey(alias)" class="people-alias-row"><span class="people-platform-mark">{{ initial(platformLabel(alias.platform), 'I') }}</span><div class="people-alias-copy"><strong>{{ alias.label || alias.external_id }}</strong><span>{{ platformLabel(alias.platform) }} · {{ String(alias.kind || 'user').replaceAll('_', ' ') }}</span><small>{{ alias.external_id }}</small></div><button class="tv-button danger" type="button" :disabled="Boolean(busy)" @click="detachAlias(alias)">Unlink</button></div></div><p v-else class="people-empty-inline">No identities linked yet.</p></section></div></section>
    </PopupTransition>

    <PopupTransition :open="Boolean(selectedFace)" backdrop-class="tv-modal-backdrop tpeople tset-modal" @close="closeGallery">
      <section v-if="selectedFace" class="tv-modal people-face-review-dialog" role="dialog" aria-modal="true" aria-labelledby="people-face-review-title"><header><div><span class="tv-eyebrow">Face ID gallery</span><h2 id="people-face-review-title">{{ selectedFace.name || "Unknown face" }}</h2><p>{{ gallery(selectedFace).length }} saved images · select the captures you want to organize.</p></div><button class="tv-button" type="button" @click="closeGallery">Close</button></header><div class="people-face-review-toolbar"><div><strong>Choose saved images</strong><span>Selected images can be moved or permanently deleted.</span></div><div class="people-face-review-selection-tools"><span class="people-face-selection-count">{{ selectedObservations.length }} selected</span><button class="tv-button" type="button" :disabled="selectedObservations.length === gallery(selectedFace).length" @click="selectedObservations = gallery(selectedFace).map((row) => String(row.id))">Select all</button><button class="tv-button" type="button" :disabled="!selectedObservations.length" @click="selectedObservations = []">Clear</button></div></div><div class="people-face-gallery"><button v-for="observation in gallery(selectedFace)" :key="observation.id" class="people-face-capture" :class="{ 'is-selected': selectedObservations.includes(String(observation.id)) }" type="button" :aria-pressed="selectedObservations.includes(String(observation.id))" @click="toggleObservation(observation.id)"><img :src="observation.image_src" :alt="`Face captured ${timeLabel(observation.seen_at)}`" loading="lazy" /><span class="people-face-capture-time">{{ timeLabel(observation.seen_at) }}</span><span class="people-face-selection-mark">✓</span></button></div><div class="people-face-review-actions"><label class="people-face-destination">Move selected images to<select v-model="galleryTarget"><option value="">Choose face profile…</option><option v-for="face in faces.filter((candidate) => candidate.id !== selectedFace?.id)" :key="face.id" :value="face.id">{{ face.name || `Unknown face · ${String(face.id).slice(-6)}` }}</option><option value="__new_unknown__">New unknown face</option></select></label><button class="tv-button primary" type="button" :disabled="Boolean(busy) || !selectedObservations.length || !galleryTarget" @click="moveObservations">Move selected</button><button class="tv-button danger" type="button" :disabled="Boolean(busy) || !selectedObservations.length" @click="removeObservations">Permanently delete</button></div></section>
    </PopupTransition>

    <PopupTransition :open="enrollmentOpen" backdrop-class="tv-modal-backdrop tpeople tset-modal" @close="closeEnrollment">
      <section v-if="enrollmentOpen" class="tv-modal people-face-enroll-dialog" role="dialog" aria-modal="true" aria-labelledby="people-face-enroll-title"><header><div><span class="tv-eyebrow">Face ID enrollment</span><h2 id="people-face-enroll-title">Add a face</h2><p>Choose a person and provide one clear, front-facing photo.</p></div><button class="tv-button" type="button" @click="closeEnrollment">Close</button></header><div class="people-face-enroll-form"><label class="people-field">Person<select v-model="enrollmentPersonId"><option value="">Choose a person</option><option v-for="person in people" :key="person.id" :value="person.id">{{ person.display_name }}</option></select></label><label class="people-field">Face photo<input type="file" accept="image/jpeg,image/png,image/webp" capture="user" @change="chooseEnrollmentFile" /></label><div class="tpeople-camera-actions"><button class="tv-button" type="button" @click="startCamera">Use camera</button><button v-if="cameraActive" class="tv-button primary" type="button" @click="captureCamera">Take photo</button><button v-if="cameraActive" class="tv-button" type="button" @click="stopCamera">Stop camera</button></div><video v-show="cameraActive" ref="cameraVideo" class="tpeople-camera" muted playsinline /><img v-if="enrollmentPreview" class="tpeople-enrollment-preview" :src="enrollmentPreview" alt="Face enrollment preview" /><div class="people-face-enroll-note">{{ faceStatus.loaded ? "Face ID is ready. The photo will be checked before it is saved." : "The Face ID model is not ready. Enable or load it under Models before adding a face." }}</div><button class="tv-button primary" type="button" :disabled="Boolean(busy) || !enrollmentPersonId || !enrollmentUpload" @click="enrollFace">{{ busy === 'people_face_enroll' ? "Adding…" : "Add face to person" }}</button></div></section>
    </PopupTransition>
  </section>
</template>
