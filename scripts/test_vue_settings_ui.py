#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueSettingsTests(unittest.TestCase):
    def test_settings_are_owned_by_the_shared_vue_shell(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        shell = (REPO_ROOT / "frontend" / "src" / "shell" / "AppShell.vue").read_text(encoding="utf-8")

        self.assertIn('import SettingsApp from "../settings/SettingsApp.vue"', shell)
        self.assertIn("settings: SettingsApp", shell)
        self.assertIn('if (view === "settings")', app_js)
        self.assertIn("createSettingsVueDescriptor", app_js)
        self.assertNotIn("mountVueSettings", app_js)
        self.assertNotIn("loadSettingsView", app_js)
        self.assertNotIn("ts-settings-legacy", app_js)
        self.assertNotIn("legacy navigation", app_js)

    def test_settings_shell_exposes_every_top_level_area(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "settings" / "SettingsApp.vue").read_text(encoding="utf-8")

        for tab_id in (
            "general",
            "people",
            "models",
            "hydra",
            "esphome",
            "redis",
            "spudhub",
            "misc",
            "advanced",
            "system",
            "logs",
        ):
            self.assertIn(f'id: "{tab_id}"', source)
        self.assertIn("onTabChange", source)
        self.assertIn("defineExpose", source)
        self.assertNotIn("<span>Sections</span>", source)

    def test_general_settings_are_vue_owned_and_use_a_canonical_resource(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "settings" / "SettingsApp.vue").read_text(encoding="utf-8")
        general = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "GeneralSettings.vue"
        ).read_text(encoding="utf-8")
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")
        base_styles = (REPO_ROOT / "frontend" / "src" / "base.css").read_text(encoding="utf-8")

        self.assertIn("GeneralSettingsPanel", source)
        self.assertIn("activeTab === 'general'", source)
        self.assertIn("postJson<GeneralSettings>(props.endpoint, payload)", general)
        self.assertIn('emit("saved", next)', general)
        self.assertIn('general: withBasePath("/api/settings/general")', app_js)
        self.assertIn("onGeneralChange", app_js)
        self.assertIn('@app.get("/api/settings/general")', backend)
        self.assertIn('@app.post("/api/settings/general")', backend)
        self.assertIn("return _general_settings_payload()", backend)
        self.assertIn(".tset-general", styles)
        self.assertIn("webui_theme: draft.webui_theme", general)
        self.assertIn("props.onThemePreview?.(theme)", general)
        for theme in ("tater", "tater-light", "blueberry", "mint", "grape", "strawberry"):
            self.assertIn(f'id: "{theme}"', general)
            self.assertIn(f':root[data-theme="{theme}"]', base_styles)
        self.assertIn('const WEBUI_THEME_CHOICES = ["tater", "tater-light", "blueberry", "mint", "grape", "strawberry"]', app_js)
        self.assertIn("function applyWebuiTheme(value)", app_js)
        self.assertIn('WEBUI_THEME_KEY = "tater:webui:theme"', backend)
        self.assertIn("def _normalize_webui_theme", backend)
        self.assertIn(".tset-theme-grid", styles)
        self.assertIn('--ui-color-scheme: light;', base_styles)
        self.assertIn(':root[data-theme="tater-light"] .webui-auth-overlay', base_styles)
        self.assertIn(':root[data-theme="tater-light"] .app-log-events', base_styles)
        self.assertIn(':root[data-theme="tater-light"] .bubble.user', base_styles)
        self.assertNotIn('.view-root[data-view="settings"] label {', base_styles)
        self.assertNotIn('.view-root[data-view="settings"] label > input', base_styles)
        self.assertIn('input:not(.toggle-input):not(.tv-checkbox)', styles)
        self.assertIn(':not([type="checkbox"])', styles)
        self.assertIn('input[type="checkbox"]:not(.toggle-input)', styles)
        self.assertIn('min-width: 17px !important', styles)
        self.assertIn('.tv-form-grid label > :is(input, select, textarea, small, span)', styles)

    def test_misc_and_advanced_settings_are_vue_owned_resources(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "settings" / "SettingsApp.vue").read_text(encoding="utf-8")
        misc = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "MiscSettings.vue"
        ).read_text(encoding="utf-8")
        advanced = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "AdvancedSettings.vue"
        ).read_text(encoding="utf-8")
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("MiscSettingsPanel", source)
        self.assertIn("AdvancedSettingsPanel", source)
        self.assertIn("postJson<MiscSettings>(props.endpoint, payload)", misc)
        self.assertIn("postJson<AdvancedSettings>(props.endpoint, payload)", advanced)
        self.assertIn("props.clearChatEndpoint", advanced)
        self.assertIn("The request’s model chooses the route", advanced)
        self.assertIn("tater/base", advanced)
        self.assertIn("tater/hydra", advanced)
        self.assertIn("Authorization: Bearer", advanced)
        self.assertIn('misc: withBasePath("/api/settings/misc")', app_js)
        self.assertIn('advanced: withBasePath("/api/settings/advanced")', app_js)
        self.assertIn("onMiscChange", app_js)
        self.assertIn("onAdvancedChange", app_js)
        self.assertIn('@app.get("/api/settings/misc")', backend)
        self.assertIn('@app.post("/api/settings/misc")', backend)
        self.assertIn('@app.get("/api/settings/advanced")', backend)
        self.assertIn('@app.post("/api/settings/advanced")', backend)
        self.assertIn(".tset-resource", styles)
        self.assertIn(".tset-danger-card", styles)
        self.assertIn(".tadvanced-api-guide", styles)

    def test_hydra_settings_metrics_and_data_are_vue_owned(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "settings" / "SettingsApp.vue").read_text(encoding="utf-8")
        hydra = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "HydraSettings.vue"
        ).read_text(encoding="utf-8")
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("HydraSettingsPanel", source)
        self.assertIn("activeTab === 'hydra'", source)
        self.assertIn("postJson<HydraSettings>(props.endpoint, payload)", hydra)
        self.assertIn("getJson<JsonRow>(`${props.metricsEndpoint}?", hydra)
        self.assertIn("getJson<JsonRow>(props.dataEndpoint)", hydra)
        self.assertIn("props.clearDataEndpoint", hydra)
        self.assertIn('hydra: withBasePath("/api/settings/hydra")', app_js)
        self.assertIn('hydraMetrics: withBasePath("/api/settings/hydra/metrics")', app_js)
        self.assertIn("onHydraChange", app_js)
        self.assertIn('@app.get("/api/settings/hydra")', backend)
        self.assertIn('@app.post("/api/settings/hydra")', backend)
        self.assertIn("return _hydra_settings_payload()", backend)
        self.assertIn(".thydra-tabs", styles)

    def test_system_tasks_have_a_settings_tab_and_live_controls(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        settings_app = (REPO_ROOT / "frontend" / "src" / "settings" / "SettingsApp.vue").read_text(encoding="utf-8")
        system_tasks = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "SystemTasksSettings.vue"
        ).read_text(encoding="utf-8")
        task_card = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "SystemTaskCard.vue"
        ).read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("SystemTasksSettingsPanel", settings_app)
        self.assertIn("activeTab === 'system'", settings_app)
        self.assertIn("Live background snapshots", settings_app)
        self.assertNotIn("manual refresh controls", settings_app)
        self.assertIn("getJson<JsonRow>(props.endpoint)", system_tasks)
        self.assertIn("postJson<JsonRow>(endpoint)", system_tasks)
        self.assertIn("onBeforeUnmount", system_tasks)
        self.assertIn("SystemTaskCard", system_tasks)
        self.assertIn("system-task-overview", system_tasks)
        self.assertIn("Overall process health", system_tasks)
        self.assertIn("system-task-list-head", system_tasks)
        self.assertIn("tsystem-live-state", system_tasks)
        self.assertIn("schedulePoll(2000)", system_tasks)
        self.assertNotIn('@click="refresh()"', system_tasks)
        self.assertIn("emit('run', taskId, coreKey)", task_card)
        self.assertIn("system-task-process", task_card)
        self.assertIn("system-task-command", task_card)
        self.assertIn('systemTasks: withBasePath("/api/settings/system-tasks")', app_js)
        self.assertIn('coreTaskRun: withBasePath("/api/settings/core-tasks")', app_js)
        self.assertNotIn("clearSettingsSystemTasksPollTimer", app_js)
        self.assertIn(".tsystem-head", styles)
        self.assertIn(".system-task-card", styles)
        self.assertIn(".system-task-health-ring", styles)
        self.assertIn(".system-task-list-head", styles)
        self.assertIn(".tsystem-live-state", styles)

    def test_logs_are_vue_owned_with_incremental_polling(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        settings_app = (REPO_ROOT / "frontend" / "src" / "settings" / "SettingsApp.vue").read_text(encoding="utf-8")
        logs = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "LogsSettings.vue"
        ).read_text(encoding="utf-8")
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("LogsSettingsPanel", settings_app)
        self.assertIn("activeTab === 'logs'", settings_app)
        self.assertIn("after_seq", logs)
        self.assertIn("nextSeq", logs)
        self.assertIn("schedulePoll(1000)", logs)
        self.assertIn("onBeforeUnmount", logs)
        self.assertIn("navigator.clipboard.writeText", logs)
        self.assertIn('logs: withBasePath("/api/settings/logs")', app_js)
        self.assertNotIn("clearSettingsLogPollTimer", app_js)
        self.assertIn('@app.get("/api/settings/logs")', backend)
        self.assertIn(".tlogs-events", styles)

    def test_people_directory_faces_and_identities_are_vue_owned(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        settings_app = (REPO_ROOT / "frontend" / "src" / "settings" / "SettingsApp.vue").read_text(encoding="utf-8")
        people = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "PeopleSettings.vue"
        ).read_text(encoding="utf-8")
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("PeopleSettingsPanel", settings_app)
        self.assertIn("activeTab === 'people'", settings_app)
        self.assertIn("getJson<PeopleSettingsPayload>(props.endpoint)", people)
        self.assertIn("postJson<JsonRow>(props.actionEndpoint", people)
        for action in (
            "people_create",
            "people_save",
            "people_delete",
            "people_alias_attach",
            "people_alias_detach",
            "people_identity_forget",
            "people_face_enroll",
            "people_face_save",
            "people_face_merge",
            "people_face_move_images",
            "people_face_remove_images",
            "people_face_delete",
        ):
            self.assertIn(f'"{action}"', people)
        self.assertIn("navigator.mediaDevices.getUserMedia", people)
        self.assertIn('people: withBasePath("/api/settings/people")', app_js)
        self.assertIn('peopleAction: withBasePath("/api/settings/people/action")', app_js)
        self.assertIn('@app.get("/api/settings/people")', backend)
        self.assertIn('@app.post("/api/settings/people/action")', backend)
        self.assertIn(".tpeople-camera", styles)
        self.assertIn('import PopupTransition from "../../shared/PopupTransition.vue"', people)
        self.assertEqual(people.count("<PopupTransition"), 3)
        self.assertIn('backdrop-class="tv-modal-backdrop tpeople tset-modal"', people)
        self.assertIn('class="tv-toggle people-admin-toggle"', people)
        self.assertIn(".people-admin-toggle > .tv-checkbox", styles)

    def test_standard_fields_and_dropdowns_share_one_settings_size(self) -> None:
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")
        self.assertIn(".tset-settings, .tset-modal { --tset-control-height: 36px;", styles)
        self.assertIn("select:not([multiple]):not([size])", styles)
        self.assertIn("height: var(--tset-control-height)", styles)
        self.assertIn("font-size: var(--tset-control-font-size)", styles)

    def test_spud_link_pairing_routing_and_status_are_vue_owned(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        settings_app = (REPO_ROOT / "frontend" / "src" / "settings" / "SettingsApp.vue").read_text(encoding="utf-8")
        spud_link = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "SpudLinkSettings.vue"
        ).read_text(encoding="utf-8")
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("SpudLinkSettingsPanel", settings_app)
        self.assertIn("activeTab === 'spudhub'", settings_app)
        self.assertIn("postJson<SpudLinkSettings>(props.endpoint, settingsPayload(overrides))", spud_link)
        self.assertIn("getJson<JsonRow>(props.statusEndpoint)", spud_link)
        self.assertIn("postJson<JsonRow>(props.pairingCodeEndpoint", spud_link)
        self.assertIn("postJson<JsonRow>(props.connectEndpoint", spud_link)
        self.assertIn("postJson<JsonRow>(props.revokeEndpoint", spud_link)
        self.assertIn("scheduleStatusPoll", spud_link)
        self.assertIn("onBeforeUnmount", spud_link)
        self.assertIn('spudLink: withBasePath("/api/settings/spud-link")', app_js)
        self.assertIn('spudLinkStatus: withBasePath("/api/spudlink/status")', app_js)
        self.assertIn('spudLinkPairingCode: withBasePath("/api/spudlink/pairing-code")', app_js)
        self.assertIn('spudLinkConnect: withBasePath("/api/spudlink/connect")', app_js)
        self.assertIn('spudLinkRevoke: withBasePath("/api/spudlink/revoke-node")', app_js)
        self.assertIn('@app.get("/api/settings/spud-link")', backend)
        self.assertIn('@app.post("/api/settings/spud-link")', backend)
        self.assertIn(".tlink-route-grid", styles)

    def test_models_workspace_has_one_bottom_action_per_subtab(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        settings_app = (REPO_ROOT / "frontend" / "src" / "settings" / "SettingsApp.vue").read_text(encoding="utf-8")
        models = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "ModelsSettings.vue"
        ).read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("ModelsSettingsPanel", settings_app)
        self.assertIn("activeTab === 'models'", settings_app)
        self.assertIn("Save & Apply LLM Settings", models)
        self.assertIn("Save Speech Settings", models)
        self.assertEqual(models.count("Apply To All Satellites"), 1)
        self.assertIn("Save & Apply Vision Settings", models)
        self.assertIn("Save & Apply Audio and Video", models)
        self.assertIn("Save Speaker ID Settings", models)
        self.assertIn("Save Emotion ID Settings", models)
        self.assertIn("Apply Face ID Settings", models)
        self.assertIn("tmodels-save-bar", models)
        self.assertNotIn("MutationObserver", models)
        self.assertNotIn("settings-hydra-model-stack", models)
        self.assertNotIn("querySelector", models)
        for component in (
            "HuggingFaceModels",
            "LlmModels",
            "SpeechModels",
            "WakeWordModels",
            "MediaModelCard",
            "IdentityModels",
        ):
            self.assertIn(component, models)
        self.assertNotIn("Save All Models", app_js)
        self.assertNotIn("Save & Load All Local LLMs", app_js)
        self.assertIn('modelsVoiceRuntime: withBasePath("/api/settings/voice/runtime")', app_js)
        self.assertIn('modelsHuggingFace: withBasePath("/api/settings/huggingface/models")', app_js)
        self.assertIn('modelsSpeechPreview: withBasePath("/api/settings/speech/tts-preview")', app_js)
        self.assertIn("MediaModelCard", models)
        self.assertIn(".tmodels-save-bar", styles)
        self.assertNotIn("renderSpudLinkRouteNotice", app_js)
        self.assertNotIn(".tmodels-retired-actions", styles)

        llm = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "models" / "LlmModels.vue"
        ).read_text(encoding="utf-8")
        self.assertIn("Prompt cache slot", llm)
        self.assertIn(":max=\"Math.max(0, Number(draft.hydra_llama_cpp_slot_count || 1) - 1)\"", llm)
        self.assertIn("A number pins it to that exact slot.", llm)
        self.assertIn("Base model pool", llm)
        self.assertIn("Base routes share requests", llm)
        self.assertIn("normal Base calls rotate through the pool", llm)
        self.assertIn("Hydra roles stay assigned to a consistent pool member", llm)
        self.assertIn("Add round-robin route", llm)
        for retired_label in ("Add fallback route", "Primary route", "Backup model route", "First choice"):
            self.assertNotIn(retired_label, llm)

    def test_local_model_saves_show_live_vue_apply_progress(self) -> None:
        models = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "ModelsSettings.vue"
        ).read_text(encoding="utf-8")
        progress = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "models" / "LocalModelApplyProgress.vue"
        ).read_text(encoding="utf-8")
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn('import LocalModelApplyProgress from "./models/LocalModelApplyProgress.vue"', models)
        self.assertIn("beginLocalModelApply()", models)
        self.assertIn("pollModelWarmup", models)
        self.assertIn("props.endpoints.modelsHfWarmup", models)
        self.assertIn("<LocalModelApplyProgress", models)
        self.assertIn('import PopupTransition from "../../../shared/PopupTransition.vue"', progress)
        for stage in ("Save settings", "Unload previous", "Load selected", "Refresh platforms", "Ready"):
            self.assertIn(stage, progress)
        self.assertIn("Cores and Portals are restarting", progress)
        self.assertIn("This continues safely if you hide the window or change tabs.", progress)
        self.assertIn(".tm-model-apply-hero", styles)
        self.assertIn(".tm-model-spud", styles)
        self.assertIn(".tm-model-apply-stages", styles)
        self.assertIn("if not clean_items and not clean_unload_items:", backend)
        self.assertIn('reason="settings-save-unload"', backend)
        self.assertIn('"active_before": active_before', backend)

    def test_speech_workspace_is_provider_focused_and_tater_themed(self) -> None:
        speech = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "models" / "SpeechModels.vue"
        ).read_text(encoding="utf-8")
        profile = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "models" / "TtsProfile.vue"
        ).read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        for area in ("listening", "replies", "announcements", "playback"):
            self.assertIn(f"area === '{area}'", speech)
        self.assertIn("tm-speech-workspace", speech)
        self.assertIn("tm-speech-provider-grid", speech)
        self.assertIn("visibleVoiceSections", speech)
        self.assertIn("function setStt", speech)
        self.assertIn('const inheritsReplyVoice = announcement && ["same_as_direct", "direct"].includes(backend)', speech)
        self.assertIn('const prefix = inheritsReplyVoice ? "speech_" : requestedPrefix', speech)

        self.assertIn('same_as_direct: { mark: "↔"', profile)
        self.assertIn("inheritsDirect", profile)
        self.assertIn("tm-speech-inherit-card", profile)
        self.assertIn("tm-speech-advanced", profile)
        self.assertIn("Discover models & voices", profile)
        self.assertNotIn('<select v-model="draft[backendKey]"', profile)

        for selector in (
            ".tm-speech-hero",
            ".tm-speech-provider-grid",
            ".tm-speech-inherit-card",
            ".tm-speech-preview-card",
        ):
            self.assertIn(selector, styles)

    def test_remaining_model_tabs_use_guided_tater_workspaces(self) -> None:
        model_dir = REPO_ROOT / "frontend" / "src" / "settings" / "components" / "models"
        models = (REPO_ROOT / "frontend" / "src" / "settings" / "components" / "ModelsSettings.vue").read_text(encoding="utf-8")
        wake = (model_dir / "WakeWordModels.vue").read_text(encoding="utf-8")
        media = (model_dir / "MediaModelCard.vue").read_text(encoding="utf-8")
        identity = (model_dir / "IdentityModels.vue").read_text(encoding="utf-8")
        face = (model_dir / "FaceModels.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        for marker in (
            "tm-wake-engine-card",
            "tm-wake-training-card",
            "tm-wake-verifier-card",
            "chooseWakeEngine",
            "chooseWakeSource",
            "chooseVerifierMode",
        ):
            self.assertIn(marker, wake)
        self.assertNotIn("<ModelFields", wake)
        self.assertEqual(wake.count("Apply To All Satellites"), 0)

        self.assertIn("tm-media-mode-grid", media)
        self.assertIn("tm-media-provider-grid", media)
        self.assertIn("Only compatible installed models appear", media)
        self.assertIn("row.is_speculative_draft === true", media)
        self.assertIn("function chooseMode", media)
        self.assertIn("function chooseProvider", media)

        self.assertIn("tm-identity-master-toggle", identity)
        self.assertIn('v-if="!bestMatch"', identity)
        self.assertIn('v-if="promptHints"', identity)
        self.assertIn("Capture voice sample", identity)
        self.assertIn('const createDraft = reactive<JsonRow>({ speaker_name: "" })', identity)
        self.assertNotIn("createDraft.preferred_selector", identity)
        self.assertIn("Capture from satellite", identity)
        self.assertIn("Used for the next voice sample", identity)
        self.assertNotIn("Model source", identity)
        self.assertNotIn("VOICE_SPEAKER_ID_MODEL_SOURCE", identity)
        self.assertNotIn("VOICE_EMOTION_ID_MODEL_SOURCE", identity)
        self.assertIn("tm-emotion-result", identity)
        self.assertIn('String((candidate as JsonRow).speaker_id || "").trim()', identity)
        self.assertIn('const speakerTab = ref<"people" | "settings">("people")', identity)
        self.assertLess(identity.index("selectSpeakerTab('people')"), identity.index("selectSpeakerTab('settings')"))
        self.assertIn("isSpeaker && speakerTab === 'people'", identity)
        self.assertIn("!isSpeaker || speakerTab === 'settings'", identity)
        self.assertNotIn("<ModelFields", identity)

        self.assertIn('import FaceModels from "./models/FaceModels.vue"', models)
        self.assertIn('@subtab="setSpeakerSubtab"', models)
        self.assertIn('speakerSubtab.value === "settings"', models)
        self.assertIn("<FaceModels v-else-if=\"activeTab === 'faceid'\"", models)
        self.assertIn("tm-face-model-grid", face)
        self.assertIn("Rebuilding face embeddings", face)
        self.assertIn("function chooseModel", face)

        for selector in (
            ".tm-model-area-hero",
            ".tm-choice-grid",
            ".tm-wake-choice-grid",
            ".tm-media-mode-grid",
            ".tm-identity-master-toggle",
            ".tm-identity-tabs",
            ".tm-face-model-grid",
        ):
            self.assertIn(selector, styles)

    def test_voice_workspace_and_all_subtabs_are_vue_owned(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        settings_app = (REPO_ROOT / "frontend" / "src" / "settings" / "SettingsApp.vue").read_text(encoding="utf-8")
        voice = (REPO_ROOT / "frontend" / "src" / "settings" / "components" / "VoiceSettings.vue").read_text(encoding="utf-8")
        voice_dir = REPO_ROOT / "frontend" / "src" / "settings" / "components" / "voice"
        satellites = (voice_dir / "VoiceSatellites.vue").read_text(encoding="utf-8")
        firmware = (voice_dir / "VoiceFirmware.vue").read_text(encoding="utf-8")
        platform = (voice_dir / "VoicePlatform.vue").read_text(encoding="utf-8")
        stats = (voice_dir / "VoiceStats.vue").read_text(encoding="utf-8")
        stereo = (voice_dir / "VoiceStereo.vue").read_text(encoding="utf-8")
        airplay = (voice_dir / "VoiceAirPlay.vue").read_text(encoding="utf-8")
        presence = (voice_dir / "VoicePresence.vue").read_text(encoding="utf-8")
        voice_routes = (REPO_ROOT / "tater_voice" / "voice_pipeline" / "routes.py").read_text(encoding="utf-8")
        model_fields = (REPO_ROOT / "frontend" / "src" / "settings" / "components" / "models" / "ModelFields.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("VoiceSettingsPanel", settings_app)
        self.assertIn("activeTab === 'esphome'", settings_app)
        self.assertIn('{ id: "esphome", label: "Satellites"', settings_app)
        for tab in ("satellites", "presence", "firmware", "stereo", "airplay", "stats", "platform"):
            self.assertIn(f'id: "{tab}"', voice)
        self.assertLess(voice.index('id: "airplay"'), voice.index('id: "presence"'))
        self.assertLess(voice.index('id: "presence"'), voice.index('id: "stats"'))
        for component in ("VoiceSatellites", "VoicePresence", "VoiceFirmware", "VoiceStereo", "VoiceAirPlay", "VoiceStats", "VoicePlatform"):
            self.assertIn(component, voice)
        self.assertIn("window.setInterval", voice)
        self.assertIn("voice_native_satellite_pairing_start", satellites)
        self.assertIn("voice_native_satellite_settings_save", satellites)
        self.assertIn("voice_display_sensors_save", satellites)
        self.assertIn('import PopupTransition from "../../../shared/PopupTransition.vue"', satellites)
        self.assertIn("openSettings(item)", satellites)
        self.assertIn('class="tv-modal tvoice-settings-modal"', satellites)
        self.assertIn('class="tv-modal tvoice-pairing-modal"', satellites)
        self.assertIn("expires_in_s", satellites)
        self.assertIn("result.paired", satellites)
        self.assertIn("View all device info", satellites)
        self.assertNotIn("<details", satellites)
        self.assertNotIn('class="tvoice-details tvoice-settings-details"', satellites)
        self.assertIn(".tvoice-settings-modal", styles)
        self.assertIn("width: clamp(142px, 16vw, 210px)", styles)
        self.assertIn("filter: drop-shadow", styles)
        self.assertIn("settingsDisplayProfile", satellites)
        self.assertIn("Display sensors", satellites)
        self.assertIn("groupSettingsFields", satellites)
        self.assertIn("tvoice-settings-overview", satellites)
        self.assertIn("tvoice-setup-mode", satellites)
        self.assertIn("Unpairs this satellite", satellites)
        self.assertIn("tm-led-preview-card", model_fields)
        self.assertIn("ledAnimation(state)", model_fields)
        self.assertIn(".tvoice-setup-mode", styles)
        self.assertIn(".tm-led-stage", styles)
        self.assertIn("@keyframes tm-led-chase", styles)
        self.assertNotIn(".tvoice-satellite-card::before", styles)
        self.assertIn("voice_firmware_flash_start", firmware)
        self.assertIn("voice_firmware_esp_usb_flash_start", firmware)
        self.assertIn("voice_firmware_amlogic_flash_start", firmware)
        self.assertIn("voice_firmware_browser_build", firmware)
        self.assertIn("voice_firmware_flash_poll", firmware)
        self.assertIn('class="tv-modal tvoice-flasher-modal"', firmware)
        self.assertIn('class="tv-modal tvoice-progress-modal"', firmware)
        self.assertIn("One satellite at a time", firmware)
        self.assertIn("function groupedOptions", model_fields)
        self.assertIn('<optgroup v-if="groupedOptions(option).length"', model_fields)
        self.assertIn("Save & Apply Satellite Settings", platform)
        self.assertIn("voice_settings_reset_defaults", platform)
        self.assertIn("voice_statistics_reset", stats)
        self.assertIn("voice_stereo_pair_save", stereo)
        self.assertIn("voice_airplay_input_save", airplay)
        self.assertIn("voice_airplay_input_stop", airplay)
        self.assertIn("AirPlay into your satellites", airplay)
        self.assertIn("tvoice-airplay-grid", airplay)
        self.assertIn("tv-checkbox", airplay)
        self.assertIn(".tvoice-airplay-hero", styles)
        self.assertIn("Whole-home Bluetooth presence", presence)
        self.assertIn("new EventSource", presence)
        self.assertIn("presence.snapshot", presence)
        self.assertIn("tvoice-presence-rooms", presence)
        self.assertIn("compareByProximity", presence)
        self.assertIn("closest first", presence)
        self.assertIn("previousRssi * 0.75", presence)
        self.assertIn("RANK_CHANGE_THRESHOLD_DBM", presence)
        self.assertIn("ROOM_PREVIEW_LIMIT = 4", presence)
        self.assertIn('class="tv-modal tvoice-presence-room-modal"', presence)
        self.assertIn("View all {{ group.devices.length }} devices", presence)
        self.assertNotIn('<TransitionGroup name="presence-device"', presence)
        self.assertIn(".tvoice-presence-room-modal-body", styles)
        self.assertIn("tater_voice.native_ble.snapshot()", presence)
        self.assertIn(".tvoice-presence-hero", styles)
        self.assertIn('@router.get("/api/tater/satellite/v1/presence")', voice_routes)
        self.assertIn('@router.get("/api/tater/satellite/v1/presence/events")', voice_routes)
        self.assertIn('voiceRuntime: withBasePath("/api/settings/voice/runtime")', app_js)
        self.assertIn('voiceAction: withBasePath("/api/settings/voice/runtime/action")', app_js)
        self.assertIn('voicePresence: withBasePath("/api/tater/satellite/v1/presence")', app_js)
        self.assertIn('voicePresenceEvents: withBasePath("/api/tater/satellite/v1/presence/events")', app_js)
        self.assertIn(".tvoice-resource", styles)

    def test_redis_connection_encryption_and_migration_are_vue_owned(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        settings_app = (REPO_ROOT / "frontend" / "src" / "settings" / "SettingsApp.vue").read_text(encoding="utf-8")
        redis = (
            REPO_ROOT / "frontend" / "src" / "settings" / "components" / "RedisSettings.vue"
        ).read_text(encoding="utf-8")
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("RedisSettingsPanel", settings_app)
        self.assertIn("activeTab === 'redis'", settings_app)
        self.assertIn("Save Redis Settings", redis)
        self.assertIn("Test Connection", redis)
        self.assertIn("Migrate to Internal", redis)
        self.assertIn("Encrypt Live Redis", redis)
        self.assertIn("Decrypt Live Redis", redis)
        self.assertIn("window.setInterval", redis)
        self.assertIn("if (hydrate) hydrateDraft(next)", redis)
        self.assertIn('redisStatus: withBasePath("/api/redis/status")', app_js)
        self.assertIn('redisConfigure: withBasePath("/api/redis/configure")', app_js)
        self.assertIn('redisMigrateInternal: withBasePath("/api/redis/migrate/internal")', app_js)
        self.assertIn('redisEncryptionStatus: withBasePath("/api/redis/encryption/status")', app_js)
        self.assertIn('redisEncrypt: withBasePath("/api/redis/encryption/encrypt")', app_js)
        self.assertIn('redisDecrypt: withBasePath("/api/redis/encryption/decrypt")', app_js)
        self.assertIn('@app.get("/api/redis/status")', backend)
        self.assertIn('@app.post("/api/redis/configure")', backend)
        self.assertIn('@app.post("/api/redis/migrate/internal")', backend)
        self.assertIn('@app.post("/api/redis/encryption/encrypt")', backend)
        self.assertIn('@app.post("/api/redis/encryption/decrypt")', backend)
        self.assertIn(".tredis-resource", styles)
        self.assertNotIn("settings-vue-ready", styles)
        self.assertNotIn("ts-settings-legacy", styles)
        self.assertNotIn('normalized === "redis"', app_js)
        self.assertNotIn("querySelector", redis)
        self.assertNotIn("getElementById", redis)

    def test_settings_keep_specialized_live_and_security_handlers(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        people = (REPO_ROOT / "frontend" / "src" / "settings" / "components" / "PeopleSettings.vue").read_text(encoding="utf-8")
        redis = (REPO_ROOT / "frontend" / "src" / "settings" / "components" / "RedisSettings.vue").read_text(encoding="utf-8")
        voice = (REPO_ROOT / "frontend" / "src" / "settings" / "components" / "VoiceSettings.vue").read_text(encoding="utf-8")
        logs = (REPO_ROOT / "frontend" / "src" / "settings" / "components" / "LogsSettings.vue").read_text(encoding="utf-8")
        advanced = (REPO_ROOT / "frontend" / "src" / "settings" / "components" / "AdvancedSettings.vue").read_text(encoding="utf-8")

        self.assertIn("postJson<JsonRow>(props.actionEndpoint", people)
        self.assertIn("window.setInterval", redis)
        self.assertIn("window.setInterval", voice)
        self.assertIn("schedulePoll(1000)", logs)
        self.assertIn("admin_only_plugins", advanced)
        self.assertIn("postJson<AdvancedSettings>(props.endpoint", advanced)
        self.assertIn('voiceAction: withBasePath("/api/settings/voice/runtime/action")', app_js)
        self.assertIn('spudLinkConnect: withBasePath("/api/spudlink/connect")', app_js)
        for removed in ("bindSettingsPeopleActions", "bindSettingsRedisSection", "ensureEspHomeRuntimeLoaded", "scheduleSettingsLogPoll", "clearLlmDebugPollTimer"):
            self.assertNotIn(removed, app_js)

    def test_image_less_voice_cards_use_the_full_summary_width(self) -> None:
        item = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "CoreManagerItem.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn(":class=\"{ 'no-image': !item.hero_image_src }\"", item)
        self.assertIn(".core-satellite-summary.no-image", styles)
        self.assertIn("grid-template-columns: minmax(0, 1fr);", styles)

    def test_settings_styles_cover_workspace_forms_and_responsive_layouts(self) -> None:
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn(".tset-settings { gap: 15px; }", styles)
        self.assertIn(".tset-context", styles)
        self.assertNotIn(".settings-vue-ready", styles)
        self.assertNotIn(".ts-settings-legacy", styles)
        self.assertIn('input[type="checkbox"]', styles)
        self.assertIn("grid-template-columns: repeat(2, minmax(0, 1fr));", styles)
        self.assertIn("@media (max-width: 620px)", styles)
        self.assertIn(".tset-save-bar { position: static; }", styles)


if __name__ == "__main__":
    unittest.main()
