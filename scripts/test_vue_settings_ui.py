#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueSettingsTests(unittest.TestCase):
    def test_settings_use_shared_vue_shell_with_complete_fallback(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")

        self.assertIn("async function mountVueSettings", app_js)
        self.assertIn("module.mountSettings", app_js)
        self.assertIn('data-tater-settings', app_js)
        self.assertIn("using the complete legacy navigation", app_js)
        self.assertIn("export function mountSettings", entry)

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

    def test_system_tasks_have_a_settings_tab_and_live_controls(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn('data-settings-tab="system">System Tasks</button>', app_js)
        self.assertIn('data-settings-panel="system"', app_js)
        self.assertIn('api("/api/settings/system-tasks"', app_js)
        self.assertIn("data-system-task-run", app_js)
        self.assertIn("scheduleSystemTasksPoll", app_js)
        self.assertIn(".system-task-card", styles)

    def test_settings_keep_specialized_live_and_security_handlers(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        for contract in (
            "bindSettingsPeopleActions",
            "bindSettingsRedisSection",
            "ensureEspHomeRuntimeLoaded",
            "scheduleSettingsLogPoll",
            "clearLlmDebugPollTimer",
            'api("/api/settings"',
            'api("/api/settings/voice/runtime/action"',
            'api("/api/spudlink/connect"',
            "admin_only_plugins",
            "settings-save-advanced",
        ):
            self.assertIn(contract, app_js)

    def test_image_less_voice_cards_use_the_full_summary_width(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "tateros_static" / "styles.css").read_text(encoding="utf-8")

        self.assertIn('core-satellite-summary${heroImageSrc ? "" : " no-image"}', app_js)
        self.assertIn(".core-satellite-summary.no-image", styles)
        self.assertIn("grid-template-columns: minmax(0, 1fr);", styles)

    def test_settings_styles_cover_workspace_forms_and_responsive_layouts(self) -> None:
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn(".tset-settings { gap: 15px; }", styles)
        self.assertIn(".tset-context", styles)
        self.assertIn(".settings-vue-ready > .ts-settings-legacy", styles)
        self.assertIn('input[type="checkbox"]', styles)
        self.assertIn("grid-template-columns: repeat(2, minmax(0, 1fr));", styles)
        self.assertIn("@media (max-width: 620px)", styles)


if __name__ == "__main__":
    unittest.main()
