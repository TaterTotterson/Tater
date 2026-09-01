#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueSpudexTests(unittest.TestCase):
    def test_spudex_uses_shared_vue_bundle_with_legacy_fallback(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")

        self.assertIn("async function mountVueSpudex", app_js)
        self.assertIn("module.mountSpudex", app_js)
        self.assertIn('withBasePath("/api/spudex")', app_js)
        self.assertIn("The Vue Spudex surface could not load; using the legacy renderer.", app_js)
        self.assertIn("export function mountSpudex", entry)

    def test_spudex_preserves_all_workspaces_and_actions(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "spudex" / "SpudexApp.vue").read_text(encoding="utf-8")

        for feature in (
            'id: "workbench"',
            'id: "manual"',
            'id: "settings"',
            "sendChat",
            "newChat",
            "runCommand",
            "stopSession",
            "closeSession",
            "fileChange",
            '"approve" | "reject"',
            "saveSettings",
            'method: "DELETE"',
            'file-changes/${action}',
        ):
            self.assertIn(feature, source)

    def test_spudex_live_updates_do_not_replace_the_workspace(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "spudex" / "SpudexApp.vue").read_text(encoding="utf-8")

        self.assertIn("schedulePoll", source)
        self.assertIn("refreshState(true)", source)
        self.assertIn("refreshLogs", source)
        self.assertIn("refreshManualLogs", source)
        self.assertIn("mergeLogRows", source)
        self.assertIn("settingsDirty", source)
        self.assertNotIn("location.reload", source)
        self.assertNotIn("innerHTML", source)

    def test_workbench_keeps_chat_and_runtime_output_separate(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "spudex" / "SpudexApp.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn('class="tsx-spud-bar"', source)
        self.assertIn('class="tv-panel tsx-chat-card"', source)
        self.assertIn('class="tv-panel tsx-terminal-card"', source)
        self.assertIn('role="log"', source)
        self.assertIn("const nonChatLogs", source)
        self.assertIn("grid-template-columns: minmax(0, 1.06fr) minmax(0, .94fr)", styles)
        self.assertIn("height: max(620px, min(690px, calc(100dvh - 225px)))", styles)
        self.assertIn("grid-template-rows: minmax(0, 1fr)", styles)
        self.assertIn(".tsx-chat-card, .tsx-terminal-card { display: flex; height: 100%; min-height: 0", styles)
        self.assertGreaterEqual(styles.count("overscroll-behavior: contain"), 2)

    def test_manual_session_is_a_single_interactive_terminal(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "spudex" / "SpudexApp.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn('class="tsx-manual-terminal"', source)
        self.assertIn('class="tsx-manual-prompt"', source)
        self.assertIn('aria-label="Terminal command"', source)
        self.assertIn("preserveLogs", source)
        self.assertIn('const manualCwd = ref("agent_lab")', source)
        self.assertIn('const manualCwdDisplay = ref("~")', source)
        self.assertIn("cwd: manualCwd.value", source)
        self.assertIn('result.builtin === "cd"', source)
        self.assertIn("Starts in agent_lab (~) with access to the host filesystem", source)
        self.assertIn("Filesystem paths are unrestricted", source)
        self.assertNotIn('class="tv-panel tsx-run-card"', source)
        self.assertNotIn('class="tv-panel tsx-manual-history"', source)
        self.assertIn(".tsx-terminal-check input.tv-checkbox", styles)

    def test_settings_checkboxes_keep_compact_dimensions(self) -> None:
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn(".tater-vue-surface.tsx-spudex input.tv-checkbox", styles)
        self.assertIn("width: 17px !important", styles)
        self.assertIn("height: 17px !important", styles)

    def test_spudex_includes_complete_session_insights_and_policy(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "spudex" / "SpudexApp.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        for feature in (
            "Plan",
            "Verification",
            "App previews",
            "File changes",
            "Session memory",
            "last_policy_block",
            "allow_network",
            "allow_installs",
            "allow_shell_commands",
            "allow_host_admin_commands",
            "allowed_platforms",
        ):
            self.assertIn(feature, source)
        self.assertIn(".tsx-workbench-grid", styles)
        self.assertIn(".tsx-manual-console", styles)
        self.assertIn(".tsx-policy-grid", styles)
        self.assertIn(".tsx-details", styles)


if __name__ == "__main__":
    unittest.main()
