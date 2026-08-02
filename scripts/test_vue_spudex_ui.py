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
