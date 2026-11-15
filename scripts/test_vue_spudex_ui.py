#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueSpudexTests(unittest.TestCase):
    def test_terminal_is_the_user_facing_name_while_routes_stay_compatible(self) -> None:
        shell = (REPO_ROOT / "frontend" / "src" / "shell" / "AppShell.vue").read_text(encoding="utf-8")
        source = (REPO_ROOT / "frontend" / "src" / "spudex" / "SpudexApp.vue").read_text(encoding="utf-8")

        self.assertIn('{ id: "spudex", label: "Terminal"', shell)
        self.assertIn("txw-project-bar", source)
        self.assertIn('aria-label="Terminal command"', source)
        self.assertNotIn('label: "Spudex"', shell)
        self.assertNotIn("<h1>Spudex</h1>", source)

    def test_spudex_is_loaded_by_the_shared_vue_shell(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        shell = (REPO_ROOT / "frontend" / "src" / "shell" / "AppShell.vue").read_text(encoding="utf-8")

        self.assertIn('import SpudexApp from "../spudex/SpudexApp.vue"', shell)
        self.assertIn("spudex: SpudexApp", shell)
        self.assertIn('if (view === "spudex")', app_js)
        self.assertIn("createSpudexVueDescriptor", app_js)
        self.assertIn('withBasePath("/api/spudex")', app_js)
        self.assertNotIn("mountVueSpudex", app_js)
        self.assertNotIn("legacy renderer", app_js)

    def test_spudex_preserves_all_workspaces_and_actions(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "spudex" / "SpudexApp.vue").read_text(encoding="utf-8")

        for feature in (
            "id:'workbench'",
            "id:'manual'",
            "setTab('settings')",
            "sendChat",
            "newChat",
            "runCommand",
            "stopSession",
            "closeSession",
            "fileChange",
            "'approve'|'reject'",
            "saveSettings",
            "method:'DELETE'",
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

    def test_task_workspace_keeps_conversation_review_and_output_available(self) -> None:
        source = (REPO_ROOT / "frontend/src/spudex/SpudexApp.vue").read_text()
        styles = (REPO_ROOT / "frontend/src/spudex/workspace.css").read_text()
        for feature in ('class="txw-conversation"', 'class="txw-inspector"', 'role="log"', 'const nonChatLogs', 'Load earlier messages', 'Continue task', 'Review changes', 'EventSource', 'onDeactivated'):
            self.assertIn(feature, source)
        self.assertIn('grid-template-columns:minmax(0,1fr) 230px', styles)
        self.assertIn('height:max(450px,calc(100dvh - 230px))', styles)
        self.assertGreaterEqual(styles.count('overscroll-behavior:contain'), 3)

    def test_manual_commands_and_real_interactive_terminal_remain_available(self) -> None:
        source = (REPO_ROOT / "frontend/src/spudex/SpudexApp.vue").read_text()
        terminal = (REPO_ROOT / "frontend/src/spudex/InteractiveTerminal.vue").read_text()
        for feature in ('aria-label="Terminal command"', "const manualCwd=ref(text(project.value.path)", "const manualCwdDisplay=ref(manualCwd.value)", 'cwd:manualCwd.value', "result.builtin==='cd'", 'Starts in Agent Lab', 'commands run through the host shell', 'Full host access', 'InteractiveTerminal', 'background:background.value'):
            self.assertIn(feature, source)
        for feature in ('@xterm/xterm', 'onData', 'ResizeObserver', 'dispose', 'disposed=true'):
            self.assertIn(feature, terminal)

    def test_settings_checkboxes_keep_compact_dimensions(self) -> None:
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn(".tater-vue-surface.tsx-spudex input.tv-checkbox", styles)
        self.assertIn("width: 17px !important", styles)
        self.assertIn("height: 17px !important", styles)

    def test_spudex_includes_complete_session_insights_and_full_access(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "spudex" / "SpudexApp.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        for feature in (
            "Plan",
            "Verification",
            "App previews",
            "File changes",
            "Session memory",
            "last_policy_block",
            "full_access",
            "Use with care.",
            "run any command available to Tater",
            "starting folder, not a filesystem boundary",
            "allowed_platforms",
        ):
            self.assertIn(feature, source)
        for old_policy_toggle in (
            "allow_network",
            "allow_installs",
            "allow_shell_commands",
            "allow_host_admin_commands",
        ):
            self.assertNotIn(old_policy_toggle, source)
        self.assertIn(".tsx-workbench-grid", styles)
        self.assertIn(".tsx-manual-console", styles)
        self.assertIn(".tsx-details", styles)


if __name__ == "__main__":
    unittest.main()
