#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueAppShellTests(unittest.TestCase):
    def test_single_vue_shell_owns_navigation_runtime_and_views(self) -> None:
        shell = (REPO_ROOT / "frontend" / "src" / "shell" / "AppShell.vue").read_text(encoding="utf-8")
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")

        self.assertIn("export function mountAppShell", entry)
        self.assertIn("<KeepAlive", shell)
        self.assertIn("syncHistory", shell)
        self.assertIn("window.addEventListener(\"popstate\"", shell)
        self.assertIn("<RuntimeStatus", shell)
        self.assertIn('id="brand-name">{{ branding.firstName }}', shell)
        self.assertIn('id="brand-subtitle">{{ branding.firstName }}OS Control Surface', shell)
        self.assertIn('`${assistantName} ${branding.versionLabel}`', shell)
        self.assertIn('`${assistantName} v${normalized}`', shell)
        self.assertIn(':aria-label="`Current ${branding.firstName || \'Tater\'} version`"', shell)
        self.assertNotIn("brand-mascot", shell)
        self.assertIn('id="toast-root"', shell)
        for component in (
            "DashboardApp",
            "ChatApp",
            "VerbasApp",
            "PortalsApp",
            "CoresApp",
            "IntegrationsApp",
            "SpudexApp",
            "SettingsApp",
        ):
            self.assertIn(component, shell)

    def test_vue_shell_is_the_only_startup_path(self) -> None:
        index = (REPO_ROOT / "tateros_static" / "index.html").read_text(encoding="utf-8")
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn('id="tater-app-root"', index)
        self.assertNotIn('id="app-shell"', index)
        self.assertNotIn('class="nav-stack"', index)
        self.assertIn("async function mountVueAppShell()", app_js)
        self.assertIn("loadVueShellView", app_js)
        self.assertIn("module.mountAppShell", app_js)
        self.assertIn("The Vue application bundle did not provide the application shell.", app_js)
        self.assertIn("Failed to initialize Tater UI:", app_js)
        self.assertIn("state.appShellController = controller", app_js)
        self.assertIn("state.appShellController?.toast", app_js)
        self.assertNotIn("using the legacy", app_js.lower())
        self.assertNotIn("function loadView(", app_js)

    def test_vue_entry_owns_all_application_css(self) -> None:
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")
        shell = (REPO_ROOT / "frontend" / "src" / "shell" / "AppShell.vue").read_text(encoding="utf-8")
        index = (REPO_ROOT / "tateros_static" / "index.html").read_text(encoding="utf-8")
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        bundle = (REPO_ROOT / "tateros_static" / "ui" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn('import "./base.css";', entry)
        self.assertIn('import "./music/music-core.css";', entry)
        self.assertIn('import "./tater-ui.css";', entry)
        self.assertLess(entry.index('import "./base.css";'), entry.index('import "./music/music-core.css";'))
        self.assertLess(entry.index('import "./music/music-core.css";'), entry.index('import "./tater-ui.css";'))
        self.assertIn('class="bg-shape bg-shape-a"', shell)
        self.assertIn('href="./static/ui/tater-ui.css"', index)
        self.assertNotIn("static/styles.css", index)
        self.assertNotIn('STATIC_DIR / "styles.css"', backend)
        self.assertIn('href="./static/ui/tater-ui.css"', backend)
        for selector in (".app-shell", ".tater-music-core", ".tater-vue-shell"):
            self.assertIn(selector, bundle)
        self.assertFalse((REPO_ROOT / "tateros_static" / "styles.css").exists())

    def test_login_gate_is_owned_by_the_vue_shell(self) -> None:
        shell = (REPO_ROOT / "frontend" / "src" / "shell" / "AppShell.vue").read_text(encoding="utf-8")
        auth = (REPO_ROOT / "frontend" / "src" / "shell" / "AuthGate.vue").read_text(encoding="utf-8")
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn('import AuthGate from "./AuthGate.vue"', shell)
        self.assertIn("<AuthGate", shell)
        self.assertIn("function requireAuth", shell)
        self.assertIn("props.authenticate", auth)
        self.assertIn('id="webui-auth-modal"', auth)
        self.assertIn("initialAuthState: appShellAuthState(state.auth)", app_js)
        self.assertIn("authenticate: authenticateWebui", app_js)

    def test_collapsing_sidebar_releases_its_layout_column_immediately(self) -> None:
        styles = (REPO_ROOT / "frontend" / "src" / "base.css").read_text(encoding="utf-8")

        self.assertIn(".app-shell.sidebar-collapsing {\n  --sidebar-width: 0px;", styles)
        self.assertIn(".app-shell.sidebar-collapsed .topbar", styles)


if __name__ == "__main__":
    unittest.main()
