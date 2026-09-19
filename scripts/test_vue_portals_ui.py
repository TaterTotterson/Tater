#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VuePortalsTests(unittest.TestCase):
    def test_portals_are_loaded_by_the_shared_vue_shell(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        shell = (REPO_ROOT / "frontend" / "src" / "shell" / "AppShell.vue").read_text(encoding="utf-8")

        self.assertIn('import PortalsApp from "../portals/PortalsApp.vue"', shell)
        self.assertIn("portals: PortalsApp", shell)
        self.assertIn('if (view === "portals")', app_js)
        self.assertIn("createPortalsVueDescriptor", app_js)
        self.assertIn('withBasePath("/api/portals")', app_js)
        self.assertIn('withBasePath("/api/shop/portals")', app_js)
        self.assertNotIn("mountVuePortals", app_js)
        self.assertNotIn("legacy renderer", app_js)

    def test_portals_preserve_runtime_shop_and_repository_management(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "portals" / "PortalsApp.vue").read_text(encoding="utf-8")

        for feature in (
            'id: "installed"',
            'id: "store"',
            'id: "manage"',
            'id: "repos"',
            'action: "start" | "stop"',
            "/settings",
            "update-all",
            "purge_redis",
            "saveRepos",
            "shopAction('install'",
            "shopAction('remove'",
            "Delete data",
            "Running Portals restart automatically",
            "Portal control center",
            "tv-manage-card",
            "Updates ready",
        ):
            self.assertIn(feature, source)

    def test_portals_share_complete_manifest_settings_renderer(self) -> None:
        portal = (REPO_ROOT / "frontend" / "src" / "portals" / "PortalsApp.vue").read_text(encoding="utf-8")
        verba = (REPO_ROOT / "frontend" / "src" / "verbas" / "VerbasApp.vue").read_text(encoding="utf-8")
        field = (REPO_ROOT / "frontend" / "src" / "shared" / "ManifestField.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn('import ManifestField from "../shared/ManifestField.vue"', portal)
        self.assertIn('import ManifestField from "../shared/ManifestField.vue"', verba)
        self.assertIn("show_when_all", field)
        self.assertIn("new FileReader", field)
        self.assertIn(".tp-card-grid { display: grid;", styles)
        self.assertIn(".tp-manage-list { display: grid;", styles)
        self.assertIn(".tp-repo-form { display: grid;", styles)

    def test_portal_overview_lazy_loads_dynamic_settings(self) -> None:
        portal = (REPO_ROOT / "frontend" / "src" / "portals" / "PortalsApp.vue").read_text(encoding="utf-8")
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        overview_handler = backend.split('@app.get("/api/portals")', 1)[1].split('@app.get("/api/portals/{portal_key}/settings")', 1)[0]

        self.assertIn('"settings": _setting_fields(portal.get("required", {}), current_settings)', backend)
        self.assertNotIn("_portal_setting_fields", overview_handler)
        self.assertIn('@app.get("/api/portals/{portal_key}/settings")', backend)
        self.assertIn("settingsLoading", portal)
        self.assertIn("async function openSettings", portal)
        self.assertIn("/settings`);", portal)
        self.assertIn("Loading live Portal settings", portal)
        self.assertIn("PORTAL_SETTINGS_HOOK_TIMEOUT_SECONDS = 2.0", backend)
        self.assertIn("PORTAL_SETTINGS_FIELD_CACHE_TTL_SECONDS = 60.0", backend)
        self.assertIn("_portal_settings_field_executor", backend)
        self.assertIn("using cached or local fields", backend)


if __name__ == "__main__":
    unittest.main()
