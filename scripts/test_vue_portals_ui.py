#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VuePortalsTests(unittest.TestCase):
    def test_portals_use_shared_versioned_vue_bundle_with_legacy_fallback(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")

        self.assertIn("async function mountVuePortals", app_js)
        self.assertIn("module.mountPortals", app_js)
        self.assertIn('withBasePath("/api/portals")', app_js)
        self.assertIn('withBasePath("/api/shop/portals")', app_js)
        self.assertIn("The Vue Portals surface could not load; using the legacy renderer.", app_js)
        self.assertIn("export function mountPortals", entry)

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


if __name__ == "__main__":
    unittest.main()
