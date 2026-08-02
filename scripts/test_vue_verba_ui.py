#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueVerbaTests(unittest.TestCase):
    def test_verba_uses_shared_versioned_vue_bundle_with_legacy_fallback(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")

        self.assertIn("async function mountVueVerbas", app_js)
        self.assertIn("module.mountVerbas", app_js)
        self.assertIn('withBasePath("/api/verbas")', app_js)
        self.assertIn('withBasePath("/api/shop/verbas")', app_js)
        self.assertIn("The Vue Verba surface could not load; using the legacy renderer.", app_js)
        self.assertIn("export function mountVerbas", entry)

    def test_verba_surface_preserves_runtime_and_shop_management(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "verbas" / "VerbasApp.vue").read_text(encoding="utf-8")

        for feature in (
            'id: "installed"',
            'id: "store"',
            'id: "manage"',
            'id: "repos"',
            "/enabled",
            "/settings",
            "update-all",
            "purge_redis",
            "saveRepos",
            "shopAction('install'",
            "shopAction('remove'",
            "Delete data",
        ):
            self.assertIn(feature, source)

    def test_verba_settings_support_manifest_field_contract(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "shared" / "ManifestField.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        for field_type in (
            "type === 'hidden'",
            "type === 'section'",
            "type === 'readonly'",
            "type === 'checkbox'",
            "type === 'multiselect'",
            "type === 'select'",
            "type === 'textarea'",
            "type === 'file'",
            "type === 'range'",
        ):
            self.assertIn(field_type, source)
        self.assertIn("show_when_all", source)
        self.assertIn("new FileReader", source)
        self.assertIn("crypto.getRandomValues", source)
        self.assertIn(".tvb-card-grid { display: grid;", styles)
        self.assertIn(".tvb-manage-list { display: grid;", styles)
        self.assertIn(".tvb-field-grid { display: grid;", styles)


if __name__ == "__main__":
    unittest.main()
