#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueCoresTests(unittest.TestCase):
    def test_cores_use_shared_vue_bundle_with_legacy_fallback(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")

        self.assertIn("async function mountVueCores", app_js)
        self.assertIn("module.mountCores", app_js)
        self.assertIn('withBasePath("/api/cores")', app_js)
        self.assertIn('withBasePath("/api/shop/cores")', app_js)
        self.assertIn('withBasePath("/api/cores/tabs")', app_js)
        self.assertIn("The Vue Cores surface could not load; using the legacy renderer.", app_js)
        self.assertIn("export function mountCores", entry)

    def test_cores_preserve_runtime_shop_settings_and_repositories(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "cores" / "CoresApp.vue").read_text(encoding="utf-8")

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
            "Running Cores restart automatically",
        ):
            self.assertIn(feature, source)

    def test_dynamic_panels_keep_live_music_and_specialized_core_contracts(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "cores" / "CoresApp.vue").read_text(encoding="utf-8")
        bridge = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "LegacyCorePanel.vue").read_text(encoding="utf-8")
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn("MusicCoreApp", source)
        self.assertIn('addEventListener("core-tab"', source)
        self.assertIn("/tab-events", source)
        self.assertIn("props.render", bridge)
        self.assertIn("host.innerHTML = renderCoreTabPayload", app_js)
        self.assertIn('state.surfaceVueView === "cores"', app_js)
        self.assertIn("state.surfaceVueController.refreshTab", app_js)

    def test_cores_share_manifest_settings_and_responsive_styles(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "cores" / "CoresApp.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn('import ManifestField from "../shared/ManifestField.vue"', source)
        self.assertIn(".tcx-card-grid { display: grid;", styles)
        self.assertIn(".tcx-manage-list { display: grid;", styles)
        self.assertIn(".tcx-repo-form { display: grid;", styles)
        self.assertIn(".tcx-legacy-host", styles)


if __name__ == "__main__":
    unittest.main()
