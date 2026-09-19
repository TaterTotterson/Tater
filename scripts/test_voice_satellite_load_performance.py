#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VoiceSatelliteLoadPerformanceTests(unittest.TestCase):
    def test_satellite_snapshot_does_not_wait_on_the_runtime_loop(self) -> None:
        source = (REPO_ROOT / "tater_voice" / "home.py").read_text(encoding="utf-8")
        start = source.index("def _native_satellite_status_snapshot")
        end = source.index("\n\ndef ", start + 1)
        function_source = source[start:end]

        self.assertIn("native_satellite.status_snapshot_sync()", function_source)
        self.assertNotIn("run_on_runtime_loop", function_source)

    def test_device_images_are_small_cacheable_urls_instead_of_json_data(self) -> None:
        helpers = (REPO_ROOT / "tater_voice" / "ui_helpers.py").read_text(encoding="utf-8")
        app_source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")

        self.assertIn('./static/device-images/{filename}', helpers)
        self.assertNotIn("base64.b64encode", helpers)
        self.assertIn('"/static/device-images"', app_source)
        self.assertLess(
            app_source.index('"/static/device-images"'),
            app_source.index('app.mount("/static",'),
        )

    def test_silent_satellite_refresh_preserves_unsaved_fields(self) -> None:
        settings = (REPO_ROOT / "frontend" / "src" / "settings" / "components" / "VoiceSettings.vue").read_text(encoding="utf-8")
        satellites = (REPO_ROOT / "frontend" / "src" / "settings" / "components" / "voice" / "VoiceSatellites.vue").read_text(encoding="utf-8")

        self.assertIn("const payload = ref<JsonRow>({})", settings)
        self.assertIn("async function refresh(silent = false)", settings)
        self.assertIn("if (!settingsDirty[token])", satellites)
        self.assertIn("if (!dirty[token])", satellites)
        self.assertIn("if (profile && !displayDirty[token])", satellites)
        self.assertIn('if (!dirty[`${token}:volume`])', satellites)

    def test_settings_bootstrap_requests_run_together(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        start = app_js.index('if (view === "settings")')
        end = app_js.index("throw new Error", start)
        function_source = app_js[start:end]

        self.assertIn("await Promise.all([", function_source)
        self.assertIn('api("/api/settings")', function_source)
        self.assertNotIn('const settings = await api("/api/settings")', function_source)


if __name__ == "__main__":
    unittest.main()
