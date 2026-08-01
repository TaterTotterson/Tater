#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import shutil
import subprocess
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class CoreMusicLibraryRendererTests(unittest.TestCase):
    def test_renderer_supports_persistent_items_and_nested_grid_paging(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn("ui?.persistent_item_groups", app_js)
        self.assertIn('class="core-manager-persistent"', app_js)
        self.assertIn("group?.page_size", app_js)
        self.assertIn("item?.show_save_button", app_js)
        self.assertIn("item?.settings_aria_label", app_js)
        self.assertIn("item?.card_variant", app_js)
        self.assertIn("ui?.live_updates", app_js)
        self.assertIn("data-core-live-updates", app_js)
        self.assertIn("data-core-track-action", app_js)
        self.assertIn("data-core-track-shuffle-action", app_js)
        self.assertIn("data-core-field-action", app_js)
        self.assertIn("silent = false", app_js)
        self.assertIn("scheduleCoreTabLivePoll", app_js)
        self.assertIn("poll_interval_ms", app_js)
        self.assertIn("coreTabRenderedHtml", app_js)
        self.assertIn("silent && previousHtml === nextHtml", app_js)
        self.assertIn("if (!liveUpdates) {\n        setCoreManagerStatus(card, workingText);", app_js)

    def test_music_library_layout_is_sticky_responsive_and_multicolumn(self) -> None:
        styles = (REPO_ROOT / "tateros_static" / "styles.css").read_text(encoding="utf-8")

        self.assertIn(".core-settings-manager-music_library .core-manager-persistent", styles)
        self.assertIn(".core-settings-manager-music_library {\n  overflow: visible;", styles)
        self.assertIn("position: sticky", styles)
        self.assertIn(".core-manager-item-variant-player_bar", styles)
        self.assertIn(".core-music-track-list", styles)
        self.assertIn(".core-music-track-row.active", styles)
        self.assertIn(".core-range-field input[type=\"range\"]", styles)
        self.assertIn(".core-manager-item-variant-music_search", styles)
        self.assertIn(".core-tab-items-group-genres", styles)
        self.assertIn("repeat(auto-fill, minmax(min(220px, 100%), 1fr))", styles)
        self.assertIn("@media (max-width: 560px)", styles)

    def test_app_javascript_parses(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("Node.js is unavailable.")
        result = subprocess.run(
            [node, "--check", str(REPO_ROOT / "tateros_static" / "app.js")],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_music_library_uses_component_frontend_and_event_stream(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        app_py = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        index_html = (REPO_ROOT / "tateros_static" / "index.html").read_text(encoding="utf-8")

        self.assertIn("isVueMusicCorePayload", app_js)
        self.assertIn("mountMusicCore", app_js)
        self.assertIn("/static/ui/tater-music-core.js", app_js)
        self.assertIn("isVueMusicCorePayload(payload) || !boolFromAny", app_js)
        self.assertIn('/api/cores/{core_key}/tab-events', app_py)
        self.assertIn('_stream_core_tab_events', app_py)
        self.assertIn('stable_payload.pop("updated_at", None)', app_py)
        self.assertIn('./static/ui/tater-music-core.css', index_html)
        self.assertTrue((REPO_ROOT / "tateros_static" / "ui" / "tater-music-core.js").is_file())
        self.assertTrue((REPO_ROOT / "tateros_static" / "ui" / "tater-music-core.css").is_file())


if __name__ == "__main__":
    unittest.main()
