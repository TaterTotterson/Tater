#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import plistlib
import shutil
import subprocess
import sys
import tempfile
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class TaterBuildVersionTests(unittest.TestCase):
    def test_source_version_comes_from_release_info_plist(self) -> None:
        from tater_version import resolve_tater_version

        info_plist = REPO_ROOT / "macos" / "Tater" / "Resources" / "Info.plist"
        with info_plist.open("rb") as handle:
            expected = str(plistlib.load(handle)["CFBundleShortVersionString"])

        self.assertEqual(resolve_tater_version(environment={}), expected)

    def test_packaged_app_discovers_bundle_info_plist(self) -> None:
        from tater_version import resolve_tater_version

        with tempfile.TemporaryDirectory() as tmp:
            app_contents = pathlib.Path(tmp) / "Tater.app" / "Contents"
            module_file = app_contents / "Resources" / "TaterSource" / "tater_version.py"
            module_file.parent.mkdir(parents=True)
            module_file.touch()
            with (app_contents / "Info.plist").open("wb") as handle:
                plistlib.dump({"CFBundleShortVersionString": "123.4"}, handle)

            self.assertEqual(
                resolve_tater_version(module_file=module_file, environment={}),
                "123.4",
            )

    def test_environment_override_accepts_a_tag_style_version(self) -> None:
        from tater_version import resolve_tater_version

        self.assertEqual(
            resolve_tater_version(environment={"TATER_APP_VERSION": "v321.7"}),
            "321.7",
        )

    def test_auth_bootstrap_and_sidebar_display_are_wired(self) -> None:
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        index = (REPO_ROOT / "tateros_static" / "index.html").read_text(encoding="utf-8")
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "tateros_static" / "styles.css").read_text(encoding="utf-8")

        self.assertIn('"app_version": app_version', backend)
        self.assertIn('id="tater-build-version"', index)
        self.assertIn("_renderTaterBuildVersion(appVersion, appVersionLabel)", app_js)
        self.assertIn(".sidebar-build-version {\n  margin-top: auto;", styles)

    def test_sidebar_uses_the_dedicated_tater_app_icon(self) -> None:
        index = (REPO_ROOT / "tateros_static" / "index.html").read_text(encoding="utf-8")
        icon = REPO_ROOT / "tateros_static" / "assets" / "tater-app-sidebar-icon.png"

        self.assertIn("./static/assets/tater-app-sidebar-icon.png", index)
        self.assertTrue(icon.is_file())
        self.assertGreater(icon.stat().st_size, 0)

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


if __name__ == "__main__":
    unittest.main()
