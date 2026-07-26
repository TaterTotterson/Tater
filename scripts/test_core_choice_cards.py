#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import shutil
import subprocess
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class CoreChoiceCardRendererTests(unittest.TestCase):
    def test_static_renderer_supports_guided_core_forms(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "tateros_static" / "styles.css").read_text(encoding="utf-8")

        self.assertIn('presentation === "cards" && (type === "select" || type === "multiselect")', app_js)
        self.assertIn("function bindCoreManagerChoiceCards()", app_js)
        self.assertIn("bindCoreManagerChoiceCards();", app_js)
        self.assertIn('type === "heading" || type === "section_heading"', app_js)
        self.assertIn('if (type === "hidden")', app_js)
        self.assertIn(".core-choice-card.selected", styles)
        self.assertIn(".core-builder-step-heading", styles)

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
