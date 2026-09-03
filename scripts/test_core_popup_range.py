#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class CorePopupRangeTests(unittest.TestCase):
    def test_runtime_popup_renders_and_binds_range_fields(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn('if (type === "range") {', app_js)
        self.assertIn('type="range"', app_js)
        self.assertIn('data-setting-type="range"', app_js)
        self.assertIn("function bindRuntimeSettingRanges(fieldsEl)", app_js)
        self.assertIn("bindRuntimeSettingRanges(fieldsEl);", app_js)
        self.assertIn("data-runtime-range-output", app_js)
        self.assertIn("data-runtime-range-suffix", app_js)

    def test_runtime_popup_saves_range_fields_as_numbers(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn('if (type === "number" || type === "range") {', app_js)


if __name__ == "__main__":
    unittest.main()
