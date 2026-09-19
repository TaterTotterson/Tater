#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class CorePopupRangeTests(unittest.TestCase):
    def test_runtime_popup_renders_and_binds_range_fields(self) -> None:
        field = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "CoreManagerField.vue").read_text(encoding="utf-8")

        self.assertIn("type === 'range'", field)
        self.assertIn('type="range"', field)
        self.assertIn("{{ modelValue ?? 0 }}{{ field.suffix || '' }}", field)
        self.assertIn('@input="update"', field)
        self.assertIn('@change="commit"', field)

    def test_runtime_popup_saves_range_fields_as_numbers(self) -> None:
        field = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "CoreManagerField.vue").read_text(encoding="utf-8")

        self.assertIn('["number", "range"].includes(type.value)', field)
        self.assertIn("Number(input.value)", field)


if __name__ == "__main__":
    unittest.main()
