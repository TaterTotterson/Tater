#!/usr/bin/env python3
from __future__ import annotations

import ast
import pathlib
import re
import unittest
from typing import Any, List


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
APP_PATH = REPO_ROOT / "tateros_app.py"
STYLES_PATH = REPO_ROOT / "tateros_static" / "styles.css"


def _css_rule(source: str, selector: str) -> str:
    matches = re.finditer(rf"{re.escape(selector)}\s*\{{(?P<body>[^}}]*)\}}", source)
    bodies = [match.group("body") for match in matches]
    if not bodies:
        raise AssertionError(f"Missing CSS rule: {selector}")
    return "\n".join(bodies)


def _load_size_helpers() -> dict[str, Any]:
    wanted = {
        "_hf_browser_object_value",
        "_hf_browser_param_size_label",
        "_hf_browser_size_number_text",
        "_hf_browser_safetensors_param_count",
        "_hf_browser_model_size_label",
    }
    tree = ast.parse(APP_PATH.read_text(encoding="utf-8"), filename=str(APP_PATH))
    functions = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted
    ]
    found = {node.name for node in functions}
    if found != wanted:
        raise AssertionError(f"Missing Hugging Face size helpers: {sorted(wanted - found)}")

    namespace: dict[str, Any] = {"Any": Any, "List": List, "re": re}
    module = ast.Module(body=functions, type_ignores=[])
    exec(compile(module, str(APP_PATH), "exec"), namespace)
    return namespace


class HuggingFaceModelBrowserSizeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.helpers = _load_size_helpers()

    def size_label(
        self,
        model_id: str,
        *,
        tags: list[str] | None = None,
        files: list[str] | None = None,
        model: dict[str, Any] | None = None,
        card_data: dict[str, Any] | None = None,
    ) -> str:
        return self.helpers["_hf_browser_model_size_label"](
            model_id,
            tags or [],
            files or [],
            model or {},
            card_data or {},
        )

    def test_integer_sizes_keep_trailing_zeroes(self) -> None:
        self.assertEqual(self.size_label("Muse-Glimmer-30B-GGUF-Tater-LowThink"), "30B")
        self.assertEqual(self.size_label("Llama-3.1-70B-Instruct"), "70B")

    def test_single_digit_and_decimal_sizes_remain_compact(self) -> None:
        self.assertEqual(self.size_label("Tiny-3B-Instruct"), "3B")
        self.assertEqual(self.size_label("Example-1.50B-Instruct"), "1.5B")

    def test_moe_size_labels_keep_integer_zeroes(self) -> None:
        self.assertEqual(self.size_label("Example-8x30B-MoE"), "8x30B")

    def test_structured_parameter_counts_take_precedence(self) -> None:
        model = {"safetensors": {"total": 30_000_000_000}}
        self.assertEqual(self.size_label("Example-3B", model=model), "30B")


class HuggingFaceModelBrowserLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.styles = STYLES_PATH.read_text(encoding="utf-8")

    def test_settings_card_does_not_trap_sticky_detail_panel(self) -> None:
        rule = _css_rule(self.styles, '.view-root[data-view="settings"] > .card')
        self.assertIn("overflow: visible", rule)

    def test_download_detail_panel_stays_in_view_on_desktop(self) -> None:
        rule = _css_rule(self.styles, ".hf-model-browser-detail")
        self.assertIn("position: sticky", rule)
        self.assertIn("align-self: start", rule)
        self.assertIn("max-height: calc(100dvh - 24px)", rule)
        self.assertIn("overflow-y: auto", rule)

    def test_download_detail_panel_returns_to_document_flow_on_mobile(self) -> None:
        media_start = self.styles.index("@media (max-width: 860px)")
        mobile_rule = _css_rule(self.styles[media_start:], ".hf-model-browser-detail")
        self.assertIn("position: static", mobile_rule)
        self.assertIn("max-height: none", mobile_rule)
        self.assertIn("overflow-y: visible", mobile_rule)


if __name__ == "__main__":
    unittest.main()
