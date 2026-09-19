#!/usr/bin/env python3
from __future__ import annotations

import ast
import pathlib
import re
import unittest
from typing import Any, List


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
APP_PATH = REPO_ROOT / "tateros_app.py"
STYLES_PATH = REPO_ROOT / "frontend" / "src" / "tater-ui.css"
COMPONENT_PATH = REPO_ROOT / "frontend" / "src" / "settings" / "components" / "models" / "HuggingFaceModels.vue"


def _css_rule(source: str, selector: str) -> str:
    matches = re.finditer(rf"{re.escape(selector)}\s*\{{(?P<body>[^}}]*)\}}", source)
    bodies = [match.group("body") for match in matches]
    if not bodies:
        raise AssertionError(f"Missing CSS rule: {selector}")
    return "\n".join(bodies)


def _load_size_helpers() -> dict[str, Any]:
    wanted = {
        "_normalize_hydra_llm_provider",
        "_is_local_hydra_llm_provider",
        "_hf_browser_object_value",
        "_hf_browser_param_size_label",
        "_hf_browser_size_number_text",
        "_hf_browser_safetensors_param_count",
        "_hf_browser_model_size_label",
        "_hf_browser_download_provider",
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

    namespace: dict[str, Any] = {
        "Any": Any,
        "List": List,
        "re": re,
        "HYDRA_LLM_PROVIDER_HF_TRANSFORMERS": "hf_transformers",
        "HYDRA_LLM_PROVIDER_LLAMA_CPP": "llama_cpp",
        "HYDRA_LLM_PROVIDER_LLAMA_CPP_REMOTE": "llama_cpp_remote",
        "HYDRA_LLM_PROVIDER_MLX_LM": "mlx_lm",
        "HYDRA_LLM_PROVIDER_OPENAI_COMPATIBLE": "openai_compatible",
        "HYDRA_LLM_PROVIDER_SPUD_LINK": "spud_link",
    }
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

    def test_download_provider_recovers_gguf_label_and_file_hints(self) -> None:
        resolve = self.helpers["_hf_browser_download_provider"]
        expected = "llama_cpp"
        self.assertEqual(resolve("llama.cpp / GGUF"), expected)
        self.assertEqual(
            resolve(
                "",
                repo_id="unsloth/Qwen3-4B-Instruct-2507-GGUF",
                model_id="unsloth/Qwen3-4B-Instruct-2507-GGUF::Qwen3-4B-Instruct-2507-Q4_K_M.gguf",
                filename="Qwen3-4B-Instruct-2507-Q4_K_M.gguf",
            ),
            expected,
        )


class HuggingFaceModelBrowserLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.styles = STYLES_PATH.read_text(encoding="utf-8")
        cls.component = COMPONENT_PATH.read_text(encoding="utf-8")
        cls.app = APP_PATH.read_text(encoding="utf-8")

    def test_download_list_stays_visible_on_desktop(self) -> None:
        rule = _css_rule(self.styles, ".tm-hf-download-center")
        self.assertIn("position: sticky", rule)

    def test_model_details_use_a_viewport_modal(self) -> None:
        rule = _css_rule(self.styles, ".tm-hf-detail-modal")
        self.assertIn("width: min(880px, 100%)", rule)
        self.assertIn("max-height: min(90dvh, 900px)", rule)
        self.assertIn("<PopupTransition :open=\"detailOpen\"", self.component)

    def test_download_list_and_activity_share_one_sticky_center(self) -> None:
        self.assertIn('class="tm-form-card tm-hf-download-center"', self.component)
        self.assertNotIn('class="tm-form-card tm-downloads"', self.component)
        self.assertIn('class="tm-hf-activity"', self.component)
        self.assertIn('v-if="warmup.running && activeDownload" class="tm-hf-current-progress"', self.component)
        self.assertIn(':value="Number(activeDownload.progress || 0)"', self.component)

    def test_tater_theme_and_clear_three_step_flow_are_present(self) -> None:
        self.assertIn(".tm-hf-guide::after", self.styles)
        self.assertIn('class="tm-hf-steps"', self.component)
        self.assertIn("<strong>Runtime</strong>", self.component)
        self.assertIn("<strong>Choose</strong>", self.component)
        self.assertIn("<strong>Auto-download</strong>", self.component)

    def test_library_controls_use_a_compact_isolated_toolbar(self) -> None:
        guide_rule = _css_rule(self.styles, ".tm-hf-guide")
        field_rule = _css_rule(self.styles, ".tm-hf-filters .tm-field")
        search_rule = _css_rule(self.styles, ".tm-hf-search")
        self.assertIn("gap: 7px", guide_rule)
        self.assertIn("padding: 10px 12px", guide_rule)
        self.assertIn("padding: 0", field_rule)
        self.assertIn("border: 0", field_rule)
        self.assertIn("padding: 0", search_rule)
        self.assertIn('class="tm-hf-search"', self.component)
        self.assertNotIn('class="tm-search"', self.component)

    def test_model_browse_switch_is_prominent_but_compact(self) -> None:
        switch_rule = _css_rule(self.styles, ".tm-hf-view-switch")
        button_rule = _css_rule(self.styles, ".tm-hf-view-switch button")
        active_rule = _css_rule(self.styles, ".tm-hf-view-switch button.active")
        self.assertIn("width: fit-content", switch_rule)
        self.assertIn("overflow-x: auto", switch_rule)
        self.assertIn("min-height: 30px", button_rule)
        self.assertIn("font-size: .64rem", button_rule)
        self.assertIn("linear-gradient", active_rule)

    def test_vue_browser_builds_a_multi_model_download_request(self) -> None:
        self.assertIn("const downloadList = ref<DownloadChoice[]>([]);", self.component)
        self.assertIn("items: queued.map((item) => ({", self.component)
        self.assertIn("const queued = downloadList.value.slice(0, 32);", self.component)
        self.assertIn("downloadList.value.filter((item) => !queuedKeys.has(item.key))", self.component)

    def test_model_choices_auto_start_and_continue_sequential_queue(self) -> None:
        self.assertIn("function scheduleQueuedDownload(delay = 180)", self.component)
        self.assertIn("scheduleQueuedDownload();", self.component)
        self.assertIn("if (downloadList.value.length) scheduleQueuedDownload();", self.component)
        self.assertIn("if (result.already_running)", self.component)
        self.assertIn("Files download one at a time.", self.component)
        self.assertIn("Retry queue", self.component)
        self.assertNotIn("Download selected file", self.component)

    def test_waiting_queue_and_current_file_have_dedicated_visual_states(self) -> None:
        waiting_rule = _css_rule(self.styles, ".tm-hf-waiting")
        current_rule = _css_rule(self.styles, ".tm-hf-current-progress")
        auto_rule = _css_rule(self.styles, ".tm-hf-auto-pill")
        self.assertIn("display: grid", waiting_rule)
        self.assertIn("linear-gradient", current_rule)
        self.assertIn("border-radius: 999px", auto_rule)

    def test_installed_models_and_multi_file_gguf_selection_are_guarded(self) -> None:
        self.assertIn("function isTargetInstalled", self.component)
        self.assertIn("directTargetInstalled(model)", self.component)
        self.assertIn("Files already on this Tater are disabled.", self.component)
        self.assertIn(":disabled=\"fileInstalled(file)\"", self.component)
        self.assertIn("selectedFiles.value.forEach((filename)", self.component)

    def test_download_endpoint_accepts_batch_and_legacy_requests(self) -> None:
        self.assertIn("items: List[HfModelDownloadItemRequest] = Field(default_factory=list)", self.app)
        self.assertIn("list(request.items or []) or [request]", self.app)
        self.assertIn("targets = _dedupe_hf_llm_warmup_targets(targets)", self.app)
        self.assertIn('result["queued_count"] = len(targets)', self.app)
        self.assertIn("provider_token = _hf_browser_download_provider(", self.app)
        self.assertIn("provider: normalizeProvider(item.provider)", self.component)


if __name__ == "__main__":
    unittest.main()
