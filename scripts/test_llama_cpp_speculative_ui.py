import ast
import re
import unittest
from pathlib import Path
from typing import Any, Dict


ROOT = Path(__file__).resolve().parents[1]


class LlamaCppSpeculativeUiTests(unittest.TestCase):
    def test_settings_offer_all_supported_speculative_methods(self):
        component = (ROOT / "frontend" / "src" / "settings" / "components" / "models" / "LlmModels.vue").read_text(encoding="utf-8")
        settings = (ROOT / "frontend" / "src" / "settings" / "components" / "ModelsSettings.vue").read_text(encoding="utf-8")
        backend = (ROOT / "tateros_app.py").read_text(encoding="utf-8")

        self.assertIn("Speculative decoding", component)
        self.assertIn('value="draft-mtp"', component)
        self.assertIn('value="draft-dflash"', component)
        self.assertIn('value="draft-dspark"', component)
        self.assertIn('"hydra_llama_cpp_speculative_method"', settings)
        self.assertIn('"hydra_llama_cpp_mtp_draft_tokens"', settings)
        for alias in ('"mtp": "draft-mtp"', '"dflash": "draft-dflash"', '"dspark": "draft-dspark"'):
            self.assertIn(alias, backend)

    def test_speculative_controls_use_generic_container(self):
        component = (ROOT / "frontend" / "src" / "settings" / "components" / "models" / "LlmModels.vue").read_text(encoding="utf-8")

        self.assertIn("v-if=\"draft.hydra_llama_cpp_mtp_enabled\"", component)
        self.assertIn('v-model="draft.hydra_llama_cpp_speculative_method"', component)
        self.assertNotIn("hydra-llama-mtp-extra", component)

    def test_llm_runtime_is_provider_scoped_and_restores_context_estimator(self):
        component = (ROOT / "frontend" / "src" / "settings" / "components" / "models" / "LlmModels.vue").read_text(encoding="utf-8")
        models = (ROOT / "frontend" / "src" / "settings" / "components" / "ModelsSettings.vue").read_text(encoding="utf-8")
        app_js = (ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        styles = (ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn('class="tm-llm-provider-picker"', component)
        self.assertNotIn('<details class="tm-form-card tm-details-card"', component)
        self.assertIn('v-if="runtimeProvider === \'hf_transformers\'"', component)
        self.assertIn('v-else-if="runtimeProvider === \'llama_cpp\'"', component)
        self.assertIn('v-else-if="runtimeProvider === \'mlx_lm\'"', component)
        self.assertIn("Context estimator", component)
        self.assertIn("refreshContextEstimate", component)
        self.assertIn("context-estimate-endpoint", models)
        self.assertIn('modelsContextEstimate: withBasePath("/api/runtime/context-estimate")', app_js)
        self.assertIn(".tm-llm-context-card", styles)

    def test_llm_boolean_controls_have_visible_state_rows(self):
        component = (ROOT / "frontend" / "src" / "settings" / "components" / "models" / "LlmModels.vue").read_text(encoding="utf-8")
        styles = (ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        for label in ("Flash attention", "GPU KV offload", "Lazy loading", "Trust remote model code"):
            self.assertIn(label, component)
        self.assertIn('class="tm-llm-switch"', component)
        self.assertIn('class="tm-llm-feature-toggle"', component)
        self.assertIn(".tm-llm-switch > input", styles)
        self.assertIn(".tm-llm-feature-toggle > span", styles)

    def test_local_inventory_classifies_speculative_sidecars(self):
        source = (ROOT / "tateros_app.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        wanted = {"_normalize_local_llm_speculative_method", "_local_llm_speculative_profile"}
        module = ast.Module(
            body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in wanted],
            type_ignores=[],
        )
        namespace = {"Any": Any, "Dict": Dict, "Path": Path, "re": re}
        exec(compile(module, str(ROOT / "tateros_app.py"), "exec"), namespace)
        classify = namespace["_local_llm_speculative_profile"]

        base = classify({"filename": "gemma-4-26B-A4B-it-UD-Q4_K_M.gguf"})
        mtp = classify({"filename": "gemma-4-26B-A4B-it-MTP-Q8_0.gguf"})
        dflash = classify({"filename": "gemma-4-26B-A4B-it-DFlash-Q8_0.gguf"})
        dspark = classify({"filename": "gemma-4-26B-A4B-it-DSpark-Q8_0.gguf"})
        metadata = classify({"filename": "opaque.gguf", "draft_method": "d-flash"})

        self.assertFalse(base["is_speculative_draft"])
        self.assertEqual(mtp["speculative_method"], "draft-mtp")
        self.assertEqual(dflash["speculative_method"], "draft-dflash")
        self.assertEqual(dspark["speculative_method"], "draft-dspark")
        self.assertEqual(metadata["speculative_detection_source"], "metadata")

    def test_model_pickers_separate_base_and_matching_draft_models(self):
        component = (ROOT / "frontend" / "src" / "settings" / "components" / "models" / "LlmModels.vue").read_text(encoding="utf-8")
        media = (ROOT / "frontend" / "src" / "settings" / "components" / "models" / "MediaModelCard.vue").read_text(encoding="utf-8")
        styles = (ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("function isSpeculativeDraft", component)
        self.assertIn("function speculativeMethodForModel", component)
        self.assertIn("function draftMatchesBase", component)
        self.assertIn("candidateRepo === baseRepo", component)
        self.assertIn("rows.filter((model) => !isSpeculativeDraft(model))", component)
        self.assertIn("speculativeMethodForModel(model) === method", component)
        self.assertIn("No compatible {{ activeSpeculative.label }} sidecar", component)
        self.assertIn("row.is_speculative_draft === true", media)
        self.assertIn(".tm-llm-draft-match.good", styles)


if __name__ == "__main__":
    unittest.main()
