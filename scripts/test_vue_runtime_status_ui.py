#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueRuntimeStatusTests(unittest.TestCase):
    def test_runtime_status_uses_shared_vue_bundle_with_legacy_fallback(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")

        self.assertIn("async function mountVueRuntimeStatus", app_js)
        self.assertIn("module.mountRuntimeStatus", app_js)
        self.assertIn("state.runtimeVueController.setHealth", app_js)
        self.assertIn("state.runtimeVueController.setStatus", app_js)
        self.assertIn("Falling back to the legacy runtime status UI", app_js)
        self.assertIn("bindRuntimeSummary();", app_js)
        self.assertIn("export function mountRuntimeStatus", entry)

    def test_runtime_popup_keeps_live_polling_and_model_controls(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "runtime" / "RuntimeStatus.vue").read_text(encoding="utf-8")

        for contract in (
            "props.options.endpoints.breakdown",
            "props.options.endpoints.unloadModel",
            "window.setInterval",
            "5000",
            "unloadModel(model)",
            "onBreakdownChange",
            "onHealthRefresh",
            'refresh=true',
            'event.key === "Escape"',
        ):
            self.assertIn(contract, source)
        self.assertNotIn("innerHTML", source)

    def test_runtime_popup_includes_every_existing_statistics_area(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "runtime" / "RuntimeStatus.vue").read_text(encoding="utf-8")

        for heading in (
            "Loaded Runtime Models",
            "GPU Devices",
            "Hydra Jobs",
            "LLM Calls",
            "Vision Calls",
            "Estimated Chat Context Window",
            "Prompt Composition",
        ):
            self.assertIn(heading, source)

    def test_runtime_breakdown_still_updates_shared_cache_consumers(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn("state.runtimeBreakdownPayload = payload || {};", app_js)
        self.assertIn('new CustomEvent("tater:runtime-breakdown-updated"', app_js)
        self.assertIn('breakdown: withBasePath("/api/runtime/breakdown")', app_js)
        self.assertIn('unloadModel: withBasePath("/api/runtime/local-llm/unload")', app_js)

    def test_unified_memory_hardware_avoids_a_duplicate_vram_meter(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "runtime" / "RuntimeStatus.vue").read_text(encoding="utf-8")

        self.assertIn('resource(unified ? "Unified" : "RAM"', source)
        self.assertIn('if (!unified) resources.push(resource("VRAM"', source)
        self.assertIn('metric(unified ? "Unified Memory" : "System RAM"', source)
        self.assertIn('if (!unified) values.push(metric("System VRAM"', source)

    def test_runtime_styles_cover_pill_modal_and_responsive_layouts(self) -> None:
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        for selector in (
            ".runtime-summary.runtime-summary-vue-host",
            ".tr-pill {",
            ".tr-resource {",
            ".tr-modal {",
            ".tr-grid {",
            ".tr-meter-grid {",
            ".tr-turns {",
        ):
            self.assertIn(selector, styles)
        self.assertIn("@media (max-width: 860px)", styles)
        self.assertIn("@media (max-width: 620px)", styles)


if __name__ == "__main__":
    unittest.main()
