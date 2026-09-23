#!/usr/bin/env python3
from __future__ import annotations

import ast
import pathlib
import types
import typing
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueRuntimeStatusTests(unittest.TestCase):
    def test_runtime_status_is_owned_by_the_shared_vue_shell(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        shell = (REPO_ROOT / "frontend" / "src" / "shell" / "AppShell.vue").read_text(encoding="utf-8")

        self.assertIn('import RuntimeStatus from "../runtime/RuntimeStatus.vue"', shell)
        self.assertIn("<RuntimeStatus", shell)
        self.assertIn(':assistant-name="branding.firstName"', shell)
        self.assertIn("function setHealth", shell)
        self.assertIn("function setStatus", shell)
        self.assertIn("function createRuntimeVueOptions()", app_js)
        self.assertNotIn("mountVueRuntimeStatus", app_js)
        self.assertNotIn("legacy runtime status", app_js.lower())

    def test_runtime_monitor_uses_streaming_telemetry_with_detail_polling_fallback(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "runtime" / "RuntimeStatus.vue").read_text(encoding="utf-8")

        for contract in (
            "props.options.endpoints.telemetry",
            "props.options.endpoints.breakdown",
            "props.options.endpoints.unloadModel",
            "new EventSource",
            'addEventListener("telemetry"',
            'document.addEventListener("visibilitychange"',
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
        self.assertNotIn(">Refresh</button>", source)
        self.assertIn("assistantRuntimeLabel", source)
        self.assertNotIn("<strong>Tater runtime</strong>", source)

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

        self.assertIn('class="tr-tab-mark"', source)
        self.assertNotIn('class="tr-modal-badge"', source)
        self.assertNotIn('class="tr-overview-mark"', source)
        self.assertNotIn('class="tr-pill-orb"', source)
        for view in ("tr-activity-view", "tr-models-view", "tr-context-view", "tr-view-hero"):
            self.assertIn(view, source)

    def test_llm_calls_show_queued_and_running_states(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "runtime" / "RuntimeStatus.vue").read_text(encoding="utf-8")

        for contract in ("llm.running_total", "llm.queued_total", "call.state_label", "call.state_age_seconds"):
            self.assertIn(contract, source)

    def test_remote_models_show_hub_status_without_fake_memory_estimates(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "runtime" / "RuntimeStatus.vue").read_text(encoding="utf-8")

        self.assertIn("model.remote", source)
        self.assertIn("model.remote ? 'Spud Hub'", source)
        self.assertIn('const estimate = model.remote ? ""', source)

    def test_runtime_breakdown_is_reactive_and_uses_shell_endpoints(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        source = (REPO_ROOT / "frontend" / "src" / "runtime" / "RuntimeStatus.vue").read_text(encoding="utf-8")

        self.assertIn("const breakdown = ref<JsonRow>({})", source)
        self.assertIn("breakdown.value = payload || {}", source)
        self.assertIn("props.options.onBreakdownChange?.(payload || {})", source)
        self.assertIn('breakdown: withBasePath("/api/runtime/breakdown")', app_js)
        self.assertIn('telemetry: withBasePath("/api/runtime/telemetry")', app_js)
        self.assertIn('unloadModel: withBasePath("/api/runtime/local-llm/unload")', app_js)
        self.assertNotIn("runtimeBreakdownPayload", app_js)

    def test_unified_memory_hardware_avoids_a_duplicate_vram_meter(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "runtime" / "RuntimeStatus.vue").read_text(encoding="utf-8")

        self.assertIn('resource(unified ? "MEM" : "RAM"', source)
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
            ".tr-live-state {",
            ".tr-tabs {",
            ".tr-overview-hero {",
            ".tr-grid {",
            ".tr-meter-grid {",
            ".tr-turns {",
        ):
            self.assertIn(selector, styles)
        self.assertIn("@media (max-width: 860px)", styles)
        self.assertIn("@media (max-width: 620px)", styles)

    def test_runtime_telemetry_backend_streams_fast_cpu_ram_and_cached_gpu(self) -> None:
        source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")

        for contract in (
            "RUNTIME_HARDWARE_TELEMETRY_INTERVAL_SECONDS = 5",
            "RUNTIME_HARDWARE_TELEMETRY_STALE_SECONDS = 15",
            "RUNTIME_TELEMETRY_STREAM_INTERVAL_SECONDS = 1.0",
            "get_system_hardware_snapshot(include_vram_probe=False)",
            "async def _stream_runtime_telemetry(request: Request)",
            '@app.get("/api/runtime/telemetry")',
            'media_type="text/event-stream"',
            "_stream_runtime_telemetry(request)",
        ):
            self.assertIn(contract, source)

    def test_local_face_id_is_included_in_runtime_model_inventory(self) -> None:
        source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")

        for contract in (
            "def _runtime_face_id_model_rows()",
            'category="face_id"',
            'kind_label="Face ID"',
            'provider_label="Face ID • Tater"',
            'row["managed_by"] = "Settings › Models › Face ID"',
            "*_runtime_face_id_model_rows()",
        ):
            self.assertIn(contract, source)

        wanted = {
            "_runtime_model_memory_kind_from_device",
            "_runtime_managed_model_row",
            "_runtime_face_id_model_rows",
        }
        tree = ast.parse(source)
        functions = [
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name in wanted
        ]
        namespace = {
            "Any": typing.Any,
            "Dict": typing.Dict,
            "List": typing.List,
            "Optional": typing.Optional,
            "redis_client": object(),
        }
        status = {
            "enabled": True,
            "loaded": True,
            "local_only": True,
            "model": "AdaFace IR-50 · WebFace4M",
            "model_id": "adaface_ir50_webface4m",
            "device_name": "METAL",
            "accelerator": "metal",
            "detector_backend": "retinaface",
            "distance_metric": "cosine",
            "model_pack_version": "6",
            "model_pack_path": "/runtime/models/face-id",
            "loaded_at": 123.0,
        }
        namespace["face_identity"] = types.SimpleNamespace(runtime_status=lambda _client: status)
        exec(compile(ast.Module(body=functions, type_ignores=[]), "tateros_app.py", "exec"), namespace)

        rows = namespace["_runtime_face_id_model_rows"]()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["category"], "face_id")
        self.assertEqual(rows[0]["model"], "AdaFace IR-50 · WebFace4M")
        self.assertEqual(rows[0]["device"], "METAL")
        self.assertEqual(rows[0]["memory_kind"], "unified")
        self.assertTrue(rows[0]["loaded"])

        status.update({"local_only": False, "routed_via": "spud_link"})
        self.assertEqual(namespace["_runtime_face_id_model_rows"](), [])


if __name__ == "__main__":
    unittest.main()
