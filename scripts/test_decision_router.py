import unittest
from pathlib import Path
from unittest import mock

import helpers
import hydra as hydra_runtime


ROOT = Path(__file__).resolve().parents[1]


class DecisionRuntimeTests(unittest.TestCase):
    def test_decision_server_uses_small_isolated_systemone_profile(self):
        with (
            mock.patch.object(helpers, "_llama_cpp_native_free_port", return_value=19931),
            mock.patch.object(helpers, "_llama_cpp_n_gpu_layers", return_value=-1),
            mock.patch.object(helpers, "_llama_cpp_n_batch", return_value=4096),
            mock.patch.object(helpers, "_llama_cpp_n_ubatch", return_value=4096),
            mock.patch.object(helpers, "_llama_cpp_flash_attn_enabled", return_value=True),
            mock.patch.object(helpers, "_llama_cpp_offload_kqv_enabled", return_value=True),
        ):
            command, metadata = helpers._llama_cpp_native_decision_server_command(
                server_bin="/tmp/llama-server",
                model_path="/tmp/Julia-1-Q8_0.gguf",
            )

        self.assertEqual(command[command.index("--alias") + 1], "tater-decision")
        self.assertEqual(command[command.index("--parallel") + 1], "1")
        self.assertEqual(command[command.index("--ctx-size") + 1], "2048")
        self.assertEqual(command[command.index("--batch-size") + 1], "2048")
        self.assertEqual(command[command.index("--cache-ram") + 1], "0")
        self.assertNotIn("--spec-type", command)
        self.assertEqual(metadata["profile"], "decision")
        self.assertFalse(metadata["mtp_enabled"])

    def test_systemone_request_is_sent_to_dedicated_engine(self):
        engine = mock.Mock()
        engine.request.return_value = {
            "answers": {
                "route": {
                    "choice": "chat",
                    "confidence": 0.98,
                    "probabilities": {"chat": 0.99, "needs_tool": 0.01},
                }
            }
        }
        with mock.patch.object(
            helpers,
            "_load_llama_cpp_decision_engine_bundle",
            return_value={"engine": engine},
        ):
            result = helpers.run_llama_cpp_decision_model(
                "ggml-org/Julia-1-GGUF::Julia-1-Q8_0.gguf",
                state={"current_message": "hello"},
                questions={"route": {"type": "choice", "instructions": "route", "criteria": {"chat": None, "needs_tool": None}}},
            )

        self.assertEqual(result["answers"]["route"]["choice"], "chat")
        op, payload = engine.request.call_args.args[:2]
        self.assertEqual(op, "decision")
        self.assertEqual(payload["state"]["current_message"], "hello")


class DecisionHydraTests(unittest.IsolatedAsyncioTestCase):
    def test_router_configuration_reads_per_model_threshold(self):
        redis = mock.Mock()
        redis.get.side_effect = lambda key: {
            hydra_runtime.HYDRA_DECISION_ROUTER_MODE_KEY: "active",
            hydra_runtime.HYDRA_DECISION_ROUTER_MODEL_KEY: "kev.gguf",
            hydra_runtime.HYDRA_DECISION_ROUTER_CONFIDENCE_KEY: "0.78",
        }.get(key)

        configured = hydra_runtime._configured_decision_router(redis)

        self.assertEqual(configured["mode"], "active")
        self.assertEqual(configured["model"], "kev.gguf")
        self.assertEqual(configured["confidence"], 0.78)

    async def test_router_normalizes_systemone_choice_and_confidence(self):
        response = {
            "answers": {
                "route": {
                    "choice": "needs_tool",
                    "confidence": 0.96,
                    "probabilities": {"chat": 0.02, "needs_tool": 0.98},
                }
            },
            "usage": {"input_tokens": 88, "output_tokens": 0},
        }
        with mock.patch.object(hydra_runtime, "run_llama_cpp_decision_model", return_value=response) as decision:
            result = await hydra_runtime._run_decision_router(
                model="julia.gguf",
                current_user_text="turn on the kitchen light",
                platform="webui",
                available_tools=[
                    {"id": "homeassistant", "description": "Read and control Home Assistant devices"}
                ],
            )

        self.assertEqual(result["choice"], "needs_tool")
        self.assertEqual(result["confidence"], 0.96)
        self.assertEqual(result["probability"], 0.98)
        self.assertEqual(result["usage"]["output_tokens"], 0)
        state = decision.call_args.kwargs["state"]
        self.assertIn("Current user message:\nturn on the kitchen light", state)
        self.assertIn("Routing policy:", state)
        self.assertIn("acknowledgments, refusals", state)
        self.assertIn("Do not infer an unstated action", state)
        self.assertIn("If no listed tool clearly fulfills the message, choose chat.", state)
        self.assertIn("Available Tater tools:", state)
        self.assertIn("homeassistant: Read and control Home Assistant devices", state)
        questions = decision.call_args.kwargs["questions"]
        self.assertEqual(set(questions["route"]["criteria"]), {"chat", "needs_tool"})
        self.assertIn("normal chat", questions["route"]["instructions"])
        self.assertNotIn("homeassistant: Read and control Home Assistant devices", str(questions))
        self.assertNotIn("Hermes", str(questions))
        self.assertNotIn("Astraeus", str(questions))

    async def test_benchmark_scores_accuracy_coverage_and_safety_without_tools(self):
        expected = {
            row["message"]: row["expected"]
            for row in hydra_runtime.DECISION_ROUTER_BENCHMARK_CASES
        }

        async def fake_router(**kwargs):
            choice = expected[kwargs["current_user_text"]]
            return {
                "choice": choice,
                "probability": 0.82,
                "confidence": 0.64,
                "elapsed_ms": 12,
                "usage": {"input_tokens": 900},
            }

        with mock.patch.object(hydra_runtime, "_run_decision_router", side_effect=fake_router) as router:
            result = await hydra_runtime.run_decision_router_benchmark(
                model="kev.gguf",
                confidence_threshold=0.78,
            )

        self.assertEqual(result["total"], 46)
        self.assertEqual(result["chat_total"], 24)
        self.assertEqual(result["chat_correct"], 24)
        self.assertEqual(result["tool_total"], 22)
        self.assertEqual(result["tool_correct"], 22)
        self.assertEqual(result["multi_tool_total"], 4)
        self.assertEqual(result["multi_tool_correct"], 4)
        self.assertEqual(result["average_confidence_percent"], 64.0)
        self.assertEqual(result["accuracy_percent"], 100.0)
        self.assertEqual(result["coverage_percent"], 100.0)
        self.assertEqual(result["precision_percent"], 100.0)
        self.assertEqual(result["unsafe"], 0)
        self.assertEqual(router.call_count, 46)
        self.assertTrue(all(call.kwargs["platform"] == "benchmark" for call in router.call_args_list))
        self.assertEqual(
            sum(1 for row in result["rows"] if row["category"] == "multi_tool"),
            4,
        )

    async def test_compact_capabilities_drop_tool_schemas_for_router_and_chat(self):
        compact = hydra_runtime._compact_tool_capabilities(
            "Enabled verba tools:\n"
            '- id: homeassistant | description: Read and control devices | usage: {"function":"homeassistant","arguments":{"nl":"..."}}\n'
            "Kernel tools:\n"
            '- id: websearch | description: Search current web information | usage: {"function":"websearch","arguments":{"query":"..."}}'
        )

        self.assertEqual(
            compact,
            [
                {"id": "homeassistant", "description": "Read and control devices"},
                {"id": "websearch", "description": "Search current web information"},
            ],
        )
        context = hydra_runtime._chat_capability_context(compact)
        self.assertIn("homeassistant: Read and control devices", context)
        self.assertIn("no listed tool can provide it", context)
        self.assertNotIn("arguments", context)

    async def test_planner_only_route_lock_is_part_of_dynamic_payload(self):
        captured = {}

        async def fake_structured_chat(_client, **kwargs):
            captured.update(kwargs)
            return {
                "mode": "execute",
                "relation": "standalone",
                "goal": "Turn on the light",
                "question": "",
                "pending_interaction": {
                    "kind": "none",
                    "goal": "",
                    "tool_hint": "",
                    "known_details": [],
                    "missing_details": [],
                    "choices": [],
                    "question": "",
                },
                "steps": [{"step_id": 1, "intent": "turn on light", "nl": "Turn on the kitchen light", "tool_hint": "homeassistant"}],
            }

        with mock.patch.object(hydra_runtime, "llm_structured_chat", side_effect=fake_structured_chat):
            await hydra_runtime._run_astraeus_plan(
                llm_client=object(),
                platform="webui",
                current_user_text="turn on the kitchen light",
                turn_request_text="turn on the kitchen light",
                topic_seed="lights",
                topic_shift_seed=False,
                history=[],
                prior_state=None,
                pending_interaction=None,
                memory_context=None,
                capability_catalog="homeassistant: control devices",
                available_tool_ids={"homeassistant"},
                platform_preamble="",
                max_tokens=None,
                operating_mode="planner_only",
            )

        dynamic = captured["messages"][-1]["content"]
        self.assertIn('"route_lock":"planner_only"', dynamic)


class DecisionUiTests(unittest.TestCase):
    def test_models_ui_exposes_decision_download_and_router_modes(self):
        llm_ui = (ROOT / "frontend/src/settings/components/models/LlmModels.vue").read_text(encoding="utf-8")
        models_ui = (ROOT / "frontend/src/settings/components/models/HuggingFaceModels.vue").read_text(encoding="utf-8")
        settings_ui = (ROOT / "frontend/src/settings/components/ModelsSettings.vue").read_text(encoding="utf-8")
        self.assertIn("Decision Router", llm_ui)
        self.assertIn("hydra_decision_router_mode === 'observe'", llm_ui)
        self.assertIn("hydra_decision_router_mode === 'active'", llm_ui)
        self.assertIn("hydra_decision_router_confidence", llm_ui)
        self.assertIn("Run router test", llm_ui)
        self.assertIn("Normal chat", llm_ui)
        self.assertIn("Multi-tool requests", llm_ui)
        self.assertIn("Multi-tool request", llm_ui)
        self.assertIn("Model confidence", llm_ui)
        self.assertIn("Recent live decisions", llm_ui)
        self.assertIn("modelsDecisionResults", settings_ui)
        self.assertIn('{ value: "decision", label: "Decision routing" }', models_ui)
        self.assertLess(llm_ui.index("Base model pool"), llm_ui.index("Beast mode</button>"))
        self.assertLess(llm_ui.index("Base model pool</button>"), llm_ui.index("Decision Router</button>"))
        self.assertLess(llm_ui.index("Decision Router</button>"), llm_ui.index("Beast mode</button>"))
        self.assertLess(llm_ui.index("Beast mode</button>"), llm_ui.index("Tater Open WebUI</button>"))

    def test_decision_results_endpoint_reads_live_ledger_scores(self):
        app_source = (ROOT / "tateros_app.py").read_text(encoding="utf-8")
        self.assertIn('@app.get("/api/settings/decision-router/results")', app_source)
        self.assertIn('astraeus_debug.get("decision_router")', app_source)
        self.assertIn('"probability": round(probability, 4)', app_source)
        self.assertIn('"confidence": round(confidence, 4)', app_source)
        self.assertIn('"route_effect": route_effect', app_source)

    def test_decision_search_uses_hub_tag_without_a_hardcoded_pick(self):
        app_source = (ROOT / "tateros_app.py").read_text(encoding="utf-8")
        picks_source = app_source.split("HF_BROWSER_TATER_PICKS:", 1)[1].split(
            "def _hf_browser_tater_pick_owner", 1
        )[0]
        self.assertNotIn("ggml-org/Julia-1-GGUF", picks_source)
        self.assertIn(
            'return "decision-model" if _normalize_hf_browser_task(task) == "decision" else ""',
            app_source,
        )
        self.assertIn(
            'return "text-classification" if _normalize_hf_browser_task(task) == "decision" else ""',
            app_source,
        )
        self.assertIn('params["expand[]"]', app_source)
        self.assertIn('_hf_browser_primary_pipeline_tag(model) == "text-classification"', app_source)
        self.assertIn("is not a native llama.cpp System One decision model", app_source)

    def test_installed_decision_models_use_native_gguf_metadata_and_refresh(self):
        app_source = (ROOT / "tateros_app.py").read_text(encoding="utf-8")
        models_source = (ROOT / "frontend/src/settings/components/ModelsSettings.vue").read_text(encoding="utf-8")
        browser_source = (ROOT / "frontend/src/settings/components/models/HuggingFaceModels.vue").read_text(encoding="utf-8")

        self.assertIn("def _local_llm_decision_support_from_gguf", app_source)
        self.assertIn('key.endswith(".decision.type")', app_source)
        self.assertIn('key == "tokenizer.chat_template.systemone"', app_source)
        self.assertIn("0 if cache_scan else 1", app_source)
        self.assertIn('["routing", "vision", "audio-understanding"]', models_source)
        self.assertIn("void refreshLocalModels()", models_source)
        self.assertIn('emit("local-models"', browser_source)


if __name__ == "__main__":
    unittest.main()
