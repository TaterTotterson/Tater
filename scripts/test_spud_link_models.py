from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import spud_link_models


class FakeRedis:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def hgetall(self, _key):
        return dict(self.values)


class FakeResponse:
    headers = {"Content-Type": "application/json"}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return b'{"ok":true,"text":"ready"}'


class SpudLinkModelRoutingTests(unittest.TestCase):
    def paired(self, **values):
        return FakeRedis(
            {
                "mode": "spudlet",
                "hub_url": "http://hub.local:8501",
                "node_token": "secret-token",
                **values,
            }
        )

    def test_existing_spudlet_only_routes_llm_by_default(self):
        with patch.object(spud_link_models, "remote_only_enabled", return_value=False):
            redis = self.paired()
            self.assertTrue(spud_link_models.should_use_hub("llm", redis_conn=redis))
            self.assertFalse(spud_link_models.should_use_hub("stt", redis_conn=redis))

    def test_edge_auto_and_explicit_routes(self):
        with patch.object(spud_link_models, "remote_only_enabled", return_value=True):
            self.assertTrue(spud_link_models.should_use_hub("face_id", redis_conn=self.paired()))
        redis = self.paired(
            model_routing_enabled="false",
            model_route_video="hub",
            model_route_tts="local",
        )
        self.assertTrue(spud_link_models.should_use_hub("video", redis_conn=redis))
        self.assertFalse(spud_link_models.should_use_hub("tts", redis_conn=redis))

    def test_gateway_uses_native_endpoint_and_pairing_token(self):
        captured = {}

        def urlopen(request, timeout):
            captured.update(
                url=request.full_url,
                authorization=request.headers.get("Authorization"),
                timeout=timeout,
                body=json.loads(request.data.decode("utf-8")),
            )
            return FakeResponse()

        with patch.object(spud_link_models.urllib.request, "urlopen", side_effect=urlopen):
            result = spud_link_models.request_json(
                "models/capabilities",
                payload={"probe": True},
                redis_conn=self.paired(),
                timeout=7.0,
            )
        self.assertEqual(result["text"], "ready")
        self.assertEqual(captured["url"], "http://hub.local:8501/api/spudlink/v1/models/capabilities")
        self.assertEqual(captured["authorization"], "Bearer secret-token")
        self.assertEqual(captured["body"], {"probe": True})

    def test_remote_runtime_inventory_groups_shared_base_model_roles(self):
        model = "TaterTotterson/gemma-4-26B-A4B-it-GGUF-Tater-NoThink::gemma-4-26B-A4B-it-UD-Q4_K_M.gguf"
        response = {
            "models": {
                "vision": {"available": True, "model": "gemma-4-26b-a4b-it-mlx"},
                "audio": {"available": True, "model": "base"},
                "video": {"available": True, "model": "base"},
            },
            "loaded_models": {
                "models": [
                    {
                        "category": "llm",
                        "kind_label": "LLM",
                        "provider": "llama_cpp",
                        "provider_label": "llama.cpp",
                        "model": model,
                        "device": "Metal",
                        "roles": ["Base LLM", "Vision", "Audio Understanding", "Video Understanding"],
                        "details": ["Roles Base LLM, Vision, Audio Understanding, Video Understanding"],
                    }
                ]
            },
        }

        rows = spud_link_models.build_remote_runtime_model_rows(
            response,
            routed_kinds=["llm", "vision", "audio", "video"],
        )

        self.assertIsNotNone(rows)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["model"], model)
        self.assertEqual(rows[0]["provider_label"], "llama.cpp")
        self.assertEqual(rows[0]["device"], "Spud Hub")
        self.assertEqual(rows[0]["memory_kind"], "remote")
        self.assertIn("Used for LLM, Vision, Audio Understanding, Video Understanding", rows[0]["details"])
        self.assertNotIn("gemma-4-26b-a4b-it-mlx", str(rows))

    def test_current_hub_empty_inventory_does_not_invent_loaded_models(self):
        rows = spud_link_models.build_remote_runtime_model_rows(
            {
                "models": {"video": {"available": True, "model": "base"}},
                "loaded_models": {"models": []},
            },
            routed_kinds=["video"],
        )
        self.assertEqual(rows, [])

    def test_older_hub_without_inventory_keeps_legacy_fallback_available(self):
        rows = spud_link_models.build_remote_runtime_model_rows(
            {"models": {"video": {"available": True, "model": "base"}}},
            routed_kinds=["video"],
        )
        self.assertIsNone(rows)

    def test_hub_capability_endpoint_exports_sanitized_runtime_inventory(self):
        source = (Path(__file__).resolve().parents[1] / "tateros_app.py").read_text(encoding="utf-8")
        self.assertIn('"loaded_models": _spud_link_public_loaded_models_payload()', source)
        self.assertIn('_spud_link_effective_modality_capability("vision", vision, base)', source)
        self.assertIn('_spud_link_effective_modality_capability("audio", audio, base)', source)
        self.assertIn('_spud_link_effective_modality_capability("video", video, base)', source)
        public_inventory = source[
            source.index("def _spud_link_public_loaded_models_payload(") : source.index(
                "def _spud_link_effective_modality_capability("
            )
        ]
        self.assertNotIn('"model_path"', public_inventory)
        self.assertNotIn('"model_root"', public_inventory)
        self.assertNotIn('"cache_key"', public_inventory)


if __name__ == "__main__":
    unittest.main()
