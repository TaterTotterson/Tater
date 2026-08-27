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


if __name__ == "__main__":
    unittest.main()
