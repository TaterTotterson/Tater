import asyncio
import base64
import unittest
from unittest.mock import patch

from plugins.automatic_plugin import AutomaticPlugin


class _FakeRedis:
    def __init__(self, data):
        self._data = data

    def hgetall(self, key):
        return dict(self._data.get(key, {}))


class AutomaticPluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = AutomaticPlugin()

    def test_settings_use_only_configured_category(self):
        fake = _FakeRedis(
            {
                "plugin_settings:Automatic111": {
                    "AUTOMATIC_URL": "http://current:7860",
                    "AUTOMATIC_SAMPLER": "Euler a",
                    "AUTOMATIC_TIMEOUT_SECONDS": "88",
                },
                "plugin_settings:Automatic": {
                    "AUTOMATIC_SAMPLER": "Legacy Sampler",
                    "AUTOMATIC_TIMEOUT_SECONDS": "15",
                },
            }
        )
        with patch("plugins.automatic_plugin.redis_client", fake):
            cfg = self.plugin._config()

        self.assertEqual(cfg["url"], "http://current:7860")
        self.assertEqual(cfg["timeout_seconds"], 88)
        self.assertEqual(cfg["sampler_name"], "Euler a")

    def test_build_payload_applies_overrides_and_seed(self):
        cfg = {
            "negative_prompt": "",
            "steps": 20,
            "cfg_scale": 7.0,
            "width": 1024,
            "height": 1024,
            "sampler_name": "DPM++ 2M",
            "scheduler": "Simple",
        }
        args = {
            "negative_prompt": "blurry",
            "steps": 31,
            "cfg_scale": 9,
            "width": 768,
            "height": 512,
            "sampler_name": "Euler a",
            "scheduler": "Karras",
            "seed": "12345",
        }
        payload = self.plugin._build_payload("a cat", args, cfg)
        self.assertEqual(payload["prompt"], "a cat")
        self.assertEqual(payload["negative_prompt"], "blurry")
        self.assertEqual(payload["steps"], 31)
        self.assertEqual(payload["cfg_scale"], 9.0)
        self.assertEqual(payload["width"], 768)
        self.assertEqual(payload["height"], 512)
        self.assertEqual(payload["sampler_name"], "Euler a")
        self.assertEqual(payload["scheduler"], "Karras")
        self.assertEqual(payload["seed"], 12345)

    def test_decode_image_data_supports_data_uri(self):
        raw = b"fakepngbytes"
        encoded = base64.b64encode(raw).decode("utf-8")
        image = self.plugin._decode_image_data(f"data:image/png;base64,{encoded}")
        self.assertEqual(image, raw)

    def test_run_requires_prompt(self):
        result = asyncio.run(self.plugin._run({}, llm_client=None))
        self.assertFalse(result.get("ok"), result)
        self.assertEqual(result.get("error", {}).get("code"), "missing_prompt")


if __name__ == "__main__":
    unittest.main()
