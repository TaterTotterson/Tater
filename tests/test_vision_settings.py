import unittest
from unittest.mock import patch

import vision_settings as vs


class VisionSettingsTests(unittest.TestCase):
    def test_get_vision_settings_prefers_shared_key(self):
        with patch.object(vs, "redis_client") as redis_mock:
            redis_mock.hgetall.return_value = {
                "api_base": "http://vision.local:1234",
                "model": "qwen-vision",
                "api_key": "shared-key",
            }

            result = vs.get_vision_settings(
                default_api_base="http://default.local:1234",
                default_model="default-model",
            )

        self.assertEqual(result["api_base"], "http://vision.local:1234")
        self.assertEqual(result["model"], "qwen-vision")
        self.assertEqual(result["api_key"], "shared-key")

    def test_get_vision_settings_uses_defaults_when_shared_missing(self):
        with patch.object(vs, "redis_client") as redis_mock:
            redis_mock.hgetall.return_value = {}
            result = vs.get_vision_settings(
                default_api_base="http://default.local:1234",
                default_model="default-model",
            )

        self.assertEqual(result["api_base"], "http://default.local:1234")
        self.assertEqual(result["model"], "default-model")
        self.assertIsNone(result["api_key"])

    def test_get_vision_settings_ignores_legacy_style_fields(self):
        with patch.object(vs, "redis_client") as redis_mock:
            redis_mock.hgetall.return_value = {
                "VISION_API_BASE": "http://legacy.local:1234",
                "VISION_MODEL": "legacy-model",
                "VISION_API_KEY": "legacy-key",
            }
            result = vs.get_vision_settings(
                default_api_base="http://default.local:1234",
                default_model="default-model",
            )

        self.assertEqual(result["api_base"], "http://default.local:1234")
        self.assertEqual(result["model"], "default-model")
        self.assertIsNone(result["api_key"])

    def test_save_vision_settings_writes_canonical_key(self):
        with patch.object(vs, "redis_client") as redis_mock:
            vs.save_vision_settings(
                api_base="http://save.local:1234/",
                model="saved-model",
                api_key="saved-key",
            )

        redis_mock.hset.assert_called_once_with(
            vs.VISION_SETTINGS_KEY,
            mapping={
                "api_base": "http://save.local:1234",
                "model": "saved-model",
                "api_key": "saved-key",
            },
        )


if __name__ == "__main__":
    unittest.main()
