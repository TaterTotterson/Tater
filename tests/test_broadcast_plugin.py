import asyncio
import unittest
from unittest.mock import patch

from plugins.broadcast import BroadcastPlugin


class _Resp:
    def __init__(self, status_code: int):
        self.status_code = status_code


class BroadcastPluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = BroadcastPlugin()

    def test_extract_announcement_arg_accepts_aliases(self):
        self.assertEqual(
            self.plugin._extract_announcement_arg({"message": "  Dinner is ready  "}),
            "Dinner is ready",
        )
        self.assertEqual(
            self.plugin._extract_announcement_arg({"content": "Lights out in 5 minutes"}),
            "Lights out in 5 minutes",
        )

    def test_targets_filters_invalid_and_duplicates(self):
        targets = self.plugin._targets(
            {
                "DEVICE_1": "media_player.kitchen",
                "DEVICE_2": "media_player.kitchen",
                "DEVICE_3": "sensor.kitchen_temp",
                "DEVICE_4": " media_player.office ",
            }
        )
        self.assertEqual(targets, ["media_player.kitchen", "media_player.office"])

    def test_tts_speak_contains_errors_per_device(self):
        def fake_post(url, headers=None, json=None, timeout=None):
            mp = (json or {}).get("media_player_entity_id")
            if mp == "media_player.ok":
                return _Resp(200)
            if mp == "media_player.fallback_ok":
                if url.endswith("/tts/speak"):
                    return _Resp(500)
                return _Resp(200)
            if mp == "media_player.fallback_fail":
                if url.endswith("/tts/speak"):
                    raise RuntimeError("connection dropped")
                return _Resp(503)
            raise AssertionError(f"Unexpected target in test: {mp}")

        with patch("plugins.broadcast.requests.post", side_effect=fake_post):
            ok_count, failures = self.plugin._tts_speak(
                "http://ha.local:8123",
                "token",
                "tts.piper",
                ["media_player.ok", "media_player.fallback_ok", "media_player.fallback_fail"],
                "Testing announcement",
                15,
            )

        self.assertEqual(ok_count, 2)
        self.assertEqual(len(failures), 1)
        self.assertIn("media_player.fallback_fail", failures[0])

    def test_broadcast_decodes_homeassistant_settings(self):
        fake_settings = {
            "DEVICE_1": "media_player.office",
            "TTS_ENTITY": "tts.piper",
            "REQUEST_TIMEOUT_SECONDS": "15",
        }
        fake_ha_settings = {b"HA_BASE_URL": b"http://ha.local:8123", b"HA_TOKEN": b"abc123"}

        with patch.object(self.plugin, "_get_settings", return_value=fake_settings), patch(
            "plugins.broadcast.redis_client.hgetall",
            return_value=fake_ha_settings,
        ), patch.object(self.plugin, "_tts_speak", return_value=(1, [])):
            result = asyncio.run(self.plugin._broadcast("Dinner is ready", llm_client=None))

        self.assertIn("Broadcast sent to 1/1 devices", result)
        self.assertIn("Announcement: Dinner is ready", result)

    def test_broadcast_reports_partial_failures(self):
        fake_settings = {
            "DEVICE_1": "media_player.office",
            "DEVICE_2": "media_player.kitchen",
            "TTS_ENTITY": "tts.piper",
            "REQUEST_TIMEOUT_SECONDS": "15",
        }
        fake_ha_settings = {"HA_BASE_URL": "http://ha.local:8123", "HA_TOKEN": "abc123"}
        fake_failures = ["media_player.kitchen (speak:500, piper_say:500)"]

        with patch.object(self.plugin, "_get_settings", return_value=fake_settings), patch(
            "plugins.broadcast.redis_client.hgetall",
            return_value=fake_ha_settings,
        ), patch.object(self.plugin, "_tts_speak", return_value=(1, fake_failures)):
            result = asyncio.run(self.plugin._broadcast("Test message", llm_client=None))

        self.assertIn("Broadcast sent to 1/2 devices", result)
        self.assertIn("Some devices failed", result)
        self.assertIn("media_player.kitchen", result)


if __name__ == "__main__":
    unittest.main()
