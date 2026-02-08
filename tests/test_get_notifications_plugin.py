import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from plugins.get_notifications import GetNotificationsPlugin


class GetNotificationsPluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = GetNotificationsPlugin()

    def test_platforms_include_chat_platforms(self):
        for platform in ("discord", "telegram", "matrix", "irc"):
            with self.subTest(platform=platform):
                self.assertIn(platform, self.plugin.platforms)

    def test_platform_base_url_decodes_bytes_port(self):
        with patch("plugins.get_notifications.redis_client.hget", return_value=b"9900"):
            self.assertEqual(self.plugin._platform_base_url(), "http://127.0.0.1:9900")

    def test_platform_base_url_invalid_port_uses_default(self):
        with patch("plugins.get_notifications.redis_client.hget", return_value=b"bad"):
            self.assertEqual(self.plugin._platform_base_url(), "http://127.0.0.1:8787")

    def test_handle_returns_service_error_when_unreachable(self):
        with patch.object(self.plugin, "_pull_notifications", AsyncMock(return_value=None)):
            result = asyncio.run(self.plugin._handle({}, llm_client=None))
        self.assertIn("could not reach", result.lower())

    def test_handle_limit_and_fallback_summary(self):
        notifs = [
            {"title": "Front Door", "message": "Motion detected", "ha_time": "09:00"},
            {"title": "Garage", "message": "Door opened", "ha_time": "09:05"},
            {"title": "Back Yard", "message": "Camera event", "ha_time": "09:10"},
        ]
        with patch.object(self.plugin, "_pull_notifications", AsyncMock(return_value=notifs)):
            result = asyncio.run(self.plugin._handle({"limit": 2}, llm_client=None))

        self.assertIn("You have 3 notifications.", result)
        self.assertIn("Showing the latest 2.", result)
        self.assertIn("1. Front Door - Motion detected (at 09:00)", result)
        self.assertIn("2. Garage - Door opened (at 09:05)", result)
        self.assertNotIn("Back Yard", result)

    def test_matrix_handler_accepts_llm_alias(self):
        with patch.object(self.plugin, "_handle", AsyncMock(return_value="ok")) as handle_mock:
            result = asyncio.run(self.plugin.handle_matrix(args={}, llm="alias-client"))

        self.assertEqual(result, "ok")
        self.assertEqual(handle_mock.await_args.args[0], {})
        self.assertEqual(handle_mock.await_args.args[1], "alias-client")


if __name__ == "__main__":
    unittest.main()
