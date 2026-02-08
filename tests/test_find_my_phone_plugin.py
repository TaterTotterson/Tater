import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from plugins.find_my_phone import FindMyPhonePlugin


class _FakeAuthor:
    def __init__(self, mention: str):
        self.mention = mention


class _FakeDiscordMessage:
    def __init__(self, mention: str):
        self.author = _FakeAuthor(mention)


class FindMyPhonePluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = FindMyPhonePlugin()

    def test_normalize_notify_service(self):
        self.assertEqual(self.plugin._normalize_notify_service("notify.mobile_app_pixel"), "mobile_app_pixel")
        self.assertEqual(self.plugin._normalize_notify_service("mobile_app_pixel"), "mobile_app_pixel")
        self.assertEqual(self.plugin._normalize_notify_service("notify.mobile-app-pixel"), "")

    def test_trigger_uses_default_notifier_service_when_missing(self):
        settings = {
            "MOBILE_NOTIFY_SERVICE": "",
            "DEFAULT_TITLE": "Find My Phone",
            "DEFAULT_MESSAGE": "Phone alert requested.",
            "ALERT_COUNT": "2",
            "ALERT_DELAY_SECONDS": "0",
            "REQUEST_TIMEOUT_SECONDS": "10",
        }
        with patch.object(self.plugin, "_load_settings", return_value=settings), patch.object(
            self.plugin,
            "_load_homeassistant_settings",
            return_value={"HA_BASE_URL": "http://ha.local:8123", "HA_TOKEN": "abc123"},
        ), patch.object(
            self.plugin, "_load_default_notify_service", return_value="notify.mobile_app_pixel"
        ), patch.object(
            self.plugin, "_ha_post_service", return_value=(200, "")
        ) as post_mock, patch(
            "plugins.find_my_phone.time.sleep"
        ):
            result = self.plugin._trigger_phone_alert({})

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("count"), 2)
        self.assertEqual(post_mock.call_args.kwargs.get("service"), "mobile_app_pixel")

    def test_trigger_count_override_is_clamped(self):
        settings = {
            "MOBILE_NOTIFY_SERVICE": "notify.mobile_app_pixel",
            "DEFAULT_TITLE": "Find My Phone",
            "DEFAULT_MESSAGE": "Phone alert requested.",
            "ALERT_COUNT": "2",
            "ALERT_DELAY_SECONDS": "0",
            "REQUEST_TIMEOUT_SECONDS": "10",
        }
        with patch.object(self.plugin, "_load_settings", return_value=settings), patch.object(
            self.plugin,
            "_load_homeassistant_settings",
            return_value={"HA_BASE_URL": "http://ha.local:8123", "HA_TOKEN": "abc123"},
        ), patch.object(
            self.plugin, "_ha_post_service", return_value=(200, "")
        ) as post_mock, patch(
            "plugins.find_my_phone.time.sleep"
        ):
            result = self.plugin._trigger_phone_alert({"count": 99})

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("count"), 5)
        self.assertEqual(post_mock.call_count, 5)

    def test_trigger_returns_error_for_invalid_service(self):
        settings = {
            "MOBILE_NOTIFY_SERVICE": "notify.mobile-app-pixel",
            "DEFAULT_TITLE": "Find My Phone",
            "DEFAULT_MESSAGE": "Phone alert requested.",
            "ALERT_COUNT": "2",
        }
        with patch.object(self.plugin, "_load_settings", return_value=settings), patch.object(
            self.plugin,
            "_load_homeassistant_settings",
            return_value={"HA_BASE_URL": "http://ha.local:8123", "HA_TOKEN": "abc123"},
        ), patch.object(self.plugin, "_load_default_notify_service", return_value=""):
            result = self.plugin._trigger_phone_alert({})

        self.assertFalse(result.get("ok"), result)
        self.assertIn("invalid", (result.get("error") or "").lower())

    def test_handle_discord_uses_author_mention(self):
        fake_message = _FakeDiscordMessage("<@123>")
        with patch.object(self.plugin, "_run", AsyncMock(return_value="ok")) as run_mock:
            result = asyncio.run(self.plugin.handle_discord(fake_message, {}, llm_client=None))

        self.assertEqual(result, "ok")
        self.assertEqual(run_mock.await_args.kwargs.get("mention"), "<@123>")


if __name__ == "__main__":
    unittest.main()
