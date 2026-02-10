import unittest
from unittest.mock import AsyncMock, patch

import plugin_registry as pr
from notify.core import _dispatch_homeassistant
from notify.queue import resolve_targets
from plugin_kernel import get_plugin_help
from plugins.send_message import SendMessagePlugin


class NotifyTargetParsingTests(unittest.TestCase):
    def test_discord_channel_id_string_falls_back_to_channel_name(self):
        resolved, err = resolve_targets(
            "discord",
            {"channel_id": "#tater"},
            origin={"platform": "webui"},
            defaults={},
        )
        self.assertIsNone(err)
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved.get("channel"), "#tater")
        self.assertFalse(resolved.get("channel_id"))

    def test_discord_channel_phrase_extracts_hashtag(self):
        resolved, err = resolve_targets(
            "discord",
            {"channel": "room #tater in discord"},
            origin={"platform": "webui"},
            defaults={},
        )
        self.assertIsNone(err)
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved.get("channel"), "#tater")

    def test_irc_channel_name_gets_hash_prefix(self):
        resolved, err = resolve_targets(
            "irc",
            {"channel": "tater"},
            origin={"platform": "webui"},
            defaults={},
        )
        self.assertIsNone(err)
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved.get("channel"), "#tater")

    def test_telegram_bare_name_becomes_username(self):
        resolved, err = resolve_targets(
            "telegram",
            {"chat_id": "taterupdates"},
            origin={"platform": "webui"},
            defaults={},
        )
        self.assertIsNone(err)
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved.get("chat_id"), "@taterupdates")


class SendMessageTargetCoercionTests(unittest.TestCase):
    def test_send_message_string_target_extracts_channel(self):
        plugin = SendMessagePlugin()
        _title, _message, _platform, targets, _origin, _meta, _attachments = plugin._extract_args(
            {"platform": "discord", "message": "hello", "targets": "room #tater in discord"}
        )
        self.assertEqual(targets.get("channel"), "#tater")

    def test_send_message_help_does_not_mark_destination_fields_required(self):
        payload = get_plugin_help(
            plugin_id="send_message",
            platform="webui",
            registry=pr.get_registry_snapshot(),
        )
        required = payload.get("required_args") or []
        required_names = {str(item.get("name")) for item in required if isinstance(item, dict)}
        self.assertNotIn("targets", required_names)
        self.assertNotIn("platform", required_names)


class HomeAssistantDispatchToggleTests(unittest.TestCase):
    def test_homeassistant_dispatch_posts_api_notification_by_default(self):
        with patch("notify.core.resolve_targets", return_value=({}, None)), patch(
            "notify.core._post_ha_notification"
        ) as post_mock, patch("notify.core._send_persistent_notification") as persistent_mock, patch(
            "notify.core._send_mobile"
        ) as mobile_mock, patch(
            "notify.core._default_homeassistant_device_service", return_value=""
        ):
            result = _dispatch_homeassistant(
                title="Camera",
                content="Motion detected",
                targets={},
                origin={"platform": "automation"},
                meta={"priority": "normal"},
            )

        self.assertEqual(result, "Queued notification for homeassistant")
        post_mock.assert_called_once()
        persistent_mock.assert_called_once()
        mobile_mock.assert_not_called()

    def test_homeassistant_dispatch_skips_api_notification_when_disabled(self):
        with patch("notify.core.resolve_targets", return_value=({"api_notification": False, "persistent": False}, None)), patch(
            "notify.core._post_ha_notification"
        ) as post_mock, patch("notify.core._send_persistent_notification") as persistent_mock, patch(
            "notify.core._send_mobile"
        ) as mobile_mock, patch(
            "notify.core._default_homeassistant_device_service", return_value=""
        ):
            result = _dispatch_homeassistant(
                title="Camera",
                content="Motion detected",
                targets={"api_notification": False, "persistent": False},
                origin={"platform": "automation"},
                meta={"priority": "normal"},
            )

        self.assertEqual(result, "Queued notification for homeassistant")
        post_mock.assert_not_called()
        persistent_mock.assert_not_called()
        mobile_mock.assert_not_called()


class SendMessageHomeAssistantApiToggleTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_message_uses_homeassistant_api_default_setting(self):
        plugin = SendMessagePlugin()
        with patch.object(plugin, "_load_settings", return_value={"ENABLE_HA_API_NOTIFICATION": "false"}), patch(
            "plugins.send_message.dispatch_notification",
            new=AsyncMock(return_value="Queued notification for homeassistant"),
        ) as dispatch_mock:
            result = await plugin._dispatch(
                {"platform": "homeassistant", "message": "hello", "targets": {"persistent": True}}
            )

        self.assertEqual(result, "Queued notification for homeassistant")
        sent_targets = dispatch_mock.await_args.kwargs.get("targets") or {}
        self.assertFalse(sent_targets.get("api_notification"))

    async def test_send_message_keeps_explicit_homeassistant_api_override(self):
        plugin = SendMessagePlugin()
        with patch.object(plugin, "_load_settings", return_value={"ENABLE_HA_API_NOTIFICATION": "true"}), patch(
            "plugins.send_message.dispatch_notification",
            new=AsyncMock(return_value="Queued notification for homeassistant"),
        ) as dispatch_mock:
            result = await plugin._dispatch(
                {"platform": "homeassistant", "message": "hello", "api_notification": False}
            )

        self.assertEqual(result, "Queued notification for homeassistant")
        sent_targets = dispatch_mock.await_args.kwargs.get("targets") or {}
        self.assertIn("api_notification", sent_targets)
        self.assertFalse(sent_targets.get("api_notification"))


if __name__ == "__main__":
    unittest.main()
