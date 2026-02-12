import unittest
from unittest.mock import patch

from kernel_tools import send_message
from notify.core import _dispatch_homeassistant
from notify.queue import resolve_targets
from tool_runtime import is_meta_tool, run_meta_tool


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


class KernelSendMessageTests(unittest.TestCase):
    def test_send_message_string_target_extracts_channel(self):
        with patch(
            "kernel_tools.dispatch_notification_sync",
            return_value="Queued notification for discord",
        ) as dispatch_mock:
            result = send_message(
                platform="discord",
                message="hello",
                targets="room #tater in discord",
            )
        self.assertTrue(result.get("ok"), result)
        sent_targets = dispatch_mock.call_args.kwargs.get("targets") or {}
        self.assertEqual(sent_targets.get("channel"), "#tater")

    def test_send_message_uses_homeassistant_api_default_setting(self):
        with patch(
            "kernel_tools._send_message_load_settings",
            return_value={"ENABLE_HA_API_NOTIFICATION": "false"},
        ), patch(
            "kernel_tools.dispatch_notification_sync",
            return_value="Queued notification for homeassistant",
        ) as dispatch_mock:
            result = send_message(
                platform="homeassistant",
                message="hello",
                targets={"persistent": True},
            )
        self.assertTrue(result.get("ok"), result)
        sent_targets = dispatch_mock.call_args.kwargs.get("targets") or {}
        self.assertFalse(sent_targets.get("api_notification"))

    def test_send_message_keeps_explicit_homeassistant_api_override(self):
        with patch(
            "kernel_tools._send_message_load_settings",
            return_value={"ENABLE_HA_API_NOTIFICATION": "true"},
        ), patch(
            "kernel_tools.dispatch_notification_sync",
            return_value="Queued notification for homeassistant",
        ) as dispatch_mock:
            result = send_message(
                platform="homeassistant",
                message="hello",
                api_notification=False,
            )
        self.assertTrue(result.get("ok"), result)
        sent_targets = dispatch_mock.call_args.kwargs.get("targets") or {}
        self.assertIn("api_notification", sent_targets)
        self.assertFalse(sent_targets.get("api_notification"))

    def test_send_message_uses_latest_image_ref_when_message_empty(self):
        with patch(
            "kernel_tools._send_message_resolve_blob_key",
            return_value="tater:blob:test-image",
        ), patch(
            "kernel_tools.dispatch_notification_sync",
            return_value="Queued notification for discord",
        ) as dispatch_mock:
            result = send_message(
                platform="discord",
                origin={
                    "platform": "discord",
                    "latest_image_ref": {
                        "blob_key": "tater:blob:test-image",
                        "name": "front.png",
                        "mimetype": "image/png",
                    },
                },
            )
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("attachment_count"), 1)
        self.assertEqual(dispatch_mock.call_args.kwargs.get("content"), "Attachment")

    def test_send_message_auto_uses_latest_image_when_text_mentions_image(self):
        with patch(
            "kernel_tools._send_message_resolve_blob_key",
            return_value="tater:blob:test-image",
        ), patch(
            "kernel_tools.dispatch_notification_sync",
            return_value="Queued notification for discord",
        ) as dispatch_mock:
            result = send_message(
                platform="discord",
                message="Here's the image you asked for:",
                origin={
                    "platform": "discord",
                    "latest_image_ref": {
                        "blob_key": "tater:blob:test-image",
                        "name": "front.png",
                        "mimetype": "image/png",
                    },
                },
            )
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("attachment_count"), 1)
        sent_attachments = dispatch_mock.call_args.kwargs.get("attachments") or []
        self.assertEqual(len(sent_attachments), 1)

    def test_send_message_does_not_auto_use_latest_image_for_plain_text(self):
        with patch(
            "kernel_tools.dispatch_notification_sync",
            return_value="Queued notification for discord",
        ) as dispatch_mock:
            result = send_message(
                platform="discord",
                message="hello",
                origin={
                    "platform": "discord",
                    "latest_image_ref": {
                        "blob_key": "tater:blob:test-image",
                        "name": "front.png",
                        "mimetype": "image/png",
                    },
                },
            )
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("attachment_count"), 0)
        sent_attachments = dispatch_mock.call_args.kwargs.get("attachments") or []
        self.assertEqual(len(sent_attachments), 0)

    def test_send_message_meta_tool_is_registered_and_routed(self):
        self.assertTrue(is_meta_tool("send_message"))
        with patch(
            "tool_runtime.send_message",
            return_value={"tool": "send_message", "ok": True, "result": "Queued notification for discord"},
        ) as mocked:
            result = run_meta_tool(
                func="send_message",
                args={"message": "hello", "platform": "discord", "targets": {"channel": "#tater"}},
                platform="webui",
                registry={},
                enabled_predicate=None,
                origin={"platform": "webui"},
            )

        self.assertTrue(result.get("ok"), result)
        mocked.assert_called_once()
        kwargs = mocked.call_args.kwargs
        self.assertEqual(kwargs.get("message"), "hello")
        self.assertEqual(kwargs.get("platform"), "discord")

    def test_get_plugin_help_supports_send_message_kernel_tool(self):
        result = run_meta_tool(
            func="get_plugin_help",
            args={"plugin_id": "send_message"},
            platform="webui",
            registry={},
            enabled_predicate=None,
            origin={"platform": "webui"},
        )
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("plugin_id"), "send_message")
        self.assertIn("usage_example", result)


if __name__ == "__main__":
    unittest.main()
