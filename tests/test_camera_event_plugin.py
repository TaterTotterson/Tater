import asyncio
import unittest
from unittest.mock import AsyncMock, Mock, patch

from plugins.camera_event import CameraEventPlugin


class CameraEventPluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = CameraEventPlugin()

    def test_notification_cooldown_skips_notifier_send(self):
        args = {"area": "front yard", "camera": "camera.front_door_high"}
        settings = {
            "DEFAULT_COOLDOWN_SECONDS": "0",
            "NOTIFICATION_COOLDOWN_SECONDS": "60",
            "ENABLE_PHONE_ALERTS": True,
        }

        with patch.object(self.plugin, "_get_settings", return_value=settings), patch.object(
            self.plugin, "_ha", return_value={"base": "http://ha.local:8123", "token": "token"}
        ), patch.object(
            self.plugin, "_vision", return_value={"api_base": "http://vision", "model": "model", "api_key": ""}
        ), patch.object(
            self.plugin, "_within_cooldown", return_value=False
        ), patch.object(
            self.plugin, "_get_camera_jpeg", return_value=b"jpeg"
        ), patch.object(
            self.plugin, "_vision_describe", return_value="Person detected near the front door."
        ), patch.object(
            self.plugin, "_post_event", return_value=None
        ), patch.object(
            self.plugin, "_mark_fired", return_value=None
        ), patch.object(
            self.plugin, "_get_phone_services", return_value=["mobile_app_iphone"]
        ), patch.object(
            self.plugin, "_within_notification_cooldown", return_value=True
        ), patch.object(
            self.plugin, "_notify_via_homeassistant_notifier", AsyncMock(return_value={"ok": True, "sent_count": 1})
        ) as notify_mock:
            result = asyncio.run(self.plugin.handle_automation(args, llm_client=None))

        self.assertEqual(result["notification"]["skipped"], "notification_cooldown")
        self.assertEqual(result["notification"]["sent_count"], 0)
        self.assertEqual(result["notification_cooldown_seconds"], 60)
        notify_mock.assert_not_awaited()

    def test_notification_cooldown_marks_when_send_succeeds(self):
        args = {
            "area": "front yard",
            "camera": "camera.front_door_high",
            "notification_cooldown_seconds": 5,
        }
        settings = {
            "DEFAULT_COOLDOWN_SECONDS": "0",
            "NOTIFICATION_COOLDOWN_SECONDS": "30",
            "ENABLE_PHONE_ALERTS": True,
        }

        with patch.object(self.plugin, "_get_settings", return_value=settings), patch.object(
            self.plugin, "_ha", return_value={"base": "http://ha.local:8123", "token": "token"}
        ), patch.object(
            self.plugin, "_vision", return_value={"api_base": "http://vision", "model": "model", "api_key": ""}
        ), patch.object(
            self.plugin, "_within_cooldown", return_value=False
        ), patch.object(
            self.plugin, "_get_camera_jpeg", return_value=b"jpeg"
        ), patch.object(
            self.plugin, "_vision_describe", return_value="Person detected near the front door."
        ), patch.object(
            self.plugin, "_post_event", return_value=None
        ), patch.object(
            self.plugin, "_mark_fired", return_value=None
        ), patch.object(
            self.plugin, "_get_phone_services", return_value=["mobile_app_iphone"]
        ), patch.object(
            self.plugin, "_within_notification_cooldown", return_value=False
        ), patch.object(
            self.plugin, "_notify_via_homeassistant_notifier", AsyncMock(return_value={"ok": True, "sent_count": 1})
        ) as notify_mock, patch.object(
            self.plugin, "_mark_notification_fired", Mock()
        ) as mark_notify_mock:
            result = asyncio.run(self.plugin.handle_automation(args, llm_client=None))

        self.assertEqual(result["notification_cooldown_seconds"], 5)
        notify_mock.assert_awaited_once()
        mark_notify_mock.assert_called_once_with("camera.front_door_high")

    def test_notify_supports_api_only_target(self):
        with patch(
            "plugins.camera_event.dispatch_notification",
            new=AsyncMock(return_value="Queued notification to Home Assistant"),
        ) as dispatch_mock:
            result = asyncio.run(
                self.plugin._notify_via_homeassistant_notifier(
                    title="Camera Event",
                    message="Motion detected",
                    priority="high",
                    send_phone_alerts=False,
                    persistent_notifications=False,
                    api_notification=True,
                    phone_services=[],
                    area="front yard",
                    camera="camera.front_door_high",
                )
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["sent_count"], 1)
        self.assertEqual(dispatch_mock.await_count, 1)
        targets = dispatch_mock.await_args.kwargs.get("targets") or {}
        self.assertTrue(targets.get("api_notification"))


if __name__ == "__main__":
    unittest.main()
