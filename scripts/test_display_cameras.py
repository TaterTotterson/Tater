#!/usr/bin/env python3
from __future__ import annotations

import unittest
import pathlib
import sys
import types
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

helpers_stub = types.ModuleType("helpers")
helpers_stub.redis_client = object()
sys.modules.setdefault("helpers", helpers_stub)
registry_stub = types.ModuleType("integration_registry")
registry_stub.get_integration_device_registry = lambda *_args, **_kwargs: {"devices": []}
registry_stub.run_integration_device_action = lambda *_args, **_kwargs: {}
sys.modules.setdefault("integration_registry", registry_stub)

from tater_voice import display_cameras


REGISTRY = {
    "devices": [
        {
            "integration_id": "unifi_protect",
            "id": "front-door",
            "name": "Front Door",
            "room": "Entry",
            "type": "camera",
            "status": "online",
            "actions": ["camera_snapshot", "camera_talkback"],
            "capabilities": ["camera", "doorbell"],
        },
        {
            "integration_id": "example",
            "id": "driveway",
            "name": "Driveway",
            "room": "Outside",
            "type": "camera",
            "status": "offline",
            "actions": ["camera_snapshot"],
        },
    ]
}


class DisplayCamerasTests(unittest.TestCase):
    @patch.object(display_cameras, "get_integration_device_registry", return_value=REGISTRY)
    def test_directory_is_opaque_and_reports_capabilities(self, _registry):
        payload = display_cameras.build_camera_directory(client=object())
        self.assertTrue(payload["available"])
        self.assertEqual([row["name"] for row in payload["cameras"]], ["Front Door", "Driveway"])
        front = payload["cameras"][0]
        self.assertNotIn("front-door", front["id"])
        self.assertTrue(front["doorbell"])
        self.assertTrue(front["talkback"])
        self.assertFalse(payload["cameras"][1]["available"])

    @patch.object(display_cameras, "run_integration_device_action", return_value={"ok": True, "bytes": b"\xff\xd8ok", "content_type": "image/jpeg"})
    @patch.object(display_cameras, "get_integration_device_registry", return_value=REGISTRY)
    def test_capture_resolves_opaque_id(self, _registry, action):
        camera_id = display_cameras.build_camera_directory(client=object())["cameras"][0]["id"]
        image, content_type, row = display_cameras.capture_frame(camera_id, client=object())
        self.assertEqual(image, b"\xff\xd8ok")
        self.assertEqual(content_type, "image/jpeg")
        self.assertEqual(row["id"], "front-door")
        action.assert_called_once_with("unifi_protect", "camera_snapshot", "front-door", {})

    def test_talkback_wake_word(self):
        self.assertTrue(display_cameras.is_talkback_wake_word("camera talkback:abc"))
        self.assertEqual(display_cameras.talkback_camera_id("camera talkback:abc"), "abc")


if __name__ == "__main__":
    unittest.main()
