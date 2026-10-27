#!/usr/bin/env python3
from __future__ import annotations

import base64
import asyncio
import pathlib
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

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
fastapi_stub = types.ModuleType("fastapi")
fastapi_stub.WebSocket = object
fastapi_stub.WebSocketDisconnect = type("WebSocketDisconnect", (Exception,), {})
sys.modules.setdefault("fastapi", fastapi_stub)

from tater_voice import display_cameras, native_satellite


REGISTRY = {
    "devices": [
        {
            "integration_id": "unifi_protect",
            "id": "front-door",
            "name": "Front Door",
            "room": "Entry",
            "type": "camera",
            "status": "online",
            "actions": ["camera_snapshot", "camera_stream", "camera_talkback"],
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
        self.assertTrue(front["stream"])
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

    @patch.object(
        display_cameras,
        "run_integration_device_action",
        return_value={"ok": True, "url": "https://ha.invalid/api/hls/token/playlist.m3u8", "format": "hls"},
    )
    @patch.object(display_cameras, "get_integration_device_registry", return_value=REGISTRY)
    def test_open_stream_resolves_opaque_id_and_validates_url(self, _registry, action):
        camera_id = display_cameras.build_camera_directory(client=object())["cameras"][0]["id"]
        stream_url, stream_format, row = display_cameras.open_stream(camera_id, client=object())
        self.assertEqual(stream_url, "https://ha.invalid/api/hls/token/playlist.m3u8")
        self.assertEqual(stream_format, "hls")
        self.assertEqual(row["id"], "front-door")
        action.assert_called_once_with("unifi_protect", "camera_stream", "front-door", {})

    @patch.object(
        display_cameras,
        "run_integration_device_action",
        return_value={"url": "file:///etc/passwd"},
    )
    @patch.object(display_cameras, "get_integration_device_registry", return_value=REGISTRY)
    def test_open_stream_rejects_non_network_url(self, _registry, _action):
        camera_id = display_cameras.build_camera_directory(client=object())["cameras"][0]["id"]
        with self.assertRaisesRegex(RuntimeError, "unsupported stream URL"):
            display_cameras.open_stream(camera_id, client=object())

    @patch.object(
        display_cameras,
        "run_integration_device_action",
        return_value={"ok": True, "url": "rtsps://protect.local:7441/private-token?enableSrtp", "format": "rtsps"},
    )
    @patch.object(display_cameras, "get_integration_device_registry", return_value=REGISTRY)
    def test_open_stream_accepts_unifi_rtsps_url(self, _registry, action):
        camera_id = display_cameras.build_camera_directory(client=object())["cameras"][0]["id"]
        stream_url, stream_format, _row = display_cameras.open_stream(camera_id, client=object())
        self.assertEqual(stream_url, "rtsps://protect.local:7441/private-token?enableSrtp")
        self.assertEqual(stream_format, "rtsps")
        action.assert_called_once_with("unifi_protect", "camera_stream", "front-door", {})

    def test_native_payload_keeps_stream_url_ephemeral(self):
        directory = {
            "available": True,
            "cameras": [{"id": "front", "name": "Front", "stream": True}],
        }
        with patch.object(display_cameras, "build_camera_directory", return_value=directory):
            selected = native_satellite._screen_camera_payload(
                selected_id="front",
                stream_url="https://ha.invalid/api/hls/token/playlist.m3u8",
                stream_active=True,
            )
            closed = native_satellite._screen_camera_payload(
                stream_url="https://ha.invalid/api/hls/token/playlist.m3u8",
            )

        self.assertEqual(selected["stream_url"], "https://ha.invalid/api/hls/token/playlist.m3u8")
        self.assertTrue(selected["stream_active"])
        self.assertEqual(closed["stream_url"], "")
        self.assertFalse(closed["stream_active"])

    @patch.object(display_cameras, "get_integration_device_registry", return_value={"devices": []})
    @patch.object(
        native_satellite,
        "status_snapshot_sync",
        return_value={
            "clients": {
                "native:show-kitchen": {
                    "connected": True,
                    "device_name": "Kitchen Show",
                    "room": "Kitchen",
                    "capabilities": {"camera_snapshot": True, "camera_stream": True},
                }
            }
        },
    )
    def test_native_echo_camera_is_in_room_vision_directory(self, _status, _registry):
        directory = display_cameras.build_camera_directory(client=object())
        self.assertEqual(len(directory["cameras"]), 1)
        row = directory["cameras"][0]
        self.assertEqual(row["name"], "Kitchen Show")
        self.assertEqual(row["room"], "Kitchen")
        self.assertTrue(row["stream"])
        stream_source, stream_format, resolved = display_cameras.open_stream(row["id"], client=object())
        self.assertEqual(stream_source, "native:show-kitchen")
        self.assertEqual(stream_format, "tater-native-mjpeg")
        self.assertEqual(resolved["integration_id"], "tater_native")

    @patch.object(display_cameras, "get_integration_device_registry", return_value={"devices": []})
    @patch.object(
        native_satellite,
        "status_snapshot_sync",
        return_value={
            "clients": {
                "native:show-kitchen": {
                    "connected": True,
                    "device_name": "Kitchen Show",
                    "capabilities": {"camera_snapshot": True, "camera_stream": True},
                }
            }
        },
    )
    def test_native_echo_camera_snapshot_uses_correlated_request(self, _status, _registry):
        row = display_cameras.build_camera_directory(client=object())["cameras"][0]
        jpeg = b"\xff\xd8native-camera\xff\xd9"

        def finish(awaitable, *, timeout):
            awaitable.close()
            self.assertEqual(timeout, 9.0)
            return {
                "ok": True,
                "image_base64": base64.b64encode(jpeg).decode("ascii"),
                "content_type": "image/jpeg",
            }

        with patch.object(native_satellite, "run_on_runtime_loop", side_effect=finish), patch.object(
            native_satellite,
            "send_request",
        ) as request:
            image, content_type, resolved = display_cameras.capture_frame(row["id"], client=object())

        self.assertEqual(image, jpeg)
        self.assertEqual(content_type, "image/jpeg")
        self.assertEqual(resolved["id"], "native:show-kitchen")
        request.assert_called_once_with(
            "native:show-kitchen",
            "camera.snapshot",
            {"reason": "room_vision_fallback"},
            timeout_s=8.0,
        )

    def test_talkback_wake_word(self):
        self.assertTrue(display_cameras.is_talkback_wake_word("camera talkback:abc"))
        self.assertEqual(display_cameras.talkback_camera_id("camera talkback:abc"), "abc")


class NativeCameraRelayTests(unittest.TestCase):
    def test_native_camera_profile_is_balanced_only_between_larger_displays(self):
        self.assertEqual(
            native_satellite._echo_camera_profile_for_boards("crown", "checkers"),
            "echo-balanced",
        )
        self.assertEqual(
            native_satellite._echo_camera_profile_for_boards("athena", "crown"),
            "echo-balanced",
        )
        self.assertEqual(
            native_satellite._echo_camera_profile_for_boards("rook", "checkers"),
            "echo-low",
        )
        self.assertEqual(
            native_satellite._echo_camera_profile_for_boards("checkers", "rook"),
            "echo-low",
        )

    def test_native_camera_relay_starts_forwards_and_stops(self):
        async def exercise():
            native_satellite._native_camera_relays.clear()
            jpeg = b"\xff\xd8live\xff\xd9"
            frame = native_satellite.CAMERA_FRAME_PREFIX + jpeg
            with patch.object(
                native_satellite,
                "_load_selector_aliases",
                return_value={},
            ), patch.object(
                native_satellite,
                "_native_camera_stream_profile",
                new=AsyncMock(return_value="echo-balanced"),
            ), patch.object(
                native_satellite,
                "send_request",
                new=AsyncMock(return_value={"ok": True}),
            ) as request, patch.object(
                native_satellite,
                "send_binary",
                new=AsyncMock(return_value={"ok": True}),
            ) as send:
                ended = await native_satellite._subscribe_native_camera(
                    "native:show-source",
                    "native:show-target",
                )
                self.assertFalse(ended.is_set())
                await native_satellite._queue_native_camera_frame("native:show-source", frame)
                for _ in range(20):
                    if send.await_count:
                        break
                    await asyncio.sleep(0)
                send.assert_awaited_once_with("native:show-target", frame)
                await native_satellite._unsubscribe_native_camera(
                    "native:show-source",
                    "native:show-target",
                )

            self.assertEqual(request.await_count, 2)
            self.assertEqual(request.await_args_list[0].args[:2], ("native:show-source", "camera.stream.start"))
            self.assertEqual(
                request.await_args_list[0].args[2],
                {"profile": "echo-balanced", "reason": "room_vision"},
            )
            self.assertEqual(request.await_args_list[1].args[:2], ("native:show-source", "camera.stream.stop"))

        asyncio.run(exercise())


if __name__ == "__main__":
    unittest.main()
