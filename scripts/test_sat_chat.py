from __future__ import annotations

import asyncio
import io
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from PIL import Image

from tater_voice import sat_chat


def _client(name: str, board: str, **capability_overrides):
    capabilities = {
        "sat_chat_audio": True,
        "sat_chat_can_initiate": board in {"checkers", "crown", "athena", "rook"},
        "sat_chat_can_end": board in {"checkers", "crown", "athena", "rook"},
        "sat_chat_ui": board in {"checkers", "crown", "athena", "rook"},
        **capability_overrides,
    }
    return {
        "connected": True,
        "device_name": name,
        "board": board,
        "room": name,
        "capabilities": capabilities,
    }


class SatChatTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await sat_chat.reset_for_tests()
        self.clients = {
            "native:kitchen": _client("Kitchen Crown", "crown"),
            "native:office": _client("Office Checkers", "checkers"),
            "native:den": _client("Den Athena", "athena"),
            "native:bedroom": _client("Bedroom Biscuit", "biscuit"),
        }

    async def asyncTearDown(self) -> None:
        await sat_chat.reset_for_tests()

    async def test_start_connects_immediately_without_ringing(self) -> None:
        sent = AsyncMock()
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=sent
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            result = await sat_chat.start("native:kitchen", "native:office")

        self.assertEqual(result["call"]["state"], "active")
        message_types = [call.args[1] for call in sent.await_args_list]
        self.assertEqual(message_types, ["sat_chat.started", "sat_chat.started"])
        self.assertEqual(sent.await_args_list[0].args[2]["video_send_profile"], "echo-low")
        self.assertEqual(sent.await_args_list[1].args[2]["video_send_profile"], "echo-low")
        self.assertNotIn("sat_chat.invite", message_types)
        self.assertNotIn("sat_chat.ringing", message_types)

    async def test_athena_uses_echo_low_video_receive_profile(self) -> None:
        sent = AsyncMock()
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=sent
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            await sat_chat.start("native:den", "native:office")

        self.assertEqual(sent.await_args_list[0].args[2]["video_send_profile"], "echo-low")

    async def test_audio_only_satellite_cannot_initiate_or_end(self) -> None:
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=AsyncMock()
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            with self.assertRaises(PermissionError):
                await sat_chat.start("native:bedroom", "native:kitchen")

            call = await sat_chat.start("native:kitchen", "native:bedroom")
            with self.assertRaises(PermissionError):
                await sat_chat.end(call_id=call["call"]["id"], actor="native:bedroom")

    async def test_active_pcm_is_relayed_only_to_peer(self) -> None:
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=AsyncMock()
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            await sat_chat.start("native:kitchen", "native:office")

        send_binary = AsyncMock()
        fake_native = SimpleNamespace(send_binary=send_binary)
        with patch.dict(sys.modules, {"tater_voice.native_satellite": fake_native}):
            routed = await sat_chat.route_audio("native:kitchen", b"\x01\x02")
            ignored = await sat_chat.route_audio("native:bedroom", b"\x03\x04")

        self.assertTrue(routed)
        self.assertFalse(ignored)
        send_binary.assert_awaited_once_with("native:office", b"\x01\x02")

    async def test_snapshot_camera_does_not_claim_video_call_support(self) -> None:
        self.clients["native:kitchen"]["capabilities"]["camera_snapshot"] = True
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)):
            directory = await sat_chat.participants(viewer="native:office")
        crown = next(row for row in directory["participants"] if row["id"] == "native:kitchen")
        self.assertFalse(crown["video"])

    async def test_video_capability_allows_video_and_relays_tagged_jpeg(self) -> None:
        self.clients["native:kitchen"]["capabilities"]["sat_chat_video"] = True
        self.clients["native:office"]["capabilities"]["sat_chat_video"] = True
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=AsyncMock()
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            await sat_chat.start("native:kitchen", "native:office", mode="video")

        frame = sat_chat.VIDEO_FRAME_PREFIX + b"\xff\xd8camera\xff\xd9"
        send_binary = AsyncMock()
        fake_native = SimpleNamespace(send_binary=send_binary)
        with patch.dict(sys.modules, {"tater_voice.native_satellite": fake_native}):
            routed = await sat_chat.route_audio("native:kitchen", frame)
            for _ in range(20):
                if send_binary.await_count:
                    break
                await asyncio.sleep(0.01)

        self.assertTrue(routed)
        send_binary.assert_awaited_once_with("native:office", frame)

    async def test_crown_source_uses_native_video_profile_for_mobile_receiver(self) -> None:
        self.clients["native:kitchen"]["capabilities"]["sat_chat_video"] = True
        sent = AsyncMock()
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=sent
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            await sat_chat.start(
                "spudlink:phone",
                "native:kitchen",
                mode="video",
                caller_name="Phone",
            )

        self.assertEqual(sent.await_args.args[0], "native:kitchen")
        self.assertNotIn("video_send_profile", sent.await_args.args[2])

    async def test_checkers_source_uses_native_video_profile_for_mobile_receiver(self) -> None:
        self.clients["native:office"]["capabilities"]["sat_chat_video"] = True
        sent = AsyncMock()
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=sent
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            await sat_chat.start(
                "spudlink:phone",
                "native:office",
                mode="video",
                caller_name="Phone",
            )

        self.assertEqual(sent.await_args.args[0], "native:office")
        self.assertNotIn("video_send_profile", sent.await_args.args[2])

    async def test_slow_video_relay_keeps_only_newest_waiting_frame(self) -> None:
        self.clients["native:kitchen"]["capabilities"]["sat_chat_video"] = True
        self.clients["native:office"]["capabilities"]["sat_chat_video"] = True
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=AsyncMock()
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            await sat_chat.start("native:kitchen", "native:office", mode="video")

        started = asyncio.Event()
        release = asyncio.Event()
        sent = []

        async def slow_send(selector, frame):
            sent.append((selector, frame))
            if len(sent) == 1:
                started.set()
                await release.wait()

        fake_native = SimpleNamespace(send_binary=slow_send)
        frames = [
            sat_chat.VIDEO_FRAME_PREFIX + b"\xff\xd8one\xff\xd9",
            sat_chat.VIDEO_FRAME_PREFIX + b"\xff\xd8two\xff\xd9",
            sat_chat.VIDEO_FRAME_PREFIX + b"\xff\xd8three\xff\xd9",
        ]
        with patch.dict(sys.modules, {"tater_voice.native_satellite": fake_native}):
            await sat_chat.route_audio("native:kitchen", frames[0])
            await started.wait()
            await sat_chat.route_audio("native:kitchen", frames[1])
            await sat_chat.route_audio("native:kitchen", frames[2])
            release.set()
            for _ in range(20):
                if len(sent) == 2:
                    break
                await asyncio.sleep(0)

        self.assertEqual(sent, [("native:office", frames[0]), ("native:office", frames[2])])

    async def test_echo_video_is_reduced_without_cropping(self) -> None:
        source = Image.new("RGB", (640, 360), (120, 80, 40))
        encoded = io.BytesIO()
        source.save(encoded, format="JPEG", quality=70)
        frame = sat_chat.VIDEO_FRAME_PREFIX + encoded.getvalue()

        resized = await asyncio.to_thread(sat_chat._resize_echo_video_frame, frame)

        with Image.open(io.BytesIO(resized[len(sat_chat.VIDEO_FRAME_PREFIX):])) as image:
            self.assertEqual(image.size, (480, 270))
            self.assertAlmostEqual(image.width / image.height, 16 / 9, places=2)

    async def test_native_end_notifies_connected_little_spud_media(self) -> None:
        event_sender = AsyncMock()
        audio_sender = AsyncMock()
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=AsyncMock()
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            started = await sat_chat.start(
                "spudlink:phone",
                "native:kitchen",
                caller_name="Phone",
            )
            await sat_chat.register_external_media(
                "spudlink:phone",
                audio_sender,
                event_sender=event_sender,
            )
            await sat_chat.end(call_id=started["call"]["id"], actor="native:kitchen")

        event_sender.assert_awaited_once()
        event = event_sender.await_args.args[0]
        self.assertEqual(event["type"], "sat_chat.ended")
        self.assertEqual(event["call"]["state"], "ended")


if __name__ == "__main__":
    unittest.main()
