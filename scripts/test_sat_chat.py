from __future__ import annotations

import asyncio
import io
import struct
import unittest
from unittest.mock import AsyncMock, patch

from PIL import Image

from tater_voice import native_satellite, sat_chat


def _client(name: str, board: str, **capability_overrides):
    capabilities = {
        "sat_chat_audio": True,
        "sat_chat_can_initiate": board in {"checkers", "cronos", "crown", "athena", "rook"},
        "sat_chat_can_end": board in {"checkers", "cronos", "crown", "athena", "rook"},
        "sat_chat_ui": board in {"checkers", "cronos", "crown", "athena", "rook"},
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
    async def test_athena_ap_calls_require_explicit_balanced_only_and_both_peers(self) -> None:
        for reverse in (False, True):
            for own_opt_in, peer_balanced, expected in ((True, True, "echo-h264-balanced"),
                                                       (False, True, "echo-balanced"),
                                                       (True, False, "echo-balanced")):
                with self.subTest(reverse=reverse, own_opt_in=own_opt_in, peer_balanced=peer_balanced):
                    await sat_chat.reset_for_tests()
                    athena = _client("Athena", "athena", sat_chat_video=True, sat_chat_h264=True,
                                     sat_chat_h264_version=1, sat_chat_h264_balanced=True,
                                     sat_chat_h264_balanced_only=own_opt_in)
                    peer = _client("Peer", "cronos", sat_chat_video=True, sat_chat_h264=True,
                                   sat_chat_h264_version=1, sat_chat_h264_balanced=peer_balanced,
                                   sat_chat_h264_receive_max_height=960)
                    a, b = (peer, athena) if reverse else (athena, peer)
                    sent = AsyncMock()
                    with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value={"native:a":a,"native:b":b})), patch.object(
                        sat_chat, "_send", new=sent
                    ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
                        await sat_chat.start("native:a", "native:b", mode="video")
                    for message in sent.await_args_list:
                        self.assertEqual(message.args[2]["video_send_profile"], expected)

    async def test_crown_hardware_calls_require_explicit_peer_capabilities(self) -> None:
        for peer in ("checkers", "cronos", "crown", "athena"):
            for reverse in (False, True):
                for enabled, version in ((True, 1), (False, 1), (True, 2)):
                    with self.subTest(peer=peer, reverse=reverse, enabled=enabled, version=version):
                        await sat_chat.reset_for_tests()
                        boards = (peer, "crown") if reverse else ("crown", peer)
                        clients = {
                            f"native:{name}": _client(
                                name, board, sat_chat_video=True,
                                sat_chat_h264=enabled if name == "a" else True,
                                sat_chat_h264_version=version if name == "a" else 1,
                                sat_chat_h264_balanced=True,
                            )
                            for name, board in zip(("a", "b"), boards)
                        }
                        sent = AsyncMock()
                        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=clients)), patch.object(
                            sat_chat, "_send", new=sent
                        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
                            await sat_chat.start("native:a", "native:b", mode="video")
                        expected = "echo-h264-balanced" if peer != "athena" and enabled and version == 1 else "echo-balanced"
                        self.assertEqual(len(sent.await_args_list), 2)
                        for message in sent.await_args_list:
                            self.assertEqual(message.args[2]["video_send_profile"], expected)

    async def test_crown_native_h264_respects_tall_cronos_receiver_gate(self) -> None:
        for maximum in (None, 720, 960, True, "960"):
            await sat_chat.reset_for_tests()
            clients = {
                "native:a": _client("A", "crown", sat_chat_video=True, sat_chat_h264=True,
                                    sat_chat_h264_version=1, sat_chat_h264_receive_max_height=maximum),
                "native:b": _client("B", "cronos", sat_chat_video=True, sat_chat_h264=True,
                                    sat_chat_h264_version=1),
            }
            sent = AsyncMock()
            with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=clients)), patch.object(
                sat_chat, "_send", new=sent
            ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
                await sat_chat.start("native:a", "native:b", mode="video")
            expected = "echo-h264" if type(maximum) is int and maximum == 960 else "echo-balanced"
            for message in sent.await_args_list:
                self.assertEqual(message.args[2]["video_send_profile"], expected)

    async def test_balanced_h264_requires_both_peers_and_validates_geometry(self) -> None:
        for a, b, expected in [(True, True, "echo-h264-balanced"), (True, False, "echo-balanced"), (False, True, "echo-balanced")]:
            await sat_chat.reset_for_tests()
            clients = {
                "native:a": _client("A", "checkers", sat_chat_video=True, sat_chat_h264=True, sat_chat_h264_version=1, sat_chat_h264_balanced=a),
                "native:b": _client("B", "cronos", sat_chat_video=True, sat_chat_h264=True, sat_chat_h264_version=1, sat_chat_h264_balanced=b),
            }
            sent = AsyncMock()
            with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=clients)), patch.object(sat_chat, "_send", new=sent), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
                await sat_chat.start("native:a", "native:b", mode="video")
            self.assertEqual(sent.await_args_list[0].args[2]["video_send_profile"], expected)
            if a and b:
                wire = b"TSCV2" + struct.pack("<IIIIIQQ", 768, 432, 30, 0, 1, 1234, 123) + b"\0\0\0\1\x65\x88"
                self.assertTrue(sat_chat._valid_h264_frame(wire, balanced=True))
                self.assertFalse(sat_chat._valid_h264_frame(wire))
                with patch.object(sat_chat, "_queue_video_frame", new=AsyncMock()) as queue:
                    self.assertTrue(await sat_chat.route_audio("native:a", wire))
                    queue.assert_awaited_once()
                bad = b"TSCV2" + struct.pack("<IIIIIQQ", 640, 432, 30, 0, 1, 0, 123) + b"\0\0\0\1\x65\x88"
                self.assertFalse(sat_chat._valid_h264_frame(bad, balanced=True))

    async def test_directory_refresh_takes_one_native_snapshot(self) -> None:
        clients = {f"native:{name}": _client(name, "checkers") for name in ("a", "b", "c")}
        native = AsyncMock(return_value=clients)
        sent = AsyncMock()
        with patch.object(sat_chat, "_native_clients", new=native), patch.object(sat_chat, "_send", new=sent):
            await sat_chat.refresh_directories()
        self.assertEqual(native.await_count, 1, "directory fanout repeated expensive full native/settings snapshots")
        self.assertEqual(sent.await_count, 3)
        for message in sent.await_args_list:
            viewer, kind, directory = message.args
            self.assertEqual(kind, "sat_chat.directory")
            self.assertEqual(directory["participant_count"], 2)
            self.assertNotIn(viewer, {row["id"] for row in directory["participants"]})

    async def test_h264_relay_preserves_dependent_burst_and_bounds_overflow(self) -> None:
        def wire(sequence, key=False):
            return b"TSCV2"+struct.pack("<IIIIIQQ",1280,960,30,sequence,int(key),sequence*33333,123)+b"\0\0\0\1\x65\x88"
        for overflow in (False, True):
            await sat_chat.reset_for_tests()
            started, release = asyncio.Event(), asyncio.Event()
            sent = []
            async def slow_send(_selector, frame):
                sent.append(frame)
                if len(sent)==1:
                    started.set()
                    await release.wait()
            with patch.object(native_satellite, "send_binary", new=slow_send):
                await sat_chat._queue_video_frame("call","native:a","native:b",wire(0,True),profile="echo-h264")
                await started.wait()
                for n in range(1,41 if overflow else 3):
                    await sat_chat._queue_video_frame("call","native:a","native:b",wire(n),profile="echo-h264")
                expected=[wire(0,True),wire(1),wire(2)]
                if overflow:
                    for n in (60,61):
                        await sat_chat._queue_video_frame("call","native:a","native:b",wire(n,n==60),profile="echo-h264")
                    expected=[wire(0,True),wire(60,True),wire(61)]
                release.set()
                for _ in range(30):
                    if len(sent)>=len(expected): break
                    await asyncio.sleep(0)
                self.assertEqual(sent,expected)
                diagnostics = await sat_chat.relay_diagnostics()
                self.assertEqual(diagnostics["count"], 1)
                stats = diagnostics["relays"][0]
                self.assertEqual(stats["received_frames"], 43 if overflow else 3)
                self.assertEqual(stats["sent_frames"], len(expected))
                self.assertGreater(stats["maximum_frame_bytes"], 41)
                self.assertGreaterEqual(stats["maximum_queue_depth"], 2)
                self.assertEqual(stats["dropped_gops"], 1 if overflow else 0)

    async def test_mixed_hardware_call_requires_tall_checkers_receiver(self) -> None:
        for boards in (("checkers", "cronos"), ("cronos", "checkers")):
            for maximum in (None, 720, 960, True, "960", 1088):
                await sat_chat.reset_for_tests()
                clients = {
                    f"native:{name}": _client(
                        name, board, sat_chat_video=True, sat_chat_h264=True,
                        sat_chat_h264_version=1,
                        **({"sat_chat_h264_receive_max_height": maximum} if board == "checkers" else {}),
                    )
                    for name, board in zip(("a", "b"), boards)
                }
                sent = AsyncMock()
                with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=clients)), patch.object(sat_chat, "_send", new=sent), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
                    await sat_chat.start("native:a", "native:b", mode="video")
                expected = "echo-h264" if type(maximum) is int and maximum == 960 else "echo-balanced"
                for message in sent.await_args_list:
                    self.assertEqual(message.args[2]["video_send_profile"], expected, (boards, maximum))

    async def test_cronos_h264_and_mixed_target_fallback(self) -> None:
        for boards, expected in [(('cronos','cronos'),'echo-h264'),(('cronos','checkers'),'echo-balanced')]:
            await sat_chat.reset_for_tests()
            clients = {f'native:{name}': _client(name,board,sat_chat_video=True,sat_chat_h264=True,sat_chat_h264_version=1)
                       for name,board in zip(('a','b'),boards)}
            sent=AsyncMock()
            with patch.object(sat_chat,'_native_clients',new=AsyncMock(return_value=clients)), patch.object(sat_chat,'_send',new=sent), patch.object(sat_chat,'refresh_directories',new=AsyncMock()):
                await sat_chat.start('native:a','native:b',mode='video')
            self.assertEqual(sent.await_args_list[0].args[2]['video_send_profile'],expected)
        wire=b'TSCV2'+struct.pack('<IIIIIQQ',1280,960,30,0,1,0,123)+b'\0\0\0\1\x65\x88'
        self.assertTrue(sat_chat._valid_h264_frame(wire))
        invalid=b'TSCV2'+struct.pack('<IIIIIQQ',1600,1200,30,0,1,0,123)+b'\0\0\0\1\x65\x88'
        self.assertFalse(sat_chat._valid_h264_frame(invalid))

    async def test_late_or_reserved_video_never_enters_assistant_audio(self) -> None:
        for data in (b"TSCV1bad", b"TSCV2bad", b"TSCV9unknown", b"TSCV"):
            self.assertTrue(await sat_chat.route_audio("native:no-call", data))
        self.assertFalse(await sat_chat.route_audio("native:no-call", b"\x00\x00"))

    async def test_h264_requires_both_qualified_checkers(self) -> None:
        clients = {
            "native:a": _client("A", "checkers", sat_chat_video=True, sat_chat_h264=True, sat_chat_h264_version=1),
            "native:b": _client("B", "checkers", sat_chat_video=True, sat_chat_h264=True, sat_chat_h264_version=1),
        }
        sent = AsyncMock()
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=clients)), patch.object(sat_chat,"_send",new=sent), patch.object(sat_chat,"refresh_directories",new=AsyncMock()):
            result = await sat_chat.start("native:a","native:b",mode="video")
        self.assertEqual(sent.await_args_list[0].args[2]["video_send_profile"],"echo-h264")
        self.assertEqual(sent.await_args_list[1].args[2]["video_send_profile"],"echo-h264")
        wire = b"TSCV2" + struct.pack("<IIIIIQQ",1280,720,30,0,1,0,123) + b"\0\0\0\1\x65\x88"
        self.assertTrue(sat_chat._valid_h264_frame(wire))
        with patch.object(sat_chat,"_queue_video_frame",new=AsyncMock()) as queue:
            self.assertTrue(await sat_chat.route_audio("native:a",wire))
            queue.assert_awaited_once()
        with patch.object(sat_chat,"_queue_video_frame",new=AsyncMock()) as queue:
            self.assertTrue(await sat_chat.route_audio("native:a",b"TSCV2bad"))
            queue.assert_not_awaited()
        await sat_chat.reset_for_tests()
        clients["native:b"]["capabilities"]["sat_chat_h264"]=False
        sent=AsyncMock()
        with patch.object(sat_chat,"_native_clients",new=AsyncMock(return_value=clients)), patch.object(sat_chat,"_send",new=sent), patch.object(sat_chat,"refresh_directories",new=AsyncMock()):
            await sat_chat.start("native:a","native:b",mode="video")
        self.assertEqual(sent.await_args_list[0].args[2]["video_send_profile"],"echo-balanced")

    async def asyncSetUp(self) -> None:
        await sat_chat.reset_for_tests()
        self.clients = {
            "native:kitchen": _client("Kitchen Crown", "crown"),
            "native:office": _client("Office Checkers", "checkers"),
            "native:den": _client("Den Athena", "athena"),
            "native:desk": _client("Desk Cronos", "cronos"),
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
        self.assertEqual(sent.await_args_list[0].args[2]["video_send_profile"], "echo-balanced")
        self.assertEqual(sent.await_args_list[1].args[2]["video_send_profile"], "echo-balanced")
        self.assertNotIn("sat_chat.invite", message_types)
        self.assertNotIn("sat_chat.ringing", message_types)

    async def test_athena_uses_echo_balanced_video_receive_profile(self) -> None:
        sent = AsyncMock()
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=sent
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            await sat_chat.start("native:den", "native:office")

        self.assertEqual(sent.await_args_list[0].args[2]["video_send_profile"], "echo-balanced")

    async def test_cronos_uses_echo_balanced_video_receive_profile(self) -> None:
        sent = AsyncMock()
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=sent
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            await sat_chat.start("native:desk", "native:office")

        self.assertEqual(sent.await_args_list[0].args[2]["video_send_profile"], "echo-balanced")

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
        with patch.object(native_satellite, "send_binary", new=send_binary):
            routed = await sat_chat.route_audio("native:kitchen", b"\x01\x02")
            ignored = await sat_chat.route_audio("native:bedroom", b"\x03\x04")
            for _ in range(20):
                if send_binary.await_count:
                    break
                await asyncio.sleep(0)

        self.assertTrue(routed)
        self.assertFalse(ignored)
        send_binary.assert_awaited_once_with("native:office", b"\x01\x02")

    async def test_slow_peer_never_blocks_source_reader_and_bounds_pcm_latency(self) -> None:
        with patch.object(sat_chat, "_native_clients", new=AsyncMock(return_value=self.clients)), patch.object(
            sat_chat, "_send", new=AsyncMock()
        ), patch.object(sat_chat, "refresh_directories", new=AsyncMock()):
            await sat_chat.start("native:kitchen", "native:office")

        started = asyncio.Event()
        release = asyncio.Event()
        sent = []

        async def slow_send(selector, frame):
            sent.append((selector, frame))
            if len(sent) == 1:
                started.set()
                await release.wait()

        frames = [bytes((index, index)) for index in range(8)]
        with patch.object(native_satellite, "send_binary", new=slow_send):
            await sat_chat.route_audio("native:kitchen", frames[0])
            await started.wait()
            # These calls model the WebSocket reader. They must only enqueue;
            # none may wait for the peer's blocked socket write.
            await asyncio.wait_for(
                asyncio.gather(
                    *(sat_chat.route_audio("native:kitchen", frame) for frame in frames[1:])
                ),
                timeout=0.1,
            )
            diagnostics = await sat_chat.relay_diagnostics()
            stats = diagnostics["relays"][0]
            self.assertEqual(stats["audio_received_frames"], len(frames))
            self.assertEqual(stats["audio_dropped_frames"], 3)
            self.assertEqual(stats["audio_queue_depth"], sat_chat.MAX_AUDIO_RELAY_FRAMES)
            release.set()
            for _ in range(40):
                if len(sent) == 1 + sat_chat.MAX_AUDIO_RELAY_FRAMES:
                    break
                await asyncio.sleep(0)

        self.assertEqual(
            sent,
            [("native:office", frames[0])]
            + [("native:office", frame) for frame in frames[-sat_chat.MAX_AUDIO_RELAY_FRAMES:]],
        )

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
        with patch.object(native_satellite, "send_binary", new=send_binary):
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

        frames = [
            sat_chat.VIDEO_FRAME_PREFIX + b"\xff\xd8one\xff\xd9",
            sat_chat.VIDEO_FRAME_PREFIX + b"\xff\xd8two\xff\xd9",
            sat_chat.VIDEO_FRAME_PREFIX + b"\xff\xd8three\xff\xd9",
        ]
        with patch.object(native_satellite, "send_binary", new=slow_send):
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

    async def test_balanced_echo_video_is_capped_at_640_by_360(self) -> None:
        source = Image.new("RGB", (1280, 720), (120, 80, 40))
        encoded = io.BytesIO()
        source.save(encoded, format="JPEG", quality=70)
        frame = sat_chat.VIDEO_FRAME_PREFIX + encoded.getvalue()

        resized = await asyncio.to_thread(
            sat_chat._resize_echo_video_frame,
            frame,
            max_size=(640, 360),
            quality=62,
        )

        with Image.open(io.BytesIO(resized[len(sat_chat.VIDEO_FRAME_PREFIX):])) as image:
            self.assertEqual(image.size, (640, 360))

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
