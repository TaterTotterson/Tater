from __future__ import annotations

import asyncio
import json
import os
import struct
import time
import unittest
from unittest.mock import AsyncMock, patch

from scripts import test_sat_chat as compatibility
from tater_voice import native_satellite, sat_chat


def picture(sequence: int, *, key: bool = False, stream: int = 123, pts: int | None = None) -> bytes:
    return b"TSCV2" + struct.pack(
        "<IIIIIQQ", 768, 432, 30, sequence, int(key),
        sequence * 66667 if pts is None else pts, stream,
    ) + b"\0\0\0\1\x65\x88"


async def settled(predicate, *, cycles: int = 300) -> None:
    for _ in range(cycles):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("relay did not settle")


class SplitRelayCompatibilityTests(compatibility.SatChatTests):
    """Run the existing compatibility/overflow tests with split video workers."""

    async def asyncSetUp(self) -> None:
        self.mode = patch.dict(os.environ, {"TATER_SAT_CHAT_RELAY_MODE": "split"})
        self.mode.start()
        await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        try:
            await super().asyncTearDown()
        finally:
            self.mode.stop()


class RelayComparisonTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await sat_chat.reset_for_tests()

    async def asyncTearDown(self) -> None:
        await sat_chat.reset_for_tests()

    async def test_opt_in_compares_whole_calls_without_changing_codec(self) -> None:
        clients = {
            f"native:{name}": compatibility._client(
                name, board, sat_chat_video=True, sat_chat_h264=True,
                sat_chat_h264_version=1, sat_chat_h264_balanced=True,
            ) for name, board in (("a", "checkers"), ("b", "cronos"))
        }
        with patch.dict(os.environ, {"TATER_SAT_CHAT_RELAY_MODE": "compare"}), patch.object(
            sat_chat, "_native_clients", new=AsyncMock(return_value=clients)
        ), patch.object(sat_chat, "_send", new=AsyncMock()) as send, patch.object(
            sat_chat, "refresh_directories", new=AsyncMock()
        ):
            for expected in ("shared", "split", "shared"):
                started = await sat_chat.start("native:a", "native:b", mode="video")
                call_id = started["call"]["id"]
                self.assertEqual(sat_chat._calls[call_id]["_relay_mode"], expected)
                for message in send.await_args_list[-2:]:
                    self.assertEqual(message.args[2]["video_send_profile"], "echo-h264-balanced")
                # Busy/failed starts must not advance the comparison sequence.
                with self.assertRaises(RuntimeError):
                    await sat_chat.start("native:a", "native:b", mode="video")
                diagnostics = await sat_chat.relay_diagnostics()
                self.assertEqual(diagnostics["calls"], [{"call_id": call_id, "relay_mode": expected}])
                # Freeze mode for the call, including both relay directions.
                with patch.dict(os.environ, {"TATER_SAT_CHAT_RELAY_MODE": "other"}):
                    with patch.object(native_satellite, "send_binary", new=AsyncMock()):
                        await sat_chat.route_audio("native:a", picture(0, key=True))
                        await sat_chat.route_audio("native:b", picture(0, key=True))
                        await settled(lambda: all(r["stats"]["sent_frames"] for r in sat_chat._video_relays.values()))
                    self.assertEqual({r["stats"]["relay_mode"] for r in sat_chat._video_relays.values()}, {expected})
                await sat_chat.end(call_id=call_id, force=True)
                await asyncio.sleep(0)

    async def test_normal_and_invalid_configuration_keep_shared_default(self) -> None:
        for value, expected in (("", "shared"), ("invalid", "shared"), ("shared", "shared"), ("split", "split")):
            with patch.dict(os.environ, {"TATER_SAT_CHAT_RELAY_MODE": value}):
                self.assertEqual(sat_chat._next_relay_mode(), expected)

    async def test_split_changes_scheduling_not_h264_bytes_or_order(self) -> None:
        for mode in ("shared", "split"):
            await sat_chat.reset_for_tests()
            audio_entered, release_audio = asyncio.Event(), asyncio.Event()
            sent = []

            async def send(_peer, frame):
                if not frame.startswith(b"TSCV"):
                    audio_entered.set()
                    await release_audio.wait()
                sent.append(frame)

            with patch.dict(os.environ, {"TATER_SAT_CHAT_RELAY_MODE": mode}), patch.object(
                native_satellite, "send_binary", new=send
            ):
                await sat_chat._queue_audio_frame("call", "native:a", "native:b", b"audio")
                await audio_entered.wait()
                frames = [picture(n, key=n == 0) for n in range(4)]
                for frame in frames:
                    await sat_chat._queue_video_frame("call", "native:a", "native:b", frame, profile="echo-h264-balanced")
                if mode == "split":
                    await settled(lambda: len(sent) == len(frames))
                    self.assertEqual(sent, frames)
                else:
                    await asyncio.sleep(0)
                    self.assertEqual(sent, [])
                release_audio.set()
                await settled(lambda: len(sent) == len(frames) + 1)
                self.assertEqual([frame for frame in sent if frame.startswith(b"TSCV")], frames)

    async def test_split_still_respects_actual_socket_write_serialization(self) -> None:
        lock = asyncio.Lock()
        entered, release = asyncio.Event(), asyncio.Event()
        sent = []

        async def serial_send(_peer, frame):
            async with lock:
                if frame == b"audio":
                    entered.set()
                    await release.wait()
                sent.append(frame)

        with patch.dict(os.environ, {"TATER_SAT_CHAT_RELAY_MODE": "split"}), patch.object(
            native_satellite, "send_binary", new=serial_send
        ):
            await sat_chat._queue_audio_frame("call", "native:a", "native:b", b"audio")
            await entered.wait()
            await sat_chat._queue_video_frame("call", "native:a", "native:b", picture(0, key=True))
            await asyncio.sleep(0)
            self.assertEqual(sent, [])
            release.set()
            await settled(lambda: len(sent) == 2)
            self.assertEqual(sent, [b"audio", picture(0, key=True)])

    async def test_timing_separates_arrival_routing_queue_preparation_and_write(self) -> None:
        with patch.object(native_satellite, "send_binary", new=AsyncMock()):
            await sat_chat._queue_video_frame("call", "native:a", "native:b", picture(0, key=True))
            await settled(lambda: sat_chat._video_relays[("call", "native:b")]["stats"]["sent_frames"] == 1)
        relay = sat_chat._video_relays[("call", "native:b")]
        relay["recent_video"].clear()
        relay["started_at"] = 10.0
        packet = sat_chat._MediaPacket(picture(1), 10.1, 300.0, 66.667, 1, 123, 10.12)
        with patch.object(sat_chat.time, "monotonic", return_value=10.3):
            sat_chat._observe_video_output(relay, packet.data, 10.25, packet=packet, dequeued=10.2)
        sample = relay["recent_video"][0]
        self.assertEqual(sample["route_wait_ms"], 20.0)
        self.assertEqual(sample["queue_wait_ms"], 80.0)
        self.assertEqual(sample["prepare_ms"], 50.0)
        self.assertEqual(sample["send_ms"], 50.0)
        self.assertEqual(sample["input_gap_ms"], 300.0)
        self.assertEqual(sample["source_gap_ms"], 66.667)

    async def test_source_gap_and_sequence_measurements_reset_with_stream(self) -> None:
        with patch.object(native_satellite, "send_binary", new=AsyncMock()):
            await sat_chat._queue_video_frame("call", "native:a", "native:b", picture(0, key=True))
            await settled(lambda: sat_chat._video_relays[("call", "native:b")]["stats"]["sent_frames"] == 1)
        relay = sat_chat._video_relays[("call", "native:b")]
        a = sat_chat._observe_video_input(relay, picture(2), received_at=20.0)
        b = sat_chat._observe_video_input(relay, picture(3), received_at=20.4)
        self.assertAlmostEqual(b.input_gap_ms, 400.0)
        self.assertAlmostEqual(b.source_gap_ms, 66.667)
        sat_chat._observe_video_input(relay, picture(3), received_at=20.41)
        sat_chat._observe_video_input(relay, picture(2), received_at=20.42)
        reset = sat_chat._observe_video_input(relay, picture(0, stream=999), received_at=20.43)
        self.assertIsNone(reset.source_gap_ms)
        self.assertEqual(relay["stats"]["source_sequence_missing"], 1)
        self.assertEqual(relay["stats"]["source_sequence_duplicates"], 1)
        self.assertEqual(relay["stats"]["source_sequence_reversed"], 1)

    async def test_diagnostics_are_bounded_metadata_without_media_contents(self) -> None:
        with patch.object(native_satellite, "send_binary", new=AsyncMock()):
            for n in range(160):
                await sat_chat._queue_video_frame("call", "native:a", "native:b", picture(n, key=n == 0))
                await settled(lambda: sat_chat._video_relays[("call", "native:b")]["stats"]["sent_frames"] == n + 1)
        result = await sat_chat.relay_diagnostics()
        row = result["relays"][0]
        self.assertEqual(len(row["recent_video"]), 120)
        self.assertEqual(row["recent_video"][0]["sequence"], 40)
        self.assertNotIn("TSCV", json.dumps(result))
        self.assertFalse(any(key.startswith("_") for key in row))

    async def test_loop_monitor_detects_host_stalls_and_exits_on_reset(self) -> None:
        with patch.object(native_satellite, "send_binary", new=AsyncMock()):
            await sat_chat._queue_video_frame("call", "native:a", "native:b", picture(0, key=True))
            await asyncio.sleep(0.015)
            time.sleep(0.12)  # Isolated synthetic test loop, never the running app.
            await asyncio.sleep(0.06)
        relay = sat_chat._video_relays[("call", "native:b")]
        self.assertGreater(relay["stats"]["maximum_event_loop_lag_ms"], 50.0)
        self.assertTrue(relay["loop_stalls"])
        owned = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        await sat_chat.reset_for_tests()
        self.assertTrue(all(t.done() for t in owned))

    async def test_split_worker_failure_cancels_sibling_and_monitor(self) -> None:
        async def fail(_peer, _frame):
            raise RuntimeError("synthetic peer failure")

        with patch.dict(os.environ, {"TATER_SAT_CHAT_RELAY_MODE": "split"}), patch.object(
            native_satellite, "send_binary", new=fail
        ), patch.object(sat_chat, "end", new=AsyncMock()) as end:
            await sat_chat._queue_video_frame("call", "native:a", "native:b", picture(0, key=True))
            task = sat_chat._video_relays[("call", "native:b")]["task"]
            await asyncio.wait_for(task, 1)
            end.assert_awaited_once()
        self.assertEqual(sat_chat._video_relays, {})
        self.assertEqual([t for t in asyncio.all_tasks() if t is not asyncio.current_task()], [])

    async def test_opt_in_stall_trace_records_code_only_and_stops_with_call(self) -> None:
        local_secret = "test-only-value-never-record-this"
        with patch.dict(os.environ, {"TATER_SAT_CHAT_STALL_TRACE": "1"}), patch.object(
            native_satellite, "send_binary", new=AsyncMock()
        ):
            await sat_chat._queue_video_frame("call", "native:a", "native:b", picture(0, key=True))
            await asyncio.sleep(0.015)
            time.sleep(0.25)  # Synthetic stall in this isolated test process.
            await asyncio.sleep(0.06)
            row = (await sat_chat.relay_diagnostics())["relays"][0]
            self.assertTrue(row["stall_stacks"])
            encoded = json.dumps(row["stall_stacks"])
            self.assertNotIn(local_secret, encoded)
            self.assertNotIn("TSCV", encoded)
            for sample in row["stall_stacks"]:
                for frame in sample["stack_innermost_first"]:
                    self.assertEqual(set(frame), {"file", "function", "line"})
            trace = sat_chat._video_relays[("call", "native:b")]["stall_trace"]
            await sat_chat.reset_for_tests()
            self.assertFalse(trace.thread.is_alive())

    async def test_both_modes_through_duplex_wsproto_with_backpressure(self) -> None:
        from scripts.test_websocket_transport import MemoryTransport
        from uvicorn.config import Config
        from uvicorn.protocols.websockets.wsproto_impl import WSProtocol
        from uvicorn.server import ServerState
        from wsproto import ConnectionType, WSConnection, events

        for mode in ("shared", "split"):
            await sat_chat.reset_for_tests()
            state = ServerState()
            protocols, transports, clients = [], [], []
            peers = ("native:a", "native:b")

            async def app(scope, receive, send):
                self.assertEqual((await receive())["type"], "websocket.connect")
                await send({"type": "websocket.accept"})
                selector = scope["path"].removeprefix("/")
                lock = asyncio.Lock()

                async def binary_sender(data):
                    # Same serialization contract as native_satellite's handler.
                    async with lock:
                        await send({"type": "websocket.send", "bytes": data})

                native_satellite._clients[selector] = {"connected": True, "send_binary": binary_sender}
                while True:
                    message = await receive()
                    if message["type"] == "websocket.disconnect":
                        return
                    if message.get("bytes") is not None:
                        await sat_chat.route_audio(selector, message["bytes"], received_at=time.monotonic())

            with patch.dict(native_satellite._clients, {}, clear=True):
                sat_chat._calls["duplex"] = {
                    "state": "active", "mode": "video", "caller": peers[0], "target": peers[1],
                    "_video_profiles": {peer: "echo-h264-balanced" for peer in peers},
                    "_relay_mode": mode,
                }
                sat_chat._endpoint_calls.update({peer: "duplex" for peer in peers})
                try:
                    for peer in peers:
                        protocol = WSProtocol(Config(
                            app, ws="wsproto", log_config=None, log_level="critical",
                            lifespan="off", ws_ping_interval=None,
                        ), state, {})
                        transport, client = MemoryTransport(), WSConnection(ConnectionType.CLIENT)
                        protocols.append(protocol)
                        transports.append(transport)
                        clients.append(client)
                        protocol.connection_made(transport)
                        protocol.data_received(client.send(events.Request(host="localhost", target="/" + peer)))
                        await settled(lambda: bool(transport.output))
                        client.receive_data(transport.take_output())
                        self.assertIsInstance(list(client.events())[0], events.AcceptConnection)
                    await settled(lambda: len(native_satellite._clients) == 2)
                    protocols[1].pause_writing()
                    for n in range(8):
                        for index in range(2):
                            wire = picture(n, key=n == 0, stream=index + 1)
                            protocols[index].data_received(clients[index].send(events.BytesMessage(data=wire)))
                            protocols[index].data_received(clients[index].send(events.BytesMessage(data=b"\0\1" * 40)))
                        # Model the normal 15fps/12.5fps producer's opportunity
                        # to run; this is a byte-integrity test, not an FPS test.
                        for _ in range(5):
                            await asyncio.sleep(0)
                    protocols[1].resume_writing()
                    await settled(lambda: len(sat_chat._video_relays) == 2 and all(
                        relay["stats"]["sent_frames"] == 8 for relay in sat_chat._video_relays.values()
                    ))
                    for index, client in enumerate(clients):
                        client.receive_data(transports[index].take_output())
                        frames = [event.data for event in client.events() if isinstance(event, events.BytesMessage)]
                        self.assertEqual(
                            [bytes(frame) for frame in frames if frame.startswith(b"TSCV")],
                            [picture(n, key=n == 0, stream=2 - index) for n in range(8)],
                        )
                    self.assertTrue(all(relay["stats"]["dropped_gops"] == 0 for relay in sat_chat._video_relays.values()))
                finally:
                    for protocol in protocols:
                        protocol.resume_writing()
                        protocol.stop_keepalive()
                    await sat_chat.reset_for_tests()
                    for task in list(state.tasks):
                        task.cancel()
                    await asyncio.gather(*list(state.tasks), return_exceptions=True)
                    state.connections.clear()


if __name__ == "__main__":
    unittest.main()
