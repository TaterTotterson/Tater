from __future__ import annotations

import asyncio
import io
import json
import struct
import sys
import time
import types
import unittest
from array import array
from pathlib import Path
from unittest import mock

from aiohttp import WSMsgType, web

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tater_voice import sendspin_playback


def _little_endian_pcm(samples: list[int]) -> bytes:
    values = array("h", samples)
    if sendspin_playback.sys.byteorder != "little":
        values.byteswap()
    return values.tobytes()


def _pcm_samples(payload: bytes) -> list[int]:
    values = array("h")
    values.frombytes(payload)
    if sendspin_playback.sys.byteorder != "little":
        values.byteswap()
    return list(values)


class SendspinReplyPcmTests(unittest.TestCase):
    def test_finite_media_reader_decodes_url_with_seek_to_sendspin_pcm(self) -> None:
        process = mock.Mock()
        process.stdout = io.BytesIO(b"pcm")
        process.stderr = io.BytesIO()
        process.wait.return_value = 0
        process.poll.return_value = 0
        with (
            mock.patch.object(sendspin_playback, "_ffmpeg_binary", return_value="/fake/ffmpeg"),
            mock.patch.object(sendspin_playback.subprocess, "Popen", return_value=process) as popen,
        ):
            reader = sendspin_playback._FfmpegPcmReader(
                "https://example.test/song.flac",
                start_position_seconds=12.5,
            )
            self.assertEqual(reader.read(16_384, 0.5), b"pcm")
            self.assertIsNone(reader.read(16_384, 0.5))
            reader.close()

        command = popen.call_args.args[0]
        self.assertIn("12.500", command)
        self.assertIn("https://example.test/song.flac", command)
        self.assertEqual(command[-4:], ["pcm_s16le", "-f", "s16le", "pipe:1"])

    def test_mono_reply_is_duplicated_with_independent_trim_and_delay(self) -> None:
        decoded = _little_endian_pcm([1000, -1000])
        completed = types.SimpleNamespace(returncode=0, stdout=decoded, stderr=b"")
        with (
            mock.patch.object(sendspin_playback, "_ffmpeg_binary", return_value="/fake/ffmpeg"),
            mock.patch.object(sendspin_playback.subprocess, "run", return_value=completed),
        ):
            prepared = sendspin_playback._prepare_pcm_sync(
                b"source",
                preserve_stereo=False,
                left_delay_ms=0,
                right_delay_ms=1,
                left_volume_percent=50,
                right_volume_percent=100,
            )

        samples = _pcm_samples(prepared.pcm)
        self.assertEqual(prepared.frames, 50)
        self.assertEqual(samples[0:4], [500, 0, -500, 0])
        self.assertEqual(samples[48 * 2 + 1], 1000)
        self.assertEqual(samples[49 * 2 + 1], -1000)

    def test_stereo_scene_preserves_left_and_right_content(self) -> None:
        decoded = _little_endian_pcm([1000, 2000, -3000, 4000])
        completed = types.SimpleNamespace(returncode=0, stdout=decoded, stderr=b"")
        with (
            mock.patch.object(sendspin_playback, "_ffmpeg_binary", return_value="/fake/ffmpeg"),
            mock.patch.object(sendspin_playback.subprocess, "run", return_value=completed),
        ):
            prepared = sendspin_playback._prepare_pcm_sync(
                b"source",
                preserve_stereo=True,
                left_delay_ms=0,
                right_delay_ms=0,
                left_volume_percent=100,
                right_volume_percent=50,
            )

        self.assertEqual(_pcm_samples(prepared.pcm), [1000, 1000, -3000, 2000])

    def test_audio_packet_uses_player_message_id_and_big_endian_timestamp(self) -> None:
        packet = sendspin_playback._audio_packet(1_234_567, b"pcm")
        self.assertEqual(packet[0], 4)
        self.assertEqual(struct.unpack(">q", packet[1:9])[0], 1_234_567)
        self.assertEqual(packet[9:], b"pcm")

    def test_live_airplay_pcm_is_resampled_scaled_and_delayed(self) -> None:
        source = _little_endian_pcm([1000, -1000] * 441)
        converter = sendspin_playback._StreamingPcmResampler(44_100)
        converted = converter.convert(source)
        frames = len(converted) // 4
        self.assertGreaterEqual(frames, 478)
        self.assertLessEqual(frames, 480)

        scaled = sendspin_playback._scale_pcm_s16le(
            _little_endian_pcm([1000, -1000]),
            50,
        )
        self.assertEqual(_pcm_samples(scaled), [500, -500])
        delay = sendspin_playback._PcmDelayLine(1)
        self.assertEqual(
            _pcm_samples(delay.apply(_little_endian_pcm([1000, -1000, 2000, -2000]))),
            [0, 0, 1000, -1000],
        )

    def test_live_resampler_has_a_python_fallback_without_audioop(self) -> None:
        source = _little_endian_pcm([1000, -1000] * 441)
        with mock.patch.object(sendspin_playback, "_audioop", None):
            converter = sendspin_playback._StreamingPcmResampler(44_100)
            converted = converter.convert(source)

        frames = len(converted) // 4
        self.assertGreaterEqual(frames, 478)
        self.assertLessEqual(frames, 480)


class _FakeWebSocket:
    def __init__(self) -> None:
        self.closed = False
        self.messages: list[str] = []

    async def send_str(self, payload: str) -> None:
        self.messages.append(payload)


class SendspinReplyHandshakeTests(unittest.IsolatedAsyncioTestCase):
    async def test_client_time_requests_receive_four_timestamp_reply(self) -> None:
        peer = sendspin_playback._SendspinPeer(
            mock.Mock(),
            {"selector": "native:left", "host": "192.0.2.10"},
        )
        peer.ws = _FakeWebSocket()

        for sequence in range(8):
            await peer._handle_json(
                json.dumps(
                    {
                        "type": "client/time",
                        "payload": {"client_transmitted": 1000 + sequence},
                    }
                )
            )

        response = json.loads(peer.ws.messages[-1])
        self.assertEqual(response["type"], "server/time")
        self.assertEqual(response["payload"]["client_transmitted"], 1007)
        self.assertIn("server_received", response["payload"])
        self.assertIn("server_transmitted", response["payload"])
        self.assertTrue(peer.time_event.is_set())

    async def test_ready_requires_pcm_48k_stereo_player(self) -> None:
        peer = sendspin_playback._SendspinPeer(
            mock.Mock(),
            {"selector": "native:left", "host": "192.0.2.10"},
        )
        peer.client_hello = {
            "version": 1,
            "supported_roles": ["player@v1"],
            "player@v1_support": {
                "supported_formats": [
                    {
                        "codec": "pcm",
                        "channels": 2,
                        "sample_rate": 48000,
                        "bit_depth": 16,
                    }
                ]
            },
        }
        peer.client_state = "synchronized"
        peer.hello_event.set()
        peer.state_event.set()
        peer.time_event.set()

        await peer.wait_ready()


class _MockSendspinPlayer:
    def __init__(self) -> None:
        self.runner: web.AppRunner | None = None
        self.port = 0
        self.audio_packets: list[bytes] = []
        self.json_types: list[str] = []
        self.time_sequence = 0

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/sendspin", self._handle)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        server = site._server
        assert server is not None
        self.port = int(server.sockets[0].getsockname()[1])

    async def close(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()

    async def _send_time(self, ws: web.WebSocketResponse) -> None:
        await ws.send_json(
            {
                "type": "client/time",
                "payload": {"client_transmitted": 10_000 + self.time_sequence},
            }
        )

    async def _handle(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.send_json(
            {
                "type": "client/hello",
                "payload": {
                    "client_id": f"mock-{self.port}",
                    "name": "Mock Player",
                    "version": 1,
                    "supported_roles": ["player@v1"],
                    "player@v1_support": {
                        "supported_formats": [
                            {
                                "codec": "pcm",
                                "channels": 2,
                                "sample_rate": 48000,
                                "bit_depth": 16,
                            }
                        ]
                    },
                },
            }
        )
        async for message in ws:
            if message.type == WSMsgType.TEXT:
                payload = json.loads(message.data)
                message_type = payload.get("type")
                self.json_types.append(message_type)
                if message_type == "server/hello":
                    await ws.send_json(
                        {
                            "type": "client/state",
                            "payload": {"state": "synchronized", "player": {}},
                        }
                    )
                    await self._send_time(ws)
                elif message_type == "server/time":
                    self.time_sequence += 1
                    if self.time_sequence < 8:
                        await self._send_time(ws)
            elif message.type == WSMsgType.BINARY:
                self.audio_packets.append(bytes(message.data))
        return ws


class SendspinReplyWireIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_players_receive_one_identical_timestamped_timeline(self) -> None:
        left = _MockSendspinPlayer()
        right = _MockSendspinPlayer()
        await left.start()
        await right.start()
        prepared = sendspin_playback._PreparedPcm(
            pcm=_little_endian_pcm([500, 500] * 1920),
            frames=1920,
            duration_s=0.04,
        )
        started = asyncio.Event()
        try:
            with (
                mock.patch.object(sendspin_playback, "SENDSPIN_START_LEAD_US", 150_000),
                mock.patch.object(sendspin_playback, "SENDSPIN_BUFFER_AHEAD_US", 100_000),
                mock.patch.object(sendspin_playback, "SENDSPIN_END_MARGIN_S", 0.01),
                mock.patch.object(sendspin_playback, "SENDSPIN_HANDSHAKE_TIMEOUT_S", 2.0),
            ):
                result = await sendspin_playback._stream_pair_pcm(
                    [
                        {"selector": "native:left", "host": "127.0.0.1", "port": left.port},
                        {"selector": "native:right", "host": "127.0.0.1", "port": right.port},
                    ],
                    prepared,
                    pair_id="bedroom12",
                    pair_name="Bedroom",
                    started_event=started,
                )
        finally:
            await left.close()
            await right.close()

        self.assertTrue(result["playback_completed"])
        self.assertTrue(started.is_set())
        self.assertEqual(left.audio_packets, right.audio_packets)
        self.assertEqual(len(left.audio_packets), 2)
        self.assertTrue(all(packet[0] == 4 for packet in left.audio_packets))
        first_timestamp = struct.unpack(">q", left.audio_packets[0][1:9])[0]
        second_timestamp = struct.unpack(">q", left.audio_packets[1][1:9])[0]
        self.assertEqual(second_timestamp - first_timestamp, 20_000)
        self.assertIn("stream/start", left.json_types)
        self.assertIn("stream/end", left.json_types)

    async def test_live_airplay_pcm_uses_one_sendspin_timeline(self) -> None:
        left = _MockSendspinPlayer()
        right = _MockSendspinPlayer()
        await left.start()
        await right.start()
        source = _little_endian_pcm([750, -750] * 441)

        def read_pcm(_maximum: int, _timeout: float) -> bytes:
            return source

        try:
            with (
                mock.patch.object(sendspin_playback, "SENDSPIN_BUFFER_AHEAD_US", 200_000),
                mock.patch.object(sendspin_playback, "SENDSPIN_HANDSHAKE_TIMEOUT_S", 2.0),
            ):
                result = await sendspin_playback.start_live_pcm_stream(
                    "airplay-test",
                    [
                        {
                            "selector": "native:left",
                            "host": "127.0.0.1",
                            "port": left.port,
                            "volume_percent": 100,
                            "delay_ms": 0,
                        },
                        {
                            "selector": "native:right",
                            "host": "127.0.0.1",
                            "port": right.port,
                            "volume_percent": 100,
                            "delay_ms": 0,
                        },
                    ],
                    read_pcm,
                    input_sample_rate=44_100,
                    start_lead_ms=250,
                )
                await asyncio.sleep(0.04)
                stopped = await sendspin_playback.stop_live_stream("airplay-test")
        finally:
            await sendspin_playback.stop_live_stream("airplay-test")
            await left.close()
            await right.close()

        self.assertTrue(result["sendspin_live_stream_started"])
        self.assertTrue(stopped["stopped"])
        self.assertGreaterEqual(len(left.audio_packets), 1)
        self.assertEqual(left.audio_packets, right.audio_packets)
        self.assertEqual(struct.unpack(">q", left.audio_packets[0][1:9])[0], result["start_server_us"])
        self.assertIn("stream/start", left.json_types)
        self.assertIn("stream/end", left.json_types)

    async def test_native_and_airplay_bridge_consume_the_same_sendspin_source(self) -> None:
        import airplay_bridge

        native = _MockSendspinPlayer()
        await native.start()
        source_reads = [
            _little_endian_pcm([900, -900] * 1920),
            None,
        ]
        airplay_pcm: list[bytes] = []

        def read_pcm(_maximum: int, _timeout: float) -> bytes | None:
            return source_reads.pop(0)

        def write_bridge(_group_id: str, pcm: bytes) -> dict[str, object]:
            airplay_pcm.append(bytes(pcm))
            return {"ok": True, "sent_count": 1}

        try:
            with (
                mock.patch.object(sendspin_playback, "SENDSPIN_AIRPLAY_PRIME_SECONDS", 0.02),
                mock.patch.object(sendspin_playback, "SENDSPIN_HANDSHAKE_TIMEOUT_S", 2.0),
                mock.patch.object(
                    airplay_bridge,
                    "prepare_sendspin_airplay_bridge",
                    return_value={
                        "ok": True,
                        "group_id": "sendspin-airplay-test",
                        "prepared_count": 1,
                        "prepared_targets": ["airplay:804af2c57d78"],
                        "routes": {
                            "airplay:804af2c57d78": {
                                "protocol": "airplay2",
                                "flow": "buffered",
                                "timing": "ptp",
                            }
                        },
                    },
                ) as prepare,
                mock.patch.object(
                    airplay_bridge,
                    "write_sendspin_airplay_bridge_pcm",
                    side_effect=write_bridge,
                ),
                mock.patch.object(
                    airplay_bridge,
                    "wait_sendspin_airplay_bridge_ready",
                    return_value={
                        "ok": True,
                        "ready_count": 1,
                        "minimum_start_unix_ms": int(time.time() * 1000) + 250,
                    },
                ),
                mock.patch.object(
                    airplay_bridge,
                    "start_sendspin_airplay_bridge",
                    return_value={
                        "ok": True,
                        "sent_count": 1,
                        "timing_mode": "ptp",
                        "start_unix_ms": int(time.time() * 1000) + 250,
                    },
                ) as commit,
                mock.patch.object(
                    airplay_bridge,
                    "stop_airplay_group_sync",
                    return_value={"ok": True, "sent_count": 1},
                ),
            ):
                result = await sendspin_playback.start_live_pcm_stream(
                    "mixed-sendspin-test",
                    [
                        {
                            "selector": "native:kitchen",
                            "host": "127.0.0.1",
                            "port": native.port,
                            "volume_percent": 100,
                            "delay_ms": 0,
                        }
                    ],
                    read_pcm,
                    input_sample_rate=48_000,
                    start_lead_ms=250,
                    airplay_targets=["airplay:804af2c57d78"],
                    airplay_volume_percent={"airplay:804af2c57d78": 65},
                    airplay_sync_offset_ms={"airplay:804af2c57d78": 40},
                    airplay_reference_sync_offset_ms=-20,
                    title="Shared Song",
                )
                await asyncio.sleep(0.05)
                await sendspin_playback.stop_live_stream("mixed-sendspin-test")
        finally:
            await sendspin_playback.stop_live_stream("mixed-sendspin-test")
            await native.close()

        self.assertEqual(
            result["members"],
            ["native:kitchen", "airplay:804af2c57d78"],
        )
        self.assertEqual(result["airplay_bridge_sent_count"], 1)
        self.assertEqual(result["airplay_bridge_timing_mode"], "ptp")
        self.assertGreaterEqual(len(native.audio_packets), 1)
        self.assertGreaterEqual(len(airplay_pcm), 1)
        self.assertEqual(len(native.audio_packets[0][9:]), 960 * 4)
        self.assertGreater(len(airplay_pcm[0]), 0)
        self.assertLess(len(airplay_pcm[0]), len(native.audio_packets[0][9:]))
        self.assertEqual(prepare.call_args.kwargs["pcm_sample_rate"], 44_100)
        self.assertEqual(commit.call_args.kwargs["reference_sync_offset_ms"], -20)


if __name__ == "__main__":
    unittest.main()
