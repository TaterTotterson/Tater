#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import struct
import threading
import time
import uuid
from typing import Any, Dict

import websocket


def envelope(message_type: str, payload: Dict[str, Any] | None = None, *, message_id: str = "") -> Dict[str, Any]:
    return {
        "v": 1,
        "type": message_type,
        "id": message_id or uuid.uuid4().hex,
        "ts": time.time(),
        "payload": payload or {},
    }


def _json_wire(message: Dict[str, Any]) -> str:
    return json.dumps(message, separators=(",", ":"))


def _send_json(ws: websocket.WebSocket, message: Dict[str, Any]) -> None:
    line = _json_wire(message)
    print(f"> {line}", flush=True)
    ws.send(line)


def _pcm_chunks(
    *,
    sample_rate: int,
    sample_width: int,
    channels: int,
    duration_s: float,
    chunk_ms: int,
    tone_hz: float,
) -> list[bytes]:
    rate = max(1, int(sample_rate or 16000))
    width = max(1, int(sample_width or 2))
    channel_count = max(1, int(channels or 1))
    chunk_frames = max(1, int(rate * max(1, int(chunk_ms or 20)) / 1000))
    total_frames = max(1, int(rate * max(0.01, float(duration_s or 0.01))))
    chunks: list[bytes] = []
    frame = 0
    amplitude = 8000 if tone_hz > 0 else 0
    while frame < total_frames:
        frames = min(chunk_frames, total_frames - frame)
        if width != 2:
            chunks.append(b"\x00" * frames * width * channel_count)
            frame += frames
            continue
        values = bytearray()
        for index in range(frames):
            sample_index = frame + index
            sample = 0
            if amplitude:
                sample = int(amplitude * math.sin(2.0 * math.pi * float(tone_hz) * sample_index / rate))
            for _channel in range(channel_count):
                values.extend(struct.pack("<h", sample))
        chunks.append(bytes(values))
        frame += frames
    return chunks


def _run_voice_test(ws: websocket.WebSocket, args: argparse.Namespace) -> None:
    start = envelope(
        "voice.start",
        {
            "conversation_id": args.conversation_id,
            "wake_word": args.voice_wake_word,
            "audio_format": {
                "rate": args.sample_rate,
                "width": args.sample_width,
                "channels": args.channels,
            },
        },
    )
    _send_json(ws, start)
    chunks = _pcm_chunks(
        sample_rate=args.sample_rate,
        sample_width=args.sample_width,
        channels=args.channels,
        duration_s=args.audio_seconds,
        chunk_ms=args.chunk_ms,
        tone_hz=args.tone_hz,
    )
    frame_delay_s = max(0.001, float(args.chunk_ms or 20) / 1000.0)
    for index, chunk in enumerate(chunks, start=1):
        print(f"> binary chunk={index} bytes={len(chunk)}", flush=True)
        ws.send(chunk, opcode=websocket.ABNF.OPCODE_BINARY)
        time.sleep(frame_delay_s)
    stop = envelope("voice.stop", {"abort": not bool(args.voice_stop_finalize)})
    _send_json(ws, stop)


def main() -> int:
    parser = argparse.ArgumentParser(description="Simulate a Tater native satellite WebSocket client.")
    parser.add_argument("--url", default="ws://127.0.0.1:8501/api/tater/satellite/v1/ws")
    parser.add_argument("--device-id", default=f"sim-{uuid.uuid4().hex[:6]}")
    parser.add_argument("--device-name", default="Native Simulator")
    parser.add_argument("--board", default="sat1")
    parser.add_argument("--room", default="Lab")
    parser.add_argument("--token", default="")
    parser.add_argument("--seconds", type=float, default=20.0)
    parser.add_argument("--voice-test", action="store_true", help="Send voice.start, binary PCM chunks, and voice.stop.")
    parser.add_argument("--audio-seconds", type=float, default=0.8)
    parser.add_argument("--tone-hz", type=float, default=440.0)
    parser.add_argument("--sample-rate", type=int, default=16000)
    parser.add_argument("--sample-width", type=int, default=2)
    parser.add_argument("--channels", type=int, default=1)
    parser.add_argument("--chunk-ms", type=int, default=20)
    parser.add_argument("--conversation-id", default="")
    parser.add_argument("--voice-wake-word", default="tater")
    parser.add_argument("--voice-stop-finalize", action="store_true", help="Send voice.stop with abort=false.")
    args = parser.parse_args()

    headers = []
    if args.token:
        headers.append(f"X-Tater-Token: {args.token}")

    ws = websocket.create_connection(args.url, header=headers, timeout=10)
    stop = threading.Event()

    def receive_loop() -> None:
        while not stop.is_set():
            try:
                message = ws.recv()
            except Exception:
                break
            print(f"< {message}", flush=True)

    receiver = threading.Thread(target=receive_loop, daemon=True)
    receiver.start()

    hello = envelope(
        "hello",
        {
            "device_id": args.device_id,
            "device_name": args.device_name,
            "board": args.board,
            "firmware_version": "sim-0.1.0",
            "room": args.room,
            "capabilities": {
                "microphone": True,
                "speaker": True,
                "led_ring": True,
                "display": False,
                "buttons": True,
                "touch": False,
                "line_out": True,
                "local_wake": True,
                "ota": True,
                "xmos": True,
                "radar": True,
            },
        },
    )
    _send_json(ws, hello)

    if args.voice_test:
        time.sleep(0.5)
        _run_voice_test(ws, args)

    deadline = time.time() + max(1.0, float(args.seconds))
    index = 0
    while time.time() < deadline:
        index += 1
        status = envelope(
            "status",
            {
                "state": "idle",
                "uptime_s": index * 2,
                "wifi_rssi": -48,
                "free_heap": 123456,
            },
        )
        log = envelope("log", {"level": "info", "message": f"simulator heartbeat {index}"})
        _send_json(ws, status)
        _send_json(ws, log)
        time.sleep(2.0)

    stop.set()
    ws.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
