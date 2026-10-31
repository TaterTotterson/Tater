from __future__ import annotations

import asyncio
import ast
import pathlib
import sys
import types
import unittest
from types import SimpleNamespace
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Keep this focused regression test runnable on developer hosts that do not
# carry Tater's web-server dependencies. native_satellite only needs these two
# FastAPI symbols at import time; its voice bridge remains ordinary asyncio.
try:
    import fastapi  # noqa: F401
except ModuleNotFoundError:
    fastapi_stub = types.ModuleType("fastapi")
    fastapi_stub.WebSocket = object
    fastapi_stub.WebSocketDisconnect = type("WebSocketDisconnect", (Exception,), {})
    sys.modules["fastapi"] = fastapi_stub

from tater_voice import native_satellite  # noqa: E402


class _VoiceSessionRuntime:
    def __init__(self, **values: object) -> None:
        self.__dict__.update(values)


def _load_pipeline_boundary_helpers():
    source = (REPO_ROOT / "tater_voice" / "voice_pipeline" / "__init__.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(source)
    wanted = {
        "_native_wake_preroll_pending",
        "_native_record_wake_preroll",
        "_native_mark_wake_preroll_complete",
    }
    functions = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted
    ]
    if {node.name for node in functions} != wanted:
        raise RuntimeError("native wake pre-roll helpers are missing from voice_pipeline")
    namespace = {
        "VoiceSessionRuntime": _VoiceSessionRuntime,
        "_text": lambda value: str(value or "").strip(),
        "_selector_runtime": lambda _selector: {},
        "logger": mock.Mock(),
    }
    module = ast.Module(body=functions, type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), "<native-wake-preroll>", "exec"), namespace)
    return namespace


PIPELINE_HELPERS = _load_pipeline_boundary_helpers()


class NativeWakePrerollTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        native_satellite._clients.clear()
        native_satellite._clients_lock = asyncio.Lock()

    async def asyncTearDown(self) -> None:
        native_satellite._clients.clear()
        native_satellite._clients_lock = asyncio.Lock()

    def test_voice_start_keeps_wake_boundary_capability(self) -> None:
        settings = native_satellite._audio_settings_from_payload(
            {
                "audio_format": {"rate": 16000, "width": 2, "channels": 1},
                "wake_preroll_boundary": 1,
            }
        )

        self.assertEqual(16000, settings["rate"])
        self.assertEqual(1, settings["wake_preroll_boundary"])

    async def test_audio_worker_preserves_preroll_boundary_order(self) -> None:
        events: list[tuple[str, bytes | str]] = []

        async def mark_boundary(selector: str) -> bool:
            events.append(("boundary", selector))
            return True

        fake_pipeline = SimpleNamespace(
            logger=mock.Mock(),
            _native_mark_wake_preroll_complete=mark_boundary,
        )
        bridge = native_satellite._NativeVoicePipelineBridge("native:test", asyncio.Queue())

        async def handle_audio(data: bytes) -> None:
            events.append(("audio", bytes(data)))

        bridge.client._callbacks["handle_audio"] = handle_audio
        with mock.patch.object(native_satellite, "_vp", return_value=fake_pipeline):
            await bridge.audio(b"wake-preroll")
            await bridge.wake_preroll_boundary()
            await bridge.audio(b"immediate-command")
            await asyncio.wait_for(bridge._audio_queue.join(), timeout=1.0)
            if bridge._audio_task is not None:
                bridge._audio_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await bridge._audio_task

        self.assertEqual(
            [
                ("audio", b"wake-preroll"),
                ("boundary", "native:test"),
                ("audio", b"immediate-command"),
            ],
            events,
        )

    async def test_protocol_boundary_reaches_voice_bridge(self) -> None:
        selector = "native:test"
        native_satellite._clients[selector] = {"hello": {}, "last_seen_ts": 0.0}
        bridge = SimpleNamespace(wake_preroll_boundary=mock.AsyncMock())

        with mock.patch.object(
            native_satellite,
            "_voice_bridge",
            new=mock.AsyncMock(return_value=bridge),
        ):
            reply = await native_satellite._handle_text_message(
                selector,
                {
                    "type": "voice.audio_boundary",
                    "payload": {"kind": "wake_preroll_end", "version": 1},
                },
            )

        self.assertIsNone(reply)
        bridge.wake_preroll_boundary.assert_awaited_once_with()

    async def test_boundary_opens_vad_without_wall_clock_pause(self) -> None:
        session = _VoiceSessionRuntime(
            selector="native:test",
            session_id="session-1",
            conversation_id="conversation-1",
            wake_word="hey_tater",
            audio_format={"rate": 16000, "width": 2, "channels": 1},
            started_ts=100.0,
            startup_gate_until_ts=100.0,
            wake_preroll_boundary_expected=True,
            wake_preroll_boundary_received=False,
            wake_preroll_boundary_deadline_ts=102.0,
            vad_startup_ignore_s=0.0,
            vad_startup_ignore_until_ts=0.0,
            wake_preroll_chunks=25,
            wake_preroll_bytes=64000,
        )
        runtime = {"lock": asyncio.Lock(), "session": session}
        PIPELINE_HELPERS["_selector_runtime"] = lambda _selector: runtime
        boundary_pending = PIPELINE_HELPERS["_native_wake_preroll_pending"]
        mark_boundary = PIPELINE_HELPERS["_native_mark_wake_preroll_complete"]

        self.assertTrue(boundary_pending(session, 100.1))
        self.assertTrue(await mark_boundary("native:test"))

        self.assertTrue(session.wake_preroll_boundary_received)
        self.assertFalse(boundary_pending(session, 100.1))
        self.assertEqual(0.0, session.vad_startup_ignore_until_ts)

    def test_pre_boundary_audio_is_accounted_for_stt_retention(self) -> None:
        session = _VoiceSessionRuntime(
            last_audio_ts=0.0,
            wake_preroll_chunks=0,
            wake_preroll_bytes=0,
            audio_buffer=bytearray(b"existing-command"),
        )
        record = PIPELINE_HELPERS["_native_record_wake_preroll"]

        record(session, b"wake-word-audio", 123.5)

        self.assertEqual(123.5, session.last_audio_ts)
        self.assertEqual(1, session.wake_preroll_chunks)
        self.assertEqual(len(b"wake-word-audio"), session.wake_preroll_bytes)
        self.assertEqual(bytearray(b"existing-command"), session.audio_buffer)

    def test_pre_boundary_path_continues_into_stt_capture(self) -> None:
        source = (
            REPO_ROOT / "tater_voice" / "voice_pipeline" / "__init__.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(source)
        subscriber = next(
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "_esphome_subscribe_voice_assistant"
        )
        handle_audio = next(
            node
            for node in subscriber.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "_handle_audio"
        )
        boundary_branch = next(
            node
            for node in ast.walk(handle_audio)
            if isinstance(node, ast.If)
            and isinstance(node.test, ast.Name)
            and node.test.id == "wake_boundary_pending"
        )

        self.assertTrue(
            any(
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "_native_record_wake_preroll"
                for node in ast.walk(boundary_branch)
            )
        )
        self.assertFalse(
            any(isinstance(node, ast.Return) for node in ast.walk(boundary_branch))
        )

    def test_wake_boundary_uses_separate_spud_stt_and_vad_paths(self) -> None:
        source = (
            REPO_ROOT / "tater_voice" / "voice_pipeline" / "__init__.py"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "hub_stt_routed and not wake_preroll_boundary_expected",
            source,
        )

    def test_session_runtime_carries_boundary_state(self) -> None:
        source = (
            REPO_ROOT / "tater_voice" / "voice_pipeline" / "conversation.py"
        ).read_text(encoding="utf-8")
        tree = ast.parse(source)
        runtime = next(
            node
            for node in tree.body
            if isinstance(node, ast.ClassDef) and node.name == "VoiceSessionRuntime"
        )
        fields = {
            node.target.id
            for node in runtime.body
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
        }
        self.assertTrue(
            {
                "wake_preroll_boundary_expected",
                "wake_preroll_boundary_received",
                "wake_preroll_boundary_deadline_ts",
                "wake_preroll_chunks",
                "wake_preroll_bytes",
            }.issubset(fields)
        )


if __name__ == "__main__":
    unittest.main()
