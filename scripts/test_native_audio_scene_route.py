from __future__ import annotations

import ast
import asyncio
import base64
import contextlib
import sys
import types
import unittest
from pathlib import Path
from typing import Any, Dict, Optional


class FakeHttpException(Exception):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class FakeReplyPlayback:
    REPLY_PLAYBACK_DEVICE = "device"
    REPLY_PLAYBACK_SILENT = "silent"

    @staticmethod
    def resolve_reply_playback_target(*_args, **_kwargs):
        return "device"


class FakeLogger:
    def warning(self, *_args, **_kwargs):
        return None


class FakeVoicePipeline:
    def __init__(self):
        self.reply_playback = FakeReplyPlayback()
        self.logger = FakeLogger()
        self.stored = []
        self.downloaded = []
        self.prepared = []
        self.rendered = []
        self.render_scene_error = None

    @staticmethod
    def _require_api_auth(_token):
        return None

    @staticmethod
    def _text(value):
        return str(value or "").strip()

    @staticmethod
    def _as_bool(value, default=False):
        if isinstance(value, bool):
            return value
        token = str(value or "").strip().lower()
        if not token:
            return bool(default)
        return token in {"1", "true", "yes", "on", "enabled"}

    @staticmethod
    def _as_float(value, default=0.0):
        try:
            return float(value)
        except Exception:
            return float(default)

    @staticmethod
    def _satellite_lookup(_selector):
        return {}

    @staticmethod
    def _esphome_client_row_snapshot_sync(_selector):
        return {}

    async def _download_media_source(self, source_url):
        self.background_source_url = source_url
        self.downloaded.append(source_url)
        return b"background", "audio/mpeg"

    async def _prepare_native_media_asset(
        self,
        media_bytes,
        *,
        media_type,
        filename,
        playback_kind="",
    ):
        data = bytes(media_bytes or b"")
        mime = str(media_type or "application/octet-stream")
        name = str(filename or "satellite-audio.bin")
        is_mp3 = mime in {"audio/mpeg", "audio/mp3"} or name.endswith(".mp3")
        prepared = {
            "bytes": data if is_mp3 else b"mp3:" + data,
            "media_type": "audio/mpeg",
            "filename": name if name.endswith(".mp3") else f"{Path(name).stem}.mp3",
            "transcoded": not is_mp3,
            "playback_kind": playback_kind,
        }
        self.prepared.append(dict(prepared))
        return prepared

    async def _render_native_audio_scene_asset(
        self,
        foreground_bytes,
        **kwargs,
    ):
        if self.render_scene_error is not None:
            raise self.render_scene_error
        render = {
            "foreground_bytes": bytes(foreground_bytes or b""),
            **kwargs,
        }
        self.rendered.append(render)
        foreground_duration_s = 1.0
        duration_s = (
            foreground_duration_s
            + (int(kwargs.get("start_delay_ms", 0)) / 1000.0)
            + (int(kwargs.get("ducking_release_ms", 0)) / 1000.0)
            + (int(kwargs.get("fade_ms", 0)) / 1000.0)
        )
        return {
            "bytes": b"rendered-scene",
            "media_type": "audio/mpeg",
            "filename": "announcement-scene.mp3",
            "rendered_audio_scene": True,
            "foreground_duration_s": foreground_duration_s,
            "duration_s": duration_s,
            "start_delay_ms": int(kwargs.get("start_delay_ms", 0)),
        }

    @staticmethod
    def _native_persistent_media_source_url(
        source_url,
        *,
        media_content_type,
        start_position_ms=0,
    ):
        url = str(source_url or "").strip()
        if media_content_type != "music" or start_position_ms or "localhost" in url:
            return ""
        return url if url.startswith(("http://", "https://")) else ""

    def _store_media_url(self, selector, session_id, media_bytes, *, media_type, filename):
        self.stored.append(
            {
                "selector": selector,
                "session_id": session_id,
                "bytes": media_bytes,
                "media_type": media_type,
                "filename": filename,
            }
        )
        if filename.startswith("announcement-scene"):
            return "http://voice-core/media/scene"
        if filename.startswith("background-audio"):
            return "http://voice-core/media/background"
        return "http://voice-core/media/foreground"


def _load_route_functions(fake_vp):
    path = (
        Path(__file__).resolve().parents[1]
        / "tater_voice"
        / "voice_pipeline"
        / "routes.py"
    )
    wanted = {
        "_native_audio_scene_payload",
        "_native_ducking_payload",
        "_render_native_audio_scene",
        "native_satellite_play_group",
        "native_satellite_play",
    }
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    selected = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted:
            node.decorator_list = []
            selected.append(node)
    if len(selected) != len(wanted):
        found = {node.name for node in selected}
        raise RuntimeError(f"Missing route functions: {sorted(wanted - found)}")

    module = types.ModuleType("tater_voice.voice_pipeline.scene_route_test")
    module.__package__ = "tater_voice.voice_pipeline"
    module.__dict__.update(
        {
            "Any": Any,
            "Dict": Dict,
            "Optional": Optional,
            "HTTPException": FakeHttpException,
            "Header": lambda default=None: default,
            "base64": base64,
            "contextlib": contextlib,
            "uuid": __import__("uuid"),
            "_vp": lambda: fake_vp,
        }
    )
    compiled = ast.Module(body=selected, type_ignores=[])
    ast.fix_missing_locations(compiled)
    exec(compile(compiled, str(path), "exec"), module.__dict__)
    return module


def _load_capability_helpers():
    path = Path(__file__).resolve().parents[1] / "tater_voice" / "native_satellite.py"
    wanted = {"_text", "_lower", "_as_bool", "_capabilities"}
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    selected = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    module = types.ModuleType("native_capability_test")
    module.__dict__.update({"Any": Any, "Dict": Dict})
    compiled = ast.Module(body=selected, type_ignores=[])
    ast.fix_missing_locations(compiled)
    exec(compile(compiled, str(path), "exec"), module.__dict__)
    return module


class NativeCapabilityTests(unittest.TestCase):
    def test_preserves_audio_scene_version_and_normalizes_boolean_strings(self) -> None:
        native = _load_capability_helpers()
        capabilities = native._capabilities(
            {
                "capabilities": {
                    "audio_scenes": "true",
                    "audio_scene_version": 1,
                    "audio_session_version": 1,
                    "legacy_feature": "false",
                }
            }
        )
        self.assertIs(capabilities["audio_scenes"], True)
        self.assertEqual(capabilities["audio_scene_version"], 1)
        self.assertEqual(capabilities["audio_session_version"], 1)
        self.assertIs(capabilities["legacy_feature"], False)


class NativeAudioSceneRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.vp = FakeVoicePipeline()
        self.commands = []
        self.stereo_calls = []
        self.single_overlay_calls = []
        self.single_overlay_error = None
        self.group_calls = []
        self.stereo_pair = {}
        self.scene_supported = True
        self.media_session_active = False
        self.unavailable_group_members = {}
        self.capabilities = {
            "audio_scenes": True,
            "persistent_media_sessions": True,
            "tts_overlays": True,
            "synchronized_media_sessions": True,
            "synchronized_tts_overlays": True,
            "media_playhead_telemetry": True,
            "media_drift_correction": True,
        }

        native = types.ModuleType("tater_voice.native_satellite")

        async def client_has_capability(_selector, capability):
            if capability == "audio_scenes":
                return self.scene_supported
            return bool(self.capabilities.get(capability))

        async def client_media_session_active(_selector):
            return self.media_session_active

        async def send_command(selector, message_type, payload):
            self.commands.append((selector, message_type, payload))
            return {"queued": True}

        async def prepare_stereo_media_session(pair, **kwargs):
            self.stereo_calls.append(("media", pair, kwargs))
            return {"stereo_session_started": True, "start_server_us": 123456789}

        async def prepare_group_media_session(members, **kwargs):
            self.group_calls.append((members, kwargs))
            return {
                "group_session_started": True,
                "group_id": kwargs["group_id"],
                "session_id": kwargs["session_id"],
                "members": members,
                "start_server_us": 123456789,
            }

        async def media_group_member_status(selectors):
            ready = [
                selector
                for selector in selectors
                if selector not in self.unavailable_group_members
            ]
            unavailable = [
                {
                    "selector": selector,
                    "reason": self.unavailable_group_members[selector],
                }
                for selector in selectors
                if selector in self.unavailable_group_members
            ]
            return {
                "ok": bool(ready),
                "selectors": list(selectors),
                "ready_selectors": ready,
                "unavailable": unavailable,
            }

        async def start_stereo_overlay(pair, **kwargs):
            self.stereo_calls.append(("overlay", pair, kwargs))
            return {"stereo_overlay_started": True}

        async def start_single_overlay(selector, **kwargs):
            self.single_overlay_calls.append((selector, kwargs))
            if self.single_overlay_error is not None:
                raise self.single_overlay_error
            return {"single_overlay_started": True}

        native.client_has_capability = client_has_capability
        native.client_media_session_active = client_media_session_active
        native.send_command = send_command
        native.prepare_stereo_media_session = prepare_stereo_media_session
        native.prepare_group_media_session = prepare_group_media_session
        native.media_group_member_status = media_group_member_status
        native.start_stereo_overlay = start_stereo_overlay
        native.start_single_overlay = start_single_overlay
        native.stereo_pair_media_active = lambda _pair: self.media_session_active

        stereo_pairs = types.ModuleType("tater_voice.stereo_pairs")
        stereo_pairs.is_stereo_selector = lambda selector: str(selector or "").startswith("stereo:")
        stereo_pairs.get_pair = lambda _selector: dict(self.stereo_pair)

        self.previous_modules = {
            name: sys.modules.get(name)
            for name in (
                "tater_voice",
                "tater_voice.voice_pipeline",
                "tater_voice.native_satellite",
                "tater_voice.stereo_pairs",
            )
        }
        package = types.ModuleType("tater_voice")
        package.__path__ = []
        voice_pipeline_package = types.ModuleType("tater_voice.voice_pipeline")
        voice_pipeline_package.__path__ = []
        package.native_satellite = native
        package.stereo_pairs = stereo_pairs
        sys.modules["tater_voice"] = package
        sys.modules["tater_voice.voice_pipeline"] = voice_pipeline_package
        sys.modules["tater_voice.native_satellite"] = native
        sys.modules["tater_voice.stereo_pairs"] = stereo_pairs
        self.routes = _load_route_functions(self.vp)

    def tearDown(self) -> None:
        for name, previous in self.previous_modules.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous

    @staticmethod
    def _payload():
        return {
            "selector": "native:kitchen",
            "audio_b64": base64.b64encode(b"foreground").decode("ascii"),
            "media_type": "audio/wav",
            "filename": "tts.wav",
            "respect_reply_playback": False,
            "audio_scene": {
                "background": {
                    "url": "https://example.test/morning.mp3",
                    "loop": True,
                    "volume_percent": 60,
                },
                "ducking": {
                    "target_percent": 35,
                    "attack_ms": 150,
                    "release_ms": 350,
                },
                "finish": {"fade_ms": 500},
            },
        }

    def test_supported_satellite_audio_scene_uses_one_rendered_session(self) -> None:
        result = asyncio.run(self.routes.native_satellite_play(self._payload(), None))

        self.assertTrue(result["audio_scene_started"])
        self.assertTrue(result["media_session_started"])
        self.assertFalse(result["audio_overlay_started"])
        self.assertTrue(result["rendered_audio_scene_started"])
        self.assertEqual(self.commands, [])
        members, scene = self.group_calls[0]
        self.assertEqual(members[0]["selector"], "native:kitchen")
        self.assertEqual(members[0]["channel"], "mono")
        self.assertEqual(members[0]["volume_percent"], 100)
        self.assertEqual(scene["media_url"], "http://voice-core/media/scene")
        self.assertFalse(scene["loop"])
        self.assertEqual(scene["content_type"], "announcement")
        self.assertEqual(self.single_overlay_calls, [])
        self.assertEqual(self.vp.background_source_url, "https://example.test/morning.mp3")
        self.assertEqual([row["playback_kind"] for row in self.vp.prepared], ["tts"])
        self.assertTrue(self.vp.prepared[0]["transcoded"])
        self.assertTrue(all(row["media_type"] == "audio/mpeg" for row in self.vp.stored))
        self.assertEqual(len(self.vp.rendered), 1)
        render = self.vp.rendered[0]
        self.assertEqual(render["background_volume_percent"], 60)
        self.assertEqual(render["ducking_target_percent"], 35)
        self.assertEqual(render["start_delay_ms"], 0)
        self.assertEqual(render["fade_ms"], 500)
        self.assertTrue(render["background_loop"])
        self.assertEqual(render["background_bytes"], b"background")

    def test_older_scene_satellite_keeps_compatibility_mixer(self) -> None:
        self.capabilities["synchronized_media_sessions"] = False

        result = asyncio.run(self.routes.native_satellite_play(self._payload(), None))

        self.assertTrue(result["audio_scene_started"])
        self.assertFalse(result["media_session_started"])
        self.assertFalse(result["audio_overlay_started"])
        self.assertEqual(self.commands[0][1], "audio.scene.start")
        self.assertEqual(self.group_calls, [])
        self.assertEqual(self.single_overlay_calls, [])

    def test_audio_scene_lead_in_is_clamped_and_preserved_for_legacy_scene(self) -> None:
        self.capabilities["synchronized_media_sessions"] = False
        payload = self._payload()
        payload["audio_scene"]["foreground"] = {"start_delay_ms": 45000}

        asyncio.run(self.routes.native_satellite_play(payload, None))

        self.assertEqual(
            self.commands[0][2]["foreground"]["start_delay_ms"],
            30000,
        )

    def test_buffered_scene_failure_does_not_race_the_legacy_mixer(self) -> None:
        self.vp.render_scene_error = RuntimeError("render rejected")

        with self.assertRaises(FakeHttpException) as raised:
            asyncio.run(self.routes.native_satellite_play(self._payload(), None))

        self.assertEqual(raised.exception.status_code, 409)
        self.assertIn("Rendered audio scene failed", raised.exception.detail)
        self.assertEqual(self.single_overlay_calls, [])
        self.assertEqual(
            [message_type for _selector, message_type, _payload in self.commands],
            ["media.session.stop"],
        )

    def test_multi_satellite_music_route_flattens_one_synchronized_group(self) -> None:
        payload = {
            "selectors": ["native:kitchen", "native:office"],
            "audio_b64": base64.b64encode(b"music").decode("ascii"),
            "media_type": "audio/mpeg",
            "media_content_type": "music",
            "filename": "song.mp3",
            "volume_percent": 65,
            "start_lead_ms": 1125,
        }

        result = asyncio.run(self.routes.native_satellite_play_group(payload, None))

        self.assertTrue(result["media_session_started"])
        self.assertEqual(result["playback_mode"], "synchronized_group")
        members, kwargs = self.group_calls[0]
        self.assertEqual(
            [row["selector"] for row in members],
            ["native:kitchen", "native:office"],
        )
        self.assertTrue(all(row["channel"] == "mono" for row in members))
        self.assertTrue(all(row["volume_percent"] == 65 for row in members))
        self.assertEqual(kwargs["start_lead_ms"], 1125)
        self.assertTrue(kwargs["compatibility_checked"])

    def test_multi_destination_audio_scene_renders_once_for_one_group_start(self) -> None:
        self.stereo_pair = {
            "id": "bedroom12",
            "selector": "stereo:bedroom12",
            "left_selector": "native:left",
            "right_selector": "native:right",
        }
        payload = {
            "selectors": ["native:kitchen", "stereo:bedroom12"],
            "audio_b64": base64.b64encode(b"group speech").decode("ascii"),
            "media_type": "audio/wav",
            "filename": "tts.wav",
            "wait_for_completion": True,
            "timeout_s": 40,
            "audio_scene": {
                "background": {
                    "url": "https://example.test/morning.mp3",
                    "loop": True,
                    "volume_percent": 60,
                },
                "foreground": {"start_delay_ms": 1500},
                "ducking": {
                    "target_percent": 35,
                    "attack_ms": 150,
                    "release_ms": 350,
                },
                "finish": {"fade_ms": 500},
            },
        }

        result = asyncio.run(self.routes.native_satellite_play_group(payload, None))

        self.assertTrue(result["audio_scene_started"])
        self.assertTrue(result["rendered_audio_scene_started"])
        self.assertFalse(result["audio_overlay_started"])
        self.assertEqual(result["played_selectors"], ["native:kitchen", "stereo:bedroom12"])
        self.assertEqual(len(self.vp.rendered), 1)
        members, scene = self.group_calls[0]
        self.assertEqual(
            [(member["selector"], member["channel"]) for member in members],
            [
                ("native:kitchen", "mono"),
                ("native:left", "left"),
                ("native:right", "right"),
            ],
        )
        self.assertFalse(members[0]["stereo_member"])
        self.assertTrue(members[1]["stereo_member"])
        self.assertTrue(members[2]["stereo_member"])
        self.assertEqual(scene["media_url"], "http://voice-core/media/scene")
        self.assertTrue(scene["session_id"].endswith("-scene"))
        self.assertEqual(scene["content_type"], "announcement")
        self.assertEqual(scene["channel_mode"], "mixed")
        self.assertFalse(scene["loop"])
        self.assertTrue(scene["wait_for_completion"])
        self.assertAlmostEqual(scene["completion_timeout_s"], 42.35, places=2)
        self.assertEqual(self.vp.prepared, [])
        self.assertEqual(len(self.vp.stored), 1)
        self.assertEqual(self.vp.stored[0]["filename"], "announcement-scene.mp3")

    def test_multi_satellite_music_keeps_remote_source_as_live_stream(self) -> None:
        source_url = "http://tube.test/api/tater/local/stream?transcode=1&profile=audio_sync"
        payload = {
            "selectors": ["native:kitchen", "native:office"],
            "source_url": source_url,
            "media_type": "audio/wav",
            "media_content_type": "music",
            "filename": "song.sync.wav",
        }

        result = asyncio.run(self.routes.native_satellite_play_group(payload, None))

        self.assertTrue(result["source_passthrough"])
        self.assertEqual(result["playback_url"], source_url)
        self.assertEqual(self.group_calls[0][1]["media_url"], source_url)
        self.assertEqual(self.vp.downloaded, [])
        self.assertEqual(self.vp.stored, [])

    def test_resumed_group_music_is_preloaded_for_seekable_playback(self) -> None:
        source_url = "http://tube.test/api/tater/local/stream?transcode=1&profile=audio_sync"
        payload = {
            "selectors": ["native:kitchen", "native:office"],
            "source_url": source_url,
            "media_type": "audio/wav",
            "media_content_type": "music",
            "filename": "song.sync.wav",
            "start_position_ms": 30000,
        }

        result = asyncio.run(self.routes.native_satellite_play_group(payload, None))

        self.assertFalse(result["source_passthrough"])
        self.assertEqual(self.vp.downloaded, [source_url])
        self.assertEqual(len(self.vp.stored), 1)

    def test_multi_satellite_music_route_applies_per_destination_calibration(self) -> None:
        payload = {
            "selectors": ["native:kitchen", "native:office"],
            "audio_b64": base64.b64encode(b"music").decode("ascii"),
            "media_type": "audio/mpeg",
            "media_content_type": "music",
            "filename": "song.mp3",
            "volume_percent": 65,
            "player_settings": {
                "native:kitchen": {"volume_percent": 41, "sync_offset_ms": -200},
                "native:office": {"volume_percent": 72, "sync_offset_ms": 100},
            },
        }

        asyncio.run(self.routes.native_satellite_play_group(payload, None))

        members, _kwargs = self.group_calls[0]
        by_selector = {row["selector"]: row for row in members}
        self.assertEqual(by_selector["native:kitchen"]["volume_percent"], 41)
        self.assertEqual(by_selector["native:kitchen"]["delay_ms"], 0)
        self.assertEqual(by_selector["native:office"]["volume_percent"], 72)
        self.assertEqual(by_selector["native:office"]["delay_ms"], 300)

    def test_multi_satellite_music_route_skips_an_offline_member(self) -> None:
        self.unavailable_group_members = {"native:office": "offline"}
        payload = {
            "selectors": ["native:kitchen", "native:office"],
            "audio_b64": base64.b64encode(b"music").decode("ascii"),
            "media_type": "audio/mpeg",
            "media_content_type": "music",
            "filename": "song.mp3",
        }

        result = asyncio.run(self.routes.native_satellite_play_group(payload, None))

        members, _kwargs = self.group_calls[0]
        self.assertEqual([row["selector"] for row in members], ["native:kitchen"])
        self.assertEqual(result["played_selectors"], ["native:kitchen"])
        self.assertEqual(result["skipped_destinations"][0]["selector"], "native:office")
        self.assertIn("offline", result["warnings"][0])

    def test_multi_satellite_music_route_skips_an_incomplete_stereo_pair(self) -> None:
        self.stereo_pair = {
            "id": "bedroom12",
            "selector": "stereo:bedroom12",
            "left_selector": "native:left",
            "right_selector": "native:right",
        }
        self.unavailable_group_members = {"native:right": "offline"}
        payload = {
            "selectors": ["stereo:bedroom12", "native:kitchen"],
            "audio_b64": base64.b64encode(b"music").decode("ascii"),
            "media_type": "audio/mpeg",
            "media_content_type": "music",
            "filename": "song.mp3",
        }

        result = asyncio.run(self.routes.native_satellite_play_group(payload, None))

        members, _kwargs = self.group_calls[0]
        self.assertEqual([row["selector"] for row in members], ["native:kitchen"])
        self.assertEqual(result["played_selectors"], ["native:kitchen"])
        self.assertEqual(result["skipped_destinations"][0]["selector"], "stereo:bedroom12")

    def test_older_satellite_falls_back_to_play_url(self) -> None:
        self.scene_supported = False
        self.capabilities["synchronized_media_sessions"] = False
        result = asyncio.run(self.routes.native_satellite_play(self._payload(), None))

        self.assertFalse(result["audio_scene_started"])
        self.assertIn("does not advertise", result["audio_scene_fallback_reason"])
        self.assertEqual(self.commands[0][1], "play.url")
        self.assertEqual(len(self.vp.stored), 1)

    def test_music_uses_persistent_media_session(self) -> None:
        payload = {
            "selector": "native:kitchen",
            "audio_b64": base64.b64encode(b"music").decode("ascii"),
            "media_type": "audio/mpeg",
            "media_content_type": "music",
            "playback_role": "media",
            "filename": "song.mp3",
            "start_position_ms": 37500,
            "respect_reply_playback": False,
        }
        result = asyncio.run(self.routes.native_satellite_play(payload, None))

        self.assertTrue(result["media_session_started"])
        self.assertEqual(self.commands[0][1], "media.session.start")
        self.assertEqual(
            self.commands[0][2]["media"]["url"],
            "http://voice-core/media/foreground",
        )
        self.assertEqual(self.commands[0][2]["media"]["start_position_ms"], 37500)

    def test_single_satellite_music_keeps_remote_source_as_live_stream(self) -> None:
        source_url = "http://tube.test/api/tater/local/stream?transcode=1&profile=audio_sync"
        payload = {
            "selector": "native:kitchen",
            "source_url": source_url,
            "media_type": "audio/wav",
            "media_content_type": "music",
            "playback_role": "media",
            "filename": "song.sync.wav",
            "respect_reply_playback": False,
        }

        result = asyncio.run(self.routes.native_satellite_play(payload, None))

        self.assertTrue(result["source_passthrough"])
        self.assertEqual(result["playback_url"], source_url)
        self.assertEqual(self.commands[0][2]["media"]["url"], source_url)
        self.assertEqual(self.vp.downloaded, [])
        self.assertEqual(self.vp.stored, [])

    def test_tts_uses_overlay_when_media_session_is_active(self) -> None:
        self.media_session_active = True
        payload = {
            "selector": "native:kitchen",
            "audio_b64": base64.b64encode(b"speech").decode("ascii"),
            "media_type": "audio/wav",
            "filename": "tts.wav",
            "respect_reply_playback": False,
            "ducking": {
                "target_percent": 28,
                "attack_ms": 90,
                "release_ms": 420,
            },
        }
        result = asyncio.run(self.routes.native_satellite_play(payload, None))

        self.assertTrue(result["audio_overlay_started"])
        self.assertEqual(self.commands[0][1], "audio.overlay.start")
        self.assertEqual(self.commands[0][2]["ducking"]["target_percent"], 28)

    def test_active_stereo_reply_propagates_exact_completion_wait(self) -> None:
        self.stereo_pair = {
            "id": "bedroom12",
            "selector": "stereo:bedroom12",
            "left_selector": "native:left",
            "right_selector": "native:right",
        }
        self.media_session_active = True
        payload = {
            "selector": "stereo:bedroom12",
            "audio_b64": base64.b64encode(b"stereo speech").decode("ascii"),
            "media_type": "audio/wav",
            "filename": "tts.wav",
            "respect_reply_playback": False,
            "wait_for_completion": True,
            "timeout_s": 42,
        }

        result = asyncio.run(self.routes.native_satellite_play(payload, None))

        self.assertTrue(result["audio_overlay_started"])
        overlay = self.stereo_calls[0][2]
        self.assertTrue(overlay["wait_for_completion"])
        self.assertEqual(overlay["completion_timeout_s"], 42)

    def test_stereo_pair_music_uses_synchronized_session(self) -> None:
        self.stereo_pair = {
            "id": "bedroom12",
            "selector": "stereo:bedroom12",
            "left_selector": "native:left",
            "right_selector": "native:right",
        }
        payload = {
            "selector": "stereo:bedroom12",
            "audio_b64": base64.b64encode(b"stereo music").decode("ascii"),
            "media_type": "audio/mpeg",
            "media_content_type": "music",
            "playback_role": "media",
            "filename": "song.mp3",
            "start_position_ms": 42000,
            "respect_reply_playback": False,
        }

        result = asyncio.run(self.routes.native_satellite_play(payload, None))

        self.assertTrue(result["media_session_started"])
        self.assertEqual(self.stereo_calls[0][0], "media")
        self.assertEqual(self.stereo_calls[0][2]["channel_mode"], "stereo")
        self.assertEqual(
            self.stereo_calls[0][2]["media_url"],
            "http://voice-core/media/foreground",
        )
        self.assertEqual(self.stereo_calls[0][2]["start_position_ms"], 42000)

    def test_stereo_pair_tts_can_wait_for_actual_pair_completion(self) -> None:
        self.stereo_pair = {
            "id": "bedroom12",
            "selector": "stereo:bedroom12",
            "left_selector": "native:left",
            "right_selector": "native:right",
        }
        payload = {
            "selector": "stereo:bedroom12",
            "audio_b64": base64.b64encode(b"stereo speech").decode("ascii"),
            "media_type": "audio/wav",
            "filename": "tts.wav",
            "respect_reply_playback": False,
            "wait_for_completion": True,
            "timeout_s": 42,
        }

        result = asyncio.run(self.routes.native_satellite_play(payload, None))

        self.assertTrue(result["media_session_started"])
        media = self.stereo_calls[0][2]
        self.assertEqual(media["content_type"], "tts")
        self.assertEqual(media["channel_mode"], "mono")
        self.assertTrue(media["wait_for_completion"])
        self.assertEqual(media["completion_timeout_s"], 42)
        self.assertEqual(len(self.vp.prepared), 1)
        self.assertEqual(self.vp.prepared[0]["playback_kind"], "tts")
        self.assertTrue(self.vp.prepared[0]["transcoded"])
        self.assertEqual(self.vp.stored[0]["media_type"], "audio/mpeg")
        self.assertEqual(self.vp.stored[0]["filename"], "tts.mp3")

    def test_stereo_pair_audio_scene_synchronizes_background_and_tts(self) -> None:
        self.stereo_pair = {
            "id": "bedroom12",
            "selector": "stereo:bedroom12",
            "left_selector": "native:left",
            "right_selector": "native:right",
        }
        payload = self._payload()
        payload["selector"] = "stereo:bedroom12"
        payload["audio_scene"]["foreground"] = {"start_delay_ms": 2500}

        result = asyncio.run(self.routes.native_satellite_play(payload, None))

        self.assertTrue(result["audio_scene_started"])
        self.assertTrue(result["media_session_started"])
        self.assertFalse(result["audio_overlay_started"])
        self.assertTrue(result["rendered_audio_scene_started"])
        self.assertEqual([row[0] for row in self.stereo_calls], ["media"])
        scene = self.stereo_calls[0][2]
        self.assertEqual(scene["media_url"], "http://voice-core/media/scene")
        self.assertFalse(scene["loop"])
        self.assertEqual(scene["volume_percent"], 100)
        self.assertEqual(scene["content_type"], "announcement")
        self.assertEqual(scene["channel_mode"], "stereo")
        self.assertEqual(scene["completion_timeout_s"], 183.35)
        self.assertEqual(self.single_overlay_calls, [])
        self.assertEqual(self.vp.rendered[0]["start_delay_ms"], 2500)
        self.assertEqual(self.vp.rendered[0]["ducking_target_percent"], 35)


if __name__ == "__main__":
    unittest.main()
