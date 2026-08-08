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
        return b"background", "audio/mpeg"

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
        return (
            "http://voice-core/media/background"
            if filename == "background-audio"
            else "http://voice-core/media/foreground"
        )


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
        self.group_calls = []
        self.stereo_pair = {}
        self.scene_supported = True
        self.media_session_active = False
        self.unavailable_group_members = {}
        self.capabilities = {
            "audio_scenes": True,
            "persistent_media_sessions": True,
            "tts_overlays": True,
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

        native.client_has_capability = client_has_capability
        native.client_media_session_active = client_media_session_active
        native.send_command = send_command
        native.prepare_stereo_media_session = prepare_stereo_media_session
        native.prepare_group_media_session = prepare_group_media_session
        native.media_group_member_status = media_group_member_status
        native.start_stereo_overlay = start_stereo_overlay
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

    def test_supported_satellite_receives_audio_scene_command(self) -> None:
        result = asyncio.run(self.routes.native_satellite_play(self._payload(), None))

        self.assertTrue(result["audio_scene_started"])
        self.assertEqual(self.commands[0][1], "audio.scene.start")
        scene = self.commands[0][2]
        self.assertEqual(scene["foreground"]["url"], "http://voice-core/media/foreground")
        self.assertEqual(scene["background"]["url"], "http://voice-core/media/background")
        self.assertEqual(scene["ducking"]["target_percent"], 35)
        self.assertEqual(self.vp.background_source_url, "https://example.test/morning.mp3")

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

    def test_stereo_pair_audio_scene_synchronizes_background_and_tts(self) -> None:
        self.stereo_pair = {
            "id": "bedroom12",
            "selector": "stereo:bedroom12",
            "left_selector": "native:left",
            "right_selector": "native:right",
        }
        payload = self._payload()
        payload["selector"] = "stereo:bedroom12"

        result = asyncio.run(self.routes.native_satellite_play(payload, None))

        self.assertTrue(result["audio_scene_started"])
        self.assertTrue(result["media_session_started"])
        self.assertTrue(result["audio_overlay_started"])
        self.assertEqual([row[0] for row in self.stereo_calls], ["media", "overlay"])
        background = self.stereo_calls[0][2]
        overlay = self.stereo_calls[1][2]
        self.assertEqual(background["media_url"], "http://voice-core/media/background")
        self.assertTrue(background["loop"])
        self.assertEqual(background["volume_percent"], 60)
        self.assertEqual(overlay["foreground_url"], "http://voice-core/media/foreground")
        self.assertEqual(overlay["ducking"]["target_percent"], 35)
        self.assertEqual(overlay["start_server_us"], 123456789)
        self.assertTrue(overlay["stop_media_when_finished"])


if __name__ == "__main__":
    unittest.main()
