#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import types
import unittest
from unittest import mock


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

fake_native_live_settings = types.ModuleType("tater_voice.native_live_settings")
fake_native_live_settings.settings_fields = lambda *_args, **_kwargs: []
fake_reply_playback = types.ModuleType("tater_voice.reply_playback")
fake_reply_playback.resolve_reply_playback_target = lambda *_args, **_kwargs: "device"
fake_reply_playback.build_reply_playback_options = lambda *_args, **_kwargs: [
    {"value": "device", "label": "This device speaker"}
]
fake_runtime = types.ModuleType("tater_voice.runtime")
fake_runtime.text = lambda value: str(value or "").strip()
fake_runtime.lower = lambda value: str(value or "").strip().lower()
fake_runtime.load_satellite_registry = lambda: []
fake_runtime.satellite_host_from_selector = lambda value: str(value or "").removeprefix("host:")
fake_voice_pipeline = types.ModuleType("tater_voice.voice_pipeline")
fake_voice_pipeline._text = lambda value: str(value or "").strip()
fake_voice_pipeline._lower = lambda value: str(value or "").strip().lower()
fake_voice_pipeline._as_float = lambda value, default=0.0: float(value if value not in (None, "") else default)
fake_voice_pipeline._payload_selector = lambda payload: str(
    (payload or {}).get("selector") or (payload or {}).get("id") or ""
).strip()
sys.modules.setdefault("tater_voice.native_live_settings", fake_native_live_settings)
sys.modules.setdefault("tater_voice.reply_playback", fake_reply_playback)
sys.modules.setdefault("tater_voice.runtime", fake_runtime)
sys.modules.setdefault("tater_voice.voice_pipeline", fake_voice_pipeline)

from tater_voice import ui_helpers  # noqa: E402


class VoiceSatelliteVolumeUiTests(unittest.TestCase):
    def test_native_volume_is_on_the_card_instead_of_the_settings_popup(self) -> None:
        status = {
            "clients": {
                "native:kitchen": {
                    "selector": "native:kitchen",
                    "source": "tater_native",
                    "name": "Kitchen",
                    "connected": True,
                    "metadata": {"native_selected": True, "board": "voice-pe"},
                    "device_info": {"model": "voice-pe"},
                    "voice_api_audio_supported": True,
                    "voice_speaker_supported": True,
                }
            }
        }
        settings_fields = [
            {"key": "volume_percent", "label": "Volume", "type": "number", "value": 63, "min": 0, "max": 100, "step": 1},
            {"key": "logging_level", "label": "Logging Level", "type": "select", "value": "info"},
        ]

        with (
            mock.patch.object(ui_helpers.esphome_runtime, "load_satellite_registry", return_value=[]),
            mock.patch.object(ui_helpers.native_live_settings, "settings_fields", return_value=settings_fields),
        ):
            items = ui_helpers.satellite_item_forms(status)

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["volume_control"]["value"], 63)
        self.assertEqual(items[0]["volume_control"]["action"], "voice_native_satellite_settings_save")
        self.assertNotIn("volume_percent", {field["key"] for field in items[0]["popup_fields"]})
        self.assertIn("logging_level", {field["key"] for field in items[0]["popup_fields"]})

    def test_card_slider_posts_only_the_device_volume(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn("renderNativeSatelliteVolumeControl(item?.volume_control, title)", app_js)
        self.assertIn('data-native-satellite-volume-action="${escapeHtml(action)}"', app_js)
        self.assertIn("values: { volume_percent: volumePercent }", app_js)
        self.assertIn("bindNativeSatelliteVolumeControls();", app_js)


if __name__ == "__main__":
    unittest.main()
