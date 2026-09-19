from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tater_voice import airplay_input
import announcement_targets


class _FakeRedis:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value
        return True


class AirPlayInputTests(unittest.TestCase):
    def setUp(self) -> None:
        self.redis = _FakeRedis()
        self.redis_patch = mock.patch.object(airplay_input, "redis_client", self.redis)
        self.redis_patch.start()

    def tearDown(self) -> None:
        self.redis_patch.stop()

    @staticmethod
    def _options(**_kwargs):
        return [
            {
                "value": "voice_core:native:kitchen",
                "label": "Tater Satellite: Kitchen",
            },
            {
                "value": "voice_core:stereo:office",
                "label": "Tater Stereo: Office",
            },
            {
                "value": "sonos:den",
                "label": "Sonos: Den",
                "airplay_bridge_target": "airplay:sonos-den",
            },
            {"value": "sonos:old", "label": "Sonos: Old"},
            {"value": "airplay:patio", "label": "AirPlay Bridge: Patio"},
            {"value": "ha:media_player.tv", "label": "Home Assistant: TV"},
        ]

    def test_defaults_are_platform_owned_and_do_not_read_music_core(self) -> None:
        self.redis.values["music_core_settings"] = json.dumps(
            {"airplay_receiver_enabled": True, "airplay_receiver_name": "Old Music Core"}
        )

        settings = airplay_input.load_settings()

        self.assertFalse(settings["enabled"])
        self.assertEqual(settings["receiver_name"], "Tater Audio")
        self.assertEqual(settings["targets"], [])

    def test_save_persists_and_configures_the_built_in_runtime(self) -> None:
        with (
            mock.patch.object(
                airplay_input.announcement_targets,
                "build_announcement_target_options",
                side_effect=self._options,
            ),
            mock.patch.object(
                airplay_input.external_audio,
                "configure_external_audio_runtime",
                return_value={"status": "ready"},
            ) as configure,
        ):
            result = airplay_input.save_settings(
                {
                    "enabled": True,
                    "receiver_name": "Whole Home Tater",
                    "receiver_pin": "3939",
                    "targets": [
                        "voice_core:native:kitchen",
                        "voice_core:stereo:office",
                        "sonos:den",
                    ],
                }
            )

        self.assertEqual(result["status"]["status"], "ready")
        document = json.loads(self.redis.values[airplay_input.REDIS_AIRPLAY_INPUT_SETTINGS_KEY])
        self.assertEqual(document["receiver_name"], "Whole Home Tater")
        config = configure.call_args.args[0]
        self.assertTrue(config["enabled"])
        self.assertEqual(config["volume_percent"], 100)
        self.assertEqual(config["target_transport_mode"], {"sonos:den": "airplay"})

    def test_destination_list_only_includes_supported_routes(self) -> None:
        with mock.patch.object(
            airplay_input.announcement_targets,
            "build_announcement_target_options",
            side_effect=self._options,
        ):
            rows = airplay_input.destination_options()

        values = {row["value"] for row in rows}
        self.assertEqual(
            values,
            {
                "voice_core:native:kitchen",
                "voice_core:stereo:office",
                "sonos:den",
                "airplay:patio",
            },
        )

    def test_enabled_receiver_requires_destination_and_pin_is_validated(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly four digits"):
            airplay_input.save_settings({"receiver_pin": "12ab"})

        with self.assertRaisesRegex(ValueError, "at least one playback destination"):
            airplay_input.save_settings({"enabled": True, "targets": []})

    def test_stop_uses_shared_external_audio_runtime(self) -> None:
        with mock.patch.object(
            airplay_input.external_audio,
            "stop_external_audio_input",
            return_value={"status": "ready"},
        ) as stop:
            result = airplay_input.stop_input()
        self.assertEqual(result["status"], "ready")
        stop.assert_called_once_with()

    def test_local_receiver_is_not_an_outbound_airplay_destination(self) -> None:
        with (
            mock.patch.object(
                airplay_input.external_audio,
                "get_external_audio_status",
                return_value={"receiver_name": "Whole Home Tater"},
            ),
            mock.patch(
                "airplay_bridge.discover_airplay_devices",
                return_value=[
                    {"id": "local", "name": "Whole Home Tater"},
                    {"id": "patio", "name": "Patio"},
                ],
            ),
        ):
            rows = announcement_targets.fetch_airplay_target_options()

        self.assertEqual([row["value"] for row in rows], ["airplay:patio"])


if __name__ == "__main__":
    unittest.main()
