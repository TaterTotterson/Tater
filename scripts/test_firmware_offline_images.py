#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import firmware, home, ui_helpers  # noqa: E402


class FirmwareOfflineImageTests(unittest.TestCase):
    BOARD_CASES = {
        "biscuit": ("biscuit", "echo-dot-2.png"),
        "checkers": ("checkers", "echo-show-5.png"),
        "radar": ("radar", "echo-2-radar.png"),
        "rook": ("rook", "echo-spot-rook.png"),
        "thirdreality-s420": ("thirdreality_s420", "thirdreality-s420.png"),
        "voice-pe": ("voicepe", "voicepe.png"),
        "satellite1": ("satellite1", "sat1.png"),
        "respeaker-lite": ("respeaker_lite", "respeaker-lite.png"),
        "respeaker-xvf3800": ("respeaker_xvf3800", "respeaker-xvf3800.png"),
        "s3box": ("s3box_display", "taterD.png"),
        "koala": ("koala", "koala-satellite.png"),
    }

    @staticmethod
    def _saved_row(board: str) -> dict[str, object]:
        return {
            "selector": "native:test-sat",
            "host": "",
            "name": "Kitchen Satellite",
            "source": "tater_native",
            "metadata": {
                "native_selected": True,
                "board": board,
                "firmware_version": "native-test-1.0.0",
                "area_name": "Kitchen",
            },
            "last_seen_ts": 123.0,
        }

    def _offline_status(self, board: str, native_row: dict[str, object] | None = None) -> dict[str, object]:
        native_status = {"clients": {"native:test-sat": native_row}} if native_row is not None else {"clients": {}}
        with (
            mock.patch.object(home.esphome_runtime, "status", return_value={"clients": {}, "voice_metrics": {}}),
            mock.patch.object(home.esphome_runtime, "load_satellite_registry", return_value=[self._saved_row(board)]),
        ):
            return home._runtime_status_with_native(native_status)

    def test_saved_board_selects_the_same_image_while_offline(self) -> None:
        for board, (expected_template, expected_image) in self.BOARD_CASES.items():
            with self.subTest(board=board):
                status = self._offline_status(board)
                client = status["clients"]["native:test-sat"]
                matched = firmware._match_template_spec("native:test-sat", client)
                device_option = firmware._firmware_device_option("native:test-sat", client)

                self.assertIsNotNone(matched)
                self.assertEqual(matched["key"], expected_template)
                self.assertIsNotNone(device_option)
                self.assertEqual(device_option["template_key"], expected_template)
                self.assertEqual(
                    ui_helpers.device_image_src(matched["key"], matched["label"]),
                    ui_helpers._named_satellite_image_src(expected_image),
                )
                self.assertEqual(
                    device_option["hero_image_src"],
                    ui_helpers._named_satellite_image_src(expected_image),
                )
                self.assertTrue((REPO_ROOT / "images" / expected_image).is_file())

    def test_saved_board_replaces_generic_disconnected_live_snapshot(self) -> None:
        status = self._offline_status(
            "satellite1",
            {
                "connected": False,
                "device_id": "test-sat",
                "device_name": "Kitchen Satellite",
                "board": "",
                "firmware_version": "",
            },
        )
        client = status["clients"]["native:test-sat"]

        self.assertFalse(client["connected"])
        self.assertTrue(client["selected"])
        self.assertEqual(client["metadata"]["board"], "satellite1")
        self.assertEqual(client["device_info"]["model"], "satellite1")
        self.assertEqual(
            firmware._match_template_spec("native:test-sat", client)["key"],
            "satellite1",
        )

    def test_satellite_cards_keep_images_for_each_device_in_a_mixed_fleet(self) -> None:
        saved_rows = [
            {
                "selector": "native:voicepe-test",
                "host": "",
                "name": "VoicePE Test",
                "source": "tater_native",
                "metadata": {
                    "native_selected": True,
                    "board": "voice-pe",
                    "firmware_target": "",
                    "firmware_version": "native-voicepe-0.4.1",
                },
                "last_seen_ts": 123.0,
            },
            {
                "selector": "native:echo-test",
                "host": "",
                "name": "Echo Test",
                "source": "tater_native",
                "metadata": {
                    "native_selected": True,
                    "board": "biscuit",
                    "firmware_target": "biscuit",
                    "firmware_version": "v0.1.1",
                },
                "last_seen_ts": 124.0,
            },
            {
                "selector": "native:radar-test",
                "host": "",
                "name": "Echo 2 Test",
                "source": "tater_native",
                "metadata": {
                    "native_selected": True,
                    "board": "radar",
                    "firmware_target": "radar",
                    "firmware_version": "v0.1.1",
                },
                "last_seen_ts": 125.0,
            },
            {
                "selector": "native:show-test",
                "host": "",
                "name": "Echo Show Test",
                "source": "tater_native",
                "metadata": {
                    "native_selected": True,
                    "board": "checkers",
                    "firmware_target": "checkers",
                    "firmware_version": "v0.1.5",
                },
                "last_seen_ts": 125.0,
            },
        ]
        native_status = {
            "clients": {
                "native:voicepe-test": {
                    "connected": True,
                    "device_id": "voicepe-test",
                    "device_name": "VoicePE Test",
                    "board": "voice-pe",
                    "firmware_target": "",
                    "firmware_version": "native-voicepe-0.4.1",
                },
                "native:echo-test": {
                    "connected": True,
                    "device_id": "echo-test",
                    "device_name": "Echo Test",
                    "board": "biscuit",
                    "firmware_target": "biscuit",
                    "firmware_version": "v0.1.1",
                },
                "native:show-test": {
                    "connected": True,
                    "device_id": "show-test",
                    "device_name": "Echo Show Test",
                    "board": "checkers",
                    "firmware_target": "checkers",
                    "firmware_version": "v0.1.5",
                },
                "native:radar-test": {
                    "connected": True,
                    "device_id": "radar-test",
                    "device_name": "Echo 2 Test",
                    "board": "radar",
                    "firmware_target": "radar",
                    "firmware_version": "v0.1.1",
                },
            }
        }

        with (
            mock.patch.object(home.esphome_runtime, "status", return_value={"clients": {}, "voice_metrics": {}}),
            mock.patch.object(home.esphome_runtime, "load_satellite_registry", return_value=saved_rows),
        ):
            status = home._runtime_status_with_native(native_status)
            cards = {row["id"]: row for row in ui_helpers.satellite_item_forms(status)}

        self.assertEqual(
            cards["native:voicepe-test"]["hero_image_src"],
            ui_helpers._named_satellite_image_src("voicepe.png"),
        )
        self.assertEqual(
            cards["native:echo-test"]["hero_image_src"],
            ui_helpers._named_satellite_image_src("echo-dot-2.png"),
        )
        self.assertEqual(
            cards["native:show-test"]["hero_image_src"],
            ui_helpers._named_satellite_image_src("echo-show-5.png"),
        )
        self.assertEqual(
            cards["native:radar-test"]["hero_image_src"],
            ui_helpers._named_satellite_image_src("echo-2-radar.png"),
        )


if __name__ == "__main__":
    unittest.main()
