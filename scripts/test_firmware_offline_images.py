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


if __name__ == "__main__":
    unittest.main()
