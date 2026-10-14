#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import firmware  # noqa: E402


class DisplayClimateSettingsTests(unittest.TestCase):
    @staticmethod
    def _profile(screen_target: str, profile_kind: str = "native_screen") -> dict[str, object]:
        return firmware._display_sensor_profile_from_context(
            f"native:{screen_target}",
            {
                "template_key": profile_kind,
                "screen_target": screen_target,
                "display_target": f"native:{screen_target}",
                "item": {"title": screen_target.title(), "connected": True},
            },
            {"ready": False, "options": [], "message": "No weather sensors."},
            {
                "ready": True,
                "options": [{"value": "", "label": "Do not show"}],
                "message": "",
            },
            {},
        ) or {}

    def test_checkers_and_crown_offer_six_room_temperature_slots(self) -> None:
        for screen_target in ("checkers", "crown"):
            fields = self._profile(screen_target).get("fields") or []
            room_fields = [
                field
                for field in fields
                if str(field.get("key") or "").startswith("room_temp_")
            ]
            self.assertEqual(6, len(room_fields), screen_target)
            self.assertEqual(
                [f"room_temp_{index}" for index in range(1, 7)],
                [field["key"] for field in room_fields],
            )

    def test_rook_offers_three_room_temperature_slots(self) -> None:
        fields = self._profile("rook").get("fields") or []
        self.assertEqual(
            ["room_temp_1", "room_temp_2", "room_temp_3"],
            [
                field["key"]
                for field in fields
                if str(field.get("key") or "").startswith("room_temp_")
            ],
        )

    def test_s3box_does_not_receive_native_climate_slots(self) -> None:
        fields = self._profile("", profile_kind="s3box_display").get("fields") or []
        self.assertFalse(
            any(str(field.get("key") or "").startswith("room_temp_") for field in fields)
        )


if __name__ == "__main__":
    unittest.main()
