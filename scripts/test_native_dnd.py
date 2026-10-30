from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tater_voice import native_live_settings  # noqa: E402


class NativeDNDTests(unittest.TestCase):
    def test_normalizes_dnd_defaults_and_schedule(self) -> None:
        settings = native_live_settings.normalize_settings(
            {
                "dnd_enabled": "true",
                "dnd_schedule_enabled": "yes",
                "dnd_start": "9:05",
                "dnd_end": "27:00",
            }
        )
        self.assertTrue(settings["dnd_enabled"])
        self.assertTrue(settings["dnd_schedule_enabled"])
        self.assertEqual(settings["dnd_start"], "09:05")
        self.assertEqual(settings["dnd_end"], "07:00")

    def test_dnd_active_supports_overnight_window(self) -> None:
        current = native_live_settings.normalize_settings(
            {
                "dnd_schedule_enabled": True,
                "dnd_start": "22:00",
                "dnd_end": "07:00",
            }
        )
        with mock.patch.object(native_live_settings, "settings_snapshot", return_value=current):
            self.assertTrue(native_live_settings.dnd_active("native:bedroom", now=datetime(2026, 10, 29, 23, 0)))
            self.assertTrue(native_live_settings.dnd_active("native:bedroom", now=datetime(2026, 10, 30, 6, 59)))
            self.assertFalse(native_live_settings.dnd_active("native:bedroom", now=datetime(2026, 10, 30, 7, 0)))

    def test_device_popup_contains_dnd_fields(self) -> None:
        current = native_live_settings.normalize_settings({})
        with (
            mock.patch.object(native_live_settings, "settings_snapshot", return_value=current),
            mock.patch.object(native_live_settings, "_global_settings_with_migration", return_value={}),
        ):
            fields = native_live_settings.settings_fields("native:bedroom", board="rook", capabilities={})
        rows = {str(field.get("key")): field for field in fields}
        self.assertIn("dnd_section", rows)
        self.assertIn("dnd_enabled", rows)
        self.assertIn("dnd_schedule_enabled", rows)
        self.assertEqual(rows["dnd_start"]["type"], "time")
        self.assertEqual(rows["dnd_end"]["show_when"]["source_key"], "dnd_schedule_enabled")

if __name__ == "__main__":
    unittest.main()
