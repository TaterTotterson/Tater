from __future__ import annotations

import pathlib
import sys
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from tater_voice import firmware


class FirmwareBatchDispatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.template = self.enterContext(mock.patch.object(firmware, "_native_template_spec_by_key", return_value={"key": "satellite1"}))
        self.client = self.enterContext(mock.patch.object(firmware, "_firmware_action_client_row", return_value={"connected": False}))
        self.enterContext(mock.patch.object(firmware, "_matched_template_key", return_value="satellite1"))
        self.context = self.enterContext(mock.patch.object(firmware, "_build_device_context", return_value={"selector": "native:sat3"}))
        self.enterContext(mock.patch.object(firmware, "_prebuilt_artifact_available", return_value=True))
        self.start = self.enterContext(mock.patch.object(firmware, "_start_flash_session", return_value={"ok": True, "session_id": "test-session", "phase": "starting"}))
        self.payload = {"selector": "native:sat3", "template_key": "satellite1", "skip_offline": True}

    def test_offline_batch_target_is_skipped_without_building_or_starting_ota(self) -> None:
        result = firmware.handle_runtime_action("voice_firmware_flash_start", self.payload)
        self.assertTrue(result["ok"])
        self.assertEqual("skipped", result["phase"])
        self.assertEqual("offline", result["reason"])
        self.assertNotIn("session_id", result)
        self.context.assert_not_called()
        self.start.assert_not_called()

    def test_single_offline_target_keeps_existing_error(self) -> None:
        self.payload.pop("skip_offline")
        with self.assertRaisesRegex(RuntimeError, "is offline"):
            firmware.handle_runtime_action("voice_firmware_flash_start", self.payload)
        self.start.assert_not_called()

    def test_online_target_starts_normally_with_batch_flag(self) -> None:
        self.client.return_value = {"connected": True}
        result = firmware.handle_runtime_action("voice_firmware_flash_start", self.payload)
        self.assertEqual("test-session", result["session_id"])
        self.context.assert_called_once()
        self.start.assert_called_once_with(self.context.return_value, follow_logs=True)


if __name__ == "__main__":
    unittest.main()
