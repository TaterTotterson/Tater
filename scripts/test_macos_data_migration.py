#!/usr/bin/env python3

import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = ROOT / "macos" / "Tater" / "Sources" / "TaterAssistant" / "main.swift"


class MacosDataMigrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = SOURCE.read_text(encoding="utf-8")

    def test_menu_and_progress_window_are_wired(self) -> None:
        self.assertIn('title: "Move Tater Data…"', self.source)
        self.assertIn("DataMigrationWindowController", self.source)
        self.assertIn('statusLabel.stringValue = "Copying Tater data"', self.source)

    def test_new_data_is_verified_before_root_switch(self) -> None:
        verify = self.source.index("copiedStats.entryCount == stats.entryCount")
        switch = self.source.index("self.backend.activateDataRoot(destination")
        trash = self.source.index("FileManager.default.trashItem(at: source")
        self.assertLess(verify, switch)
        self.assertLess(switch, trash)

    def test_external_drive_disappearance_does_not_create_empty_root(self) -> None:
        validate = self.source.index("private func validateDataRootAvailability()")
        create = self.source.index("private func ensurePrivateFolders()")
        start_call = self.source.index("try self.validateDataRootAvailability()")
        ensure_call = self.source.index('self.appendLauncherLog("Ensuring private folders.')
        self.assertLess(start_call, ensure_call)
        self.assertIn("will not create an empty replacement automatically", self.source[validate : validate + 1800])
        self.assertGreater(validate, create)

if __name__ == "__main__":
    unittest.main()

