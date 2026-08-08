#!/usr/bin/env python3
from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAUNCHER_SOURCE = ROOT / "macos" / "Tater" / "Sources" / "TaterAssistant" / "main.swift"
RUN_UI_SOURCE = ROOT / "run_ui.sh"


class MacOSLauncherShutdownTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = LAUNCHER_SOURCE.read_text(encoding="utf-8")
        cls.run_ui_source = RUN_UI_SOURCE.read_text(encoding="utf-8")

    def test_uvicorn_connection_drain_has_a_bounded_timeout(self) -> None:
        self.assertIn(
            'HTMLUI_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS="${HTMLUI_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS:-8}"',
            self.run_ui_source,
        )
        self.assertIn(
            '--timeout-graceful-shutdown "${HTMLUI_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS}"',
            self.run_ui_source,
        )

    def test_backend_output_handler_detaches_at_eof(self) -> None:
        self.assertRegex(
            self.source,
            re.compile(
                r"guard !data\.isEmpty else \{\s*"
                r"reader\.readabilityHandler = nil\s*"
                r"return\s*"
                r"\}",
                re.MULTILINE,
            ),
        )

    def test_managed_process_wait_detaches_pipe_before_final_wait(self) -> None:
        start = self.source.index("private func waitForManagedProcessExit")
        end = self.source.index("private func terminateBackendProcess", start)
        implementation = self.source[start:end]

        detach_index = implementation.index("detachOutputPipe()")
        wait_index = implementation.index("process.waitUntilExit()")
        self.assertLess(detach_index, wait_index)

    def test_detach_helper_clears_handler_and_pipe_reference(self) -> None:
        start = self.source.index("private func detachOutputPipe")
        end = self.source.index("private func appendLog", start)
        implementation = self.source[start:end]

        self.assertIn("outputPipe?.fileHandleForReading.readabilityHandler = nil", implementation)
        self.assertIn("outputPipe = nil", implementation)

    def test_runtime_requirements_are_fingerprinted(self) -> None:
        start = self.source.index("private func pinnedRuntimeDependenciesReady")
        end = self.source.index("private func pinnedRequirementVersion", start)
        implementation = self.source[start:end]

        self.assertIn('runtimeDir.appendingPathComponent("tater-requirements.sha256")', implementation)
        self.assertIn("recorded == expected", implementation)
        self.assertIn('installedPackageVersion("onnx-asr", using: python)', implementation)
        self.assertNotIn("return true\n    }", implementation.split("guard recorded == expected", 1)[0])

    def test_successful_setup_records_requirements_fingerprint(self) -> None:
        start = self.source.index("private func bootstrapIfNeeded")
        end = self.source.index("private func localLLMPythonDependenciesReady", start)
        implementation = self.source[start:end]

        status_guard = implementation.index("guard process.terminationStatus == 0")
        record = implementation.index("try recordRuntimeRequirementsFingerprint()")
        self.assertLess(status_guard, record)

    def test_termination_reply_runs_in_appkit_modal_run_loop(self) -> None:
        start = self.source.index("func applicationShouldTerminate(")
        end = self.source.index("private func startRecoveryWatchdog", start)
        implementation = self.source[start:end]

        self.assertIn("CFRunLoopPerformBlock(", implementation)
        self.assertIn("RunLoop.Mode.modalPanel.rawValue as CFString", implementation)
        self.assertIn("CFRunLoopWakeUp(mainRunLoop)", implementation)
        self.assertNotIn("DispatchQueue.main.async {", implementation)

    def test_installer_forces_stuck_old_app_to_exit_before_replacing_it(self) -> None:
        start = self.source.index("private func writeInstallerScript()")
        end = self.source.index("private func safePathComponent", start)
        implementation = self.source[start:end]

        term_index = implementation.index('kill -TERM "$APP_PID"')
        kill_index = implementation.index('kill -KILL "$APP_PID"')
        target_index = implementation.index('TARGET_PARENT="$(dirname "$TARGET_APP")"')
        self.assertLess(term_index, kill_index)
        self.assertLess(kill_index, target_index)


if __name__ == "__main__":
    unittest.main()
