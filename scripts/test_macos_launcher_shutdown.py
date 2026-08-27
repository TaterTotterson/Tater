#!/usr/bin/env python3
from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAUNCHER_SOURCE = ROOT / "macos" / "Tater" / "Sources" / "TaterAssistant" / "main.swift"
RUN_UI_SOURCE = ROOT / "run_ui.sh"
REQUIREMENTS = ROOT / "requirements.txt"
SETUP_SOURCE = ROOT / "setup_tater.sh"
HELPERS_SOURCE = ROOT / "helpers.py"
FIRMWARE_SOURCE = ROOT / "tater_voice" / "firmware.py"


class MacOSLauncherShutdownTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = LAUNCHER_SOURCE.read_text(encoding="utf-8")
        cls.run_ui_source = RUN_UI_SOURCE.read_text(encoding="utf-8")
        cls.requirements = REQUIREMENTS.read_text(encoding="utf-8")
        cls.setup_source = SETUP_SOURCE.read_text(encoding="utf-8")
        cls.helpers_source = HELPERS_SOURCE.read_text(encoding="utf-8")
        cls.firmware_source = FIRMWARE_SOURCE.read_text(encoding="utf-8")

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

    def test_face_id_dependencies_are_part_of_the_managed_runtime(self) -> None:
        self.assertIn("deepface==0.0.100", self.requirements)
        self.assertIn("tensorflow-metal==1.2.0", self.requirements)
        self.assertNotIn("requirements-face.txt", self.setup_source)
        self.assertNotIn("filtered_face_id_requirements", self.setup_source)
        self.assertNotIn("install_face_id_runtime", self.setup_source)
        self.assertIn('face_id_required = ["cv2", "deepface", "retinaface", "tensorflow", "tf_keras"]', self.setup_source)
        self.assertIn("cleanup_legacy_runtime", self.setup_source)
        self.assertIn("aioesphomeapi", self.setup_source)
        self.assertIn("nanowakeword", self.setup_source)
        self.assertIn('rm -rf "${managed_venv}"', self.setup_source)

        start = self.source.index("private func pinnedRuntimeDependenciesReady")
        end = self.source.index("private func sourceRequirementsFingerprint", start)
        implementation = self.source[start:end]
        self.assertIn('"deepface", "tensorflow", "tf-keras", "opencv-python", "retina-face"', implementation)
        self.assertIn('faceIDPackages.append("tensorflow-metal")', implementation)
        self.assertIn("using: python", implementation)
        self.assertNotIn("faceIDPython", implementation)

    def test_obsolete_runtime_content_triggers_setup_cleanup(self) -> None:
        start = self.source.index("private func pinnedRuntimeDependenciesReady")
        end = self.source.index("private func sourceRequirementsFingerprint", start)
        implementation = self.source[start:end]

        self.assertIn('"aioesphomeapi"', implementation)
        self.assertIn('"nanowakeword"', implementation)
        self.assertIn('"openwakeword"', implementation)
        self.assertIn('appendingPathComponent("models/face-id/venv/pyvenv.cfg")', implementation)
        self.assertIn('agentRoot.appendingPathComponent("esphome", isDirectory: true)', implementation)
        self.assertIn('agentRoot.appendingPathComponent("firmware", isDirectory: true)', implementation)

    def test_native_firmware_workspace_has_a_current_name(self) -> None:
        self.assertIn('FIRMWARE_WORKSPACE_ROOT = agent_lab_path("firmware")', self.firmware_source)
        self.assertNotIn('agent_lab_path("esphome")', self.firmware_source)
        self.assertIn('legacy_firmware_root="${AGENT_ROOT}/esphome"', self.setup_source)
        self.assertIn('firmware_root="${AGENT_ROOT}/firmware"', self.setup_source)

    def test_bootstrap_and_recovery_share_one_lifecycle_gate(self) -> None:
        start = self.source.index("func start()")
        restart = self.source.index("func restart()", start)
        start_implementation = self.source[start:restart]
        self.assertIn("guard beginLifecycleOperation() else", start_implementation)
        self.assertIn("self.endLifecycleOperation()", start_implementation)

        recovery = self.source.index("func recoverIfBackendMissing()")
        recent_logs = self.source.index("func recentLogText", recovery)
        recovery_implementation = self.source[recovery:recent_logs]
        gate = recovery_implementation.index("guard self.beginLifecycleOperation() else")
        setup_guard = recovery_implementation.index("guard self.setupProcess == nil, self.process == nil")
        launch = recovery_implementation.index("try self.launchBackend()")
        self.assertLess(gate, setup_guard)
        self.assertLess(setup_guard, launch)
        self.assertIn("self.endLifecycleOperation()", recovery_implementation)
        self.assertIn("private let lifecycleLock = NSLock()", self.source)

    def test_mlx_engine_runtime_dependencies_are_installed_and_import_verified(self) -> None:
        self.assertIn('dill==0.4.1; platform_system == "Darwin"', self.requirements)
        self.assertIn('xxhash==3.8.1; platform_system == "Darwin"', self.requirements)
        self.assertIn("from mlx_engine.generate import create_generator, load_model, tokenize", self.setup_source)

        start = self.source.index("private func localLLMPythonDependenciesReady")
        end = self.source.index("private func localLLMRuntimesReady", start)
        implementation = self.source[start:end]
        self.assertIn('"outlines_core", "dill", "xxhash"', implementation)
        self.assertIn("from mlx_engine.generate import create_generator, load_model, tokenize", implementation)
        self.assertIn("process.environment = backendEnvironment()", implementation)

    def test_mlx_engine_import_error_explains_automatic_repair(self) -> None:
        start = self.helpers_source.index("def _mlx_engine_import_helpers")
        end = self.helpers_source.index("def _mlx_engine_config_json", start)
        implementation = self.helpers_source[start:end]
        self.assertIn("Restart Tater to let the macOS app repair its private runtime", implementation)
        self.assertIn("isinstance(exc, ModuleNotFoundError) and exc.name", implementation)

    def test_termination_reply_runs_in_appkit_modal_run_loop(self) -> None:
        start = self.source.index("func applicationShouldTerminate(")
        end = self.source.index("private func startRecoveryWatchdog", start)
        implementation = self.source[start:end]

        self.assertIn("CFRunLoopPerformBlock(", implementation)
        self.assertIn("RunLoop.Mode.modalPanel.rawValue as CFString", implementation)
        self.assertIn("CFRunLoopWakeUp(mainRunLoop)", implementation)
        self.assertNotIn("DispatchQueue.main.async {", implementation)

    def test_backend_cleans_stale_tater_llama_servers_before_redis_bootstrap(self) -> None:
        app_source = (ROOT / "tateros_app.py").read_text(encoding="utf-8")
        startup = app_source[app_source.index("async def _startup_event()") :]

        cleanup_index = startup.index("cleanup_stale_llama_cpp_servers")
        redis_index = startup.index("_redis_reachable_for_startup()")

        self.assertLess(cleanup_index, redis_index)

    def test_launcher_cleans_only_bundled_tater_llama_aliases(self) -> None:
        start = self.source.index("private func cleanupStaleBundledLlamaServers")
        end = self.source.index("private func waitForProcessExit", start)
        implementation = self.source[start:end]

        self.assertIn("bundledLlamaServerURL()?.standardizedFileURL.path", implementation)
        self.assertIn('command.contains("--alias tater-llama")', implementation)
        self.assertIn('command.contains("--alias qwen3-asr")', implementation)
        self.assertIn('command.hasPrefix(serverPath + " ")', implementation)
        self.assertIn("Darwin.kill(pid, SIGKILL)", implementation)

        stop_start = self.source.index("func stop(waitForExit:")
        stop_end = self.source.index("func openLogsFolder", stop_start)
        stop_implementation = self.source[stop_start:stop_end]
        self.assertGreaterEqual(stop_implementation.count("cleanupStaleBundledLlamaServers()"), 3)

    def test_process_capture_drains_output_before_waiting_for_exit(self) -> None:
        start = self.source.index("private func runProcessCapture")
        end = self.source.index("private func waitForWebReady", start)
        implementation = self.source[start:end]

        read_index = implementation.index("readDataToEndOfFile()")
        wait_index = implementation.index("process.waitUntilExit()")
        self.assertLess(read_index, wait_index)
        self.assertIn("process.standardError = FileHandle.nullDevice", implementation)

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
