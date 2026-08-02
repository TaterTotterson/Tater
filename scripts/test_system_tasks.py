#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import pathlib
import unittest

from system_tasks import CoreTaskRunManager, SystemTaskManager


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class SystemTaskManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_runs_are_coalesced_and_report_timing(self) -> None:
        manager = SystemTaskManager()
        started = asyncio.Event()
        release = asyncio.Event()

        async def runner(_reason: str) -> None:
            started.set()
            await release.wait()

        manager.register(
            "snapshot",
            label="Snapshot",
            description="Test snapshot",
            interval_seconds=60,
            initial_delay_seconds=60,
            runner=runner,
        )
        manager.start()
        try:
            self.assertTrue(await manager.trigger("snapshot", reason="manual"))
            await asyncio.wait_for(started.wait(), timeout=1.0)
            self.assertFalse(await manager.trigger("snapshot", reason="manual"))
            running = manager.get("snapshot") or {}
            self.assertTrue(running.get("running"))
            self.assertEqual(running.get("status"), "running")

            release.set()
            for _ in range(100):
                if not bool((manager.get("snapshot") or {}).get("running")):
                    break
                await asyncio.sleep(0.01)
            finished = manager.get("snapshot") or {}
            self.assertFalse(finished.get("running"))
            self.assertEqual(finished.get("run_count"), 1)
            self.assertGreater(float(finished.get("finished_at") or 0.0), 0.0)
            self.assertGreater(float(finished.get("next_run_at") or 0.0), float(finished.get("finished_at") or 0.0))
        finally:
            await manager.stop()

    async def test_scheduler_runs_due_tasks_and_stop_cleans_up(self) -> None:
        manager = SystemTaskManager()
        completed = asyncio.Event()

        async def runner(_reason: str) -> None:
            completed.set()

        manager.register(
            "quick",
            label="Quick",
            description="Quick task",
            interval_seconds=0.05,
            initial_delay_seconds=0,
            runner=runner,
        )
        manager.start()
        try:
            await asyncio.wait_for(completed.wait(), timeout=2.0)
            for _ in range(100):
                if int((manager.get("quick") or {}).get("run_count") or 0) >= 1:
                    break
                await asyncio.sleep(0.01)
            self.assertGreaterEqual(int((manager.get("quick") or {}).get("run_count") or 0), 1)
        finally:
            await manager.stop()

    async def test_event_bursts_debounce_and_queue_one_follow_up(self) -> None:
        manager = SystemTaskManager()
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        second_finished = asyncio.Event()
        reasons: list[str] = []

        async def runner(reason: str) -> None:
            reasons.append(reason)
            if len(reasons) == 1:
                first_started.set()
                await release_first.wait()
            elif len(reasons) == 2:
                second_finished.set()

        manager.register(
            "snapshot",
            label="Snapshot",
            description="Event snapshot",
            interval_seconds=60,
            initial_delay_seconds=60,
            runner=runner,
        )
        manager.start()
        try:
            for _ in range(3):
                self.assertTrue(
                    manager.request_run_debounced(
                        "snapshot",
                        reason="satellite-connected",
                        delay_seconds=0.01,
                    )
                )
            await asyncio.wait_for(first_started.wait(), timeout=1.0)
            self.assertEqual(reasons, ["satellite-connected"])

            for _ in range(3):
                self.assertTrue(
                    manager.request_run_debounced(
                        "snapshot",
                        reason="satellite-settings",
                        delay_seconds=0.01,
                    )
                )
            await asyncio.sleep(0.03)
            release_first.set()
            await asyncio.wait_for(second_finished.wait(), timeout=1.0)
            await asyncio.sleep(0.03)

            self.assertEqual(reasons, ["satellite-connected", "satellite-settings"])
            self.assertEqual(int((manager.get("snapshot") or {}).get("run_count") or 0), 2)
        finally:
            await manager.stop()


class CoreTaskRunManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_core_runs_are_backgrounded_and_coalesced(self) -> None:
        manager = CoreTaskRunManager()
        started = asyncio.Event()
        release = asyncio.Event()

        async def runner() -> None:
            started.set()
            await release.wait()

        try:
            self.assertTrue(await manager.trigger("memory_core", "memory_extraction", runner))
            await asyncio.wait_for(started.wait(), timeout=1.0)
            self.assertFalse(await manager.trigger("memory_core", "memory_extraction", runner))
            self.assertTrue(manager.state("memory_core", "memory_extraction").get("running"))
            release.set()
            for _ in range(100):
                if not manager.state("memory_core", "memory_extraction").get("running"):
                    break
                await asyncio.sleep(0.01)
            state = manager.state("memory_core", "memory_extraction")
            self.assertFalse(state.get("running"))
            self.assertEqual(state.get("run_count"), 1)
            self.assertGreater(float(state.get("duration_ms") or 0.0), 0.0)
        finally:
            await manager.stop()


class SystemTaskIntegrationContractTests(unittest.TestCase):
    def test_backend_registers_cached_satellite_and_dashboard_tasks(self) -> None:
        source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")

        for task_id in (
            "hardware_telemetry",
            "runtime_model_snapshot",
            "satellite_ui_snapshot",
            "integration_device_registry",
            "dashboard_snapshot",
            "dashboard_briefs",
        ):
            self.assertIn(f'"{task_id}"', source)
        self.assertIn('RUNTIME_HARDWARE_TELEMETRY_KEY = "tater:runtime:hardware-telemetry:v1"', source)
        self.assertIn('RUNTIME_MODEL_SNAPSHOT_KEY = "tater:runtime:model-snapshot:v1"', source)
        self.assertIn('VOICE_SATELLITE_SNAPSHOT_KEY = "tater:voice:satellites:ui-snapshot:v1"', source)
        self.assertIn('@app.get("/api/settings/system-tasks")', source)
        self.assertIn('@app.post("/api/settings/system-tasks/{task_id}/run")', source)

    def test_backend_discovers_core_owned_tasks_and_exposes_run_endpoint(self) -> None:
        source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")

        self.assertIn('getattr(module, "get_core_system_tasks", None)', source)
        self.assertIn('getattr(module, "run_core_system_task", None)', source)
        self.assertIn('@app.post("/api/settings/core-tasks/{core_key}/{task_id}/run")', source)
        self.assertIn('payload.update(_core_tasks_snapshot())', source)

    def test_core_task_contract_supports_automatic_event_driven_jobs(self) -> None:
        app_source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        ui_source = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn('raw_task.get("manual")', app_source)
        self.assertIn('raw_task.get("schedule_label")', app_source)
        self.assertIn('raw_task.get("next_run_label")', app_source)
        self.assertIn('detail = "This core task runs automatically."', app_source)
        self.assertIn('boolFromAny(task?.manual, true)', ui_source)
        self.assertIn('task?.schedule_label', ui_source)
        self.assertIn('task?.next_run_label', ui_source)
        self.assertIn('>Automatic</button>', ui_source)

    def test_satellite_snapshot_is_event_driven_with_five_minute_fallback(self) -> None:
        app_source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        native_source = (REPO_ROOT / "tater_voice" / "native_satellite.py").read_text(encoding="utf-8")

        self.assertIn("VOICE_SATELLITE_SNAPSHOT_INTERVAL_SECONDS = 60 * 5", app_source)
        self.assertIn("VOICE_SATELLITE_SNAPSHOT_DEBOUNCE_SECONDS = 2.0", app_source)
        self.assertIn("add_state_change_listener(_handle_native_satellite_state_change)", app_source)
        self.assertIn('request_run_debounced(\n        "satellite_ui_snapshot"', app_source)
        for event in ("connected", "disconnected", "settings"):
            self.assertIn(f'_notify_state_change("{event}"', native_source)

    def test_satellites_endpoint_serves_cache_and_refreshes_in_background(self) -> None:
        source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        start = source.index("def get_voice_runtime_payload")
        end = source.index("\n\n@app.post(\"/api/settings/voice/runtime/action\")", start)
        endpoint_source = source[start:end]

        self.assertIn("_voice_satellite_snapshot_load()", endpoint_source)
        self.assertIn('request_run("satellite_ui_snapshot", reason="satellites-stale")', endpoint_source)
        self.assertIn("_voice_satellite_snapshot_save(runtime_payload)", endpoint_source)

    def test_integration_registry_uses_events_and_five_minute_fallback(self) -> None:
        app_source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        runtime_source = (REPO_ROOT / "integration_runtime.py").read_text(encoding="utf-8")

        self.assertIn("INTEGRATION_DEVICE_REGISTRY_INTERVAL_SECONDS = 60 * 5", app_source)
        self.assertIn("INTEGRATION_DEVICE_REGISTRY_DEBOUNCE_SECONDS = 2.0", app_source)
        self.assertIn("add_device_registry_change_listener(_handle_integration_device_registry_change)", app_source)
        self.assertIn("manage_device_registry_cache=False", app_source)
        self.assertIn('_notify_device_registry_change("device-discovered"', runtime_source)

    def test_integration_tabs_serve_cached_registry_and_cached_playback_options(self) -> None:
        source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")

        self.assertIn("def _settings_cached_integration_device_registry()", source)
        self.assertIn("get_cached_integration_device_registry(redis_client)", source)
        self.assertIn("_integration_room_media_player_options_load()", source)
        self.assertIn('INTEGRATION_ROOM_MEDIA_PLAYER_OPTIONS_KEY = "tater:integration_runtime:room-media-player-options:v1"', source)

    def test_runtime_stats_and_context_settings_use_background_snapshots(self) -> None:
        app_source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        ui_source = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        helpers_source = (REPO_ROOT / "helpers.py").read_text(encoding="utf-8")

        self.assertIn("RUNTIME_HARDWARE_TELEMETRY_INTERVAL_SECONDS = 15", app_source)
        self.assertIn("RUNTIME_MODEL_SNAPSHOT_INTERVAL_SECONDS = 30", app_source)
        self.assertIn("_runtime_cached_loaded_models(include_models=False)", app_source)
        self.assertIn('@app.get("/api/runtime/context-estimate")', app_source)
        self.assertIn('api("/api/runtime/context-estimate"', ui_source)
        self.assertNotIn("Open the top stats bubble to refresh", ui_source)
        self.assertIn("enable_apple_ioreg_probe=True", app_source)
        self.assertIn("enable_ioreg_probe: Optional[bool] = None", helpers_source)


if __name__ == "__main__":
    unittest.main()
