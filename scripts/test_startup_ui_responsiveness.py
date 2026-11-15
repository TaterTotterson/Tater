from __future__ import annotations

import ast
import asyncio
import contextlib
from pathlib import Path
import sys
import threading
import types
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


def load_functions(path, names, namespace):
    tree = ast.parse((ROOT / path).read_text())
    functions = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names]
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec"), namespace)
    return namespace


class StartupUiResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_voice_preload_keeps_http_loop_free_and_watchdog_on_owner_loop(self):
        owner_loop = asyncio.get_running_loop()
        owner_thread = threading.get_ident()
        started, release = threading.Event(), threading.Event()
        preload_threads = []
        binding_loops = []

        def slow_preload():
            preload_threads.append(threading.get_ident())
            started.set()
            release.wait(1.0)

        vp = mock.MagicMock()
        vp.__version__ = "test"
        vp.contextlib = contextlib
        vp._resolve_stt_backend.return_value = ("local", "")
        vp._resolve_tts_backend.return_value = ("local", "")
        vp._voice_config_snapshot.return_value = {"eou": {"backend": "silero"}}
        vp._normalize_vad_backend.return_value = "silero"
        vp._as_bool.side_effect = lambda value, default=False: bool(value)
        vp._text.side_effect = lambda value: str(value or "")
        vp._load_satellite_registry.return_value = [
            {"selector": "office", "metadata": {"native_selected": True}},
            {"selector": "kitchen", "metadata": {"native_selected": True}},
            {"selector": "unused", "metadata": {"native_selected": False}},
        ]
        vp.SileroVadBackend._ensure_shared.side_effect = slow_preload
        vp.SileroVadBackend._shared_ready = True
        vp.SileroVadBackend.preload_owner.return_value = True
        vp._background_tasks = {}

        async def watchdog():
            await asyncio.Event().wait()

        package = types.ModuleType("startup_test")
        package.native_satellite = types.SimpleNamespace(bind_runtime_loop=lambda: binding_loops.append(asyncio.get_running_loop()))
        namespace = load_functions("tater_voice/voice_pipeline/routes.py", {"startup", "_prepare_startup_models"}, {
            "__package__": "startup_test.voice_pipeline", "Any": object,
            "asyncio": asyncio, "sys": sys, "_vp": lambda: vp, "_event_loop_watchdog": watchdog,
        })
        with mock.patch.dict(sys.modules, {"startup_test": package}):
            startup = asyncio.create_task(namespace["startup"]())
            try:
                self.assertTrue(await asyncio.to_thread(started.wait, 1.0))
                self.assertNotIn(owner_thread, preload_threads)
                self.assertFalse(startup.done(), "Voice models should still be preparing")
                # An HTTP handler runs on this same event loop during model loading.
                async def ui_request():
                    return "UI and model progress available"
                self.assertEqual(await asyncio.wait_for(ui_request(), 0.1), "UI and model progress available")
            finally:
                release.set()
                await asyncio.wait_for(startup, 2.0)
                for task in vp._background_tasks.values():
                    task.cancel()
                await asyncio.gather(*vp._background_tasks.values(), return_exceptions=True)

        self.assertEqual(binding_loops, [owner_loop])
        self.assertIs(vp._background_tasks["event_loop_watchdog"].get_loop(), owner_loop)
        self.assertEqual([call.kwargs["owner_key"] for call in vp.SileroVadBackend.preload_owner.call_args_list], ["office", "kitchen"])

    async def test_airplay_preparation_keeps_http_loop_free_and_startup_order(self):
        owner_thread = threading.get_ident()
        started, release = threading.Event(), threading.Event()
        events, worker_threads = [], []

        async def voice_startup():
            events.append("voice")

        def airplay_startup():
            events.append("airplay")
            worker_threads.append(threading.get_ident())
            started.set()
            release.wait(1.0)

        namespace = load_functions("tater_voice/home.py", {"startup"}, {
            "asyncio": asyncio,
            "esphome_settings": types.SimpleNamespace(cleanup_removed_user_settings=lambda: events.append("settings")),
            "native_satellite": types.SimpleNamespace(bind_runtime_loop=lambda: events.append("bind")),
            "esphome_runtime": types.SimpleNamespace(startup=voice_startup),
            "airplay_input": types.SimpleNamespace(startup=airplay_startup),
        })
        startup = asyncio.create_task(namespace["startup"]())
        try:
            self.assertTrue(await asyncio.to_thread(started.wait, 1.0))
            self.assertNotIn(owner_thread, worker_threads)
            self.assertFalse(startup.done())
        finally:
            release.set()
            await asyncio.wait_for(startup, 2.0)
        self.assertEqual(events, ["settings", "bind", "voice", "airplay"])


if __name__ == "__main__":
    unittest.main()
