from __future__ import annotations

import ast
import asyncio
from pathlib import Path
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock
from typing import Any


def poll_namespace(enabled, module):
    path = Path(__file__).resolve().parents[1] / 'tater/extensions/integration_runtime.py'
    tree = ast.parse(path.read_text(), filename=str(path))
    function = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "_generic_integration_poll_loop")

    async def background(fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def finish(stop_event, _delay):
        stop_event.set()

    namespace = {
        "asyncio": asyncio, "Any": Any, "time": time, "logger": Mock(),
        "run_background": background, "_runtime_client": lambda client: client,
        "integration_store_module": SimpleNamespace(get_enabled_integration_ids=enabled),
        "_integration_module": module, "_runtime_poller": lambda _module: None,
        "_runtime_poll_seconds": lambda _module: 30,
        "_GENERIC_RUNTIME_NEXT_POLL": {}, "_GENERIC_RUNTIME_CURSOR": {},
        "_publish_generic_runtime_result": AsyncMock(), "_status_set": Mock(),
        "_sleep": finish,
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), namespace)
    return namespace


class IntegrationPollResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def check_lookup(self, kind):
        entered, release = threading.Event(), threading.Event()
        worker = None

        def slow():
            nonlocal worker
            worker = threading.get_ident()
            entered.set()
            release.wait(0.4)

        def enabled():
            if kind == "enabled":
                slow()
            return ["synthetic"]

        def module(_name):
            if kind == "module":
                slow()
            return object()

        namespace = poll_namespace(enabled, module)
        task = asyncio.create_task(namespace["_generic_integration_poll_loop"](asyncio.Event(), object()))
        try:
            for _ in range(100):
                if entered.is_set():
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(entered.is_set())
            self.assertFalse(task.done(), kind + " lookup blocked the event loop")
            self.assertNotEqual(worker, threading.get_ident())
        finally:
            release.set()
            await task

    async def test_enabled_lookup_runs_off_loop(self):
        await self.check_lookup("enabled")

    async def test_module_discovery_runs_off_loop(self):
        await self.check_lookup("module")

    async def test_polling_cursor_result_and_status_are_preserved(self):
        poller = Mock(return_value={"cursor": "next", "events": []})
        namespace = poll_namespace(lambda: ["synthetic"], lambda _name: object())
        namespace["_runtime_poller"] = lambda _module: poller
        namespace["_call_runtime_poller"] = lambda fn, **kwargs: fn(**kwargs)
        namespace["_GENERIC_RUNTIME_CURSOR"]["synthetic"] = "previous"
        client = object()
        await namespace["_generic_integration_poll_loop"](asyncio.Event(), client)
        poller.assert_called_once_with(client=client, cursor="previous")
        namespace["_publish_generic_runtime_result"].assert_awaited_once_with(client, "synthetic", poller.return_value)
        self.assertTrue(namespace["_status_set"].call_args.kwargs["synthetic_generic_poll_connected"])
        self.assertIn("synthetic", namespace["_GENERIC_RUNTIME_NEXT_POLL"])


if __name__ == "__main__":
    unittest.main()
