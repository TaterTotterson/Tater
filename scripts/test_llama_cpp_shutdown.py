#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import helpers  # noqa: E402


class _FakeNativeProcess:
    def __init__(self) -> None:
        self.returncode = None
        self.killed = False
        self.terminated = False

    def poll(self):
        return self.returncode

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -15

    def wait(self, timeout=None):
        return self.returncode


class _FakeStdin:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeEngineProcess(_FakeNativeProcess):
    def __init__(self, pid: int) -> None:
        super().__init__()
        self.pid = pid
        self.stdin = _FakeStdin()

    def wait(self, timeout=None):
        self.returncode = 0
        return self.returncode


class LlamaCppShutdownTests(unittest.TestCase):
    def test_macos_native_server_skips_unstable_metal_finalizer(self) -> None:
        process = _FakeNativeProcess()
        state = {"process": process}

        with mock.patch.object(helpers.sys, "platform", "darwin"):
            helpers._llama_cpp_native_shutdown_state(state)

        self.assertTrue(process.killed)
        self.assertFalse(process.terminated)
        self.assertEqual(state, {})

    def test_non_macos_native_server_keeps_graceful_shutdown(self) -> None:
        process = _FakeNativeProcess()
        state = {"process": process}

        with mock.patch.object(helpers.sys, "platform", "linux"):
            helpers._llama_cpp_native_shutdown_state(state)

        self.assertFalse(process.killed)
        self.assertTrue(process.terminated)
        self.assertEqual(state, {})

    def test_stale_selection_excludes_servers_owned_by_current_backend(self) -> None:
        server_bin = "/Applications/Tater.app/Contents/Resources/Native/llama.cpp/bin/llama-server"
        rows = [
            {"pid": 100, "ppid": 1, "command": "python -m uvicorn tateros_app:app"},
            {"pid": 110, "ppid": 100, "command": "python helpers.py --tater-llama-cpp-engine-worker"},
            {"pid": 111, "ppid": 110, "command": f"{server_bin} --alias tater-llama"},
            {"pid": 200, "ppid": 1, "command": f"{server_bin} --alias tater-llama"},
            {"pid": 300, "ppid": 1, "command": "python helpers.py --tater-llama-cpp-engine-worker"},
            {"pid": 301, "ppid": 300, "command": f"{server_bin} --alias=tater-llama"},
            {"pid": 400, "ppid": 1, "command": f"{server_bin} --alias qwen3-asr"},
            {"pid": 500, "ppid": 1, "command": f"{server_bin} --alias unrelated"},
            {"pid": 600, "ppid": 1, "command": "/usr/bin/llama-server --alias tater-llama"},
        ]

        stale = helpers._llama_cpp_native_managed_server_pids(
            rows,
            server_bins=[server_bin],
            exclude_descendants_of=100,
        )

        self.assertEqual(stale, [200, 301, 400])

    def test_engine_shutdown_finds_only_its_server_tree(self) -> None:
        server_bin = "/opt/llama.cpp/build/bin/llama-server"
        rows = [
            {"pid": 700, "ppid": 10, "command": "python helpers.py --tater-llama-cpp-engine-worker"},
            {"pid": 701, "ppid": 700, "command": f"{server_bin} --alias tater-llama"},
            {"pid": 800, "ppid": 10, "command": "python helpers.py --tater-llama-cpp-engine-worker"},
            {"pid": 801, "ppid": 800, "command": f"{server_bin} --alias tater-llama"},
        ]

        owned = helpers._llama_cpp_native_managed_server_pids(
            rows,
            server_bins=[server_bin],
            aliases={"tater-llama"},
            descendant_of=700,
        )

        self.assertEqual(owned, [701])

    def test_packaged_binary_with_spaces_is_recognized(self) -> None:
        server_bin = "/Applications/Tater Test.app/Contents/Resources/Native/llama.cpp/bin/llama-server"
        row = {
            "pid": 900,
            "ppid": 1,
            "command": f'"{server_bin}" --alias tater-llama',
        }

        self.assertTrue(
            helpers._llama_cpp_native_managed_server_row(
                row,
                server_bins=[server_bin],
            )
        )

    def test_engine_shutdown_reaps_tracked_and_discovered_server_pid(self) -> None:
        server_bin = "/Applications/Tater.app/Contents/Resources/Native/llama.cpp/bin/llama-server"
        worker = _FakeEngineProcess(pid=700)
        engine = helpers._TaterLlamaCppEngineProcess(cache_key=("model",), model_token="model")
        engine.process = worker
        engine._server_pids.add(701)
        engine.request = mock.Mock(return_value={"shutdown": True})
        rows = [
            {"pid": 700, "ppid": 100, "command": "python helpers.py --tater-llama-cpp-engine-worker"},
            {"pid": 701, "ppid": 700, "command": f"{server_bin} --alias tater-llama"},
        ]
        cleanup_result = {"requested": [701], "terminated": [701], "remaining": []}

        with (
            mock.patch.object(helpers, "_llama_cpp_native_server_candidates", return_value=[server_bin]),
            mock.patch.object(helpers, "_llama_cpp_native_process_rows", return_value=rows),
            mock.patch.object(
                helpers,
                "_terminate_managed_llama_cpp_server_pids",
                return_value=cleanup_result,
            ) as cleanup,
        ):
            engine.shutdown()

        cleanup.assert_called_once()
        self.assertEqual(cleanup.call_args.args[0], [701])
        self.assertIsNone(engine.process)
        self.assertFalse(engine._server_pids)
        self.assertTrue(worker.stdin.closed)


if __name__ == "__main__":
    unittest.main()
