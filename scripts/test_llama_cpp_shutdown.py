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


if __name__ == "__main__":
    unittest.main()
