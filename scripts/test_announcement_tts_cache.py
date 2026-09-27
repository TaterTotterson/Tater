from __future__ import annotations

import ast
import contextlib
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import threading
import time
import unittest
import uuid
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]


def _load_cache_helpers(cache_root: Path):
    source = (ROOT / "speech_tts.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    names = {
        "_text",
        "_announcement_tts_clone_audio_identity",
        "_announcement_tts_cache_key",
        "_announcement_tts_cache_path",
        "_is_wav_bytes",
        "_announcement_tts_cache_read",
        "_cleanup_announcement_tts_cache_locked",
        "_announcement_tts_cache_write",
    }
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    namespace = {
        "ANNOUNCEMENT_TTS_CACHE_SCHEMA": 1,
        "ANNOUNCEMENT_TTS_CACHE_MAX_ITEMS": 2,
        "ANNOUNCEMENT_TTS_CACHE_MAX_BYTES": 1024 * 1024,
        "ANNOUNCEMENT_TTS_CACHE_MAX_AGE_SECONDS": 30 * 24 * 60 * 60,
        "ANNOUNCEMENT_TTS_CACHE_ROOT": cache_root,
        "_announcement_tts_cache_lock": threading.RLock(),
        "_announcement_tts_clone_digest_cache": {},
        "contextlib": contextlib,
        "hashlib": hashlib,
        "json": json,
        "os": os,
        "Path": Path,
        "re": re,
        "threading": threading,
        "time": time,
        "uuid": uuid,
        "logger": mock.Mock(),
    }
    future = ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)
    module = ast.Module(body=[future, *functions], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), "<announcement-tts-cache>", "exec"), namespace)
    return namespace


def _wav(payload: bytes) -> bytes:
    return b"RIFF" + len(payload).to_bytes(4, "little") + b"WAVE" + payload


class AnnouncementTtsCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.cache_root = Path(self.tempdir.name)
        self.helpers = _load_cache_helpers(self.cache_root)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_cache_persists_wav_and_reuses_it(self) -> None:
        key = self.helpers["_announcement_tts_cache_key"](
            text="The back door is open.",
            backend="omnivoice",
            model="voice-model",
            openai_api_key="secret-value",
        )
        payload = _wav(b"announcement")

        self.assertTrue(self.helpers["_announcement_tts_cache_write"](key, payload))
        self.assertEqual(self.helpers["_announcement_tts_cache_read"](key), payload)
        self.assertNotIn("secret-value", self.helpers["_announcement_tts_cache_path"](key).name)

    def test_final_text_and_voice_settings_invalidate_cache_key(self) -> None:
        make_key = self.helpers["_announcement_tts_cache_key"]
        base = make_key(text="Door open", backend="omnivoice", model="one")

        self.assertNotEqual(base, make_key(text="Custom reminder", backend="omnivoice", model="one"))
        self.assertNotEqual(base, make_key(text="Door open", backend="omnivoice", model="two"))

    def test_cleanup_keeps_only_the_newest_bounded_entries(self) -> None:
        write = self.helpers["_announcement_tts_cache_write"]
        for index in range(3):
            write(f"{index + 1:064x}", _wav(f"audio-{index}".encode()))
            time.sleep(0.01)

        rows = sorted(self.cache_root.glob("*.wav"))
        self.assertEqual(len(rows), 2)
        self.assertFalse((self.cache_root / f"{1:064x}.wav").exists())


if __name__ == "__main__":
    unittest.main()
