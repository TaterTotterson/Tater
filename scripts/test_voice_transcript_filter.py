#!/usr/bin/env python3
from __future__ import annotations

import ast
import pathlib
import re
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
PIPELINE_PATH = REPO_ROOT / "tater_voice" / "voice_pipeline" / "__init__.py"


class _VoiceSessionRuntime:
    pass


def _process_voice_turn_node() -> ast.AsyncFunctionDef:
    tree = ast.parse(PIPELINE_PATH.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_process_voice_turn":
            return node
    raise AssertionError("_process_voice_turn was not found")


def _voice_turn_has_low_speech_evidence():
    tree = ast.parse(PIPELINE_PATH.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_voice_turn_has_low_speech_evidence":
            namespace = {
                "VoiceSessionRuntime": _VoiceSessionRuntime,
                "_text": lambda value: str(value or "").strip(),
                "re": re,
            }
            module = ast.Module(body=[node], type_ignores=[])
            exec(compile(ast.fix_missing_locations(module), str(PIPELINE_PATH), "exec"), namespace)
            return namespace[node.name]
    raise AssertionError("_voice_turn_has_low_speech_evidence was not found")


class VoiceTranscriptFilterTests(unittest.TestCase):
    @staticmethod
    def _session(**overrides: object) -> _VoiceSessionRuntime:
        session = _VoiceSessionRuntime()
        for key, value in overrides.items():
            setattr(session, key, value)
        return session

    def test_vad_confirmed_short_sentence_is_not_rejected(self) -> None:
        session = self._session(
            speech_duration_s=0.12,
            max_probability=0.24,
            vad_audio_peak_dbfs=-40.0,
            vad_audio_level_chunks=4,
        )

        low_speech_evidence = _voice_turn_has_low_speech_evidence()
        self.assertFalse(low_speech_evidence(session, "what time is it"))
        self.assertFalse(low_speech_evidence(session, "turn on the tv"))

    def test_unconfirmed_transcript_still_requires_speech_evidence(self) -> None:
        session = self._session(
            speech_duration_s=0.11,
            max_probability=0.24,
            vad_audio_peak_dbfs=-40.0,
            vad_audio_level_chunks=4,
        )

        low_speech_evidence = _voice_turn_has_low_speech_evidence()
        self.assertTrue(low_speech_evidence(session, "thank you for watching"))

    def test_process_turn_no_longer_rejects_incomplete_transcripts(self) -> None:
        node = _process_voice_turn_node()
        called_names = {
            child.func.id
            for child in ast.walk(node)
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
        }
        string_constants = {
            child.value
            for child in ast.walk(node)
            if isinstance(child, ast.Constant) and isinstance(child.value, str)
        }

        self.assertNotIn("_transcript_completeness_assessment", called_names)
        self.assertNotIn("clipped_ambiguous_transcript", string_constants)

    def test_process_turn_keeps_empty_transcript_no_op(self) -> None:
        node = _process_voice_turn_node()
        string_constants = {
            child.value
            for child in ast.walk(node)
            if isinstance(child, ast.Constant) and isinstance(child.value, str)
        }

        self.assertIn("empty_transcript", string_constants)
        self.assertIn("no_op", string_constants)


if __name__ == "__main__":
    unittest.main()
