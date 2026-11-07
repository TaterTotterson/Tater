#!/usr/bin/env python3
from __future__ import annotations

import ast
import pathlib
import re
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
PIPELINE_PATH = REPO_ROOT / "tater_voice" / "voice_pipeline" / "__init__.py"


def _process_voice_turn_node() -> ast.AsyncFunctionDef:
    tree = ast.parse(PIPELINE_PATH.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_process_voice_turn":
            return node
    raise AssertionError("_process_voice_turn was not found")


def _finalize_session_node() -> ast.AsyncFunctionDef:
    tree = ast.parse(PIPELINE_PATH.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_finalize_session":
            return node
    raise AssertionError("_finalize_session was not found")


def _pipeline_function(name: str):
    tree = ast.parse(PIPELINE_PATH.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            namespace = {
                "_text": lambda value: str(value or "").strip(),
                "re": re,
            }
            module = ast.Module(body=[node], type_ignores=[])
            exec(compile(ast.fix_missing_locations(module), str(PIPELINE_PATH), "exec"), namespace)
            return namespace[node.name]
    raise AssertionError(f"{name} was not found")


def _transcript_completeness_assessment():
    tree = ast.parse(PIPELINE_PATH.read_text(encoding="utf-8"))
    dependency_names = {
        "_TRANSCRIPT_COMPLETE_SHORT_COMMANDS",
        "_TRANSCRIPT_INCOMPLETE_TRAILING_WORDS",
        "_TRANSCRIPT_INCOMPLETE_TRAILING_PHRASES",
    }
    nodes = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in dependency_names
            for target in node.targets
        ):
            nodes.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == "_transcript_completeness_assessment":
            nodes.append(node)

    namespace = {
        "Any": object,
        "Dict": dict,
        "_text": lambda value: str(value or "").strip(),
        "re": re,
    }
    module = ast.Module(body=nodes, type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(PIPELINE_PATH), "exec"), namespace)
    return namespace["_transcript_completeness_assessment"]


class VoiceTranscriptFilterTests(unittest.TestCase):
    def test_only_blank_or_filler_transcripts_are_rejected(self) -> None:
        actionable = _pipeline_function("_transcript_is_actionable")

        for transcript in ("", "...", "um", "um uh hmm"):
            with self.subTest(transcript=transcript):
                self.assertFalse(actionable(transcript))

        for transcript in ("go", "tv", "yes", "weather", "you're cool", "um weather"):
            with self.subTest(transcript=transcript):
                self.assertTrue(actionable(transcript))

    def test_short_complete_phrase_is_not_mistaken_for_clipped_speech(self) -> None:
        assess = _transcript_completeness_assessment()

        self.assertTrue(assess("you're cool")["complete"])
        self.assertTrue(assess("go")["complete"])
        self.assertFalse(assess("turn on the")["complete"])

    def test_process_turn_uses_words_not_vad_duration_as_validity_gate(self) -> None:
        node = _process_voice_turn_node()
        called_names = {
            child.func.id
            for child in ast.walk(node)
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
        }

        self.assertIn("_transcript_is_actionable", called_names)
        self.assertNotIn("_voice_turn_has_low_speech_evidence", called_names)

    def test_process_turn_retries_incomplete_transcripts(self) -> None:
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

        self.assertIn("_transcript_completeness_assessment", called_names)
        self.assertIn("_incomplete_transcript_retry_response_text", called_names)
        self.assertIn("transcript_retry", string_constants)

    def test_no_speech_recovery_uses_the_same_actionable_policy(self) -> None:
        node = _finalize_session_node()
        called_names = {
            child.func.id
            for child in ast.walk(node)
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
        }
        referenced_names = {
            child.id
            for child in ast.walk(node)
            if isinstance(child, ast.Name)
        }

        self.assertIn("_transcript_is_actionable", called_names)
        self.assertNotIn("recovery_prob_threshold", referenced_names)
        self.assertNotIn("recovery_voice_evidence", referenced_names)

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
