#!/usr/bin/env python3
from __future__ import annotations

import ast
import pathlib
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
PIPELINE_PATH = REPO_ROOT / "tater_voice" / "voice_pipeline" / "__init__.py"


def _process_voice_turn_node() -> ast.AsyncFunctionDef:
    tree = ast.parse(PIPELINE_PATH.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_process_voice_turn":
            return node
    raise AssertionError("_process_voice_turn was not found")


class VoiceTranscriptFilterTests(unittest.TestCase):
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
