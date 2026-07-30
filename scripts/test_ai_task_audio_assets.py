#!/usr/bin/env python3
from __future__ import annotations

import ast
import tempfile
import types
import unittest
from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import FileResponse


def _load_audio_response_helper(root: Path):
    source_path = Path(__file__).resolve().parents[1] / "tateros_app.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    selected = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_ai_task_background_audio_response"
    ]
    if len(selected) != 1:
        raise RuntimeError("Missing AI Task background audio response helper.")

    module = types.ModuleType("test_ai_task_audio_response")
    module.__dict__.update(
        {
            "Path": Path,
            "HTTPException": HTTPException,
            "FileResponse": FileResponse,
            "mimetypes": __import__("mimetypes"),
            "agent_lab_path": lambda *parts: root.joinpath(*parts),
        }
    )
    compiled = ast.Module(body=selected, type_ignores=[])
    ast.fix_missing_locations(compiled)
    exec(compile(compiled, str(source_path), "exec"), module.__dict__)
    return module._ai_task_background_audio_response


class AiTaskAudioAssetTests(unittest.TestCase):
    def test_serves_only_supported_files_inside_agent_lab_audio_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            presets = root / "ai_task" / "background_audio" / "presets"
            presets.mkdir(parents=True)
            audio_path = presets / "morning.wav"
            audio_path.write_bytes(b"RIFF\x00\x00\x00\x00WAVE")
            response_for = _load_audio_response_helper(root)

            response = response_for("presets", "morning.wav")
            self.assertIsInstance(response, FileResponse)
            self.assertEqual(Path(response.path).resolve(), audio_path.resolve())
            self.assertIn(response.media_type, {"audio/wav", "audio/x-wav"})

            with self.assertRaises(HTTPException) as traversal:
                response_for("presets", "../morning.wav")
            self.assertEqual(traversal.exception.status_code, 404)

            with self.assertRaises(HTTPException) as unsupported:
                response_for("presets", "notes.txt")
            self.assertEqual(unsupported.exception.status_code, 404)

            with self.assertRaises(HTTPException) as unknown_kind:
                response_for("private", "morning.wav")
            self.assertEqual(unknown_kind.exception.status_code, 404)


if __name__ == "__main__":
    unittest.main()
