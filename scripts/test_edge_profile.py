from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
FORBIDDEN_REQUIREMENTS = {
    "accelerate",
    "deepface",
    "faster-whisper",
    "onnx-asr",
    "onnxruntime",
    "opencv-python",
    "piper-tts",
    "pocket-tts",
    "python-olm",
    "pykokoro",
    "redislite",
    "silero-vad",
    "speechbrain",
    "tensorflow",
    "tf-keras",
    "torch",
    "transformers",
    "vosk",
}


def requirement_name(line: str) -> str:
    token = line.split(";", 1)[0].strip().lower()
    for separator in ("[", "=", "<", ">", "!", "~", " "):
        token = token.split(separator, 1)[0]
    return token.replace("_", "-")


class EdgeProfileTests(unittest.TestCase):
    def test_edge_requirements_exclude_local_model_stacks(self) -> None:
        names = {
            requirement_name(line)
            for line in (ROOT / "requirements-edge.txt").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        self.assertTrue({"fastapi", "uvicorn", "openai", "wyoming", "webrtcvad-wheels"}.issubset(names))
        self.assertEqual(names & FORBIDDEN_REQUIREMENTS, set())

    def test_runtime_profile_aliases_and_override(self) -> None:
        from tater_runtime_profile import remote_only_enabled

        self.assertTrue(remote_only_enabled(environ={"TATER_SETUP_PROFILE": "edge-remote"}))
        self.assertTrue(remote_only_enabled(environ={"TATER_REMOTE_ONLY": "1"}))
        self.assertFalse(
            remote_only_enabled(
                environ={"TATER_SETUP_PROFILE": "edge", "TATER_REMOTE_ONLY": "false"}
            )
        )

    def test_edge_defaults_are_remote_and_lightweight(self) -> None:
        script = "import speech_settings; import tater_voice.voice_pipeline as v; print(speech_settings.DEFAULT_STT_BACKEND, v.DEFAULT_STT_BACKEND, v.DEFAULT_VAD_BACKEND)"
        env = os.environ.copy()
        env["TATER_SETUP_PROFILE"] = "edge"
        env["TATER_REMOTE_ONLY"] = "1"
        env["TATER_RUNTIME_DIR"] = str(ROOT / ".runtime-edge-test")
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "wyoming wyoming webrtc")

    def test_setup_help_lists_edge_profile(self) -> None:
        completed = subprocess.run(
            ["sh", "setup_tater.sh", "--help"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("edge", completed.stdout)


if __name__ == "__main__":
    unittest.main()
