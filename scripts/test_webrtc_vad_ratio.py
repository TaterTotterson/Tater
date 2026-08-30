from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
import unittest


ROOT = Path(__file__).resolve().parents[1]
PIPELINE_SOURCE = (ROOT / "tater_voice" / "voice_pipeline" / "__init__.py").read_text(encoding="utf-8")


def _load_webrtc_backend():
    tree = ast.parse(PIPELINE_SOURCE)
    class_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "WebRtcVadBackend"
    )
    constants = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            continue
        name = node.targets[0].id
        if name.startswith("DEFAULT_WEBRTC_VAD_"):
            constants[name] = ast.literal_eval(node.value)

    def _bounded_number(value, default, *, minimum, maximum, cast):
        try:
            result = cast(value)
        except (TypeError, ValueError):
            result = cast(default)
        return max(minimum, min(maximum, result))

    namespace = {
        "Any": object,
        "Dict": dict,
        "VadBackendBase": object,
        "_as_int": lambda value, default, *, minimum, maximum: _bounded_number(
            value, default, minimum=minimum, maximum=maximum, cast=int
        ),
        "_as_float": lambda value, default, *, minimum, maximum: _bounded_number(
            value, default, minimum=minimum, maximum=maximum, cast=float
        ),
        "_pcm_to_pcm16_mono_16k": lambda audio_bytes, _audio_format, *, ratecv_state: (
            audio_bytes,
            ratecv_state,
        ),
        "importlib": SimpleNamespace(import_module=lambda _name: None),
        **constants,
    }
    module = ast.Module(body=[class_node], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), "<webrtc-vad-backend>", "exec"), namespace)
    return namespace["WebRtcVadBackend"], namespace


WebRtcVadBackend, WEBRTC_NAMESPACE = _load_webrtc_backend()


class _FakeVad:
    def __init__(self, decisions: list[bool]) -> None:
        self._decisions = iter(decisions)

    def is_speech(self, _frame: bytes, _rate: int) -> bool:
        return next(self._decisions)


def _backend(
    *,
    decisions: list[bool],
    min_speech_ratio: float | None = None,
) -> WebRtcVadBackend:
    fake_vad = _FakeVad(decisions)
    fake_module = SimpleNamespace(Vad=lambda _mode: fake_vad)
    config = {
        "webrtc_aggressiveness": 3,
        "webrtc_frame_ms": 30,
    }
    if min_speech_ratio is not None:
        config["webrtc_min_speech_ratio"] = min_speech_ratio
    WEBRTC_NAMESPACE["importlib"] = SimpleNamespace(import_module=lambda _name: fake_module)
    return WebRtcVadBackend(config)


class WebRtcVadRatioTests(unittest.TestCase):
    _FORMAT = {"rate": 16000, "width": 2, "channels": 1}
    _ONE_FRAME = b"\0" * (16000 * 30 // 1000 * 2)
    _TWO_FRAMES = b"\0" * (2 * 16000 * 30 // 1000 * 2)

    def test_default_ratio_mode_preserves_single_frame_detection(self) -> None:
        result = _backend(decisions=[True]).process(
            self._ONE_FRAME,
            self._FORMAT,
        )

        self.assertTrue(result["is_speech"])
        self.assertEqual(result["probability"], 1.0)
        self.assertEqual(result["speech_ratio"], 1.0)

    def test_legacy_mode_keeps_any_positive_frame_behavior(self) -> None:
        result = _backend(decisions=[True, False], min_speech_ratio=0.0).process(
            self._TWO_FRAMES,
            self._FORMAT,
        )

        self.assertTrue(result["is_speech"])
        self.assertEqual(result["probability"], 1.0)
        self.assertEqual(result["speech_ratio"], 0.5)

    def test_default_ratio_mode_rejects_one_positive_frame_in_two(self) -> None:
        result = _backend(decisions=[True, False]).process(
            self._TWO_FRAMES,
            self._FORMAT,
        )

        self.assertFalse(result["is_speech"])
        self.assertEqual(result["probability"], 0.0)
        self.assertEqual(result["speech_ratio"], 0.5)

    def test_default_ratio_mode_accepts_sustained_speech(self) -> None:
        result = _backend(decisions=[True, True]).process(
            self._TWO_FRAMES,
            self._FORMAT,
        )

        self.assertTrue(result["is_speech"])
        self.assertEqual(result["probability"], 1.0)
        self.assertEqual(result["speech_ratio"], 1.0)


if __name__ == "__main__":
    unittest.main()
