#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class EmotionIdCachePathTests(unittest.TestCase):
    def test_speechbrain_wav2vec2_cache_is_placed_beside_the_model(self) -> None:
        source = (REPO_ROOT / "tater_voice" / "emotion_id.py").read_text(encoding="utf-8")

        self.assertIn('legacy_cache_path = "save_path: wav2vec2_checkpoints"', source)
        self.assertIn('wav2vec2_cache = savedir / "wav2vec2_checkpoints"', source)
        self.assertIn('wav2vec2_cache.mkdir(parents=True, exist_ok=True)', source)
        self.assertIn('f"save_path: {json.dumps(str(wav2vec2_cache))}"', source)


if __name__ == "__main__":
    unittest.main()
