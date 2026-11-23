from __future__ import annotations

import contextlib
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
helpers = types.ModuleType("helpers")
helpers.redis_client = object()
sys.modules.setdefault("helpers", helpers)
integration = types.ModuleType("tateros.integration_store")
integration.huggingface_environment = lambda *_args, **_kwargs: {}
integration.huggingface_token = lambda *_args, **_kwargs: ""
sys.modules.setdefault("tateros.integration_store", integration)

from tater_voice import emotion_id, identity_backends, speaker_id


class VoicePipeline:
    @staticmethod
    def _text(value):
        return value.decode() if isinstance(value, bytes) else str(value or "")

    @classmethod
    def _lower(cls, value):
        return cls._text(value).lower()

    @staticmethod
    def _as_bool(value, default=False):
        return default if value is None else str(value).lower() in {"1", "true", "yes"}

    @staticmethod
    def _as_float(value, default=0, minimum=None, maximum=None):
        number = float(default if value is None else value)
        if minimum is not None:
            number = max(number, minimum)
        return min(number, maximum) if maximum is not None else number


class Redis:
    def __init__(self, row):
        self.row = dict(row)
        self.writes = 0

    def hgetall(self, _key):
        return dict(self.row)

    def hset(self, _key, mapping):
        self.row.update(mapping)
        self.writes += 1


class IdentityMigrationTests(unittest.TestCase):
    def test_old_model_settings_reset_scores_and_preserve_enable_preferences(self):
        for module, prefix, old in (
            (speaker_id, "SPEAKER", "speechbrain/spkrec-ecapa-voxceleb"),
            (emotion_id, "EMOTION", "speechbrain/emotion-recognition-wav2vec2-IEMOCAP"),
        ):
            client = Redis({f"VOICE_{prefix}_ID_MODEL_SOURCE": old, f"VOICE_{prefix}_ID_ENABLED": "true"})
            with mock.patch.object(module, "redis_client", client), mock.patch.object(module, "_vp", return_value=VoicePipeline):
                migrated = module._settings()
                self.assertEqual(migrated[f"VOICE_{prefix}_ID_MODEL_SOURCE"], module._model_source())
                self.assertEqual(migrated[f"VOICE_{prefix}_ID_ENABLED"], "true")
                module._settings()
                self.assertEqual(client.writes, 1)

    def test_old_speaker_profiles_are_not_reinterpreted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "profiles.json").write_text(json.dumps({"version": 1, "speakers": [{"name": "Old voice"}]}))
            with mock.patch.object(speaker_id, "SPEAKER_ID_AGENT_LABS_ROOT", root), \
                    mock.patch.object(speaker_id, "SPEAKER_ID_MODEL_ROOT", root / "models"), \
                    mock.patch.object(speaker_id, "SPEAKER_ID_PROFILES_PATH", root / "profiles-w2v-bert2.json"):
                self.assertEqual(speaker_id._load_profiles()["speakers"], [])

    def test_checkpoint_revision_is_part_of_embedding_compatibility(self):
        with mock.patch.object(speaker_id, "_vp", return_value=VoicePipeline):
            model = speaker_id.speaker_embedding_model_metadata(dimensions=256)
            changed = dict(model, model_revision="different")
            self.assertNotEqual(speaker_id._embedding_model_signature(model), speaker_id._embedding_model_signature(changed))

    def test_cache_cleanup_does_not_remove_unrelated_models(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old = root / "speechbrain-spkrec-ecapa-voxceleb"
            old.mkdir()
            keep = root / "my-other-model"
            keep.mkdir()
            identity_backends.retire_legacy_cache(root, "speaker_id")
            self.assertFalse(old.exists())
            self.assertTrue(keep.exists())

    def test_weight_changes_invalidate_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            weight = Path(directory) / "model.pt"
            weight.write_bytes(b"expected")
            digest = hashlib.sha256(b"expected").hexdigest()
            identity_backends._verify(weight, digest)
            weight.write_bytes(b"corrupted")
            with self.assertRaisesRegex(RuntimeError, "checksum"):
                identity_backends._verify(weight, digest)


class EmotionTests(unittest.TestCase):
    def test_short_audio_retains_model_version_for_hub_routing(self):
        with mock.patch.object(emotion_id, "_vp", return_value=VoicePipeline), \
                mock.patch.object(emotion_id, "spud_link_should_use_hub", return_value=False), \
                mock.patch.object(emotion_id, "emotion_id_enabled", return_value=True), \
                mock.patch.object(emotion_id, "_min_speech_seconds", return_value=1.0):
            result = emotion_id.classify_emotion_for_audio(audio_bytes=b"pcm", audio_format={}, speech_s=0.3)
        self.assertEqual(result["reason"], "too_short")
        self.assertEqual(result["model_revision"], identity_backends.EMOTION_REVISION)

    def test_backend_reports_funasr_cpu_fallback(self):
        import torch
        engine = types.SimpleNamespace(model=torch.nn.Linear(2, 2))
        funasr = types.ModuleType("funasr")
        funasr.AutoModel = mock.Mock(return_value=engine)
        with tempfile.TemporaryDirectory() as directory, \
                mock.patch.dict(sys.modules, {"funasr": funasr}), \
                mock.patch.object(identity_backends, "_files"), \
                mock.patch.object(identity_backends, "_verify"):
            backend = identity_backends.EmotionBackend(Path(directory), "mps")
        self.assertEqual(backend.device, "cpu")

    def classify(self, scores, *, threshold=0.6, neutral=True):
        with contextlib.ExitStack() as stack:
            for name, value in (("_vp", VoicePipeline), ("spud_link_should_use_hub", False),
                                ("emotion_id_enabled", True), ("_min_speech_seconds", 1.0),
                                ("_model_state", (True, "")), ("prompt_hint_enabled", True),
                                ("include_neutral_in_prompt", neutral), ("_confidence_threshold", threshold)):
                stack.enter_context(mock.patch.object(emotion_id, name, return_value=value))
            stack.enter_context(mock.patch.object(emotion_id, "_save_last_result"))
            stack.enter_context(mock.patch.object(emotion_id, "_ENGINE", types.SimpleNamespace(classify=lambda _: scores)))
            return emotion_id.classify_emotion_for_audio(audio_bytes=b"\0\0" * 32000,
                                                         audio_format={"rate": 16000}, speech_s=2.0)

    def test_all_seven_categories_are_recognized(self):
        for label in identity_backends.EMOTION_LABELS:
            result = self.classify({"中文/" + label: 0.8, "unknown": 0.2})
            self.assertEqual(result["emotion"], label)
            self.assertTrue(result["detected"])
            self.assertTrue(result["prompt_hint"])

    def test_unknown_and_other_are_not_forced_into_an_emotion(self):
        for label in ("unknown", "other"):
            result = self.classify({label: 0.8, "angry": 0.2})
            self.assertFalse(result["detected"])
            self.assertEqual(result["prompt_hint"], "")

    def test_low_confidence_and_disabled_neutral_do_not_add_hints(self):
        self.assertFalse(self.classify({"sad": 0.4, "neutral": 0.3, "unknown": 0.3})["prompt_hint_enabled"])
        self.assertFalse(self.classify({"neutral": 0.9, "sad": 0.1}, neutral=False)["prompt_hint_enabled"])

    def test_old_hub_emotion_results_require_a_hub_update(self):
        with mock.patch.object(emotion_id, "spud_link_should_use_hub", return_value=True), \
                mock.patch.object(emotion_id, "spud_link_allow_local_fallback", return_value=False), \
                mock.patch.object(emotion_id, "spud_link_request_json", return_value={"result": {"emotion": "sad"}}):
            with self.assertRaisesRegex(RuntimeError, "Update the paired Spud Hub"):
                emotion_id.classify_emotion_for_audio(audio_bytes=b"pcm", audio_format={}, speech_s=2)


class AudioAndSpeakerTests(unittest.TestCase):
    def test_stereo_48khz_audio_is_downmixed_and_resampled(self):
        import numpy as np
        pcm = np.tile(np.array([1000, -1000], dtype="<i2"), 48000).tobytes()
        waveform = identity_backends.pcm_waveform(pcm, {"rate": 48000, "channels": 2})
        self.assertEqual(waveform.shape, (16000,))
        self.assertTrue(np.allclose(waveform, 0))

    def test_incomplete_pcm_frames_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "incomplete PCM"):
            identity_backends.pcm_waveform(b"\0\0\0", {"channels": 2})

    def test_speaker_checkpoint_must_load_every_parameter_and_produce_unit_embeddings(self):
        import numpy as np
        import torch
        from transformers import SeamlessM4TFeatureExtractor, Wav2Vec2BertConfig
        config = Wav2Vec2BertConfig(hidden_size=32, intermediate_size=64, num_hidden_layers=1,
                                   num_attention_heads=4, conv_depthwise_kernel_size=3,
                                   position_embeddings_type="relative_key", apply_spec_augment=False)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = identity_backends.speaker_core(config).state_dict()
            checkpoint = root / identity_backends.SPEAKER_CHECKPOINT
            torch.save({"modules": {"spk_model": state}}, checkpoint)
            with mock.patch.object(identity_backends, "_files"), mock.patch.object(identity_backends, "_verify"), \
                    mock.patch.object(Wav2Vec2BertConfig, "from_pretrained", return_value=config), \
                    mock.patch.object(SeamlessM4TFeatureExtractor, "from_pretrained", return_value=SeamlessM4TFeatureExtractor()):
                engine = identity_backends.SpeakerBackend(root, "cpu")
                vector = engine.embed(np.random.default_rng(4).normal(0, 0.1, 32000).astype("float32"))
                self.assertEqual(vector.shape, (256,))
                self.assertAlmostEqual(float(vector.norm()), 1.0, places=5)
                state.pop("bottleneck.weight")
                torch.save({"modules": {"spk_model": state}}, checkpoint)
                with self.assertRaisesRegex(RuntimeError, "Missing key"):
                    identity_backends.SpeakerBackend(root, "cpu")


if __name__ == "__main__":
    unittest.main()
