import unittest
from pathlib import Path
import sys
import types
from unittest.mock import patch

# These focused routing tests do not need Tater's external API clients.
helpers_stub = types.ModuleType("helpers")
helpers_stub.redis_client = object()
sys.modules.setdefault("helpers", helpers_stub)
integration_store_stub = types.ModuleType("tateros.integration_store")
integration_store_stub.huggingface_environment = lambda *_args, **_kwargs: {}
integration_store_stub.huggingface_token = lambda *_args, **_kwargs: ""
sys.modules.setdefault("tateros.integration_store", integration_store_stub)

from tater_voice import speaker_id


ROOT = Path(__file__).resolve().parents[1]


class SpudLinkRemoteIdentityTests(unittest.TestCase):
    class FakeVoicePipeline:
        DEFAULT_VOICE_SAMPLE_RATE_HZ = 16000
        DEFAULT_VOICE_SAMPLE_WIDTH = 2
        DEFAULT_VOICE_CHANNELS = 1

        @staticmethod
        def _text(value):
            if isinstance(value, bytes):
                return value.decode("utf-8", errors="ignore").strip()
            return str(value or "").strip()

    def test_remote_identity_routes_run_before_local_enable_gates(self):
        speaker_source = (ROOT / "tater_voice" / "speaker_id.py").read_text(encoding="utf-8")
        speaker_match = speaker_source[
            speaker_source.index("def match_speaker_for_audio(") : speaker_source.index("def add_enrollment_sample(")
        ]
        self.assertIn('uses_hub = spud_link_should_use_hub("speaker_id"', speaker_match)
        self.assertIn("if not uses_hub and not speaker_id_enabled()", speaker_match)

        emotion_source = (ROOT / "tater_voice" / "emotion_id.py").read_text(encoding="utf-8")
        function_source = emotion_source[emotion_source.index('spud_link_should_use_hub("emotion_id"') :]
        self.assertLess(
            function_source.index('spud_link_should_use_hub("emotion_id"'),
            function_source.index("if not emotion_id_enabled()"),
        )

    def test_face_id_remote_status_is_effectively_enabled(self):
        source = (ROOT / "face_id_runtime.py").read_text(encoding="utf-8")
        status_source = source[source.index("def status(") : source.index("def settings_payload(")]

        self.assertLess(
            status_source.index('spud_link_should_use_hub("face_id"'),
            status_source.index("enabled = is_enabled(redis_client)"),
        )
        self.assertIn('"enabled": True', status_source)
        self.assertIn('"routed_via": "spud_link"', status_source)

    @staticmethod
    def _speaker(model_source="speechbrain/spkrec-ecapa-voxceleb"):
        model = {
            "provider": "speechbrain",
            "model_source": model_source,
            "distance_metric": "cosine_similarity",
            "embedding_dimensions": 2,
        }
        signature = f"speechbrain|{model_source}|cosine_similarity|2"
        return {
            "id": "speaker_fred",
            "name": "Fred",
            "samples": [
                {
                    "embedding": [1.0, 0.0],
                    "embedding_model": model,
                    "embedding_model_signature": signature,
                }
            ],
        }

    @staticmethod
    def _remote_embedding(model_source="speechbrain/spkrec-ecapa-voxceleb"):
        return {
            "result": {
                "embedding": [0.999, 0.001],
                "model": {
                    "provider": "speechbrain",
                    "model_source": model_source,
                    "distance_metric": "cosine_similarity",
                    "embedding_dimensions": 2,
                },
                "stored": False,
            }
        }

    def test_spudlet_speaker_id_matches_hub_embedding_against_local_profiles(self):
        with (
            patch.object(speaker_id, "_vp", return_value=self.FakeVoicePipeline),
            patch.object(speaker_id, "spud_link_should_use_hub", return_value=True),
            patch.object(speaker_id, "spud_link_request_json", return_value=self._remote_embedding()) as request,
            patch.object(speaker_id, "speaker_id_enabled", return_value=False),
            patch.object(speaker_id, "_estimate_audio_duration_s", return_value=2.0),
            patch.object(speaker_id, "_best_match_enabled", return_value=False),
            patch.object(speaker_id, "_min_speech_seconds", return_value=1.0),
            patch.object(speaker_id, "_match_threshold", return_value=0.68),
            patch.object(speaker_id, "_match_margin", return_value=0.05),
            patch.object(speaker_id, "_all_speakers", return_value=[self._speaker()]),
            patch.object(speaker_id, "_save_last_result"),
            patch.object(speaker_id, "_debug"),
            patch.object(speaker_id, "_log_info"),
        ):
            result = speaker_id.match_speaker_for_audio(
                audio_bytes=b"pcm",
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                speech_s=2.0,
            )

        self.assertTrue(result["matched"])
        self.assertEqual(result["speaker_id"], "speaker_fred")
        self.assertEqual(result["speaker_name"], "Fred")
        self.assertEqual(result["identity_owner"], "This Tater")
        self.assertEqual(result["loaded_on"], "Spud Hub")
        payload = request.call_args.kwargs["payload"]
        self.assertEqual(payload["operation"], "embed")
        self.assertNotIn("speaker_id", payload)
        self.assertNotIn("speakers", payload)
        self.assertNotIn("person_id", payload)
        self.assertNotIn("speech_s", payload)

    def test_spudlet_speaker_enrollment_embeds_on_hub_and_saves_locally(self):
        saved = {}

        def save_speakers(rows):
            saved["rows"] = rows

        local_speaker = {"id": "speaker_fred", "name": "Fred", "samples": []}
        with (
            patch.object(speaker_id, "_vp", return_value=self.FakeVoicePipeline),
            patch.object(speaker_id, "spud_link_should_use_hub", return_value=True),
            patch.object(speaker_id, "spud_link_request_json", return_value=self._remote_embedding()) as request,
            patch.object(speaker_id, "_estimate_audio_duration_s", return_value=3.0),
            patch.object(speaker_id, "_enroll_min_speech_seconds", return_value=2.0),
            patch.object(speaker_id, "_all_speakers", return_value=[local_speaker]),
            patch.object(speaker_id, "_save_speakers", side_effect=save_speakers),
            patch.object(speaker_id, "_debug"),
            patch.object(speaker_id, "_log_info"),
        ):
            result = speaker_id.add_enrollment_sample(
                speaker_id="speaker_fred",
                audio_bytes=b"pcm",
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                speech_s=3.0,
            )

        self.assertEqual(request.call_count, 1)
        self.assertEqual(result["speaker_id"], "speaker_fred")
        self.assertEqual(result["routed_via"], "spud_link")
        sample = saved["rows"][0]["samples"][0]
        self.assertEqual(sample["embedding"], [0.999, 0.001])
        self.assertTrue(sample["embedding_model_signature"])

    def test_spudlet_speaker_id_does_not_mix_incompatible_models(self):
        with (
            patch.object(speaker_id, "_vp", return_value=self.FakeVoicePipeline),
            patch.object(speaker_id, "spud_link_should_use_hub", return_value=True),
            patch.object(
                speaker_id,
                "spud_link_request_json",
                return_value=self._remote_embedding("different/speaker-model"),
            ),
            patch.object(speaker_id, "_estimate_audio_duration_s", return_value=2.0),
            patch.object(speaker_id, "_best_match_enabled", return_value=False),
            patch.object(speaker_id, "_min_speech_seconds", return_value=1.0),
            patch.object(speaker_id, "_all_speakers", return_value=[self._speaker()]),
            patch.object(speaker_id, "_save_last_result"),
            patch.object(speaker_id, "_debug"),
            patch.object(speaker_id, "_log_info"),
        ):
            result = speaker_id.match_speaker_for_audio(
                audio_bytes=b"pcm",
                audio_format={"rate": 16000, "width": 2, "channels": 1},
                speech_s=2.0,
            )

        self.assertFalse(result["matched"])
        self.assertEqual(result["reason"], "no_embeddings")

    def test_existing_local_speaker_samples_gain_compatible_model_metadata(self):
        with patch.object(speaker_id, "_vp", return_value=self.FakeVoicePipeline):
            normalized = speaker_id._normalize_speaker_row(
                {
                    "id": "speaker_fred",
                    "name": "Fred",
                    "samples": [{"embedding": [1.0, 0.0], "speech_s": 3.0}],
                }
            )

        sample = normalized["samples"][0]
        self.assertEqual(
            sample["embedding_model"]["model_source"],
            speaker_id.DEFAULT_SPEAKER_ID_MODEL_SOURCE,
        )
        self.assertTrue(sample["embedding_model_signature"])


if __name__ == "__main__":
    unittest.main()
