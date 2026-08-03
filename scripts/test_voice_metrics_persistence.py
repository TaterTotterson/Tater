#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
import pathlib
import sys
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import voice_pipeline as vp  # noqa: E402


class _FakeRedis:
    def __init__(self, value: str | None = None) -> None:
        self.value = value
        self.expirations: list[int] = []

    def get(self, key: str) -> str | None:
        self.last_get_key = key
        return self.value

    def set(self, key: str, value: str) -> bool:
        self.last_set_key = key
        self.value = value
        return True

    def expire(self, key: str, seconds: int) -> bool:
        self.last_expire_key = key
        self.expirations.append(int(seconds))
        return True


class VoiceMetricsPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        with vp._VOICE_METRICS_LOCK:
            self.saved_metrics = copy.deepcopy(vp._VOICE_METRICS)
            self.saved_loaded = vp._VOICE_METRICS_LOADED
            self._reset_module_state()

    def tearDown(self) -> None:
        with vp._VOICE_METRICS_LOCK:
            vp._VOICE_METRICS.clear()
            vp._VOICE_METRICS.update(self.saved_metrics)
            vp._VOICE_METRICS_LOADED = self.saved_loaded

    def _reset_module_state(self) -> None:
        vp._VOICE_METRICS.clear()
        vp._VOICE_METRICS.update(copy.deepcopy(vp._VOICE_METRICS_TEMPLATE))
        vp._VOICE_METRICS_LOADED = False

    def test_wake_verifier_metrics_survive_a_reload(self) -> None:
        fake_redis = _FakeRedis()
        result = {
            "accepted": False,
            "available": True,
            "reason": "transcript_mismatch",
            "transcript": "unrelated speech",
            "stt_engine": "parakeet_onnx",
        }
        with (
            mock.patch.object(vp, "redis_client", fake_redis),
            mock.patch.object(vp, "_now", return_value=1_000.0),
        ):
            vp._voice_metrics_record_wake_verification("native:test", result)
            first_payload = json.loads(str(fake_redis.value))
            self._reset_module_state()
            restored = vp._voice_metrics_snapshot()

        self.assertEqual(first_payload["wake_verifier_checks"], 1)
        self.assertEqual(first_payload["wake_verifier_rejections"], 1)
        self.assertEqual(restored["wake_verifier_checks"], 1)
        self.assertEqual(restored["devices"]["native:test"]["wake_verifier_checks"], 1)
        self.assertEqual(restored["devices"]["native:test"]["wake_verifier_rejections"], 1)
        self.assertEqual(
            restored["devices"]["native:test"]["wake_verifier_last"]["transcript"],
            "unrelated speech",
        )
        self.assertTrue(fake_redis.expirations)
        self.assertLessEqual(fake_redis.expirations[-1], vp.VOICE_METRICS_RETENTION_SECONDS)

    def test_expired_statistics_begin_a_fresh_period(self) -> None:
        saved = copy.deepcopy(vp._VOICE_METRICS_TEMPLATE)
        saved.update(
            {
                "period_started_ts": 100.0,
                "period_expires_ts": 200.0,
                "sessions_started": 7,
                "wake_verifier_checks": 5,
                "devices": {"native:test": {"sessions_started": 7, "wake_verifier_checks": 5}},
            }
        )
        fake_redis = _FakeRedis(json.dumps(saved))
        with (
            mock.patch.object(vp, "redis_client", fake_redis),
            mock.patch.object(vp, "_now", return_value=201.0),
        ):
            snapshot = vp._voice_metrics_snapshot()

        self.assertEqual(snapshot["sessions_started"], 0)
        self.assertEqual(snapshot["wake_verifier_checks"], 0)
        self.assertEqual(snapshot["devices"], {})
        self.assertEqual(snapshot["period_started_ts"], 201.0)
        self.assertEqual(
            snapshot["period_expires_ts"],
            201.0 + vp.VOICE_METRICS_RETENTION_SECONDS,
        )

    def test_wake_reset_preserves_other_voice_metrics(self) -> None:
        fake_redis = _FakeRedis()
        with (
            mock.patch.object(vp, "redis_client", fake_redis),
            mock.patch.object(vp, "_now", return_value=2_000.0),
        ):
            vp._voice_metrics_record_session_start(
                selector="native:test",
                continued_chat_reopen=False,
                stt_fallback_used=False,
                tts_fallback_used=False,
            )
            vp._voice_metrics_record_wake_verification(
                "native:test",
                {"accepted": True, "available": False, "reason": "server_timeout_fail_open"},
            )
            vp._voice_metrics_reset_wake_verifier()
            snapshot = vp._voice_metrics_snapshot()

        self.assertEqual(snapshot["sessions_started"], 1)
        self.assertEqual(snapshot["devices"]["native:test"]["sessions_started"], 1)
        self.assertEqual(snapshot["wake_verifier_checks"], 0)
        self.assertEqual(snapshot["wake_verifier_fail_open"], 0)
        self.assertEqual(snapshot["devices"]["native:test"]["wake_verifier_last"], {})

    def test_full_reset_clears_every_metric_and_restarts_retention(self) -> None:
        fake_redis = _FakeRedis()
        with (
            mock.patch.object(vp, "redis_client", fake_redis),
            mock.patch.object(vp, "_now", side_effect=[3_000.0] * 20 + [4_000.0] * 20),
        ):
            vp._voice_metrics_record_session_start(
                selector="native:test",
                continued_chat_reopen=False,
                stt_fallback_used=False,
                tts_fallback_used=False,
            )
            vp._voice_metrics_reset_all()
            snapshot = vp._voice_metrics_snapshot()

        self.assertEqual(snapshot["sessions_started"], 0)
        self.assertEqual(snapshot["devices"], {})
        self.assertEqual(snapshot["period_started_ts"], 3_000.0)
        self.assertEqual(snapshot["retention_days"], 30)


if __name__ == "__main__":
    unittest.main()
