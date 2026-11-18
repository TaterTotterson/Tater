from __future__ import annotations

import asyncio
import pathlib
import sys
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import home, native_live_settings as live, settings, wake_verifier


class WakeVerifierTimingSettingsTests(unittest.TestCase):
    def setUp(self):
        self.rows = {
            live.SETTINGS_HASH_KEY: {
                live.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
                live.UNIFIED_WAKE_SETTINGS_MIGRATION_KEY: "true",
                "wake_verifier_window_ms": "1000",
                "wake_verifier_timeout_ms": "500",
                "volume_percent": "63",
            }
        }
        self.redis = mock.Mock()
        self.redis.hgetall.side_effect = lambda key: dict(self.rows.get(key, {}))
        self.redis.hget.side_effect = lambda key, field: self.rows.get(key, {}).get(
            field
        )
        self.redis.scan_iter.return_value = []
        self.redis.hset.side_effect = lambda key, mapping: self.rows.setdefault(
            key, {}
        ).update(mapping)
        patch = mock.patch.object(live, "redis_client", self.redis)
        patch.start()
        self.addCleanup(patch.stop)

    def test_partial_timing_save_preserves_wait_and_other_settings(self):
        result = live.save_wake_verifier_timings({"wake_verifier_window_ms": 2000})
        self.assertEqual(result["changed_keys"], ["wake_verifier_window_ms"])
        self.redis.hset.assert_called_once_with(
            live.SETTINGS_HASH_KEY, mapping={"wake_verifier_window_ms": "2000"}
        )
        self.assertEqual(live.settings_snapshot()["wake_verifier_timeout_ms"], 500)
        self.assertEqual(live.settings_snapshot()["volume_percent"], 63)
        self.redis.hset.reset_mock()
        self.assertEqual(
            live.save_wake_verifier_timings({"wake_verifier_window_ms": 2000})[
                "updated_count"
            ],
            0,
        )
        self.redis.hset.assert_not_called()

    def test_invalid_durations_are_rejected_before_any_write(self):
        for key in ("wake_verifier_window_ms", "wake_verifier_timeout_ms"):
            for value in (
                None,
                True,
                499,
                2001,
                "bad",
                1000.5,
                float("nan"),
                float("inf"),
            ):
                with (
                    self.subTest(key=key, value=value),
                    self.assertRaisesRegex(ValueError, "500 and 2000 ms"),
                ):
                    live.save_wake_verifier_timings({key: value})
        self.redis.hset.assert_not_called()

    def test_saved_values_reach_shared_server_and_reconnecting_satellite_payloads(self):
        for window, wait in ((500, 500), (2000, 1500)):
            live.save_wake_verifier_timings(
                {"wake_verifier_window_ms": window, "wake_verifier_timeout_ms": wait}
            )
            # Stale device/family copies must not override the global controls.
            self.rows[live.settings_hash_key("native:test")] = {
                "wake_verifier_window_ms": "700",
                "wake_verifier_timeout_ms": "800",
            }
            with mock.patch.object(
                live,
                "_shared_wake_raw_settings",
                return_value={"wake_verifier_window_ms": "600"},
            ):
                snapshots = [live.shared_wake_settings_snapshot()]
                for board in ("satellite1", "voice-pe", "s3-box", "respeaker", "crown"):
                    snapshots.append(
                        live.firmware_settings_snapshot("native:test", board=board)
                    )
                for snapshot in snapshots:
                    self.assertEqual(snapshot["wake_verifier_window_ms"], window)
                    self.assertEqual(snapshot["wake_verifier_timeout_ms"], wait)

    def test_form_explains_and_exposes_distinct_millisecond_ranges(self):
        live.save_wake_verifier_timings(
            {"wake_verifier_window_ms": 2000, "wake_verifier_timeout_ms": 1000}
        )
        with (
            mock.patch.object(
                home.esphome_runtime, "voice_metrics_snapshot", return_value={}
            ),
            mock.patch.object(
                home.esphome_runtime, "load_satellite_registry", return_value=[]
            ),
        ):
            form = home._wake_verifier_item_form({"clients": {}})
        fields = {
            field["key"]: field
            for section in form["sections"]
            for field in section["fields"]
        }
        for key, default, value in (
            ("wake_verifier_window_ms", 1000, 2000),
            ("wake_verifier_timeout_ms", 500, 1000),
        ):
            field = fields[key]
            self.assertEqual(
                (field["min"], field["max"], field["step"]), (500, 2000, 100)
            )
            self.assertEqual(field["format"], "milliseconds")
            self.assertEqual(field["default"], default)
            self.assertEqual(field["value"], value)
        self.assertNotIn("500 ms deadline", form["subtitle"])

    def test_save_action_persists_before_broadcasting(self):
        with (
            mock.patch.object(
                home.esphome_firmware, "handle_runtime_action", return_value=None
            ),
            mock.patch.object(
                home.esphome_speaker_id, "handle_runtime_action", return_value=None
            ),
            mock.patch.object(
                home.esphome_emotion_id, "handle_runtime_action", return_value=None
            ),
            mock.patch.object(
                home.esphome_settings,
                "save_settings_values",
                return_value={"updated_count": 0},
            ),
            mock.patch.object(
                home.native_satellite, "push_live_settings", new=lambda: "push-all"
            ),
            mock.patch.object(
                home.native_satellite,
                "run_on_runtime_loop",
                side_effect=lambda *a, **k: (
                    {"count": 2}
                    if self.rows[live.SETTINGS_HASH_KEY]["wake_verifier_window_ms"]
                    == "2000"
                    else self.fail("broadcast before save")
                ),
            ) as push,
            mock.patch.object(home, "_wake_verifier_item_form", return_value={}),
            mock.patch.object(
                home, "_native_satellite_status_snapshot", return_value={}
            ),
        ):
            result = home.handle_runtime_action(
                action="voice_wake_verifier_save",
                payload={
                    "values": {
                        settings.VOICE_WAKE_VERIFIER_MODE_KEY: "enforce",
                        settings.VOICE_WAKE_VERIFIER_THRESHOLD_KEY: 0.85,
                        "wake_verifier_window_ms": 2000,
                        "wake_verifier_timeout_ms": 1000,
                    }
                },
            )
        self.assertTrue(result["ok"])
        self.assertEqual(result["updated_count"], 2)
        self.assertEqual(
            live.shared_wake_settings_snapshot()["wake_verifier_timeout_ms"], 1000
        )
        push.assert_called_once_with("push-all", timeout=10.0)


class WakeVerifierTimingRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_transcript_allows_listening_and_wait_reserves_transport_time(
        self,
    ):
        from tater_voice import voice_pipeline as vp

        packet = wake_verifier.build_packet(b"\0\0" * 32000, request_id=4, enforce=True)
        original_wait = asyncio.wait_for
        with (
            mock.patch.object(
                live,
                "shared_wake_settings_snapshot",
                return_value={
                    "wake_word": "hey_tater",
                    "wake_verifier_timeout_ms": 1000,
                },
            ),
            mock.patch.object(
                vp, "_selected_stt_backend", return_value="parakeet_onnx"
            ),
            mock.patch.object(
                vp, "_resolve_stt_backend", return_value=("parakeet_onnx", "")
            ),
            mock.patch.object(
                vp,
                "_native_transcribe_wake_audio_bytes",
                new=mock.AsyncMock(return_value="  "),
            ) as stt,
            mock.patch.object(
                wake_verifier.asyncio, "wait_for", wraps=original_wait
            ) as wait,
        ):
            result = await wake_verifier.verify_packet(packet)
        self.assertTrue(result["accepted"])
        self.assertFalse(result["available"])
        self.assertEqual(result["reason"], "stt_no_text_fail_open")
        self.assertEqual((result["timeout_ms"], result["stt_budget_ms"]), (1000, 750))
        self.assertEqual(wait.call_args.kwargs["timeout"], 0.75)
        self.assertEqual(len(stt.call_args.kwargs["audio_bytes"]), 64000)

    async def test_provider_timeout_still_allows_listening(self):
        from tater_voice import voice_pipeline as vp

        with (
            mock.patch.object(
                live,
                "shared_wake_settings_snapshot",
                return_value={
                    "wake_word": "hey_tater",
                    "wake_verifier_timeout_ms": 1500,
                },
            ),
            mock.patch.object(
                vp, "_selected_stt_backend", return_value="parakeet_onnx"
            ),
            mock.patch.object(
                vp, "_resolve_stt_backend", return_value=("parakeet_onnx", "")
            ),
            mock.patch.object(
                wake_verifier,
                "_verify_pcm",
                new=mock.AsyncMock(side_effect=TimeoutError),
            ),
        ):
            result = await wake_verifier.verify_packet(
                wake_verifier.build_packet(b"\0\0" * 16000, request_id=5, enforce=True)
            )
        self.assertTrue(result["accepted"])
        self.assertFalse(result["available"])
        self.assertEqual(result["reason"], "server_timeout_fail_open")
        self.assertEqual(result["stt_ms"], 1250)


if __name__ == "__main__":
    unittest.main()
