#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import sys
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import home, native_live_settings, settings  # noqa: E402


class GlobalWakeVerifierSettingsTests(unittest.TestCase):
    def test_existing_device_values_migrate_to_global_satellite_settings(self) -> None:
        device_prefix = native_live_settings.DEVICE_SETTINGS_HASH_PREFIX
        rows = {
            native_live_settings.SETTINGS_HASH_KEY: {
                "wake_word": "hey_tater",
                "wake_word_url": "",
                "capture_wake_audio": "True",
            },
            f"{device_prefix}native:one": {
                "wake_word": "custom_url",
                "wake_word_url": "http://trainer.test/hey_tater.json",
                "capture_wake_audio": "True",
                "wake_profile_name": "Hey Tater",
            },
            f"{device_prefix}native:two": {
                "wake_word": "custom_url",
                "wake_word_url": "http://trainer.test/hey_tater.json",
                "capture_wake_audio": "False",
                "wake_profile_name": "Hey Tater",
            },
            f"{device_prefix}native:three": {
                "wake_word": "custom_url",
                "wake_word_url": "http://trainer.test/hey_tater.json",
                "capture_wake_audio": "True",
                "wake_profile_name": "Hey Tater",
            },
        }
        fake_redis = mock.Mock()
        fake_redis.hgetall.side_effect = lambda key: rows.get(str(key), {})
        fake_redis.scan_iter.return_value = [key for key in rows if key.startswith(device_prefix)]

        with mock.patch.object(native_live_settings, "redis_client", fake_redis):
            migrated = native_live_settings._global_settings_with_migration()

        self.assertEqual(migrated["wake_word"], "custom_url")
        self.assertEqual(migrated["wake_word_url"], "http://trainer.test/hey_tater.json")
        self.assertEqual(migrated["capture_wake_audio"], "True")
        self.assertEqual(migrated["wake_profile_name"], "Hey Tater")
        self.assertEqual(migrated[native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY], "true")

    def test_global_satellite_settings_override_stale_device_copies(self) -> None:
        global_values = {
            native_live_settings.GLOBAL_SATELLITE_SETTINGS_MIGRATION_KEY: "true",
            "wake_engine": "micro_wake_word",
            "wake_word": "custom_url",
            "wake_word_url": "http://trainer.test/hey_tater.json",
            "continued_chat": "True",
        }
        device_values = {
            "wake_word": "hey_tater",
            "wake_word_url": "",
            "continued_chat": "False",
            "wake_sensitivity": "high",
            "led_color": "#112233",
            "led_listening_animation": "comet",
        }
        fake_redis = mock.Mock()
        fake_redis.hgetall.side_effect = lambda key: (
            device_values if str(key).endswith("native:test-sat") else global_values
        )
        fake_redis.hget.return_value = "observe"

        with mock.patch.object(native_live_settings, "redis_client", fake_redis):
            snapshot = native_live_settings.settings_snapshot("native:test-sat")
            device_field_keys = {
                str(field.get("key") or "")
                for field in native_live_settings.settings_fields("native:test-sat", board="voice-pe")
            }
            global_field_keys = {
                str(field.get("key") or "")
                for section in native_live_settings.global_settings_sections()
                for field in section.get("fields") or []
            }

        self.assertEqual(snapshot["wake_word"], "custom_url")
        self.assertEqual(snapshot["wake_word_url"], "http://trainer.test/hey_tater.json")
        self.assertTrue(snapshot["continued_chat"])
        self.assertEqual(snapshot["wake_sensitivity"], "high")
        self.assertEqual(snapshot["led_color"], "#112233")
        self.assertNotIn("wake_word", device_field_keys)
        self.assertNotIn("continued_chat", device_field_keys)
        self.assertIn("led_color", device_field_keys)
        self.assertIn("led_listening_animation", device_field_keys)
        self.assertIn("wake_word", global_field_keys)
        self.assertIn("continued_chat", global_field_keys)
        self.assertNotIn("led_color", global_field_keys)

    def test_shared_wake_settings_are_split_from_voice_runtime_settings(self) -> None:
        model_sections = native_live_settings.global_model_settings_sections()
        runtime_sections = native_live_settings.global_voice_runtime_settings_sections()

        self.assertEqual([section["label"] for section in model_sections], ["Wake Word", "Trainer Feedback"])
        self.assertEqual([section["label"] for section in runtime_sections], ["Wake Sound & Conversation"])

        model_keys = {
            str(field.get("key") or "")
            for section in model_sections
            for field in section.get("fields") or []
        }
        runtime_keys = {
            str(field.get("key") or "")
            for section in runtime_sections
            for field in section.get("fields") or []
        }
        self.assertIn("wake_word", model_keys)
        self.assertIn("capture_wake_audio", model_keys)
        self.assertIn("continued_chat", runtime_keys)
        self.assertTrue(model_keys.isdisjoint(runtime_keys))

    def test_wake_verification_card_is_below_shared_wake_settings(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        shared_wake_host = 'id="settings-models-wake-satellite-settings"'
        wake_verifier_host = 'id="settings-models-wake-verifier"'

        self.assertIn('getElementById("settings-models-wake-verifier")', app_js)
        self.assertIn(shared_wake_host, app_js)
        self.assertIn(wake_verifier_host, app_js)
        self.assertLess(app_js.index(shared_wake_host), app_js.index(wake_verifier_host))
        self.assertNotIn('id="settings-esphome-runtime-wake-verifier"', app_js)

    def test_led_brightness_is_a_percentage(self) -> None:
        normalized = native_live_settings.normalize_settings({"led_brightness": 255})
        brightness_field = next(
            field
            for field in native_live_settings.settings_fields()
            if field.get("key") == "led_brightness"
        )

        self.assertEqual(normalized["led_brightness"], 100)
        self.assertEqual(brightness_field["min"], 0)
        self.assertEqual(brightness_field["max"], 100)

    def test_global_mode_is_not_duplicated_in_pipeline_sections(self) -> None:
        section_keys = {
            str(field.get("key") or "")
            for section in settings.settings_sections()
            for field in section.get("fields") or []
            if isinstance(field, dict)
        }
        self.assertNotIn(settings.VOICE_WAKE_VERIFIER_MODE_KEY, section_keys)

    def test_internal_endpoint_tuning_is_not_user_configurable(self) -> None:
        voice_pipeline = settings._vp()
        with mock.patch.object(voice_pipeline, "_voice_settings", return_value={}):
            field_keys = {
                str(field.get("key") or "")
                for field in settings.settings_fields()
                if isinstance(field, dict)
            }
            section_labels = {
                str(section.get("label") or "")
                for section in settings.settings_sections()
                if isinstance(section, dict)
            }

        self.assertTrue(settings.VOICE_INTERNAL_TUNING_KEYS.isdisjoint(field_keys))
        self.assertNotIn("Voice Activity Detection", section_labels)
        self.assertNotIn("Listening", section_labels)

    def test_legacy_satellite_discovery_is_removed(self) -> None:
        removed_keys = {
            "VOICE_DISCOVERY_ENABLED",
            "VOICE_DISCOVERY_SCAN_SECONDS",
            "VOICE_DISCOVERY_MDNS_TIMEOUT_S",
        }
        spec_keys = {
            str(field.get("key") or "")
            for field in settings.voice_ui_setting_specs()
            if isinstance(field, dict)
        }
        config = settings._vp()._voice_config_snapshot()

        self.assertTrue(removed_keys.isdisjoint(spec_keys))
        self.assertNotIn("discovery", config)
        self.assertFalse(hasattr(home.esphome_runtime, "discover_once"))
        self.assertFalse(hasattr(home.esphome_runtime, "discovery_stats"))

    def test_removed_user_tuning_overrides_are_cleaned_up(self) -> None:
        fake_redis = mock.Mock()
        fake_redis.hdel.return_value = 4
        voice_pipeline = settings._vp()
        with (
            mock.patch.object(settings, "redis_client", fake_redis),
            mock.patch.object(voice_pipeline, "_invalidate_voice_config_cache") as invalidate_mock,
        ):
            result = settings.cleanup_removed_user_settings()

        deleted_keys = set(fake_redis.hdel.call_args.args[1:])
        self.assertEqual(fake_redis.hdel.call_args.args[0], settings.settings_hash_key())
        self.assertEqual(deleted_keys, settings.REMOVED_USER_SETTING_KEYS)
        self.assertEqual(result["removed_count"], 4)
        invalidate_mock.assert_called_once_with()

    def test_global_mode_overrides_device_value(self) -> None:
        def fake_hgetall(key: str) -> dict[str, str]:
            if key.endswith("native:test-sat"):
                return {"wake_verifier_mode": "enforce", "wake_word": "hey_tater"}
            return {}

        fake_redis = mock.Mock()
        fake_redis.hgetall.side_effect = fake_hgetall
        fake_redis.hget.return_value = "observe"
        with mock.patch.object(native_live_settings, "redis_client", fake_redis):
            snapshot = native_live_settings.settings_snapshot("native:test-sat")

        self.assertEqual(snapshot["wake_verifier_mode"], "observe")

    def test_satellite_settings_do_not_expose_verifier_controls(self) -> None:
        fake_redis = mock.Mock()
        fake_redis.hgetall.return_value = {}
        fake_redis.hget.return_value = "off"
        with mock.patch.object(native_live_settings, "redis_client", fake_redis):
            keys = {str(field.get("key") or "") for field in native_live_settings.settings_fields()}

        self.assertNotIn("wake_verifier_mode", keys)
        self.assertNotIn("wake_verifier_threshold", keys)
        self.assertNotIn("wake_verifier_window_ms", keys)

    def test_result_card_aggregates_device_counters(self) -> None:
        native_status = {
            "clients": {
                "native:test-sat": {
                    "connected": True,
                    "device_name": "Test Satellite",
                    "wake_verifier": {
                        "count": 2,
                        "rejections": 1,
                        "last": {
                            "accepted": False,
                            "available": True,
                            "transcript": "unrelated speech",
                            "score": 0.2,
                            "stt_ms": 24.5,
                            "stt_engine": "faster_whisper",
                        },
                    },
                    "last_status": {
                        "wake_engine": {
                            "verifier": {
                                "completed": 3,
                                "rejections": 2,
                                "fail_open": 1,
                                "last_reason": "transcript_mismatch",
                            }
                        }
                    },
                }
            }
        }
        with (
            mock.patch.object(settings, "wake_verifier_mode", return_value="observe"),
            mock.patch(
                "tater_voice.voice_pipeline._selected_stt_backend",
                return_value="faster_whisper",
            ),
            mock.patch.object(home.esphome_runtime, "voice_metrics_snapshot", return_value={}),
        ):
            card = home._wake_verifier_item_form(native_status)

        self.assertEqual(card["group"], "wake_verifier")
        self.assertEqual(card["sections"][0]["fields"][0]["value"], "observe")
        self.assertIn("3 checks", card["sections"][0]["fields"][1]["value"])
        self.assertEqual(
            card["sections"][0]["fields"][2]["value"],
            "faster_whisper",
        )
        rows = card["sections"][1]["fields"][0]["rows"]
        self.assertEqual(rows[0]["satellite"], "Test Satellite")
        self.assertEqual(rows[0]["accepted"], 1)
        self.assertEqual(rows[0]["rejected"], 2)
        self.assertEqual(rows[0]["fail_open"], 1)
        self.assertEqual(rows[0]["stt_engine"], "faster_whisper")

    def test_result_card_prefers_persisted_wake_verifier_counters(self) -> None:
        native_status = {
            "clients": {
                "native:test-sat": {
                    "connected": True,
                    "device_name": "Test Satellite",
                    "wake_verifier": {"count": 99, "rejections": 88, "last": {}},
                    "last_status": {"wake_engine": {"verifier": {"completed": 99, "rejections": 88}}},
                }
            }
        }
        metrics = {
            "period_started_ts": 100.0,
            "retention_days": 30,
            "devices": {
                "native:test-sat": {
                    "wake_verifier_checks": 4,
                    "wake_verifier_rejections": 1,
                    "wake_verifier_fail_open": 1,
                    "wake_verifier_last": {
                        "accepted": True,
                        "available": False,
                        "transcript": "",
                        "score": 0.0,
                        "stt_ms": 500.0,
                        "stt_engine": "parakeet_onnx",
                        "recorded_at": 120.0,
                    },
                }
            },
        }
        with (
            mock.patch.object(settings, "wake_verifier_mode", return_value="enforce"),
            mock.patch("tater_voice.voice_pipeline._selected_stt_backend", return_value="parakeet_onnx"),
            mock.patch.object(home.esphome_runtime, "voice_metrics_snapshot", return_value=metrics),
        ):
            card = home._wake_verifier_item_form(native_status)

        self.assertIn("4 checks", card["sections"][0]["fields"][1]["value"])
        row = card["sections"][1]["fields"][0]["rows"][0]
        self.assertEqual(row["accepted"], 3)
        self.assertEqual(row["rejected"], 1)
        self.assertEqual(row["fail_open"], 1)
        self.assertEqual(row["last_result"], "Fail-open")
        self.assertEqual(card["reset_action"], "voice_wake_verifier_stats_reset")

    def test_persisted_wake_stats_remain_visible_while_satellite_is_offline(self) -> None:
        metrics = {
            "period_started_ts": 100.0,
            "retention_days": 30,
            "devices": {
                "native:offline": {
                    "wake_verifier_checks": 6,
                    "wake_verifier_rejections": 2,
                    "wake_verifier_fail_open": 0,
                    "wake_verifier_last": {},
                }
            },
        }
        with (
            mock.patch.object(settings, "wake_verifier_mode", return_value="observe"),
            mock.patch("tater_voice.voice_pipeline._selected_stt_backend", return_value="faster_whisper"),
            mock.patch.object(home.esphome_runtime, "voice_metrics_snapshot", return_value=metrics),
            mock.patch.object(
                home.esphome_runtime,
                "load_satellite_registry",
                return_value=[
                    {
                        "selector": "native:offline",
                        "name": "Offline Satellite",
                        "source": "tater_native",
                    }
                ],
            ),
        ):
            card = home._wake_verifier_item_form({"clients": {}})

        row = card["sections"][1]["fields"][0]["rows"][0]
        self.assertEqual(row["satellite"], "Offline Satellite")
        self.assertEqual(row["status"], "Offline")
        self.assertEqual(row["checks"], 6)
        self.assertEqual(row["rejected"], 2)

    def test_cleared_wake_stats_hide_offline_satellites_but_keep_connected_satellites(self) -> None:
        cleared_metrics = {
            "period_started_ts": 100.0,
            "retention_days": 30,
            "devices": {
                "native:offline": {
                    "wake_verifier_checks": 0,
                    "wake_verifier_rejections": 0,
                    "wake_verifier_fail_open": 0,
                    "wake_verifier_last": {},
                    "voice_sessions": 12,
                },
                "native:connected": {
                    "wake_verifier_checks": 0,
                    "wake_verifier_rejections": 0,
                    "wake_verifier_fail_open": 0,
                    "wake_verifier_last": {},
                },
            },
        }
        native_status = {
            "clients": {
                "native:offline": {
                    "connected": False,
                    "device_name": "Offline Satellite",
                    "wake_verifier": {"count": 0, "rejections": 0, "last": {}},
                },
                "native:connected": {
                    "connected": True,
                    "device_name": "Connected Satellite",
                }
            }
        }
        with (
            mock.patch.object(settings, "wake_verifier_mode", return_value="observe"),
            mock.patch("tater_voice.voice_pipeline._selected_stt_backend", return_value="faster_whisper"),
            mock.patch.object(home.esphome_runtime, "voice_metrics_snapshot", return_value=cleared_metrics),
            mock.patch.object(
                home.esphome_runtime,
                "load_satellite_registry",
                return_value=[
                    {"selector": "native:offline", "name": "Offline Satellite", "source": "tater_native"},
                    {"selector": "native:connected", "name": "Connected Satellite", "source": "tater_native"},
                ],
            ),
        ):
            card = home._wake_verifier_item_form(native_status)

        rows = card["sections"][1]["fields"][0]["rows"]
        self.assertEqual([row["satellite"] for row in rows], ["Connected Satellite"])
        self.assertEqual(rows[0]["status"], "No verifier firmware")

    def test_save_action_broadcasts_to_all_connected_satellites(self) -> None:
        with (
            mock.patch.object(home.esphome_firmware, "handle_runtime_action", return_value=None),
            mock.patch.object(home, "_runtime_status_with_native", return_value={}),
            mock.patch.object(home.esphome_speaker_id, "handle_runtime_action", return_value=None),
            mock.patch.object(home.esphome_emotion_id, "handle_runtime_action", return_value=None),
            mock.patch.object(home.esphome_settings, "save_settings_values", return_value={"updated_count": 1}),
            mock.patch.object(home.native_satellite, "push_live_settings", new=lambda: "push-all"),
            mock.patch.object(home.native_satellite, "run_on_runtime_loop", return_value={"count": 4}) as run_mock,
            mock.patch.object(home, "_native_satellite_status_snapshot", return_value={"clients": {}}),
        ):
            result = home.handle_runtime_action(
                action="voice_wake_verifier_save",
                payload={"values": {settings.VOICE_WAKE_VERIFIER_MODE_KEY: "observe"}},
            )

        self.assertTrue(result["ok"])
        self.assertIn("4 connected satellite(s)", result["message"])
        run_mock.assert_called_once_with("push-all", timeout=10.0)

    def test_reset_action_clears_redis_and_live_wake_counters(self) -> None:
        with (
            mock.patch.object(home.esphome_firmware, "handle_runtime_action", return_value=None),
            mock.patch.object(home, "_runtime_status_with_native", return_value={}),
            mock.patch.object(home.esphome_speaker_id, "handle_runtime_action", return_value=None),
            mock.patch.object(home.esphome_emotion_id, "handle_runtime_action", return_value=None),
            mock.patch.object(
                home.esphome_runtime,
                "reset_voice_metrics",
                return_value={"retention_days": 30},
            ) as reset_mock,
            mock.patch.object(home.native_satellite, "reset_wake_verifier_runtime_stats", new=lambda: "reset-live"),
            mock.patch.object(
                home.native_satellite,
                "run_on_runtime_loop",
                return_value={"ok": True, "cleared_clients": 3},
            ) as run_mock,
        ):
            result = home.handle_runtime_action(
                action="voice_statistics_reset",
                payload={},
            )

        self.assertTrue(result["ok"])
        self.assertIn("new 30-day", result["message"])
        reset_mock.assert_called_once_with()
        run_mock.assert_called_once_with("reset-live", timeout=5.0)

    def test_wake_verifier_reset_reports_that_disconnected_rows_are_cleared(self) -> None:
        with (
            mock.patch.object(home.esphome_firmware, "handle_runtime_action", return_value=None),
            mock.patch.object(home, "_runtime_status_with_native", return_value={}),
            mock.patch.object(home.esphome_speaker_id, "handle_runtime_action", return_value=None),
            mock.patch.object(home.esphome_emotion_id, "handle_runtime_action", return_value=None),
            mock.patch.object(
                home.esphome_runtime,
                "reset_wake_verifier_metrics",
                return_value={"retention_days": 30},
            ),
            mock.patch.object(home.native_satellite, "reset_wake_verifier_runtime_stats", new=lambda: "reset-live"),
            mock.patch.object(
                home.native_satellite,
                "run_on_runtime_loop",
                return_value={"ok": True, "cleared_clients": 1},
            ),
        ):
            result = home.handle_runtime_action(
                action="voice_wake_verifier_stats_reset",
                payload={},
            )

        self.assertTrue(result["ok"])
        self.assertIn("disconnected satellites", result["message"])

    def test_global_satellite_save_broadcasts_and_syncs_continued_chat(self) -> None:
        values = {
            "wake_word": "hey_tater",
            "continued_chat": True,
            "led_color": "#ffffff",
        }
        with (
            mock.patch.object(home.esphome_firmware, "handle_runtime_action", return_value=None),
            mock.patch.object(home, "_runtime_status_with_native", return_value={}),
            mock.patch.object(home.esphome_speaker_id, "handle_runtime_action", return_value=None),
            mock.patch.object(home.esphome_emotion_id, "handle_runtime_action", return_value=None),
            mock.patch.object(
                home.native_satellite,
                "save_live_settings",
                new=lambda incoming: ("save-all", incoming),
            ),
            mock.patch.object(
                home.native_satellite,
                "run_on_runtime_loop",
                return_value={"push": {"count": 4}},
            ) as run_mock,
            mock.patch.object(
                home.esphome_settings,
                "save_settings_values",
                return_value={"updated_count": 1},
            ) as voice_save_mock,
            mock.patch.object(
                home,
                "_global_satellite_settings_item_form",
                return_value={"group": "global_satellite_settings"},
            ),
            mock.patch.object(home, "_native_satellite_status_snapshot", return_value={"clients": {}}),
        ):
            result = home.handle_runtime_action(
                action="voice_global_satellite_settings_save",
                payload={"values": values},
            )

        self.assertTrue(result["ok"])
        self.assertIn("4 connected satellite(s)", result["message"])
        run_mock.assert_called_once_with(
            ("save-all", {"wake_word": "hey_tater", "continued_chat": True}),
            timeout=15.0,
        )
        voice_save_mock.assert_called_once_with({"VOICE_CONTINUED_CHAT_ENABLED": True})

if __name__ == "__main__":
    unittest.main()
