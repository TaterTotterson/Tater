from __future__ import annotations

import base64
import struct
import sys
import threading
import types
import unittest
from pathlib import Path
from unittest import mock


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import external_audio

class ExternalAudioTests(unittest.TestCase):
    def test_macos_receiver_launch_avoids_fork_only_cwd(self) -> None:
        self.assertIsNone(external_audio._receiver_launch_cwd("darwin"))
        self.assertEqual(
            external_audio._receiver_launch_cwd("linux"),
            str(external_audio._runtime_root()),
        )

    def test_receiver_restart_failures_are_bounded_until_reenabled(self) -> None:
        runtime = external_audio._ExternalAudioRuntime()
        runtime._config["enabled"] = True

        for attempt in range(external_audio.DEFAULT_RECEIVER_MAX_CONSECUTIVE_FAILURES):
            runtime._record_receiver_failure_locked(f"failure {attempt + 1}")

        status = runtime.status()
        self.assertTrue(status["receiver_restart_paused"])
        self.assertEqual(
            status["receiver_consecutive_failures"],
            external_audio.DEFAULT_RECEIVER_MAX_CONSECUTIVE_FAILURES,
        )
        self.assertIn("Automatic restarts paused", status["receiver_error"])

        runtime.configure({"enabled": False})
        status = runtime.status()
        self.assertFalse(status["receiver_restart_paused"])
        self.assertEqual(status["receiver_consecutive_failures"], 0)

    def test_target_group_change_restarts_receiver_and_stops_live_route(self) -> None:
        runtime = external_audio._ExternalAudioRuntime()
        runtime._config = runtime._normalized_config(
            {"enabled": True, "targets": ["voice_core:native:kitchen"]}
        )
        runtime._input_active = True
        runtime._active_session = {
            "id": "airplay-session",
            "routed": True,
            "route_result": {"voice_core_sessions": []},
        }

        with (
            mock.patch.object(runtime, "_ensure_service_threads_locked"),
            mock.patch.object(runtime, "_stop_receiver_locked") as stop_receiver,
            mock.patch.object(runtime, "_ensure_receiver_locked") as ensure_receiver,
        ):
            status = runtime.configure(
                {"enabled": True, "targets": ["voice_core:native:office"]}
            )

        stop_receiver.assert_called_once_with()
        ensure_receiver.assert_called_once_with()
        self.assertFalse(runtime._input_active)
        self.assertEqual(runtime._active_session, {})
        self.assertEqual(status["targets"], ["voice_core:native:office"])

    def test_volume_only_change_reroutes_without_restarting_receiver(self) -> None:
        runtime = external_audio._ExternalAudioRuntime()
        runtime._config = runtime._normalized_config(
            {
                "enabled": True,
                "targets": ["voice_core:native:kitchen"],
                "volume_percent": 60,
            }
        )
        runtime._input_active = True
        runtime._active_session = {
            "id": "airplay-session",
            "routed": True,
            "route_result": {"voice_core_sessions": []},
        }

        with (
            mock.patch.object(runtime, "_ensure_service_threads_locked"),
            mock.patch.object(runtime, "_stop_receiver_locked") as stop_receiver,
            mock.patch.object(runtime, "_ensure_receiver_locked"),
        ):
            runtime.configure(
                {
                    "enabled": True,
                    "targets": ["voice_core:native:kitchen"],
                    "volume_percent": 75,
                }
            )

        stop_receiver.assert_not_called()
        self.assertFalse(runtime._input_active)

    def test_docker_and_macos_package_the_same_pinned_receiver(self) -> None:
        root = Path(__file__).resolve().parents[1]
        for filename in ("Dockerfile", "Dockerfile.nvidia"):
            source = (root / filename).read_text(encoding="utf-8")
            self.assertIn(
                f"ARG SHAIRPORT_SYNC_VERSION={external_audio.SHAIRPORT_SYNC_VERSION}",
                source,
            )
            self.assertIn(
                "TATER_SHAIRPORT_SYNC_PATH=/usr/local/bin/shairport-sync",
                source,
            )
            self.assertIn("--with-stdout", source)
            self.assertIn('shairport-sync -h 2>&1 | grep -q -- "stdout"', source)
        installer = (root / "scripts" / "install_shairport_sync_receiver_macos.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            f"SHAIRPORT_SYNC_VERSION={external_audio.SHAIRPORT_SYNC_VERSION}",
            installer,
        )
        self.assertIn("brew install autoconf automake libtool", installer)
        self.assertIn("shairport-sync-v${SHAIRPORT_SYNC_VERSION}", installer)
        self.assertIn("shairport_sync_configured_port.patch", installer)
        self.assertNotIn("LSUIElement", installer)
        patch = (root / "scripts" / "shairport_sync_configured_port.patch").read_text(
            encoding="utf-8"
        )
        self.assertIn("Preserve a non-zero port", patch)

        app_source = (root / "tateros_app.py").read_text(encoding="utf-8")
        self.assertIn(
            'path.startswith("/api/external-audio/v1/streams/")',
            app_source,
        )

    def test_shairport_config_is_shared_by_linux_and_macos(self) -> None:
        config = external_audio.build_shairport_sync_config_for_test(
            receiver_port=50123,
            metadata_port=50150,
            receiver_name="Kitchen Tater",
            receiver_pin="3939",
        )
        command = external_audio.build_shairport_sync_command_for_test(
            "/opt/tater/bin/shairport-sync"
        )

        self.assertEqual(command[0], "/opt/tater/bin/shairport-sync")
        self.assertIn("classic", command)
        self.assertIn("stdout", command)
        self.assertNotIn("sh", command)
        self.assertIn('name = "Kitchen Tater";', config)
        self.assertIn('password = "3939";', config)
        self.assertIn("port = 50123;", config)
        self.assertIn("udp_port_base = 50124;", config)
        self.assertIn("socket_port = 50150;", config)
        self.assertIn('output_format = "S16_LE";', config)
        self.assertIn('ignore_volume_control = "yes";', config)
        self.assertIn("volume_range_db = 30;", config)
        self.assertIn('volume_control_profile = "standard";', config)
        self.assertIn("default_airplay_volume = 0.0;", config)

    def test_live_defaults_leave_enough_shared_audio_for_a_scheduled_start(self) -> None:
        runtime = external_audio._ExternalAudioRuntime()
        config = runtime._normalized_config({"enabled": True})

        self.assertEqual(config["prebuffer_seconds"], 3.0)
        self.assertEqual(config["input_idle_seconds"], 8.0)
        self.assertLessEqual(
            external_audio.PCM_IO_CHUNK_BYTES
            / (external_audio.SAMPLE_RATE * external_audio.FRAME_BYTES),
            0.1,
        )

    def test_raw_little_endian_pcm_is_written_frame_aligned(self) -> None:
        runtime = external_audio._ExternalAudioRuntime()
        runtime._config = runtime._normalized_config(
            {"enabled": True, "targets": [], "input_idle_seconds": 10}
        )
        runtime.ingest_pcm(b"\x01\x02\x03\x04\x05")
        runtime.ingest_pcm(b"\x05\x06\x07\x08")

        generation = int(runtime._active_session["generation"])
        pcm, _next_cursor = runtime._timeline.read(0, generation, timeout=0)
        self.assertEqual(
            pcm,
            b"\x01\x02\x03\x04\x05\x06\x07\x08",
        )
        self.assertEqual(runtime.status()["pcm_chunks_received"], 2)

    def test_two_satellite_streams_read_the_same_cursor(self) -> None:
        runtime = external_audio._ExternalAudioRuntime()
        runtime._config = runtime._normalized_config(
            {"enabled": True, "targets": [], "input_idle_seconds": 10}
        )
        runtime.ingest_pcm((b"\x11\x22\x33\x44") * 32)
        session = dict(runtime._active_session)
        first = runtime.stream(session["id"], session["token"], 0)
        second = runtime.stream(session["id"], session["token"], 0)

        self.assertEqual(next(first), external_audio._wav_stream_header())
        self.assertEqual(next(second), external_audio._wav_stream_header())
        self.assertEqual(next(first), next(second))
        with self.assertRaises(external_audio.ExternalAudioStreamError):
            runtime.stream(session["id"], "wrong-token", 0)

    def test_prebuffer_routes_one_live_url_to_the_synchronized_media_path(self) -> None:
        calls = []
        stopped = []
        stopped_event = threading.Event()
        stopped_airplay_groups = []

        stopped_sessions = []

        def stop_targets(targets, **kwargs):
            stopped.extend(targets)
            stopped_sessions.extend(kwargs.get("expected_sessions") or [])
            stopped_event.set()
            return []

        media_module = types.ModuleType("media_playback")
        media_module.play_media_url_targets = lambda **kwargs: calls.append(kwargs) or {
            "ok": True,
            "sent_count": 3,
            "synchronized_group": True,
            "airplay_bridge_group_id": "airplay-live-1",
            "voice_core_sessions": [
                {
                    "session_id": "airplay-session-1",
                    "selectors": ["native:kitchen", "native:office"],
                }
            ],
        }
        media_module._voice_core_stop_media_sync = stop_targets
        airplay_module = types.ModuleType("airplay_bridge")
        airplay_module.stop_airplay_group_sync = (
            lambda group_id: stopped_airplay_groups.append(group_id)
            or {"ok": True, "sent_count": 1}
        )
        speech_module = types.ModuleType("speech_tts")
        speech_module._service_base_url_for_peer = lambda: "http://10.0.0.20:8501"
        old_media = sys.modules.get("media_playback")
        old_airplay = sys.modules.get("airplay_bridge")
        old_speech = sys.modules.get("speech_tts")
        sys.modules["media_playback"] = media_module
        sys.modules["airplay_bridge"] = airplay_module
        sys.modules["speech_tts"] = speech_module
        try:
            runtime = external_audio._ExternalAudioRuntime()
            runtime._config = runtime._normalized_config(
                {
                    "enabled": True,
                    "targets": [
                        "voice_core:native:kitchen",
                        "voice_core:native:office",
                        "sonos:RINCON_DEN",
                        "airplay:living-room",
                    ],
                    "prebuffer_seconds": 0.2,
                    "input_idle_seconds": 10,
                    "target_volume_percent": {
                        "voice_core:native:kitchen": 51,
                        "voice_core:native:office": 62,
                    },
                    "target_transport_mode": {"sonos:RINCON_DEN": "airplay"},
                }
            )
            frame_count = int(0.2 * external_audio.SAMPLE_RATE)
            runtime.ingest_pcm((b"\x00\x01\x00\x02") * frame_count)
            self.assertIsNotNone(runtime._route_thread)
            runtime._route_thread.join(timeout=3)

            self.assertEqual(len(calls), 1)
            call = calls[0]
            self.assertEqual(
                call["targets"],
                [
                    "voice_core:native:kitchen",
                    "voice_core:native:office",
                    "sonos:RINCON_DEN",
                    "airplay:living-room",
                ],
            )
            self.assertEqual(
                call["target_volume_percent"],
                {
                    "voice_core:native:kitchen": 100,
                    "voice_core:native:office": 100,
                    "sonos:RINCON_DEN": 100,
                    "airplay:living-room": 100,
                },
            )
            self.assertEqual(
                call["target_transport_mode"],
                {"sonos:RINCON_DEN": "airplay"},
            )
            self.assertEqual(call["volume_percent"], 100)
            self.assertEqual(call["media_type"], "audio/wav")
            self.assertEqual(call["media_content_type"], "music")
            self.assertEqual(call["source_owner"], "external_audio")
            self.assertEqual(
                call["minimum_native_start_lead_ms"],
                external_audio.EXTERNAL_NATIVE_START_LEAD_MS,
            )
            self.assertIn("/api/external-audio/v1/streams/", call["source_url"])
            self.assertIn("cursor=0", call["source_url"])
            self.assertIn("token=", call["source_url"])
            self.assertEqual(runtime.status()["status"], "playing")
            runtime.stop_input()
            self.assertTrue(stopped_event.wait(1))
            self.assertEqual(stopped, [])
            self.assertEqual(
                stopped_sessions,
                [
                    {
                        "session_id": "airplay-session-1",
                        "selectors": ["native:kitchen", "native:office"],
                    }
                ],
            )
            self.assertEqual(stopped_airplay_groups, ["airplay-live-1"])
        finally:
            if old_media is None:
                sys.modules.pop("media_playback", None)
            else:
                sys.modules["media_playback"] = old_media
            if old_airplay is None:
                sys.modules.pop("airplay_bridge", None)
            else:
                sys.modules["airplay_bridge"] = old_airplay
            if old_speech is None:
                sys.modules.pop("speech_tts", None)
            else:
                sys.modules["speech_tts"] = old_speech

    def test_shairport_metadata_parser_accepts_dmap_items(self) -> None:
        encoded = base64.b64encode("Bob Marley".encode()).decode()
        xml_values = external_audio._metadata_item_values(
            f"<item><type>636f7265</type><code>61736172</code>"
            f'<data encoding="base64">{encoded}</data></item>'
        )
        udp_values = external_audio._metadata_item_values(
            struct.pack("!II", int.from_bytes(b"core", "big"), int.from_bytes(b"minm", "big"))
            + b"Three Little Birds"
        )
        self.assertEqual(xml_values, {"artist": "Bob Marley"})
        self.assertEqual(udp_values, {"title": "Three Little Birds"})

    def test_shairport_metadata_parser_exposes_sender_volume(self) -> None:
        values = external_audio._metadata_item_values(
            struct.pack("!II", int.from_bytes(b"ssnc", "big"), int.from_bytes(b"pvol", "big"))
            + b"-15.00,-15.00,-30.00,0.00"
        )
        muted = external_audio._metadata_item_values(
            struct.pack("!II", int.from_bytes(b"ssnc", "big"), int.from_bytes(b"pvol", "big"))
            + b"-144.00,-96.20,-96.20,0.00"
        )

        self.assertEqual(values["airplay_volume_percent"], "50")
        self.assertEqual(values["airplay_volume_db"], "-15.00")
        self.assertEqual(values["output_volume_db"], "-15.00")
        self.assertEqual(muted["airplay_volume_percent"], "0")

    def test_stereo_pair_sender_volume_maps_equally_to_both_members(self) -> None:
        from tater_voice import stereo_pairs

        with mock.patch.object(stereo_pairs, "is_stereo_selector", return_value=True), mock.patch.object(
            stereo_pairs,
            "get_pair",
            return_value={
                "left_selector": "native:office-left",
                "right_selector": "native:office-right",
                "left_volume_percent": 100,
                "right_volume_percent": 100,
            },
        ):
            volumes = external_audio._ExternalAudioRuntime._native_member_volumes(
                ["voice_core:stereo:office"],
                100,
            )

        self.assertEqual(
            volumes,
            {
                "native:office-left": 100,
                "native:office-right": 100,
            },
        )

    def test_stereo_balance_remains_a_relative_trim_below_sender_volume(self) -> None:
        from tater_voice import stereo_pairs

        with mock.patch.object(stereo_pairs, "is_stereo_selector", return_value=True), mock.patch.object(
            stereo_pairs,
            "get_pair",
            return_value={
                "left_selector": "native:office-left",
                "right_selector": "native:office-right",
                "left_volume_percent": 80,
                "right_volume_percent": 100,
            },
        ):
            volumes = external_audio._ExternalAudioRuntime._native_member_volumes(
                ["voice_core:stereo:office"],
                50,
            )

        self.assertEqual(volumes["native:office-left"], 40)
        self.assertEqual(volumes["native:office-right"], 50)

    def test_bridged_sonos_sender_volume_updates_matching_airplay_session(self) -> None:
        import airplay_bridge

        runtime = external_audio._ExternalAudioRuntime()
        runtime._config = runtime._normalized_config(
            {
                "enabled": True,
                "targets": ["sonos:RINCON_DEN"],
                "target_transport_mode": {"sonos:RINCON_DEN": "airplay"},
            }
        )
        runtime._active_session = {"id": "airplay-input", "routed": True}
        route_result = {
            "sonos_airplay_routes": {
                "sonos:RINCON_DEN": "airplay:den-airplay",
            }
        }

        with mock.patch.object(
            airplay_bridge,
            "set_airplay_target_volumes",
            return_value={"sent_count": 1, "warnings": []},
        ) as set_volumes:
            runtime._apply_route_volume(
                "airplay-input",
                route_result,
                ["sonos:RINCON_DEN"],
                73,
            )

        set_volumes.assert_called_once_with({"airplay:den-airplay": 73})

    def test_failed_route_is_bounded_and_backed_off(self) -> None:
        runtime = external_audio._ExternalAudioRuntime()
        runtime._config = runtime._normalized_config(
            {"enabled": True, "targets": ["voice_core:native:kitchen"]}
        )
        runtime._input_active = True
        runtime._active_session = {
            "id": "session",
            "generation": 0,
            "routed": False,
            "routing": False,
            "route_attempts": external_audio.DEFAULT_ROUTE_MAX_ATTEMPTS,
            "route_retry_at": 0.0,
        }
        runtime._timeline.write(bytes(int(0.8 * external_audio.SAMPLE_RATE * external_audio.FRAME_BYTES)))

        runtime._start_route_if_ready_locked()

        self.assertIsNone(runtime._route_thread)
        self.assertFalse(runtime._active_session["routing"])

    def test_newer_playback_releases_only_matching_external_audio_owners(self) -> None:
        runtime = external_audio._ExternalAudioRuntime()
        runtime._status = "playing"
        runtime._input_active = True
        runtime._active_session = {
            "id": "airplay-input",
            "routed": True,
            "route_result": {
                "voice_core_sessions": [
                    {
                        "session_id": "airplay-session-1",
                        "selectors": ["native:kitchen", "native:office-left"],
                    }
                ]
            },
        }

        result = runtime.release_sessions(
            [
                {
                    "session_id": "airplay-session-1",
                    "selectors": ["native:kitchen"],
                },
                {
                    "session_id": "different-session",
                    "selectors": ["native:office-left"],
                },
            ]
        )

        self.assertEqual(result["released_selectors"], ["native:kitchen"])
        remaining = runtime._active_session["route_result"]["voice_core_sessions"]
        self.assertEqual(remaining[0]["selectors"], ["native:office-left"])
        self.assertEqual(runtime._status, "playing")


if __name__ == "__main__":
    unittest.main()
