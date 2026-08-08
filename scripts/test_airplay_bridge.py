from __future__ import annotations

import io
import sys
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import airplay_bridge
import announcement_targets


class AirPlayBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        with airplay_bridge._ptp_daemon_lock:
            airplay_bridge._ptp_daemon_process = None
            airplay_bridge._ptp_daemon_binary = ""
            airplay_bridge._ptp_daemon_source_ip = ""
            airplay_bridge._ptp_daemon_external = False
            airplay_bridge._ptp_daemon_stop_requested = False
            airplay_bridge._ptp_daemon_restart_count = 0
            airplay_bridge._ptp_daemon_last_ack = ""
            airplay_bridge._ptp_daemon_last_error = ""

    def tearDown(self) -> None:
        airplay_bridge.shutdown_airplay_bridge_runtime()
        with airplay_bridge._ptp_daemon_lock:
            airplay_bridge._ptp_daemon_stop_requested = False

    def test_docker_images_include_offline_airplay_runtime(self) -> None:
        root = Path(__file__).resolve().parents[1]
        for filename in ("Dockerfile", "Dockerfile.nvidia"):
            source = (root / filename).read_text(encoding="utf-8")
            self.assertIn("ffmpeg", source)
            self.assertIn("TATER_FFMPEG_PATH=/usr/bin/ffmpeg", source)
            self.assertIn("TATER_AIRPLAY_CLI_PATH=/usr/local/bin/cliairplay", source)
            self.assertIn("cliairplay-linux-x86_64", source)
            self.assertIn("cliairplay-linux-aarch64", source)
            self.assertIn(airplay_bridge.AIRPLAY_CLI_ASSETS[("linux", "x86_64")][1], source)
            self.assertIn(airplay_bridge.AIRPLAY_CLI_ASSETS[("linux", "aarch64")][1], source)
        compose = (root / "docker-compose.yml").read_text(encoding="utf-8")
        self.assertIn("network_mode: host", compose)
        self.assertIn("NET_BIND_SERVICE", compose)

    def test_shared_ptp_daemon_starts_once_and_exposes_its_clock(self) -> None:
        process = mock.Mock()
        process.pid = 4321
        process.poll.return_value = None
        process.stdout = io.BytesIO()
        with (
            mock.patch.object(
                airplay_bridge,
                "_ptp_daemon_probe",
                side_effect=[
                    (False, ""),
                    (True, "OK peers=0 gm=0123456789abcdef role=grandmaster"),
                ],
            ),
            mock.patch.object(airplay_bridge.subprocess, "Popen", return_value=process) as popen,
            mock.patch.object(airplay_bridge.threading, "Thread") as thread,
        ):
            result = airplay_bridge.ensure_airplay_ptp_daemon(
                binary="/tmp/cliairplay",
                source_ip="10.0.0.10",
            )

        args = popen.call_args.args[0]
        self.assertEqual(args[0], "/tmp/cliairplay")
        self.assertIn("--ptp-daemon", args)
        self.assertEqual(args[args.index("--if") + 1], "10.0.0.10")
        self.assertEqual(args[args.index("--dacp") + 1], airplay_bridge._dacp_id())
        self.assertTrue(result["running"])
        self.assertTrue(result["owned"])
        self.assertEqual(result["pid"], 4321)
        thread.return_value.start.assert_called_once()

    def test_runtime_assets_follow_tater_runtime_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.dict(
            "os.environ",
            {"TATER_RUNTIME_DIR": temp_dir},
        ):
            self.assertEqual(
                airplay_bridge._runtime_root(),
                Path(temp_dir).resolve() / "airplay_bridge",
            )

    def test_ffmpeg_lookup_supports_packaged_macos_and_docker_paths(self) -> None:
        with (
            mock.patch.dict("os.environ", {}, clear=True),
            mock.patch.object(airplay_bridge.shutil, "which", return_value=None),
            mock.patch.object(Path, "is_file", return_value=True),
            mock.patch.object(airplay_bridge.os, "access", return_value=True),
        ):
            self.assertEqual(airplay_bridge._find_ffmpeg(), "/opt/homebrew/bin/ffmpeg")

    def test_airplay_children_use_posix_spawn_safe_options(self) -> None:
        options = airplay_bridge._safe_subprocess_options()
        self.assertFalse(options["close_fds"])
        self.assertFalse(options["start_new_session"])

        class TrackingPopen(subprocess.Popen):
            used_posix_spawn = False

            def _posix_spawn(self, *args, **kwargs):
                type(self).used_posix_spawn = True
                return super()._posix_spawn(*args, **kwargs)

        process = TrackingPopen(
            ["/usr/bin/true"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **options,
        )
        process.communicate(timeout=5)
        self.assertEqual(process.returncode, 0)
        self.assertTrue(TrackingPopen.used_posix_spawn)

    def test_airplay_and_raop_services_merge_by_device_id(self) -> None:
        rows = airplay_bridge._merge_discovery_records(
            [
                {
                    "service_type": "_airplay._tcp.local.",
                    "service_name": "Kitchen._airplay._tcp.local.",
                    "addresses": ["10.0.0.24"],
                    "port": 7000,
                    "server": "Kitchen.local.",
                    "properties": {
                        "deviceid": "80:4A:F2:C5:7D:78",
                        "manufacturer": "Sonos",
                        "model": "Era 100",
                        "features": "0x4A7FCA00,0x3C356BD0",
                    },
                },
                {
                    "service_type": "_raop._tcp.local.",
                    "service_name": "804AF2C57D78@Kitchen._raop._tcp.local.",
                    "addresses": ["10.0.0.24"],
                    "port": 5000,
                    "server": "Kitchen.local.",
                    "properties": {"am": "Era 100", "cn": "0,1,2,3"},
                },
            ]
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "804af2c57d78")
        self.assertEqual(rows[0]["target"], "airplay:804af2c57d78")
        self.assertEqual(rows[0]["name"], "Kitchen")
        self.assertEqual(rows[0]["airplay_port"], 7000)
        self.assertEqual(rows[0]["raop_port"], 5000)
        self.assertEqual(rows[0]["protocol"], "airplay2")

    def test_sonos_sender_uses_automatic_airplay_route_and_lan_interface(self) -> None:
        member = airplay_bridge._AirPlayMember(
            target="airplay:804af2c57d78",
            device={
                "name": "Kitchen",
                "manufacturer": "Sonos",
                "host": "10.0.0.24",
                "server": "Kitchen.local.",
                "airplay_port": 7000,
                "raop_port": 5000,
                "airplay_properties": {
                    "deviceid": "80:4A:F2:C5:7D:78",
                    "features": "0x4A7FCA00,0x3C356BD0",
                },
                "raop_properties": {"am": "Era 100", "cn": "0,1,2,3"},
                "raop_service_name": "804AF2C57D78@Kitchen._raop._tcp.local.",
            },
            binary="/tmp/cliairplay",
            ffmpeg="/tmp/ffmpeg",
            source_url="https://example.test/song.mp3",
            start_position_seconds=0,
            volume_percent=61,
            title="Song",
            artist="Artist",
            album="Album",
            duration_seconds=123,
            group_id="airplay-test",
        )

        with mock.patch.object(
            airplay_bridge,
            "_source_ip_for_peer",
            return_value="10.0.0.10",
        ):
            args = member._build_args(Path("/tmp/commands.pipe"))
        self.assertEqual(args[args.index("--protocol") + 1], "auto")
        self.assertEqual(args[args.index("--port") + 1], "7000")
        self.assertEqual(args[args.index("--volume") + 1], "61")
        self.assertNotIn("--no-ptp", args)
        self.assertIn("--ptp-shared", args)
        self.assertEqual(
            args[args.index("--latency") + 1],
            str(airplay_bridge.AIRPLAY_SONOS_BUFFER_DEPTH_MS),
        )
        self.assertIn("--txt", args)
        self.assertEqual(args[args.index("--if") + 1], "10.0.0.10")
        self.assertEqual(args[-1], "10.0.0.24")

    def test_metadata_command_values_cannot_inject_extra_commands(self) -> None:
        self.assertEqual(
            airplay_bridge._command_value("Song\nACTION=STOP"),
            "Song ACTION=STOP",
        )

    def test_airplay_pcm_feed_leaves_pacing_to_cliairplay(self) -> None:
        member = airplay_bridge._AirPlayMember(
            target="airplay:804af2c57d78",
            device={"name": "Kitchen", "host": "10.0.0.24"},
            binary="/tmp/cliairplay",
            ffmpeg="/tmp/ffmpeg",
            source_url="https://example.test/song.flac",
            start_position_seconds=12.5,
            volume_percent=61,
            title="Song",
            artist="Artist",
            album="Album",
            duration_seconds=123,
            group_id="airplay-test",
        )
        cli_process = mock.Mock()
        cli_process.poll.return_value = None
        cli_process.stdin = mock.Mock()
        member.process = cli_process
        member.connected = True
        ffmpeg_process = mock.Mock()
        ffmpeg_process.poll.return_value = None
        ffmpeg_process.stderr = io.BytesIO()

        with (
            mock.patch.object(airplay_bridge.subprocess, "Popen", return_value=ffmpeg_process) as popen,
            mock.patch.object(member, "_wait_for", return_value=True),
            mock.patch.object(member, "send_metadata"),
            mock.patch.object(member, "send_command"),
        ):
            member.begin_audio()

        args = popen.call_args.args[0]
        self.assertNotIn("-re", args)
        self.assertLess(args.index("-ss"), args.index("-i"))
        self.assertEqual(args[args.index("-ss") + 1], "12.500")
        cli_process.stdin.close.assert_not_called()

    def test_member_parses_warm_transition_constraints(self) -> None:
        member = airplay_bridge._AirPlayMember(
            target="airplay:804af2c57d78",
            device={"name": "Kitchen", "host": "10.0.0.24"},
            binary="/tmp/cliairplay",
            ffmpeg="/tmp/ffmpeg",
            source_url="https://example.test/song.flac",
            start_position_seconds=0,
            volume_percent=61,
            title="Song",
            artist="Artist",
            album="Album",
            duration_seconds=123,
            group_id="airplay-test",
        )

        member._record_line(
            "[STATUS] latency lead_ms=1800 device_render_ms=1750 warm_lead_ms=1750"
        )
        member._record_line("[STATUS] flushed head_unix_ms=2000000001400")

        self.assertEqual(member.latency_lead_ms, 1800)
        self.assertEqual(member.warm_lead_ms, 1750)
        self.assertTrue(member.flushed)
        self.assertEqual(member.flushed_head_unix_ms, 2000000001400)

    def test_member_does_not_treat_projected_ptp_readiness_as_stable(self) -> None:
        member = airplay_bridge._AirPlayMember(
            target="airplay:804af2c57d78",
            device={"name": "Kitchen", "host": "10.0.0.24"},
            binary="/tmp/cliairplay",
            ffmpeg="/tmp/ffmpeg",
            source_url="https://example.test/song.flac",
            start_position_seconds=0,
            volume_percent=61,
            title="Song",
            artist="Artist",
            album="Album",
            duration_seconds=123,
            group_id="airplay-test",
        )

        member._record_line(
            "[STATUS] clock_ready mode=ptp state=probing streak_ms=150 "
            "exchanges=2 ready_in_ms=2150 ready_at_unix_ms=2000000002150"
        )

        self.assertFalse(member.clock_ready_resolved)
        self.assertEqual(member.clock_ready_state, "probing")
        self.assertEqual(member.clock_ready_at_unix_ms, 2000000002150)

        member._record_line(
            "[STATUS] clock_ready mode=ptp state=ready streak_ms=2300 "
            "exchanges=18 ready_in_ms=0 ready_at_unix_ms=2000000002150"
        )

        self.assertTrue(member.clock_ready_resolved)
        self.assertEqual(member.clock_ready_state, "ready")

    def test_member_does_not_wait_for_unmeasurable_ntp_readiness(self) -> None:
        member = airplay_bridge._AirPlayMember(
            target="airplay:804af2c57d78",
            device={"name": "Kitchen", "host": "10.0.0.24"},
            binary="/tmp/cliairplay",
            ffmpeg="/tmp/ffmpeg",
            source_url="https://example.test/song.flac",
            start_position_seconds=0,
            volume_percent=61,
            title="Song",
            artist="Artist",
            album="Album",
            duration_seconds=123,
            group_id="airplay-test",
        )

        member._record_line(
            "[STATUS] clock_ready mode=ntp state=cold streak_ms=0 "
            "exchanges=0 ready_in_ms=0 ready_at_unix_ms=0"
        )

        self.assertTrue(member.clock_ready_resolved)
        self.assertEqual(member.clock_ready_mode, "ntp")

    def test_member_rejects_a_cold_start_when_clock_never_stabilizes(self) -> None:
        member = airplay_bridge._AirPlayMember(
            target="airplay:804af2c57d78",
            device={"name": "Kitchen", "host": "10.0.0.24"},
            binary="/tmp/cliairplay",
            ffmpeg="/tmp/ffmpeg",
            source_url="https://example.test/song.flac",
            start_position_seconds=0,
            volume_percent=61,
            title="Song",
            artist="Artist",
            album="Album",
            duration_seconds=123,
            group_id="airplay-test",
        )
        cli_process = mock.Mock()
        cli_process.poll.return_value = None
        cli_process.stdin = mock.Mock()
        member.process = cli_process
        member.connected = True
        ffmpeg_process = mock.Mock()
        ffmpeg_process.poll.return_value = None
        ffmpeg_process.stderr = io.BytesIO()

        with (
            mock.patch.object(airplay_bridge.subprocess, "Popen", return_value=ffmpeg_process),
            mock.patch.object(member, "_wait_for", side_effect=[True, False]),
            mock.patch.object(member, "send_metadata"),
            mock.patch.object(member, "send_command"),
        ):
            with self.assertRaisesRegex(RuntimeError, "did not stabilize"):
                member.begin_audio()

    def test_group_reuse_refills_members_and_reports_safe_warm_anchor(self) -> None:
        member = mock.Mock()
        member.target = "airplay:804af2c57d78"
        member.warm_lead_ms = 1750
        member.flushed_head_unix_ms = 1001800
        member.clock_ready_mode = "ptp"
        member.clock_ready_state = "ready"
        member.clock_ready_at_unix_ms = 1000000
        member.route_protocol = "airplay2"
        member.route_flow = "native"
        member.route_timing = "ptp"
        group = airplay_bridge._AirPlayGroup("airplay-reuse-test", [member])
        with airplay_bridge._session_lock:
            airplay_bridge._active_groups[group.group_id] = group
            airplay_bridge._target_groups[member.target] = group.group_id
        try:
            with mock.patch.object(airplay_bridge.time, "time", return_value=1000.0):
                result = airplay_bridge.reuse_airplay_group_sync(
                    group_id=group.group_id,
                    targets=[member.target],
                    source_url="https://example.test/next.flac",
                    target_sync_offset_ms={member.target: -100},
                    reference_sync_offset_ms=-80,
                    title="Next Song",
                )
        finally:
            airplay_bridge._forget_group(group.group_id)

        self.assertTrue(result["ok"])
        self.assertTrue(result["reused"])
        self.assertEqual(result["minimum_start_unix_ms"], 1001970)
        self.assertEqual(result["minimum_start_lead_ms"], 1970)
        member.replace_audio.assert_called_once()

    def test_commit_reports_the_active_timing_mode(self) -> None:
        member = mock.Mock()
        member.target = "airplay:804af2c57d78"
        member.audio_present = True
        member.route_timing = "ptp"
        member.start.side_effect = lambda requested: requested
        group = airplay_bridge._AirPlayGroup("airplay-commit-test", [member])
        with airplay_bridge._session_lock:
            airplay_bridge._active_groups[group.group_id] = group
            airplay_bridge._target_groups[member.target] = group.group_id
        try:
            start_unix_ms = int(airplay_bridge.time.time() * 1000) + 1000
            result = airplay_bridge.commit_airplay_group_sync(
                group_id=group.group_id,
                start_unix_ms=start_unix_ms,
            )
        finally:
            airplay_bridge._forget_group(group.group_id)

        self.assertTrue(result["ok"])
        self.assertEqual(result["timing_mode"], "ptp")
        self.assertEqual(result["start_unix_ms"], start_unix_ms)

    def test_target_normalization_is_stable(self) -> None:
        self.assertEqual(
            airplay_bridge.airplay_target_value("80:4A:F2:C5:7D:78"),
            "airplay:804af2c57d78",
        )
        self.assertEqual(
            airplay_bridge.airplay_target_value("airplay:804af2c57d78"),
            "airplay:804af2c57d78",
        )

    def test_airplay_receivers_are_exposed_as_bridge_targets(self) -> None:
        with mock.patch.object(
            airplay_bridge,
            "discover_airplay_devices",
            return_value=[
                {
                    "id": "804af2c57d78",
                    "name": "Kitchen",
                    "manufacturer": "Sonos",
                    "model": "Era 100",
                    "host": "10.0.0.24",
                    "available": True,
                }
            ],
        ):
            rows = announcement_targets.fetch_airplay_target_options()

        self.assertEqual(rows[0]["value"], "airplay:804af2c57d78")
        self.assertIn("AirPlay Bridge: Kitchen", rows[0]["label"])
        self.assertIn("Sonos", rows[0]["label"])
        grouped = announcement_targets.split_announcement_targets(
            ["voice_core:native:kitchen", "airplay:804af2c57d78"]
        )
        self.assertEqual(grouped["airplay_players"], ["804af2c57d78"])

    def test_matching_sonos_and_airplay_endpoints_become_one_option(self) -> None:
        rows = announcement_targets.merge_sonos_airplay_target_options(
            [
                {
                    "value": "sonos:RINCON_804AF2C57D7801400",
                    "label": "Sonos: Kitchen",
                    "bridge_match_ids": ["804af2c57d78"],
                    "bridge_match_hosts": ["10.0.0.24"],
                }
            ],
            [
                {
                    "value": "airplay:804af2c57d78",
                    "label": "AirPlay Bridge: Kitchen",
                    "bridge_match_ids": ["804af2c57d78"],
                    "bridge_match_hosts": ["10.0.0.24"],
                },
                {
                    "value": "airplay:112233445566",
                    "label": "AirPlay Bridge: Office",
                    "bridge_match_ids": ["112233445566"],
                    "bridge_match_hosts": ["10.0.0.30"],
                },
            ],
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["value"], "sonos:RINCON_804AF2C57D7801400")
        self.assertEqual(rows[0]["airplay_bridge_target"], "airplay:804af2c57d78")
        self.assertEqual(
            [option["value"] for option in rows[0]["transport_options"]],
            ["auto", "native", "airplay"],
        )
        self.assertEqual(rows[1]["value"], "airplay:112233445566")

    def test_sonos_rincon_id_resolves_when_registry_is_temporarily_empty(self) -> None:
        with (
            mock.patch.object(announcement_targets, "resolve_sonos_target", return_value={}),
            mock.patch.object(
                airplay_bridge,
                "discover_airplay_devices",
                return_value=[
                    {
                        "id": "804af2c57d78",
                        "target": "airplay:804af2c57d78",
                        "name": "Kitchen",
                        "host": "10.0.0.24",
                    }
                ],
            ),
        ):
            target = announcement_targets.resolve_sonos_airplay_target(
                "sonos:RINCON_804AF2C57D7801400"
            )

        self.assertEqual(target, "airplay:804af2c57d78")


if __name__ == "__main__":
    unittest.main()
