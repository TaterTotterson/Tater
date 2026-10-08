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

    def test_native_setup_and_macos_app_check_airplay_dependencies(self) -> None:
        root = Path(__file__).resolve().parents[1]
        requirements = (root / "requirements.txt").read_text(encoding="utf-8")
        edge_requirements = (root / "requirements-edge.txt").read_text(encoding="utf-8")
        self.assertIn("imageio-ffmpeg==0.6.0", requirements)
        self.assertIn("imageio-ffmpeg==0.6.0", edge_requirements)

        setup = (root / "setup_tater.sh").read_text(encoding="utf-8")
        self.assertIn('install_airplay_runtime_dependencies "${profile}" "${venv_python}"', setup)
        self.assertIn("TATER_AIRPLAY_CLI_PATH", setup)
        self.assertIn("TATER_FFMPEG_PATH", setup)
        self.assertIn("cap_net_bind_service=+ep", setup)
        self.assertIn("linux_airplay_ptp_ports_available", setup)

        launcher = (
            root / "macos" / "Tater" / "Sources" / "TaterAssistant" / "main.swift"
        ).read_text(encoding="utf-8")
        self.assertIn("airPlayDependenciesReady(using: python)", launcher)
        self.assertIn('environment["TATER_AIRPLAY_CLI_PATH"]', launcher)
        self.assertIn('environment["TATER_SHAIRPORT_SYNC_PATH"]', launcher)

        app_builder = (
            root / "macos" / "Tater" / "scripts" / "build_app.sh"
        ).read_text(encoding="utf-8")
        self.assertIn("prepare_bundled_airplay_runtime", app_builder)
        self.assertIn("AIRPLAY_CLI_MACOS_ARM64_SHA256", app_builder)
        self.assertIn("copy_macos_airplay_libraries", app_builder)

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

    def test_recent_airplay2_survives_an_incomplete_raop_only_scan(self) -> None:
        ap2 = {
            "id": "804af2c57d78",
            "name": "Kitchen",
            "host": "10.0.0.24",
            "airplay_port": 7000,
            "airplay_properties": {"features": "0x1234"},
            "airplay_service_name": "Kitchen._airplay._tcp.local.",
            "manufacturer": "Sonos",
            "protocol": "airplay2",
            "airplay2_seen_ts": 1000.0,
        }
        raop = {
            "id": "804af2c57d78",
            "name": "Kitchen",
            "host": "10.0.0.24",
            "airplay_port": 0,
            "raop_port": 5000,
            "protocol": "raop",
        }
        retained = airplay_bridge._retain_recent_airplay2_rows(
            [raop], [ap2], now_ts=1010.0
        )[0]
        self.assertEqual(retained["airplay_port"], 7000)
        self.assertTrue(retained["discovery_retained_airplay2"])
        self.assertEqual(retained["discovery_legacy_fallback"]["raop_port"], 5000)
        self.assertEqual(retained["manufacturer"], "Sonos")
        expired = airplay_bridge._retain_recent_airplay2_rows(
            [raop], [ap2], now_ts=1000.0 + airplay_bridge.AIRPLAY_RECENT_AP2_TTL_SECONDS + 1
        )[0]
        self.assertEqual(expired["protocol"], "raop")
        different_host = airplay_bridge._retain_recent_airplay2_rows(
            [{**raop, "host": "10.0.0.99"}], [ap2], now_ts=1010.0
        )[0]
        self.assertEqual(different_host["protocol"], "raop")

    def test_discovery_retries_before_retaining_recent_airplay2(self) -> None:
        ap2 = {
            "id": "804af2c57d78", "host": "10.0.0.24", "airplay_port": 7000,
            "airplay_properties": {"features": "0x1234"}, "protocol": "airplay2",
        }
        raop = {
            "id": "804af2c57d78", "host": "10.0.0.24", "airplay_port": 0,
            "raop_port": 5000, "protocol": "raop",
        }
        with (
            mock.patch.object(airplay_bridge.time, "time", return_value=1000.0),
            mock.patch.object(airplay_bridge, "_cached_discovery_rows", return_value=(990.0, [ap2])),
            mock.patch.object(airplay_bridge, "_browse_airplay", side_effect=[[raop], [raop]]) as browse,
            mock.patch.object(airplay_bridge, "_cache_discovery_rows"),
        ):
            airplay_bridge._discovery_rows = []
            airplay_bridge._discovery_ts = 0.0
            rows = airplay_bridge.discover_airplay_devices(force=True)
        self.assertEqual(browse.call_count, 2)
        self.assertEqual(rows[0]["protocol"], "airplay2")
        self.assertTrue(rows[0]["discovery_retained_airplay2"])
        airplay_bridge._discovery_rows = []
        airplay_bridge._discovery_ts = 0.0

    def test_discovery_retry_recovers_airplay2_and_keeps_raop_details(self) -> None:
        ap2 = {
            "id": "804af2c57d78", "host": "10.0.0.24", "airplay_port": 7000,
            "airplay_properties": {"features": "0x1234"}, "protocol": "airplay2",
        }
        raop = {
            "id": "804af2c57d78", "host": "10.0.0.24", "airplay_port": 0,
            "raop_port": 5000, "protocol": "raop",
        }
        with (
            mock.patch.object(airplay_bridge.time, "time", return_value=1000.0),
            mock.patch.object(airplay_bridge, "_cached_discovery_rows", return_value=(990.0, [ap2])),
            mock.patch.object(airplay_bridge, "_browse_airplay", side_effect=[[raop], [ap2]]),
            mock.patch.object(airplay_bridge, "_cache_discovery_rows"),
        ):
            airplay_bridge._discovery_rows = []
            airplay_bridge._discovery_ts = 0.0
            rows = airplay_bridge.discover_airplay_devices(force=True)
        self.assertEqual(rows[0]["protocol"], "airplay2")
        self.assertEqual(rows[0]["raop_port"], 5000)
        self.assertNotIn("discovery_retained_airplay2", rows[0])
        airplay_bridge._discovery_rows = []
        airplay_bridge._discovery_ts = 0.0

    def test_retained_airplay2_connection_can_fall_back_to_raop(self) -> None:
        class Member:
            def __init__(self, **kwargs):
                self.target = kwargs["target"]
                self.device = kwargs["device"]
                self.volume_percent = kwargs["volume_percent"]
                self.route_protocol = ""
                self.route_flow = ""
                self.route_timing = ""

            def prepare(self, _timeout):
                if self.device.get("airplay_port"):
                    raise RuntimeError("AirPlay 2 connection failed")
                self.route_protocol = "raop"
                self.route_flow = "legacy"
                self.route_timing = "ntp"

            def stop(self):
                return None

        legacy = {
            "id": "804af2c57d78", "name": "Kitchen", "host": "10.0.0.24",
            "raop_port": 5000, "airplay_port": 0,
        }
        retained = {
            **legacy, "airplay_port": 7000,
            "discovery_retained_airplay2": True,
            "discovery_legacy_fallback": legacy,
        }
        with (
            mock.patch.object(airplay_bridge, "ensure_airplay_cli", return_value="/bin/cliairplay"),
            mock.patch.object(airplay_bridge, "stop_airplay_targets"),
            mock.patch.object(airplay_bridge, "discover_airplay_devices", return_value=[retained]),
            mock.patch.object(airplay_bridge, "ensure_airplay_ptp_daemon", return_value={}),
            mock.patch.object(airplay_bridge, "_AirPlayMember", Member),
        ):
            result = airplay_bridge.prepare_sendspin_airplay_bridge(
                targets=["804af2c57d78"]
            )
        self.assertTrue(result["ok"])
        self.assertEqual(result["routes"]["airplay:804af2c57d78"]["timing"], "ntp")
        self.assertIn("using legacy RAOP timing", result["warnings"][0])

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

    def test_sendspin_bridge_writes_pcm_directly_without_ffmpeg(self) -> None:
        member = airplay_bridge._AirPlayMember(
            target="airplay:804af2c57d78",
            device={"name": "Kitchen", "host": "10.0.0.24"},
            binary="/tmp/cliairplay",
            volume_percent=61,
            title="Song",
            artist="Artist",
            album="Album",
            duration_seconds=123,
            group_id="sendspin-airplay-test",
            pcm_sample_rate=44_100,
        )
        cli_process = mock.Mock()
        cli_process.poll.return_value = None
        cli_process.stdin = io.BytesIO()
        member.process = cli_process
        member.connected = True

        member.write_sendspin_pcm(b"shared-pcm")

        self.assertEqual(cli_process.stdin.getvalue(), b"shared-pcm")
        args = member._build_args(Path("/tmp/commands.pipe"))
        self.assertEqual(args[args.index("--samplerate") + 1], "44100")

    def test_sendspin_bridge_preparation_does_not_require_ffmpeg(self) -> None:
        captured: list[dict[str, object]] = []

        class Member:
            def __init__(self, **kwargs):
                captured.append(kwargs)
                self.target = kwargs["target"]
                self.device = kwargs["device"]
                self.volume_percent = kwargs["volume_percent"]
                self.route_protocol = "raop"
                self.route_flow = "buffered"
                self.route_timing = "ntp"

            def prepare(self, _timeout):
                return None

            def stop(self):
                return None

        device = {
            "id": "804af2c57d78",
            "name": "Kitchen",
            "host": "10.0.0.24",
            "raop_port": 5000,
            "airplay_port": 0,
            "available": True,
        }
        with (
            mock.patch.object(airplay_bridge, "ensure_airplay_cli", return_value="/bin/cliairplay"),
            mock.patch.object(airplay_bridge, "stop_airplay_targets"),
            mock.patch.object(airplay_bridge, "discover_airplay_devices", return_value=[device]),
            mock.patch.object(airplay_bridge, "_AirPlayMember", Member),
        ):
            result = airplay_bridge.prepare_sendspin_airplay_bridge(
                targets=["804af2c57d78"],
                pcm_sample_rate=44_100,
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["sendspin_bridge"])
        self.assertNotIn("ffmpeg", captured[0])
        self.assertNotIn("source_url", captured[0])
        self.assertEqual(captured[0]["pcm_sample_rate"], 44_100)

    def test_member_parses_receiver_latency(self) -> None:
        member = airplay_bridge._AirPlayMember(
            target="airplay:804af2c57d78",
            device={"name": "Kitchen", "host": "10.0.0.24"},
            binary="/tmp/cliairplay",
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
        self.assertEqual(member.latency_lead_ms, 1800)

    def test_member_does_not_treat_projected_ptp_readiness_as_stable(self) -> None:
        member = airplay_bridge._AirPlayMember(
            target="airplay:804af2c57d78",
            device={"name": "Kitchen", "host": "10.0.0.24"},
            binary="/tmp/cliairplay",
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
        with (
            mock.patch.object(member, "_wait_for", side_effect=[True, False]),
            mock.patch.object(member, "send_metadata"),
            mock.patch.object(member, "send_command"),
        ):
            with self.assertRaisesRegex(RuntimeError, "did not stabilize"):
                member.wait_sendspin_ready()

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
            result = airplay_bridge.start_sendspin_airplay_bridge(
                group_id=group.group_id,
                start_unix_ms=start_unix_ms,
            )
        finally:
            airplay_bridge._forget_group(group.group_id)

        self.assertTrue(result["ok"])
        self.assertEqual(result["timing_mode"], "ptp")
        self.assertEqual(result["start_unix_ms"], start_unix_ms)

    def test_sendspin_bridge_start_is_atomic_when_one_receiver_fails(self) -> None:
        ready = mock.Mock()
        ready.target = "airplay:804af2c57d78"
        ready.audio_present = True
        ready.route_timing = "ptp"
        ready.start.side_effect = lambda requested: requested
        failed = mock.Mock()
        failed.target = "airplay:112233445566"
        failed.audio_present = True
        failed.route_timing = "ptp"
        failed.start.side_effect = RuntimeError("receiver rejected start")
        group = airplay_bridge._AirPlayGroup(
            "sendspin-airplay-atomic-test",
            [ready, failed],
        )
        with airplay_bridge._session_lock:
            airplay_bridge._active_groups[group.group_id] = group
            for member in group.members:
                airplay_bridge._target_groups[member.target] = group.group_id

        start_unix_ms = int(airplay_bridge.time.time() * 1000) + 1000
        result = airplay_bridge.start_sendspin_airplay_bridge(
            group_id=group.group_id,
            start_unix_ms=start_unix_ms,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["sent_count"], 1)
        self.assertIn("receiver rejected start", result["error"])
        ready.stop.assert_called_once()
        failed.stop.assert_called_once()
        with airplay_bridge._session_lock:
            self.assertNotIn(group.group_id, airplay_bridge._active_groups)

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
