from __future__ import annotations

import unittest

from tater_voice import native_ble


class NativeBleTests(unittest.TestCase):
    def setUp(self) -> None:
        native_ble.reset_for_tests()

    def test_ingest_tracks_latest_observation_and_source(self) -> None:
        result = native_ble.ingest_advertisements(
            "native:kitchen",
            {
                "batch_id": 7,
                "adverts": [
                    {
                        "address": "AA:BB:CC:DD:EE:FF",
                        "address_type": 1,
                        "rssi": -52,
                        "event_type": 0,
                        "data": "020106",
                        "age_ms": 25,
                    }
                ],
            },
            metadata={"device_name": "Kitchen", "room": "Kitchen", "board": "satellite1"},
            received_ts=100.0,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(1, result["accepted"])
        snapshot = native_ble.snapshot(now_ts=100.0)
        self.assertEqual(1, snapshot["count"])
        self.assertEqual("aa:bb:cc:dd:ee:ff", snapshot["observations"][0]["address"])
        self.assertEqual(99.975, snapshot["observations"][0]["observed_ts"])
        self.assertEqual("Kitchen", snapshot["devices"][0]["strongest_room"])

    def test_latest_rssi_wins_per_satellite_address_and_event_type(self) -> None:
        for batch_id, rssi in ((1, -70), (2, -45)):
            native_ble.ingest_advertisements(
                "native:office",
                {
                    "batch_id": batch_id,
                    "adverts": [
                        {
                            "address": "10:20:30:40:50:60",
                            "rssi": rssi,
                            "event_type": 0,
                            "data": "01",
                        }
                    ],
                },
                metadata={"room": "Office"},
                received_ts=100.0 + batch_id,
            )

        snapshot = native_ble.snapshot(now_ts=102.0)
        self.assertEqual(1, snapshot["count"])
        self.assertEqual(-45, snapshot["observations"][0]["rssi"])
        self.assertEqual(2, snapshot["sources"][0]["batches_received"])

    def test_rejects_invalid_and_caps_batches(self) -> None:
        rows = [
            {"address": f"00:00:00:00:00:{index:02x}", "rssi": -60, "data": ""}
            for index in range(native_ble.MAX_BATCH_ADVERTS + 3)
        ]
        rows[0]["address"] = "not-an-address"
        rows[1]["data"] = "xyz"
        result = native_ble.ingest_advertisements(
            "native:test",
            {"adverts": rows},
            received_ts=200.0,
        )

        self.assertEqual(native_ble.MAX_BATCH_ADVERTS - 2, result["accepted"])
        self.assertEqual(2, result["invalid"])
        self.assertEqual(3, result["dropped"])

    def test_presence_selects_strongest_satellite(self) -> None:
        for selector, room, rssi in (
            ("native:kitchen", "Kitchen", -72),
            ("native:office", "Office", -41),
        ):
            native_ble.ingest_advertisements(
                selector,
                {
                    "adverts": [
                        {
                            "address": "de:ad:be:ef:00:01",
                            "rssi": rssi,
                            "event_type": 0,
                            "data": "020106",
                        }
                    ]
                },
                metadata={"room": room},
                received_ts=300.0,
            )

        device = native_ble.snapshot(now_ts=300.0)["devices"][0]
        self.assertEqual("native:office", device["strongest_selector"])
        self.assertEqual("Office", device["strongest_room"])

    def test_presence_lists_each_satellite_once_across_event_types(self) -> None:
        native_ble.ingest_advertisements(
            "native:kitchen",
            {
                "adverts": [
                    {"address": "de:ad:be:ef:00:02", "rssi": -50, "event_type": 0, "data": "01"},
                    {"address": "de:ad:be:ef:00:02", "rssi": -48, "event_type": 4, "data": "02"},
                ]
            },
            metadata={"room": "Kitchen"},
            received_ts=400.0,
        )

        device = native_ble.snapshot(now_ts=400.0)["devices"][0]
        self.assertEqual(1, len(device["sources"]))

    def test_snapshot_decodes_identity_and_exposes_presence_metadata(self) -> None:
        native_ble.ingest_advertisements(
            "native:family-room",
            {
                "adverts": [
                    {
                        "address": "aa:bb:cc:dd:ee:ff",
                        "rssi": -48,
                        "event_type": 0,
                        "data": "02010607094d792054616703ff4c0003030f18",
                    }
                ]
            },
            metadata={"device_name": "Family Room VoicePE", "room": "Family Room"},
            received_ts=500.0,
        )

        snapshot = native_ble.snapshot(now_ts=501.0)
        device = snapshot["devices"][0]
        self.assertEqual("My Tag", device["display_name"])
        self.assertEqual(0x004C, device["manufacturer_id"])
        self.assertEqual("0x004C", device["manufacturer_id_hex"])
        self.assertEqual(["180f"], device["service_uuids"])
        self.assertEqual("Family Room", device["strongest_room"])
        self.assertEqual("excellent", device["signal"])
        self.assertEqual({"Family Room": 1}, snapshot["rooms"])
        self.assertGreater(snapshot["revision"], 0)
        self.assertEqual(501.0, snapshot["generated_ts"])
        compact = native_ble.snapshot(now_ts=501.0, include_observations=False)
        self.assertEqual([], compact["observations"])
        self.assertEqual(1, compact["count"])

    def test_recent_stronger_signal_is_not_displaced_by_packet_timing(self) -> None:
        address = "de:ad:be:ef:00:03"
        native_ble.ingest_advertisements(
            "native:kitchen",
            {"adverts": [{"address": address, "rssi": -42, "data": "020106"}]},
            metadata={"room": "Kitchen"},
            received_ts=600.0,
        )
        native_ble.ingest_advertisements(
            "native:office",
            {"adverts": [{"address": address, "rssi": -58, "data": "020106"}]},
            metadata={"room": "Office"},
            received_ts=612.0,
        )

        device = native_ble.snapshot(now_ts=612.0)["devices"][0]
        self.assertEqual("Kitchen", device["strongest_room"])
        self.assertEqual("high", device["confidence"])

    def test_small_or_brief_signal_changes_do_not_move_a_stationary_device(self) -> None:
        address = "de:ad:be:ef:00:04"
        for selector, room, rssi in (
            ("native:kitchen", "Kitchen", -55),
            ("native:office", "Office", -80),
        ):
            native_ble.ingest_advertisements(
                selector,
                {"adverts": [{"address": address, "rssi": rssi, "data": "020106"}]},
                metadata={"room": room},
                received_ts=700.0,
            )
        self.assertEqual("Kitchen", native_ble.snapshot(now_ts=700.0)["devices"][0]["strongest_room"])

        # A few much stronger office readings are enough to nominate Office,
        # but not enough to record a room movement.
        for timestamp in (701.0, 702.0, 703.0, 704.0):
            native_ble.ingest_advertisements(
                "native:kitchen",
                {"adverts": [{"address": address, "rssi": -65, "data": "020106"}]},
                metadata={"room": "Kitchen"},
                received_ts=timestamp,
            )
            native_ble.ingest_advertisements(
                "native:office",
                {"adverts": [{"address": address, "rssi": -40, "data": "020106"}]},
                metadata={"room": "Office"},
                received_ts=timestamp,
            )
            device = native_ble.snapshot(now_ts=timestamp)["devices"][0]

        self.assertEqual("Kitchen", device["strongest_room"])
        self.assertEqual("Office", device["raw_strongest_room"])
        self.assertEqual("Office", device["candidate_room"])

        # Returning to the original signal pattern clears the pending move.
        native_ble.ingest_advertisements(
            "native:kitchen",
            {"adverts": [{"address": address, "rssi": -35, "data": "020106"}]},
            metadata={"room": "Kitchen"},
            received_ts=705.0,
        )
        native_ble.ingest_advertisements(
            "native:office",
            {"adverts": [{"address": address, "rssi": -80, "data": "020106"}]},
            metadata={"room": "Office"},
            received_ts=705.0,
        )
        device = native_ble.snapshot(now_ts=705.0)["devices"][0]
        self.assertEqual("Kitchen", device["strongest_room"])
        self.assertNotIn("candidate_room", device)

    def test_sustained_stronger_signal_moves_the_device_after_dwell(self) -> None:
        address = "de:ad:be:ef:00:05"
        for selector, room, rssi in (
            ("native:kitchen", "Kitchen", -55),
            ("native:office", "Office", -80),
        ):
            native_ble.ingest_advertisements(
                selector,
                {"adverts": [{"address": address, "rssi": rssi, "data": "020106"}]},
                metadata={"room": room},
                received_ts=800.0,
            )
        native_ble.snapshot(now_ts=800.0)

        device = {}
        for timestamp in range(801, 814):
            for selector, room, rssi in (
                ("native:kitchen", "Kitchen", -65),
                ("native:office", "Office", -40),
            ):
                native_ble.ingest_advertisements(
                    selector,
                    {"adverts": [{"address": address, "rssi": rssi, "data": "020106"}]},
                    metadata={"room": room},
                    received_ts=float(timestamp),
                )
            device = native_ble.snapshot(now_ts=float(timestamp))["devices"][0]
            if timestamp < 812:
                self.assertEqual("Kitchen", device["strongest_room"])

        self.assertEqual("Office", device["strongest_room"])
        self.assertEqual("Office", device["raw_strongest_room"])
        self.assertNotIn("candidate_room", device)

    def test_recent_packets_get_a_freshness_grace_period(self) -> None:
        address = "de:ad:be:ef:00:06"
        native_ble.ingest_advertisements(
            "native:kitchen",
            {"adverts": [{"address": address, "rssi": -60, "data": "020106"}]},
            metadata={"room": "Kitchen"},
            received_ts=900.0,
        )
        native_ble.ingest_advertisements(
            "native:living",
            {"adverts": [{"address": address, "rssi": -61, "data": "020106"}]},
            metadata={"room": "Living Room"},
            received_ts=900.0,
        )
        native_ble.snapshot(now_ts=900.0)

        native_ble.ingest_advertisements(
            "native:living",
            {"adverts": [{"address": address, "rssi": -61, "data": "020106"}]},
            metadata={"room": "Living Room"},
            received_ts=903.0,
        )
        device = native_ble.snapshot(now_ts=903.0)["devices"][0]
        self.assertEqual("Kitchen", device["strongest_room"])
        self.assertEqual("Kitchen", device["raw_strongest_room"])

    def test_filtered_snapshot_does_not_change_the_canonical_room(self) -> None:
        address = "de:ad:be:ef:00:07"
        for selector, room, rssi in (
            ("native:kitchen", "Kitchen", -50),
            ("native:office", "Office", -65),
        ):
            native_ble.ingest_advertisements(
                selector,
                {"adverts": [{"address": address, "rssi": rssi, "data": "020106"}]},
                metadata={"room": room},
                received_ts=1000.0,
            )
        self.assertEqual("Kitchen", native_ble.snapshot(now_ts=1000.0)["devices"][0]["strongest_room"])

        filtered = native_ble.snapshot(selector="native:office", now_ts=1001.0)["devices"][0]
        self.assertEqual("Office", filtered["strongest_room"])
        self.assertEqual("Kitchen", native_ble.snapshot(now_ts=1001.0)["devices"][0]["strongest_room"])

    def test_periodic_snapshots_do_not_move_between_two_stale_sources(self) -> None:
        address = "de:ad:be:ef:00:08"
        native_ble.ingest_advertisements(
            "native:kitchen",
            {"adverts": [{"address": address, "rssi": -50, "data": "020106"}]},
            metadata={"room": "Kitchen"},
            received_ts=1100.0,
        )
        native_ble.ingest_advertisements(
            "native:office",
            {"adverts": [{"address": address, "rssi": -60, "data": "020106"}]},
            metadata={"room": "Office"},
            received_ts=1100.0,
        )
        self.assertEqual("Kitchen", native_ble.snapshot(now_ts=1100.0)["devices"][0]["strongest_room"])

        native_ble.ingest_advertisements(
            "native:office",
            {"adverts": [{"address": address, "rssi": -48, "data": "020106"}]},
            metadata={"room": "Office"},
            received_ts=1101.0,
        )
        self.assertEqual("Kitchen", native_ble.snapshot(now_ts=1101.0)["devices"][0]["strongest_room"])
        self.assertEqual("Kitchen", native_ble.snapshot(now_ts=1115.0)["devices"][0]["strongest_room"])
        device = native_ble.snapshot(now_ts=1120.0)["devices"][0]
        self.assertEqual("Kitchen", device["strongest_room"])
        self.assertNotIn("candidate_room", device)

    def test_sparse_stationary_adverts_do_not_make_the_latest_packet_win(self) -> None:
        address = "de:ad:be:ef:00:09"
        native_ble.ingest_advertisements(
            "native:living",
            {"adverts": [{"address": address, "rssi": -51, "data": "020106"}]},
            metadata={"room": "Living Room"},
            received_ts=1200.0,
        )
        native_ble.ingest_advertisements(
            "native:master",
            {"adverts": [{"address": address, "rssi": -63, "data": "020106"}]},
            metadata={"room": "Master Bedroom"},
            received_ts=1200.0,
        )
        self.assertEqual("Living Room", native_ble.snapshot(now_ts=1200.0)["devices"][0]["strongest_room"])

        # Master Bedroom hears a newer advert eighteen seconds later. Living
        # Room remains the better signal and must not be treated as stale.
        native_ble.ingest_advertisements(
            "native:master",
            {"adverts": [{"address": address, "rssi": -62, "data": "020106"}]},
            metadata={"room": "Master Bedroom"},
            received_ts=1218.0,
        )
        device = native_ble.snapshot(now_ts=1218.0)["devices"][0]
        self.assertEqual("Living Room", device["strongest_room"])
        self.assertEqual("Living Room", device["raw_strongest_room"])

    def test_candidate_room_requires_two_distinct_observations(self) -> None:
        address = "de:ad:be:ef:00:0a"
        for selector, room, rssi in (
            ("native:kitchen", "Kitchen", -55),
            ("native:office", "Office", -60),
        ):
            native_ble.ingest_advertisements(
                selector,
                {"adverts": [{"address": address, "rssi": rssi, "data": "020106"}]},
                metadata={"room": room},
                received_ts=1300.0,
            )
        native_ble.snapshot(now_ts=1300.0)

        native_ble.ingest_advertisements(
            "native:office",
            {"adverts": [{"address": address, "rssi": 20, "data": "020106"}]},
            metadata={"room": "Office"},
            received_ts=1301.0,
        )
        device = native_ble.snapshot(now_ts=1301.0)["devices"][0]
        self.assertEqual("Kitchen", device["strongest_room"])
        self.assertEqual("Office", device["candidate_room"])

        device = native_ble.snapshot(now_ts=1310.0)["devices"][0]
        self.assertEqual("Kitchen", device["strongest_room"])
        self.assertEqual("Office", device["candidate_room"])

        native_ble.ingest_advertisements(
            "native:office",
            {"adverts": [{"address": address, "rssi": 20, "data": "020106"}]},
            metadata={"room": "Office"},
            received_ts=1311.0,
        )
        device = native_ble.snapshot(now_ts=1311.0)["devices"][0]
        self.assertEqual("Office", device["strongest_room"])

    def test_stale_current_room_moves_after_two_fresh_observations(self) -> None:
        address = "de:ad:be:ef:00:0b"
        for selector, room, rssi in (
            ("native:kitchen", "Kitchen", -50),
            ("native:office", "Office", -70),
        ):
            native_ble.ingest_advertisements(
                selector,
                {"adverts": [{"address": address, "rssi": rssi, "data": "020106"}]},
                metadata={"room": room},
                received_ts=1400.0,
            )
        self.assertEqual("Kitchen", native_ble.snapshot(now_ts=1400.0)["devices"][0]["strongest_room"])

        # The old room has gone quiet, while the new room hears two separate
        # advertisements across the shorter stale-room dwell window.
        native_ble.ingest_advertisements(
            "native:office",
            {"adverts": [{"address": address, "rssi": -20, "data": "020106"}]},
            metadata={"room": "Office"},
            received_ts=1446.0,
        )
        device = native_ble.snapshot(now_ts=1446.0)["devices"][0]
        self.assertEqual("Kitchen", device["strongest_room"])
        self.assertEqual("Office", device["candidate_room"])

        native_ble.ingest_advertisements(
            "native:office",
            {"adverts": [{"address": address, "rssi": -20, "data": "020106"}]},
            metadata={"room": "Office"},
            received_ts=1449.0,
        )
        device = native_ble.snapshot(now_ts=1449.0)["devices"][0]
        self.assertEqual("Office", device["strongest_room"])


if __name__ == "__main__":
    unittest.main()
