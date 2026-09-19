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

    def test_fresher_room_wins_when_an_old_signal_was_stronger(self) -> None:
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
        self.assertEqual("Office", device["strongest_room"])
        self.assertEqual("medium", device["confidence"])


if __name__ == "__main__":
    unittest.main()
