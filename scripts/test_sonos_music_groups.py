from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from integrations import sonos


class SonosMusicGroupTests(unittest.TestCase):
    def setUp(self) -> None:
        sonos._sonos_music_groups.clear()

    def tearDown(self) -> None:
        sonos._sonos_music_groups.clear()

    def test_multiple_music_targets_join_one_native_sonos_group(self) -> None:
        speakers = {
            "RINCON_LIVING": {
                "id": "RINCON_LIVING",
                "root_url": "http://10.0.0.10:1400",
            },
            "RINCON_KITCHEN": {
                "id": "RINCON_KITCHEN",
                "root_url": "http://10.0.0.11:1400",
            },
        }
        set_uris = []
        volumes = []
        played = []

        def set_uri(root_url, source_url, *, timeout_s):
            set_uris.append((root_url, source_url, timeout_s))

        def play_url(**kwargs):
            played.append(kwargs)

        with (
            mock.patch.object(sonos, "resolve_sonos_target", side_effect=lambda target: speakers[target]),
            mock.patch.object(sonos, "_sonos_snapshot_player", return_value={"media": {"CurrentURI": ""}}),
            mock.patch.object(sonos, "_sonos_set_transport_uri", side_effect=set_uri),
            mock.patch.object(
                sonos,
                "_sonos_set_volume",
                side_effect=lambda root_url, value, *, timeout_s: volumes.append(
                    (root_url, value, timeout_s)
                ),
            ),
            mock.patch.object(sonos, "sonos_play_url_sync", side_effect=play_url),
        ):
            result = sonos.sonos_play_media_sync(
                speakers=["RINCON_LIVING", "RINCON_KITCHEN"],
                source_url="http://tater.local:8501/api/media/runtime/asset/song.mp3",
                media_content_type="music",
                volume_percent=50,
                volume_by_speaker={
                    "RINCON_LIVING": 38,
                    "RINCON_KITCHEN": 67,
                },
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["sent_count"], 2)
        self.assertEqual(set_uris[0][1], "x-rincon:RINCON_LIVING")
        self.assertEqual(played[0]["speaker"]["id"], "RINCON_LIVING")
        self.assertEqual(result["group"]["joined_count"], 1)
        self.assertEqual([row[1] for row in volumes], [38, 67])

    def test_stop_restores_temporarily_joined_sonos_zone(self) -> None:
        sonos._sonos_music_groups["session-1"] = {
            "leader_id": "RINCON_LIVING",
            "leader_root_url": "http://10.0.0.10:1400",
            "target_ids": ["RINCON_LIVING", "RINCON_KITCHEN"],
            "joined": [
                {
                    "target_id": "RINCON_KITCHEN",
                    "root_url": "http://10.0.0.11:1400",
                    "original_uri": "",
                }
            ],
        }
        with (
            mock.patch.object(sonos, "_sonos_stop") as stop,
            mock.patch.object(sonos, "_sonos_become_standalone") as unjoin,
        ):
            result = sonos.sonos_stop_media_sync(speakers=["RINCON_LIVING"])

        self.assertTrue(result["ok"])
        stop.assert_called_once_with("http://10.0.0.10:1400", timeout_s=sonos.SONOS_DEFAULT_PLAY_TIMEOUT_SECONDS)
        unjoin.assert_called_once_with(
            "http://10.0.0.11:1400",
            timeout_s=sonos.SONOS_DEFAULT_PLAY_TIMEOUT_SECONDS,
        )
        self.assertNotIn("session-1", sonos._sonos_music_groups)


if __name__ == "__main__":
    unittest.main()
