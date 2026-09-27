from __future__ import annotations

import unittest
from unittest import mock

from tater_voice import reply_playback


class ReplyPlaybackTests(unittest.TestCase):
    def test_options_keep_all_saved_external_targets_when_discovery_fails(self) -> None:
        with (
            mock.patch.object(reply_playback, "load_homeassistant_config", side_effect=RuntimeError),
            mock.patch.object(
                reply_playback,
                "build_announcement_target_options",
                side_effect=RuntimeError,
            ),
        ):
            options = reply_playback.build_reply_playback_options(
                ["voice_core:stereo:office", "sonos:kitchen", "voice_core:stereo:office"]
            )

        self.assertIn(
            {"value": "voice_core:stereo:office", "label": "voice_core:stereo:office (saved)"},
            options,
        )
        self.assertIn(
            {"value": "sonos:kitchen", "label": "sonos:kitchen (saved)"},
            options,
        )


if __name__ == "__main__":
    unittest.main()
