import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from plugins.lowfi_video import LowfiVideoPlugin


class LowfiVideoPluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = LowfiVideoPlugin()

    def test_extract_prompt_accepts_aliases(self):
        self.assertEqual(self.plugin._extract_prompt({"query": "  rain room  "}), "rain room")
        self.assertEqual(self.plugin._extract_prompt({"request": "lofi train"}), "lofi train")
        self.assertEqual(self.plugin._extract_prompt({"text": "coffee shop"}), "coffee shop")

    def test_handle_webui_requires_prompt_or_alias(self):
        result = asyncio.run(self.plugin.handle_webui({}, llm_client=None))
        self.assertIsInstance(result, list)
        self.assertIn("No prompt given", result[0])

    def test_handle_webui_clamps_durations_and_uses_defaults_without_llm(self):
        with patch("plugins.lowfi_video.redis_client.hgetall", return_value={"LENGTH": "15"}), patch.object(
            self.plugin, "_refine_prompt_for_lofi", AsyncMock(return_value="refined scene")
        ), patch.object(
            self.plugin, "_generate_audio", AsyncMock(return_value="/tmp/audio.mp3")
        ) as gen_audio, patch.object(
            self.plugin, "_generate_loop_clip", AsyncMock(return_value="/tmp/loop.mp4")
        ) as gen_loop, patch.object(
            self.plugin, "_mux", AsyncMock(return_value="/tmp/final.mp4")
        ) as mux, patch.object(
            self.plugin, "_read_binary", return_value=b"video-bytes"
        ):
            result = asyncio.run(
                self.plugin.handle_webui(
                    {"query": "calm library", "audio_minutes": 9, "video_minutes": 1, "loop_seconds": 999},
                    llm_client=None,
                )
            )

        self.assertEqual(gen_audio.await_args.args[1], 3)
        self.assertEqual(gen_loop.await_args.args[2], 60)
        self.assertEqual(mux.await_args.kwargs.get("video_seconds"), 180)
        self.assertEqual(result[0]["type"], "video")
        self.assertEqual(result[0]["mimetype"], "video/mp4")
        self.assertEqual(result[1], "Here is your chill lofi video.")

    def test_handle_webui_aligns_video_minutes_when_cap_prevents_next_multiple(self):
        with patch("plugins.lowfi_video.redis_client.hgetall", return_value={"LENGTH": "15"}), patch.object(
            self.plugin, "_refine_prompt_for_lofi", AsyncMock(return_value="refined scene")
        ), patch.object(
            self.plugin, "_generate_audio", AsyncMock(return_value="/tmp/audio.mp3")
        ), patch.object(
            self.plugin, "_generate_loop_clip", AsyncMock(return_value="/tmp/loop.mp4")
        ), patch.object(
            self.plugin, "_mux", AsyncMock(return_value="/tmp/final.mp4")
        ) as mux, patch.object(
            self.plugin, "_read_binary", return_value=b"video-bytes"
        ):
            asyncio.run(
                self.plugin.handle_webui(
                    {"prompt": "city rain", "audio_minutes": 3, "video_minutes": 22, "loop_seconds": 15},
                    llm_client=None,
                )
            )

        self.assertEqual(mux.await_args.kwargs.get("video_seconds"), 21 * 60)

    def test_handle_webui_uses_llm_followup_when_available(self):
        llm_client = AsyncMock()
        llm_client.chat = AsyncMock(return_value={"message": {"content": "Freshly brewed for your evening focus."}})

        with patch("plugins.lowfi_video.redis_client.hgetall", return_value={"LENGTH": "15"}), patch.object(
            self.plugin, "_refine_prompt_for_lofi", AsyncMock(return_value="refined scene")
        ), patch.object(
            self.plugin, "_generate_audio", AsyncMock(return_value="/tmp/audio.mp3")
        ), patch.object(
            self.plugin, "_generate_loop_clip", AsyncMock(return_value="/tmp/loop.mp4")
        ), patch.object(
            self.plugin, "_mux", AsyncMock(return_value="/tmp/final.mp4")
        ), patch.object(
            self.plugin, "_read_binary", return_value=b"video-bytes"
        ):
            result = asyncio.run(self.plugin.handle_webui({"prompt": "night bus stop"}, llm_client=llm_client))

        self.assertEqual(result[1], "Freshly brewed for your evening focus.")


if __name__ == "__main__":
    unittest.main()
