import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from plugins.list_feeds import ListFeedsPlugin
from plugins.unwatch_feed import UnwatchFeedPlugin
from plugins.watch_feed import WatchFeedPlugin


class RSSFeedPluginTests(unittest.TestCase):
    def setUp(self):
        self.list_plugin = ListFeedsPlugin()
        self.watch_plugin = WatchFeedPlugin()
        self.unwatch_plugin = UnwatchFeedPlugin()

    def test_plugins_are_renamed(self):
        self.assertEqual(self.list_plugin.name, "rss_list")
        self.assertEqual(self.watch_plugin.name, "rss_watch")
        self.assertEqual(self.unwatch_plugin.name, "rss_unwatch")
        self.assertEqual(self.list_plugin.plugin_name, "RSS List")
        self.assertEqual(self.watch_plugin.plugin_name, "RSS Watch")
        self.assertEqual(self.unwatch_plugin.plugin_name, "RSS Unwatch")

    def test_watch_rejects_invalid_url(self):
        result = asyncio.run(self.watch_plugin._watch_feed("not-a-url"))
        self.assertIn("valid http/https", result.lower())

    def test_watch_normalizes_url_and_saves_feed(self):
        parsed = SimpleNamespace(bozo=0, entries=[{"id": "1"}], feed={"title": "Example Feed"})
        with patch("plugins.watch_feed.get_feed", return_value=None), patch(
            "plugins.watch_feed.feedparser.parse", return_value=parsed
        ), patch("plugins.watch_feed.ensure_feed") as ensure_mock:
            result = asyncio.run(self.watch_plugin._watch_feed("HTTPS://Example.COM/rss/"))

        self.assertIn("https://example.com/rss", result)
        self.assertIn("Example Feed", result)
        self.assertEqual(ensure_mock.call_args.args[1], "https://example.com/rss")
        self.assertEqual(ensure_mock.call_args.args[2], 0.0)

    def test_watch_returns_already_watching(self):
        with patch("plugins.watch_feed.get_feed", return_value={"enabled": True}), patch(
            "plugins.watch_feed.feedparser.parse"
        ) as parse_mock, patch("plugins.watch_feed.ensure_feed") as ensure_mock:
            result = asyncio.run(self.watch_plugin._watch_feed("https://example.com/rss"))

        self.assertIn("Already watching", result)
        parse_mock.assert_not_called()
        ensure_mock.assert_not_called()

    def test_watch_handle_webui_accepts_url_alias(self):
        with patch.object(self.watch_plugin, "_watch_feed", AsyncMock(return_value="ok")) as watch_mock:
            out = asyncio.run(self.watch_plugin.handle_webui({"url": "https://example.com/rss"}, llm_client=None))
        self.assertEqual(out, "ok")
        self.assertEqual(watch_mock.await_args.args[0], "https://example.com/rss")

    def test_unwatch_matches_normalized_existing_feed(self):
        feeds = {
            "https://example.com/rss/": {"last_ts": 0.0, "enabled": True, "platforms": {}},
        }
        with patch("plugins.unwatch_feed.get_all_feeds", return_value=feeds), patch(
            "plugins.unwatch_feed.delete_feed", return_value=True
        ) as delete_mock:
            result = asyncio.run(self.unwatch_plugin._unwatch_feed("https://EXAMPLE.com/rss"))

        self.assertIn("Stopped watching feed: https://example.com/rss/", result)
        self.assertEqual(delete_mock.call_args.args[1], "https://example.com/rss/")

    def test_unwatch_handle_webui_accepts_url_alias(self):
        with patch.object(self.unwatch_plugin, "_unwatch_feed", AsyncMock(return_value="ok")) as unwatch_mock:
            out = asyncio.run(self.unwatch_plugin.handle_webui({"rss_url": "https://example.com/rss"}, llm_client=None))
        self.assertEqual(out, "ok")
        self.assertEqual(unwatch_mock.await_args.args[0], "https://example.com/rss")

    def test_list_formats_never_and_overrides(self):
        feeds = {
            "https://example.com/rss": {
                "last_ts": 0,
                "enabled": True,
                "platforms": {
                    "telegram": {"enabled": False, "targets": {"chat_id": "123"}},
                },
            }
        }
        with patch("plugins.list_feeds.get_all_feeds", return_value=feeds):
            result = asyncio.run(self.list_plugin._list_feeds())

        self.assertIn("Watching 1 RSS feed(s):", result)
        self.assertIn("last update: never", result)
        self.assertIn("overrides: telegram:off (chat_id=123)", result)


if __name__ == "__main__":
    unittest.main()
