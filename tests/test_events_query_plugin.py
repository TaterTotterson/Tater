import asyncio
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

from plugins.events_query import EventsQueryPlugin


class EventsQueryPluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = EventsQueryPlugin()

    @staticmethod
    def _event(source: str, ha_time: str, message: str = "Person detected near door"):
        return {
            "source": source,
            "title": "Camera Event",
            "message": message,
            "type": "camera",
            "ha_time": ha_time,
        }

    def test_contains_person_text_ignores_negative_phrases(self):
        self.assertFalse(self.plugin._contains_person_text("No person detected at the front yard"))
        self.assertTrue(self.plugin._contains_person_text("Person detected at the front yard"))

    def test_resolve_areas_uses_heuristic_when_llm_missing(self):
        resolved = asyncio.run(
            self.plugin._resolve_areas_with_llm(
                "is anyone currently in the back yard",
                ["front_yard", "back_yard", "garage"],
                llm_client=None,
            )
        )
        self.assertEqual(resolved, ["back_yard"])

    def test_presence_answer_stale_reports_not_recent(self):
        now = datetime(2026, 2, 8, 12, 0, 0)
        events = [self._event("back_yard", "2026-02-08T08:00:00")]
        answer = self.plugin._presence_answer_for_area(now, "back yard", events)
        self.assertIn("No recent person detection", answer)

    def test_presence_query_fetches_today_even_if_timeframe_is_yesterday(self):
        now = datetime(2026, 2, 8, 12, 0, 0)
        yesterday_items = [self._event("back_yard", "2026-02-07T20:00:00")]
        today_items = [self._event("back_yard", "2026-02-08T11:59:00")]

        fetch_mock = AsyncMock(side_effect=[yesterday_items, today_items])

        with patch.object(self.plugin, "_ha_now", return_value=now), patch.object(
            self.plugin, "_discover_sources", return_value=["back_yard"]
        ), patch.object(
            self.plugin, "_resolve_areas_with_llm", AsyncMock(return_value=["back_yard"])
        ), patch.object(
            self.plugin, "_fetch_sources_window", fetch_mock
        ):
            result = asyncio.run(
                self.plugin._handle(
                    {"timeframe": "yesterday", "query": "is anyone currently in the back yard"},
                    llm_client=None,
                )
            )

        self.assertEqual(fetch_mock.await_count, 2)
        self.assertIn("just seen", result.lower())

    def test_handle_no_longer_requires_homeassistant_token(self):
        now = datetime(2026, 2, 8, 12, 0, 0)
        with patch.object(self.plugin, "_ha_now", return_value=now), patch.object(
            self.plugin, "_discover_sources", return_value=["front_yard"]
        ), patch.object(
            self.plugin, "_resolve_areas_with_llm", AsyncMock(return_value=None)
        ), patch.object(
            self.plugin, "_fetch_sources_window", AsyncMock(return_value=[])
        ), patch.object(
            self.plugin, "_summarize", AsyncMock(return_value="No events found.")
        ):
            result = asyncio.run(
                self.plugin._handle(
                    {"timeframe": "today", "query": "what happened today"},
                    llm_client=None,
                )
            )
        self.assertEqual(result, "No events found.")

    def test_evening_window_is_clamped_when_before_evening(self):
        now = datetime(2026, 2, 8, 9, 0, 0)
        fetch_mock = AsyncMock(return_value=[])
        with patch.object(self.plugin, "_ha_now", return_value=now), patch.object(
            self.plugin, "_discover_sources", return_value=["front_yard"]
        ), patch.object(
            self.plugin, "_resolve_areas_with_llm", AsyncMock(return_value=None)
        ), patch.object(
            self.plugin, "_fetch_sources_window", fetch_mock
        ), patch.object(
            self.plugin, "_summarize", AsyncMock(return_value="No events.")
        ):
            asyncio.run(
                self.plugin._handle(
                    {"timeframe": "evening", "query": "what happened this evening"},
                    llm_client=None,
                )
            )

        called_start = fetch_mock.await_args.args[1]
        called_end = fetch_mock.await_args.args[2]
        self.assertEqual(called_start, called_end)


if __name__ == "__main__":
    unittest.main()
