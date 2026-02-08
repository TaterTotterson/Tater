import os
import unittest
from unittest.mock import patch

from planner_loop import (
    _canonical_tool_name,
    _creation_explicit_only,
    _creation_request_analysis,
    _search_web_retry_args,
    _search_web_should_retry,
    _should_try_search_fallback,
)


class _FakeRedis:
    def __init__(self, value=None):
        self.value = value

    def get(self, _key):
        return self.value


class PlannerCreationGatingTests(unittest.TestCase):
    def test_canonical_tool_name_maps_legacy_web_search(self):
        self.assertEqual(_canonical_tool_name("web_search"), "search_web")
        self.assertEqual(_canonical_tool_name("google_search"), "search_web")
        self.assertEqual(_canonical_tool_name("search_web"), "search_web")

    def test_explicit_plugin_request_is_create(self):
        result = _creation_request_analysis("create a plugin that posts weather updates")
        self.assertEqual(result.get("mode"), "create")
        self.assertTrue(result.get("explicit"))
        self.assertTrue(result.get("need_plugin"))
        self.assertFalse(result.get("need_platform"))

    def test_search_fallback_triggers_for_world_events_question(self):
        self.assertTrue(
            _should_try_search_fallback(
                "what's been going on in the world",
                "latest_news",
                needs_creation=False,
            )
        )

    def test_search_fallback_not_for_action_command(self):
        self.assertFalse(
            _should_try_search_fallback(
                "turn on the porch lights",
                "unknown_light_tool",
                needs_creation=False,
            )
        )

    def test_search_web_should_retry_on_thin_results(self):
        payload = {
            "ok": True,
            "count": 1,
            "results": [{"title": "x", "url": "https://x", "snippet": "short"}],
            "has_more": True,
            "next_start": 11,
        }
        self.assertTrue(_search_web_should_retry(payload, retry_count=0))
        self.assertFalse(_search_web_should_retry(payload, retry_count=1))

    def test_search_web_retry_args_uses_next_page(self):
        args = {"query": "world events", "num_results": 5, "start": 1}
        payload = {"ok": True, "has_more": True, "next_start": 11}
        retry = _search_web_retry_args(args, payload, "world events")
        self.assertIsInstance(retry, dict)
        self.assertEqual(retry.get("query"), "world events")
        self.assertEqual(retry.get("start"), 11)
        self.assertEqual(retry.get("num_results"), 5)

    def test_search_web_retry_args_broadens_without_next_page(self):
        args = {"query": "small topic", "num_results": 3, "site": "example.com"}
        payload = {"ok": True, "has_more": False, "next_start": None}
        retry = _search_web_retry_args(args, payload, "small topic")
        self.assertIsInstance(retry, dict)
        self.assertIn("latest", str(retry.get("query")))
        self.assertEqual(retry.get("start"), 1)
        self.assertGreaterEqual(int(retry.get("num_results") or 0), 5)
        self.assertNotIn("site", retry)

    def test_run_negative_guard_blocks_creation(self):
        result = _creation_request_analysis("run this plugin")
        self.assertEqual(result.get("mode"), "none")
        self.assertIn("run", result.get("guards") or [])

    def test_agent_lab_ambiguous_is_ask(self):
        result = _creation_request_analysis("agent lab plugin for jokes")
        self.assertEqual(result.get("mode"), "ask")
        self.assertTrue(result.get("need_plugin"))

    def test_creation_explicit_only_defaults_true(self):
        with patch.dict(os.environ, {}, clear=False):
            self.assertTrue(_creation_explicit_only(r=_FakeRedis(None)))

    def test_creation_explicit_only_env_override_false(self):
        with patch.dict(os.environ, {"TATER_CREATION_EXPLICIT_ONLY": "false"}, clear=False):
            self.assertFalse(_creation_explicit_only(r=_FakeRedis("true")))


if __name__ == "__main__":
    unittest.main()
