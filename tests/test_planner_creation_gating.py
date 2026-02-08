import os
import unittest
from unittest.mock import patch

from planner_loop import (
    _canonical_tool_name,
    _creation_explicit_only,
    _creation_request_analysis,
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
