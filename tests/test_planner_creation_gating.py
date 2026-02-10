import asyncio
import os
import unittest
from unittest.mock import patch

from planner_loop import (
    AGENT_CREATION_PLATFORM_REPAIR_PROMPT,
    AGENT_CREATION_PLUGIN_REPAIR_PROMPT,
    AGENT_CREATION_SHARED_REPAIR_PROMPT,
    _agent_system_instructions,
    _autofill_delivery_args,
    _canonical_tool_name,
    _enabled_tool_mini_index,
    _creation_advanced_reference_paths,
    _creation_explicit_only,
    _creation_repair_prompt_for_intent,
    _creation_request_analysis,
    _force_send_message_call,
    _infer_destination_platform,
    _looks_like_platform_followup,
    _resolve_generic_followup_user_text,
    _resolve_delivery_followup_user_text,
    _search_web_retry_args,
    _search_web_should_retry,
    _should_try_search_fallback,
    run_planner_loop,
)


class _FakeRedis:
    def __init__(self, value=None):
        self.value = value

    def get(self, _key):
        return self.value


class _StateRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value

    def delete(self, key):
        self.store.pop(key, None)


class PlannerCreationGatingTests(unittest.TestCase):
    def test_canonical_tool_name_maps_legacy_web_search(self):
        self.assertEqual(_canonical_tool_name("web_search"), "search_web")
        self.assertEqual(_canonical_tool_name("google_search"), "search_web")
        self.assertEqual(_canonical_tool_name("search_web"), "search_web")

    def test_agent_system_instructions_prefer_kernel_tools(self):
        text = _agent_system_instructions(6, 8)
        self.assertIn("Prefer kernel tools first", text)
        self.assertIn("Use plugin tools for platform/service actions", text)

    def test_enabled_tool_mini_index_includes_kernel_tools_section(self):
        text = _enabled_tool_mini_index(
            platform="discord",
            registry={},
            enabled_predicate=None,
        )
        self.assertIn("Kernel tools (prefer first for generic tasks):", text)
        self.assertIn("- search_web", text)
        self.assertIn("- read_url", text)
        self.assertIn("- write_file", text)
        self.assertIn("- truth_list", text)
        self.assertIn("Enabled plugin tools on this platform:", text)

    def test_explicit_plugin_request_is_create(self):
        result = _creation_request_analysis("create a plugin that posts weather updates")
        self.assertEqual(result.get("mode"), "create")
        self.assertTrue(result.get("explicit"))
        self.assertTrue(result.get("need_plugin"))
        self.assertFalse(result.get("need_platform"))

    def test_creation_advanced_references_plugin_api_and_artifacts(self):
        refs = _creation_advanced_reference_paths(
            need_plugin=True,
            need_platform=False,
            request_text="create a plugin that calls an oauth api and returns image attachments",
        )
        self.assertIn("skills/agent_lab/references/plugin_api_auth.md", refs)
        self.assertIn("skills/agent_lab/references/plugin_artifacts.md", refs)
        self.assertNotIn("skills/agent_lab/references/plugin_ai_generation.md", refs)

    def test_creation_advanced_references_plugin_reliability_patterns(self):
        refs = _creation_advanced_reference_paths(
            need_plugin=True,
            need_platform=False,
            request_text=(
                "create a multi-platform notification plugin with timeout retry backoff, "
                "required_settings secrets, action_failure needs, and argument_schema"
            ),
        )
        self.assertIn("skills/agent_lab/references/plugin_http_resilience.md", refs)
        self.assertIn("skills/agent_lab/references/plugin_settings_and_secrets.md", refs)
        self.assertIn("skills/agent_lab/references/plugin_result_contract.md", refs)
        self.assertIn("skills/agent_lab/references/plugin_multiplatform_handlers.md", refs)
        self.assertIn("skills/agent_lab/references/plugin_notification_delivery.md", refs)
        self.assertIn("skills/agent_lab/references/plugin_argument_schema.md", refs)

    def test_creation_advanced_references_platform_network_and_workers(self):
        refs = _creation_advanced_reference_paths(
            need_plugin=False,
            need_platform=True,
            request_text="build a websocket server bridge with queue worker and retry backoff",
        )
        self.assertIn("skills/agent_lab/references/platform_network_events.md", refs)
        self.assertIn("skills/agent_lab/references/platform_pollers_workers.md", refs)

    def test_creation_advanced_references_empty_for_simple_request(self):
        refs = _creation_advanced_reference_paths(
            need_plugin=True,
            need_platform=False,
            request_text="create a basic plugin that echoes text",
        )
        self.assertEqual(refs, [])

    def test_creation_repair_prompt_plugin_only(self):
        prompt = _creation_repair_prompt_for_intent(
            {"need_plugin": True, "need_platform": False}
        )
        self.assertIn(AGENT_CREATION_SHARED_REPAIR_PROMPT, prompt)
        self.assertIn(AGENT_CREATION_PLUGIN_REPAIR_PROMPT, prompt)
        self.assertNotIn(AGENT_CREATION_PLATFORM_REPAIR_PROMPT, prompt)

    def test_creation_repair_prompt_platform_only(self):
        prompt = _creation_repair_prompt_for_intent(
            {"need_plugin": False, "need_platform": True}
        )
        self.assertIn(AGENT_CREATION_SHARED_REPAIR_PROMPT, prompt)
        self.assertIn(AGENT_CREATION_PLATFORM_REPAIR_PROMPT, prompt)
        self.assertNotIn(AGENT_CREATION_PLUGIN_REPAIR_PROMPT, prompt)

    def test_creation_repair_prompt_both(self):
        prompt = _creation_repair_prompt_for_intent(
            {"need_plugin": True, "need_platform": True}
        )
        self.assertIn(AGENT_CREATION_SHARED_REPAIR_PROMPT, prompt)
        self.assertIn(AGENT_CREATION_PLUGIN_REPAIR_PROMPT, prompt)
        self.assertIn(AGENT_CREATION_PLATFORM_REPAIR_PROMPT, prompt)

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

    def test_infer_destination_platform_from_text(self):
        self.assertEqual(_infer_destination_platform("send this in discord"), "discord")
        self.assertEqual(_infer_destination_platform("notify via Matrix room"), "matrix")
        self.assertEqual(_infer_destination_platform("post to Home Assistant"), "homeassistant")

    def test_platform_followup_detection(self):
        text = "I need to know which platform to use for sending the message."
        self.assertTrue(_looks_like_platform_followup(text))

    def test_autofill_delivery_args_sets_platform_for_send_message(self):
        out = _autofill_delivery_args(
            "send_message",
            {"message": "hello", "targets": {"channel": "#tater"}},
            user_text="send to room #tater in discord saying hello",
            origin={"platform": "webui"},
        )
        self.assertEqual(out.get("platform"), "discord")

    def test_force_send_message_call_parses_common_phrase(self):
        forced = _force_send_message_call("send a message to room #tater in discord saying hello")
        self.assertIsInstance(forced, dict)
        self.assertEqual(forced.get("function"), "send_message")
        args = forced.get("arguments") or {}
        self.assertEqual(args.get("platform"), "discord")
        self.assertEqual((args.get("targets") or {}).get("channel"), "#tater")
        self.assertEqual(args.get("message"), "hello")

    def test_resolve_delivery_followup_rebuilds_request_from_platform_reply(self):
        history = [
            {"role": "system", "content": "x"},
            {"role": "user", "content": "send a message to room #tater saying hello"},
            {"role": "assistant", "content": "I need to know which platform to use for sending the message."},
            {"role": "user", "content": "discord"},
        ]
        rebuilt, recovered = _resolve_delivery_followup_user_text(history, "discord")
        self.assertTrue(recovered)
        self.assertIn("send a message", rebuilt.lower())
        self.assertIn("discord", rebuilt.lower())
        self.assertIn("in discord saying hello", rebuilt.lower())

    def test_resolve_delivery_followup_requires_platform_question_context(self):
        history = [
            {"role": "system", "content": "x"},
            {"role": "user", "content": "send a message to room #tater saying hello"},
            {"role": "assistant", "content": "Done."},
            {"role": "user", "content": "discord"},
        ]
        rebuilt, recovered = _resolve_delivery_followup_user_text(history, "discord")
        self.assertFalse(recovered)
        self.assertEqual(rebuilt, "discord")

    def test_resolve_generic_followup_rebuilds_short_answer(self):
        history = [
            {"role": "system", "content": "x"},
            {"role": "user", "content": "set the downstairs thermostat to 72"},
            {"role": "assistant", "content": "Which room should I apply this to?"},
            {"role": "user", "content": "living room"},
        ]
        rebuilt, recovered = _resolve_generic_followup_user_text(history, "living room")
        self.assertTrue(recovered)
        self.assertIn("set the downstairs thermostat to 72", rebuilt.lower())
        self.assertIn("additional detail from user: living room", rebuilt.lower())

    def test_resolve_generic_followup_skips_when_not_question(self):
        history = [
            {"role": "system", "content": "x"},
            {"role": "user", "content": "turn on porch lights"},
            {"role": "assistant", "content": "Done."},
            {"role": "user", "content": "living room"},
        ]
        rebuilt, recovered = _resolve_generic_followup_user_text(history, "living room")
        self.assertFalse(recovered)
        self.assertEqual(rebuilt, "living room")

    def test_resolve_generic_followup_skips_new_full_request(self):
        history = [
            {"role": "system", "content": "x"},
            {"role": "user", "content": "turn on porch lights"},
            {"role": "assistant", "content": "Which room should I target?"},
            {"role": "user", "content": "send a message to room #tater in discord saying hello"},
        ]
        current = "send a message to room #tater in discord saying hello"
        rebuilt, recovered = _resolve_generic_followup_user_text(history, current)
        self.assertFalse(recovered)
        self.assertEqual(rebuilt, current)

    def test_wait_callback_runs_for_kernel_meta_tools(self):
        events = []

        class _LLM:
            def __init__(self):
                self.calls = 0

            async def chat(self, messages, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    return {
                        "message": {
                            "content": "{\"function\":\"get_plugin_help\",\"arguments\":{\"plugin_id\":\"missing_plugin\"}}"
                        }
                    }
                return {"message": {"content": "Done"}}

        async def _wait(func_name, plugin_obj):
            events.append((func_name, plugin_obj is None))

        async def _run():
            return await run_planner_loop(
                llm_client=_LLM(),
                platform="webui",
                history_messages=[{"role": "system", "content": "system"}],
                registry={},
                enabled_predicate=lambda _name: True,
                context={},
                user_text="show available tools",
                scope="test",
                redis_client=_StateRedis(),
                wait_callback=_wait,
                max_rounds=4,
                max_tool_calls=4,
            )

        result = asyncio.run(_run())
        self.assertEqual(result.get("status"), "done")
        self.assertIn(("get_plugin_help", True), events)


if __name__ == "__main__":
    unittest.main()
