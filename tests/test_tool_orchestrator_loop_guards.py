import asyncio
import unittest
from unittest.mock import patch

from tool_orchestrator import run_tool_loop


class ToolOrchestratorLoopGuardTests(unittest.TestCase):
    def test_repeated_meta_call_stops_with_loop_message(self):
        class _LLM:
            async def chat(self, messages, **kwargs):
                return {
                    "message": {
                        "content": (
                            "{\"function\":\"get_plugin_help\","
                            "\"arguments\":{\"plugin_id\":\"missing_plugin\"}}"
                        )
                    }
                }

        async def _run():
            return await run_tool_loop(
                llm_client=_LLM(),
                platform="webui",
                history_messages=[{"role": "user", "content": "help"}],
                registry={},
                enabled_predicate=lambda _name: True,
                tool_context={},
                max_steps=6,
            )

        result = asyncio.run(_run())
        self.assertIn("Loop detected", str(result.get("text") or ""))

    def test_repeated_failed_creation_stops_with_guidance(self):
        class _LLM:
            def __init__(self):
                self.calls = 0

            async def chat(self, messages, **kwargs):
                self.calls += 1
                return {
                    "message": {
                        "content": (
                            "{\"function\":\"create_plugin\",\"arguments\":{"
                            f"\"name\":\"tmp_fail_{self.calls}\","
                            "\"code_lines\":[\"from plugin_base import ToolPlugin\"]"
                            "}}"
                        )
                    }
                }

        def _fake_run_meta_tool(*, func, args, platform, registry, enabled_predicate=None, origin=None):
            if func == "create_plugin":
                return {"tool": "create_plugin", "ok": False, "error": "Validation failed."}
            return {"tool": func, "ok": True}

        async def _run():
            with patch("tool_orchestrator.run_meta_tool", side_effect=_fake_run_meta_tool):
                return await run_tool_loop(
                    llm_client=_LLM(),
                    platform="webui",
                    history_messages=[{"role": "user", "content": "create a plugin"}],
                    registry={},
                    enabled_predicate=lambda _name: True,
                    tool_context={},
                    max_steps=8,
                )

        result = asyncio.run(_run())
        self.assertIn("Creation kept failing for plugin", str(result.get("text") or ""))


if __name__ == "__main__":
    unittest.main()
