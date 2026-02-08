import unittest
import uuid
from pathlib import Path

from kernel_tools import (
    AGENT_PLATFORMS_DIR,
    AGENT_PLUGINS_DIR,
    create_platform,
    create_plugin,
)
from tool_runtime import run_meta_tool


def _plugin_source(name: str, description: str) -> str:
    return f"""from plugin_base import ToolPlugin

class TempPlugin(ToolPlugin):
    name = "{name}"
    plugin_name = "{name}"
    version = "1.0.0"
    description = "{description}"
    platforms = ["webui"]
    usage = '{{"function":"{name}","arguments":{{}}}}'
    when_to_use = "Use for overwrite guard tests."
    waiting_prompt_template = "Write a short wait message. Only output that message."

    async def handle_webui(self, args, llm_client, context=None):
        return {{"ok": True}}

plugin = TempPlugin()
"""


def _platform_source(name: str, description: str) -> str:
    return f"""PLATFORM = {{"key": "{name}", "name": "{name}", "description": "{description}"}}

def run(stop_event):
    return None
"""


class KernelToolsCreationOverwriteTests(unittest.TestCase):
    def setUp(self):
        AGENT_PLUGINS_DIR.mkdir(parents=True, exist_ok=True)
        AGENT_PLATFORMS_DIR.mkdir(parents=True, exist_ok=True)
        self._created_paths = []

    def tearDown(self):
        for path in self._created_paths:
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                pass

    def test_create_plugin_requires_explicit_overwrite(self):
        name = f"tmp_plugin_overwrite_{uuid.uuid4().hex}"
        path = AGENT_PLUGINS_DIR / f"{name}.py"
        self._created_paths.append(path)

        first = create_plugin(name=name, code=_plugin_source(name, "first version"))
        self.assertTrue(first.get("ok"), first)
        original = path.read_text(encoding="utf-8")

        second = create_plugin(name=name, code=_plugin_source(name, "second version"))
        self.assertFalse(second.get("ok"), second)
        self.assertEqual(second.get("error_code"), "already_exists")
        self.assertTrue(second.get("overwrite_required"))
        self.assertTrue(second.get("needs"))
        self.assertEqual(path.read_text(encoding="utf-8"), original)

        third = create_plugin(
            name=name,
            code=_plugin_source(name, "third version"),
            overwrite=True,
        )
        self.assertTrue(third.get("ok"), third)
        updated = path.read_text(encoding="utf-8")
        self.assertNotEqual(updated, original)
        self.assertIn("third version", updated)

    def test_create_platform_requires_explicit_overwrite(self):
        name = f"tmp_platform_overwrite_{uuid.uuid4().hex}"
        path = AGENT_PLATFORMS_DIR / f"{name}.py"
        self._created_paths.append(path)

        first = create_platform(name=name, code=_platform_source(name, "first version"))
        self.assertTrue(first.get("ok"), first)
        original = path.read_text(encoding="utf-8")

        second = create_platform(name=name, code=_platform_source(name, "second version"))
        self.assertFalse(second.get("ok"), second)
        self.assertEqual(second.get("error_code"), "already_exists")
        self.assertTrue(second.get("overwrite_required"))
        self.assertTrue(second.get("needs"))
        self.assertEqual(path.read_text(encoding="utf-8"), original)

        third = create_platform(
            name=name,
            code=_platform_source(name, "third version"),
            overwrite=True,
        )
        self.assertTrue(third.get("ok"), third)
        updated = path.read_text(encoding="utf-8")
        self.assertNotEqual(updated, original)
        self.assertIn("third version", updated)

    def test_run_meta_tool_parses_overwrite_flag_for_create_plugin(self):
        name = f"tmp_plugin_meta_overwrite_{uuid.uuid4().hex}"
        path = AGENT_PLUGINS_DIR / f"{name}.py"
        self._created_paths.append(path)

        first = run_meta_tool(
            func="create_plugin",
            args={"name": name, "code": _plugin_source(name, "first version")},
            platform="webui",
            registry={},
            enabled_predicate=None,
        )
        self.assertTrue(first.get("ok"), first)
        original = path.read_text(encoding="utf-8")

        blocked = run_meta_tool(
            func="create_plugin",
            args={"name": name, "code": _plugin_source(name, "blocked version")},
            platform="webui",
            registry={},
            enabled_predicate=None,
        )
        self.assertFalse(blocked.get("ok"), blocked)
        self.assertEqual(path.read_text(encoding="utf-8"), original)

        replaced = run_meta_tool(
            func="create_plugin",
            args={
                "name": name,
                "code": _plugin_source(name, "replacement version"),
                "overwrite": "true",
            },
            platform="webui",
            registry={},
            enabled_predicate=None,
        )
        self.assertTrue(replaced.get("ok"), replaced)
        self.assertIn("replacement version", path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
