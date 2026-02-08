import tempfile
import unittest
import uuid
from pathlib import Path

from kernel_tools import AGENT_PLUGINS_DIR, test_plugin, validate_plugin
from plugin_loader import load_plugins_from_directory


def _plugin_source(declared_name: str, plugin_name: str, usage_function: str) -> str:
    return f"""from plugin_base import ToolPlugin

class TempPlugin(ToolPlugin):
    name = "{declared_name}"
    plugin_name = "{plugin_name}"
    version = "1.0.0"
    description = "Temp plugin for validation tests."
    platforms = ["webui"]
    usage = '{{"function":"{usage_function}","arguments":{{}}}}'
    when_to_use = "Use for validation tests."
    waiting_prompt_template = "Write a short wait message. Only output that message."

    async def handle_webui(self, args, llm_client, context=None):
        return {{"ok": True}}

plugin = TempPlugin()
"""


class KernelToolsPluginValidationTests(unittest.TestCase):
    def setUp(self):
        AGENT_PLUGINS_DIR.mkdir(parents=True, exist_ok=True)
        self._paths = []

    def tearDown(self):
        for path in self._paths:
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                pass

    def _write_agent_plugin(self, file_id: str, declared_name: str, plugin_name: str) -> Path:
        path = AGENT_PLUGINS_DIR / f"{file_id}.py"
        path.write_text(
            _plugin_source(
                declared_name=declared_name,
                plugin_name=plugin_name,
                usage_function=file_id,
            ),
            encoding="utf-8",
        )
        self._paths.append(path)
        return path

    def test_validate_plugin_fails_when_declared_name_mismatches_filename(self):
        file_id = f"tmp_plugin_{uuid.uuid4().hex}"
        self._write_agent_plugin(file_id, declared_name="Hukked Jokes", plugin_name="Hukked Jokes")

        result = validate_plugin(file_id, auto_install=False)

        self.assertFalse(result.get("ok"), result)
        self.assertIn("name", result.get("missing_fields", []))
        warnings = " ".join(result.get("warnings", []))
        self.assertIn("must match filename id", warnings)

    def test_validate_plugin_passes_when_declared_name_matches_filename(self):
        file_id = f"tmp_plugin_{uuid.uuid4().hex}"
        self._write_agent_plugin(file_id, declared_name=file_id, plugin_name="Temp Plugin")

        result = validate_plugin(file_id, auto_install=False)

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("missing_fields"), [])

    def test_loader_can_use_filename_ids_for_agent_lab(self):
        file_id = f"tmp_plugin_{uuid.uuid4().hex}"
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / f"{file_id}.py"
            path.write_text(
                _plugin_source(
                    declared_name="Friendly Plugin Label",
                    plugin_name="",
                    usage_function=file_id,
                ),
                encoding="utf-8",
            )
            registry = load_plugins_from_directory(str(path.parent), id_from_filename=True)

        self.assertIn(file_id, registry)
        plugin = registry[file_id]
        self.assertEqual(plugin.name, file_id)
        self.assertEqual(getattr(plugin, "plugin_name", ""), "Friendly Plugin Label")

    def test_test_plugin_static_pass_and_live_not_run(self):
        file_id = f"tmp_plugin_{uuid.uuid4().hex}"
        self._write_agent_plugin(file_id, declared_name=file_id, plugin_name="Temp Plugin")

        result = test_plugin(file_id, platform="webui", auto_install=False)

        self.assertTrue(result.get("ok"), result)
        self.assertTrue(result.get("static_tested"), result)
        self.assertFalse(result.get("live_tested"), result)
        self.assertTrue(result.get("handler_present"), result)
        self.assertTrue(result.get("usage_parse_ok"), result)
        self.assertTrue(result.get("usage_matches_name"), result)

    def test_test_plugin_reports_missing_handler(self):
        file_id = f"tmp_plugin_{uuid.uuid4().hex}"
        self._write_agent_plugin(file_id, declared_name=file_id, plugin_name="Temp Plugin")

        result = test_plugin(file_id, platform="discord", auto_install=False)

        self.assertFalse(result.get("ok"), result)
        self.assertFalse(result.get("handler_present"), result)
        errors = " ".join(result.get("errors", []))
        self.assertIn("handle_discord", errors)


if __name__ == "__main__":
    unittest.main()
