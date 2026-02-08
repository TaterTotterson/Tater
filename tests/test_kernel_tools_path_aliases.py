import unittest
import uuid
from pathlib import Path

from kernel_tools import AGENT_DOCUMENTS_DIR, AGENT_PLUGINS_DIR, list_directory, read_file


class KernelToolsPathAliasTests(unittest.TestCase):
    def setUp(self):
        AGENT_DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
        AGENT_PLUGINS_DIR.mkdir(parents=True, exist_ok=True)
        self.filename = f"tmp_alias_{uuid.uuid4().hex}.txt"
        self.test_path = AGENT_DOCUMENTS_DIR / self.filename
        self.content = "agent lab alias path test"
        self.test_path.write_text(self.content, encoding="utf-8")
        self.plugin_filename = f"tmp_alias_plugin_{uuid.uuid4().hex}.py"
        self.plugin_path = AGENT_PLUGINS_DIR / self.plugin_filename
        self.plugin_content = "from plugin_base import ToolPlugin\nplugin = ToolPlugin()\n"
        self.plugin_path.write_text(self.plugin_content, encoding="utf-8")

    def tearDown(self):
        try:
            if self.test_path.exists():
                self.test_path.unlink()
        except Exception:
            pass
        try:
            if self.plugin_path.exists():
                self.plugin_path.unlink()
        except Exception:
            pass

    def test_read_file_documents_shortcut(self):
        result = read_file(f"documents/{self.filename}")
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("content"), self.content)
        self.assertEqual(Path(result.get("path", "")).resolve(), self.test_path.resolve())

    def test_read_file_agent_lab_absolute_alias(self):
        result = read_file(f"/agent_lab/documents/{self.filename}")
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("content"), self.content)
        self.assertEqual(Path(result.get("path", "")).resolve(), self.test_path.resolve())

    def test_list_directory_agent_lab_absolute_alias(self):
        result = list_directory("/agent_lab/documents")
        self.assertTrue(result.get("ok"), result)
        self.assertIn(self.filename, result.get("files", []))

    def test_read_file_skills_path_still_works(self):
        result = read_file("skills/agent_lab/plugin_authoring.md")
        self.assertTrue(result.get("ok"), result)
        normalized = str(result.get("path", "")).replace("\\", "/")
        self.assertTrue(normalized.endswith("/skills/agent_lab/plugin_authoring.md"), normalized)

    def test_read_file_plugins_shortcut_maps_to_agent_lab(self):
        result = read_file(f"plugins/{self.plugin_filename}")
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(Path(result.get("path", "")).resolve(), self.plugin_path.resolve())
        self.assertEqual(result.get("content"), self.plugin_content)

    def test_read_file_plugins_absolute_shortcut_maps_to_agent_lab(self):
        result = read_file(f"/plugins/{self.plugin_filename}")
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(Path(result.get("path", "")).resolve(), self.plugin_path.resolve())
        self.assertEqual(result.get("content"), self.plugin_content)


if __name__ == "__main__":
    unittest.main()
