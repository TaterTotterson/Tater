import asyncio
import unittest
from unittest.mock import patch

from plugins.obsidian_note import ObsidianNotePlugin
from plugins.obsidian_search import ObsidianSearchPlugin


class ObsidianNotePluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = ObsidianNotePlugin()

    def test_normalize_note_path_rejects_parent_traversal(self):
        path, err = self.plugin._normalize_note_path("../secrets")
        self.assertIsNone(path)
        self.assertIsNotNone(err)
        self.assertIn("..", err)

    def test_normalize_note_path_appends_md_extension(self):
        path, err = self.plugin._normalize_note_path("inbox/today")
        self.assertIsNone(err)
        self.assertEqual(path, "inbox/today.md")

    def test_parse_tags_dedupes_and_normalizes(self):
        tags = self.plugin._parse_tags("Project Notes, #project-notes\nops ready")
        self.assertEqual(tags, ["Project-Notes", "project-notes", "ops-ready"])

    def test_add_frontmatter_tags_skips_existing_frontmatter(self):
        content = "---\ntags:\n  - old\n---\nbody"
        updated = self.plugin._add_frontmatter_tags(content, ["new"])
        self.assertEqual(updated, content)

    def test_resolve_title_uses_first_line_mode(self):
        cfg = {"default_title_mode": "first_line"}
        title = asyncio.run(
            self.plugin._resolve_title(
                "# Build Notes\nDetails",
                {"title_mode": "first_line"},
                cfg,
                llm_client=None,
            )
        )
        self.assertEqual(title, "Build Notes")


class ObsidianSearchPluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = ObsidianSearchPlugin()

    def test_query_terms_remove_stopwords(self):
        terms = self.plugin._query_terms("what is the status of kitchen lights today")
        self.assertIn("status", terms)
        self.assertIn("kitchen", terms)
        self.assertNotIn("what", terms)
        self.assertNotIn("the", terms)

    def test_scan_hits_returns_ranked_matches(self):
        cfg = {}
        with patch.object(
            self.plugin,
            "_list_markdown_files",
            return_value=["notes/alpha.md", "notes/beta.md"],
        ), patch.object(
            self.plugin,
            "_read_markdown",
            side_effect=lambda path, _cfg: (
                "alpha appears twice alpha" if path.endswith("alpha.md") else "nothing useful here"
            ),
        ):
            hits, scanned_count, total_count = self.plugin._scan_hits(
                query="alpha",
                terms=["alpha"],
                cfg=cfg,
                max_files=20,
                max_note_chars=5000,
                max_results=5,
                max_snippet_chars=200,
            )

        self.assertEqual(total_count, 2)
        self.assertEqual(scanned_count, 2)
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["path"], "notes/alpha.md")
        self.assertGreater(hits[0]["score"], 0)

    def test_handle_webui_requires_query(self):
        result = asyncio.run(self.plugin.handle_webui({}, llm_client=None))
        self.assertFalse(result.get("ok"), result)
        self.assertEqual(result.get("error", {}).get("code"), "missing_query")

    def test_handle_webui_formats_hits_without_llm(self):
        fake_cfg = {
            "max_results": 6,
            "max_files_scan": 120,
            "max_note_chars": 24000,
            "max_snippet_chars": 320,
            "ai_synthesis": True,
        }
        fake_hits = [
            {
                "path": "notes/project-plan.md",
                "score": 12,
                "matched_terms": ["project", "plan"],
                "snippet": "Project plan draft and milestones.",
            }
        ]

        with patch.object(self.plugin, "_config", return_value=fake_cfg), patch.object(
            self.plugin,
            "_scan_hits",
            return_value=(fake_hits, 5, 10),
        ):
            result = asyncio.run(
                self.plugin.handle_webui(
                    {"query": "project plan", "ai_synthesis": False},
                    llm_client=None,
                )
            )

        self.assertIsInstance(result, list)
        self.assertTrue(result)
        self.assertIn("project-plan.md", result[0])
        self.assertIn("Scanned 5 of 10", result[0])


if __name__ == "__main__":
    unittest.main()
