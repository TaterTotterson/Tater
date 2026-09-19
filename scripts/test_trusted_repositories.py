#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest
from unittest.mock import patch

from tateros import trusted_repositories


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class TrustedRepositoryDirectoryTests(unittest.TestCase):
    def setUp(self) -> None:
        trusted_repositories._cache.clear()

    def test_directory_normalizes_author_repository_and_manifest(self) -> None:
        payload = {
            "schema": 1,
            "kind": "core",
            "repositories": [
                {
                    "id": "heapsoftware-tater-jarvis-screen",
                    "name": "Jarvis Screen",
                    "repository": "Tater-Jarvis-Screen",
                    "author": {"name": "heapsoftware", "url": "https://github.com/heapsoftware"},
                    "manifest_url": "https://raw.githubusercontent.com/heapsoftware/Tater-Jarvis-Screen/main/core_manifest.json",
                    "homepage": "https://github.com/heapsoftware/Tater-Jarvis-Screen",
                    "tags": ["display", "dashboard"],
                },
                {"name": "Unsafe", "manifest_url": "http://example.com/core_manifest.json"},
            ],
        }
        with patch.object(trusted_repositories, "_fetch_directory", return_value=payload) as fetch:
            rows, errors = trusted_repositories.load_trusted_repositories("core")

        self.assertEqual(errors, [])
        self.assertEqual(rows[0]["repository"], "Tater-Jarvis-Screen")
        self.assertEqual(rows[0]["author"], "heapsoftware")
        self.assertEqual(rows[0]["author_url"], "https://github.com/heapsoftware")
        self.assertTrue(rows[0]["url"].endswith("/core_manifest.json"))
        self.assertEqual(len(rows), 1)
        fetch.assert_called_once()

    def test_directory_cache_is_used_and_is_last_known_good(self) -> None:
        payload = {"schema": 1, "kind": "portal", "repositories": []}
        with patch.object(trusted_repositories, "_fetch_directory", return_value=payload) as fetch:
            first, errors = trusted_repositories.load_trusted_repositories("portal")
            second, second_errors = trusted_repositories.load_trusted_repositories("portal")
        self.assertEqual(first, second)
        self.assertEqual(errors, [])
        self.assertEqual(second_errors, [])
        fetch.assert_called_once()

        with patch.object(trusted_repositories, "_fetch_directory", side_effect=RuntimeError("offline")):
            cached, cached_errors = trusted_repositories.load_trusted_repositories("portal", force_refresh=True)
        self.assertEqual(cached, [])
        self.assertEqual(cached_errors, [])

    def test_failed_directory_fetch_is_briefly_cached(self) -> None:
        with patch.object(trusted_repositories, "_fetch_directory", side_effect=RuntimeError("offline")) as fetch:
            first, first_errors = trusted_repositories.load_trusted_repositories("verba")
            second, second_errors = trusted_repositories.load_trusted_repositories("verba")
        self.assertEqual(first, [])
        self.assertEqual(second, [])
        self.assertTrue(first_errors)
        self.assertEqual(first_errors, second_errors)
        fetch.assert_called_once()

    def test_vue_stores_use_trusted_and_custom_repository_tabs(self) -> None:
        for relative in (
            "frontend/src/verbas/VerbasApp.vue",
            "frontend/src/portals/PortalsApp.vue",
            "frontend/src/cores/CoresApp.vue",
        ):
            source = (REPO_ROOT / relative).read_text(encoding="utf-8")
            with self.subTest(relative=relative):
                self.assertIn("Trusted repositories", source)
                self.assertIn("Custom repositories", source)
                self.assertIn("toggleTrustedRepo", source)
                self.assertIn("author_url", source)
                self.assertIn("Added to Store", source)
                self.assertIn("combinedRepos", source)

    def test_shop_payload_exposes_trusted_directories(self) -> None:
        source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        self.assertIn('trusted_repositories_module.load_trusted_repositories(kind)', source)
        for kind in ("verba", "portal", "core"):
            self.assertIn(f'_shop_repo_config_payload("{kind}",', source)


if __name__ == "__main__":
    unittest.main()
