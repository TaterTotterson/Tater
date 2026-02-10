import shutil
import unittest
import uuid
import zipfile
import importlib.util
from pathlib import Path

from kernel_tools import (
    AGENT_DOCUMENTS_DIR,
    AGENT_DOWNLOADS_DIR,
    AGENT_WORKSPACE_DIR,
    extract_archive,
    list_archive,
    search_files,
)


class KernelToolsSearchArchiveTests(unittest.TestCase):
    def setUp(self):
        AGENT_DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
        AGENT_DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
        AGENT_WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)
        self._cleanup_paths = []

    def tearDown(self):
        for path in self._cleanup_paths:
            try:
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
                elif path.exists():
                    path.unlink()
            except Exception:
                pass

    def _mkfile(self, parent: Path, suffix: str, content: str) -> Path:
        path = parent / f"tmp_kernel_tools_{uuid.uuid4().hex}{suffix}"
        path.write_text(content, encoding="utf-8")
        self._cleanup_paths.append(path)
        return path

    def test_search_files_finds_match(self):
        doc = self._mkfile(AGENT_DOCUMENTS_DIR, ".txt", "alpha needle bravo")
        result = search_files("needle", path="documents", max_results=10)
        self.assertTrue(result.get("ok"), result)
        self.assertGreaterEqual(result.get("count", 0), 1)
        hits = [x.get("path", "") for x in result.get("results", [])]
        self.assertIn(str(doc), hits)

    def test_archive_list_and_extract_zip_with_unsafe_entry(self):
        archive = AGENT_DOWNLOADS_DIR / f"tmp_archive_{uuid.uuid4().hex}.zip"
        self._cleanup_paths.append(archive)
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("safe/hello.txt", "hello")
            zf.writestr("../evil.txt", "evil")

        listed = list_archive(f"downloads/{archive.name}")
        self.assertTrue(listed.get("ok"), listed)
        names = [e.get("name") for e in listed.get("entries", [])]
        self.assertIn("safe/hello.txt", names)

        dest_rel = f"workspace/extract_{uuid.uuid4().hex}"
        dest_abs = AGENT_WORKSPACE_DIR / dest_rel.split("/", 1)[1]
        self._cleanup_paths.append(dest_abs)

        extracted = extract_archive(f"downloads/{archive.name}", destination=dest_rel)
        self.assertTrue(extracted.get("ok"), extracted)
        self.assertEqual(extracted.get("extracted_count"), 1)
        self.assertTrue((dest_abs / "safe" / "hello.txt").exists())
        self.assertFalse((dest_abs / "evil.txt").exists())
        skipped = extracted.get("skipped", [])
        self.assertTrue(any(item.get("reason") == "unsafe_path" for item in skipped), skipped)

    def test_extract_archive_default_destination_removed_when_empty(self):
        archive = AGENT_DOWNLOADS_DIR / f"tmp_archive_{uuid.uuid4().hex}.zip"
        self._cleanup_paths.append(archive)
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("../evil.txt", "evil")

        extracted = extract_archive(f"downloads/{archive.name}")
        self.assertTrue(extracted.get("ok"), extracted)
        self.assertEqual(extracted.get("extracted_count"), 0)

        dest = Path(str(extracted.get("destination", "")))
        self.assertTrue(str(dest).startswith(str(AGENT_WORKSPACE_DIR)))
        self.assertFalse(dest.exists())

    def test_extract_archive_error_cleans_default_destination(self):
        archive = AGENT_DOWNLOADS_DIR / f"tmp_archive_{uuid.uuid4().hex}.7z"
        self._cleanup_paths.append(archive)
        expected_dest = AGENT_WORKSPACE_DIR / f"extracted_{archive.stem}"
        self._cleanup_paths.append(expected_dest)
        archive.write_bytes(b"not-a-7z")

        extracted = extract_archive(f"downloads/{archive.name}")
        self.assertFalse(extracted.get("ok"), extracted)
        self.assertFalse(expected_dest.exists())

    def test_list_archive_7z_reports_dependency_or_invalid_archive(self):
        p = AGENT_DOWNLOADS_DIR / f"tmp_archive_{uuid.uuid4().hex}.7z"
        p.write_bytes(b"not-a-7z")
        self._cleanup_paths.append(p)
        result = list_archive(f"downloads/{p.name}")
        self.assertFalse(result.get("ok"), result)
        err = str(result.get("error", "")).lower()
        if importlib.util.find_spec("py7zr") is None:
            self.assertIn("py7zr", err)
        else:
            self.assertIn("7z", err)

    def test_list_archive_rar_reports_dependency_or_invalid_archive(self):
        p = AGENT_DOWNLOADS_DIR / f"tmp_archive_{uuid.uuid4().hex}.rar"
        p.write_bytes(b"not-a-rar")
        self._cleanup_paths.append(p)
        result = list_archive(f"downloads/{p.name}")
        self.assertFalse(result.get("ok"), result)
        err = str(result.get("error", "")).lower()
        if importlib.util.find_spec("rarfile") is None:
            self.assertIn("rarfile", err)
        else:
            self.assertIn("rar", err)

    def test_extract_archive_7z_reports_dependency_or_invalid_archive(self):
        p = AGENT_DOWNLOADS_DIR / f"tmp_archive_{uuid.uuid4().hex}.7z"
        p.write_bytes(b"not-a-7z")
        self._cleanup_paths.append(p)
        result = extract_archive(f"downloads/{p.name}")
        self.assertFalse(result.get("ok"), result)
        err = str(result.get("error", "")).lower()
        if importlib.util.find_spec("py7zr") is None:
            self.assertIn("py7zr", err)
        else:
            self.assertIn("7z", err)

    def test_extract_archive_rar_reports_dependency_or_invalid_archive(self):
        p = AGENT_DOWNLOADS_DIR / f"tmp_archive_{uuid.uuid4().hex}.rar"
        p.write_bytes(b"not-a-rar")
        self._cleanup_paths.append(p)
        result = extract_archive(f"downloads/{p.name}")
        self.assertFalse(result.get("ok"), result)
        err = str(result.get("error", "")).lower()
        if importlib.util.find_spec("rarfile") is None:
            self.assertIn("rarfile", err)
        else:
            self.assertIn("rar", err)


if __name__ == "__main__":
    unittest.main()
