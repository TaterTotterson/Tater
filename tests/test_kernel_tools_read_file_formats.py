import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from kernel_tools import AGENT_DOCUMENTS_DIR, read_file


class KernelToolsReadFileFormatTests(unittest.TestCase):
    def setUp(self):
        AGENT_DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
        self._paths = []

    def tearDown(self):
        for path in self._paths:
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                pass

    def _make_file(self, suffix: str, data: bytes) -> Path:
        path = AGENT_DOCUMENTS_DIR / f"tmp_read_format_{uuid.uuid4().hex}{suffix}"
        path.write_bytes(data)
        self._paths.append(path)
        return path

    def test_pdf_dispatches_to_pdf_reader(self):
        path = self._make_file(".pdf", b"%PDF-1.7\n")
        rel = f"documents/{path.name}"
        with patch("kernel_tools._read_pdf_text", return_value=("pdf content", {"format": "pdf", "pages": 2})):
            result = read_file(rel)
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("format"), "pdf")
        self.assertEqual(result.get("content"), "pdf content")
        self.assertEqual(result.get("pages"), 2)

    def test_docx_dispatches_to_docx_reader(self):
        path = self._make_file(".docx", b"PK\x03\x04")
        rel = f"documents/{path.name}"
        with patch("kernel_tools._read_docx_text", return_value=("docx content", {"format": "docx"})):
            result = read_file(rel)
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("format"), "docx")
        self.assertEqual(result.get("content"), "docx content")

    def test_xlsx_dispatches_to_xlsx_reader(self):
        path = self._make_file(".xlsx", b"PK\x03\x04")
        rel = f"documents/{path.name}"
        with patch("kernel_tools._read_xlsx_text", return_value=("xlsx content", {"format": "xlsx"})):
            result = read_file(rel)
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("format"), "xlsx")
        self.assertEqual(result.get("content"), "xlsx content")

    def test_csv_dispatches_to_csv_reader(self):
        path = self._make_file(".csv", b"a,b\n1,2\n")
        rel = f"documents/{path.name}"
        with patch("kernel_tools._read_csv_text", return_value=("csv content", {"format": "csv"})):
            result = read_file(rel)
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("format"), "csv")
        self.assertEqual(result.get("content"), "csv content")

    def test_pptx_dispatches_to_pptx_reader(self):
        path = self._make_file(".pptx", b"PK\x03\x04")
        rel = f"documents/{path.name}"
        with patch("kernel_tools._read_pptx_text", return_value=("pptx content", {"format": "pptx"})):
            result = read_file(rel)
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("format"), "pptx")
        self.assertEqual(result.get("content"), "pptx content")

    def test_read_file_chunking_window(self):
        path = self._make_file(".txt", b"abcdef")
        rel = f"documents/{path.name}"
        result = read_file(rel, start=2, max_chars=2)
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("content"), "cd")
        self.assertEqual(result.get("start"), 2)
        self.assertEqual(result.get("end"), 4)
        self.assertEqual(result.get("next_start"), 4)
        self.assertTrue(result.get("has_more"))

    def test_unknown_binary_file_returns_readable_error(self):
        path = self._make_file(".bin", b"\x00\x01binary")
        rel = f"documents/{path.name}"
        result = read_file(rel)
        self.assertFalse(result.get("ok"), result)
        self.assertIn("Binary file is not readable as plain text", result.get("error", ""))

    def test_utf16_text_file_is_decoded(self):
        payload = "hello from utf16".encode("utf-16")
        path = self._make_file(".txt", payload)
        rel = f"documents/{path.name}"
        result = read_file(rel)
        self.assertTrue(result.get("ok"), result)
        self.assertIn("hello from utf16", result.get("content", ""))


if __name__ == "__main__":
    unittest.main()
