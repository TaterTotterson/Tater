import unittest
from unittest.mock import patch

from kernel_tools import inspect_webpage
from tool_runtime import is_meta_tool, run_meta_tool


class _FakeResponse:
    def __init__(self, payload: bytes, content_type: str = "text/html; charset=utf-8", final_url: str = ""):
        self._payload = payload
        self.headers = {"Content-Type": content_type}
        self._final_url = final_url

    def read(self, max_bytes=None):
        if max_bytes is None:
            return self._payload
        return self._payload[:max_bytes]

    def geturl(self):
        return self._final_url

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class KernelToolsInspectWebpageTests(unittest.TestCase):
    def test_inspect_webpage_extracts_logo_candidate_and_latest_ref(self):
        html = b"""
        <html>
          <head>
            <title>Tater News</title>
            <meta name="description" content="Daily updates for tater fans.">
          </head>
          <body>
            <header>
              <img src="/assets/logo.svg" alt="Tater News Logo">
            </header>
            <main>
              <p>Welcome to Tater News.</p>
              <a href="/about">About</a>
            </main>
          </body>
        </html>
        """

        with patch("kernel_tools._validate_url", return_value=None), patch(
            "kernel_tools.urllib.request.urlopen",
            return_value=_FakeResponse(html, final_url="https://taternews.com"),
        ):
            result = inspect_webpage("taternews.com")

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("title"), "Tater News")
        self.assertEqual(result.get("description"), "Daily updates for tater fans.")
        self.assertEqual(result.get("best_image_url"), "https://taternews.com/assets/logo.svg")
        latest_ref = result.get("latest_image_ref") or {}
        self.assertEqual(latest_ref.get("url"), "https://taternews.com/assets/logo.svg")
        self.assertEqual(latest_ref.get("source"), "inspect_webpage")
        images = result.get("images") or []
        self.assertTrue(images)
        self.assertTrue(bool(images[0].get("logo_hint")))

    def test_inspect_webpage_rejects_non_html_content_type(self):
        with patch("kernel_tools._validate_url", return_value=None), patch(
            "kernel_tools.urllib.request.urlopen",
            return_value=_FakeResponse(b"PNG", content_type="image/png", final_url="https://cdn.example.com/logo.png"),
        ):
            result = inspect_webpage("https://cdn.example.com/logo.png")

        self.assertFalse(result.get("ok"), result)
        self.assertIn("Non-HTML content type", result.get("error", ""))

    def test_meta_runtime_routes_inspect_webpage(self):
        self.assertTrue(is_meta_tool("inspect_webpage"))
        with patch(
            "tool_runtime.inspect_webpage",
            return_value={"tool": "inspect_webpage", "ok": True, "best_image_url": "https://example.com/logo.png"},
        ) as mocked:
            result = run_meta_tool(
                func="inspect_webpage",
                args={"url": "https://example.com"},
                platform="webui",
                registry={},
                enabled_predicate=None,
                origin={"platform": "webui"},
            )

        self.assertTrue(result.get("ok"), result)
        mocked.assert_called_once()


if __name__ == "__main__":
    unittest.main()
