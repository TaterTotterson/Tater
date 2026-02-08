import json
import unittest
import urllib.parse
from unittest.mock import Mock, patch

from kernel_tools import search_web


class _FakeResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self, _max_bytes=None):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class KernelToolsWebSearchTests(unittest.TestCase):
    def test_search_web_requires_query(self):
        result = search_web("")
        self.assertFalse(result.get("ok"), result)
        self.assertIn("query is required", result.get("error", ""))

    def test_search_web_requires_configuration(self):
        fake_redis = Mock()
        fake_redis.get.return_value = ""
        fake_redis.hgetall.return_value = {}
        with patch("kernel_tools.redis_client", fake_redis):
            result = search_web("tater")
        self.assertFalse(result.get("ok"), result)
        self.assertIn("not configured", result.get("error", "").lower())
        self.assertTrue(result.get("needs"), result)

    def test_search_web_uses_legacy_settings_fallback(self):
        fake_redis = Mock()
        fake_redis.get.return_value = ""
        fake_redis.hgetall.return_value = {
            "GOOGLE_API_KEY": "legacy_key",
            "GOOGLE_CX": "legacy_cx",
        }

        payload = {
            "items": [
                {
                    "title": "Example Result",
                    "link": "https://example.com/post",
                    "snippet": "Short snippet",
                    "displayLink": "example.com",
                }
            ],
            "searchInformation": {"searchTime": 0.42},
        }
        captured = {}

        def _fake_urlopen(req, timeout=0):
            captured["url"] = req.full_url
            captured["timeout"] = timeout
            return _FakeResponse(json.dumps(payload).encode("utf-8"))

        with patch("kernel_tools.redis_client", fake_redis), patch(
            "kernel_tools.urllib.request.urlopen", side_effect=_fake_urlopen
        ):
            result = search_web("agent lab", num_results=3, site="example.com")

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("count"), 1)
        self.assertEqual(result["results"][0]["url"], "https://example.com/post")
        self.assertEqual(captured.get("timeout"), 15)

        parsed = urllib.parse.urlparse(captured["url"])
        params = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(params.get("key"), ["legacy_key"])
        self.assertEqual(params.get("cx"), ["legacy_cx"])
        self.assertEqual(params.get("num"), ["3"])
        self.assertEqual(params.get("siteSearch"), ["example.com"])


if __name__ == "__main__":
    unittest.main()
