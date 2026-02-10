import json
import unittest
import uuid
from unittest.mock import patch

from kernel_tools import vision_describer
from tool_runtime import is_meta_tool, run_meta_tool
from kernel_tools import AGENT_DOWNLOADS_DIR


class _FakeTextRedis:
    def __init__(self, lists=None):
        self.lists = lists or {}

    def lrange(self, key, start, end):
        items = list(self.lists.get(key, []))
        n = len(items)
        if n == 0:
            return []
        if start < 0:
            start = n + start
        if end < 0:
            end = n + end
        start = max(0, start)
        end = min(end, n - 1)
        if end < start:
            return []
        return items[start : end + 1]


class _FakeBlobRedis:
    def __init__(self, data=None):
        self.data = data or {}

    def get(self, key):
        return self.data.get(key)


class _FakeResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self, _max_bytes=None):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class KernelToolsVisionDescriberTests(unittest.TestCase):
    def test_vision_describer_reads_origin_latest_image_ref_and_calls_vision_api(self):
        fake_text = _FakeTextRedis()
        fake_blob = _FakeBlobRedis(data={"tater:blob:test-image": b"\x89PNG\r\n\x1a\nfake"})
        captured = {}

        def _fake_urlopen(req, timeout=0):
            captured["url"] = req.full_url
            captured["timeout"] = timeout
            captured["payload"] = json.loads(req.data.decode("utf-8"))
            captured["auth"] = req.headers.get("Authorization")
            payload = {
                "choices": [
                    {
                        "message": {
                            "content": "A person is standing at the front door."
                        }
                    }
                ]
            }
            return _FakeResponse(json.dumps(payload).encode("utf-8"))

        with patch("kernel_tools.redis_client", fake_text), patch(
            "kernel_tools._get_blob_redis_client", return_value=fake_blob
        ), patch(
            "kernel_tools.get_shared_vision_settings",
            return_value={
                "api_base": "http://vision.local:1234",
                "model": "qwen-vision",
                "api_key": "secret-key",
            },
        ), patch(
            "kernel_tools.urllib.request.urlopen",
            side_effect=_fake_urlopen,
        ):
            result = vision_describer(
                platform="discord",
                origin={
                    "platform": "discord",
                    "channel_id": "123",
                    "latest_image_ref": {
                        "blob_key": "tater:blob:test-image",
                        "name": "front.png",
                        "mimetype": "image/png",
                    },
                },
                prompt="Describe who is at the door.",
            )

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("source"), "origin.latest_image_ref")
        self.assertEqual(result.get("description"), "A person is standing at the front door.")
        self.assertEqual(captured.get("url"), "http://vision.local:1234/v1/chat/completions")
        self.assertEqual(captured.get("timeout"), 90)
        self.assertEqual(captured.get("auth"), "Bearer secret-key")
        self.assertEqual(captured["payload"]["model"], "qwen-vision")
        self.assertIn("Describe who is at the door.", json.dumps(captured["payload"]))

    def test_vision_describer_returns_need_when_no_source(self):
        fake_text = _FakeTextRedis()
        with patch("kernel_tools.redis_client", fake_text), patch(
            "kernel_tools._get_blob_redis_client", return_value=_FakeBlobRedis()
        ), patch(
            "kernel_tools.get_shared_vision_settings",
            return_value={"api_base": "http://vision.local:1234", "model": "qwen-vision", "api_key": ""},
        ):
            result = vision_describer()

        self.assertFalse(result.get("ok"), result)
        self.assertIn("No image source was provided", result.get("error", ""))
        self.assertTrue(result.get("needs"), result)

    def test_vision_describer_reads_local_path(self):
        AGENT_DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
        filename = f"vision_test_{uuid.uuid4().hex}.png"
        path = AGENT_DOWNLOADS_DIR / filename
        path.write_bytes(b"\x89PNG\r\n\x1a\nfake")
        captured = {}

        def _fake_urlopen(req, timeout=0):
            captured["payload"] = json.loads(req.data.decode("utf-8"))
            payload = {"choices": [{"message": {"content": "local ok"}}]}
            return _FakeResponse(json.dumps(payload).encode("utf-8"))

        try:
            with patch("kernel_tools.urllib.request.urlopen", side_effect=_fake_urlopen), patch(
                "kernel_tools.get_shared_vision_settings",
                return_value={"api_base": "http://vision.local:1234", "model": "qwen-vision", "api_key": ""},
            ):
                result = vision_describer(path=str(path))
        finally:
            try:
                path.unlink()
            except Exception:
                pass

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("source"), "path")
        self.assertEqual(captured["payload"]["model"], "qwen-vision")

    def test_meta_runtime_routes_vision_describer(self):
        self.assertTrue(is_meta_tool("vision_describer"))
        with patch(
            "tool_runtime.vision_describer",
            return_value={"tool": "vision_describer", "ok": True, "description": "ok"},
        ) as mocked:
            result = run_meta_tool(
                func="vision_describer",
                args={"prompt": "describe"},
                platform="discord",
                registry={},
                enabled_predicate=None,
                origin={"platform": "discord", "channel_id": "42"},
            )

        self.assertTrue(result.get("ok"), result)
        mocked.assert_called_once()
        kwargs = mocked.call_args.kwargs
        self.assertEqual(kwargs.get("platform"), "discord")
        self.assertEqual(kwargs.get("prompt"), "describe")
        self.assertEqual(kwargs.get("origin"), {"platform": "discord", "channel_id": "42"})


if __name__ == "__main__":
    unittest.main()
