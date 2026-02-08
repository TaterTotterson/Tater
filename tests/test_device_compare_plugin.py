import asyncio
import unittest
from unittest.mock import patch

from plugins.device_compare import DeviceComparePlugin


class DeviceComparePluginTests(unittest.TestCase):
    def setUp(self):
        self.plugin = DeviceComparePlugin()

    def test_extract_devices_supports_aliases_and_query(self):
        a, b = self.plugin._extract_devices({"device1": "RTX 4070", "device2": "RX 7800 XT"})
        self.assertEqual(a, "RTX 4070")
        self.assertEqual(b, "RX 7800 XT")

        a2, b2 = self.plugin._extract_devices({"query": "PS5 vs Xbox Series X"})
        self.assertEqual(a2, "PS5")
        self.assertEqual(b2, "Xbox Series X")

    def test_build_fps_rows_handles_non_dict_values(self):
        rows = self.plugin._build_fps_rows(
            {"fps_by_game": {"Cyberpunk 2077": "60 fps @ 1080p"}},
            {"fps_by_game": ["not", "a", "dict"]},
            max_rows=10,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "Cyberpunk 2077")

    def test_pipeline_requires_llm_client(self):
        with patch.object(
            self.plugin,
            "_get_settings",
            return_value={
                "api_key": "key",
                "cx": "cx",
                "spec_results": 5,
                "fps_results": 5,
                "timeout": 10,
                "enable_fps": True,
                "max_fps_rows": 20,
            },
        ):
            result = asyncio.run(self.plugin._pipeline("Device A", "Device B", llm_client=None))
        self.assertIn("error", result)
        self.assertIn("requires an available LLM client", result["error"])

    def test_handle_matrix_returns_image_payloads(self):
        fake_data = {
            "spec_headers": ["Spec", "Device A", "Device B"],
            "spec_rows": [["CPU", "A", "B"]],
            "fps_headers": ["Game", "Device A", "Device B"],
            "fps_rows": [["Cyberpunk 2077", "60 fps", "55 fps"]],
            "title": "Device A vs Device B",
            "sources_text": "- Device A (specs): https://example.com/specs-a",
        }

        with patch.object(self.plugin, "_pipeline", return_value=fake_data), patch.object(
            self.plugin,
            "_render_table_image",
            side_effect=[b"spec-image", b"fps-image"],
        ):
            result = asyncio.run(
                self.plugin.handle_matrix(
                    client=None,
                    room=None,
                    sender="@user:server",
                    body="compare",
                    args={"device_a": "Device A", "device_b": "Device B"},
                    llm_client=object(),
                )
            )

        self.assertEqual(len(result), 3)
        self.assertIsInstance(result[0], dict)
        self.assertEqual(result[0].get("type"), "image")
        self.assertEqual(result[0].get("mimetype"), "image/png")
        self.assertIsInstance(result[0].get("bytes"), (bytes, bytearray))
        self.assertIn("Sources", result[2])


if __name__ == "__main__":
    unittest.main()
