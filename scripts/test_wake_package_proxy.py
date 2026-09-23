from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest import mock
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tater_voice import native_live_settings, native_satellite, wake_package_proxy


class WakePackageProxyTests(unittest.TestCase):
    def setUp(self) -> None:
        wake_package_proxy._PACKAGES.clear()

    def test_echo_firmware_settings_use_tater_lan_proxy(self) -> None:
        selector = "native:echo-test"
        source_url = "https://models.example/jojo.json"
        native_satellite._clients[selector] = {"client_host": "10.4.20.238"}
        settings = {"wake_word": "custom_url", "wake_word_url": source_url}
        vp = mock.Mock()
        vp._service_base_url_for_peer.return_value = "http://10.4.20.210:8501"

        with (
            mock.patch.object(native_live_settings, "firmware_settings_snapshot", return_value=dict(settings)),
            mock.patch.object(native_satellite, "_vp", return_value=vp),
        ):
            output = native_satellite._firmware_settings_payload(selector, board="biscuit")

        parsed = urlsplit(output["wake_word_url"])
        self.assertEqual("http", parsed.scheme)
        self.assertEqual("10.4.20.210:8501", parsed.netloc)
        self.assertRegex(parsed.path, r"/wake-package/[0-9a-f]{32}/manifest\.json$")
        package_id = parsed.path.split("/")[-2]
        self.assertEqual(source_url, wake_package_proxy._PACKAGES[package_id]["manifest_url"])

    def test_non_echo_settings_keep_original_wake_url(self) -> None:
        settings = {"wake_word": "custom_url", "wake_word_url": "https://models.example/jojo.json"}
        with mock.patch.object(
            native_live_settings,
            "firmware_settings_snapshot",
            return_value=dict(settings),
        ):
            output = native_satellite._firmware_settings_payload("native:s3", board="s3-box")
        self.assertEqual(settings, output)

    def test_manifest_and_model_are_exposed_as_valid_local_bundle(self) -> None:
        source_url = "https://models.example/wakes/jojo.json"
        local_url = wake_package_proxy.register_manifest("http://10.4.20.210:8501", source_url)
        package_id = urlsplit(local_url).path.split("/")[-2]
        remote_manifest = {
            "type": "micro",
            "wake_word": "jojo",
            "model": "models/jojo.tflite",
            "version": 1,
        }
        tflite = b"\x1c\x00\x00\x00TFL3test-model"

        with mock.patch.object(
            wake_package_proxy,
            "_download",
            side_effect=[
                (json.dumps(remote_manifest).encode(), source_url),
                (tflite, "https://models.example/wakes/models/jojo.tflite"),
            ],
        ) as download:
            proxied_manifest = json.loads(wake_package_proxy.manifest_bytes(package_id))
            proxied_model = wake_package_proxy.model_bytes(package_id)

        self.assertEqual("model.tflite", proxied_manifest["model"])
        self.assertEqual(tflite, proxied_model)
        self.assertEqual(
            "https://models.example/wakes/models/jojo.tflite",
            download.call_args_list[1].args[0],
        )

    def test_invalid_model_is_rejected(self) -> None:
        local_url = wake_package_proxy.register_manifest(
            "http://10.4.20.210:8501",
            "https://models.example/jojo.json",
        )
        package_id = urlsplit(local_url).path.split("/")[-2]
        wake_package_proxy._PACKAGES[package_id]["model_url"] = "https://models.example/jojo.tflite"
        with mock.patch.object(wake_package_proxy, "_download", return_value=(b"not-tflite", "")):
            with self.assertRaisesRegex(wake_package_proxy.WakePackageProxyError, "TFLite"):
                wake_package_proxy.model_bytes(package_id)


if __name__ == "__main__":
    unittest.main()
