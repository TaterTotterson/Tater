from __future__ import annotations

import asyncio
import hashlib
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
        wake_package_proxy._PACKAGE_FETCH_LOCKS.clear()

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
        ), mock.patch.object(
            native_live_settings,
            "shared_wake_settings_snapshot",
            return_value={"wake_detector_mode": "mww"},
        ):
            output = native_satellite._firmware_settings_payload("native:s3", board="s3-box")
        self.assertEqual({**settings, "output_channel_mode": "stereo"}, output)

    def test_mww_only_satellite_gets_the_mww_half_of_shared_oww_bundle(self) -> None:
        selector = "native:esp-test"
        source_url = "https://trainer.example/jarvis.wake-bundle.json"
        native_satellite._clients[selector] = {"client_host": "10.4.20.239"}
        settings = {"wake_word": "custom_url", "wake_word_url": "https://models.example/jarvis.json"}
        shared = {
            "wake_detector_mode": "oww",
            "oww_wake_word": "custom_url",
            "oww_wake_word_url": source_url,
        }
        vp = mock.Mock()
        vp._service_base_url_for_peer.return_value = "http://10.4.20.210:8501"
        with (
            mock.patch.object(native_live_settings, "firmware_settings_snapshot", return_value=dict(settings)),
            mock.patch.object(native_live_settings, "shared_wake_settings_snapshot", return_value=shared),
            mock.patch.object(native_satellite, "_vp", return_value=vp),
        ):
            output = native_satellite._firmware_settings_payload(selector, board="s3-box")

        self.assertEqual(output["wake_word"], "custom_url")
        self.assertTrue(output["wake_word_url"].endswith("/manifest.json"))
        self.assertNotIn("oww_wake_word", output)
        package_id = urlsplit(output["wake_word_url"]).path.split("/")[-2]
        self.assertEqual(wake_package_proxy._PACKAGES[package_id]["bundle_url"], source_url)

    def test_echo_custom_oww_bundle_uses_tater_lan_proxy(self) -> None:
        selector = "native:echo-test"
        source_url = "https://models.example/jojo.wake-bundle.json"
        native_satellite._clients[selector] = {"client_host": "10.4.20.238"}
        settings = {
            "wake_word": "hey_tater",
            "oww_wake_word": "custom_url",
            "oww_wake_word_url": source_url,
        }
        vp = mock.Mock()
        vp._service_base_url_for_peer.return_value = "http://10.4.20.210:8501"
        with (
            mock.patch.object(native_live_settings, "firmware_settings_snapshot", return_value=dict(settings)),
            mock.patch.object(native_satellite, "_vp", return_value=vp),
        ):
            output = native_satellite._firmware_settings_payload(selector, board="rook")
        parsed = urlsplit(output["oww_wake_word_url"])
        self.assertEqual(parsed.netloc, "10.4.20.210:8501")
        self.assertTrue(parsed.path.endswith("/bundle.wake-bundle.json"))

    def test_dual_mode_uses_one_bundle_for_both_detector_routes(self) -> None:
        selector = "native:echo-test"
        native_satellite._clients[selector] = {"client_host": "10.4.20.238"}
        settings = {
            "wake_mww_enabled": True,
            "wake_oww_enabled": True,
            "wake_word": "custom_url",
            "wake_word_url": "https://models.example/accidentally-different.json",
            "oww_wake_word": "custom_url",
            "oww_wake_word_url": "https://trainer.example/jojo.wake-bundle.json",
        }
        vp = mock.Mock()
        vp._service_base_url_for_peer.return_value = "http://10.4.20.210:8501"
        with (
            mock.patch.object(native_live_settings, "firmware_settings_snapshot", return_value=dict(settings)),
            mock.patch.object(native_satellite, "_vp", return_value=vp),
        ):
            output = native_satellite._firmware_settings_payload(selector, board="biscuit")

        manifest_parts = urlsplit(output["wake_word_url"]).path.split("/")
        bundle_parts = urlsplit(output["oww_wake_word_url"]).path.split("/")
        self.assertEqual(manifest_parts[-2], bundle_parts[-2])
        self.assertEqual(output["oww_wake_word"], "paired_bundle")
        self.assertTrue(output["wake_word_url"].endswith("/manifest.json"))
        self.assertTrue(output["oww_wake_word_url"].endswith("/bundle.wake-bundle.json"))

    def test_oww_where_supported_uses_standalone_bundle_and_omits_mww_model(self) -> None:
        selector = "native:thirdreality-test"
        native_satellite._clients[selector] = {"client_host": "10.4.20.240"}
        settings = {
            "wake_detector_mode": "oww",
            "wake_mww_enabled": False,
            "wake_oww_enabled": True,
            "wake_word": "custom_url",
            "wake_word_url": "https://models.example/jojo.json",
            "oww_wake_word": "custom_url",
            "oww_wake_word_url": "https://trainer.example/jojo.wake-bundle.json",
        }
        vp = mock.Mock()
        vp._service_base_url_for_peer.return_value = "http://10.4.20.210:8501"
        with (
            mock.patch.object(native_live_settings, "firmware_settings_snapshot", return_value=dict(settings)),
            mock.patch.object(native_satellite, "_vp", return_value=vp),
        ):
            output = native_satellite._firmware_settings_payload(
                selector,
                board="s420",
                capabilities={"openwakeword": True, "wake_detector_selection": True},
            )

        self.assertEqual(output["wake_detector_mode"], "oww")
        self.assertFalse(output["wake_mww_enabled"])
        self.assertTrue(output["wake_oww_enabled"])
        self.assertNotIn("wake_word", output)
        self.assertNotIn("wake_word_url", output)
        self.assertEqual(output["oww_wake_word"], "custom_url")
        self.assertTrue(output["oww_wake_word_url"].endswith("/bundle.wake-bundle.json"))

    def test_explicit_mww_mode_ignores_stale_dual_flags_and_bundle(self) -> None:
        selector = "native:echo-test"
        source_url = "https://models.example/jarvis.json"
        native_satellite._clients[selector] = {"client_host": "10.4.20.238"}
        settings = {
            "wake_detector_mode": "mww",
            "wake_mww_enabled": True,
            "wake_oww_enabled": True,
            "wake_word": "custom_url",
            "wake_word_url": source_url,
            "oww_wake_word": "custom_url",
            "oww_wake_word_url": "https://models.example/stale.wake-bundle.json",
        }
        vp = mock.Mock()
        vp._service_base_url_for_peer.return_value = "http://10.4.20.210:8501"
        with (
            mock.patch.object(native_live_settings, "firmware_settings_snapshot", return_value=dict(settings)),
            mock.patch.object(native_satellite, "_vp", return_value=vp),
        ):
            output = native_satellite._firmware_settings_payload(selector, board="biscuit")

        self.assertEqual(output["wake_detector_mode"], "mww")
        self.assertTrue(output["wake_mww_enabled"])
        self.assertFalse(output["wake_oww_enabled"])
        self.assertNotIn("oww_wake_word", output)
        self.assertNotIn("oww_wake_word_url", output)
        package_id = urlsplit(output["wake_word_url"]).path.split("/")[-2]
        self.assertEqual(source_url, wake_package_proxy._PACKAGES[package_id]["manifest_url"])
        self.assertNotIn("mww_manifest_sha256", wake_package_proxy._PACKAGES[package_id])

    def test_deferred_family_save_does_not_push_intermediate_settings(self) -> None:
        async def exercise() -> dict[str, object]:
            with (
                mock.patch.object(
                    native_live_settings,
                    "save_wake_family_settings",
                    return_value={"ok": True, "settings": {"wake_detector_mode": "mww"}},
                ),
                mock.patch.object(native_satellite, "push_live_settings", new=mock.AsyncMock()) as push,
            ):
                result = await native_satellite.save_live_settings(
                    {"wake_detector_mode": "mww"},
                    wake_family="echo",
                    push=False,
                )
                push.assert_not_awaited()
                return result

        result = asyncio.run(exercise())
        self.assertTrue(result["push"]["deferred"])
        self.assertEqual(result["push"]["count"], 0)

    def test_oww_bundle_and_artifacts_are_rewritten_to_local_routes(self) -> None:
        source_url = "https://models.example/wakes/jojo.wake-bundle.json"
        local_url = wake_package_proxy.register_bundle("http://10.4.20.210:8501", source_url)
        package_id = urlsplit(local_url).path.split("/")[-2]
        manifest = json.dumps({"wake_word": "Jojo", "model": "jojo.tflite"}).encode()
        metadata = b'{"format":"openwakeword"}'
        model = b"\x08\x07onnx-model"
        bundle = {
            "schema_version": 1,
            "type": "tater_wake_word_bundle",
            "wake_word": "Jojo",
            "micro_wake_word": {
                "manifest": "jojo.json",
                "model": "jojo.tflite",
                "manifest_sha256": hashlib.sha256(manifest).hexdigest(),
                "model_sha256": "b" * 64,
            },
            "open_wake_word": {
                "metadata": "jojo.oww.json",
                "metadata_sha256": hashlib.sha256(metadata).hexdigest(),
                "recommended_threshold": 0.95,
                "recommended_patience": 3,
                "recommended_confirmation_threshold": 0.90,
                "recommended_confirmation_patience": 3,
                "artifacts": {"onnx": {"file": "jojo.oww.onnx", "sha256": hashlib.sha256(model).hexdigest(), "size_bytes": len(model)}},
            },
        }
        with mock.patch.object(
            wake_package_proxy,
            "_download",
            side_effect=[
                (json.dumps(bundle).encode(), source_url),
                (manifest, "https://models.example/wakes/jojo.json"),
                (metadata, "https://models.example/wakes/jojo.oww.json"),
                (model, "https://models.example/wakes/jojo.oww.onnx"),
            ],
        ) as download:
            proxied = json.loads(wake_package_proxy.bundle_bytes(package_id))
            self.assertEqual(wake_package_proxy.oww_metadata_bytes(package_id), metadata)
            self.assertEqual(wake_package_proxy.oww_model_bytes(package_id), model)
            repeated_url = wake_package_proxy.register_bundle("http://10.4.20.210:8501", source_url)
            self.assertEqual(repeated_url, local_url)
            self.assertEqual(json.loads(wake_package_proxy.bundle_bytes(package_id)), proxied)
            self.assertEqual(wake_package_proxy.oww_metadata_bytes(package_id), metadata)
            self.assertEqual(wake_package_proxy.oww_model_bytes(package_id), model)
            self.assertEqual(download.call_count, 4)
        self.assertEqual(proxied["open_wake_word"]["metadata"], "oww-metadata.json")
        self.assertEqual(proxied["open_wake_word"]["artifacts"]["onnx"]["file"], "oww-model.onnx")
        self.assertEqual(proxied["open_wake_word"]["recommended_threshold"], 0.95)
        self.assertEqual(proxied["open_wake_word"]["recommended_confirmation_threshold"], 0.90)
        self.assertEqual(proxied["micro_wake_word"]["manifest"], "manifest.json")
        self.assertEqual(proxied["micro_wake_word"]["model"], "model.tflite")
        proxied_manifest = json.dumps(
            {"wake_word": "Jojo", "model": "model.tflite"},
            separators=(",", ":"),
        ).encode()
        self.assertEqual(
            proxied["micro_wake_word"]["manifest_sha256"],
            hashlib.sha256(proxied_manifest).hexdigest(),
        )

    def test_schema_three_nww_models_are_rewritten_and_verified(self) -> None:
        source_url = "https://trainer.example/wakes/hey_tater.wake-bundle.json"
        local_url = wake_package_proxy.register_bundle("http://10.4.20.210:8501", source_url)
        package_id = urlsplit(local_url).path.split("/")[-2]
        tflite = b"\x1c\x00\x00\x00TFL3nano-fallback"
        cnn = b"\x08\x07cnn-onnx-model"
        gru = b"\x08\x07gru-onnx-model"
        bundle = {
            "schema_version": 3,
            "type": "tater_wake_word_bundle",
            "wake_word": "Hey Tater",
            "key": "hey_tater",
            "micro_wake_word": {
                "type": "micro",
                "wake_word": "Hey Tater",
                "model": "hey_tater.tflite",
                "model_sha256": hashlib.sha256(tflite).hexdigest(),
                "model_size_bytes": len(tflite),
                "version": 2,
                "micro": {"probability_cutoff": 0.82, "sliding_window_size": 4},
            },
            "nano_wake_word": {
                "default_architecture": "cnn",
                "variants": {
                    "cnn": {
                        "architecture": "cnn",
                        "model": "hey_tater.nww.cnn.onnx",
                        "model_sha256": hashlib.sha256(cnn).hexdigest(),
                        "model_size_bytes": len(cnn),
                        "recommended_threshold": 0.72,
                        "recommended_patience": 2,
                    },
                    "gru": {
                        "architecture": "gru",
                        "model": "hey_tater.nww.gru.onnx",
                        "model_sha256": hashlib.sha256(gru).hexdigest(),
                        "model_size_bytes": len(gru),
                        "recommended_threshold": 0.76,
                        "recommended_patience": 3,
                    },
                },
            },
        }
        with mock.patch.object(
            wake_package_proxy,
            "_download",
            side_effect=[
                (json.dumps(bundle).encode(), source_url),
                (cnn, "https://trainer.example/wakes/hey_tater.nww.cnn.onnx"),
                (gru, "https://trainer.example/wakes/hey_tater.nww.gru.onnx"),
            ],
        ):
            proxied = json.loads(wake_package_proxy.bundle_bytes(package_id))
            self.assertEqual(wake_package_proxy.nww_model_bytes(package_id, "cnn"), cnn)
            self.assertEqual(wake_package_proxy.nww_model_bytes(package_id, "gru"), gru)

        variants = proxied["nano_wake_word"]["variants"]
        self.assertEqual(variants["cnn"]["model"], "nww-cnn-model.onnx")
        self.assertEqual(variants["gru"]["model"], "nww-gru-model.onnx")
        manifest = json.loads(wake_package_proxy.manifest_bytes(package_id))
        self.assertEqual(manifest["model"], "model.tflite")
        self.assertEqual(manifest["micro"]["probability_cutoff"], 0.82)

    def test_mww_nww_mode_uses_one_proxied_bundle_for_both_models(self) -> None:
        selector = "native:echo-test"
        source_url = "https://trainer.example/hey_tater.wake-bundle.json"
        native_satellite._clients[selector] = {"client_host": "10.4.20.238"}
        settings = {
            "wake_detector_mode": "mww_nww",
            "wake_mww_enabled": True,
            "wake_oww_enabled": False,
            "wake_nww_enabled": True,
            "wake_word": "custom_url",
            "wake_word_url": source_url,
            "nww_wake_word": "custom_url",
            "nww_wake_word_url": source_url,
            "nww_architecture": "gru",
        }
        vp = mock.Mock()
        vp._service_base_url_for_peer.return_value = "http://10.4.20.210:8501"
        with (
            mock.patch.object(native_live_settings, "firmware_settings_snapshot", return_value=dict(settings)),
            mock.patch.object(native_satellite, "_vp", return_value=vp),
        ):
            output = native_satellite._firmware_settings_payload(
                selector,
                board="biscuit",
                capabilities={"nanowakeword": True, "wake_detector_selection": True},
            )

        manifest_parts = urlsplit(output["wake_word_url"]).path.split("/")
        bundle_parts = urlsplit(output["nww_wake_word_url"]).path.split("/")
        self.assertEqual(manifest_parts[-2], bundle_parts[-2])
        self.assertTrue(output["wake_word_url"].endswith("/manifest.json"))
        self.assertTrue(output["nww_wake_word_url"].endswith("/bundle.wake-bundle.json"))
        self.assertEqual(output["nww_architecture"], "gru")

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
