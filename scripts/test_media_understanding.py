#!/usr/bin/env python3
import base64
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import helpers  # noqa: E402
    import kernel_tools  # noqa: E402
    import media_understanding_settings  # noqa: E402
    import tool_runtime  # noqa: E402
    RUNTIME_IMPORT_ERROR = None
except ModuleNotFoundError as exc:  # Source-only developer shells may not have app dependencies installed.
    helpers = None
    kernel_tools = None
    media_understanding_settings = None
    tool_runtime = None
    RUNTIME_IMPORT_ERROR = exc


class _AliveEngine:
    def __init__(self):
        self.shutdown_calls = 0

    def alive(self):
        return True

    def shutdown(self):
        self.shutdown_calls += 1


class MediaUnderstandingTests(unittest.TestCase):
    @unittest.skipIf(RUNTIME_IMPORT_ERROR is not None, f"Tater runtime dependencies unavailable: {RUNTIME_IMPORT_ERROR}")
    def test_native_message_parser_preserves_audio_and_video_payloads(self):
        encoded = base64.b64encode(b"test-media").decode("ascii")
        content = [
            {"type": "text", "text": "Analyze both."},
            {"type": "input_audio", "input_audio": {"data": encoded, "format": "wav"}},
            {"type": "input_video", "input_video": {"data": encoded, "format": "mp4"}},
        ]
        text, media = helpers._llama_cpp_native_message_text_and_media(content)
        self.assertIn("Analyze both.", text)
        self.assertEqual(media, [encoded, encoded])

    @unittest.skipIf(RUNTIME_IMPORT_ERROR is not None, f"Tater runtime dependencies unavailable: {RUNTIME_IMPORT_ERROR}")
    def test_payload_kind_detects_all_supported_modalities(self):
        self.assertEqual(helpers._multimodal_payload_kind({"type": "input_audio"}), "audio")
        self.assertEqual(helpers._multimodal_payload_kind({"type": "input_video"}), "video")
        self.assertEqual(helpers._multimodal_payload_kind({"type": "image_url"}), "vision")

    @unittest.skipIf(RUNTIME_IMPORT_ERROR is not None, f"Tater runtime dependencies unavailable: {RUNTIME_IMPORT_ERROR}")
    def test_same_as_base_reuses_alive_audio_capable_engine(self):
        model = "owner/audio-model::model.gguf"
        cache_key = helpers._llama_cpp_engine_cache_key(model)
        engine = _AliveEngine()
        cached = {"engine": engine, "supports_audio": True, "runtime": "test"}
        with helpers._LLAMA_CPP_ENGINE_CACHE_LOCK:
            previous = helpers._LLAMA_CPP_ENGINE_CACHE.get(cache_key)
            helpers._LLAMA_CPP_ENGINE_CACHE[cache_key] = cached
        try:
            loaded = helpers._load_llama_cpp_engine_bundle(model, media_kind="audio")
            self.assertIs(loaded, cached)
            self.assertEqual(engine.shutdown_calls, 0)
        finally:
            with helpers._LLAMA_CPP_ENGINE_CACHE_LOCK:
                if previous is None:
                    helpers._LLAMA_CPP_ENGINE_CACHE.pop(cache_key, None)
                else:
                    helpers._LLAMA_CPP_ENGINE_CACHE[cache_key] = previous

    @unittest.skipIf(RUNTIME_IMPORT_ERROR is not None, f"Tater runtime dependencies unavailable: {RUNTIME_IMPORT_ERROR}")
    def test_incapable_base_is_not_restarted_or_duplicated(self):
        model = "owner/text-model::model.gguf"
        cache_key = helpers._llama_cpp_engine_cache_key(model)
        engine = _AliveEngine()
        cached = {"engine": engine, "supports_audio": False, "runtime": "test"}
        with helpers._LLAMA_CPP_ENGINE_CACHE_LOCK:
            previous = helpers._LLAMA_CPP_ENGINE_CACHE.get(cache_key)
            helpers._LLAMA_CPP_ENGINE_CACHE[cache_key] = cached
        try:
            with self.assertRaisesRegex(RuntimeError, "does not advertise support for audio"):
                helpers._load_llama_cpp_engine_bundle(model, media_kind="audio")
            self.assertEqual(engine.shutdown_calls, 0)
        finally:
            with helpers._LLAMA_CPP_ENGINE_CACHE_LOCK:
                if previous is None:
                    helpers._LLAMA_CPP_ENGINE_CACHE.pop(cache_key, None)
                else:
                    helpers._LLAMA_CPP_ENGINE_CACHE[cache_key] = previous

    @unittest.skipIf(RUNTIME_IMPORT_ERROR is not None, f"Tater runtime dependencies unavailable: {RUNTIME_IMPORT_ERROR}")
    def test_media_tools_are_kernel_tools(self):
        self.assertTrue(tool_runtime.is_meta_tool("audio_analyze"))
        self.assertTrue(tool_runtime.is_meta_tool("video_analyze"))
        self.assertIn("STT", tool_runtime.kernel_tool_purpose_hint(tool_id="audio_analyze", platform="webui"))

    @unittest.skipIf(RUNTIME_IMPORT_ERROR is not None, f"Tater runtime dependencies unavailable: {RUNTIME_IMPORT_ERROR}")
    def test_media_settings_default_to_same_as_base(self):
        fake_redis = mock.Mock()
        fake_redis.hgetall.return_value = {}
        with mock.patch.object(media_understanding_settings, "redis_client", fake_redis):
            audio = media_understanding_settings.get_media_understanding_settings("audio")
            video = media_understanding_settings.get_media_understanding_settings("video")
        self.assertEqual(audio["mode"], "base")
        self.assertEqual(video["mode"], "base")
        self.assertEqual(audio["max_seconds"], 60)
        self.assertEqual(video["max_seconds"], 15)

    @unittest.skipIf(RUNTIME_IMPORT_ERROR is not None, f"Tater runtime dependencies unavailable: {RUNTIME_IMPORT_ERROR}")
    def test_explicit_audio_reference_resolves_without_io(self):
        encoded = base64.b64encode(b"RIFF-test").decode("ascii")
        data, filename, mime, source, error = kernel_tools._media_understanding_resolve(
            "audio",
            media_ref={"name": "sample.wav", "mimetype": "audio/wav", "data": encoded},
        )
        self.assertEqual(data, b"RIFF-test")
        self.assertEqual(filename, "sample.wav")
        self.assertEqual(mime, "audio/wav")
        self.assertEqual(source, "explicit_ref")
        self.assertEqual(error, "")

    def test_models_ui_groups_image_and_video_under_vision_tab(self):
        settings = (ROOT / "frontend" / "src" / "settings" / "components" / "ModelsSettings.vue").read_text(encoding="utf-8")
        card = (ROOT / "frontend" / "src" / "settings" / "components" / "models" / "MediaModelCard.vue").read_text(encoding="utf-8")
        self.assertIn('{ id: "vision", label: "Vision"', settings)
        self.assertIn('{ id: "audio-understanding", label: "Audio & Video"', settings)
        self.assertNotIn('id: "video-understanding"', settings)
        self.assertIn('kind="vision"', settings)
        self.assertIn('kind="audio"', settings)
        self.assertIn('kind="video"', settings)
        self.assertIn('{ value: "base", label: "Same as Base"', card)
        self.assertIn("function chooseMode", card)

    def test_models_tabs_follow_the_user_workflow_order(self):
        source = (ROOT / "frontend" / "src" / "settings" / "components" / "ModelsSettings.vue").read_text(encoding="utf-8")
        expected_tabs = (
            '{ id: "huggingface", label: "Hugging Face"',
            '{ id: "routing", label: "LLM"',
            '{ id: "speech", label: "Speech"',
            '{ id: "wake", label: "Wake Word"',
            '{ id: "vision", label: "Vision"',
            '{ id: "audio-understanding", label: "Audio & Video"',
            '{ id: "speakerid", label: "Speaker ID"',
            '{ id: "emotionid", label: "Emotion ID"',
            '{ id: "faceid", label: "Face ID"',
        )
        positions = [source.index(tab) for tab in expected_tabs]
        self.assertEqual(positions, sorted(positions))
        self.assertIn('v-for="tab in tabs"', source)
        self.assertIn('@click="select(tab.id)"', source)

    def test_runtime_source_routes_modalities_and_reuses_cache(self):
        source = (ROOT / "helpers.py").read_text(encoding="utf-8")
        self.assertIn('supports_audio = bool(modalities.get("audio"))', source)
        self.assertIn('supports_video = bool(modalities.get("video"))', source)
        self.assertIn('media_kind or _multimodal_payload_kind(messages)', source)
        self.assertIn('return cached', source)
        self.assertIn('"supports_audio": bool((bundle or {}).get("supports_audio"))', source)
        self.assertIn('"supports_video": bool((bundle or {}).get("supports_video"))', source)

    def test_active_dedicated_media_models_are_part_of_startup_warmup(self):
        source = (ROOT / "tateros_app.py").read_text(encoding="utf-8")
        self.assertIn("def _active_local_llm_startup_targets", source)
        self.assertIn('*_current_media_local_warmup_targets("audio")', source)
        self.assertIn('*_current_media_local_warmup_targets("video")', source)
        self.assertIn("models = _active_local_llm_startup_targets(rows)", source)
        self.assertIn('media_kind=requested_media_kind', source)
        self.assertIn('if str(settings.get("mode") or "").strip().lower() != "dedicated"', source)

    def test_save_and_load_targets_keep_roles_and_media_capabilities(self):
        source = (ROOT / "frontend" / "src" / "settings" / "components" / "ModelsSettings.vue").read_text(encoding="utf-8")
        self.assertIn('roles: [kind === "vision" ? "Image" : kind === "audio" ? "Audio" : "Video"]', source)
        self.assertIn("media_kinds: [kind]", source)
        self.assertIn('["roles", "media_kinds"].forEach', source)

    def test_runtime_stats_include_selected_model_roles(self):
        source = (ROOT / "tateros_app.py").read_text(encoding="utf-8")
        self.assertIn("def _runtime_local_llm_roles", source)
        self.assertIn("def _runtime_managed_tts_worker_rows", source)
        self.assertIn("managed_tts_workers_snapshot()", source)
        self.assertIn('role_detail = f"Roles {\', \'.join(roles)}"', source)

    def test_kernel_tools_are_registered_with_llm_guidance(self):
        source = (ROOT / "tool_runtime.py").read_text(encoding="utf-8")
        self.assertIn('"audio_analyze"', source)
        self.assertIn('"video_analyze"', source)
        self.assertIn("use STT for plain transcription", source)


if __name__ == "__main__":
    unittest.main()
