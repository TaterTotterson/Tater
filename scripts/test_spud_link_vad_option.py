from __future__ import annotations

import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
PIPELINE_SOURCE = (ROOT / "tater_voice" / "voice_pipeline" / "__init__.py").read_text(encoding="utf-8")
SETTINGS_SOURCE = (ROOT / "tater_voice" / "settings.py").read_text(encoding="utf-8")


def _load_vad_normalizer():
    tree = ast.parse(PIPELINE_SOURCE)
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_normalize_vad_backend"
    )
    module = ast.Module(body=[function], type_ignores=[])
    namespace = {
        "Any": object,
        "DEFAULT_VAD_BACKEND": "silero",
        "_lower": lambda value: str(value or "").strip().lower(),
    }
    exec(compile(ast.fix_missing_locations(module), "<vad-normalizer>", "exec"), namespace)
    return namespace["_normalize_vad_backend"]


class SpudLinkVadOptionTests(unittest.TestCase):
    def test_spud_hub_aliases_normalize_to_one_backend(self) -> None:
        normalize = _load_vad_normalizer()
        for value in ("spud_link", "Spud Hub", "spudhub", "hub"):
            with self.subTest(value=value):
                self.assertEqual(normalize(value), "spud_link")

    def test_vad_backend_is_a_saved_user_setting(self) -> None:
        internal_block = SETTINGS_SOURCE[
            SETTINGS_SOURCE.index("VOICE_INTERNAL_TUNING_KEYS = {") : SETTINGS_SOURCE.index(
                "REMOVED_USER_SETTING_KEYS ="
            )
        ]
        self.assertNotIn('"VOICE_VAD_BACKEND"', internal_block)

    def test_voice_settings_offer_local_vad_fallback(self) -> None:
        self.assertNotIn('{"value": "spud_link", "label": "Spud Hub (paired Spudlets)"}', SETTINGS_SOURCE)
        self.assertIn('"label": "Speech-End Detection (VAD)"', SETTINGS_SOURCE)
        model_groups = SETTINGS_SOURCE[
            SETTINGS_SOURCE.index("VOICE_MODEL_SETTING_GROUPS = [") : SETTINGS_SOURCE.index(
                "VOICE_MODEL_SETTING_KEYS ="
            )
        ]
        self.assertIn('"Voice Activity Detection"', model_groups)
        self.assertIn('"VOICE_VAD_BACKEND"', model_groups)

        app_source = (ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        speech_panel = app_source[
            app_source.index('data-models-panel="speech"') : app_source.index(
                'data-models-panel="routing"',
                app_source.index('data-models-panel="speech"'),
            )
        ]
        for tab in ("listening", "replies", "announcements", "playback"):
            self.assertIn(f'data-speech-settings-tab="{tab}"', speech_panel)
            self.assertIn(f'data-speech-settings-panel="{tab}"', speech_panel)
        self.assertIn('data-spud-link-partial-lock="speech"', speech_panel)
        self.assertIn('renderSpudLinkRouteNotice("vad", "stt", "tts")', speech_panel)
        self.assertIn("${voiceVadSettingsHtml}", speech_panel)
        self.assertLess(speech_panel.index("${voiceVadSettingsHtml}"), speech_panel.index("Speech to Text"))
        self.assertLess(speech_panel.index("Announcement TTS"), speech_panel.index("Announcement Backend"))
        self.assertLess(speech_panel.index("Announcement Backend"), speech_panel.index("Announcement Model"))
        self.assertNotIn("data-spud-link-route-locked", speech_panel[: speech_panel.index("${voiceVadSettingsHtml}")])
        self.assertIn('data-spud-link-route-locked="${spudLinkRouteUsesHub("stt") ? "true" : "false"}"', speech_panel)
        self.assertGreaterEqual(
            speech_panel.count('data-spud-link-route-locked="${spudLinkRouteUsesHub("tts") ? "true" : "false"}"'),
            2,
        )

        route_ui = app_source[
            app_source.index("const spudLinkModelRouteSpecs = [") : app_source.index(
                "const spudLinkModeLabel",
                app_source.index("const spudLinkModelRouteSpecs = ["),
            )
        ]
        self.assertIn('{ id: "vad", label: "Speech-End Detection (VAD)"', route_ui)
        self.assertIn('label === "Voice Activity Detection" && spudLinkRouteUsesHub("vad")', app_source)

    def test_vad_spud_link_route_controls_effective_backend(self) -> None:
        model_source = (ROOT / "spud_link_models.py").read_text(encoding="utf-8")
        self.assertIn('"vad",', model_source[model_source.index("MODEL_KINDS = (") : model_source.index("ROUTE_CHOICES")])
        self.assertIn('spud_link_should_use_hub("vad"', PIPELINE_SOURCE)
        self.assertIn('spud_link_route_for("vad"', PIPELINE_SOURCE)
        self.assertIn('"local_backend": local_vad_backend', PIPELINE_SOURCE)

    def test_hub_stream_can_run_endpointing_without_duplicate_stt(self) -> None:
        source = (ROOT / "tateros_app.py").read_text(encoding="utf-8")
        self.assertIn('transcribe_stream = _ws_query_bool("transcribe", True)', source)
        self.assertIn('"endpointing_only": True', source)


if __name__ == "__main__":
    unittest.main()
