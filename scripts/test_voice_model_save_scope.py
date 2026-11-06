#!/usr/bin/env python3
from __future__ import annotations

import ast
import os
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict
from unittest import mock
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
MODELS_COMPONENT = ROOT / "frontend" / "src" / "settings" / "components" / "ModelsSettings.vue"
SETTINGS_TYPES = ROOT / "frontend" / "src" / "settings" / "types.ts"
SETTINGS_STYLES = ROOT / "frontend" / "src" / "tater-ui.css"
APP_BOOTSTRAP = ROOT / "tateros_static" / "app.js"
BACKEND_APP = ROOT / "tateros_app.py"


def _load_pocket_access_helper():
    tree = ast.parse(BACKEND_APP.read_text(encoding="utf-8"), filename=str(BACKEND_APP))
    wanted_names = {"POCKET_TTS_HF_REPO", "POCKET_TTS_HF_WEIGHT_PATHS"}
    body = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in wanted_names for target in node.targets
        ):
            body.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == "_pocket_tts_weights_access_status":
            body.append(node)
    namespace = {
        "Any": Any,
        "Dict": Dict,
        "os": os,
        "quote": quote,
        "urllib": __import__("urllib"),
    }
    exec(compile(ast.Module(body=body, type_ignores=[]), str(BACKEND_APP), "exec"), namespace)
    return namespace["_pocket_tts_weights_access_status"]


class VoiceModelSaveScopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = MODELS_COMPONENT.read_text(encoding="utf-8")
        cls.types = SETTINGS_TYPES.read_text(encoding="utf-8")
        cls.styles = SETTINGS_STYLES.read_text(encoding="utf-8")
        cls.bootstrap = APP_BOOTSTRAP.read_text(encoding="utf-8")
        cls.backend = BACKEND_APP.read_text(encoding="utf-8")

    def test_voice_panel_has_a_dedicated_save_action(self) -> None:
        self.assertIn('case "speech": return { label: "Save Speech Settings"', self.source)
        self.assertIn('activeTab.value === "speech"', self.source)
        self.assertIn("speechPayload()", self.source)

    def test_voice_scope_only_collects_speech_settings(self) -> None:
        start = self.source.index("const speechKeys = [")
        end = self.source.index("function mediaPayload", start)
        implementation = self.source[start:end]

        self.assertIn("function speechPayload", implementation)
        self.assertIn("Object.fromEntries(speechKeys.map", implementation)
        self.assertIn("esphome_settings", implementation)
        self.assertNotIn("hydra_local_model_load_targets", implementation)

    def test_every_pocket_tts_save_blocks_only_when_model_access_fails(self) -> None:
        self.assertNotIn("pocketTtsNewlySelected", self.source)
        self.assertIn('activeTab.value === "speech" && usesPocketTts(draft)', self.source)
        self.assertIn("const pocketReady = await refreshPocketSetupStatus()", self.source)
        self.assertIn("if (!pocketReady)", self.source)
        self.assertIn("pocketSetupOpen.value = true", self.source)
        self.assertIn('<PopupTransition :open="pocketSetupOpen"', self.source)
        self.assertIn("Install and enable the Hugging Face integration", self.source)
        self.assertIn("Add your Hugging Face access token", self.source)
        self.assertIn("Accept the Pocket TTS terms", self.source)
        self.assertIn("https://huggingface.co/kyutai/pocket-tts", self.source)
        self.assertIn(':disabled="!pocketSetupReady"', self.source)
        self.assertNotIn("I accepted the Pocket TTS terms", self.source)

    def test_pocket_tts_guard_uses_live_hugging_face_status(self) -> None:
        endpoint = "/api/settings/huggingface/status"
        self.assertIn("modelsHuggingFaceStatus: string", self.types)
        self.assertIn(f'modelsHuggingFaceStatus: withBasePath("{endpoint}")', self.bootstrap)
        self.assertIn(f'@app.get("{endpoint}")', self.backend)
        self.assertIn("return _pocket_tts_huggingface_status()", self.backend)
        self.assertIn("props.endpoints.modelsHuggingFaceStatus", self.source)
        self.assertIn("pocketSetupStatus.weights_accessible", self.source)
        self.assertIn('method="HEAD"', self.backend)

    def test_gated_weight_redirect_confirms_download_access_without_downloading(self) -> None:
        helper = _load_pocket_access_helper()
        redirect = urllib.error.HTTPError("https://example.test", 302, "Found", {}, None)
        opener = mock.Mock()
        opener.open.side_effect = redirect
        with mock.patch.object(urllib.request, "build_opener", return_value=opener):
            result = helper("hf_test")
        self.assertTrue(result["weights_accessible"])
        self.assertEqual(result["weights_http_status"], 302)
        request = opener.open.call_args.args[0]
        self.assertEqual(request.get_method(), "HEAD")

    def test_gated_weight_auth_failure_keeps_pocket_tts_blocked(self) -> None:
        helper = _load_pocket_access_helper()
        denied = urllib.error.HTTPError("https://example.test", 401, "Unauthorized", {}, None)
        opener = mock.Mock()
        opener.open.side_effect = denied
        with mock.patch.object(urllib.request, "build_opener", return_value=opener):
            result = helper("hf_test")
        self.assertFalse(result["weights_accessible"])
        self.assertEqual(result["weights_http_status"], 401)
        self.assertIn("accept the Pocket TTS terms", result["weights_error"])

    def test_pocket_tts_setup_modal_has_responsive_styles(self) -> None:
        self.assertIn(".tm-pocket-setup {", self.styles)
        self.assertIn(".tm-pocket-setup-steps > li", self.styles)
        self.assertIn(".tm-pocket-setup-actions", self.styles)


if __name__ == "__main__":
    unittest.main()
