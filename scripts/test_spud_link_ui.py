import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class SpudLinkUiTests(unittest.TestCase):
    def test_pairing_flows_are_separate_and_role_bound(self):
        app = (ROOT / "frontend" / "src" / "settings" / "components" / "SpudLinkSettings.vue").read_text(encoding="utf-8")
        backend = (ROOT / "tateros_app.py").read_text(encoding="utf-8")

        self.assertIn("startPairing('little_spud')", app)
        self.assertIn("startPairing('spudlet')", app)
        self.assertIn("startPairing('tater_open_webui')", app)
        self.assertIn("Show Little Spud QR", app)
        self.assertIn("Create Spudlet code", app)
        self.assertIn("Create Tater Open WebUI code", app)
        self.assertIn('v-model="draft.home_url"', app)
        self.assertIn('v-model="draft.public_url"', app)
        self.assertIn("function validUrl", app)
        self.assertIn("function pollPairing", app)
        self.assertIn('"pairing_role": pairing_role', backend)
        self.assertIn("This pairing invite is only for", backend)
        pairing_endpoint = backend[backend.index("def create_spud_link_pairing_code(") : backend.index("def pair_spud_link_node(")]
        self.assertIn("pairing_settings = dict(settings)", pairing_endpoint)
        self.assertNotIn("_save_spud_link_settings_from_updates", pairing_endpoint)

    def test_tater_open_webui_has_a_scoped_spudlink_role(self):
        app = (ROOT / "frontend" / "src" / "settings" / "components" / "SpudLinkSettings.vue").read_text(encoding="utf-8")
        backend = (ROOT / "tateros_app.py").read_text(encoding="utf-8")

        self.assertIn('SPUD_LINK_MODE_TATER_OPEN_WEBUI = "tater_open_webui"', backend)
        self.assertIn('"tater_open_webui": "Tater Open WebUI"', backend)
        self.assertIn('allow_tater_open_webui', backend)
        self.assertIn('@app.get("/api/spudlink/v1/tater-open-webui/status")', backend)
        self.assertIn('@app.get("/api/spudlink/v1/tater-open-webui/identity")', backend)
        self.assertIn('@app.post("/api/spudlink/v1/tater-open-webui/identity")', backend)
        self.assertIn('"tater_open_webui"', (ROOT / "people.py").read_text(encoding="utf-8"))
        self.assertIn('"hydra_tools_enabled": True', backend)
        self.assertIn('role == SPUD_LINK_MODE_TATER_OPEN_WEBUI', backend)
        self.assertIn('This Tater Open WebUI link is not authorized', backend)
        self.assertIn('"/api/spudlink/v1/tts/speech"', backend)
        self.assertIn('"/api/spudlink/v1/stt/transcribe"', backend)
        self.assertIn('generated_file_prefix = "/api/spudlink/v1/files/"', backend)
        self.assertIn("path.startswith(generated_file_prefix)", backend)
        self.assertIn('v-model="draft.allow_tater_open_webui"', app)
        self.assertIn('Link the coding workspace', app)

    def test_spud_link_has_clear_subsections_and_hub_route_feedback(self):
        app = (ROOT / "frontend" / "src" / "settings" / "components" / "SpudLinkSettings.vue").read_text(encoding="utf-8")
        backend = (ROOT / "tateros_app.py").read_text(encoding="utf-8")

        for tab in ("pair", "spudlet", "settings"):
            self.assertIn(f"activeTab === '{tab}'", app)
        self.assertIn("Use Hub for all models", app)
        self.assertIn("function routeUsesHub", app)
        self.assertIn("Loaded on Spud Hub", app)
        self.assertIn("profiles stay here", app)
        self.assertIn("People and the face library stay here", app)
        self.assertIn('label: "Speech to Text"', app)
        self.assertIn('label: "Text to Speech"', app)
        self.assertNotIn("renderSpudLinkRouteBadge", app)
        self.assertIn("include_spud_link=False", backend)


if __name__ == "__main__":
    unittest.main()
