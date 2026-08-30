import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class SpudLinkUiTests(unittest.TestCase):
    def test_pairing_flows_are_separate_and_role_bound(self):
        app = (ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        backend = (ROOT / "tateros_app.py").read_text(encoding="utf-8")

        self.assertIn('data-spud-link-start-pairing="little_spud"', app)
        self.assertIn('data-spud-link-start-pairing="spudlet"', app)
        self.assertIn("Show Little Spud QR", app)
        self.assertIn("Create Spudlet Code", app)
        self.assertIn('id="pair_spud_link_home_url"', app)
        self.assertIn('id="pair_spud_link_public_url"', app)
        self.assertIn("validateSpudLinkPairUrl", app)
        self.assertNotIn('id="set_spud_link_home_url"', app)
        self.assertNotIn('id="set_spud_link_public_url"', app)
        self.assertIn("startSpudLinkPairingSuccessPoll", app)
        self.assertIn('"pairing_role": pairing_role', backend)
        self.assertIn("This pairing invite is only for", backend)
        pairing_endpoint = backend[backend.index("def create_spud_link_pairing_code(") : backend.index("def pair_spud_link_node(")]
        self.assertIn("pairing_settings = dict(settings)", pairing_endpoint)
        self.assertNotIn("_save_spud_link_settings_from_updates", pairing_endpoint)

    def test_spud_link_has_clear_subsections_and_hub_route_feedback(self):
        app = (ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        styles = (ROOT / "tateros_static" / "styles.css").read_text(encoding="utf-8")
        backend = (ROOT / "tateros_app.py").read_text(encoding="utf-8")

        self.assertIn('data-spud-link-tab="pair"', app)
        self.assertIn('data-spud-link-tab="spudlet"', app)
        self.assertIn('data-spud-link-tab="settings"', app)
        self.assertIn("Use Spud Hub for all models", app)
        self.assertNotIn("renderSpudLinkRouteBadge", app)
        self.assertIn("renderSpudLinkRouteNotice", app)
        self.assertIn("is-spud-link-locked", app)
        self.assertIn('child.setAttribute("inert", "")', app)
        self.assertIn('data-spud-link-lock-without-notice=', app)
        self.assertIn('panel.dataset.spudLinkLockWithoutNotice === "true"', app)
        self.assertIn('label: "Spud Hub"', app)
        self.assertIn('speechSttBackendStatusEl.textContent = "Controlled by the paired Spud Hub."', app)
        self.assertIn('data-spud-link-identity-owner="local"', app)
        self.assertIn('panel.dataset.spudLinkIdentityOwner === "local"', app)
        self.assertIn("This Spudlet still owns speaker profiles", app)
        self.assertIn("This Spudlet still owns its People links and face gallery", app)
        self.assertNotIn('label: "Spudlet via Spud Hub"', app)
        self.assertNotIn("includeSpudLink", app)
        self.assertNotIn('hydraBaseProviderEl.value = "spud_link"', app)
        self.assertIn("include_spud_link=False", backend)
        self.assertIn(".spud-link-pair-dialog", styles)
        self.assertIn(".settings-subpanel.is-spud-link-locked", styles)
        self.assertNotIn(".settings-route-badge", styles)


if __name__ == "__main__":
    unittest.main()
