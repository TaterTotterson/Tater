import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class LlamaCppSpeculativeUiTests(unittest.TestCase):
    def test_settings_offer_all_supported_speculative_methods(self):
        app = (ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn(">Speculative Decoding", app)
        self.assertIn('value="draft-mtp"', app)
        self.assertIn('value="draft-dflash"', app)
        self.assertIn('value="draft-dspark"', app)
        self.assertIn('"draft-mtp": 3', app)
        self.assertIn('"draft-dflash": 15', app)
        self.assertIn('"draft-dspark": 7', app)
        self.assertIn("hydra_llama_cpp_speculative_method: llamaCppSpeculativeMethod", app)

    def test_speculative_controls_use_generic_container(self):
        app = (ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        styles = (ROOT / "tateros_static" / "styles.css").read_text(encoding="utf-8")

        self.assertIn('id="hydra-llama-speculative-extra"', app)
        self.assertNotIn('id="hydra-llama-mtp-extra"', app)
        self.assertIn(".hydra-speculative-extra", styles)


if __name__ == "__main__":
    unittest.main()
