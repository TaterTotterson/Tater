import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class SpudLinkRemoteIdentityTests(unittest.TestCase):
    def test_remote_identity_routes_run_before_local_enable_gates(self):
        cases = (
            (ROOT / "tater_voice" / "speaker_id.py", 'spud_link_should_use_hub("speaker_id"', "if not speaker_id_enabled()"),
            (ROOT / "tater_voice" / "emotion_id.py", 'spud_link_should_use_hub("emotion_id"', "if not emotion_id_enabled()"),
        )
        for path, remote_gate, local_gate in cases:
            source = path.read_text(encoding="utf-8")
            function_source = source[source.index(remote_gate) :]
            self.assertLess(function_source.index(remote_gate), function_source.index(local_gate))

    def test_face_id_remote_status_is_effectively_enabled(self):
        source = (ROOT / "face_id_runtime.py").read_text(encoding="utf-8")
        status_source = source[source.index("def status(") : source.index("def settings_payload(")]

        self.assertLess(
            status_source.index('spud_link_should_use_hub("face_id"'),
            status_source.index("enabled = is_enabled(redis_client)"),
        )
        self.assertIn('"enabled": True', status_source)
        self.assertIn('"routed_via": "spud_link"', status_source)


if __name__ == "__main__":
    unittest.main()
