from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import contextlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

from tater_voice import native_satellite as native


class NativePairingSecurityTests(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        self.directory = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.credentials = Path(self.directory) / "credentials.json"
        self.stack.enter_context(mock.patch.dict(native._pairing_sessions, {}, clear=True))
        self.stack.enter_context(
            mock.patch.object(native, "_credentials_path", return_value=self.credentials)
        )
        self.clock = self.stack.enter_context(
            mock.patch.object(native, "_now", return_value=1000.0)
        )
        self.api_key = self.stack.enter_context(
            mock.patch.object(native, "_expected_auth_token", return_value="")
        )
        self.open_mode = self.stack.enter_context(
            mock.patch.object(native, "_allow_unpaired_open_connections", return_value=False)
        )

    def authorize(self, credential, device="sat1", host="192.0.2.10", hardware="aabbcc112233"):
        websocket = SimpleNamespace(
            headers={"x-tater-token": credential},
            query_params={},
            client=SimpleNamespace(host=host),
        )
        return native._authorize_websocket_hello(
            websocket,
            f"native:{device}",
            {
                "device_id": device,
                "hardware_id": hardware,
                "device_name": "Test satellite",
            },
        )

    def session(self, public):
        return native._pairing_sessions[public["pairing_id"]]

    def exhaust(self):
        for index in range(native.PAIRING_MAX_FAILED_GUESSES):
            self.assertFalse(
                self.authorize(f"wrong-{index}", device=f"sat-{index}", host=f"192.0.2.{index+1}")[
                    "ok"
                ]
            )

    def test_five_different_guesses_expire_all_pending_codes_across_identities_and_sources(self):
        first, second = native.start_pairing_session(), native.start_pairing_session()
        self.exhaust()
        for public in (first, second):
            status = native.pairing_status(public["pairing_id"])
            self.assertTrue(status["expired"])
            self.assertEqual(status["reason"], "too_many_guesses")
            self.assertEqual(status["expires_in_s"], 0)
            self.assertFalse(self.authorize(public["code"])["ok"])
        self.assertFalse(self.credentials.exists())

    def test_repeated_typo_and_formatting_changes_count_once_then_correction_pairs(self):
        public = native.start_pairing_session()
        wrong = "000000" if native._normalize_pairing_code(public["code"]) != "000000" else "999999"
        for index in range(30):
            code = (wrong, wrong[:3] + "-" + wrong[3:], wrong[:3] + " " + wrong[3:])[index % 3]
            self.assertFalse(
                self.authorize(code, device=f"sat-{index}", host=f"192.0.2.{index+1}")["ok"]
            )
        self.assertEqual(len(self.session(public)["failed_guesses"]), 1)
        self.assertEqual(native.pairing_status(public["pairing_id"])["state"], "waiting")
        result = self.authorize(public["code"])
        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "paired")
        self.assertTrue(result["device_token"].startswith("tns_"))

    def test_saved_device_credentials_and_handoff_retry_do_not_consume_another_session(self):
        first = native.start_pairing_session()
        paired = self.authorize(first["code"])
        pending = native.start_pairing_session()
        retry = self.authorize(first["code"])
        self.assertEqual(retry["device_token"], paired["device_token"])
        self.assertFalse(self.authorize(first["code"], hardware="ffffffffffff")["ok"])
        reconnect = self.authorize(paired["device_token"])
        self.assertTrue(reconnect["ok"])
        self.assertEqual(reconnect["mode"], "device_token")
        self.assertNotIn("device_token", self.session(first))
        for _ in range(10):
            self.assertTrue(self.authorize(paired["device_token"])["ok"])
        self.assertEqual(len(self.session(pending)["failed_guesses"]), 1)
        self.assertEqual(native.pairing_status(pending["pairing_id"])["state"], "waiting")

    def test_lockout_revokes_pending_handoff_but_preserves_the_permanent_device_token(self):
        public = native.start_pairing_session()
        paired = self.authorize(public["code"])
        self.exhaust()
        self.assertNotIn("device_token", self.session(public))
        self.assertFalse(self.authorize(public["code"])["ok"])
        self.assertTrue(self.authorize(paired["device_token"])["ok"])

    def test_a_new_pairing_session_has_an_independent_guess_limit(self):
        expired = native.start_pairing_session()
        self.exhaust()
        self.assertTrue(native.pairing_status(expired["pairing_id"])["expired"])
        fresh = native.start_pairing_session()
        self.exhaust()
        self.assertTrue(native.pairing_status(fresh["pairing_id"])["expired"])
        self.assertEqual(
            len(self.session(fresh)["failed_guesses"]), native.PAIRING_MAX_FAILED_GUESSES
        )

    def test_counters_stop_growing_after_lockout_and_stay_out_of_public_and_saved_data(self):
        public = native.start_pairing_session()
        self.exhaust()
        for index in range(100):
            self.assertFalse(self.authorize(f"more-wrong-{index}")["ok"])
        guesses = self.session(public)["failed_guesses"]
        self.assertEqual(len(guesses), native.PAIRING_MAX_FAILED_GUESSES)
        self.assertTrue(all(len(value) == 64 and "wrong" not in value for value in guesses))
        status = native.pairing_status(public["pairing_id"])
        self.assertNotIn("failed_guesses", json.dumps(status))
        self.assertFalse(self.credentials.exists())

    def test_parallel_requests_cannot_exceed_the_guess_budget(self):
        public = native.start_pairing_session()
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(lambda index: self.authorize(f"parallel-{index}"), range(30))
            )
        self.assertTrue(all(not result["ok"] for result in results))
        self.assertEqual(
            len(self.session(public)["failed_guesses"]), native.PAIRING_MAX_FAILED_GUESSES
        )
        self.assertTrue(native.pairing_status(public["pairing_id"])["expired"])

    def test_time_expiry_still_prevents_pairing(self):
        public = native.start_pairing_session()
        self.clock.return_value = public["expires_at"]
        self.assertFalse(self.authorize(public["code"])["ok"])
        self.assertTrue(native.pairing_status(public["pairing_id"])["expired"])
        self.assertNotIn("failed_guesses", self.session(public))

    def test_existing_api_key_and_explicit_open_mode_keep_their_behavior(self):
        public = native.start_pairing_session()
        self.api_key.return_value = "configured-key"
        self.assertEqual(self.authorize("configured-key")["mode"], "api_key")
        self.open_mode.return_value = True
        self.assertEqual(self.authorize("not-a-key")["mode"], "open")
        self.assertNotIn("failed_guesses", self.session(public))

    def test_unicode_input_is_rejected_without_breaking_key_comparison(self):
        public = native.start_pairing_session()
        self.api_key.return_value = "configured-key"
        self.assertFalse(self.authorize("１２３٤٥٦")["ok"])
        self.assertEqual(native._normalize_pairing_code("１２３٤٥٦"), "")
        self.assertEqual(len(self.session(public)["failed_guesses"]), 1)

    def test_full_credentials_are_not_collapsed_like_formatted_pairing_codes(self):
        self.assertEqual(
            native._rejected_pairing_credential_hash("123 456"),
            native._rejected_pairing_credential_hash("123-456"),
        )
        self.assertNotEqual(
            native._rejected_pairing_credential_hash("tns-123456"),
            native._rejected_pairing_credential_hash("tns123456"),
        )


if __name__ == "__main__":
    unittest.main()
