#!/usr/bin/env python3
from __future__ import annotations

import unittest

from tateros_app import AppSettingsRequest


class LlmSettingsRequestTests(unittest.TestCase):
    def test_numeric_llm_ports_are_coerced_to_strings(self) -> None:
        payload = AppSettingsRequest(
            hydra_llm_port=11434,
            tater_open_webui_llm_port=1234,
            hydra_beast_mode_enabled=True,
            hydra_llm_astraeus_port=1234,
            hydra_llm_thanatos_port=1235,
            hydra_llm_hermes_port=1236,
        ).model_dump(exclude_none=True)

        self.assertEqual(payload["hydra_llm_port"], "11434")
        self.assertEqual(payload["tater_open_webui_llm_port"], "1234")
        self.assertEqual(payload["hydra_llm_astraeus_port"], "1234")
        self.assertEqual(payload["hydra_llm_thanatos_port"], "1235")
        self.assertEqual(payload["hydra_llm_hermes_port"], "1236")


if __name__ == "__main__":
    unittest.main()
