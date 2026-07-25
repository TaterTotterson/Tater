#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import pathlib
import sys
import unittest
from unittest import mock

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tater_voice import home, wake_trainer_link  # noqa: E402
from tater_voice.voice_pipeline import routes  # noqa: E402


class FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}

    def hset(self, name, key=None, value=None, mapping=None):
        row = self.hashes.setdefault(str(name), {})
        if mapping is not None:
            row.update({str(k): str(v) for k, v in mapping.items()})
        elif key is not None:
            row[str(key)] = str(value)
        return 1

    def hget(self, name, key):
        return self.hashes.get(str(name), {}).get(str(key))

    def hgetall(self, name):
        return dict(self.hashes.get(str(name), {}))

    def hdel(self, name, *keys):
        row = self.hashes.get(str(name), {})
        for key in keys:
            row.pop(str(key), None)

    def delete(self, name):
        self.hashes.pop(str(name), None)


class WakeTrainerLinkTests(unittest.TestCase):
    def setUp(self) -> None:
        self.redis = FakeRedis()
        self.redis_patch = mock.patch.object(wake_trainer_link, "redis_client", self.redis)
        self.redis_patch.start()

    def tearDown(self) -> None:
        self.redis_patch.stop()

    def test_tater_creates_one_time_code_and_trainer_claims_it(self) -> None:
        pairing = wake_trainer_link.start_pairing()
        self.assertRegex(pairing["display_code"], r"^[A-Z2-9]{4}-[A-Z2-9]{4}$")
        stored = str(self.redis.hgetall(wake_trainer_link.PAIRING_HASH_KEY))
        self.assertNotIn(pairing["display_code"].replace("-", ""), stored)

        claim = wake_trainer_link.claim_pairing(
            pairing_code=pairing["display_code"],
            trainer_id="trainer-1",
            trainer_name="Wake Word Trainer",
            trainer_url="http://10.4.20.210:8789",
            publish_base_url="http://10.4.20.210:8789",
        )

        self.assertTrue(claim["linked"])
        self.assertGreaterEqual(len(claim["token"]), 32)
        self.assertTrue(wake_trainer_link.status()["linked"])
        self.assertEqual(
            wake_trainer_link.pairing_status(pairing["pairing_id"])["state"],
            "linked",
        )
        raw_link = self.redis.hgetall(wake_trainer_link.LINK_HASH_KEY)
        self.assertNotIn(claim["token"], raw_link.values())
        wake_trainer_link.authorize(claim["token"])
        with self.assertRaises(PermissionError):
            wake_trainer_link.authorize("wrong-token")
        with self.assertRaises(ValueError):
            wake_trainer_link.claim_pairing(
                pairing_code=pairing["display_code"],
                trainer_id="trainer-2",
                trainer_name="Other Trainer",
                trainer_url="http://10.4.20.211:8789",
                publish_base_url="http://10.4.20.211:8789",
            )

    def test_link_only_accepts_wake_json_from_paired_trainer_origin(self) -> None:
        link = {"publish_base_url": "http://10.4.20.210:8789"}
        expected = "http://10.4.20.210:8789/api/trained_wake_words/hey_tater.json"
        self.assertEqual(wake_trainer_link.validate_wake_word_url(expected, link), expected)
        with self.assertRaises(ValueError):
            wake_trainer_link.validate_wake_word_url(
                "http://attacker.test/api/trained_wake_words/hey_tater.json",
                link,
            )
        with self.assertRaises(ValueError):
            wake_trainer_link.validate_wake_word_url(
                "http://10.4.20.210:8789/api/session",
                link,
            )

    def test_public_claim_route_returns_one_time_token(self) -> None:
        pairing = wake_trainer_link.start_pairing()
        result = asyncio.run(
            routes.linked_trainer_claim(
                {
                    "pairing_code": pairing["display_code"],
                    "trainer_id": "trainer-1",
                    "trainer_name": "Wake Word Trainer",
                    "trainer_url": "http://10.4.20.210:8789",
                    "publish_base_url": "http://10.4.20.210:8789",
                }
            )
        )
        self.assertTrue(result["ok"])
        self.assertIn("token", result)
        self.assertNotIn("token", wake_trainer_link.status())

    def test_linked_trainer_can_publish_only_its_wake_word_url(self) -> None:
        pairing = wake_trainer_link.start_pairing()
        claim = wake_trainer_link.claim_pairing(
            pairing_code=pairing["display_code"],
            trainer_id="trainer-1",
            trainer_name="Wake Word Trainer",
            trainer_url="http://10.4.20.210:8789",
            publish_base_url="http://10.4.20.210:8789",
        )
        wake_url = "http://10.4.20.210:8789/api/trained_wake_words/hey_tater.json"
        with mock.patch(
            "tater_voice.native_satellite.save_live_settings",
            new=mock.AsyncMock(return_value={"push": {"count": 2}}),
        ) as save:
            result = asyncio.run(
                routes.linked_trainer_wake_word_save(
                    {
                        "wake_word_name": "hey_tater",
                        "wake_word_url": wake_url,
                    },
                    claim["token"],
                )
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["push"]["count"], 2)
        self.assertEqual(wake_trainer_link.status()["last_wake_word_url"], wake_url)
        save.assert_awaited_once_with(
            {
                "wake_word": "custom_url",
                "wake_word_url": wake_url,
            }
        )

    def test_voice_settings_actions_start_and_poll_tater_pairing(self) -> None:
        with (
            mock.patch.object(home.esphome_firmware, "handle_runtime_action", return_value=None),
            mock.patch.object(home, "_runtime_status_with_native", return_value={}),
            mock.patch.object(home.esphome_speaker_id, "handle_runtime_action", return_value=None),
            mock.patch.object(home.esphome_emotion_id, "handle_runtime_action", return_value=None),
            mock.patch.object(home, "_wake_trainer_link_item_form", return_value={"group": "wake_trainer_link"}),
        ):
            started = home.handle_runtime_action(
                action="voice_wake_trainer_link_pairing_start",
                payload={},
            )
            status = home.handle_runtime_action(
                action="voice_wake_trainer_link_pairing_status",
                payload={"values": {"pairing_id": started["pairing_id"]}},
            )

        self.assertEqual(status["state"], "waiting")

    def test_voice_settings_embeds_link_in_trainer_feedback_card(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "tateros_static" / "styles.css").read_text(encoding="utf-8")

        self.assertIn("wakeTrainerLink: wakeTrainerLinkItem", app_js)
        self.assertIn("trainer-feedback-section", app_js)
        self.assertNotIn('id="settings-esphome-runtime-wake-trainer-link"', app_js)
        self.assertIn(".wake-trainer-link-panel", styles)
        self.assertIn(".wake-trainer-link-summary", styles)


if __name__ == "__main__":
    unittest.main()
