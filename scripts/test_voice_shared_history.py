import json
import unittest
from types import SimpleNamespace
from unittest import mock

from tater_voice import voice_pipeline
from tater_voice.voice_pipeline import conversation


class _Pipeline:
    def __init__(self, redis):
        self.redis = redis

    def rpush(self, key, value):
        self.redis.rows.setdefault(key, []).append(value)
        return self

    def ltrim(self, key, start, end):
        rows = self.redis.rows.setdefault(key, [])
        normalized_start = max(0, len(rows) + start) if start < 0 else start
        normalized_end = len(rows) if end == -1 else end + 1
        self.redis.rows[key] = rows[normalized_start:normalized_end]
        return self

    def expire(self, key, ttl):
        self.redis.ttls[key] = ttl
        return self

    def execute(self):
        return []


class _Redis:
    def __init__(self):
        self.rows = {}
        self.ttls = {}

    def pipeline(self):
        return _Pipeline(self)

    def lrange(self, key, start, end):
        rows = list(self.rows.get(key, []))
        if start < 0:
            start = max(0, len(rows) + start)
        if end < 0:
            end = len(rows) + end
        return rows[start : end + 1]


class SharedVoiceHistoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.redis = _Redis()
        self.limits = {
            "history_store": 20,
            "history_llm": 8,
            "session_ttl_s": 2 * 60 * 60,
        }
        self.vp = SimpleNamespace(
            redis_client=self.redis,
            DEFAULT_HISTORY_MAX_STORE=20,
            DEFAULT_HISTORY_MAX_LLM=8,
            DEFAULT_SESSION_TTL_SECONDS=2 * 60 * 60,
            _voice_config_snapshot=lambda: {"limits": dict(self.limits)},
            _text=lambda value: str(value or "").strip(),
        )
        self.patch = mock.patch.object(conversation, "_vp", return_value=self.vp)
        self.patch.start()

    def tearDown(self):
        self.patch.stop()

    async def test_history_is_shared_across_satellites_and_user_aware(self):
        await conversation._save_history_message(
            "conversation-one",
            "user",
            "At two o'clock alert us to wake up.",
            voice_turn={
                "person_id": "person-one",
                "person_name": "User One",
                "area_name": "Kitchen",
            },
        )
        await conversation._save_history_message(
            "conversation-one",
            "assistant",
            {"marker": "plugin_response", "phase": "final", "content": "The alert is set for 2:00."},
        )
        await conversation._save_history_message(
            "conversation-two",
            "user",
            "No, make that three.",
            voice_turn={
                "person_id": "person-two",
                "person_name": "User Two",
                "area_name": "Bedroom",
            },
        )

        history = await conversation._load_history("conversation-two", 8, shared=True)

        self.assertEqual([row["role"] for row in history], ["user", "assistant", "user"])
        self.assertIn('person="User One"', history[0]["content"])
        self.assertIn('person_id="person-one"', history[0]["content"])
        self.assertIn("At two o'clock", history[0]["content"])
        self.assertEqual(history[1]["content"], "The alert is set for 2:00.")
        self.assertIn('person="User Two"', history[2]["content"])
        self.assertIn("No, make that three.", history[2]["content"])
        self.assertEqual(
            self.redis.ttls[conversation.SHARED_VOICE_HISTORY_KEY],
            2 * 60 * 60,
        )
        self.assertEqual(len(self.redis.rows[conversation._history_key("conversation-one")]), 2)
        self.assertEqual(len(self.redis.rows[conversation._history_key("conversation-two")]), 1)

    async def test_progress_messages_do_not_consume_shared_history(self):
        await conversation._save_history_message(
            "conversation-one",
            "assistant",
            {"marker": "plugin_wait", "content": "Working on it."},
            include_shared=False,
        )

        self.assertNotIn(conversation.SHARED_VOICE_HISTORY_KEY, self.redis.rows)
        stored = json.loads(self.redis.rows[conversation._history_key("conversation-one")][0])
        self.assertEqual(stored["content"]["marker"], "plugin_wait")

    async def test_shared_history_uses_configured_storage_limit(self):
        self.limits["history_store"] = 2
        for index in range(4):
            await conversation._save_history_message(
                "conversation-one",
                "user",
                f"Message {index}",
            )

        shared = self.redis.rows[conversation.SHARED_VOICE_HISTORY_KEY]
        per_conversation = self.redis.rows[conversation._history_key("conversation-one")]
        self.assertEqual(len(shared), 2)
        self.assertEqual(len(per_conversation), 2)
        self.assertEqual(json.loads(shared[0])["content"], "Message 2")

    async def test_zero_storage_limit_keeps_unlimited_shared_history(self):
        self.limits["history_store"] = 0
        for index in range(25):
            await conversation._save_history_message(
                "conversation-one",
                "user",
                f"Message {index}",
            )

        self.assertEqual(len(self.redis.rows[conversation.SHARED_VOICE_HISTORY_KEY]), 25)
        self.assertEqual(len(self.redis.rows[conversation._history_key("conversation-one")]), 25)


class VoiceHistoryConfigurationTests(unittest.TestCase):
    def test_voice_history_uses_shared_hydra_conversation_settings(self):
        values = {
            voice_pipeline.HYDRA_HISTORY_MAX_STORE_KEY: "64",
            voice_pipeline.HYDRA_HISTORY_MAX_LLM_KEY: "18",
        }
        with mock.patch.object(
            voice_pipeline.redis_client,
            "get",
            side_effect=lambda key: values.get(key),
        ):
            limits = voice_pipeline._conversation_history_limits()

        self.assertEqual(limits, {"history_store": 64, "history_llm": 18})

    def test_zero_max_store_preserves_unlimited_ui_behavior(self):
        values = {
            voice_pipeline.HYDRA_HISTORY_MAX_STORE_KEY: "0",
            voice_pipeline.HYDRA_HISTORY_MAX_LLM_KEY: "1",
        }
        with mock.patch.object(
            voice_pipeline.redis_client,
            "get",
            side_effect=lambda key: values.get(key),
        ):
            limits = voice_pipeline._conversation_history_limits()

        self.assertEqual(limits, {"history_store": 0, "history_llm": 1})


if __name__ == "__main__":
    unittest.main()
