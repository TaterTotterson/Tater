import json
import unittest
from types import SimpleNamespace
from unittest import mock

from tater_voice.voice_pipeline import conversation


class _Pipeline:
    def __init__(self, redis):
        self.redis = redis

    def rpush(self, key, value):
        self.redis.rows.setdefault(key, []).append(value)
        return self

    def ltrim(self, key, start, end):
        rows = self.redis.rows.setdefault(key, [])
        self.redis.rows[key] = rows[start:] if start < 0 else rows[start : end + 1]
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
        self.vp = SimpleNamespace(
            redis_client=self.redis,
            DEFAULT_HISTORY_MAX_STORE=20,
            DEFAULT_SESSION_TTL_SECONDS=2 * 60 * 60,
            _voice_config_snapshot=lambda: {
                "limits": {"history_store": 20, "session_ttl_s": 2 * 60 * 60}
            },
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


if __name__ == "__main__":
    unittest.main()
