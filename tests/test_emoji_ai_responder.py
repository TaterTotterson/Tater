import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from plugins.emoji_ai_responder import EmojiAIResponderPlugin


class _FakeMessage:
    def __init__(self, content: str, reactions=None):
        self.content = content
        self.reactions = list(reactions or [])
        self.added = []

    async def add_reaction(self, emoji: str):
        self.added.append(emoji)


class _FakeReaction:
    def __init__(self, message):
        self.message = message


class _FakeUser:
    def __init__(self, bot: bool = False):
        self.bot = bot


class _FakeExistingReaction:
    def __init__(self, emoji: str):
        self.emoji = emoji


class _FakeLLM:
    def __init__(self, content: str):
        self._content = content

    async def chat(self, messages, **kwargs):
        return {"message": {"content": self._content}}


class EmojiAIResponderTests(unittest.TestCase):
    def setUp(self):
        self.plugin = EmojiAIResponderPlugin()

    def test_normalize_emoji_rejects_ascii(self):
        self.assertEqual(self.plugin._normalize_emoji("thumbs_up"), "")
        self.assertEqual(self.plugin._normalize_emoji("🔥"), "🔥")

    def test_suggest_emoji_parses_wrapped_json(self):
        llm = _FakeLLM('Output:\n{"function":"suggest_emoji","arguments":{"emoji":"🎯"}}')
        emoji = asyncio.run(self.plugin._suggest_emoji("Great launch", llm_client=llm))
        self.assertEqual(emoji, "🎯")

    def test_on_assistant_response_respects_chance_and_cooldown(self):
        settings = {
            "enable_auto_reaction_on_reply": True,
            "auto_reaction_chance_percent": 10,
            "auto_reaction_cooldown_seconds": 120,
            "min_message_length": 1,
        }

        with patch("plugins.emoji_ai_responder.get_plugin_enabled", return_value=True), patch.object(
            self.plugin, "_get_settings", return_value=settings
        ), patch.object(self.plugin, "_cooldown_allows", return_value=True), patch(
            "plugins.emoji_ai_responder.random.random", return_value=0.99
        ):
            result = asyncio.run(
                self.plugin.on_assistant_response(
                    platform="discord",
                    user_text="compare device a vs b",
                    assistant_text="done",
                    llm_client=None,
                    scope="123",
                )
            )
            self.assertEqual(result, "")

        with patch("plugins.emoji_ai_responder.get_plugin_enabled", return_value=True), patch.object(
            self.plugin, "_get_settings", return_value=settings
        ), patch.object(self.plugin, "_cooldown_allows", return_value=True), patch(
            "plugins.emoji_ai_responder.random.random", return_value=0.0
        ), patch.object(
            self.plugin, "_suggest_emoji", AsyncMock(return_value="🔥")
        ) as suggest_mock, patch.object(
            self.plugin, "_mark_cooldown"
        ) as mark_mock:
            result = asyncio.run(
                self.plugin.on_assistant_response(
                    platform="discord",
                    user_text="compare device a vs b",
                    assistant_text="done",
                    llm_client=None,
                    scope="123",
                )
            )
            self.assertEqual(result, "🔥")
            suggest_mock.assert_awaited()
            mark_mock.assert_called_once()

    def test_on_reaction_add_adds_new_emoji(self):
        message = _FakeMessage("This is awesome")
        reaction = _FakeReaction(message)
        user = _FakeUser(bot=False)
        settings = {
            "enable_on_reaction_add": True,
            "min_message_length": 1,
        }

        with patch("plugins.emoji_ai_responder.get_plugin_enabled", return_value=True), patch.object(
            self.plugin, "_get_settings", return_value=settings
        ), patch.object(
            self.plugin, "_suggest_emoji", AsyncMock(return_value="🚀")
        ):
            asyncio.run(self.plugin.on_reaction_add(reaction, user))

        self.assertEqual(message.added, ["🚀"])

    def test_on_reaction_add_skips_duplicate_emoji(self):
        message = _FakeMessage("This is awesome", reactions=[_FakeExistingReaction("🚀")])
        reaction = _FakeReaction(message)
        user = _FakeUser(bot=False)
        settings = {
            "enable_on_reaction_add": True,
            "min_message_length": 1,
        }

        with patch("plugins.emoji_ai_responder.get_plugin_enabled", return_value=True), patch.object(
            self.plugin, "_get_settings", return_value=settings
        ), patch.object(
            self.plugin, "_suggest_emoji", AsyncMock(return_value="🚀")
        ):
            asyncio.run(self.plugin.on_reaction_add(reaction, user))

        self.assertEqual(message.added, [])


if __name__ == "__main__":
    unittest.main()
