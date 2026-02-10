# plugins/emoji_ai_responder.py
import json
import logging
import os
import random
import time
from typing import Any, Dict

from dotenv import load_dotenv

from helpers import LLMClientWrapper, extract_json, redis_client
from plugin_base import ToolPlugin
from plugin_settings import get_plugin_enabled

load_dotenv()

logger = logging.getLogger("emoji_ai_responder")

_FALLBACK_LLM = None


def _fallback_llm_client():
    global _FALLBACK_LLM
    if _FALLBACK_LLM is None:
        llm_host = os.getenv("LLM_HOST", "127.0.0.1")
        llm_port = os.getenv("LLM_PORT", "11434")
        _FALLBACK_LLM = LLMClientWrapper(host=f"http://{llm_host}:{llm_port}")
    return _FALLBACK_LLM


class EmojiAIResponderPlugin(ToolPlugin):
    name = "emoji_ai_responder"
    plugin_name = "Emoji AI Responder"
    pretty_name = "Emoji AI Responder"
    version = "1.3.0"
    min_tater_version = "50"
    description = (
        "Picks a context-aware emoji reaction. Supports reaction-chain mode and optional low-frequency "
        "auto reactions on replies (Discord/Telegram/Matrix)."
    )
    plugin_dec = "Pick a contextual emoji reaction."
    platforms = ["passive"]
    settings_category = "Emoji AI Responder"
    required_settings = {
        "ENABLE_ON_REACTION_ADD": {
            "label": "Enable reaction-chain mode",
            "type": "checkbox",
            "default": True,
            "description": "When a user reacts to a Discord message, optionally add one matching emoji reaction.",
        },
        "ENABLE_AUTO_REACTION_ON_REPLY": {
            "label": "Enable auto reactions on replies",
            "type": "checkbox",
            "default": True,
            "description": "When the assistant replies, occasionally add a matching emoji reaction to the triggering message.",
        },
        "REACTION_CHAIN_CHANCE_PERCENT": {
            "label": "Reaction-chain chance (%)",
            "type": "number",
            "default": 100,
            "description": "Chance per reaction-add event to add one contextual emoji (0-100).",
        },
        "REPLY_REACTION_CHANCE_PERCENT": {
            "label": "Reply reaction chance (%)",
            "type": "number",
            "default": 12,
            "description": "Chance per assistant reply to add a contextual emoji (0-100).",
        },
        "REACTION_CHAIN_COOLDOWN_SECONDS": {
            "label": "Reaction-chain cooldown (seconds)",
            "type": "number",
            "default": 30,
            "description": "Minimum seconds between chain reactions per room/channel/chat (0 disables).",
        },
        "REPLY_REACTION_COOLDOWN_SECONDS": {
            "label": "Reply reaction cooldown (seconds)",
            "type": "number",
            "default": 120,
            "description": "Minimum seconds between reply reactions per room/channel/chat (0 disables).",
        },
        "MIN_MESSAGE_LENGTH": {
            "label": "Minimum message length",
            "type": "number",
            "default": 4,
            "description": "Ignore very short inputs for auto reactions.",
        },
    }

    @staticmethod
    def _decode_text(value: Any, default: str = "") -> str:
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", "ignore")
        if value is None:
            return default
        return str(value)

    @classmethod
    def _to_bool(cls, value: Any, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        raw = cls._decode_text(value).strip().lower()
        if raw in ("1", "true", "yes", "y", "on", "enabled"):
            return True
        if raw in ("0", "false", "no", "n", "off", "disabled"):
            return False
        return default

    @classmethod
    def _to_int(cls, value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(float(cls._decode_text(value, str(default)).strip()))
        except Exception:
            parsed = int(default)
        if parsed < minimum:
            return minimum
        if parsed > maximum:
            return maximum
        return parsed

    def _get_settings(self) -> Dict[str, Any]:
        raw = redis_client.hgetall(f"plugin_settings:{self.settings_category}") or redis_client.hgetall(
            f"plugin_settings: {self.settings_category}"
        ) or {}
        reply_chance_raw = raw.get("REPLY_REACTION_CHANCE_PERCENT")
        if reply_chance_raw in (None, ""):
            # Backward compatibility for previous single shared chance setting.
            reply_chance_raw = raw.get("AUTO_REACTION_CHANCE_PERCENT")
        reply_cooldown_raw = raw.get("REPLY_REACTION_COOLDOWN_SECONDS")
        if reply_cooldown_raw in (None, ""):
            # Backward compatibility for previous single shared cooldown setting.
            reply_cooldown_raw = raw.get("AUTO_REACTION_COOLDOWN_SECONDS")
        return {
            "enable_on_reaction_add": self._to_bool(raw.get("ENABLE_ON_REACTION_ADD"), True),
            "enable_auto_reaction_on_reply": self._to_bool(raw.get("ENABLE_AUTO_REACTION_ON_REPLY"), True),
            "reaction_chain_chance_percent": self._to_int(
                raw.get("REACTION_CHAIN_CHANCE_PERCENT"), default=100, minimum=0, maximum=100
            ),
            "reply_reaction_chance_percent": self._to_int(
                reply_chance_raw, default=12, minimum=0, maximum=100
            ),
            "reaction_chain_cooldown_seconds": self._to_int(
                raw.get("REACTION_CHAIN_COOLDOWN_SECONDS"), default=30, minimum=0, maximum=86400
            ),
            "reply_reaction_cooldown_seconds": self._to_int(
                reply_cooldown_raw, default=120, minimum=0, maximum=86400
            ),
            "min_message_length": self._to_int(raw.get("MIN_MESSAGE_LENGTH"), default=4, minimum=0, maximum=200),
        }

    @staticmethod
    def _message_has_emoji(platform: str, emoji: str, **kwargs) -> bool:
        clean = str(emoji or "").strip()
        if not clean:
            return False

        if str(platform or "").strip().lower() == "discord":
            msg = kwargs.get("message")
            reactions = list(getattr(msg, "reactions", []) or [])
            return any(str(getattr(r, "emoji", "")).strip() == clean for r in reactions)

        if str(platform or "").strip().lower() == "telegram":
            msg = kwargs.get("message")
            if not isinstance(msg, dict):
                return False
            for key in ("reaction", "reactions"):
                raw = msg.get(key)
                if isinstance(raw, list):
                    for entry in raw:
                        if isinstance(entry, dict):
                            val = str(entry.get("emoji") or (entry.get("type") or {}).get("emoji") or "").strip()
                            if val == clean:
                                return True
                        elif str(entry or "").strip() == clean:
                            return True
            return False

        return False

    @staticmethod
    def _normalize_emoji(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        token = text.split()[0].strip().strip('"').strip("'").strip("`")
        if not token:
            return ""
        # Require non-ASCII to avoid LLM returning plain words.
        if all(ord(ch) < 128 for ch in token):
            return ""
        # Favor unicode emoji for cross-platform compatibility.
        if token.startswith("<") and token.endswith(">"):
            return ""
        return token[:16]

    @staticmethod
    def _cooldown_key(platform: str, scope: str, mode: str) -> str:
        safe_platform = str(platform or "unknown").strip().lower() or "unknown"
        safe_scope = str(scope or "global").strip() or "global"
        safe_mode = str(mode or "reply").strip().lower() or "reply"
        return f"tater:emoji_ai_responder:last:{safe_platform}:{safe_scope}:{safe_mode}"

    def _cooldown_allows(self, *, platform: str, scope: str, mode: str, cooldown_seconds: int) -> bool:
        if cooldown_seconds <= 0:
            return True
        key = self._cooldown_key(platform, scope, mode)
        raw = redis_client.get(key)
        try:
            last = int(str(raw).strip()) if raw is not None else 0
        except Exception:
            last = 0
        now = int(time.time())
        return (now - last) >= cooldown_seconds

    def _mark_cooldown(self, *, platform: str, scope: str, mode: str, cooldown_seconds: int) -> None:
        if cooldown_seconds <= 0:
            return
        key = self._cooldown_key(platform, scope, mode)
        now = int(time.time())
        ttl = max(3600, cooldown_seconds * 10)
        try:
            redis_client.set(key, str(now), ex=ttl)
        except Exception:
            redis_client.set(key, str(now))

    @staticmethod
    def _reaction_scope(reaction: Any) -> str:
        message = getattr(reaction, "message", None)
        if message is None:
            return "global"

        channel = getattr(message, "channel", None)
        channel_id = getattr(channel, "id", None)
        if channel_id is not None:
            return str(channel_id)

        room_id = getattr(message, "room_id", None)
        if room_id:
            return str(room_id)

        chat_id = getattr(message, "chat_id", None)
        if chat_id:
            return str(chat_id)

        return "global"

    async def _suggest_emoji(self, context_text: str, llm_client=None) -> str:
        text = (context_text or "").strip()
        if not text:
            return ""

        client = llm_client or _fallback_llm_client()
        prompt = (
            "Choose exactly one Unicode emoji that best matches the sentiment or intent of this text.\n"
            "Return JSON only:\n"
            '{\n'
            '  "function": "suggest_emoji",\n'
            '  "arguments": {"emoji": "🔥"}\n'
            "}\n"
            "No markdown. No extra text.\n\n"
            f"TEXT:\n{text}"
        )

        try:
            response = await client.chat(
                messages=[
                    {"role": "system", "content": "You pick one context-appropriate Unicode emoji."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=60,
            )
        except Exception as exc:
            logger.debug("[emoji_ai_responder] LLM call failed: %s", exc)
            return ""

        ai_reply = (response.get("message") or {}).get("content", "")
        ai_reply = str(ai_reply or "").strip()
        if not ai_reply:
            return ""

        parsed = None
        try:
            parsed = json.loads(ai_reply)
        except Exception:
            try:
                parsed = json.loads(extract_json(ai_reply) or "{}")
            except Exception:
                parsed = None

        emoji = ""
        if isinstance(parsed, dict):
            if parsed.get("function") == "suggest_emoji":
                emoji = self._normalize_emoji((parsed.get("arguments") or {}).get("emoji"))
            elif "emoji" in parsed:
                emoji = self._normalize_emoji(parsed.get("emoji"))

        if emoji:
            return emoji
        return self._normalize_emoji(ai_reply)

    async def on_assistant_response(
        self,
        *,
        platform: str,
        user_text: str,
        assistant_text: str = "",
        llm_client=None,
        scope: str = "",
        **kwargs,
    ) -> str:
        if not get_plugin_enabled(self.name):
            return ""

        settings = self._get_settings()
        if not settings["enable_auto_reaction_on_reply"]:
            return ""

        message_text = (user_text or "").strip()
        if len(message_text) < settings["min_message_length"]:
            return ""

        if not self._cooldown_allows(
            platform=platform,
            scope=scope,
            mode="reply",
            cooldown_seconds=settings["reply_reaction_cooldown_seconds"],
        ):
            return ""

        chance = float(settings["reply_reaction_chance_percent"]) / 100.0
        if chance <= 0 or random.random() > chance:
            return ""

        emoji = await self._suggest_emoji(message_text, llm_client=llm_client)
        if emoji and self._message_has_emoji(platform, emoji, **kwargs):
            return ""
        if emoji:
            self._mark_cooldown(
                platform=platform,
                scope=scope,
                mode="reply",
                cooldown_seconds=settings["reply_reaction_cooldown_seconds"],
            )
        return emoji

    async def on_reaction_add(self, reaction, user):
        if getattr(user, "bot", False):
            return
        if not get_plugin_enabled(self.name):
            return

        settings = self._get_settings()
        if not settings["enable_on_reaction_add"]:
            return

        message = getattr(reaction, "message", None)
        message_content = str(getattr(message, "content", "") or "").strip()
        if len(message_content) < settings["min_message_length"]:
            return

        scope = self._reaction_scope(reaction)
        if not self._cooldown_allows(
            platform="discord",
            scope=scope,
            mode="chain",
            cooldown_seconds=settings["reaction_chain_cooldown_seconds"],
        ):
            return

        chance = float(settings["reaction_chain_chance_percent"]) / 100.0
        if chance <= 0 or random.random() > chance:
            return

        emoji = await self._suggest_emoji(message_content, llm_client=None)
        if not emoji:
            return

        try:
            existing = list(getattr(message, "reactions", []) or [])
            if any(str(getattr(r, "emoji", "")) == emoji for r in existing):
                return
            await message.add_reaction(emoji)
            self._mark_cooldown(
                platform="discord",
                scope=scope,
                mode="chain",
                cooldown_seconds=settings["reaction_chain_cooldown_seconds"],
            )
        except Exception as exc:
            logger.debug("[emoji_ai_responder] add_reaction failed: %s", exc)


plugin = EmojiAIResponderPlugin()
