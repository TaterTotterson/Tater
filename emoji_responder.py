import json
import logging
import random
import time
from typing import Any, Dict, Optional

from helpers import extract_json, get_llm_client_from_env, redis_client

logger = logging.getLogger("emoji_responder")

EMOJI_SETTINGS_KEY = "emoji_responder_settings"
EMOJI_SETTINGS_LEGACY_KEYS = (
    "verba_settings:Emoji AI Responder",
    "verba_settings: Emoji AI Responder",
)

_FALLBACK_LLM = None


def _fallback_llm_client():
    global _FALLBACK_LLM
    if _FALLBACK_LLM is None:
        _FALLBACK_LLM = get_llm_client_from_env()
    return _FALLBACK_LLM


class EmojiResponder:
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

    def _load_raw_settings(self) -> Dict[str, Any]:
        raw = redis_client.hgetall(EMOJI_SETTINGS_KEY) or {}
        if raw:
            return raw
        for key in EMOJI_SETTINGS_LEGACY_KEYS:
            legacy = redis_client.hgetall(key) or {}
            if legacy:
                return legacy
        return {}

    def _get_settings(self) -> Dict[str, Any]:
        raw = self._load_raw_settings()
        reply_chance_raw = raw.get("REPLY_REACTION_CHANCE_PERCENT")
        if reply_chance_raw in (None, ""):
            reply_chance_raw = raw.get("AUTO_REACTION_CHANCE_PERCENT")
        reply_cooldown_raw = raw.get("REPLY_REACTION_COOLDOWN_SECONDS")
        if reply_cooldown_raw in (None, ""):
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
    def save_settings(settings: Dict[str, Any]) -> None:
        payload = {
            "ENABLE_ON_REACTION_ADD": "true" if bool(settings.get("enable_on_reaction_add", True)) else "false",
            "ENABLE_AUTO_REACTION_ON_REPLY": "true"
            if bool(settings.get("enable_auto_reaction_on_reply", True))
            else "false",
            "REACTION_CHAIN_CHANCE_PERCENT": str(int(settings.get("reaction_chain_chance_percent", 100))),
            "REPLY_REACTION_CHANCE_PERCENT": str(int(settings.get("reply_reaction_chance_percent", 12))),
            "REACTION_CHAIN_COOLDOWN_SECONDS": str(int(settings.get("reaction_chain_cooldown_seconds", 30))),
            "REPLY_REACTION_COOLDOWN_SECONDS": str(int(settings.get("reply_reaction_cooldown_seconds", 120))),
            "MIN_MESSAGE_LENGTH": str(int(settings.get("min_message_length", 4))),
        }
        redis_client.hset(EMOJI_SETTINGS_KEY, mapping=payload)

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
        if all(ord(ch) < 128 for ch in token):
            return ""
        if token.startswith("<") and token.endswith(">"):
            return ""
        return token[:16]

    @staticmethod
    def _cooldown_key(platform: str, scope: str, mode: str) -> str:
        safe_platform = str(platform or "unknown").strip().lower() or "unknown"
        safe_scope = str(scope or "global").strip() or "global"
        safe_mode = str(mode or "reply").strip().lower() or "reply"
        return f"tater:emoji_responder:last:{safe_platform}:{safe_scope}:{safe_mode}"

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
            logger.debug("[emoji_responder] LLM call failed: %s", exc)
            return ""

        ai_reply = str((response.get("message") or {}).get("content", "") or "").strip()
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

    async def on_reaction_add(self, reaction: Any, user: Any, *, llm_client=None) -> str:
        if getattr(user, "bot", False):
            return ""

        settings = self._get_settings()
        if not settings["enable_on_reaction_add"]:
            return ""

        message = getattr(reaction, "message", None)
        message_content = str(getattr(message, "content", "") or "").strip()
        if len(message_content) < settings["min_message_length"]:
            return ""

        scope = self._reaction_scope(reaction)
        if not self._cooldown_allows(
            platform="discord",
            scope=scope,
            mode="chain",
            cooldown_seconds=settings["reaction_chain_cooldown_seconds"],
        ):
            return ""

        chance = float(settings["reaction_chain_chance_percent"]) / 100.0
        if chance <= 0 or random.random() > chance:
            return ""

        emoji = await self._suggest_emoji(message_content, llm_client=llm_client)
        if not emoji:
            return ""

        try:
            existing = list(getattr(message, "reactions", []) or [])
            if any(str(getattr(r, "emoji", "")).strip() == emoji for r in existing):
                return ""
            await message.add_reaction(emoji)
            self._mark_cooldown(
                platform="discord",
                scope=scope,
                mode="chain",
                cooldown_seconds=settings["reaction_chain_cooldown_seconds"],
            )
            return emoji
        except Exception as exc:
            logger.debug("[emoji_responder] add_reaction failed: %s", exc)
            return ""


emoji_responder = EmojiResponder()


def get_emoji_settings() -> Dict[str, Any]:
    return emoji_responder._get_settings()


def save_emoji_settings(settings: Dict[str, Any]) -> None:
    emoji_responder.save_settings(settings)
