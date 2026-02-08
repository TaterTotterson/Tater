# plugins/broadcast.py
import asyncio
import logging
import re
from typing import Any, Dict, List, Tuple
from dotenv import load_dotenv
import requests

from plugin_base import ToolPlugin
from helpers import redis_client, get_tater_name

load_dotenv()
logger = logging.getLogger("broadcast")
logger.setLevel(logging.INFO)


class BroadcastPlugin(ToolPlugin):
    """
    Broadcast a spoken announcement via Home Assistant TTS to up to 5 configured playback devices.
    Mimics the doorbell_alert TTS behavior (tts/speak with fallback to tts/piper_say).
    """
    name = "broadcast"
    plugin_name = "Broadcast"
    version = "1.1.1"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "broadcast",\n'
        '  "arguments": {\n'
        '    "text": "<what to announce>"\n'
        "  }\n"
        "}\n"
    )
    required_args = ["text"]
    optional_args = ["message", "content", "announcement", "request"]
    description = (
        "Send a one-time whole-house spoken announcement using Home Assistant TTS on the configured devices. "
        "Use ONLY when the user explicitly asks to broadcast/announce/page an audio message (e.g., 'announce dinner is ready', "
        "'broadcast this', 'page the house') and provides what to say."
    )
    plugin_dec = "Send a one-time spoken announcement to your Home Assistant media players."
    pretty_name = "Broadcast Announcement"
    settings_category = "Broadcast"

    required_settings = {
        "TTS_ENTITY": {
            "label": "TTS Entity",
            "type": "string",
            "default": "tts.piper",
            "description": "TTS entity to use (e.g., tts.piper)."
        },

        # ---- Playback targets (media_player.*) ----
        "DEVICE_1": {"label": "Broadcast device #1", "type": "string", "default": "", "description": "media_player.*"},
        "DEVICE_2": {"label": "Broadcast device #2", "type": "string", "default": "", "description": "media_player.*"},
        "DEVICE_3": {"label": "Broadcast device #3", "type": "string", "default": "", "description": "media_player.*"},
        "DEVICE_4": {"label": "Broadcast device #4", "type": "string", "default": "", "description": "media_player.*"},
        "DEVICE_5": {"label": "Broadcast device #5", "type": "string", "default": "", "description": "media_player.*"},
        "REQUEST_TIMEOUT_SECONDS": {
            "label": "Request Timeout (seconds)",
            "type": "number",
            "default": 15,
            "description": "Timeout for each Home Assistant TTS service call.",
        },
    }

    waiting_prompt_template = (
        "Write a short friendly message telling {mention} you’re broadcasting that announcement now. "
        "Only output that message."
    )

    platforms = ["homeassistant", "homekit", "xbmc", "webui", "discord", "telegram", "matrix", "irc"]

    # ──────────────────────────────────────────────────────────────────────────
    # Settings / HA
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _decode_redis_map(raw: Dict[Any, Any] | None) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for key, value in (raw or {}).items():
            k = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
            if isinstance(value, (bytes, bytearray)):
                out[k] = value.decode("utf-8", "ignore")
            else:
                out[k] = str(value or "")
        return out

    def _get_settings(self) -> dict:
        raw = redis_client.hgetall(f"plugin_settings:{self.settings_category}") or redis_client.hgetall(
            f"plugin_settings: {self.settings_category}"
        )
        return self._decode_redis_map(raw)

    @staticmethod
    def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(float(value))
        except Exception:
            parsed = int(default)
        if parsed < minimum:
            return minimum
        if parsed > maximum:
            return maximum
        return parsed

    def _ha_headers(self, token: str) -> dict:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _targets(self, s: dict) -> list[str]:
        ids = [
            (s.get("DEVICE_1") or "").strip(),
            (s.get("DEVICE_2") or "").strip(),
            (s.get("DEVICE_3") or "").strip(),
            (s.get("DEVICE_4") or "").strip(),
            (s.get("DEVICE_5") or "").strip(),
        ]
        out: List[str] = []
        seen = set()
        for entity in ids:
            if not entity:
                continue
            if not entity.startswith("media_player."):
                logger.warning("[broadcast] Ignoring non-media_player target: %s", entity)
                continue
            if entity in seen:
                continue
            seen.add(entity)
            out.append(entity)
        return out

    # ──────────────────────────────────────────────────────────────────────────
    # Formatting helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _siri_flatten(self, text: str | None) -> str:
        if not text:
            return "No announcement."
        out = str(text)
        out = re.sub(r"[`*_]{1,3}", "", out)
        out = re.sub(r"\s+", " ", out).strip()
        return out[:450]

    @staticmethod
    def _extract_announcement_arg(args: Dict[str, Any]) -> str:
        args = args or {}
        for key in ("text", "announcement", "message", "content", "request"):
            value = args.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    async def _make_announcement_text(self, raw_text: str, llm_client) -> str:
        raw_text = (raw_text or "").strip()
        if not raw_text:
            return ""

        first, last = get_tater_name()
        max_chars = 220

        prompt = (
            f"Your name is {first} {last}. The user wants you to broadcast an announcement.\n\n"
            f"User said: {raw_text}\n\n"
            "Rewrite that into ONE short, natural-sounding sentence suitable for a whole-house announcement.\n"
            "Rules:\n"
            "- No emojis\n"
            "- No markdown\n"
            "- Keep it friendly and clear\n"
            f"- Under {max_chars} characters\n"
            "Only output the sentence."
        )

        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": "You rewrite announcements for clear spoken delivery."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=120,
                temperature=0.2,
            )
            txt = (resp.get("message") or {}).get("content", "")
            txt = (txt or "").strip().strip('"').strip()
            if txt:
                return txt[:max_chars]
        except Exception as e:
            logger.warning(f"[broadcast] LLM rewrite failed, using raw text: {e}")

        return raw_text[:max_chars]

    # ──────────────────────────────────────────────────────────────────────────
    # HA TTS (mimic doorbell_alert)
    # ──────────────────────────────────────────────────────────────────────────

    def _tts_speak(
        self,
        ha_base: str,
        token: str,
        tts_entity: str,
        players: list[str],
        message: str,
        timeout_seconds: int,
    ) -> Tuple[int, List[str]]:
        """
        Mimics doorbell_alert:
          - POST /api/services/tts/speak with entity_id + media_player_entity_id + message + cache
          - fallback to /api/services/tts/piper_say if speak fails
        Speaks per-player for max compatibility.
        """
        svc_url = f"{ha_base}/api/services/tts/speak"
        headers = self._ha_headers(token)
        ok_count = 0
        failures: List[str] = []
        fallback_url = f"{ha_base}/api/services/tts/piper_say"

        for mp in players:
            data = {
                "entity_id": tts_entity,
                "media_player_entity_id": mp,
                "message": message,
                "cache": True,
            }
            speak_status: int | None = None
            try:
                r = requests.post(svc_url, headers=headers, json=data, timeout=timeout_seconds)
                speak_status = int(r.status_code)
                if speak_status < 400:
                    ok_count += 1
                    continue
            except Exception as exc:
                logger.warning("[broadcast] speak failed for %s: %s", mp, exc)

            try:
                r2 = requests.post(fallback_url, headers=headers, json=data, timeout=timeout_seconds)
                fallback_status = int(r2.status_code)
                if fallback_status < 400:
                    ok_count += 1
                else:
                    if speak_status is None:
                        failures.append(f"{mp} (speak:error, piper_say:{fallback_status})")
                    else:
                        failures.append(f"{mp} (speak:{speak_status}, piper_say:{fallback_status})")
            except Exception as exc:
                logger.warning("[broadcast] piper_say failed for %s: %s", mp, exc)
                if speak_status is None:
                    failures.append(f"{mp} (speak:error, piper_say:error)")
                else:
                    failures.append(f"{mp} (speak:{speak_status}, piper_say:error)")
        return ok_count, failures

    async def _broadcast(self, raw_text: str, llm_client) -> str:
        s = self._get_settings()
        timeout_seconds = self._as_int(s.get("REQUEST_TIMEOUT_SECONDS"), default=15, minimum=5, maximum=120)

        ha_settings = self._decode_redis_map(redis_client.hgetall("homeassistant_settings") or {})
        ha_base = (ha_settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        token = (ha_settings.get("HA_TOKEN") or "").strip()
        if not token:
            return (
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        tts_entity = (s.get("TTS_ENTITY") or "tts.piper").strip() or "tts.piper"
        players = self._targets(s)
        if not players:
            return "No broadcast devices are configured (DEVICE_1..DEVICE_5)."

        announcement = (
            await self._make_announcement_text(raw_text, llm_client)
            if llm_client
            else (raw_text or "").strip()
        )
        if not announcement:
            return "No announcement text provided."

        try:
            ok_count, failures = await asyncio.to_thread(
                self._tts_speak,
                ha_base,
                token,
                tts_entity,
                players,
                announcement,
                timeout_seconds,
            )
        except Exception as e:
            logger.error(f"[broadcast] TTS call failed: {e}")
            return "Broadcast failed (TTS service call error)."

        total = len(players)
        if ok_count <= 0:
            detail = f" Failures: {', '.join(failures[:3])}." if failures else ""
            return f"Broadcast failed (all target devices rejected TTS).{detail}"
        if failures:
            return (
                f"Broadcast sent to {ok_count}/{total} devices. "
                f"Some devices failed: {', '.join(failures[:3])}. "
                f"Announcement: {announcement}"
            )
        return f"Broadcast sent to {ok_count}/{total} devices. Announcement: {announcement}"

    # ──────────────────────────────────────────────────────────────────────────
    # Platform handlers
    # ──────────────────────────────────────────────────────────────────────────

    async def handle_homeassistant(self, args, llm_client):
        args = args or {}
        text = self._extract_announcement_arg(args)
        return (await self._broadcast(text, llm_client)).strip()

    async def handle_webui(self, args, llm_client):
        args = args or {}
        raw_text = self._extract_announcement_arg(args)
        return await self._broadcast(raw_text, llm_client)

    async def handle_homekit(self, args, llm_client):
        args = args or {}
        final = await self._broadcast(self._extract_announcement_arg(args), llm_client)
        return self._siri_flatten(final)

    async def handle_xbmc(self, args, llm_client):
        args = args or {}
        final = await self._broadcast(self._extract_announcement_arg(args), llm_client)
        return (final or "No announcement.").strip()

    async def handle_discord(self, message, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        return await self.handle_webui(args or {}, llm_client)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)


plugin = BroadcastPlugin()
