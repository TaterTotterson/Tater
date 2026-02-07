# plugins/broadcast.py
import asyncio
import logging
import re
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
    version = "1.0.2"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "broadcast",\n'
        '  "arguments": {\n'
        '    "text": "<what to announce>"\n'
        "  }\n"
        "}\n"
    )
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
    }

    waiting_prompt_template = (
        "Write a short friendly message telling {mention} you’re broadcasting that announcement now. "
        "Only output that message."
    )

    platforms = ["homeassistant", "homekit", "xbmc", "webui", "discord", "telegram", "matrix", "irc"]

    # ──────────────────────────────────────────────────────────────────────────
    # Settings / HA
    # ──────────────────────────────────────────────────────────────────────────

    def _get_settings(self) -> dict:
        return (
            redis_client.hgetall(f"plugin_settings:{self.settings_category}")
            or redis_client.hgetall(f"plugin_settings: {self.settings_category}")
            or {}
        )

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
        return [e for e in ids if e]

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
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
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

    def _tts_speak(self, ha_base: str, token: str, tts_entity: str, players: list[str], message: str) -> None:
        """
        Mimics doorbell_alert:
          - POST /api/services/tts/speak with entity_id + media_player_entity_id + message + cache
          - fallback to /api/services/tts/piper_say if speak fails
        Speaks per-player for max compatibility.
        """
        svc_url = f"{ha_base}/api/services/tts/speak"
        headers = self._ha_headers(token)

        for mp in players:
            data = {
                "entity_id": tts_entity,
                "media_player_entity_id": mp,
                "message": message,
                "cache": True,
            }
            r = requests.post(svc_url, headers=headers, json=data, timeout=15)
            if r.status_code >= 400:
                fallback_url = f"{ha_base}/api/services/tts/piper_say"
                r2 = requests.post(fallback_url, headers=headers, json=data, timeout=15)
                if r2.status_code >= 400:
                    raise RuntimeError(
                        f"TTS failed (speak:{r.status_code}, piper_say:{r2.status_code})"
                    )

    async def _broadcast(self, raw_text: str, llm_client) -> str:
        s = self._get_settings()

        ha_settings = redis_client.hgetall("homeassistant_settings") or {}
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
            await asyncio.to_thread(self._tts_speak, ha_base, token, tts_entity, players, announcement)
        except Exception as e:
            logger.error(f"[broadcast] TTS call failed: {e}")
            return "Broadcast failed (TTS service call error)."

        return announcement

    # ──────────────────────────────────────────────────────────────────────────
    # Platform handlers
    # ──────────────────────────────────────────────────────────────────────────

    async def handle_homeassistant(self, args, llm_client):
        args = args or {}
        return (await self._broadcast(args.get("text") or "", llm_client)).strip()

    async def handle_webui(self, args, llm_client):
        args = args or {}
        raw_text = args.get("text") or ""

        async def inner():
            return await self._broadcast(raw_text, llm_client)

        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_homekit(self, args, llm_client):
        args = args or {}
        final = await self._broadcast(args.get("text") or "", llm_client)
        return self._siri_flatten(final)

    async def handle_xbmc(self, args, llm_client):
        args = args or {}
        final = await self._broadcast(args.get("text") or "", llm_client)
        return (final or "No announcement.").strip()

    async def handle_discord(self, message, args, llm_client):
        return await self.handle_webui(args, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return await self.handle_webui(args, llm_client)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self.handle_webui(args, llm_client)


plugin = BroadcastPlugin()
