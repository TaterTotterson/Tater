# plugins/find_my_phone.py
import asyncio
import logging
import re
import time
import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client

load_dotenv()
logger = logging.getLogger("find_my_phone")
logger.setLevel(logging.INFO)


class FindMyPhonePlugin(ToolPlugin):
    """
    Make a phone play an alert sound via Home Assistant Companion App notify service.

    Natural asks:
      - "where is my phone?"
      - "find my phone"
      - "ring my phone"
      - "locate my phone"
      - "make my phone beep"
    """

    name = "find_my_phone"
    plugin_name = "Find My Phone"
    version = "1.0.2"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "find_my_phone",\n'
        '  "arguments": {}\n'
        "}\n"
    )
    description = (
        "Use this when the user asks where their phone is, or asks to find, ring, "
        "locate, or make their phone play a sound."
    )
    plugin_dec = "Ping or ring your phone through Home Assistant so you can locate it."
    pretty_name = "Finding Your Phone"
    settings_category = "Find My Phone"
    required_settings = {
        "MOBILE_NOTIFY_SERVICE": {
            "label": "Notify service (ex: notify.mobile_app_taters_iphone)",
            "type": "string",
            "default": "",
        },
        "DEFAULT_TITLE": {
            "label": "Default notification title",
            "type": "string",
            "default": "Find My Phone",
        },
        "DEFAULT_MESSAGE": {
            "label": "Default notification message",
            "type": "string",
            "default": "🔔 Phone alert requested!",
        },
        "ALERT_COUNT": {
            "label": "How many times to send the alert (1-5)",
            "type": "int",
            "default": 2,
        },
    }
    waiting_prompt_template = (
        "Write a short, friendly message telling {mention} you're looking for their phone now. "
        "Only output that message."
    )
    # We will use this to generate the *final* confirmation message
    final_prompt_template = (
        "Write a short, friendly message telling {mention} their phone should be making noise now "
        "and they should go find it. Only output that message."
    )
    platforms = ["webui", "homeassistant", "homekit", "xbmc", "discord", "telegram", "matrix", "irc"]

    def _load_settings(self) -> dict:
        return redis_client.hgetall(f"plugin_settings:{self.settings_category}")

    def _normalize_notify_service(self, raw: str) -> str:
        raw = (raw or "").strip()
        if raw.startswith("notify."):
            raw = raw.split("notify.", 1)[1].strip()
        return raw

    def _ha_post_service(self, base_url: str, token: str, domain: str, service: str, payload: dict) -> tuple[int, str]:
        base_url = (base_url or "").rstrip("/")
        url = f"{base_url}/api/services/{domain}/{service}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        return resp.status_code, resp.text

    def _trigger_phone_alert(self) -> dict:
        """
        Returns a dict:
          - ok: bool
          - count: int (how many alerts we attempted)
          - error: str
        """
        settings = self._load_settings()
        ha_settings = redis_client.hgetall("homeassistant_settings") or {}
        ha_base = (ha_settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        ha_token = (ha_settings.get("HA_TOKEN") or "").strip()
        notify_service_raw = settings.get("MOBILE_NOTIFY_SERVICE", "").strip()

        title = (settings.get("DEFAULT_TITLE", "") or "Find My Phone").strip()
        message = (settings.get("DEFAULT_MESSAGE", "") or "Phone alert requested.").strip()

        if not ha_token:
            return {
                "ok": False,
                "count": 0,
                "error": (
                    "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                    "and add a Long-Lived Access Token."
                ),
            }

        if not notify_service_raw:
            return {
                "ok": False,
                "count": 0,
                "error": "Find My Phone is not configured. Please set MOBILE_NOTIFY_SERVICE in plugin settings.",
            }

        service = self._normalize_notify_service(notify_service_raw)
        if not service:
            return {
                "ok": False,
                "count": 0,
                "error": "MOBILE_NOTIFY_SERVICE is invalid. Use notify.mobile_app_<device> or mobile_app_<device>.",
            }

        # Parse count
        raw_count = settings.get("ALERT_COUNT", 2)
        try:
            count = int(raw_count)
        except Exception:
            count = 2
        count = max(1, min(count, 5))

        # Companion app notify payload
        payload = {
            "title": title,
            "message": message,
            "data": {
                # common “be loud / immediate” knobs
                "ttl": 0,
                "priority": "high",

                # Android hint (best-effort; device/OS varies)
                "channel": "alarm_stream",

                # iOS hint (best-effort; critical alerts require proper setup/permissions)
                "push": {
                    "sound": {
                        "name": "default",
                        "critical": 1,
                        "volume": 1.0
                    }
                },

                # best-effort vibration hints for some clients
                "vibrationPattern": [100, 1000, 100, 1000, 100],
            },
        }

        last_status, last_text = 0, ""
        for i in range(count):
            last_status, last_text = self._ha_post_service(
                base_url=ha_base,
                token=ha_token,
                domain="notify",
                service=service,
                payload=payload,
            )

            if last_status not in (200, 201):
                logger.error(f"[find_my_phone] HA notify failed HTTP {last_status}: {last_text}")
                return {
                    "ok": False,
                    "count": i + 1,
                    "error": f"Home Assistant notify failed (HTTP {last_status}). Check HA URL/token/service name.",
                }

            # Small pause so multiple alerts are less likely to collapse into one
            if i < count - 1:
                time.sleep(0.6)

        return {"ok": True, "count": count, "error": ""}

    async def _llm_final_message(self, llm_client, mention: str, count: int) -> str:
        fallback = f"✅ Okay! I sent the phone alert {count} time(s). Listen for it now."
        if not llm_client:
            return fallback

        prompt = self.final_prompt_template.format(mention=mention, count=count)
        try:
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            text = (resp.get("message", {}) or {}).get("content", "")
            text = (text or "").strip()
            return text or fallback
        except Exception as e:
            logger.error(f"[find_my_phone] final LLM message error: {e}")
            return fallback

    def _siri_flatten(self, text: str | None) -> str:
        if not text:
            return "No answer available."
        out = str(text)
        out = re.sub(r"[`*_]{1,3}", "", out)
        out = re.sub(r"\s+", " ", out).strip()
        return out[:280]

    async def _run(self, llm_client, mention: str = "you") -> str:
        result = await asyncio.to_thread(self._trigger_phone_alert)

        if not result.get("ok"):
            return (result.get("error") or "Something went wrong triggering the phone alert.").strip()

        count = int(result.get("count", 1) or 1)
        return await self._llm_final_message(llm_client, mention=mention, count=count)

    # ---- platform handlers ----
    async def handle_webui(self, args, llm_client):
        async def inner():
            return await self._run(llm_client, mention="you")

        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_homeassistant(self, args, llm_client):
        return (await self._run(llm_client, mention="you")).strip()

    async def handle_homekit(self, args, llm_client):
        answer = await self._run(llm_client, mention="you")
        return self._siri_flatten(answer)

    async def handle_xbmc(self, args, llm_client):
        return (await self._run(llm_client, mention="you")).strip()

    async def handle_discord(self, message, args, llm_client):
        return (await self._run(llm_client, mention="you")).strip()

    async def handle_telegram(self, update, args, llm_client):
        return (await self._run(llm_client, mention="you")).strip()

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return (await self._run(llm_client, mention="you")).strip()

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return (await self._run(llm_client, mention="you")).strip()


plugin = FindMyPhonePlugin()
