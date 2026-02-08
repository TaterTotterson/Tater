# plugins/find_my_phone.py
import asyncio
import logging
import re
import time
from typing import Any, Dict

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
    version = "1.0.3"
    min_tater_version = "50"
    when_to_use = "Use when the user asks to find, ring, locate, or make their phone beep."
    usage = (
        "{\n"
        '  "function": "find_my_phone",\n'
        '  "arguments": {\n'
        '    "notify_service": "optional override (notify.mobile_app_<device> or mobile_app_<device>)",\n'
        '    "count": 2,\n'
        '    "title": "Find My Phone",\n'
        '    "message": "🔔 Phone alert requested!"\n'
        "  }\n"
        "}\n"
    )
    optional_args = ["notify_service", "mobile_notify_service", "device_service", "count", "alert_count", "title", "message", "content"]
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
            "type": "number",
            "default": 2,
        },
        "ALERT_DELAY_SECONDS": {
            "label": "Delay between alerts (seconds)",
            "type": "number",
            "default": 0.6,
        },
        "REQUEST_TIMEOUT_SECONDS": {
            "label": "HTTP timeout (seconds)",
            "type": "number",
            "default": 15,
        },
    }
    waiting_prompt_template = (
        "Write a short, friendly message telling {mention} you're looking for their phone now. "
        "Only output that message."
    )
    # We will use this to generate the *final* confirmation message
    final_prompt_template = (
        "Write a short, friendly message telling {mention} their phone should be making noise now "
        "and they should go find it. Mention that the alert was sent {count} time(s). "
        "Only output that message."
    )
    platforms = ["webui", "homeassistant", "homekit", "xbmc", "discord", "telegram", "matrix", "irc"]

    @staticmethod
    def _decode_map(raw: Dict[Any, Any] | None) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for key, value in (raw or {}).items():
            k = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
            if isinstance(value, (bytes, bytearray)):
                out[k] = value.decode("utf-8", "ignore")
            else:
                out[k] = str(value or "")
        return out

    @classmethod
    def _coerce_int(cls, value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(float(str(value).strip()))
        except Exception:
            parsed = int(default)
        if parsed < minimum:
            return minimum
        if parsed > maximum:
            return maximum
        return parsed

    @classmethod
    def _coerce_float(cls, value: Any, default: float, minimum: float, maximum: float) -> float:
        try:
            parsed = float(str(value).strip())
        except Exception:
            parsed = float(default)
        if parsed < minimum:
            return float(minimum)
        if parsed > maximum:
            return float(maximum)
        return float(parsed)

    def _load_settings(self) -> Dict[str, str]:
        raw = redis_client.hgetall(f"plugin_settings:{self.settings_category}") or redis_client.hgetall(
            f"plugin_settings: {self.settings_category}"
        )
        return self._decode_map(raw)

    def _load_homeassistant_settings(self) -> Dict[str, str]:
        return self._decode_map(redis_client.hgetall("homeassistant_settings") or {})

    def _load_default_notify_service(self) -> str:
        settings = self._decode_map(redis_client.hgetall("plugin_settings:Home Assistant Notifier") or {})
        return (settings.get("DEFAULT_DEVICE_SERVICE") or "").strip()

    def _normalize_notify_service(self, raw: str) -> str:
        raw = (raw or "").strip()
        raw = raw.replace("/", ".")
        if raw.startswith("notify."):
            raw = raw.split("notify.", 1)[1].strip()
        raw = raw.strip().strip(".")
        if not raw:
            return ""
        if not re.match(r"^[A-Za-z0-9_]+$", raw):
            return ""
        return raw

    def _extract_overrides(self, args: Dict[str, Any] | None) -> Dict[str, Any]:
        args = args or {}
        return {
            "notify_service": (
                args.get("notify_service")
                or args.get("mobile_notify_service")
                or args.get("device_service")
                or ""
            ),
            "title": args.get("title") or "",
            "message": args.get("message") or args.get("content") or "",
            "count": args.get("count") if args.get("count") is not None else args.get("alert_count"),
        }

    def _ha_post_service(
        self,
        base_url: str,
        token: str,
        domain: str,
        service: str,
        payload: Dict[str, Any],
        timeout_seconds: float,
    ) -> tuple[int, str]:
        base_url = (base_url or "").rstrip("/")
        url = f"{base_url}/api/services/{domain}/{service}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_seconds)
        return resp.status_code, resp.text

    def _trigger_phone_alert(self, overrides: Dict[str, Any] | None = None) -> dict:
        """
        Returns a dict:
          - ok: bool
          - count: int (how many alerts we attempted)
          - error: str
        """
        overrides = overrides or {}
        settings = self._load_settings()
        ha_settings = self._load_homeassistant_settings()
        ha_base = (ha_settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        ha_token = (ha_settings.get("HA_TOKEN") or "").strip()
        notify_service_raw = (
            (overrides.get("notify_service") or "").strip()
            or (settings.get("MOBILE_NOTIFY_SERVICE") or "").strip()
            or self._load_default_notify_service()
        )

        title = ((overrides.get("title") or "").strip() or (settings.get("DEFAULT_TITLE", "") or "Find My Phone")).strip()
        message = ((overrides.get("message") or "").strip() or (settings.get("DEFAULT_MESSAGE", "") or "Phone alert requested.")).strip()

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
                "error": (
                    "MOBILE_NOTIFY_SERVICE is invalid. Use notify.mobile_app_<device> or mobile_app_<device> "
                    "(letters/numbers/underscore)."
                ),
            }

        # Parse count
        raw_count = overrides.get("count") if overrides.get("count") is not None else settings.get("ALERT_COUNT", 2)
        count = self._coerce_int(raw_count, default=2, minimum=1, maximum=5)
        delay_seconds = self._coerce_float(settings.get("ALERT_DELAY_SECONDS", 0.6), default=0.6, minimum=0.0, maximum=10.0)
        timeout_seconds = self._coerce_float(settings.get("REQUEST_TIMEOUT_SECONDS", 15), default=15.0, minimum=3.0, maximum=120.0)

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
                timeout_seconds=timeout_seconds,
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
                time.sleep(delay_seconds)

        return {"ok": True, "count": count, "error": ""}

    async def _llm_final_message(self, llm_client, mention: str, count: int) -> str:
        fallback = f"✅ Okay! I sent the phone alert {count} time(s). Listen for it now."
        if not llm_client:
            return fallback

        prompt = self.final_prompt_template.format(mention=mention or "you", count=count)
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": "Write one short, friendly confirmation line."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=80,
            )
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

    async def _run(self, args: Dict[str, Any] | None, llm_client, mention: str = "you") -> str:
        overrides = self._extract_overrides(args)
        result = await asyncio.to_thread(self._trigger_phone_alert, overrides)

        if not result.get("ok"):
            return (result.get("error") or "Something went wrong triggering the phone alert.").strip()

        count = int(result.get("count", 1) or 1)
        return await self._llm_final_message(llm_client, mention=mention, count=count)

    # ---- platform handlers ----
    async def handle_webui(self, args, llm_client):
        return await self._run(args or {}, llm_client, mention="you")

    async def handle_homeassistant(self, args, llm_client):
        return (await self._run(args or {}, llm_client, mention="you")).strip()

    async def handle_homekit(self, args, llm_client):
        answer = await self._run(args or {}, llm_client, mention="you")
        return self._siri_flatten(answer)

    async def handle_xbmc(self, args, llm_client):
        return (await self._run(args or {}, llm_client, mention="you")).strip()

    async def handle_discord(self, message, args, llm_client):
        mention = getattr(getattr(message, "author", None), "mention", None) or "you"
        return (await self._run(args or {}, llm_client, mention=mention)).strip()

    async def handle_telegram(self, update, args, llm_client):
        sender = (update or {}).get("from") or {}
        username = (sender.get("username") or sender.get("first_name") or "you").strip()
        mention = f"@{username}" if username and username != "you" and not username.startswith("@") else (username or "you")
        return (await self._run(args or {}, llm_client, mention=mention)).strip()

    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        mention = str(sender or "you").strip() or "you"
        return (await self._run(args or {}, llm_client, mention=mention)).strip()

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        mention = str(user or "you").strip() or "you"
        return (await self._run(args or {}, llm_client, mention=mention)).strip()


plugin = FindMyPhonePlugin()
