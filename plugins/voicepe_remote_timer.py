# plugins/voicepe_remote_timer.py
import asyncio
import logging
import re
from dotenv import load_dotenv
import requests

from plugin_base import ToolPlugin
from helpers import redis_client

load_dotenv()
logger = logging.getLogger("voicepe_remote_timer")
logger.setLevel(logging.INFO)


class VoicePERemoteTimerPlugin(ToolPlugin):
    """
    Start (and optionally cancel) the *device-local* Voice PE remote timer you added via ESPHome.

    It works by calling HA services:
      - number.set_value  -> sets number.voicepe_remote_timer_seconds
      - button.press      -> presses button.voicepe_remote_timer_start
      - (optional) button.press -> presses button.voicepe_remote_timer_cancel
    """

    name = "voicepe_remote_timer"
    pretty_name = "Voice PE Remote Timer"
    settings_category = "Voice PE Remote Timer"

    usage = (
        "{\n"
        '  "function": "voicepe_remote_timer",\n'
        '  "arguments": {\n'
        '    "duration": "5 minutes"\n'
        "  }\n"
        "}\n"
    )

    description = (
        "Start a device-local timer on the configured Voice PE (ESPHome) by setting the remote timer seconds "
        "and pressing the start button in Home Assistant. Use when the user asks to set a timer."
    )

    required_settings = {
        "HA_BASE_URL": {
            "label": "Home Assistant Base URL",
            "type": "string",
            "default": "http://homeassistant.local:8123",
            "description": "Base URL of your Home Assistant instance."
        },
        "HA_TOKEN": {
            "label": "Home Assistant Long-Lived Token",
            "type": "string",
            "default": "",
            "description": "Create in HA: Profile → Long-Lived Access Tokens."
        },
        "TIMER_SECONDS_ENTITY": {
            "label": "Remote Timer Seconds (number.*)",
            "type": "string",
            "default": "number.voicepe_remote_timer_seconds",
            "description": "The ESPHome number entity that sets the timer duration in seconds."
        },
        "START_BUTTON_ENTITY": {
            "label": "Remote Timer Start (button.*)",
            "type": "string",
            "default": "button.voicepe_remote_timer_start",
            "description": "The ESPHome button entity that starts the device-local timer."
        },
        "CANCEL_BUTTON_ENTITY": {
            "label": "Remote Timer Cancel (button.*, optional)",
            "type": "string",
            "default": "button.voicepe_remote_timer_cancel",
            "description": "Optional cancel button entity. Used if you implement a cancel action later."
        },
        "MAX_SECONDS": {
            "label": "Max Seconds",
            "type": "number",
            "default": 7200,
            "description": "Clamp very large durations (default 2 hours)."
        },
    }

    waiting_prompt_template = (
        "Write a short friendly message telling {mention} you’re starting that timer now. "
        "Only output that message."
    )

    platforms = ["homeassistant", "homekit", "xbmc", "webui"]

    # ─────────────────────────────────────────────────────────────
    # Settings / HA helpers
    # ─────────────────────────────────────────────────────────────

    def _get_settings(self) -> dict:
        return (
            redis_client.hgetall(f"plugin_settings:{self.settings_category}")
            or redis_client.hgetall(f"plugin_settings: {self.settings_category}")
            or {}
        )

    def _ha_headers(self, token: str) -> dict:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _post_service(self, ha_base: str, token: str, domain: str, service: str, data: dict) -> None:
        url = f"{ha_base}/api/services/{domain}/{service}"
        r = requests.post(url, headers=self._ha_headers(token), json=data, timeout=15)
        if r.status_code >= 400:
            raise RuntimeError(f"{domain}.{service} failed: {r.status_code} {r.text}")

    # ─────────────────────────────────────────────────────────────
    # Duration parsing
    # ─────────────────────────────────────────────────────────────

    def _parse_duration_to_seconds(self, text: str, max_seconds: int) -> int:
        """
        Accepts:
          - "20 seconds", "20 sec", "20s"
          - "5 minutes", "5 min", "5m"
          - "1 hour", "1h"
          - "1h 5m 10s"
        """
        t = (text or "").strip().lower()
        t = re.sub(r"\s+", " ", t)
        if not t:
            return 0

        # quick: plain integer = seconds
        if re.fullmatch(r"\d+", t):
            return max(0, min(int(t), max_seconds))

        unit_map = {
            "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
            "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
            "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
        }

        total = 0
        for num, unit in re.findall(r"(\d+)\s*([a-z]+)", t):
            mult = unit_map.get(unit)
            if mult:
                total += int(num) * mult

        # fallback: "5:00" or "mm:ss"
        if total == 0 and re.fullmatch(r"\d{1,2}:\d{2}", t):
            mm, ss = t.split(":")
            total = int(mm) * 60 + int(ss)

        if total < 0:
            total = 0
        if total > max_seconds:
            total = max_seconds
        return total

    # ─────────────────────────────────────────────────────────────
    # Core action
    # ─────────────────────────────────────────────────────────────

    async def _start_timer(self, duration_text: str) -> str:
        s = self._get_settings()

        ha_base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
        token = (s.get("HA_TOKEN") or "").strip()
        if not token:
            return "Voice PE Remote Timer is missing HA_TOKEN in settings."

        seconds_entity = (s.get("TIMER_SECONDS_ENTITY") or "").strip()
        start_entity = (s.get("START_BUTTON_ENTITY") or "").strip()
        if not seconds_entity or not start_entity:
            return "Voice PE Remote Timer is missing TIMER_SECONDS_ENTITY or START_BUTTON_ENTITY in settings."

        try:
            max_seconds = int(s.get("MAX_SECONDS") or 7200)
        except Exception:
            max_seconds = 7200

        seconds = self._parse_duration_to_seconds(duration_text, max_seconds)
        if seconds <= 0:
            return "No valid timer duration provided (example: '20 seconds', '5 minutes', '1h 10m')."

        def do_calls():
            # 1) set number value (seconds)
            self._post_service(
                ha_base,
                token,
                "number",
                "set_value",
                {"entity_id": seconds_entity, "value": seconds},
            )
            # 2) press start button
            self._post_service(
                ha_base,
                token,
                "button",
                "press",
                {"entity_id": start_entity},
            )

        try:
            await asyncio.to_thread(do_calls)
        except Exception as e:
            logger.error(f"[voicepe_remote_timer] HA service calls failed: {e}")
            return "Failed to start the Voice PE timer (Home Assistant service call error)."

        # friendly return for chat logs
        if seconds < 60:
            return f"Timer started for {seconds} seconds."
        mins = seconds // 60
        rem = seconds % 60
        if rem == 0:
            return f"Timer started for {mins} minute" + ("s." if mins != 1 else ".")
        return f"Timer started for {mins}m {rem}s."

    # ─────────────────────────────────────────────────────────────
    # Platform handlers (hybrid-safe webui)
    # ─────────────────────────────────────────────────────────────

    async def handle_homeassistant(self, args, llm_client):
        args = args or {}
        return (await self._start_timer(args.get("duration") or "")).strip()

    async def handle_webui(self, args, llm_client):
        args = args or {}
        dur = args.get("duration") or ""

        async def inner():
            return await self._start_timer(dur)

        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_homekit(self, args, llm_client):
        args = args or {}
        return (await self._start_timer(args.get("duration") or "")).strip()

    async def handle_xbmc(self, args, llm_client):
        args = args or {}
        return (await self._start_timer(args.get("duration") or "")).strip()


plugin = VoicePERemoteTimerPlugin()