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
    Voice PE Remote Timer (device-local, ESPHome-driven)

    Features:
      - Start a timer (device-local countdown + LEDs)
      - Ask how much time is left (reads remaining seconds sensor)
      - Cancel a running timer (press cancel button)

    Behavior:
      - If a timer is already running, starting a new one is BLOCKED.
        The user must cancel first.
      - Uses the dedicated RUNNING binary_sensor as the primary source of truth.
    """

    name = "voicepe_remote_timer"
    plugin_name = "Voice PE Remote Timer"
    pretty_name = "Voice PE Remote Timer"
    settings_category = "Voice PE Remote Timer"

    usage = (
        "{\n"
        '  "function": "voicepe_remote_timer",\n'
        '  "arguments": {\n'
        '    "duration": "5 minutes (or omit duration to check remaining time)",\n'
        '    "action": "cancel (optional, to cancel the running timer)"\n'
        "  }\n"
        "}\n"
    )

    description = (
        "Start, cancel, or check remaining time for a device-local timer on a Voice PE (ESPHome). "
        "If duration is provided, starts a timer (unless one is already running). "
        "If duration is omitted, reports remaining time. "
        "If action is 'cancel', cancels the current timer."
    )
    plugin_dec = "Start, cancel, or check a Voice PE (ESPHome) timer device."

    required_settings = {
        "HA_BASE_URL": {
            "label": "Home Assistant Base URL",
            "type": "string",
            "default": "http://homeassistant.local:8123",
            "description": "Base URL of your Home Assistant instance.",
        },
        "HA_TOKEN": {
            "label": "Home Assistant Long-Lived Token",
            "type": "string",
            "default": "",
            "description": "Create in HA: Profile → Long-Lived Access Tokens.",
        },
        "TIMER_SECONDS_ENTITY": {
            "label": "Remote Timer Seconds (number.*)",
            "type": "string",
            "default": "number.voicepe_remote_timer_seconds",
            "description": "The ESPHome number entity that sets the timer duration in seconds.",
        },
        "START_BUTTON_ENTITY": {
            "label": "Remote Timer Start (button.*)",
            "type": "string",
            "default": "button.voicepe_remote_timer_start",
            "description": "The ESPHome button entity that starts the device-local timer.",
        },
        "CANCEL_BUTTON_ENTITY": {
            "label": "Remote Timer Cancel (button.*)",
            "type": "string",
            "default": "button.voicepe_remote_timer_cancel",
            "description": "The ESPHome button entity that cancels/stops the device-local timer.",
        },
        "REMAINING_SENSOR_ENTITY": {
            "label": "Remote Timer Remaining Seconds (sensor.*)",
            "type": "string",
            "default": "sensor.voicepe_remote_timer_remaining_seconds",
            "description": "The ESPHome sensor entity that reports remaining seconds.",
        },
        "RUNNING_SENSOR_ENTITY": {
            "label": "Remote Timer Running (binary_sensor.*)",
            "type": "string",
            "default": "binary_sensor.voicepe_remote_timer_running",
            "description": "Binary sensor that reports if a timer is currently running (ON/OFF).",
        },
        "MAX_SECONDS": {
            "label": "Max Seconds",
            "type": "number",
            "default": 7200,
            "description": "Clamp very large durations (default 2 hours).",
        },
    }

    waiting_prompt_template = (
        "Write a short friendly message telling {mention} you’re working on the Voice PE timer now. "
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

    def _get_state(self, ha_base: str, token: str, entity_id: str) -> dict:
        url = f"{ha_base}/api/states/{entity_id}"
        r = requests.get(url, headers=self._ha_headers(token), timeout=15)
        if r.status_code >= 400:
            raise RuntimeError(f"GET state failed for {entity_id}: {r.status_code} {r.text}")
        return r.json() if r.text else {}

    # ─────────────────────────────────────────────────────────────
    # Duration parsing (forgiving)
    # ─────────────────────────────────────────────────────────────

    def _parse_duration_to_seconds(self, text: str, max_seconds: int) -> int:
        t = (text or "").strip().lower()
        if not t:
            return 0

        t = re.sub(r"[,;]+", " ", t)
        t = re.sub(r"[()]+", " ", t)
        t = t.replace(".", " ")
        t = re.sub(r"\s+", " ", t).strip()

        if re.fullmatch(r"\d+", t):
            return max(0, min(int(t), max_seconds))

        if re.fullmatch(r"\d{1,2}:\d{2}", t):
            mm, ss = t.split(":")
            total = int(mm) * 60 + int(ss)
            return max(0, min(total, max_seconds))

        unit_map = {
            "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
            "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
            "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
        }

        t_spaced = re.sub(r"(\d)([a-z])", r"\1 \2", t)
        t_spaced = re.sub(r"([a-z])(\d)", r"\1 \2", t_spaced)
        t_spaced = re.sub(r"\s+", " ", t_spaced).strip()

        total = 0
        for n, u in re.findall(r"(\d+)\s*([a-z]+)", t_spaced):
            mult = unit_map.get(u)
            if mult:
                total += int(n) * mult

        return max(0, min(total, max_seconds))

    def _format_remaining(self, seconds: int) -> str:
        seconds = max(0, int(seconds))
        if seconds < 60:
            return f"{seconds} seconds"
        m = seconds // 60
        s = seconds % 60
        if s == 0:
            return f"{m} minute" + ("s" if m != 1 else "")
        return f"{m}m {s}s"

    # ─────────────────────────────────────────────────────────────
    # LLM phrasing helpers
    # ─────────────────────────────────────────────────────────────

    async def _llm_phrase(self, llm_client, prompt: str, fallback: str, max_chars: int = 240) -> str:
        if not llm_client:
            return fallback[:max_chars]
        try:
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            txt = (resp.get("message") or {}).get("content", "")
            txt = (txt or "").strip().strip('"').strip()
            if txt:
                txt = re.sub(r"[`*_]{1,3}", "", txt)
                txt = re.sub(r"\s+", " ", txt).strip()
                return txt[:max_chars]
        except Exception as e:
            logger.warning(f"[voicepe_remote_timer] LLM phrasing failed: {e}")
        return fallback[:max_chars]

    async def _llm_time_left_message(self, remaining_seconds: int, llm_client) -> str:
        remaining_text = self._format_remaining(remaining_seconds)
        fallback = f"You've got {remaining_text} left on the timer."
        prompt = (
            "The user asked how much time is left on a timer.\n\n"
            f"Remaining time: {remaining_text}\n\n"
            "Write ONE short, natural sentence answering the user.\n"
            "Rules:\n- No emojis\n- No markdown\n- Friendly but concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    async def _llm_no_timer_message(self, llm_client) -> str:
        fallback = "No timer is currently running."
        prompt = (
            "The user asked about a timer.\n\n"
            "Fact: There is no timer currently running.\n\n"
            "Write ONE short, natural sentence telling the user.\n"
            "Rules:\n- No emojis\n- No markdown\n- Friendly but concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    async def _llm_started_message(self, seconds: int, llm_client) -> str:
        dur = self._format_remaining(seconds)
        fallback = f"Timer started for {dur}."
        prompt = (
            "The user asked you to start a timer.\n\n"
            f"Fact: The timer has been started for {dur}.\n\n"
            "Write ONE short, friendly confirmation sentence.\n"
            "Rules:\n- No emojis\n- No markdown\n- Keep it concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    async def _llm_cancelled_message(self, llm_client) -> str:
        fallback = "Timer cancelled."
        prompt = (
            "The user asked you to cancel a timer.\n\n"
            "Fact: The timer has been cancelled.\n\n"
            "Write ONE short, friendly confirmation sentence.\n"
            "Rules:\n- No emojis\n- No markdown\n- Keep it concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    async def _llm_cancel_nothing_message(self, llm_client) -> str:
        fallback = "No timer is currently running."
        prompt = (
            "The user asked you to cancel a timer.\n\n"
            "Fact: There is no timer running.\n\n"
            "Write ONE short, friendly sentence telling the user there's nothing to cancel.\n"
            "Rules:\n- No emojis\n- No markdown\n- Keep it concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    async def _llm_block_new_timer_message(self, remaining_seconds: int | None, llm_client) -> str:
        if remaining_seconds is None:
            fallback = "A timer is already running. Cancel it first if you want to start a new one."
            prompt = (
                "The user asked to start a timer, but a timer is already running.\n\n"
                "Write ONE short, friendly sentence telling the user they need to cancel the current timer first.\n"
                "Rules:\n- No emojis\n- No markdown\n- Keep it concise\n"
                "Only output the sentence."
            )
            return await self._llm_phrase(llm_client, prompt, fallback)

        remaining_text = self._format_remaining(remaining_seconds)
        fallback = f"A timer is already running with {remaining_text} remaining. Cancel it first if you want to start a new one."
        prompt = (
            "The user asked to start a timer, but a timer is already running.\n\n"
            f"Fact: Remaining time on current timer: {remaining_text}.\n\n"
            "Write ONE short, friendly sentence telling the user a timer is already running and they need to cancel it first to start another.\n"
            "Rules:\n- No emojis\n- No markdown\n- Keep it concise\n"
            "Only output the sentence."
        )
        return await self._llm_phrase(llm_client, prompt, fallback)

    # ─────────────────────────────────────────────────────────────
    # Read timer state (running + remaining)
    # ─────────────────────────────────────────────────────────────

    async def _is_timer_running(self) -> bool | None:
        s = self._get_settings()

        ha_base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
        token = (s.get("HA_TOKEN") or "").strip()
        running_entity = (s.get("RUNNING_SENSOR_ENTITY") or "").strip()

        if not token or not running_entity:
            return None

        def do_get():
            st = self._get_state(ha_base, token, running_entity)
            raw = (st.get("state") or "").strip().lower()
            # HA binary sensors typically "on"/"off"
            if raw in ("on", "true", "1", "yes"):
                return True
            if raw in ("off", "false", "0", "no"):
                return False
            return None

        try:
            return await asyncio.to_thread(do_get)
        except Exception as e:
            logger.error(f"[voicepe_remote_timer] Failed reading running sensor: {e}")
            return None

    async def _get_remaining_seconds(self) -> int | None:
        s = self._get_settings()

        ha_base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
        token = (s.get("HA_TOKEN") or "").strip()
        remaining_entity = (s.get("REMAINING_SENSOR_ENTITY") or "").strip()

        if not token or not remaining_entity:
            return None

        def do_get():
            st = self._get_state(ha_base, token, remaining_entity)
            raw = st.get("state")
            try:
                return int(float(raw))
            except Exception:
                return None

        try:
            return await asyncio.to_thread(do_get)
        except Exception as e:
            logger.error(f"[voicepe_remote_timer] Failed reading remaining sensor: {e}")
            return None

    # ─────────────────────────────────────────────────────────────
    # Actions: start / status / cancel
    # ─────────────────────────────────────────────────────────────

    async def _start_timer(self, duration_text: str, llm_client) -> str:
        s = self._get_settings()

        ha_base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
        token = (s.get("HA_TOKEN") or "").strip()
        if not token:
            return "Voice PE Remote Timer is missing HA_TOKEN in settings."

        seconds_entity = (s.get("TIMER_SECONDS_ENTITY") or "").strip()
        start_entity = (s.get("START_BUTTON_ENTITY") or "").strip()
        remaining_entity = (s.get("REMAINING_SENSOR_ENTITY") or "").strip()
        running_entity = (s.get("RUNNING_SENSOR_ENTITY") or "").strip()
        if not seconds_entity or not start_entity or not remaining_entity or not running_entity:
            return "Voice PE Remote Timer is missing required entity IDs in settings."

        try:
            max_seconds = int(s.get("MAX_SECONDS") or 7200)
        except Exception:
            max_seconds = 7200

        new_seconds = self._parse_duration_to_seconds(duration_text, max_seconds)
        if new_seconds <= 0:
            return "Please provide a valid timer duration (examples: 20s, 2min, 5 minutes, 1h 10m)."

        running = await self._is_timer_running()
        if running is None:
            return "I couldn't read the Voice PE running sensor (check plugin settings)."

        # BLOCK if already running (use running sensor as source of truth)
        if running:
            remaining = await self._get_remaining_seconds()
            return (await self._llm_block_new_timer_message(remaining, llm_client)).strip()

        def do_calls():
            self._post_service(
                ha_base, token, "number", "set_value",
                {"entity_id": seconds_entity, "value": new_seconds},
            )
            self._post_service(
                ha_base, token, "button", "press",
                {"entity_id": start_entity},
            )

        try:
            await asyncio.to_thread(do_calls)
        except Exception as e:
            logger.error(f"[voicepe_remote_timer] HA start calls failed: {e}")
            return "Failed to start the Voice PE timer (Home Assistant service call error)."

        return (await self._llm_started_message(new_seconds, llm_client)).strip()

    async def _status(self, llm_client) -> str:
        running = await self._is_timer_running()
        if running is None:
            return "I couldn't read the Voice PE running sensor (check plugin settings)."

        if not running:
            return (await self._llm_no_timer_message(llm_client)).strip()

        remaining = await self._get_remaining_seconds()
        # if remaining is missing/unknown but running is ON, still respond sanely
        if remaining is None:
            fallback = "A timer is running, but I couldn't read the remaining time."
            return (await self._llm_phrase(
                llm_client,
                "The user asked how much time is left on a timer.\n\n"
                "Fact: A timer is running, but remaining time is unavailable.\n\n"
                "Write ONE short sentence explaining that.\n"
                "Rules:\n- No emojis\n- No markdown\n- Friendly but concise\n"
                "Only output the sentence.",
                fallback
            )).strip()

        if remaining <= 0:
            # edge case: running true but remaining 0 (transition/ringing). Keep it simple.
            fallback = "The timer is running, but it’s at the end right now."
            return (await self._llm_phrase(
                llm_client,
                "The user asked how much time is left on a timer.\n\n"
                "Fact: The timer is running but remaining time is 0 (end/transition).\n\n"
                "Write ONE short, natural sentence explaining it.\n"
                "Rules:\n- No emojis\n- No markdown\n- Friendly but concise\n"
                "Only output the sentence.",
                fallback
            )).strip()

        return (await self._llm_time_left_message(remaining, llm_client)).strip()

    async def _cancel(self, llm_client) -> str:
        s = self._get_settings()

        ha_base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
        token = (s.get("HA_TOKEN") or "").strip()
        cancel_entity = (s.get("CANCEL_BUTTON_ENTITY") or "").strip()
        running_entity = (s.get("RUNNING_SENSOR_ENTITY") or "").strip()
        if not token or not cancel_entity or not running_entity:
            return "Voice PE Remote Timer is missing CANCEL_BUTTON_ENTITY / RUNNING_SENSOR_ENTITY / HA_TOKEN in settings."

        running = await self._is_timer_running()
        if running is None:
            return "I couldn't read the Voice PE running sensor (check plugin settings)."
        if not running:
            return (await self._llm_cancel_nothing_message(llm_client)).strip()

        def do_cancel():
            self._post_service(
                ha_base, token, "button", "press",
                {"entity_id": cancel_entity},
            )

        try:
            await asyncio.to_thread(do_cancel)
        except Exception as e:
            logger.error(f"[voicepe_remote_timer] HA cancel call failed: {e}")
            return "Failed to cancel the Voice PE timer (Home Assistant service call error)."

        return (await self._llm_cancelled_message(llm_client)).strip()

    # ─────────────────────────────────────────────────────────────
    # Main dispatcher
    # ─────────────────────────────────────────────────────────────

    async def _handle(self, args, llm_client) -> str:
        args = args or {}
        action = (args.get("action") or "").strip().lower()
        duration = (args.get("duration") or "").strip()

        if action in ("cancel", "stop", "clear"):
            return await self._cancel(llm_client)

        # no duration -> status
        if not duration:
            return await self._status(llm_client)

        return await self._start_timer(duration, llm_client)

    # ─────────────────────────────────────────────────────────────
    # Platform handlers (hybrid-safe webui)
    # ─────────────────────────────────────────────────────────────

    async def handle_homeassistant(self, args, llm_client):
        return (await self._handle(args, llm_client)).strip()

    async def handle_homekit(self, args, llm_client):
        return (await self._handle(args, llm_client)).strip()

    async def handle_xbmc(self, args, llm_client):
        return (await self._handle(args, llm_client)).strip()

    async def handle_webui(self, args, llm_client):
        args = args or {}

        async def inner():
            return await self._handle(args, llm_client)

        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())


plugin = VoicePERemoteTimerPlugin()
