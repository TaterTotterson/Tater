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
    """

    name = "voicepe_remote_timer"
    plugin_name = "Voice PE Remote Timer"
    version = "1.0.1"
    min_tater_version = "50"
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
        "MAX_SECONDS": {
            "label": "Max Seconds",
            "type": "number",
            "default": 7200,
            "description": "Clamp very large durations (default 2 hours).",
        },
        # (optional): if your entity ids follow a predictable pattern, you can override this.
        "VOICEPE_ENTITY_PREFIX": {
            "label": "Voice PE entity prefix (optional)",
            "type": "string",
            "default": "",
            "description": (
                "Optional prefix used when inferring entities from the speaking device. "
                "Example: 'voicepe_office' would produce number.voicepe_office_remote_timer_seconds, etc. "
                "If blank, we will slugify context.device_name."
            ),
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

    def _ha_settings(self) -> dict:
        settings = redis_client.hgetall("homeassistant_settings") or {}
        base_url = (settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        token = (settings.get("HA_TOKEN") or "").strip()
        return {"base_url": base_url, "token": token}

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

    def _list_states(self, ha_base: str, token: str) -> list[dict]:
        url = f"{ha_base}/api/states"
        r = requests.get(url, headers=self._ha_headers(token), timeout=20)
        if r.status_code >= 400:
            raise RuntimeError(f"GET states failed: {r.status_code} {r.text}")
        return r.json() if r.text else []

    # ─────────────────────────────────────────────────────────────
    # Entity inference (Voice context)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _slugify(s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"[^a-z0-9]+", "_", s)
        s = re.sub(r"_+", "_", s).strip("_")
        return s

    def _infer_entities_from_context(self, context: dict | None) -> dict | None:
        """
        Try to infer the correct entity ids based on the speaking device (context.device_name).

        Strategy:
          1) Determine a base prefix:
              - VOICEPE_ENTITY_PREFIX setting, OR slugified context.device_name
          2) Try common suffix patterns. We test existence via /api/states/<entity_id>.
          3) If that fails, scan all states and try fuzzy match by entity_id containing the prefix + suffix.

        Returns dict with keys:
          seconds_entity, start_entity, cancel_entity, remaining_entity, running_entity
        """
        s = self._get_settings()
        ha = self._ha_settings()
        ha_base = ha["base_url"]
        token = ha["token"]
        if not token:
            return None

        ctx = context or {}
        device_name = (ctx.get("device_name") or "").strip()
        if not device_name and (s.get("VOICEPE_ENTITY_PREFIX") or "").strip():
            # if user set prefix, allow inference without device_name
            device_name = s.get("VOICEPE_ENTITY_PREFIX")

        prefix = (s.get("VOICEPE_ENTITY_PREFIX") or "").strip()
        if prefix:
            base = self._slugify(prefix)
        else:
            base = self._slugify(device_name)

        if not base:
            return None

        # Common patterns you might use across devices
        # Example target:
        #  number.voicepe_office_remote_timer_seconds
        #  button.voicepe_office_remote_timer_start
        #  button.voicepe_office_remote_timer_cancel
        #  sensor.voicepe_office_remote_timer_remaining_seconds
        #  binary_sensor.voicepe_office_remote_timer_running
        candidates = [
            {
                "seconds": f"number.{base}_remote_timer_seconds",
                "start": f"button.{base}_remote_timer_start",
                "cancel": f"button.{base}_remote_timer_cancel",
                "remaining": f"sensor.{base}_remote_timer_remaining_seconds",
                "running": f"binary_sensor.{base}_remote_timer_running",
            },
            # Alternative: some folks keep "voicepe_" prefix already in base, or use "timer_" names
            {
                "seconds": f"number.{base}_timer_seconds",
                "start": f"button.{base}_timer_start",
                "cancel": f"button.{base}_timer_cancel",
                "remaining": f"sensor.{base}_timer_remaining_seconds",
                "running": f"binary_sensor.{base}_timer_running",
            },
        ]

        # Fast path: verify one of the candidate sets exists (running sensor is a good test)
        def exists(eid: str) -> bool:
            try:
                st = self._get_state(ha_base, token, eid)
                return bool(st and st.get("entity_id") == eid)
            except Exception:
                return False

        for c in candidates:
            if exists(c["running"]) and exists(c["remaining"]) and exists(c["start"]) and exists(c["cancel"]) and exists(c["seconds"]):
                return {
                    "TIMER_SECONDS_ENTITY": c["seconds"],
                    "START_BUTTON_ENTITY": c["start"],
                    "CANCEL_BUTTON_ENTITY": c["cancel"],
                    "REMAINING_SENSOR_ENTITY": c["remaining"],
                    "RUNNING_SENSOR_ENTITY": c["running"],
                }

        # Fallback: scan all states and attempt fuzzy matching by entity_id containing the base string.
        try:
            all_states = self._list_states(ha_base, token)
        except Exception:
            return None

        def find(domain: str, contains: list[str]) -> str | None:
            for st in all_states:
                eid = (st.get("entity_id") or "")
                if not eid.startswith(domain + "."):
                    continue
                low = eid.lower()
                ok = True
                for part in contains:
                    if part not in low:
                        ok = False
                        break
                if ok:
                    return eid
            return None

        # Try to find each required entity by pieces
        seconds_eid = find("number", [base, "seconds"]) or find("number", [base, "timer", "seconds"])
        start_eid = find("button", [base, "start"]) or find("button", [base, "timer", "start"])
        cancel_eid = find("button", [base, "cancel"]) or find("button", [base, "timer", "cancel"])
        remaining_eid = find("sensor", [base, "remaining"]) or find("sensor", [base, "timer", "remaining"])
        running_eid = find("binary_sensor", [base, "running"]) or find("binary_sensor", [base, "timer", "running"])

        if seconds_eid and start_eid and cancel_eid and remaining_eid and running_eid:
            return {
                "TIMER_SECONDS_ENTITY": seconds_eid,
                "START_BUTTON_ENTITY": start_eid,
                "CANCEL_BUTTON_ENTITY": cancel_eid,
                "REMAINING_SENSOR_ENTITY": remaining_eid,
                "RUNNING_SENSOR_ENTITY": running_eid,
            }

        return None

    def _resolve_entities(self, context: dict | None = None) -> dict | None:
        return self._infer_entities_from_context(context)

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

    async def _is_timer_running(self, context: dict | None = None) -> bool | None:
        s = self._get_settings()
        ha = self._ha_settings()
        ha_base = ha["base_url"]
        token = ha["token"]

        ents = self._resolve_entities(context=context)
        if not token or not ents:
            return None

        running_entity = (ents.get("RUNNING_SENSOR_ENTITY") or "").strip()
        if not running_entity:
            return None

        def do_get():
            st = self._get_state(ha_base, token, running_entity)
            raw = (st.get("state") or "").strip().lower()
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

    async def _get_remaining_seconds(self, context: dict | None = None) -> int | None:
        s = self._get_settings()
        ha = self._ha_settings()
        ha_base = ha["base_url"]
        token = ha["token"]

        ents = self._resolve_entities(context=context)
        if not token or not ents:
            return None

        remaining_entity = (ents.get("REMAINING_SENSOR_ENTITY") or "").strip()
        if not remaining_entity:
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

    async def _start_timer(self, duration_text: str, llm_client, context: dict | None = None) -> str:
        s = self._get_settings()
        ha = self._ha_settings()
        ha_base = ha["base_url"]
        token = ha["token"]
        if not token:
            return (
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        ents = self._resolve_entities(context=context)
        if not ents:
            dev = ((context or {}).get("device_name") or (context or {}).get("device_id") or "").strip()
            area = ((context or {}).get("area_name") or (context or {}).get("area_id") or "").strip()
            hint = ""
            if dev or area:
                hint = f" (I heard you from device={dev or 'unknown'}, area={area or 'unknown'}.)"
            return (
                "Voice PE Remote Timer couldn't determine which device timer entities to use."
                f"{hint} "
                "Either set the timer entity IDs in the plugin settings, or ensure your Voice PE devices "
                "use predictable entity IDs that include the device name."
            )

        seconds_entity = (ents.get("TIMER_SECONDS_ENTITY") or "").strip()
        start_entity = (ents.get("START_BUTTON_ENTITY") or "").strip()
        remaining_entity = (ents.get("REMAINING_SENSOR_ENTITY") or "").strip()
        running_entity = (ents.get("RUNNING_SENSOR_ENTITY") or "").strip()
        if not seconds_entity or not start_entity or not remaining_entity or not running_entity:
            return "Voice PE Remote Timer is missing required entity IDs."

        try:
            max_seconds = int(s.get("MAX_SECONDS") or 7200)
        except Exception:
            max_seconds = 7200

        new_seconds = self._parse_duration_to_seconds(duration_text, max_seconds)
        if new_seconds <= 0:
            return "Please provide a valid timer duration (examples: 20s, 2min, 5 minutes, 1h 10m)."

        running = await self._is_timer_running(context=context)
        if running is None:
            return "I couldn't read the Voice PE running sensor (check plugin settings)."

        if running:
            remaining = await self._get_remaining_seconds(context=context)
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

    async def _status(self, llm_client, context: dict | None = None) -> str:
        ha = self._ha_settings()
        if not ha["token"]:
            return (
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )
        running = await self._is_timer_running(context=context)
        if running is None:
            return "I couldn't read the Voice PE running sensor (check plugin settings)."

        if not running:
            return (await self._llm_no_timer_message(llm_client)).strip()

        remaining = await self._get_remaining_seconds(context=context)
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

    async def _cancel(self, llm_client, context: dict | None = None) -> str:
        s = self._get_settings()
        ha = self._ha_settings()
        ha_base = ha["base_url"]
        token = ha["token"]
        if not token:
            return (
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        ents = self._resolve_entities(context=context)
        if not ents:
            dev = ((context or {}).get("device_name") or (context or {}).get("device_id") or "").strip()
            area = ((context or {}).get("area_name") or (context or {}).get("area_id") or "").strip()
            hint = ""
            if dev or area:
                hint = f" (I heard you from device={dev or 'unknown'}, area={area or 'unknown'}.)"
            return (
                "Voice PE Remote Timer couldn't determine which device timer entities to use."
                f"{hint} "
                "Either set the timer entity IDs in the plugin settings, or ensure your Voice PE devices "
                "use predictable entity IDs that include the device name."
            )

        cancel_entity = (ents.get("CANCEL_BUTTON_ENTITY") or "").strip()
        running_entity = (ents.get("RUNNING_SENSOR_ENTITY") or "").strip()
        if not cancel_entity or not running_entity:
            return "Voice PE Remote Timer is missing CANCEL_BUTTON_ENTITY / RUNNING_SENSOR_ENTITY."

        running = await self._is_timer_running(context=context)
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

    async def _handle(self, args, llm_client, context: dict | None = None) -> str:
        args = args or {}
        action = (args.get("action") or "").strip().lower()
        duration = (args.get("duration") or "").strip()

        if action in ("cancel", "stop", "clear"):
            return await self._cancel(llm_client, context=context)

        if not duration:
            return await self._status(llm_client, context=context)

        return await self._start_timer(duration, llm_client, context=context)

    # ─────────────────────────────────────────────────────────────
    # Platform handlers (hybrid-safe webui)
    # ─────────────────────────────────────────────────────────────

    async def handle_homeassistant(self, args, llm_client, context: dict | None = None):
        return (await self._handle(args, llm_client, context=context)).strip()

    async def handle_homekit(self, args, llm_client, context: dict | None = None):
        return (await self._handle(args, llm_client, context=context)).strip()

    async def handle_xbmc(self, args, llm_client, context: dict | None = None):
        return (await self._handle(args, llm_client, context=context)).strip()

    async def handle_webui(self, args, llm_client, context: dict | None = None):
        args = args or {}

        async def inner():
            return await self._handle(args, llm_client, context=context)

        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())


plugin = VoicePERemoteTimerPlugin()
