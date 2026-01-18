# plugins/zen_greeting.py
import logging
import json
import re
from datetime import datetime
from typing import Any, Dict

import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client

load_dotenv()
logger = logging.getLogger("zen_greeting")
logger.setLevel(logging.INFO)


class ZenGreetingPlugin(ToolPlugin):
    """
    Automation-only: generates a peaceful "zen greeting" + message of the day via LLM.

    Notes:
    - Designed for Home Assistant dashboards (very short output).
    - Optionally writes directly into an input_text helper in Home Assistant.
    - Uses HA time sensor so the greeting matches HA's local time.
    """
    name = "zen_greeting"
    plugin_name = "Zen Greeting"
    pretty_name = "Zen Greeting"

    description = (
        "Automation-only: creates a short, calming greeting and zen message of the day. "
        "Uses the LLM so the message varies a bit each run. Designed for dashboards."
    )
    plugin_dec = "Generate a calming daily greeting and message for dashboards."

    usage = (
        "{\n"
        '  "function": "zen_greeting",\n'
        '  "arguments": {\n'
        '    // all optional\n'
        '    "input_text_entity": "input_text.daily_zen_greeting",\n'
        '    "include_date": false,\n'
        '    "tone": "zen",\n'
        '    "prompt_hint": "focus on patience and gratitude"\n'
        "  }\n"
        "}\n"
    )

    platforms = ["automation"]
    settings_category = "Zen Greeting"

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
        "TIME_SENSOR_ENTITY": {
            "label": "Time Sensor (ISO)",
            "type": "string",
            "default": "sensor.date_time_iso",
            "description": "Sensor with local-naive ISO time (e.g., 2025-10-19T20:07:00)."
        },
        "INPUT_TEXT_ENTITY": {
            "label": "Input Text Entity to Update (optional)",
            "type": "string",
            "default": "",
            "description": (
                "If set (e.g., input_text.daily_zen_greeting), the plugin writes the result into this helper."
            )
        },
        "MAX_CHARS": {
            "label": "Max Characters",
            "type": "number",
            "default": 100,
            "description": "Hard limit to keep output safe for HA text fields."
        },
    }

    waiting_prompt_template = "Creating a calm zen message of the day. This will be quick."

    # ---------- Settings / HA ----------
    def _get_settings(self) -> Dict[str, str]:
        s = redis_client.hgetall(f"plugin_settings:{self.settings_category}") or \
            redis_client.hgetall(f"plugin_settings: {self.settings_category}")
        return s or {}

    def _ha(self, s: Dict[str, str]) -> Dict[str, str]:
        base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
        token = (s.get("HA_TOKEN") or "").strip()
        if not token:
            raise ValueError("HA_TOKEN is missing in Zen Greeting settings.")
        time_sensor = (s.get("TIME_SENSOR_ENTITY") or "sensor.date_time_iso").strip()
        return {"base": base, "token": token, "time_sensor": time_sensor}

    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _get_ha_time(self, ha_base: str, token: str, time_entity: str) -> datetime:
        try:
            url = f"{ha_base}/api/states/{time_entity}"
            r = requests.get(url, headers=self._ha_headers(token), timeout=5)
            if r.status_code < 400:
                state = (r.json() or {}).get("state", "")
                if isinstance(state, str) and state.strip() and state not in ("unknown", "unavailable"):
                    dt = datetime.fromisoformat(state.strip())
                    return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except Exception:
            logger.info("[zen_greeting] HA time fetch failed; using local system time")
        return datetime.now()

    def _set_input_text(self, ha_base: str, token: str, entity_id: str, value: str) -> None:
        entity_id = (entity_id or "").strip()
        if not entity_id:
            return
        url = f"{ha_base}/api/services/input_text/set_value"
        payload = {"entity_id": entity_id, "value": value}
        try:
            r = requests.post(url, headers=self._ha_headers(token), json=payload, timeout=10)
            if r.status_code >= 400:
                logger.warning("[zen_greeting] input_text.set_value failed %s: %s", r.status_code, r.text[:200])
        except Exception as e:
            logger.warning("[zen_greeting] input_text.set_value error: %s", e)

    # ---------- Helpers ----------
    @staticmethod
    def _time_greeting(dt: datetime) -> str:
        h = dt.hour
        if 5 <= h < 12:
            return "Good morning"
        if 12 <= h < 17:
            return "Good afternoon"
        if 17 <= h < 22:
            return "Good evening"
        return "Hello"

    @staticmethod
    def _clamp(text: str, limit: int) -> str:
        text = re.sub(r"\s+", " ", (text or "")).strip()
        if len(text) <= limit:
            return text
        cut = text[:limit]
        last_space = cut.rfind(" ")
        if last_space > 30:
            cut = cut[:last_space]
        return cut.rstrip(". ,;:") + "…"

    async def _generate(
        self,
        *,
        now: datetime,
        greeting: str,
        include_date: bool,
        tone: str,
        prompt_hint: str,
        llm_client,
        max_chars: int
    ) -> str:
        # Keep this extremely constrained for HA dashboards.
        system = (
            "You write short, calming messages for a smart-home dashboard.\n"
            "HARD RULES:\n"
            "- Plain text only (no markdown, no emojis).\n"
            "- Output MUST be a single line.\n"
            "- Output MUST be ONE sentence only.\n"
            f"- Output MUST be {max_chars} characters or fewer (count every character).\n"
            "- Do not use quotes.\n"
            "- Gentle, peaceful, grounded.\n"
            "- No metaphysics, no medical advice, no therapy talk.\n"
            "- Avoid clichés like 'journey' and 'universe' if possible.\n"
            "Return ONLY the final message text. No extra commentary.\n"
        )

        date_str = now.strftime("%A, %B %d") if include_date else ""
        user_payload = {
            "time": now.strftime("%H:%M"),
            "date": date_str,
            "opening_greeting": greeting,
            "tone": tone or "zen",
            "extra_hint": prompt_hint or "",
            "instruction": (
                "Create a zen message of the day that starts with the opening greeting. "
                "If a date is provided, you may include it naturally, but keep it short."
            ),
        }

        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
                temperature=0.8,   # a bit of variety
                max_tokens=90,
                timeout_ms=30_000,
            )
            text = (resp.get("message", {}) or {}).get("content", "").strip()
            if text:
                text = self._clamp(text, max_chars)
                if len(text) <= max_chars:
                    return text

                # One quick retry: force a shorter version.
                tighten = (
                    f"Shorten the message to {max_chars} characters or fewer. "
                    "One sentence, plain text, keep the same meaning. "
                    "Return only the shortened message."
                )
                resp2 = await llm_client.chat(
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": tighten + "\n\n" + text},
                    ],
                    temperature=0.2,
                    max_tokens=60,
                    timeout_ms=30_000,
                )
                text2 = (resp2.get("message", {}) or {}).get("content", "").strip()
                text2 = self._clamp(text2, max_chars)
                if text2:
                    return text2

        except Exception as e:
            logger.info(f"[zen_greeting] LLM generation failed; using fallback: {e}")

        # Fallback if LLM is unavailable
        if include_date and date_str:
            fallback = f"{greeting} — {date_str}. Breathe and begin gently."
        else:
            fallback = f"{greeting}. Breathe and begin gently."
        return self._clamp(fallback, max_chars)

    # ---------- Automation entrypoint ----------
    async def handle_automation(self, args: Dict[str, Any], llm_client) -> Any:
        s = self._get_settings()
        ha = self._ha(s)

        # Enforce a strict ceiling that matches HA input_text constraints (your target: 100 chars).
        max_chars = int(s.get("MAX_CHARS") or 100)
        max_chars = max(40, min(max_chars, 100))

        now = self._get_ha_time(ha["base"], ha["token"], ha["time_sensor"])
        greeting = self._time_greeting(now)

        include_date = bool(args.get("include_date", False))
        tone = (args.get("tone") or "zen").strip()
        prompt_hint = (args.get("prompt_hint") or "").strip()

        raw = await self._generate(
            now=now,
            greeting=greeting,
            include_date=include_date,
            tone=tone,
            prompt_hint=prompt_hint,
            llm_client=llm_client,
            max_chars=max_chars,
        )

        out = self._clamp(raw, max_chars) or self._clamp(f"{greeting}. Breathe and begin gently.", max_chars)

        # Write to input_text if configured (or overridden)
        input_text_entity = (args.get("input_text_entity") or s.get("INPUT_TEXT_ENTITY") or "").strip()
        if input_text_entity:
            self._set_input_text(ha["base"], ha["token"], input_text_entity, out)

        # Also return the string (useful for logs / tracing)
        return out


plugin = ZenGreetingPlugin()