# plugins/weather_brief.py
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client

load_dotenv()
logger = logging.getLogger("weather_brief")
logger.setLevel(logging.INFO)


class WeatherBriefPlugin(ToolPlugin):
    """
    Automation-only: summarize recent weather conditions (temp, wind, rain)
    from Home Assistant sensors over the last N hours (default: 12) into a
    very short, HA-safe text summary.

    Optional: write the result directly into an input_text helper in HA.
    """
    name = "weather_brief"
    plugin_name = "Weather Brief"
    version = "1.0.2"
    min_tater_version = "50"
    pretty_name = "Weather Brief"

    description = (
        "Automation-only: returns a very short summary of recent weather conditions "
        "(temperature, wind, rain) over the last N hours using Home Assistant sensors."
    )
    plugin_dec = "Give a short automation-friendly recap of recent weather conditions."

    usage = '{"function":"weather_brief","arguments":{"hours":12,"query":"What was the weather like outside in the last 12 hours?"}}'

    platforms = ["automation"]
    settings_category = "Weather Brief"

    required_settings = {
        # Weather sensors
        "TEMP_ENTITY": {
            "label": "Temperature Entity",
            "type": "string",
            "default": "sensor.outdoor_temperature",
            "description": "Outdoor temperature sensor entity_id."
        },
        "WIND_ENTITY": {
            "label": "Wind Speed Entity",
            "type": "string",
            "default": "sensor.outdoor_wind_speed",
            "description": "Wind speed sensor entity_id (optional)."
        },
        "RAIN_ENTITY": {
            "label": "Rain / Precipitation Entity",
            "type": "string",
            "default": "sensor.outdoor_rain_rate",
            "description": "Rain/precip sensor entity_id (optional)."
        },

        # NEW: optional HA helper to write into
        "INPUT_TEXT_ENTITY": {
            "label": "Input Text Entity to Update (optional)",
            "type": "string",
            "default": "",
            "description": "If set (e.g., input_text.weather_brief), the plugin will write the summary into this helper."
        },
    }

    waiting_prompt_template = (
        "Tell {mention} you’re checking recent weather sensor data and will return a very short summary. "
        "Keep it to a single short sentence. No emojis. Only output that message."
    )

    # ---------- Settings / HA ----------
    def _get_settings(self) -> Dict[str, str]:
        s = redis_client.hgetall(f"plugin_settings:{self.settings_category}") or \
            redis_client.hgetall(f"plugin_settings: {self.settings_category}")
        return s or {}

    def _ha(self, s: Dict[str, str]) -> Dict[str, str]:
        ha_settings = redis_client.hgetall("homeassistant_settings") or {}
        base = (ha_settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        token = (ha_settings.get("HA_TOKEN") or "").strip()
        if not token:
            raise ValueError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )
        return {"base": base, "token": token}

    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # ---------- NEW: Write to input_text ----------
    def _set_input_text(self, ha_base: str, token: str, entity_id: str, value: str) -> None:
        """
        Writes a value into an HA input_text helper using the service call:
          input_text.set_value
        """
        entity_id = (entity_id or "").strip()
        if not entity_id:
            return

        url = f"{ha_base}/api/services/input_text/set_value"
        payload = {"entity_id": entity_id, "value": value}

        try:
            r = requests.post(url, headers=self._ha_headers(token), json=payload, timeout=10)
            if r.status_code >= 400:
                logger.warning("[weather_brief] input_text.set_value failed %s: %s", r.status_code, r.text[:200])
        except Exception as e:
            logger.warning("[weather_brief] input_text.set_value error: %s", e)

    # ---------- Time helpers ----------
    @staticmethod
    def _format_iso_naive(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    # ---------- HA history ----------
    def _fetch_history(
        self,
        ha_base: str,
        token: str,
        entities: List[str],
        start: datetime,
        end: datetime,
    ) -> Dict[str, List[Dict[str, Any]]]:
        entities = [e.strip() for e in entities if e and isinstance(e, str)]
        if not entities:
            return {}

        start_iso = self._format_iso_naive(start)
        end_iso = self._format_iso_naive(end)

        url = f"{ha_base}/api/history/period/{start_iso}"
        params = {"filter_entity_id": ",".join(entities), "end_time": end_iso}

        try:
            r = requests.get(url, headers=self._ha_headers(token), params=params, timeout=15)
            if r.status_code >= 400:
                logger.warning("[weather_brief] history HTTP %s: %s", r.status_code, r.text[:200])
                return {}
            data = r.json() or []
        except Exception as e:
            logger.warning("[weather_brief] history request error: %s", e)
            return {}

        out: Dict[str, List[Dict[str, Any]]] = {}
        for series in data:
            if not isinstance(series, list) or not series:
                continue
            eid = (series[0] or {}).get("entity_id")
            if not isinstance(eid, str):
                continue
            out[eid] = series
        return out

    @staticmethod
    def _extract_numeric_values(series: List[Dict[str, Any]]) -> List[float]:
        vals: List[float] = []
        for item in series or []:
            state = (item.get("state") or "").strip()
            if state in ("unknown", "unavailable", ""):
                continue
            try:
                vals.append(float(state))
            except Exception:
                continue
        return vals

    @staticmethod
    def _summarize_series(values: List[float]) -> Optional[Dict[str, float]]:
        if not values:
            return None
        v_min = min(values)
        v_max = max(values)
        v_avg = sum(values) / len(values)
        return {"min": v_min, "max": v_max, "avg": v_avg}

    # ---------- HA-safe compact helper ----------
    @staticmethod
    def _compact_for_ha(text: str, limit: int = 240) -> str:
        if not text:
            return ""
        t = re.sub(r"\s+", " ", text).strip()
        if len(t) <= limit:
            return t
        cut = t[:limit]
        last_space = cut.rfind(" ")
        if last_space > 40:
            cut = cut[:last_space]
        return cut.rstrip(". ,;:") + "…"

    # ---------- LLM summary ----------
    async def _summarize_weather(
        self,
        *,
        hours: int,
        metrics: Dict[str, Any],
        user_query: Optional[str],
        llm_client,
    ) -> str:
        system = (
            "You are summarizing recent outdoor weather conditions for a smart home dashboard.\n"
            "HARD REQUIREMENTS:\n"
            "- Plain text only.\n"
            "- At most 3 short sentences.\n"
            "- Max ~240 characters.\n"
            "- Mention temperature range, wind, and whether it rained (if available).\n"
            "- If no data, say 'No recent weather data available.'\n"
        )

        payload = {
            "user_request": user_query or f"Summarize outdoor weather for the last {hours} hours.",
            "timeframe_hours": hours,
            "metrics": metrics,
        }

        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                temperature=0.2,
                max_tokens=160,
                timeout_ms=30_000,
            )
            text = (resp.get("message", {}) or {}).get("content", "").strip()
            if text:
                return text
        except Exception as e:
            logger.info(f"[weather_brief] LLM summary failed; using fallback: {e}")

        temp = metrics.get("temperature")
        wind = metrics.get("wind")
        rain = metrics.get("rain")

        parts: List[str] = [f"In the last {hours} hours"]
        if temp:
            parts.append(f"temps ranged {round(temp['min'])}–{round(temp['max'])}")
        if wind:
            parts.append(f"winds up to {round(wind['max'])}")
        if rain:
            parts.append(f"rain ~{round(rain['total'], 1)}" if rain.get("rained") else "no rain")

        if len(parts) == 1:
            return f"No recent weather data available for the last {hours} hours."
        return ". ".join(parts) + "."

    # ---------- Core ----------
    async def _handle(self, args: Dict[str, Any], llm_client) -> str:
        s = self._get_settings()
        ha = self._ha(s)

        # Timeframe (hours)
        try:
            hours = int(args.get("hours", 12))
        except Exception:
            hours = 12
        hours = max(1, min(hours, 72))

        now = datetime.now()
        start = now - timedelta(hours=hours)

        temp_entity = (s.get("TEMP_ENTITY") or "").strip()
        wind_entity = (s.get("WIND_ENTITY") or "").strip()
        rain_entity = (s.get("RAIN_ENTITY") or "").strip()

        entities: List[str] = []
        if temp_entity:
            entities.append(temp_entity)
        if wind_entity:
            entities.append(wind_entity)
        if rain_entity:
            entities.append(rain_entity)

        if not entities:
            raise ValueError("No weather sensors configured. Set TEMP_ENTITY, WIND_ENTITY, or RAIN_ENTITY.")

        history = self._fetch_history(ha["base"], ha["token"], entities, start, now)

        metrics: Dict[str, Any] = {"temperature": None, "wind": None, "rain": None}

        if temp_entity and temp_entity in history:
            vals = self._extract_numeric_values(history[temp_entity])
            summary = self._summarize_series(vals)
            if summary:
                metrics["temperature"] = {"entity_id": temp_entity, **summary}

        if wind_entity and wind_entity in history:
            vals = self._extract_numeric_values(history[wind_entity])
            summary = self._summarize_series(vals)
            if summary:
                metrics["wind"] = {"entity_id": wind_entity, **summary}

        if rain_entity and rain_entity in history:
            vals = self._extract_numeric_values(history[rain_entity])
            if vals:
                total = sum(v for v in vals if v >= 0)
                metrics["rain"] = {
                    "entity_id": rain_entity,
                    "total": total,
                    "max": max(vals),
                    "rained": total > 0,
                }

        if not any(metrics.values()):
            raw = f"No recent weather data available for the last {hours} hours."
        else:
            user_query = (args.get("query") or "").strip()
            raw = await self._summarize_weather(
                hours=hours,
                metrics=metrics,
                user_query=user_query,
                llm_client=llm_client,
            )

        compact = self._compact_for_ha(raw, limit=240) or "No recent weather data available."

        # NEW: write into input_text if configured (or overridden by args)
        input_text_entity = (args.get("input_text_entity") or s.get("INPUT_TEXT_ENTITY") or "").strip()
        if input_text_entity:
            self._set_input_text(ha["base"], ha["token"], input_text_entity, compact)

        return compact

    # ---------- Automation entrypoint ----------
    async def handle_automation(self, args: Dict[str, Any], llm_client):
        """
        Args:
          - hours (optional)
          - query (optional)
          - input_text_entity (optional override; otherwise uses plugin setting INPUT_TEXT_ENTITY)
        """
        return await self._handle(args, llm_client)


plugin = WeatherBriefPlugin()
