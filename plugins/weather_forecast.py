# plugins/weather_forecast.py
import asyncio
import logging
import re
from datetime import datetime, date
from typing import Any, Dict, Optional, Tuple, List

import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client, get_tater_name

load_dotenv()
logger = logging.getLogger("weather_forecast")
logger.setLevel(logging.INFO)


class WeatherForecastPlugin(ToolPlugin):
    """
    Weather Forecast (WeatherAPI.com)

    Tool-call schema (simple):
      - LLM passes ONLY args["request"] (the exact user request text).
      - Plugin parses it + calls WeatherAPI.
      - Then it calls the LLM again to answer ONLY what the user asked,
        using a compact facts block (prevents data dumps).

    Supports:
      - Current conditions
      - Forecast (daily + optional hourly peek)
      - Alerts, AQI, pollen (optional, plan permitting)
    """

    name = "weather_forecast"
    plugin_name = "Weather Forecast"
    version = "1.0.0"
    min_tater_version = "50"
    description = "Get current weather + forecast (and optional AQI/pollen/alerts) from WeatherAPI.com."
    plugin_dec = "Fetch WeatherAPI.com weather and answer only what the user asked (LLM-guided)."
    pretty_name = "Checking the Weather"
    settings_category = "Weather Forecast"

    # IMPORTANT: keep the tool call simple for the LLM
    usage = (
        "{\n"
        '  "function": "weather_forecast",\n'
        '  "arguments": {\n'
        '    "request": "<exact user request text>"\n'
        "  }\n"
        "}\n"
    )

    required_settings = {
        "WEATHERAPI_KEY": {
            "label": "WeatherAPI.com API Key",
            "type": "string",
            "default": "",
            "description": "Your WeatherAPI.com key."
        },
        "DEFAULT_LOCATION": {
            "label": "Default Location (zip/city/lat,lon)",
            "type": "string",
            "default": "60614",
            "description": "Used unless the user explicitly specifies a location (city/zip/lat,lon)."
        },
        "DEFAULT_DAYS": {
            "label": "Default Forecast Days",
            "type": "number",
            "default": 3,
            "description": "Default number of forecast days to request (1–14)."
        },
        "DEFAULT_UNITS": {
            "label": "Default Units",
            "type": "select",
            "options": ["us", "metric"],
            "default": "us",
            "description": "us = °F/mph/mi, metric = °C/kph/km."
        },
        "INCLUDE_AQI": {
            "label": "Include Air Quality",
            "type": "select",
            "options": ["true", "false"],
            "default": "true",
            "description": "Adds air quality fields when available (aqi=yes)."
        },
        "INCLUDE_POLLEN": {
            "label": "Include Pollen",
            "type": "select",
            "options": ["true", "false"],
            "default": "true",
            "description": "Adds pollen fields when available (pollen=yes). Some plans required."
        },
        "INCLUDE_ALERTS": {
            "label": "Include Alerts",
            "type": "select",
            "options": ["true", "false"],
            "default": "true",
            "description": "Adds weather alerts when available (alerts=yes)."
        },
        "SHOW_HOURLY_PEEK": {
            "label": "Show next N hours (0 disables)",
            "type": "number",
            "default": 6,
            "description": "Optional: show a short next-hours preview."
        },
        "MAX_RESPONSE_CHARS": {
            "label": "Max response characters",
            "type": "number",
            "default": 650,
            "description": "Hard cap on returned text (helps Discord/HA voice output)."
        },
        "TIMEOUT_SECONDS": {
            "label": "HTTP Timeout Seconds",
            "type": "number",
            "default": 12,
            "description": "Request timeout for WeatherAPI calls."
        },
    }

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re checking the weather now. "
        "Only output that message."
    )

    platforms = ["discord", "webui", "irc", "homeassistant", "matrix", "homekit", "xbmc"]

    # -------------------- Settings helpers --------------------

    def _get_settings(self) -> Dict[str, Any]:
        s = redis_client.hgetall("plugin_settings:Weather Forecast") or {}

        def _bool(v, default=False):
            if isinstance(v, bool):
                return v
            if v is None:
                return default
            return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

        def _int(v, default=0):
            try:
                return int(v)
            except Exception:
                return default

        out = {
            "WEATHERAPI_KEY": s.get("WEATHERAPI_KEY", ""),
            "DEFAULT_LOCATION": s.get("DEFAULT_LOCATION", "60614"),
            "DEFAULT_DAYS": _int(s.get("DEFAULT_DAYS", 3), 3),
            "DEFAULT_UNITS": (s.get("DEFAULT_UNITS", "us") or "us").strip().lower(),
            "INCLUDE_AQI": _bool(s.get("INCLUDE_AQI", "true"), True),
            "INCLUDE_POLLEN": _bool(s.get("INCLUDE_POLLEN", "true"), True),
            "INCLUDE_ALERTS": _bool(s.get("INCLUDE_ALERTS", "true"), True),
            "SHOW_HOURLY_PEEK": _int(s.get("SHOW_HOURLY_PEEK", 6), 6),
            "MAX_RESPONSE_CHARS": _int(s.get("MAX_RESPONSE_CHARS", 650), 650),
            "TIMEOUT_SECONDS": _int(s.get("TIMEOUT_SECONDS", 12), 12),
        }

        out["DEFAULT_DAYS"] = max(1, min(out["DEFAULT_DAYS"], 14))
        out["SHOW_HOURLY_PEEK"] = max(0, min(out["SHOW_HOURLY_PEEK"], 48))
        out["MAX_RESPONSE_CHARS"] = max(120, min(out["MAX_RESPONSE_CHARS"], 4000))
        if out["DEFAULT_UNITS"] not in ("us", "metric"):
            out["DEFAULT_UNITS"] = "us"
        return out

    # -------------------- Utility helpers --------------------

    @staticmethod
    def _siri_flatten(text: Optional[str]) -> str:
        if not text:
            return "No weather available."
        out = str(text)
        out = re.sub(r"[`*_]{1,3}", "", out)
        out = re.sub(r"\s+", " ", out).strip()
        return out[:450]

    @staticmethod
    def _safe_get(d: Dict[str, Any], *path, default=None):
        cur = d
        for p in path:
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur

    @staticmethod
    def _parse_date(datestr: Optional[str]) -> Optional[date]:
        if not datestr:
            return None
        try:
            return datetime.strptime(datestr.strip(), "%Y-%m-%d").date()
        except Exception:
            return None

    @staticmethod
    def _us_epa_label(idx: Optional[int]) -> Optional[str]:
        if idx is None:
            return None
        try:
            idx = int(idx)
        except Exception:
            return None
        mapping = {
            1: "Good",
            2: "Moderate",
            3: "Unhealthy (Sensitive Groups)",
            4: "Unhealthy",
            5: "Very Unhealthy",
            6: "Hazardous",
        }
        return mapping.get(idx)

    # -------------------- WeatherAPI calls --------------------

    def _request_weatherapi(
        self,
        endpoint: str,
        params: Dict[str, Any],
        timeout: int
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        url = f"https://api.weatherapi.com/v1/{endpoint}"
        headers = {"User-Agent": "TaterTotterson/WeatherForecastPlugin"}
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code != 200:
                try:
                    j = resp.json()
                    msg = self._safe_get(j, "error", "message", default=resp.text)
                except Exception:
                    msg = resp.text
                return None, f"WeatherAPI error (HTTP {resp.status_code}): {msg}"
            return resp.json(), None
        except Exception as e:
            return None, f"Weather request failed: {e}"

    # -------------------- Request parsing --------------------

    def _extract_location_override(self, request: str) -> Optional[str]:
        """
        Default location comes from plugin settings.
        Only override if the user clearly specifies a location.

        Supports:
          - lat,lon anywhere
          - US ZIP anywhere
          - trailing "in/for/at <location>"
        """
        if not request:
            return None
        text = request.strip()

        m2 = re.search(r"(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)", text)
        if m2:
            return f"{m2.group(1)},{m2.group(2)}"

        m3 = re.search(r"\b(\d{5})(?:-\d{4})?\b", text)
        if m3:
            return m3.group(0)

        m = re.search(r"\b(?:in|for|at)\s+([A-Za-z0-9 .,'-]+)\s*$", text, flags=re.IGNORECASE)
        if m:
            loc = m.group(1).strip()
            if loc.lower() in ("the morning", "morning", "the afternoon", "afternoon", "today", "tomorrow"):
                return None
            return loc

        return None

    def _interpret_request(self, request: str, default_days: int, default_hours: int) -> Dict[str, Any]:
        """
        Turns a natural request into query intent:
          - days: forecast window (1..14)
          - date: optional YYYY-MM-DD if user wrote it
          - hours: optional hourly peek (0..48)
        """
        request_l = (request or "").lower().strip()

        days = default_days
        hours = default_hours
        date_str = None

        # Current-only cues
        if any(x in request_l for x in ["right now", "currently", "current weather", "now"]):
            days = max(1, min(days, 2))

        # Tomorrow
        if "tomorrow" in request_l:
            days = max(days, 2)

        # Weekend
        if "weekend" in request_l:
            days = max(days, 3)

        # "next X days" / "X day forecast"
        m = re.search(r"\b(\d{1,2})\s*-\s*day\b|\b(\d{1,2})\s*day\s*forecast\b|\bnext\s+(\d{1,2})\s+days\b", request_l)
        if m:
            n = next((g for g in m.groups() if g), None)
            if n:
                try:
                    days = max(1, min(int(n), 14))
                except Exception:
                    pass

        # explicit date
        mdate = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", request_l)
        if mdate:
            date_str = mdate.group(1)
            days = max(days, 3)

        # Hourly
        if "hourly" in request_l or "next hour" in request_l or "next few hours" in request_l:
            hours = max(hours, 12)

        return {"days": days, "date": date_str, "hours": hours}

    # -------------------- Facts builder (for LLM) --------------------

    def _build_facts_block(
        self,
        data: Dict[str, Any],
        units: str,
        want_days: int,
        wanted_date: Optional[date],
        hourly_peek_hours: int,
        used_default_location: bool
    ) -> str:
        loc = data.get("location", {}) or {}
        cur = data.get("current", {}) or {}
        forecast = data.get("forecast", {}) or {}
        alerts = data.get("alerts", {}) or {}

        name = loc.get("name") or "Unknown location"
        region = loc.get("region") or ""
        country = loc.get("country") or ""
        loc_line = ", ".join([p for p in [name, region, country] if p])
        if used_default_location:
            loc_line += " (default location)"

        temp_unit = "°F" if units == "us" else "°C"

        facts: List[str] = []
        facts.append(f"Location: {loc_line}")

        cond = (cur.get("condition") or {}).get("text") or "Unknown"
        temp = cur.get("temp_f") if units == "us" else cur.get("temp_c")
        feels = cur.get("feelslike_f") if units == "us" else cur.get("feelslike_c")
        wind = cur.get("wind_mph") if units == "us" else cur.get("wind_kph")
        wind_unit = "mph" if units == "us" else "kph"
        humidity = cur.get("humidity")
        uv = cur.get("uv")

        facts.append(
            f"Current: {cond}, {temp}{temp_unit} (feels {feels}{temp_unit}), wind {wind} {wind_unit}, "
            f"humidity {humidity}%, UV {uv}."
        )

        # AQI (compact)
        aq = self._safe_get(cur, "air_quality", default=None)
        if isinstance(aq, dict):
            epa = aq.get("us-epa-index")
            epa_label = self._us_epa_label(epa)
            if epa_label:
                facts.append(f"AQI: US EPA {epa} ({epa_label}). PM2.5 {aq.get('pm2_5')} µg/m³.")

        # Pollen (compact)
        pollen = self._safe_get(cur, "pollen", default=None)
        if isinstance(pollen, dict) and pollen:
            keep = []
            for k in ("grass", "ragweed", "mugwort", "oak", "birch"):
                v = pollen.get(k)
                if v is not None:
                    keep.append(f"{k} {v}")
            if keep:
                facts.append("Pollen: " + ", ".join(keep) + ".")

        # Forecast (compact)
        fdays = (forecast.get("forecastday") or []) if isinstance(forecast, dict) else []
        if fdays:
            if wanted_date:
                fd = next((d for d in fdays if d.get("date") == wanted_date.strftime("%Y-%m-%d")), None)
                if fd:
                    facts.append("Forecast (requested date): " + self._format_one_day_plain(fd, units))
                else:
                    facts.append(f"Forecast note: requested date {wanted_date} not in returned window.")
            else:
                shown = fdays[: max(1, want_days)]
                facts.append("Forecast:")
                for fd in shown:
                    facts.append("- " + self._format_one_day_plain(fd, units))

            # Hourly peek (compact)
            if hourly_peek_hours and fdays:
                hours = (fdays[0].get("hour") or [])
                if hours:
                    now_epoch = cur.get("last_updated_epoch") or 0
                    upcoming = [h for h in hours if (h.get("time_epoch") or 0) >= now_epoch]
                    peek = upcoming[:hourly_peek_hours]
                    if peek:
                        bits = []
                        for h in peek:
                            t = (h.get("time") or "")[-5:]
                            ht = h.get("temp_f") if units == "us" else h.get("temp_c")
                            hc = (h.get("condition") or {}).get("text") or ""
                            pop = h.get("chance_of_rain")
                            bits.append(f"{t} {ht}{temp_unit} {hc} ({pop}% rain)")
                        facts.append("Next hours: " + " | ".join(bits))

        # Alerts (titles only)
        alert_list = alerts.get("alert") if isinstance(alerts, dict) else None
        if isinstance(alert_list, list) and alert_list:
            facts.append("Alerts:")
            for a in alert_list[:6]:
                headline = a.get("headline") or a.get("event") or "Alert"
                severity = a.get("severity") or ""
                expires = a.get("expires") or ""
                bits = headline
                if severity:
                    bits += f" — {severity}"
                if expires:
                    bits += f" (expires {expires})"
                facts.append(f"- {bits}")

        return "\n".join(facts)

    @staticmethod
    def _format_one_day_plain(fd: Dict[str, Any], units: str) -> str:
        d = fd.get("day") or {}
        date_str = fd.get("date") or "Unknown date"
        cond = (d.get("condition") or {}).get("text") or "Unknown"
        hi = d.get("maxtemp_f") if units == "us" else d.get("maxtemp_c")
        lo = d.get("mintemp_f") if units == "us" else d.get("mintemp_c")
        temp_unit = "°F" if units == "us" else "°C"
        rain_chance = d.get("daily_chance_of_rain")
        snow_chance = d.get("daily_chance_of_snow")
        maxwind = d.get("maxwind_mph") if units == "us" else d.get("maxwind_kph")
        wind_unit = "mph" if units == "us" else "kph"
        return (
            f"{date_str}: {cond}. High {hi}{temp_unit} / Low {lo}{temp_unit}. "
            f"Wind up to {maxwind} {wind_unit}. Rain {rain_chance}% (snow {snow_chance}%)."
        )

    # -------------------- LLM answer --------------------

    async def _answer_with_llm(self, llm_client, user_request: str, facts: str, max_chars: int) -> str:
        first, last = get_tater_name()

        prompt = (
            f"You are {first} {last}. The user asked:\n"
            f"{user_request}\n\n"
            "You have weather facts below.\n\n"
            "RULES:\n"
            "- Answer ONLY what the user asked. Do not dump everything.\n"
            "- If the user asked about tomorrow, focus on tomorrow.\n"
            "- If they asked current weather, focus on current.\n"
            "- Mention alerts ONLY if the user asked about alerts, OR if they are safety-critical.\n"
            "- Keep it short, clear, and practical.\n"
            f"- Hard limit: {max_chars} characters.\n"
            "- Output plain text (no JSON, no markdown).\n\n"
            "WEATHER FACTS:\n"
            f"{facts}\n"
        )

        resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
        text = (resp.get("message", {}) or {}).get("content", "") or ""
        text = re.sub(r"\s+", " ", text).strip()
        return text[:max_chars]

    # -------------------- Core execution --------------------

    async def _get_weather_text(self, args: Dict[str, Any], llm_client) -> str:
        settings = self._get_settings()
        api_key = settings["WEATHERAPI_KEY"]
        if not api_key:
            return "Weather is not configured. Please set your WeatherAPI.com API key in the plugin settings."

        if llm_client is None:
            return "Weather failed: LLM client not provided."

        args = args or {}
        request_text = (args.get("request") or "").strip()
        if not request_text:
            return "No weather request provided."

        location_override = self._extract_location_override(request_text)
        used_default_location = location_override is None
        location = location_override or settings["DEFAULT_LOCATION"]

        parsed = self._interpret_request(
            request_text,
            default_days=settings["DEFAULT_DAYS"],
            default_hours=settings["SHOW_HOURLY_PEEK"],
        )

        days = max(1, min(int(parsed.get("days") or settings["DEFAULT_DAYS"]), 14))
        wanted_date = self._parse_date(parsed.get("date"))
        hours = max(0, min(int(parsed.get("hours") or settings["SHOW_HOURLY_PEEK"]), 48))
        units = settings["DEFAULT_UNITS"]
        max_chars = settings["MAX_RESPONSE_CHARS"]

        params = {
            "key": api_key,
            "q": location,
            "days": days,
            "aqi": "yes" if settings["INCLUDE_AQI"] else "no",
            "alerts": "yes" if settings["INCLUDE_ALERTS"] else "no",
            "pollen": "yes" if settings["INCLUDE_POLLEN"] else "no",
        }

        timeout = settings["TIMEOUT_SECONDS"]
        data, err = await asyncio.to_thread(self._request_weatherapi, "forecast.json", params, timeout)
        if err:
            return err
        if not data:
            return "No weather data returned."

        facts = self._build_facts_block(
            data=data,
            units=units,
            want_days=days,
            wanted_date=wanted_date,
            hourly_peek_hours=hours,
            used_default_location=used_default_location,
        )

        # Always summarize via LLM (no fallback)
        try:
            answered = await self._answer_with_llm(llm_client, request_text, facts, max_chars=max_chars)
            if answered:
                return answered
        except Exception as e:
            logger.error(f"[weather_forecast] summarize failed: {e}", exc_info=True)

        return "I couldn’t summarize the weather right now. Please try again."

    # -------------------- Platform handlers --------------------

    async def handle_discord(self, message, args, llm_client):
        return await self._get_weather_text(args or {}, llm_client=llm_client)

    async def handle_webui(self, args, llm_client):
        async def inner():
            return await self._get_weather_text(args or {}, llm_client=llm_client)

        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        text = await self._get_weather_text(args or {}, llm_client=llm_client)
        one_line = re.sub(r"\s+", " ", text).strip()
        return f"{user}: {one_line[:430]}"

    async def handle_homeassistant(self, args, llm_client):
        return (await self._get_weather_text(args or {}, llm_client=llm_client)).strip()

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return await self._get_weather_text(args or {}, llm_client=llm_client)

    async def handle_homekit(self, args, llm_client):
        return self._siri_flatten(await self._get_weather_text(args or {}, llm_client=llm_client))

    async def handle_xbmc(self, args, llm_client):
        return (await self._get_weather_text(args or {}, llm_client=llm_client)).strip()


plugin = WeatherForecastPlugin()
