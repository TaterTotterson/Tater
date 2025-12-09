# plugins/events_query_brief.py
import logging
import json
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from urllib.parse import urlencode
import re

import httpx
import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client, extract_json

load_dotenv()
logger = logging.getLogger("events_query_brief")
logger.setLevel(logging.INFO)


class EventsQueryBriefPlugin(ToolPlugin):
    """
    Automation-only version of events_query.

    Produces a very short, plain-text summary of household events
    suitable for automation variables or sensor states.
    """
    name = "events_query_brief"
    pretty_name = "Events Query (Brief)"

    description = (
        "Use this to generate a brief, plain-text summary of household events for automations. "
        "Call this when the user explicitly asks to run events query brief, or when an automation "
        "needs a short summary of what happened in a specific area and timeframe. "
        "Always extract the area and timeframe from the user’s request, and pass the full original "
        "user request as the query argument so context is preserved."
    )

    usage = (
        "{\n"
        '  "function": "events_query_brief",\n'
        '  "arguments": {\n'
        '    "area": "front yard",            // optional: specific location or area\n'
        '    "timeframe": "today|yesterday|last_24h|<date like Oct 14 or 2025-10-14>",\n'
        '    "query": "run events query brief for the front yard today"  // full original user request\n'
        "  }\n"
        "}\n"
    )

    # Automation platform only
    platforms = ["automation"]

    # Share config with events_query
    settings_category = "Events Query"

    required_settings = {
        "HA_BASE_URL": {
            "label": "Home Assistant Base URL",
            "type": "string",
            "default": "http://homeassistant.local:8123",
        },
        "HA_TOKEN": {
            "label": "Home Assistant Long-Lived Token",
            "type": "string",
            "default": "",
        },
        "TIME_SENSOR_ENTITY": {
            "label": "Time Sensor (ISO)",
            "type": "string",
            "default": "sensor.date_time_iso",
        },
    }

    waiting_prompt_template = (
        "Checking recent home events now. This will be quick."
    )

    # ─────────────────────────────────────────────────────────────
    # Settings / HA helpers
    # ─────────────────────────────────────────────────────────────

    def _s(self) -> Dict[str, str]:
        return redis_client.hgetall(f"plugin_settings:{self.settings_category}") or {}

    def _ha(self, s: Dict[str, str]) -> Dict[str, str]:
        base = (s.get("HA_BASE_URL") or "").rstrip("/")
        token = s.get("HA_TOKEN") or ""
        if not token:
            raise ValueError("Missing HA_TOKEN in Events Query settings")
        sensor = s.get("TIME_SENSOR_ENTITY") or "sensor.date_time_iso"
        return {"base": base, "token": token, "time_sensor": sensor}

    def _automation_base(self) -> str:
        try:
            port = int(redis_client.hget("ha_automations_platform_settings", "bind_port") or 8788)
        except Exception:
            port = 8788
        return f"http://127.0.0.1:{port}"

    # ─────────────────────────────────────────────────────────────
    # Time helpers
    # ─────────────────────────────────────────────────────────────

    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _ha_now(self, ha: Dict[str, str]) -> datetime:
        try:
            url = f"{ha['base']}/api/states/{ha['time_sensor']}"
            r = requests.get(url, headers=self._ha_headers(ha["token"]), timeout=5)
            if r.status_code < 400:
                state = (r.json() or {}).get("state", "")
                dt = datetime.fromisoformat(state)
                return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except Exception:
            pass
        return datetime.now()

    @staticmethod
    def _day_bounds(dt: datetime) -> Tuple[datetime, datetime]:
        start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return start, start + timedelta(days=1) - timedelta(seconds=1)

    @staticmethod
    def _yesterday_bounds(dt: datetime) -> Tuple[datetime, datetime]:
        start = (dt - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, start + timedelta(days=1) - timedelta(seconds=1)

    # ─────────────────────────────────────────────────────────────
    # Event fetch
    # ─────────────────────────────────────────────────────────────

    def _discover_sources(self) -> List[str]:
        prefix = "tater:automations:events:"
        sources = []
        for key in redis_client.scan_iter(match=f"{prefix}*", count=500):
            src = key.split(":", maxsplit=3)[-1]
            if src and src not in sources:
                sources.append(src)
        return sources

    async def _fetch_one(self, base: str, src: str, start: datetime, end: datetime):
        params = {
            "source": src,
            "since": start.isoformat(),
            "until": end.isoformat(),
            "limit": 1000,
        }
        url = f"{base}/tater-ha/v1/events/search?{urlencode(params)}"
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(url)
                r.raise_for_status()
                items = (r.json() or {}).get("items", [])
                for i in items:
                    i.setdefault("source", src)
                return items
        except Exception:
            return []

    async def _fetch(self, sources: List[str], start: datetime, end: datetime):
        base = self._automation_base()
        events = []
        for src in sources:
            events.extend(await self._fetch_one(base, src, start, end))
        return events

    # ─────────────────────────────────────────────────────────────
    # Compact summarization
    # ─────────────────────────────────────────────────────────────

    async def _summarize(self, events, area, label, llm_client, query):
        payload = {
            "area": area or "all areas",
            "timeframe": label,
            "user_query": query,
            "events": [
                {
                    "area": e.get("source", "").replace("_", " "),
                    "title": e.get("title", ""),
                    "message": e.get("message", ""),
                }
                for e in events
            ],
        }

        system = (
            "Summarize household events for an automation.\n"
            "Rules:\n"
            "- Plain text only\n"
            "- Max 3 short sentences\n"
            "- Very concise\n"
            "- If nothing happened, clearly say so\n"
        )

        try:
            r = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(payload)},
                ],
                temperature=0.1,
                max_tokens=120,
            )
            return r["message"]["content"].strip()
        except Exception:
            pass

        if not events:
            return f"No activity detected {label}."

        msgs = []
        for e in events[:2]:
            msg = e.get("message") or e.get("title")
            if msg:
                msgs.append(msg.strip())
        return "; ".join(msgs) or f"Activity detected {label}."

    @staticmethod
    def _compact(text: str, limit: int = 240) -> str:
        t = re.sub(r"\s+", " ", text or "").strip()
        if len(t) <= limit:
            return t
        cut = t[:limit]
        cut = cut[: cut.rfind(" ")] if " " in cut[40:] else cut
        return cut.rstrip(". ,;:") + "…"

    # ─────────────────────────────────────────────────────────────
    # Core handler
    # ─────────────────────────────────────────────────────────────

    async def _handle(self, args, llm_client):
        s = self._s()
        ha = self._ha(s)
        now = self._ha_now(ha)

        tf = (args.get("timeframe") or "today").lower()
        area = (args.get("area") or "").strip()
        query = (args.get("query") or "").strip()

        if tf == "yesterday":
            start, end = self._yesterday_bounds(now)
            label = "yesterday"
        elif tf in ("last_24h", "last24h"):
            start, end = now - timedelta(hours=24), now
            label = "in the last 24 hours"
        else:
            start, end = self._day_bounds(now)
            label = "today"

        sources = self._discover_sources()
        events = await self._fetch(sources, start, end)

        summary = await self._summarize(events, area, label, llm_client, query)
        return self._compact(summary)

    # ─────────────────────────────────────────────────────────────
    # Automation platform entry
    # ─────────────────────────────────────────────────────────────

    async def handle_automation(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)


plugin = EventsQueryBriefPlugin()