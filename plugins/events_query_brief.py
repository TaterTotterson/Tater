# plugins/events_query_brief.py
import logging
import json
from typing import Any, Dict, List, Tuple
from datetime import datetime, timedelta
from urllib.parse import urlencode
import re

import httpx
import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client

load_dotenv()
logger = logging.getLogger("events_query_brief")
logger.setLevel(logging.INFO)


class EventsQueryBriefPlugin(ToolPlugin):
    """
    Automation-only: generates a short, plain-text summary of stored events.

    (push mode):
      - Optionally writes the summary directly into an HA input_text entity via:
        POST /api/services/input_text/set_value

    Result:
      - Still returns the compact string (useful for logs/tests)
      - Can also "set" input_text.* without needing YAML templates.
    """

    name = "events_query_brief"
    plugin_name = "Events Query Brief"
    version = "1.0.2"
    min_tater_version = "50"
    pretty_name = "Events Query (Brief)"

    description = (
        "Automation tool that returns a very short summary of recent events "
        "(safe for dashboards). Can optionally write the summary into a Home Assistant "
        "input_text entity automatically."
    )
    plugin_dec = "Produce a terse dashboard-friendly summary of recent home events and optionally write it to Home Assistant."

    # Cleaner: automation users usually just pick the tool name.
    # Args are optional overrides.
    usage = '{"function":"events_query_brief","arguments":{"area":"front yard","timeframe":"today|yesterday|last_24h|<date>","query":"brief summary","input_text_entity":"input_text.front_yard_events_brief"}}'

    platforms = ["automation"]
    settings_category = "Events Query"

    required_settings = {
        # where to write the brief (optional)
        "INPUT_TEXT_ENTITY": {
            "label": "Input Text Entity to Update (optional)",
            "type": "string",
            "default": "",
            "description": "If set (example: input_text.event_brief), this plugin will write the summary into it.",
        },
    }

    waiting_prompt_template = "Checking recent home events now. This will be quick."

    # ─────────────────────────────────────────────────────────────
    # Settings / HA helpers
    # ─────────────────────────────────────────────────────────────

    def _s(self) -> Dict[str, str]:
        return (
            redis_client.hgetall(f"plugin_settings:{self.settings_category}")
            or redis_client.hgetall(f"plugin_settings: {self.settings_category}")
            or {}
        )

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

    def _automation_base(self) -> str:
        try:
            port = int(redis_client.hget("ha_automations_platform_settings", "bind_port") or 8788)
        except Exception:
            port = 8788
        return f"http://127.0.0.1:{port}"

    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _ha_now(self, ha: Dict[str, str]) -> datetime:
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
        sources: List[str] = []
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
        events: List[Dict[str, Any]] = []
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
    # write result into input_text
    # ─────────────────────────────────────────────────────────────

    def _set_input_text(self, ha_base: str, token: str, entity_id: str, value: str) -> None:
        entity_id = (entity_id or "").strip()
        if not entity_id:
            return

        url = f"{ha_base}/api/services/input_text/set_value"
        payload = {"entity_id": entity_id, "value": value}

        r = requests.post(url, headers=self._ha_headers(token), json=payload, timeout=10)
        if r.status_code >= 400:
            raise RuntimeError(f"input_text.set_value HTTP {r.status_code}: {r.text[:200]}")

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
        summary = self._compact(summary, limit=240)

        # push to input_text if configured
        # Priority:
        #   1) automation args override
        #   2) plugin setting default
        input_text_entity = (args.get("input_text_entity") or s.get("INPUT_TEXT_ENTITY") or "").strip()
        if input_text_entity:
            try:
                self._set_input_text(ha["base"], ha["token"], input_text_entity, summary)
            except Exception as e:
                # Don't break the automation result just because HA write failed
                logger.warning("[events_query_brief] Failed to set %s: %s", input_text_entity, e)

        return summary

    async def handle_automation(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)


plugin = EventsQueryBriefPlugin()
