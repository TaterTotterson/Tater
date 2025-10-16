# plugins/events_query.py
import logging
import json
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
import re

import httpx     # async HTTP client (prevents HA self-call deadlocks)
import requests  # sync for HA time read
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client

load_dotenv()
logger = logging.getLogger("events_query")
logger.setLevel(logging.INFO)


class EventsQueryPlugin(ToolPlugin):
    """
    Retrieve and summarize ANY stored house events from the Automations bridge,
    across all sources saved under Redis key pattern: tater:automations:events:*.

    Natural asks:
      - "what happened in the front yard today?"
      - "was there anyone in the back yard yesterday?"
      - "what happened in the house on Oct 14th?"
      - "what happened in the garage last 24 hours?"
      - "what happened outside today?"
    """
    name = "events_query"
    pretty_name = "Events Query"
    description = (
        "Answer questions about stored household events (all sources) by area and timeframe. "
        "Use this when the user asks what happened, who was seen, or how long something occurred "
        "in a specific place or time. The model should always include the original user question "
        "as the 'query' argument so context is preserved."
    )

    usage = (
        "{\n"
        '  "function": "events_query",\n'
        '  "arguments": {\n'
        '    "area": "front yard",            // optional\n'
        '    "timeframe": "today|yesterday|last_24h|<date like Oct 14 or 2025-10-14>",\n'
        '    "query": "how long were the dogs outside today"  // original user question\n'
        "  }\n"
        "}\n"
    )

    platforms = ["webui", "homeassistant"]
    settings_category = "Events Query"

    required_settings = {
        # ---- Home Assistant time sync ----
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
            "description": "Sensor with ISO-8601 time (e.g., 2025-10-16T14:05:00-05:00)."
        },
    }

    waiting_prompt_template = (
        "Let {mention} know you’re checking recent home events now. "
        "Keep it short and friendly. No emojis. Only output that message."
    )

    # ---------- Settings / Env ----------
    def _s(self) -> Dict[str, str]:
        return redis_client.hgetall(f"plugin_settings:{self.settings_category}") or \
               redis_client.hgetall(f"plugin_settings: {self.settings_category}") or {}

    def _ha(self, s: Dict[str, str]) -> Dict[str, str]:
        base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
        token = s.get("HA_TOKEN") or ""
        if not token:
            raise ValueError("Missing HA_TOKEN in Events Query settings.")
        time_sensor = (s.get("TIME_SENSOR_ENTITY") or "sensor.date_time_iso").strip()
        return {"base": base, "token": token, "time_sensor": time_sensor}

    def _automation_base(self) -> str:
        try:
            raw = redis_client.hget("automation_platform_settings", "bind_port")
            port = int(raw) if raw is not None else 8788
        except Exception:
            port = 8788
        return f"http://127.0.0.1:{port}"

    # ---------- Area helpers ----------
    def _area_catalog_from_events(self, events: List[Dict[str, Any]]) -> List[str]:
        return sorted({
            ((e.get("data") or {}).get("area") or "").strip()
            for e in events
            if ((e.get("data") or {}).get("area") or "").strip()
        })

    def _guess_outdoor_areas(self, catalog: List[str]) -> List[str]:
        outdoor_keywords = [
            "yard", "porch", "patio", "drive", "driveway",
            "front", "back", "garden", "lawn", "deck"
        ]
        def _norm(s: str) -> str:
            return re.sub(r"[^a-z0-9]+", "", s.lower())
        return [a for a in catalog if any(k in _norm(a) for k in outdoor_keywords)]

    def _phrase_means_outdoors(self, phrase: str) -> bool:
        p = (phrase or "").lower()
        return any(w in p for w in ["outside", "outdoors", "around the house", "around the yard", "yard"])

    def _phrase_means_whole_home(self, phrase: str) -> bool:
        p = (phrase or "").lower()
        return any(w in p for w in ["house", "home", "whole house", "whole home", "entire house", "around the home"])

    async def _resolve_areas_with_llm(
        self, user_area: str, events: List[Dict[str, Any]], llm_client
    ) -> Optional[List[str]]:
        """
        Ask the LLM to map a user-provided area phrase (e.g., 'outside', 'around the house')
        to a subset of canonical areas actually present in stored events.
        Returns a list of selected area strings (exactly as they appear in events), or None on failure.
        """
        catalog = self._area_catalog_from_events(events)
        if not catalog:
            return None

        system = (
            "You map a user's area phrase to one or more of the known areas below.\n"
            "Return ONLY a JSON array of strings (no prose). Choose multiple if implied.\n"
            "Rules:\n"
            "- If the phrase implies outdoors (e.g., 'outside', 'outdoors', 'around the house', 'yard'), "
            "  select all outdoor-like areas from the catalog.\n"
            "- If the phrase implies the whole home (e.g., 'house', 'home', 'around the home'), "
            "  select ALL areas from the catalog.\n"
            "- If the phrase names a specific area (e.g., 'front yard', 'porch'), pick that (and close variants if needed).\n"
            "- If you cannot decide, return an empty array []."
        )
        user = json.dumps({
            "user_area_phrase": (user_area or "").strip(),
            "known_areas_catalog": catalog
        }, ensure_ascii=False)

        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                timeout_ms=30_000
            )
            raw = (resp.get("message", {}) or {}).get("content", "").strip()
            selected = json.loads(raw)  # expect strictly a JSON array
            if not isinstance(selected, list):
                return None

            cat_set = {c.strip() for c in catalog}
            out = []
            for s in selected:
                if isinstance(s, str) and s.strip() in cat_set:
                    out.append(s.strip())

            return out or []
        except Exception as e:
            logger.info("[events_query] area LLM mapping failed: %s", e)
            return None

    @staticmethod
    def _norm_area(s: Optional[str]) -> str:
        # lowercase, remove all non-alphanumerics (spaces, underscores, punctuation)
        return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

    def _areas_match(self, a: Optional[str], b: Optional[str]) -> bool:
        na = self._norm_area(a)
        nb = self._norm_area(b)
        if not na or not nb:
            return False
        return na == nb or na in nb or nb in na

    # ---------- Time helpers ----------
    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _ha_now(self, ha_base: str, token: str, sensor_entity: str) -> datetime:
        """
        Try HA ISO sensor for authoritative local time (includes tz offset).
        Fallback: UTC-now if unavailable.
        """
        try:
            url = f"{ha_base}/api/states/{sensor_entity}"
            r = requests.get(url, headers=self._ha_headers(token), timeout=5)
            if r.status_code < 400:
                state = (r.json() or {}).get("state", "")
                if state:
                    return datetime.fromisoformat(state)
        except Exception:
            logger.info("[events_query] HA time sensor fetch failed; using UTC-now")
        return datetime.now(timezone.utc)

    @staticmethod
    def _day_bounds(dt: datetime) -> Tuple[datetime, datetime]:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1) - timedelta(microseconds=1)
        return start, end

    @staticmethod
    def _yesterday_bounds(dt: datetime) -> Tuple[datetime, datetime]:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        start = (dt - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1) - timedelta(microseconds=1)
        return start, end

    @staticmethod
    def _strip_ordinal(day_str: str) -> str:
        # "14th" -> "14"
        return re.sub(r"(?i)(\d+)(st|nd|rd|th)", r"\1", day_str.strip())

    def _parse_loose_date(self, s: str, assume_year: int) -> Optional[datetime]:
        """
        Accepts:
          - YYYY-MM-DD (ISO)
          - Oct 14, October 14, Oct 14th, October 14th (with or without year)
          - 2025/10/14
        Returns a timezone-aware date (no time) using HA timezone if present.
        """
        if not s:
            return None
        s = s.strip()
        # ISO date
        try:
            if len(s) == 10 and s[4] == "-" and s[7] == "-":
                dt = datetime.fromisoformat(s)
                return dt
        except Exception:
            pass

        # Slash format
        for fmt in ("%Y/%m/%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                pass

        # Month name forms, with optional ordinal and optional year
        s2 = self._strip_ordinal(s)
        for fmt in ("%b %d %Y", "%B %d %Y", "%b %d", "%B %d"):
            try:
                dt = datetime.strptime(s2, fmt)
                if "%Y" not in fmt:
                    dt = dt.replace(year=assume_year)
                return dt
            except Exception:
                continue

        return None

    # ---------- Source discovery ----------
    @staticmethod
    def _discover_sources() -> List[str]:
        # Use Redis to enumerate all event lists: tater:automations:events:<source>
        prefix = "tater:automations:events:"
        sources = []
        try:
            for key in redis_client.scan_iter(match=f"{prefix}*", count=500):
                src = key.split(":", maxsplit=3)[-1]
                if src and src not in sources:
                    sources.append(src)
        except Exception as e:
            logger.warning(f"[events_query] source discovery failed: {e}")
        # Fallback to a few common defaults if none found
        return sources or ["camera_event", "doorbell_alert", "door_event", "hvac_event", "motion_event", "general"]

    # ---------- Fetch ----------
    async def _fetch_one_source(self, base: str, source: str, limit: int = 1000) -> List[Dict[str, Any]]:
        params = {"source": source, "limit": int(limit)}
        url = f"{base}/tater-ha/v1/events/search?{urlencode(params)}"
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                r = await client.get(url)
                r.raise_for_status()
                data = r.json() if r.headers.get("content-type", "").lower().startswith("application/json") else {}
                items = (data or {}).get("items", [])
                # Tag source for completeness
                for it in items:
                    it.setdefault("source", source)
                return items if isinstance(items, list) else []
        except Exception as e:
            logger.error(f"[events_query] fetch failed for source={source}: {e}")
            return []

    async def _fetch_all_sources(self) -> List[Dict[str, Any]]:
        base = self._automation_base()
        items: List[Dict[str, Any]] = []
        for src in self._discover_sources():
            items.extend(await self._fetch_one_source(base, src, limit=1000))
        return items

    # ---------- Filtering ----------
    @staticmethod
    def _within_window(e: Dict[str, Any], start: datetime, end: datetime) -> bool:
        # Prefer server ts (epoch seconds), fallback to ha_time (ISO)
        ts = e.get("ts")
        if isinstance(ts, int):
            tdt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return start.astimezone(timezone.utc) <= tdt <= end.astimezone(timezone.utc)
        ha_time = (e.get("ha_time") or "").strip()
        if ha_time:
            try:
                hdt = datetime.fromisoformat(ha_time)
                return start <= hdt <= end
            except Exception:
                return False
        return False

    @staticmethod
    def _eq_ci(a: Optional[str], b: Optional[str]) -> bool:
        return (a or "").strip().lower() == (b or "").strip().lower()

    def _event_matches(self, e: Dict[str, Any], area: Optional[str], start: datetime, end: datetime) -> bool:
        if not self._within_window(e, start, end):
            return False
        if area:
            ev_area = ((e.get("data") or {}).get("area") or "").strip()
            if not self._areas_match(area, ev_area):
                return False
        return True

    # ---------- Summarization ----------
    async def _summarize(
        self,
        events: List[Dict[str, Any]],
        area: Optional[str],
        label: str,
        llm_client,
        user_query: Optional[str] = None
    ) -> str:
        simplified = []
        for e in events:
            simplified.append({
                "source": (e.get("source") or ""),
                "title": (e.get("title") or "").strip(),
                "message": (e.get("message") or "").strip(),
                "type": (e.get("type") or "").strip(),
                "area": ((e.get("data") or {}).get("area") or "").strip(),
                "entity": (e.get("entity_id") or "").strip(),
                "time": (e.get("ha_time") or "").strip(),
                "level": (e.get("level") or "info").strip(),
            })

        system = (
            "You are summarizing household events for the homeowner.\n"
            "Your goal is to directly answer the user's question based on the provided events.\n"
            "- Be concise, natural, and conversational.\n"
            "- Mention the timeframe and area naturally (e.g., 'In the front yard today...').\n"
            "- Group related events by area (e.g., all front yard activity together).\n"
            "- If the user asks 'how many' or 'who', reason over the events and answer with counts or brief descriptions.\n"
            "- If the user asks 'how long', calculate or estimate the duration between the earliest and latest relevant events "
            "in that area and timeframe (e.g., 'about 3 hours'). Mention it naturally (e.g., 'The dogs were playing outside for about 3 hours').\n"
            "- Include times when relevant, but avoid repeating them excessively.\n"
            "- Avoid technical terms, entity IDs, or raw timestamps.\n"
            "- If there are no relevant events, clearly say so."
        )

        user_payload = {
            "user_request": user_query or f"Show events {label} in {area or 'the home'}",
            "context": {
                "area": area or "all areas",
                "timeframe": label,
                "events": simplified
            }
        }

        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
                ],
                timeout_ms=60_000,
            )
            text = (resp.get("message", {}) or {}).get("content", "").strip()
            if text:
                return text
        except Exception as e:
            logger.info(f"[events_query] LLM summary failed; using fallback: {e}")

        # --- Fallback listing ---
        if not events:
            return f"No events found for {area or 'all areas'} {label}."
        lines = [f"Here’s what I found for {area or 'all areas'} {label}:"]
        for i, e in enumerate(events, 1):
            t = (e.get("title") or "").strip()
            m = (e.get("message") or "").strip()
            typ = (e.get("type") or "").strip()
            ar = ((e.get("data") or {}).get("area") or "").strip()
            when = (e.get("ha_time") or "").strip()
            bits = []
            if typ: bits.append(typ)
            if ar: bits.append(ar)
            head = " • ".join(bits)
            suffix = f" at {when}" if when else ""
            if head: head = f"[{head}] "
            if t and m:
                lines.append(f"{i}. {head}{t} — {m}{suffix}")
            elif t:
                lines.append(f"{i}. {head}{t}{suffix}")
            elif m:
                lines.append(f"{i}. {head}{m}{suffix}")
            else:
                lines.append(f"{i}. {head}(no details){suffix}")
        return "\n".join(lines)

    # ---------- Core ----------
    async def _handle(self, args: Dict[str, Any], llm_client) -> str:
        s = self._s()
        ha = self._ha(s)

        # Align with HA local time
        now = self._ha_now(ha["base"], ha["token"], ha["time_sensor"])

        # Timeframe: today|yesterday|last_24h|<date>
        tf_raw = (args.get("timeframe") or "today").strip()
        tf = tf_raw.lower()
        if tf == "today":
            start, end = self._day_bounds(now)
            label = "today"
        elif tf == "yesterday":
            start, end = self._yesterday_bounds(now)
            label = "yesterday"
        elif tf in ("last_24h", "last24h", "past_24h"):
            end = now
            start = end - timedelta(hours=24)
            label = "in the last 24 hours"
        else:
            parsed = self._parse_loose_date(tf_raw, assume_year=now.year)
            if not parsed:
                start, end = self._day_bounds(now)
                label = f"today (unrecognized date: {tf_raw})"
            else:
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=now.tzinfo or timezone.utc)
                start, end = self._day_bounds(parsed)
                label = f"on {parsed.strftime('%b %d, %Y')}"

        if start.tzinfo is None: start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:   end = end.replace(tzinfo=timezone.utc)

        # Fetch all sources first (for both data & area catalog)
        items = await self._fetch_all_sources()
        catalog = self._area_catalog_from_events(items)

        # Resolve area phrase via LLM (if provided), else all areas
        area_arg = (args.get("area") or "").strip()
        resolved_areas: Optional[List[str]] = None
        if area_arg:
            # 1) Try LLM mapping against current catalog
            resolved_areas = await self._resolve_areas_with_llm(area_arg, items, llm_client)
            logger.info("[events_query] area phrase=%r catalog=%s -> llm_selected=%s",
                        area_arg, catalog, resolved_areas)

            # 2) Heuristic bundles if LLM is unsure or returns []
            if resolved_areas is None or len(resolved_areas) == 0:
                if self._phrase_means_outdoors(area_arg):
                    resolved_areas = self._guess_outdoor_areas(catalog)
                    logger.info("[events_query] heuristic outdoors -> %s", resolved_areas)
                elif self._phrase_means_whole_home(area_arg):
                    resolved_areas = list(catalog)
                    logger.info("[events_query] heuristic whole-home -> %s", resolved_areas)

        def _norm(s: Optional[str]) -> str:
            return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

        # Filtering logic
        if area_arg:
            if resolved_areas is not None and len(resolved_areas) > 0:
                target_norms = {_norm(a) for a in resolved_areas}
                def _match_area_list(e: Dict[str, Any]) -> bool:
                    ev_area = ((e.get("data") or {}).get("area") or "").strip()
                    return _norm(ev_area) in target_norms
                filtered = [e for e in items if self._within_window(e, start, end) and _match_area_list(e)]
                label_hint = f"{label} (areas: {', '.join(resolved_areas)})"
                # Pass the resolved areas as a readable label to the summarizer
                resolved_label = ", ".join(resolved_areas)
                return await self._summarize(filtered, resolved_label, label_hint, llm_client, user_query=args.get("query"))
            else:
                # Last resort: avoid over-filtering; show ALL areas for the timeframe
                logger.info("[events_query] unresolved area phrase; falling back to all areas for timeframe")
                filtered = [e for e in items if self._within_window(e, start, end)]
                label_hint = f"{label} (all areas; unresolved phrase: {area_arg})"
                return await self._summarize(filtered, None, label_hint, llm_client, user_query=args.get("query"))
        else:
            # No area specified at all -> all areas
            filtered = [e for e in items if self._within_window(e, start, end)]
            return await self._summarize(filtered, None, label, llm_client, user_query=args.get("query"))

    # ---------- Platform shims ----------
    async def handle_webui(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

    async def handle_homeassistant(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)


plugin = EventsQueryPlugin()