# plugins/events_query.py
import logging
import json
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
import re

import httpx
import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client, extract_json_array

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
      - "is anyone currently in the back yard?"
      - "what happened in the garage last 24 hours?"
      - "anything happen around the house today?"
    """
    name = "events_query"
    pretty_name = "Events Query"
    description = (
        "Answer questions about stored household events (all sources) by area and timeframe. "
        "Use this when the user asks what happened, who was seen, how long something occurred, "
        "or whether someone is currently there. The model should always include the original user question "
        "as the 'query' argument so context is preserved."
    )

    usage = (
        "{\n"
        '  "function": "events_query",\n'
        '  "arguments": {\n'
        '    "area": "front yard",            // optional\n'
        '    "timeframe": "today|yesterday|last_24h|<date like Oct 14 or 2025-10-14>",\n'
        '    "query": "is anyone currently in the back yard"  // original user question\n'
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
            raw = redis_client.hget("ha_automations_platform_settings", "bind_port")
            port = int(raw) if raw is not None else 8788
        except Exception:
            port = 8788
        return f"http://127.0.0.1:{port}"

    # ---------- Common helpers ----------
    @staticmethod
    def _norm_area(s: Optional[str]) -> str:
        # lowercase, remove all non-alphanumerics (spaces, underscores, punctuation)
        return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

    def _areas_match(self, a: Optional[str], b: Optional[str]) -> bool:
        na = self._norm_area(a)
        nb = self._norm_area(b)
        if not na or not nb:
            return False
        # exact or contains either direction (handles "back yard" vs "backyard" or "back porch")
        return na == nb or na in nb or nb in na

    # ---------- Time helpers ----------
    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _ha_now(self, ha_base: str, token: str, sensor_entity: str) -> datetime:
        """
        Use HA's reported time exactly as local time. If HA provides an offset, strip it.
        Fallback to local system time (also naive).
        """
        try:
            url = f"{ha_base}/api/states/{sensor_entity}"
            r = requests.get(url, headers=self._ha_headers(token), timeout=5)
            if r.status_code < 400:
                state = (r.json() or {}).get("state", "")
                if state:
                    dt = datetime.fromisoformat(state)
                    return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except Exception:
            logger.info("[events_query] HA time sensor fetch failed; using local system time")
        return datetime.now()

    @staticmethod
    def _day_bounds(dt: datetime) -> Tuple[datetime, datetime]:
        # dt is naive local; keep it that way
        start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1) - timedelta(microseconds=1)
        return start, end

    @staticmethod
    def _yesterday_bounds(dt: datetime) -> Tuple[datetime, datetime]:
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
        Returns a naive local date (no tz).
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
        prefix = "tater:automations:events:"
        sources = []
        try:
            for key in redis_client.scan_iter(match=f"{prefix}*", count=500):
                src = key.split(":", maxsplit=3)[-1]
                if src and src not in sources:
                    sources.append(src)
        except Exception as e:
            logger.warning(f"[events_query] source discovery failed: {e}")
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

    # ---------- Time/Message helpers ----------
    @staticmethod
    def _parse_event_dt(e: Dict[str, Any]) -> Optional[datetime]:
        """
        Prefer ha_time. Treat whatever HA gave as local; strip timezone if present.
        Fallback to ts (epoch seconds) as local naive.
        """
        ha_time = (e.get("ha_time") or "").strip()
        if ha_time:
            try:
                dt = datetime.fromisoformat(ha_time)
                return dt.replace(tzinfo=None) if dt.tzinfo else dt
            except Exception:
                pass

        ts = e.get("ts")
        if isinstance(ts, int):
            try:
                # Treat server epoch as local naive for display/windowing
                return datetime.fromtimestamp(ts)
            except Exception:
                pass

        return None

    @staticmethod
    def _human_time(dt: datetime) -> str:
        # Return "3:41 PM" style local time
        try:
            return dt.strftime("%-I:%M %p")
        except Exception:
            return dt.strftime("%I:%M %p").lstrip("0")

    @staticmethod
    def _minutes_ago(now: datetime, dt: datetime) -> Optional[int]:
        try:
            delta = now - dt  # both naive local
            return max(0, int(delta.total_seconds() // 60))
        except Exception:
            return None

    @staticmethod
    def _contains_person_text(s: str) -> bool:
        s = (s or "").lower()
        keywords = [
            "person", "someone", "people", "man", "woman", "kid", "child",
            "visitor", "delivery", "driver", "courier", "walker", "intruder"
        ]
        return any(k in s for k in keywords)

    # ---------- Filtering ----------
    @staticmethod
    def _within_window(e: Dict[str, Any], start: datetime, end: datetime) -> bool:
        dt = EventsQueryPlugin._parse_event_dt(e)
        if not dt:
            return False
        return start <= dt <= end

    def _event_matches(self, e: Dict[str, Any], area: Optional[str], start: datetime, end: datetime) -> bool:
        if not self._within_window(e, start, end):
            return False
        if area:
            ev_area = ((e.get("data") or {}).get("area") or "").strip()
            if not self._areas_match(area, ev_area):
                return False
        return True

    # ---------- Presence intent ----------
    @staticmethod
    def _is_presence_query(q: Optional[str]) -> bool:
        if not q:
            return False
        s = q.lower()
        triggers = [
            "is anyone", "anyone there", "currently", "right now", "there now",
            "still there", "present", "on site", "on the porch", "in the yard now"
        ]
        return any(t in s for t in triggers)

    def _presence_answer_for_area(self, now: datetime, area_name: str, events: List[Dict[str, Any]]) -> str:
        # Find most recent "person-like" event for this area today
        latest: Optional[Tuple[datetime, Dict[str, Any]]] = None
        for e in events:
            ev_area = ((e.get("data") or {}).get("area") or "").strip()
            if not self._areas_match(area_name, ev_area):
                continue
            msg = (e.get("message") or "") + " " + (e.get("title") or "")
            if not self._contains_person_text(msg):
                continue
            dt = self._parse_event_dt(e)
            if not dt:
                continue
            if (latest is None) or (dt > latest[0]):
                latest = (dt, e)

        if not latest:
            return f"No one has been detected in {area_name} today."

        last_dt = latest[0]
        mins = self._minutes_ago(now, last_dt)
        if mins is not None:
            if mins <= 2:
                return f"Yes — someone was just seen in {area_name}."
            if mins <= 59:
                return f"Yes — someone was seen in {area_name} about {mins} minutes ago."
        # Fallback to clock time
        return f"Yes — someone was seen in {area_name} at {self._human_time(last_dt)}."

    @staticmethod
    def _looks_like_whole_home(phrase: str) -> bool:
        s = (phrase or "").lower()
        return any(t in s for t in ["around the house", "the house", "house", "home", "entire", "everywhere"])

    @staticmethod
    def _looks_like_outside(phrase: str) -> bool:
        s = (phrase or "").lower()
        return any(t in s for t in ["outside", "yard", "outdoors", "outside the", "around the yard"])

    @staticmethod
    def _filter_outdoor_like(catalog: List[str]) -> List[str]:
        outs = []
        for a in catalog:
            al = a.lower()
            if any(k in al for k in [
                "yard", "porch", "driveway", "garage", "garden", "deck", "patio", "courtyard", "front", "back", "sidewalk", "outside"
            ]):
                outs.append(a)
        # If we filtered out everything, fall back to catalog rather than empty
        return outs or catalog

    async def _resolve_areas_with_llm(self, user_area: str, events: List[Dict[str, Any]], llm_client) -> Optional[List[str]]:
        """
        Ask the LLM to map a user-provided area phrase (e.g., 'outside', 'around the house')
        to a subset of canonical areas actually present in stored events.
        Returns a list of selected area strings (exactly as they appear in events), or None on failure.
        """
        # Build a catalog of known areas from events (normalized & deduped)
        raw_catalog = [
            ((e.get("data") or {}).get("area") or "").strip()
            for e in events
            if ((e.get("data") or {}).get("area") or "").strip()
        ]
        # Normalize trivial variants (trim spaces), keep original casing
        cat_set = {}
        for a in raw_catalog:
            key = a.strip()
            if key:
                cat_set[key] = True
        catalog = sorted(cat_set.keys())
        if not catalog:
            return None

        # Heuristic short-circuit for common phrases
        if self._looks_like_whole_home(user_area):
            return catalog[:]  # all areas
        if self._looks_like_outside(user_area):
            return self._filter_outdoor_like(catalog)

        # Few-shot prompt with strict instructions
        examples = (
            "Examples:\n"
            "User phrase: \"around the house\"\n"
            "Known areas: [\"front yard\",\"back yard\",\"garage\",\"living room\",\"kitchen\"]\n"
            "Return: [\"front yard\",\"back yard\",\"garage\",\"living room\",\"kitchen\"]\n\n"
            "User phrase: \"outside\"\n"
            "Known areas: [\"front yard\",\"back yard\",\"garage\",\"living room\",\"kitchen\",\"porch\"]\n"
            "Return: [\"front yard\",\"back yard\",\"porch\",\"garage\"]\n\n"
            "User phrase: \"front porch\"\n"
            "Known areas: [\"front yard\",\"porch\",\"back yard\"]\n"
            "Return: [\"porch\"]\n"
        )
        system = (
            "Map a user's area phrase to one or more of the known areas.\n"
            "Output strictly a JSON array of strings (no code fences, no prose).\n"
            "Pick multiple if implied.\n"
            "- If the phrase implies outdoors (e.g., 'outside', 'yard'), choose all outdoor-like areas.\n"
            "- If the phrase implies the whole home (e.g., 'house', 'home', 'around the home'), choose ALL areas.\n"
            "- If the phrase names a specific area (e.g., 'front yard', 'porch'), pick that (and close variants if needed).\n"
            "- If you cannot decide, return an empty list [].\n\n" + examples
        )
        user = json.dumps({
            "user_area_phrase": (user_area or "").strip(),
            "known_areas_catalog": catalog
        }, ensure_ascii=False)

        try:
            # If your llm_client supports temperature/max_tokens, pass them; otherwise they are ignored harmlessly.
            resp = await llm_client.chat(
                messages=[{"role": "system", "content": system},
                          {"role": "user", "content": user}],
                temperature=0.1,
                max_tokens=64,
                timeout_ms=30_000
            )

            raw = (resp.get("message", {}) or {}).get("content", "").strip()
            selected = extract_json_array(raw)
            if selected is None or not isinstance(selected, list):
                # fall through to heuristics below
                selected = []

            # Keep only intersections with the catalog (exact string match after strip)
            cat_set_exact = {c.strip() for c in catalog}
            out = []
            for s in selected:
                if isinstance(s, str) and s.strip() in cat_set_exact:
                    out.append(s.strip())

            # If the model gave none, try heuristic expansions
            if not out:
                if self._looks_like_whole_home(user_area):
                    out = catalog[:]
                elif self._looks_like_outside(user_area):
                    out = self._filter_outdoor_like(catalog)

            return out  # may be []
        except Exception:
            # As a last resort, heuristics; else None to signal fallback path
            if self._looks_like_whole_home(user_area):
                return catalog[:]
            if self._looks_like_outside(user_area):
                return self._filter_outdoor_like(catalog)
            return None

    # ---------- Summarization ----------
    async def _summarize(self, events: List[Dict[str, Any]], area: Optional[str],
                         label: str, llm_client, user_query: Optional[str] = None) -> str:
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
            "in that area and timeframe (e.g., 'about 3 hours'). Mention it naturally.\n"
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
                temperature=0.2,
                max_tokens=400,
                timeout_ms=60_000,
            )
            text = (resp.get("message", {}) or {}).get("content", "").strip()
            if text:
                return text
        except Exception as e:
            logger.info(f"[events_query] LLM summary failed; using fallback: {e}")

        # Fallback list
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
        if tf in ("tonight", "this evening"):
            # heuristic window: 5pm -> end of day (or now if earlier)
            start = now.replace(hour=17, minute=0, second=0, microsecond=0)
            end = max(now, start.replace(hour=23, minute=59, second=59, microsecond=999999))
            label = "tonight"
        elif tf in ("this morning", "morning"):
            start = now.replace(hour=5, minute=0, second=0, microsecond=0)
            end = now.replace(hour=12, minute=0, second=0, microsecond=0) - timedelta(microseconds=1)
            label = "this morning"
        elif tf == "today":
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
                # Always treat parsed date as local-naive
                parsed = parsed.replace(tzinfo=None)
                start, end = self._day_bounds(parsed)
                label = f"on {parsed.strftime('%b %d, %Y')}"

        # Fetch all sources first
        items = await self._fetch_all_sources()

        # Resolve area phrase via LLM (if provided)
        area_arg = (args.get("area") or "").strip()
        resolved_areas: Optional[List[str]] = None
        if area_arg:
            resolved_areas = await self._resolve_areas_with_llm(area_arg, items, llm_client)

        # Presence intent?
        user_query = (args.get("query") or "").strip()
        if self._is_presence_query(user_query) and (area_arg or (resolved_areas and len(resolved_areas) > 0)):
            # Use resolved areas if available; else fallback to the raw area_arg
            areas_to_check = resolved_areas if (resolved_areas and len(resolved_areas) > 0) else [area_arg]
            # Limit window to today regardless of timeframe (matches the requirement)
            p_start, p_end = self._day_bounds(now)

            # Keep only today's events for speed
            todays = [e for e in items if self._within_window(e, p_start, p_end)]

            answers = []
            for a in areas_to_check:
                answers.append(self._presence_answer_for_area(now, a, todays))

            # If multiple areas (e.g., "outside"), join them on new lines
            return "\n".join(answers)

        # Otherwise: normal summarization path
        # Filter using resolved areas if available, else fallback to legacy area matching
        if resolved_areas is not None and len(resolved_areas) > 0:
            target_norms = {self._norm_area(a) for a in resolved_areas}
            def _match_area_list(e: Dict[str, Any]) -> bool:
                ev_area = ((e.get("data") or {}).get("area") or "").strip()
                return self._norm_area(ev_area) in target_norms
            filtered = [e for e in items if self._within_window(e, start, end) and _match_area_list(e)]
            label_hint = f"{label} (areas: {', '.join(resolved_areas)})"
            return await self._summarize(filtered, area_arg or None, label_hint, llm_client, user_query=user_query)
        else:
            # If the phrase clearly means "whole home" or "outside" and we couldn't resolve via LLM,
            # expand heuristically so we don't miss events.
            heuristic_areas: Optional[List[str]] = None
            if area_arg:
                # Build catalog again for heuristic expansion
                catalog = sorted({
                    ((e.get("data") or {}).get("area") or "").strip()
                    for e in items if ((e.get("data") or {}).get("area") or "").strip()
                })
                if self._looks_like_whole_home(area_arg):
                    heuristic_areas = catalog[:]
                elif self._looks_like_outside(area_arg):
                    heuristic_areas = self._filter_outdoor_like(catalog)

            if heuristic_areas:
                target_norms = {self._norm_area(a) for a in heuristic_areas}
                def _match_area_list2(e: Dict[str, Any]) -> bool:
                    ev_area = ((e.get("data") or {}).get("area") or "").strip()
                    return self._norm_area(ev_area) in target_norms
                filtered = [e for e in items if self._within_window(e, start, end) and _match_area_list2(e)]
                label_hint = f"{label} (areas: {', '.join(heuristic_areas)})"
                return await self._summarize(filtered, area_arg, label_hint, llm_client, user_query=user_query)

            # Fallback to loose area matching (or unfiltered if no area provided)
            area = area_arg or None
            filtered = [e for e in items if self._event_matches(e, area, start, end)]
            return await self._summarize(filtered, area, label, llm_client, user_query=user_query)

    # ---------- Platform shims ----------
    async def handle_webui(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

    async def handle_homeassistant(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

plugin = EventsQueryPlugin()