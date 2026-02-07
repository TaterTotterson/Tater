# plugins/events_query.py
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
logger = logging.getLogger("events_query")
logger.setLevel(logging.INFO)


class EventsQueryPlugin(ToolPlugin):
    """
    Retrieve and summarize stored house events from the Automations bridge,
    across all sources saved under Redis key pattern: tater:automations:events:*.

    Natural asks:
      - "what happened in the front yard today?"
      - "was there anyone in the back yard yesterday?"
      - "is anyone currently in the back yard?"
      - "what happened in the garage last 24 hours?"
      - "anything happen around the house today?"
    """
    name = "events_query"
    plugin_name = "Events Query"
    version = "1.0.3"
    min_tater_version = "50"
    pretty_name = "Events Query"
    description = (
        "Answer questions about stored household events (all sources) by area and timeframe. "
        "Use this when the user asks what happened, who was seen, how long something occurred, "
        "or whether someone is currently there. The model should always include the original user question "
        "as the 'query' argument so context is preserved."
    )
    plugin_dec = "Answer questions about stored household events by area and timeframe."
    when_to_use = "Use for historical event summaries from stored automations logs (not live camera snapshots)."

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

    platforms = ["webui", "homeassistant", "homekit", "discord", "telegram", "matrix", "irc"]
    settings_category = "Events Query"

    required_settings = {
        # ---- Home Assistant time sync ----
        "TIME_SENSOR_ENTITY": {
            "label": "Time Sensor (ISO)",
            "type": "string",
            "default": "sensor.date_time_iso",
            "description": "Sensor with local-naive ISO time (e.g., 2025-10-19T20:07:00)."
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
        ha_settings = redis_client.hgetall("homeassistant_settings") or {}
        base = (ha_settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        token = (ha_settings.get("HA_TOKEN") or "").strip()
        if not token:
            raise ValueError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )
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
        # normalize for matching ("front yard" == "front_yard" == "FrontYard")
        return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

    @staticmethod
    def _source_to_area(source: str) -> str:
        # reverse slug → friendly name: "front_yard" -> "front yard"
        s = (source or "").strip().lower()
        s = s.replace("_", " ")
        return s

    @staticmethod
    def _area_to_source_slug(area: str) -> str:
        # friendly phrase → slug we store under: "Front Yard" -> "front_yard"
        s = (area or "").strip().lower()
        s = re.sub(r"\s+", "_", s)
        s = re.sub(r"[^a-z0-9_:-]", "", s)
        return s

    def _areas_match(self, a: Optional[str], b: Optional[str]) -> bool:
        na = self._norm_area(a)
        nb = self._norm_area(b)
        if not na or not nb:
            return False
        return na == nb or na in nb or nb in na

    # ---------- Time helpers (naive ISO only) ----------
    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _ha_now(self, ha_base: str, token: str, sensor_entity: str) -> datetime:
        """
        Use HA's reported time exactly as local-naive. Strip tz if present.
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
    def _iso(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    @staticmethod
    def _day_bounds(dt: datetime) -> Tuple[datetime, datetime]:
        start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1) - timedelta(seconds=1)
        return start, end

    @staticmethod
    def _yesterday_bounds(dt: datetime) -> Tuple[datetime, datetime]:
        start = (dt - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1) - timedelta(seconds=1)
        return start, end

    @staticmethod
    def _strip_ordinal(day_str: str) -> str:
        # "14th" -> "14"
        return re.sub(r"(?i)(\d+)(st|nd|rd|th)", r"\1", day_str.strip())

    def _parse_loose_date(self, s: str, assume_year: int) -> Optional[datetime]:
        """
        Accepts:
          - YYYY-MM-DD (ISO date)
          - Oct 14, October 14, Oct 14th, October 14th (with or without year)
          - 2025/10/14
        Returns a naive local date (no tz).
        """
        if not s:
            return None
        s = s.strip()
        # YYYY-MM-DD
        try:
            if len(s) == 10 and s[4] == "-" and s[7] == "-":
                return datetime.fromisoformat(s)
        except Exception:
            pass

        for fmt in ("%Y/%m/%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                pass

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
        """
        Return list of source names (the part after 'tater:automations:events:').
        With your current camera plugin, these are area slugs like 'front_yard'.
        """
        prefix = "tater:automations:events:"
        sources = []
        try:
            for key in redis_client.scan_iter(match=f"{prefix}*", count=500):
                src = key.split(":", maxsplit=3)[-1]
                if src and src not in sources:
                    sources.append(src)
        except Exception as e:
            logger.warning(f"[events_query] source discovery failed: {e}")
        return sources

    # ---------- Fetch ----------
    async def _fetch_one_source(self, base: str, source: str,
                                since: Optional[datetime], until: Optional[datetime],
                                limit: int = 1000) -> List[Dict[str, Any]]:
        params = {"source": source, "limit": int(limit)}
        if since:
            params["since"] = self._iso(since)
        if until:
            params["until"] = self._iso(until)

        url = f"{base}/tater-ha/v1/events/search?{urlencode(params)}"
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                r = await client.get(url)
                r.raise_for_status()
                data = r.json() if r.headers.get("content-type", "").lower().startswith("application/json") else {}
                items = (data or {}).get("items", [])
                # Populate source in case server didn't echo it back
                for it in items:
                    it.setdefault("source", source)
                return items if isinstance(items, list) else []
        except Exception as e:
            logger.error(f"[events_query] fetch failed for source={source}: {e}")
            return []

    async def _fetch_sources_window(self, sources: List[str], start: datetime, end: datetime) -> List[Dict[str, Any]]:
        base = self._automation_base()
        items: List[Dict[str, Any]] = []
        # fetch per source with window to reduce payload
        for src in sources:
            items.extend(await self._fetch_one_source(base, src, start, end, limit=1000))
        return items

    # ---------- Time/Message helpers ----------
    @staticmethod
    def _parse_event_dt(e: Dict[str, Any]) -> Optional[datetime]:
        """Use ha_time only; assume naive ISO."""
        ha_time = (e.get("ha_time") or "").strip()
        if not ha_time:
            return None
        try:
            # must match YYYY-MM-DDTHH:MM:SS (ignore any offset if present by stripping)
            dt = datetime.fromisoformat(ha_time)
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        except Exception:
            return None

    @staticmethod
    def _human_time(dt: datetime) -> str:
        # "3:41 PM" or "03:41 PM" depending on platform
        try:
            return dt.strftime("%-I:%M %p")
        except Exception:
            return dt.strftime("%I:%M %p").lstrip("0") or dt.strftime("%I:%M %p")

    @staticmethod
    def _minutes_ago(now: datetime, dt: datetime) -> Optional[int]:
        try:
            delta = now - dt
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

    def _event_matches(self, e: Dict[str, Any], area_phrase: Optional[str], start: datetime, end: datetime) -> bool:
        if not self._within_window(e, start, end):
            return False

        if not area_phrase:
            return True

        # Prefer matching by SOURCE (per-area storage). Fallback to data.area for legacy events.
        src_area = self._source_to_area(e.get("source", ""))
        if self._areas_match(area_phrase, src_area):
            return True

        ev_area = ((e.get("data") or {}).get("area") or "").strip()
        if ev_area and self._areas_match(area_phrase, ev_area):
            return True

        return False

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
        # Find most recent person-like event for this area today (match by source first)
        latest: Optional[Tuple[datetime, Dict[str, Any]]] = None
        for e in events:
            src_area = self._source_to_area(e.get("source", ""))
            ev_area = ((e.get("data") or {}).get("area") or "").strip()
            if not (self._areas_match(area_name, src_area) or self._areas_match(area_name, ev_area)):
                continue
            msg = (e.get("message") or "") + " " + (e.get("title") or "")
            if not self._contains_person_text(msg):
                continue
            dt = self._parse_event_dt(e)
            if not dt:
                continue
            if (latest is None) or (dt > latest[0]):
                latest = (dt, e)

        friendly = area_name
        if not latest:
            return f"No one has been detected in {friendly} today."

        last_dt = latest[0]
        mins = self._minutes_ago(now, last_dt)
        if mins is not None:
            if mins <= 2:
                return f"Yes — someone was just seen in {friendly}."
            if mins <= 59:
                return f"Yes — someone was seen in {friendly} about {mins} minutes ago."
        return f"Yes — someone was seen in {friendly} at {self._human_time(last_dt)}."

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
        return outs or catalog

    async def _resolve_areas_with_llm(self, user_area: str, sources_catalog: List[str], llm_client) -> Optional[List[str]]:
        """
        Map a user-provided area phrase to one or more actual event storages (sources).
        Catalog is a list of source slugs (e.g., ['front_yard','back_yard','garage']).
        We show the LLM their human-friendly forms and translate back to the slugs.
        """
        if not sources_catalog:
            return None

        # Build friendly ↔ slug maps
        friendly_to_slug: Dict[str, str] = {}
        friendly_catalog: List[str] = []
        for src in sources_catalog:
            friendly = self._source_to_area(src)
            friendly_to_slug[friendly] = src
            friendly_catalog.append(friendly)
        friendly_catalog = sorted(set([f for f in friendly_catalog if f]))

        # Heuristic short-circuit
        if self._looks_like_whole_home(user_area):
            return sorted(set(sources_catalog))
        if self._looks_like_outside(user_area):
            outs = self._filter_outdoor_like(friendly_catalog)
            return [friendly_to_slug[a] for a in outs if a in friendly_to_slug]

        # Few-shot prompt
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
            "- If the phrase names a specific area (e.g., 'front yard', 'porch'), pick that.\n"
            "- If you cannot decide, return an empty list [].\n\n" + examples
        )
        user = json.dumps({
            "user_area_phrase": (user_area or "").strip(),
            "known_areas_catalog": friendly_catalog
        }, ensure_ascii=False)

        try:
            resp = await llm_client.chat(
                messages=[{"role": "system", "content": system},
                          {"role": "user", "content": user}],
                temperature=0.1,
                max_tokens=64,
                timeout_ms=30_000
            )

            raw = (resp.get("message", {}) or {}).get("content", "").strip()
            selected_raw = extract_json(raw)
            selected_friendly = []
            if selected_raw:
                try:
                    parsed = json.loads(selected_raw)
                    if isinstance(parsed, list):
                        selected_friendly = [s for s in parsed if isinstance(s, str)]
                except Exception:
                    selected_friendly = []

            # Map back to slugs & keep only those in catalog
            selected_slugs = []
            valid_set = set(sources_catalog)
            for name in selected_friendly:
                slug = friendly_to_slug.get(name.strip().lower())
                if slug and slug in valid_set:
                    selected_slugs.append(slug)

            # Heuristic expansions if empty
            if not selected_slugs:
                if self._looks_like_whole_home(user_area):
                    selected_slugs = sorted(set(sources_catalog))
                elif self._looks_like_outside(user_area):
                    outs = self._filter_outdoor_like(friendly_catalog)
                    selected_slugs = [friendly_to_slug[a] for a in outs if a in friendly_to_slug]

            return selected_slugs
        except Exception:
            # Fallback heuristics
            if self._looks_like_whole_home(user_area):
                return sorted(set(sources_catalog))
            if self._looks_like_outside(user_area):
                outs = self._filter_outdoor_like(friendly_catalog)
                return [friendly_to_slug[a] for a in outs if a in friendly_to_slug]
            return None

    # ---------- Summarization ----------
    async def _summarize(self, events: List[Dict[str, Any]], area_phrase: Optional[str],
                         label: str, llm_client, user_query: Optional[str] = None) -> str:
        simplified = []
        for e in events:
            # prefer source-as-area; fallback to data.area if missing
            pretty_area = self._source_to_area(e.get("source", "")) or ((e.get("data") or {}).get("area") or "").strip()
            simplified.append({
                "source": (e.get("source") or ""),
                "title": (e.get("title") or "").strip(),
                "message": (e.get("message") or "").strip(),
                "type": (e.get("type") or "").strip(),
                "area": pretty_area,
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
            "user_request": user_query or f"Show events {label} in {area_phrase or 'the home'}",
            "context": {
                "area": area_phrase or "all areas",
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
            return f"No events found for {area_phrase or 'all areas'} {label}."
        lines = [f"Here’s what I found for {area_phrase or 'all areas'} {label}:"]
        for i, e in enumerate(events, 1):
            t = (e.get("title") or "").strip()
            m = (e.get("message") or "").strip()
            typ = (e.get("type") or "").strip()
            ar = self._source_to_area(e.get("source", "")) or ((e.get("data") or {}).get("area") or "").strip()
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

        # Align with HA local time (naive)
        now = self._ha_now(ha["base"], ha["token"], ha["time_sensor"])

        # Timeframe: today|yesterday|last_24h|<date>
        tf_raw = (args.get("timeframe") or "today").strip()
        tf = tf_raw.lower()

        if tf in ("evening", "tonight", "this evening"):
            # 5:00 PM → 11:59:59 PM (or now→EOD if we're already past start)
            start = now.replace(hour=17, minute=0, second=0, microsecond=0)
            end = max(now, start.replace(hour=23, minute=59, second=59, microsecond=0))
            label = "this evening"
        elif tf in ("afternoon", "this afternoon"):
            # 12:00:00 PM → 4:59:59 PM
            start = now.replace(hour=12, minute=0, second=0, microsecond=0)
            end = now.replace(hour=17, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
            label = "this afternoon"
        elif tf in ("this morning", "morning"):
            # 5:00:00 AM → 11:59:59 AM
            start = now.replace(hour=5, minute=0, second=0, microsecond=0)
            end = now.replace(hour=12, minute=0, second=0, microsecond=0) - timedelta(seconds=1)
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
                parsed = parsed.replace(hour=0, minute=0, second=0, microsecond=0)
                start, end = self._day_bounds(parsed)
                label = f"on {parsed.strftime('%b %d, %Y')}"

        # Discover area sources from Redis
        sources_catalog = self._discover_sources()

        # If a specific area was given, try to map it to one or more sources
        area_raw = (args.get("area") or "").strip()
        user_query = (args.get("query") or "").strip()
        area_phrase = area_raw or user_query  # allow natural-language fallback

        resolved_sources: Optional[List[str]] = None
        if area_phrase:
            resolved_sources = await self._resolve_areas_with_llm(area_phrase, sources_catalog, llm_client)

        # Choose sources to fetch
        if resolved_sources is not None and len(resolved_sources) > 0:
            chosen_sources = resolved_sources
        else:
            # No mapping → whole-home query (all sources)
            chosen_sources = sources_catalog

        # Fetch only within window to limit payload
        items = await self._fetch_sources_window(chosen_sources, start, end)

        # Presence intent path (force "today")
        if self._is_presence_query(user_query) and area_phrase:
            p_start, p_end = self._day_bounds(now)
            todays = [e for e in items if self._within_window(e, p_start, p_end)]
            # Present per resolved friendly area (mapped from chosen sources)
            friendly_targets: List[str] = []
            if resolved_sources:
                friendly_targets = [self._source_to_area(src) for src in resolved_sources]
            else:
                # If not resolved, try to interpret the phrase as-is
                friendly_targets = [area_phrase]

            answers = []
            for a in friendly_targets:
                answers.append(self._presence_answer_for_area(now, a, todays))
            return "\n".join(answers)

        # Summarization path
        if resolved_sources:
            # Filter just in case & pass a helpful label
            resolved_friendly = [self._source_to_area(src) for src in resolved_sources]
            # Already fetched only chosen sources & windowed, so just forward
            label_hint = f"{label} (areas: {', '.join(resolved_friendly)})"
            return await self._summarize(items, area_phrase or None, label_hint, llm_client, user_query=user_query)
        else:
            # Whole-home or unresolved phrase
            return await self._summarize(items, area_phrase or None, label, llm_client, user_query=user_query)

    # ---------- Platform shims ----------
    async def handle_webui(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

    async def handle_homeassistant(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

    async def handle_homekit(self, args: Dict[str, Any], llm_client):
        return await self._handle(args, llm_client)

    async def handle_discord(self, message, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self._handle(args, llm_client)

plugin = EventsQueryPlugin()
