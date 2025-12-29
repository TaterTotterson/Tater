# plugins/phone_events_alert.py
import asyncio
import json
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx
import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client

load_dotenv()
logger = logging.getLogger("phone_events_alert")
logger.setLevel(logging.INFO)


class PhoneEventsAlertPlugin(ToolPlugin):
    """
    Automation-only:
    - Pull recent events from the Automations event store (tater:automations:events:*)
    - Generate a brief summary (like events_query_brief)
    - Send it to your phone via Home Assistant Companion App notify service
    - Enforce a configurable cooldown so you don't get spammed
    """

    # ─────────────────────────────────────────
    # Identity (renamed everywhere)
    # ─────────────────────────────────────────
    name = "phone_events_alert"
    plugin_name = "Phone Events Alert"
    plugin_dec = "Send a short household event summary directly to your phone with cooldown control."
    pretty_name = "Phone Events Alert"
    settings_category = "Phone Events Alert"

    description = (
        "Use this when an automation needs to send a brief household event summary to a phone notification. "
        "Useful for motion/doorbell triggers where you want a quick text alert."
    )

    usage = (
        "{\n"
        '  "function": "phone_events_alert",\n'
        '  "arguments": {\n'
        '    "area": "front yard",\n'
        '    "timeframe": "today|yesterday|last_24h",\n'
        '    "query": "brief activity alert"\n'
        "  }\n"
        "}\n"
    )

    platforms = ["automation"]

    required_settings = {
        "HA_BASE_URL": {
            "label": "Home Assistant Base URL",
            "type": "string",
            "default": "http://homeassistant.local:8123",
            "description": "Base URL of your Home Assistant instance.",
        },
        "HA_TOKEN": {
            "label": "Home Assistant Long-Lived Access Token",
            "type": "string",
            "default": "",
            "description": "Create in HA: Profile → Long-Lived Access Tokens.",
        },
        "MOBILE_NOTIFY_SERVICE": {
            "label": "Notify service (ex: notify.mobile_app_taters_iphone)",
            "type": "string",
            "default": "",
            "description": "Your HA Companion App notify service.",
        },
        "DEFAULT_TITLE": {
            "label": "Default notification title",
            "type": "string",
            "default": "Tater Alert",
            "description": "Notification title used if not overridden by arguments.",
        },
        "COOLDOWN_SECONDS": {
            "label": "Cooldown seconds (how often notifications may be sent)",
            "type": "int",
            "default": 120,
            "description": "Minimum seconds between alerts (per plugin).",
        },
        "DEFAULT_PRIORITY": {
            "label": "Default priority",
            "type": "select",
            "default": "critical",
            "options": ["critical", "high", "normal", "low"],
            "description": "How urgent the push should be. Critical is loudest (best-effort).",
        },
        "SEND_IF_NO_ACTIVITY": {
            "label": "Send alert even if no activity was found",
            "type": "boolean",
            "default": False,
            "description": "If false, the plugin will skip sending when the summary is basically 'nothing happened'.",
        },
    }

    waiting_prompt_template = "Sending a quick phone alert summary now. This will be quick."

    # ─────────────────────────────────────────
    # Settings helpers
    # ─────────────────────────────────────────
    def _s(self) -> Dict[str, str]:
        return (
            redis_client.hgetall(f"plugin_settings:{self.settings_category}")
            or redis_client.hgetall(f"plugin_settings: {self.settings_category}")
            or {}
        )

    def _normalize_notify_service(self, raw: str) -> str:
        raw = (raw or "").strip()
        if raw.startswith("notify."):
            raw = raw.split("notify.", 1)[1].strip()
        return raw

    # ─────────────────────────────────────────
    # Cooldown
    # ─────────────────────────────────────────
    def _cooldown_key(self) -> str:
        return f"tater:plugin_cooldown:{self.name}"

    def _cooldown_remaining(self, cooldown_seconds: int) -> int:
        """
        Returns remaining seconds (0 means OK to send).
        """
        try:
            last = redis_client.get(self._cooldown_key())
            last_ts = float(last) if last else 0.0
        except Exception:
            last_ts = 0.0

        now = time.time()
        remaining = int((last_ts + float(cooldown_seconds)) - now)
        return max(0, remaining)

    def _mark_sent_now(self) -> None:
        try:
            redis_client.set(self._cooldown_key(), str(time.time()))
        except Exception:
            pass

    # ─────────────────────────────────────────
    # Automations event store access
    # ─────────────────────────────────────────
    def _automation_base(self) -> str:
        # This plugin runs inside Tater; we talk to the local Automations platform.
        try:
            port = int(redis_client.hget("ha_automations_platform_settings", "bind_port") or 8788)
        except Exception:
            port = 8788
        return f"http://127.0.0.1:{port}"

    def _discover_sources(self) -> List[str]:
        prefix = "tater:automations:events:"
        sources: List[str] = []
        try:
            for key in redis_client.scan_iter(match=f"{prefix}*", count=500):
                src = key.split(":", maxsplit=3)[-1]
                if src and src not in sources:
                    sources.append(src)
        except Exception:
            pass
        return sources

    @staticmethod
    def _slug_area(s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"\s+", "_", s)
        s = re.sub(r"[^a-z0-9_:-]", "", s)
        return s

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

    async def _fetch_one(self, base: str, src: str, start: datetime, end: datetime) -> List[Dict[str, Any]]:
        params = {
            "source": src,
            "since": start.isoformat(),
            "until": end.isoformat(),
            "limit": 500,
        }
        url = f"{base}/tater-ha/v1/events/search?{urlencode(params)}"
        try:
            async with httpx.AsyncClient(timeout=8) as c:
                r = await c.get(url)
                r.raise_for_status()
                items = (r.json() or {}).get("items", [])
                if isinstance(items, list):
                    for i in items:
                        i.setdefault("source", src)
                    return items
        except Exception:
            return []
        return []

    async def _fetch(self, sources: List[str], start: datetime, end: datetime) -> List[Dict[str, Any]]:
        base = self._automation_base()
        events: List[Dict[str, Any]] = []
        for src in sources:
            events.extend(await self._fetch_one(base, src, start, end))
        return events

    # ─────────────────────────────────────────
    # Summary (brief)
    # ─────────────────────────────────────────
    @staticmethod
    def _compact(text: str, limit: int = 220) -> str:
        t = re.sub(r"\s+", " ", text or "").strip()
        if len(t) <= limit:
            return t
        cut = t[:limit]
        # try not to cut mid-word
        if " " in cut[40:]:
            cut = cut[: cut.rfind(" ")]
        return cut.rstrip(". ,;:") + "…"

    async def _summarize_brief(
        self,
        *,
        events: List[Dict[str, Any]],
        area_label: str,
        timeframe_label: str,
        query: str,
        llm_client,
    ) -> str:
        payload = {
            "area": area_label or "all areas",
            "timeframe": timeframe_label,
            "user_query": query or "brief activity alert",
            "events": [
                {
                    "area": (e.get("source", "") or "").replace("_", " "),
                    "title": (e.get("title", "") or "").strip(),
                    "message": (e.get("message", "") or "").strip(),
                }
                for e in (events or [])[:25]
            ],
        }

        system = (
            "Write a VERY short phone notification message summarizing household events.\n"
            "Rules:\n"
            "- Plain text only.\n"
            "- 1–2 short sentences.\n"
            "- Max ~220 characters.\n"
            "- If nothing happened, say so clearly in one short sentence.\n"
            "- Do not include entity IDs or timestamps.\n"
        )

        # Try LLM (best output)
        try:
            if llm_client:
                r = await llm_client.chat(
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                    ],
                    temperature=0.2,
                    max_tokens=120,
                    timeout_ms=25_000,
                )
                text = (r.get("message", {}) or {}).get("content", "") or ""
                text = text.strip()
                if text:
                    return self._compact(text)
        except Exception as e:
            logger.info(f"[phone_events_alert] LLM summary failed; fallback: {e}")

        # Fallback summary
        if not events:
            base = f"No activity detected {timeframe_label}."
            if area_label:
                base = f"No activity detected in {area_label} {timeframe_label}."
            return self._compact(base)

        # take first couple messages
        bits = []
        for e in events[:2]:
            msg = (e.get("message") or e.get("title") or "").strip()
            if msg:
                bits.append(msg)
        head = f"{area_label}: " if area_label else ""
        tail = "; ".join(bits) if bits else "Activity detected."
        return self._compact(f"{head}{tail}")

    @staticmethod
    def _looks_like_no_activity(summary: str) -> bool:
        s = (summary or "").strip().lower()
        return any(
            phrase in s
            for phrase in [
                "no activity",
                "no events",
                "nothing happened",
                "nothing detected",
                "no recent activity",
            ]
        )

    # ─────────────────────────────────────────
    # HA Notify
    # ─────────────────────────────────────────
    def _ha_post_service(self, base_url: str, token: str, domain: str, service: str, payload: dict) -> tuple[int, str]:
        base_url = (base_url or "").rstrip("/")
        url = f"{base_url}/api/services/{domain}/{service}"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        return resp.status_code, resp.text

    @staticmethod
    def _build_notify_data(priority: str) -> Dict[str, Any]:
        p = (priority or "critical").strip().lower()
        if p not in ("critical", "high", "normal", "low"):
            p = "critical"

        # Reasonable defaults; HA Companion behaves differently per OS/device.
        data: Dict[str, Any] = {"ttl": 0}

        if p == "critical":
            data.update(
                {
                    "priority": "high",
                    "push": {"sound": {"name": "default", "critical": 1, "volume": 1.0}},
                    "channel": "alarm_stream",
                }
            )
        elif p == "high":
            data.update(
                {
                    "priority": "high",
                    "push": {"sound": {"name": "default", "critical": 0, "volume": 1.0}},
                }
            )
        elif p == "normal":
            data.update({"priority": "normal"})
        else:  # low
            data.update({"priority": "low"})

        return data

    def _send_phone_notification(
        self,
        *,
        ha_base: str,
        ha_token: str,
        notify_service_raw: str,
        title: str,
        message: str,
        priority: str,
    ) -> Dict[str, Any]:
        if not ha_base or not ha_token:
            return {"ok": False, "error": "HA_BASE_URL / HA_TOKEN not configured."}

        if not notify_service_raw:
            return {"ok": False, "error": "MOBILE_NOTIFY_SERVICE not configured."}

        service = self._normalize_notify_service(notify_service_raw)
        if not service:
            return {"ok": False, "error": "MOBILE_NOTIFY_SERVICE is invalid."}

        payload = {
            "title": (title or "Tater Alert").strip(),
            "message": (message or "").strip(),
            "data": self._build_notify_data(priority),
        }

        status, text = self._ha_post_service(
            base_url=ha_base,
            token=ha_token,
            domain="notify",
            service=service,
            payload=payload,
        )

        if status not in (200, 201):
            logger.error(f"[phone_events_alert] HA notify failed HTTP {status}: {text}")
            return {"ok": False, "error": f"Home Assistant notify failed (HTTP {status})."}

        return {"ok": True, "error": ""}

    # ─────────────────────────────────────────
    # Core
    # ─────────────────────────────────────────
    async def _run(self, args: Dict[str, Any], llm_client) -> Dict[str, Any]:
        s = self._s()

        ha_base = (s.get("HA_BASE_URL") or "").strip()
        ha_token = (s.get("HA_TOKEN") or "").strip()
        notify_service = (s.get("MOBILE_NOTIFY_SERVICE") or "").strip()
        default_title = (s.get("DEFAULT_TITLE") or "Tater Alert").strip()

        # Cooldown
        try:
            cooldown = int(args.get("cooldown_seconds", s.get("COOLDOWN_SECONDS", 120)))
        except Exception:
            cooldown = 120
        cooldown = max(0, min(cooldown, 86_400))

        remaining = self._cooldown_remaining(cooldown)
        if remaining > 0:
            return {
                "ok": True,
                "sent": False,
                "skipped": "cooldown",
                "cooldown_remaining_seconds": remaining,
                "summary": "",
            }

        # Priority (setting default; allow override via args)
        priority = (args.get("priority") or s.get("DEFAULT_PRIORITY") or "critical").strip().lower()

        # Timeframe / area / query
        tf = (args.get("timeframe") or "today").strip().lower()
        area = (args.get("area") or "").strip()
        query = (args.get("query") or "brief activity alert").strip()

        # Determine window
        now = datetime.now()
        if tf == "yesterday":
            start, end = self._yesterday_bounds(now)
            tf_label = "yesterday"
        elif tf in ("last_24h", "last24h", "past_24h"):
            end = now
            start = now - timedelta(hours=24)
            tf_label = "in the last 24 hours"
        else:
            start, end = self._day_bounds(now)
            tf_label = "today"

        # Choose sources
        sources = self._discover_sources()
        chosen_sources = sources

        area_slug = self._slug_area(area) if area else ""
        if area_slug and area_slug in sources:
            chosen_sources = [area_slug]

        events = await self._fetch(chosen_sources, start, end)

        # If user provided area but it wasn't a direct source match, filter best-effort by source text
        if area and area_slug and chosen_sources == sources:
            filtered = [e for e in events if (e.get("source", "") or "").lower() == area_slug]
            if filtered:
                events = filtered

        area_label = area.strip() if area else ""
        summary = await self._summarize_brief(
            events=events,
            area_label=area_label,
            timeframe_label=tf_label,
            query=query,
            llm_client=llm_client,
        )

        # Optional: don't send "no activity" pings
        send_if_no_activity = str(s.get("SEND_IF_NO_ACTIVITY", "false")).strip().lower() in ("1", "true", "yes", "on")
        if (not send_if_no_activity) and self._looks_like_no_activity(summary):
            # Still mark cooldown? I'd say NO — let the next real event through.
            return {"ok": True, "sent": False, "skipped": "no_activity", "summary": summary}

        # Title override
        title = (args.get("title") or default_title).strip() or "Tater Alert"

        # Send
        result = await asyncio.to_thread(
            self._send_phone_notification,
            ha_base=ha_base,
            ha_token=ha_token,
            notify_service_raw=notify_service,
            title=title,
            message=summary,
            priority=priority,
        )

        if result.get("ok"):
            self._mark_sent_now()

        return {"ok": bool(result.get("ok")), "sent": bool(result.get("ok")), "summary": summary, "error": result.get("error", "")}

    # ─────────────────────────────────────────
    # Automation entrypoint
    # ─────────────────────────────────────────
    async def handle_automation(self, args: Dict[str, Any], llm_client):
        """
        Typical usage from Home Assistant Automation:
          Tool: phone_events_alert
          Arguments:
            timeframe: today
            area: front yard
            query: brief activity alert
            # optional overrides:
            # priority: critical|high|normal|low
            # cooldown_seconds: 120
            # title: "Front Yard Alert"
        """
        return await self._run(args or {}, llm_client)


plugin = PhoneEventsAlertPlugin()