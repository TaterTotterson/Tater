# plugins/phone_events_alert.py
import asyncio
import base64
import json
import logging
import re
import time
from typing import Any, Dict, Optional
from urllib.parse import quote

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
    - Cooldown check FIRST (skip everything if still cooling down)
    - Fetch a Home Assistant camera snapshot
    - Describe snapshot with a Vision LLM (OpenAI-compatible)
    - Send the description to your phone using HA notify service
    """

    name = "phone_events_alert"
    plugin_name = "Phone Events Alert"
    plugin_dec = "Capture a camera snapshot, describe it with vision AI, and send it to your phone (with cooldown + priority)."
    pretty_name = "Phone Events Alert"
    settings_category = "Phone Events Alert"

    description = (
        "Use this when an automation needs to send a phone notification describing a camera snapshot. "
        "Great for doorbell/person triggers where you want a quick 'what is happening' alert."
    )

    usage = (
        "{\n"
        '  "function": "phone_events_alert",\n'
        '  "arguments": {\n'
        '    "area": "front yard",\n'
        '    "camera": "camera.front_door_high",\n'
        '    "query": "brief alert description",\n'
        '    "title": "Optional override title",\n'
        '    "priority": "critical|high|normal|low",\n'
        '    "cooldown_seconds": 120\n'
        "  }\n"
        "}\n"
    )

    platforms = ["automation"]

    required_settings = {
        # ---- Home Assistant ----
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

        # ---- Vision LLM ----
        "VISION_API_BASE": {
            "label": "Vision API Base URL",
            "type": "string",
            "default": "http://127.0.0.1:1234",
            "description": "OpenAI-compatible base URL (ex: http://127.0.0.1:1234).",
        },
        "VISION_MODEL": {
            "label": "Vision Model",
            "type": "string",
            "default": "qwen2.5-vl-7b-instruct",
            "description": "OpenAI-compatible vision model name.",
        },
        "VISION_API_KEY": {
            "label": "Vision API Key",
            "type": "string",
            "default": "",
            "description": "Optional; leave blank for local stacks.",
        },

        # ---- Notification behavior ----
        "DEFAULT_TITLE": {
            "label": "Default notification title",
            "type": "string",
            "default": "Phone Events Alert",
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
    }

    waiting_prompt_template = "Sending a quick phone snapshot alert now. This will be quick."

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
    # Cooldown (plugin-wide)
    # ─────────────────────────────────────────
    def _cooldown_key(self) -> str:
        return f"tater:plugin_cooldown:{self.name}"

    def _cooldown_remaining(self, cooldown_seconds: int) -> int:
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
    # HA helpers
    # ─────────────────────────────────────────
    @staticmethod
    def _ha_headers(token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _get_camera_jpeg(self, ha_base: str, token: str, camera_entity: str) -> bytes:
        ha_base = (ha_base or "").rstrip("/")
        url = f"{ha_base}/api/camera_proxy/{quote(camera_entity, safe='')}"
        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=12)
        if r.status_code >= 400:
            raise RuntimeError(f"camera_proxy HTTP {r.status_code}: {r.text[:200]}")
        return r.content

    # ─────────────────────────────────────────
    # Vision describe
    # ─────────────────────────────────────────
    def _vision_describe(self, *, image_bytes: bytes, api_base: str, model: str, api_key: Optional[str], query: str) -> str:
        b64 = base64.b64encode(image_bytes).decode("utf-8")
        data_url = f"data:image/jpeg;base64,{b64}"

        prompt = (
            "Describe what is happening in this camera snapshot for a phone notification. "
            "Be specific (people, packages, vehicles, pets). "
            "If a person is visible, mention clothing and what they appear to be doing. "
            "If nothing notable is happening, clearly say so. "
            "Output 1 short sentence (max ~200 characters). "
            f"User hint: {query or 'brief alert'}"
        )

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a concise vision assistant for smart home alerts."},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                },
            ],
            "temperature": 0.2,
            "max_tokens": 120,
        }

        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        api_base = (api_base or "").rstrip("/")
        url = f"{api_base}/v1/chat/completions"
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=35)
        if r.status_code >= 400:
            raise RuntimeError(f"Vision HTTP {r.status_code}: {r.text[:200]}")

        res = r.json() or {}
        text = ((res.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        return (text or "").strip()

    @staticmethod
    def _compact(text: str, limit: int = 220) -> str:
        t = re.sub(r"\s+", " ", text or "").strip()
        if len(t) <= limit:
            return t
        cut = t[:limit]
        if " " in cut[40:]:
            cut = cut[: cut.rfind(" ")]
        return cut.rstrip(". ,;:") + "…"

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
        """
        Maps to HA Companion notify 'data' fields (best-effort; varies by device/OS/permissions).
        """
        p = (priority or "critical").strip().lower()
        if p not in ("critical", "high", "normal", "low"):
            p = "critical"

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
        else:
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
            "title": (title or "Phone Events Alert").strip(),
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
            logger.error("[phone_events_alert] HA notify failed HTTP %s: %s", status, text[:200])
            return {"ok": False, "error": f"Home Assistant notify failed (HTTP {status})."}

        return {"ok": True, "error": ""}

    # ─────────────────────────────────────────
    # Core
    # ─────────────────────────────────────────
    async def _run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        s = self._s()

        ha_base = (s.get("HA_BASE_URL") or "").strip()
        ha_token = (s.get("HA_TOKEN") or "").strip()
        notify_service = (s.get("MOBILE_NOTIFY_SERVICE") or "").strip()

        vis_base = (s.get("VISION_API_BASE") or "http://127.0.0.1:1234").strip()
        vis_model = (s.get("VISION_MODEL") or "qwen2.5-vl-7b-instruct").strip()
        vis_key = (s.get("VISION_API_KEY") or "").strip() or None

        default_title = (s.get("DEFAULT_TITLE") or "Phone Events Alert").strip()

        area = (args.get("area") or "").strip()
        camera = (args.get("camera") or "").strip()
        query = (args.get("query") or "brief snapshot alert").strip()

        if not camera:
            raise ValueError("Missing 'camera' (example: camera.front_door_high).")

        # Cooldown FIRST
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
            }

        priority = (args.get("priority") or s.get("DEFAULT_PRIORITY") or "critical").strip().lower()

        title = (args.get("title") or default_title).strip() or "Phone Events Alert"
        if area and title == default_title:
            title = f"{area.title()} Alert"

        # Snapshot
        try:
            jpeg = await asyncio.to_thread(self._get_camera_jpeg, ha_base, ha_token, camera)
        except Exception as e:
            logger.exception("[phone_events_alert] Failed to fetch camera snapshot: %s", e)
            # Keep it simple: send a failure notice anyway (still useful to know it fired)
            message = "Motion triggered, but the camera snapshot was not available."
            result = await asyncio.to_thread(
                self._send_phone_notification,
                ha_base=ha_base,
                ha_token=ha_token,
                notify_service_raw=notify_service,
                title=title,
                message=message,
                priority=priority,
            )
            if result.get("ok"):
                self._mark_sent_now()
            return {"ok": bool(result.get("ok")), "sent": bool(result.get("ok")), "summary": message, "error": result.get("error", "")}

        # Vision
        try:
            raw_desc = await asyncio.to_thread(
                self._vision_describe,
                image_bytes=jpeg,
                api_base=vis_base,
                model=vis_model,
                api_key=vis_key,
                query=query,
            )
        except Exception as e:
            logger.exception("[phone_events_alert] Vision analysis failed: %s", e)
            raw_desc = "Motion triggered, but vision analysis failed."

        desc = self._compact(raw_desc)
        if area and area.lower() not in desc.lower():
            desc = self._compact(f"{area.title()}: {desc}")

        # Send
        result = await asyncio.to_thread(
            self._send_phone_notification,
            ha_base=ha_base,
            ha_token=ha_token,
            notify_service_raw=notify_service,
            title=title,
            message=desc,
            priority=priority,
        )

        if result.get("ok"):
            self._mark_sent_now()

        return {"ok": bool(result.get("ok")), "sent": bool(result.get("ok")), "summary": desc, "error": result.get("error", "")}

    async def handle_automation(self, args: Dict[str, Any], llm_client):
        return await self._run(args or {})


plugin = PhoneEventsAlertPlugin()