# plugins/phone_events_alert.py
import asyncio
import base64
import json
import logging
import re
import time
from typing import Any, Dict, Optional, List
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
    - Send the description to your phone(s) using HA notify service(s)

    Updates:
    - Optional per-camera cooldown (toggle in settings)
    - Optional ignore_cars / ignore vehicles (per-call arg + optional default setting)
    - Delivery detection instructions (UPS/FedEx/USPS/Amazon/DHL)
    - Supports multiple notify services (up to 5) in settings
    - Prompt explicitly describes animals/pets including color and what they are doing
    - Skip sending notification if model responds with "Nothing notable"
    """

    name = "phone_events_alert"
    plugin_name = "Phone Events Alert"
    version = "1.0.1"
    min_tater_version = "50"
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
        '    "cooldown_seconds": 120,\n'
        '    "ignore_cars": true\n'
        "  }\n"
        "}\n"
    )

    platforms = ["automation"]

    required_settings = {
        # Primary + additional notify services (up to 5)
        "MOBILE_NOTIFY_SERVICE": {
            "label": "Notify service #1 (ex: notify.mobile_app_taters_iphone)",
            "type": "string",
            "default": "",
            "description": "Your HA Companion App notify service.",
        },
        "MOBILE_NOTIFY_SERVICE_2": {
            "label": "Notify service #2 (optional)",
            "type": "string",
            "default": "",
            "description": "Optional additional notify service.",
        },
        "MOBILE_NOTIFY_SERVICE_3": {
            "label": "Notify service #3 (optional)",
            "type": "string",
            "default": "",
            "description": "Optional additional notify service.",
        },
        "MOBILE_NOTIFY_SERVICE_4": {
            "label": "Notify service #4 (optional)",
            "type": "string",
            "default": "",
            "description": "Optional additional notify service.",
        },
        "MOBILE_NOTIFY_SERVICE_5": {
            "label": "Notify service #5 (optional)",
            "type": "string",
            "default": "",
            "description": "Optional additional notify service.",
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
            "description": "Minimum seconds between alerts.",
        },
        "PER_CAMERA_COOLDOWN": {
            "label": "Cooldown is per camera (instead of global)",
            "type": "select",
            "default": "false",
            "options": ["true", "false"],
            "description": "If enabled, each camera has its own cooldown timer.",
        },
        "IGNORE_CARS_DEFAULT": {
            "label": "Ignore vehicles by default",
            "type": "select",
            "default": "false",
            "options": ["true", "false"],
            "description": "If enabled, the vision prompt will not mention vehicles/cars at all.",
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

    def _get_notify_services(self, s: Dict[str, str]) -> List[str]:
        keys = [
            "MOBILE_NOTIFY_SERVICE",
            "MOBILE_NOTIFY_SERVICE_2",
            "MOBILE_NOTIFY_SERVICE_3",
            "MOBILE_NOTIFY_SERVICE_4",
            "MOBILE_NOTIFY_SERVICE_5",
        ]
        out: List[str] = []
        seen = set()

        for k in keys:
            raw = (s.get(k) or "").strip()
            if not raw:
                continue
            svc = self._normalize_notify_service(raw)
            if not svc:
                continue
            if svc in seen:
                continue
            seen.add(svc)
            out.append(svc)

        return out

    @staticmethod
    def _truthy(v: Any) -> bool:
        if isinstance(v, bool):
            return v
        if v is None:
            return False
        s = str(v).strip().lower()
        return s in ("1", "true", "yes", "y", "on", "enabled")

    @staticmethod
    def _safe_key_part(v: str) -> str:
        t = (v or "").strip().lower()
        t = re.sub(r"[^a-z0-9_]+", "_", t.replace(".", "_"))
        t = re.sub(r"_+", "_", t).strip("_")
        return t or "unknown"

    # ─────────────────────────────────────────
    # Cooldown (global or per camera)
    # ─────────────────────────────────────────
    def _cooldown_key(self, camera_entity: Optional[str] = None, per_camera: bool = False) -> str:
        if per_camera:
            cam = self._safe_key_part(camera_entity or "")
            return f"tater:plugin_cooldown:{self.name}:camera:{cam}"
        return f"tater:plugin_cooldown:{self.name}"

    def _cooldown_remaining(self, cooldown_seconds: int, *, camera_entity: Optional[str] = None, per_camera: bool = False) -> int:
        try:
            last = redis_client.get(self._cooldown_key(camera_entity, per_camera))
            last_ts = float(last) if last else 0.0
        except Exception:
            last_ts = 0.0

        now = time.time()
        remaining = int((last_ts + float(cooldown_seconds)) - now)
        return max(0, remaining)

    def _mark_sent_now(self, *, camera_entity: Optional[str] = None, per_camera: bool = False) -> None:
        try:
            redis_client.set(self._cooldown_key(camera_entity, per_camera), str(time.time()))
        except Exception:
            pass

    # ─────────────────────────────────────────
    # HA helpers
    # ─────────────────────────────────────────
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
    def _vision_describe(
        self,
        *,
        image_bytes: bytes,
        api_base: str,
        model: str,
        api_key: Optional[str],
        query: str,
        ignore_cars: bool,
    ) -> str:
        b64 = base64.b64encode(image_bytes).decode("utf-8")
        data_url = f"data:image/jpeg;base64,{b64}"

        prompt = (
            "Write exactly ONE short sentence for a phone notification describing what is happening in this snapshot. "
            "Be specific and ONLY mention things you can actually see. "
            "DO NOT mention what is NOT present (no 'no pets', 'no delivery', etc). "
            "DO NOT add disclaimers, guesses, or filler. "
            "If a person is visible, mention clothing and what they are doing. "
            "If an animal/pet is visible (dog, cat, etc), mention what it is, its color (black, brown, white, gray, etc), "
            "and what it is doing (running, sniffing, sitting at the door, etc). "
            "If a package is visible, say so. "
            "If you can identify a delivery service by uniform, logo, or clear branding, be explicit: "
            "UPS, FedEx, USPS, Amazon, DHL, etc. "
            "Prefer phrasing like: 'UPS is delivering a package' or 'FedEx driver at the door.' "
            "If nothing notable is happening, output exactly: 'Nothing notable.' "
            f"User hint: {query or 'brief alert'}"
        )

        if ignore_cars:
            prompt += " Do NOT mention, describe, or reference vehicles or cars under any circumstances."

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
        notify_service: str,
        title: str,
        message: str,
        priority: str,
    ) -> Dict[str, Any]:
        if not ha_token:
            return {
                "ok": False,
                "error": (
                    "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                    "and add a Long-Lived Access Token."
                ),
            }
        if not notify_service:
            return {"ok": False, "error": "No notify service configured."}

        payload = {
            "title": (title or "Phone Events Alert").strip(),
            "message": (message or "").strip(),
            "data": self._build_notify_data(priority),
        }

        status, text = self._ha_post_service(
            base_url=ha_base,
            token=ha_token,
            domain="notify",
            service=notify_service,
            payload=payload,
        )

        if status not in (200, 201):
            logger.error("[phone_events_alert] HA notify failed (%s) HTTP %s: %s", notify_service, status, text[:200])
            return {"ok": False, "error": f"Home Assistant notify failed (HTTP {status}) for {notify_service}."}

        return {"ok": True, "error": ""}

    async def _send_to_all_devices(
        self,
        *,
        ha_base: str,
        ha_token: str,
        services: List[str],
        title: str,
        message: str,
        priority: str,
    ) -> Dict[str, Any]:
        if not services:
            return {"ok": False, "sent_count": 0, "errors": ["No notify services configured."]}

        results = await asyncio.gather(
            *[
                asyncio.to_thread(
                    self._send_phone_notification,
                    ha_base=ha_base,
                    ha_token=ha_token,
                    notify_service=svc,
                    title=title,
                    message=message,
                    priority=priority,
                )
                for svc in services
            ],
            return_exceptions=True,
        )

        sent = 0
        errors: List[str] = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                errors.append(f"{services[i]}: {str(r)}")
                continue
            if r.get("ok"):
                sent += 1
            else:
                errors.append(r.get("error") or f"{services[i]}: failed")

        return {"ok": sent > 0, "sent_count": sent, "errors": errors}

    # ─────────────────────────────────────────
    # Core
    # ─────────────────────────────────────────
    async def _run(self, args: Dict[str, Any]) -> Dict[str, Any]:
        s = self._s()

        ha_settings = redis_client.hgetall("homeassistant_settings") or {}
        ha_base = (ha_settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        ha_token = (ha_settings.get("HA_TOKEN") or "").strip()
        notify_services = self._get_notify_services(s)

        vis_base = (s.get("VISION_API_BASE") or "http://127.0.0.1:1234").strip()
        vis_model = (s.get("VISION_MODEL") or "qwen2.5-vl-7b-instruct").strip()
        vis_key = (s.get("VISION_API_KEY") or "").strip() or None

        default_title = (s.get("DEFAULT_TITLE") or "Phone Events Alert").strip()

        area = (args.get("area") or "").strip()
        camera = (args.get("camera") or "").strip()
        query = (args.get("query") or "brief snapshot alert").strip()

        if not camera:
            raise ValueError("Missing 'camera' (example: camera.front_door_high).")
        if not ha_token:
            return {
                "ok": False,
                "error": (
                    "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                    "and add a Long-Lived Access Token."
                ),
            }

        # Cooldown FIRST
        try:
            cooldown = int(args.get("cooldown_seconds", s.get("COOLDOWN_SECONDS", 120)))
        except Exception:
            cooldown = 120
        cooldown = max(0, min(cooldown, 86_400))

        per_cam = self._truthy(s.get("PER_CAMERA_COOLDOWN", "false"))

        remaining = self._cooldown_remaining(cooldown, camera_entity=camera, per_camera=per_cam)
        if remaining > 0:
            return {
                "ok": True,
                "sent": False,
                "skipped": "cooldown",
                "cooldown_scope": "camera" if per_cam else "global",
                "cooldown_remaining_seconds": remaining,
            }

        # Ignore vehicles (default setting, overridable per-call via args.ignore_cars)
        ignore_default = self._truthy(s.get("IGNORE_CARS_DEFAULT", "false"))
        ignore_arg = args.get("ignore_cars", None)
        ignore_cars = ignore_default if ignore_arg is None else self._truthy(ignore_arg)

        priority = (args.get("priority") or s.get("DEFAULT_PRIORITY") or "critical").strip().lower()

        title = (args.get("title") or default_title).strip() or "Phone Events Alert"
        if area and title == default_title:
            title = f"{area.title()} Alert"

        # Snapshot
        try:
            jpeg = await asyncio.to_thread(self._get_camera_jpeg, ha_base, ha_token, camera)
        except Exception as e:
            logger.exception("[phone_events_alert] Failed to fetch camera snapshot: %s", e)
            message = "Motion triggered, but the camera snapshot was not available."
            result = await self._send_to_all_devices(
                ha_base=ha_base,
                ha_token=ha_token,
                services=notify_services,
                title=title,
                message=message,
                priority=priority,
            )
            if result.get("ok"):
                self._mark_sent_now(camera_entity=camera, per_camera=per_cam)
            return {
                "ok": bool(result.get("ok")),
                "sent": bool(result.get("ok")),
                "sent_count": int(result.get("sent_count", 0)),
                "summary": message,
                "errors": result.get("errors", []),
            }

        # Vision
        try:
            raw_desc = await asyncio.to_thread(
                self._vision_describe,
                image_bytes=jpeg,
                api_base=vis_base,
                model=vis_model,
                api_key=vis_key,
                query=query,
                ignore_cars=ignore_cars,
            )
        except Exception as e:
            logger.exception("[phone_events_alert] Vision analysis failed: %s", e)
            raw_desc = "Motion triggered."

        desc = self._compact(raw_desc)
        if not desc:
            desc = "Motion detected."

        # NEW: if model says "Nothing notable...", skip sending and do NOT mark cooldown
        if desc.strip().lower().startswith("nothing notable"):
            logger.info("[phone_events_alert] Skipping notification (nothing notable)")
            return {
                "ok": True,
                "sent": False,
                "skipped": "nothing_notable",
                "summary": desc,
                "ignore_cars": bool(ignore_cars),
            }

        if area and area.lower() not in desc.lower():
            desc = self._compact(f"{area.title()}: {desc}")

        # Send to all devices
        result = await self._send_to_all_devices(
            ha_base=ha_base,
            ha_token=ha_token,
            services=notify_services,
            title=title,
            message=desc,
            priority=priority,
        )

        if result.get("ok"):
            self._mark_sent_now(camera_entity=camera, per_camera=per_cam)

        return {
            "ok": bool(result.get("ok")),
            "sent": bool(result.get("ok")),
            "sent_count": int(result.get("sent_count", 0)),
            "summary": desc,
            "ignore_cars": bool(ignore_cars),
            "errors": result.get("errors", []),
        }

    async def handle_automation(self, args: Dict[str, Any], llm_client):
        return await self._run(args or {})


plugin = PhoneEventsAlertPlugin()
