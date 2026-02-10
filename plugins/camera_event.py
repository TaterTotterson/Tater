# plugins/camera_event.py
import base64
import json
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client
from notify import dispatch_notification
from vision_settings import get_vision_settings as get_shared_vision_settings

load_dotenv()
logger = logging.getLogger("camera_event")
logger.setLevel(logging.INFO)


class CameraEventPlugin(ToolPlugin):
    """
    Automation-only camera event handler.

    Flow:
    - Enforce per-camera cooldown.
    - Fetch a Home Assistant camera snapshot.
    - Describe the image with a vision model.
    - Record the event to the Automations event endpoint.
    - Optionally send Home Assistant notifications through the core notifier
      (phone push and/or persistent notifications).
    """

    name = "camera_event"
    plugin_name = "Camera Event"
    version = "1.2.0"
    min_tater_version = "50"
    description = (
        "Capture a Home Assistant camera snapshot, describe it with vision AI, log the event, "
        "and optionally send Home Assistant phone/persistent notifications."
    )
    plugin_dec = (
        "Capture a camera snapshot, describe it, store an event, and optionally notify via Home Assistant Notifier."
    )
    usage = '{"function":"camera_event","arguments":{"area":"front yard | back yard | garage | ...","camera":"camera.front_door_high","query":"optional hint for the vision model","title":"optional notification title override","priority":"critical|high|normal|low","cooldown_seconds":30,"notification_cooldown_seconds":0,"ignore_vehicles":false,"send_phone_alerts":false,"persistent_notifications":false,"api_notification":true}}'

    platforms = ["automation"]
    settings_category = "Camera Event"
    required_settings = {
        "VISION_API_BASE": {
            "label": "Vision API Base URL",
            "type": "string",
            "default": "http://127.0.0.1:1234",
            "description": "OpenAI-compatible base (for example http://127.0.0.1:1234).",
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
        "DEFAULT_COOLDOWN_SECONDS": {
            "label": "Per-Camera Cooldown (seconds)",
            "type": "number",
            "default": 30,
            "description": "Skip processing if the same camera fired recently.",
        },
        "IGNORE_VEHICLES_DEFAULT": {
            "label": "Ignore vehicles by default",
            "type": "select",
            "default": "false",
            "options": ["true", "false"],
            "description": "If enabled, vehicle/car details are omitted from the vision description.",
        },
        "ENABLE_PHONE_ALERTS": {
            "label": "Enable phone alerts",
            "type": "checkbox",
            "default": False,
            "description": "If enabled, send mobile alerts through Home Assistant Notifier.",
        },
        "ENABLE_PERSISTENT_NOTIFICATIONS": {
            "label": "Enable persistent notifications",
            "type": "checkbox",
            "default": False,
            "description": "If enabled, send Home Assistant persistent notifications.",
        },
        "ENABLE_HA_API_NOTIFICATION": {
            "label": "Enable Home Assistant API notifications",
            "type": "checkbox",
            "default": True,
            "description": "If enabled, also send to the Home Assistant platform notification endpoint.",
        },
        "DEFAULT_NOTIFICATION_TITLE": {
            "label": "Default notification title",
            "type": "string",
            "default": "Camera Event",
            "description": "Notification title used unless overridden in arguments.",
        },
        "DEFAULT_PRIORITY": {
            "label": "Default priority",
            "type": "select",
            "default": "high",
            "options": ["critical", "high", "normal", "low"],
            "description": "critical/high map to high; normal/low map to normal for notifier delivery.",
        },
        "NOTIFICATION_COOLDOWN_SECONDS": {
            "label": "Notification cooldown (seconds)",
            "type": "number",
            "default": 0,
            "description": "Minimum seconds between Home Assistant notifier sends for the same camera. 0 disables this cooldown.",
        },
        "MOBILE_NOTIFY_SERVICE": {
            "label": "Phone notifier #1 (optional)",
            "type": "string",
            "default": "",
            "description": "Example: notify.mobile_app_my_iphone",
        },
        "MOBILE_NOTIFY_SERVICE_2": {
            "label": "Phone notifier #2 (optional)",
            "type": "string",
            "default": "",
            "description": "Optional additional Home Assistant mobile notify service.",
        },
        "MOBILE_NOTIFY_SERVICE_3": {
            "label": "Phone notifier #3 (optional)",
            "type": "string",
            "default": "",
            "description": "Optional additional Home Assistant mobile notify service.",
        },
        "MOBILE_NOTIFY_SERVICE_4": {
            "label": "Phone notifier #4 (optional)",
            "type": "string",
            "default": "",
            "description": "Optional additional Home Assistant mobile notify service.",
        },
        "MOBILE_NOTIFY_SERVICE_5": {
            "label": "Phone notifier #5 (optional)",
            "type": "string",
            "default": "",
            "description": "Optional additional Home Assistant mobile notify service.",
        },
    }

    waiting_prompt_template = "Checking the camera event and posting updates now."

    def _get_settings(self) -> Dict[str, str]:
        settings = (
            redis_client.hgetall(f"plugin_settings:{self.settings_category}")
            or redis_client.hgetall(f"plugin_settings: {self.settings_category}")
            or {}
        )
        legacy = (
            redis_client.hgetall("plugin_settings:Phone Events Alert")
            or redis_client.hgetall("plugin_settings: Phone Events Alert")
            or {}
        )
        if not legacy:
            return settings

        # Smooth migration from phone_events_alert without forcing users to re-enter values.
        mapping = {
            "VISION_API_BASE": "VISION_API_BASE",
            "VISION_MODEL": "VISION_MODEL",
            "VISION_API_KEY": "VISION_API_KEY",
            "MOBILE_NOTIFY_SERVICE": "MOBILE_NOTIFY_SERVICE",
            "MOBILE_NOTIFY_SERVICE_2": "MOBILE_NOTIFY_SERVICE_2",
            "MOBILE_NOTIFY_SERVICE_3": "MOBILE_NOTIFY_SERVICE_3",
            "MOBILE_NOTIFY_SERVICE_4": "MOBILE_NOTIFY_SERVICE_4",
            "MOBILE_NOTIFY_SERVICE_5": "MOBILE_NOTIFY_SERVICE_5",
            "COOLDOWN_SECONDS": "DEFAULT_COOLDOWN_SECONDS",
            "IGNORE_CARS_DEFAULT": "IGNORE_VEHICLES_DEFAULT",
            "DEFAULT_TITLE": "DEFAULT_NOTIFICATION_TITLE",
            "DEFAULT_PRIORITY": "DEFAULT_PRIORITY",
        }
        merged = dict(settings)
        for source_key, target_key in mapping.items():
            if str(merged.get(target_key) or "").strip():
                continue
            legacy_val = legacy.get(source_key)
            if legacy_val is None:
                continue
            merged[target_key] = legacy_val
        return merged

    @staticmethod
    def _boolish(value: Any, default: bool = False) -> bool:
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on", "enabled"}:
            return True
        if text in {"0", "false", "no", "n", "off", "disabled"}:
            return False
        return bool(default)

    @staticmethod
    def _normalize_notify_service(raw: str) -> str:
        value = (raw or "").strip()
        if not value:
            return ""
        if value.lower().startswith("notify."):
            return value.split(".", 1)[1].strip()
        if "." in value:
            left, right = value.split(".", 1)
            if left.strip().lower() == "notify":
                return right.strip()
        return value

    def _get_phone_services(self, settings: Dict[str, str]) -> List[str]:
        keys = [
            "MOBILE_NOTIFY_SERVICE",
            "MOBILE_NOTIFY_SERVICE_2",
            "MOBILE_NOTIFY_SERVICE_3",
            "MOBILE_NOTIFY_SERVICE_4",
            "MOBILE_NOTIFY_SERVICE_5",
        ]
        services: List[str] = []
        seen = set()
        for key in keys:
            normalized = self._normalize_notify_service(settings.get(key, ""))
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            services.append(normalized)
        return services

    @staticmethod
    def _normalize_priority(raw: Any) -> str:
        text = str(raw or "high").strip().lower()
        if text in {"critical", "high"}:
            return "high"
        if text in {"normal", "low"}:
            return "normal"
        return "high"

    def _ha(self) -> Dict[str, str]:
        settings = redis_client.hgetall("homeassistant_settings") or {}
        base = (settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        token = (settings.get("HA_TOKEN") or "").strip()
        if not token:
            raise ValueError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )
        return {"base": base, "token": token}

    def _vision(self, settings: Dict[str, str]) -> Dict[str, Optional[str]]:
        return get_shared_vision_settings(
            default_api_base="http://127.0.0.1:1234",
            default_model="qwen2.5-vl-7b-instruct",
        )

    def _automation_base_url(self) -> str:
        try:
            raw_port = redis_client.hget("ha_automations_platform_settings", "bind_port")
            port = int(raw_port) if raw_port is not None else 8788
        except Exception:
            port = 8788
        return f"http://localhost:{port}"

    @staticmethod
    def _slug(text: str) -> str:
        out = (text or "").strip().lower()
        out = re.sub(r"\s+", "_", out)
        out = re.sub(r"[^a-z0-9_:-]", "", out)
        return out or "unknown"

    @staticmethod
    def _format_iso_naive(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    def _ha_now(self) -> datetime:
        return datetime.now()

    def _cooldown_key(self, camera_entity: str) -> str:
        return f"tater:camera_event:last_ts:{camera_entity}"

    def _notification_cooldown_key(self, camera_entity: str) -> str:
        return f"tater:camera_event:last_notify_ts:{camera_entity}"

    def _within_cooldown(self, camera_entity: str, cooldown_seconds: int) -> bool:
        try:
            last = redis_client.get(self._cooldown_key(camera_entity))
            if not last:
                return False
            return (int(time.time()) - int(last)) < max(0, int(cooldown_seconds))
        except Exception:
            return False

    def _within_notification_cooldown(self, camera_entity: str, cooldown_seconds: int) -> bool:
        try:
            last = redis_client.get(self._notification_cooldown_key(camera_entity))
            if not last:
                return False
            return (int(time.time()) - int(last)) < max(0, int(cooldown_seconds))
        except Exception:
            return False

    def _mark_fired(self, camera_entity: str) -> None:
        try:
            redis_client.set(self._cooldown_key(camera_entity), str(int(time.time())))
        except Exception:
            logger.warning("[camera_event] failed to set cooldown key for %s", camera_entity)

    def _mark_notification_fired(self, camera_entity: str) -> None:
        try:
            redis_client.set(self._notification_cooldown_key(camera_entity), str(int(time.time())))
        except Exception:
            logger.warning("[camera_event] failed to set notification cooldown key for %s", camera_entity)

    def _get_camera_jpeg(self, ha_base: str, token: str, camera_entity: str) -> bytes:
        url = f"{ha_base}/api/camera_proxy/{quote(camera_entity, safe='')}"
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=12)
        if resp.status_code >= 400:
            raise RuntimeError(f"camera_proxy HTTP {resp.status_code}: {resp.text[:200]}")
        return resp.content

    def _vision_describe(
        self,
        *,
        image_bytes: bytes,
        api_base: str,
        model: str,
        api_key: Optional[str],
        query: str,
        ignore_vehicles: bool,
    ) -> str:
        b64 = base64.b64encode(image_bytes).decode("utf-8")
        data_url = f"data:image/jpeg;base64,{b64}"

        prompt = (
            "Write exactly one short sentence describing what is happening in this camera snapshot. "
            "Be specific and only mention visible details. "
            "If a person is visible, mention clothing and what they are doing. "
            "If a package is visible, say so. "
            "If nothing notable is happening, output exactly: 'Nothing notable.' "
            f"User hint: {query or 'brief camera alert'}"
        )
        if ignore_vehicles:
            prompt += " Do not mention vehicles or cars."

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a concise vision assistant for smart home camera alerts."},
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

        url = f"{api_base}/v1/chat/completions"
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=35)
        if resp.status_code >= 400:
            raise RuntimeError(f"Vision HTTP {resp.status_code}: {resp.text[:200]}")

        body = resp.json() or {}
        text = ((body.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        return (text or "").strip()

    @staticmethod
    def _compact(text: str, limit: int = 220) -> str:
        out = re.sub(r"\s+", " ", text or "").strip()
        if len(out) <= limit:
            return out
        clipped = out[:limit]
        if " " in clipped[40:]:
            clipped = clipped[: clipped.rfind(" ")]
        return clipped.rstrip(". ,;:") + "…"

    def _post_event(
        self,
        *,
        source: str,
        title: str,
        message: str,
        entity_id: str,
        area: Optional[str],
        ha_time: str,
    ) -> None:
        url = f"{self._automation_base_url()}/tater-ha/v1/events/add"
        payload = {
            "source": source,
            "title": title,
            "type": "camera_motion",
            "message": message,
            "entity_id": entity_id,
            "level": "info",
            "ha_time": ha_time,
            "data": {"area": (area or "").strip()},
        }
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code >= 400:
                logger.warning("[camera_event] events/add failed %s: %s", resp.status_code, resp.text[:200])
        except Exception as exc:
            logger.warning("[camera_event] events/add error: %s", exc)

    async def _notify_via_homeassistant_notifier(
        self,
        *,
        title: str,
        message: str,
        priority: str,
        send_phone_alerts: bool,
        persistent_notifications: bool,
        api_notification: bool,
        phone_services: List[str],
        area: str,
        camera: str,
    ) -> Dict[str, Any]:
        if not (send_phone_alerts or persistent_notifications or api_notification):
            return {"ok": True, "sent_count": 0, "skipped": "notifications_disabled"}

        dispatch_targets: List[Dict[str, Any]] = []
        if send_phone_alerts and phone_services:
            for idx, service in enumerate(phone_services):
                targets: Dict[str, Any] = {"device_service": service}
                if persistent_notifications and idx == 0:
                    targets["persistent"] = True
                dispatch_targets.append(targets)
        elif send_phone_alerts:
            targets = {}
            if persistent_notifications:
                targets["persistent"] = True
            dispatch_targets.append(targets)
        elif persistent_notifications:
            dispatch_targets.append({"persistent": True})
        elif api_notification:
            dispatch_targets.append({})

        if not dispatch_targets:
            return {
                "ok": True,
                "sent_count": 0,
                "skipped": "no_dispatch_targets",
            }

        origin = {
            "platform": "automation",
            "scope": "camera_event",
            "area": area,
            "camera": camera,
        }
        meta = {"priority": priority}

        sent_count = 0
        errors: List[str] = []

        for targets in dispatch_targets:
            targets["api_notification"] = bool(api_notification)
            try:
                result = await dispatch_notification(
                    platform="homeassistant",
                    title=title,
                    content=message,
                    targets=targets,
                    origin=origin,
                    meta=meta,
                )

                result_text = str(result or "").strip()
                if result_text.lower().startswith("queued notification"):
                    sent_count += 1
                else:
                    errors.append(result_text or "homeassistant notifier returned an empty response")
            except Exception as exc:
                errors.append(str(exc))

        return {
            "ok": sent_count > 0,
            "sent_count": sent_count,
            "errors": errors,
        }

    async def handle_automation(self, args: Dict[str, Any], llm_client) -> Any:
        area = (args.get("area") or "").strip()
        camera = (args.get("camera") or "").strip()
        query = (args.get("query") or "brief camera alert").strip()

        if not area:
            raise ValueError("Missing 'area' (for example: 'front yard').")
        if not camera:
            raise ValueError("Missing 'camera' (for example: 'camera.front_door_high').")

        settings = self._get_settings()
        ha = self._ha()
        vis = self._vision(settings)

        try:
            cooldown_seconds = int(args.get("cooldown_seconds", settings.get("DEFAULT_COOLDOWN_SECONDS", 30)))
        except Exception:
            cooldown_seconds = 30
        cooldown_seconds = max(0, min(cooldown_seconds, 86_400))

        try:
            notification_cooldown_seconds = int(
                args.get(
                    "notification_cooldown_seconds",
                    settings.get("NOTIFICATION_COOLDOWN_SECONDS", 0),
                )
            )
        except Exception:
            notification_cooldown_seconds = 0
        notification_cooldown_seconds = max(0, min(notification_cooldown_seconds, 86_400))

        ignore_default = self._boolish(settings.get("IGNORE_VEHICLES_DEFAULT"), False)
        if "ignore_vehicles" in args:
            ignore_vehicles = self._boolish(args.get("ignore_vehicles"), ignore_default)
        elif "ignore_cars" in args:
            ignore_vehicles = self._boolish(args.get("ignore_cars"), ignore_default)
        else:
            ignore_vehicles = ignore_default

        send_phone_default = self._boolish(settings.get("ENABLE_PHONE_ALERTS"), False)
        if "ENABLE_PHONE_ALERTS" not in settings and self._get_phone_services(settings):
            send_phone_default = True
        send_phone_alerts = self._boolish(args.get("send_phone_alerts"), send_phone_default)

        persistent_default = self._boolish(settings.get("ENABLE_PERSISTENT_NOTIFICATIONS"), False)
        if "persistent_notifications" in args:
            persistent_notifications = self._boolish(args.get("persistent_notifications"), persistent_default)
        elif "persistent" in args:
            persistent_notifications = self._boolish(args.get("persistent"), persistent_default)
        else:
            persistent_notifications = persistent_default

        api_notification_default = self._boolish(settings.get("ENABLE_HA_API_NOTIFICATION"), True)
        if "api_notification" in args:
            api_notification = self._boolish(args.get("api_notification"), api_notification_default)
        else:
            api_notification = api_notification_default

        default_title = (settings.get("DEFAULT_NOTIFICATION_TITLE") or "Camera Event").strip()
        notification_title = (args.get("title") or default_title or f"{area.title()} Alert").strip()

        priority_input = args.get("priority") or settings.get("DEFAULT_PRIORITY") or "high"
        notification_priority = self._normalize_priority(priority_input)

        phone_services = self._get_phone_services(settings)

        if self._within_cooldown(camera, cooldown_seconds):
            logger.info("[camera_event] Skipping due to cooldown for %s (%ss)", camera, cooldown_seconds)
            return {
                "ok": True,
                "skipped": "cooldown",
                "camera": camera,
                "area": area,
                "cooldown_seconds": cooldown_seconds,
            }

        now_iso = self._format_iso_naive(self._ha_now())

        description = ""
        used_generic_snapshot = False
        try:
            jpeg = self._get_camera_jpeg(ha["base"], ha["token"], camera)
            description = self._vision_describe(
                image_bytes=jpeg,
                api_base=vis["api_base"] or "",
                model=vis["model"] or "",
                api_key=vis["api_key"],
                query=query,
                ignore_vehicles=ignore_vehicles,
            )
        except Exception:
            logger.exception("[camera_event] Snapshot or vision analysis failed")
            description = "Motion event detected, but camera analysis was unavailable."
            used_generic_snapshot = True

        description = self._compact(description) or "Motion event detected."

        event_title = f"{area.title()} motion"
        self._post_event(
            source=self._slug(area),
            title=event_title,
            message=description,
            entity_id=camera,
            area=area,
            ha_time=now_iso,
        )

        self._mark_fired(camera)

        notify_result: Dict[str, Any] = {"ok": True, "sent_count": 0, "skipped": "notifications_disabled"}
        notify_requested = bool(send_phone_alerts or persistent_notifications or api_notification)
        if description.strip().lower().startswith("nothing notable"):
            notify_result = {"ok": True, "sent_count": 0, "skipped": "nothing_notable"}
        elif (
            notify_requested
            and notification_cooldown_seconds > 0
            and self._within_notification_cooldown(camera, notification_cooldown_seconds)
        ):
            notify_result = {
                "ok": True,
                "sent_count": 0,
                "skipped": "notification_cooldown",
                "notification_cooldown_seconds": notification_cooldown_seconds,
            }
        else:
            notify_result = await self._notify_via_homeassistant_notifier(
                title=notification_title,
                message=description,
                priority=notification_priority,
                send_phone_alerts=send_phone_alerts,
                persistent_notifications=persistent_notifications,
                api_notification=api_notification,
                phone_services=phone_services,
                area=area,
                camera=camera,
            )
            if bool((notify_result or {}).get("ok")) and int((notify_result or {}).get("sent_count") or 0) > 0:
                self._mark_notification_fired(camera)

        return {
            "ok": True,
            "stored": True,
            "camera": camera,
            "area": area,
            "cooldown_seconds": cooldown_seconds,
            "notification_cooldown_seconds": notification_cooldown_seconds,
            "ignore_vehicles": ignore_vehicles,
            "summary": description,
            "vision_fallback_used": used_generic_snapshot,
            "notification": notify_result,
        }


plugin = CameraEventPlugin()
