import logging
from typing import Any, Dict

import requests

from plugin_base import ToolPlugin
from helpers import redis_client
from notify_queue import resolve_targets

logger = logging.getLogger("notify_homeassistant")
logger.setLevel(logging.INFO)


def _ha_settings() -> tuple[str, str]:
    settings = redis_client.hgetall("homeassistant_settings") or {}
    base = (settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
    token = (settings.get("HA_TOKEN") or "").strip()
    return base, token


def _ha_call_service(domain: str, service: str, data: dict) -> None:
    base, token = _ha_settings()
    if not token:
        raise RuntimeError("HA_TOKEN missing in Home Assistant Settings.")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{base}/api/services/{domain}/{service}"
    resp = requests.post(url, headers=headers, json=data, timeout=10)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")


def _send_mobile(device_service: str, title: str | None, message: str) -> None:
    if not device_service:
        return
    if "." in device_service:
        domain, service = device_service.split(".", 1)
    else:
        domain, service = "notify", device_service
    data = {"message": message}
    if title:
        data["title"] = title
    _ha_call_service(domain, service, data)


def _get_default_device_service() -> str:
    settings = redis_client.hgetall("plugin_settings:Home Assistant Notifier") or {}
    return (settings.get("DEFAULT_DEVICE_SERVICE") or "").strip()


def _ha_bridge_port() -> int:
    try:
        raw_port = redis_client.hget("homeassistant_platform_settings", "bind_port")
        return int(raw_port) if raw_port is not None else 8787
    except Exception:
        return 8787


def _post_ha_notification(
    title: str | None,
    message: str,
    source: str,
    level: str,
    data: Dict[str, Any] | None = None,
) -> None:
    port = _ha_bridge_port()
    url = f"http://127.0.0.1:{port}/tater-ha/v1/notifications/add"
    payload = {
        "source": (source or "notify").strip(),
        "title": (title or "Notification").strip(),
        "type": "notify",
        "message": (message or "").strip(),
        "entity_id": "",
        "ha_time": "",
        "level": (level or "info").strip(),
        "data": data or {},
    }
    resp = requests.post(url, json=payload, timeout=5)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")


def _send_persistent_notification(title: str | None, message: str) -> None:
    data = {
        "message": (message or "").strip(),
        "title": (title or "Notification").strip(),
    }
    _ha_call_service("persistent_notification", "create", data)


class HomeAssistantNotifier(ToolPlugin):
    name = "notify_homeassistant"
    pretty_name = "Home Assistant Notifier"
    version = "1.0.1"
    description = "Queue notifications for Home Assistant delivery."
    notifier = True
    platforms = []  # internal; not exposed as a direct tool
    settings_category = "Home Assistant Notifier"
    required_settings = {
        "ENABLE_PERSISTENT": {
            "label": "Enable Home Assistant Notifications",
            "type": "checkbox",
            "default": True,
            "description": "If disabled, all Home Assistant notifications are skipped.",
        },
        "ENABLE_PERSISTENT_NOTIFICATION": {
            "label": "Enable HA Persistent Notifications",
            "type": "checkbox",
            "default": True,
            "description": "If enabled, also create Home Assistant persistent notifications.",
        },
        "DEFAULT_DEVICE_SERVICE": {
            "label": "Default Mobile Notify Service",
            "type": "string",
            "default": "",
            "description": "Optional fallback mobile notify service (e.g., notify.mobile_app_iphone).",
        },
    }

    def _persistent_enabled(self) -> bool:
        settings = redis_client.hgetall("plugin_settings:Home Assistant Notifier") or {}
        raw = settings.get("ENABLE_PERSISTENT")
        if raw is None:
            return True
        val = str(raw).strip().lower()
        if val in ("1", "true", "yes", "y", "on", "enabled"):
            return True
        if val in ("0", "false", "no", "n", "off", "disabled"):
            return False
        return True

    def _boolish(self, value, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        val = str(value).strip().lower()
        if val in ("1", "true", "yes", "y", "on", "enabled"):
            return True
        if val in ("0", "false", "no", "n", "off", "disabled"):
            return False
        return default

    def _persistent_notification_enabled(self, targets: Dict[str, Any]) -> bool:
        if isinstance(targets, dict) and "persistent" in targets:
            return self._boolish(targets.get("persistent"), True)
        settings = redis_client.hgetall("plugin_settings:Home Assistant Notifier") or {}
        raw = settings.get("ENABLE_PERSISTENT_NOTIFICATION")
        return self._boolish(raw, True)

    def _extract_args(self, args: Dict[str, Any]):
        args = args or {}
        title = args.get("title")
        message = args.get("message") or args.get("content")
        targets = dict(args.get("targets") or {})

        if args.get("device_service") and "device_service" not in targets:
            targets["device_service"] = args.get("device_service")
        if "persistent" in args and "persistent" not in targets:
            targets["persistent"] = args.get("persistent")

        meta = {
            "priority": args.get("priority"),
            "tags": args.get("tags"),
            "ttl_sec": args.get("ttl_sec"),
        }
        origin = args.get("origin")
        return title, message, targets, origin, meta

    async def notify(
        self,
        title: str,
        content: str,
        targets: Dict[str, Any] | None = None,
        origin: Dict[str, Any] | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> str:
        return self._enqueue(title=title, message=content, targets=targets, origin=origin, meta=meta)

    async def handle_webui(self, args, llm_client):
        title, message, targets, origin, meta = self._extract_args(args)
        return self._enqueue(title=title, message=message, targets=targets, origin=origin, meta=meta)

    async def handle_discord(self, message, args, llm_client):
        title, message_text, targets, origin, meta = self._extract_args(args)
        return self._enqueue(title=title, message=message_text, targets=targets, origin=origin, meta=meta)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        title, message_text, targets, origin, meta = self._extract_args(args)
        return self._enqueue(title=title, message=message_text, targets=targets, origin=origin, meta=meta)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        title, message_text, targets, origin, meta = self._extract_args(args)
        return self._enqueue(title=title, message=message_text, targets=targets, origin=origin, meta=meta)

    async def handle_homeassistant(self, args, llm_client):
        title, message_text, targets, origin, meta = self._extract_args(args)
        return self._enqueue(title=title, message=message_text, targets=targets, origin=origin, meta=meta)

    def _enqueue(self, title, message, targets, origin, meta):
        message = (message or "").strip()
        if not message:
            return "Cannot queue: missing message"

        if not self._persistent_enabled():
            return "Cannot queue: Home Assistant notifications disabled"

        resolved, err = resolve_targets("homeassistant", targets, origin, defaults=None)
        if err:
            return err

        source = (origin or {}).get("platform") or "notify"
        priority = (meta or {}).get("priority") or "normal"
        level = "warn" if str(priority).lower() == "high" else "info"
        try:
            _post_ha_notification(
                title, message,
                source=source,
                level=level,
                data={"origin": origin, "targets": resolved},
            )
        except Exception as e:
            logger.warning(f"[notify] HA notifications add failed: {e}")

        if self._persistent_notification_enabled(resolved):
            try:
                _send_persistent_notification(title, message)
            except Exception as e:
                logger.warning(f"[notify] HA persistent notification failed: {e}")

        device_service = (resolved.get("device_service") or _get_default_device_service()).strip()
        if device_service:
            try:
                _send_mobile(device_service, title, message)
            except Exception as e:
                logger.warning(f"[notify] mobile push failed: {e}")

        return "Queued notification for homeassistant"


plugin = HomeAssistantNotifier()
