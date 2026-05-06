import json
import re
from typing import Any, Dict, Iterable, Optional

from helpers import redis_client as _redis_client
from verba_kernel import normalize_platform


REDIS_KEY = "tater:admin_only_plugins"

DEFAULT_ADMIN_ONLY_PLUGINS = {
    "broadcast",
    "discord_admin",
    "events_query",
    "find_my_phone",
    "ha_control",
    "music_assistant",
    "overseerr_request",
    "unifi_network",
    "unifi_protect",
    "voicepe_remote_timer",
}


def normalize_admin_list(raw: str | Iterable[str]) -> set[str]:
    if raw is None:
        return set()

    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return set()
        if text.startswith("["):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return {str(item).strip().lower() for item in parsed if str(item).strip()}
            except Exception:
                pass
        items = re.split(r"[,\n]+", text)
    else:
        items = list(raw or [])
    return {str(item).strip().lower() for item in items if str(item).strip()}


def get_admin_only_plugins(redis_client=None) -> set[str]:
    rc = redis_client or _redis_client
    raw = rc.get(REDIS_KEY) if rc else None
    if raw is None:
        return set(DEFAULT_ADMIN_ONLY_PLUGINS)
    return normalize_admin_list(raw)


def is_admin_only_plugin(plugin_id: str) -> bool:
    return (plugin_id or "").strip().lower() in get_admin_only_plugins()


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="replace").strip()
        except Exception:
            return ""
    return str(value or "").strip()


def _platform(value: Any) -> str:
    token = normalize_platform(_text(value))
    return token or _text(value).lower() or "unknown"


def _origin_person_id(origin: Dict[str, Any]) -> str:
    if not isinstance(origin, dict):
        return ""
    for key in ("person_id", "master_user_id"):
        value = _text(origin.get(key))
        if value:
            return value
    resolution = origin.get("people_resolution")
    if isinstance(resolution, dict):
        for key in ("person_id", "master_user_id"):
            value = _text(resolution.get(key))
            if value:
                return value
    return ""


def _origin_person_name(origin: Dict[str, Any]) -> str:
    if not isinstance(origin, dict):
        return ""
    for key in ("person_name", "display_name"):
        value = _text(origin.get(key))
        if value:
            return value
    resolution = origin.get("people_resolution")
    if isinstance(resolution, dict):
        for key in ("display_name", "person_name"):
            value = _text(resolution.get(key))
            if value:
                return value
    return ""


def _load_people_module() -> Any:
    try:
        import people as people_module  # type: ignore

        return people_module
    except Exception:
        return None


def resolve_admin_status(
    *,
    platform: str,
    origin: Optional[Dict[str, Any]],
    redis_client=None,
) -> Dict[str, Any]:
    rc = redis_client or _redis_client
    origin_payload = origin if isinstance(origin, dict) else {}
    resolved_platform = _platform(platform or origin_payload.get("platform"))
    people_module = _load_people_module()

    if people_module is not None and not _origin_person_id(origin_payload):
        try:
            people_module.apply_resolution_to_origin(
                platform=resolved_platform,
                origin=origin_payload,
                redis_client=rc,
            )
        except Exception:
            pass

    person_id = _origin_person_id(origin_payload)
    person_name = _origin_person_name(origin_payload)
    is_admin = False
    if people_module is not None and person_id:
        try:
            is_admin = bool(people_module.person_is_admin(person_id, rc))
        except Exception:
            is_admin = False

    return {
        "platform": resolved_platform,
        "matched": bool(person_id),
        "person_id": person_id,
        "person_name": person_name,
        "is_admin": bool(is_admin),
    }


def origin_is_admin(platform: str, origin: Optional[Dict[str, Any]], redis_client=None) -> bool:
    return bool(resolve_admin_status(platform=platform, origin=origin, redis_client=redis_client).get("is_admin"))


def admin_denial_message(platform: str, origin: Optional[Dict[str, Any]], redis_client=None) -> str:
    status = resolve_admin_status(platform=platform, origin=origin, redis_client=redis_client)
    platform_label = str(status.get("platform") or platform or "this portal").strip()
    person_name = _text(status.get("person_name"))
    if not bool(status.get("matched")):
        return (
            "This tool is restricted to People marked as admin. "
            f"Link this {platform_label} identity to an admin Person in Settings > People."
        )
    if person_name:
        return f"This tool is restricted to People marked as admin. {person_name} is not marked admin."
    return "This tool is restricted to People marked as admin."
