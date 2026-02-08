import json
import re
from typing import Iterable

from helpers import redis_client as _redis_client


REDIS_KEY = "tater:admin_only_plugins"
CREATION_GATE_KEY = "tater:admin_gate_agent_lab_creation"
CREATION_GATED_FUNCTIONS = {"create_plugin", "create_platform"}

DEFAULT_ADMIN_ONLY_PLUGINS = {
    "broadcast",
    "events_query",
    "find_my_phone",
    "get_notifications",
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


def _as_bool(value, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def is_agent_lab_creation_tool(func_name: str) -> bool:
    return (func_name or "").strip().lower() in CREATION_GATED_FUNCTIONS


def is_agent_lab_creation_admin_gated(redis_client=None) -> bool:
    rc = redis_client or _redis_client
    raw = rc.get(CREATION_GATE_KEY) if rc else None
    return _as_bool(raw, default=False)
