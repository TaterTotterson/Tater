import json
from typing import Any, Dict, Iterable


SPUDEX_SETTINGS_KEY = "tater:spudex:settings"

DEFAULT_SPUDEX_SETTINGS: Dict[str, Any] = {
    "enabled": False,
    "allowed_platforms": ["webui"],
    "policy_enabled": True,
    "require_approval": True,
    "require_file_approval": False,
    "allow_absolute_executables": False,
    "allow_shell_commands": False,
    "allow_host_admin_commands": False,
    "allow_remote_control": False,
    "allow_containers": False,
    "allow_host_package_managers": False,
    "allow_inline_eval": False,
    "max_task_steps": 6,
    "command_timeout_sec": 45,
    "max_output_chars": 12000,
    "max_log_entries": 4000,
    "max_sessions": 80,
    "allow_network": False,
    "allow_installs": False,
    "sandbox_mode": "agent_lab",
    "default_cwd": "workspace",
    "llm_host": "",
    "llm_model": "",
}

_BOOL_KEYS = {
    "enabled",
    "policy_enabled",
    "require_approval",
    "require_file_approval",
    "allow_absolute_executables",
    "allow_shell_commands",
    "allow_host_admin_commands",
    "allow_remote_control",
    "allow_containers",
    "allow_host_package_managers",
    "allow_inline_eval",
    "allow_network",
    "allow_installs",
}

_INT_LIMITS = {
    "max_task_steps": (1, 30),
    "command_timeout_sec": (5, 3600),
    "max_output_chars": (1000, 500000),
    "max_log_entries": (100, 50000),
    "max_sessions": (10, 500),
}


def _decode(value: Any) -> Any:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return ""
    return value


def _as_bool(value: Any, fallback: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(_decode(value) or "").strip().lower()
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    return fallback


def _as_int(value: Any, fallback: int, *, lo: int, hi: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(fallback)
    return max(lo, min(hi, parsed))


def _normalize_platforms(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items: Iterable[Any] = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = DEFAULT_SPUDEX_SETTINGS["allowed_platforms"]
    out: list[str] = []
    for item in raw_items:
        token = str(_decode(item) or "").strip().lower()
        if not token:
            continue
        if token in {"all", "*"}:
            return ["all"]
        if token not in out:
            out.append(token)
    return out or ["webui"]


def normalize_spudex_settings(values: Dict[str, Any] | None = None) -> Dict[str, Any]:
    source = dict(DEFAULT_SPUDEX_SETTINGS)
    if isinstance(values, dict):
        source.update(values)

    out: Dict[str, Any] = {}
    for key, default in DEFAULT_SPUDEX_SETTINGS.items():
        value = source.get(key, default)
        if key in _BOOL_KEYS:
            out[key] = _as_bool(value, bool(default))
        elif key in _INT_LIMITS:
            lo, hi = _INT_LIMITS[key]
            out[key] = _as_int(value, int(default), lo=lo, hi=hi)
        elif key == "allowed_platforms":
            out[key] = _normalize_platforms(value)
        else:
            out[key] = str(_decode(value) or default).strip() or str(default)
    return out


def get_spudex_settings(redis_client: Any = None) -> Dict[str, Any]:
    raw: Any = None
    if redis_client is not None:
        try:
            raw = redis_client.get(SPUDEX_SETTINGS_KEY)
        except Exception:
            raw = None
    decoded = _decode(raw)
    if isinstance(decoded, str) and decoded.strip():
        try:
            parsed = json.loads(decoded)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            return normalize_spudex_settings(parsed)
    return normalize_spudex_settings()


def save_spudex_settings(values: Dict[str, Any], redis_client: Any = None) -> Dict[str, Any]:
    current = get_spudex_settings(redis_client)
    updates = values if isinstance(values, dict) else {}
    next_settings = normalize_spudex_settings({**current, **updates})
    if redis_client is not None:
        try:
            redis_client.set(SPUDEX_SETTINGS_KEY, json.dumps(next_settings, sort_keys=True))
        except Exception:
            pass
    return next_settings


def spudex_enabled_for_platform(platform: str, redis_client: Any = None) -> bool:
    settings = get_spudex_settings(redis_client)
    if not bool(settings.get("enabled")):
        return False
    allowed = [str(item or "").strip().lower() for item in settings.get("allowed_platforms") or []]
    if "all" in allowed:
        return True
    token = str(platform or "").strip().lower() or "webui"
    return token in allowed


def spudex_llm_overrides(redis_client: Any = None) -> Dict[str, str]:
    settings = get_spudex_settings(redis_client)
    return {
        "host": str(settings.get("llm_host") or "").strip(),
        "model": str(settings.get("llm_model") or "").strip(),
    }
