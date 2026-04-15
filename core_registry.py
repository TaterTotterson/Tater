import importlib
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger("core_registry")

CORE_DIR = Path(os.getenv("TATER_CORE_DIR", "cores"))
_SAFE_MODULE_RE = re.compile(r"^[A-Za-z0-9_]+_core$")
_RESERVED_BUILTIN_CORE_MODULE_KEYS = {
    "voice_core",
}

_DEFAULT_CORE_ORDER = [
    "ai_task_core",
    "awareness_core",
    "memory_core",
    "rss_core",
]

_DEFAULT_CORE_LABELS = {
    "ai_task_core": "AI Task Scheduler Core Settings",
    "awareness_core": "Awareness Core Settings",
    "memory_core": "Memory Core Settings",
    "rss_core": "RSS Core Settings",
}


core_registry: List[Dict[str, Any]] = []
core_registry_errors: List[str] = []


def _core_sort_key(module_key: str) -> tuple[int, str]:
    try:
        idx = _DEFAULT_CORE_ORDER.index(module_key)
        return idx, module_key
    except ValueError:
        return len(_DEFAULT_CORE_ORDER), module_key


def _discover_core_module_keys() -> List[str]:
    if not CORE_DIR.exists() or not CORE_DIR.is_dir():
        return []

    discovered = set()
    for file_path in CORE_DIR.glob("*_core.py"):
        stem = str(file_path.stem or "").strip()
        if stem and _SAFE_MODULE_RE.fullmatch(stem):
            if stem in _RESERVED_BUILTIN_CORE_MODULE_KEYS:
                continue
            discovered.add(stem)

    return sorted(discovered, key=_core_sort_key)


def _humanize_core_key(module_key: str) -> str:
    base = module_key
    if base.endswith("_core"):
        base = base[: -len("_core")]
    parts = [part for part in base.split("_") if part]
    if not parts:
        return module_key
    return " ".join(part.capitalize() for part in parts)


def _derive_label(module_key: str, settings: Dict[str, Any]) -> str:
    default_label = _DEFAULT_CORE_LABELS.get(module_key)
    if default_label:
        return default_label

    explicit_label = str(settings.get("label") or "").strip()
    if explicit_label:
        return explicit_label

    category = str(settings.get("category") or "").strip()
    if category:
        return category

    return f"{_humanize_core_key(module_key)} Core Settings"


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    return default


def _load_webui_tab_config(module: Any) -> Dict[str, Any]:
    raw = getattr(module, "CORE_WEBUI_TAB", None)
    if not isinstance(raw, dict):
        return {}

    label = str(raw.get("label") or "").strip()
    if not label:
        return {}

    order_raw = raw.get("order", 1000)
    try:
        order = int(order_raw)
    except Exception:
        order = 1000

    return {
        "label": label,
        "order": order,
        "requires_running": _coerce_bool(raw.get("requires_running"), True),
    }


def _load_core_settings(module_key: str) -> tuple[Dict[str, Any] | None, str]:
    module_name = f"cores.{module_key}"
    try:
        if module_name in sys.modules:
            module = sys.modules[module_name]
        else:
            module = importlib.import_module(module_name)
    except Exception as exc:
        return None, f"{module_key}: {exc}"

    raw_settings = getattr(module, "CORE_SETTINGS", None)
    settings = dict(raw_settings) if isinstance(raw_settings, dict) else {}

    required = settings.get("required")
    if not isinstance(required, dict):
        settings["required"] = {}

    settings["module_import_name"] = str(getattr(module, "__name__", "")).strip()
    settings["webui_tab"] = _load_webui_tab_config(module)
    has_legacy_tab_renderer = callable(getattr(module, "render_webui_tab", None))
    has_htmlui_provider = callable(getattr(module, "get_htmlui_tab_data", None))
    settings["has_webui_tab_renderer"] = bool(has_legacy_tab_renderer or has_htmlui_provider)
    settings["has_htmlui_tab_provider"] = bool(has_htmlui_provider)
    settings["has_manager_extras_renderer"] = callable(getattr(module, "render_core_manager_extras", None))
    return settings, ""


def refresh_core_registry() -> List[Dict[str, Any]]:
    importlib.invalidate_caches()
    discovered = _discover_core_module_keys()

    updated: List[Dict[str, Any]] = []
    errors: List[str] = []

    for module_key in discovered:
        settings, error = _load_core_settings(module_key)
        if settings is None:
            errors.append(error or f"{module_key}: failed to import")
            continue
        entry: Dict[str, Any] = {
            **settings,
            "key": module_key,
            "label": _derive_label(module_key, settings),
        }
        if not isinstance(entry.get("required"), dict):
            entry["required"] = {}
        updated.append(entry)

    core_registry.clear()
    core_registry.extend(updated)
    core_registry_errors.clear()
    core_registry_errors.extend(errors)

    if errors:
        logger.warning("Core registry load issues: %s", "; ".join(errors))
    return core_registry


def get_core_registry() -> List[Dict[str, Any]]:
    return list(core_registry)


refresh_core_registry()
