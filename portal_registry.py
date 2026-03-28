import importlib
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger("portal_registry")

PORTAL_DIR = Path(os.getenv("TATER_PORTAL_DIR", "portals"))
_SAFE_MODULE_RE = re.compile(r"^[A-Za-z0-9_]+_portal$")

_DEFAULT_PORTAL_ORDER = [
    "macos_portal",
    "discord_portal",
    "homeassistant_portal",
    "homekit_portal",
    "irc_portal",
    "matrix_portal",
    "telegram_portal",
    "xbmc_portal",
]

_DEFAULT_PORTAL_LABELS = {
    "macos_portal": "macOS Portal Settings",
    "discord_portal": "Discord Portal Settings",
    "homeassistant_portal": "Home Assistant Portal Settings",
    "homekit_portal": "HomeKit / Siri Portal Settings",
    "irc_portal": "IRC Portal Settings",
    "matrix_portal": "Matrix Portal Settings",
    "telegram_portal": "Telegram Portal Settings",
    "xbmc_portal": "XBMC / Original Xbox Portal Settings",
}


portal_registry: List[Dict[str, Any]] = []
portal_registry_errors: List[str] = []


def _portal_sort_key(module_key: str) -> tuple[int, str]:
    try:
        idx = _DEFAULT_PORTAL_ORDER.index(module_key)
        return idx, module_key
    except ValueError:
        return len(_DEFAULT_PORTAL_ORDER), module_key


def _discover_portal_module_keys() -> List[str]:
    if not PORTAL_DIR.exists() or not PORTAL_DIR.is_dir():
        return []

    discovered = set()
    for file_path in PORTAL_DIR.glob("*_portal.py"):
        stem = str(file_path.stem or "").strip()
        if stem and _SAFE_MODULE_RE.fullmatch(stem):
            discovered.add(stem)

    return sorted(discovered, key=_portal_sort_key)


def _humanize_portal_key(module_key: str) -> str:
    base = module_key
    if base.endswith("_portal"):
        base = base[: -len("_portal")]
    parts = [part for part in base.split("_") if part]
    if not parts:
        return module_key
    return " ".join(part.capitalize() for part in parts)


def _derive_label(module_key: str, settings: Dict[str, Any]) -> str:
    default_label = _DEFAULT_PORTAL_LABELS.get(module_key)
    if default_label:
        return default_label

    explicit_label = str(settings.get("label") or "").strip()
    if explicit_label:
        return explicit_label

    category = str(settings.get("category") or "").strip()
    if category:
        return category

    return f"{_humanize_portal_key(module_key)} Portal Settings"


def _load_portal_settings(module_key: str) -> tuple[Dict[str, Any] | None, str]:
    module_name = f"portals.{module_key}"
    try:
        if module_name in sys.modules:
            module = sys.modules[module_name]
        else:
            module = importlib.import_module(module_name)
    except Exception as exc:
        return None, f"{module_key}: {exc}"

    raw_settings = getattr(module, "PORTAL_SETTINGS", None)
    settings = dict(raw_settings) if isinstance(raw_settings, dict) else {}

    required = settings.get("required")
    if not isinstance(required, dict):
        settings["required"] = {}

    settings["module_import_name"] = str(getattr(module, "__name__", "")).strip()
    return settings, ""


def refresh_portal_registry() -> List[Dict[str, Any]]:
    importlib.invalidate_caches()
    discovered = _discover_portal_module_keys()

    updated: List[Dict[str, Any]] = []
    errors: List[str] = []

    for module_key in discovered:
        settings, error = _load_portal_settings(module_key)
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

    portal_registry.clear()
    portal_registry.extend(updated)
    portal_registry_errors.clear()
    portal_registry_errors.extend(errors)

    if errors:
        logger.warning("Portal registry load issues: %s", "; ".join(errors))
    return portal_registry


def get_portal_registry() -> List[Dict[str, Any]]:
    return list(portal_registry)


refresh_portal_registry()
