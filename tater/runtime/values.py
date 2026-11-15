"""Focused helpers extracted from the legacy shared runtime."""

from typing import Any


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _boolish(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    token = _text(value).strip().lower()
    if token in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)
