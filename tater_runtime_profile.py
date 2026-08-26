from __future__ import annotations

import os
from typing import Any


REMOTE_ONLY_PROFILES = frozenset(
    {
        "edge",
        "edge_remote",
        "remote",
        "remote_only",
        "sat1_edge",
    }
)
_TRUE_VALUES = frozenset({"1", "true", "yes", "on", "enabled"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off", "disabled"})


def normalize_profile(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def remote_only_enabled(*, environ: dict[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    explicit = normalize_profile(env.get("TATER_REMOTE_ONLY"))
    if explicit in _TRUE_VALUES:
        return True
    if explicit in _FALSE_VALUES:
        return False
    return normalize_profile(env.get("TATER_SETUP_PROFILE")) in REMOTE_ONLY_PROFILES

