from __future__ import annotations

import os
import plistlib
from functools import lru_cache
from pathlib import Path
from typing import Mapping, Optional, Sequence


def _normalize_version(value: object) -> str:
    version = str(value or "").strip()
    if version[:1].lower() == "v":
        version = version[1:].strip()
    return version


def _default_info_plist_paths(
    *,
    module_file: Optional[Path] = None,
    environment: Optional[Mapping[str, str]] = None,
) -> tuple[Path, ...]:
    env = os.environ if environment is None else environment
    source_root = Path(module_file or __file__).resolve().parent
    candidates = []

    configured_path = str(env.get("TATER_INFO_PLIST") or "").strip()
    if configured_path:
        candidates.append(Path(configured_path).expanduser())

    # Packaged layout: Tater.app/Contents/Resources/TaterSource/tater_version.py
    candidates.append(source_root.parent.parent / "Info.plist")
    # Source layout used by local development and the release workflow.
    candidates.append(source_root / "macos" / "Tater" / "Resources" / "Info.plist")

    unique_paths = []
    seen = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique_paths.append(candidate)
    return tuple(unique_paths)


def resolve_tater_version(
    *,
    module_file: Optional[Path] = None,
    environment: Optional[Mapping[str, str]] = None,
    info_plist_paths: Optional[Sequence[Path]] = None,
) -> str:
    env = os.environ if environment is None else environment
    configured_version = _normalize_version(env.get("TATER_APP_VERSION"))
    if configured_version:
        return configured_version

    candidates = (
        tuple(Path(path) for path in info_plist_paths)
        if info_plist_paths is not None
        else _default_info_plist_paths(module_file=module_file, environment=env)
    )
    for info_plist in candidates:
        try:
            with info_plist.open("rb") as handle:
                payload = plistlib.load(handle)
        except (OSError, ValueError, plistlib.InvalidFileException):
            continue
        version = _normalize_version(payload.get("CFBundleShortVersionString"))
        if version:
            return version
    return ""


@lru_cache(maxsize=1)
def current_tater_version() -> str:
    return resolve_tater_version()
