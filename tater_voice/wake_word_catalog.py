from __future__ import annotations

import json
import os
import re
import threading
import time
from typing import Any, Dict, List
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


CATALOG_REPOSITORY_URL = "https://github.com/TaterTotterson/Tater-Wake-Words"
CATALOG_MANIFEST_URL = str(
    os.getenv(
        "TATER_WAKE_WORD_CATALOG_MANIFEST_URL",
        "https://raw.githubusercontent.com/TaterTotterson/Tater-Wake-Words/main/wake_word_manifest.json",
    )
).strip()

_REMOTE_TIMEOUT_SECONDS = 6.0
_CACHE_TTL_SECONDS = 10 * 60.0
_MAX_MANIFEST_BYTES = 2 * 1024 * 1024
_SOURCE_PATTERN = re.compile(r"^microWakeWordsV(?P<version>[0-9]+)$", re.IGNORECASE)
_CATALOG_PATH_PATTERN = re.compile(
    r"^/TaterTotterson/Tater-Wake-Words/main/microWakeWordsV[0-9]+/[^/]+\.json$",
    re.IGNORECASE,
)
_CACHE_LOCK = threading.Lock()
_CACHE: Dict[str, Any] = {"ts": 0.0, "payload": None}


def _text(value: Any) -> str:
    return str(value or "").strip()


def is_catalog_url(value: Any) -> bool:
    token = _text(value)
    if not token:
        return False
    try:
        parsed = urlparse(token)
    except ValueError:
        return False
    return (
        parsed.scheme.lower() == "https"
        and parsed.netloc.lower() == "raw.githubusercontent.com"
        and bool(_CATALOG_PATH_PATTERN.fullmatch(parsed.path))
    )


def require_catalog_url(value: Any) -> str:
    token = _text(value)
    if not is_catalog_url(token):
        raise ValueError("Select a wake word from the official Tater Wake Word Catalog.")
    return token


def _source_version(source: Any) -> tuple[int, str]:
    token = _text(source)
    match = _SOURCE_PATTERN.fullmatch(token)
    if not match:
        return 999, token or "Catalog"
    version = int(match.group("version"))
    return version, f"V{version}"


def _raw_url(path: Any) -> str:
    token = _text(path).lstrip("/")
    return (
        f"https://raw.githubusercontent.com/TaterTotterson/Tater-Wake-Words/main/{token}"
        if token
        else ""
    )


def entries_from_manifest(payload: Any) -> List[Dict[str, Any]]:
    rows: List[Any] = []
    if isinstance(payload, list):
        rows = list(payload)
    elif isinstance(payload, dict):
        for key in ("entries", "wake_words", "words", "models", "items"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                rows = list(candidate)
                break

    entries: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        source = _text(row.get("source") or row.get("source_label") or row.get("folder"))
        version, version_label = _source_version(source)
        path = _text(row.get("path"))
        url = _text(row.get("url") or row.get("download_url") or row.get("json_url")) or _raw_url(path)
        if not is_catalog_url(url):
            continue
        slug = _text(row.get("slug") or row.get("name") or row.get("key"))
        label = _text(row.get("label") or row.get("title"))
        if not label:
            label = (slug or url.rsplit("/", 1)[-1].removesuffix(".json")).replace("_", " ").title()
        entries[url] = {
            "id": _text(row.get("id")) or f"{source}:{slug}",
            "slug": slug,
            "label": label,
            "url": url,
            "source": source,
            "version": version,
            "version_label": version_label,
        }

    return sorted(
        entries.values(),
        key=lambda row: (
            int(row.get("version") or 999),
            _text(row.get("label")).casefold(),
            _text(row.get("slug")).casefold(),
        ),
    )


def _fetch_manifest() -> Dict[str, Any]:
    request = Request(CATALOG_MANIFEST_URL, headers={"User-Agent": "Tater-Wake-Word-Catalog/1.0"})
    with urlopen(request, timeout=_REMOTE_TIMEOUT_SECONDS) as response:
        raw = response.read(_MAX_MANIFEST_BYTES + 1)
    if len(raw) > _MAX_MANIFEST_BYTES:
        raise ValueError("Wake word catalog manifest is too large.")
    payload = json.loads(raw.decode("utf-8"))
    entries = entries_from_manifest(payload)
    if not entries:
        raise ValueError("Wake word catalog manifest contains no usable models.")
    source_versions = sorted(
        {int(row.get("version") or 0) for row in entries if int(row.get("version") or 0) < 999}
    )
    return {
        "entries": entries,
        "count": len(entries),
        "versions": source_versions,
        "warning": "",
        "manifest_url": CATALOG_MANIFEST_URL,
        "repository_url": CATALOG_REPOSITORY_URL,
    }


def load_catalog(*, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.time()
    with _CACHE_LOCK:
        cached = _CACHE.get("payload")
        cached_ts = float(_CACHE.get("ts") or 0.0)
        if not force_refresh and isinstance(cached, dict) and now - cached_ts < _CACHE_TTL_SECONDS:
            return dict(cached)

    try:
        payload = _fetch_manifest()
    except (OSError, URLError, ValueError, json.JSONDecodeError) as exc:
        warning = f"Could not refresh the Tater Wake Word Catalog: {exc}"
        with _CACHE_LOCK:
            stale = _CACHE.get("payload")
            if isinstance(stale, dict) and stale.get("entries"):
                return {**stale, "warning": f"{warning} Showing the cached catalog."}
        return {
            "entries": [],
            "count": 0,
            "versions": [],
            "warning": warning,
            "manifest_url": CATALOG_MANIFEST_URL,
            "repository_url": CATALOG_REPOSITORY_URL,
        }

    with _CACHE_LOCK:
        _CACHE["ts"] = now
        _CACHE["payload"] = dict(payload)
    return payload


def field_payload(*, current_url: Any = "", current_label: Any = "") -> Dict[str, Any]:
    selected_url = _text(current_url) if is_catalog_url(current_url) else ""
    catalog = load_catalog()
    entries = catalog.get("entries") if isinstance(catalog.get("entries"), list) else []
    options = [
        {
            "value": _text(row.get("url")),
            "label": f"{_text(row.get('label'))} [{_text(row.get('version_label'))}]",
        }
        for row in entries
        if _text(row.get("url"))
    ]
    if selected_url and not any(_text(row.get("value")) == selected_url for row in options):
        options.insert(
            0,
            {
                "value": selected_url,
                "label": f"{_text(current_label) or 'Current Catalog Wake Word'} [Catalog]",
            },
        )

    versions = catalog.get("versions") if isinstance(catalog.get("versions"), list) else []
    version_text = "–".join(f"V{version}" for version in (versions[:1] + versions[-1:])) if versions else ""
    if len(versions) == 1:
        version_text = f"V{versions[0]}"
    description = (
        f"{len(options)} official wake models"
        f"{f' across {version_text}' if version_text else ''}. "
        "The version is shown beside every wake word."
    )
    warning = _text(catalog.get("warning"))
    if warning:
        description = f"{description} {warning}"
    return {
        "options": options,
        "selected_url": selected_url,
        "description": description,
        "warning": warning,
        "repository_url": CATALOG_REPOSITORY_URL,
    }
