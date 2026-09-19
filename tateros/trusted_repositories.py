"""Curated third-party repository directories published by Tater Shop."""

from __future__ import annotations

import copy
import json
import os
import threading
import time
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen


_DIRECTORY_URLS = {
    "verba": os.getenv(
        "TATER_TRUSTED_VERBA_REPOS_URL",
        "https://raw.githubusercontent.com/TaterTotterson/Tater_Shop/main/verba_repositories.json",
    ),
    "portal": os.getenv(
        "TATER_TRUSTED_PORTAL_REPOS_URL",
        "https://raw.githubusercontent.com/TaterTotterson/Tater_Shop/main/portal_repositories.json",
    ),
    "core": os.getenv(
        "TATER_TRUSTED_CORE_REPOS_URL",
        "https://raw.githubusercontent.com/TaterTotterson/Tater_Shop/main/core_repositories.json",
    ),
}
_CACHE_TTL_SECONDS = max(0.0, float(os.getenv("TATER_TRUSTED_REPOS_CACHE_SECONDS", "60") or 60))
_REQUEST_TIMEOUT_SECONDS = max(1.0, float(os.getenv("TATER_TRUSTED_REPOS_TIMEOUT_SECONDS", "3") or 3))
_cache: dict[str, dict[str, Any]] = {}
_cache_lock = threading.Lock()


def _text(value: Any) -> str:
    return str(value or "").strip()


def _web_url(value: Any) -> str:
    url = _text(value)
    if not url:
        return ""
    parsed = urlparse(url)
    return url if parsed.scheme.lower() == "https" and parsed.netloc else ""


def _normalize_entry(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    manifest_url = _web_url(raw.get("manifest_url") or raw.get("url"))
    if not manifest_url:
        return None

    author_raw = raw.get("author")
    if isinstance(author_raw, dict):
        author_name = _text(author_raw.get("name") or author_raw.get("login") or author_raw.get("label"))
        author_url = _web_url(author_raw.get("url") or author_raw.get("profile_url"))
    else:
        author_name = _text(author_raw or raw.get("publisher") or raw.get("owner"))
        author_url = _web_url(raw.get("author_url") or raw.get("publisher_url"))

    homepage = _web_url(raw.get("homepage") or raw.get("repository_url") or raw.get("repo_url"))
    name = _text(raw.get("name") or raw.get("repository") or raw.get("repo"))
    if not name:
        name = _text(urlparse(homepage or manifest_url).path.rstrip("/").split("/")[-1]) or "Trusted repository"
    repository = _text(raw.get("repository") or raw.get("repo")) or name
    entry_id = _text(raw.get("id")) or f"{author_name}-{repository}".lower().replace(" ", "-")
    tags_raw = raw.get("tags") if isinstance(raw.get("tags"), list) else []
    tags = []
    for value in tags_raw:
        tag = _text(value)
        if tag and tag not in tags:
            tags.append(tag)

    return {
        "id": entry_id,
        "name": name,
        "repository": repository,
        "description": _text(raw.get("description")),
        "author": author_name,
        "author_url": author_url,
        "url": manifest_url,
        "homepage": homepage,
        "tags": tags,
    }


def _normalize_directory(data: Any, kind: str) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        raise ValueError("directory must be a JSON object")
    schema = data.get("schema", data.get("schema_version", 1))
    if str(schema).strip() != "1":
        raise ValueError(f"unsupported directory schema: {schema}")
    declared_kind = _text(data.get("kind")).lower().rstrip("s")
    if declared_kind and declared_kind != kind:
        raise ValueError(f"directory kind is {declared_kind}, expected {kind}")
    raw_rows = data.get("repositories")
    if not isinstance(raw_rows, list):
        raise ValueError("directory must include a repositories list")

    rows: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for raw in raw_rows:
        row = _normalize_entry(raw)
        if not row:
            continue
        token = row["url"].casefold()
        if token in seen_urls:
            continue
        seen_urls.add(token)
        rows.append(row)
    rows.sort(key=lambda row: (row["name"].casefold(), row["author"].casefold()))
    return rows


def _fetch_directory(url: str) -> Any:
    request = Request(url, headers={"User-Agent": "Tater-Trusted-Repositories/1"})
    with urlopen(request, timeout=_REQUEST_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def load_trusted_repositories(kind: str, *, force_refresh: bool = False) -> tuple[list[dict[str, Any]], list[str]]:
    """Return a validated curated directory and retain a last-known-good copy."""

    normalized_kind = _text(kind).lower().rstrip("s")
    url = _text(_DIRECTORY_URLS.get(normalized_kind))
    if not url:
        return [], [f"No trusted {normalized_kind} repository directory is configured."]

    now = time.monotonic()
    with _cache_lock:
        cached = _cache.get(normalized_kind)
        if (
            not force_refresh
            and cached
            and cached.get("url") == url
            and now - float(cached.get("fetched_at") or 0) < _CACHE_TTL_SECONDS
        ):
            return copy.deepcopy(cached["rows"]), list(cached.get("errors") or [])

    try:
        rows = _normalize_directory(_fetch_directory(url), normalized_kind)
    except Exception as exc:
        with _cache_lock:
            cached = _cache.get(normalized_kind)
            if cached and cached.get("url") == url and not cached.get("errors"):
                return copy.deepcopy(cached["rows"]), []
            errors = [f"Trusted {normalized_kind} repositories could not be loaded: {exc}"]
            _cache[normalized_kind] = {"url": url, "rows": [], "errors": errors, "fetched_at": now}
        return [], errors

    with _cache_lock:
        _cache[normalized_kind] = {"url": url, "rows": copy.deepcopy(rows), "errors": [], "fetched_at": now}
    return rows, []


def directory_url(kind: str) -> str:
    return _text(_DIRECTORY_URLS.get(_text(kind).lower().rstrip("s")))
