import hashlib
import json
import logging
import os
import re as _re
from urllib.parse import urljoin, urlparse

import redis
import requests
import streamlit as st
from webui.webui_portals import render_portal_controls

PORTAL_DIR = os.getenv("TATER_PORTAL_DIR", "portals")

PORTAL_SHOP_MANIFEST_URL_DEFAULT = os.getenv(
    "TATER_PORTAL_SHOP_MANIFEST_URL",
    "https://raw.githubusercontent.com/TaterTotterson/Tater_Shop/main/portal_manifest.json",
)
PORTAL_SHOP_MANIFEST_URLS_KEY = "tater:portal_shop_manifest_urls"
DEFAULT_PORTAL_SHOP_LABEL = "Tater Portal Shop"
PORTAL_MANAGER_FLASH_KEY = "portal_manager_flash_messages"

redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)


def _normalize_manifest_url(url: str | None) -> str:
    return str(url or "").strip()


def _normalize_manifest_name(name: str | None) -> str:
    return str(name or "").strip()


def _default_shop_manifest_repo() -> dict[str, str]:
    return {"name": DEFAULT_PORTAL_SHOP_LABEL, "url": PORTAL_SHOP_MANIFEST_URL_DEFAULT}


def _normalize_portal_id(raw_id: str | None) -> str:
    platform_id = str(raw_id or "").strip().lower()
    if platform_id.endswith("_portal"):
        platform_id = platform_id[: -len("_portal")]
    return platform_id


def _safe_portal_file_path(platform_id: str) -> str:
    normalized_id = _normalize_portal_id(platform_id)
    if not _re.fullmatch(r"[a-zA-Z0-9_]+", normalized_id or ""):
        raise ValueError("Invalid portal id")
    return os.path.join(PORTAL_DIR, f"{normalized_id}_portal.py")


def _normalize_manifest_repo_entry(raw) -> dict[str, str] | None:
    if isinstance(raw, str):
        url = _normalize_manifest_url(raw)
        name = ""
    elif isinstance(raw, dict):
        url = _normalize_manifest_url(raw.get("url") or raw.get("manifest_url"))
        name = _normalize_manifest_name(raw.get("name") or raw.get("label"))
    else:
        return None

    if not url:
        return None

    return {"name": name, "url": url}


def _dedupe_manifest_repos(
    repos,
    *,
    include_default: bool = False,
    exclude_default: bool = False,
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    by_url: dict[str, dict[str, str]] = {}

    if include_default:
        default_repo = _default_shop_manifest_repo()
        if default_repo["url"]:
            out.append(default_repo)
            by_url[default_repo["url"]] = default_repo

    for raw in repos or []:
        entry = _normalize_manifest_repo_entry(raw)
        if not entry:
            continue

        url = entry["url"]
        if exclude_default and url == PORTAL_SHOP_MANIFEST_URL_DEFAULT:
            continue

        existing = by_url.get(url)
        if existing:
            if entry["name"] and not existing.get("name"):
                existing["name"] = entry["name"]
            continue

        normalized = {"name": entry["name"], "url": url}
        out.append(normalized)
        by_url[url] = normalized

    return out


def get_additional_portal_shop_manifest_repos() -> list[dict[str, str]]:
    raw = redis_client.get(PORTAL_SHOP_MANIFEST_URLS_KEY)
    repos = []

    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                repos = data
            elif isinstance(data, dict):
                repos = [data]
            elif isinstance(data, str):
                repos = [data.strip()]
        except Exception:
            repos = [line.strip() for line in str(raw).splitlines()]

    return _dedupe_manifest_repos(repos, exclude_default=True)


def get_configured_portal_shop_manifest_repos() -> list[dict[str, str]]:
    return _dedupe_manifest_repos(
        [_default_shop_manifest_repo(), *get_additional_portal_shop_manifest_repos()],
        include_default=False,
    )


def get_configured_portal_shop_manifest_urls() -> list[str]:
    return [repo["url"] for repo in get_configured_portal_shop_manifest_repos() if repo.get("url")]


def save_additional_portal_shop_manifest_repos(repos) -> None:
    extras = _dedupe_manifest_repos(repos, exclude_default=True)
    payload = json.dumps([{"name": repo["name"], "url": repo["url"]} for repo in extras])
    redis_client.set(PORTAL_SHOP_MANIFEST_URLS_KEY, payload)


def fetch_shop_manifest(url: str) -> dict:
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()


def _manifest_items(manifest: dict) -> list[dict]:
    if "portals" not in manifest:
        raise ValueError("Manifest missing portals.")
    items = manifest.get("portals") or []
    if not isinstance(items, list):
        raise ValueError("Manifest format unexpected (expected list under portals).")
    return [item for item in items if isinstance(item, dict)]


def _manifest_source_label(url: str, manifest: dict | None = None, configured_name: str | None = None) -> str:
    if url == PORTAL_SHOP_MANIFEST_URL_DEFAULT:
        return DEFAULT_PORTAL_SHOP_LABEL

    configured_name = _normalize_manifest_name(configured_name)
    if configured_name:
        return configured_name

    if isinstance(manifest, dict):
        for key in ("name", "title", "shop_name", "repo_name"):
            value = str(manifest.get(key) or "").strip()
            if value:
                return value

    parsed = urlparse(url)
    if parsed.netloc:
        path_parts = [part for part in parsed.path.split("/") if part]
        if len(path_parts) >= 2:
            return f"{parsed.netloc}/{path_parts[-2]}"
        return parsed.netloc

    return url


def load_portal_shop_catalog(manifest_sources=None) -> tuple[list[dict], list[str]]:
    repo_entries = (
        get_configured_portal_shop_manifest_repos()
        if manifest_sources is None
        else _dedupe_manifest_repos(manifest_sources)
    )

    merged_items: list[dict] = []
    errors: list[str] = []
    seen_ids = set()

    for repo in repo_entries:
        url = repo["url"]
        configured_name = repo.get("name") or ""
        try:
            manifest = fetch_shop_manifest(url)
            source_label = _manifest_source_label(url, manifest, configured_name=configured_name)
            items = _manifest_items(manifest)
        except Exception as exc:
            errors.append(f"{_manifest_source_label(url, configured_name=configured_name)}: {exc}")
            continue

        for raw_item in items:
            platform_id = _normalize_portal_id(raw_item.get("id"))
            if not platform_id or platform_id in seen_ids:
                continue

            item = dict(raw_item)
            item["id"] = platform_id
            item["_source_manifest_url"] = url
            item["_source_label"] = source_label
            seen_ids.add(platform_id)
            merged_items.append(item)

    return merged_items, errors


def is_portal_installed(platform_id: str) -> bool:
    try:
        return os.path.exists(_safe_portal_file_path(platform_id))
    except Exception:
        return False


def _sha256_bytes(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def install_portal_from_shop_item(item: dict, manifest_url: str | None = None) -> tuple[bool, str]:
    """
    Downloads a platform .py from the manifest entry, verifies sha256 if provided,
    and writes it to PORTAL_DIR as <id>_portal.py.
    """

    try:
        platform_id = _normalize_portal_id(item.get("id"))
        entry = (item.get("entry") or "").strip()
        expected_sha = (item.get("sha256") or "").strip().lower()
        source_manifest_url = _normalize_manifest_url(manifest_url or item.get("_source_manifest_url"))

        if not platform_id:
            return False, "Manifest item missing 'id'."
        if not entry:
            return False, f"{platform_id}: manifest item missing 'entry'."
        if not source_manifest_url:
            return False, f"{platform_id}: manifest source URL is missing."

        entry = entry.lstrip("/")
        full_url = urljoin(source_manifest_url, entry)

        path = _safe_portal_file_path(platform_id)
        os.makedirs(PORTAL_DIR, exist_ok=True)

        response = requests.get(full_url, timeout=30)
        response.raise_for_status()
        data = response.content

        if expected_sha:
            got = _sha256_bytes(data)
            if got.lower() != expected_sha:
                return False, f"SHA256 mismatch for {platform_id}. expected={expected_sha} got={got}"

        try:
            text = data.decode("utf-8")
        except Exception:
            return False, f"{platform_id}: downloaded file is not valid UTF-8 text."

        if "def run" not in text:
            return False, f"{platform_id}: file does not look like a runnable portal module."

        with open(path, "w", encoding="utf-8") as file_obj:
            file_obj.write(text)

        return True, f"Installed {platform_id}"
    except Exception as exc:
        return False, f"Install failed: {exc}"


def _enabled_missing_portal_ids() -> list[str]:
    """
    Returns portal ids that should be running (redis <id>_portal_running=true)
    but whose module file is missing from disk.
    """

    missing: list[str] = []
    seen = set()

    try:
        for raw_key in redis_client.scan_iter(match="*_portal_running", count=200):
            state_key = str(raw_key or "").strip()
            if not state_key.endswith("_portal_running"):
                continue

            enabled = str(redis_client.get(state_key) or "").strip().lower() == "true"
            if not enabled:
                continue

            module_key = state_key[: -len("_running")]
            if not module_key.endswith("_portal"):
                continue

            platform_id = _normalize_portal_id(module_key)
            if not platform_id or platform_id in seen:
                continue

            if not is_portal_installed(platform_id):
                seen.add(platform_id)
                missing.append(platform_id)
    except Exception:
        return missing

    return missing


def auto_restore_missing_portals(
    manifest_urls: list[str] | str | None = None,
    progress_cb=None,
) -> tuple[bool, list[str], list[str]]:
    """
    Restore any portals that are enabled in Redis but missing on disk.
    """

    restored: list[str] = []
    changed = False

    if manifest_urls is None:
        manifest_url_list = get_configured_portal_shop_manifest_urls()
    elif isinstance(manifest_urls, str):
        manifest_url_list = [manifest_urls]
    else:
        manifest_url_list = manifest_urls

    missing = _enabled_missing_portal_ids()
    if not missing:
        return changed, restored, missing

    total = len(missing)
    if progress_cb:
        try:
            progress_cb(0.0, f"Found {total} enabled portal(s) missing - preparing downloads...")
        except Exception:
            pass

    catalog_items, catalog_errors = load_portal_shop_catalog(manifest_url_list)
    by_id = {
        str(item.get("id") or "").strip(): item
        for item in catalog_items
        if str(item.get("id") or "").strip()
    }

    if catalog_errors:
        logging.warning("[portal-restore] Failed to load some portal repos: %s", catalog_errors)

    for idx, platform_id in enumerate(missing, start=1):
        item = by_id.get(platform_id)
        if not item:
            state_key = f"{platform_id}_portal_running"
            if catalog_errors:
                logging.warning(
                    "[portal-restore] %s enabled but not found in loaded manifests; preserving running state because some repos failed",
                    platform_id,
                )
                if progress_cb:
                    try:
                        progress_cb(
                            (idx - 1) / max(1, total),
                            f"{platform_id} not found in loaded repos; keeping running state because some repos failed ({idx}/{total})",
                        )
                    except Exception:
                        pass
                continue

            logging.error("[portal-restore] %s enabled but not found in manifest", platform_id)
            try:
                redis_client.set(state_key, "false")
                logging.info("[portal-restore] Disabled stale running key for %s", platform_id)
            except Exception as exc:
                logging.error("[portal-restore] Failed to disable stale running key for %s: %s", platform_id, exc)

            if progress_cb:
                try:
                    progress_cb(
                        (idx - 1) / max(1, total),
                        f"{platform_id} missing and not in manifests; disabled stale running key ({idx}/{total})",
                    )
                except Exception:
                    pass
            continue

        if progress_cb:
            try:
                progress_cb((idx - 1) / max(1, total), f"Downloading {platform_id}... ({idx}/{total})")
            except Exception:
                pass

        ok, msg = install_portal_from_shop_item(item)
        if ok:
            restored.append(platform_id)
            changed = True
            logging.info("[portal-restore] %s: %s", platform_id, msg)
        else:
            logging.error("[portal-restore] %s: %s", platform_id, msg)

        if progress_cb:
            try:
                progress_cb(idx / max(1, total), f"Finished {platform_id} ({idx}/{total})")
            except Exception:
                pass

    return changed, restored, missing


def ensure_portals_ready(progress_cb=None):
    """
    Ensure any portals marked running in Redis and missing on disk are restored
    from the configured portal shop manifests.
    """

    os.makedirs(PORTAL_DIR, exist_ok=True)

    missing = _enabled_missing_portal_ids()
    if not missing:
        if progress_cb:
            try:
                progress_cb(1.0, "All enabled portals are present.")
            except Exception:
                pass
        return

    shop_urls = get_configured_portal_shop_manifest_urls()
    if not shop_urls:
        if progress_cb:
            try:
                progress_cb(1.0, "Portal shop manifest URLs are not configured.")
            except Exception:
                pass
        return

    if progress_cb:
        try:
            progress_cb(0.0, f"Restoring {len(missing)} missing portal(s) from {len(shop_urls)} repo(s)...")
        except Exception:
            pass

    _changed, restored, enabled_missing = auto_restore_missing_portals(
        shop_urls,
        progress_cb=progress_cb,
    )

    if progress_cb and not restored and enabled_missing:
        try:
            progress_cb(1.0, "Missing portals could not be restored from the configured repos.")
        except Exception:
            pass
    elif progress_cb:
        try:
            progress_cb(1.0, "")
        except Exception:
            pass


def _portal_module_key(platform_id: str) -> str:
    normalized = _normalize_portal_id(platform_id)
    return f"{normalized}_portal" if normalized else ""


def _portal_display_name(platform_id: str, fallback: str | None = None) -> str:
    if fallback:
        text = str(fallback).strip()
        if text:
            return text
    parts = [part for part in _normalize_portal_id(platform_id).split("_") if part]
    if not parts:
        return str(platform_id or "").strip() or "Unknown Portal"
    return " ".join(part.capitalize() for part in parts)


def _installed_portal_ids() -> list[str]:
    installed_ids = set()
    if os.path.isdir(PORTAL_DIR):
        for filename in os.listdir(PORTAL_DIR):
            if filename == "__init__.py" or not filename.endswith("_portal.py"):
                continue
            stem = filename[:-3]
            platform_id = _normalize_portal_id(stem)
            if platform_id:
                installed_ids.add(platform_id)
    return sorted(installed_ids)


def _semver_tuple(v: str) -> tuple[int, int, int]:
    if not v:
        return (0, 0, 0)
    value = str(v).strip().lower()
    if value.startswith("v"):
        value = value[1:].strip()
    match = _re.match(r"^([0-9]+(\.[0-9]+){0,2})", value)
    core = match.group(1) if match else "0.0.0"
    parts = (core.split(".") + ["0", "0", "0"])[:3]
    try:
        return (int(parts[0]), int(parts[1]), int(parts[2]))
    except Exception:
        return (0, 0, 0)


def _get_installed_portal_version(platform_id: str) -> str:
    try:
        path = _safe_portal_file_path(platform_id)
    except Exception:
        return "0.0.0"
    if not os.path.exists(path):
        return "0.0.0"
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            text = file_obj.read()
    except Exception:
        return "0.0.0"
    for key in ("__version__", "PORTAL_VERSION", "VERSION"):
        match = _re.search(rf"^\s*{_re.escape(key)}\s*=\s*['\"]([^'\"]+)['\"]", text, flags=_re.MULTILINE)
        if match:
            version = str(match.group(1) or "").strip()
            if version:
                return version
    return "0.0.0"


def uninstall_portal_file(platform_id: str) -> tuple[bool, str]:
    try:
        path = _safe_portal_file_path(platform_id)
        if not os.path.exists(path):
            return True, "Portal file not found (already removed)."
        os.remove(path)
        return True, f"Removed {path}"
    except Exception as exc:
        return False, f"Uninstall failed: {exc}"


def clear_portal_redis_data(platform_id: str, module_key: str | None = None) -> tuple[bool, str]:
    try:
        normalized_id = _normalize_portal_id(platform_id)
        resolved_module_key = str(module_key or _portal_module_key(normalized_id)).strip()
        if not resolved_module_key:
            return False, "Invalid portal id."

        deleted = []
        settings_key = f"{resolved_module_key}_settings"
        state_key = f"{resolved_module_key}_running"
        cooldown_key = f"tater:cooldown:{resolved_module_key}"

        if redis_client.exists(settings_key):
            redis_client.delete(settings_key)
            deleted.append(settings_key)

        if redis_client.get(state_key) is not None:
            redis_client.set(state_key, "false")
            deleted.append(f"{state_key}=false")

        if redis_client.exists(cooldown_key):
            redis_client.delete(cooldown_key)
            deleted.append(cooldown_key)

        if deleted:
            return True, "Deleted: " + ", ".join(deleted)
        return True, "No Redis keys found for this portal."
    except Exception as exc:
        return False, f"Redis cleanup failed: {exc}"


def _build_installed_portal_entries(catalog_items: list[dict]) -> list[dict]:
    catalog_by_id = {
        str(item.get("id") or "").strip(): item
        for item in catalog_items
        if str(item.get("id") or "").strip()
    }

    entries = []
    for platform_id in _installed_portal_ids():
        catalog_item = catalog_by_id.get(platform_id)
        display_name = _portal_display_name(platform_id, fallback=(catalog_item or {}).get("name"))
        description = str((catalog_item or {}).get("description") or "").strip() or "Local portal module."
        module_key = str((catalog_item or {}).get("module_key") or _portal_module_key(platform_id)).strip()
        installed_ver = _get_installed_portal_version(platform_id)
        store_ver = str((catalog_item or {}).get("version") or "").strip()
        source_label = str((catalog_item or {}).get("_source_label") or "Local portal").strip()
        running = str(redis_client.get(f"{module_key}_running") or "").strip().lower() == "true"
        update_available = bool(catalog_item and _semver_tuple(store_ver) > _semver_tuple(installed_ver))
        entries.append(
            {
                "id": platform_id,
                "catalog_item": catalog_item,
                "display_name": display_name,
                "description": description,
                "module_key": module_key,
                "installed_ver": installed_ver,
                "store_ver": store_ver,
                "source_label": source_label,
                "running": running,
                "required_settings_count": int((catalog_item or {}).get("required_settings_count") or 0),
                "update_available": update_available,
            }
        )

    entries.sort(key=lambda item: item["display_name"].lower())
    return entries


def _queue_portal_manager_messages(messages: list[dict]) -> None:
    normalized = []
    for item in messages or []:
        if not isinstance(item, dict):
            continue
        level = str(item.get("level") or "info").strip().lower()
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        if level not in {"success", "error", "warning", "info"}:
            level = "info"
        normalized.append({"level": level, "text": text})
    if normalized:
        st.session_state[PORTAL_MANAGER_FLASH_KEY] = normalized


def _render_portal_manager_messages() -> None:
    messages = st.session_state.pop(PORTAL_MANAGER_FLASH_KEY, None)
    if not isinstance(messages, list):
        return
    for item in messages:
        if not isinstance(item, dict):
            continue
        level = str(item.get("level") or "info").strip().lower()
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        if level == "success":
            st.success(text)
        elif level == "error":
            st.error(text)
        elif level == "warning":
            st.warning(text)
        else:
            st.info(text)


def _render_catalog_warnings(catalog_errors: list[str]) -> None:
    if not catalog_errors:
        return
    st.warning("Some portal repos could not be loaded.")
    for error in catalog_errors:
        st.caption(error)


def _update_portal_entry(entry: dict) -> tuple[bool, str]:
    platform_id = str(entry.get("id") or "").strip()
    catalog_item = entry.get("catalog_item")
    if not platform_id:
        return False, "Portal id is missing."
    if not isinstance(catalog_item, dict):
        return False, f"{platform_id}: no catalog update source is available."

    ok, msg = install_portal_from_shop_item(catalog_item)
    if not ok:
        return False, msg

    installed_ver = str(entry.get("installed_ver") or "0.0.0").strip() or "0.0.0"
    store_ver = str(entry.get("store_ver") or installed_ver).strip() or installed_ver
    display_name = str(entry.get("display_name") or platform_id).strip() or platform_id
    return True, f"{display_name} updated {installed_ver} -> {store_ver}"


def _update_portal_entries(entries: list[dict]) -> tuple[list[str], list[str]]:
    updated: list[str] = []
    failed: list[str] = []

    for entry in entries:
        ok, msg = _update_portal_entry(entry)
        if ok:
            updated.append(str(entry.get("display_name") or entry.get("id") or "").strip() or msg)
        else:
            failed.append(msg)
    return updated, failed


def _render_portal_store_tab(catalog_items: list[dict], catalog_errors: list[str], manifest_repos: list[dict[str, str]]):
    _render_catalog_warnings(catalog_errors)

    available_items = [
        item for item in catalog_items
        if not is_portal_installed(str(item.get("id") or "").strip())
    ]

    search_q = st.text_input(
        "Search available portals",
        value="",
        placeholder="Search name, id, description...",
        key="portal_manager_store_search",
    ).strip().lower()

    filtered_items = []
    for item in available_items:
        platform_id = str(item.get("id") or "").strip()
        name = str(item.get("name") or _portal_display_name(platform_id)).strip()
        description = str(item.get("description") or "").strip()
        if search_q:
            haystack = f"{platform_id}\n{name}\n{description}".lower()
            if search_q not in haystack:
                continue
        filtered_items.append(item)

    st.caption(
        f"Showing {len(filtered_items)} of {len(available_items)} available portal(s) across {len(manifest_repos)} repo(s)."
    )

    if not filtered_items:
        st.info("No downloadable portals match the current filters.")
        return

    for row_start in range(0, len(filtered_items), 2):
        row_items = filtered_items[row_start:row_start + 2]
        row_cols = st.columns(2)

        for col_idx, item in enumerate(row_items):
            platform_id = str(item.get("id") or "").strip()
            display_name = str(item.get("name") or _portal_display_name(platform_id)).strip()
            description = str(item.get("description") or "").strip()
            source_label = str(item.get("_source_label") or "Custom Repo").strip()
            module_key = str(item.get("module_key") or _portal_module_key(platform_id)).strip()
            min_ver = str(item.get("min_tater_version") or "0.0.0").strip()
            store_ver = str(item.get("version") or "0.0.0").strip()
            required_settings_count = int(item.get("required_settings_count") or 0)

            with row_cols[col_idx]:
                with st.container(border=True):
                    st.subheader(display_name)
                    st.caption(
                        f"ID: {platform_id} | module: {module_key} | version: {store_ver} | min tater: {min_ver} | source: {source_label}"
                    )
                    if description:
                        st.write(description)
                    st.caption(f"Required settings: {required_settings_count}")

                    if st.button(
                        "Install",
                        key=f"portal_manager_install_{platform_id}",
                        type="primary",
                        use_container_width=True,
                    ):
                        ok, msg = install_portal_from_shop_item(item)
                        if ok:
                            _queue_portal_manager_messages([{"level": "success", "text": msg}])
                            st.rerun()
                        st.error(msg)


def _render_installed_portals_tab(
    catalog_items: list[dict],
    catalog_errors: list[str],
    *,
    portal_registry: list[dict],
    start_portal_fn,
    stop_portal_fn,
    wipe_memory_core_data_fn,
):
    _render_catalog_warnings(catalog_errors)
    installed_entries = _build_installed_portal_entries(catalog_items)
    portal_by_key = {
        str(item.get("key") or "").strip(): item
        for item in (portal_registry or [])
        if isinstance(item, dict) and str(item.get("key") or "").strip()
    }

    search_q = st.text_input(
        "Search installed portals",
        value="",
        placeholder="Search name, id, description...",
        key="portal_manager_installed_search",
    ).strip().lower()

    filtered_entries = []
    for entry in installed_entries:
        platform_id = entry["id"]
        display_name = entry["display_name"]
        description = entry["description"]
        if search_q:
            haystack = f"{platform_id}\n{display_name}\n{description}".lower()
            if search_q not in haystack:
                continue
        filtered_entries.append(entry)

    st.caption(f"Showing {len(filtered_entries)} of {len(installed_entries)} installed portal(s).")
    if not filtered_entries:
        st.info("No installed portals match the current filters.")
        return

    for entry in filtered_entries:
        platform_id = entry["id"]
        display_name = entry["display_name"]
        purge_key = f"portal_manager_purge_{platform_id}"

        with st.container(border=True):
            st.subheader(display_name)
            meta_parts = [
                f"ID: {platform_id}",
                f"module: {entry['module_key']}",
                f"installed: {entry['installed_ver'] or '0.0.0'}",
            ]
            if entry["store_ver"]:
                meta_parts.append(f"store: {entry['store_ver']}")
            meta_parts.append(f"source: {entry['source_label']}")
            st.caption(" | ".join(meta_parts))

            if entry["description"]:
                st.write(entry["description"])

            portal_settings = portal_by_key.get(str(entry.get("module_key") or "").strip())
            if portal_settings:
                with st.expander("Settings", expanded=False):
                    render_portal_controls(
                        portal_settings,
                        redis_client,
                        start_portal_fn=start_portal_fn,
                        stop_portal_fn=stop_portal_fn,
                        wipe_memory_core_data_fn=wipe_memory_core_data_fn,
                    )
            else:
                running_text = "running" if entry["running"] else "stopped"
                st.caption(f"Status: {running_text} | Required settings: {entry['required_settings_count']}")
                st.caption("Settings are unavailable because this portal is not currently importable.")

            controls = st.columns([1, 1, 3])
            if entry["update_available"]:
                if controls[0].button(
                    "Update",
                    key=f"portal_manager_update_{platform_id}",
                    type="primary",
                    use_container_width=True,
                ):
                    ok, msg = _update_portal_entry(entry)
                    if ok:
                        _queue_portal_manager_messages([{"level": "success", "text": msg}])
                        st.rerun()
                    st.error(msg)
            else:
                controls[0].button(
                    "Up to date",
                    disabled=True,
                    key=f"portal_manager_uptodate_{platform_id}",
                    use_container_width=True,
                )

            purge_redis = controls[2].checkbox("Delete Data?", value=False, key=purge_key)

            if controls[1].button(
                "Remove",
                key=f"portal_manager_remove_{platform_id}",
                type="secondary",
                use_container_width=True,
            ):
                ok, msg = uninstall_portal_file(platform_id)
                if not ok:
                    st.error(msg)
                    continue

                try:
                    redis_client.set(f"{entry['module_key']}_running", "false")
                except Exception:
                    pass

                flash_messages = [{"level": "success", "text": msg}]
                if purge_redis:
                    ok2, msg2 = clear_portal_redis_data(platform_id, module_key=entry["module_key"])
                    flash_messages.append({"level": "success" if ok2 else "error", "text": f"Redis cleanup: {msg2}"})
                _queue_portal_manager_messages(flash_messages)
                st.rerun()


def _render_updates_tab(catalog_items: list[dict], catalog_errors: list[str]):
    _render_catalog_warnings(catalog_errors)

    installed_entries = _build_installed_portal_entries(catalog_items)
    update_entries = [entry for entry in installed_entries if entry["update_available"]]
    catalog_backed_entries = [entry for entry in installed_entries if entry["catalog_item"]]
    local_only_entries = [entry for entry in installed_entries if not entry["catalog_item"]]
    up_to_date_entries = [entry for entry in catalog_backed_entries if not entry["update_available"]]

    summary_cols = st.columns(4)
    summary_cols[0].metric("Updates Available", len(update_entries))
    summary_cols[1].metric("Catalog-backed", len(catalog_backed_entries))
    summary_cols[2].metric("Up to Date", len(up_to_date_entries))
    summary_cols[3].metric("Local Only", len(local_only_entries))

    action_col, text_col = st.columns([1, 3])
    if action_col.button(
        "Update All",
        key="portal_manager_update_all",
        disabled=not update_entries,
        type="primary",
        use_container_width=True,
    ):
        with st.spinner(f"Updating {len(update_entries)} portal(s)..."):
            updated, failed = _update_portal_entries(update_entries)
        flash_messages = []
        if updated:
            updated_text = ", ".join(updated[:8])
            if len(updated) > 8:
                updated_text += f", and {len(updated) - 8} more"
            flash_messages.append({"level": "success", "text": f"Updated {len(updated)} portal(s): {updated_text}"})
        if failed:
            failed_text = "; ".join(failed[:4])
            if len(failed) > 4:
                failed_text += f"; and {len(failed) - 4} more"
            flash_messages.append({"level": "error", "text": f"{len(failed)} portal update(s) failed: {failed_text}"})
        if not flash_messages:
            flash_messages.append({"level": "info", "text": "No portal updates were available."})
        _queue_portal_manager_messages(flash_messages)
        st.rerun()

    if update_entries:
        text_col.caption("Only portals with a newer catalog version are listed below. Update one at a time or update all in one pass.")
    else:
        text_col.caption("No catalog-backed portal updates are currently available.")

    if not update_entries:
        if local_only_entries:
            st.info("All catalog-backed portals are up to date. Some installed portals are local-only and do not have a catalog update source.")
        else:
            st.success("All installed catalog-backed portals are up to date.")
        return

    search_q = st.text_input(
        "Search updates",
        value="",
        placeholder="Search name, id, description...",
        key="portal_manager_updates_search",
    ).strip().lower()

    filtered_entries = []
    for entry in update_entries:
        haystack = f"{entry['id']}\n{entry['display_name']}\n{entry['description']}".lower()
        if search_q and search_q not in haystack:
            continue
        filtered_entries.append(entry)

    st.caption(f"Showing {len(filtered_entries)} of {len(update_entries)} portal(s) with available updates.")
    if not filtered_entries:
        st.info("No portal updates match the current filters.")
        return

    for entry in filtered_entries:
        platform_id = entry["id"]
        with st.container(border=True):
            st.subheader(entry["display_name"])
            st.caption(
                " | ".join(
                    [
                        f"ID: {platform_id}",
                        f"module: {entry['module_key']}",
                        f"installed: {entry['installed_ver'] or '0.0.0'}",
                        f"store: {entry['store_ver'] or '0.0.0'}",
                        f"source: {entry['source_label']}",
                    ]
                )
            )
            if entry["description"]:
                st.write(entry["description"])
            if st.button(
                "Update",
                key=f"portal_manager_updates_update_{platform_id}",
                type="primary",
                use_container_width=True,
            ):
                ok, msg = _update_portal_entry(entry)
                if ok:
                    _queue_portal_manager_messages([{"level": "success", "text": msg}])
                    st.rerun()
                st.error(msg)


def _render_settings_tab(catalog_errors: list[str], manifest_repos: list[dict[str, str]]):
    st.caption("The default Portal Shop is always enabled. Add optional names for extra repos to control the source label shown in the store.")

    default_name_col, default_url_col = st.columns([1, 2])
    with default_name_col:
        st.text_input(
            "Default repo name",
            value=DEFAULT_PORTAL_SHOP_LABEL,
            disabled=True,
            key="portal_manager_default_repo_name",
        )
    with default_url_col:
        st.text_input(
            "Default manifest URL",
            value=PORTAL_SHOP_MANIFEST_URL_DEFAULT,
            disabled=True,
            key="portal_manager_default_manifest_url",
        )

    extra_repos = get_additional_portal_shop_manifest_repos()
    if "portal_manager_repo_form_count" not in st.session_state:
        st.session_state["portal_manager_repo_form_count"] = max(1, len(extra_repos))
        for idx, repo in enumerate(extra_repos):
            st.session_state[f"portal_manager_repo_name_{idx}"] = repo.get("name", "")
            st.session_state[f"portal_manager_repo_url_{idx}"] = repo.get("url", "")

    st.caption("Additional repos")
    for idx in range(max(1, int(st.session_state.get("portal_manager_repo_form_count", 1)))):
        name_key = f"portal_manager_repo_name_{idx}"
        url_key = f"portal_manager_repo_url_{idx}"
        cols = st.columns([1, 2])
        with cols[0]:
            st.text_input(
                f"Repo {idx + 1} name",
                key=name_key,
                placeholder="Optional display name",
            )
        with cols[1]:
            st.text_input(
                f"Repo {idx + 1} manifest URL",
                key=url_key,
                placeholder="https://example.com/portal_manifest.json",
            )

    add_col, remove_col, save_col, refresh_col = st.columns([1, 1, 1, 1])

    if add_col.button("Add Repo", key="portal_manager_add_repo", type="secondary", use_container_width=True):
        next_idx = int(st.session_state.get("portal_manager_repo_form_count", 1))
        st.session_state["portal_manager_repo_form_count"] = next_idx + 1
        st.session_state[f"portal_manager_repo_name_{next_idx}"] = ""
        st.session_state[f"portal_manager_repo_url_{next_idx}"] = ""
        st.rerun()

    if remove_col.button("Remove Last", key="portal_manager_remove_repo", type="secondary", use_container_width=True):
        count = max(1, int(st.session_state.get("portal_manager_repo_form_count", 1)))
        if count > 1:
            last_idx = count - 1
            st.session_state.pop(f"portal_manager_repo_name_{last_idx}", None)
            st.session_state.pop(f"portal_manager_repo_url_{last_idx}", None)
            st.session_state["portal_manager_repo_form_count"] = count - 1
        else:
            st.session_state["portal_manager_repo_name_0"] = ""
            st.session_state["portal_manager_repo_url_0"] = ""
        st.rerun()

    if save_col.button("Save Repos", key="portal_manager_save_repos", type="primary", use_container_width=True):
        parsed_repos = []
        row_count = max(1, int(st.session_state.get("portal_manager_repo_form_count", 1)))
        for idx in range(row_count):
            name = _normalize_manifest_name(st.session_state.get(f"portal_manager_repo_name_{idx}"))
            url = _normalize_manifest_url(st.session_state.get(f"portal_manager_repo_url_{idx}"))

            if not name and not url:
                continue
            if not url:
                st.error(f"Repo {idx + 1} is missing a manifest URL.")
                return

            parsed_repos.append({"name": name, "url": url})

        save_additional_portal_shop_manifest_repos(parsed_repos)
        st.success("Portal repos saved.")
        st.rerun()

    if refresh_col.button("Refresh Catalog", key="portal_manager_refresh_catalog", type="secondary", use_container_width=True):
        st.rerun()

    st.caption(
        "Catalog merge order is fixed: the default Portal Shop loads first, then your additional repos. "
        "If two repos publish the same portal id, the first one wins."
    )
    st.caption("Leave the repo name blank if you want Tater to fall back to the manifest name or URL.")
    st.caption(f"Configured repos: {len(manifest_repos)}")
    _render_catalog_warnings(catalog_errors)


def render_portal_store_page(
    *,
    portal_registry: list[dict],
    start_portal_fn,
    stop_portal_fn,
    wipe_memory_core_data_fn,
):
    st.title("Portal Manager")
    st.caption("Install portals from configured repos, manage installed portals, and edit portal repo settings.")
    _render_portal_manager_messages()

    manifest_repos = get_configured_portal_shop_manifest_repos()
    catalog_items, catalog_errors = load_portal_shop_catalog(manifest_repos)

    installed_tab, store_tab, updates_tab, settings_tab = st.tabs(
        ["Installed Portals", "Portal Store", "Updates", "Settings"]
    )

    with installed_tab:
        _render_installed_portals_tab(
            catalog_items,
            catalog_errors,
            portal_registry=portal_registry,
            start_portal_fn=start_portal_fn,
            stop_portal_fn=stop_portal_fn,
            wipe_memory_core_data_fn=wipe_memory_core_data_fn,
        )

    with store_tab:
        _render_portal_store_tab(catalog_items, catalog_errors, manifest_repos)

    with updates_tab:
        _render_updates_tab(catalog_items, catalog_errors)

    with settings_tab:
        _render_settings_tab(catalog_errors, manifest_repos)
