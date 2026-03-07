import hashlib
import json
import logging
import os
import re as _re
from urllib.parse import urljoin, urlparse

import redis
import requests
import streamlit as st

import plugin_registry as plugin_registry_mod
from plugin_kernel import expand_plugin_platforms
from plugin_settings import set_plugin_enabled

PLUGIN_DIR = os.getenv("TATER_PLUGIN_DIR", "plugins")
SHOP_MANIFEST_URL_DEFAULT = os.getenv(
    "TATER_SHOP_MANIFEST_URL",
    "https://raw.githubusercontent.com/TaterTotterson/Tater_Shop/main/manifest.json",
)
SHOP_MANIFEST_URLS_KEY = "tater:shop_manifest_urls"
LEGACY_SHOP_MANIFEST_URL_KEY = "tater:shop_manifest_url"
DEFAULT_SHOP_LABEL = "Tater Shop"
RETIRED_PLUGIN_IDS = {
    "web_search",
    "send_message",
    "notify_discord",
    "notify_irc",
    "notify_matrix",
    "notify_homeassistant",
    "notify_ntfy",
    "notify_telegram",
    "notify_wordpress",
}
COMMON_PLATFORM_ORDER = [
    "discord",
    "webui",
    "homeassistant",
    "homekit",
    "irc",
    "matrix",
    "telegram",
    "wordpress",
    "xbmc",
    "automation",
]
PLUGIN_MANAGER_FLASH_KEY = "plugin_manager_flash_messages"

redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)


def get_registry():
    return plugin_registry_mod.plugin_registry


def _enabled_missing_plugin_ids() -> list[str]:
    """
    Returns a list of plugin ids that are ENABLED in Redis but missing on disk.
    This is fast and lets us avoid showing UI unless we truly need to download.
    """

    def _to_bool(v) -> bool:
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("true", "1", "yes", "on")

    missing: list[str] = []
    seen = set()

    try:
        enabled_states = redis_client.hgetall("plugin_enabled") or {}
    except Exception:
        return missing

    for pid, raw in enabled_states.items():
        if isinstance(pid, (bytes, bytearray)):
            pid = pid.decode("utf-8", "ignore")
        pid = str(pid).strip()
        if not pid:
            continue
        if pid in RETIRED_PLUGIN_IDS:
            try:
                redis_client.hdel("plugin_enabled", pid)
            except Exception:
                pass
            continue

        if _to_bool(raw) and not is_plugin_installed(pid):
            if pid not in seen:
                seen.add(pid)
                missing.append(pid)

    return missing


def _normalize_manifest_url(url: str | None) -> str:
    return str(url or "").strip()


def _normalize_manifest_name(name: str | None) -> str:
    return str(name or "").strip()


def _default_shop_manifest_repo() -> dict[str, str]:
    return {"name": DEFAULT_SHOP_LABEL, "url": SHOP_MANIFEST_URL_DEFAULT}


def _safe_plugin_file_path(plugin_id: str) -> str:
    if not _re.fullmatch(r"[a-zA-Z0-9_\-]+", plugin_id or ""):
        raise ValueError("Invalid plugin id")
    return os.path.join(PLUGIN_DIR, f"{plugin_id}.py")


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
        out.append(default_repo)
        by_url[default_repo["url"]] = default_repo

    for raw in repos or []:
        entry = _normalize_manifest_repo_entry(raw)
        if not entry:
            continue

        url = entry["url"]
        if exclude_default and url == SHOP_MANIFEST_URL_DEFAULT:
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


def _load_legacy_manifest_url() -> str:
    legacy_url = _normalize_manifest_url(redis_client.get(LEGACY_SHOP_MANIFEST_URL_KEY))
    if legacy_url and legacy_url != SHOP_MANIFEST_URL_DEFAULT:
        return legacy_url
    return ""


def get_additional_shop_manifest_repos() -> list[dict[str, str]]:
    raw = redis_client.get(SHOP_MANIFEST_URLS_KEY)
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
    else:
        legacy_url = _load_legacy_manifest_url()
        if legacy_url:
            repos = [{"name": "", "url": legacy_url}]

    return _dedupe_manifest_repos(repos, exclude_default=True)


def get_additional_shop_manifest_urls() -> list[str]:
    return [repo["url"] for repo in get_additional_shop_manifest_repos()]


def get_configured_shop_manifest_repos() -> list[dict[str, str]]:
    return [_default_shop_manifest_repo(), *get_additional_shop_manifest_repos()]


def get_configured_shop_manifest_urls() -> list[str]:
    return [repo["url"] for repo in get_configured_shop_manifest_repos()]


def save_additional_shop_manifest_repos(repos) -> None:
    extras = _dedupe_manifest_repos(repos, exclude_default=True)
    redis_client.set(
        SHOP_MANIFEST_URLS_KEY,
        json.dumps([{"name": repo["name"], "url": repo["url"]} for repo in extras]),
    )


def save_additional_shop_manifest_urls(urls: list[str]) -> None:
    save_additional_shop_manifest_repos([{"name": "", "url": url} for url in urls])


def fetch_shop_manifest(url: str) -> dict:
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()


def _manifest_items(manifest: dict) -> list[dict]:
    items = manifest.get("plugins") or manifest.get("items") or manifest.get("data") or []
    if not isinstance(items, list):
        raise ValueError("Manifest format unexpected (expected list under plugins/items/data).")
    return [item for item in items if isinstance(item, dict)]


def _manifest_source_label(url: str, manifest: dict | None = None, configured_name: str | None = None) -> str:
    if url == SHOP_MANIFEST_URL_DEFAULT:
        return DEFAULT_SHOP_LABEL

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


def load_shop_catalog(manifest_sources=None) -> tuple[list[dict], list[str]]:
    repo_entries = (
        get_configured_shop_manifest_repos()
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
        except Exception as e:
            errors.append(f"{_manifest_source_label(url, configured_name=configured_name)}: {e}")
            continue

        for raw_item in items:
            plugin_id = str(raw_item.get("id") or "").strip()
            if not plugin_id or plugin_id in seen_ids:
                continue

            item = dict(raw_item)
            item["_source_manifest_url"] = url
            item["_source_label"] = source_label
            seen_ids.add(plugin_id)
            merged_items.append(item)

    return merged_items, errors


def is_plugin_installed(plugin_id: str) -> bool:
    try:
        return os.path.exists(_safe_plugin_file_path(plugin_id))
    except Exception:
        return False


def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def install_plugin_from_shop_item(item: dict, manifest_url: str | None = None) -> tuple[bool, str]:
    """
    Downloads a plugin .py from the shop manifest entry, verifies sha256 if provided,
    and writes it to PLUGIN_DIR as <id>.py.
    Supports relative 'entry' paths.
    """
    try:
        plugin_id = (item.get("id") or "").strip()
        entry = (item.get("entry") or "").strip()
        expected_sha = (item.get("sha256") or "").strip().lower()
        source_manifest_url = _normalize_manifest_url(manifest_url or item.get("_source_manifest_url"))

        if not plugin_id:
            return False, "Manifest item missing 'id'."
        if not entry:
            return False, f"{plugin_id}: manifest item missing 'entry'."
        if not source_manifest_url:
            return False, f"{plugin_id}: manifest source URL is missing."

        # Resolve relative paths against the manifest URL.
        entry = entry.lstrip("/")
        full_url = urljoin(source_manifest_url, entry)

        path = _safe_plugin_file_path(plugin_id)
        os.makedirs(PLUGIN_DIR, exist_ok=True)

        r = requests.get(full_url, timeout=30)
        r.raise_for_status()
        data = r.content

        if expected_sha:
            got = _sha256_bytes(data)
            if got.lower() != expected_sha:
                return False, f"SHA256 mismatch for {plugin_id}. expected={expected_sha} got={got}"

        try:
            text = data.decode("utf-8")
        except Exception:
            return False, f"{plugin_id}: downloaded file is not valid UTF-8 text."

        if "class " not in text and "def " not in text:
            return False, f"{plugin_id}: file does not look like a python plugin."

        with open(path, "w", encoding="utf-8") as f:
            f.write(text)

        return True, f"Installed {plugin_id}"
    except Exception as e:
        return False, f"Install failed: {e}"


def uninstall_plugin_file(plugin_id: str) -> tuple[bool, str]:
    """
    Remove only the plugin .py file.
    Do NOT clear Redis settings.
    """
    try:
        path = _safe_plugin_file_path(plugin_id)
        if not os.path.exists(path):
            return True, "Plugin file not found (already removed)."

        os.remove(path)
        return True, f"Removed {path}"
    except Exception as e:
        return False, f"Uninstall failed: {e}"


def clear_plugin_redis_data(plugin_id: str, category_hint: str | None = None) -> tuple[bool, str]:
    """
    Best-effort cleanup for plugin-related Redis keys.

    What we delete:
      - plugin_settings:<category> (if we can determine the category)
      - plugin_enabled hash field for this plugin_id
    """
    try:
        deleted = []

        category = (category_hint or "").strip() or None
        if not category:
            loaded = get_registry().get(plugin_id)
            category = getattr(loaded, "settings_category", None) if loaded else None

        if category:
            settings_key = f"plugin_settings:{category}"
            if redis_client.exists(settings_key):
                redis_client.delete(settings_key)
                deleted.append(settings_key)

        if redis_client.hexists("plugin_enabled", plugin_id):
            redis_client.hdel("plugin_enabled", plugin_id)
            deleted.append(f"plugin_enabled[{plugin_id}]")

        if deleted:
            return True, "Deleted: " + ", ".join(deleted)

        return True, "No Redis keys found for this plugin."
    except Exception as e:
        return False, f"Redis cleanup failed: {e}"


def _refresh_plugins_after_fs_change():
    plugin_registry_mod.reload_plugins()


def _semver_tuple(v: str) -> tuple[int, int, int]:
    if not v:
        return (0, 0, 0)

    v = str(v).strip().lower()
    if v.startswith("v"):
        v = v[1:].strip()

    match = _re.match(r"^([0-9]+(\.[0-9]+){0,2})", v)
    core = match.group(1) if match else "0.0.0"
    parts = (core.split(".") + ["0", "0", "0"])[:3]

    try:
        return (int(parts[0]), int(parts[1]), int(parts[2]))
    except Exception:
        return (0, 0, 0)


def _get_installed_version(plugin_id: str) -> str:
    if not plugin_id:
        return "0.0.0"

    loaded = get_registry().get(plugin_id)
    if not loaded:
        return "0.0.0"

    version = (
        getattr(loaded, "version", None)
        or getattr(loaded, "__version__", None)
        or getattr(loaded, "plugin_version", None)
    )
    version = str(version).strip() if version is not None else ""
    return version or "0.0.0"


def _normalize_platform_alias(platform_name: str) -> str:
    normalized = str(platform_name or "").strip().lower()
    alias_map = {
        "automations": "automation",
        "ha_automation": "automation",
        "ha_automations": "automation",
    }
    return alias_map.get(normalized, normalized)


def _platform_display_label(platform_name: str) -> str:
    labels = {
        "webui": "WebUI",
        "homeassistant": "Home Assistant",
        "homekit": "HomeKit",
        "xbmc": "XBMC",
        "automation": "Automations",
    }
    normalized = _normalize_platform_alias(platform_name)
    return labels.get(normalized, normalized.title())


def _normalize_plats(plats) -> list[str]:
    if not plats:
        return []

    raw_items: list[str] = []

    def _collect(raw_value):
        if not raw_value:
            return
        if isinstance(raw_value, str):
            for part in _re.split(r"[\s,]+", raw_value.strip()):
                normalized = str(part).strip().lower()
                if normalized:
                    raw_items.append(normalized)
            return
        if isinstance(raw_value, (list, tuple, set)):
            for part in raw_value:
                _collect(part)
            return

        normalized = str(raw_value).strip().lower()
        if normalized:
            raw_items.append(normalized)

    _collect(plats)

    seen = set()
    out: list[str] = []
    for platform_name in expand_plugin_platforms(raw_items):
        normalized = _normalize_platform_alias(platform_name)
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def _get_item_platforms(item):
    pid = (item.get("id") or "").strip()
    if pid and is_plugin_installed(pid):
        loaded = get_registry().get(pid)
        if loaded:
            loaded_platforms = getattr(loaded, "platforms", []) or []
            normalized = _normalize_plats(loaded_platforms)
            if normalized:
                return normalized

    return _normalize_plats(item.get("platforms") or item.get("platform") or [])


def _get_item_display_platforms(item) -> str:
    platforms = _ordered_platforms(set(_get_item_platforms(item)))
    return ", ".join(_platform_display_label(platform) for platform in platforms) if platforms else "(not provided)"


def _get_loaded_plugin_display_name(plugin, fallback_id: str) -> str:
    if not plugin:
        return fallback_id
    return (
        getattr(plugin, "plugin_name", None)
        or getattr(plugin, "pretty_name", None)
        or getattr(plugin, "name", None)
        or fallback_id
    )


def _get_loaded_plugin_description(plugin) -> str:
    if not plugin:
        return ""
    return getattr(plugin, "plugin_dec", None) or getattr(plugin, "description", "") or ""


def _installed_plugin_ids() -> list[str]:
    installed_ids = set()

    if os.path.isdir(PLUGIN_DIR):
        for filename in os.listdir(PLUGIN_DIR):
            if not filename.endswith(".py") or filename == "__init__.py":
                continue
            installed_ids.add(filename[:-3])

    installed_ids.update(str(plugin_id).strip() for plugin_id in get_registry().keys() if str(plugin_id).strip())

    return sorted(installed_ids)


def _ordered_platforms(platform_names: set[str]) -> list[str]:
    normalized = {
        str(platform_name or "").strip().lower()
        for platform_name in (platform_names or set())
        if str(platform_name or "").strip()
    }
    return sorted(normalized)


def _entry_platforms(entry: dict) -> list[str]:
    loaded = entry.get("loaded")
    catalog_item = entry.get("catalog_item")

    platforms = []
    if loaded:
        platforms = _normalize_plats(getattr(loaded, "platforms", []) or [])
    if not platforms and catalog_item:
        platforms = _get_item_platforms(catalog_item)
    return _ordered_platforms(set(platforms))


def _build_installed_entries(catalog_items: list[dict]) -> list[dict]:
    catalog_by_id = {
        str(item.get("id") or "").strip(): item
        for item in catalog_items
        if str(item.get("id") or "").strip()
    }

    installed_entries = []
    for plugin_id in _installed_plugin_ids():
        loaded = get_registry().get(plugin_id)
        catalog_item = catalog_by_id.get(plugin_id)
        display_name = _get_loaded_plugin_display_name(
            loaded,
            (catalog_item.get("name") if catalog_item else None) or plugin_id,
        )
        description = _get_loaded_plugin_description(loaded) or (
            (catalog_item.get("description") or "").strip() if catalog_item else ""
        )
        installed_ver = _get_installed_version(plugin_id)
        store_ver = (catalog_item.get("version") or "").strip() if catalog_item else ""
        source_label = (catalog_item.get("_source_label") or "Local plugin").strip() if catalog_item else "Local plugin"
        update_available = bool(catalog_item and _semver_tuple(store_ver) > _semver_tuple(installed_ver))
        platforms = _entry_platforms({"loaded": loaded, "catalog_item": catalog_item})
        platforms_str = ", ".join(_platform_display_label(platform_name) for platform_name in platforms)
        if not platforms_str:
            platforms_str = _get_item_display_platforms(catalog_item or {})

        installed_entries.append(
            {
                "id": plugin_id,
                "loaded": loaded,
                "catalog_item": catalog_item,
                "display_name": display_name,
                "description": description,
                "installed_ver": installed_ver,
                "store_ver": store_ver,
                "source_label": source_label,
                "update_available": update_available,
                "platforms": platforms,
                "platforms_str": platforms_str,
            }
        )

    installed_entries.sort(key=lambda item: item["display_name"].lower())
    return installed_entries


def _queue_plugin_manager_messages(messages: list[dict]) -> None:
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
        st.session_state[PLUGIN_MANAGER_FLASH_KEY] = normalized


def _render_plugin_manager_messages() -> None:
    messages = st.session_state.pop(PLUGIN_MANAGER_FLASH_KEY, None)
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


def _update_plugin_entry(entry: dict) -> tuple[bool, str]:
    plugin_id = str(entry.get("id") or "").strip()
    catalog_item = entry.get("catalog_item")
    if not plugin_id:
        return False, "Plugin id is missing."
    if not isinstance(catalog_item, dict):
        return False, f"{plugin_id}: no catalog update source is available."

    ok, msg = install_plugin_from_shop_item(catalog_item)
    if not ok:
        return False, msg

    installed_ver = str(entry.get("installed_ver") or "0.0.0").strip() or "0.0.0"
    store_ver = str(entry.get("store_ver") or installed_ver).strip() or installed_ver
    display_name = str(entry.get("display_name") or plugin_id).strip() or plugin_id
    return True, f"{display_name} updated {installed_ver} -> {store_ver}"


def _update_plugin_entries(entries: list[dict]) -> tuple[list[str], list[str]]:
    updated: list[str] = []
    failed: list[str] = []

    for entry in entries:
        ok, msg = _update_plugin_entry(entry)
        if ok:
            updated.append(str(entry.get("display_name") or entry.get("id") or "").strip() or msg)
        else:
            failed.append(msg)

    if updated:
        _refresh_plugins_after_fs_change()

    return updated, failed


def auto_restore_missing_plugins(
    manifest_urls: list[str] | str | None = None,
    progress_cb=None,
) -> tuple[bool, list[str], list[str]]:
    """
    Restore any plugins that are ENABLED in Redis but missing on disk.
    Uses the configured manifests as the source of install URLs.

    progress_cb (optional): callable(progress_float_0_to_1, status_text)
    """
    enabled_missing: list[str] = []
    restored: list[str] = []
    changed = False

    if manifest_urls is None:
        manifest_url_list = get_configured_shop_manifest_urls()
    elif isinstance(manifest_urls, str):
        manifest_url_list = [manifest_urls]
    else:
        manifest_url_list = manifest_urls

    try:
        enabled_states = redis_client.hgetall("plugin_enabled") or {}
    except Exception as e:
        logging.error(f"[restore] Failed to read plugin_enabled: {e}")
        return changed, restored, enabled_missing

    for plugin_id, raw in enabled_states.items():
        enabled = str(raw).lower() == "true"
        if enabled and not is_plugin_installed(plugin_id):
            enabled_missing.append(plugin_id)

    if not enabled_missing:
        return changed, restored, enabled_missing

    total = len(enabled_missing)
    if progress_cb:
        try:
            progress_cb(0.0, f"Found {total} enabled plugin(s) missing - preparing downloads...")
        except Exception:
            pass

    catalog_items, catalog_errors = load_shop_catalog(manifest_url_list)
    by_id = {
        str(item.get("id") or "").strip(): item
        for item in catalog_items
        if str(item.get("id") or "").strip()
    }

    if catalog_errors:
        logging.warning(f"[restore] Failed to load some plugin repos: {catalog_errors}")

    for idx, plugin_id in enumerate(enabled_missing, start=1):
        item = by_id.get(plugin_id)
        if not item:
            if catalog_errors:
                logging.warning(
                    f"[restore] {plugin_id} enabled but not found in loaded manifests; preserving enable state because some repos failed"
                )
                if progress_cb:
                    try:
                        progress_cb(
                            (idx - 1) / max(1, total),
                            f"{plugin_id} not found in loaded repos; keeping enable state because some repos failed ({idx}/{total})",
                        )
                    except Exception:
                        pass
                continue

            logging.error(f"[restore] {plugin_id} enabled but not found in manifest")
            try:
                redis_client.hdel("plugin_enabled", plugin_id)
                logging.info(f"[restore] Removed stale plugin_enabled key for {plugin_id}")
            except Exception as e:
                logging.error(f"[restore] Failed to remove stale plugin_enabled key for {plugin_id}: {e}")
            if progress_cb:
                try:
                    progress_cb(
                        (idx - 1) / max(1, total),
                        f"{plugin_id} missing and not in manifests; removed stale enable key ({idx}/{total})",
                    )
                except Exception:
                    pass
            continue

        if progress_cb:
            try:
                progress_cb((idx - 1) / max(1, total), f"Downloading {plugin_id}... ({idx}/{total})")
            except Exception:
                pass

        ok, msg = install_plugin_from_shop_item(item)
        if ok:
            restored.append(plugin_id)
            changed = True
            logging.info(f"[restore] {plugin_id}: {msg}")
        else:
            logging.error(f"[restore] {plugin_id}: {msg}")

        if progress_cb:
            try:
                progress_cb(idx / max(1, total), f"Finished {plugin_id} ({idx}/{total})")
            except Exception:
                pass

    return changed, restored, enabled_missing


def ensure_plugins_ready(progress_cb=None):
    """
    Ensure any ENABLED plugins that are missing on disk are restored from the shop.

    progress_cb (optional): callable(progress_float_0_to_1, status_text)
    """
    os.makedirs(PLUGIN_DIR, exist_ok=True)

    shop_urls = get_configured_shop_manifest_urls()
    if not shop_urls:
        if progress_cb:
            try:
                progress_cb(1.0, "Plugin shop manifest URLs are not configured.")
            except Exception:
                pass
        return

    missing = _enabled_missing_plugin_ids()
    if not missing:
        if progress_cb:
            try:
                progress_cb(1.0, "All enabled plugins are present.")
            except Exception:
                pass
        return

    if progress_cb:
        try:
            progress_cb(0.0, f"Restoring {len(missing)} missing plugin(s) from {len(shop_urls)} repo(s)...")
        except Exception:
            pass

    changed, restored, enabled_missing = auto_restore_missing_plugins(
        shop_urls,
        progress_cb=progress_cb,
    )

    if changed:
        if progress_cb:
            try:
                progress_cb(0.98, "Reloading plugins...")
            except Exception:
                pass
        _refresh_plugins_after_fs_change()

    if progress_cb and not restored and enabled_missing:
        try:
            progress_cb(1.0, "Missing plugins could not be restored from the configured repos.")
        except Exception:
            pass
    elif progress_cb:
        try:
            progress_cb(1.0, "")
        except Exception:
            pass


def _render_catalog_warnings(catalog_errors: list[str]):
    if not catalog_errors:
        return

    st.warning("Some plugin repos could not be loaded.")
    for error in catalog_errors:
        st.caption(error)


def _render_plugin_store_tab(catalog_items: list[dict], catalog_errors: list[str], manifest_repos: list[dict[str, str]]):
    _render_catalog_warnings(catalog_errors)

    available_items = [
        item for item in catalog_items
        if not is_plugin_installed((item.get("id") or "").strip())
    ]

    all_platforms = set()
    for item in available_items:
        for platform_name in _get_item_platforms(item):
            all_platforms.add(platform_name)

    ordered = _ordered_platforms(all_platforms)

    search_q = st.text_input(
        "Search available plugins",
        value="",
        placeholder="Search name, id, description...",
        key="plugin_manager_store_search",
    ).strip().lower()

    search_filtered_items = []
    for item in available_items:
        plugin_id = (item.get("id") or "").strip()
        name = (item.get("name") or plugin_id).strip()
        description = (item.get("description") or "").strip()

        if search_q:
            haystack = f"{plugin_id}\n{name}\n{description}".lower()
            if search_q not in haystack:
                continue

        search_filtered_items.append(item)

    if not search_filtered_items:
        st.caption(
            f"Showing 0 of {len(available_items)} available plugin(s) across {len(manifest_repos)} repo(s)."
        )
        st.info("No downloadable plugins match the current filters.")
        return

    tab_labels = ["All", *[_platform_display_label(platform_name) for platform_name in ordered]]
    tab_views = st.tabs(tab_labels)

    for idx, tab in enumerate(tab_views):
        selected_platform = None if idx == 0 else ordered[idx - 1]
        if selected_platform is None:
            filtered_items = search_filtered_items
        else:
            filtered_items = [
                item for item in search_filtered_items
                if selected_platform in _get_item_platforms(item)
            ]

        with tab:
            platform_label = "all platforms" if selected_platform is None else _platform_display_label(selected_platform)
            st.caption(
                f"Showing {len(filtered_items)} of {len(available_items)} available plugin(s) across {len(manifest_repos)} repo(s) for {platform_label}."
            )

            if not filtered_items:
                st.info("No downloadable plugins match this platform.")
                continue

            for item in filtered_items:
                plugin_id = (item.get("id") or "").strip()
                name = (item.get("name") or plugin_id).strip()
                description = (item.get("description") or "").strip()
                min_ver = (item.get("min_tater_version") or "0.0.0").strip()
                store_ver = (item.get("version") or "0.0.0").strip()
                source_label = (item.get("_source_label") or "Custom Repo").strip()
                platforms_str = _get_item_display_platforms(item)

                with st.container(border=True):
                    st.subheader(name)
                    st.caption(
                        f"ID: {plugin_id} | version: {store_ver} | min tater: {min_ver} | source: {source_label}"
                    )

                    if description:
                        st.write(description)

                    st.caption(f"Platforms: {platforms_str}")

                    if st.button("Install", key=f"plugin_manager_install_{selected_platform or 'all'}_{plugin_id}"):
                        ok, msg = install_plugin_from_shop_item(item)
                        if ok:
                            st.success(msg)
                            _refresh_plugins_after_fs_change()
                            st.rerun()
                        else:
                            st.error(msg)


def _render_installed_plugins_tab(catalog_items: list[dict], catalog_errors: list[str]):
    _render_catalog_warnings(catalog_errors)
    installed_entries = _build_installed_entries(catalog_items)

    all_platforms = set()
    for entry in installed_entries:
        all_platforms.update(entry["platforms"])
    ordered = _ordered_platforms(all_platforms)

    search_q = st.text_input(
        "Search installed plugins",
        value="",
        placeholder="Search name, id, description...",
        key="plugin_manager_installed_search",
    ).strip().lower()

    search_filtered_entries = []
    for entry in installed_entries:
        plugin_id = entry["id"]
        display_name = entry["display_name"]
        description = entry["description"]

        if search_q:
            haystack = f"{plugin_id}\n{display_name}\n{description}".lower()
            if search_q not in haystack:
                continue

        search_filtered_entries.append(entry)

    if not search_filtered_entries:
        st.caption(f"Showing 0 of {len(installed_entries)} installed plugin(s).")
        st.info("No installed plugins match the current filters.")
        return

    tab_labels = ["All", *[_platform_display_label(platform_name) for platform_name in ordered]]
    tab_views = st.tabs(tab_labels)

    for idx, tab in enumerate(tab_views):
        selected_platform = None if idx == 0 else ordered[idx - 1]
        if selected_platform is None:
            filtered_entries = search_filtered_entries
        else:
            filtered_entries = []
            for entry in search_filtered_entries:
                if selected_platform in entry["platforms"]:
                    filtered_entries.append(entry)

        with tab:
            platform_label = "all platforms" if selected_platform is None else _platform_display_label(selected_platform)
            st.caption(
                f"Showing {len(filtered_entries)} of {len(installed_entries)} installed plugin(s) for {platform_label}."
            )

            if not filtered_entries:
                st.info("No installed plugins match this platform.")
                continue

            for entry in filtered_entries:
                plugin_id = entry["id"]
                loaded = entry["loaded"]
                catalog_item = entry["catalog_item"]
                display_name = entry["display_name"]
                description = entry["description"]

                installed_ver = entry["installed_ver"]
                store_ver = entry["store_ver"]
                source_label = entry["source_label"]
                update_available = bool(entry["update_available"])
                platforms_str = entry["platforms_str"]

                tab_token = selected_platform or "all"
                purge_key = f"plugin_manager_purge_{tab_token}_{plugin_id}"

                with st.container(border=True):
                    st.subheader(display_name)

                    meta_parts = [f"ID: {plugin_id}", f"installed: {installed_ver or '0.0.0'}"]
                    if store_ver:
                        meta_parts.append(f"store: {store_ver}")
                    meta_parts.append(f"source: {source_label}")
                    st.caption(" | ".join(meta_parts))

                    if description:
                        st.write(description)

                    st.caption(f"Platforms: {platforms_str}")

                    if not loaded:
                        st.caption("Status: file exists on disk but the plugin is not currently loaded in the registry.")

                    controls = st.columns([1, 1, 3])
                    if update_available:
                        if controls[0].button("Update", key=f"plugin_manager_update_{tab_token}_{plugin_id}"):
                            ok, msg = _update_plugin_entry(entry)
                            if ok:
                                _refresh_plugins_after_fs_change()
                                _queue_plugin_manager_messages([{"level": "success", "text": msg}])
                                st.rerun()
                            else:
                                st.error(msg)
                    else:
                        controls[0].button("Up to date", disabled=True, key=f"plugin_manager_uptodate_{tab_token}_{plugin_id}")

                    purge_redis = controls[2].checkbox("Delete Data?", value=False, key=purge_key)

                    if controls[1].button("Remove", key=f"plugin_manager_remove_{tab_token}_{plugin_id}"):
                        category_hint = getattr(loaded, "settings_category", None) if loaded else None

                        ok, msg = uninstall_plugin_file(plugin_id)
                        if ok:
                            st.success(msg)

                            try:
                                set_plugin_enabled(plugin_id, False)
                            except Exception:
                                pass

                            if purge_redis:
                                ok2, msg2 = clear_plugin_redis_data(plugin_id, category_hint=category_hint)
                                if ok2:
                                    st.success(f"Redis cleanup: {msg2}")
                                else:
                                    st.error(msg2)

                            _refresh_plugins_after_fs_change()
                            st.rerun()
                        else:
                            st.error(msg)


def _render_updates_tab(catalog_items: list[dict], catalog_errors: list[str]):
    _render_catalog_warnings(catalog_errors)

    installed_entries = _build_installed_entries(catalog_items)
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
    if action_col.button("Update All", key="plugin_manager_update_all", disabled=not update_entries):
        with st.spinner(f"Updating {len(update_entries)} plugin(s)..."):
            updated, failed = _update_plugin_entries(update_entries)
        flash_messages = []
        if updated:
            updated_text = ", ".join(updated[:8])
            if len(updated) > 8:
                updated_text += f", and {len(updated) - 8} more"
            flash_messages.append(
                {"level": "success", "text": f"Updated {len(updated)} plugin(s): {updated_text}"}
            )
        if failed:
            failed_text = "; ".join(failed[:4])
            if len(failed) > 4:
                failed_text += f"; and {len(failed) - 4} more"
            flash_messages.append(
                {"level": "error", "text": f"{len(failed)} plugin update(s) failed: {failed_text}"}
            )
        if not flash_messages:
            flash_messages.append({"level": "info", "text": "No plugin updates were available."})
        _queue_plugin_manager_messages(flash_messages)
        st.rerun()

    if update_entries:
        text_col.caption("Only plugins with a newer catalog version are listed below. Update one at a time or update all in one pass.")
    else:
        text_col.caption("No catalog-backed plugin updates are currently available.")

    if not update_entries:
        if local_only_entries:
            st.info("All catalog-backed plugins are up to date. Some installed plugins are local-only and do not have a catalog update source.")
        else:
            st.success("All installed catalog-backed plugins are up to date.")
        return

    all_platforms = set()
    for entry in update_entries:
        all_platforms.update(entry["platforms"])
    ordered = _ordered_platforms(all_platforms)

    search_q = st.text_input(
        "Search updates",
        value="",
        placeholder="Search name, id, description...",
        key="plugin_manager_updates_search",
    ).strip().lower()

    search_filtered_entries = []
    for entry in update_entries:
        haystack = f"{entry['id']}\n{entry['display_name']}\n{entry['description']}".lower()
        if search_q and search_q not in haystack:
            continue
        search_filtered_entries.append(entry)

    if not search_filtered_entries:
        st.caption(f"Showing 0 of {len(update_entries)} plugin(s) with available updates.")
        st.info("No plugin updates match the current filters.")
        return

    tab_labels = ["All", *[_platform_display_label(platform_name) for platform_name in ordered]]
    tab_views = st.tabs(tab_labels)

    for idx, tab in enumerate(tab_views):
        selected_platform = None if idx == 0 else ordered[idx - 1]
        if selected_platform is None:
            filtered_entries = search_filtered_entries
        else:
            filtered_entries = [
                entry for entry in search_filtered_entries if selected_platform in entry["platforms"]
            ]

        with tab:
            platform_label = "all platforms" if selected_platform is None else _platform_display_label(selected_platform)
            st.caption(
                f"Showing {len(filtered_entries)} of {len(update_entries)} plugin(s) with available updates for {platform_label}."
            )

            if not filtered_entries:
                st.info("No plugin updates match this platform.")
                continue

            for entry in filtered_entries:
                plugin_id = entry["id"]
                tab_token = selected_platform or "all"

                with st.container(border=True):
                    st.subheader(entry["display_name"])
                    st.caption(
                        " | ".join(
                            [
                                f"ID: {plugin_id}",
                                f"installed: {entry['installed_ver'] or '0.0.0'}",
                                f"store: {entry['store_ver'] or '0.0.0'}",
                                f"source: {entry['source_label']}",
                            ]
                        )
                    )

                    if entry["description"]:
                        st.write(entry["description"])

                    st.caption(f"Platforms: {entry['platforms_str']}")

                    if st.button("Update", key=f"plugin_manager_updates_update_{tab_token}_{plugin_id}"):
                        ok, msg = _update_plugin_entry(entry)
                        if ok:
                            _refresh_plugins_after_fs_change()
                            _queue_plugin_manager_messages([{"level": "success", "text": msg}])
                            st.rerun()
                        else:
                            st.error(msg)


def _render_settings_tab(catalog_errors: list[str], manifest_repos: list[dict[str, str]]):
    st.caption("The default Tater Shop is always enabled. Add optional names for extra repos to control the source label shown in the store.")

    default_name_col, default_url_col = st.columns([1, 2])
    with default_name_col:
        st.text_input(
            "Default repo name",
            value=DEFAULT_SHOP_LABEL,
            disabled=True,
            key="plugin_manager_default_repo_name",
        )
    with default_url_col:
        st.text_input(
            "Default manifest URL",
            value=SHOP_MANIFEST_URL_DEFAULT,
            disabled=True,
            key="plugin_manager_default_manifest_url",
        )

    extra_repos = get_additional_shop_manifest_repos()
    if "plugin_manager_repo_form_count" not in st.session_state:
        st.session_state["plugin_manager_repo_form_count"] = max(1, len(extra_repos))
        for idx, repo in enumerate(extra_repos):
            st.session_state[f"plugin_manager_repo_name_{idx}"] = repo.get("name", "")
            st.session_state[f"plugin_manager_repo_url_{idx}"] = repo.get("url", "")

    st.caption("Additional repos")
    for idx in range(max(1, int(st.session_state.get("plugin_manager_repo_form_count", 1)))):
        name_key = f"plugin_manager_repo_name_{idx}"
        url_key = f"plugin_manager_repo_url_{idx}"
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
                placeholder="https://example.com/manifest.json",
            )

    add_col, remove_col, save_col, refresh_col = st.columns([1, 1, 1, 1])

    if add_col.button("Add Repo", key="plugin_manager_add_repo"):
        next_idx = int(st.session_state.get("plugin_manager_repo_form_count", 1))
        st.session_state["plugin_manager_repo_form_count"] = next_idx + 1
        st.session_state[f"plugin_manager_repo_name_{next_idx}"] = ""
        st.session_state[f"plugin_manager_repo_url_{next_idx}"] = ""
        st.rerun()

    if remove_col.button("Remove Last", key="plugin_manager_remove_repo"):
        count = max(1, int(st.session_state.get("plugin_manager_repo_form_count", 1)))
        if count > 1:
            last_idx = count - 1
            st.session_state.pop(f"plugin_manager_repo_name_{last_idx}", None)
            st.session_state.pop(f"plugin_manager_repo_url_{last_idx}", None)
            st.session_state["plugin_manager_repo_form_count"] = count - 1
        else:
            st.session_state["plugin_manager_repo_name_0"] = ""
            st.session_state["plugin_manager_repo_url_0"] = ""
        st.rerun()

    if save_col.button("Save Repos", key="plugin_manager_save_repos"):
        parsed_repos = []
        row_count = max(1, int(st.session_state.get("plugin_manager_repo_form_count", 1)))
        for idx in range(row_count):
            name = _normalize_manifest_name(st.session_state.get(f"plugin_manager_repo_name_{idx}"))
            url = _normalize_manifest_url(st.session_state.get(f"plugin_manager_repo_url_{idx}"))

            if not name and not url:
                continue
            if not url:
                st.error(f"Repo {idx + 1} is missing a manifest URL.")
                return

            parsed_repos.append({"name": name, "url": url})

        save_additional_shop_manifest_repos(parsed_repos)
        st.success("Plugin repos saved.")
        st.rerun()

    if refresh_col.button("Refresh Catalog", key="plugin_manager_refresh_catalog"):
        st.rerun()

    st.caption(
        "Catalog merge order is fixed: the default Tater Shop loads first, then your additional repos. "
        "If two repos publish the same plugin id, the first one wins."
    )
    st.caption("Leave the repo name blank if you want Tater to fall back to the manifest name or URL.")
    st.caption(f"Configured repos: {len(manifest_repos)}")

    _render_catalog_warnings(catalog_errors)


def render_plugin_store_page():
    st.title("Verba Plugin Manager")
    st.caption("Install plugins from configured repos, manage installed plugins, and edit plugin repo settings.")
    _render_plugin_manager_messages()

    manifest_repos = get_configured_shop_manifest_repos()
    catalog_items, catalog_errors = load_shop_catalog(manifest_repos)

    store_tab, installed_tab, updates_tab, settings_tab = st.tabs(
        ["Plugin Store", "Installed Plugins", "Updates", "Settings"]
    )

    with store_tab:
        _render_plugin_store_tab(catalog_items, catalog_errors, manifest_repos)

    with installed_tab:
        _render_installed_plugins_tab(catalog_items, catalog_errors)

    with updates_tab:
        _render_updates_tab(catalog_items, catalog_errors)

    with settings_tab:
        _render_settings_tab(catalog_errors, manifest_repos)
