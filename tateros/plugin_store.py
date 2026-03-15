import hashlib
import json
import logging
import os
import re as _re
from urllib.parse import urljoin, urlparse

import redis
import requests

import plugin_registry as plugin_registry_mod
from plugin_kernel import expand_plugin_platforms

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
redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)


def get_registry():
    return plugin_registry_mod.get_registry()


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

    return _normalize_plats(
        item.get("portals")
        or item.get("portal")
        or []
    )


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

