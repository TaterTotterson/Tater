import hashlib
import json
import logging
import os
import re as _re
from urllib.parse import urljoin, urlparse

import requests
from helpers import redis_client

CORE_DIR = os.getenv("TATER_CORE_DIR", "cores")

CORE_SHOP_MANIFEST_URL_DEFAULT = os.getenv(
    "TATER_CORE_SHOP_MANIFEST_URL",
    "https://raw.githubusercontent.com/TaterTotterson/Tater_Shop/main/core_manifest.json",
)
CORE_SHOP_MANIFEST_URLS_KEY = "tater:core_shop_manifest_urls"
DEFAULT_CORE_SHOP_LABEL = "Tater Core Shop"

def _normalize_manifest_url(url: str | None) -> str:
    return str(url or "").strip()


def _normalize_manifest_name(name: str | None) -> str:
    return str(name or "").strip()


def _default_shop_manifest_repo() -> dict[str, str]:
    return {"name": DEFAULT_CORE_SHOP_LABEL, "url": CORE_SHOP_MANIFEST_URL_DEFAULT}


def _normalize_core_id(raw_id: str | None) -> str:
    platform_id = str(raw_id or "").strip().lower()
    if platform_id.endswith("_core"):
        platform_id = platform_id[: -len("_core")]
    return platform_id


def _safe_core_file_path(platform_id: str) -> str:
    normalized_id = _normalize_core_id(platform_id)
    if not _re.fullmatch(r"[a-zA-Z0-9_]+", normalized_id or ""):
        raise ValueError("Invalid core id")
    return os.path.join(CORE_DIR, f"{normalized_id}_core.py")


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
        if exclude_default and url == CORE_SHOP_MANIFEST_URL_DEFAULT:
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


def get_additional_core_shop_manifest_repos() -> list[dict[str, str]]:
    raw = redis_client.get(CORE_SHOP_MANIFEST_URLS_KEY)
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


def get_configured_core_shop_manifest_repos() -> list[dict[str, str]]:
    return _dedupe_manifest_repos(
        [_default_shop_manifest_repo(), *get_additional_core_shop_manifest_repos()],
        include_default=False,
    )


def get_configured_core_shop_manifest_urls() -> list[str]:
    return [repo["url"] for repo in get_configured_core_shop_manifest_repos() if repo.get("url")]


def save_additional_core_shop_manifest_repos(repos) -> None:
    extras = _dedupe_manifest_repos(repos, exclude_default=True)
    payload = json.dumps([{"name": repo["name"], "url": repo["url"]} for repo in extras])
    redis_client.set(CORE_SHOP_MANIFEST_URLS_KEY, payload)


def fetch_shop_manifest(url: str) -> dict:
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()


def _manifest_items(manifest: dict) -> list[dict]:
    if "cores" not in manifest:
        raise ValueError("Manifest missing cores.")
    items = manifest.get("cores") or []
    if not isinstance(items, list):
        raise ValueError("Manifest format unexpected (expected list under cores).")
    return [item for item in items if isinstance(item, dict)]


def _manifest_source_label(url: str, manifest: dict | None = None, configured_name: str | None = None) -> str:
    if url == CORE_SHOP_MANIFEST_URL_DEFAULT:
        return DEFAULT_CORE_SHOP_LABEL

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


def load_core_shop_catalog(manifest_sources=None) -> tuple[list[dict], list[str]]:
    repo_entries = (
        get_configured_core_shop_manifest_repos()
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
            platform_id = _normalize_core_id(raw_item.get("id"))
            if not platform_id or platform_id in seen_ids:
                continue

            item = dict(raw_item)
            item["id"] = platform_id
            item["_source_manifest_url"] = url
            item["_source_label"] = source_label
            seen_ids.add(platform_id)
            merged_items.append(item)

    return merged_items, errors


def is_core_installed(platform_id: str) -> bool:
    try:
        return os.path.exists(_safe_core_file_path(platform_id))
    except Exception:
        return False


def _sha256_bytes(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def install_core_from_shop_item(item: dict, manifest_url: str | None = None) -> tuple[bool, str]:
    """
    Downloads a platform .py from the manifest entry, verifies sha256 if provided,
    and writes it to CORE_DIR as <id>_core.py.
    """

    try:
        platform_id = _normalize_core_id(item.get("id"))
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

        path = _safe_core_file_path(platform_id)
        os.makedirs(CORE_DIR, exist_ok=True)

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
            return False, f"{platform_id}: file does not look like a runnable core module."

        with open(path, "w", encoding="utf-8") as file_obj:
            file_obj.write(text)

        return True, f"Installed {platform_id}"
    except Exception as exc:
        return False, f"Install failed: {exc}"


def _enabled_missing_core_ids() -> list[str]:
    """
    Returns core ids that should be running (redis <id>_core_running=true)
    but whose module file is missing from disk.
    """

    missing: list[str] = []
    seen = set()

    try:
        for raw_key in redis_client.scan_iter(match="*_core_running", count=200):
            state_key = str(raw_key or "").strip()
            if not state_key.endswith("_core_running"):
                continue

            enabled = str(redis_client.get(state_key) or "").strip().lower() == "true"
            if not enabled:
                continue

            module_key = state_key[: -len("_running")]
            if not module_key.endswith("_core"):
                continue

            platform_id = _normalize_core_id(module_key)
            if not platform_id or platform_id in seen:
                continue

            if not is_core_installed(platform_id):
                seen.add(platform_id)
                missing.append(platform_id)
    except Exception:
        return missing

    return missing


def auto_restore_missing_cores(
    manifest_urls: list[str] | str | None = None,
    progress_cb=None,
) -> tuple[bool, list[str], list[str]]:
    """
    Restore any cores that are enabled in Redis but missing on disk.
    """

    restored: list[str] = []
    changed = False

    if manifest_urls is None:
        manifest_url_list = get_configured_core_shop_manifest_urls()
    elif isinstance(manifest_urls, str):
        manifest_url_list = [manifest_urls]
    else:
        manifest_url_list = manifest_urls

    missing = _enabled_missing_core_ids()
    if not missing:
        return changed, restored, missing

    total = len(missing)
    if progress_cb:
        try:
            progress_cb(0.0, f"Found {total} enabled core(s) missing - preparing downloads...")
        except Exception:
            pass

    catalog_items, catalog_errors = load_core_shop_catalog(manifest_url_list)
    by_id = {
        str(item.get("id") or "").strip(): item
        for item in catalog_items
        if str(item.get("id") or "").strip()
    }

    if catalog_errors:
        logging.warning("[core-restore] Failed to load some core repos: %s", catalog_errors)

    for idx, platform_id in enumerate(missing, start=1):
        item = by_id.get(platform_id)
        if not item:
            state_key = f"{platform_id}_core_running"
            if catalog_errors:
                logging.warning(
                    "[core-restore] %s enabled but not found in loaded manifests; preserving running state because some repos failed",
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

            logging.error("[core-restore] %s enabled but not found in manifest", platform_id)
            try:
                redis_client.set(state_key, "false")
                logging.info("[core-restore] Disabled stale running key for %s", platform_id)
            except Exception as exc:
                logging.error("[core-restore] Failed to disable stale running key for %s: %s", platform_id, exc)

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

        ok, msg = install_core_from_shop_item(item)
        if ok:
            restored.append(platform_id)
            changed = True
            logging.info("[core-restore] %s: %s", platform_id, msg)
        else:
            logging.error("[core-restore] %s: %s", platform_id, msg)

        if progress_cb:
            try:
                progress_cb(idx / max(1, total), f"Finished {platform_id} ({idx}/{total})")
            except Exception:
                pass

    return changed, restored, missing


def ensure_cores_ready(progress_cb=None):
    """
    Ensure any cores marked running in Redis and missing on disk are restored
    from the configured core shop manifests.
    """

    os.makedirs(CORE_DIR, exist_ok=True)

    missing = _enabled_missing_core_ids()
    if not missing:
        if progress_cb:
            try:
                progress_cb(1.0, "All enabled cores are present.")
            except Exception:
                pass
        return

    shop_urls = get_configured_core_shop_manifest_urls()
    if not shop_urls:
        if progress_cb:
            try:
                progress_cb(1.0, "Core shop manifest URLs are not configured.")
            except Exception:
                pass
        return

    if progress_cb:
        try:
            progress_cb(0.0, f"Restoring {len(missing)} missing core(s) from {len(shop_urls)} repo(s)...")
        except Exception:
            pass

    _changed, restored, enabled_missing = auto_restore_missing_cores(
        shop_urls,
        progress_cb=progress_cb,
    )

    if progress_cb and not restored and enabled_missing:
        try:
            progress_cb(1.0, "Missing cores could not be restored from the configured repos.")
        except Exception:
            pass
    elif progress_cb:
        try:
            progress_cb(1.0, "")
        except Exception:
            pass


def _core_module_key(platform_id: str) -> str:
    normalized = _normalize_core_id(platform_id)
    return f"{normalized}_core" if normalized else ""


def _core_display_name(platform_id: str, fallback: str | None = None) -> str:
    if fallback:
        text = str(fallback).strip()
        if text:
            return text
    parts = [part for part in _normalize_core_id(platform_id).split("_") if part]
    if not parts:
        return str(platform_id or "").strip() or "Unknown Core"
    return " ".join(part.capitalize() for part in parts)


def _installed_core_ids() -> list[str]:
    installed_ids = set()
    if os.path.isdir(CORE_DIR):
        for filename in os.listdir(CORE_DIR):
            if filename == "__init__.py" or not filename.endswith("_core.py"):
                continue
            stem = filename[:-3]
            platform_id = _normalize_core_id(stem)
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


def _get_installed_core_version(platform_id: str) -> str:
    try:
        path = _safe_core_file_path(platform_id)
    except Exception:
        return "0.0.0"
    if not os.path.exists(path):
        return "0.0.0"
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            text = file_obj.read()
    except Exception:
        return "0.0.0"
    for key in ("__version__", "CORE_VERSION", "VERSION"):
        match = _re.search(rf"^\s*{_re.escape(key)}\s*=\s*['\"]([^'\"]+)['\"]", text, flags=_re.MULTILINE)
        if match:
            version = str(match.group(1) or "").strip()
            if version:
                return version
    return "0.0.0"


def uninstall_core_file(platform_id: str) -> tuple[bool, str]:
    try:
        path = _safe_core_file_path(platform_id)
        if not os.path.exists(path):
            return True, "Core file not found (already removed)."
        os.remove(path)
        return True, f"Removed {path}"
    except Exception as exc:
        return False, f"Uninstall failed: {exc}"


def clear_core_redis_data(platform_id: str, module_key: str | None = None) -> tuple[bool, str]:
    try:
        normalized_id = _normalize_core_id(platform_id)
        resolved_module_key = str(module_key or _core_module_key(normalized_id)).strip()
        if not resolved_module_key:
            return False, "Invalid core id."

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
        return True, "No Redis keys found for this core."
    except Exception as exc:
        return False, f"Redis cleanup failed: {exc}"


def _build_installed_core_entries(catalog_items: list[dict]) -> list[dict]:
    catalog_by_id = {
        str(item.get("id") or "").strip(): item
        for item in catalog_items
        if str(item.get("id") or "").strip()
    }

    entries = []
    for platform_id in _installed_core_ids():
        catalog_item = catalog_by_id.get(platform_id)
        display_name = _core_display_name(platform_id, fallback=(catalog_item or {}).get("name"))
        description = str((catalog_item or {}).get("description") or "").strip() or "Local core module."
        module_key = str((catalog_item or {}).get("module_key") or _core_module_key(platform_id)).strip()
        installed_ver = _get_installed_core_version(platform_id)
        store_ver = str((catalog_item or {}).get("version") or "").strip()
        source_label = str((catalog_item or {}).get("_source_label") or "Local core").strip()
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

