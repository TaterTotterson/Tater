import ast
import hashlib
import importlib
import json
import logging
import os
import re as _re
import sys
from pathlib import Path
from types import ModuleType
from urllib.parse import unquote, urljoin, urlparse

import requests

from helpers import redis_client

logger = logging.getLogger("integration_store")

INTEGRATION_DIR = os.getenv("TATER_INTEGRATION_DIR", "integrations")
INTEGRATION_BUILTIN_DIR = os.getenv("TATER_INTEGRATION_BUILTIN_DIR", "")
INTEGRATION_SHOP_MANIFEST_URLS_KEY = "tater:integration_shop_manifest_urls"
INTEGRATION_ENABLED_HASH = "tater:integration_enabled"
DEFAULT_INTEGRATION_SHOP_LABEL = "Tater Integration Shop"
DEFAULT_INTEGRATION_IDS = [
    "homeassistant",
    "hue",
    "aladdin",
    "homekit",
    "sonos",
    "unifi_network",
    "huggingface",
    "unifi_protect",
    "weather_api",
]
REQUIRED_INTEGRATION_IDS = {
    token.strip().lower()
    for token in os.getenv("TATER_REQUIRED_INTEGRATION_IDS", "").split(",")
    if token.strip()
}
KNOWN_INTEGRATION_REDIS_KEYS = {
    "homeassistant": {"hashes": ["homeassistant_settings"], "strings": []},
    "hue": {"hashes": ["hue_settings"], "strings": []},
    "aladdin": {"hashes": ["aladdin_settings"], "strings": []},
    "homekit": {
        "hashes": ["ecobee_homekit_settings", "ecobee_homekit_pairings"],
        "strings": [],
    },
    "ecobee_homekit": {
        "hashes": ["ecobee_homekit_settings", "ecobee_homekit_pairings"],
        "strings": [],
    },
    "sonos": {"hashes": ["sonos_settings"], "strings": []},
    "unifi_network": {
        "hashes": [],
        "strings": ["tater:unifi_network:base_url", "tater:unifi_network:api_key"],
    },
    "huggingface": {"hashes": ["huggingface_settings"], "strings": []},
    "unifi_protect": {
        "hashes": [],
        "strings": ["tater:unifi_protect:base_url", "tater:unifi_protect:api_key"],
    },
    "weather_api": {"hashes": ["weather_api_settings"], "strings": []},
    "google_search": {
        "hashes": ["google_search_settings", "verba_settings:Web Search"],
        "strings": ["tater:web_search:google_api_key", "tater:web_search:google_cx"],
    },
    "brave_search": {
        "hashes": ["brave_search_settings"],
        "strings": ["tater:web_search:brave_api_key"],
    },
    "searxng_search": {
        "hashes": ["searxng_search_settings"],
        "strings": ["tater:web_search:searxng_url", "tater:web_search:searxng_api_key"],
    },
    "serper_search": {
        "hashes": ["serper_search_settings"],
        "strings": ["tater:web_search:serper_api_key"],
    },
}


def _redis_text(value) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="ignore").strip()
        except Exception:
            return str(value or "").strip()
    return str(value or "").strip()


def ensure_integration_import_context() -> None:
    integration_dirs = _integration_dirs()
    for integration_dir in reversed(integration_dirs):
        parent = str(integration_dir.parent)
        if parent and parent not in sys.path:
            sys.path.insert(0, parent)

    package = sys.modules.get("integrations")
    if package is not None and not isinstance(package, ModuleType):
        sys.modules.pop("integrations", None)
        package = None

    importlib.invalidate_caches()
    if package is None:
        package = importlib.import_module("integrations")

    package_paths = getattr(package, "__path__", None)
    if package_paths is not None:
        normalized = {str(Path(path).resolve()) for path in package_paths}
        for integration_dir in reversed(integration_dirs):
            expected = str(integration_dir)
            if expected in normalized:
                continue
            try:
                package_paths.insert(0, expected)
            except AttributeError:
                package_paths.append(expected)
            normalized.add(expected)


def _normalize_manifest_url(url: str | None) -> str:
    return str(url or "").strip()


def _normalize_manifest_name(name: str | None) -> str:
    return str(name or "").strip()


def _default_local_manifest_url() -> str:
    configured_dir = str(os.getenv("TATER_LOCAL_INTEGRATION_SHOP_DIR") or "").strip()
    candidates = []
    if configured_dir:
        candidates.append(Path(configured_dir).expanduser() / "manifest.json")
    app_root = Path(__file__).resolve().parents[1]
    candidates.append(app_root.parent / "Tater_Integrations" / "manifest.json")
    for candidate in candidates:
        try:
            if candidate.exists():
                return candidate.resolve().as_uri()
        except Exception:
            continue
    return ""


def _default_manifest_url() -> str:
    local_url = _default_local_manifest_url()
    if local_url:
        return local_url
    return "https://raw.githubusercontent.com/TaterTotterson/Tater_Integrations/main/manifest.json"


INTEGRATION_SHOP_MANIFEST_URL_DEFAULT = os.getenv(
    "TATER_INTEGRATION_SHOP_MANIFEST_URL",
    _default_manifest_url(),
)


def _default_shop_manifest_repo() -> dict[str, str]:
    return {"name": DEFAULT_INTEGRATION_SHOP_LABEL, "url": INTEGRATION_SHOP_MANIFEST_URL_DEFAULT}


def _normalize_integration_id(raw_id: str | None) -> str:
    integration_id = str(raw_id or "").strip().lower()
    if integration_id.endswith("_integration"):
        integration_id = integration_id[: -len("_integration")]
    return integration_id


def _safe_integration_file_path(integration_id: str) -> str:
    normalized_id = _normalize_integration_id(integration_id)
    if not _re.fullmatch(r"[a-zA-Z0-9_]+", normalized_id or ""):
        raise ValueError("Invalid integration id")
    return os.path.join(INTEGRATION_DIR, f"{normalized_id}.py")


def _integration_dirs() -> list[Path]:
    dirs = [Path(INTEGRATION_DIR)]
    if INTEGRATION_BUILTIN_DIR:
        dirs.append(Path(INTEGRATION_BUILTIN_DIR))

    out: list[Path] = []
    seen = set()
    for raw in dirs:
        path = raw.expanduser().resolve()
        token = str(path)
        if token and token not in seen:
            seen.add(token)
            out.append(path)
    return out


def _integration_file_paths(integration_id: str) -> list[str]:
    primary = _safe_integration_file_path(integration_id)
    paths = [primary]
    if INTEGRATION_BUILTIN_DIR:
        normalized_id = _normalize_integration_id(integration_id)
        paths.append(os.path.join(INTEGRATION_BUILTIN_DIR, f"{normalized_id}.py"))
    out: list[str] = []
    seen = set()
    for path in paths:
        token = os.path.abspath(path)
        if token not in seen:
            seen.add(token)
            out.append(path)
    return out


def _boolish(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)


def get_integration_enabled(integration_id: str, *, default: bool = False) -> bool:
    normalized_id = _normalize_integration_id(integration_id)
    if not normalized_id:
        return bool(default)
    if normalized_id in REQUIRED_INTEGRATION_IDS:
        return True
    try:
        return _boolish(redis_client.hget(INTEGRATION_ENABLED_HASH, normalized_id), default)
    except Exception:
        return bool(default)


def set_integration_enabled(integration_id: str, enabled: bool) -> None:
    normalized_id = _normalize_integration_id(integration_id)
    if not normalized_id:
        raise ValueError("Invalid integration id")
    try:
        redis_client.hset(INTEGRATION_ENABLED_HASH, normalized_id, "true" if enabled else "false")
    except Exception as exc:
        raise RuntimeError(f"Failed to save integration enabled state: {exc}") from exc


def get_enabled_integration_ids() -> list[str]:
    ids = set(REQUIRED_INTEGRATION_IDS)
    try:
        raw = redis_client.hgetall(INTEGRATION_ENABLED_HASH) or {}
    except Exception:
        raw = {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            integration_id = _normalize_integration_id(key)
            if not integration_id:
                continue
            if _boolish(value, False):
                ids.add(integration_id)
    return sorted(ids)


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
        if exclude_default and url == INTEGRATION_SHOP_MANIFEST_URL_DEFAULT:
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


def get_additional_integration_shop_manifest_repos() -> list[dict[str, str]]:
    try:
        raw = redis_client.get(INTEGRATION_SHOP_MANIFEST_URLS_KEY)
    except Exception:
        raw = ""
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


def get_configured_integration_shop_manifest_repos() -> list[dict[str, str]]:
    return _dedupe_manifest_repos(
        [_default_shop_manifest_repo(), *get_additional_integration_shop_manifest_repos()],
        include_default=False,
    )


def get_configured_integration_shop_manifest_urls() -> list[str]:
    return [repo["url"] for repo in get_configured_integration_shop_manifest_repos() if repo.get("url")]


def save_additional_integration_shop_manifest_repos(repos) -> None:
    extras = _dedupe_manifest_repos(repos, exclude_default=True)
    payload = json.dumps([{"name": repo["name"], "url": repo["url"]} for repo in extras])
    redis_client.set(INTEGRATION_SHOP_MANIFEST_URLS_KEY, payload)


def _local_path_from_url(url: str) -> Path | None:
    parsed = urlparse(str(url or ""))
    if parsed.scheme == "file":
        return Path(unquote(parsed.path)).expanduser()
    if not parsed.scheme:
        path = Path(str(url or "")).expanduser()
        if path.exists():
            return path
    return None


def _read_url_bytes(url: str, *, timeout: int = 30) -> bytes:
    local_path = _local_path_from_url(url)
    if local_path is not None:
        return local_path.read_bytes()
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.content


def fetch_shop_manifest(url: str) -> dict:
    data = _read_url_bytes(url, timeout=15)
    text = data.decode("utf-8")
    manifest = json.loads(text)
    if not isinstance(manifest, dict):
        raise ValueError("Manifest root must be an object.")
    return manifest


def _manifest_items(manifest: dict) -> list[dict]:
    if "integrations" not in manifest:
        raise ValueError("Manifest missing integrations.")
    items = manifest.get("integrations") or []
    if not isinstance(items, list):
        raise ValueError("Manifest format unexpected (expected list under integrations).")
    return [item for item in items if isinstance(item, dict)]


def _manifest_source_label(url: str, manifest: dict | None = None, configured_name: str | None = None) -> str:
    if url == INTEGRATION_SHOP_MANIFEST_URL_DEFAULT:
        return DEFAULT_INTEGRATION_SHOP_LABEL

    configured_name = _normalize_manifest_name(configured_name)
    if configured_name:
        return configured_name

    if isinstance(manifest, dict):
        for key in ("name", "title", "shop_name", "repo_name"):
            value = str(manifest.get(key) or "").strip()
            if value:
                return value

    parsed = urlparse(url)
    if parsed.scheme == "file":
        parent = Path(unquote(parsed.path)).parent
        return parent.name or "Local integration repo"
    if parsed.netloc:
        path_parts = [part for part in parsed.path.split("/") if part]
        if len(path_parts) >= 2:
            return f"{parsed.netloc}/{path_parts[-2]}"
        return parsed.netloc

    return url


def load_integration_shop_catalog(manifest_sources=None) -> tuple[list[dict], list[str]]:
    repo_entries = (
        get_configured_integration_shop_manifest_repos()
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
            integration_id = _normalize_integration_id(raw_item.get("id"))
            if not integration_id or integration_id in seen_ids:
                continue

            item = dict(raw_item)
            item["id"] = integration_id
            item["_source_manifest_url"] = url
            item["_source_label"] = source_label
            seen_ids.add(integration_id)
            merged_items.append(item)

    return merged_items, errors


def is_integration_installed(integration_id: str) -> bool:
    try:
        return any(os.path.exists(path) for path in _integration_file_paths(integration_id))
    except Exception:
        return False


def _sha256_bytes(data: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(data)
    return digest.hexdigest()


def _resolve_entry_url(source_manifest_url: str, entry: str) -> str:
    source = _normalize_manifest_url(source_manifest_url)
    entry = str(entry or "").strip().lstrip("/")
    if not entry:
        return ""
    parsed = urlparse(source)
    if parsed.scheme in {"http", "https", "file"}:
        return urljoin(source, entry)
    source_path = Path(source).expanduser()
    base_dir = source_path.parent if source_path.suffix else source_path
    return str((base_dir / entry).resolve())


def install_integration_from_shop_item(item: dict, manifest_url: str | None = None) -> tuple[bool, str]:
    try:
        integration_id = _normalize_integration_id(item.get("id"))
        entry = str(item.get("entry") or "").strip()
        expected_sha = str(item.get("sha256") or "").strip().lower()
        source_manifest_url = _normalize_manifest_url(manifest_url or item.get("_source_manifest_url"))

        if not integration_id:
            return False, "Manifest item missing 'id'."
        if not entry:
            return False, f"{integration_id}: manifest item missing 'entry'."
        if not source_manifest_url:
            return False, f"{integration_id}: manifest source URL is missing."

        full_url = _resolve_entry_url(source_manifest_url, entry)
        if not full_url:
            return False, f"{integration_id}: integration entry URL could not be resolved."

        path = _safe_integration_file_path(integration_id)
        os.makedirs(INTEGRATION_DIR, exist_ok=True)

        data = _read_url_bytes(full_url, timeout=30)
        if expected_sha:
            got = _sha256_bytes(data)
            if got.lower() != expected_sha:
                return False, f"SHA256 mismatch for {integration_id}. expected={expected_sha} got={got}"

        try:
            text = data.decode("utf-8")
        except Exception:
            return False, f"{integration_id}: downloaded file is not valid UTF-8 text."

        if "INTEGRATION" not in text or "def " not in text:
            return False, f"{integration_id}: file does not look like a Tater integration module."

        with open(path, "w", encoding="utf-8") as file_obj:
            file_obj.write(text)

        importlib.invalidate_caches()
        return True, f"Installed {integration_id}"
    except Exception as exc:
        return False, f"Install failed: {exc}"


def _enabled_missing_integration_ids(integration_ids: list[str] | None = None) -> list[str]:
    required = integration_ids if integration_ids is not None else get_enabled_integration_ids()
    missing: list[str] = []
    seen = set()
    for raw_id in required or []:
        integration_id = _normalize_integration_id(raw_id)
        if not integration_id or integration_id in seen:
            continue
        seen.add(integration_id)
        if not is_integration_installed(integration_id):
            missing.append(integration_id)
    return missing


def auto_restore_missing_integrations(
    manifest_urls: list[str] | str | None = None,
    integration_ids: list[str] | None = None,
    progress_cb=None,
) -> tuple[bool, list[str], list[str]]:
    restored: list[str] = []
    changed = False

    if manifest_urls is None:
        manifest_url_list = get_configured_integration_shop_manifest_urls()
    elif isinstance(manifest_urls, str):
        manifest_url_list = [manifest_urls]
    else:
        manifest_url_list = manifest_urls

    missing = _enabled_missing_integration_ids(integration_ids)
    if not missing:
        return changed, restored, missing

    total = len(missing)
    if progress_cb:
        try:
            progress_cb(0.0, f"Found {total} enabled integration(s) missing - preparing downloads...")
        except Exception:
            pass

    catalog_items, catalog_errors = load_integration_shop_catalog(manifest_url_list)
    by_id = {
        str(item.get("id") or "").strip(): item
        for item in catalog_items
        if str(item.get("id") or "").strip()
    }

    if catalog_errors:
        logger.warning("[integration-restore] Failed to load some integration repos: %s", catalog_errors)

    for idx, integration_id in enumerate(missing, start=1):
        item = by_id.get(integration_id)
        if not item:
            logger.error("[integration-restore] %s missing and not found in manifests", integration_id)
            if progress_cb:
                try:
                    progress_cb(
                        (idx - 1) / max(1, total),
                        f"{integration_id} missing and not in manifests ({idx}/{total})",
                    )
                except Exception:
                    pass
            continue

        if progress_cb:
            try:
                progress_cb((idx - 1) / max(1, total), f"Downloading {integration_id}... ({idx}/{total})")
            except Exception:
                pass

        ok, msg = install_integration_from_shop_item(item)
        if ok:
            restored.append(integration_id)
            changed = True
            logger.info("[integration-restore] %s: %s", integration_id, msg)
        else:
            logger.error("[integration-restore] %s: %s", integration_id, msg)

        if progress_cb:
            try:
                progress_cb(idx / max(1, total), f"Finished {integration_id} ({idx}/{total})")
            except Exception:
                pass

    return changed, restored, missing


def ensure_enabled_integrations_ready(progress_cb=None):
    os.makedirs(INTEGRATION_DIR, exist_ok=True)
    ensure_integration_import_context()

    missing = _enabled_missing_integration_ids()
    if not missing:
        if progress_cb:
            try:
                progress_cb(1.0, "All enabled integrations are present.")
            except Exception:
                pass
        return

    shop_urls = get_configured_integration_shop_manifest_urls()
    if not shop_urls:
        if progress_cb:
            try:
                progress_cb(1.0, "Integration shop manifest URLs are not configured.")
            except Exception:
                pass
        return

    if progress_cb:
        try:
            progress_cb(0.0, f"Restoring {len(missing)} enabled integration(s) from {len(shop_urls)} repo(s)...")
        except Exception:
            pass

    _changed, restored, enabled_missing = auto_restore_missing_integrations(
        shop_urls,
        progress_cb=progress_cb,
    )
    ensure_integration_import_context()

    if progress_cb and not restored and enabled_missing:
        try:
            progress_cb(1.0, "Missing integrations could not be restored from the configured repos.")
        except Exception:
            pass


def ensure_required_integrations_ready(progress_cb=None):
    return ensure_enabled_integrations_ready(progress_cb=progress_cb)


def integration_module(
    integration_id: str,
    *,
    auto_restore: bool = True,
    require_enabled: bool = True,
):
    normalized_id = _normalize_integration_id(integration_id)
    if not normalized_id:
        return None
    if require_enabled and not get_integration_enabled(normalized_id):
        return None
    if auto_restore and not is_integration_installed(normalized_id):
        auto_restore_missing_integrations(integration_ids=[normalized_id])
    if not is_integration_installed(normalized_id):
        return None
    ensure_integration_import_context()
    try:
        importlib.invalidate_caches()
        return importlib.import_module(f"integrations.{normalized_id}")
    except Exception as exc:
        logger.warning("[integrations] failed to import %s: %s", normalized_id, exc)
        return None


def integration_function(
    integration_id: str,
    function_name: str,
    *,
    require_enabled: bool = True,
    auto_restore: bool = True,
):
    module = integration_module(integration_id, require_enabled=require_enabled, auto_restore=auto_restore)
    if module is None:
        return None
    fn = getattr(module, str(function_name or "").strip(), None)
    return fn if callable(fn) else None


def _huggingface_saved_token(client=None) -> str:
    store = client or redis_client
    try:
        raw = store.hgetall("huggingface_settings") or {}
    except Exception:
        raw = {}
    normalized: dict[str, str] = {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            key_text = _redis_text(key)
            if key_text:
                value_text = _redis_text(value)
                normalized[key_text] = value_text
                normalized[key_text.lower()] = value_text
    return (
        normalized.get("HUGGINGFACE_TOKEN")
        or normalized.get("huggingface_token")
        or normalized.get("HF_TOKEN")
        or normalized.get("hf_token")
        or ""
    ).strip()


def huggingface_token(client=None) -> str:
    saved = _huggingface_saved_token(client)
    if saved:
        return saved
    for key in (
        "HF_TOKEN",
        "HUGGINGFACE_HUB_TOKEN",
        "HUGGING_FACE_HUB_TOKEN",
        "HUGGINGFACE_TOKEN",
        "HF_HUB_TOKEN",
        "HUGGINGFACE_API_TOKEN",
    ):
        token = _redis_text(os.getenv(key))
        if token:
            return token
    return ""


def huggingface_environment(overrides: dict | None = None, client=None) -> dict:
    env = dict(overrides or {})
    fn = integration_function(
        "huggingface",
        "huggingface_environment",
        require_enabled=False,
        auto_restore=False,
    )
    if fn:
        try:
            provided = fn(env, client)
            if isinstance(provided, dict):
                env.update(provided)
        except Exception as exc:
            logger.debug("[integrations] Hugging Face environment provider failed: %s", exc)

    token = _redis_text(
        env.get("HF_TOKEN")
        or env.get("HUGGINGFACE_HUB_TOKEN")
        or env.get("HUGGING_FACE_HUB_TOKEN")
        or env.get("HUGGINGFACE_TOKEN")
        or env.get("HF_HUB_TOKEN")
        or env.get("HUGGINGFACE_API_TOKEN")
        or huggingface_token(client)
    )
    if token:
        env["HF_TOKEN"] = token
        env["HUGGINGFACE_HUB_TOKEN"] = token
        env["HUGGING_FACE_HUB_TOKEN"] = token
        env["HUGGINGFACE_TOKEN"] = token
        env["HF_HUB_TOKEN"] = token
        env["HUGGINGFACE_API_TOKEN"] = token
    return env


def _installed_integration_ids() -> list[str]:
    installed_ids = set()
    for directory in [INTEGRATION_DIR, INTEGRATION_BUILTIN_DIR]:
        if not directory or not os.path.isdir(directory):
            continue
        for filename in os.listdir(directory):
            if filename == "__init__.py" or not filename.endswith(".py"):
                continue
            integration_id = _normalize_integration_id(filename[:-3])
            if integration_id:
                installed_ids.add(integration_id)
    return sorted(installed_ids)


def _literal_integration_definition(text: str) -> dict:
    try:
        module_ast = ast.parse(text)
    except Exception:
        return {}
    for node in module_ast.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "INTEGRATION" for target in node.targets):
            continue
        try:
            value = ast.literal_eval(node.value)
        except Exception:
            return {}
        return value if isinstance(value, dict) else {}
    return {}


def _get_installed_integration_version(integration_id: str) -> str:
    try:
        paths = _integration_file_paths(integration_id)
    except Exception:
        return "0.0.0"
    path = next((candidate for candidate in paths if os.path.exists(candidate)), "")
    if not path:
        return "0.0.0"
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            text = file_obj.read()
    except Exception:
        return "0.0.0"
    for key in ("__version__", "INTEGRATION_VERSION", "VERSION"):
        match = _re.search(rf"^\s*{_re.escape(key)}\s*=\s*['\"]([^'\"]+)['\"]", text, flags=_re.MULTILINE)
        if match:
            version = str(match.group(1) or "").strip()
            if version:
                return version
    definition = _literal_integration_definition(text)
    version = str(definition.get("version") or "").strip()
    return version or "0.0.0"


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


def _integration_display_name(integration_id: str, fallback: str | None = None) -> str:
    if fallback:
        text = str(fallback).strip()
        if text:
            return text
    parts = [part for part in _normalize_integration_id(integration_id).split("_") if part]
    if not parts:
        return str(integration_id or "").strip() or "Unknown Integration"
    return " ".join(part.capitalize() for part in parts)


def _load_installed_definition(integration_id: str) -> dict:
    try:
        paths = _integration_file_paths(integration_id)
    except Exception:
        return {}
    path = next((candidate for candidate in paths if os.path.exists(candidate)), "")
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as file_obj:
            text = file_obj.read()
    except Exception:
        return {}
    definition = _literal_integration_definition(text)
    return dict(definition) if isinstance(definition, dict) else {}


def uninstall_integration_file(integration_id: str) -> tuple[bool, str]:
    normalized_id = _normalize_integration_id(integration_id)
    if normalized_id in REQUIRED_INTEGRATION_IDS:
        return False, f"{normalized_id} is required by Tater and cannot be removed."
    try:
        path = _safe_integration_file_path(normalized_id)
        if not os.path.exists(path):
            return True, "Integration file not found (already removed)."
        os.remove(path)
        importlib.invalidate_caches()
        return True, f"Removed {path}"
    except Exception as exc:
        return False, f"Uninstall failed: {exc}"


def clear_integration_redis_data(integration_id: str, catalog_item: dict | None = None) -> tuple[bool, str]:
    try:
        normalized_id = _normalize_integration_id(integration_id)
        deleted = []
        configured = catalog_item if isinstance(catalog_item, dict) else {}
        redis_hashes = list(configured.get("redis_hashes") or configured.get("redis_keys") or [])
        redis_strings = list(configured.get("redis_strings") or [])
        known = KNOWN_INTEGRATION_REDIS_KEYS.get(normalized_id) or {}
        redis_hashes.extend(list(known.get("hashes") or []))
        redis_strings.extend(list(known.get("strings") or []))

        for key in sorted({str(key or "").strip() for key in redis_hashes if str(key or "").strip()}):
            if redis_client.exists(key):
                redis_client.delete(key)
                deleted.append(key)

        for key in sorted({str(key or "").strip() for key in redis_strings if str(key or "").strip()}):
            if redis_client.get(key) is not None:
                redis_client.delete(key)
                deleted.append(key)

        if deleted:
            return True, "Deleted: " + ", ".join(deleted)
        return True, "No Redis keys found for this integration."
    except Exception as exc:
        return False, f"Redis cleanup failed: {exc}"


def _build_installed_integration_entries(catalog_items: list[dict]) -> list[dict]:
    catalog_by_id = {
        str(item.get("id") or "").strip(): item
        for item in catalog_items
        if str(item.get("id") or "").strip()
    }

    entries = []
    for integration_id in _installed_integration_ids():
        catalog_item = catalog_by_id.get(integration_id)
        definition = _load_installed_definition(integration_id)
        display_name = _integration_display_name(
            integration_id,
            fallback=str(definition.get("name") or (catalog_item or {}).get("name") or "").strip(),
        )
        description = str(
            definition.get("description")
            or (catalog_item or {}).get("description")
            or "Local integration module."
        ).strip()
        installed_ver = _get_installed_integration_version(integration_id)
        store_ver = str((catalog_item or {}).get("version") or "").strip()
        source_label = str((catalog_item or {}).get("_source_label") or "Local integration").strip()
        update_available = bool(catalog_item and _semver_tuple(store_ver) > _semver_tuple(installed_ver))
        entries.append(
            {
                "id": integration_id,
                "catalog_item": catalog_item,
                "display_name": display_name,
                "description": description,
                "installed_ver": installed_ver,
                "store_ver": store_ver,
                "source_label": source_label,
                "required": integration_id in REQUIRED_INTEGRATION_IDS,
                "enabled": get_integration_enabled(integration_id),
                "update_available": update_available,
            }
        )

    entries.sort(key=lambda item: item["display_name"].lower())
    return entries
