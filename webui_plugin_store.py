import hashlib
import logging
import os
import re as _re
from urllib.parse import urljoin

import redis
import requests
import streamlit as st

import plugin_registry as plugin_registry_mod
from plugin_settings import set_plugin_enabled

PLUGIN_DIR = os.getenv("TATER_PLUGIN_DIR", "plugins")
SHOP_MANIFEST_URL_DEFAULT = os.getenv(
    "TATER_SHOP_MANIFEST_URL",
    "https://raw.githubusercontent.com/TaterTotterson/Tater_Shop/main/manifest.json",
)
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


def _safe_plugin_file_path(plugin_id: str) -> str:
    if not _re.fullmatch(r"[a-zA-Z0-9_\-]+", plugin_id or ""):
        raise ValueError("Invalid plugin id")
    return os.path.join(PLUGIN_DIR, f"{plugin_id}.py")


def fetch_shop_manifest(url: str) -> dict:
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()


def is_plugin_installed(plugin_id: str) -> bool:
    try:
        return os.path.exists(_safe_plugin_file_path(plugin_id))
    except Exception:
        return False


def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def install_plugin_from_shop_item(item: dict, manifest_url: str) -> tuple[bool, str]:
    """
    Downloads a plugin .py from the shop manifest entry, verifies sha256 if provided,
    and writes it to PLUGIN_DIR as <id>.py.
    Supports relative 'entry' paths.
    """
    try:
        plugin_id = (item.get("id") or "").strip()
        entry = (item.get("entry") or "").strip()
        expected_sha = (item.get("sha256") or "").strip().lower()

        if not plugin_id:
            return False, "Manifest item missing 'id'."
        if not entry:
            return False, f"{plugin_id}: manifest item missing 'entry'."

        # Resolve relative paths against the manifest URL
        entry = entry.lstrip("/")  # urljoin breaks raw GitHub paths if entry starts with /
        full_url = urljoin(manifest_url, entry)

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


def auto_restore_missing_plugins(manifest_url: str, progress_cb=None) -> tuple[bool, list[str], list[str]]:
    """
    Restore any plugins that are ENABLED in Redis but missing on disk.
    Uses the shop manifest as the source of install URLs.

    progress_cb (optional): callable(progress_float_0_to_1, status_text)
    """
    enabled_missing: list[str] = []
    restored: list[str] = []
    changed = False

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

    try:
        manifest = fetch_shop_manifest(manifest_url)
    except Exception as e:
        logging.error(f"[restore] Failed to load manifest: {e}")
        if progress_cb:
            try:
                progress_cb(0.0, f"Failed to load manifest: {e}")
            except Exception:
                pass
        return changed, restored, enabled_missing

    items = manifest.get("plugins") or manifest.get("items") or manifest.get("data") or []
    if not isinstance(items, list):
        logging.error("[restore] Manifest format unexpected.")
        if progress_cb:
            try:
                progress_cb(0.0, "Manifest format unexpected (expected list under plugins/items/data).")
            except Exception:
                pass
        return changed, restored, enabled_missing

    by_id: dict[str, dict] = {}
    for item in items:
        pid = (item.get("id") or "").strip()
        if pid:
            by_id[pid] = item

    for idx, pid in enumerate(enabled_missing, start=1):
        item = by_id.get(pid)
        if not item:
            logging.error(f"[restore] {pid} enabled but not found in manifest")
            try:
                redis_client.hdel("plugin_enabled", pid)
                logging.info(f"[restore] Removed stale plugin_enabled key for {pid}")
            except Exception as e:
                logging.error(f"[restore] Failed to remove stale plugin_enabled key for {pid}: {e}")
            if progress_cb:
                try:
                    progress_cb(
                        (idx - 1) / max(1, total),
                        f"{pid} missing and not in manifest; removed stale enable key ({idx}/{total})",
                    )
                except Exception:
                    pass
            continue

        if progress_cb:
            try:
                progress_cb((idx - 1) / max(1, total), f"Downloading {pid}... ({idx}/{total})")
            except Exception:
                pass

        ok, msg = install_plugin_from_shop_item(item, manifest_url)
        if ok:
            restored.append(pid)
            changed = True
            logging.info(f"[restore] {pid}: {msg}")
        else:
            logging.error(f"[restore] {pid}: {msg}")

        if progress_cb:
            try:
                progress_cb(idx / max(1, total), f"Finished {pid} ({idx}/{total})")
            except Exception:
                pass

    return changed, restored, enabled_missing


def ensure_plugins_ready(progress_cb=None):
    """
    Ensure any ENABLED plugins that are missing on disk are restored from the shop.

    progress_cb (optional): callable(progress_float_0_to_1, status_text)
    """
    os.makedirs(PLUGIN_DIR, exist_ok=True)

    shop_url = (redis_client.get("tater:shop_manifest_url") or SHOP_MANIFEST_URL_DEFAULT)
    shop_url = (shop_url or "").strip()

    if not shop_url:
        if progress_cb:
            try:
                progress_cb(1.0, "Plugin shop manifest URL is not configured.")
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
            progress_cb(0.0, f"Restoring {len(missing)} missing plugin(s)...")
        except Exception:
            pass

    changed, restored, enabled_missing = auto_restore_missing_plugins(
        shop_url,
        progress_cb=progress_cb,
    )

    if changed:
        if progress_cb:
            try:
                progress_cb(0.98, "Reloading plugins...")
            except Exception:
                pass
        _refresh_plugins_after_fs_change()

    if progress_cb:
        try:
            progress_cb(1.0, "")
        except Exception:
            pass


def render_plugin_store_page():
    st.title("Plugin Store")
    st.caption("Browse and install plugins from the Tater Shop manifest.")

    url = st.text_input(
        "Shop manifest URL",
        value=(redis_client.get("tater:shop_manifest_url") or SHOP_MANIFEST_URL_DEFAULT),
        key="shop_manifest_url",
    )

    try:
        manifest = fetch_shop_manifest(url.strip())
    except Exception as e:
        st.error(f"Failed to load manifest: {e}")
        return

    plugins = manifest.get("plugins") or manifest.get("items") or manifest.get("data") or []
    if not isinstance(plugins, list):
        st.error("Manifest format unexpected (expected a list under 'plugins').")
        return

    def _semver_tuple(v: str):
        if not v:
            return (0, 0, 0)

        v = str(v).strip().lower()
        if v.startswith("v"):
            v = v[1:].strip()

        m = _re.match(r"^([0-9]+(\.[0-9]+){0,2})", v)
        core = m.group(1) if m else "0.0.0"

        parts = core.split(".")
        parts = (parts + ["0", "0", "0"])[:3]
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

        v = (
            getattr(loaded, "version", None)
            or getattr(loaded, "__version__", None)
            or getattr(loaded, "plugin_version", None)
        )
        v = str(v).strip() if v is not None else ""
        return v or "0.0.0"

    def _normalize_plats(plats):
        if not plats:
            return []
        if isinstance(plats, str):
            plats = [plats]
        if not isinstance(plats, list):
            return []
        out = []
        for p in plats:
            if not p:
                continue
            out.append(str(p).strip().lower())
        seen = set()
        uniq = []
        for p in out:
            if p not in seen:
                seen.add(p)
                uniq.append(p)
        return uniq

    def _get_item_platforms(item):
        pid = (item.get("id") or "").strip()
        if pid and is_plugin_installed(pid):
            loaded = get_registry().get(pid)
            if loaded:
                lp = getattr(loaded, "platforms", []) or []
                norm = _normalize_plats(lp)
                if norm:
                    return norm
        return _normalize_plats(item.get("platforms") or item.get("platform") or [])

    def _get_item_display_platforms(item):
        plats = _get_item_platforms(item)
        return ", ".join(p.title() for p in plats) if plats else "(not provided)"

    def _is_update_available(item: dict) -> tuple[bool, str, str]:
        pid = (item.get("id") or "").strip()
        store_ver = (item.get("version") or "0.0.0").strip()
        if not pid:
            return (False, "0.0.0", store_ver)

        if not is_plugin_installed(pid):
            return (False, "0.0.0", store_ver)

        installed_ver = _get_installed_version(pid)
        return (_semver_tuple(store_ver) > _semver_tuple(installed_ver), installed_ver, store_ver)

    updatable = []
    for it in plugins:
        pid = (it.get("id") or "").strip()
        if not pid:
            continue
        ok, inst_v, store_v = _is_update_available(it)
        if ok:
            updatable.append((it, inst_v, store_v))

    bar1, bar2, bar3, bar4 = st.columns([1, 1, 1, 3])

    with bar1:
        if st.button("Save URL", key="shop_save_url"):
            redis_client.set("tater:shop_manifest_url", url.strip())
            st.success("Saved.")
            st.rerun()

    with bar2:
        if st.button("Refresh", key="shop_refresh"):
            st.rerun()

    with bar3:
        if st.button("Update All", disabled=(len(updatable) == 0), key="shop_update_all"):
            updated = []
            failed = []

            prog = st.progress(0)
            total = max(1, len(updatable))

            for idx, (it, inst_v, store_v) in enumerate(updatable, start=1):
                pid = (it.get("id") or "").strip()
                ok, msg = install_plugin_from_shop_item(it, url.strip())
                if ok:
                    updated.append(f"{pid} ({inst_v} -> {store_v})")
                else:
                    failed.append(f"{pid}: {msg}")

                prog.progress(min(1.0, idx / total))

            if updated:
                st.success("Updated:\n" + "\n".join(updated))
            if failed:
                st.error("Failed:\n" + "\n".join(failed))

            _refresh_plugins_after_fs_change()
            st.rerun()

    with bar4:
        if updatable:
            st.caption(f"Updates available: {len(updatable)}")
        else:
            st.caption("No updates available.")

    st.markdown("---")

    all_platforms = set()
    for it in plugins:
        for p in _get_item_platforms(it):
            all_platforms.add(p)

    common_order = [
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
    ordered = [p for p in common_order if p in all_platforms]
    ordered += sorted([p for p in all_platforms if p not in set(common_order)])

    filter_options = ["All"] + [p.title() for p in ordered]
    selected_platform_label = st.selectbox(
        "Filter by platform",
        options=filter_options,
        index=0,
        key="shop_platform_filter",
    )

    search_q = st.text_input(
        "Search",
        value="",
        placeholder="Search name, id, description...",
        key="shop_search",
    ).strip().lower()

    selected_platform = None
    if selected_platform_label != "All":
        selected_platform = selected_platform_label.strip().lower()

    filtered = []
    for item in plugins:
        pid = (item.get("id") or "").strip()
        name = (item.get("name") or pid).strip()
        desc = (item.get("description") or "").strip()

        if selected_platform:
            plats = _get_item_platforms(item)
            if selected_platform not in plats:
                continue

        if search_q:
            hay = f"{pid}\n{name}\n{desc}".lower()
            if search_q not in hay:
                continue

        filtered.append(item)

    st.caption(f"Showing {len(filtered)} of {len(plugins)} plugin(s).")

    for item in filtered:
        pid = (item.get("id") or "").strip()
        name = (item.get("name") or pid).strip()
        desc = (item.get("description") or "").strip()
        min_ver = (item.get("min_tater_version") or "0.0.0").strip()
        store_ver = (item.get("version") or "0.0.0").strip()

        installed = is_plugin_installed(pid)
        platforms_str = _get_item_display_platforms(item)

        installed_ver = _get_installed_version(pid) if installed else "0.0.0"
        update_available = installed and (_semver_tuple(store_ver) > _semver_tuple(installed_ver))

        with st.container(border=True):
            st.subheader(name)

            if installed:
                if update_available:
                    st.caption(
                        f"ID: {pid} | installed: {installed_ver} | store: {store_ver} | min tater: {min_ver}  update available"
                    )
                else:
                    st.caption(
                        f"ID: {pid} | installed: {installed_ver} | store: {store_ver} | min tater: {min_ver}"
                    )
            else:
                st.caption(f"ID: {pid} | version: {store_ver} | min tater: {min_ver}")

            if desc:
                st.write(desc)

            st.caption(f"Platforms: {platforms_str}")

            cols = st.columns([1, 1, 3])

            purge_store = cols[2].checkbox("Delete Data?", value=False, key=f"store_purge_{pid}")

            if installed:
                if update_available:
                    cols[0].warning("Update available")
                    if cols[1].button("Update", key=f"store_update_{pid}"):
                        ok, msg = install_plugin_from_shop_item(item, url.strip())
                        if ok:
                            st.success(f"{msg} (updated {installed_ver} -> {store_ver})")

                            plugin_registry_mod.reload_plugins()
                            st.session_state.pop("shop_platform_filter", None)
                            st.session_state.pop("shop_search", None)

                            st.rerun()
                        else:
                            st.error(msg)
                else:
                    cols[0].success("Installed")
                    if cols[1].button("Remove", key=f"store_remove_{pid}"):
                        ok, msg = uninstall_plugin_file(pid)
                        if ok:
                            st.success(msg)
                            try:
                                set_plugin_enabled(pid, False)
                            except Exception:
                                pass

                            if purge_store:
                                ok2, msg2 = clear_plugin_redis_data(pid)
                                if ok2:
                                    st.success(f"Redis cleanup: {msg2}")
                                else:
                                    st.error(msg2)

                            _refresh_plugins_after_fs_change()
                            st.rerun()
                        else:
                            st.error(msg)
            else:
                cols[0].warning("Not installed")
                if cols[1].button("Install", key=f"store_install_{pid}"):
                    ok, msg = install_plugin_from_shop_item(item, url.strip())
                    if ok:
                        st.success(msg)

                        plugin_registry_mod.reload_plugins()
                        st.session_state.pop("shop_platform_filter", None)
                        st.session_state.pop("shop_search", None)

                        st.rerun()
                    else:
                        st.error(msg)
