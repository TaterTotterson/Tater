# plugin_registry.py
import importlib
import threading
import os
from typing import Dict

from plugin_loader import load_plugins_from_directory

# Build initial registry
plugin_registry: Dict[str, object] = load_plugins_from_directory(
    os.getenv("TATER_PLUGIN_DIR", "plugins")
)

# Global lock to prevent concurrent reloads (WebUI + platform threads, etc.)
_reload_lock = threading.RLock()


def reload_plugins() -> Dict[str, object]:
    """
    Reload plugins from disk and rebuild plugin_registry IN PLACE.
    This keeps existing references to plugin_registry valid.

    Key behaviors:
    - Loads from filesystem (TATER_PLUGIN_DIR), not Python package discovery.
    - Guarded by a lock so two threads can't reload at the same time.
    - If reload yields 0 plugins, keep existing registry (last-known-good).
    """
    with _reload_lock:
        importlib.invalidate_caches()

        try:
            new_registry = load_plugins_from_directory(
                os.getenv("TATER_PLUGIN_DIR", "plugins")
            )
        except Exception as e:
            print(f"⚠️ Plugin reload crashed; keeping existing registry: {e}")
            return plugin_registry

        if not new_registry:
            print("⚠️ Reload produced 0 plugins; keeping existing registry.")
            return plugin_registry

        # Mutate in place so any modules holding a reference keep working.
        plugin_registry.clear()
        plugin_registry.update(new_registry)

        print(f"✅ Reloaded {len(plugin_registry)} plugins from disk.")
        return plugin_registry


def get_registry_snapshot() -> Dict[str, object]:
    """
    Return a stable snapshot (copy) of the current registry.

    Use this in platforms/RSS when building prompts so iteration is safe even if
    WebUI triggers a reload concurrently.
    """
    with _reload_lock:
        return dict(plugin_registry)


def get_registry() -> Dict[str, object]:
    """
    Backwards-compat convenience: return the live registry object (not a copy).
    Prefer get_registry_snapshot() when iterating.
    """
    return plugin_registry