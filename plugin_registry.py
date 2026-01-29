# plugin_registry.py
import importlib
import sys
import threading
from plugin_loader import load_plugins_from_package

# Build initial registry
plugin_registry = load_plugins_from_package("plugins")

# Global lock to prevent concurrent reloads (WebUI + platform threads, etc.)
_reload_lock = threading.RLock()


def reload_plugins() -> dict:
    """
    Reload plugin modules from disk and rebuild plugin_registry IN PLACE.
    This keeps existing references to plugin_registry valid.

    Key behaviors:
    - Do NOT delete sys.modules["plugins"] (the package). Deleting it can cause
      transient KeyError/races while importlib/pkgutil expects it to exist.
    - Only remove submodules under plugins.* so fresh imports reflect filesystem.
    - Guard with a lock so two threads can't reload at the same time.
    """
    with _reload_lock:
        importlib.invalidate_caches()

        # Ensure the package itself exists/loads (but do not remove it)
        try:
            importlib.import_module("plugins")
        except Exception as e:
            # If the package can't be imported, leave existing registry as-is
            print(f"⚠️ Unable to import 'plugins' package during reload: {e}")
            return plugin_registry

        # Drop imported plugin submodules so the next import sees new code/files.
        # IMPORTANT: do not remove the 'plugins' package itself.
        for mod in list(sys.modules.keys()):
            if mod.startswith("plugins."):
                sys.modules.pop(mod, None)

        # Rebuild registry from disk
        new_registry = load_plugins_from_package("plugins")

        # Mutate in place so any `from plugin_registry import plugin_registry`
        # references keep working.
        plugin_registry.clear()
        plugin_registry.update(new_registry)

        return plugin_registry