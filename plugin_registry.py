import importlib
import sys
import threading

from plugin_loader import load_plugins_from_package

plugin_registry = load_plugins_from_package("plugins")
_reload_lock = threading.RLock()


def get_registry_snapshot() -> dict:
    with _reload_lock:
        return dict(plugin_registry)


def reload_plugins() -> dict:
    with _reload_lock:
        importlib.invalidate_caches()
        try:
            importlib.import_module("plugins")
        except Exception as e:
            print(f"Unable to import 'plugins' package during reload: {e}")
            return plugin_registry

        for mod in list(sys.modules.keys()):
            if mod.startswith("plugins."):
                sys.modules.pop(mod, None)

        new_registry = load_plugins_from_package("plugins")

        if not new_registry:
            print("Reload produced 0 plugins; keeping existing registry.")
            return plugin_registry

        plugin_registry.update(new_registry)
        for k in list(plugin_registry.keys()):
            if k not in new_registry:
                plugin_registry.pop(k, None)

        return plugin_registry
