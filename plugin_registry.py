# plugin_registry.py
import importlib
import sys
from plugin_loader import load_plugins_from_package

plugin_registry = load_plugins_from_package("plugins")

def reload_plugins() -> dict:
    """
    Reload plugin modules from disk and rebuild plugin_registry IN PLACE.
    This keeps existing references to plugin_registry valid.
    """
    importlib.invalidate_caches()

    # Drop all imported plugin modules so fresh imports reflect filesystem
    for mod in list(sys.modules.keys()):
        if mod == "plugins" or mod.startswith("plugins."):
            sys.modules.pop(mod, None)

    new_registry = load_plugins_from_package("plugins")

    # Mutate in place so any `from plugin_registry import plugin_registry`
    # references keep working.
    plugin_registry.clear()
    plugin_registry.update(new_registry)

    return plugin_registry