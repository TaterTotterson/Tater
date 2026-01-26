# plugin_loader.py
import importlib
import pkgutil
from plugin_base import ToolPlugin

def load_plugins_from_package(package_name: str = "plugins") -> dict[str, ToolPlugin]:
    pkg = importlib.import_module(package_name)
    registry: dict[str, ToolPlugin] = {}

    for modinfo in pkgutil.iter_modules(pkg.__path__, package_name + "."):
        name = modinfo.name.split(".")[-1]
        if name.startswith("_"):
            continue

        try:
            module = importlib.import_module(modinfo.name)
        except Exception as e:
            print(f"⚠️ Plugin import failed: {modinfo.name}: {e}")
            continue

        plugin = getattr(module, "plugin", None)
        if not isinstance(plugin, ToolPlugin):
            continue

        pid = getattr(plugin, "name", None) or name
        plugin.name = pid  # normalize
        if pid in registry:
            print(f"⚠️ Duplicate plugin name '{pid}' in {modinfo.name}; skipping.")
            continue

        registry[pid] = plugin

    return registry