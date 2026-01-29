# plugin_loader.py
import importlib
import pkgutil
from typing import Dict

from plugin_base import ToolPlugin


def load_plugins_from_package(package_name: str = "plugins") -> Dict[str, ToolPlugin]:
    """
    Discover and import plugin modules from a package and return a registry mapping:
      { plugin_id: plugin_instance }

    Notes:
    - Handles the case where the package isn't a "real" package (no __path__) gracefully.
    - Skips modules starting with "_" (private helpers).
    - Expects each module to expose a global named `plugin` which is an instance of ToolPlugin.
    """
    pkg = importlib.import_module(package_name)

    # If plugins isn't a package (missing __path__), we can't iterate modules.
    pkg_path = getattr(pkg, "__path__", None)
    if pkg_path is None:
        # This prevents crashes like: AttributeError: module 'plugins' has no attribute '__path__'
        print(f"⚠️ Package '{package_name}' has no __path__; no plugins loaded.")
        return {}

    registry: Dict[str, ToolPlugin] = {}

    for modinfo in pkgutil.iter_modules(pkg_path, package_name + "."):
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