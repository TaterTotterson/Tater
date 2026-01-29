# plugin_loader.py
import os
import re
import importlib.util
from pathlib import Path
from typing import Dict, Optional

from plugin_base import ToolPlugin


_SAFE_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")


def _plugin_dir() -> Path:
    # Match WebUI behavior
    plugin_dir = os.getenv("TATER_PLUGIN_DIR", "plugins")
    return Path(plugin_dir)


def load_plugins_from_directory(plugin_dir: Optional[str] = None) -> Dict[str, ToolPlugin]:
    """
    Load plugins by scanning a directory for *.py files and importing them directly
    from file paths. Each plugin module must expose a global named `plugin` which
    is an instance of ToolPlugin.

    Returns:
        { plugin_id: plugin_instance }
    """
    base = Path(plugin_dir) if plugin_dir else _plugin_dir()
    base.mkdir(parents=True, exist_ok=True)

    registry: Dict[str, ToolPlugin] = {}

    # Stable order: deterministic loads help debugging
    for path in sorted(base.glob("*.py")):
        name = path.stem

        # Skip private helpers
        if name.startswith("_"):
            continue

        # Safety: only allow simple ids
        if not _SAFE_ID_RE.fullmatch(name):
            print(f"⚠️ Skipping plugin with unsafe filename: {path.name}")
            continue

        try:
            # Give each import a unique module name so we don't fight sys.modules caching.
            module_name = f"tater_plugin_{name}_{int(path.stat().st_mtime_ns)}"

            spec = importlib.util.spec_from_file_location(module_name, str(path))
            if spec is None or spec.loader is None:
                print(f"⚠️ Plugin load failed (no spec/loader): {path}")
                continue

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore[attr-defined]

        except Exception as e:
            print(f"⚠️ Plugin import failed: {path.name}: {e}")
            continue

        plugin = getattr(module, "plugin", None)
        if not isinstance(plugin, ToolPlugin):
            # Not a ToolPlugin plugin module; ignore
            continue

        pid = getattr(plugin, "name", None) or name
        pid = str(pid).strip() or name

        # Normalize plugin.name to the registry id
        plugin.name = pid

        if pid in registry:
            print(f"⚠️ Duplicate plugin id '{pid}' from file {path.name}; skipping.")
            continue

        registry[pid] = plugin

    return registry
