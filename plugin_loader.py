# plugin_loader.py
import os
import re
import importlib.util
from pathlib import Path
from typing import Dict, Optional

from plugin_base import ToolPlugin
from johnny5 import state


_SAFE_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")


def _plugin_dir() -> Path:
    # Match WebUI behavior
    plugin_dir = os.getenv("TATER_PLUGIN_DIR", "plugins")
    return Path(plugin_dir)


def _load_plugin_from_path(path: Path) -> Optional[ToolPlugin]:
    name = path.stem
    try:
        module_name = f"tater_plugin_{name}_{int(path.stat().st_mtime_ns)}"
        spec = importlib.util.spec_from_file_location(module_name, str(path))
        if spec is None or spec.loader is None:
            print(f"⚠️ Plugin load failed (no spec/loader): {path}")
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
    except Exception as e:
        print(f"⚠️ Plugin import failed: {path.name}: {e}")
        return None

    plugin = getattr(module, "plugin", None)
    if not isinstance(plugin, ToolPlugin):
        return None
    return plugin


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

        plugin = _load_plugin_from_path(path)
        if plugin is None:
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


def load_plugins_with_overrides(
    plugin_dir: Optional[str] = None,
    candidate_dir: Optional[str] = None,
) -> Dict[str, ToolPlugin]:
    base_registry = load_plugins_from_directory(plugin_dir)
    candidate_base = Path(candidate_dir) if candidate_dir else state.PLUGIN_CANDIDATE_DIR
    if not candidate_base.exists():
        return base_registry

    for pid in list(base_registry.keys()):
        if not state.is_override_enabled("plugin", pid):
            continue
        candidate_path = candidate_base / f"{pid}.py"
        if not candidate_path.exists():
            continue
        candidate_plugin = _load_plugin_from_path(candidate_path)
        if candidate_plugin is None:
            continue
        candidate_plugin.name = pid
        base_registry[pid] = candidate_plugin

    for cid in state.list_candidates("plugin").keys():
        if cid in base_registry:
            continue
        if not state.is_override_enabled("plugin", cid):
            continue
        candidate_path = candidate_base / f"{cid}.py"
        if not candidate_path.exists():
            continue
        candidate_plugin = _load_plugin_from_path(candidate_path)
        if candidate_plugin is None:
            continue
        candidate_plugin.name = cid
        base_registry[cid] = candidate_plugin
    return base_registry
