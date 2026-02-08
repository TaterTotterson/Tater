import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from helpers import redis_client
from plugin_loader import load_plugins_from_directory


BASE_DIR = Path(__file__).resolve().parent
AGENT_LAB_DIR = BASE_DIR / "agent_lab"
AGENT_PLUGINS_DIR = AGENT_LAB_DIR / "plugins"

AGENT_PLUGIN_ENABLED_KEY = "exp:plugin_enabled"

_agent_registry: Dict[str, Any] = {}
_lock = threading.RLock()


def _load_agent_plugins() -> Dict[str, Any]:
    AGENT_PLUGINS_DIR.mkdir(parents=True, exist_ok=True)
    return load_plugins_from_directory(str(AGENT_PLUGINS_DIR), id_from_filename=True)


try:
    _agent_registry = _load_agent_plugins()
except Exception:
    _agent_registry = {}


def reload_agent_plugins() -> Dict[str, Any]:
    with _lock:
        try:
            new_registry = _load_agent_plugins()
        except Exception:
            return _agent_registry

        if not new_registry:
            return _agent_registry

        _agent_registry.clear()
        _agent_registry.update(new_registry)
        return _agent_registry


def get_agent_registry_snapshot(auto_reload: bool = True) -> Dict[str, Any]:
    if auto_reload:
        reload_agent_plugins()
    with _lock:
        return dict(_agent_registry)


def get_agent_registry() -> Dict[str, Any]:
    return _agent_registry


def exp_get_plugin_enabled(plugin_name: str, r=None) -> bool:
    r = r or redis_client
    raw = r.hget(AGENT_PLUGIN_ENABLED_KEY, plugin_name)
    return str(raw or "").strip().lower() == "true"


def build_agent_registry(
    stable_registry: Dict[str, Any],
    stable_enabled_fn: Callable[[str], bool],
) -> Tuple[Dict[str, Any], Callable[[str], bool], List[str]]:
    agent_registry = get_agent_registry_snapshot(auto_reload=True)
    merged: Dict[str, Any] = dict(stable_registry or {})
    agent_ids = set()
    collisions: List[str] = []

    for plugin_id, plugin in agent_registry.items():
        if plugin_id in merged:
            collisions.append(plugin_id)
            continue
        merged[plugin_id] = plugin
        agent_ids.add(plugin_id)

    def _enabled(plugin_id: str) -> bool:
        if plugin_id in agent_ids:
            return exp_get_plugin_enabled(plugin_id)
        return bool(stable_enabled_fn(plugin_id))

    return merged, _enabled, collisions
