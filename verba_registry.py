import importlib
import os
import threading
from typing import Dict

from verba_loader import load_verbas_from_directory

# Keep module import side-effects minimal: verba code is loaded lazily.
verba_registry: Dict[str, object] = {}
_initialized = False

# Global lock to prevent concurrent reloads (WebUI + platform threads, etc.)
_reload_lock = threading.RLock()


def _verba_dir() -> str:
    return os.getenv("TATER_VERBA_DIR", "verba")


def ensure_verbas_loaded() -> Dict[str, object]:
    """
    Lazy-load verba modules once.

    This prevents verba side-effects from running while verba_registry itself
    is being imported, which can destabilize startup.
    """
    global _initialized
    if _initialized:
        return verba_registry

    with _reload_lock:
        if _initialized:
            return verba_registry
        importlib.invalidate_caches()
        try:
            verba_registry.clear()
            verba_registry.update(load_verbas_from_directory(_verba_dir()))
        except Exception as e:
            print(f"WARNING: Initial verba load crashed; starting with empty registry: {e}")
        _initialized = True
        return verba_registry


def reload_verbas() -> Dict[str, object]:
    """
    Reload verbas from disk and rebuild verba_registry IN PLACE.
    This keeps existing references to verba_registry valid.

    Key behaviors:
    - Loads from filesystem (TATER_VERBA_DIR), not package discovery.
    - Guarded by a lock so two threads can't reload at the same time.
    - If reload yields 0 verbas, keep existing registry (last-known-good).
    """
    global _initialized
    with _reload_lock:
        importlib.invalidate_caches()

        try:
            new_registry = load_verbas_from_directory(_verba_dir())
        except Exception as e:
            print(f"WARNING: Verba reload crashed; keeping existing registry: {e}")
            return verba_registry

        if _initialized and verba_registry and not new_registry:
            print("WARNING: Reload produced 0 verbas; keeping existing registry.")
            return verba_registry

        # Mutate in place so any modules holding a reference keep working.
        verba_registry.clear()
        verba_registry.update(new_registry)
        _initialized = True

        print(f"Reloaded {len(verba_registry)} verbas from disk.")
        return verba_registry


def get_verba_registry_snapshot() -> Dict[str, object]:
    """
    Return a stable snapshot (copy) of the current registry.

    Use this in portals/RSS when building prompts so iteration is safe even if
    WebUI triggers a reload concurrently.
    """
    ensure_verbas_loaded()
    with _reload_lock:
        return dict(verba_registry)


def get_verba_registry() -> Dict[str, object]:
    """
    Backwards-compat convenience: return the live registry object (not a copy).
    Prefer get_verba_registry_snapshot() when iterating.
    """
    ensure_verbas_loaded()
    return verba_registry
