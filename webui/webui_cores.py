import importlib
import os
import sys
from pathlib import Path
from typing import Any, Dict

import streamlit as st

from webui.webui_portals import render_portal_controls


_APP_ROOT = Path(__file__).resolve().parent.parent


def _resolve_module_dir(env_name: str, default_subdir: str) -> Path:
    raw = str(os.getenv(env_name, "") or "").strip()
    if not raw:
        return (_APP_ROOT / default_subdir).resolve()
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = (_APP_ROOT / candidate).resolve()
    return candidate.resolve()


CORE_MODULE_DIR = _resolve_module_dir("TATER_CORE_DIR", "cores")


def _ensure_core_import_context() -> None:
    try:
        parent = str(CORE_MODULE_DIR.parent)
        if parent and parent not in sys.path:
            sys.path.insert(0, parent)
        importlib.invalidate_caches()
        if "cores" not in sys.modules:
            importlib.import_module("cores")
    except Exception:
        return


def _import_core_module_for_ui(module_key: str):
    token = str(module_key or "").strip()
    if not token:
        raise ImportError("Missing core module key")
    _ensure_core_import_context()
    module_name = f"cores.{token}"
    importlib.invalidate_caches()
    if module_name in sys.modules:
        return sys.modules[module_name]
    return importlib.import_module(module_name)


def _render_core_manager_extras(
    core: Dict[str, Any],
    redis_client,
) -> None:
    key = str((core or {}).get("key") or "").strip()
    if not key:
        return

    try:
        module = _import_core_module_for_ui(key)
    except Exception:
        return

    render_fn = getattr(module, "render_core_manager_extras", None)
    if not callable(render_fn):
        return

    st.markdown("---")
    try:
        render_fn(
            core=core,
            redis_client=redis_client,
            surface_kind="core",
        )
    except Exception as exc:
        st.error(f"Failed to render core extras for {key}: {exc}")


def render_core_controls(
    core,
    redis_client,
    *,
    start_core_fn,
    stop_core_fn,
):
    def _extras_renderer(**_kwargs):
        _render_core_manager_extras(
            core,
            redis_client,
        )

    return render_portal_controls(
        core,
        redis_client,
        start_portal_fn=start_core_fn,
        stop_portal_fn=stop_core_fn,
        surface_kind="core",
        render_surface_extras_fn=_extras_renderer,
    )
