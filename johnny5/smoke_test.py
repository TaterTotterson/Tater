import importlib.util
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from plugin_base import ToolPlugin
from johnny5 import state


@dataclass
class SmokeResult:
    ok: bool
    details: str


def _load_module(path: Path, label: str):
    module_name = f"{label}_{path.stem}_{int(path.stat().st_mtime_ns)}"
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load spec")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module


def smoke_test_plugin(path: Path) -> SmokeResult:
    try:
        module = _load_module(path, "johnny5_plugin")
        plugin = getattr(module, "plugin", None)
        if not isinstance(plugin, ToolPlugin):
            return SmokeResult(False, "Module does not expose ToolPlugin instance as `plugin`.")

        required = ["name", "description", "usage", "platforms", "required_settings"]
        missing = [attr for attr in required if not getattr(plugin, attr, None)]
        if missing:
            return SmokeResult(False, f"Missing required attributes: {', '.join(missing)}")

        smoke = getattr(plugin, "smoke_test", None)
        if callable(smoke):
            smoke()

        return SmokeResult(True, "Plugin import + basic checks ok.")
    except Exception:
        return SmokeResult(False, traceback.format_exc())


def smoke_test_platform(path: Path) -> SmokeResult:
    try:
        module = _load_module(path, "johnny5_platform")
        run = getattr(module, "run", None)
        if not callable(run):
            return SmokeResult(False, "Platform has no run(stop_event) function.")
        return SmokeResult(True, "Platform import ok.")
    except Exception:
        return SmokeResult(False, traceback.format_exc())


def run_smoke_test(kind: str, candidate_id: str) -> Dict[str, Any]:
    path = state.candidate_path(kind, candidate_id)
    if not path.exists():
        result = SmokeResult(False, "Candidate file not found.")
    elif kind == "plugin":
        result = smoke_test_plugin(path)
    else:
        result = smoke_test_platform(path)

    last_test = {"ok": result.ok, "details": result.details, "ts": state._now_iso()}
    status = "tested" if result.ok else "failed"
    state.update_candidate_status(kind, candidate_id, status, last_test=last_test)
    return last_test
