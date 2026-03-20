import importlib.util
import os
import re
from pathlib import Path
from typing import Dict, Optional

from verba_base import ToolVerba

_SAFE_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")


def _verba_dir() -> Path:
    verba_dir = os.getenv("TATER_VERBA_DIR", "verba")
    return Path(verba_dir)


def load_verbas_from_directory(
    verba_dir: Optional[str] = None,
    *,
    id_from_filename: bool = False,
) -> Dict[str, ToolVerba]:
    """
    Load verbas by scanning a directory for *.py files and importing them directly
    from file paths. Each module must expose a global `verba` instance.
    """
    base = Path(verba_dir) if verba_dir else _verba_dir()
    base.mkdir(parents=True, exist_ok=True)

    registry: Dict[str, ToolVerba] = {}

    # Stable order: deterministic loads help debugging.
    for path in sorted(base.glob("*.py")):
        name = path.stem
        if name.startswith("_"):
            continue
        if not _SAFE_ID_RE.fullmatch(name):
            print(f"WARNING: Skipping verba with unsafe filename: {path.name}")
            continue

        try:
            module_name = f"tater_verba_{name}_{int(path.stat().st_mtime_ns)}"
            spec = importlib.util.spec_from_file_location(module_name, str(path))
            if spec is None or spec.loader is None:
                print(f"WARNING: Verba load failed (no spec/loader): {path}")
                continue
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore[attr-defined]
        except Exception as exc:
            print(f"WARNING: Verba import failed: {path.name}: {exc}")
            continue

        verba = getattr(module, "verba", None)
        if not isinstance(verba, ToolVerba):
            continue

        declared_id = str(getattr(verba, "name", "") or "").strip()
        vid = declared_id or name
        if id_from_filename:
            vid = name
            if declared_id and declared_id != name:
                current_label = str(getattr(verba, "verba_name", "") or "").strip()
                if not current_label:
                    verba.verba_name = declared_id
                print(
                    f"WARNING: Verba '{path.name}' declares name '{declared_id}'; "
                    f"using filename id '{name}'."
                )
        vid = str(vid).strip() or name

        # Normalize verba.name to registry id.
        verba.name = vid

        if not str(getattr(verba, "verba_name", "") or "").strip():
            verba.verba_name = vid

        try:
            examples = getattr(verba, "example_calls", None)
            if not isinstance(examples, list) or not examples:
                usage = getattr(verba, "usage", "") or ""
                if isinstance(usage, str) and usage.strip():
                    verba.example_calls = [usage.strip()]
        except Exception:
            pass

        if vid in registry:
            print(f"WARNING: Duplicate verba id '{vid}' from file {path.name}; skipping.")
            continue

        registry[vid] = verba

    return registry
