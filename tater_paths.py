from __future__ import annotations

import os
from pathlib import Path


APP_ROOT = Path(__file__).resolve().parent


def _root_from_env(env_name: str, default_name: str) -> Path:
    raw = str(os.getenv(env_name, "") or "").strip()
    if raw:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = APP_ROOT / path
        return path.resolve()
    return (APP_ROOT / default_name).resolve()


def agent_lab_dir() -> Path:
    return _root_from_env("TATER_AGENT_ROOT", "agent_lab")


def agent_lab_path(*parts: str) -> Path:
    return agent_lab_dir().joinpath(*parts)


def runtime_dir() -> Path:
    return _root_from_env("TATER_RUNTIME_DIR", ".runtime")
