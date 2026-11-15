"""Focused helpers extracted from the legacy shared runtime."""

import os
import sys
from typing import Any, Dict


def _macos_posix_spawn_kwargs() -> Dict[str, Any]:
    if sys.platform == "darwin":
        return {"close_fds": False}
    return {}


def _llama_cpp_engine_uses_process_group() -> bool:
    # subprocess.Popen can use posix_spawn on macOS only when a new session is
    # not requested. Falling back to fork from Tater's warmup thread can crash
    # inside Apple's Network framework child-at-fork handlers before the engine
    # worker starts. The worker still shuts down its llama-server child cleanly.
    return os.name != "nt" and sys.platform != "darwin"
