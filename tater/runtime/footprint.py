"""Focused helpers extracted from the legacy shared runtime."""

import os
from typing import Any


def _safe_path_size_bytes(path: Any, *, file_limit: int = 20000) -> int:
    raw = str(path or "").strip()
    if not raw:
        return 0
    try:
        target = os.path.abspath(os.path.expanduser(raw))
    except Exception:
        target = raw
    try:
        if os.path.isfile(target):
            return max(0, int(os.path.getsize(target)))
        if not os.path.isdir(target):
            return 0
        total = 0
        count = 0
        for root, _dirs, files in os.walk(target):
            for filename in files:
                count += 1
                if count > int(file_limit):
                    return total
                try:
                    total += max(0, int(os.path.getsize(os.path.join(root, filename))))
                except Exception:
                    continue
        return total
    except Exception:
        return 0


def _model_memory_footprint_bytes(model: Any) -> int:
    if model is None:
        return 0
    try:
        getter = getattr(model, "get_memory_footprint", None)
        if callable(getter):
            value = getter()
            return max(0, int(value or 0))
    except Exception:
        pass
    total = 0
    for attr in ("parameters", "buffers"):
        try:
            getter = getattr(model, attr, None)
            if not callable(getter):
                continue
            for item in getter():
                try:
                    total += int(item.nelement()) * int(item.element_size())
                except Exception:
                    continue
        except Exception:
            continue
    return max(0, int(total))


def runtime_object_memory_footprint_bytes(model: Any) -> int:
    return _model_memory_footprint_bytes(model)


def runtime_path_size_bytes(path: Any, *, file_limit: int = 20000) -> int:
    return _safe_path_size_bytes(path, file_limit=file_limit)
