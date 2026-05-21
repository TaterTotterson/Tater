import os
import platform as host_platform
from typing import Any, Dict, List


def _human_bytes(value: Any) -> str:
    try:
        size = float(value)
    except Exception:
        return ""
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if abs(size) < 1024.0 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024.0
    return ""


def _memory_info() -> Dict[str, str]:
    out: Dict[str, str] = {}
    meminfo_path = "/proc/meminfo"
    try:
        if os.path.exists(meminfo_path):
            values: Dict[str, int] = {}
            with open(meminfo_path, "r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    key, _, rest = line.partition(":")
                    parts = rest.strip().split()
                    if not key or not parts:
                        continue
                    try:
                        amount_kb = int(parts[0])
                    except Exception:
                        continue
                    values[key] = amount_kb * 1024
            total = values.get("MemTotal")
            available = values.get("MemAvailable") or values.get("MemFree")
            if total:
                out["memory_total_bytes"] = str(total)
                out["memory_total"] = _human_bytes(total)
            if available:
                out["memory_available_bytes"] = str(available)
                out["memory_available"] = _human_bytes(available)
            if out:
                out["memory_source"] = meminfo_path
                return out
    except Exception:
        pass

    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        phys_pages = os.sysconf("SC_PHYS_PAGES")
        total = int(page_size) * int(phys_pages)
        out["memory_total_bytes"] = str(total)
        out["memory_total"] = _human_bytes(total)
        try:
            available_pages = os.sysconf("SC_AVPHYS_PAGES")
            available = int(page_size) * int(available_pages)
            out["memory_available_bytes"] = str(available)
            out["memory_available"] = _human_bytes(available)
        except Exception:
            pass
        out["memory_source"] = "os.sysconf"
    except Exception:
        pass
    return out


def _process_info(limit: int = 50) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    proc_root = "/proc"
    try:
        names = [name for name in os.listdir(proc_root) if name.isdigit()]
    except Exception:
        return out

    names.sort(key=lambda item: int(item))
    rows: List[Dict[str, str]] = []
    for name in names[: max(1, limit)]:
        base = os.path.join(proc_root, name)
        proc_name = ""
        command = ""
        try:
            with open(os.path.join(base, "comm"), "r", encoding="utf-8", errors="replace") as handle:
                proc_name = handle.read().strip()
        except Exception:
            proc_name = ""
        try:
            with open(os.path.join(base, "cmdline"), "rb") as handle:
                raw = handle.read(400)
            command = raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
        except Exception:
            command = ""
        process_label = proc_name or (command.split(" ")[0] if command else "") or "unknown"
        rows.append(
            {
                "pid": name,
                "name": process_label[:120],
                "command": (command or proc_name or "unknown")[:240],
            }
        )
    if rows:
        out["process_source"] = proc_root
        out["process_count_visible"] = str(len(names))
        out["process_sample"] = rows
    return out


def spudex_system_info() -> Dict[str, Any]:
    system = host_platform.system() or os.name
    info: Dict[str, Any] = {
        "host_os": system,
        "host_os_release": host_platform.release(),
        "host_os_version": host_platform.version(),
        "machine": host_platform.machine(),
        "python_version": host_platform.python_version(),
        "path_separator": os.sep,
        "command_execution": "argv subprocess execution; no shell, pipes, redirects, or command separators",
    }
    info.update(_memory_info())
    info.update(_process_info())
    return info
