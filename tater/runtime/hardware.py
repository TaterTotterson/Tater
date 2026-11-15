"""Focused helpers extracted from the legacy shared runtime."""

import contextlib
import json
import os
import platform
import re
import shutil
import subprocess
from typing import Any, Dict, List, Optional, Tuple
from .values import _boolish
from .processes import _macos_posix_spawn_kwargs


def _system_ram_snapshot() -> Dict[str, Any]:
    try:
        import psutil  # type: ignore

        mem = psutil.virtual_memory()
        total = max(0, int(getattr(mem, "total", 0) or 0))
        available = max(0, int(getattr(mem, "available", 0) or 0))
        used = max(0, int(getattr(mem, "used", 0) or max(0, total - available)))
        return {
            "available": True,
            "total_bytes": total,
            "available_bytes": available,
            "used_bytes": used,
            "percent": round((used / total) * 100.0, 2) if total > 0 else 0.0,
        }
    except Exception:
        pass
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        pages = int(os.sysconf("SC_PHYS_PAGES"))
        total = max(0, page_size * pages)
        return {
            "available": total > 0,
            "total_bytes": total,
            "available_bytes": 0,
            "used_bytes": 0,
            "percent": 0.0,
        }
    except Exception:
        return {
            "available": False,
            "total_bytes": 0,
            "available_bytes": 0,
            "used_bytes": 0,
            "percent": 0.0,
        }


def _system_cpu_snapshot() -> Dict[str, Any]:
    try:
        import psutil  # type: ignore

        percent = float(psutil.cpu_percent(interval=None))
        load_avg: List[float] = []
        try:
            load_avg = [round(float(item), 2) for item in os.getloadavg()]
        except Exception:
            load_avg = []
        return {
            "available": True,
            "percent": round(max(0.0, min(100.0, percent)), 2),
            "logical_count": int(psutil.cpu_count(logical=True) or 0),
            "physical_count": int(psutil.cpu_count(logical=False) or 0),
            "load_average": load_avg,
        }
    except Exception:
        pass
    try:
        load_avg = [round(float(item), 2) for item in os.getloadavg()]
    except Exception:
        load_avg = []
    return {
        "available": False,
        "percent": 0.0,
        "logical_count": int(os.cpu_count() or 0),
        "physical_count": 0,
        "load_average": load_avg,
    }


def _system_gpu_float(value: Any) -> Optional[float]:
    token = str(value or "").strip()
    if not token or token.lower() in {"n/a", "[not supported]", "not supported", "none"}:
        return None
    token = token.replace("%", "").replace("MiB", "").replace("W", "").strip()
    try:
        parsed = float(token)
    except Exception:
        return None
    return parsed if parsed >= 0 else None


def _system_gpu_mib_to_bytes(value: Any) -> int:
    parsed = _system_gpu_float(value)
    if parsed is None:
        return 0
    return max(0, int(parsed * 1024 * 1024))


def _system_gpu_bytes(value: Any) -> int:
    token = str(value or "").strip().replace(",", "")
    if not token or token.lower() in {"n/a", "[not supported]", "not supported", "none"}:
        return 0
    match = re.search(r"[-+]?\d+(?:\.\d+)?", token)
    if not match:
        return 0
    try:
        parsed = float(match.group(0))
    except Exception:
        return 0
    if parsed <= 0:
        return 0
    lower = token.lower()
    if "gib" in lower or re.search(r"\bgb\b", lower):
        return max(0, int(parsed * 1024 * 1024 * 1024))
    if "mib" in lower or re.search(r"\bmb\b", lower):
        return max(0, int(parsed * 1024 * 1024))
    if "kib" in lower or re.search(r"\bkb\b", lower):
        return max(0, int(parsed * 1024))
    return max(0, int(parsed))


def _system_gpu_clamped_percent(value: Any) -> Optional[float]:
    parsed = _system_gpu_float(value)
    if parsed is None:
        return None
    return round(max(0.0, min(100.0, parsed)), 2)


def _system_gpu_json_payload(text: Any) -> Optional[Any]:
    raw = str(text or "").strip()
    if not raw:
        return None
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start : end + 1]
    try:
        return json.loads(raw)
    except Exception:
        return None


def _system_gpu_row_value(
    row: Dict[str, Any],
    options: List[Tuple[str, ...]],
    *,
    excluded_fragments: Tuple[str, ...] = tuple(),
) -> Any:
    normalized_items = [(re.sub(r"[^a-z0-9%]+", " ", str(key or "").lower()).strip(), value) for key, value in row.items()]
    excluded = [re.sub(r"[^a-z0-9%]+", " ", str(fragment or "").lower()).strip() for fragment in excluded_fragments]
    excluded = [fragment for fragment in excluded if fragment]
    for fragments in options:
        wanted = [re.sub(r"[^a-z0-9%]+", " ", str(fragment or "").lower()).strip() for fragment in fragments]
        wanted = [fragment for fragment in wanted if fragment]
        if not wanted:
            continue
        for key, value in normalized_items:
            if any(fragment in key for fragment in excluded):
                continue
            if all(fragment in key for fragment in wanted):
                return value
    return None


def _nvidia_smi_vram_snapshot() -> Optional[Dict[str, Any]]:
    exe = shutil.which("nvidia-smi")
    if not exe:
        return None
    query = (
        "index,name,utilization.gpu,memory.total,memory.used,memory.free,"
        "temperature.gpu,power.draw,power.limit"
    )
    try:
        proc = subprocess.run(
            [exe, f"--query-gpu={query}", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=1.2,
            check=False,
            **_macos_posix_spawn_kwargs(),
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None

    devices: List[Dict[str, Any]] = []
    total = 0
    used = 0
    free = 0
    utilization_values: List[float] = []
    for line in str(proc.stdout or "").splitlines():
        parts = [part.strip() for part in line.split(",")]
        if len(parts) < 9:
            continue
        index_raw, name, util_raw, total_raw, used_raw, free_raw, temp_raw, power_raw, power_limit_raw = parts[:9]
        device_total = _system_gpu_mib_to_bytes(total_raw)
        device_used = _system_gpu_mib_to_bytes(used_raw)
        device_free = _system_gpu_mib_to_bytes(free_raw)
        utilization = _system_gpu_float(util_raw)
        temperature = _system_gpu_float(temp_raw)
        power_draw = _system_gpu_float(power_raw)
        power_limit = _system_gpu_float(power_limit_raw)
        if utilization is not None:
            utilization_values.append(max(0.0, min(100.0, utilization)))
        total += device_total
        used += device_used
        free += device_free
        try:
            index = int(float(index_raw))
        except Exception:
            index = len(devices)
        devices.append(
            {
                "index": index,
                "name": name or f"cuda:{index}",
                "total_bytes": device_total,
                "free_bytes": device_free,
                "used_bytes": device_used,
                "percent": round((device_used / device_total) * 100.0, 2) if device_total > 0 else 0.0,
                "utilization_percent": round(max(0.0, min(100.0, utilization)), 2) if utilization is not None else None,
                "temperature_c": round(temperature, 1) if temperature is not None else None,
                "power_draw_w": round(power_draw, 1) if power_draw is not None else None,
                "power_limit_w": round(power_limit, 1) if power_limit is not None else None,
            }
        )

    if not devices:
        return None
    utilization_percent: Optional[float] = None
    if utilization_values:
        utilization_percent = round(sum(utilization_values) / len(utilization_values), 2)
    return {
        "available": total > 0,
        "backend": "nvidia",
        "total_bytes": total,
        "free_bytes": free,
        "used_bytes": used,
        "percent": round((used / total) * 100.0, 2) if total > 0 else 0.0,
        "utilization_percent": utilization_percent,
        "devices": devices,
    }


def _rocm_smi_vram_snapshot() -> Optional[Dict[str, Any]]:
    exe = shutil.which("rocm-smi") or shutil.which("rocm-smi.py")
    if not exe:
        return None
    commands = [
        [exe, "--showproductname", "--showuse", "--showmemuse", "--showmeminfo", "vram", "--showtemp", "--showpower", "--json"],
        [exe, "--showuse", "--showmemuse", "--showmeminfo", "vram", "--showtemp", "--showpower", "--json"],
        [exe, "--json"],
    ]
    payload: Optional[Any] = None
    for command in commands:
        try:
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=1.8,
                check=False,
                **_macos_posix_spawn_kwargs(),
            )
        except Exception:
            continue
        if proc.returncode != 0:
            continue
        payload = _system_gpu_json_payload(proc.stdout)
        if payload is not None:
            break
    if not isinstance(payload, dict):
        return None

    devices: List[Dict[str, Any]] = []
    total = 0
    used = 0
    free = 0
    utilization_values: List[float] = []
    for key, raw_row in payload.items():
        if not isinstance(raw_row, dict):
            continue
        row = dict(raw_row)
        key_text = str(key or "")
        if not key_text.lower().startswith(("card", "gpu")) and not any("vram" in str(k).lower() or "gpu use" in str(k).lower() for k in row.keys()):
            continue
        try:
            index = int(float(re.sub(r"[^0-9.]+", "", key_text) or len(devices)))
        except Exception:
            index = len(devices)
        name = str(
            _system_gpu_row_value(
                row,
                [
                    ("card", "series"),
                    ("card", "model"),
                    ("product", "name"),
                    ("gpu", "name"),
                    ("device", "name"),
                ],
            )
            or f"ROCm GPU {index}"
        ).strip()
        utilization = _system_gpu_clamped_percent(
            _system_gpu_row_value(
                row,
                [
                    ("gpu", "use"),
                    ("gpu", "busy"),
                    ("activity",),
                    ("use", "%"),
                ],
            )
        )
        device_total = _system_gpu_bytes(
            _system_gpu_row_value(
                row,
                [
                    ("vram", "total", "memory"),
                    ("vram", "total"),
                    ("memory", "total", "vram"),
                ],
                excluded_fragments=("used", "free", "allocated", "%"),
            )
        )
        device_used = _system_gpu_bytes(
            _system_gpu_row_value(
                row,
                [
                    ("vram", "used", "memory"),
                    ("vram", "used"),
                    ("memory", "used", "vram"),
                ],
                excluded_fragments=("%",),
            )
        )
        device_free = _system_gpu_bytes(
            _system_gpu_row_value(
                row,
                [
                    ("vram", "free", "memory"),
                    ("vram", "free"),
                    ("memory", "free", "vram"),
                ],
            )
        )
        memory_percent = _system_gpu_clamped_percent(
            _system_gpu_row_value(
                row,
                [
                    ("vram%",),
                    ("memory", "allocated", "vram"),
                    ("memory", "use"),
                ],
            )
        )
        if device_used <= 0 and device_total > 0 and memory_percent is not None:
            device_used = max(0, int(device_total * (memory_percent / 100.0)))
        if device_free <= 0 and device_total > 0:
            device_free = max(0, device_total - device_used)
        temperature = _system_gpu_float(
            _system_gpu_row_value(row, [("temperature", "edge"), ("temperature", "junction"), ("temperature",), ("temp",)])
        )
        power_draw = _system_gpu_float(
            _system_gpu_row_value(row, [("average", "power"), ("power", "draw"), ("power", "w")])
        )
        if utilization is not None:
            utilization_values.append(utilization)
        total += device_total
        used += device_used
        free += device_free
        devices.append(
            {
                "index": index,
                "name": name or f"ROCm GPU {index}",
                "total_bytes": device_total,
                "free_bytes": device_free,
                "used_bytes": device_used,
                "percent": round((device_used / device_total) * 100.0, 2) if device_total > 0 else 0.0,
                "utilization_percent": utilization,
                "temperature_c": round(temperature, 1) if temperature is not None else None,
                "power_draw_w": round(power_draw, 1) if power_draw is not None else None,
            }
        )

    if not devices:
        return None
    utilization_percent: Optional[float] = None
    if utilization_values:
        utilization_percent = round(sum(utilization_values) / len(utilization_values), 2)
    return {
        "available": True,
        "backend": "rocm",
        "total_bytes": total,
        "free_bytes": free,
        "used_bytes": used,
        "percent": round((used / total) * 100.0, 2) if total > 0 else 0.0,
        "utilization_percent": utilization_percent,
        "devices": devices,
    }


def _jetson_tegrastats_snapshot() -> Optional[Dict[str, Any]]:
    exe = shutil.which("tegrastats")
    if not exe:
        return None
    output = ""
    commands = ([exe, "--interval", "200", "--count", "1"], [exe, "--interval", "200"])
    for command in commands:
        try:
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=1.2,
                check=False,
                **_macos_posix_spawn_kwargs(),
            )
            output = str(proc.stdout or proc.stderr or "").strip()
        except subprocess.TimeoutExpired as exc:
            raw_output = exc.stdout or exc.stderr or ""
            output = raw_output.decode("utf-8", "replace") if isinstance(raw_output, bytes) else str(raw_output or "")
            output = output.strip()
        except Exception:
            output = ""
        if output:
            break
    if not output:
        return None
    line = output.splitlines()[0]
    util_match = re.search(r"\bGR3D_FREQ\s+(\d+(?:\.\d+)?)%", line)
    utilization = _system_gpu_clamped_percent(util_match.group(1) if util_match else None)
    ram_match = re.search(r"\bRAM\s+(\d+(?:\.\d+)?)/(\d+(?:\.\d+)?)MB\b", line, flags=re.IGNORECASE)
    shared_used = _system_gpu_mib_to_bytes(ram_match.group(1)) if ram_match else 0
    shared_total = _system_gpu_mib_to_bytes(ram_match.group(2)) if ram_match else 0
    temp_match = re.search(r"\bGPU@(\d+(?:\.\d+)?)C\b", line)
    temperature = _system_gpu_float(temp_match.group(1) if temp_match else None)
    device = {
        "index": 0,
        "name": "Jetson GPU",
        "total_bytes": 0,
        "free_bytes": 0,
        "used_bytes": 0,
        "percent": 0.0,
        "utilization_percent": utilization,
        "temperature_c": round(temperature, 1) if temperature is not None else None,
        "shared_memory_total_bytes": shared_total,
        "shared_memory_used_bytes": shared_used,
        "unified": True,
    }
    if utilization is None and shared_total <= 0:
        return None
    return {
        "available": True,
        "backend": "jetson",
        "total_bytes": 0,
        "free_bytes": 0,
        "used_bytes": 0,
        "percent": 0.0,
        "utilization_percent": utilization,
        "unified": True,
        "devices": [device],
    }


def _apple_ioreg_float(text: str, key: str) -> Optional[float]:
    match = re.search(rf'"{re.escape(key)}"\s*=\s*([-+]?\d+(?:\.\d+)?)', text)
    if not match:
        return None
    return _system_gpu_float(match.group(1))


def _apple_ioreg_int(text: str, key: str) -> int:
    parsed = _apple_ioreg_float(text, key)
    if parsed is None:
        return 0
    return max(0, int(parsed))


def _apple_ioreg_metal_snapshot() -> Optional[Dict[str, Any]]:
    exe = shutil.which("ioreg")
    if not exe:
        return None
    output = ""
    for class_name in ("IOAccelerator", "AGXAccelerator"):
        try:
            proc = subprocess.run(
                [exe, "-r", "-d", "1", "-w", "0", "-c", class_name],
                capture_output=True,
                text=True,
                timeout=1.0,
                check=False,
                **_macos_posix_spawn_kwargs(),
            )
        except Exception:
            continue
        if proc.returncode == 0 and str(proc.stdout or "").strip():
            output = str(proc.stdout or "")
            break
    if not output or "PerformanceStatistics" not in output:
        return None

    perf_match = re.search(r'"PerformanceStatistics"\s*=\s*\{([^}]*)\}', output, flags=re.DOTALL)
    perf_text = perf_match.group(1) if perf_match else output
    utilization_values = [
        value
        for value in (
            _apple_ioreg_float(perf_text, "Device Utilization %"),
            _apple_ioreg_float(perf_text, "Renderer Utilization %"),
            _apple_ioreg_float(perf_text, "Tiler Utilization %"),
        )
        if value is not None
    ]
    utilization = max(utilization_values) if utilization_values else None

    used_candidates = [
        _apple_ioreg_int(perf_text, "In use system memory"),
        _apple_ioreg_int(perf_text, "In use system memory (driver)"),
        _apple_ioreg_int(perf_text, "In use video memory"),
        _apple_ioreg_int(perf_text, "In use vid memory"),
        _apple_ioreg_int(perf_text, "vramUsedBytes"),
    ]
    allocated = _apple_ioreg_int(perf_text, "Alloc system memory")
    used = max([value for value in used_candidates if value > 0] or [0])
    if used <= 0 and allocated > 0:
        used = allocated

    model_match = re.search(r'"model"\s*=\s*"([^"]+)"', output)
    model = str(model_match.group(1) if model_match else "").strip()
    if model and "gpu" not in model.lower():
        model = f"{model} GPU"
    core_count = _apple_ioreg_int(output, "gpu-core-count")

    return {
        "name": model or "Apple Metal GPU",
        "used_bytes": used,
        "allocated_bytes": allocated,
        "utilization_percent": round(max(0.0, min(100.0, utilization)), 2) if utilization is not None else None,
        "core_count": core_count,
    }


def _apple_metal_vram_snapshot(*, enable_ioreg_probe: Optional[bool] = None) -> Optional[Dict[str, Any]]:
    if platform.system().lower() != "darwin":
        return None
    apple_silicon = platform.machine().lower() in {"arm64", "aarch64"}
    used_candidates: List[int] = []
    total_candidates: List[int] = []
    detail_parts: List[str] = []
    ioreg_enabled = (
        _boolish(os.getenv("TATER_APPLE_IOREG_GPU_PROBE_ENABLED"), default=False)
        if enable_ioreg_probe is None
        else bool(enable_ioreg_probe)
    )
    ioreg_snapshot = _apple_ioreg_metal_snapshot() if ioreg_enabled else None
    if ioreg_snapshot:
        detail_parts.append("IORegistry")
        used_candidates.append(max(0, int(ioreg_snapshot.get("used_bytes") or 0)))
    if not ioreg_snapshot:
        try:
            import torch  # type: ignore

            mps_backend = getattr(getattr(torch, "backends", None), "mps", None)
            mps_runtime = getattr(torch, "mps", None)
            if mps_backend is not None and bool(getattr(mps_backend, "is_available", lambda: False)()):
                detail_parts.append("MPS")
                for attr in ("current_allocated_memory", "driver_allocated_memory"):
                    getter = getattr(mps_runtime, attr, None) if mps_runtime is not None else None
                    if callable(getter):
                        with contextlib.suppress(Exception):
                            used_candidates.append(max(0, int(getter() or 0)))
                getter = getattr(mps_runtime, "recommended_max_memory", None) if mps_runtime is not None else None
                if callable(getter):
                    with contextlib.suppress(Exception):
                        total_candidates.append(max(0, int(getter() or 0)))
        except Exception:
            pass
    ram_snapshot = _system_ram_snapshot()
    unified_total = max(0, int(ram_snapshot.get("total_bytes") or 0))
    unified_memory = bool(apple_silicon)
    if unified_total > 0 and unified_memory:
        total_candidates.append(unified_total)
    if not detail_parts and not apple_silicon:
        return None
    used = max(used_candidates) if used_candidates else 0
    total = max(total_candidates) if total_candidates else 0
    if total > 0:
        used = min(used, total)
    free = max(0, total - used) if total > 0 else 0
    utilization_percent = None
    if ioreg_snapshot:
        utilization_percent = ioreg_snapshot.get("utilization_percent")
    utilization_percent = _system_gpu_clamped_percent(utilization_percent)
    device_name = str((ioreg_snapshot or {}).get("name") or "Apple Metal GPU").strip() or "Apple Metal GPU"
    core_count = max(0, int((ioreg_snapshot or {}).get("core_count") or 0))
    device_detail = " / ".join(dict.fromkeys(detail_parts)) if detail_parts else "Unified memory"
    return {
        "available": True,
        "backend": "metal",
        "total_bytes": total,
        "free_bytes": free,
        "used_bytes": used,
        "percent": round((used / total) * 100.0, 2) if total > 0 else 0.0,
        "utilization_percent": utilization_percent,
        "unified": unified_memory,
        "devices": [
            {
                "index": 0,
                "name": device_name,
                "total_bytes": total,
                "free_bytes": free,
                "used_bytes": used,
                "percent": round((used / total) * 100.0, 2) if total > 0 else 0.0,
                "utilization_percent": utilization_percent,
                "unified": unified_memory,
                "shared_memory_total_bytes": total if unified_memory else 0,
                "shared_memory_used_bytes": used if unified_memory else 0,
                "core_count": core_count,
                "detail": device_detail,
            }
        ],
    }


def _torch_cuda_vram_snapshot() -> Optional[Dict[str, Any]]:
    try:
        import torch  # type: ignore

        cuda = getattr(torch, "cuda", None)
        if cuda is None or not bool(cuda.is_available()):
            return None
        total = 0
        free = 0
        devices: List[Dict[str, Any]] = []
        for index in range(int(cuda.device_count())):
            props = cuda.get_device_properties(index)
            device_total = max(0, int(getattr(props, "total_memory", 0) or 0))
            device_free = 0
            try:
                free_pair = cuda.mem_get_info(index)
                device_free = max(0, int(free_pair[0] or 0))
                if free_pair and len(free_pair) > 1:
                    device_total = max(device_total, int(free_pair[1] or 0))
            except Exception:
                pass
            device_used = max(0, device_total - device_free) if device_free else 0
            total += device_total
            free += device_free
            devices.append(
                {
                    "index": index,
                    "name": str(getattr(props, "name", "") or f"cuda:{index}"),
                    "total_bytes": device_total,
                    "free_bytes": device_free,
                    "used_bytes": device_used,
                    "percent": round((device_used / device_total) * 100.0, 2) if device_total > 0 and device_used > 0 else 0.0,
                    "utilization_percent": None,
                }
            )
        used = max(0, total - free) if free else 0
        backend = "cuda"
        try:
            if str(getattr(getattr(torch, "version", None), "hip", "") or "").strip():
                backend = "rocm"
        except Exception:
            pass
        return {
            "available": total > 0,
            "backend": backend,
            "total_bytes": total,
            "free_bytes": free,
            "used_bytes": used,
            "percent": round((used / total) * 100.0, 2) if total > 0 and used > 0 else 0.0,
            "utilization_percent": None,
            "devices": devices,
        }
    except Exception:
        return None


def _merge_gpu_usage_snapshot(memory_snapshot: Dict[str, Any], usage_snapshot: Dict[str, Any], *, backend: str) -> Dict[str, Any]:
    merged = dict(memory_snapshot or {})
    merged["backend"] = backend
    usage_percent = _system_gpu_clamped_percent((usage_snapshot or {}).get("utilization_percent"))
    if usage_percent is not None:
        merged["utilization_percent"] = usage_percent
    usage_devices = [row for row in list((usage_snapshot or {}).get("devices") or []) if isinstance(row, dict)]
    memory_devices = [dict(row) for row in list(merged.get("devices") or []) if isinstance(row, dict)]
    if usage_devices and memory_devices:
        for index, row in enumerate(memory_devices):
            usage_row = usage_devices[min(index, len(usage_devices) - 1)]
            device_usage = _system_gpu_clamped_percent(usage_row.get("utilization_percent"))
            if device_usage is not None:
                row["utilization_percent"] = device_usage
            for key in ("temperature_c", "power_draw_w", "power_limit_w", "shared_memory_total_bytes", "shared_memory_used_bytes", "unified"):
                if usage_row.get(key) is not None:
                    row[key] = usage_row.get(key)
        merged["devices"] = memory_devices
    return merged


def _system_vram_snapshot(*, enable_apple_ioreg_probe: Optional[bool] = None) -> Dict[str, Any]:
    nvidia_snapshot = _nvidia_smi_vram_snapshot()
    if nvidia_snapshot:
        return nvidia_snapshot

    rocm_snapshot = _rocm_smi_vram_snapshot()
    if rocm_snapshot:
        return rocm_snapshot

    jetson_snapshot = _jetson_tegrastats_snapshot()
    if jetson_snapshot:
        torch_snapshot = _torch_cuda_vram_snapshot()
        if torch_snapshot and int(torch_snapshot.get("total_bytes") or 0) > 0:
            return _merge_gpu_usage_snapshot(torch_snapshot, jetson_snapshot, backend="jetson-cuda")
        return jetson_snapshot

    apple_snapshot = _apple_metal_vram_snapshot(enable_ioreg_probe=enable_apple_ioreg_probe)
    if apple_snapshot:
        return apple_snapshot

    torch_snapshot = _torch_cuda_vram_snapshot()
    if torch_snapshot:
        return torch_snapshot

    return _empty_system_vram_snapshot()


def _empty_system_vram_snapshot() -> Dict[str, Any]:
    return {
        "available": False,
        "backend": "",
        "total_bytes": 0,
        "free_bytes": 0,
        "used_bytes": 0,
        "percent": 0.0,
        "utilization_percent": None,
        "devices": [],
    }
