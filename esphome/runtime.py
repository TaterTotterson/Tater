from __future__ import annotations

from typing import Any, Dict, List

from . import device_runtime


def _vp():
    from . import voice_pipeline as vp

    return vp


def selector_runtime(selector: str) -> Dict[str, Any]:
    return _vp()._selector_runtime(selector)


def run_async_blocking(awaitable: Any, *, timeout: float = 20.0) -> Any:
    return _vp()._run_async_blocking(awaitable, timeout=timeout)


def text(value: Any) -> str:
    return _vp()._text(value)


def lower(value: Any) -> str:
    return _vp()._lower(value)


def as_int(value: Any, default: int = 0, *, minimum: int | None = None, maximum: int | None = None) -> int:
    return _vp()._as_int(value, default, minimum=minimum, maximum=maximum)


def as_bool(value: Any, default: bool = False) -> bool:
    return _vp()._as_bool(value, default)


def payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _vp()._payload_values(payload)


def payload_selector(payload: Dict[str, Any]) -> str:
    return _vp()._payload_selector(payload)


def voice_config_snapshot() -> Dict[str, Any]:
    return _vp()._voice_config_snapshot()


def voice_metrics_snapshot() -> Dict[str, Any]:
    return _vp()._voice_metrics_snapshot()


def discovery_stats() -> Dict[str, Any]:
    return device_runtime.discovery_stats()


def native_stats() -> Dict[str, Any]:
    return device_runtime.native_stats()


def client_row_snapshot_sync(selector: str) -> Dict[str, Any]:
    return device_runtime.client_row_snapshot_sync(selector)


def load_satellite_registry() -> List[Dict[str, Any]]:
    rows = _vp()._load_satellite_registry()
    return rows if isinstance(rows, list) else []


def satellite_lookup(selector: str) -> Dict[str, Any]:
    result = _vp()._satellite_lookup(selector)
    return result if isinstance(result, dict) else {}


def upsert_satellite(row: Dict[str, Any]) -> Dict[str, Any]:
    result = _vp()._upsert_satellite(row)
    return result if isinstance(result, dict) else {}


def remove_satellite(selector: str) -> bool:
    return bool(_vp()._remove_satellite(selector))


def set_satellite_selected(selector: str, selected: bool) -> None:
    _vp()._set_satellite_selected(selector, selected)


def satellite_host_from_selector(selector: str) -> str:
    return _vp()._satellite_host_from_selector(selector)


def load_wyoming_tts_voice_catalog() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows, meta = _vp()._load_wyoming_tts_voice_catalog()
    return (rows if isinstance(rows, list) else [], meta if isinstance(meta, dict) else {})


def load_piper_tts_model_catalog() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows, meta = _vp()._load_piper_tts_model_catalog()
    return (rows if isinstance(rows, list) else [], meta if isinstance(meta, dict) else {})


def resolve_stt_backend() -> tuple[str, str]:
    return _vp()._resolve_stt_backend()


def resolve_tts_backend() -> tuple[str, str]:
    return _vp()._resolve_tts_backend()


def status() -> Dict[str, Any]:
    result = device_runtime.status()
    return result if isinstance(result, dict) else {}


def discover_once() -> List[Dict[str, Any]]:
    rows = run_async_blocking(device_runtime.discover_mdns_once(), timeout=30.0)
    return rows if isinstance(rows, list) else []


def reconcile_once(*, force: bool = True, timeout: float = 45.0) -> Dict[str, Any]:
    result = run_async_blocking(device_runtime.reconcile_once(force=force), timeout=timeout)
    return result if isinstance(result, dict) else {}


def disconnect_selector(selector: str, *, reason: str, timeout: float = 20.0) -> None:
    run_async_blocking(device_runtime.disconnect_selector(selector, reason=reason), timeout=timeout)


def command_entity(
    selector: str,
    *,
    entity_key: str,
    command: str,
    value: Any = None,
    options: Dict[str, Any] | None = None,
    timeout: float = 20.0,
) -> Dict[str, Any]:
    result = run_async_blocking(
        device_runtime.command_entity(
            selector,
            entity_key=entity_key,
            command=command,
            value=value,
            options=options,
        ),
        timeout=timeout,
    )
    return result if isinstance(result, dict) else {}


def logs_start(selector: str, *, timeout: float = 20.0) -> Dict[str, Any]:
    result = run_async_blocking(device_runtime.logs_start(selector), timeout=timeout)
    return result if isinstance(result, dict) else {}


def logs_poll(selector: str, *, after_seq: int = 0, timeout: float = 20.0) -> Dict[str, Any]:
    result = run_async_blocking(device_runtime.logs_poll(selector, after_seq=after_seq), timeout=timeout)
    return result if isinstance(result, dict) else {}


def logs_stop(selector: str, *, force: bool = False, timeout: float = 20.0) -> Dict[str, Any]:
    result = run_async_blocking(device_runtime.logs_stop(selector, force=force), timeout=timeout)
    return result if isinstance(result, dict) else {}


def is_running() -> bool:
    runtime = selector_runtime("__esphome_home__")
    return bool(runtime.get("service_running"))


async def startup() -> None:
    if is_running():
        return
    await _vp().startup()
    selector_runtime("__esphome_home__")["service_running"] = True


async def shutdown() -> None:
    if not is_running():
        return
    await _vp().shutdown()
    selector_runtime("__esphome_home__")["service_running"] = False


def include_routes(app: Any) -> None:
    app.include_router(_vp().router)
