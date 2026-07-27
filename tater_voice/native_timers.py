from __future__ import annotations

import asyncio
import contextlib
import math
import re
import uuid
from typing import Any, Dict, List, Optional

DEFAULT_SNOOZE_SECONDS = 300
MAX_TIMER_SECONDS = 7 * 24 * 60 * 60


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        with contextlib.suppress(Exception):
            return value.decode("utf-8")
    return str(value).strip()


def _as_int(
    value: Any,
    default: int = 0,
    *,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    try:
        out = int(round(float(value)))
    except Exception:
        out = int(default)
    if minimum is not None:
        out = max(int(minimum), out)
    if maximum is not None:
        out = min(int(maximum), out)
    return out


def _norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _text(value).lower())


def _public_timer(raw: Any, selector: str = "") -> Dict[str, Any]:
    row = raw if isinstance(raw, dict) else {}
    remaining_ms = _as_int(row.get("remaining_ms"), 0, minimum=0)
    original_duration_ms = _as_int(
        row.get("original_duration_ms") or row.get("duration_ms"),
        0,
        minimum=0,
    )
    name = _text(row.get("name") or row.get("label"))
    state = _text(row.get("state") or ("ringing" if row.get("ringing") else "armed"))
    return {
        "id": _text(row.get("id") or row.get("timer_id")),
        "selector": _text(row.get("selector") or selector),
        "name": name,
        "label": name,
        "state": state,
        "duration_s": int(math.ceil(original_duration_ms / 1000.0)) if original_duration_ms else 0,
        "duration_ms": original_duration_ms,
        "original_duration_s": int(math.ceil(original_duration_ms / 1000.0)) if original_duration_ms else 0,
        "original_duration_ms": original_duration_ms,
        "remaining_s": int(math.ceil(remaining_ms / 1000.0)) if remaining_ms else 0,
        "remaining_ms": remaining_ms,
    }


async def _request(
    selector: str,
    message_type: str,
    payload: Optional[Dict[str, Any]] = None,
    *,
    timeout_s: float = 3.0,
) -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        return {
            "ok": False,
            "code": "missing_selector",
            "message": "I need a satellite for that timer.",
        }
    try:
        from . import native_satellite

        result = await native_satellite.send_request(
            token,
            message_type,
            payload if isinstance(payload, dict) else {},
            timeout_s=timeout_s,
        )
    except asyncio.TimeoutError:
        return {
            "ok": False,
            "code": "timer_timeout",
            "message": "The satellite did not answer the timer request.",
        }
    except Exception as exc:
        return {
            "ok": False,
            "code": "satellite_unavailable",
            "message": str(exc) or "The satellite is unavailable.",
        }

    out = dict(result) if isinstance(result, dict) else {}
    out.setdefault("ok", False)
    timers = out.get("timers")
    if isinstance(timers, list):
        out["timers"] = [_public_timer(row, token) for row in timers if isinstance(row, dict)]
    timer = out.get("timer")
    if isinstance(timer, dict):
        out["timer"] = _public_timer(timer, token)
    return out


async def _connected_selectors(*, room: str = "") -> List[str]:
    from . import native_satellite

    snapshot = await native_satellite.status()
    clients = snapshot.get("clients") if isinstance(snapshot, dict) else {}
    if not isinstance(clients, dict):
        return []
    room_token = _norm(room)
    selectors: List[str] = []
    for selector, row in clients.items():
        if not isinstance(row, dict) or not bool(row.get("connected")):
            continue
        if room_token and _norm(row.get("room")) != room_token:
            continue
        selectors.append(_text(selector))
    return [token for token in selectors if token]


def _filter_timers(
    timers: List[Dict[str, Any]],
    *,
    timer_id: str = "",
    name: str = "",
    duration_s: int = 0,
) -> List[Dict[str, Any]]:
    timer_token = _text(timer_id)
    name_token = _norm(name)
    duration = _as_int(duration_s, 0, minimum=0)
    rows = timers
    if timer_token:
        rows = [row for row in rows if _text(row.get("id")) == timer_token]
    if name_token:
        rows = [row for row in rows if _norm(row.get("name") or row.get("label")) == name_token]
    if duration > 0:
        rows = [
            row
            for row in rows
            if _as_int(row.get("original_duration_s") or row.get("duration_s"), 0) == duration
        ]
    return rows


async def create_timer(
    selector: str,
    duration_s: int,
    *,
    label: str = "",
    name: str = "",
    room: str = "",
    source: str = "",
) -> Dict[str, Any]:
    del room
    token = _text(selector)
    seconds = _as_int(duration_s, 0, minimum=1, maximum=MAX_TIMER_SECONDS)
    if not token:
        return {
            "ok": False,
            "code": "missing_selector",
            "message": "I need a satellite to start a timer.",
        }
    if seconds <= 0:
        return {
            "ok": False,
            "code": "invalid_duration",
            "message": "The timer duration must be greater than zero.",
        }

    timer_id = uuid.uuid4().hex[:12]
    timer_name = _text(name or label)[:63]
    result = await _request(
        token,
        "timer.start",
        {
            "id": timer_id,
            "name": timer_name,
            "label": timer_name,
            "duration_ms": seconds * 1000,
            "original_duration_ms": seconds * 1000,
            "source": _text(source) or "tater",
        },
    )
    result["delivered"] = bool(result.get("ok"))
    return result


async def status(
    *,
    selector: str = "",
    timer_id: str = "",
    room: str = "",
    name: str = "",
    duration_s: int = 0,
) -> Dict[str, Any]:
    token = _text(selector)
    selectors = [token] if token else await _connected_selectors(room=room)
    if not selectors:
        return {
            "ok": False if token else True,
            "code": "satellite_unavailable" if token else "",
            "running": False,
            "timers": [],
            "count": 0,
        }

    responses = await asyncio.gather(
        *[_request(target, "timer.list", {}, timeout_s=2.0) for target in selectors],
        return_exceptions=True,
    )
    timers: List[Dict[str, Any]] = []
    answered = 0
    for response in responses:
        if not isinstance(response, dict) or not bool(response.get("ok")):
            continue
        answered += 1
        rows = response.get("timers")
        if isinstance(rows, list):
            timers.extend(row for row in rows if isinstance(row, dict))

    timers = _filter_timers(
        timers,
        timer_id=timer_id,
        name=name,
        duration_s=duration_s,
    )
    timers.sort(
        key=lambda row: (
            _text(row.get("state")) != "ringing",
            _as_int(row.get("remaining_ms"), 0),
            _text(row.get("name")),
        )
    )
    return {
        "ok": answered > 0,
        "running": bool(timers),
        "timers": timers,
        "count": len(timers),
        "satellites_queried": len(selectors),
        "satellites_answered": answered,
    }


async def cancel_timer(
    *,
    timer_id: str = "",
    selector: str = "",
    room: str = "",
    name: str = "",
    duration_s: int = 0,
    cancel_all: bool = False,
    source: str = "",
) -> Dict[str, Any]:
    token = _text(selector)
    selectors = [token] if token else await _connected_selectors(room=room)
    if not selectors:
        return {
            "ok": False,
            "code": "satellite_unavailable",
            "cancelled": 0,
            "timers": [],
        }

    payload = {
        "id": _text(timer_id),
        "name": _text(name)[:63],
        "original_duration_ms": _as_int(duration_s, 0, minimum=0) * 1000,
        "all": bool(cancel_all),
        "source": _text(source) or "tater",
    }
    responses = await asyncio.gather(
        *[_request(target, "timer.cancel", payload) for target in selectors],
        return_exceptions=True,
    )
    cancelled = 0
    timers: List[Dict[str, Any]] = []
    code = ""
    message = ""
    answered = 0
    for response in responses:
        if not isinstance(response, dict):
            continue
        if bool(response.get("ok")):
            answered += 1
        cancelled += _as_int(response.get("affected") or response.get("cancelled"), 0, minimum=0)
        rows = response.get("timers")
        if isinstance(rows, list):
            timers.extend(row for row in rows if isinstance(row, dict))
        if not code and response.get("code"):
            code = _text(response.get("code"))
            message = _text(response.get("message"))
    return {
        "ok": answered > 0,
        "code": code,
        "message": message,
        "cancelled": cancelled,
        "delivered": answered,
        "timers": timers,
    }


async def snooze_timer(
    *,
    selector: str = "",
    timer_id: str = "",
    room: str = "",
    name: str = "",
    original_duration_s: int = 0,
    duration_s: int = DEFAULT_SNOOZE_SECONDS,
    source: str = "",
) -> Dict[str, Any]:
    token = _text(selector)
    selectors = [token] if token else await _connected_selectors(room=room)
    seconds = _as_int(duration_s, DEFAULT_SNOOZE_SECONDS, minimum=1, maximum=24 * 60 * 60)
    if not selectors:
        return {
            "ok": False,
            "code": "satellite_unavailable",
            "snoozed": 0,
            "timers": [],
        }

    payload = {
        "id": _text(timer_id),
        "name": _text(name)[:63],
        "original_duration_ms": _as_int(original_duration_s, 0, minimum=0) * 1000,
        "duration_ms": seconds * 1000,
        "source": _text(source) or "tater",
    }
    responses = await asyncio.gather(
        *[_request(target, "timer.snooze", payload) for target in selectors],
        return_exceptions=True,
    )
    snoozed = 0
    timers: List[Dict[str, Any]] = []
    answered = 0
    code = ""
    message = ""
    for response in responses:
        if not isinstance(response, dict):
            continue
        if bool(response.get("ok")):
            answered += 1
        snoozed += _as_int(response.get("affected") or response.get("snoozed"), 0, minimum=0)
        rows = response.get("timers")
        if isinstance(rows, list):
            timers.extend(row for row in rows if isinstance(row, dict))
        if not code and response.get("code"):
            code = _text(response.get("code"))
            message = _text(response.get("message"))
    return {
        "ok": answered > 0,
        "code": code,
        "message": message,
        "snoozed": snoozed,
        "delivered": answered,
        "timers": timers,
    }


async def sync_selector(selector: str) -> Dict[str, Any]:
    # Timer state lives only on the satellite. Reconnects must never clear,
    # restore, or otherwise mutate it.
    return {"ok": True, "selector": _text(selector), "synced": 0}


async def handle_device_event(selector: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    # Lifecycle events are telemetry only. Tater intentionally keeps no timer
    # database and asks the satellite for current state when needed.
    body = payload if isinstance(payload, dict) else {}
    return {
        "ok": True,
        "selector": _text(selector),
        "event": _text(body.get("event") or body.get("state")),
        "timer_id": _text(body.get("id") or body.get("timer_id")),
    }


def start_scheduler() -> None:
    # Compatibility no-op for callers from older Tater startup paths.
    return None


async def stop_scheduler() -> None:
    # Compatibility no-op for callers from older Tater shutdown paths.
    return None
