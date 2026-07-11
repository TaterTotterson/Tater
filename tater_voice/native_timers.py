from __future__ import annotations

import asyncio
import contextlib
import json
import math
import re
import time
import uuid
from typing import Any, Dict, List, Optional

from helpers import redis_client

TIMER_HASH_KEY = "voice_native_satellite_timers"
SCHEDULER_INTERVAL_S = 0.5
ALARM_RETRY_S = 4.0
DEFAULT_SNOOZE_SECONDS = 300

ACTIVE_STATES = {"armed", "ringing"}

_timer_lock = asyncio.Lock()
_scheduler_task: Optional[asyncio.Task] = None


def _now() -> float:
    return time.time()


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        with contextlib.suppress(Exception):
            return value.decode("utf-8")
    return str(value)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _as_int(value: Any, default: int = 0, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
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
    return re.sub(r"[^a-z0-9]+", "", _text(value).strip().lower())


def _decode_row(timer_id: Any, raw: Any) -> Optional[Dict[str, Any]]:
    try:
        row = json.loads(_text(raw))
    except Exception:
        return None
    if not isinstance(row, dict):
        return None
    row["id"] = _text(row.get("id") or timer_id)
    row["selector"] = _text(row.get("selector"))
    row["room"] = _text(row.get("room"))
    row["label"] = _text(row.get("label"))
    row["state"] = _text(row.get("state") or "armed")
    row["deadline_ts"] = _as_float(row.get("deadline_ts"), 0.0)
    row["duration_s"] = _as_int(row.get("duration_s"), 0, minimum=0)
    row["created_ts"] = _as_float(row.get("created_ts"), _now())
    row["updated_ts"] = _as_float(row.get("updated_ts"), row["created_ts"])
    row["last_alarm_attempt_ts"] = _as_float(row.get("last_alarm_attempt_ts"), 0.0)
    return row


def _load_rows_sync() -> List[Dict[str, Any]]:
    raw_rows = redis_client.hgetall(TIMER_HASH_KEY) or {}
    rows: List[Dict[str, Any]] = []
    if not isinstance(raw_rows, dict):
        return rows
    stale: List[str] = []
    for timer_id, raw in raw_rows.items():
        row = _decode_row(timer_id, raw)
        if row is None:
            stale.append(_text(timer_id))
            continue
        rows.append(row)
    for timer_id in stale:
        with contextlib.suppress(Exception):
            redis_client.hdel(TIMER_HASH_KEY, timer_id)
    return rows


def _save_row_sync(row: Dict[str, Any]) -> None:
    timer_id = _text(row.get("id"))
    if not timer_id:
        return
    row["updated_ts"] = _now()
    redis_client.hset(TIMER_HASH_KEY, timer_id, json.dumps(row, ensure_ascii=False, sort_keys=True))


def _delete_row_sync(timer_id: str) -> None:
    token = _text(timer_id)
    if token:
        redis_client.hdel(TIMER_HASH_KEY, token)


def _remaining_s(row: Dict[str, Any], *, now_ts: Optional[float] = None) -> int:
    state = _text(row.get("state"))
    if state == "ringing":
        return 0
    deadline = _as_float(row.get("deadline_ts"), 0.0)
    if deadline <= 0:
        return 0
    current = _now() if now_ts is None else float(now_ts)
    return max(0, int(math.ceil(deadline - current)))


def _public_row(row: Dict[str, Any], *, now_ts: Optional[float] = None) -> Dict[str, Any]:
    remaining = _remaining_s(row, now_ts=now_ts)
    return {
        "id": _text(row.get("id")),
        "selector": _text(row.get("selector")),
        "room": _text(row.get("room")),
        "label": _text(row.get("label")),
        "state": _text(row.get("state")),
        "duration_s": _as_int(row.get("duration_s"), 0, minimum=0),
        "remaining_s": remaining,
        "remaining_ms": max(0, remaining * 1000),
        "deadline_ts": _as_float(row.get("deadline_ts"), 0.0),
        "created_ts": _as_float(row.get("created_ts"), 0.0),
        "updated_ts": _as_float(row.get("updated_ts"), 0.0),
    }


def _command_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    public = _public_row(row)
    public["duration_ms"] = max(0, _as_int(row.get("duration_s"), 0, minimum=0) * 1000)
    public["source"] = "tater"
    return public


def _matches_selector(row: Dict[str, Any], selector: str) -> bool:
    token = _text(selector)
    return bool(token) and _text(row.get("selector")) == token


def _matches_room(row: Dict[str, Any], room: str) -> bool:
    token = _norm(room)
    return bool(token) and _norm(row.get("room")) == token


async def _send_timer_command(selector: str, message_type: str, payload: Dict[str, Any]) -> bool:
    token = _text(selector)
    if not token:
        return False
    try:
        from . import native_satellite

        if message_type == "timer.alarm" and not await native_satellite.client_has_capability(token, "timers"):
            await native_satellite.send_command(
                token,
                "play.tone",
                {
                    "frequency_hz": 880,
                    "duration_ms": 700,
                    "volume_percent": 80,
                    "tts_kind": "timer",
                    "visual_mode": "timer",
                    "state_after": "idle",
                },
            )
            return True
        await native_satellite.send_command(token, message_type, payload)
        return True
    except Exception:
        return False


async def create_timer(
    selector: str,
    duration_s: int,
    *,
    label: str = "",
    room: str = "",
    source: str = "",
) -> Dict[str, Any]:
    token = _text(selector)
    seconds = _as_int(duration_s, 0, minimum=1, maximum=7 * 24 * 60 * 60)
    if not token:
        return {"ok": False, "code": "missing_selector", "message": "I need a satellite to start a timer."}

    now_ts = _now()
    async with _timer_lock:
        rows = _load_rows_sync()
        existing = [
            row
            for row in rows
            if _text(row.get("state")) in ACTIVE_STATES and _matches_selector(row, token)
        ]
        if existing:
            current = sorted(existing, key=lambda row: _as_float(row.get("deadline_ts"), 0.0))[0]
            return {
                "ok": False,
                "code": "already_running",
                "timer": _public_row(current, now_ts=now_ts),
                "remaining_s": _remaining_s(current, now_ts=now_ts),
            }

        row = {
            "id": uuid.uuid4().hex[:12],
            "selector": token,
            "room": _text(room),
            "label": _text(label),
            "source": _text(source),
            "state": "armed",
            "duration_s": seconds,
            "deadline_ts": now_ts + seconds,
            "created_ts": now_ts,
            "updated_ts": now_ts,
            "last_alarm_attempt_ts": 0.0,
        }
        _save_row_sync(row)

    delivered = await _send_timer_command(token, "timer.arm", _command_payload(row))
    public = _public_row(row)
    return {"ok": True, "timer": public, "delivered": delivered}


async def cancel_timer(
    *,
    timer_id: str = "",
    selector: str = "",
    room: str = "",
    source: str = "",
) -> Dict[str, Any]:
    token = _text(selector)
    timer_token = _text(timer_id)
    room_token = _text(room)
    now_ts = _now()
    async with _timer_lock:
        rows = [row for row in _load_rows_sync() if _text(row.get("state")) in ACTIVE_STATES]
        if timer_token:
            matches = [row for row in rows if _text(row.get("id")) == timer_token]
        else:
            ringing = [row for row in rows if _text(row.get("state")) == "ringing"]
            if ringing:
                matches = ringing
            elif token:
                matches = [row for row in rows if _matches_selector(row, token)]
                if not matches and len(rows) == 1:
                    matches = rows
            elif room_token:
                matches = [row for row in rows if _matches_room(row, room_token)]
                if not matches and len(rows) == 1:
                    matches = rows
            else:
                matches = []

        for row in matches:
            _delete_row_sync(_text(row.get("id")))

    delivered = 0
    for row in matches:
        payload = _public_row(row, now_ts=now_ts)
        payload["source"] = _text(source) or "tater"
        if await _send_timer_command(_text(row.get("selector")), "timer.clear", payload):
            delivered += 1

    return {
        "ok": True,
        "cancelled": len(matches),
        "delivered": delivered,
        "timers": [_public_row(row, now_ts=now_ts) for row in matches],
    }


async def snooze_timer(
    *,
    selector: str = "",
    timer_id: str = "",
    duration_s: int = DEFAULT_SNOOZE_SECONDS,
    source: str = "",
) -> Dict[str, Any]:
    token = _text(selector)
    timer_token = _text(timer_id)
    seconds = _as_int(duration_s, DEFAULT_SNOOZE_SECONDS, minimum=1, maximum=24 * 60 * 60)
    now_ts = _now()
    async with _timer_lock:
        rows = [row for row in _load_rows_sync() if _text(row.get("state")) in ACTIVE_STATES]
        if timer_token:
            matches = [row for row in rows if _text(row.get("id")) == timer_token]
        else:
            ringing = [row for row in rows if _text(row.get("state")) == "ringing"]
            matches = ringing or ([row for row in rows if _matches_selector(row, token)] if token else [])
            if not matches and len(rows) == 1:
                matches = rows
        updated: List[Dict[str, Any]] = []
        for row in matches:
            row["state"] = "armed"
            row["duration_s"] = seconds
            row["deadline_ts"] = now_ts + seconds
            row["last_alarm_attempt_ts"] = 0.0
            row["source"] = _text(source) or _text(row.get("source"))
            _save_row_sync(row)
            updated.append(dict(row))

    delivered = 0
    for row in updated:
        if await _send_timer_command(_text(row.get("selector")), "timer.arm", _command_payload(row)):
            delivered += 1
    return {
        "ok": True,
        "snoozed": len(updated),
        "delivered": delivered,
        "timers": [_public_row(row, now_ts=now_ts) for row in updated],
    }


async def status(*, selector: str = "", timer_id: str = "", room: str = "") -> Dict[str, Any]:
    token = _text(selector)
    timer_token = _text(timer_id)
    room_token = _text(room)
    now_ts = _now()
    async with _timer_lock:
        rows = [row for row in _load_rows_sync() if _text(row.get("state")) in ACTIVE_STATES]
    if timer_token:
        matches = [row for row in rows if _text(row.get("id")) == timer_token]
    else:
        ringing = [row for row in rows if _text(row.get("state")) == "ringing"]
        if ringing:
            matches = ringing
        elif token:
            matches = [row for row in rows if _matches_selector(row, token)]
            if not matches and len(rows) == 1:
                matches = rows
        elif room_token:
            matches = [row for row in rows if _matches_room(row, room_token)]
            if not matches and len(rows) == 1:
                matches = rows
        else:
            matches = rows
    public = sorted(
        [_public_row(row, now_ts=now_ts) for row in matches],
        key=lambda row: (row.get("state") != "ringing", row.get("remaining_s") or 0, row.get("created_ts") or 0),
    )
    return {"ok": True, "running": bool(public), "timers": public, "count": len(public)}


async def sync_selector(selector: str) -> Dict[str, Any]:
    token = _text(selector)
    if not token:
        return {"ok": False, "synced": 0}
    async with _timer_lock:
        rows = [
            row
            for row in _load_rows_sync()
            if _text(row.get("state")) in ACTIVE_STATES and _matches_selector(row, token)
        ]
    if not rows:
        cleared = await _send_timer_command(token, "timer.clear", {"source": "tater", "reason": "sync"})
        return {"ok": True, "synced": 0, "cleared": cleared}

    delivered = 0
    for row in rows:
        msg_type = "timer.alarm" if _text(row.get("state")) == "ringing" else "timer.arm"
        if await _send_timer_command(token, msg_type, _command_payload(row)):
            delivered += 1
    return {"ok": True, "synced": len(rows), "delivered": delivered}


async def handle_device_event(selector: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    token = _text(selector)
    body = payload if isinstance(payload, dict) else {}
    event = _text(body.get("event") or body.get("state")).strip().lower()
    timer_id = _text(body.get("id") or body.get("timer_id"))
    if event in {"stopped", "stop", "cancelled", "canceled", "clear", "cleared"}:
        return await cancel_timer(timer_id=timer_id, selector=token, source="device")
    if event in {"expired", "ringing", "alarm"}:
        now_ts = _now()
        async with _timer_lock:
            rows = [
                row
                for row in _load_rows_sync()
                if _text(row.get("state")) in ACTIVE_STATES
                and ((timer_id and _text(row.get("id")) == timer_id) or (not timer_id and _matches_selector(row, token)))
            ]
            for row in rows:
                row["state"] = "ringing"
                row["ringing_ts"] = now_ts
                row["last_alarm_attempt_ts"] = now_ts
                _save_row_sync(row)
        return {"ok": True, "updated": len(rows)}
    if event in {"sync", "status"}:
        return await status(selector=token, timer_id=timer_id)
    return {"ok": True, "ignored": event or "unknown"}


async def _scheduler_loop() -> None:
    while True:
        try:
            now_ts = _now()
            due: List[Dict[str, Any]] = []
            retry: List[Dict[str, Any]] = []
            async with _timer_lock:
                rows = _load_rows_sync()
                for row in rows:
                    state = _text(row.get("state"))
                    if state not in ACTIVE_STATES:
                        continue
                    if state == "armed" and _as_float(row.get("deadline_ts"), 0.0) <= now_ts:
                        row["state"] = "ringing"
                        row["ringing_ts"] = now_ts
                        row["last_alarm_attempt_ts"] = now_ts
                        _save_row_sync(row)
                        due.append(dict(row))
                    elif state == "ringing":
                        last_attempt = _as_float(row.get("last_alarm_attempt_ts"), 0.0)
                        if now_ts - last_attempt >= ALARM_RETRY_S:
                            row["last_alarm_attempt_ts"] = now_ts
                            _save_row_sync(row)
                            retry.append(dict(row))

            for row in due + retry:
                await _send_timer_command(_text(row.get("selector")), "timer.alarm", _command_payload(row))
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(SCHEDULER_INTERVAL_S)


def start_scheduler() -> None:
    global _scheduler_task
    if _scheduler_task is not None and not _scheduler_task.done():
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    _scheduler_task = loop.create_task(_scheduler_loop())


async def stop_scheduler() -> None:
    global _scheduler_task
    task = _scheduler_task
    _scheduler_task = None
    if task is None or task.done():
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
