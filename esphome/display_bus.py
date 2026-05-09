from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

from helpers import redis_client


DISPLAY_EVENTS_KEY = "tater:display:events:v1"
DISPLAY_EVENT_SEQ_KEY = "tater:display:events:seq:v1"
_MAX_EVENTS = 250
_DEFAULT_TTL_SECONDS = 90
_MAX_TTL_SECONDS = 60 * 60
_ALLOWED_KINDS = {
    "notification",
    "camera",
    "doorbell",
    "image",
    "tool_call",
    "voice",
    "status",
    "alert",
}
_ALLOWED_PRIORITIES = {"low", "normal", "high", "critical"}


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="replace").strip()
        except Exception:
            return str(value).strip()
    return str(value).strip()


def _lower(value: Any) -> str:
    return _text(value).lower()


def _as_int(value: Any, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        out = int(float(_text(value)))
    except Exception:
        out = int(default)
    if minimum is not None:
        out = max(int(minimum), out)
    if maximum is not None:
        out = min(int(maximum), out)
    return out


def _clip(value: Any, limit: int) -> str:
    text = _text(value)
    if len(text) <= int(limit):
        return text
    return text[: int(limit)].rstrip()


def _clean_kind(value: Any) -> str:
    token = _lower(value).replace("-", "_").replace(" ", "_")
    if token == "notify":
        token = "notification"
    if token in {"tool", "toolcall", "tool_progress", "tool_call_start", "tool_call_progress"}:
        token = "tool_call"
    return token if token in _ALLOWED_KINDS else "notification"


def _clean_priority(value: Any) -> str:
    token = _lower(value)
    return token if token in _ALLOWED_PRIORITIES else "normal"


def _clean_target(value: Any) -> str:
    token = _text(value)
    if not token:
        return "all"
    clean = re.sub(r"[^A-Za-z0-9:._,@-]+", "_", token).strip("._,")
    return clean[:120] or "all"


def _url_ok(value: Any) -> bool:
    token = _text(value)
    if not token:
        return False
    if token.startswith("/"):
        return True
    try:
        parsed = urlparse(token)
    except Exception:
        return False
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _first_image_url(payload: Dict[str, Any]) -> str:
    for key in ("image_url", "thumbnail_url", "snapshot_url", "clip_thumbnail_url"):
        url = _text(payload.get(key))
        if _url_ok(url):
            return url

    media = payload.get("media") if isinstance(payload.get("media"), dict) else {}
    for key in ("image_url", "thumbnail_url", "snapshot_url", "url"):
        url = _text(media.get(key))
        if _url_ok(url):
            return url

    attachments = payload.get("attachments")
    if isinstance(attachments, list):
        for item in attachments:
            if not isinstance(item, dict):
                continue
            kind = _lower(item.get("type"))
            mimetype = _lower(item.get("mimetype"))
            if kind != "image" and not mimetype.startswith("image/"):
                continue
            url = _text(item.get("url") or item.get("image_url"))
            if _url_ok(url):
                return url
    return ""


def _media_format(url: str, payload: Dict[str, Any]) -> str:
    explicit = _lower(payload.get("image_format") or payload.get("format"))
    if explicit in {"png", "jpeg", "jpg", "bmp"}:
        return "jpeg" if explicit == "jpg" else explicit
    path = urlparse(_text(url)).path.lower()
    if path.endswith(".jpg") or path.endswith(".jpeg"):
        return "jpeg"
    if path.endswith(".bmp"):
        return "bmp"
    return "png"


def _clean_actions(value: Any) -> List[Dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: List[Dict[str, str]] = []
    for item in value[:4]:
        if not isinstance(item, dict):
            continue
        action_id = _clip(item.get("id") or item.get("key") or item.get("action"), 48)
        label = _clip(item.get("label") or item.get("title") or action_id, 32)
        if not action_id or not label:
            continue
        out.append({"id": action_id, "label": label})
    return out


def _optional_int_field(body: Dict[str, Any], key: str, *, minimum: int = 0, maximum: int = 999) -> Optional[int]:
    if key not in body:
        return None
    if body.get(key) is None or _text(body.get(key)) == "":
        return None
    return _as_int(body.get(key), 0, minimum=minimum, maximum=maximum)


def _target_values(event: Dict[str, Any]) -> set[str]:
    out = {_lower(event.get("target") or "all")}
    targets = event.get("targets")
    if isinstance(targets, list):
        for item in targets:
            token = _lower(item)
            if token:
                out.add(token)
    return out


def _matches_target(event: Dict[str, Any], target: str = "") -> bool:
    token = _lower(target)
    if not token:
        return True
    targets = _target_values(event)
    return "all" in targets or token in targets


def _event_from_payload(payload: Dict[str, Any], *, seq: int) -> Dict[str, Any]:
    body = payload if isinstance(payload, dict) else {}
    now = time.time()
    ttl = _as_int(
        body.get("ttl_seconds", body.get("ttl_sec", _DEFAULT_TTL_SECONDS)),
        _DEFAULT_TTL_SECONDS,
        minimum=1,
        maximum=_MAX_TTL_SECONDS,
    )
    kind = _clean_kind(body.get("kind") or body.get("type"))
    title = _clip(body.get("title") or body.get("name"), 96)
    message = _clip(body.get("body") or body.get("message") or body.get("content") or body.get("text"), 480)
    description = _clip(body.get("description") or body.get("summary") or body.get("caption"), 720)
    image_url = _first_image_url(body)
    media_url = _text(body.get("media_url") or body.get("clip_url") or body.get("video_url"))
    if media_url and not _url_ok(media_url):
        media_url = ""

    target = _clean_target(body.get("target") or body.get("device") or body.get("selector"))
    targets = []
    if isinstance(body.get("targets"), list):
        targets = [_clean_target(item) for item in body.get("targets") if _clean_target(item)]
    if target not in targets:
        targets.insert(0, target)

    event = {
        "id": _text(body.get("id")) or uuid.uuid4().hex,
        "seq": int(seq),
        "kind": kind,
        "target": target,
        "targets": targets[:12],
        "priority": _clean_priority(body.get("priority")),
        "title": title,
        "message": message,
        "description": description,
        "created_at": now,
        "ttl_seconds": ttl,
        "expires_at": now + ttl,
        "source": _clip(body.get("source") or body.get("origin") or "api", 80),
        "actions": _clean_actions(body.get("actions")),
        "meta": body.get("meta") if isinstance(body.get("meta"), dict) else {},
    }
    for field, limit in (
        ("phase", 48),
        ("status", 48),
        ("tool", 80),
        ("label", 80),
        ("animation", 48),
    ):
        value = _clip(body.get(field), limit)
        if value:
            event[field] = value
    for field in ("step_index", "step_total"):
        value = _optional_int_field(body, field, minimum=0, maximum=999)
        if value is not None:
            event[field] = value
    if image_url:
        event["image_url"] = image_url
        event["image_format"] = _media_format(image_url, body)
    if media_url:
        event["media_url"] = media_url
    return event


def _load_events(client: Any = None) -> List[Dict[str, Any]]:
    store = client or redis_client
    try:
        raw_rows = store.lrange(DISPLAY_EVENTS_KEY, 0, -1)
    except Exception:
        return []
    events: List[Dict[str, Any]] = []
    now = time.time()
    for raw in raw_rows:
        try:
            row = json.loads(_text(raw))
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        if float(row.get("expires_at") or 0.0) <= now:
            continue
        events.append(row)
    events.sort(key=lambda item: int(item.get("seq") or 0))
    return events


def publish_display_event(payload: Dict[str, Any], *, client: Any = None) -> Dict[str, Any]:
    store = client or redis_client
    seq = _as_int(store.incr(DISPLAY_EVENT_SEQ_KEY), 0, minimum=1)
    event = _event_from_payload(payload if isinstance(payload, dict) else {}, seq=seq)
    store.rpush(DISPLAY_EVENTS_KEY, json.dumps(event, sort_keys=True, separators=(",", ":"), default=str))
    store.ltrim(DISPLAY_EVENTS_KEY, -_MAX_EVENTS, -1)
    return {"ok": True, "event": event}


def list_display_events(
    *,
    after_seq: int = 0,
    target: str = "",
    limit: int = 20,
    client: Any = None,
) -> Dict[str, Any]:
    max_rows = _as_int(limit, 20, minimum=1, maximum=50)
    threshold = _as_int(after_seq, 0, minimum=0)
    events = [
        event
        for event in _load_events(client)
        if int(event.get("seq") or 0) > threshold and _matches_target(event, target)
    ]
    events = events[:max_rows]
    last_seq = threshold
    for event in events:
        last_seq = max(last_seq, int(event.get("seq") or 0))
    return {
        "ok": True,
        "events": events,
        "count": len(events),
        "after_seq": threshold,
        "last_seq": last_seq,
        "target": _text(target),
    }


def display_feed_events(target: str = "", *, limit: int = 3, client: Any = None) -> List[Dict[str, Any]]:
    rows = [
        event
        for event in reversed(_load_events(client))
        if _matches_target(event, target)
    ]
    return rows[: _as_int(limit, 3, minimum=0, maximum=10)]
