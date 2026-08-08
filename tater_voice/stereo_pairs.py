from __future__ import annotations

import json
import re
import uuid
from typing import Any, Dict, List

from helpers import redis_client

REDIS_STEREO_PAIRS_KEY = "tater:voice:stereo_pairs:v1"
STEREO_SELECTOR_PREFIX = "stereo:"
_PAIR_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{5,63}$")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _native_selector(value: Any) -> str:
    token = _text(value)
    return token if token.startswith("native:") else ""


def _integer(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        result = int(float(value))
    except Exception:
        result = int(default)
    return max(int(minimum), min(int(maximum), result))


def pair_selector(pair_id: Any) -> str:
    token = _text(pair_id)
    if token.startswith(STEREO_SELECTOR_PREFIX):
        return token
    return f"{STEREO_SELECTOR_PREFIX}{token}" if token else ""


def pair_id_from_selector(selector: Any) -> str:
    token = _text(selector)
    if token.startswith(STEREO_SELECTOR_PREFIX):
        return _text(token[len(STEREO_SELECTOR_PREFIX) :])
    return token if _PAIR_ID_RE.match(token) else ""


def is_stereo_selector(selector: Any) -> bool:
    return bool(pair_id_from_selector(selector) and _text(selector).startswith(STEREO_SELECTOR_PREFIX))


def _load_document() -> Dict[str, Any]:
    try:
        raw = redis_client.get(REDIS_STEREO_PAIRS_KEY)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        parsed = json.loads(str(raw)) if raw else {}
    except Exception:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}
    rows = parsed.get("pairs") if isinstance(parsed.get("pairs"), list) else []
    return {"version": 1, "pairs": [dict(row) for row in rows if isinstance(row, dict)]}


def _save_document(document: Dict[str, Any]) -> None:
    rows = document.get("pairs") if isinstance(document.get("pairs"), list) else []
    redis_client.set(
        REDIS_STEREO_PAIRS_KEY,
        json.dumps({"version": 1, "pairs": rows}, ensure_ascii=False),
    )


def list_pairs() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for raw in _load_document()["pairs"]:
        pair_id = pair_id_from_selector(raw.get("id") or raw.get("selector"))
        left_selector = _native_selector(raw.get("left_selector"))
        right_selector = _native_selector(raw.get("right_selector"))
        if not pair_id or not left_selector or not right_selector or left_selector == right_selector:
            continue
        rows.append(
            {
                "id": pair_id,
                "selector": pair_selector(pair_id),
                "name": _text(raw.get("name")) or "Stereo Pair",
                "left_selector": left_selector,
                "right_selector": right_selector,
                "left_delay_ms": _integer(raw.get("left_delay_ms"), 0, 0, 250),
                "right_delay_ms": _integer(raw.get("right_delay_ms"), 0, 0, 250),
                "left_volume_percent": _integer(raw.get("left_volume_percent"), 100, 0, 100),
                "right_volume_percent": _integer(raw.get("right_volume_percent"), 100, 0, 100),
            }
        )
    rows.sort(key=lambda row: (_text(row.get("name")).lower(), _text(row.get("id"))))
    return rows


def get_pair(selector_or_id: Any) -> Dict[str, Any]:
    wanted = pair_id_from_selector(selector_or_id)
    if not wanted:
        return {}
    for row in list_pairs():
        if row.get("id") == wanted:
            return dict(row)
    return {}


def save_pair(values: Dict[str, Any], *, pair_id: Any = "") -> Dict[str, Any]:
    data = values if isinstance(values, dict) else {}
    existing_id = pair_id_from_selector(pair_id)
    normalized_id = existing_id or uuid.uuid4().hex[:12]
    if not _PAIR_ID_RE.match(normalized_id):
        raise ValueError("Stereo pair id is invalid.")

    name = _text(data.get("name"))[:80]
    left_selector = _native_selector(data.get("left_selector"))
    right_selector = _native_selector(data.get("right_selector"))
    if not name:
        raise ValueError("Stereo pair name is required.")
    if not left_selector or not right_selector:
        raise ValueError("Choose two Tater Native satellites.")
    if left_selector == right_selector:
        raise ValueError("Left and right satellites must be different.")

    rows = list_pairs()
    for row in rows:
        if row.get("id") == normalized_id:
            continue
        occupied = {row.get("left_selector"), row.get("right_selector")}
        if left_selector in occupied or right_selector in occupied:
            raise ValueError(
                f"A satellite is already assigned to the stereo pair {row.get('name') or row.get('id')}."
            )

    saved = {
        "id": normalized_id,
        "selector": pair_selector(normalized_id),
        "name": name,
        "left_selector": left_selector,
        "right_selector": right_selector,
        "left_delay_ms": _integer(data.get("left_delay_ms"), 0, 0, 250),
        "right_delay_ms": _integer(data.get("right_delay_ms"), 0, 0, 250),
        "left_volume_percent": _integer(data.get("left_volume_percent"), 100, 0, 100),
        "right_volume_percent": _integer(data.get("right_volume_percent"), 100, 0, 100),
    }
    updated = [row for row in rows if row.get("id") != normalized_id]
    updated.append(saved)
    _save_document({"version": 1, "pairs": updated})
    return dict(saved)


def remove_pair(selector_or_id: Any) -> Dict[str, Any]:
    wanted = pair_id_from_selector(selector_or_id)
    if not wanted:
        raise ValueError("Stereo pair id is required.")
    rows = list_pairs()
    kept = [row for row in rows if row.get("id") != wanted]
    removed = len(kept) != len(rows)
    if removed:
        _save_document({"version": 1, "pairs": kept})
    return {"ok": True, "removed": removed, "id": wanted, "selector": pair_selector(wanted)}


def migrate_member_selector(old_selector: Any, new_selector: Any) -> bool:
    """Update saved stereo pairs after a native board identity correction."""
    old_token = _native_selector(old_selector)
    new_token = _native_selector(new_selector)
    if not old_token or not new_token or old_token == new_token:
        return False
    document = _load_document()
    changed = False
    rows: List[Dict[str, Any]] = []
    for raw in document.get("pairs", []):
        row = dict(raw) if isinstance(raw, dict) else {}
        for key in ("left_selector", "right_selector"):
            if _text(row.get(key)) == old_token:
                row[key] = new_token
                changed = True
        rows.append(row)
    if changed:
        _save_document({"version": 1, "pairs": rows})
    return changed
