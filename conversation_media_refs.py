import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Sequence, Set


CONVERSATION_MEDIA_REF_PREFIX = "tater:media_refs"
DEFAULT_MEDIA_REF_TTL_SEC = int(
    os.getenv("TATER_MEDIA_REF_TTL_SEC", str(60 * 60 * 24 * 14))
)
DEFAULT_MEDIA_REF_MAX_ITEMS = int(os.getenv("TATER_MEDIA_REF_MAX_ITEMS", "64"))

_ALLOWED_MEDIA_TYPES = {"image", "audio", "video", "file"}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _platform_token(platform: Any) -> str:
    raw = _clean(platform).lower() or "unknown"
    return re.sub(r"[^a-z0-9_.:\-]+", "_", raw).strip("_") or "unknown"


def _scope_token(scope: Any) -> str:
    raw = _clean(scope).lower() or "default"
    return re.sub(r"[^a-z0-9_.:\-!@#]+", "_", raw).strip("_") or "default"


def media_refs_key(platform: Any, scope: Any) -> str:
    return f"{CONVERSATION_MEDIA_REF_PREFIX}:{_platform_token(platform)}:{_scope_token(scope)}"


def _media_type_from_mimetype(mimetype: Any) -> str:
    mm = _clean(mimetype).lower()
    if mm.startswith("image/"):
        return "image"
    if mm.startswith("audio/"):
        return "audio"
    if mm.startswith("video/"):
        return "video"
    return "file"


def normalize_media_ref(ref: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(ref, dict):
        return None

    blob_key = _clean(ref.get("blob_key"))
    file_id = _clean(ref.get("file_id"))
    path = _clean(ref.get("path"))
    url = _clean(ref.get("url"))
    if not any((blob_key, file_id, path, url)):
        return None

    mimetype = _clean(ref.get("mimetype")).lower()
    media_type = _clean(ref.get("type")).lower()
    if media_type not in _ALLOWED_MEDIA_TYPES:
        media_type = _media_type_from_mimetype(mimetype)
    if media_type not in _ALLOWED_MEDIA_TYPES:
        media_type = "file"

    if not mimetype:
        if media_type == "image":
            mimetype = "image/png"
        elif media_type == "audio":
            mimetype = "audio/mpeg"
        elif media_type == "video":
            mimetype = "video/mp4"
        else:
            mimetype = "application/octet-stream"

    out: Dict[str, Any] = {
        "type": media_type,
        "blob_key": blob_key or None,
        "file_id": file_id or None,
        "path": path or None,
        "url": url or None,
        "name": _clean(ref.get("name")) or f"{media_type}.bin",
        "mimetype": mimetype,
        "source": _clean(ref.get("source")) or "chat",
        "updated_at": ref.get("updated_at"),
    }

    try:
        out["updated_at"] = float(out["updated_at"]) if out["updated_at"] is not None else time.time()
    except Exception:
        out["updated_at"] = time.time()

    if "size" in ref:
        try:
            size = int(ref.get("size"))
            if size >= 0:
                out["size"] = size
        except Exception:
            pass

    return out


def save_media_ref(
    redis_client: Any,
    *,
    platform: Any,
    scope: Any,
    ref: Dict[str, Any],
    ttl_sec: Optional[int] = None,
    max_items: Optional[int] = None,
) -> bool:
    normalized = normalize_media_ref(ref)
    if not normalized:
        return False

    key = media_refs_key(platform, scope)
    payload = json.dumps(normalized, ensure_ascii=False)
    keep = DEFAULT_MEDIA_REF_MAX_ITEMS if max_items is None else max(1, int(max_items))
    ttl = DEFAULT_MEDIA_REF_TTL_SEC if ttl_sec is None else int(ttl_sec)

    try:
        pipe = redis_client.pipeline()
        pipe.lpush(key, payload)
        pipe.ltrim(key, 0, keep - 1)
        if ttl > 0:
            pipe.expire(key, ttl)
        pipe.execute()
    except Exception:
        return False
    return True


def _decode_json_row(row: Any) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    if isinstance(row, bytes):
        try:
            row = row.decode("utf-8", errors="ignore")
        except Exception:
            return None
    if not isinstance(row, str):
        row = str(row)
    try:
        parsed = json.loads(row)
    except Exception:
        return None
    return normalize_media_ref(parsed)


def load_recent_media_refs(
    redis_client: Any,
    *,
    platform: Any,
    scope: Any,
    limit: int = 8,
    media_types: Optional[Sequence[str]] = None,
    fresh_within_sec: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []

    key = media_refs_key(platform, scope)
    try:
        rows = redis_client.lrange(key, 0, max(limit * 4, limit) - 1)
    except Exception:
        return []

    allowed_types: Optional[Set[str]] = None
    if media_types:
        cleaned = {_clean(t).lower() for t in media_types}
        allowed_types = {t for t in cleaned if t in _ALLOWED_MEDIA_TYPES}
        if not allowed_types:
            allowed_types = None

    max_age = None
    if fresh_within_sec is not None:
        try:
            sec = int(fresh_within_sec)
            if sec > 0:
                max_age = float(sec)
        except Exception:
            max_age = None

    now = time.time()
    out: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        item = _decode_json_row(row)
        if not item:
            continue
        if allowed_types and item.get("type") not in allowed_types:
            continue
        if max_age is not None:
            try:
                ts = float(item.get("updated_at") or 0.0)
            except Exception:
                ts = 0.0
            if ts <= 0 or (now - ts) > max_age:
                continue
        dedupe_key = (
            item.get("type"),
            item.get("blob_key"),
            item.get("file_id"),
            item.get("path"),
            item.get("url"),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(item)
        if len(out) >= limit:
            break
    return out
