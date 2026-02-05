import base64
import json
import os
import uuid
from typing import Any, Dict, List, Optional

import redis

MEDIA_TYPES = {"image", "audio", "video", "file"}
MEDIA_REF_PREFIX = "notifyq:media"
BLOB_PREFIX = "tater:blob:notify"
DEFAULT_TTL_SECONDS = 60 * 60 * 24 * 7


def _redis_host() -> str:
    return os.getenv("REDIS_HOST", "127.0.0.1")


def _redis_port() -> int:
    return int(os.getenv("REDIS_PORT", 6379))


def _blob_client():
    return redis.Redis(
        host=_redis_host(),
        port=_redis_port(),
        db=0,
        decode_responses=False,
    )


def _blob_key() -> str:
    return f"{BLOB_PREFIX}:{uuid.uuid4().hex}"


def _load_blob(blob_key: str) -> Optional[bytes]:
    if not blob_key:
        return None
    return _blob_client().get(blob_key.encode("utf-8"))


def _store_blob(binary: bytes, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> str:
    key = _blob_key()
    client = _blob_client()
    client.set(key.encode("utf-8"), binary)
    if ttl_seconds and ttl_seconds > 0:
        client.expire(key.encode("utf-8"), int(ttl_seconds))
    return key


def _coerce_bytes(value: Any) -> Optional[bytes]:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return None


def _decode_b64(value: str) -> Optional[bytes]:
    try:
        return base64.b64decode(value)
    except Exception:
        return None


def _read_path(path: str) -> Optional[bytes]:
    try:
        with open(path, "rb") as f:
            return f.read()
    except Exception:
        return None


def _coerce_size(value: Any) -> Optional[int]:
    try:
        n = int(value)
        if n >= 0:
            return n
    except Exception:
        pass
    return None


def _normalize_media_item(item: Any, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    kind = str(item.get("type") or "").strip().lower()
    if kind not in MEDIA_TYPES:
        return None

    name = str(item.get("name") or f"{kind}.bin").strip()
    mimetype = str(item.get("mimetype") or "application/octet-stream").strip()
    size = _coerce_size(item.get("size"))

    blob_key = str(item.get("blob_key") or "").strip()
    if blob_key and _load_blob(blob_key) is None:
        blob_key = ""

    if not blob_key:
        binary = _coerce_bytes(item.get("bytes"))
        if binary is None and isinstance(item.get("data"), str):
            binary = _decode_b64(item.get("data"))
        if binary is None and isinstance(item.get("path"), str):
            binary = _read_path(item.get("path"))

        if binary is not None:
            blob_key = _store_blob(binary, ttl_seconds=ttl_seconds)
            if size is None:
                size = len(binary)

    if not blob_key:
        return None

    out: Dict[str, Any] = {
        "type": kind,
        "name": name,
        "mimetype": mimetype,
        "blob_key": blob_key,
    }
    if size is not None:
        out["size"] = size
    return out


def media_refs_key(item_id: str) -> str:
    return f"{MEDIA_REF_PREFIX}:{item_id}"


def store_queue_attachments(
    redis_client,
    item_id: str,
    attachments: Any,
    ttl_seconds: int = DEFAULT_TTL_SECONDS,
) -> int:
    item_id = str(item_id or "").strip()
    if not item_id:
        return 0

    normalized: List[Dict[str, Any]] = []
    if isinstance(attachments, list):
        for raw in attachments:
            media_item = _normalize_media_item(raw, ttl_seconds=ttl_seconds)
            if media_item:
                normalized.append(media_item)

    key = media_refs_key(item_id)
    if not normalized:
        try:
            redis_client.delete(key)
        except Exception:
            pass
        return 0

    redis_client.set(key, json.dumps(normalized))
    if ttl_seconds and ttl_seconds > 0:
        redis_client.expire(key, int(ttl_seconds))
    return len(normalized)


def load_queue_attachments(redis_client, item_id: Any) -> List[Dict[str, Any]]:
    item_id = str(item_id or "").strip()
    if not item_id:
        return []

    raw = redis_client.get(media_refs_key(item_id))
    if not raw:
        return []

    try:
        parsed = json.loads(raw)
    except Exception:
        return []

    if not isinstance(parsed, list):
        return []

    out: List[Dict[str, Any]] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            continue
        kind = str(entry.get("type") or "").strip().lower()
        blob_key = str(entry.get("blob_key") or "").strip()
        if kind not in MEDIA_TYPES or not blob_key:
            continue
        out.append(entry)
    return out
