import json
import os
import re
import time
from typing import Any, Dict, Optional


LATEST_IMAGE_REF_PREFIX = "tater:latest_image_ref"
DEFAULT_LATEST_IMAGE_REF_TTL_SEC = int(
    os.getenv("TATER_LATEST_IMAGE_REF_TTL_SEC", str(60 * 60 * 24 * 14))
)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _scope_token(scope: Any) -> str:
    raw = _clean(scope).lower() or "default"
    return re.sub(r"[^a-z0-9_.:\-!@#]+", "_", raw).strip("_") or "default"


def latest_image_ref_key(platform: Any, scope: Any) -> str:
    p = re.sub(r"[^a-z0-9_.:\-]+", "_", _clean(platform).lower() or "unknown").strip("_") or "unknown"
    s = _scope_token(scope)
    return f"{LATEST_IMAGE_REF_PREFIX}:{p}:{s}"


def normalize_latest_image_ref(ref: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(ref, dict):
        return None

    blob_key = _clean(ref.get("blob_key"))
    file_id = _clean(ref.get("file_id"))
    path = _clean(ref.get("path"))
    url = _clean(ref.get("url"))
    if not any((blob_key, file_id, path, url)):
        return None

    out: Dict[str, Any] = {
        "blob_key": blob_key or None,
        "file_id": file_id or None,
        "path": path or None,
        "url": url or None,
        "name": _clean(ref.get("name")) or "image.png",
        "mimetype": _clean(ref.get("mimetype")) or "image/png",
        "source": _clean(ref.get("source")) or "chat",
        "updated_at": ref.get("updated_at"),
    }

    try:
        out["updated_at"] = float(out["updated_at"]) if out["updated_at"] is not None else time.time()
    except Exception:
        out["updated_at"] = time.time()

    return out


def load_latest_image_ref(redis_client, *, platform: Any, scope: Any) -> Optional[Dict[str, Any]]:
    key = latest_image_ref_key(platform, scope)
    try:
        raw = redis_client.get(key)
    except Exception:
        return None
    if not raw:
        return None
    try:
        parsed = json.loads(str(raw))
    except Exception:
        return None
    return normalize_latest_image_ref(parsed)


def save_latest_image_ref(
    redis_client,
    *,
    platform: Any,
    scope: Any,
    ref: Dict[str, Any],
    ttl_sec: Optional[int] = None,
) -> bool:
    normalized = normalize_latest_image_ref(ref)
    if not normalized:
        return False
    key = latest_image_ref_key(platform, scope)
    payload = json.dumps(normalized, ensure_ascii=False)
    try:
        redis_client.set(key, payload)
        ttl = DEFAULT_LATEST_IMAGE_REF_TTL_SEC if ttl_sec is None else int(ttl_sec)
        if ttl > 0:
            redis_client.expire(key, ttl)
    except Exception:
        return False
    return True
