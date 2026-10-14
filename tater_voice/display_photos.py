from __future__ import annotations

import contextlib
import hashlib
import io
import os
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import quote

from PIL import Image, ImageOps


MAX_PHOTOS = 12
MAX_UPLOAD_BYTES = 12 * 1024 * 1024
MAX_PIXELS = 50_000_000
MAX_SIZE = (1920, 1200)
_PHOTO_ID_RE = re.compile(r"^[a-f0-9]{16}-[a-f0-9]{32}\.jpg$")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _root() -> Path:
    configured = _text(os.getenv("TATER_DISPLAY_PHOTO_DIR"))
    return Path(configured).expanduser() if configured else Path.home() / ".taterassistant" / "display_photos"


def _selector_prefix(selector: str) -> str:
    token = _text(selector).lower()
    if not token:
        raise ValueError("selector is required")
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def _photo_path(photo_id: str) -> Path:
    token = _text(photo_id).lower()
    if not _PHOTO_ID_RE.fullmatch(token):
        raise ValueError("invalid display photo")
    return _root() / token


def _photo_row(path: Path) -> Dict[str, Any]:
    stat = path.stat()
    photo_id = path.name
    return {
        "id": photo_id,
        "url": f"/api/tater/satellite/v1/display-photo/{quote(photo_id)}",
        "created_at": stat.st_mtime,
        "size": stat.st_size,
    }


def list_photos(selector: str) -> List[Dict[str, Any]]:
    prefix = _selector_prefix(selector)
    root = _root()
    if not root.exists():
        return []
    rows = []
    for path in root.glob(f"{prefix}-*.jpg"):
        if path.is_file() and _PHOTO_ID_RE.fullmatch(path.name):
            rows.append(_photo_row(path))
    rows.sort(key=lambda row: (float(row.get("created_at") or 0.0), str(row.get("id") or "")))
    return rows[:MAX_PHOTOS]


def save_photo(selector: str, contents: bytes, *, filename: str = "", content_type: str = "") -> Dict[str, Any]:
    if not contents:
        raise ValueError("Choose a photo to upload.")
    if len(contents) > MAX_UPLOAD_BYTES:
        raise ValueError("Display photos must be 12 MB or smaller.")
    if len(list_photos(selector)) >= MAX_PHOTOS:
        raise ValueError(f"A satellite can keep up to {MAX_PHOTOS} display photos.")
    mime = _text(content_type).lower().split(";", 1)[0]
    if mime and mime not in {"image/jpeg", "image/png", "image/webp"}:
        raise ValueError("Use a JPEG, PNG, or WebP photo.")

    try:
        with Image.open(io.BytesIO(contents)) as source:
            source.load()
            if source.width <= 0 or source.height <= 0 or source.width * source.height > MAX_PIXELS:
                raise ValueError("Display photo dimensions are too large.")
            image = ImageOps.exif_transpose(source).convert("RGB")
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("Tater could not read that image.") from exc

    resampling = getattr(Image, "Resampling", Image).LANCZOS
    image.thumbnail(MAX_SIZE, resampling)
    root = _root()
    root.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(PermissionError):
        root.chmod(0o700)
    photo_id = f"{_selector_prefix(selector)}-{uuid.uuid4().hex}.jpg"
    destination = root / photo_id
    temporary = root / f".{photo_id}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    try:
        image.save(temporary, format="JPEG", quality=86, optimize=True, progressive=True)
        os.chmod(temporary, 0o644)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    row = _photo_row(destination)
    row["source_name"] = Path(_text(filename)).name
    return row


def remove_photo(selector: str, photo_id: str) -> bool:
    prefix = _selector_prefix(selector) + "-"
    path = _photo_path(photo_id)
    if not path.name.startswith(prefix):
        raise ValueError("That photo does not belong to this satellite.")
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False


def resolve_photo(photo_id: str) -> Path:
    path = _photo_path(photo_id)
    if not path.is_file():
        raise FileNotFoundError(photo_id)
    return path


def firmware_urls(selector: str, service_base_url: str) -> List[str]:
    base = _text(service_base_url).rstrip("/")
    if not base:
        return []
    return [f"{base}{row['url']}" for row in list_photos(selector)]
