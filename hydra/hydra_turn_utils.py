import re
from typing import Any, Callable, Dict, Optional


def llm_backend_label(
    llm_client: Any,
    *,
    short_text_fn: Callable[..., str],
) -> str:
    model = short_text_fn(getattr(llm_client, "model", ""), limit=120)
    host = short_text_fn(getattr(llm_client, "host", ""), limit=180)
    if not host:
        client_obj = getattr(llm_client, "client", None)
        host = short_text_fn(getattr(client_obj, "base_url", ""), limit=180)
    host = re.sub(r"^https?://", "", str(host or "").strip(), flags=re.IGNORECASE).rstrip("/")
    host = host.split("/", 1)[0].strip()
    if host and model:
        return f"{host}:{model}"
    return model or host


def origin_preview_for_ledger(
    origin: Optional[Dict[str, Any]],
    *,
    short_text_fn: Callable[..., str],
) -> Dict[str, str]:
    src = origin if isinstance(origin, dict) else {}
    keys = (
        "channel_id",
        "chat_id",
        "room_id",
        "device_id",
        "area_id",
        "user_id",
        "session_id",
        "chat_type",
        "target",
        "request_id",
    )
    out: Dict[str, str] = {}
    for key in keys:
        value = short_text_fn(src.get(key), limit=72)
        if value:
            out[key] = value
        if len(out) >= 6:
            break
    return out


def normalize_outcome(
    status: str,
    checker_reason: str,
    *,
    short_text_fn: Callable[..., str],
) -> tuple[str, str]:
    status_key = str(status or "").strip().lower()
    reason_code = short_text_fn(checker_reason, limit=96) or "unknown"
    if status_key == "done":
        return "done", reason_code if reason_code else "complete"
    if status_key == "blocked":
        return "blocked", reason_code
    return "failed", reason_code
