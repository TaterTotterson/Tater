import os
import asyncio
import threading
import inspect
from openai import AsyncOpenAI
import requests
import nest_asyncio
import redis
from dotenv import load_dotenv
import re
import json
import base64
import uuid
import time
import websocket
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse, urlunparse

load_dotenv()
nest_asyncio.apply()

# Redis setup
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', '127.0.0.1'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True
)

def get_tater_name():
    """Return the assistant's first and last name from Redis."""
    first = redis_client.get("tater:first_name")
    if not first:
        first = "Tater"
        redis_client.set("tater:first_name", first)

    last = redis_client.get("tater:last_name")
    if not last:
        last = "Totterson"
        redis_client.set("tater:last_name", last)

    return first, last

def get_tater_personality():
    """
    Return the assistant's personality / style description from Redis.
    Empty string means 'no forced personality'.
    """
    personality = redis_client.get("tater:personality")
    if not personality:
        personality = ""
        redis_client.set("tater:personality", personality)

    return personality

# ---------------------------------------------------------
# Main event loop reference + run_async helper
# ---------------------------------------------------------
_main_loop = None

def set_main_loop(loop):
    global _main_loop
    _main_loop = loop

def run_async(coro):
    loop = _main_loop or asyncio.get_event_loop_policy().get_event_loop()
    return loop.run_until_complete(coro)

# ---------------------------------------------------------
# LLM client wrapper (OpenAI-compatible /v1/chat/completions)
# ---------------------------------------------------------
HYDRA_LLM_HOST_KEY = "tater:hydra:llm_host"
HYDRA_LLM_PORT_KEY = "tater:hydra:llm_port"
HYDRA_LLM_MODEL_KEY = "tater:hydra:llm_model"
HYDRA_LLM_BASE_SERVERS_KEY = "tater:hydra:llm_base_servers"
HYDRA_LLM_SETUP_ERROR = (
    "Hydra LLM is not configured. Open Settings > Hydra and set Hydra LLM Host/IP, Port, and Model."
)
_HYDRA_BASE_RR_LOCK = threading.Lock()
_HYDRA_BASE_RR_INDEX: Dict[str, int] = {}
_ACTIVE_LLM_CALLS_LOCK = threading.RLock()
_ACTIVE_LLM_CALLS: Dict[str, Dict[str, Any]] = {}
_LLM_CALL_HISTORY: List[Dict[str, Any]] = []
_LLM_CALL_HISTORY_MAX = 5000
_LLM_CALL_COUNTERS: Dict[str, int] = {"started": 0, "completed": 0, "failed": 0}

_LLM_ORIGIN_KIND_LABELS = {
    "hydra": "Hydra",
    "webui": "WebUI",
    "verba": "Verba",
    "portal": "Portal",
    "core": "Core",
    "other": "Other",
}


def _normalize_base_url(host: str) -> str:
    """
    Ensure base_url ends with /v1 and includes scheme.
    Accepts: 127.0.0.1:11434  -> http://127.0.0.1:11434/v1
             http://host:port -> http://host:port/v1 (if missing)
             https://api.foo/v1 -> unchanged
    """
    if not host.startswith("http://") and not host.startswith("https://"):
        host = f"http://{host}"
    parsed = urlparse(host.rstrip("/"))
    path = parsed.path
    if not path.endswith("/v1"):
        path = (path + "/v1").replace("//", "/")
    return urlunparse(parsed._replace(path=path))

def _sanitize_chat_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Defensive sanitizer for OpenAI-compatible chat endpoints.

    - Drops empty user turns (content == "" after coercion)
      These can cause some backends (LM Studio / some Qwen templates) to return empty completions.
    - Coerces non-string message content (lists/dicts) into plain text,
      so we don't send multimodal structures to backends that don't support them.
    - Drops messages with missing role/content.
    """
    if not isinstance(messages, list):
        return []

    cleaned: List[Dict[str, Any]] = []
    for m in messages:
        if not isinstance(m, dict):
            continue

        role = (m.get("role") or "").strip()
        if role not in ("system", "user", "assistant", "tool"):
            # keep it strict; unknown roles can confuse some servers
            continue

        raw_content = m.get("content", None)

        # Convert any non-string (list/dict/etc) to text for maximum compatibility
        content_text = _coerce_content_to_text(raw_content).strip()

        # Drop empty user turns entirely (this is the big one)
        if role == "user" and content_text == "":
            continue

        # Also drop empty assistant/tool turns (optional but generally helpful)
        if role in ("assistant", "tool") and content_text == "":
            continue

        # System messages should not be empty either
        if role == "system" and content_text == "":
            continue

        cleaned.append({"role": role, "content": content_text})

    return cleaned

def _coerce_content_to_text(content) -> str:
    if isinstance(content, str):
        return content

    if isinstance(content, dict):
        # Common places providers hide text
        for k in ("text", "content", "value"):
            v = content.get(k)
            if isinstance(v, str):
                return v.strip()
        # Sometimes it's a list inside a dict
        for k in ("parts", "content", "messages"):
            v = content.get(k)
            if isinstance(v, list):
                return _coerce_content_to_text(v)
        return ""

    if isinstance(content, list):
        parts = []
        for p in content:
            if isinstance(p, str):
                parts.append(p)
            elif isinstance(p, dict):
                # Prefer explicit text/content/value keys
                for k in ("text", "content", "value"):
                    v = p.get(k)
                    if isinstance(v, str):
                        parts.append(v)
                        break
        return "\n".join(s for s in parts if s).strip()

    return "" if content is None else str(content)


def _safe_redis_text_get(key: str, *, redis_conn: Any = None) -> str:
    client = redis_conn or redis_client
    try:
        return str(client.get(key) or "").strip()
    except Exception:
        return ""


def _build_hydra_llm_endpoint(host: Any, port: Any) -> str:
    raw_host = str(host or "").strip()
    raw_port = str(port or "").strip()
    if not raw_host:
        return ""

    candidate = raw_host if raw_host.startswith(("http://", "https://")) else f"http://{raw_host}"
    parsed = urlparse(candidate)
    hostname = str(parsed.hostname or "").strip()
    if not hostname:
        return ""

    resolved_port = raw_port or (str(parsed.port) if parsed.port is not None else "")
    if resolved_port:
        try:
            port_int = int(str(resolved_port).strip())
        except Exception:
            return ""
        if port_int < 1 or port_int > 65535:
            return ""
        netloc = f"{hostname}:{port_int}"
    else:
        netloc = hostname

    return urlunparse((parsed.scheme or "http", netloc, "", "", "", "")).rstrip("/")


def _resolve_hydra_llm_defaults(*, redis_conn: Any = None) -> tuple[str, str]:
    host = _safe_redis_text_get(HYDRA_LLM_HOST_KEY, redis_conn=redis_conn)
    port = _safe_redis_text_get(HYDRA_LLM_PORT_KEY, redis_conn=redis_conn)
    model = _safe_redis_text_get(HYDRA_LLM_MODEL_KEY, redis_conn=redis_conn)
    endpoint = _build_hydra_llm_endpoint(host, port)
    return endpoint, model


def _normalize_hydra_base_server_row(row: Any) -> Optional[Dict[str, str]]:
    if not isinstance(row, dict):
        return None

    raw_host = str(row.get("host") or "").strip()
    raw_port = str(row.get("port") or "").strip()
    raw_model = str(row.get("model") or "").strip()
    if not raw_host and not raw_port and not raw_model:
        return None

    endpoint = _build_hydra_llm_endpoint(raw_host, raw_port)
    if not endpoint or not raw_model:
        return None

    parsed = urlparse(endpoint)
    hostname = str(parsed.hostname or "").strip()
    if not hostname:
        return None

    host_with_scheme = raw_host.startswith(("http://", "https://"))
    canonical_host = f"{parsed.scheme}://{hostname}" if host_with_scheme else hostname
    canonical_port = str(parsed.port) if parsed.port is not None else ""

    return {
        "host": canonical_host,
        "port": canonical_port,
        "model": raw_model,
        "endpoint": endpoint,
    }


def resolve_hydra_base_servers(*, redis_conn: Any = None, include_legacy: bool = True) -> List[Dict[str, str]]:
    client = redis_conn or redis_client
    rows: List[Dict[str, str]] = []
    seen: set[Tuple[str, str]] = set()

    raw_payload = _safe_redis_text_get(HYDRA_LLM_BASE_SERVERS_KEY, redis_conn=client)
    parsed_payload: Any = []
    if raw_payload:
        try:
            parsed_payload = json.loads(raw_payload)
        except Exception:
            parsed_payload = []

    if isinstance(parsed_payload, list):
        for item in parsed_payload:
            normalized = _normalize_hydra_base_server_row(item)
            if not normalized:
                continue
            signature = (normalized["endpoint"], normalized["model"])
            if signature in seen:
                continue
            seen.add(signature)
            rows.append(normalized)

    if rows or not include_legacy:
        return rows

    legacy_host = _safe_redis_text_get(HYDRA_LLM_HOST_KEY, redis_conn=client)
    legacy_port = _safe_redis_text_get(HYDRA_LLM_PORT_KEY, redis_conn=client)
    legacy_model = _safe_redis_text_get(HYDRA_LLM_MODEL_KEY, redis_conn=client)
    legacy_row = _normalize_hydra_base_server_row(
        {"host": legacy_host, "port": legacy_port, "model": legacy_model}
    )
    if legacy_row:
        signature = (legacy_row["endpoint"], legacy_row["model"])
        if signature not in seen:
            rows.append(legacy_row)
    return rows


def _next_hydra_base_rr_index(pool_key: str, size: int) -> int:
    if size <= 1:
        return 0
    key = str(pool_key or "").strip() or "__default__"
    with _HYDRA_BASE_RR_LOCK:
        next_index = int(_HYDRA_BASE_RR_INDEX.get(key, 0))
        selected = next_index % int(size)
        _HYDRA_BASE_RR_INDEX[key] = (selected + 1) % int(size)
    return selected


def _llm_origin_kind_label(kind: Any) -> str:
    token = str(kind or "").strip().lower() or "other"
    return _LLM_ORIGIN_KIND_LABELS.get(token, token.capitalize() or "Other")


def _classify_llm_call_origin(filename: str, module_name: str) -> Dict[str, str]:
    raw_path = str(filename or "").strip()
    normalized = raw_path.replace("\\", "/")
    lowered = normalized.lower()
    stem = os.path.splitext(os.path.basename(normalized))[0].strip().lower()
    module_token = str(module_name or "").strip()
    source = stem or module_token or "unknown"
    kind = "other"

    if "/hydra/" in lowered:
        kind = "hydra"
        source = "hydra"
    elif "/verba/" in lowered or "/verbas/" in lowered:
        kind = "verba"
        source = source.removesuffix("_verba")
    elif "/portals/" in lowered:
        kind = "portal"
        source = source.removesuffix("_portal")
    elif "/cores/" in lowered:
        kind = "core"
        source = source.removesuffix("_core")
    elif stem == "tateros_app":
        kind = "webui"
        source = "webui"
    elif stem.endswith("_core"):
        kind = "core"
        source = stem.removesuffix("_core")
    elif stem.endswith("_portal"):
        kind = "portal"
        source = stem.removesuffix("_portal")
    elif stem.endswith("_verba"):
        kind = "verba"
        source = stem.removesuffix("_verba")

    source = str(source or "unknown").strip() or "unknown"
    rel_path = normalized
    try:
        cwd = os.getcwd()
        rel_candidate = os.path.relpath(raw_path, cwd)
        if rel_candidate and not rel_candidate.startswith(".."):
            rel_path = rel_candidate.replace("\\", "/")
    except Exception:
        pass

    return {
        "kind": kind,
        "source": source,
        "module": module_token or source,
        "path": rel_path,
    }


def _infer_llm_call_origin(max_depth: int = 48) -> Dict[str, str]:
    frame = inspect.currentframe()
    try:
        if frame is not None:
            frame = frame.f_back
        depth = 0
        while frame is not None and depth < int(max_depth):
            code = frame.f_code
            filename = str(getattr(code, "co_filename", "") or "")
            function_name = str(getattr(code, "co_name", "") or "").strip()
            module_name = str(frame.f_globals.get("__name__") or "").strip()
            depth += 1
            frame = frame.f_back

            if not filename:
                continue
            normalized = filename.replace("\\", "/").lower()
            if normalized.endswith("/helpers.py"):
                continue

            info = _classify_llm_call_origin(filename, module_name)
            info["function"] = function_name
            return info
    finally:
        # Explicitly clear frame references.
        del frame

    return {
        "kind": "other",
        "source": "unknown",
        "module": "",
        "path": "",
        "function": "",
    }


def _register_active_llm_call(
    *,
    host: str,
    model: str,
    stream: bool,
    message_count: int,
) -> str:
    origin = _infer_llm_call_origin()
    call_id = str(uuid.uuid4())
    started_at = time.time()
    row = {
        "id": call_id,
        "host": str(host or "").strip(),
        "model": str(model or "").strip(),
        "stream": bool(stream),
        "message_count": max(0, int(message_count)),
        "started_at": started_at,
        "kind": str(origin.get("kind") or "other"),
        "source": str(origin.get("source") or "unknown"),
        "module": str(origin.get("module") or ""),
        "path": str(origin.get("path") or ""),
        "function": str(origin.get("function") or ""),
    }
    with _ACTIVE_LLM_CALLS_LOCK:
        _ACTIVE_LLM_CALLS[call_id] = row
        _LLM_CALL_COUNTERS["started"] = int(_LLM_CALL_COUNTERS.get("started") or 0) + 1
    return call_id


def _finish_active_llm_call(
    call_id: str,
    *,
    error: Optional[Exception] = None,
    response_model: str = "",
) -> None:
    call_token = str(call_id or "").strip()
    if not call_token:
        return

    finished_at = time.time()
    with _ACTIVE_LLM_CALLS_LOCK:
        row = _ACTIVE_LLM_CALLS.pop(call_token, None)
        if not isinstance(row, dict):
            return

        started_at = float(row.get("started_at") or 0.0)
        duration_ms = max(0.0, (finished_at - started_at) * 1000.0) if started_at > 0.0 else 0.0
        ok = error is None

        if ok:
            _LLM_CALL_COUNTERS["completed"] = int(_LLM_CALL_COUNTERS.get("completed") or 0) + 1
        else:
            _LLM_CALL_COUNTERS["failed"] = int(_LLM_CALL_COUNTERS.get("failed") or 0) + 1

        history_row = {
            "id": call_token,
            "finished_at": finished_at,
            "started_at": started_at,
            "duration_ms": duration_ms,
            "ok": bool(ok),
            "error": str(error) if error is not None else "",
            "kind": str(row.get("kind") or "other"),
            "source": str(row.get("source") or "unknown"),
            "module": str(row.get("module") or ""),
            "path": str(row.get("path") or ""),
            "function": str(row.get("function") or ""),
            "host": str(row.get("host") or ""),
            "model": str(response_model or row.get("model") or "").strip(),
            "stream": bool(row.get("stream")),
            "message_count": max(0, int(row.get("message_count") or 0)),
        }
        _LLM_CALL_HISTORY.append(history_row)
        overflow = len(_LLM_CALL_HISTORY) - int(_LLM_CALL_HISTORY_MAX)
        if overflow > 0:
            del _LLM_CALL_HISTORY[:overflow]


def get_active_llm_calls_snapshot(*, limit: int = 100) -> List[Dict[str, Any]]:
    max_items = max(1, min(int(limit or 0), 500))
    now = time.time()

    with _ACTIVE_LLM_CALLS_LOCK:
        rows = [dict(item) for item in _ACTIVE_LLM_CALLS.values() if isinstance(item, dict)]

    rows.sort(key=lambda row: float(row.get("started_at") or 0.0))
    out: List[Dict[str, Any]] = []
    for row in rows[-max_items:]:
        started_at = float(row.get("started_at") or 0.0)
        age_seconds = max(0, int(now - started_at)) if started_at > 0 else 0
        kind = str(row.get("kind") or "other")
        source = str(row.get("source") or "unknown")
        out.append(
            {
                "id": str(row.get("id") or ""),
                "kind": kind,
                "kind_label": _llm_origin_kind_label(kind),
                "source": source,
                "source_label": f"{_llm_origin_kind_label(kind)} - {source}",
                "module": str(row.get("module") or ""),
                "path": str(row.get("path") or ""),
                "function": str(row.get("function") or ""),
                "host": str(row.get("host") or ""),
                "model": str(row.get("model") or ""),
                "stream": bool(row.get("stream")),
                "message_count": max(0, int(row.get("message_count") or 0)),
                "started_at": started_at,
                "age_seconds": age_seconds,
            }
        )
    return out


def _llm_call_history_windows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    now = time.time()
    windows = [
        ("24h", "Last 24 hours", 24 * 60 * 60),
        ("7d", "Last 7 days", 7 * 24 * 60 * 60),
        ("30d", "Last 30 days", 30 * 24 * 60 * 60),
    ]
    buckets: Dict[str, Dict[str, Any]] = {
        key: {
            "key": key,
            "label": label,
            "calls": 0,
            "completed": 0,
            "failed": 0,
            "duration_ms_total": 0.0,
            "source_counts": {},
        }
        for key, label, _ in windows
    }

    for row in rows:
        finished_at = float(row.get("finished_at") or 0.0)
        if finished_at <= 0.0:
            continue
        age = max(0.0, now - finished_at)
        ok = bool(row.get("ok"))
        duration_ms = max(0.0, float(row.get("duration_ms") or 0.0))
        kind = str(row.get("kind") or "other")
        source = str(row.get("source") or "unknown")
        source_key = f"{kind}:{source}"

        for key, _label, seconds in windows:
            if age > float(seconds):
                continue
            bucket = buckets[key]
            bucket["calls"] = int(bucket.get("calls") or 0) + 1
            if ok:
                bucket["completed"] = int(bucket.get("completed") or 0) + 1
            else:
                bucket["failed"] = int(bucket.get("failed") or 0) + 1
            bucket["duration_ms_total"] = float(bucket.get("duration_ms_total") or 0.0) + duration_ms
            source_counts = bucket.get("source_counts")
            if isinstance(source_counts, dict):
                source_counts[source_key] = int(source_counts.get(source_key) or 0) + 1

    window_rows: List[Dict[str, Any]] = []
    for key, label, _seconds in windows:
        bucket = buckets.get(key) or {"key": key, "label": label}
        calls = int(bucket.get("calls") or 0)
        duration_ms_total = float(bucket.get("duration_ms_total") or 0.0)
        avg_ms = (duration_ms_total / calls) if calls > 0 else 0.0

        source_rows: List[Dict[str, Any]] = []
        raw_source_counts = bucket.get("source_counts") if isinstance(bucket.get("source_counts"), dict) else {}
        for source_key, count in raw_source_counts.items():
            kind, _sep, source = str(source_key or "").partition(":")
            source_rows.append(
                {
                    "kind": kind or "other",
                    "source": source or "unknown",
                    "label": f"{_llm_origin_kind_label(kind or 'other')} - {source or 'unknown'}",
                    "calls": int(count),
                }
            )
        source_rows.sort(key=lambda row: (-int(row.get("calls") or 0), str(row.get("label") or "")))

        window_rows.append(
            {
                "key": key,
                "label": label,
                "calls": calls,
                "completed": int(bucket.get("completed") or 0),
                "failed": int(bucket.get("failed") or 0),
                "avg_ms": round(avg_ms, 2),
                "top_sources": source_rows[:4],
            }
        )

    return {
        "windows": window_rows,
        "sample_size": int(len(rows)),
    }


def get_llm_call_runtime_summary(*, include_history: bool = False) -> Dict[str, Any]:
    active_calls = get_active_llm_calls_snapshot(limit=120)
    by_kind_counts: Dict[str, int] = {}
    by_source_counts: Dict[str, int] = {}

    for row in active_calls:
        kind = str(row.get("kind") or "other")
        source = str(row.get("source") or "unknown")
        by_kind_counts[kind] = int(by_kind_counts.get(kind) or 0) + 1
        source_key = f"{kind}:{source}"
        by_source_counts[source_key] = int(by_source_counts.get(source_key) or 0) + 1

    by_kind = [
        {
            "kind": kind,
            "label": _llm_origin_kind_label(kind),
            "calls": int(count),
        }
        for kind, count in by_kind_counts.items()
    ]
    by_kind.sort(key=lambda row: (-int(row.get("calls") or 0), str(row.get("label") or "")))

    by_source = []
    for source_key, count in by_source_counts.items():
        kind, _sep, source = str(source_key or "").partition(":")
        by_source.append(
            {
                "kind": kind or "other",
                "source": source or "unknown",
                "label": f"{_llm_origin_kind_label(kind or 'other')} - {source or 'unknown'}",
                "calls": int(count),
            }
        )
    by_source.sort(key=lambda row: (-int(row.get("calls") or 0), str(row.get("label") or "")))

    with _ACTIVE_LLM_CALLS_LOCK:
        totals = {
            "started": int(_LLM_CALL_COUNTERS.get("started") or 0),
            "completed": int(_LLM_CALL_COUNTERS.get("completed") or 0),
            "failed": int(_LLM_CALL_COUNTERS.get("failed") or 0),
        }
        history_rows = [dict(item) for item in _LLM_CALL_HISTORY] if include_history else []

    out = {
        "active_total": int(len(active_calls)),
        "totals": totals,
        "active_by_kind": by_kind,
        "active_by_source": by_source,
        "active_calls": active_calls,
    }
    if include_history:
        out["history"] = _llm_call_history_windows(history_rows)
    return out


def build_llm_host_from_env(default_host="127.0.0.1", default_port="11434") -> str:
    """
    Legacy helper name kept for compatibility.
    Returns Hydra Base LLM endpoint from Redis settings.
    """
    endpoint, _ = _resolve_hydra_llm_defaults()
    return endpoint

def get_llm_client_from_env(host: Optional[str] = None, model: Optional[str] = None, **kwargs) -> "LLMClientWrapper":
    """
    Construct an LLMClientWrapper using explicit host/model overrides,
    with Hydra Base LLM settings fallback from Redis.
    No .env host/model fallback is used.
    """
    redis_conn = kwargs.pop("redis_conn", None)
    explicit_host = str(host or "").strip()
    explicit_model = str(model or "").strip()

    base_servers = resolve_hydra_base_servers(redis_conn=redis_conn, include_legacy=True)
    default_host = str(base_servers[0]["endpoint"]).strip() if base_servers else ""
    default_model = str(base_servers[0]["model"]).strip() if base_servers else ""
    if not default_host or not default_model:
        fallback_host, fallback_model = _resolve_hydra_llm_defaults(redis_conn=redis_conn)
        if not default_host:
            default_host = fallback_host
        if not default_model:
            default_model = fallback_model

    resolved_host = explicit_host or default_host
    resolved_model = explicit_model or default_model
    if not resolved_host or not resolved_model:
        raise RuntimeError(HYDRA_LLM_SETUP_ERROR)

    if not explicit_host and not explicit_model and len(base_servers) > 1:
        clients: List[LLMClientWrapper] = []
        signature_parts: List[str] = []
        for row in base_servers:
            endpoint = str(row.get("endpoint") or "").strip()
            row_model = str(row.get("model") or "").strip()
            if not endpoint or not row_model:
                continue
            clients.append(LLMClientWrapper(host=endpoint, model=row_model, **kwargs))
            signature_parts.append(f"{endpoint}|{row_model}")
        if len(clients) > 1:
            pool_key = "||".join(signature_parts)
            return RoundRobinLLMClientWrapper(clients=clients, pool_key=pool_key)
        if len(clients) == 1:
            return clients[0]

    return LLMClientWrapper(host=resolved_host, model=resolved_model, **kwargs)

class LLMClientWrapper:
    def __init__(self, host, model=None, **kwargs):
        resolved_host = str(host or "").strip()
        resolved_model = str(model or "").strip()
        if not resolved_host or not resolved_model:
            raise RuntimeError(HYDRA_LLM_SETUP_ERROR)

        base_url = _normalize_base_url(resolved_host)

        self.client = AsyncOpenAI(
            base_url=base_url,
            api_key=os.getenv("LLM_API_KEY", "not-needed"),
            **kwargs
        )

        self.host = base_url.rstrip("/")
        self.model = resolved_model

        # Common generation defaults (caller can override per-call)
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "1024"))
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.7"))

        # Per-instance perf aggregation for one chat turn.
        self._llm_calls = 0
        self._llm_elapsed_sec = 0.0
        self._llm_prompt_tokens = 0
        self._llm_completion_tokens = 0
        self._llm_total_tokens = 0
        self._llm_model_last = str(resolved_model or "").strip() or "LLM"

    async def aclose(self):
        client = getattr(self, "client", None)
        if client is None:
            return

        close_async = getattr(client, "aclose", None)
        if callable(close_async):
            try:
                await close_async()
            except RuntimeError as exc:
                # Defensive shutdown guard: can happen if app is tearing down.
                if "Event loop is closed" not in str(exc):
                    raise
            return

        close_sync = getattr(client, "close", None)
        if callable(close_sync):
            result = close_sync()
            if asyncio.iscoroutine(result):
                try:
                    await result
                except RuntimeError as exc:
                    if "Event loop is closed" not in str(exc):
                        raise

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()

    def get_perf_stats(self, *, reset: bool = False) -> Dict[str, Any]:
        elapsed = max(0.0, float(self._llm_elapsed_sec))
        prompt_tokens = max(0, int(self._llm_prompt_tokens))
        completion_tokens = max(0, int(self._llm_completion_tokens))
        total_tokens = max(0, int(self._llm_total_tokens))
        tps_total = (float(total_tokens) / elapsed) if elapsed > 0.0 and total_tokens > 0 else 0.0
        tps_comp = (float(completion_tokens) / elapsed) if elapsed > 0.0 and completion_tokens > 0 else 0.0

        out: Dict[str, Any] = {
            "model": str(self._llm_model_last or self.model or "LLM"),
            "elapsed": round(elapsed, 6),
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "tps_total": round(tps_total, 4),
            "tps_comp": round(tps_comp, 4),
            "calls": max(0, int(self._llm_calls)),
        }

        if reset:
            self._llm_calls = 0
            self._llm_elapsed_sec = 0.0
            self._llm_prompt_tokens = 0
            self._llm_completion_tokens = 0
            self._llm_total_tokens = 0
        return out

    async def chat(self, messages, **kwargs):
        """
        Thin wrapper around OpenAI-compatible /v1/chat/completions.
        Accepts either timeout (seconds) or timeout_ms (milliseconds).
        Returns: {"model": str, "message": {"role": "assistant", "content": "..."}}
        """
        # Normalize timeout variants
        timeout = kwargs.pop("timeout", None)
        timeout_ms = kwargs.pop("timeout_ms", None)
        if timeout is None and timeout_ms is not None:
            try:
                timeout = float(timeout_ms) / 1000.0
            except Exception:
                timeout = None

        stream = kwargs.pop("stream", False)
        model = kwargs.pop("model", self.model)

        # Provide sensible defaults if not supplied. A caller can pass
        # max_tokens=None to explicitly disable token capping for this call.
        if "max_tokens" not in kwargs:
            kwargs["max_tokens"] = self.max_tokens
        elif kwargs.get("max_tokens") is None:
            kwargs.pop("max_tokens", None)
        if "temperature" not in kwargs:
            kwargs["temperature"] = self.temperature

        # sanitize messages (prevents empty-user poison + normalizes non-string content)
        try:
            messages = _sanitize_chat_messages(messages if isinstance(messages, list) else [])
        except Exception:
            # fail open: at least avoid crashing
            messages = messages if isinstance(messages, list) else []

        call_id = _register_active_llm_call(
            host=str(self.host or "").strip(),
            model=str(model or "").strip(),
            stream=bool(stream),
            message_count=(len(messages) if isinstance(messages, list) else 0),
        )
        call_error: Optional[Exception] = None
        final_model = str(model or "").strip()

        try:
            started_at = asyncio.get_running_loop().time()
            response = await self.client.chat.completions.create(
                model=model,
                messages=messages,
                stream=stream,
                timeout=timeout,
                **kwargs,
            )
            elapsed = max(0.0, float(asyncio.get_running_loop().time() - started_at))
            self._llm_calls += 1
            self._llm_elapsed_sec += elapsed

            response_model = getattr(response, "model", model)
            if response_model:
                self._llm_model_last = str(response_model)
            final_model = str(response_model or final_model or "").strip()

            usage = getattr(response, "usage", None)
            prompt_tokens = 0
            completion_tokens = 0
            total_tokens = 0
            try:
                if isinstance(usage, dict):
                    prompt_tokens = int(usage.get("prompt_tokens") or 0)
                    completion_tokens = int(usage.get("completion_tokens") or 0)
                    total_tokens = int(usage.get("total_tokens") or 0)
                elif usage is not None:
                    prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
                    completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
                    total_tokens = int(getattr(usage, "total_tokens", 0) or 0)
            except Exception:
                prompt_tokens = 0
                completion_tokens = 0
                total_tokens = 0
            if total_tokens <= 0:
                total_tokens = max(0, prompt_tokens + completion_tokens)

            self._llm_prompt_tokens += max(0, prompt_tokens)
            self._llm_completion_tokens += max(0, completion_tokens)
            self._llm_total_tokens += max(0, total_tokens)

            if stream:
                return response

            # Defensive: choices can be empty in edge cases / errors
            if not getattr(response, "choices", None):
                return {
                    "model": getattr(response, "model", model),
                    "message": {"role": "assistant", "content": ""},
                }

            choice = response.choices[0].message or {}
            raw_content = getattr(choice, "content", "") if hasattr(choice, "content") else choice.get("content", "")
            content_text = _coerce_content_to_text(raw_content)

            return {
                "model": getattr(response, "model", model),
                "message": {"role": getattr(choice, "role", "assistant"), "content": content_text},
            }
        except Exception as exc:
            call_error = exc
            raise
        finally:
            _finish_active_llm_call(
                call_id,
                error=call_error,
                response_model=final_model,
            )


class RoundRobinLLMClientWrapper:
    def __init__(self, *, clients: List[LLMClientWrapper], pool_key: str = ""):
        self._clients = [client for client in (clients or []) if client is not None]
        if not self._clients:
            raise RuntimeError(HYDRA_LLM_SETUP_ERROR)
        self._pool_key = str(pool_key or "").strip()
        self.host = str(getattr(self._clients[0], "host", "") or "")
        self.model = str(getattr(self._clients[0], "model", "") or "")

    def _select_client(self) -> LLMClientWrapper:
        if len(self._clients) == 1:
            return self._clients[0]
        idx = _next_hydra_base_rr_index(self._pool_key, len(self._clients))
        return self._clients[idx]

    async def chat(self, messages, **kwargs):
        client = self._select_client()
        return await client.chat(messages, **kwargs)

    async def aclose(self):
        seen: set[int] = set()
        for client in self._clients:
            ident = id(client)
            if ident in seen:
                continue
            seen.add(ident)
            close_async = getattr(client, "aclose", None)
            if callable(close_async):
                try:
                    await close_async()
                except RuntimeError as exc:
                    if "Event loop is closed" not in str(exc):
                        raise

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()

    def get_perf_stats(self, *, reset: bool = False) -> Dict[str, Any]:
        elapsed_total = 0.0
        prompt_tokens_total = 0
        completion_tokens_total = 0
        total_tokens_total = 0
        calls_total = 0
        model_names: List[str] = []

        for client in self._clients:
            getter = getattr(client, "get_perf_stats", None)
            if not callable(getter):
                continue
            stats = getter(reset=reset)
            if not isinstance(stats, dict):
                continue
            elapsed_total += max(0.0, float(stats.get("elapsed") or 0.0))
            prompt_tokens_total += max(0, int(stats.get("prompt_tokens") or 0))
            completion_tokens_total += max(0, int(stats.get("completion_tokens") or 0))
            total_tokens_total += max(0, int(stats.get("total_tokens") or 0))
            calls_total += max(0, int(stats.get("calls") or 0))
            model_name = str(stats.get("model") or "").strip()
            if model_name and model_name not in model_names:
                model_names.append(model_name)

        if total_tokens_total <= 0:
            total_tokens_total = max(0, prompt_tokens_total + completion_tokens_total)

        tps_total = (
            float(total_tokens_total) / float(elapsed_total)
            if elapsed_total > 0.0 and total_tokens_total > 0
            else 0.0
        )
        tps_comp = (
            float(completion_tokens_total) / float(elapsed_total)
            if elapsed_total > 0.0 and completion_tokens_total > 0
            else 0.0
        )

        model_label = ", ".join(model_names[:4]).strip()
        if len(model_names) > 4:
            model_label = f"{model_label}, +{len(model_names) - 4}"
        if model_label:
            model_label = f"round_robin({model_label})"
        else:
            model_label = "round_robin"

        return {
            "model": model_label,
            "elapsed": round(elapsed_total, 6),
            "prompt_tokens": prompt_tokens_total,
            "completion_tokens": completion_tokens_total,
            "total_tokens": total_tokens_total,
            "tps_total": round(tps_total, 4),
            "tps_comp": round(tps_comp, 4),
            "calls": calls_total,
        }

# ---------------------------------------------------------
# Function JSON parsing helpers (unchanged)
# ---------------------------------------------------------
def extract_json(text: str):
    """
    Extract the first valid JSON object or array from text.
    Strips code fences and tolerates extra prose around it.
    Works for both { ... } and [ ... ] blocks.
    """
    if not text:
        return None

    s = text.strip()

    # Remove ```json fences
    if s.startswith("```") and s.endswith("```"):
        s = re.sub(r"^```(?:json)?\s*|\s*```$", "", s, flags=re.MULTILINE).strip()

    # Try whole text first
    try:
        json.loads(s)
        return s
    except Exception:
        pass

    # Bracket scanning for either {...} or [...]
    stack = []
    start_idx = None
    for i, char in enumerate(s):
        if char in "{[":
            if not stack:
                start_idx = i
            stack.append(char)
        elif char in "}]":
            if stack:
                opening = stack.pop()
                if not stack and start_idx is not None:
                    candidate = s[start_idx:i+1]
                    try:
                        json.loads(candidate)
                        return candidate
                    except json.JSONDecodeError:
                        continue
    return None

TOOL_MARKUP_REPAIR_PROMPT = (
    "Formatting error: do not emit tool channel markup like <|channel|> or to=... . "
    "If you need to call a tool, respond with exactly ONE strict JSON object and nothing else: "
    "{\"function\":\"name\",\"arguments\":{...}}. "
    "Do not include extra prose, and do not output multiple tool calls. "
    "Otherwise respond with NO_TOOL."
)
TOOL_MARKUP_FAILURE_TEXT = "Sorry, I had trouble formatting a tool call. Please try again."

def looks_like_tool_markup(text: str) -> bool:
    if not text:
        return False
    s = str(text)
    if "<|" in s and "|>" in s:
        return True
    if re.search(r"\bto=[A-Za-z0-9_.-]+\b", s) and ("commentary" in s or "message" in s):
        return True
    return False

def parse_function_json(response_text: str):
    _DECISION_PREFIX_RE = re.compile(
        r"^\s*(CONTINUE|RETRY|ASK[\s_-]*USER|FAIL|FINAL|FINAL[\s_-]*ANSWER|RETRY[\s_-]*TOOL|NEED[\s_-]*USER[\s_-]*INFO|ANSWER|NO_TOOL)\s*:\s*(.*)$",
        re.IGNORECASE | re.DOTALL,
    )
    _NON_TOOL_PREFIXES = {
        "continue",
        "retry",
        "ask_user",
        "fail",
        "final",
        "retry_tool",
        "final_answer",
        "need_user_info",
        "answer",
        "no_tool",
    }

    def _pick(obj):
        if isinstance(obj, dict):
            if "function" in obj and isinstance(obj["function"], str):
                return {"function": obj["function"], "arguments": obj.get("arguments", {}) or {}}
            if "tool" in obj and isinstance(obj["tool"], str):
                return {"function": obj["tool"], "arguments": obj.get("arguments", {}) or {}}
        return None

    if not response_text:
        return None

    s = str(response_text).strip()

    # Remove code fence markers anywhere to avoid treating ```json as a tool name.
    if "```" in s:
        s = re.sub(r"```(?:json)?", "", s, flags=re.IGNORECASE).strip()

    # ---------------------------------------------------------
    # Tool-call markup (Codex/OpenAI style) support:
    #   <|channel|>commentary to=repo_browser.get_verba_help <|message|>{"verba_id":"weather_forecast"}
    # ---------------------------------------------------------
    m_tool = re.search(r"to=([a-zA-Z0-9_.-]+).*?<\|message\|>(\{.*\})", s, re.DOTALL)
    if m_tool:
        tool_name = m_tool.group(1).strip()
        tool_name = tool_name.split(".")[-1] if tool_name else tool_name
        blob = m_tool.group(2).strip()
        try:
            args = json.loads(blob)
            if isinstance(args, dict):
                if tool_name == "get_verba_help":
                    if "verba_id" not in args and "name" in args:
                        args["verba_id"] = args.get("name")
                return {"function": tool_name, "arguments": args}
        except Exception:
            return {"function": tool_name, "arguments": {}}

    # strip code fences early so shorthand/prefix parsing still works
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.MULTILINE).strip()
    if s.endswith("```"):
        s = re.sub(r"\s*```$", "", s, flags=re.MULTILINE).strip()

    # Handle channel wrappers:
    #   <|channel|>final ANSWER<|message|>RETRY_TOOL: {...}
    if "<|message|>" in s:
        s = s.rsplit("<|message|>", 1)[-1].strip()

    # Handle Minos decision wrappers:
    #   RETRY_TOOL: {...}
    #   FINAL_ANSWER: ...
    decision_match = _DECISION_PREFIX_RE.match(s)
    if decision_match:
        decision_kind = str(decision_match.group(1) or "").strip().lower().replace(" ", "_").replace("-", "_")
        decision_body = str(decision_match.group(2) or "").strip()
        if decision_kind.startswith("retry"):
            s = decision_body
        else:
            # FINAL_ANSWER / NEED_USER_INFO / NO_TOOL are not tool calls.
            return None

    # Accept shorthand ONLY when it's the whole message: plugin_id{"arg":"..."}
    m = re.match(r'^([a-zA-Z0-9_]+)\s*(\{.*\})\s*$', s, re.DOTALL)
    if m:
        func = m.group(1)
        blob = m.group(2)
        try:
            args = json.loads(blob)
            if isinstance(args, dict):
                return {"function": func, "arguments": args}
        except Exception:
            pass

    try:
        response_json = json.loads(s)
    except json.JSONDecodeError:
        json_str = extract_json(s)
        if not json_str:
            json_str = None

        if json_str:
            prefix = s.split(json_str, 1)[0].strip()
        else:
            prefix = ""

        # Slightly stricter "possible func" match
        m2 = re.search(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*[:(]*\s*$', prefix)
        if json_str:
            try:
                args = json.loads(json_str)
                picked = _pick(args)
                if picked:
                    return picked
            except Exception:
                args = None

            if m2:
                possible_func = str(m2.group(1) or "").strip()
                if possible_func and possible_func.lower() not in _NON_TOOL_PREFIXES:
                    try:
                        args = json.loads(json_str)
                        if isinstance(args, dict):
                            return {"function": possible_func, "arguments": args}
                    except Exception:
                        pass

        if json_str:
            try:
                response_json = json.loads(json_str)
            except Exception:
                response_json = None
        else:
            response_json = None

    picked = _pick(response_json)
    if picked:
        return picked

    if isinstance(response_json, list):
        for item in response_json:
            picked = _pick(item)
            if picked:
                return picked

    # ---------------------------------------------------------
    # Relaxed fallback: tolerate invalid JSON for file/code tools
    # ---------------------------------------------------------
    def _extract_relaxed_string(text: str, key: str) -> str | None:
        pat = rf'"{re.escape(key)}"\s*:\s*"(.*)"\s*\}}\s*\}}\s*$'
        m = re.search(pat, text, flags=re.DOTALL)
        if m:
            return m.group(1)
        pat = rf"'{re.escape(key)}'\s*:\s*'(.*)'\s*\}}\s*\}}\s*$"
        m = re.search(pat, text, flags=re.DOTALL)
        if m:
            return m.group(1)
        return None

    def _extract_relaxed_scalar(text: str, key: str) -> str | None:
        pat = rf'"{re.escape(key)}"\s*:\s*"([^"]+)"'
        m = re.search(pat, text)
        if m:
            return m.group(1)
        pat = rf"'{re.escape(key)}'\s*:\s*'([^']+)'"
        m = re.search(pat, text)
        if m:
            return m.group(1)
        return None

    def _relaxed_tool_call(text: str) -> dict | None:
        fm = re.search(r'"function"\s*:\s*"([^"]+)"', text)
        if not fm:
            fm = re.search(r"'function'\s*:\s*'([^']+)'", text)
        if not fm:
            return None
        func = fm.group(1).strip()
        if not func:
            return None

        if func in {"write_file"}:
            path = _extract_relaxed_scalar(text, "path")
            content = _extract_relaxed_string(text, "content")
            if path and content is not None:
                return {"function": func, "arguments": {"path": path, "content": content}}
        return None

    relaxed = _relaxed_tool_call(s)
    if relaxed:
        return relaxed

    return None

# ---------------------------------------------------------
# Get latest image from redis
# ---------------------------------------------------------
def get_latest_image_from_history(key: str, allowed_mimetypes=None):
    if allowed_mimetypes is None:
        allowed_mimetypes = ["image/png", "image/jpeg"]

    history = redis_client.lrange(key, 0, -1)
    for entry in reversed(history):
        try:
            msg = json.loads(entry)
            content = msg.get("content")

            # 🔥 Unwrap plugin_response wrapper
            if isinstance(content, dict) and content.get("marker") == "plugin_response":
                content = content.get("content", {})

            if isinstance(content, dict):
                mimetype = content.get("mimetype", "")
                filename = content.get("name", "").lower()

                if (
                    content.get("type") == "image"
                    and content.get("data")
                    and mimetype in allowed_mimetypes
                    and not filename.endswith(".webp")
                ):
                    image_bytes = base64.b64decode(content["data"])
                    return image_bytes, filename or "input.png"
        except Exception:
            continue

    return None, None

# ---------------------------------------------------------
# Get latest file from redis
# ---------------------------------------------------------
def get_latest_file_from_history(channel_id, filetype="file", extensions=None):
    history_key = f"tater:channel:{channel_id}:history"
    raw_history = redis_client.lrange(history_key, 0, -1)

    for entry in reversed(raw_history):
        try:
            data = json.loads(entry)
            content = data.get("content")

            # 🔥 Unwrap plugin_response wrapper
            if isinstance(content, dict) and content.get("marker") == "plugin_response":
                content = content.get("content", {})

            if isinstance(content, dict) and content.get("type") == filetype:
                filename = content.get("name", "").lower()
                if not extensions or any(filename.endswith(ext) for ext in extensions):
                    return content
        except Exception:
            continue

    return None

# ---------------------------------------------------------
# ComfyUI websocket (no timeouts, Ctrl-C friendly)
# ---------------------------------------------------------
def run_comfy_prompt(base_http: str, base_ws: str, prompt: dict):
    client_id = str(uuid.uuid4())

    # 1) Open dedicated WS for this job (no timeout)
    ws = websocket.create_connection(f"{base_ws}/ws?clientId={client_id}")

    try:
        # 2) POST the prompt, include client_id (no timeout)
        resp = requests.post(
            f"{base_http}/prompt",
            json={"prompt": prompt, "client_id": client_id}
        )
        resp.raise_for_status()
        data = resp.json()
        prompt_id = data.get("prompt_id") or data.get("promptId")
        if not prompt_id:
            raise RuntimeError(f"ComfyUI /prompt did not return prompt_id: {data}")

        # 3) Listen until our prompt is finished
        while True:
            try:
                raw = ws.recv()  # blocks; KeyboardInterrupt will break out cleanly
            except KeyboardInterrupt:
                # Graceful cancel: close socket and bubble up so caller can handle it
                try:
                    ws.close()
                finally:
                    raise
            except Exception as e:
                # Other WS errors bubble as runtime errors
                raise RuntimeError(f"ComfyUI WS error for prompt {prompt_id}: {e}")

            if not raw:
                continue

            try:
                evt = json.loads(raw)
            except Exception:
                continue

            etype = evt.get("type")
            edata = evt.get("data") or {}
            evt_prompt_id = edata.get("prompt_id") or evt.get("prompt_id")

            # Only react to our own prompt
            if evt_prompt_id != prompt_id:
                continue

            # Finished: 'executing' with node == None indicates completion
            if etype == "executing" and edata.get("node") is None:
                return prompt_id, evt

            # (Optional: handle other terminal frames here if your setup emits them.)

    finally:
        try:
            ws.close()
        except Exception:
            pass
