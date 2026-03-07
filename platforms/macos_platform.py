import asyncio
import base64
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import redis
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, Response
from pydantic import BaseModel, Field

import plugin_registry as pr
from cerberus import resolve_agent_limits, run_cerberus_turn
from conversation_artifacts import load_conversation_artifacts, save_conversation_artifacts
from helpers import build_llm_host_from_env, get_llm_client_from_env, get_tater_name
from notify.core import dispatch_notification_sync
from notify.media import load_queue_attachments
from notify.queue import is_expired as notify_item_is_expired, queue_key as notify_queue_key
from plugin_kernel import plugin_supports_platform
from plugin_result import narrate_result, result_artifacts
from tool_runtime import execute_plugin_call

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("macos")

BIND_HOST = "0.0.0.0"
DEFAULT_PORT = 8791
DEFAULT_SESSION_HISTORY_MAX = 6
DEFAULT_MAX_HISTORY_CAP = 20
DEFAULT_SESSION_TTL_SECONDS = 2 * 60 * 60
INLINE_ATTACHMENT_MAX_BYTES = int(os.getenv("MACOS_INLINE_ATTACHMENT_MAX_BYTES", "0"))
MAX_NOTIFY_WAIT_SECONDS = float(os.getenv("MACOS_NOTIFY_MAX_WAIT_SECONDS", "45"))
NOTIFY_QUEUE_LOCK_KEY = "notifyq:macos:lock"
_POLL_ACCESS_LOG_PATHS = ("/macos/notifications/next", "/macos/bootstrap")


class _SuppressPollingAccessLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        return not any(path in message for path in _POLL_ACCESS_LOG_PATHS)


def _configure_access_log_filters() -> None:
    access_logger = logging.getLogger("uvicorn.access")
    for existing in list(access_logger.filters):
        if isinstance(existing, _SuppressPollingAccessLogFilter):
            return
    access_logger.addFilter(_SuppressPollingAccessLogFilter())

redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
redis_blob_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=False)

PLATFORM_SETTINGS = {
    "category": "macOS Settings",
    "required": {
        "bind_port": {
            "label": "Bind Port",
            "type": "number",
            "default": DEFAULT_PORT,
            "description": "TCP port for the Tater macOS bridge.",
        },
        "SESSION_HISTORY_MAX": {
            "label": "Session History (turns)",
            "type": "number",
            "default": DEFAULT_SESSION_HISTORY_MAX,
            "description": "How many recent turns to include per Mac conversation.",
        },
        "MAX_HISTORY_CAP": {
            "label": "Max History Cap",
            "type": "number",
            "default": DEFAULT_MAX_HISTORY_CAP,
            "description": "Hard ceiling to prevent runaway context sizes.",
        },
        "SESSION_TTL_SECONDS": {
            "label": "Session TTL",
            "type": "select",
            "options": ["5m", "30m", "1h", "2h", "6h", "24h"],
            "default": "2h",
            "description": "How long to keep a Mac conversation history alive.",
        },
        "AUTH_TOKEN": {
            "label": "Auth Token (optional)",
            "type": "password",
            "default": "",
            "description": "If set, the macOS client must send this in the X-Tater-Token header.",
        },
    },
}


class MacOSAssetIn(BaseModel):
    asset_id: Optional[str] = None
    type: Optional[str] = None
    name: Optional[str] = None
    mimetype: Optional[str] = None
    data: Optional[str] = None
    data_base64: Optional[str] = None
    blob_key: Optional[str] = None
    path: Optional[str] = None
    url: Optional[str] = None
    size: Optional[int] = None
    source: Optional[str] = None


class MacOSChatRequest(BaseModel):
    scope: Optional[str] = None
    device_id: Optional[str] = None
    user_text: Optional[str] = None
    clipboard_text: Optional[str] = None
    assets: List[MacOSAssetIn] = Field(default_factory=list)
    flags: Dict[str, Any] = Field(default_factory=dict)
    context: Dict[str, Any] = Field(default_factory=dict)


class MacOSPluginRequest(BaseModel):
    scope: Optional[str] = None
    device_id: Optional[str] = None
    plugin_name: str
    plugin_args: Dict[str, Any] = Field(default_factory=dict)
    user_text: Optional[str] = None
    clipboard_text: Optional[str] = None
    assets: List[MacOSAssetIn] = Field(default_factory=list)
    flags: Dict[str, Any] = Field(default_factory=dict)
    context: Dict[str, Any] = Field(default_factory=dict)


class MacOSAssetUploadRequest(BaseModel):
    scope: Optional[str] = None
    device_id: Optional[str] = None
    asset: MacOSAssetIn


def _platform_settings() -> Dict[str, str]:
    return redis_client.hgetall("macos_platform_settings") or {}


def _parse_duration_seconds(value: Any, default_seconds: int) -> int:
    if value is None:
        return default_seconds
    text = str(value).strip().lower()
    try:
        return int(text)
    except ValueError:
        pass
    if text.endswith("m") and text[:-1].isdigit():
        return int(text[:-1]) * 60
    if text.endswith("h") and text[:-1].isdigit():
        return int(text[:-1]) * 3600
    if text.endswith("d") and text[:-1].isdigit():
        return int(text[:-1]) * 86400
    return default_seconds


def _get_duration_setting(name: str, default_seconds: int) -> int:
    return _parse_duration_seconds(_platform_settings().get(name), default_seconds)


def _get_int_setting(name: str, default: int) -> int:
    raw = _platform_settings().get(name)
    try:
        return int(str(raw).strip()) if raw is not None and str(raw).strip() else default
    except Exception:
        return default


def _get_str_setting(name: str, default: str = "") -> str:
    raw = _platform_settings().get(name)
    return str(raw) if raw is not None else default


def get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    return bool(enabled and enabled.lower() == "true")


def _scope_value(scope: Optional[str], device_id: Optional[str]) -> str:
    scope_text = str(scope or "").strip()
    if scope_text:
        return scope_text
    device_text = str(device_id or "").strip()
    if device_text:
        return f"mac:{device_text}"
    return "default"


def _remember_active_macos_client(scope: str, device_id: Optional[str]) -> None:
    scope_text = str(scope or "").strip()
    device_text = str(device_id or "").strip()
    try:
        pipe = redis_client.pipeline()
        if scope_text and scope_text != "default":
            pipe.set("tater:macos:last_scope", scope_text)
        if device_text:
            pipe.set("tater:macos:last_device_id", device_text)
        pipe.set("tater:macos:last_seen_ts", str(time.time()))
        pipe.execute()
    except Exception:
        return


def _history_key(scope: str) -> str:
    return f"tater:macos:session:{scope}:history"


def _load_scope_artifacts(scope: str, limit: int = 64) -> List[Dict[str, Any]]:
    return load_conversation_artifacts(
        redis_client,
        platform="macos",
        scope=scope,
        limit=limit,
    )


def _find_scope_artifact(scope: str, asset_id: str) -> Optional[Dict[str, Any]]:
    target = str(asset_id or "").strip()
    if not target:
        return None
    for item in _load_scope_artifacts(scope, limit=64):
        if str(item.get("artifact_id") or "").strip() == target:
            return item
    return None


def _flatten_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: List[str] = []
        for item in value:
            text = _flatten_to_text(item)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()
    if isinstance(value, dict):
        for key in ("message", "summary", "text", "content"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
        if str(value.get("type") or "").strip():
            kind = str(value.get("type") or "file").strip()
            name = str(value.get("name") or "").strip()
            return f"[{kind}:{name}]" if name else f"[{kind}]"
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    return str(value)


def _to_template_msg(role: str, content: Any) -> Optional[Dict[str, Any]]:
    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        if content.get("phase", "final") != "final":
            return None
        payload = content.get("content")
        text = _flatten_to_text(payload).strip()
        if len(text) > 4000:
            text = text[:4000] + " ..."
        return {"role": "assistant", "content": text}

    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        text = json.dumps(
            {
                "function": content.get("plugin"),
                "arguments": content.get("arguments", {}),
            },
            ensure_ascii=False,
        )
        return {"role": "assistant", "content": text}

    if isinstance(content, dict) and str(content.get("type") or "").strip().lower() in {"image", "audio", "video", "file"}:
        return {"role": role, "content": _flatten_to_text(content)}

    if isinstance(content, str):
        return {"role": role, "content": content}

    return {"role": role, "content": _flatten_to_text(content)}


def _enforce_user_assistant_alternation(loop_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for message in loop_messages:
        if not message:
            continue
        if not merged:
            merged.append(message)
            continue
        if merged[-1]["role"] == message["role"]:
            a = merged[-1]["content"]
            b = message["content"]
            merged[-1]["content"] = (str(a) + "\n\n" + str(b)).strip()
        else:
            merged.append(message)
    return merged


async def _load_history(scope: str, limit: int) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []
    raw = redis_client.lrange(_history_key(scope), -limit, -1)
    loop_messages: List[Dict[str, Any]] = []
    for entry in raw:
        try:
            parsed = json.loads(entry)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        role = str(parsed.get("role") or "user").strip().lower()
        if role not in {"user", "assistant"}:
            role = "assistant"
        templ = _to_template_msg(role, parsed.get("content"))
        if templ is not None:
            loop_messages.append(templ)
    return _enforce_user_assistant_alternation(loop_messages)


async def _history_for_client(scope: str, limit: int) -> List[Dict[str, str]]:
    history = await _load_history(scope, limit)
    out: List[Dict[str, str]] = []
    for entry in history:
        if not isinstance(entry, dict):
            continue
        role = str(entry.get("role") or "assistant").strip().lower()
        if role not in {"user", "assistant"}:
            role = "assistant"
        text = str(entry.get("content") or "").strip()
        if not text:
            continue
        out.append({"role": role, "text": text})
    return out


def _assistant_identity_payload() -> Dict[str, str]:
    first_name, last_name = get_tater_name()
    first = str(first_name or "").strip() or "Tater"
    last = str(last_name or "").strip()
    display_name = " ".join(part for part in (first, last) if part).strip() or "Tater"
    return {
        "first_name": first,
        "last_name": last,
        "display_name": display_name,
        "name": display_name,
    }


async def _save_message(
    scope: str,
    role: str,
    content: Any,
    *,
    max_store: int,
    ttl_seconds: int,
    username: str = "",
    user_id: str = "",
) -> None:
    payload: Dict[str, Any] = {
        "role": role,
        "content": content,
        "timestamp": time.time(),
    }
    if username:
        payload["username"] = username
    if user_id:
        payload["user_id"] = user_id
    key = _history_key(scope)
    pipe = redis_client.pipeline()
    pipe.rpush(key, json.dumps(payload, ensure_ascii=False))
    if max_store > 0:
        pipe.ltrim(key, -max_store, -1)
    pipe.expire(key, ttl_seconds)
    pipe.execute()


def _decode_base64_payload(data: Any) -> Optional[bytes]:
    text = str(data or "").strip()
    if not text:
        return None
    if text.startswith("data:") and "," in text:
        text = text.split(",", 1)[1]
    pad = len(text) % 4
    if pad:
        text += "=" * (4 - pad)
    try:
        decoded = base64.b64decode(text)
    except Exception:
        return None
    return bytes(decoded) if decoded else None


def _artifact_history_content(item: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "type": str(item.get("type") or "file").strip() or "file",
        "name": str(item.get("name") or "").strip(),
        "mimetype": str(item.get("mimetype") or "").strip(),
        "size": item.get("size"),
    }
    artifact_id = str(item.get("artifact_id") or "").strip()
    if artifact_id:
        out["artifact_id"] = artifact_id
    blob_key = str(item.get("blob_key") or "").strip()
    if blob_key:
        out["blob_key"] = blob_key
    url = str(item.get("url") or "").strip()
    if url:
        out["url"] = url
    return out


def _save_request_assets(scope: str, assets: List[MacOSAssetIn]) -> List[Dict[str, Any]]:
    resolved: List[Dict[str, Any]] = []
    pending_new: List[Dict[str, Any]] = []
    for asset in assets or []:
        asset_dict = asset.model_dump(exclude_none=True)
        asset_id = str(asset_dict.get("asset_id") or "").strip()
        if asset_id:
            existing = _find_scope_artifact(scope, asset_id)
            if existing is None:
                raise HTTPException(status_code=404, detail=f"Asset '{asset_id}' was not found for this scope.")
            resolved.append(existing)
            continue

        if asset_dict.get("data_base64") and not asset_dict.get("data"):
            asset_dict["data"] = asset_dict.pop("data_base64")
        if not any(asset_dict.get(key) for key in ("data", "blob_key", "path", "url")):
            continue
        asset_dict.setdefault("source", "macos_client")
        pending_new.append(asset_dict)

    if pending_new:
        saved = save_conversation_artifacts(
            redis_client,
            platform="macos",
            scope=scope,
            artifacts=pending_new,
        )
        resolved.extend(saved)

    return resolved


def _build_runtime_context(
    *,
    context_payload: Dict[str, Any],
    origin: Dict[str, Any],
    request_text: str,
    device_id: str,
) -> Dict[str, Any]:
    runtime_context: Dict[str, Any] = dict(context_payload or {})
    runtime_context["context"] = dict(context_payload or {})
    runtime_context["origin"] = dict(origin or {})
    if request_text:
        runtime_context["request_text"] = request_text
        runtime_context["raw_message"] = request_text
        runtime_context["raw"] = request_text
    if device_id:
        runtime_context.setdefault("device_id", device_id)
    return runtime_context


def _compose_user_text(
    *,
    user_text: str,
    clipboard_text: str,
    context_payload: Dict[str, Any],
    has_assets: bool,
) -> str:
    base = str(user_text or "").strip()
    clipboard = str(clipboard_text or "").strip()
    selected_text = str((context_payload or {}).get("selected_text") or "").strip()

    extras: List[str] = []
    if selected_text and selected_text != clipboard:
        extras.append(f"Selected text:\n{selected_text[:8000]}")
    if clipboard:
        extras.append(f"Clipboard text:\n{clipboard[:8000]}")

    if extras:
        prefix = "Mac context:"
        context_block = prefix + "\n" + "\n\n".join(extras)
        if base:
            return f"{base}\n\n{context_block}".strip()
        return context_block

    if base:
        return base
    if has_assets:
        return "Please analyze the attached Mac content."
    return ""


def _filter_origin_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in (payload or {}).items():
        if value in (None, ""):
            continue
        out[key] = value
    return out


def _build_origin(
    *,
    scope: str,
    device_id: str,
    flags: Dict[str, Any],
    context_payload: Dict[str, Any],
    input_artifacts: List[Dict[str, Any]],
    request_kind: str,
) -> Dict[str, Any]:
    origin: Dict[str, Any] = {
        "platform": "macos",
        "scope": scope,
        "device_id": device_id,
        "request_kind": request_kind,
    }
    if context_payload:
        for key in ("device_name", "current_app", "current_url", "app_version", "os_version"):
            value = context_payload.get(key)
            if value not in (None, ""):
                origin[key] = value
    if flags:
        origin["flags"] = dict(flags)
    if input_artifacts:
        origin["input_artifacts"] = [dict(item) for item in input_artifacts]
    return _filter_origin_payload(origin)


def build_system_prompt() -> str:
    return (
        "You are a macOS menu bar AI assistant.\n"
        "Keep replies concise, direct, and clean.\n"
        "Return plain text only; the client renders attachments and native actions separately.\n"
    )


def _read_blob_bytes(blob_key: str) -> Optional[bytes]:
    key = str(blob_key or "").strip()
    if not key:
        return None
    try:
        data = redis_blob_client.get(key)
    except Exception:
        data = None
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8", errors="replace")
    return None


def _read_path_bytes(path_value: str) -> Optional[bytes]:
    path_text = str(path_value or "").strip()
    if not path_text:
        return None
    try:
        path = Path(path_text)
        if not path.is_file():
            return None
        return path.read_bytes()
    except Exception:
        return None


def _artifact_binary(item: Dict[str, Any]) -> Optional[bytes]:
    if isinstance(item.get("bytes"), (bytes, bytearray)):
        return bytes(item.get("bytes"))
    if isinstance(item.get("data"), (bytes, bytearray)):
        return bytes(item.get("data"))
    decoded = _decode_base64_payload(item.get("data"))
    if decoded:
        return decoded
    blob_key = str(item.get("blob_key") or "").strip()
    if blob_key:
        return _read_blob_bytes(blob_key)
    path_value = str(item.get("path") or "").strip()
    if path_value:
        return _read_path_bytes(path_value)
    return None


def _asset_download_url(scope: str, asset_id: str) -> str:
    asset_text = str(asset_id or "").strip()
    if not asset_text:
        return ""
    query = urlencode({"scope": str(scope or "").strip()})
    return f"/macos/asset/{asset_text}?{query}" if query else f"/macos/asset/{asset_text}"


def _attachment_payload(scope: str, item: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "type": str(item.get("type") or "file").strip() or "file",
        "name": str(item.get("name") or "file.bin").strip() or "file.bin",
        "mimetype": str(item.get("mimetype") or "application/octet-stream").strip() or "application/octet-stream",
    }
    for key in ("size", "url"):
        value = item.get(key)
        if value not in (None, ""):
            out[key] = value

    asset_id = str(item.get("artifact_id") or "").strip()
    if asset_id:
        out["asset_id"] = asset_id
        out["download_url"] = _asset_download_url(scope, asset_id)

    if INLINE_ATTACHMENT_MAX_BYTES > 0:
        binary = _artifact_binary(item)
        if binary is not None:
            out["size"] = len(binary)
            if len(binary) <= INLINE_ATTACHMENT_MAX_BYTES:
                out["data_base64"] = base64.b64encode(binary).decode("ascii")
                out["inline"] = True
            else:
                out["inline"] = False
    elif asset_id:
        out["inline"] = False

    return out


def _notification_targets_match(targets: Dict[str, Any], scope: str, device_id: str) -> bool:
    if not isinstance(targets, dict):
        return False
    target_scope = str(targets.get("scope") or "").strip()
    target_device_id = str(targets.get("device_id") or "").strip()
    if target_scope and target_scope != scope:
        return False
    if target_device_id and target_device_id != device_id:
        return False
    return bool(target_scope or target_device_id)


def _pop_macos_notification(scope: str, device_id: str) -> Optional[Dict[str, Any]]:
    key = notify_queue_key("macos")
    if not key:
        return None

    lock = None
    acquired = False
    try:
        lock = redis_client.lock(NOTIFY_QUEUE_LOCK_KEY, timeout=10, blocking_timeout=1)
        acquired = bool(lock.acquire(blocking=True))
    except Exception:
        acquired = False

    try:
        raw_items = redis_client.lrange(key, 0, -1)
        for raw in raw_items:
            try:
                item = json.loads(raw)
            except Exception:
                redis_client.lrem(key, 1, raw)
                continue
            if not isinstance(item, dict):
                redis_client.lrem(key, 1, raw)
                continue
            if notify_item_is_expired(item):
                redis_client.lrem(key, 1, raw)
                continue
            if not _notification_targets_match(item.get("targets") or {}, scope, device_id):
                continue
            if redis_client.lrem(key, 1, raw):
                return item
    finally:
        if acquired and lock is not None:
            try:
                lock.release()
            except Exception:
                pass

    return None


def _notification_payload(scope: str, item: Dict[str, Any]) -> Dict[str, Any]:
    queue_attachments = load_queue_attachments(redis_client, item.get("id"))
    stored_output_artifacts = _store_output_artifacts(scope, queue_attachments) if queue_attachments else []
    attachments = [_attachment_payload(scope, artifact) for artifact in stored_output_artifacts]
    return {
        "id": str(item.get("id") or "").strip(),
        "title": str(item.get("title") or "").strip(),
        "message": str(item.get("message") or "").strip(),
        "attachments": attachments,
        "meta": dict(item.get("meta") or {}),
        "origin": dict(item.get("origin") or {}),
        "created_at": item.get("created_at"),
    }


def _store_output_artifacts(scope: str, artifacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not artifacts:
        return []
    return save_conversation_artifacts(
        redis_client,
        platform="macos",
        scope=scope,
        artifacts=artifacts,
    )


def _extract_actions(payload: Any) -> List[Dict[str, Any]]:
    allowed = {"copy_to_clipboard", "open_url", "notify", "save_file", "reveal_in_finder"}
    if not isinstance(payload, dict):
        return []
    raw_actions = payload.get("actions")
    if not isinstance(raw_actions, list):
        data = payload.get("data")
        raw_actions = data.get("actions") if isinstance(data, dict) else None
    if not isinstance(raw_actions, list):
        return []

    actions: List[Dict[str, Any]] = []
    for item in raw_actions:
        if not isinstance(item, dict):
            continue
        action_type = str(item.get("type") or item.get("action") or "").strip()
        if action_type not in allowed:
            continue
        clean: Dict[str, Any] = {"type": action_type}
        for key, value in item.items():
            if key in {"type", "action"} or value is None:
                continue
            if isinstance(value, (str, int, float, bool)):
                clean[key] = value
            elif isinstance(value, list):
                clean[key] = [entry for entry in value if isinstance(entry, (str, int, float, bool))]
            elif isinstance(value, dict):
                clean[key] = {
                    str(k): v for k, v in value.items() if isinstance(v, (str, int, float, bool))
                }
        actions.append(clean)
    return actions


def _response_payload(
    *,
    scope: str,
    assistant_text: str,
    actions: List[Dict[str, Any]],
    attachments: List[Dict[str, Any]],
    ok: bool = True,
) -> Dict[str, Any]:
    return {
        "ok": bool(ok),
        "scope": scope,
        "assistant_text": str(assistant_text or "").strip(),
        "actions": actions or [],
        "attachments": attachments or [],
    }


async def _emit_tool_wait_message(
    *,
    scope: str,
    device_id: str,
    tool_name: str,
    plugin_obj: Any,
    mention: str,
    origin: Dict[str, Any],
    max_store: int,
    ttl_seconds: int,
) -> None:
    if _llm is None or plugin_obj is None:
        return
    if not plugin_supports_platform(plugin_obj, "macos"):
        return

    template = str(getattr(plugin_obj, "waiting_prompt_template", "") or "").strip()
    if not template:
        return
    try:
        wait_msg = template.format(mention=mention)
    except Exception:
        wait_msg = template

    wait_response = await _llm.chat(
        messages=[
            {"role": "system", "content": "Write one short, friendly status line."},
            {"role": "user", "content": str(wait_msg).strip()},
        ]
    )
    wait_text = str((wait_response.get("message", {}) or {}).get("content", "") or "").strip()
    if not wait_text:
        return

    await _save_message(
        scope,
        "assistant",
        {"marker": "plugin_wait", "content": wait_text},
        max_store=max_store,
        ttl_seconds=ttl_seconds,
        username="assistant",
        user_id="assistant",
    )

    wait_origin = dict(origin or {})
    wait_origin["request_kind"] = "tool_wait"
    if tool_name:
        wait_origin["tool"] = tool_name

    wait_targets: Dict[str, Any] = {"scope": scope}
    if device_id:
        wait_targets["device_id"] = device_id
    wait_meta: Dict[str, Any] = {
        "priority": "normal",
        "tags": ["tool_wait"],
        "ttl_sec": 300,
        "kind": "tool_wait",
    }
    title = _assistant_identity_payload().get("display_name") or "Tater"
    await asyncio.to_thread(
        dispatch_notification_sync,
        "macos",
        title,
        wait_text,
        wait_targets,
        wait_origin,
        wait_meta,
        None,
    )


def _require_auth(x_tater_token: Optional[str]) -> None:
    configured = _get_str_setting("AUTH_TOKEN", "").strip()
    if not configured:
        return
    supplied = str(x_tater_token or "").strip()
    if supplied != configured:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Tater-Token header.")


app = FastAPI(title="Tater macOS Bridge", version="1.0")
_llm = None


@app.on_event("startup")
async def _on_startup():
    global _llm
    _llm = get_llm_client_from_env()
    logger.info(f"[macOS] LLM client -> {build_llm_host_from_env()}")


@app.get("/macos/health")
async def health():
    return {"ok": True, "platform": "macos", "version": "1.0"}


@app.get("/macos/assistant")
async def assistant_identity(x_tater_token: Optional[str] = Header(None)):
    _require_auth(x_tater_token)
    return {"ok": True, "assistant": _assistant_identity_payload()}


@app.get("/macos/history")
async def session_history(
    scope: Optional[str] = Query(None),
    device_id: Optional[str] = Query(None),
    limit: int = Query(40, ge=1, le=200),
    x_tater_token: Optional[str] = Header(None),
):
    _require_auth(x_tater_token)
    resolved_scope = _scope_value(scope, device_id)
    _remember_active_macos_client(resolved_scope, device_id)
    max_cap = max(1, _get_int_setting("MAX_HISTORY_CAP", DEFAULT_MAX_HISTORY_CAP))
    resolved_limit = min(max(1, int(limit)), max_cap * 4)
    return {
        "ok": True,
        "scope": resolved_scope,
        "history": await _history_for_client(resolved_scope, resolved_limit),
    }


@app.get("/macos/bootstrap")
async def bootstrap_state(
    scope: Optional[str] = Query(None),
    device_id: Optional[str] = Query(None),
    history_limit: int = Query(40, ge=1, le=200),
    x_tater_token: Optional[str] = Header(None),
):
    _require_auth(x_tater_token)
    resolved_scope = _scope_value(scope, device_id)
    _remember_active_macos_client(resolved_scope, device_id)
    max_cap = max(1, _get_int_setting("MAX_HISTORY_CAP", DEFAULT_MAX_HISTORY_CAP))
    resolved_limit = min(max(1, int(history_limit)), max_cap * 4)
    logger.debug(
        "[macOS] bootstrap poll scope=%s device_id=%s history_limit=%s",
        resolved_scope,
        str(device_id or "").strip() or "-",
        resolved_limit,
    )
    return {
        "ok": True,
        "scope": resolved_scope,
        "assistant": _assistant_identity_payload(),
        "history": await _history_for_client(resolved_scope, resolved_limit),
    }


@app.post("/macos/asset")
async def upload_asset(payload: MacOSAssetUploadRequest, x_tater_token: Optional[str] = Header(None)):
    _require_auth(x_tater_token)
    scope = _scope_value(payload.scope, payload.device_id)
    _remember_active_macos_client(scope, payload.device_id)
    saved = _save_request_assets(scope, [payload.asset])
    if not saved:
        raise HTTPException(status_code=400, detail="No asset payload was provided.")
    asset = saved[0]
    return {
        "ok": True,
        "scope": scope,
        "asset_id": str(asset.get("artifact_id") or "").strip(),
        "attachment": _attachment_payload(scope, asset),
    }


@app.get("/macos/asset/{asset_id}")
async def download_asset(
    asset_id: str,
    scope: Optional[str] = Query(None),
    device_id: Optional[str] = Query(None),
    x_tater_token: Optional[str] = Header(None),
):
    _require_auth(x_tater_token)
    resolved_scope = _scope_value(scope, device_id)
    asset = _find_scope_artifact(resolved_scope, asset_id)
    if asset is None:
        raise HTTPException(status_code=404, detail=f"Asset '{asset_id}' was not found for this scope.")

    binary = _artifact_binary(asset)
    if binary is None:
        raise HTTPException(status_code=404, detail=f"Asset '{asset_id}' bytes are unavailable.")

    name = str(asset.get("name") or "file.bin").strip() or "file.bin"
    mimetype = str(asset.get("mimetype") or "application/octet-stream").strip() or "application/octet-stream"
    safe_name = name.replace('"', "_")
    headers = {"Content-Disposition": f'inline; filename="{safe_name}"'}
    return Response(content=binary, media_type=mimetype, headers=headers)


@app.get("/macos/notifications/next")
async def next_notification(
    scope: Optional[str] = Query(None),
    device_id: Optional[str] = Query(None),
    wait_seconds: float = Query(0.0, ge=0.0, le=MAX_NOTIFY_WAIT_SECONDS),
    x_tater_token: Optional[str] = Header(None),
):
    _require_auth(x_tater_token)
    resolved_scope = _scope_value(scope, device_id)
    device_text = str(device_id or "").strip()
    _remember_active_macos_client(resolved_scope, device_text)
    deadline = time.monotonic() + max(0.0, min(float(wait_seconds), MAX_NOTIFY_WAIT_SECONDS))

    while True:
        item = _pop_macos_notification(resolved_scope, device_text)
        if item is not None:
            logger.debug(
                "[macOS] notification poll hit scope=%s device_id=%s notification_id=%s",
                resolved_scope,
                device_text or "-",
                str(item.get("id") or "").strip() or "-",
            )
            return {"ok": True, "notification": _notification_payload(resolved_scope, item)}
        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(0.25)

    logger.debug(
        "[macOS] notification poll timeout scope=%s device_id=%s wait_seconds=%.1f",
        resolved_scope,
        device_text or "-",
        max(0.0, min(float(wait_seconds), MAX_NOTIFY_WAIT_SECONDS)),
    )
    return {"ok": True, "notification": None}


@app.post("/macos/chat")
async def chat(payload: MacOSChatRequest, x_tater_token: Optional[str] = Header(None)):
    _require_auth(x_tater_token)
    if _llm is None:
        raise HTTPException(status_code=503, detail="LLM backend not initialized.")

    scope = _scope_value(payload.scope, payload.device_id)
    _remember_active_macos_client(scope, payload.device_id)
    context_payload = dict(payload.context or {})
    input_artifacts = _save_request_assets(scope, list(payload.assets or []))
    effective_user_text = _compose_user_text(
        user_text=str(payload.user_text or ""),
        clipboard_text=str(payload.clipboard_text or ""),
        context_payload=context_payload,
        has_assets=bool(input_artifacts),
    )
    if not effective_user_text:
        raise HTTPException(status_code=400, detail="Provide user_text, clipboard_text, selected_text, or assets.")

    history_limit = min(
        max(_get_int_setting("SESSION_HISTORY_MAX", DEFAULT_SESSION_HISTORY_MAX), 0),
        _get_int_setting("MAX_HISTORY_CAP", DEFAULT_MAX_HISTORY_CAP),
    )
    history_limit = max(1, history_limit)
    ttl_seconds = _get_duration_setting("SESSION_TTL_SECONDS", DEFAULT_SESSION_TTL_SECONDS)
    user_id = str(payload.device_id or scope).strip() or scope
    username = str(context_payload.get("device_name") or "macos_user").strip() or "macos_user"

    await _save_message(
        scope,
        "user",
        effective_user_text,
        max_store=history_limit,
        ttl_seconds=ttl_seconds,
        username=username,
        user_id=user_id,
    )
    for artifact in input_artifacts:
        await _save_message(
            scope,
            "user",
            _artifact_history_content(artifact),
            max_store=history_limit,
            ttl_seconds=ttl_seconds,
            username=username,
            user_id=user_id,
        )

    history = await _load_history(scope, history_limit)
    origin = _build_origin(
        scope=scope,
        device_id=str(payload.device_id or "").strip(),
        flags=dict(payload.flags or {}),
        context_payload=context_payload,
        input_artifacts=input_artifacts,
        request_kind="chat",
    )
    runtime_context = _build_runtime_context(
        context_payload=context_payload,
        origin=origin,
        request_text=effective_user_text,
        device_id=str(payload.device_id or "").strip(),
    )
    mention_target = str(context_payload.get("device_name") or "there").strip() or "there"

    async def _wait_callback(func_name: str, plugin_obj: Any):
        try:
            await _emit_tool_wait_message(
                scope=scope,
                device_id=str(payload.device_id or "").strip(),
                tool_name=str(func_name or "").strip(),
                plugin_obj=plugin_obj,
                mention=mention_target,
                origin=origin,
                max_store=history_limit,
                ttl_seconds=ttl_seconds,
            )
        except Exception:
            return

    try:
        agent_max_rounds, agent_max_tool_calls = resolve_agent_limits(redis_client)
        result = await run_cerberus_turn(
            llm_client=_llm,
            platform="macos",
            history_messages=history,
            registry=dict(pr.get_registry_snapshot() or {}),
            enabled_predicate=get_plugin_enabled,
            context=runtime_context,
            user_text=effective_user_text,
            scope=scope,
            origin=origin,
            wait_callback=_wait_callback,
            redis_client=redis_client,
            max_rounds=agent_max_rounds,
            max_tool_calls=agent_max_tool_calls,
            platform_preamble=build_system_prompt(),
        )
    except Exception:
        logger.exception("[macOS] chat request failed")
        fallback = "Sorry, I ran into a problem processing that."
        await _save_message(
            scope,
            "assistant",
            {"marker": "plugin_response", "phase": "final", "content": fallback},
            max_store=history_limit,
            ttl_seconds=ttl_seconds,
            username="assistant",
            user_id="assistant",
        )
        return _response_payload(scope=scope, assistant_text=fallback, actions=[], attachments=[], ok=False)

    final_text = str(result.get("text") or "").strip()
    await _save_message(
        scope,
        "assistant",
        {"marker": "plugin_response", "phase": "final", "content": final_text},
        max_store=history_limit,
        ttl_seconds=ttl_seconds,
        username="assistant",
        user_id="assistant",
    )

    stored_output_artifacts = _store_output_artifacts(scope, result.get("artifacts") or [])
    attachments = [_attachment_payload(scope, item) for item in stored_output_artifacts]
    for artifact in stored_output_artifacts:
        await _save_message(
            scope,
            "assistant",
            {"marker": "plugin_response", "phase": "final", "content": _artifact_history_content(artifact)},
            max_store=history_limit,
            ttl_seconds=ttl_seconds,
            username="assistant",
            user_id="assistant",
        )
    actions = _extract_actions(result.get("raw_tool_payload"))
    return _response_payload(
        scope=scope,
        assistant_text=final_text,
        actions=actions,
        attachments=attachments,
        ok=True,
    )


@app.post("/macos/plugin")
async def plugin_call(payload: MacOSPluginRequest, x_tater_token: Optional[str] = Header(None)):
    _require_auth(x_tater_token)
    if _llm is None:
        raise HTTPException(status_code=503, detail="LLM backend not initialized.")

    scope = _scope_value(payload.scope, payload.device_id)
    _remember_active_macos_client(scope, payload.device_id)
    history_limit = min(
        max(_get_int_setting("SESSION_HISTORY_MAX", DEFAULT_SESSION_HISTORY_MAX), 0),
        _get_int_setting("MAX_HISTORY_CAP", DEFAULT_MAX_HISTORY_CAP),
    )
    history_limit = max(1, history_limit)
    ttl_seconds = _get_duration_setting("SESSION_TTL_SECONDS", DEFAULT_SESSION_TTL_SECONDS)
    context_payload = dict(payload.context or {})
    input_artifacts = _save_request_assets(scope, list(payload.assets or []))
    request_text = _compose_user_text(
        user_text=str(payload.user_text or f"Quick action: {payload.plugin_name}"),
        clipboard_text=str(payload.clipboard_text or ""),
        context_payload=context_payload,
        has_assets=bool(input_artifacts),
    )
    user_id = str(payload.device_id or scope).strip() or scope
    username = str(context_payload.get("device_name") or "macos_user").strip() or "macos_user"

    await _save_message(
        scope,
        "user",
        request_text,
        max_store=history_limit,
        ttl_seconds=ttl_seconds,
        username=username,
        user_id=user_id,
    )
    for artifact in input_artifacts:
        await _save_message(
            scope,
            "user",
            _artifact_history_content(artifact),
            max_store=history_limit,
            ttl_seconds=ttl_seconds,
            username=username,
            user_id=user_id,
        )

    origin = _build_origin(
        scope=scope,
        device_id=str(payload.device_id or "").strip(),
        flags=dict(payload.flags or {}),
        context_payload=context_payload,
        input_artifacts=input_artifacts,
        request_kind="plugin",
    )
    runtime_context = _build_runtime_context(
        context_payload=context_payload,
        origin=origin,
        request_text=request_text,
        device_id=str(payload.device_id or "").strip(),
    )
    registry_snapshot = dict(pr.get_registry_snapshot() or {})
    plugin_name = str(payload.plugin_name or "").strip()
    plugin_obj = registry_snapshot.get(plugin_name)
    mention_target = str(context_payload.get("device_name") or "there").strip() or "there"
    args = dict(payload.plugin_args or {})
    args.setdefault("origin", origin)

    await _save_message(
        scope,
        "assistant",
        {"marker": "plugin_call", "plugin": payload.plugin_name, "arguments": args},
        max_store=history_limit,
        ttl_seconds=ttl_seconds,
        username="assistant",
        user_id="assistant",
    )

    try:
        await _emit_tool_wait_message(
            scope=scope,
            device_id=str(payload.device_id or "").strip(),
            tool_name=plugin_name,
            plugin_obj=plugin_obj,
            mention=mention_target,
            origin=origin,
            max_store=history_limit,
            ttl_seconds=ttl_seconds,
        )
    except Exception:
        pass

    exec_result = await execute_plugin_call(
        func=plugin_name,
        args=args,
        platform="macos",
        registry=registry_snapshot,
        enabled_predicate=get_plugin_enabled,
        llm_client=_llm,
        context=runtime_context,
    )
    normalized_result = exec_result.get("result") if isinstance(exec_result.get("result"), dict) else {}
    assistant_text = await narrate_result(normalized_result, llm_client=_llm, platform="macos")
    artifact_list = result_artifacts(normalized_result)
    stored_output_artifacts = _store_output_artifacts(scope, artifact_list)
    attachments = [_attachment_payload(scope, item) for item in stored_output_artifacts]
    actions = _extract_actions(normalized_result)

    await _save_message(
        scope,
        "assistant",
        {"marker": "plugin_response", "phase": "final", "content": assistant_text},
        max_store=history_limit,
        ttl_seconds=ttl_seconds,
        username="assistant",
        user_id="assistant",
    )
    for artifact in stored_output_artifacts:
        await _save_message(
            scope,
            "assistant",
            {"marker": "plugin_response", "phase": "final", "content": _artifact_history_content(artifact)},
            max_store=history_limit,
            ttl_seconds=ttl_seconds,
            username="assistant",
            user_id="assistant",
        )

    return _response_payload(
        scope=scope,
        assistant_text=assistant_text,
        actions=actions,
        attachments=attachments,
        ok=bool(normalized_result.get("ok", False)),
    )


def run(stop_event: Optional[threading.Event] = None):
    _configure_access_log_filters()
    raw_port = redis_client.hget("macos_platform_settings", "bind_port")
    try:
        port = int(raw_port) if raw_port is not None else DEFAULT_PORT
    except (TypeError, ValueError):
        logger.warning(f"[macOS] Invalid bind_port '{raw_port}', defaulting to {DEFAULT_PORT}")
        port = DEFAULT_PORT

    config = uvicorn.Config(app, host=BIND_HOST, port=port, log_level="info")
    server = uvicorn.Server(config)

    def _serve():
        import asyncio

        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()

        async def _start():
            try:
                await server.serve()
            except SystemExit as exc:
                code = getattr(exc, "code", 1)
                if code not in (None, 0):
                    logger.error(f"[macOS] Server failed to start on {BIND_HOST}:{port}.")
            except Exception:
                logger.exception(f"[macOS] Server failed on {BIND_HOST}:{port}")

        task = loop.create_task(_start())

        def _watch():
            if not stop_event:
                return
            while not stop_event.is_set():
                time.sleep(0.5)
            try:
                server.should_exit = True
            except Exception:
                pass

        if stop_event:
            threading.Thread(target=_watch, daemon=True).start()

        try:
            loop.run_until_complete(task)
        finally:
            if not loop.is_closed():
                loop.stop()
                loop.close()

    logger.info(f"[macOS] Listening on http://{BIND_HOST}:{port}")
    _serve()
