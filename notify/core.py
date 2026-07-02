import asyncio
import base64
import html
import json
import logging
import os
import re
import threading
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse, urlunparse

import requests

from helpers import redis_client
from notify.media import store_queue_attachments
from notify.queue import (
    build_queue_item,
    load_default_targets,
    normalize_platform,
    queue_key,
    resolve_targets,
)
from runtime_executors import run_background
from tateros import integration_store as integration_store_module
from verba_settings import get_verba_settings

logger = logging.getLogger("notify_core")
logger.setLevel(logging.INFO)

ALWAYS_ON_NOTIFIERS: Tuple[str, ...] = (
    "discord",
    "irc",
    "matrix",
    "homeassistant",
    "ntfy",
    "telegram",
    "meshtastic",
    "macos",
    "little_spud",
    "webui",
    "display",
    "wordpress",
)

_ATTACHMENT_PLATFORMS = {"discord", "matrix", "telegram", "macos", "little_spud"}

_URL_PATTERN = re.compile(r"https?://\S+")
_BARE_URL_PATTERN = re.compile(r"(?<!\()(?<!\])\bhttps?://\S+\b")
_WEBUI_CHAT_HISTORY_KEY = "webui:chat_history"
_WEBUI_DEFAULT_MAX_STORE = 20
_SPUD_LINK_NODES_KEY = "tater:spudlink:nodes:v1"
_LITTLE_SPUD_PUSH_GATEWAY_URL = "https://push.taterassistant.com/little-spud/send"
HOMEASSISTANT_DEFAULT_BASE_URL = "http://homeassistant.local:8123"


def load_homeassistant_config(*, required: bool = False, client: Any = None) -> Dict[str, str]:
    fn = integration_store_module.integration_function("homeassistant", "load_homeassistant_config")
    if fn:
        return fn(required=required, client=client)
    if required:
        raise ValueError("Home Assistant integration is not enabled.")
    return {"base": HOMEASSISTANT_DEFAULT_BASE_URL, "token": ""}


def core_notifier_platforms() -> Tuple[str, ...]:
    return ALWAYS_ON_NOTIFIERS


def notifier_supports_attachments(platform: str) -> bool:
    return normalize_platform(platform) in _ATTACHMENT_PLATFORMS


def _boolish(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    val = str(value).strip().lower()
    if val in ("1", "true", "yes", "y", "on", "enabled"):
        return True
    if val in ("0", "false", "no", "n", "off", "disabled"):
        return False
    return default


def _coerce_attachments(attachments: Any) -> List[Dict[str, Any]]:
    if not isinstance(attachments, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in attachments:
        if isinstance(item, dict):
            out.append(dict(item))
    return out


def _little_spud_text(value: Any, *, limit: int = 500) -> str:
    text = str(value or "").strip()
    text = re.sub(r"[\x00-\x1f\x7f]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[: max(1, int(limit or 500))]


def _little_spud_push_identity(node: Dict[str, Any]) -> Dict[str, str]:
    metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
    user_name = _little_spud_text(metadata.get("user_name") or metadata.get("username") or metadata.get("user"), limit=80)
    device_name = _little_spud_text(
        metadata.get("device_name") or metadata.get("device") or node.get("name"),
        limit=80,
    )
    scope = f"user:{user_name}:{device_name}" if user_name and device_name else ""
    return {
        "user_name": user_name,
        "device_name": device_name,
        "scope": scope,
    }


def _little_spud_push_identity_key(node: Dict[str, Any]) -> str:
    metadata = node.get("metadata") if isinstance(node.get("metadata"), dict) else {}
    stable_keys = (
        "app_instance_id",
        "app_install_id",
        "installation_id",
        "install_id",
        "client_id",
        "client_device_id",
        "device_uuid",
        "device_id",
    )
    for key in stable_keys:
        value = _little_spud_text(metadata.get(key) or node.get(key), limit=240).casefold()
        if value:
            return f"stable:{key}:{value}"

    identity = _little_spud_push_identity(node)
    scope = _little_spud_text(identity.get("scope"), limit=240).casefold()
    if scope:
        return f"scope:{scope}"
    return ""


def _little_spud_push_target_matches(targets: Dict[str, Any], node: Dict[str, Any]) -> bool:
    target_map = targets if isinstance(targets, dict) else {}
    wildcard_tokens = {"*", "all", "any", "broadcast"}
    identity = _little_spud_push_identity(node)
    node_id = _little_spud_text(node.get("id"), limit=120)
    checks = (
        ("node_id", node_id),
        ("destination", node_id),
        ("scope", identity.get("scope") or ""),
        ("device_id", identity.get("device_name") or ""),
        ("device_name", identity.get("device_name") or ""),
        ("user", identity.get("user_name") or ""),
        ("user_name", identity.get("user_name") or ""),
    )
    for key, expected in checks:
        wanted = _little_spud_text(target_map.get(key), limit=240)
        if not wanted:
            continue
        if wanted.lower() in wildcard_tokens:
            return True
        if expected and wanted == expected:
            return True
    return False


def _little_spud_push_node_score(node: Dict[str, Any]) -> float:
    push = node.get("push") if isinstance(node.get("push"), dict) else {}
    score = 0.0
    for value in (
        push.get("updated_at"),
        push.get("registered_at"),
        node.get("last_seen_at"),
        node.get("created_at"),
    ):
        try:
            score = max(score, float(value or 0))
        except Exception:
            continue
    return score


def _little_spud_push_nodes_for_targets(targets: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        raw_nodes = redis_client.hgetall(_SPUD_LINK_NODES_KEY) or {}
    except Exception:
        logger.exception("[notify] failed loading Little Spud push registrations")
        return []

    by_push_device: Dict[str, Dict[str, Any]] = {}
    for node_id, raw_value in raw_nodes.items():
        try:
            node = json.loads(str(raw_value or "{}"))
        except Exception:
            continue
        if not isinstance(node, dict):
            continue
        role = str(node.get("role") or "").strip().lower()
        if role != "little_spud":
            continue
        node["id"] = str(node.get("id") or node_id or "").strip()
        push = node.get("push") if isinstance(node.get("push"), dict) else {}
        if not _boolish(push.get("enabled"), default=False):
            continue
        provider = str(push.get("provider") or "").strip().lower()
        if provider not in {"fcm", "firebase"}:
            continue
        push_device_id = str(push.get("push_device_id") or "").strip()
        if not push_device_id or not str(push.get("push_secret") or "").strip():
            continue
        if _little_spud_push_target_matches(targets, node):
            existing = by_push_device.get(push_device_id)
            if existing is None or _little_spud_push_node_score(node) >= _little_spud_push_node_score(existing):
                by_push_device[push_device_id] = node

    by_identity: Dict[str, Dict[str, Any]] = {}
    for node in by_push_device.values():
        push = node.get("push") if isinstance(node.get("push"), dict) else {}
        identity_key = _little_spud_push_identity_key(node) or f"push:{str(push.get('push_device_id') or '').strip()}"
        existing = by_identity.get(identity_key)
        if existing is None or _little_spud_push_node_score(node) >= _little_spud_push_node_score(existing):
            by_identity[identity_key] = node
    return list(by_identity.values())


def _little_spud_push_body(item: Dict[str, Any]) -> Tuple[str, str]:
    del item
    return "Little Spud", "New Little Spud notification"


def _little_spud_push_gateway_url(push: Dict[str, Any]) -> str:
    configured = _little_spud_text(os.getenv("TATER_LITTLE_SPUD_PUSH_GATEWAY_URL"), limit=500)
    if configured:
        return configured
    stored = _little_spud_text(push.get("gateway_url") or push.get("relay_url"), limit=500)
    return stored or _LITTLE_SPUD_PUSH_GATEWAY_URL


def _dispatch_little_spud_push(item: Dict[str, Any]) -> None:
    targets = item.get("targets") if isinstance(item.get("targets"), dict) else {}
    nodes = _little_spud_push_nodes_for_targets(targets)
    if not nodes:
        return
    title, body = _little_spud_push_body(item)
    meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
    event_id = _little_spud_text(item.get("id"), limit=120)
    for node in nodes:
        push = node.get("push") if isinstance(node.get("push"), dict) else {}
        url = _little_spud_push_gateway_url(push)
        if not url:
            continue
        payload = {
            "push_device_id": str(push.get("push_device_id") or "").strip(),
            "push_secret": str(push.get("push_secret") or "").strip(),
            "event_id": event_id,
            "title": title,
            "body": body,
            "content_mode": "generic",
            "priority": str(meta.get("priority") or "normal"),
            "platform": "little_spud",
        }
        try:
            response = requests.post(url, json=payload, timeout=5)
            if response.status_code >= 400:
                logger.warning("[notify] Little Spud push gateway failed (%s): %s", response.status_code, response.text[:300])
        except Exception as exc:
            logger.warning("[notify] Little Spud push gateway failed: %s", exc)


def _schedule_little_spud_push(item: Dict[str, Any]) -> None:
    payload = dict(item)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        thread = threading.Thread(target=_dispatch_little_spud_push, args=(payload,), daemon=True)
        thread.start()
        return
    loop.create_task(run_background(_dispatch_little_spud_push, payload))


def _coerce_webui_data_b64(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.lower().startswith("data:"):
        comma = text.find(",")
        if comma <= 0:
            return ""
        header = text[:comma].lower()
        if ";base64" not in header:
            return ""
        text = text[comma + 1 :].strip()
    return "".join(text.split())


def _webui_media_type_from_mimetype(mimetype: Any) -> str:
    token = str(mimetype or "").strip().lower()
    if token.startswith("image/"):
        return "image"
    if token.startswith("audio/"):
        return "audio"
    if token.startswith("video/"):
        return "video"
    return "file"


def _normalize_webui_attachment(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None

    mimetype = str(item.get("mimetype") or "application/octet-stream").strip() or "application/octet-stream"
    media_type = str(item.get("type") or "").strip().lower()
    if media_type not in {"image", "audio", "video", "file"}:
        media_type = _webui_media_type_from_mimetype(mimetype)

    normalized: Dict[str, Any] = {
        "type": media_type,
        "name": str(item.get("name") or f"{media_type}.bin").strip() or f"{media_type}.bin",
        "mimetype": mimetype,
    }

    file_id = str(item.get("id") or "").strip()
    if file_id:
        normalized["id"] = file_id

    data_b64 = _coerce_webui_data_b64(item.get("data_b64"))
    if not data_b64:
        data_b64 = _coerce_webui_data_b64(item.get("data"))
    if data_b64:
        normalized["data_b64"] = data_b64

    try:
        size = int(item.get("size"))
    except Exception:
        size = -1
    if size >= 0:
        normalized["size"] = size

    if "id" not in normalized and "data_b64" not in normalized:
        return None
    return normalized


def _webui_max_store() -> int:
    try:
        raw = redis_client.get("tater:max_store")
        value = int(str(raw).strip()) if raw is not None else int(_WEBUI_DEFAULT_MAX_STORE)
    except Exception:
        value = int(_WEBUI_DEFAULT_MAX_STORE)
    return max(0, value)


def _save_webui_history_row(content: Any) -> None:
    payload = {
        "role": "assistant",
        "username": "assistant",
        "content": content,
    }
    redis_client.rpush(_WEBUI_CHAT_HISTORY_KEY, json.dumps(payload))
    max_store = _webui_max_store()
    if max_store > 0:
        redis_client.ltrim(_WEBUI_CHAT_HISTORY_KEY, -max_store, -1)


def _dispatch_webui(
    title: Optional[str],
    content: str,
    origin: Optional[Dict[str, Any]],
    meta: Optional[Dict[str, Any]],
    attachments: Optional[List[Dict[str, Any]]],
) -> str:
    del origin, meta
    message = str(content or "").strip()
    payload_attachments = _coerce_attachments(attachments)
    if not message and not payload_attachments:
        return "Cannot queue: missing message"

    title_text = str(title or "").strip()
    composed_message = message
    if title_text and message:
        composed_message = f"**{title_text}**\n\n{message}"
    elif title_text and not message:
        composed_message = f"**{title_text}**"

    if not composed_message and payload_attachments:
        composed_message = "Attachment"

    if composed_message:
        _save_webui_history_row(composed_message)

    rendered_attachments = 0
    for item in payload_attachments:
        normalized = _normalize_webui_attachment(item)
        if not isinstance(normalized, dict):
            continue
        _save_webui_history_row(normalized)
        rendered_attachments += 1

    if not composed_message and rendered_attachments <= 0:
        return "Cannot queue: missing message"
    return "Queued notification for webui"


def _dispatch_display(
    title: Optional[str],
    content: str,
    targets: Optional[Dict[str, Any]],
    origin: Optional[Dict[str, Any]],
    meta: Optional[Dict[str, Any]],
    attachments: Optional[List[Dict[str, Any]]],
) -> str:
    message = (content or "").strip()
    payload_attachments = _coerce_attachments(attachments)
    if not message and not payload_attachments and not title:
        return "Cannot queue: missing message"

    target_map = targets if isinstance(targets, dict) else {}
    meta_map = meta if isinstance(meta, dict) else {}
    origin_map = origin if isinstance(origin, dict) else {}
    try:
        from tater_voice import display_bus

        display_bus.publish_display_event(
            {
                "kind": meta_map.get("kind") or meta_map.get("display_kind") or "notification",
                "target": target_map.get("target") or target_map.get("device") or target_map.get("selector") or "all",
                "targets": target_map.get("targets") if isinstance(target_map.get("targets"), list) else [],
                "title": title or "",
                "message": message,
                "description": meta_map.get("description") or meta_map.get("summary") or "",
                "priority": meta_map.get("priority") or "normal",
                "ttl_seconds": meta_map.get("ttl_sec") or meta_map.get("ttl_seconds") or 90,
                "image_url": meta_map.get("image_url") or meta_map.get("snapshot_url") or "",
                "image_format": meta_map.get("image_format") or "",
                "media_url": meta_map.get("media_url") or meta_map.get("clip_url") or "",
                "source": origin_map.get("platform") or origin_map.get("source") or "notify",
                "attachments": payload_attachments,
                "meta": meta_map,
            }
        )
    except Exception as exc:
        logger.warning("[notify] display event failed: %s", exc)
        return f"Cannot queue display notification: {exc}"
    return "Queued notification for display"


def _matrix_room_ref(room_ref: Any) -> str:
    ref = str(room_ref or "").strip()
    if not ref:
        return ""
    if ref.startswith("!") or ref.startswith("#"):
        return ref
    if ":" in ref:
        return f"#{ref}"
    return ref


def _telegram_legacy_defaults() -> Dict[str, str]:
    settings = redis_client.hgetall("verba_settings:Telegram Notifier") or {}
    chat_id = str(settings.get("telegram_chat_id") or "").strip()
    if chat_id:
        return {"chat_id": chat_id}
    return {}


def _enqueue_queue_notification(
    platform: str,
    title: Optional[str],
    content: str,
    targets: Optional[Dict[str, Any]],
    origin: Optional[Dict[str, Any]],
    meta: Optional[Dict[str, Any]],
    attachments: Optional[List[Dict[str, Any]]] = None,
) -> str:
    payload_attachments = _coerce_attachments(attachments)
    message = (content or "").strip()
    if not message and not payload_attachments:
        return "Cannot queue: missing message"
    if not message and payload_attachments:
        message = "Attachment"

    platform = normalize_platform(platform)
    target_map = dict(targets or {})
    defaults = load_default_targets(platform, redis_client)

    if platform == "matrix":
        room_ref = target_map.get("room_id") or target_map.get("room_alias") or target_map.get("channel")
        if room_ref and not target_map.get("room_id"):
            target_map["room_id"] = _matrix_room_ref(room_ref)
        elif target_map.get("room_id"):
            target_map["room_id"] = _matrix_room_ref(target_map.get("room_id"))

    if platform == "telegram":
        if not target_map.get("chat_id"):
            if target_map.get("channel_id"):
                target_map["chat_id"] = target_map.get("channel_id")
            elif target_map.get("channel"):
                target_map["chat_id"] = target_map.get("channel")
        if not defaults.get("chat_id"):
            defaults = {**defaults, **_telegram_legacy_defaults()}

    resolved, err = resolve_targets(platform, target_map, origin, defaults)
    if err:
        return err

    item = build_queue_item(platform, title, message, resolved, origin, meta)
    if payload_attachments and platform in _ATTACHMENT_PLATFORMS:
        store_queue_attachments(redis_client, item.get("id"), payload_attachments)

    key = queue_key(platform)
    if not key:
        return "Cannot queue: missing destination queue"

    redis_client.rpush(key, json.dumps(item))
    if platform == "little_spud":
        _schedule_little_spud_push(item)
    return f"Queued notification for {platform}"


def _ha_settings() -> Tuple[str, str]:
    ha = load_homeassistant_config(required=False)
    return ha.get("base", ""), ha.get("token", "")


def _ha_call_service(domain: str, service: str, data: Dict[str, Any]) -> None:
    base, token = _ha_settings()
    if not token:
        raise RuntimeError("HA_TOKEN missing in Home Assistant Settings.")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{base}/api/services/{domain}/{service}"
    resp = requests.post(url, headers=headers, json=data, timeout=10)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")


def _send_persistent_notification(title: Optional[str], message: str) -> None:
    data = {
        "message": (message or "").strip(),
        "title": (title or "Notification").strip(),
    }
    _ha_call_service("persistent_notification", "create", data)


def _send_mobile(device_service: str, title: Optional[str], message: str) -> None:
    if not device_service:
        return
    if "." in device_service:
        domain, service = device_service.split(".", 1)
    else:
        domain, service = "notify", device_service
    data: Dict[str, Any] = {"message": message}
    if title:
        data["title"] = title
    _ha_call_service(domain, service, data)


def _default_homeassistant_device_service() -> str:
    settings = redis_client.hgetall("verba_settings:Home Assistant Notifier") or {}
    return (settings.get("DEFAULT_DEVICE_SERVICE") or "").strip()


def _dispatch_homeassistant(
    title: Optional[str],
    content: str,
    targets: Optional[Dict[str, Any]],
    origin: Optional[Dict[str, Any]],
    meta: Optional[Dict[str, Any]],
) -> str:
    message = (content or "").strip()
    if not message:
        return "Cannot queue: missing message"

    resolved, err = resolve_targets("homeassistant", targets, origin, defaults=None)
    if err:
        return err

    persistent_enabled = True
    if isinstance(resolved, dict) and "persistent" in resolved:
        persistent_enabled = _boolish(resolved.get("persistent"), True)

    if persistent_enabled:
        try:
            _send_persistent_notification(title, message)
        except Exception as exc:
            logger.warning("[notify] HA persistent notification failed: %s", exc)

    device_service = (resolved.get("device_service") or _default_homeassistant_device_service()).strip()
    if device_service:
        try:
            _send_mobile(device_service, title, message)
        except Exception as exc:
            logger.warning("[notify] mobile push failed: %s", exc)

    return "Queued notification for homeassistant"


def _strip_utm(url: str) -> str:
    try:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        clean_query = {k: v for k, v in query.items() if not k.lower().startswith("utm_")}
        parsed = parsed._replace(query="&".join(f"{k}={v[0]}" for k, v in clean_query.items()))
        return urlunparse(parsed)
    except Exception:
        return url


def _ntfy_first_url(text: str) -> Optional[str]:
    match = _URL_PATTERN.search(text or "")
    if not match:
        return None
    return _strip_utm(match.group(0))


def _ntfy_short_url_text(url: str) -> str:
    try:
        parsed = urlparse(url)
        return f"{parsed.netloc}{parsed.path}".rstrip("/")
    except Exception:
        return url


def _ntfy_linkify_bare_urls(text: str) -> str:
    def repl(match: re.Match) -> str:
        raw = match.group(0)
        clean = _strip_utm(raw)
        label = _ntfy_short_url_text(clean)
        return f"[{label}]({clean})"

    return _BARE_URL_PATTERN.sub(repl, text)


def _ntfy_markdown(title: Optional[str], message: str) -> str:
    msg = html.unescape(message or "").strip()

    lines: List[str] = []
    for line in msg.splitlines():
        clean = line.rstrip()
        if clean.startswith(("*", "-", "•")) and not clean.startswith(("* ", "- ", "• ")):
            clean = clean[0] + " " + clean[1:].lstrip()
        lines.append(clean)

    body = _ntfy_linkify_bare_urls("\n".join(lines).strip())
    head = f"## {title.strip()}\n\n" if title and title.strip() else ""
    return f"{head}{body}"


def _ntfy_settings(targets: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    settings = get_verba_settings("NTFY Notifier") or {}
    target_map = dict(targets or {})

    server = (
        target_map.get("ntfy_server")
        or target_map.get("server")
        or settings.get("ntfy_server")
        or "https://ntfy.sh"
    )
    topic = (
        target_map.get("ntfy_topic")
        or target_map.get("topic")
        or target_map.get("channel")
        or settings.get("ntfy_topic")
        or ""
    )
    priority = (
        target_map.get("ntfy_priority")
        or target_map.get("priority")
        or settings.get("ntfy_priority")
        or "3"
    )
    tags = (
        target_map.get("ntfy_tags")
        or target_map.get("tags")
        or settings.get("ntfy_tags")
        or ""
    )

    click_default = _boolish(settings.get("ntfy_click_from_first_url"), True)
    click_override = target_map.get("ntfy_click_from_first_url")
    if click_override is None:
        click_override = target_map.get("click_from_first_url")
    use_click = _boolish(click_override, click_default)

    token = (
        target_map.get("ntfy_token")
        or target_map.get("token")
        or settings.get("ntfy_token")
        or ""
    )
    username = (
        target_map.get("ntfy_username")
        or target_map.get("username")
        or settings.get("ntfy_username")
        or ""
    )
    password = (
        target_map.get("ntfy_password")
        or target_map.get("password")
        or settings.get("ntfy_password")
        or ""
    )

    return {
        "server": str(server or "").strip().rstrip("/"),
        "topic": str(topic or "").strip(),
        "priority": str(priority or "3").strip(),
        "tags": str(tags or "").strip(),
        "use_click": use_click,
        "token": str(token or "").strip(),
        "username": str(username or "").strip(),
        "password": str(password or "").strip(),
    }


def _send_ntfy(title: Optional[str], message: str, targets: Optional[Dict[str, Any]]) -> bool:
    cfg = _ntfy_settings(targets)
    topic = cfg.get("topic")
    if not topic:
        logger.debug("ntfy topic not set; skipping")
        return False

    url = f"{cfg['server']}/{topic}"
    headers = {
        "Priority": cfg["priority"] if cfg["priority"] in {"1", "2", "3", "4", "5"} else "3",
        "Markdown": "yes",
    }

    tags = cfg.get("tags")
    if tags:
        norm = ",".join([t.strip() for t in re.split(r"[,\s]+", tags) if t.strip()])
        if norm:
            headers["Tags"] = norm

    if cfg.get("use_click"):
        click_url = _ntfy_first_url(message)
        if click_url:
            headers["Click"] = click_url

    auth = None
    token = cfg.get("token")
    username = cfg.get("username")
    password = cfg.get("password")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    elif username and password:
        auth = (username, password)

    try:
        body = _ntfy_markdown(title, message)
        resp = requests.post(url, data=body.encode("utf-8"), headers=headers, auth=auth, timeout=10)
        if resp.status_code >= 300:
            logger.warning("ntfy publish failed (%s): %s", resp.status_code, resp.text[:300])
            return False
        return True
    except Exception as exc:
        logger.warning("Failed to send ntfy message: %s", exc)
        return False


def _setting_first(settings: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = settings.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _env_first(*keys: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _meshtastic_int(value: Any, default: int, *, minimum: int = 0, maximum: int = 1024) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    return max(minimum, min(maximum, out))


def _meshtastic_settings(targets: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    settings = get_verba_settings("Meshtastic Notifier") or {}
    target_map = dict(targets or {})

    bridge_url = (
        str(target_map.get("bridge_url") or "").strip()
        or _setting_first(settings, "bridge_url", "meshtastic_bridge_url", "MESHTASTIC_BRIDGE_URL")
        or _env_first("TATER_MESHTASTIC_BRIDGE_URL", "MESHTASTIC_BRIDGE_URL")
        or "http://127.0.0.1:8433"
    ).rstrip("/")

    token = (
        _setting_first(settings, "api_token", "bridge_token", "token", "MESHTASTIC_API_TOKEN")
        or _env_first("TATER_MESHTASTIC_BRIDGE_TOKEN", "MESHTASTIC_API_TOKEN", "MESHTASTIC_BRIDGE_TOKEN")
    )

    timeout = _meshtastic_int(
        target_map.get("timeout")
        or _setting_first(settings, "timeout", "timeout_seconds", "MESHTASTIC_TIMEOUT_SECONDS")
        or _env_first("TATER_MESHTASTIC_TIMEOUT_SECONDS", "MESHTASTIC_TIMEOUT_SECONDS")
        or 10,
        10,
        minimum=1,
        maximum=60,
    )
    channel = _meshtastic_int(
        target_map.get("channel")
        or _setting_first(settings, "DEFAULT_CHANNEL", "channel", "default_channel")
        or 0,
        0,
        minimum=0,
        maximum=1024,
    )
    destination = (
        str(target_map.get("destination") or target_map.get("node_id") or "").strip()
        or _setting_first(settings, "DEFAULT_DESTINATION", "destination", "default_destination")
        or "broadcast"
    )

    return {
        "bridge_url": bridge_url,
        "token": token,
        "timeout": timeout,
        "channel": channel,
        "destination": destination,
    }


def _meshtastic_body(title: Optional[str], message: str) -> str:
    body = (message or "").strip()
    head = str(title or "").strip()
    if head and body:
        return f"{head}: {body}"
    return body or head


def _send_meshtastic(title: Optional[str], message: str, targets: Optional[Dict[str, Any]]) -> bool:
    cfg = _meshtastic_settings(targets)
    body = _meshtastic_body(title, message)
    if not body:
        return False

    headers = {"Content-Type": "application/json"}
    if cfg.get("token"):
        headers["X-Tater-Token"] = cfg["token"]

    payload = {
        "text": body,
        "channel": int(cfg["channel"]),
        "destination": cfg["destination"],
    }

    try:
        resp = requests.post(
            f"{cfg['bridge_url']}/send",
            headers=headers,
            json=payload,
            timeout=int(cfg["timeout"]),
        )
        if resp.status_code >= 300:
            logger.warning("Meshtastic notify failed (%s): %s", resp.status_code, resp.text[:300])
            return False
        try:
            data = resp.json()
        except Exception:
            data = {}
        if isinstance(data, dict) and data.get("ok") is False:
            logger.warning("Meshtastic notify failed: %s", data)
            return False
        return True
    except Exception as exc:
        logger.warning("Failed to send Meshtastic notification: %s", exc)
        return False


def _wordpress_markdown_to_html(text: str) -> str:
    body = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    body = re.sub(r"\*(.+?)\*", r"<i>\1</i>", body)
    body = html.escape(body, quote=False)
    body = body.replace("&lt;b&gt;", "<b>").replace("&lt;/b&gt;", "</b>")
    body = body.replace("&lt;i&gt;", "<i>").replace("&lt;/i&gt;", "</i>")
    body = body.replace("&amp;", "&")
    body = re.sub(
        r"\[([^\]]+)\]\((https?://[^\s)]+)\)",
        lambda m: f'<a href="{_strip_utm(m.group(2))}">{m.group(1)}</a>',
        body,
    )
    body = re.sub(r"^## (.+)$", r"<h2>\1</h2>", body, flags=re.MULTILINE)
    body = re.sub(r"^# (.+)$", r"<h1>\1</h1>", body, flags=re.MULTILINE)

    lines = body.split("\n")
    html_parts: List[str] = []
    in_list = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("- ") or stripped.startswith("* "):
            if not in_list:
                html_parts.append("<ul>")
                in_list = True
            html_parts.append(f"<li>{stripped[2:]}</li>")
            continue

        if in_list:
            html_parts.append("</ul>")
            in_list = False

        if stripped:
            match = re.match(r"^(https?://\S+)$", stripped)
            if match:
                clean = _strip_utm(match.group(1))
                html_parts.append(f'<p><a href="{clean}">{clean}</a></p>')
            else:
                html_parts.append(f"<p>{stripped}</p>")

    if in_list:
        html_parts.append("</ul>")

    return "\n\n".join(html_parts) + "\n"


def _wordpress_settings(targets: Optional[Dict[str, Any]]) -> Dict[str, str]:
    settings = get_verba_settings("WordPress Poster") or {}
    target_map = dict(targets or {})

    site_url = (
        target_map.get("wordpress_site_url")
        or target_map.get("site_url")
        or settings.get("wordpress_site_url")
        or ""
    )
    username = (
        target_map.get("wordpress_username")
        or target_map.get("username")
        or settings.get("wordpress_username")
        or ""
    )
    password = (
        target_map.get("wordpress_app_password")
        or target_map.get("app_password")
        or target_map.get("password")
        or settings.get("wordpress_app_password")
        or ""
    )
    post_status = (
        target_map.get("post_status")
        or settings.get("post_status")
        or "draft"
    )
    category_id = (
        target_map.get("category_id")
        or settings.get("category_id")
        or ""
    )

    return {
        "site_url": str(site_url or "").strip().rstrip("/"),
        "username": str(username or "").strip(),
        "password": str(password or "").strip(),
        "post_status": str(post_status or "draft").strip(),
        "category_id": str(category_id or "").strip(),
    }


def _send_wordpress(title: Optional[str], message: str, targets: Optional[Dict[str, Any]]) -> bool:
    cfg = _wordpress_settings(targets)
    site_url = cfg["site_url"]
    username = cfg["username"]
    password = cfg["password"]
    post_status = cfg["post_status"]
    category_id = cfg["category_id"]

    if not site_url or not username or not password:
        logger.info("[WordPress] Missing required settings.")
        return False

    api_url = f"{site_url}/wp-json/wp/v2/posts"
    auth = base64.b64encode(f"{username}:{password}".encode()).decode()

    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json",
    }

    payload: Dict[str, Any] = {
        "title": title or "Notification",
        "content": _wordpress_markdown_to_html(message),
        "status": post_status,
    }
    if category_id.isdigit():
        payload["categories"] = [int(category_id)]

    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=10)
        if response.status_code == 201:
            return True
        logger.warning("[WordPress] Failed: %s %s", response.status_code, response.text[:200])
        return False
    except Exception as exc:
        logger.warning("[WordPress] Error: %s", exc)
        return False


def dispatch_notification_sync(
    platform: str,
    title: Optional[str],
    content: str,
    targets: Optional[Dict[str, Any]] = None,
    origin: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    attachments: Optional[List[Dict[str, Any]]] = None,
) -> str:
    dest = normalize_platform(platform)
    if dest not in ALWAYS_ON_NOTIFIERS:
        return "Cannot queue: missing destination platform"

    if dest in ("discord", "irc", "matrix", "telegram", "macos", "little_spud"):
        return _enqueue_queue_notification(dest, title, content, targets, origin, meta, attachments=attachments)

    if dest == "webui":
        return _dispatch_webui(title, content, origin, meta, attachments)

    if dest == "display":
        return _dispatch_display(title, content, targets, origin, meta, attachments)

    if dest == "homeassistant":
        return _dispatch_homeassistant(title, content, targets, origin, meta)

    if dest == "ntfy":
        ok = _send_ntfy(title, content, targets)
        if ok:
            return "Queued notification for ntfy"
        return "Cannot queue: missing ntfy topic or send failed"

    if dest == "meshtastic":
        resolved, err = resolve_targets("meshtastic", targets, origin, load_default_targets("meshtastic", redis_client))
        if err:
            return err
        ok = _send_meshtastic(title, content, resolved)
        if ok:
            return "Queued notification for meshtastic"
        return "Cannot queue: meshtastic bridge send failed"

    if dest == "wordpress":
        ok = _send_wordpress(title, content, targets)
        if ok:
            return "Queued notification for wordpress"
        return "Cannot queue: missing wordpress settings or send failed"

    return "Cannot queue: missing destination platform"


async def dispatch_notification(
    platform: str,
    title: Optional[str],
    content: str,
    targets: Optional[Dict[str, Any]] = None,
    origin: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
    attachments: Optional[List[Dict[str, Any]]] = None,
) -> str:
    dest = normalize_platform(platform)
    if dest in {"ntfy", "wordpress"}:
        return await run_background(
            dispatch_notification_sync,
            platform=platform,
            title=title,
            content=content,
            targets=targets,
            origin=origin,
            meta=meta,
            attachments=attachments,
        )
    return dispatch_notification_sync(
        platform=platform,
        title=title,
        content=content,
        targets=targets,
        origin=origin,
        meta=meta,
        attachments=attachments,
    )
