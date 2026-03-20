import asyncio
import base64
import html
import json
import logging
import re
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
    "macos",
    "wordpress",
)

_ATTACHMENT_PLATFORMS = {"discord", "matrix", "telegram", "macos"}

_URL_PATTERN = re.compile(r"https?://\S+")
_BARE_URL_PATTERN = re.compile(r"(?<!\()(?<!\])\bhttps?://\S+\b")


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
    return f"Queued notification for {platform}"


def _ha_settings() -> Tuple[str, str]:
    settings = redis_client.hgetall("homeassistant_settings") or {}
    base = (settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
    token = (settings.get("HA_TOKEN") or "").strip()
    return base, token


def _ha_call_service(domain: str, service: str, data: Dict[str, Any]) -> None:
    base, token = _ha_settings()
    if not token:
        raise RuntimeError("HA_TOKEN missing in Home Assistant Settings.")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    url = f"{base}/api/services/{domain}/{service}"
    resp = requests.post(url, headers=headers, json=data, timeout=10)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")


def _ha_bridge_port() -> int:
    try:
        raw_port = redis_client.hget("homeassistant_portal_settings", "bind_port")
        return int(raw_port) if raw_port is not None else 8787
    except Exception:
        return 8787


def _post_ha_notification(
    title: Optional[str],
    message: str,
    source: str,
    level: str,
    data: Optional[Dict[str, Any]] = None,
) -> None:
    port = _ha_bridge_port()
    url = f"http://127.0.0.1:{port}/tater-ha/v1/notifications/add"
    payload = {
        "source": (source or "notify").strip(),
        "title": (title or "Notification").strip(),
        "type": "notify",
        "message": (message or "").strip(),
        "entity_id": "",
        "ha_time": "",
        "level": (level or "info").strip(),
        "data": data or {},
    }
    resp = requests.post(url, json=payload, timeout=5)
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

    source = (origin or {}).get("platform") or "notify"
    priority = (meta or {}).get("priority") or "normal"
    level = "warn" if str(priority).lower() == "high" else "info"

    api_notification_enabled = True
    if isinstance(resolved, dict) and "api_notification" in resolved:
        api_notification_enabled = _boolish(resolved.get("api_notification"), True)

    if api_notification_enabled:
        try:
            _post_ha_notification(
                title,
                message,
                source=source,
                level=level,
                data={"origin": origin, "targets": resolved},
            )
        except Exception as exc:
            logger.warning("[notify] HA notifications add failed: %s", exc)

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

    if dest in ("discord", "irc", "matrix", "telegram", "macos"):
        return _enqueue_queue_notification(dest, title, content, targets, origin, meta, attachments=attachments)

    if dest == "homeassistant":
        return _dispatch_homeassistant(title, content, targets, origin, meta)

    if dest == "ntfy":
        ok = _send_ntfy(title, content, targets)
        if ok:
            return "Queued notification for ntfy"
        return "Cannot queue: missing ntfy topic or send failed"

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
        return await asyncio.to_thread(
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
