import json
import logging
from typing import Any, Dict

from plugin_base import ToolPlugin
from helpers import redis_client
from notify_queue import (
    build_queue_item,
    load_default_targets,
    queue_key,
    resolve_targets,
)
from notify_media import store_queue_attachments

logger = logging.getLogger("notify_telegram")
logger.setLevel(logging.INFO)


class TelegramNotifierPlugin(ToolPlugin):
    name = "notify_telegram"
    plugin_name = "Telegram Notifier"
    pretty_name = "Telegram Notifier"
    version = "1.1.0"
    min_tater_version = "50"
    description = "Queue notifications for Telegram delivery."
    plugin_dec = "Send messages to Telegram via the Telegram platform queue."
    usage = ""
    platforms = []  # internal; not exposed as a direct tool
    settings_category = "Telegram Notifier"
    notifier = True  # Used by RSS manager to detect notifiers

    required_settings = {
        "DEFAULT_CHAT_ID": {
            "label": "Default Chat ID",
            "type": "string",
            "default": "",
            "description": "Fallback Telegram chat ID when no target is provided.",
        }
    }

    def _extract_args(self, args: Dict[str, Any]):
        args = args or {}
        title = args.get("title")
        message = args.get("message") or args.get("content")
        targets = dict(args.get("targets") or {})
        attachments = args.get("attachments") if isinstance(args.get("attachments"), list) else []

        for key in ("chat_id", "channel_id", "channel"):
            if args.get(key) and key not in targets:
                targets[key] = args.get(key)

        if not targets.get("chat_id"):
            if targets.get("channel_id"):
                targets["chat_id"] = targets.get("channel_id")
            elif targets.get("channel"):
                targets["chat_id"] = targets.get("channel")

        meta = {
            "priority": args.get("priority"),
            "tags": args.get("tags"),
            "ttl_sec": args.get("ttl_sec"),
        }
        origin = args.get("origin")
        return title, message, targets, origin, meta, attachments

    def _legacy_defaults(self) -> Dict[str, str]:
        settings = redis_client.hgetall("plugin_settings:Telegram Notifier") or {}
        chat_id = str(settings.get("telegram_chat_id") or "").strip()
        if chat_id:
            return {"chat_id": chat_id}
        return {}

    async def notify(
        self,
        title: str,
        content: str,
        targets: Dict[str, Any] | None = None,
        origin: Dict[str, Any] | None = None,
        meta: Dict[str, Any] | None = None,
        attachments: list[Dict[str, Any]] | None = None,
    ) -> str:
        return self._enqueue(
            title=title,
            message=content,
            targets=targets,
            origin=origin,
            meta=meta,
            attachments=attachments,
        )

    async def handle_webui(self, args, llm_client):
        title, message, targets, origin, meta, attachments = self._extract_args(args)
        return self._enqueue(
            title=title,
            message=message,
            targets=targets,
            origin=origin,
            meta=meta,
            attachments=attachments,
        )

    async def handle_discord(self, message, args, llm_client):
        title, message_text, targets, origin, meta, attachments = self._extract_args(args)
        return self._enqueue(
            title=title,
            message=message_text,
            targets=targets,
            origin=origin,
            meta=meta,
            attachments=attachments,
        )

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        title, message_text, targets, origin, meta, attachments = self._extract_args(args)
        return self._enqueue(
            title=title,
            message=message_text,
            targets=targets,
            origin=origin,
            meta=meta,
            attachments=attachments,
        )

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        title, message_text, targets, origin, meta, attachments = self._extract_args(args)
        return self._enqueue(
            title=title,
            message=message_text,
            targets=targets,
            origin=origin,
            meta=meta,
            attachments=attachments,
        )

    async def handle_homeassistant(self, args, llm_client):
        title, message_text, targets, origin, meta, attachments = self._extract_args(args)
        return self._enqueue(
            title=title,
            message=message_text,
            targets=targets,
            origin=origin,
            meta=meta,
            attachments=attachments,
        )

    async def handle_telegram(self, update, args, llm_client):
        title, message_text, targets, origin, meta, attachments = self._extract_args(args)
        return self._enqueue(
            title=title,
            message=message_text,
            targets=targets,
            origin=origin,
            meta=meta,
            attachments=attachments,
        )

    def _enqueue(self, title, message, targets, origin, meta, attachments=None):
        attachments = attachments if isinstance(attachments, list) else []
        message = (message or "").strip()
        if not message and not attachments:
            return "Cannot queue: missing message"
        if not message and attachments:
            message = "Attachment"

        defaults = load_default_targets("telegram", redis_client)
        if not defaults.get("chat_id"):
            defaults = {**defaults, **self._legacy_defaults()}
        resolved, err = resolve_targets("telegram", targets, origin, defaults)
        if err:
            return err

        item = build_queue_item("telegram", title, message, resolved, origin, meta)
        if attachments:
            store_queue_attachments(redis_client, item.get("id"), attachments)
        key = queue_key("telegram")
        if not key:
            return "Cannot queue: missing destination queue"

        redis_client.rpush(key, json.dumps(item))
        return "Queued notification for telegram"


plugin = TelegramNotifierPlugin()
