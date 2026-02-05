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

logger = logging.getLogger("notify_discord")
logger.setLevel(logging.INFO)


class DiscordNotifier(ToolPlugin):
    name = "notify_discord"
    pretty_name = "Discord Notifier"
    description = "Queue notifications for Discord delivery."
    notifier = True
    platforms = []  # internal; not exposed as a direct tool

    settings_category = "Discord Notifier"
    required_settings = {
        "DEFAULT_CHANNEL_ID": {
            "label": "Default Channel ID",
            "type": "string",
            "default": "",
            "description": "Fallback Discord channel ID when no target is provided",
        },
    }

    def _extract_args(self, args: Dict[str, Any]):
        args = args or {}
        title = args.get("title")
        message = args.get("message") or args.get("content")
        targets = dict(args.get("targets") or {})
        attachments = args.get("attachments") if isinstance(args.get("attachments"), list) else []

        for key in ("channel_id", "channel", "guild_id"):
            if args.get(key) and key not in targets:
                targets[key] = args.get(key)

        meta = {
            "priority": args.get("priority"),
            "tags": args.get("tags"),
            "ttl_sec": args.get("ttl_sec"),
        }
        origin = args.get("origin")
        return title, message, targets, origin, meta, attachments

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

    def _enqueue(self, title, message, targets, origin, meta, attachments=None):
        attachments = attachments if isinstance(attachments, list) else []
        message = (message or "").strip()
        if not message and not attachments:
            return "Cannot queue: missing message"
        if not message and attachments:
            message = "Attachment"

        defaults = load_default_targets("discord", redis_client)
        resolved, err = resolve_targets("discord", targets, origin, defaults)
        if err:
            return err

        item = build_queue_item("discord", title, message, resolved, origin, meta)
        if attachments:
            store_queue_attachments(redis_client, item.get("id"), attachments)
        key = queue_key("discord")
        if not key:
            return "Cannot queue: missing destination queue"

        redis_client.rpush(key, json.dumps(item))
        return "Queued notification for discord"


plugin = DiscordNotifier()
