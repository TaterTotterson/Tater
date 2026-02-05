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

logger = logging.getLogger("notify_irc")
logger.setLevel(logging.INFO)


class IRCNotifier(ToolPlugin):
    name = "notify_irc"
    pretty_name = "IRC Notifier"
    description = "Queue notifications for IRC delivery."
    notifier = True
    platforms = []  # internal; not exposed as a direct tool

    settings_category = "IRC Notifier"
    required_settings = {
        "DEFAULT_CHANNEL": {
            "label": "Default IRC Channel",
            "type": "string",
            "default": "#tater",
            "description": "Fallback IRC channel when no target is provided",
        },
    }

    def _extract_args(self, args: Dict[str, Any]):
        args = args or {}
        title = args.get("title")
        message = args.get("message") or args.get("content")
        targets = dict(args.get("targets") or {})

        if args.get("channel") and "channel" not in targets:
            targets["channel"] = args.get("channel")

        meta = {
            "priority": args.get("priority"),
            "tags": args.get("tags"),
            "ttl_sec": args.get("ttl_sec"),
        }
        origin = args.get("origin")
        return title, message, targets, origin, meta

    async def notify(
        self,
        title: str,
        content: str,
        targets: Dict[str, Any] | None = None,
        origin: Dict[str, Any] | None = None,
        meta: Dict[str, Any] | None = None,
    ) -> str:
        return self._enqueue(title=title, message=content, targets=targets, origin=origin, meta=meta)

    async def handle_webui(self, args, llm_client):
        title, message, targets, origin, meta = self._extract_args(args)
        return self._enqueue(title=title, message=message, targets=targets, origin=origin, meta=meta)

    async def handle_discord(self, message, args, llm_client):
        title, message_text, targets, origin, meta = self._extract_args(args)
        return self._enqueue(title=title, message=message_text, targets=targets, origin=origin, meta=meta)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        title, message_text, targets, origin, meta = self._extract_args(args)
        return self._enqueue(title=title, message=message_text, targets=targets, origin=origin, meta=meta)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        title, message_text, targets, origin, meta = self._extract_args(args)
        return self._enqueue(title=title, message=message_text, targets=targets, origin=origin, meta=meta)

    async def handle_homeassistant(self, args, llm_client):
        title, message_text, targets, origin, meta = self._extract_args(args)
        return self._enqueue(title=title, message=message_text, targets=targets, origin=origin, meta=meta)

    def _enqueue(self, title, message, targets, origin, meta):
        message = (message or "").strip()
        if not message:
            return "Cannot queue: missing message"

        defaults = load_default_targets("irc", redis_client)
        resolved, err = resolve_targets("irc", targets, origin, defaults)
        if err:
            return err

        item = build_queue_item("irc", title, message, resolved, origin, meta)
        key = queue_key("irc")
        if not key:
            return "Cannot queue: missing destination queue"

        redis_client.rpush(key, json.dumps(item))
        return "Queued notification for irc"


plugin = IRCNotifier()
