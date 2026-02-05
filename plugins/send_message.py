import asyncio
import inspect
import logging
from typing import Any, Dict, List, Optional

from plugin_base import ToolPlugin
from notify_queue import ALLOWED_PLATFORMS, normalize_platform

logger = logging.getLogger("send_message")
logger.setLevel(logging.INFO)


class SendMessagePlugin(ToolPlugin):
    name = "send_message"
    usage = (
        "{\n"
        "  \"function\": \"send_message\",\n"
        "  \"arguments\": {\n"
        "    \"message\": \"Optional body text when sending attachments\",\n"
        "    \"title\": \"Optional short title\",\n"
        "    \"platform\": \"discord|irc|matrix|homeassistant|ntfy (optional; defaults to origin)\",\n"
        "    \"targets\": {\n"
        "      \"channel_id\": \"discord only, optional\",\n"
        "      \"channel\": \"discord/irc, optional (#tater)\",\n"
        "      \"guild_id\": \"discord only, optional\",\n"
        "      \"room_id\": \"matrix room id or alias, optional (!id:server or #alias:server)\",\n"
        "      \"room_alias\": \"matrix alias, optional (#alias:server)\",\n"
        "      \"device_service\": \"homeassistant only, optional\"\n"
        "    },\n"
        "    \"attachments\": [{\"type\":\"image|audio|video|file\",\"name\":\"optional\",\"mimetype\":\"optional\",\"bytes\":\"optional-bytes\",\"blob_key\":\"optional\"}],\n"
        "    \"priority\": \"normal|high\",\n"
        "    \"tags\": [\"optional\", \"strings\"],\n"
        "    \"ttl_sec\": 0\n"
        "  }\n"
        "}\n"
    )
    description = (
        "Queue a message via the notifier system. If no destination is provided, "
        "it defaults to the origin platform (Discord/IRC/Matrix/Home Assistant). "
        "When used from Discord, attached files are forwarded automatically."
    )
    pretty_name = "Send Message"

    platforms = ["discord", "irc", "matrix", "homeassistant", "webui"]

    @staticmethod
    def _normalize_matrix_room_ref(room_ref: Any) -> str:
        ref = str(room_ref or "").strip()
        if not ref:
            return ""
        if ref.startswith("!") or ref.startswith("#"):
            return ref
        if ":" in ref:
            return f"#{ref}"
        return ref

    @staticmethod
    def _clean_attachment_payload(payload: Any) -> List[Dict[str, Any]]:
        if not isinstance(payload, list):
            return []
        out: List[Dict[str, Any]] = []
        for item in payload:
            if isinstance(item, dict):
                out.append(dict(item))
        return out

    @staticmethod
    def _notifier_supports_attachments(notifier: Any) -> bool:
        notify_fn = getattr(notifier, "notify", None)
        if not callable(notify_fn):
            return False
        try:
            sig = inspect.signature(notify_fn)
        except Exception:
            return False
        if "attachments" in sig.parameters:
            return True
        return any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())

    async def _discord_attachments(self, message) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for attachment in list(getattr(message, "attachments", []) or []):
            try:
                binary = await attachment.read()
                if not binary:
                    continue

                content_type = str(getattr(attachment, "content_type", "") or "").strip().lower()
                kind = "file"
                if content_type.startswith("image/"):
                    kind = "image"
                elif content_type.startswith("audio/"):
                    kind = "audio"
                elif content_type.startswith("video/"):
                    kind = "video"

                items.append({
                    "type": kind,
                    "name": str(getattr(attachment, "filename", "") or "attachment.bin"),
                    "mimetype": content_type or "application/octet-stream",
                    "bytes": binary,
                    "size": len(binary),
                })
            except Exception as e:
                logger.warning(f"[send_message] Failed to read Discord attachment: {e}")
        return items

    def _extract_args(self, args: Dict[str, Any]):
        args = args or {}
        title = args.get("title")
        message = args.get("message") or args.get("content")
        platform = args.get("platform")
        attachments = self._clean_attachment_payload(args.get("attachments"))

        targets = dict(args.get("targets") or {})
        for key in ("channel_id", "channel", "guild_id", "room_id", "room_alias", "device_service"):
            if args.get(key) and key not in targets:
                targets[key] = args.get(key)

        meta = {
            "priority": args.get("priority"),
            "tags": args.get("tags"),
            "ttl_sec": args.get("ttl_sec"),
        }
        origin = args.get("origin")
        return title, message, platform, targets, origin, meta, attachments

    async def _dispatch(self, args: Dict[str, Any], attachments_override: Optional[List[Dict[str, Any]]] = None):
        title, message, platform, targets, origin, meta, attachments = self._extract_args(args)
        if attachments_override:
            attachments.extend(attachments_override)

        message = (message or "").strip()
        if not message and not attachments:
            return "Cannot queue: missing message"
        if not message and attachments:
            message = "Attachment"

        dest = normalize_platform(platform)
        if not dest:
            if isinstance(origin, dict):
                origin_platform = normalize_platform(origin.get("platform"))
                if origin_platform in ALLOWED_PLATFORMS:
                    dest = origin_platform

        if dest not in ALLOWED_PLATFORMS:
            return "Cannot queue: missing destination platform"

        if dest == "matrix":
            if not targets.get("room_id"):
                alias = targets.get("room_alias") or targets.get("channel")
                if alias:
                    targets["room_id"] = alias
            if targets.get("room_id"):
                targets["room_id"] = self._normalize_matrix_room_ref(targets.get("room_id"))

        notifier_name = f"notify_{dest}"

        try:
            import plugin_registry as pr
            from plugin_settings import get_plugin_enabled
        except Exception as e:
            logger.error(f"Failed to load plugin registry: {e}")
            return "Cannot queue: notifier registry unavailable"

        plugins = pr.get_registry_snapshot()
        notifier = plugins.get(notifier_name)
        if not notifier or not getattr(notifier, "notifier", False):
            return f"Cannot queue: no notifier for {dest}"

        if not get_plugin_enabled(notifier_name):
            return f"Cannot queue: notifier for {dest} is disabled"

        kwargs = {
            "title": title,
            "content": message,
            "targets": targets,
            "origin": origin,
            "meta": meta,
        }
        if attachments and self._notifier_supports_attachments(notifier):
            kwargs["attachments"] = attachments

        result = notifier.notify(**kwargs)
        if asyncio.iscoroutine(result):
            result = await result
        return result

    async def handle_discord(self, message, args, llm_client):
        auto_attachments: List[Dict[str, Any]] = []
        parsed = dict(args or {})
        if not self._clean_attachment_payload(parsed.get("attachments")):
            auto_attachments = await self._discord_attachments(message)
        return await self._dispatch(parsed, attachments_override=auto_attachments)

    async def handle_webui(self, args, llm_client):
        return await self._dispatch(args)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self._dispatch(args)

    async def handle_homeassistant(self, args, llm_client):
        return await self._dispatch(args)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        return await self._dispatch(args)


plugin = SendMessagePlugin()
