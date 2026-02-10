import logging
import re
from typing import Any, Dict, List, Optional

from plugin_base import ToolPlugin
from helpers import redis_client
from notify import dispatch_notification
from notify.queue import ALLOWED_PLATFORMS, normalize_platform

logger = logging.getLogger("send_message")
logger.setLevel(logging.INFO)


class SendMessagePlugin(ToolPlugin):
    name = "send_message"
    required_args = []
    optional_args = [
        "message",
        "title",
        "platform",
        "targets",
        "attachments",
        "priority",
        "tags",
        "ttl_sec",
        "origin",
        "channel_id",
        "channel",
        "guild_id",
        "room_id",
        "room_alias",
        "device_service",
        "persistent",
        "api_notification",
        "chat_id",
    ]
    common_needs = [
        "message to send",
        "destination platform (optional; defaults to current platform)",
        "destination room/channel (optional; names like #tater are valid)",
    ]
    usage = '{"function":"send_message","arguments":{"message":"Optional body text when sending attachments","title":"Optional short title","platform":"discord|irc|matrix|homeassistant|ntfy|telegram (optional; defaults to origin)","targets":{"channel":"discord/irc channel name, optional (#tater works; numeric IDs are not required)","channel_id":"discord only, optional numeric ID","guild_id":"discord only, optional","room_id":"matrix room id or alias, optional (!id:server or #alias:server)","room_alias":"matrix alias, optional (#alias:server)","device_service":"homeassistant only, optional","persistent":"homeassistant only, optional (true/false)","api_notification":"homeassistant only, optional (true/false)","chat_id":"telegram chat id or @username, optional"},"attachments":[{"type":"image|audio|video|file","name":"optional","mimetype":"optional","bytes":"optional-bytes","blob_key":"optional"}],"priority":"normal|high","tags":["optional","strings"],"ttl_sec":0}}'
    description = (
        "Queue a message via the notifier system. If no destination is provided, "
        "it defaults to the origin platform (Discord/IRC/Matrix/Home Assistant/Telegram). "
        "Room/channel names like #tater are accepted for routing. "
        "When used from Discord, attached files are forwarded automatically."
    )
    pretty_name = "Send Message"

    platforms = ["discord", "irc", "matrix", "homeassistant", "telegram", "webui"]
    settings_category = "Send Message"
    required_settings = {
        "ENABLE_HA_API_NOTIFICATION": {
            "label": "Enable Home Assistant API notifications",
            "type": "checkbox",
            "default": True,
            "description": "Default for Home Assistant sends when api_notification is not set in arguments.",
        },
    }

    @staticmethod
    def _boolish(value: Any, default: bool = False) -> bool:
        if value is None:
            return bool(default)
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on", "enabled"}:
            return True
        if text in {"0", "false", "no", "n", "off", "disabled"}:
            return False
        return bool(default)

    def _load_settings(self) -> Dict[str, str]:
        return (
            redis_client.hgetall(f"plugin_settings:{self.settings_category}")
            or redis_client.hgetall(f"plugin_settings: {self.settings_category}")
            or {}
        )

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
    def _extract_target_hint(raw: Any) -> str:
        text = str(raw or "").strip()
        if not text:
            return ""
        for pattern in (r"![^\s]+", r"#[A-Za-z0-9][A-Za-z0-9._:-]*", r"@[A-Za-z0-9_]+"):
            m = re.search(pattern, text)
            if m:
                return m.group(0)
        text = re.sub(r"^(?:room|channel|chat)\s+", "", text, flags=re.IGNORECASE)
        text = re.sub(
            r"\s+(?:in|on)\s+(?:discord|irc|matrix|telegram|home\s*assistant|homeassistant)\b.*$",
            "",
            text,
            flags=re.IGNORECASE,
        )
        return text.strip(" .")

    @staticmethod
    def _coerce_targets(payload: Any) -> Dict[str, Any]:
        if isinstance(payload, dict):
            return dict(payload)
        # LLMs sometimes send a raw channel/room string instead of a targets map.
        if isinstance(payload, str):
            hint = SendMessagePlugin._extract_target_hint(payload)
            if hint:
                return {"channel": hint}
        return {}

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

        targets = self._coerce_targets(args.get("targets"))
        for key in (
            "channel_id",
            "channel",
            "guild_id",
            "room_id",
            "room_alias",
            "device_service",
            "persistent",
            "api_notification",
            "chat_id",
        ):
            if key in args and key not in targets:
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
        elif dest == "homeassistant":
            if "api_notification" not in targets:
                settings = self._load_settings()
                targets["api_notification"] = self._boolish(settings.get("ENABLE_HA_API_NOTIFICATION"), True)

        result = await dispatch_notification(
            platform=dest,
            title=title,
            content=message,
            targets=targets,
            origin=origin,
            meta=meta,
            attachments=attachments,
        )
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

    async def handle_telegram(self, update, args, llm_client):
        return await self._dispatch(args)


plugin = SendMessagePlugin()
