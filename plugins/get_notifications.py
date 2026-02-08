import logging
import json as _json
from typing import Any, Dict, List

import httpx

from plugin_base import ToolPlugin
from helpers import redis_client

logger = logging.getLogger("get_notifications")
logger.setLevel(logging.INFO)


class GetNotificationsPlugin(ToolPlugin):
    name = "get_notifications"
    plugin_name = "Get Notifications"
    version = "1.1.0"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "get_notifications",\n'
        '  "arguments": {"limit": 8}\n'
        "}\n"
    )
    optional_args = ["limit"]
    when_to_use = (
        "Use when the user asks for notifications, alerts, or what happened recently in Home Assistant."
    )
    description = (
        "Fetches queued notifications from the Home Assistant bridge. "
        "Call this when the user asks for notifications or recent alerts."
    )
    plugin_dec = "Fetch queued notifications from the Home Assistant bridge."
    pretty_name = "Get Notifications"
    settings_category = "Notifications"
    platforms = ["webui", "homeassistant", "discord", "telegram", "matrix", "irc"]

    waiting_prompt_template = (
        "Let {mention} know you are checking for notifications now. "
        "Keep it short and friendly. No emojis. Only output that message."
    )

    # ----------------------------
    # Helpers
    # ----------------------------
    @staticmethod
    def _decode_text(value: Any) -> str:
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", "ignore")
        if value is None:
            return ""
        return str(value)

    @staticmethod
    def _coerce_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(float(str(value).strip()))
        except Exception:
            parsed = int(default)
        if parsed < minimum:
            return minimum
        if parsed > maximum:
            return maximum
        return parsed

    @staticmethod
    def _platform_base_url() -> str:
        """
        Resolve the HA bridge base URL using the configured bind_port:
          Redis hash: 'homeassistant_platform_settings' -> 'bind_port'
        Falls back to 8787 if missing/invalid.
        """
        try:
            raw_port = redis_client.hget("homeassistant_platform_settings", "bind_port")
            port = GetNotificationsPlugin._coerce_int(
                GetNotificationsPlugin._decode_text(raw_port),
                default=8787,
                minimum=1,
                maximum=65535,
            )
        except Exception:
            port = 8787
        return f"http://127.0.0.1:{port}"

    @classmethod
    async def _pull_notifications(cls) -> List[Dict[str, Any]] | None:
        base = cls._platform_base_url()
        url = f"{base}/tater-ha/v1/notifications"
        try:
            timeout = httpx.Timeout(connect=2.0, read=5.0, write=5.0, pool=5.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get(url)
                response.raise_for_status()
                content_type = (response.headers.get("content-type") or "").lower()
                if "application/json" not in content_type:
                    logger.warning("[get_notifications] non-JSON response from HA bridge")
                    return []
                payload = response.json()
                if not isinstance(payload, dict):
                    return []
                notifications = payload.get("notifications")
                if not isinstance(notifications, list):
                    return []
                return [item for item in notifications if isinstance(item, dict)]
        except httpx.TimeoutException:
            logger.error("[get_notifications] timeout talking to HA bridge")
            return None
        except httpx.HTTPStatusError as exc:
            logger.error("[get_notifications] HTTP error from HA bridge: %s", exc)
            return None
        except Exception as e:
            logger.error(f"[get_notifications] fetch failed: {e}")
            return None

    @staticmethod
    async def _llm_summary(
        notifs: List[Dict[str, Any]],
        llm_client,
        total_count: int,
        omitted_count: int,
    ) -> str:
        if llm_client is None:
            return ""

        trimmed = [
            {
                "title": (n.get("title") or "").strip(),
                "message": (n.get("message") or "").strip(),
                "level": (n.get("level") or "info"),
                "source": (n.get("source") or ""),
                "type": (n.get("type") or ""),
                "entity_id": (n.get("entity_id") or ""),
                "ha_time": (n.get("ha_time") or ""),  # include only if present
                "data": n.get("data") or {},
            }
            for n in notifs
        ]

        system = (
            "You summarize home notifications for spoken output.\n"
            "Rules:\n"
            f"1) Start with exactly: 'You have {total_count} notifications.'\n"
            "2) Merge similar items (for example, repeated doorbell visitors) into one short roll-up.\n"
            "3) Include the 'ha_time' when available. If missing, omit the time entirely.\n"
            "4) Keep it short, natural, and clear. No emojis. No code blocks. No technical IDs.\n"
            "5) If omitted_count > 0, add one short sentence saying you are showing recent items only."
        )

        user = _json.dumps(
            {
                "total_count": total_count,
                "shown_count": len(trimmed),
                "omitted_count": omitted_count,
                "notifications": trimmed,
            },
            ensure_ascii=False,
        )

        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                temperature=0.2,
                max_tokens=320,
                timeout_ms=45000,
            )
            text = (resp.get("message", {}) or {}).get("content", "").strip()
            return text or f"You have {total_count} notification{'s' if total_count != 1 else ''}."
        except Exception as e:
            logger.warning(f"[get_notifications] LLM summary failed: {e}")
            return ""

    @staticmethod
    def _fallback_summary(notifs: List[Dict[str, Any]], total_count: int, omitted_count: int) -> str:
        lines = [f"You have {total_count} notification{'s' if total_count != 1 else ''}."]
        if omitted_count > 0:
            lines.append(f"Showing the latest {len(notifs)}.")

        doorish = [
            n for n in notifs if "door" in ((n.get("title", "") + " " + n.get("message", "")).lower())
        ]
        if len(doorish) >= 2:
            lines.append("Multiple doorbell notifications recently.")

        for index, n in enumerate(notifs, 1):
            title = (n.get("title") or "").strip()
            message = (n.get("message") or "").strip()
            when = (n.get("ha_time") or "").strip()
            time_suffix = f" (at {when})" if when else ""
            if title and message:
                lines.append(f"{index}. {title} - {message}{time_suffix}")
            elif title:
                lines.append(f"{index}. {title}{time_suffix}")
            elif message:
                lines.append(f"{index}. {message}{time_suffix}")
            else:
                lines.append(f"{index}. (no details){time_suffix}")
        return "\n".join(lines)

    # ----------------------------
    # Platform handlers
    # ----------------------------
    async def handle_webui(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_discord(self, message=None, args=None, llm_client=None):
        return await self._handle(args, llm_client)

    async def handle_telegram(self, update=None, args=None, llm_client=None):
        return await self._handle(args, llm_client)

    async def handle_matrix(self, client=None, room=None, sender=None, body=None, args=None, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        return await self._handle(args, llm_client)

    async def handle_irc(self, bot=None, channel=None, user=None, raw_message=None, args=None, llm_client=None):
        return await self._handle(args, llm_client)

    # ----------------------------
    # Core handler
    # ----------------------------
    async def _handle(self, _args, llm_client):
        args = _args or {}
        limit = self._coerce_int(args.get("limit", 8), default=8, minimum=1, maximum=25)

        notifs = await self._pull_notifications()
        if notifs is None:
            return "I could not reach the notifications service."
        if len(notifs) == 0:
            return "You have no notifications."

        shown = notifs[:limit]
        omitted_count = max(0, len(notifs) - len(shown))

        summary = await self._llm_summary(shown, llm_client, total_count=len(notifs), omitted_count=omitted_count)
        if summary:
            return summary

        return self._fallback_summary(shown, total_count=len(notifs), omitted_count=omitted_count)


plugin = GetNotificationsPlugin()
