# plugins/get_notifications.py
import logging
import json as _json
import httpx
from plugin_base import ToolPlugin
from helpers import redis_client

logger = logging.getLogger("get_notifications")
logger.setLevel(logging.INFO)


class GetNotificationsPlugin(ToolPlugin):
    name = "get_notifications"
    plugin_name = "Get Notifications"
    version = "1.0.0"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "get_notifications",\n'
        '  "arguments": {}\n'
        "}\n"
    )
    description = (
        "Fetches queued notifications from the Home Assistant bridge. "
        "Call this when the user asks for notifications or recent alerts."
    )
    plugin_dec = "Fetch queued notifications from the Home Assistant bridge."
    pretty_name = "Get Notifications"
    settings_category = "Notifications"
    platforms = ["webui", "homeassistant"]

    waiting_prompt_template = (
        "Let {mention} know you’re checking for notifications now. "
        "Keep it short and friendly. No emojis. Only output that message."
    )

    # ----------------------------
    # Helpers
    # ----------------------------
    @staticmethod
    def _platform_base_url() -> str:
        """
        Resolve the HA bridge base URL using the configured bind_port:
          Redis hash: 'homeassistant_platform_settings' -> 'bind_port'
        Falls back to 8787 if missing/invalid.
        """
        try:
            raw_port = redis_client.hget("homeassistant_platform_settings", "bind_port")
            port = int(raw_port) if raw_port is not None else 8787
        except Exception:
            port = 8787
        return f"http://127.0.0.1:{port}"

    @classmethod
    async def _pull_notifications(cls):
        base = cls._platform_base_url()
        url = f"{base}/tater-ha/v1/notifications"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get(url)
                r.raise_for_status()
                data = r.json() if r.headers.get("content-type", "").lower().startswith("application/json") else {}
                return data.get("notifications", []) if isinstance(data, dict) else []
        except httpx.TimeoutException:
            logger.error("[get_notifications] timeout talking to HA bridge")
            return None
        except Exception as e:
            logger.error(f"[get_notifications] fetch failed: {e}")
            return None

    @staticmethod
    async def _llm_summary(notifs, llm_client):
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
            "1) Start with: 'You have N notifications.' (or 'You have no notifications.' if N=0)\n"
            "2) Merge similar items (e.g., doorbell visitors) into one short roll-up.\n"
            "3) Include the 'ha_time' when available. If missing, omit the time entirely.\n"
            "4) Keep it short, natural, and clear. No emojis. No code blocks. No technical IDs."
        )

        user = _json.dumps({"count": len(trimmed), "notifications": trimmed}, ensure_ascii=False)

        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ]
            )
            text = (resp.get("message", {}) or {}).get("content", "").strip()
            return text or f"You have {len(trimmed)} notification{'s' if len(trimmed) != 1 else ''}."
        except Exception as e:
            logger.warning(f"[get_notifications] LLM summary failed: {e}")
            return ""

    # ----------------------------
    # Platform handlers
    # ----------------------------
    async def handle_webui(self, args, llm_client):
        return await self._handle(args, llm_client)

    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args, llm_client)

    # ----------------------------
    # Core handler
    # ----------------------------
    async def _handle(self, _args, llm_client):
        notifs = await self._pull_notifications()
        if notifs is None:
            return "I couldn’t reach the notifications service."
        if len(notifs) == 0:
            return "You have no notifications."

        summary = await self._llm_summary(notifs, llm_client)
        if not summary:
            lines = [f"You have {len(notifs)} notification{'s' if len(notifs) != 1 else ''}."]
            doorish = [
                n for n in notifs
                if "door" in ((n.get("title", "") + " " + n.get("message", "")).lower())
            ]
            if len(doorish) >= 2:
                lines.append("Multiple doorbell notifications recently.")

            for i, n in enumerate(notifs, 1):
                t = (n.get("title") or "").strip()
                m = (n.get("message") or "").strip()
                when = (n.get("ha_time") or "").strip()
                time_suffix = f" (at {when})" if when else ""
                if t and m:
                    lines.append(f"{i}. {t} — {m}{time_suffix}")
                elif t:
                    lines.append(f"{i}. {t}{time_suffix}")
                elif m:
                    lines.append(f"{i}. {m}{time_suffix}")
                else:
                    lines.append(f"{i}. (no details){time_suffix}")
            return "\n".join(lines)

        return summary


plugin = GetNotificationsPlugin()
