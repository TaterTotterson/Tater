# plugins/get_notifications.py
import logging
import json as _json
import httpx  # async HTTP client to avoid self-call deadlocks on HA platform
from plugin_base import ToolPlugin

logger = logging.getLogger("get_notifications")
logger.setLevel(logging.INFO)


class GetNotificationsPlugin(ToolPlugin):
    name = "get_notifications"
    usage = (
        "{\n"
        '  "function": "get_notifications",\n'
        '  "arguments": {}\n'
        "}\n"
    )
    description = (
        "Fetches notifications from the Home Assistant bridge and summarizes them. "
        "Call this when the user asks for notifications or what's new."
    )
    pretty_name = "Get Notifications"
    settings_category = "Notifications"
    platforms = ["webui", "homeassistant"]

    waiting_prompt_template = (
        "Let {mention} know you’re checking for notifications now. "
        "Keep it short and friendly. No emojis."
    )

    # ----------------------------
    # Helpers
    # ----------------------------
    @staticmethod
    def _platform_base_url() -> str:
        # Hard-coded to same-host bridge
        return "http://localhost:8787"

    @classmethod
    async def _pull_notifications(cls):
        """
        Async fetch so the HA platform can serve this nested request
        without blocking its own event loop.
        """
        base = cls._platform_base_url()
        url = f"{base}/tater-ha/v1/notifications"
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.get(url)
                r.raise_for_status()
                ctype = (r.headers.get("content-type") or "").lower()
                data = r.json() if "application/json" in ctype or (r.text or "").strip().startswith("{") else {}
                return data.get("notifications", []) if isinstance(data, dict) else []
        except httpx.TimeoutException:
            logger.error("[get_notifications] timeout talking to HA bridge")
            return None  # signal error to caller
        except Exception as e:
            logger.error(f"[get_notifications] fetch failed: {e}")
            return None  # signal error

    @staticmethod
    async def _llm_summary(notifs, llm_client):
        trimmed = [
            {
                "title": (n.get("title") or "").strip(),
                "body": (n.get("body") or "").strip(),
                "level": (n.get("level") or "info"),
                "source": (n.get("source") or ""),
                "ts": n.get("ts", 0),
            }
            for n in notifs
        ]

        system = (
            "You summarize home notifications for spoken output.\n"
            "1) Start with: 'You have N notifications.' (or 'You have no notifications.' if N=0)\n"
            "2) If multiple notifications are similar (e.g., door visitors), merge them into one roll-up sentence "
            "   (e.g., '2 people were at your door: a UPS driver and your wife').\n"
            "3) Then provide a concise list of each item (or merged group).\n"
            "Keep it short, natural, and clear. No emojis. No code blocks. No technical IDs."
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
        return await self._handle(args, llm_client, caller="webui")

    async def handle_homeassistant(self, args, llm_client):
        return await self._handle(args, llm_client, caller="homeassistant")

    # ----------------------------
    # Core handler
    # ----------------------------
    async def _handle(self, _args, llm_client, caller: str):
        notifs = await self._pull_notifications()  # <- async fetch (prevents deadlock)
        if notifs is None:
            return "I couldn’t reach the notifications service."

        if len(notifs) == 0:
            return "You have no notifications."

        summary = await self._llm_summary(notifs, llm_client)
        if not summary:
            # Fallback if LLM unavailable
            lines = [f"You have {len(notifs)} notification{'s' if len(notifs) != 1 else ''}."]
            # Simple heuristic roll-up fallback
            doorish = [
                n for n in notifs
                if "door" in ((n.get("title", "") + " " + n.get("body", "")).lower())
            ]
            if len(doorish) >= 2:
                lines.append("Multiple people were at your door recently.")
            # List each
            for i, n in enumerate(notifs, 1):
                t = (n.get("title") or "").strip()
                b = (n.get("body") or "").strip()
                if t and b:
                    lines.append(f"{i}. {t} — {b}")
                elif t:
                    lines.append(f"{i}. {t}")
                elif b:
                    lines.append(f"{i}. {b}")
                else:
                    lines.append(f"{i}. (no details)")
            return "\n".join(lines)

        return summary


plugin = GetNotificationsPlugin()