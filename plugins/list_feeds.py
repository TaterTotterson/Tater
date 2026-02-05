# plugins/list_feeds.py
import os
import logging
import redis
import time
from dotenv import load_dotenv
from plugin_base import ToolPlugin
from rss_store import get_all_feeds

load_dotenv()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)


class ListFeedsPlugin(ToolPlugin):
    name = "list_feeds"
    plugin_name = "List Feeds"
    version = "1.0.0"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "list_feeds",\n'
        '  "arguments": {}\n'
        "}\n"
    )
    description = "Lists the RSS feeds currently being watched."
    plugin_dec = "Show the RSS feeds currently being watched."
    pretty_name = "Getting RSS Feeds"
    waiting_prompt_template = (
        "Write a friendly, casual message telling {mention} you’re grabbing the current watched feeds now! Only output that message."
    )
    platforms = ["discord", "webui", "irc", "matrix"]

    async def _list_feeds(self, username: str | None = None) -> str:
        feeds = get_all_feeds(redis_client) or {}
        # sort for stable output
        items = sorted(feeds.items(), key=lambda kv: kv[0].lower())

        prefix = f"{username}: " if username else ""
        if not items:
            return f"{prefix}No RSS feeds are currently being watched."

        def _fmt_ts(ts):
            try:
                return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ts)))
            except Exception:
                return str(ts)

        lines = []
        for name, cfg in items:
            status = "enabled" if cfg.get("enabled", True) else "disabled"
            last = _fmt_ts(cfg.get("last_ts") or 0)
            lines.append(f"{name} | {status} | last update: {last}")

            platforms = cfg.get("platforms") or {}
            if platforms:
                parts = []
                for p, pcfg in sorted(platforms.items(), key=lambda kv: kv[0]):
                    pstat = "on" if pcfg.get("enabled", True) else "off"
                    targets = pcfg.get("targets") or {}
                    if targets:
                        tgt = ", ".join(f"{k}={v}" for k, v in targets.items())
                        parts.append(f"{p}:{pstat} ({tgt})")
                    else:
                        parts.append(f"{p}:{pstat}")
                lines.append(f"Overrides: {', '.join(parts)}")

        header = f"{username}: Currently watched feeds:" if username else "Currently watched feeds:"
        return "\n".join([header] + lines)

    # ---------- Discord ----------
    async def handle_discord(self, message, args, llm_client):
        return await self._list_feeds()

    # ---------- WebUI ----------
    async def handle_webui(self, args, llm_client):
        return await self._list_feeds()

    # ---------- IRC ----------
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self._list_feeds(username=user)

    # ---------- Matrix ----------
    async def handle_matrix(self, client, room, sender, body, args, ll_client=None, **kwargs):
        # Keep it simple: same content style as Discord/WebUI; no username prefix needed.
        # (If you prefer sender-prefixed lines, pass username=sender instead.)
        return await self._list_feeds()


plugin = ListFeedsPlugin()
