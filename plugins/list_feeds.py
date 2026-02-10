import logging
import time

from helpers import redis_client
from plugin_base import ToolPlugin
from rss_store import get_all_feeds

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ListFeedsPlugin(ToolPlugin):
    name = "rss_list"
    plugin_name = "RSS List"
    version = "1.1.0"
    min_tater_version = "50"
    when_to_use = "Use when the user asks to list, show, or review watched RSS feeds."
    usage = '{"function":"list_feeds","arguments":{}}'
    description = "Lists the RSS feeds currently being watched."
    plugin_dec = "Show the RSS feeds currently being watched."
    pretty_name = "Listing RSS Feeds"
    waiting_prompt_template = (
        "Write a friendly, casual message telling {mention} you are grabbing the current watched feeds now. Only output that message."
    )
    platforms = ["discord", "webui", "irc", "matrix", "telegram"]

    @staticmethod
    def _boolish(value, default: bool = True) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "on", "enabled"}:
            return True
        if text in {"0", "false", "no", "off", "disabled"}:
            return False
        return default

    @staticmethod
    def _format_timestamp(value) -> str:
        try:
            ts = float(value or 0.0)
        except Exception:
            return "unknown"
        if ts <= 0:
            return "never"
        try:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
        except Exception:
            return "unknown"

    @staticmethod
    def _format_overrides(platforms_cfg: dict) -> str:
        if not isinstance(platforms_cfg, dict) or not platforms_cfg:
            return ""
        parts = []
        for platform_name, platform_cfg in sorted(platforms_cfg.items(), key=lambda kv: str(kv[0]).lower()):
            if not isinstance(platform_cfg, dict):
                continue
            enabled = ListFeedsPlugin._boolish(platform_cfg.get("enabled"), True)
            targets = platform_cfg.get("targets") or {}
            bit = f"{platform_name}:{'on' if enabled else 'off'}"
            if isinstance(targets, dict) and targets:
                target_pairs = [f"{k}={v}" for k, v in sorted(targets.items(), key=lambda kv: str(kv[0])) if v not in (None, "")]
                if target_pairs:
                    bit += f" ({', '.join(target_pairs)})"
            parts.append(bit)
        return "; ".join(parts)

    async def _list_feeds(self, username: str | None = None) -> str:
        feeds = get_all_feeds(redis_client) or {}
        # sort for stable output
        items = sorted(feeds.items(), key=lambda kv: kv[0].lower())

        prefix = f"{username}: " if username else ""
        if not items:
            return f"{prefix}No RSS feeds are currently being watched."

        lines = []
        for index, (name, cfg) in enumerate(items, 1):
            status = "enabled" if self._boolish((cfg or {}).get("enabled"), True) else "disabled"
            last = self._format_timestamp((cfg or {}).get("last_ts"))
            lines.append(f"{index}. {name} [{status}] - last update: {last}")

            overrides = self._format_overrides((cfg or {}).get("platforms") or {})
            if overrides:
                lines.append(f"   overrides: {overrides}")

        header = f"{prefix}Watching {len(items)} RSS feed(s):"
        return "\n".join([header] + lines)

    # ---------- Discord ----------
    async def handle_discord(self, message, args, llm_client):
        return await self._list_feeds()

    # ---------- WebUI ----------
    async def handle_webui(self, args, llm_client):
        return await self._list_feeds()

    # ---------- Telegram ----------
    async def handle_telegram(self, update, args, llm_client):
        return await self._list_feeds()

    # ---------- IRC ----------
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return await self._list_feeds(username=user)

    # ---------- Matrix ----------
    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        return await self._list_feeds()


plugin = ListFeedsPlugin()
