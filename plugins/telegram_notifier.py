# plugins/telegram_notifier.py
from plugin_base import ToolPlugin

class TelegramNotifierPlugin(ToolPlugin):
    name = "telegram_notifier"
    description = "Provides Telegram bot token and chat ID settings for RSS announcements."
    
    # Just for settings — no tool usage
    usage = ""
    platforms = []  # Not exposed to Discord or WebUI tools

    settings_category = "Telegram"
    required_settings = {
        "telegram_bot_token": {
            "label": "Telegram Bot Token",
            "type": "string",
            "default": "",
            "description": "Bot token from @BotFather"
        },
        "telegram_chat_id": {
            "label": "Telegram Channel ID",
            "type": "string",
            "default": "",
            "description": "Channel or group ID (usually starts with -100...)"
        }
    }

plugin = TelegramNotifierPlugin()