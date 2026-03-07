# platform_registry.py
from platforms.ai_task_platform import PLATFORM_SETTINGS as ai_task_settings
from platforms.discord_platform import PLATFORM_SETTINGS as discord_settings
from platforms.ha_automations_platform import PLATFORM_SETTINGS as automations_settings
from platforms.homeassistant_platform import PLATFORM_SETTINGS as ha_settings
from platforms.homekit_platform import PLATFORM_SETTINGS as homekit_settings
from platforms.irc_platform import PLATFORM_SETTINGS as irc_settings
from platforms.matrix_platform import PLATFORM_SETTINGS as matrix_settings
from platforms.memory_platform import PLATFORM_SETTINGS as memory_settings
from platforms.macos_platform import PLATFORM_SETTINGS as macos_settings
from platforms.rss_platform import PLATFORM_SETTINGS as rss_settings
from platforms.telegram_platform import PLATFORM_SETTINGS as telegram_settings
from platforms.xbmc_platform import PLATFORM_SETTINGS as xbmc_settings

platform_registry = [
    {
        **ai_task_settings,
        "key": "ai_task_platform",
        "label": "AI Task Scheduler Settings",
    },
    {
        **memory_settings,
        "key": "memory_platform",
        "label": "Memory Platform Settings",
    },
    {
        **macos_settings,
        "key": "macos_platform",
        "label": "macOS Settings",
    },
    {
        **discord_settings,
        "key": "discord_platform",
        "label": "Discord Settings",
    },
    {
        **ha_settings,
        "key": "homeassistant_platform",
        "label": "Home Assistant Settings",
    },
    {
        **automations_settings,
        "key": "ha_automations_platform",
        "label": "Automation Settings",
    },
    {
        **homekit_settings,
        "key": "homekit_platform",
        "label": "HomeKit / Siri Settings",
    },
    {
        **irc_settings,
        "key": "irc_platform",
        "label": "IRC Settings",
    },
    {
        **matrix_settings,
        "key": "matrix_platform",
        "label": "Matrix Settings",
    },
    {
        **rss_settings,
        "key": "rss_platform",
        "label": "RSS Settings",
    },
    {
        **telegram_settings,
        "key": "telegram_platform",
        "label": "Telegram Settings",
    },
    {
        **xbmc_settings,
        "key": "xbmc_platform",
        "label": "XBMC / Original Xbox Settings",
    },
]
