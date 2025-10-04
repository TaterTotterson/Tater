# platform_registry.py
from platforms.discord_platform import PLATFORM_SETTINGS as discord_settings
from platforms.irc_platform import PLATFORM_SETTINGS as irc_settings
from platforms.homeassistant_platform import PLATFORM_SETTINGS as ha_settings

platform_registry = [
    {
        **discord_settings,
        "key": "discord_platform",
        "label": "Discord Settings"
    },
    {
        **irc_settings,
        "key": "irc_platform",
        "label": "IRC Settings"
    },
    {
        **ha_settings,
        "key": "homeassistant_platform",
        "label": "Home Assistant Settings"
    }
]