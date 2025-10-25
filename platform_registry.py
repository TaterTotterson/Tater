# platform_registry.py
from platforms.discord_platform import PLATFORM_SETTINGS as discord_settings
from platforms.irc_platform import PLATFORM_SETTINGS as irc_settings
from platforms.homeassistant_platform import PLATFORM_SETTINGS as ha_settings
from platforms.ha_automations_platform import PLATFORM_SETTINGS as automations_settings
from platforms.matrix_platform import PLATFORM_SETTINGS as matrix_settings

platform_registry = [
    {
        **discord_settings,
        "key": "discord_platform",
        "label": "Discord Settings",
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
        **ha_settings,
        "key": "homeassistant_platform",
        "label": "Home Assistant Settings",
    },
    {
        **automations_settings,
        "key": "ha_automations_platform",
        "label": "Automation Settings",
    },
]