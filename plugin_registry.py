from plugins.youtube_summary import plugin as youtube_plugin
from plugins.web_summary import plugin as web_summary_plugin
from plugins.web_search import plugin as web_search_plugin
from plugins.comfyui_image_plugin import plugin as comfyui_image_plugin
from plugins.comfyui_video_plugin import plugin as comfyui_video_plugin
from plugins.comfyui_image_video_plugin import plugin as comfyui_image_video_plugin
from plugins.comfyui_audio_ace import plugin as comfyui_audio_ace_plugin
from plugins.comfyui_music_video import plugin as comfyui_music_video_plugin
from plugins.automatic_plugin import plugin as automatic_plugin
from plugins.premiumize_download import plugin as premiumize_download_plugin
from plugins.premiumize_torrent import plugin as premiumize_torrent_plugin
from plugins.watch_feed import plugin as watch_feed_plugin
from plugins.unwatch_feed import plugin as unwatch_feed_plugin
from plugins.list_feeds import plugin as list_feeds_plugin
from plugins.sftpgo_account import plugin as sftpgo_account_plugin
from plugins.sftpgo_activity import plugin as sftpgo_activity_plugin
from plugins.vision_describer import plugin as vision_describer_plugin
from plugins.ftp_browser import plugin as ftp_browser_plugin
from plugins.telegram_notifier import plugin as telegram_notifier_plugin
from plugins.wordpress_poster import plugin as wordpress_poster_plugin
from plugins.discord_notifier import plugin as discord_notifier_plugin
from plugins.emoji_ai_responder import plugin as emoji_ai_responder_plugin

plugin_registry = {
    youtube_plugin.name: youtube_plugin,
    web_summary_plugin.name: web_summary_plugin,
    web_search_plugin.name: web_search_plugin,
    comfyui_image_plugin.name: comfyui_image_plugin,
    comfyui_video_plugin.name: comfyui_video_plugin,
    comfyui_image_video_plugin.name: comfyui_image_video_plugin,
    comfyui_audio_ace_plugin.name: comfyui_audio_ace_plugin,
    comfyui_music_video_plugin.name: comfyui_music_video_plugin,
    automatic_plugin.name: automatic_plugin,
    premiumize_download_plugin.name: premiumize_download_plugin,
    premiumize_torrent_plugin.name: premiumize_torrent_plugin,
    watch_feed_plugin.name: watch_feed_plugin,
    unwatch_feed_plugin.name: unwatch_feed_plugin,
    list_feeds_plugin.name: list_feeds_plugin,
    sftpgo_account_plugin.name: sftpgo_account_plugin,
    sftpgo_activity_plugin.name: sftpgo_activity_plugin,
    vision_describer_plugin.name: vision_describer_plugin,
    ftp_browser_plugin.name: ftp_browser_plugin,
    telegram_notifier_plugin.name: telegram_notifier_plugin,
    wordpress_poster_plugin.name: wordpress_poster_plugin,
    discord_notifier_plugin.name: discord_notifier_plugin,
    emoji_ai_responder_plugin.name: emoji_ai_responder_plugin,
}