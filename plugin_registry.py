from plugins.youtube_summary import plugin as youtube_plugin
from plugins.web_summary import plugin as web_summary_plugin
from plugins.web_search import plugin as web_search_plugin
from plugins.draw_picture import plugin as draw_picture_plugin
from plugins.premiumize_download import plugin as premiumize_download_plugin
from plugins.premiumize_torrent import plugin as premiumize_torrent_plugin
from plugins.watch_feed import plugin as watch_feed_plugin
from plugins.unwatch_feed import plugin as unwatch_feed_plugin
from plugins.list_feeds import plugin as list_feeds_plugin

plugin_registry = {
    youtube_plugin.name: youtube_plugin,
    web_summary_plugin.name: web_summary_plugin,
    web_search_plugin.name: web_search_plugin,
    draw_picture_plugin.name: draw_picture_plugin,
    premiumize_download_plugin.name: premiumize_download_plugin,
    premiumize_torrent_plugin.name: premiumize_torrent_plugin,
    watch_feed_plugin.name: watch_feed_plugin,
    unwatch_feed_plugin.name: unwatch_feed_plugin,
    list_feeds_plugin.name: list_feeds_plugin,
}