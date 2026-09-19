#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueCoresTests(unittest.TestCase):
    def test_cores_are_loaded_by_the_shared_vue_shell(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        shell = (REPO_ROOT / "frontend" / "src" / "shell" / "AppShell.vue").read_text(encoding="utf-8")

        self.assertIn('import CoresApp from "../cores/CoresApp.vue"', shell)
        self.assertIn("cores: CoresApp", shell)
        self.assertIn('if (view === "cores")', app_js)
        self.assertIn("createCoresVueDescriptor", app_js)
        self.assertIn('withBasePath("/api/cores")', app_js)
        self.assertIn('withBasePath("/api/shop/cores")', app_js)
        self.assertIn('withBasePath("/api/cores/tabs")', app_js)
        self.assertNotIn("mountVueCores", app_js)
        self.assertNotIn("legacy renderer", app_js)

    def test_cores_preserve_runtime_shop_settings_and_repositories(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "cores" / "CoresApp.vue").read_text(encoding="utf-8")

        for feature in (
            'id: "installed"',
            'id: "store"',
            'id: "manage"',
            'id: "repos"',
            'action: "start" | "stop"',
            "/settings",
            "update-all",
            "purge_redis",
            "saveRepos",
            "shopAction('install'",
            "shopAction('remove'",
            "Delete data",
            "Running Cores restart automatically",
            '{ id: "manage", label: "Manage" }',
            "Core control center",
            "tv-manage-card",
            "Updates ready",
        ):
            self.assertIn(feature, source)

        self.assertNotIn('{ id: "manage", label: "Maintenance" }', source)

    def test_dynamic_panels_use_native_vue_renderers_and_live_core_contracts(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "cores" / "CoresApp.vue").read_text(encoding="utf-8")
        renderer = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "CorePanelRenderer.vue").read_text(encoding="utf-8")
        item = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "CoreManagerItem.vue").read_text(encoding="utf-8")
        field = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "CoreManagerField.vue").read_text(encoding="utf-8")
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        shell = (REPO_ROOT / "frontend" / "src" / "shell" / "AppShell.vue").read_text(encoding="utf-8")

        self.assertIn("MusicCoreApp", source)
        self.assertIn("CorePanelRenderer", source)
        self.assertIn('addEventListener("core-tab"', source)
        self.assertIn("/tab-events", source)
        self.assertIn('data-core-renderer="vue"', renderer)
        self.assertIn("CoreManagerItems", renderer)
        self.assertIn("CoreManagerField", item)
        self.assertIn("type === 'image_checklist'", field)
        self.assertNotIn("renderCorePanel:", app_js)
        self.assertFalse((REPO_ROOT / "frontend" / "src" / "cores" / "components" / "LegacyCorePanel.vue").exists())
        self.assertIn("selectViewTab", shell)
        self.assertIn("refreshTab", shell)
        self.assertNotIn("renderCoreTabPayload", app_js)

    def test_cores_share_manifest_settings_and_responsive_styles(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "cores" / "CoresApp.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn('import ManifestField from "../shared/ManifestField.vue"', source)
        self.assertIn(".tcx-card-grid { display: grid;", styles)
        self.assertIn(".tcx-manage-list { display: grid;", styles)
        self.assertIn(".tcx-repo-form { display: grid;", styles)
        self.assertIn(".tcx-native-panel", styles)
        self.assertIn(
            ".tcx-native-manager .core-manager-tabs, .tcx-native-manager .core-manager-subtabs { flex-wrap: nowrap; overflow-x: auto; }",
            styles,
        )
        self.assertIn(".tcx-native-popup .tcx-native-fields { grid-template-columns: 1fr; }", styles)

    def test_vue_input_styles_do_not_stretch_native_core_toggles(self) -> None:
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn(".tater-vue-surface input:not(.toggle-input)", styles)
        self.assertIn(".tv-modal input:not(.tv-checkbox):not(.toggle-input)", styles)
        self.assertNotIn(
            ".tater-vue-surface input, .tater-vue-surface select, .tv-modal input:not(.tv-checkbox)",
            styles,
        )
        self.assertNotIn(
            ".tater-vue-surface input:focus, .tater-vue-surface select:focus, .tv-modal input:focus",
            styles,
        )

    def test_awareness_controls_and_event_list_have_scoped_layouts(self) -> None:
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn(
            ".core-settings-manager-awareness .tcx-native-stats-controls { gap: 7px !important;",
            styles,
        )
        self.assertIn(
            ".tcx-native-toggle .toggle-input { width: 34px;",
            styles,
        )
        self.assertIn(
            ".core-settings-manager-awareness .core-tab-items.core-tab-items-group-event_list { grid-template-columns: minmax(0, 1fr);",
            styles,
        )
        self.assertIn(
            ".core-settings-manager-awareness .core-manager-item-variant-event_list .core-satellite-summary { grid-template-columns: 96px minmax(0, 1fr);",
            styles,
        )

    def test_automation_enabled_toggle_uses_compact_scoped_layout(self) -> None:
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn(".tcx-native-toggle .toggle-input:checked::before { transform: translateX(15px);", styles)

    def test_runtime_edit_popups_refresh_dependent_core_fields(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "CoreManagerField.vue").read_text(encoding="utf-8")

        self.assertIn("const options = computed(() =>", source)
        self.assertIn("props.field.dependent_options", source)
        self.assertIn("dependent.source_key", source)
        self.assertIn("dependent.options_by_source[sourceValue]", source)
        self.assertIn("props.allValues[sourceKey]", source)

    def test_core_media_route_supports_browser_metadata_and_range_requests(self) -> None:
        source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")

        self.assertIn('@app.head("/api/cores/{core_key}/media/{media_id}")', source)
        self.assertIn('if request.method.upper() == "HEAD":', source)
        self.assertIn('"Accept-Ranges": "bytes"', source)
        self.assertIn('"Content-Range": f"bytes {start}-{end}/{size}"', source)

    def test_core_video_can_return_to_its_poster_after_playback(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "CoreManagerField.vue").read_text(encoding="utf-8")

        self.assertIn("function showVideoPoster()", source)
        self.assertIn("function handleVideoPause", source)
        self.assertIn("async function playVideoFromPoster", source)
        self.assertIn('@pause="handleVideoPause"', source)
        self.assertIn('@ended="showVideoPoster"', source)
        self.assertIn('v-show="videoPosterVisible"', source)
        self.assertIn('class="tcx-native-video-poster"', source)

    def test_music_player_selectors_group_targets_and_use_friendly_names(self) -> None:
        display = (REPO_ROOT / "frontend" / "src" / "music" / "playerDisplay.ts").read_text(
            encoding="utf-8"
        )
        dynamic = (
            REPO_ROOT / "frontend" / "src" / "music" / "components" / "DynamicField.vue"
        ).read_text(encoding="utf-8")
        player = (
            REPO_ROOT / "frontend" / "src" / "music" / "components" / "MusicPlayer.vue"
        ).read_text(encoding="utf-8")

        for heading in ("Tater Native Sats", "Tater Stereo Pairs", "AirPlay Devices"):
            self.assertIn(heading, display)
        self.assertIn("groupPlayerTargets", dynamic)
        self.assertIn("optionDisplayName", dynamic)
        self.assertIn("groupPlayerTargets", player)
        self.assertIn("playerDisplayName", player)
        self.assertNotIn('<strong>Audio sync</strong>', player)
        self.assertNotIn('class="tm-player-row-control tm-sync-control"', player)
        self.assertIn("sync_offset_ms: clampNumber", player)

    def test_webui_assets_are_cache_busted_by_the_current_build(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        backend = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        native = (
            REPO_ROOT / "macos" / "Tater" / "Sources" / "TaterAssistant" / "main.swift"
        ).read_text(encoding="utf-8")

        self.assertIn('searchParams.get("v")', app_js)
        self.assertIn("def _webui_asset_version()", backend)
        self.assertIn('"X-Tater-Asset-Version": version', backend)
        self.assertIn("reloadIgnoringLocalAndRemoteCacheData", native)

    def test_music_uses_one_slim_player_and_a_playlist_tab(self) -> None:
        app = (REPO_ROOT / "frontend" / "src" / "music" / "MusicCoreApp.vue").read_text(
            encoding="utf-8"
        )
        player = (
            REPO_ROOT / "frontend" / "src" / "music" / "components" / "MusicPlayer.vue"
        ).read_text(encoding="utf-8")
        track_list = (
            REPO_ROOT / "frontend" / "src" / "music" / "components" / "TrackList.vue"
        ).read_text(encoding="utf-8")

        self.assertIn("activeManagerTab?.source === 'player_queue'", app)
        self.assertIn("<TrackList", app)
        self.assertNotIn("tm-player-size-toggle", player)
        self.assertNotIn("is-collapsed", player)
        self.assertNotIn("progressStyle", player)
        self.assertIn("tm-player-volume", player)
        self.assertIn("selectedPlayerCount", player)
        self.assertIn("tm-queue-tab", track_list)

    def test_music_play_triangle_uses_geometric_centering(self) -> None:
        player = (
            REPO_ROOT / "frontend" / "src" / "music" / "components" / "MusicPlayer.vue"
        ).read_text(encoding="utf-8")
        styles = (
            REPO_ROOT / "frontend" / "src" / "music" / "music-core.css"
        ).read_text(encoding="utf-8")

        # Keep both the circle and SVG explicitly sized. Percentage-sized SVGs
        # can contribute their 300px intrinsic width to an auto-sized button in
        # WebKit and blow out the slim player layout.
        self.assertIn('viewBox="0 0 24 24"', player)
        self.assertIn('<path d="M10 6.5 22 13.5 10 20.5Z" />', player)
        self.assertNotIn('return "▶"', player)
        self.assertIn(".tm-transport-play-icon {", styles)
        self.assertIn("width: 14px;", styles)
        self.assertIn("height: 14px;", styles)
        self.assertIn("flex: 0 0 30px;", styles)
        self.assertIn("flex-basis: 36px;", styles)
        self.assertNotIn("width: 44%;", styles)
        self.assertNotIn("button.is-play .tm-transport-glyph", styles)


if __name__ == "__main__":
    unittest.main()
