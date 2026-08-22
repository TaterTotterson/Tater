#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueCoresTests(unittest.TestCase):
    def test_cores_use_shared_vue_bundle_with_legacy_fallback(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")

        self.assertIn("async function mountVueCores", app_js)
        self.assertIn("module.mountCores", app_js)
        self.assertIn('withBasePath("/api/cores")', app_js)
        self.assertIn('withBasePath("/api/shop/cores")', app_js)
        self.assertIn('withBasePath("/api/cores/tabs")', app_js)
        self.assertIn("The Vue Cores surface could not load; using the legacy renderer.", app_js)
        self.assertIn("export function mountCores", entry)

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
        ):
            self.assertIn(feature, source)

    def test_dynamic_panels_keep_live_music_and_specialized_core_contracts(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "cores" / "CoresApp.vue").read_text(encoding="utf-8")
        bridge = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "LegacyCorePanel.vue").read_text(encoding="utf-8")
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn("MusicCoreApp", source)
        self.assertIn('addEventListener("core-tab"', source)
        self.assertIn("/tab-events", source)
        self.assertIn("props.render", bridge)
        self.assertIn("host.innerHTML = renderCoreTabPayload", app_js)
        self.assertIn('state.surfaceVueView === "cores"', app_js)
        self.assertIn("state.surfaceVueController.refreshTab", app_js)

    def test_cores_share_manifest_settings_and_responsive_styles(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "cores" / "CoresApp.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn('import ManifestField from "../shared/ManifestField.vue"', source)
        self.assertIn(".tcx-card-grid { display: grid;", styles)
        self.assertIn(".tcx-manage-list { display: grid;", styles)
        self.assertIn(".tcx-repo-form { display: grid;", styles)
        self.assertIn(".tcx-legacy-host", styles)

    def test_vue_input_styles_do_not_stretch_legacy_core_toggles(self) -> None:
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
            ".core-settings-manager-awareness label.core-stats-control-toggle { display: inline-flex;",
            styles,
        )
        self.assertIn(
            ".core-settings-manager-awareness .core-stats-control-toggle .toggle-input { grid-column: auto;",
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

        self.assertIn(
            '.core-settings-manager-automation input.toggle-input[data-core-field-key="enabled"] { grid-column: auto;',
            styles,
        )
        self.assertIn(
            '.core-settings-manager-automation input.toggle-input[data-core-field-key="enabled"]:checked::before { transform: translateX(13px);',
            styles,
        )

    def test_runtime_edit_popups_refresh_dependent_core_fields(self) -> None:
        source = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn("function bindRuntimeSettingDependentSelects(fieldsEl)", source)
        self.assertIn('data-runtime-filter-source-key="', source)
        self.assertIn("targetSelect.dataset.runtimeDependentBound", source)
        self.assertIn("bindRuntimeSettingDependentSelects(fieldsEl);", source)
        self.assertIn(
            "_coreRenderSelectOptions(targetSelect, nextRows, preferredValue, preferredValues)",
            source,
        )

    def test_core_media_route_supports_browser_metadata_and_range_requests(self) -> None:
        source = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")

        self.assertIn('@app.head("/api/cores/{core_key}/media/{media_id}")', source)
        self.assertIn('if request.method.upper() == "HEAD":', source)
        self.assertIn('"Accept-Ranges": "bytes"', source)
        self.assertIn('"Content-Range": f"bytes {start}-{end}/{size}"', source)

    def test_core_video_can_return_to_its_poster_after_playback(self) -> None:
        source = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn('data-core-video-reset-to-poster="1"', source)
        self.assertIn("data-core-video-poster-button", source)
        self.assertIn("function bindCoreVideoPosterReset(root = document)", source)
        self.assertIn('video.addEventListener("pause"', source)
        self.assertIn('video.addEventListener("ended", showPoster)', source)
        self.assertIn("video.hidden = true", source)
        self.assertIn('video.style.display = "none"', source)
        self.assertIn('video.style.display = "block"', source)
        self.assertIn("posterButton.hidden = false", source)
        self.assertIn('posterButton.style.display = "block"', source)
        self.assertIn('posterButton.addEventListener("click", async () => {', source)
        self.assertIn("bindCoreVideoPosterReset();", source)

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
        self.assertIn("progressStyle", player)
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
