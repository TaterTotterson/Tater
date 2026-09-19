#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import shutil
import subprocess
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class CoreMusicLibraryRendererTests(unittest.TestCase):
    def test_renderer_supports_persistent_items_and_nested_grid_paging(self) -> None:
        renderer = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "CorePanelRenderer.vue").read_text(encoding="utf-8")
        items = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "CoreManagerItems.vue").read_text(encoding="utf-8")
        item = (REPO_ROOT / "frontend" / "src" / "cores" / "components" / "CoreManagerItem.vue").read_text(encoding="utf-8")
        cores = (REPO_ROOT / "frontend" / "src" / "cores" / "CoresApp.vue").read_text(encoding="utf-8")
        player = (REPO_ROOT / "frontend" / "src" / "music" / "components" / "MusicPlayer.vue").read_text(encoding="utf-8")
        tracks = (REPO_ROOT / "frontend" / "src" / "music" / "components" / "TrackList.vue").read_text(encoding="utf-8")

        self.assertIn("ui.value.persistent_item_groups", renderer)
        self.assertIn('class="core-manager-persistent"', renderer)
        self.assertIn("props.options?.page_size", items)
        self.assertIn("item.show_save_button !== false", item)
        self.assertIn("item.settings_aria_label", player)
        self.assertIn("item.card_variant", item)
        self.assertIn("payload?.ui?.live_updates", cores)
        self.assertIn("data-core-live-updates", renderer)
        self.assertIn("track_list_shuffle_action", tracks)
        self.assertIn("serverPagination", items)

    def test_music_library_uses_vue_styles_without_legacy_renderer_css(self) -> None:
        styles = (REPO_ROOT / "frontend" / "src" / "base.css").read_text(encoding="utf-8")
        music_styles = (REPO_ROOT / "frontend" / "src" / "music" / "music-core.css").read_text(encoding="utf-8")

        self.assertNotIn(".core-settings-manager-music_library", styles)
        self.assertNotIn(".core-music-track-list", styles)
        self.assertNotIn(".core-music-track-row.active", styles)
        self.assertIn(".tm-playback-dock", music_styles)
        self.assertIn(".tm-library-grid", music_styles)
        self.assertIn("position: sticky", music_styles)
        self.assertIn("repeat(auto-fill, minmax(min(185px, 100%), 1fr))", music_styles)
        self.assertIn("@media (max-width: 460px)", music_styles)

    def test_app_javascript_parses(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("Node.js is unavailable.")
        result = subprocess.run(
            [node, "--check", str(REPO_ROOT / "tateros_static" / "app.js")],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_music_library_uses_component_frontend_and_event_stream(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        app_py = (REPO_ROOT / "tateros_app.py").read_text(encoding="utf-8")
        index_html = (REPO_ROOT / "tateros_static" / "index.html").read_text(encoding="utf-8")
        cores_app = (REPO_ROOT / "frontend" / "src" / "cores" / "CoresApp.vue").read_text(encoding="utf-8")
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")

        self.assertIn("isMusicPayload", cores_app)
        self.assertIn("<MusicCoreApp", cores_app)
        self.assertIn("export function mountMusicCore", entry)
        self.assertIn("/static/ui/tater-ui.js", app_js)
        self.assertIn('/api/cores/{core_key}/tab-events', app_py)
        self.assertIn('_stream_core_tab_events', app_py)
        self.assertIn('stable_payload.pop("updated_at", None)', app_py)
        self.assertIn('async def _call_surface_handler(', app_py)
        self.assertIn('result = await asyncio.to_thread(handler, **kwargs)', app_py)
        self.assertIn('result = await _call_surface_handler(\n            handler,\n            webhook=hook,', app_py)
        self.assertNotIn('tater-music-core.css', index_html)
        self.assertTrue((REPO_ROOT / "tateros_static" / "ui" / "tater-ui.js").is_file())
        self.assertTrue((REPO_ROOT / "tateros_static" / "ui" / "tater-ui.css").is_file())
        self.assertIn("taterVueAssetVersion", app_js)
        self.assertIn('state.auth?.appVersion', app_js)

        dynamic_field = (
            REPO_ROOT / "frontend" / "src" / "music" / "components" / "DynamicField.vue"
        ).read_text(encoding="utf-8")
        settings_card = (
            REPO_ROOT / "frontend" / "src" / "music" / "components" / "SettingsCard.vue"
        ).read_text(encoding="utf-8")
        music_styles = (
            REPO_ROOT / "frontend" / "src" / "music" / "music-core.css"
        ).read_text(encoding="utf-8")
        music_app = (
            REPO_ROOT / "frontend" / "src" / "music" / "MusicCoreApp.vue"
        ).read_text(encoding="utf-8")
        track_list = (
            REPO_ROOT / "frontend" / "src" / "music" / "components" / "TrackList.vue"
        ).read_text(encoding="utf-8")
        recommendations = (
            REPO_ROOT / "frontend" / "src" / "music" / "components" / "RecommendationsBrowser.vue"
        ).read_text(encoding="utf-8")
        music_player = (
            REPO_ROOT / "frontend" / "src" / "music" / "components" / "MusicPlayer.vue"
        ).read_text(encoding="utf-8")
        library_browser = (
            REPO_ROOT / "frontend" / "src" / "music" / "components" / "LibraryBrowser.vue"
        ).read_text(encoding="utf-8")
        self.assertIn('class="tm-option-copy"', dynamic_field)
        self.assertIn(':compact="Boolean(field.compact)"', settings_card)
        self.assertIn('const fieldGrid = ref<HTMLElement | null>(null)', settings_card)
        self.assertIn('new ResizeObserver(scheduleFieldLayout)', settings_card)
        self.assertIn('field.style.gridRowEnd = `span ${span}`', settings_card)
        self.assertIn('const dirty = new Set<string>()', settings_card)
        self.assertIn('if (!dirty.has(field.key))', settings_card)
        self.assertNotIn('if (saved) dirty.clear();', settings_card)
        self.assertIn('PopupTransition', settings_card)
        self.assertIn('item.settings_title', settings_card)
        self.assertIn(':fields-popup="Boolean(ui.item_fields_popup) && item.fields_popup !== false"', music_app)
        self.assertIn(':fields-dropdown="Boolean(item.fields_dropdown ?? ui.item_fields_dropdown)"', music_app)
        self.assertIn('result.message.trim()', music_app)
        self.assertIn('props.options.onToast(message, "success")', music_app)
        self.assertIn('let fieldLayoutSignature = ""', settings_card)
        self.assertIn('function layoutSignature(fields: MusicField[]): string', settings_card)
        self.assertIn('if (nextLayoutSignature !== fieldLayoutSignature)', settings_card)
        self.assertNotIn('{ immediate: true, deep: true }', settings_card)
        self.assertIn('option.friendly_name', dynamic_field)
        self.assertIn('const cardPresentation = computed(', dynamic_field)
        self.assertIn('class="tm-choice-card"', dynamic_field)
        self.assertIn("'full-width': Boolean(field.full_width)", dynamic_field)
        self.assertIn('.tm-option-copy strong {', music_styles)
        self.assertIn('.tm-choice-card-grid {', music_styles)
        self.assertIn('.tm-choice-card-copy strong {', music_styles)
        self.assertIn('.tm-modal-backdrop {\n  --tm-surface:', music_styles)
        self.assertIn('.tm-option input[type="checkbox"] {', music_styles)
        self.assertIn('width: 16px;', music_styles)
        self.assertIn('position: sticky;', music_styles)
        self.assertIn('.tm-recommendation-grid {', music_styles)
        self.assertIn('RecommendationsBrowser', music_app)
        self.assertIn("activeManagerTab?.key === 'recommendations'", music_app)
        self.assertIn('class="tm-playback-dock"', music_app)
        self.assertIn('aria-label="Playback and navigation"', music_app)
        self.assertIn('class="tm-subtabs tm-dock-subtabs"', music_app)
        self.assertIn(':selected-group="selectedLibraryGroup"', music_app)
        self.assertIn('.tm-playback-dock {', music_styles)
        self.assertIn('.tm-playback-dock .tm-dock-subtabs {', music_styles)
        self.assertIn('grid-template-columns: 50px minmax(150px, 1fr)', music_styles)
        self.assertIn('border-top: 1px solid var(--tm-border);', music_styles)
        self.assertIn('v-if="showNavigation"', library_browser)
        self.assertIn('emit("update:selectedGroup", group)', library_browser)
        self.assertIn('class="tm-queue tm-queue-tab"', track_list)
        self.assertNotIn('class="tm-queue" open', track_list)
        self.assertIn('class="tm-recommendation-card"', recommendations)
        self.assertIn('const assistantName = computed(', recommendations)
        self.assertIn(':aria-label="recommendationsTitle"', recommendations)
        self.assertIn('`${assistantName} is mixing…`', recommendations)
        self.assertIn('assistant_name?: string;', (
            REPO_ROOT / "frontend" / "src" / "music" / "types.ts"
        ).read_text(encoding="utf-8"))
        self.assertNotIn('class="tm-progress"', music_player)
        self.assertNotIn('item.hero_badges', music_player)
        self.assertNotIn('item.summary_rows', music_player)
        self.assertNotIn('tm-player-facts', music_styles)
        self.assertIn('.tm-queue {', music_styles)
        self.assertNotIn('aria-label="Track position"', music_player)
        self.assertNotIn('async function commitSeek(', music_player)
        self.assertIn('@change="setVolume"', music_player)
        self.assertIn('@input="updateVolumeFromEvent"', music_player)
        self.assertIn('.tater-music-core .tm-shuffle input[type="checkbox"]', music_styles)
        self.assertIn('min-height: 16px;', music_styles)
        self.assertIn("const speakersDirty = ref(false)", music_player)
        self.assertIn("if (!speakersOpen.value || !speakersDirty.value)", music_player)
        self.assertIn('class="tm-player-row"', music_player)
        self.assertIn("player_settings: playerSettings.value", music_player)
        self.assertIn("item.test_sync_action", music_player)
        self.assertIn("const volumeEditing = ref(false)", music_player)
        self.assertIn("if (field && !volumeEditing.value)", music_player)
        self.assertIn('class="tm-player-volume"', music_player)
        self.assertIn('class="tm-player-row-control"', music_player)
        self.assertIn('.tm-settings-card .tm-field.compact:not(.tm-checkbox) {', music_styles)
        self.assertIn('align-self: start;', music_styles)
        self.assertIn('width: min(100%, 150px);', music_styles)
        self.assertIn('min-height: 34px;', music_styles)
        self.assertIn('grid-auto-flow: row dense;', music_styles)
        self.assertIn('grid-auto-rows: 8px;', music_styles)
        self.assertIn('.tm-player-row-controls {', music_styles)
        self.assertIn('.tm-sync-quality.is-precise {', music_styles)


if __name__ == "__main__":
    unittest.main()
