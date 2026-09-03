#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class VueDashboardIntegrationsTests(unittest.TestCase):
    def test_dashboard_and_integrations_use_shared_vue_bundle(self) -> None:
        app_js = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")
        entry = (REPO_ROOT / "frontend" / "src" / "entry.ts").read_text(encoding="utf-8")

        self.assertIn("mountVueDashboard", app_js)
        self.assertIn("mountVueIntegrations", app_js)
        self.assertIn("module.mountDashboard", app_js)
        self.assertIn("module.mountIntegrations", app_js)
        self.assertIn("export function mountDashboard", entry)
        self.assertIn("export function mountIntegrations", entry)

    def test_dashboard_keeps_live_controls_and_navigation(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "dashboard" / "DashboardApp.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("refresh_snapshot=true", source)
        self.assertIn("refreshBriefsEndpoint", source)
        self.assertIn("personal_person_id", source)
        self.assertIn("onNavigate", source)
        self.assertIn("window.setInterval", source)
        self.assertNotIn('class="td-status-grid"', source)
        self.assertNotIn("Status tiles", source)
        self.assertIn('class="tv-modal td-dashboard-controls"', source)
        self.assertEqual(source.count('class="tv-checkbox" type="checkbox"'), 2)
        self.assertIn(".td-dashboard-controls .tv-toggle .tv-checkbox { width: 15px !important;", styles)
        self.assertIn(".td-dashboard-controls .tv-toggle > span { min-width: 0; flex: 1; }", styles)

    def test_environment_artwork_is_full_width_and_uncropped(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "dashboard" / "DashboardApp.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        self.assertIn("grid-auto-flow: row dense; grid-auto-rows: 8px;", styles)
        self.assertIn(".td-section { display: grid; align-content: start; height: auto;", styles)
        self.assertIn('const sectionGrid = ref<HTMLElement | null>(null);', source)
        self.assertIn("const sortedSections = computed(() =>", source)
        self.assertIn('v-for="section in sortedSections"', source)
        self.assertIn("new ResizeObserver(scheduleSectionLayout)", source)
        self.assertIn("card.style.gridRowEnd = `span ${span}`;", source)
        self.assertIn(".td-section.section-environment .td-items { grid-template-columns: minmax(0, 1fr);", styles)
        self.assertIn(".td-section.section-environment .td-item img { width: 100%; height: auto;", styles)
        self.assertIn("object-fit: contain;", styles)

    def test_integrations_surface_includes_full_management_area(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "integrations" / "IntegrationsApp.vue").read_text(encoding="utf-8")
        styles = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        for feature in (
            "update-all",
            "saveRepos",
            "saveIntegrationSettings",
            "refreshRegistry",
            "create_room",
            "assign_device_room",
            "rename_device",
            "set_room_preferred_media_player",
            "refreshActivity",
        ):
            self.assertIn(feature, source)
        self.assertIn('.tater-vue-surface .ti-purge input[type="checkbox"]', styles)
        self.assertIn("min-width: 13px;", styles)
        self.assertIn("white-space: nowrap;", styles)
        self.assertIn(".ti-browser-layout { display: grid;", styles)
        self.assertIn("align-items: start;", styles)
        self.assertIn(".ti-device-content { display: grid; min-width: 0; gap: 12px; align-content: start; }", styles)
        self.assertIn(".ti-room-grid { display: grid; grid-template-columns: minmax(0, 1fr);", styles)
        self.assertIn(".ti-room-devices { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr));", styles)
        self.assertIn(".ti-room-devices { grid-template-columns: 1fr; }", styles)

    def test_integration_select_fields_render_as_dropdowns(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "integrations" / "IntegrationsApp.vue").read_text(encoding="utf-8")
        legacy = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn('v-else-if="field.type === \'select\'"', source)
        self.assertIn('v-for="option in field.options || []"', source)
        self.assertIn("optionValue(option)", source)
        self.assertIn("optionLabel(option)", source)
        self.assertIn('if (type === "select")', legacy)
        self.assertIn("settingsIntegrationOptionValue(option)", legacy)
        self.assertIn("settingsIntegrationOptionLabel(option)", legacy)

    def test_device_and_organize_tabs_use_background_registry_refresh(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "integrations" / "IntegrationsApp.vue").read_text(encoding="utf-8")

        self.assertIn("async function loadRegistry", source)
        self.assertIn("async function waitForRegistryWarmup", source)
        self.assertIn("integration_device_registry/run", source)
        self.assertIn("Integration devices refreshed.", source)
        self.assertNotIn("refresh=true", source)
        self.assertIn('if (tab === "devices") void loadRegistry(false);', source)
        self.assertIn('if (tab === "rooms") void loadRegistry(true);', source)


if __name__ == "__main__":
    unittest.main()
