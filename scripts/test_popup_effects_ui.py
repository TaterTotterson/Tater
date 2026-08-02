#!/usr/bin/env python3
from __future__ import annotations

import pathlib
import unittest


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class PopupEffectsUiTests(unittest.TestCase):
    def test_shared_transition_keeps_vue_popups_mounted_for_leave_animation(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "shared" / "PopupTransition.vue").read_text(encoding="utf-8")

        for contract in (
            '<Transition name="tater-popup" appear',
            'class="tater-popup-effect-backdrop"',
            'class="tater-popup-effect-field"',
            'class="tater-popup-effect-burst"',
            '@after-leave="syncBodyLock"',
        ):
            self.assertIn(contract, source)

    def test_every_vue_popup_surface_uses_shared_transition(self) -> None:
        sources = (
            "dashboard/DashboardApp.vue",
            "runtime/RuntimeStatus.vue",
            "music/components/MusicPlayer.vue",
            "integrations/IntegrationsApp.vue",
            "verbas/VerbasApp.vue",
            "portals/PortalsApp.vue",
            "cores/CoresApp.vue",
            "spudex/SpudexApp.vue",
        )
        for relative in sources:
            source = (REPO_ROOT / "frontend" / "src" / relative).read_text(encoding="utf-8")
            with self.subTest(source=relative):
                self.assertIn("PopupTransition", source)
                self.assertNotIn('<Teleport to="body">', source)

    def test_effect_styles_include_all_modes_and_reduced_motion(self) -> None:
        source = (REPO_ROOT / "frontend" / "src" / "tater-ui.css").read_text(encoding="utf-8")

        for effect in ("disabled", "flame", "dust", "glitch", "portal", "melt"):
            self.assertIn(f'data-popup-effect="{effect}"', source)
        for keyframe in (
            "tpx-dialog-flame-in",
            "tpx-dialog-dust-out",
            "tpx-dialog-glitch-in",
            "tpx-dialog-portal-out",
            "tpx-dialog-melt-in",
        ):
            self.assertIn(f"@keyframes {keyframe}", source)
        self.assertIn("@media (prefers-reduced-motion: reduce)", source)

    def test_misc_settings_has_live_preview(self) -> None:
        source = (REPO_ROOT / "tateros_static" / "app.js").read_text(encoding="utf-8")

        self.assertIn('id="settings-popup-effect-preview"', source)
        self.assertIn("function openPopupEffectPreview", source)
        self.assertIn("function closePopupEffectPreview", source)
        self.assertIn("tater-popup-effect-backdrop tater-popup-enter-active", source)
        self.assertIn(".tater-popup-effect-backdrop", source)


if __name__ == "__main__":
    unittest.main()
