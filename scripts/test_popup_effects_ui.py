#!/usr/bin/env python3
from __future__ import annotations
import ast
import pathlib
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]

def source(relative):
    return (REPO_ROOT / relative).read_text(encoding='utf-8')

class PopupEffectsUiTests(unittest.TestCase):
    def test_shared_transition_waits_for_motion_and_cleans_up(self):
        text = source('frontend/src/shared/PopupTransition.vue')
        for contract in (':css="false"', '@enter="enter"', '@leave="leave"', '@after-leave="syncBodyLock"', '@leave-cancelled="cancelPopupMotion"', 'onBeforeUnmount', '.tater-popup-effect-backdrop'):
            self.assertIn(contract, text)
        self.assertNotIn('tater-popup-effect-field', text)

    def test_application_dialogs_share_the_same_motion(self):
        for path in (REPO_ROOT / 'frontend/src').rglob('*.vue'):
            text = path.read_text(encoding='utf-8')
            if 'role="dialog"' not in text:
                continue
            with self.subTest(source=str(path.relative_to(REPO_ROOT))):
                self.assertIn('PopupTransition', text)
                self.assertNotIn('<Transition', text)
        toast = source('frontend/src/shared/PopupToast.vue')
        self.assertIn('animatePopup', toast)
        self.assertIn('@after-leave="emit(\'dismissed\')"', toast)
        shell = source('frontend/src/shell/AppShell.vue')
        self.assertIn('@dismissed="removeToast(item.id)"', shell)
        self.assertNotIn("'flame-out'", shell)

    def test_previews_are_isolated_and_reaction_settings_are_retained(self):
        text = source('frontend/src/settings/components/MiscSettings.vue')
        self.assertIn('<fieldset class="popup-motion-options">', text)
        self.assertIn('type="radio"', text)
        self.assertIn('Preview dialog', text)
        self.assertIn('Preview notification', text)
        self.assertIn(':effect="draft.popup_effect_style"', text)
        self.assertNotIn('document.body.dataset.popupEffect =', text)
        for field in ('emoji_enable_on_reaction_add', 'emoji_enable_auto_reaction_on_reply', 'emoji_reaction_chain_chance_percent', 'emoji_reply_reaction_chance_percent', 'emoji_reaction_chain_cooldown_seconds', 'emoji_reply_reaction_cooldown_seconds', 'emoji_min_message_length'):
            self.assertIn('draft.' + field, text)
            self.assertIn(field + ':', text)

    def test_backend_accepts_old_preferences_and_returns_supported_effects(self):
        tree = ast.parse(source('tateros_app.py'))
        names = {'DEFAULT_WEBUI_POPUP_EFFECT_STYLE', 'WEBUI_POPUP_EFFECT_STYLE_CHOICES', 'WEBUI_POPUP_EFFECT_STYLE_ALIASES'}
        nodes = [n for n in tree.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in names for t in n.targets)]
        nodes += [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_normalize_popup_effect_style']
        scope = {'Any': object}
        exec(compile(ast.Module(body=nodes, type_ignores=[]), '<popup settings>', 'exec'), scope)
        normalize = scope['_normalize_popup_effect_style']
        for old, new in scope['WEBUI_POPUP_EFFECT_STYLE_ALIASES'].items():
            self.assertEqual(normalize(' ' + old.upper() + ' '), new)
            self.assertEqual(normalize('unknown', old), new)
        for effect in ('disabled', 'fade', 'scale', 'slide', 'tilt', 'fold'):
            self.assertEqual(normalize(effect), effect)
        self.assertEqual(normalize(None), 'scale')
        self.assertEqual(normalize('unknown', 'invalid'), 'scale')
        self.assertIn('raw_style not in WEBUI_POPUP_EFFECT_STYLE_ALIASES', source('tateros_app.py'))

if __name__ == '__main__':
    unittest.main()
