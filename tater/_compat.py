"""Legacy module aliases for installed extensions and older launch commands.

Both names must resolve to the same module object: copying exported names would
split mutable state and make patches or singleton caches depend on import order.
Keep package initializers lightweight so circular imports retain their old order.
"""
from __future__ import annotations

import importlib
import sys
from types import ModuleType

LEGACY_MODULES = {'admin_gate': 'tater.identity.access',
 'airplay_bridge': 'tater.audio.airplay',
 'announcement_targets': 'tater.audio.announcements',
 'conversation_artifacts': 'tater.conversation.artifacts',
 'core_registry': 'tater.extensions.cores',
 'emoji_responder': 'tater.conversation.emoji',
 'external_audio': 'tater.audio.external',
 'face_id_runtime': 'tater.identity.runtime',
 'face_id_worker': 'tater.identity.worker',
 'face_identity': 'tater.identity.faces',
 'helpers': 'tater.llm.runtime',
 'hydra_core_extensions': 'tater.extensions.hydra',
 'integration_registry': 'tater.extensions.integrations',
 'integration_runtime': 'tater.extensions.integration_runtime',
 'kernel_tools': 'tater.extensions.tools',
 'little_spud_home': 'tater.spudlink.home',
 'managed_tts': 'tater.audio.managed',
 'managed_tts_worker': 'tater.audio.worker',
 'media_playback': 'tater.audio.playback',
 'media_understanding_settings': 'tater.llm.media_settings',
 'people': 'tater.identity.people',
 'portal_registry': 'tater.extensions.portals',
 'redis_runtime': 'tater.runtime.redis',
 'rss_store': 'tater.personal.rss',
 'runtime_executors': 'tater.runtime.executors',
 'speech_settings': 'tater.audio.settings',
 'speech_tts': 'tater.audio.speech',
 'spud_link_models': 'tater.spudlink.models',
 'system_tasks': 'tater.runtime.tasks',
 'tateros_app': 'tater.web.application',
 'tool_runtime': 'tater.extensions.runtime',
 'verba_base': 'tater.extensions.base',
 'verba_diagnostics': 'tater.extensions.diagnostics',
 'verba_kernel': 'tater.extensions.kernel',
 'verba_loader': 'tater.extensions.loader',
 'verba_registry': 'tater.extensions.verbas',
 'verba_result': 'tater.extensions.results',
 'verba_settings': 'tater.extensions.settings',
 'verba_supersession': 'tater.extensions.supersession',
 'vision_settings': 'tater.llm.vision_settings',
 'voice_core_entities': 'tater.voice.entities',
 'web_research': 'tater.llm.research'}


def expose_legacy(module_name: str) -> None:
    """Publish a canonical module before its dependencies load, as Python does."""
    for legacy, canonical in LEGACY_MODULES.items():
        if canonical == module_name and module_name in sys.modules:
            sys.modules[legacy] = sys.modules[module_name]
            return


def redirect_module(legacy: str, canonical: str) -> ModuleType:
    module = importlib.import_module(canonical)
    sys.modules[legacy] = module
    return module
