from tater._compat import expose_legacy as _expose_legacy
_expose_legacy(__name__)

from tater_voice.entities import VoiceCoreEntityClient

__all__ = ["VoiceCoreEntityClient"]
