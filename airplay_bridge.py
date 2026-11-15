"""Compatibility import for :mod:`tater.audio.airplay`. Implementation lives there."""
from tater._compat import redirect_module as _redirect_module
_redirect_module(__name__, "tater.audio.airplay")
