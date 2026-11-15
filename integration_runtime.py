"""Compatibility import for :mod:`tater.extensions.integration_runtime`. Implementation lives there."""
from tater._compat import redirect_module as _redirect_module
_redirect_module(__name__, "tater.extensions.integration_runtime")
