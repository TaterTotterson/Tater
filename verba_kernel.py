"""Compatibility import for :mod:`tater.extensions.kernel`. Implementation lives there."""
from tater._compat import redirect_module as _redirect_module
_redirect_module(__name__, "tater.extensions.kernel")
