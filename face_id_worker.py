"""Compatibility import for :mod:`tater.identity.worker`. Implementation lives there."""
if __name__ == "__main__":
    from tater.identity.worker import main
    raise SystemExit(main())

from tater._compat import redirect_module as _redirect_module
_redirect_module(__name__, "tater.identity.worker")
