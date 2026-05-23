"""Runtime integration package for Tater.

Integration source lives outside the main Tater repo and is restored into this
package only after an integration is enabled.
"""

_BOOTSTRAPPED = False


def _bootstrap_enabled_integrations() -> None:
    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return
    _BOOTSTRAPPED = True
    try:
        from tateros import integration_store

        integration_store.ensure_enabled_integrations_ready()
    except Exception:
        pass


_bootstrap_enabled_integrations()
