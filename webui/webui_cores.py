from webui.webui_portals import render_portal_controls


def render_core_controls(
    core,
    redis_client,
    *,
    start_core_fn,
    stop_core_fn,
    wipe_memory_core_data_fn,
):
    return render_portal_controls(
        core,
        redis_client,
        start_portal_fn=start_core_fn,
        stop_portal_fn=stop_core_fn,
        wipe_memory_core_data_fn=wipe_memory_core_data_fn,
        surface_kind="core",
    )
