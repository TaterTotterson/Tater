import asyncio
import json
import os
import re

import streamlit as st

import plugin_registry as plugin_registry_mod
from helpers import run_async
from kernel_tools import AGENT_PLUGINS_DIR, validate_plugin, delete_file
from plugin_settings import (
    get_plugin_enabled,
    set_plugin_enabled,
    get_plugin_settings,
    save_plugin_settings,
)
from .webui_platforms import (
    exp_get_plugin_enabled,
    exp_set_plugin_enabled,
    _load_exp_validation,
    _validation_status,
    _dependency_lines,
    render_exp_plugin_settings_form,
)
from .webui_plugin_store import (
    _safe_plugin_file_path,
    uninstall_plugin_file,
    clear_plugin_redis_data,
    _refresh_plugins_after_fs_change,
)


def get_registry():
    return plugin_registry_mod.plugin_registry


def render_plugin_controls(plugin_name, label=None):
    current_state = get_plugin_enabled(plugin_name)
    toggle_state = st.toggle(label or plugin_name, value=current_state, key=f"plugin_toggle_{plugin_name}")

    if toggle_state != current_state:
        set_plugin_enabled(plugin_name, toggle_state)
        st.rerun()


def get_plugin_description(plugin):
    return getattr(plugin, "plugin_dec", None) or getattr(plugin, "description", "")


def render_plugin_settings_form(plugin):
    category = getattr(plugin, "settings_category", None)
    settings = getattr(plugin, "required_settings", None) or {}
    if not category or not settings:
        return

    with st.expander("Settings", expanded=False):
        current_settings = get_plugin_settings(category)
        new_settings = {}
        has_fields = False

        for key, info in settings.items():
            input_type = info.get("type", "text")
            label = info.get("label", key)
            desc = info.get("description", "")
            default_value = current_settings.get(key, info.get("default", ""))

            if input_type == "button":
                if st.button(label, key=f"{plugin.name}_{category}_{key}_button"):
                    if hasattr(plugin, "handle_setting_button"):
                        try:
                            result = plugin.handle_setting_button(key)
                            if asyncio.iscoroutine(result):
                                result = run_async(result)
                            if result:
                                st.success(result)
                        except Exception as e:
                            st.error(f"Error running {label}: {e}")
                if desc:
                    st.caption(desc)
                continue

            has_fields = True

            if input_type == "password":
                new_value = st.text_input(
                    label,
                    value=str(default_value),
                    help=desc,
                    type="password",
                    key=f"{plugin.name}_{category}_{key}",
                )

            elif input_type == "file":
                uploaded_file = st.file_uploader(
                    label,
                    type=["json"],
                    key=f"{plugin.name}_{category}_{key}",
                )
                if uploaded_file is not None:
                    try:
                        file_content = uploaded_file.read().decode("utf-8")
                        json.loads(file_content)
                        new_value = file_content
                    except Exception as e:
                        st.error(f"Error in uploaded file for {key}: {e}")
                        new_value = default_value
                else:
                    new_value = default_value

            elif input_type == "select":
                options = info.get("options", []) or ["Option 1", "Option 2"]
                current_index = options.index(default_value) if default_value in options else 0
                new_value = st.selectbox(
                    label,
                    options,
                    index=current_index,
                    help=desc,
                    key=f"{plugin.name}_{category}_{key}",
                )

            elif input_type == "checkbox":
                is_checked = (
                    default_value
                    if isinstance(default_value, bool)
                    else str(default_value).lower() in ("true", "1", "yes")
                )
                new_value = st.checkbox(
                    label,
                    value=is_checked,
                    help=desc,
                    key=f"{plugin.name}_{category}_{key}",
                )

            elif input_type == "number":
                raw_value = str(default_value).strip()
                is_int_like = bool(re.fullmatch(r"-?\d+", raw_value))

                if is_int_like:
                    try:
                        current_num = int(raw_value)
                    except Exception:
                        current_num = 0
                    new_value = st.number_input(
                        label,
                        value=current_num,
                        step=1,
                        format="%d",
                        help=desc,
                        key=f"{plugin.name}_{category}_{key}",
                    )
                else:
                    try:
                        current_num = float(raw_value) if raw_value else 0.0
                    except Exception:
                        current_num = 0.0
                    new_value = st.number_input(
                        label,
                        value=current_num,
                        step=1.0,
                        help=desc,
                        key=f"{plugin.name}_{category}_{key}",
                    )

            elif input_type in ("textarea", "multiline") or info.get("multiline") is True:
                rows = int(info.get("rows") or 8)
                height = int(info.get("height") or (rows * 24 + 40))
                placeholder = info.get("placeholder", None)

                new_value = st.text_area(
                    label,
                    value=str(default_value),
                    help=desc,
                    height=height,
                    placeholder=placeholder,
                    key=f"{plugin.name}_{category}_{key}",
                )

            else:
                new_value = st.text_input(
                    label,
                    value=str(default_value),
                    help=desc,
                    key=f"{plugin.name}_{category}_{key}",
                )

            new_settings[key] = new_value

        if has_fields and st.button(f"Save {category} Settings", key=f"save_{plugin.name}_{category}"):
            save_plugin_settings(category, new_settings)
            st.success(f"{category} settings saved.")
            st.rerun()


def render_plugin_card(plugin):
    display_name = (
        getattr(plugin, "plugin_name", None)
        or getattr(plugin, "pretty_name", None)
        or plugin.name
    )
    description = get_plugin_description(plugin)
    platforms = getattr(plugin, "platforms", []) or []

    registry_id = plugin.name
    plugin_id = getattr(plugin, "id", None) or registry_id

    removable = False
    try:
        removable = os.path.exists(_safe_plugin_file_path(plugin_id))
    except Exception:
        removable = False

    purge_key = f"purge_plugin_redis_{plugin_id}"
    purge_label = "Delete Data?"

    with st.container(border=True):
        header_cols = st.columns([4, 1, 1])

        with header_cols[0]:
            st.subheader(display_name)
            st.caption(f"ID: {registry_id}")

        with header_cols[1]:
            render_plugin_controls(registry_id, label="Enabled")

        with header_cols[2]:
            if removable:
                purge_redis = st.checkbox(purge_label, value=False, key=purge_key)

                if st.button("Remove", key=f"uninstall_{plugin_id}"):
                    loaded = get_registry().get(registry_id)
                    category_hint = getattr(loaded, "settings_category", None) if loaded else None

                    ok, msg = uninstall_plugin_file(plugin_id)
                    if ok:
                        st.success(msg)

                        try:
                            set_plugin_enabled(registry_id, False)
                        except Exception:
                            pass

                        if purge_redis:
                            try:
                                ok2, msg2 = clear_plugin_redis_data(plugin_id, category_hint=category_hint)
                                if ok2:
                                    st.success(f"Redis cleanup: {msg2}")
                                else:
                                    st.error(msg2)
                            except Exception as e:
                                st.error(f"Redis cleanup failed: {e}")

                        _refresh_plugins_after_fs_change()
                        st.rerun()
                    else:
                        st.error(msg)
            else:
                st.button("Remove", disabled=True, key=f"uninstall_disabled_{plugin_id}")

        if description:
            st.write(description)
        if platforms:
            st.caption(f"Platforms: {', '.join(platforms)}")

        render_plugin_settings_form(plugin)


def render_agent_lab_plugin_card(plugin):
    display_name = (
        getattr(plugin, "plugin_name", None)
        or getattr(plugin, "pretty_name", None)
        or plugin.name
    )
    description = get_plugin_description(plugin)
    platforms = getattr(plugin, "platforms", []) or []
    registry_id = plugin.name

    enabled = exp_get_plugin_enabled(registry_id)
    plugin_file = AGENT_PLUGINS_DIR / f"{registry_id}.py"
    report = _load_exp_validation("plugin", registry_id)
    status_label, status_detail = _validation_status(report)

    with st.container(border=True):
        header_cols = st.columns([4, 1.1, 1.1, 1.1])

        with header_cols[0]:
            st.subheader(display_name)
            st.caption(f"ID: {registry_id}")
            st.caption(f"Enabled: {'yes' if enabled else 'no'}")

        with header_cols[1]:
            if st.button("Validate", key=f"exp_validate_{registry_id}"):
                report = validate_plugin(registry_id)
                if report.get("ok"):
                    st.success("Validation passed.")
                else:
                    st.error(f"Validation failed: {report.get('error') or report.get('missing_fields')}")

        with header_cols[2]:
            if st.button("Enable", key=f"exp_enable_{registry_id}"):
                report = validate_plugin(registry_id)
                if report.get("ok"):
                    exp_set_plugin_enabled(registry_id, True)
                    st.success("Enabled.")
                    st.rerun()
                else:
                    st.error(f"Enable blocked: {report.get('error') or report.get('missing_fields')}")

        with header_cols[3]:
            if st.button("Disable", key=f"exp_disable_{registry_id}"):
                exp_set_plugin_enabled(registry_id, False)
                st.success("Disabled.")
                st.rerun()

        if description:
            st.write(description)
        if platforms:
            st.caption(f"Platforms: {', '.join(platforms)}")
        if status_label:
            status_line = f"Validation: {status_label}"
            if status_detail:
                status_line = f"{status_line} ({status_detail})"
            st.caption(status_line)
        for line in _dependency_lines(report):
            st.caption(line)

        if plugin_file.exists():
            if st.button("Delete", key=f"exp_delete_{registry_id}"):
                result = delete_file(str(plugin_file))
                if result.get("ok"):
                    exp_set_plugin_enabled(registry_id, False)
                    st.success("Deleted Agent Lab plugin file.")
                    st.rerun()
                else:
                    st.error(result.get("error") or "Delete failed.")

        render_exp_plugin_settings_form(plugin)


def render_agent_lab_plugin_error_card(name: str, path: str):
    report = _load_exp_validation("plugin", name)
    status_label, status_detail = _validation_status(report, fallback_error="Failed to load")
    missing_fields = report.get("missing_fields") if isinstance(report, dict) else []

    with st.container(border=True):
        header_cols = st.columns([4, 1, 1])
        with header_cols[0]:
            st.subheader(name)
            st.caption(f"ID: {name}")
            status_line = f"Validation: {status_label}"
            if status_detail:
                status_line = f"{status_line} ({status_detail})"
            st.caption(status_line)
        with header_cols[1]:
            if st.button("Validate", key=f"exp_validate_error_{name}"):
                report = validate_plugin(name)
                if report.get("ok"):
                    st.success("Validation passed.")
                else:
                    st.error(f"Validation failed: {report.get('error') or report.get('missing_fields')}")
        with header_cols[2]:
            if st.button("Delete", key=f"exp_delete_error_{name}"):
                result = delete_file(str(path))
                if result.get("ok"):
                    exp_set_plugin_enabled(name, False)
                    st.success("Deleted Agent Lab plugin file.")
                    st.rerun()
                else:
                    st.error(result.get("error") or "Delete failed.")

        if isinstance(missing_fields, list) and "name" in missing_fields:
            st.caption("Tip: set plugin class `name` to match this file id exactly.")

        for line in _dependency_lines(report):
            st.caption(line)


def _sort_plugins_for_display(plugins):
    return sorted(
        plugins,
        key=lambda p: (
            getattr(p, "plugin_name", None)
            or getattr(p, "pretty_name", None)
            or p.name
        ).lower(),
    )


def render_plugin_list(plugins, empty_message):
    sorted_plugins = _sort_plugins_for_display(plugins)
    if not sorted_plugins:
        st.info(empty_message)
        return
    for plugin in sorted_plugins:
        render_plugin_card(plugin)
