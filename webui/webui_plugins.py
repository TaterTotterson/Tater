import asyncio
import json
import re

import streamlit as st

import plugin_registry as plugin_registry_mod
from helpers import run_async
from plugin_settings import (
    get_plugin_enabled,
    set_plugin_enabled,
    get_plugin_settings,
    save_plugin_settings,
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

    with st.container(border=True):
        header_cols = st.columns([5, 1])

        with header_cols[0]:
            st.subheader(display_name)
            st.caption(f"ID: {registry_id}")

        with header_cols[1]:
            render_plugin_controls(registry_id, label="Enabled")

        if description:
            st.write(description)
        if platforms:
            st.caption(f"Platforms: {', '.join(platforms)}")

        render_plugin_settings_form(plugin)

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
