import json
import os
import re
import secrets
import time
from typing import Any, Dict, List, Optional

import redis
import streamlit as st


redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', '127.0.0.1'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True,
)


def _looks_like_token_setting(setting_key: str, label: str) -> bool:
    key_text = str(setting_key or "").strip().lower()
    label_text = str(label or "").strip().lower()
    return "token" in key_text or "token" in label_text


def _token_text_input(
    *,
    label: str,
    value: str,
    help_text: str,
    widget_key: str,
) -> str:
    input_col, button_col = st.columns([6, 1])
    with input_col:
        new_val = st.text_input(
            label,
            value=str(value),
            help=help_text,
            type="password",
            key=widget_key,
        )
    with button_col:
        st.write("")
        if st.button("Generate", key=f"{widget_key}_generate"):
            st.session_state[widget_key] = secrets.token_urlsafe(24)
            st.rerun()
    return new_val


def render_portal_controls(
    portal,
    redis_client,
    *,
    start_portal_fn,
    stop_portal_fn,
    surface_kind: str = "portal",
    render_surface_extras_fn=None,
):
    category     = portal["label"]
    key          = portal["key"]
    required     = portal["required"]
    short_name   = category.replace(" Settings", "").strip()
    surface_text = "core" if str(surface_kind or "").strip().lower() == "core" else "portal"
    state_key    = f"{key}_running"
    cooldown_key = f"tater:cooldown:{key}"
    cooldown_secs = 10
    toggle_key = f"{category}_toggle"
    cooldown_notice_key = f"{category}_cooldown_notice"
    toggle_reset_key = f"{category}_toggle_reset_to"

    # read current on/off from Redis
    is_running = (redis_client.get(state_key) == "true")
    emoji      = "🟢" if is_running else "🔴"

    # show one-time cooldown notice if we blocked restart on previous click
    cooldown_notice = st.session_state.pop(cooldown_notice_key, None)
    if cooldown_notice:
        st.warning(cooldown_notice)

    # If we asked for a reset on the previous run, apply it before rendering widget.
    if toggle_reset_key in st.session_state:
        st.session_state[toggle_key] = bool(st.session_state.pop(toggle_reset_key))
    elif toggle_key not in st.session_state:
        st.session_state[toggle_key] = is_running

    new_toggle = st.toggle(
        f"{emoji} Enable {short_name}",
        key=toggle_key,
    )
    is_enabled = new_toggle

    # --- TURNING ON ---
    if is_enabled and not is_running:
        # cooldown check
        last = redis_client.get(cooldown_key)
        now  = time.time()
        if last and now - float(last) < cooldown_secs:
            remaining = int(cooldown_secs - (now - float(last)))
            st.session_state[cooldown_notice_key] = f"⏳ Wait {remaining}s before restarting {short_name}."
            st.session_state[toggle_reset_key] = False
            st.rerun()

        # actually start it
        start_portal_fn(key)
        redis_client.set(state_key, "true")
        st.success(f"{short_name} started.")
        st.rerun()

    # --- TURNING OFF ---
    elif not is_enabled and is_running:
        stop_portal_fn(key)
        redis_client.set(state_key, "false")
        redis_client.set(cooldown_key, str(time.time()))
        st.success(f"{short_name} stopped.")
        st.rerun()

    # --- SETTINGS FORM ---
    redis_key = f"{key}_settings"
    current_settings = redis_client.hgetall(redis_key)
    new_settings = {}

    for setting_key, setting in required.items():
        label       = setting.get("label", setting_key)
        input_type  = setting.get("type", "text")
        desc        = setting.get("description", "")
        default_val = setting.get("default", "")
        current_val = current_settings.get(setting_key, default_val)
        widget_key  = f"{category}_{setting_key}"
        is_token_setting = _looks_like_token_setting(setting_key, label)

        # normalize bools from redis strings
        def _to_bool(v):
            if isinstance(v, bool):
                return v
            return str(v).lower() in ("true", "1", "yes", "on")

        if input_type == "number":
            s = str(current_val).strip()

            # Decide if int-like or float-like
            is_int_like = bool(re.fullmatch(r"-?\d+", s))
            if is_int_like:
                try:
                    current_num = int(s)
                except Exception:
                    # fallback to default as int, then 0
                    try:
                        current_num = int(str(default_val).strip())
                    except Exception:
                        current_num = 0
                new_val = st.number_input(
                    label,
                    value=current_num,
                    step=1,
                    format="%d",
                    help=desc,
                    key=widget_key
                )
            else:
                # treat everything else as float (including "8787.0", "0.5", "")
                try:
                    current_num = float(s)
                except Exception:
                    try:
                        current_num = float(str(default_val).strip())
                    except Exception:
                        current_num = 0.0
                new_val = st.number_input(
                    label,
                    value=current_num,
                    step=1.0,
                    help=desc,
                    key=widget_key
                )

            # store back (Redis expects strings later)
            new_settings[setting_key] = new_val

        elif input_type == "password":
            if is_token_setting:
                new_val = _token_text_input(
                    label=label,
                    value=str(current_val),
                    help_text=desc,
                    widget_key=widget_key,
                )
            else:
                new_val = st.text_input(
                    label, value=str(current_val), help=desc, type="password",
                    key=widget_key
                )
            new_settings[setting_key] = new_val

        elif input_type == "checkbox":
            new_val = st.checkbox(
                label, value=_to_bool(current_val), help=desc,
                key=widget_key
            )
            new_settings[setting_key] = new_val

        elif input_type == "select":
            raw_options = list(setting.get("options", []) or [])
            options: List[str] = []
            option_labels: Dict[str, str] = {}
            for raw_option in raw_options:
                if isinstance(raw_option, dict):
                    raw_value = (
                        raw_option.get("value")
                        if raw_option.get("value") is not None
                        else raw_option.get("id")
                    )
                    if raw_value is None:
                        raw_value = raw_option.get("key")
                    if raw_value is None:
                        raw_value = raw_option.get("label")
                    raw_label = raw_option.get("label", raw_value)
                else:
                    raw_value = raw_option
                    raw_label = raw_option

                value_text = str(raw_value).strip()
                if not value_text:
                    continue
                if value_text in option_labels:
                    continue
                label_text = str(raw_label).strip() or value_text
                options.append(value_text)
                option_labels[value_text] = label_text

            if not options:
                fallback = str(default_val).strip()
                options = [fallback]
                option_labels[fallback] = fallback

            def _norm_select_value(value: Any) -> str:
                text = str(value).strip()
                if re.fullmatch(r"-?\d+(\.\d+)?", text):
                    try:
                        num = float(text)
                        if num.is_integer():
                            return str(int(num))
                    except Exception:
                        return text
                return text

            selected_value = ""
            candidate_values = [str(current_val).strip(), str(default_val).strip()]
            for candidate in candidate_values:
                candidate_norm = _norm_select_value(candidate)
                for opt in options:
                    if _norm_select_value(opt) == candidate_norm:
                        selected_value = opt
                        break
                if selected_value:
                    break

            if not selected_value:
                selected_value = options[0]
            new_val = st.selectbox(
                label, options,
                index=(options.index(selected_value) if options else 0),
                format_func=lambda value: option_labels.get(str(value), str(value)),
                help=desc, key=widget_key
            )
            new_settings[setting_key] = new_val

        else:
            # default: text
            if is_token_setting:
                new_val = _token_text_input(
                    label=label,
                    value=str(current_val),
                    help_text=desc,
                    widget_key=widget_key,
                )
            else:
                new_val = st.text_input(
                    label, value=str(current_val), help=desc,
                    key=widget_key
                )
            new_settings[setting_key] = new_val

    if new_settings:
        if st.button(f"Save {short_name} Settings", key=f"save_{category}_unique"):
            # coerce all values to strings for Redis HSET
            save_map = {
                k: (json.dumps(v) if isinstance(v, (dict, list, bool)) else str(v))
                for k, v in new_settings.items()
            }
            redis_client.hset(redis_key, mapping=save_map)
            st.success(f"{short_name} settings saved.")
    else:
        st.caption(f"No {surface_text} settings to configure.")

    if callable(render_surface_extras_fn):
        render_surface_extras_fn(
            surface=portal,
            redis_client=redis_client,
            surface_kind=surface_text,
        )


def _portal_sort_name(p):
    return (p.get("label") or p.get("category") or p.get("key") or "").lower()


def render_portals_panel(
    *,
    portal_registry,
    redis_client,
    start_portal_fn,
    stop_portal_fn,
    auto_connected=None,
):
    st.subheader("Portals")
    for portal in sorted(portal_registry, key=_portal_sort_name):
        label = portal.get("label") or portal.get("category") or portal.get("key")
        with st.expander(label, expanded=False):
            render_portal_controls(
                portal,
                redis_client,
                start_portal_fn=start_portal_fn,
                stop_portal_fn=stop_portal_fn,
            )
