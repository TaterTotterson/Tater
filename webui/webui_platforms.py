import asyncio
import json
import os
import re
import time
from typing import Any, Dict, List, Optional

import feedparser
import redis
import streamlit as st

from helpers import run_async
from rss_store import get_all_feeds, set_feed, update_feed, delete_feed


redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', '127.0.0.1'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True,
)

def exp_get_plugin_enabled(plugin_name: str) -> bool:
    raw = redis_client.hget("exp:plugin_enabled", plugin_name)
    return str(raw or "").strip().lower() == "true"


def exp_set_plugin_enabled(plugin_name: str, enabled: bool) -> None:
    redis_client.hset("exp:plugin_enabled", plugin_name, "true" if enabled else "false")


def exp_get_plugin_settings(category: str) -> dict:
    return redis_client.hgetall(f"exp:plugin_settings:{category}") or {}


def exp_save_plugin_settings(category: str, settings: dict) -> None:
    redis_client.hset(f"exp:plugin_settings:{category}", mapping={k: str(v) for k, v in settings.items()})


def exp_get_platform_settings(platform_key: str) -> dict:
    return redis_client.hgetall(f"exp:platform_settings:{platform_key}") or {}


def exp_save_platform_settings(platform_key: str, settings: dict) -> None:
    redis_client.hset(f"exp:platform_settings:{platform_key}", mapping={k: str(v) for k, v in settings.items()})


def _load_exp_validation(kind: str, name: str) -> dict | None:
    key = f"exp:validation:{kind}:{name}"
    raw = redis_client.get(key)
    if not raw:
        return None
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _validation_status(report: dict | None, fallback_error: str | None = None) -> tuple[str, str]:
    if report:
        if report.get("ok"):
            return ("Valid", "")
        missing_deps = report.get("missing_dependencies") or []
        if missing_deps:
            return ("Missing dependencies", ", ".join(map(str, missing_deps)))
        err = report.get("error") or report.get("missing_fields") or "Invalid"
        if isinstance(err, list):
            err = ", ".join(map(str, err))
        return ("Invalid", str(err))
    if fallback_error:
        return ("Load error", fallback_error)
    return ("Not validated", "")


def _dependency_lines(report: dict | None) -> list[str]:
    if not report:
        return []
    lines: list[str] = []
    declared = report.get("declared_dependencies") or []
    missing = report.get("missing_dependencies") or []
    installed = report.get("installed_dependencies") or []
    install_errors = report.get("install_errors") or []
    if declared:
        lines.append(f"Declared deps: {', '.join(map(str, declared))}")
    if missing:
        lines.append(f"Missing deps: {', '.join(map(str, missing))}")
    if installed:
        lines.append(f"Installed deps: {', '.join(map(str, installed))}")
    if install_errors:
        lines.append(f"Install errors: {', '.join(map(str, install_errors))}")
    return lines


def render_exp_plugin_settings_form(plugin):
    category = getattr(plugin, "settings_category", None)
    settings = getattr(plugin, "required_settings", None)
    if not category or not settings:
        return
    if not isinstance(settings, dict):
        with st.expander("Settings", expanded=False):
            st.warning("Settings schema invalid (expected a dictionary).")
        return

    with st.expander("Settings", expanded=False):
        current_settings = exp_get_plugin_settings(category)
        new_settings = {}
        has_fields = False

        for key, info in settings.items():
            input_type = info.get("type", "text")
            label = info.get("label", key)
            desc = info.get("description", "")
            default_value = current_settings.get(key, info.get("default", ""))

            if input_type == "button":
                if st.button(label, key=f"exp_{plugin.name}_{category}_{key}_button"):
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
                    key=f"exp_{plugin.name}_{category}_{key}"
                )
            elif input_type == "file":
                uploaded_file = st.file_uploader(
                    label,
                    type=["json"],
                    key=f"exp_{plugin.name}_{category}_{key}"
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
                    key=f"exp_{plugin.name}_{category}_{key}"
                )
            elif input_type == "checkbox":
                is_checked = (
                    default_value if isinstance(default_value, bool)
                    else str(default_value).lower() in ("true", "1", "yes")
                )
                new_value = st.checkbox(
                    label,
                    value=is_checked,
                    help=desc,
                    key=f"exp_{plugin.name}_{category}_{key}"
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
                        key=f"exp_{plugin.name}_{category}_{key}"
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
                        key=f"exp_{plugin.name}_{category}_{key}"
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
                    key=f"exp_{plugin.name}_{category}_{key}"
                )
            else:
                new_value = st.text_input(
                    label,
                    value=str(default_value),
                    help=desc,
                    key=f"exp_{plugin.name}_{category}_{key}"
                )

            new_settings[key] = new_value

        if has_fields and st.button(f"Save {category} Settings", key=f"exp_save_{plugin.name}_{category}"):
            exp_save_plugin_settings(category, new_settings)
            st.success(f"{category} settings saved.")
            st.rerun()


def render_exp_platform_settings_form(platform_key: str, required: dict):
    if not required:
        return

    with st.expander("Settings", expanded=False):
        current_settings = exp_get_platform_settings(platform_key)
        new_settings = {}
        has_fields = False

        for key, info in required.items():
            input_type = info.get("type", "text")
            label = info.get("label", key)
            desc = info.get("description", "")
            default_value = current_settings.get(key, info.get("default", ""))

            has_fields = True

            if input_type == "password":
                new_value = st.text_input(
                    label,
                    value=str(default_value),
                    help=desc,
                    type="password",
                    key=f"exp_platform_{platform_key}_{key}"
                )
            elif input_type == "file":
                uploaded_file = st.file_uploader(
                    label,
                    type=["json"],
                    key=f"exp_platform_{platform_key}_{key}"
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
                    key=f"exp_platform_{platform_key}_{key}"
                )
            elif input_type in ("checkbox", "boolean", "bool"):
                is_checked = (
                    default_value if isinstance(default_value, bool)
                    else str(default_value).lower() in ("true", "1", "yes")
                )
                new_value = st.checkbox(
                    label,
                    value=is_checked,
                    help=desc,
                    key=f"exp_platform_{platform_key}_{key}"
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
                        key=f"exp_platform_{platform_key}_{key}"
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
                        key=f"exp_platform_{platform_key}_{key}"
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
                    key=f"exp_platform_{platform_key}_{key}"
                )
            else:
                new_value = st.text_input(
                    label,
                    value=str(default_value),
                    help=desc,
                    key=f"exp_platform_{platform_key}_{key}"
                )

            new_settings[key] = new_value

        if has_fields and st.button(f"Save {platform_key} Settings", key=f"exp_save_platform_{platform_key}"):
            exp_save_platform_settings(platform_key, new_settings)
            st.success("Platform settings saved.")
            st.rerun()


def render_platform_controls(
    platform,
    redis_client,
    *,
    start_platform_fn,
    stop_platform_fn,
    wipe_memory_platform_data_fn,
):
    category     = platform["label"]
    key          = platform["key"]
    required     = platform["required"]
    short_name   = category.replace(" Settings", "").strip()
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
        value=is_running,
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
        start_platform_fn(key)
        redis_client.set(state_key, "true")
        st.success(f"{short_name} started.")
        st.rerun()

    # --- TURNING OFF ---
    elif not is_enabled and is_running:
        stop_platform_fn(key)
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
                    key=f"{category}_{setting_key}"
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
                    key=f"{category}_{setting_key}"
                )

            # store back (Redis expects strings later)
            new_settings[setting_key] = new_val

        elif input_type == "password":
            new_val = st.text_input(
                label, value=str(current_val), help=desc, type="password",
                key=f"{category}_{setting_key}"
            )
            new_settings[setting_key] = new_val

        elif input_type == "checkbox":
            new_val = st.checkbox(
                label, value=_to_bool(current_val), help=desc,
                key=f"{category}_{setting_key}"
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
                help=desc, key=f"{category}_{setting_key}"
            )
            new_settings[setting_key] = new_val

        else:
            # default: text
            new_val = st.text_input(
                label, value=str(current_val), help=desc,
                key=f"{category}_{setting_key}"
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
        st.caption("No platform settings to configure.")

    if key == "rss_platform":
        st.markdown("---")
        render_rss_feed_manager()
    if key == "memory_platform":
        st.markdown("---")
        st.subheader("Danger Zone")
        st.caption("Wipe all Memory Platform data (user docs, room docs, cursors, and runtime stats).")
        confirm_wipe = st.checkbox(
            "Confirm wipe all memory platform data",
            value=False,
            key=f"{key}_wipe_all_confirm",
        )
        if st.button(
            "Wipe All Memory Data",
            key=f"{key}_wipe_all_button",
            disabled=not confirm_wipe,
        ):
            wipe_result = wipe_memory_platform_data_fn()
            if wipe_result.get("ok"):
                deleted_total = int(wipe_result.get("deleted_total") or 0)
                deleted_by_pattern = wipe_result.get("deleted_by_pattern") or {}
                detail = (
                    f"user={int(deleted_by_pattern.get('mem:user:*') or 0)}, "
                    f"room={int(deleted_by_pattern.get('mem:room:*') or 0)}, "
                    f"cursor={int(deleted_by_pattern.get('mem:cursor:*') or 0)}, "
                    f"stats={int(deleted_by_pattern.get('mem:stats:memory_platform') or 0)}"
                )
                st.success(f"Wiped memory platform data. Deleted {deleted_total} keys ({detail}).")
                st.rerun()
            else:
                st.error(wipe_result.get("error") or "Failed to wipe memory platform data.")


def render_rss_feed_manager():
    st.subheader("Feeds")
    st.caption("Add feeds and customize delivery per feed. Leave targets blank to use default routing.")

    add_url = st.text_input("RSS Feed URL", key="rss_add_url")
    cols = st.columns([1, 1, 2])
    if cols[0].button("Add Feed", key="rss_add_btn"):
        feed_url = (add_url or "").strip()
        if not feed_url:
            st.warning("Please enter a feed URL.")
            st.stop()
        existing = get_all_feeds(redis_client) or {}
        if feed_url in existing:
            st.warning("That feed is already configured.")
            st.stop()
        try:
            parsed = feedparser.parse(feed_url)
        except Exception:
            parsed = None
        if not parsed or (getattr(parsed, "bozo", 0) and not getattr(parsed, "entries", None)):
            st.error("Failed to parse that feed URL.")
            st.stop()
        # Set last_ts=0 so the poller posts only the newest item once.
        set_feed(redis_client, feed_url, {"last_ts": 0.0, "enabled": True, "platforms": {}})
        st.success("Feed added.")
        st.rerun()

    feeds = get_all_feeds(redis_client) or {}
    if not feeds:
        st.info("No feeds configured yet.")
        return

    default_cfg = {
        "send_discord": True,
        "discord_channel_id": "",
        "send_irc": True,
        "irc_channel": "",
        "send_matrix": True,
        "matrix_room_id": "",
        "send_homeassistant": True,
        "ha_device_service": "",
        "send_ntfy": True,
        "send_telegram": True,
        "send_wordpress": True,
    }

    for idx, (feed_url, cfg) in enumerate(sorted(feeds.items(), key=lambda kv: kv[0].lower())):
        exp_key = f"rss_feed_{idx}"
        with st.expander(feed_url, expanded=False):
            enabled_key = f"{exp_key}_enabled"
            enabled_val = st.checkbox("Enabled", value=cfg.get("enabled", True), key=enabled_key)

            platforms = cfg.get("platforms") or {}

            # Discord
            discord_override = platforms.get("discord") or {}
            discord_enabled = st.checkbox(
                "Send to Discord",
                value=discord_override.get("enabled", default_cfg["send_discord"]),
                key=f"{exp_key}_discord_enabled",
            )
            discord_channel_id = st.text_input(
                "Discord Channel ID (override)",
                value=(discord_override.get("targets") or {}).get("channel_id", ""),
                placeholder=default_cfg["discord_channel_id"],
                key=f"{exp_key}_discord_channel_id",
            )

            # IRC
            irc_override = platforms.get("irc") or {}
            irc_enabled = st.checkbox(
                "Send to IRC",
                value=irc_override.get("enabled", default_cfg["send_irc"]),
                key=f"{exp_key}_irc_enabled",
            )
            irc_channel = st.text_input(
                "IRC Channel (override)",
                value=(irc_override.get("targets") or {}).get("channel", ""),
                placeholder=default_cfg["irc_channel"],
                key=f"{exp_key}_irc_channel",
            )

            # Matrix
            matrix_override = platforms.get("matrix") or {}
            matrix_enabled = st.checkbox(
                "Send to Matrix",
                value=matrix_override.get("enabled", default_cfg["send_matrix"]),
                key=f"{exp_key}_matrix_enabled",
            )
            matrix_room_id = st.text_input(
                "Matrix Room ID or Alias (override)",
                value=(matrix_override.get("targets") or {}).get("room_id", ""),
                placeholder=default_cfg["matrix_room_id"],
                key=f"{exp_key}_matrix_room_id",
            )

            # Home Assistant
            ha_override = platforms.get("homeassistant") or {}
            ha_enabled = st.checkbox(
                "Send to Home Assistant Notifications",
                value=ha_override.get("enabled", default_cfg["send_homeassistant"]),
                key=f"{exp_key}_ha_enabled",
            )
            ha_device = st.text_input(
                "HA Mobile Notify Service (optional override)",
                value=(ha_override.get("targets") or {}).get("device_service", ""),
                placeholder=default_cfg["ha_device_service"],
                key=f"{exp_key}_ha_device_service",
            )
            ha_persistent = (ha_override.get("targets") or {}).get("persistent")
            if isinstance(ha_persistent, str):
                ha_persistent = ha_persistent.strip().lower() in ("1", "true", "yes", "on")
            if ha_persistent is True:
                ha_persist_choice = "Force on"
            elif ha_persistent is False:
                ha_persist_choice = "Force off"
            else:
                ha_persist_choice = "Use default"
            ha_persist_choice = st.selectbox(
                "HA Persistent Notification",
                options=["Use default", "Force on", "Force off"],
                index=["Use default", "Force on", "Force off"].index(ha_persist_choice),
                key=f"{exp_key}_ha_persist_choice",
            )

            # Other notifiers
            ntfy_override = platforms.get("ntfy") or {}
            ntfy_enabled = st.checkbox(
                "Send to Ntfy",
                value=ntfy_override.get("enabled", default_cfg["send_ntfy"]),
                key=f"{exp_key}_ntfy_enabled",
            )
            telegram_override = platforms.get("telegram") or {}
            telegram_enabled = st.checkbox(
                "Send to Telegram",
                value=telegram_override.get("enabled", default_cfg["send_telegram"]),
                key=f"{exp_key}_telegram_enabled",
            )
            wp_override = platforms.get("wordpress") or {}
            wp_enabled = st.checkbox(
                "Send to WordPress",
                value=wp_override.get("enabled", default_cfg["send_wordpress"]),
                key=f"{exp_key}_wp_enabled",
            )

            save_cols = st.columns([1, 1, 2])
            if save_cols[0].button("Save Feed Settings", key=f"{exp_key}_save"):
                new_platforms = {}

                if discord_enabled != default_cfg["send_discord"] or discord_channel_id:
                    new_platforms["discord"] = {
                        "enabled": discord_enabled,
                        "targets": {"channel_id": discord_channel_id} if discord_channel_id else {},
                    }
                if irc_enabled != default_cfg["send_irc"] or irc_channel:
                    new_platforms["irc"] = {
                        "enabled": irc_enabled,
                        "targets": {"channel": irc_channel} if irc_channel else {},
                    }
                if matrix_enabled != default_cfg["send_matrix"] or matrix_room_id:
                    new_platforms["matrix"] = {
                        "enabled": matrix_enabled,
                        "targets": {"room_id": matrix_room_id} if matrix_room_id else {},
                    }
                if ha_enabled != default_cfg["send_homeassistant"] or ha_device or ha_persist_choice != "Use default":
                    targets = {}
                    if ha_device:
                        targets["device_service"] = ha_device
                    if ha_persist_choice == "Force on":
                        targets["persistent"] = True
                    elif ha_persist_choice == "Force off":
                        targets["persistent"] = False
                    new_platforms["homeassistant"] = {
                        "enabled": ha_enabled,
                        "targets": targets,
                    }
                if ntfy_enabled != default_cfg["send_ntfy"]:
                    new_platforms["ntfy"] = {"enabled": ntfy_enabled, "targets": {}}
                if telegram_enabled != default_cfg["send_telegram"]:
                    new_platforms["telegram"] = {"enabled": telegram_enabled, "targets": {}}
                if wp_enabled != default_cfg["send_wordpress"]:
                    new_platforms["wordpress"] = {"enabled": wp_enabled, "targets": {}}

                update_feed(redis_client, feed_url, {"enabled": enabled_val, "platforms": new_platforms})
                st.success("Feed settings saved.")
                st.rerun()

            if save_cols[1].button("Remove Feed", key=f"{exp_key}_remove"):
                delete_feed(redis_client, feed_url)
                st.success("Feed removed.")
                st.rerun()


def _platform_sort_name(p):
    return (p.get("label") or p.get("category") or p.get("key") or "").lower()


def render_platforms_panel(
    *,
    platform_registry,
    redis_client,
    start_platform_fn,
    stop_platform_fn,
    wipe_memory_platform_data_fn,
    auto_connected=None,
):
    st.subheader("Platforms")
    for platform in sorted(platform_registry, key=_platform_sort_name):
        label = platform.get("label") or platform.get("category") or platform.get("key")
        with st.expander(label, expanded=False):
            render_platform_controls(
                platform,
                redis_client,
                start_platform_fn=start_platform_fn,
                stop_platform_fn=stop_platform_fn,
                wipe_memory_platform_data_fn=wipe_memory_platform_data_fn,
            )
