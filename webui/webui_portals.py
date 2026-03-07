import json
import os
import re
import secrets
import time
from typing import Any, Dict, List, Optional

import feedparser
import redis
import streamlit as st
from rss_store import get_all_feeds, set_feed, update_feed, delete_feed


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
    wipe_memory_core_data_fn,
    surface_kind: str = "portal",
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

    if key == "rss_core":
        st.markdown("---")
        render_rss_feed_manager()
    if key == "memory_core":
        st.markdown("---")
        st.subheader("Danger Zone")
        st.caption(f"Wipe all Memory {surface_text.capitalize()} data (user docs, room docs, cursors, and runtime stats).")
        confirm_wipe = st.checkbox(
            f"Confirm wipe all memory {surface_text} data",
            value=False,
            key=f"{key}_wipe_all_confirm",
        )
        if st.button(
            "Wipe All Memory Data",
            key=f"{key}_wipe_all_button",
            disabled=not confirm_wipe,
        ):
            wipe_result = wipe_memory_core_data_fn()
            if wipe_result.get("ok"):
                deleted_total = int(wipe_result.get("deleted_total") or 0)
                deleted_by_pattern = wipe_result.get("deleted_by_pattern") or {}
                stats_key = "mem:stats:memory_core"
                detail = (
                    f"user={int(deleted_by_pattern.get('mem:user:*') or 0)}, "
                    f"room={int(deleted_by_pattern.get('mem:room:*') or 0)}, "
                    f"cursor={int(deleted_by_pattern.get('mem:cursor:*') or 0)}, "
                    f"stats={int(deleted_by_pattern.get(stats_key) or 0)}"
                )
                st.success(f"Wiped memory {surface_text} data. Deleted {deleted_total} keys ({detail}).")
                st.rerun()
            else:
                st.error(wipe_result.get("error") or f"Failed to wipe memory {surface_text} data.")


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
        set_feed(redis_client, feed_url, {"last_ts": 0.0, "enabled": True, "portals": {}})
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

            platforms = cfg.get("portals") or {}

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

                update_feed(redis_client, feed_url, {"enabled": enabled_val, "portals": new_platforms})
                st.success("Feed settings saved.")
                st.rerun()

            if save_cols[1].button("Remove Feed", key=f"{exp_key}_remove"):
                delete_feed(redis_client, feed_url)
                st.success("Feed removed.")
                st.rerun()


def _portal_sort_name(p):
    return (p.get("label") or p.get("category") or p.get("key") or "").lower()


def render_portals_panel(
    *,
    portal_registry,
    redis_client,
    start_portal_fn,
    stop_portal_fn,
    wipe_memory_core_data_fn,
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
                wipe_memory_core_data_fn=wipe_memory_core_data_fn,
            )
