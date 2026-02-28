import base64
import json
from typing import Any, Callable

import streamlit as st


def render_webui_settings(
    *,
    redis_client,
    redis_blob_client,
    get_chat_settings_fn: Callable[[], dict],
    save_chat_settings_fn: Callable[..., None],
    clear_chat_history_fn: Callable[[], None],
    webui_attach_max_mb_each: int,
    webui_attach_max_mb_total: int,
    webui_attach_ttl_seconds: int,
    file_index_key: str,
    file_blob_key_prefix: str,
) -> None:
    st.subheader("WebUI Settings")
    current_chat = get_chat_settings_fn()
    username = st.text_input("Username", value=current_chat["username"], key="webui_username")

    raw_display = redis_client.get("tater:max_display") or 8
    try:
        display_count = int(float(raw_display))
    except (TypeError, ValueError):
        display_count = 8

    new_display = st.number_input(
        "Messages Shown in WebUI",
        min_value=1,
        max_value=500,
        value=display_count,
        step=1,
        format="%d",
        key="webui_display_count",
    )

    show_speed_default = (redis_client.get("tater:show_speed_stats") or "true").lower() == "true"
    show_speed = st.checkbox("Show tokens/sec", value=show_speed_default, key="show_speed_stats")

    uploaded_avatar = st.file_uploader(
        "Upload your avatar", type=["png", "jpg", "jpeg"], key="avatar_uploader"
    )

    if st.button("Save WebUI Settings", key="save_webui_settings"):
        if uploaded_avatar is not None:
            avatar_bytes = uploaded_avatar.read()
            avatar_b64 = base64.b64encode(avatar_bytes).decode("utf-8")
            save_chat_settings_fn(username, avatar_b64)
        else:
            save_chat_settings_fn(username)

        redis_client.set("tater:max_display", new_display)
        redis_client.set("tater:show_speed_stats", "true" if show_speed else "false")
        st.success("WebUI settings updated.")

    if st.button("Clear Chat History", key="clear_history"):
        clear_chat_history_fn()
        st.success("Chat history cleared.")

    st.markdown("---")
    st.subheader("Attachments")
    st.caption("Uploaded files are stored in Redis. Images/audio/video render inline. Other files appear as attachments with a download button.")
    ttl_label = "none" if webui_attach_ttl_seconds <= 0 else f"{webui_attach_ttl_seconds}s"
    st.caption(
        f"Per-file limit: {webui_attach_max_mb_each}MB • Per-message total limit: {webui_attach_max_mb_total}MB • TTL: {ttl_label}"
    )

    if st.button("Clear Stored Attachment Blobs", key="clear_attachment_blobs"):
        ids = redis_client.lrange(file_index_key, 0, -1)
        if ids:
            pipe = redis_blob_client.pipeline()
            for fid in ids:
                pipe.delete(f"{file_blob_key_prefix}{fid}")
            pipe.execute()

            redis_client.delete(file_index_key)

        st.success("Attachment blobs cleared (chat history entries remain).")
        st.rerun()


def render_web_search_settings(*, redis_client) -> None:
    st.subheader("Web Search")
    st.caption("Used by kernel `search_web` for research and current information.")

    legacy_web_search = redis_client.hgetall("plugin_settings:Web Search") or {}
    web_search_api_default = (
        redis_client.get("tater:web_search:google_api_key")
        or legacy_web_search.get("GOOGLE_API_KEY")
        or ""
    )
    web_search_cx_default = (
        redis_client.get("tater:web_search:google_cx")
        or legacy_web_search.get("GOOGLE_CX")
        or ""
    )

    web_search_api = st.text_input(
        "Google API Key",
        value=web_search_api_default,
        type="password",
        key="web_search_google_api_key",
    )
    web_search_cx = st.text_input(
        "Google Search Engine ID (CX)",
        value=web_search_cx_default,
        key="web_search_google_cx",
    )

    if st.button("Save Web Search Settings", key="save_web_search_settings"):
        redis_client.set("tater:web_search:google_api_key", web_search_api.strip())
        redis_client.set("tater:web_search:google_cx", web_search_cx.strip())
        st.success("Web Search settings updated.")


def render_homeassistant_settings(
    *,
    get_homeassistant_settings_fn: Callable[[], dict],
    save_homeassistant_settings_fn: Callable[[str, str], None],
) -> None:
    st.subheader("Home Assistant Settings")
    current_settings = get_homeassistant_settings_fn()

    base_url = st.text_input(
        "Home Assistant Base URL",
        value=current_settings["HA_BASE_URL"],
        help="Example: http://homeassistant.local:8123 or http://192.168.1.50:8123",
        key="homeassistant_base_url",
    )
    token = st.text_input(
        "Home Assistant Long-Lived Access Token",
        value=current_settings["HA_TOKEN"],
        help="Create in Home Assistant Profile → Long-Lived Access Tokens.",
        type="password",
        key="homeassistant_token",
    )

    if st.button("Save Home Assistant Settings", key="save_homeassistant_settings"):
        save_homeassistant_settings_fn(base_url.strip(), token.strip())
        st.success("Home Assistant settings updated.")


def render_vision_settings(
    *,
    get_vision_settings_fn: Callable[[], dict],
    save_vision_settings_fn: Callable[[str, str, str], None],
) -> None:
    st.subheader("Vision Settings")
    current_settings = get_vision_settings_fn()

    api_base = st.text_input(
        "Vision API Base URL",
        value=current_settings["api_base"],
        help="OpenAI-compatible base URL for vision calls (example: http://127.0.0.1:1234).",
        key="vision_api_base",
    )
    model = st.text_input(
        "Vision Model",
        value=current_settings["model"],
        help="Shared vision model used by all vision-enabled plugins.",
        key="vision_model",
    )
    api_key = st.text_input(
        "Vision API Key (optional)",
        value=current_settings["api_key"],
        help="Leave blank for local stacks that do not require authentication.",
        type="password",
        key="vision_api_key",
    )

    if st.button("Save Vision Settings", key="save_vision_settings"):
        save_vision_settings_fn(api_base.strip(), model.strip(), api_key.strip())
        st.success("Vision settings updated.")


def render_emoji_responder_settings(
    *,
    get_emoji_responder_settings_fn: Callable[[], dict],
    save_emoji_responder_settings_fn: Callable[..., None],
) -> None:
    st.subheader("Emoji Responder Settings")
    settings = get_emoji_responder_settings_fn()

    enable_on_reaction_add = st.checkbox(
        "Enable reaction-chain mode (Discord)",
        value=bool(settings["enable_on_reaction_add"]),
        help="When a user reacts to a Discord message, optionally add one matching emoji reaction.",
        key="emoji_enable_on_reaction_add",
    )
    enable_auto_reaction_on_reply = st.checkbox(
        "Enable auto reactions on replies",
        value=bool(settings["enable_auto_reaction_on_reply"]),
        help="When the assistant replies on Discord/Telegram/Matrix, occasionally add a matching emoji reaction.",
        key="emoji_enable_auto_reaction_on_reply",
    )
    reaction_chain_chance_percent = int(
        st.number_input(
            "Reaction-chain chance (%)",
            min_value=0,
            max_value=100,
            value=int(settings["reaction_chain_chance_percent"]),
            step=1,
            format="%d",
            key="emoji_reaction_chain_chance_percent",
        )
    )
    reply_reaction_chance_percent = int(
        st.number_input(
            "Reply reaction chance (%)",
            min_value=0,
            max_value=100,
            value=int(settings["reply_reaction_chance_percent"]),
            step=1,
            format="%d",
            key="emoji_reply_reaction_chance_percent",
        )
    )
    reaction_chain_cooldown_seconds = int(
        st.number_input(
            "Reaction-chain cooldown (seconds)",
            min_value=0,
            max_value=86_400,
            value=int(settings["reaction_chain_cooldown_seconds"]),
            step=1,
            format="%d",
            key="emoji_reaction_chain_cooldown_seconds",
        )
    )
    reply_reaction_cooldown_seconds = int(
        st.number_input(
            "Reply reaction cooldown (seconds)",
            min_value=0,
            max_value=86_400,
            value=int(settings["reply_reaction_cooldown_seconds"]),
            step=1,
            format="%d",
            key="emoji_reply_reaction_cooldown_seconds",
        )
    )
    min_message_length = int(
        st.number_input(
            "Minimum message length",
            min_value=0,
            max_value=200,
            value=int(settings["min_message_length"]),
            step=1,
            format="%d",
            key="emoji_min_message_length",
        )
    )

    if st.button("Save Emoji Settings", key="save_emoji_settings"):
        save_emoji_responder_settings_fn(
            enable_on_reaction_add=enable_on_reaction_add,
            enable_auto_reaction_on_reply=enable_auto_reaction_on_reply,
            reaction_chain_chance_percent=reaction_chain_chance_percent,
            reply_reaction_chance_percent=reply_reaction_chance_percent,
            reaction_chain_cooldown_seconds=reaction_chain_cooldown_seconds,
            reply_reaction_cooldown_seconds=reply_reaction_cooldown_seconds,
            min_message_length=min_message_length,
        )
        st.success("Emoji settings updated.")


def render_tater_settings(*, redis_client, first_name: str, last_name: str) -> None:
    def _read_non_negative_int_setting(key: str, default: int) -> int:
        raw = redis_client.get(key)
        try:
            value = int(str(raw).strip()) if raw is not None else int(default)
        except Exception:
            value = int(default)
        if value < 0:
            return 0
        return value

    st.subheader(f"{first_name} Settings")
    stored_count = _read_non_negative_int_setting("tater:max_store", 20)
    llm_count = max(1, _read_non_negative_int_setting("tater:max_llm", 8))
    default_first = redis_client.get("tater:first_name") or first_name
    default_last = redis_client.get("tater:last_name") or last_name
    default_personality = redis_client.get("tater:personality") or ""
    first_input = st.text_input("First Name", value=default_first, key="tater_first_name")
    last_input = st.text_input("Last Name", value=default_last, key="tater_last_name")

    personality_input = st.text_area(
        "Personality / Style (optional)",
        value=default_personality,
        help=(
            "Describe how you want Tater to talk and behave. "
            "Examples:\n"
            "- A calm and confident starship captain.\n"
            "- Captain Jahn-Luek Picard of the Starship Enterprise.\n"
            "- A laid-back hippy stoner who still explains things clearly."
        ),
        height=120,
        key="tater_personality",
    )

    uploaded_tater_avatar = st.file_uploader(
        f"Upload {first_input}'s avatar", type=["png", "jpg", "jpeg"], key="tater_avatar_uploader"
    )
    if uploaded_tater_avatar is not None:
        avatar_bytes = uploaded_tater_avatar.read()
        avatar_b64 = base64.b64encode(avatar_bytes).decode("utf-8")
        redis_client.set("tater:avatar", avatar_b64)

    new_store = st.number_input("Max Stored Messages (0 = unlimited)", min_value=0, value=stored_count, key="tater_store_limit")
    if new_store == 0:
        st.warning("⚠️ Unlimited history enabled — this may grow Redis memory usage over time.")
    new_llm = st.number_input("Messages Sent to LLM", min_value=1, value=llm_count, key="tater_llm_limit")
    if new_store > 0 and new_llm > new_store:
        st.warning("⚠️ You're trying to send more messages to LLM than you’re storing. Consider increasing Max Stored Messages.")

    if st.button("Save Tater Settings", key="save_tater_settings"):
        redis_client.set("tater:max_store", new_store)
        redis_client.set("tater:max_llm", new_llm)
        redis_client.set("tater:first_name", first_input)
        redis_client.set("tater:last_name", last_input)
        redis_client.set("tater:personality", personality_input)
        st.success("Tater settings updated.")
        st.rerun()


def render_admin_gating_settings(
    *,
    redis_client,
    admin_gate_key: str,
    get_admin_only_plugins_fn: Callable[[Any], list[str]],
    get_registry_fn: Callable[[], dict],
) -> None:
    st.subheader("Admin Tool Gating")
    st.caption(
        "Only the configured admin user can run these plugins on Discord, Telegram, Matrix, and IRC. "
        "If a platform’s admin user setting is blank, these tools are disabled for everyone on that platform."
    )

    registry = get_registry_fn() or {}
    plugin_ids = sorted(registry.keys())
    current = sorted(get_admin_only_plugins_fn(redis_client))
    known_current = [p for p in current if p in plugin_ids]
    unknown_current = [p for p in current if p not in plugin_ids]

    using_default_plugin_list = redis_client.get(admin_gate_key) is None
    if using_default_plugin_list:
        st.info("Currently using the default admin-only plugin list. Save to customize.")

    if unknown_current:
        st.warning(f"Unknown plugin IDs currently stored: {', '.join(unknown_current)}")

    selected = st.multiselect(
        "Admin-only plugins (by plugin id)",
        options=plugin_ids,
        default=known_current,
        help="Selected plugins can only be run by the admin user on Discord/Telegram/Matrix/IRC.",
        key="admin_gate_plugins",
    )
    col1, col2 = st.columns(2)
    if col1.button("Save Admin Tool Gating", key="save_admin_gating"):
        redis_client.set(admin_gate_key, json.dumps(selected))
        st.success("Admin tool gating saved.")

    if col2.button("Reset to Defaults", key="reset_admin_gating"):
        redis_client.delete(admin_gate_key)
        st.success("Admin tool gating reset to defaults.")
        st.rerun()


def render_settings_page(
    *,
    redis_client,
    redis_blob_client,
    first_name: str,
    last_name: str,
    get_registry_fn: Callable[[], dict],
    admin_gate_key: str,
    get_admin_only_plugins_fn: Callable[[Any], list[str]],
    get_chat_settings_fn: Callable[[], dict],
    save_chat_settings_fn: Callable[..., None],
    clear_chat_history_fn: Callable[[], None],
    get_homeassistant_settings_fn: Callable[[], dict],
    save_homeassistant_settings_fn: Callable[[str, str], None],
    get_vision_settings_fn: Callable[[], dict],
    save_vision_settings_fn: Callable[[str, str, str], None],
    get_emoji_responder_settings_fn: Callable[[], dict],
    save_emoji_responder_settings_fn: Callable[..., None],
    webui_attach_max_mb_each: int,
    webui_attach_max_mb_total: int,
    webui_attach_ttl_seconds: int,
    file_index_key: str,
    file_blob_key_prefix: str,
    render_cerberus_settings_fn: Callable[[], None],
    render_cerberus_metrics_dashboard_fn: Callable[..., None],
    render_cerberus_data_tools_fn: Callable[..., None],
) -> None:
    st.title("Settings")
    tab_general, tab_integrations, tab_emoji, tab_cerberus, tab_advanced = st.tabs(
        ["General", "Integrations", "Emoji", "Cerberus", "Advanced"]
    )

    with tab_general:
        render_webui_settings(
            redis_client=redis_client,
            redis_blob_client=redis_blob_client,
            get_chat_settings_fn=get_chat_settings_fn,
            save_chat_settings_fn=save_chat_settings_fn,
            clear_chat_history_fn=clear_chat_history_fn,
            webui_attach_max_mb_each=webui_attach_max_mb_each,
            webui_attach_max_mb_total=webui_attach_max_mb_total,
            webui_attach_ttl_seconds=webui_attach_ttl_seconds,
            file_index_key=file_index_key,
            file_blob_key_prefix=file_blob_key_prefix,
        )
        st.markdown("---")
        render_tater_settings(redis_client=redis_client, first_name=first_name, last_name=last_name)

    with tab_integrations:
        render_web_search_settings(redis_client=redis_client)
        st.markdown("---")
        render_homeassistant_settings(
            get_homeassistant_settings_fn=get_homeassistant_settings_fn,
            save_homeassistant_settings_fn=save_homeassistant_settings_fn,
        )
        st.markdown("---")
        render_vision_settings(
            get_vision_settings_fn=get_vision_settings_fn,
            save_vision_settings_fn=save_vision_settings_fn,
        )

    with tab_emoji:
        render_emoji_responder_settings(
            get_emoji_responder_settings_fn=get_emoji_responder_settings_fn,
            save_emoji_responder_settings_fn=save_emoji_responder_settings_fn,
        )

    with tab_cerberus:
        cerberus_tab_settings, cerberus_tab_metrics, cerberus_tab_data = st.tabs(
            ["Cerberus", "Cerberus Metrics", "Cerberus Data"]
        )
        with cerberus_tab_settings:
            render_cerberus_settings_fn()
        with cerberus_tab_metrics:
            render_cerberus_metrics_dashboard_fn(key_prefix="cerberus_tab_dashboard", allow_controls=False)
        with cerberus_tab_data:
            render_cerberus_data_tools_fn(key_prefix="cerberus_tab_data")

    with tab_advanced:
        render_admin_gating_settings(
            redis_client=redis_client,
            admin_gate_key=admin_gate_key,
            get_admin_only_plugins_fn=get_admin_only_plugins_fn,
            get_registry_fn=get_registry_fn,
        )
