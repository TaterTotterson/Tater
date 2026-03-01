import base64
import json
from io import BytesIO

import streamlit as st
from PIL import Image

_redis_client = None


def configure_chat_helpers(*, redis_client) -> None:
    global _redis_client
    _redis_client = redis_client


def _require_redis_client():
    if _redis_client is None:
        raise RuntimeError("webui_chat is not configured. Call configure_chat_helpers(...) first.")
    return _redis_client


def save_message(role, username, content):
    redis_client = _require_redis_client()

    message_data = {
        "role": role,
        "username": username,
        "content": content,
    }

    history_key = "webui:chat_history"
    redis_client.rpush(history_key, json.dumps(message_data))

    try:
        max_store = int(redis_client.get("tater:max_store") or 20)
    except (ValueError, TypeError):
        max_store = 20

    if max_store > 0:
        redis_client.ltrim(history_key, -max_store, -1)


def _media_type_from_mimetype(mimetype: str) -> str:
    mm = str(mimetype or "").strip().lower()
    if mm.startswith("image/"):
        return "image"
    if mm.startswith("audio/"):
        return "audio"
    if mm.startswith("video/"):
        return "video"
    return "file"


def load_chat_history_tail(n: int):
    redis_client = _require_redis_client()
    if n <= 0:
        return []
    raw = redis_client.lrange("webui:chat_history", -n, -1)
    out = []
    for msg in raw:
        try:
            out.append(json.loads(msg))
        except Exception:
            continue
    return out


def load_chat_history():
    redis_client = _require_redis_client()
    history = redis_client.lrange("webui:chat_history", 0, -1)
    return [json.loads(msg) for msg in history]


def clear_chat_history():
    redis_client = _require_redis_client()
    redis_client.delete("webui:chat_history")
    st.session_state.pop("chat_messages", None)


def load_default_tater_avatar():
    return Image.open("images/tater.png")


def get_tater_avatar():
    redis_client = _require_redis_client()
    avatar_b64 = redis_client.get("tater:avatar")
    if avatar_b64:
        try:
            avatar_bytes = base64.b64decode(avatar_b64)
            return Image.open(BytesIO(avatar_bytes))
        except Exception:
            redis_client.delete("tater:avatar")
    return load_default_tater_avatar()


def get_chat_settings():
    redis_client = _require_redis_client()
    settings = redis_client.hgetall("chat_settings")
    return {
        "username": settings.get("username", "User"),
        "avatar": settings.get("avatar", None),
    }


def save_chat_settings(username, avatar=None):
    redis_client = _require_redis_client()
    mapping = {"username": username}
    if avatar is not None:
        mapping["avatar"] = avatar
    redis_client.hset("chat_settings", mapping=mapping)


def load_avatar_image(avatar_b64):
    redis_client = _require_redis_client()
    try:
        avatar_bytes = base64.b64decode(avatar_b64)
        return Image.open(BytesIO(avatar_bytes))
    except Exception:
        redis_client.hdel("chat_settings", "avatar")
        return None


def build_system_prompt():
    return (
        "You are a WebUI-savvy AI assistant.\n"
        "Keep replies concise and clear.\n"
    )


def _to_template_msg(role, content):
    """
    Return a dict shaped for the Jinja template:
      - string -> {"role": role, "content": "text"}
      - image  -> {"role": role, "content": "[Image attached]"}
      - audio  -> {"role": role, "content": "[Audio attached]"}
      - video  -> {"role": role, "content": "[Video attached]"}
      - file   -> {"role": role, "content": "[File attached] name (mimetype, size)"}
      - plugin_call -> stringify call as assistant text
      - plugin_response -> include final responses (skip waiting lines)
    """

    if isinstance(content, dict) and content.get("marker") == "plugin_wait":
        return None

    if isinstance(content, dict) and content.get("marker") == "plugin_response":
        phase = content.get("phase", "final")
        if phase != "final":
            return None

        payload = content.get("content")

        if isinstance(payload, str):
            txt = payload.strip()
            if len(txt) > 4000:
                txt = txt[:4000] + " ..."
            return {"role": "assistant", "content": txt}

        if isinstance(payload, dict) and payload.get("type") in ("image", "audio", "video"):
            kind = payload.get("type")
            name = payload.get("name") or ""
            return {"role": "assistant", "content": f"[{kind.capitalize()} from tool]{f' {name}' if name else ''}".strip()}

        if isinstance(payload, dict):
            for key in ("summary", "text", "message", "content"):
                if isinstance(payload.get(key), str) and payload.get(key).strip():
                    txt = payload[key].strip()
                    if len(txt) > 4000:
                        txt = txt[:4000] + " ..."
                    return {"role": "assistant", "content": txt}

            try:
                compact = json.dumps(payload, ensure_ascii=False)
                if len(compact) > 2000:
                    compact = compact[:2000] + " ..."
                return {"role": "assistant", "content": compact}
            except Exception:
                return None

    if isinstance(content, dict) and content.get("marker") == "plugin_call":
        as_text = json.dumps(
            {
                "function": content.get("plugin"),
                "arguments": content.get("arguments", {}),
            },
            indent=2,
        )
        if role == "assistant":
            return {"role": "assistant", "content": as_text}
        return {"role": role, "content": as_text}

    if isinstance(content, dict) and content.get("type") == "image":
        return {"role": role, "content": "[Image attached]"}
    if isinstance(content, dict) and content.get("type") == "audio":
        return {"role": role, "content": "[Audio attached]"}
    if isinstance(content, dict) and content.get("type") == "video":
        return {"role": role, "content": "[Video attached]"}
    if isinstance(content, dict) and content.get("type") == "file":
        name = content.get("name") or "file"
        mimetype = content.get("mimetype") or ""
        size = content.get("size") or ""
        return {"role": role, "content": f"[File attached] {name} ({mimetype}, {size} bytes)"}

    if isinstance(content, str):
        return {"role": role, "content": content}
    return {"role": role, "content": str(content)}


def _enforce_user_assistant_alternation(loop_messages):
    """
    Enforces strict alternation and requires the first message to be 'user'.
    """
    merged = []
    for m in loop_messages:
        if not m:
            continue
        if not merged:
            merged.append(m)
            continue
        if merged[-1]["role"] == m["role"]:
            a, b = merged[-1]["content"], m["content"]
            if isinstance(a, str) and isinstance(b, str):
                merged[-1]["content"] = (a + "\n\n" + b).strip()
            elif isinstance(a, list) and isinstance(b, list):
                merged[-1]["content"] = a + b
            else:
                merged[-1]["content"] = (
                    (a if isinstance(a, str) else str(a))
                    + "\n\n"
                    + (b if isinstance(b, str) else str(b))
                ).strip()
        else:
            merged.append(m)

    if merged and merged[0]["role"] != "user":
        merged.insert(0, {"role": "user", "content": ""})

    return merged
