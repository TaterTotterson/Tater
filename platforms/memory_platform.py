import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import redis
from dotenv import load_dotenv

from helpers import extract_json, get_llm_client_from_env
from memory_platform_store import (
    cursor_key,
    load_doc,
    merge_doc_facts,
    merge_observation,
    normalize_fact_key,
    normalize_segment,
    resolve_user_doc_key,
    room_doc_key,
    save_doc,
    user_doc_key,
)

load_dotenv()

logger = logging.getLogger("memory_platform")
logger.setLevel(logging.INFO)

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True,
)

PLATFORM_SETTINGS = {
    "category": "Memory Platform Settings",
    "required": {
        "interval_seconds": {
            "label": "Interval (sec)",
            "type": "select",
            "default": "180",
            "options": [
                {"value": "60", "label": "1 minute"},
                {"value": "120", "label": "2 minutes"},
                {"value": "180", "label": "3 minutes"},
                {"value": "300", "label": "5 minutes"},
                {"value": "600", "label": "10 minutes"},
                {"value": "900", "label": "15 minutes"},
                {"value": "1800", "label": "30 minutes"},
                {"value": "3600", "label": "1 hour"},
            ],
            "description": "How often memory extraction runs.",
        },
        "lookback_limit": {
            "label": "Lookback Limit",
            "type": "number",
            "default": 80,
            "description": "Max new messages processed per scope each pass.",
        },
        "min_confidence": {
            "label": "Min Confidence",
            "type": "number",
            "default": 0.65,
            "description": "Minimum confidence required before storing facts.",
        },
        "extraction_max_tokens": {
            "label": "Extraction Max Tokens",
            "type": "number",
            "default": 2700,
            "description": "Max completion tokens for memory extraction.",
        },
        "cerberus_max_items": {
            "label": "Cerberus Memory Items",
            "type": "number",
            "default": 12,
            "description": "Max memory facts injected per scope (user and room).",
        },
        "cerberus_value_max_chars": {
            "label": "Cerberus Value Chars",
            "type": "number",
            "default": 288,
            "description": "Max characters per memory fact value in context.",
        },
        "cerberus_summary_max_chars": {
            "label": "Cerberus Summary Chars",
            "type": "number",
            "default": 2100,
            "description": "Max characters for user/room memory summaries in context.",
        },
        "write_user_memory": {
            "label": "Write User Memory",
            "type": "checkbox",
            "default": True,
            "description": (
                "Enable writes to durable user docs. With auto-link on: mem:user:identity:{identity_id}; "
                "with auto-link off: mem:user:{platform}:{user_id}."
            ),
        },
        "write_room_memory": {
            "label": "Write Room Memory",
            "type": "checkbox",
            "default": True,
            "description": "Enable writes to mem:room:{platform}:{room_id}.",
        },
        "auto_link_identities": {
            "label": "Auto-link Identity by Name",
            "type": "checkbox",
            "default": False,
            "description": "Automatically map matching usernames across platforms to one shared identity profile.",
        },
    },
}

_SUPPORTED_PLATFORMS = ("webui", "macos", "discord", "telegram", "irc", "matrix", "homeassistant", "homekit", "xbmc")
_AUTO_STATE_KEYS = {
    "macos": "macos_platform_running",
    "discord": "discord_platform_running",
    "telegram": "telegram_platform_running",
    "irc": "irc_platform_running",
    "matrix": "matrix_platform_running",
    "homeassistant": "homeassistant_platform_running",
    "homekit": "homekit_platform_running",
    "xbmc": "xbmc_platform_running",
}
_HISTORY_SCAN_SPECS = {
    "macos": ("tater:macos:session:*:history", "tater:macos:session:", ":history"),
    "discord": ("tater:channel:*:history", "tater:channel:", ":history"),
    "telegram": ("tater:telegram:*:history", "tater:telegram:", ":history"),
    "irc": ("tater:irc:*:history", "tater:irc:", ":history"),
    "matrix": ("tater:matrix:*:history", "tater:matrix:", ":history"),
    "homeassistant": ("tater:ha:session:*:history", "tater:ha:session:", ":history"),
    "homekit": ("tater:homekit:session:*:history", "tater:homekit:session:", ":history"),
    "xbmc": ("tater:xbmc:session:*:history", "tater:xbmc:session:", ":history"),
}
_MARKER_SKIP = {"plugin_wait"}
_TRANSIENT_FACT_PREFIXES = (
    "request_",
    "requests_",
    "requested_",
    "asking_",
    "asks_",
    "ask_",
    "follow_up_",
    "followup_",
    "needs_",
    "need_",
    "wants_",
    "want_",
    "do_",
    "did_",
    "is_",
    "are_",
    "was_",
    "were_",
)
_TRANSIENT_FACT_EXACT = {
    "latest_request",
    "current_request",
    "recent_request",
}
_USER_PROFILE_SCHEMA: Dict[str, Tuple[str, ...]] = {
    "demographic_information": (
        "age",
        "gender",
        "ethnicity",
        "occupation",
        "education",
    ),
    "physical_traits": (
        "height",
        "weight",
        "hair_color",
        "eye_color",
        "distinguishing_features",
    ),
    "personality_traits": (
        "temperament",
        "values_and_beliefs",
        "interests",
        "strengths_and_weaknesses",
    ),
    "behavioral_patterns": (
        "communication_style",
        "conflict_resolution",
        "social_interactions",
    ),
    "lifestyle_choices": (
        "daily_routine",
        "health_habits",
        "recreational_activities",
    ),
    "social_media_presence": (
        "platforms_used",
        "content_shared",
        "network",
    ),
    "life_experiences": (
        "major_life_events",
        "achievements",
        "challenges",
    ),
    "family_background": (
        "family_dynamics",
        "cultural_influences",
        "traditions",
    ),
    "mental_and_emotional_health": (
        "mental_well_being",
        "coping_mechanisms",
        "resilience",
    ),
    "goals_and_aspirations": (
        "short_term_goals",
        "long_term_aspirations",
    ),
}
_USER_ALLOWED_FACT_KEYS: Tuple[str, ...] = tuple(
    fact_key
    for fields in _USER_PROFILE_SCHEMA.values()
    for fact_key in fields
)
_USER_ALLOWED_FACT_KEY_SET = set(_USER_ALLOWED_FACT_KEYS)
_USER_SCHEMA_PROMPT_TEXT = "\n".join(
    f"- {category.replace('_', ' ').title()}: {', '.join(fields)}"
    for category, fields in _USER_PROFILE_SCHEMA.items()
)
_USER_ALLOWED_FACT_KEYS_PROMPT = ", ".join(_USER_ALLOWED_FACT_KEYS)
_ROOM_PROFILE_SCHEMA: Dict[str, Tuple[str, ...]] = {
    "room_identity": (
        "room_purpose",
        "shared_topics",
        "shared_projects",
        "inside_jokes",
    ),
    "communication_norms": (
        "communication_tone",
        "response_style",
        "conflict_norms",
    ),
    "shared_defaults": (
        "default_units",
        "default_timezone",
        "default_language",
        "decision_preferences",
    ),
    "collaboration_context": (
        "active_goals",
        "recurring_tasks",
        "shared_tools",
        "reference_links",
    ),
    "constraints_and_policies": (
        "shared_constraints",
        "moderation_preferences",
        "privacy_expectations",
    ),
}
_ROOM_ALLOWED_FACT_KEYS: Tuple[str, ...] = tuple(
    fact_key
    for fields in _ROOM_PROFILE_SCHEMA.values()
    for fact_key in fields
)
_ROOM_ALLOWED_FACT_KEY_SET = set(_ROOM_ALLOWED_FACT_KEYS)
_ROOM_SCHEMA_PROMPT_TEXT = "\n".join(
    f"- {category.replace('_', ' ').title()}: {', '.join(fields)}"
    for category, fields in _ROOM_PROFILE_SCHEMA.items()
)
_ROOM_ALLOWED_FACT_KEYS_PROMPT = ", ".join(_ROOM_ALLOWED_FACT_KEYS)


def _is_transient_fact_key(fact_key: Any) -> bool:
    key = normalize_fact_key(fact_key)
    if not key:
        return False
    if key in _TRANSIENT_FACT_EXACT:
        return True
    return any(key.startswith(prefix) for prefix in _TRANSIENT_FACT_PREFIXES)


def _canonical_user_fact_key(raw_key: Any) -> str:
    fact_key = normalize_fact_key(raw_key)
    if not fact_key:
        return ""
    if fact_key in _USER_ALLOWED_FACT_KEY_SET:
        return fact_key
    return ""


def _canonical_room_fact_key(raw_key: Any) -> str:
    fact_key = normalize_fact_key(raw_key)
    if not fact_key:
        return ""
    if fact_key in _ROOM_ALLOWED_FACT_KEY_SET:
        return fact_key
    return ""


def _scrub_transient_facts(doc: Dict[str, Any], *, now: Optional[float] = None) -> int:
    if not isinstance(doc, dict):
        return 0
    facts = doc.get("facts")
    if not isinstance(facts, dict):
        return 0
    drop = [key for key in list(facts.keys()) if _is_transient_fact_key(key)]
    if not drop:
        return 0
    for key in drop:
        facts.pop(key, None)
    doc["last_updated"] = float(now if now is not None else time.time())
    return len(drop)


def _scrub_non_schema_user_facts(doc: Dict[str, Any], *, now: Optional[float] = None) -> int:
    if not isinstance(doc, dict):
        return 0
    facts = doc.get("facts")
    if not isinstance(facts, dict):
        return 0
    drop = [key for key in list(facts.keys()) if normalize_fact_key(key) not in _USER_ALLOWED_FACT_KEY_SET]
    if not drop:
        return 0
    for key in drop:
        facts.pop(key, None)
    doc["last_updated"] = float(now if now is not None else time.time())
    return len(drop)


def _scrub_non_schema_room_facts(doc: Dict[str, Any], *, now: Optional[float] = None) -> int:
    if not isinstance(doc, dict):
        return 0
    facts = doc.get("facts")
    if not isinstance(facts, dict):
        return 0
    drop = [key for key in list(facts.keys()) if normalize_fact_key(key) not in _ROOM_ALLOWED_FACT_KEY_SET]
    if not drop:
        return 0
    for key in drop:
        facts.pop(key, None)
    doc["last_updated"] = float(now if now is not None else time.time())
    return len(drop)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    return str(value)


def _as_int(value: Any, default: int, min_value: int = 0, max_value: Optional[int] = None) -> int:
    try:
        out = int(float(value))
    except Exception:
        out = int(default)
    if out < min_value:
        out = min_value
    if max_value is not None and out > max_value:
        out = max_value
    return out


def _as_float(value: Any, default: float, min_value: float = 0.0, max_value: Optional[float] = None) -> float:
    try:
        out = float(value)
    except Exception:
        out = float(default)
    if out < min_value:
        out = min_value
    if max_value is not None and out > max_value:
        out = max_value
    return out


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = _as_text(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _load_settings() -> Dict[str, Any]:
    settings = redis_client.hgetall("memory_platform_settings") or {}
    if "enabled_platforms" in settings:
        try:
            redis_client.hdel("memory_platform_settings", "enabled_platforms")
        except Exception:
            pass
    if "default_ttl_sec" in settings:
        try:
            redis_client.hdel("memory_platform_settings", "default_ttl_sec")
        except Exception:
            pass
    if "allow_new_room_keys" in settings:
        try:
            redis_client.hdel("memory_platform_settings", "allow_new_room_keys")
        except Exception:
            pass
    if "allow_new_keys" in settings:
        try:
            redis_client.hdel("memory_platform_settings", "allow_new_keys")
        except Exception:
            pass
    return {
        "interval_seconds": _as_int(settings.get("interval_seconds"), 180, min_value=30, max_value=3600),
        "lookback_limit": _as_int(settings.get("lookback_limit"), 80, min_value=10, max_value=500),
        "min_confidence": _as_float(settings.get("min_confidence"), 0.65, min_value=0.0, max_value=1.0),
        "extraction_max_tokens": _as_int(settings.get("extraction_max_tokens"), 2700, min_value=200, max_value=4000),
        "write_user_memory": _as_bool(settings.get("write_user_memory"), True),
        "write_room_memory": _as_bool(settings.get("write_room_memory"), True),
        "auto_link_identities": _as_bool(settings.get("auto_link_identities"), False),
    }


def _resolve_enabled_platforms() -> List[str]:
    auto_enabled = ["webui"]
    for platform, state_key in _AUTO_STATE_KEYS.items():
        if _as_text(redis_client.get(state_key)).strip().lower() == "true":
            auto_enabled.append(platform)
    return auto_enabled


def _discover_scopes(platform: str) -> List[Tuple[str, str]]:
    p = normalize_segment(platform, default="")
    if not p:
        return []

    if p == "webui":
        try:
            if int(redis_client.llen("webui:chat_history") or 0) > 0:
                return [("webui:chat_history", "chat")]
        except Exception:
            return []
        return []

    specs: List[Tuple[str, str, str]] = []
    if p == "discord":
        # Primary keyspace used by discord_platform.save_message.
        specs.append(("tater:channel:*:history", "tater:channel:", ":history"))
        # Compatibility for older/alternate key layouts.
        specs.append(("tater:discord:channel:*:history", "tater:discord:channel:", ":history"))
        specs.append(("tater:discord:dm:*:history", "tater:discord:dm:", ":history"))
    else:
        spec = _HISTORY_SCAN_SPECS.get(p)
        if spec:
            specs.append(spec)
    if not specs:
        return []

    out: List[Tuple[str, str]] = []
    seen_keys: set[str] = set()
    try:
        for pattern, prefix, suffix in specs:
            for raw_key in redis_client.scan_iter(match=pattern, count=500):
                key = _as_text(raw_key).strip()
                if not key or key in seen_keys:
                    continue
                seen_keys.add(key)
                if not key.startswith(prefix) or not key.endswith(suffix):
                    continue
                scope_id = key[len(prefix) : len(key) - len(suffix)].strip()
                if not scope_id:
                    continue
                out.append((key, scope_id))
    except Exception:
        return []

    out.sort(key=lambda row: (str(row[1]), str(row[0])))
    return out


def _message_content_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, dict):
        marker = _as_text(content.get("marker")).strip().lower()
        if marker in _MARKER_SKIP:
            return ""
        if marker == "plugin_response":
            return _message_content_text(content.get("content"))
        if marker == "plugin_call":
            plugin_name = _as_text(content.get("plugin")).strip()
            return f"[tool_call:{plugin_name}]" if plugin_name else "[tool_call]"

        content_type = _as_text(content.get("type")).strip().lower()
        if content_type in {"image", "audio", "video", "file"}:
            name = _as_text(content.get("name")).strip()
            return f"[{content_type}:{name}]" if name else f"[{content_type}]"

        for key in ("text", "content", "message", "value"):
            value = content.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        try:
            return json.dumps(content, ensure_ascii=False, sort_keys=True)
        except Exception:
            return _as_text(content).strip()
    if isinstance(content, list):
        parts = [_message_content_text(item) for item in content]
        joined = "\n".join([part for part in parts if part])
        return joined.strip()
    if content is None:
        return ""
    return _as_text(content).strip()


def _message_timestamp(entry: Dict[str, Any], now_ts: float) -> float:
    for key in ("timestamp", "ts", "time", "updated_at"):
        if key in entry:
            return _as_float(entry.get(key), now_ts, min_value=0.0)
    content = entry.get("content")
    if isinstance(content, dict):
        for key in ("timestamp", "ts", "time", "updated_at"):
            if key in content:
                return _as_float(content.get(key), now_ts, min_value=0.0)
    return now_ts


def _normalized_message_id(platform: str, scope_id: str, index: int) -> str:
    p = normalize_segment(platform, default="platform")
    s = normalize_segment(scope_id, default="scope")
    return f"msg_{p}_{s}_{int(index)}"


def _normalize_history_entry(
    *,
    platform: str,
    scope_id: str,
    index: int,
    raw_entry: Any,
    now_ts: float,
) -> Optional[Dict[str, Any]]:
    try:
        entry = json.loads(_as_text(raw_entry))
    except Exception:
        return None
    if not isinstance(entry, dict):
        return None

    role_raw = _as_text(entry.get("role")).strip().lower()
    role = "user" if role_raw == "user" else "assistant"
    text = _message_content_text(entry.get("content"))
    if not text:
        return None

    username = _as_text(entry.get("username")).strip()
    user_handle = _as_text(entry.get("user_handle")).strip()
    entry_user_id = _as_text(entry.get("user_id")).strip()
    if not username:
        username = entry_user_id or _as_text(entry.get("user")).strip()

    if platform == "telegram":
        handle_text = user_handle.strip()
        if handle_text.startswith("@"):
            handle_text = handle_text[1:].strip()

        def _generic_telegram_name(value: str) -> bool:
            v = str(value or "").strip().lower()
            return (
                not v
                or v in {"telegram_user", "unknown_user", "unknown"}
                or v.startswith("telegram_user_")
            )

        preferred = ""
        if handle_text:
            preferred = handle_text
        elif not _generic_telegram_name(username):
            preferred = username
        elif entry_user_id and entry_user_id.lower() not in {"assistant", "unknown_user"}:
            # Stable fallback when Telegram display name is unavailable.
            preferred = f"tg_{entry_user_id}" if entry_user_id.lstrip("-").isdigit() else entry_user_id

        if preferred:
            username = preferred
    if role == "assistant":
        user_id = "assistant"
    elif entry_user_id and entry_user_id.lower() not in {"assistant", "unknown_user"}:
        # Prefer stable platform IDs when present; this avoids cross-user merges by display name.
        user_id = entry_user_id
    elif username:
        user_id = username
    elif platform in {"macos", "homeassistant", "homekit", "xbmc"}:
        user_id = scope_id
    else:
        user_id = "unknown_user"

    return {
        "platform": platform,
        "room_id": scope_id,
        "user_id": user_id,
        "user_name": username if username else user_id,
        "role": role,
        "timestamp": _message_timestamp(entry, now_ts),
        "text": text,
        "message_id": _normalized_message_id(platform, scope_id, index),
    }


def _candidate_user_ids(messages: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    for item in messages:
        if not isinstance(item, dict):
            continue
        if _as_text(item.get("role")).strip().lower() != "user":
            continue
        user_id = _as_text(item.get("user_id")).strip()
        if not user_id or user_id.lower() in {"assistant", "unknown_user"}:
            continue
        if user_id not in out:
            out.append(user_id)
    return out


def _user_display_names(messages: List[Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in messages:
        if not isinstance(item, dict):
            continue
        if _as_text(item.get("role")).strip().lower() != "user":
            continue
        user_id = _as_text(item.get("user_id")).strip()
        if not user_id or user_id.lower() in {"assistant", "unknown_user"}:
            continue
        user_name = _as_text(item.get("user_name")).strip() or user_id
        if user_id not in out and user_name:
            out[user_id] = user_name
    return out


def _existing_user_profile_snapshot(
    platform: str,
    user_ids: List[str],
    *,
    now_ts: float,
    user_display_names: Optional[Dict[str, str]] = None,
    auto_link_identities: bool = False,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for raw_user_id in user_ids:
        user_id = _as_text(raw_user_id).strip()
        if not user_id or user_id.lower() in {"assistant", "unknown_user"}:
            continue
        display_name = (
            _as_text((user_display_names or {}).get(user_id)).strip()
            if isinstance(user_display_names, dict)
            else ""
        )
        if bool(auto_link_identities):
            doc_key = resolve_user_doc_key(
                redis_client,
                platform,
                user_id,
                create=False,
                display_name=display_name or user_id,
                auto_link_name=True,
            ) or user_doc_key(platform, user_id)
        else:
            doc_key = user_doc_key(platform, user_id)
        try:
            doc = load_doc(redis_client, doc_key, now=now_ts)
        except Exception:
            doc = {}
        facts = doc.get("facts")
        if not isinstance(facts, dict):
            continue
        profile: Dict[str, Dict[str, Any]] = {}
        for fact_key in _USER_ALLOWED_FACT_KEYS:
            fact = facts.get(fact_key)
            if not isinstance(fact, dict) or "value" not in fact:
                continue
            profile[fact_key] = {
                "value": fact.get("value"),
                "confidence": _as_float(fact.get("confidence"), 0.0, min_value=0.0, max_value=1.0),
                "updated_at": _as_float(fact.get("updated_at"), 0.0, min_value=0.0),
            }
        if profile:
            out[user_id] = profile
    return out


def _existing_room_profile_snapshot(
    platform: str,
    scope_id: str,
    *,
    now_ts: float,
) -> Dict[str, Dict[str, Any]]:
    try:
        doc = load_doc(redis_client, room_doc_key(platform, scope_id), now=now_ts)
    except Exception:
        doc = {}
    facts = doc.get("facts")
    if not isinstance(facts, dict):
        return {}

    profile: Dict[str, Dict[str, Any]] = {}
    for fact_key in _ROOM_ALLOWED_FACT_KEYS:
        fact = facts.get(fact_key)
        if not isinstance(fact, dict) or "value" not in fact:
            continue
        profile[fact_key] = {
            "value": fact.get("value"),
            "confidence": _as_float(fact.get("confidence"), 0.0, min_value=0.0, max_value=1.0),
            "updated_at": _as_float(fact.get("updated_at"), 0.0, min_value=0.0),
        }
    return profile


def _normalize_observation(
    row: Dict[str, Any],
    *,
    scope: str,
    known_message_ids: set[str],
    fallback_evidence_id: str,
) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    raw_fact_key = row.get("candidate_key") or row.get("key")
    scope_name = str(scope or "").strip().lower()
    if scope_name == "user":
        fact_key = _canonical_user_fact_key(raw_fact_key)
    elif scope_name == "room":
        fact_key = _canonical_room_fact_key(raw_fact_key)
    else:
        fact_key = normalize_fact_key(raw_fact_key)
    if not fact_key:
        return None
    if _is_transient_fact_key(fact_key):
        return None
    if "value" not in row:
        return None

    confidence = _as_float(row.get("confidence"), 0.0, min_value=0.0, max_value=1.0)
    # Platform memory is durable by default: disable TTL-based expiration.
    ttl_sec = 0

    evidence_raw = row.get("evidence") or row.get("evidence_message_ids") or []
    evidence: List[str] = []
    if isinstance(evidence_raw, (list, tuple)):
        for item in evidence_raw:
            msg_id = _as_text(item).strip()
            if not msg_id:
                continue
            if msg_id in known_message_ids and msg_id not in evidence:
                evidence.append(msg_id)
    elif isinstance(evidence_raw, str):
        msg_id = evidence_raw.strip()
        if msg_id in known_message_ids:
            evidence.append(msg_id)

    if not evidence and fallback_evidence_id:
        evidence = [fallback_evidence_id]

    return {
        "candidate_key": fact_key,
        "value": row.get("value"),
        "confidence": confidence,
        "ttl_sec": ttl_sec,
        "evidence": evidence[:8],
    }


def _normalize_observation_payload(
    parsed: Any,
    *,
    known_user_ids: List[str],
    known_message_ids: set[str],
    fallback_evidence_id: str,
) -> Dict[str, List[Dict[str, Any]]]:
    user_ids_lookup = {normalize_segment(user_id): user_id for user_id in known_user_ids}
    room_rows: List[Dict[str, Any]] = []
    user_rows: List[Dict[str, Any]] = []

    def _coerce_user_id(raw_value: Any) -> str:
        text = _as_text(raw_value).strip()
        if not text:
            return ""
        norm = normalize_segment(text)
        return user_ids_lookup.get(norm, text)

    def _append_room(row: Dict[str, Any]) -> None:
        obs = _normalize_observation(
            row,
            scope="room",
            known_message_ids=known_message_ids,
            fallback_evidence_id=fallback_evidence_id,
        )
        if obs:
            room_rows.append(obs)

    def _append_user(row: Dict[str, Any]) -> None:
        obs = _normalize_observation(
            row,
            scope="user",
            known_message_ids=known_message_ids,
            fallback_evidence_id=fallback_evidence_id,
        )
        if not obs:
            return
        user_id = _coerce_user_id(row.get("user_id"))
        if not user_id and len(known_user_ids) == 1:
            user_id = known_user_ids[0]
        if not user_id or user_id.lower() in {"assistant", "unknown_user"}:
            return
        obs["user_id"] = user_id
        user_rows.append(obs)

    if isinstance(parsed, dict):
        user_list = parsed.get("user_observations") or parsed.get("user_updates") or []
        room_list = parsed.get("room_observations") or parsed.get("room_updates") or []
        if isinstance(user_list, list):
            for item in user_list:
                if isinstance(item, dict):
                    _append_user(item)
        if isinstance(room_list, list):
            for item in room_list:
                if isinstance(item, dict):
                    _append_room(item)

        generic_list = parsed.get("observations")
        if isinstance(generic_list, list):
            for item in generic_list:
                if not isinstance(item, dict):
                    continue
                scope = _as_text(item.get("scope")).strip().lower()
                if scope == "user" or item.get("user_id"):
                    _append_user(item)
                else:
                    _append_room(item)
    elif isinstance(parsed, list):
        for item in parsed:
            if not isinstance(item, dict):
                continue
            scope = _as_text(item.get("scope")).strip().lower()
            if scope == "user" or item.get("user_id"):
                _append_user(item)
            else:
                _append_room(item)

    return {"user": user_rows, "room": room_rows}


def _llm_extract_observations(
    llm_client: Any,
    *,
    platform: str,
    scope_id: str,
    messages: List[Dict[str, Any]],
    current_user_profiles: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None,
    current_room_profile: Optional[Dict[str, Dict[str, Any]]] = None,
    extraction_max_tokens: int = 2700,
) -> Optional[Dict[str, List[Dict[str, Any]]]]:
    if llm_client is None:
        return None

    known_user_ids = _candidate_user_ids(messages)
    known_message_ids = {item.get("message_id") for item in messages if isinstance(item, dict) and item.get("message_id")}
    fallback_evidence_id = ""
    if messages:
        fallback_evidence_id = _as_text(messages[-1].get("message_id")).strip()

    payload = {
        "platform": platform,
        "room_id": scope_id,
        "known_user_ids": known_user_ids,
        "allowed_user_keys": list(_USER_ALLOWED_FACT_KEYS),
        "allowed_room_keys": list(_ROOM_ALLOWED_FACT_KEYS),
        "current_user_profiles": current_user_profiles or {},
        "current_room_profile": current_room_profile or {},
        "messages": messages,
    }
    system_prompt = (
        "Extract durable memory observations from normalized chat messages as incremental profile updates.\n"
        "Output strict JSON only with this shape:\n"
        "{"
        "\"user_observations\":[{\"user_id\":\"...\",\"candidate_key\":\"snake_case\",\"value\":...,\"confidence\":0-1,\"evidence\":[\"message_id\"]}],"
        "\"room_observations\":[{\"candidate_key\":\"snake_case\",\"value\":...,\"confidence\":0-1,\"evidence\":[\"message_id\"]}]"
        "}\n"
        "User profile schema and allowed candidate_key values (exact):\n"
        f"{_USER_SCHEMA_PROMPT_TEXT}\n"
        f"Flat allowed keys: {_USER_ALLOWED_FACT_KEYS_PROMPT}\n"
        "Room profile schema and allowed candidate_key values (exact):\n"
        f"{_ROOM_SCHEMA_PROMPT_TEXT}\n"
        f"Flat allowed keys: {_ROOM_ALLOWED_FACT_KEYS_PROMPT}\n"
        "The payload includes current_user_profiles and current_room_profile (already stored facts).\n"
        "Rules:\n"
        "- Use only explicit evidence from messages.\n"
        "- Keep keys short snake_case.\n"
        "- User observations must use only exact allowed user keys (no aliases/prefixes/new keys).\n"
        "- Room observations must use only exact allowed room keys (no aliases/prefixes/new keys).\n"
        "- Prefer concrete values (string/number/list) over booleans when possible.\n"
        "- Never guess sensitive profile attributes; include only directly stated details.\n"
        "- User observations must target one known_user_id.\n"
        "- Compare messages with current_user_profiles and emit only meaningful updates/additions.\n"
        "- For existing facts: if new evidence refines value or increases confidence, emit an update for the same key.\n"
        "- If no meaningful change for a user key, omit it.\n"
        "- Room observations are shared room context.\n"
        "- Compare messages with current_room_profile and emit only meaningful room updates/additions.\n"
        "- If no meaningful change for a room key, omit it.\n"
        "- Do not include ttl/expiry fields.\n"
        "- If no valid observations exist, return empty arrays.\n"
    )

    async def _call() -> Dict[str, Any]:
        return await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            max_tokens=max(200, int(extraction_max_tokens)),
            temperature=0.1,
        )

    try:
        response = asyncio.run(_call())
    except Exception as exc:
        logger.warning("[memory_platform] LLM extraction failed for %s/%s: %s", platform, scope_id, exc)
        return None

    text = _as_text((response.get("message", {}) or {}).get("content")).strip()
    if not text:
        return {"user": [], "room": []}

    blob = extract_json(text) or text
    try:
        parsed = json.loads(blob)
    except Exception:
        logger.warning("[memory_platform] Non-JSON extraction response for %s/%s; skipping write.", platform, scope_id)
        return {"user": [], "room": []}

    return _normalize_observation_payload(
        parsed,
        known_user_ids=known_user_ids,
        known_message_ids=known_message_ids,
        fallback_evidence_id=fallback_evidence_id,
    )


def _process_scope(
    *,
    llm_client: Any,
    platform: str,
    history_key: str,
    scope_id: str,
    settings: Dict[str, Any],
) -> Dict[str, int]:
    lookback_limit = _as_int(settings.get("lookback_limit"), 80, min_value=10, max_value=500)
    min_confidence = _as_float(settings.get("min_confidence"), 0.65, min_value=0.0, max_value=1.0)
    extraction_max_tokens = _as_int(settings.get("extraction_max_tokens"), 2700, min_value=200, max_value=4000)
    write_user_memory = _as_bool(settings.get("write_user_memory"), True)
    write_room_memory = _as_bool(settings.get("write_room_memory"), True)
    auto_link_identities = _as_bool(settings.get("auto_link_identities"), False)

    cursor_redis_key = cursor_key(platform, scope_id)
    raw_cursor = redis_client.get(cursor_redis_key)
    cursor_idx = _as_int(raw_cursor, 0, min_value=0)

    total_len = _as_int(redis_client.llen(history_key), 0, min_value=0)
    if total_len <= 0:
        return {"processed_messages": 0, "updated_facts": 0, "updated_docs": 0}

    if cursor_idx > total_len:
        cursor_idx = max(0, total_len - lookback_limit)

    if cursor_idx >= total_len:
        return {"processed_messages": 0, "updated_facts": 0, "updated_docs": 0}

    end_idx_exclusive = min(total_len, cursor_idx + lookback_limit)
    raw_rows = redis_client.lrange(history_key, cursor_idx, end_idx_exclusive - 1) or []
    if not raw_rows:
        return {"processed_messages": 0, "updated_facts": 0, "updated_docs": 0}

    ts_now = time.time()
    normalized_messages: List[Dict[str, Any]] = []
    for offset, raw_row in enumerate(raw_rows):
        abs_index = cursor_idx + offset
        item = _normalize_history_entry(
            platform=platform,
            scope_id=scope_id,
            index=abs_index,
            raw_entry=raw_row,
            now_ts=ts_now,
        )
        if item:
            normalized_messages.append(item)

    if not normalized_messages:
        redis_client.set(cursor_redis_key, str(end_idx_exclusive))
        return {"processed_messages": 0, "updated_facts": 0, "updated_docs": 0}

    known_user_ids = _candidate_user_ids(normalized_messages)
    user_display_names = _user_display_names(normalized_messages)
    current_user_profiles = _existing_user_profile_snapshot(
        platform,
        known_user_ids,
        now_ts=ts_now,
        user_display_names=user_display_names,
        auto_link_identities=auto_link_identities,
    )
    current_room_profile = _existing_room_profile_snapshot(
        platform,
        scope_id,
        now_ts=ts_now,
    )

    observations = _llm_extract_observations(
        llm_client,
        platform=platform,
        scope_id=scope_id,
        messages=normalized_messages,
        current_user_profiles=current_user_profiles,
        current_room_profile=current_room_profile,
        extraction_max_tokens=extraction_max_tokens,
    )
    if observations is None:
        return {"processed_messages": 0, "updated_facts": 0, "updated_docs": 0}

    updated_facts = 0
    updated_docs = 0

    if write_room_memory:
        r_key = room_doc_key(platform, scope_id)
        room_doc = load_doc(redis_client, r_key, now=ts_now)
        room_changed = False
        if _scrub_transient_facts(room_doc, now=ts_now) > 0:
            room_changed = True
        if _scrub_non_schema_room_facts(room_doc, now=ts_now) > 0:
            room_changed = True
        for obs in observations.get("room", []):
            if merge_observation(
                room_doc,
                obs,
                min_confidence=min_confidence,
                default_ttl_sec=0,
                allow_new_keys=True,
                now=ts_now,
            ):
                updated_facts += 1
                room_changed = True
        if room_changed:
            save_doc(redis_client, r_key, room_doc, now=ts_now)
            updated_docs += 1

    if write_user_memory:
        by_user: Dict[str, List[Dict[str, Any]]] = {}
        for obs in observations.get("user", []):
            user_id = _as_text(obs.get("user_id")).strip()
            if not user_id:
                continue
            by_user.setdefault(user_id, []).append(obs)

        for user_id, rows in by_user.items():
            display_name = _as_text(user_display_names.get(user_id)).strip() or user_id
            legacy_key = user_doc_key(platform, user_id)
            if bool(auto_link_identities):
                u_key = resolve_user_doc_key(
                    redis_client,
                    platform,
                    user_id,
                    create=True,
                    display_name=display_name,
                    auto_link_name=True,
                ) or legacy_key
            else:
                u_key = legacy_key
            user_doc = load_doc(redis_client, u_key, now=ts_now)
            user_changed = False
            if u_key != legacy_key:
                legacy_doc = load_doc(redis_client, legacy_key, now=ts_now)
                if merge_doc_facts(user_doc, legacy_doc, min_confidence=0.0, now=ts_now) > 0:
                    user_changed = True
            if _scrub_transient_facts(user_doc, now=ts_now) > 0:
                user_changed = True
            if _scrub_non_schema_user_facts(user_doc, now=ts_now) > 0:
                user_changed = True
            for obs in rows:
                if merge_observation(
                    user_doc,
                    obs,
                    min_confidence=min_confidence,
                    default_ttl_sec=0,
                    allow_new_keys=True,
                    now=ts_now,
                ):
                    updated_facts += 1
                    user_changed = True
            if user_changed:
                save_doc(redis_client, u_key, user_doc, now=ts_now)
                updated_docs += 1
            if u_key != legacy_key:
                try:
                    redis_client.delete(legacy_key)
                except Exception:
                    pass

    redis_client.set(cursor_redis_key, str(end_idx_exclusive))
    return {
        "processed_messages": len(normalized_messages),
        "updated_facts": updated_facts,
        "updated_docs": updated_docs,
    }


def _run_cycle(llm_client: Any, settings: Dict[str, Any]) -> Dict[str, int]:
    enabled_platforms = _resolve_enabled_platforms()
    enabled_with_history = 0
    processed_messages = 0
    updated_facts = 0
    updated_docs = 0
    scanned_scopes = 0

    for platform in enabled_platforms:
        scopes = _discover_scopes(platform)
        if scopes:
            enabled_with_history += 1
        for history_key, scope_id in scopes:
            scanned_scopes += 1
            result = _process_scope(
                llm_client=llm_client,
                platform=platform,
                history_key=history_key,
                scope_id=scope_id,
                settings=settings,
            )
            processed_messages += int(result.get("processed_messages") or 0)
            updated_facts += int(result.get("updated_facts") or 0)
            updated_docs += int(result.get("updated_docs") or 0)

    stats = {
        "enabled_platform_count": enabled_with_history,
        "scanned_scopes": scanned_scopes,
        "processed_messages": processed_messages,
        "updated_facts": updated_facts,
        "updated_docs": updated_docs,
    }
    return stats


def _sleep_with_stop(seconds: int, stop_event) -> None:
    target = max(1, int(seconds))
    elapsed = 0.0
    while elapsed < target:
        if stop_event and getattr(stop_event, "is_set", lambda: False)():
            return
        step = min(0.5, target - elapsed)
        time.sleep(step)
        elapsed += step


def run(stop_event=None):
    llm_client = None
    logger.info("[memory_platform] started")

    while True:
        if stop_event and getattr(stop_event, "is_set", lambda: False)():
            break

        settings = _load_settings()
        interval_seconds = _as_int(settings.get("interval_seconds"), 180, min_value=30, max_value=3600)
        cycle_start = time.time()

        if llm_client is None:
            try:
                llm_client = get_llm_client_from_env()
            except Exception as exc:
                logger.warning("[memory_platform] failed to initialize LLM client: %s", exc)
                llm_client = None

        try:
            stats = _run_cycle(llm_client, settings) if llm_client is not None else {
                "enabled_platform_count": 0,
                "scanned_scopes": 0,
                "processed_messages": 0,
                "updated_facts": 0,
                "updated_docs": 0,
            }
            stats_key = "mem:stats:memory_platform"
            redis_client.hset(
                stats_key,
                mapping={
                    "last_run_ts": str(cycle_start),
                    "enabled_platform_count": str(stats.get("enabled_platform_count") or 0),
                    "scanned_scopes": str(stats.get("scanned_scopes") or 0),
                    "processed_messages": str(stats.get("processed_messages") or 0),
                    "updated_facts": str(stats.get("updated_facts") or 0),
                    "updated_docs": str(stats.get("updated_docs") or 0),
                },
            )
        except Exception as exc:
            logger.exception("[memory_platform] cycle failed: %s", exc)

        _sleep_with_stop(interval_seconds, stop_event)

    logger.info("[memory_platform] stopped")
