import json
import re
import time
from typing import Any, Dict, List, Optional


MEMORY_SCHEMA_VERSION = 1
MEMORY_USER_PREFIX = "mem:user"
MEMORY_ROOM_PREFIX = "mem:room"
MEMORY_CURSOR_PREFIX = "mem:cursor"
MEMORY_IDENTITY_DOC_PLATFORM = "identity"
MEMORY_IDENTITY_ALIAS_PREFIX = "mem:identity_alias"
MEMORY_IDENTITY_NAME_PREFIX = "mem:identity_name"

_SEGMENT_RE = re.compile(r"[^a-z0-9_.:\-]+")
_FACT_KEY_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_IDENTITY_PLACEHOLDER_NAMES = {
    "assistant",
    "bot",
    "user",
    "unknown",
    "unknown_user",
    "telegram_user",
    "discord_user",
    "matrix_user",
    "irc_user",
    "webui_user",
    "macos_user",
}


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        try:
            return value.decode("utf-8", errors="replace")
        except Exception:
            return str(value)
    return str(value)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0, min_value: int = 0, max_value: Optional[int] = None) -> int:
    try:
        out = int(float(value))
    except Exception:
        out = int(default)
    if out < min_value:
        out = min_value
    if max_value is not None and out > max_value:
        out = max_value
    return out


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value, ensure_ascii=False)
        return value
    except Exception:
        return _as_text(value)


def normalize_segment(value: Any, *, default: str = "unknown") -> str:
    raw = _as_text(value).strip().lower()
    if not raw:
        raw = default
    cleaned = _SEGMENT_RE.sub("_", raw).strip("_")
    return cleaned or default


def normalize_fact_key(value: Any) -> str:
    raw = _as_text(value).strip().lower()
    if not raw:
        return ""
    raw = re.sub(r"[^a-z0-9_]+", "_", raw).strip("_")
    if not raw:
        return ""
    if raw[0].isdigit():
        raw = f"fact_{raw}"
    if len(raw) > 64:
        raw = raw[:64].rstrip("_")
    if not _FACT_KEY_RE.fullmatch(raw):
        return ""
    return raw


def user_doc_key(platform: Any, user_id: Any) -> str:
    return f"{MEMORY_USER_PREFIX}:{normalize_segment(platform, default='webui')}:{normalize_segment(user_id)}"


def room_doc_key(platform: Any, room_id: Any) -> str:
    return f"{MEMORY_ROOM_PREFIX}:{normalize_segment(platform, default='webui')}:{normalize_segment(room_id, default='chat')}"


def cursor_key(platform: Any, scope_id: Any) -> str:
    return f"{MEMORY_CURSOR_PREFIX}:{normalize_segment(platform, default='webui')}:{normalize_segment(scope_id, default='chat')}"


def identity_doc_key(identity_id: Any) -> str:
    return user_doc_key(MEMORY_IDENTITY_DOC_PLATFORM, identity_id)


def identity_alias_key(platform: Any, user_id: Any) -> str:
    return (
        f"{MEMORY_IDENTITY_ALIAS_PREFIX}:"
        f"{normalize_segment(platform, default='webui')}:"
        f"{normalize_segment(user_id)}"
    )


def identity_name_key(name: Any) -> str:
    return f"{MEMORY_IDENTITY_NAME_PREFIX}:{normalize_segment(name)}"


def normalize_identity_name(value: Any) -> str:
    raw = _as_text(value).strip()
    if raw.startswith("@"):
        raw = raw[1:].strip()
    if ":" in raw and re.fullmatch(r"[A-Za-z0-9._\-]+:[A-Za-z0-9._\-]+", raw):
        local, _, _ = raw.partition(":")
        raw = local.strip() or raw
    normalized = normalize_segment(raw, default="")
    if not normalized:
        return ""
    if normalized in _IDENTITY_PLACEHOLDER_NAMES:
        return ""
    if re.fullmatch(r"-?\d+", normalized):
        return ""
    return normalized


def default_identity_id(
    platform: Any,
    user_id: Any,
    *,
    display_name: Any = None,
) -> str:
    preferred = normalize_identity_name(display_name if display_name is not None else user_id)
    if preferred:
        return preferred
    return (
        f"{normalize_segment(platform, default='webui')}_"
        f"{normalize_segment(user_id)}"
    )


def resolve_identity_id(
    redis_client: Any,
    platform: Any,
    user_id: Any,
    *,
    create: bool = False,
    display_name: Any = None,
    auto_link_name: bool = False,
) -> str:
    platform_seg = normalize_segment(platform, default="webui")
    user_seg = normalize_segment(user_id, default="")
    if not user_seg:
        return ""

    alias_k = identity_alias_key(platform_seg, user_seg)
    alias_raw = ""
    try:
        alias_raw = _as_text(redis_client.get(alias_k)).strip()
    except Exception:
        alias_raw = ""
    if alias_raw:
        alias_id = normalize_segment(alias_raw, default="")
        if alias_id:
            return alias_id

    name_token = normalize_identity_name(display_name if display_name is not None else user_id)
    if auto_link_name and name_token:
        name_k = identity_name_key(name_token)
        try:
            name_raw = _as_text(redis_client.get(name_k)).strip()
        except Exception:
            name_raw = ""
        if name_raw:
            linked_id = normalize_segment(name_raw, default="")
            if linked_id:
                if create:
                    try:
                        redis_client.set(alias_k, linked_id)
                    except Exception:
                        pass
                return linked_id

    if not create:
        return ""

    if auto_link_name and name_token:
        identity_id = default_identity_id(platform_seg, user_seg, display_name=name_token)
    else:
        # Keep identities isolated per-platform unless name auto-linking is enabled.
        identity_id = f"{platform_seg}_{user_seg}"
    try:
        redis_client.set(alias_k, identity_id)
    except Exception:
        pass

    if auto_link_name and name_token:
        name_k = identity_name_key(name_token)
        try:
            existing_name_raw = _as_text(redis_client.get(name_k)).strip()
        except Exception:
            existing_name_raw = ""
        existing_id = normalize_segment(existing_name_raw, default="") if existing_name_raw else ""
        if existing_id:
            identity_id = existing_id
            try:
                redis_client.set(alias_k, identity_id)
            except Exception:
                pass
        else:
            try:
                redis_client.set(name_k, identity_id)
            except Exception:
                pass

    return identity_id


def resolve_user_doc_key(
    redis_client: Any,
    platform: Any,
    user_id: Any,
    *,
    create: bool = False,
    display_name: Any = None,
    auto_link_name: bool = False,
) -> str:
    identity_id = resolve_identity_id(
        redis_client,
        platform,
        user_id,
        create=create,
        display_name=display_name,
        auto_link_name=auto_link_name,
    )
    if identity_id:
        return identity_doc_key(identity_id)
    user_seg = normalize_segment(user_id, default="")
    if not user_seg:
        return ""
    return user_doc_key(platform, user_seg)


def set_identity_alias(
    redis_client: Any,
    platform: Any,
    user_id: Any,
    identity_id: Any,
    *,
    display_name: Any = None,
    auto_link_name: bool = True,
) -> str:
    platform_seg = normalize_segment(platform, default="webui")
    user_seg = normalize_segment(user_id, default="")
    identity_seg = normalize_segment(identity_id, default="")
    if not platform_seg or not user_seg or not identity_seg:
        return ""
    alias_k = identity_alias_key(platform_seg, user_seg)
    try:
        redis_client.set(alias_k, identity_seg)
    except Exception:
        return ""
    if auto_link_name:
        name_token = normalize_identity_name(display_name if display_name is not None else user_id)
        if name_token:
            try:
                redis_client.set(identity_name_key(name_token), identity_seg)
            except Exception:
                pass
    return identity_seg


def clear_identity_alias(redis_client: Any, platform: Any, user_id: Any) -> int:
    key = identity_alias_key(platform, user_id)
    try:
        return int(redis_client.delete(key) or 0)
    except Exception:
        return 0


def list_identity_aliases(redis_client: Any) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    try:
        for raw_key in redis_client.scan_iter(match=f"{MEMORY_IDENTITY_ALIAS_PREFIX}:*", count=500):
            key = _as_text(raw_key).strip()
            if not key:
                continue
            payload = key.split(f"{MEMORY_IDENTITY_ALIAS_PREFIX}:", 1)[-1]
            platform_seg, sep, user_seg = payload.partition(":")
            if not sep or not platform_seg or not user_seg:
                continue
            try:
                raw_identity = _as_text(redis_client.get(key)).strip()
            except Exception:
                raw_identity = ""
            identity_seg = normalize_segment(raw_identity, default="")
            if not identity_seg:
                continue
            rows.append(
                {
                    "platform": platform_seg,
                    "user_id": user_seg,
                    "identity_id": identity_seg,
                    "alias_key": key,
                    "doc_key": identity_doc_key(identity_seg),
                }
            )
    except Exception:
        return []

    rows.sort(key=lambda row: (row.get("identity_id") or "", row.get("platform") or "", row.get("user_id") or ""))
    return rows


def _value_fingerprint(value: Any) -> str:
    try:
        return json.dumps(_json_safe(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except Exception:
        return _as_text(value)


def _coerce_evidence(raw: Any) -> List[str]:
    if isinstance(raw, list):
        source = raw
    elif isinstance(raw, tuple):
        source = list(raw)
    else:
        source = []

    out: List[str] = []
    for item in source:
        text = _as_text(item).strip()
        if not text:
            continue
        if text not in out:
            out.append(text)
    return out[:12]


def _coerce_confidence(raw: Any) -> float:
    value = _safe_float(raw, default=0.0)
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return float(value)


def _coerce_ttl(raw: Any, default_ttl_sec: int) -> int:
    default_i = _safe_int(default_ttl_sec, default=0, min_value=0, max_value=31_536_000)
    return _safe_int(raw, default=default_i, min_value=0, max_value=31_536_000)


def empty_memory_doc(*, now: Optional[float] = None) -> Dict[str, Any]:
    ts = float(now if now is not None else time.time())
    return {
        "schema_version": MEMORY_SCHEMA_VERSION,
        "last_updated": ts,
        "facts": {},
    }


def parse_memory_doc(raw: Any, *, now: Optional[float] = None) -> Dict[str, Any]:
    ts_now = float(now if now is not None else time.time())
    if isinstance(raw, dict):
        parsed = dict(raw)
    else:
        text = _as_text(raw).strip()
        if not text:
            return empty_memory_doc(now=ts_now)

        try:
            parsed = json.loads(text)
        except Exception:
            return empty_memory_doc(now=ts_now)

    if not isinstance(parsed, dict):
        return empty_memory_doc(now=ts_now)

    facts_in = parsed.get("facts")
    facts_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(facts_in, dict):
        for raw_key, raw_fact in facts_in.items():
            fact_key = normalize_fact_key(raw_key)
            if not fact_key or not isinstance(raw_fact, dict):
                continue
            value = _json_safe(raw_fact.get("value"))
            confidence = _coerce_confidence(raw_fact.get("confidence"))
            # Platform memory facts are durable; TTL is intentionally disabled.
            ttl_sec = 0
            evidence = _coerce_evidence(raw_fact.get("evidence"))
            updated_at = _safe_float(raw_fact.get("updated_at"), default=0.0)
            facts_map[fact_key] = {
                "value": value,
                "confidence": confidence,
                "evidence": evidence,
                "ttl_sec": ttl_sec,
                "updated_at": updated_at,
                "expires_at": None,
            }

    return {
        "schema_version": MEMORY_SCHEMA_VERSION,
        "last_updated": _safe_float(parsed.get("last_updated"), default=ts_now),
        "facts": facts_map,
    }


def fact_is_expired(fact: Dict[str, Any], *, now: Optional[float] = None) -> bool:
    # Expiration is disabled for platform memory.
    return False


def prune_expired_facts(doc: Dict[str, Any], *, now: Optional[float] = None) -> bool:
    ts_now = float(now if now is not None else time.time())
    facts = doc.get("facts")
    if not isinstance(facts, dict):
        doc["facts"] = {}
        return False

    expired = [key for key, fact in facts.items() if isinstance(fact, dict) and fact_is_expired(fact, now=ts_now)]
    if not expired:
        return False

    for key in expired:
        facts.pop(key, None)
    doc["last_updated"] = ts_now
    return True


def memory_doc_to_json(doc: Dict[str, Any], *, now: Optional[float] = None) -> str:
    ts_now = float(now if now is not None else time.time())
    parsed = parse_memory_doc(doc, now=ts_now)
    prune_expired_facts(parsed, now=ts_now)
    return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def merge_observation(
    doc: Dict[str, Any],
    observation: Dict[str, Any],
    *,
    min_confidence: float,
    default_ttl_sec: int,
    allow_new_keys: bool,
    now: Optional[float] = None,
) -> bool:
    ts_now = float(now if now is not None else time.time())
    if not isinstance(observation, dict):
        return False

    fact_key = normalize_fact_key(observation.get("candidate_key") or observation.get("key"))
    if not fact_key:
        return False

    if "value" not in observation:
        return False
    value = _json_safe(observation.get("value"))
    confidence = _coerce_confidence(observation.get("confidence"))
    if confidence < float(min_confidence):
        return False

    # Expiration is disabled for platform memory.
    ttl_sec = 0
    evidence = _coerce_evidence(
        observation.get("evidence")
        or observation.get("evidence_message_ids")
    )

    facts = doc.get("facts")
    if not isinstance(facts, dict):
        facts = {}
        doc["facts"] = facts

    if not allow_new_keys and fact_key not in facts:
        return False

    new_fact = {
        "value": value,
        "confidence": confidence,
        "evidence": evidence,
        "ttl_sec": ttl_sec,
        "updated_at": ts_now,
        "expires_at": (ts_now + ttl_sec) if ttl_sec > 0 else None,
    }

    existing = facts.get(fact_key)
    if not isinstance(existing, dict) or fact_is_expired(existing, now=ts_now):
        facts[fact_key] = new_fact
        doc["last_updated"] = ts_now
        return True

    existing_conf = _coerce_confidence(existing.get("confidence"))
    same_value = _value_fingerprint(existing.get("value")) == _value_fingerprint(value)

    if not same_value:
        if confidence <= existing_conf:
            return False
        facts[fact_key] = new_fact
        doc["last_updated"] = ts_now
        return True

    existing_evidence = _coerce_evidence(existing.get("evidence"))
    added = [item for item in evidence if item not in existing_evidence]
    merged_evidence = existing_evidence + added

    merged_conf = max(existing_conf, confidence)
    if added:
        merged_conf = min(0.99, max(merged_conf, existing_conf + min(0.12, 0.03 * len(added))))

    existing_ttl = 0
    merged_ttl = 0
    merged_updated_at = ts_now if (confidence >= existing_conf or bool(added)) else _safe_float(existing.get("updated_at"), default=ts_now)
    merged_fact = {
        "value": value,
        "confidence": merged_conf,
        "evidence": merged_evidence,
        "ttl_sec": merged_ttl,
        "updated_at": merged_updated_at,
        "expires_at": (merged_updated_at + merged_ttl) if merged_ttl > 0 else None,
    }

    if _value_fingerprint(existing) == _value_fingerprint(merged_fact):
        return False

    facts[fact_key] = merged_fact
    doc["last_updated"] = ts_now
    return True


def merge_doc_facts(
    target_doc: Dict[str, Any],
    source_doc: Dict[str, Any],
    *,
    min_confidence: float = 0.0,
    now: Optional[float] = None,
) -> int:
    ts_now = float(now if now is not None else time.time())
    source = parse_memory_doc(source_doc, now=ts_now)
    source_facts = source.get("facts") if isinstance(source.get("facts"), dict) else {}
    if not source_facts:
        return 0

    changed = 0
    for fact_key, fact in source_facts.items():
        if not isinstance(fact, dict):
            continue
        if "value" not in fact:
            continue
        observation = {
            "candidate_key": fact_key,
            "value": fact.get("value"),
            "confidence": _coerce_confidence(fact.get("confidence")),
            "evidence": _coerce_evidence(fact.get("evidence")),
            "ttl_sec": 0,
        }
        if merge_observation(
            target_doc,
            observation,
            min_confidence=float(min_confidence),
            default_ttl_sec=0,
            allow_new_keys=True,
            now=ts_now,
        ):
            changed += 1
    return changed


def load_doc(redis_client: Any, key: str, *, now: Optional[float] = None) -> Dict[str, Any]:
    ts_now = float(now if now is not None else time.time())
    raw = None
    try:
        raw = redis_client.get(key)
    except Exception:
        raw = None
    doc = parse_memory_doc(raw, now=ts_now)
    if prune_expired_facts(doc, now=ts_now):
        save_doc(redis_client, key, doc, now=ts_now)
    return doc


def save_doc(redis_client: Any, key: str, doc: Dict[str, Any], *, now: Optional[float] = None) -> None:
    ts_now = float(now if now is not None else time.time())
    text = memory_doc_to_json(doc, now=ts_now)
    redis_client.set(key, text)


def forget_fact_keys(doc: Dict[str, Any], keys: List[str]) -> int:
    facts = doc.get("facts")
    if not isinstance(facts, dict):
        return 0

    deleted = 0
    for raw_key in keys:
        fact_key = normalize_fact_key(raw_key)
        if not fact_key:
            continue
        if fact_key in facts:
            facts.pop(fact_key, None)
            deleted += 1
    if deleted > 0:
        doc["last_updated"] = time.time()
    return deleted


def summarize_doc(
    doc: Dict[str, Any],
    *,
    max_items: int = 6,
    min_confidence: float = 0.0,
    now: Optional[float] = None,
) -> List[Dict[str, Any]]:
    ts_now = float(now if now is not None else time.time())
    parsed = parse_memory_doc(doc, now=ts_now)
    prune_expired_facts(parsed, now=ts_now)
    facts = parsed.get("facts") if isinstance(parsed.get("facts"), dict) else {}

    rows: List[Dict[str, Any]] = []
    for key, fact in facts.items():
        if not isinstance(fact, dict):
            continue
        confidence = _coerce_confidence(fact.get("confidence"))
        if confidence < float(min_confidence):
            continue
        rows.append(
            {
                "key": key,
                "value": fact.get("value"),
                "confidence": confidence,
                "evidence": _coerce_evidence(fact.get("evidence")),
                "ttl_sec": _coerce_ttl(fact.get("ttl_sec"), 0),
                "updated_at": _safe_float(fact.get("updated_at"), default=0.0),
            }
        )

    rows.sort(
        key=lambda row: (
            -float(row.get("confidence") or 0.0),
            -float(row.get("updated_at") or 0.0),
            str(row.get("key") or ""),
        )
    )
    return rows[: max(1, int(max_items))]


def value_to_text(value: Any, *, max_chars: int = 100) -> str:
    if isinstance(value, str):
        text = value.strip()
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, sort_keys=True)
        except Exception:
            text = _as_text(value).strip()
    if len(text) <= max_chars:
        return text
    return text[: max(8, max_chars - 3)].rstrip() + "..."
