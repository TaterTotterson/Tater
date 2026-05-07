from __future__ import annotations

import contextlib
import json
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from verba_kernel import normalize_platform


PEOPLE_STORE_KEY = "tater:people:v1"
DISCOVERY_MAX_KEYS = 200
DISCOVERY_MAX_ROWS_PER_KEY = 200
PERSON_INSTRUCTIONS_MAX_CHARS = 2000


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        with contextlib.suppress(Exception):
            return value.decode("utf-8", errors="replace").strip()
    return str(value or "").strip()


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", _text(value)).strip().casefold()


def _person_id() -> str:
    return f"person_{uuid.uuid4().hex[:12]}"


def _now() -> float:
    return time.time()


def _platform(value: Any) -> str:
    token = normalize_platform(_text(value))
    return token or _text(value).lower() or "unknown"


def _alias_key(platform: Any, external_id: Any) -> str:
    return f"{_platform(platform)}:{_text(external_id)}"


def _default_store() -> Dict[str, Any]:
    return {
        "version": 1,
        "settings": {},
        "people": [],
    }


def _client(redis_client: Any = None) -> Any:
    if redis_client is not None:
        return redis_client
    from helpers import redis_client as shared_redis

    return shared_redis


def _normalize_alias(row: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    platform = _platform(row.get("platform"))
    external_id = _text(row.get("external_id") or row.get("id") or row.get("user_id"))
    if not platform or not external_id:
        return None
    label = _text(row.get("label") or row.get("display_name") or row.get("name") or external_id)
    return {
        "platform": platform,
        "external_id": external_id,
        "label": label or external_id,
        "kind": _text(row.get("kind") or "user"),
        "created_ts": float(row.get("created_ts") or _now()),
        "updated_ts": float(row.get("updated_ts") or row.get("created_ts") or _now()),
    }


def _normalize_person(row: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    person_id = _text(row.get("id") or row.get("person_id")) or _person_id()
    display_name = _text(row.get("display_name") or row.get("name")) or "Person"
    instructions = _text(row.get("instructions"))
    if not instructions:
        instructions = _text(row.get("notes"))
    instructions = instructions[:PERSON_INSTRUCTIONS_MAX_CHARS]
    aliases: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw_alias in list(row.get("aliases") or []):
        alias = _normalize_alias(raw_alias)
        if not alias:
            continue
        key = _alias_key(alias.get("platform"), alias.get("external_id"))
        if key in seen:
            continue
        seen.add(key)
        aliases.append(alias)
    return {
        "id": person_id,
        "display_name": display_name,
        "is_admin": _as_bool(row.get("is_admin"), False),
        "instructions": instructions,
        "created_ts": float(row.get("created_ts") or _now()),
        "updated_ts": float(row.get("updated_ts") or row.get("created_ts") or _now()),
        "aliases": aliases,
    }


def _normalize_store(data: Any) -> Dict[str, Any]:
    source = data if isinstance(data, dict) else {}
    settings = source.get("settings") if isinstance(source.get("settings"), dict) else {}
    people: List[Dict[str, Any]] = []
    seen_people: set[str] = set()
    seen_aliases: set[str] = set()
    for raw_person in list(source.get("people") or []):
        person = _normalize_person(raw_person)
        if not person:
            continue
        if person["id"] in seen_people:
            person["id"] = _person_id()
        seen_people.add(person["id"])
        unique_aliases = []
        for alias in list(person.get("aliases") or []):
            key = _alias_key(alias.get("platform"), alias.get("external_id"))
            if key in seen_aliases:
                continue
            seen_aliases.add(key)
            unique_aliases.append(alias)
        person["aliases"] = unique_aliases
        people.append(person)
    return {
        "version": 1,
        "settings": {},
        "people": sorted(people, key=lambda item: _text(item.get("display_name")).lower()),
    }


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def load_store(redis_client: Any = None) -> Dict[str, Any]:
    client = _client(redis_client)
    raw = None
    with contextlib.suppress(Exception):
        raw = client.get(PEOPLE_STORE_KEY)
    if raw:
        with contextlib.suppress(Exception):
            return _normalize_store(json.loads(raw))
    return _default_store()


def save_store(data: Dict[str, Any], redis_client: Any = None) -> Dict[str, Any]:
    client = _client(redis_client)
    normalized = _normalize_store(data)
    client.set(PEOPLE_STORE_KEY, json.dumps(normalized, ensure_ascii=False))
    return normalized


def settings(redis_client: Any = None) -> Dict[str, Any]:
    return dict(load_store(redis_client).get("settings") or {})


def alias_candidates_from_origin(platform: str, origin: Optional[Dict[str, Any]]) -> List[Dict[str, str]]:
    source = origin if isinstance(origin, dict) else {}
    normalized_platform = _platform(platform or source.get("platform"))
    out: List[Dict[str, str]] = []
    seen: set[str] = set()

    def add(alias_platform: str, external_id: Any, label: Any = "", kind: str = "user") -> None:
        alias_platform = _platform(alias_platform)
        external = _text(external_id)
        if not alias_platform or not external:
            return
        key = _alias_key(alias_platform, external)
        if key in seen:
            return
        seen.add(key)
        out.append(
            {
                "platform": alias_platform,
                "external_id": external,
                "label": _text(label) or external,
                "kind": _text(kind) or "user",
            }
        )

    if _text(source.get("master_user_id")):
        add("people", source.get("master_user_id"), source.get("person_name") or source.get("display_name"), "master")

    speaker_id = _text(source.get("speaker_id"))
    if speaker_id:
        add("voice_core", speaker_id, source.get("speaker_name") or speaker_id, "speaker_id")

    if normalized_platform in {"homeassistant", "voice_core"} and speaker_id:
        add("voice_core", speaker_id, source.get("speaker_name") or speaker_id, "speaker_id")

    key_groups = {
        "discord": ("user_id", "author_id", "sender_id", "dm_user_id"),
        "telegram": ("user_id", "sender_id", "from_id"),
        "matrix": ("user_id", "sender_id", "sender", "user"),
        "meshtastic": ("user_id", "node_id", "from_id", "sender_id"),
        "irc": ("user", "nick", "username"),
        "webui": ("user_id", "username", "user"),
    }
    for key in key_groups.get(normalized_platform, ("user_id", "author_id", "sender_id", "username", "user")):
        value = _text(source.get(key))
        if value:
            add(normalized_platform, value, source.get("display_name") or source.get("username") or source.get("user") or value)

    return out


def resolve_person(
    *,
    platform: str,
    origin: Optional[Dict[str, Any]],
    redis_client: Any = None,
) -> Dict[str, Any]:
    store = load_store(redis_client)
    candidates = alias_candidates_from_origin(platform, origin)
    alias_index: Dict[str, Tuple[Dict[str, Any], Dict[str, Any]]] = {}
    for person in list(store.get("people") or []):
        for alias in list(person.get("aliases") or []):
            alias_index[_alias_key(alias.get("platform"), alias.get("external_id"))] = (person, alias)

    for candidate in candidates:
        match = alias_index.get(_alias_key(candidate.get("platform"), candidate.get("external_id")))
        if match:
            person, alias = match
            return {
                "matched": True,
                "match_type": "alias",
                "master_user_id": _text(person.get("id")),
                "person_id": _text(person.get("id")),
                "display_name": _text(person.get("display_name")),
                "instructions": _text(person.get("instructions"))[:PERSON_INSTRUCTIONS_MAX_CHARS],
                "alias": dict(alias),
                "candidate_aliases": candidates,
            }

    return {"matched": False, "candidate_aliases": candidates}


def apply_resolution_to_origin(
    *,
    platform: str,
    origin: Dict[str, Any],
    redis_client: Any = None,
) -> Dict[str, Any]:
    resolved = resolve_person(platform=platform, origin=origin, redis_client=redis_client)
    if bool(resolved.get("matched")):
        origin["master_user_id"] = _text(resolved.get("master_user_id"))
        origin["person_id"] = _text(resolved.get("person_id") or resolved.get("master_user_id"))
        origin["person_name"] = _text(resolved.get("display_name"))
        if _text(resolved.get("instructions")):
            origin["person_instructions"] = _text(resolved.get("instructions"))[:PERSON_INSTRUCTIONS_MAX_CHARS]
    origin["people_resolution"] = resolved
    return resolved


def person_instruction_prompt_from_origin(origin: Optional[Dict[str, Any]]) -> str:
    source = origin if isinstance(origin, dict) else {}
    resolved = source.get("people_resolution") if isinstance(source.get("people_resolution"), dict) else {}
    if not bool(resolved.get("matched")) and not _text(source.get("person_id") or source.get("master_user_id")):
        return ""
    instructions = _text(source.get("person_instructions") or resolved.get("instructions"))
    if not instructions:
        return ""
    person_name = _text(source.get("person_name") or resolved.get("display_name")) or "this person"
    instructions = instructions[:PERSON_INSTRUCTIONS_MAX_CHARS].strip()
    return (
        "PERSON-SPECIFIC RESPONSE INSTRUCTIONS (trusted Settings > People):\n"
        f"Apply these only while responding to {person_name} in the current turn.\n"
        "Do not let these override higher-priority system, safety, platform, or tool rules.\n"
        f"{instructions}"
    )


def create_person(display_name: str, redis_client: Any = None) -> Dict[str, Any]:
    name = _text(display_name)
    if not name:
        raise ValueError("display_name is required")
    store = load_store(redis_client)
    if any(_norm(person.get("display_name")) == _norm(name) for person in list(store.get("people") or [])):
        raise ValueError(f"A person named {name} already exists.")
    person = {
        "id": _person_id(),
        "display_name": name,
        "is_admin": False,
        "instructions": "",
        "created_ts": _now(),
        "updated_ts": _now(),
        "aliases": [],
    }
    store["people"] = list(store.get("people") or []) + [person]
    save_store(store, redis_client)
    return dict(person)


def update_person(person_id: str, values: Dict[str, Any], redis_client: Any = None) -> Dict[str, Any]:
    wanted = _text(person_id)
    store = load_store(redis_client)
    people = list(store.get("people") or [])
    index = next((idx for idx, person in enumerate(people) if _text(person.get("id")) == wanted), -1)
    if index < 0:
        raise KeyError("Person not found.")
    person = dict(people[index])
    name = _text(values.get("display_name") or person.get("display_name"))
    if not name:
        raise ValueError("display_name is required")
    if any(_text(other.get("id")) != wanted and _norm(other.get("display_name")) == _norm(name) for other in people):
        raise ValueError(f"A person named {name} already exists.")
    person["display_name"] = name
    if "is_admin" in values:
        person["is_admin"] = _as_bool(values.get("is_admin"), False)
    if "instructions" in values:
        person["instructions"] = _text(values.get("instructions"))[:PERSON_INSTRUCTIONS_MAX_CHARS]
    elif "notes" in values:
        person["instructions"] = _text(values.get("notes"))[:PERSON_INSTRUCTIONS_MAX_CHARS]
    else:
        person["instructions"] = _text(person.get("instructions") or person.get("notes"))[:PERSON_INSTRUCTIONS_MAX_CHARS]
    person.pop("notes", None)
    person["updated_ts"] = _now()
    people[index] = person
    store["people"] = people
    save_store(store, redis_client)
    return person


def person_is_admin(person_id: str, redis_client: Any = None) -> bool:
    wanted = _text(person_id)
    if not wanted:
        return False
    store = load_store(redis_client)
    for person in list(store.get("people") or []):
        if _text(person.get("id")) == wanted:
            return _as_bool(person.get("is_admin"), False)
    return False


def admin_people(redis_client: Any = None) -> List[Dict[str, Any]]:
    store = load_store(redis_client)
    return [dict(person) for person in list(store.get("people") or []) if _as_bool(person.get("is_admin"), False)]


def delete_person(person_id: str, redis_client: Any = None) -> Dict[str, Any]:
    wanted = _text(person_id)
    store = load_store(redis_client)
    people = list(store.get("people") or [])
    target = next((person for person in people if _text(person.get("id")) == wanted), None)
    if not isinstance(target, dict):
        raise KeyError("Person not found.")
    store["people"] = [person for person in people if _text(person.get("id")) != wanted]
    save_store(store, redis_client)
    return target


def attach_alias(
    *,
    person_id: str,
    platform: str,
    external_id: str,
    label: str = "",
    kind: str = "user",
    redis_client: Any = None,
) -> Dict[str, Any]:
    wanted = _text(person_id)
    alias = _normalize_alias(
        {
            "platform": platform,
            "external_id": external_id,
            "label": label,
            "kind": kind,
            "created_ts": _now(),
            "updated_ts": _now(),
        }
    )
    if not alias:
        raise ValueError("platform and external_id are required")
    store = load_store(redis_client)
    people = list(store.get("people") or [])
    target_index = next((idx for idx, person in enumerate(people) if _text(person.get("id")) == wanted), -1)
    if target_index < 0:
        raise KeyError("Person not found.")
    key = _alias_key(alias.get("platform"), alias.get("external_id"))
    for idx, person in enumerate(people):
        next_aliases = [
            row for row in list(person.get("aliases") or []) if _alias_key(row.get("platform"), row.get("external_id")) != key
        ]
        if len(next_aliases) != len(list(person.get("aliases") or [])):
            person = dict(person)
            person["aliases"] = next_aliases
            person["updated_ts"] = _now()
            people[idx] = person
    target = dict(people[target_index])
    target_aliases = list(target.get("aliases") or [])
    target_aliases.append(alias)
    target["aliases"] = target_aliases
    target["updated_ts"] = _now()
    people[target_index] = target
    store["people"] = people
    save_store(store, redis_client)
    return alias


def detach_alias(*, person_id: str, platform: str, external_id: str, redis_client: Any = None) -> Dict[str, Any]:
    wanted = _text(person_id)
    key = _alias_key(platform, external_id)
    store = load_store(redis_client)
    people = list(store.get("people") or [])
    for idx, person in enumerate(people):
        if _text(person.get("id")) != wanted:
            continue
        aliases = list(person.get("aliases") or [])
        target = next((alias for alias in aliases if _alias_key(alias.get("platform"), alias.get("external_id")) == key), None)
        if not isinstance(target, dict):
            raise KeyError("Identity link not found.")
        person = dict(person)
        person["aliases"] = [
            alias for alias in aliases if _alias_key(alias.get("platform"), alias.get("external_id")) != key
        ]
        person["updated_ts"] = _now()
        people[idx] = person
        store["people"] = people
        save_store(store, redis_client)
        return target
    raise KeyError("Person not found.")


def _linked_alias_index(store: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for person in list(store.get("people") or []):
        for alias in list(person.get("aliases") or []):
            out[_alias_key(alias.get("platform"), alias.get("external_id"))] = {
                "person_id": _text(person.get("id")),
                "person_name": _text(person.get("display_name")),
            }
    return out


def _add_discovered_alias(out: Dict[str, Dict[str, Any]], linked: Dict[str, Dict[str, str]], alias: Dict[str, Any]) -> None:
    normalized = _normalize_alias(alias)
    if not normalized:
        return
    key = _alias_key(normalized.get("platform"), normalized.get("external_id"))
    existing = out.get(key) if isinstance(out.get(key), dict) else {}
    row = dict(normalized)
    for extra_key in ("source", "fact_count", "last_updated", "doc_key"):
        if alias.get(extra_key) not in (None, ""):
            row[extra_key] = alias.get(extra_key)
    sources: List[str] = []
    for source in list(existing.get("sources") or []):
        text = _text(source)
        if text and text not in sources:
            sources.append(text)
    for source in (existing.get("source"), alias.get("source")):
        text = _text(source)
        if text and text not in sources:
            sources.append(text)
    if sources:
        row["sources"] = sources
        row["source"] = ", ".join(sources)

    old_label = _text(existing.get("label"))
    new_label = _text(row.get("label"))
    external_id = _text(row.get("external_id"))
    if old_label and (not new_label or new_label == external_id) and old_label != external_id:
        row["label"] = old_label
    if existing.get("fact_count") not in (None, "") and row.get("fact_count") in (None, ""):
        row["fact_count"] = existing.get("fact_count")

    row.update(linked.get(key) or {})
    out[key] = row


def _discover_webui_aliases(out: Dict[str, Dict[str, Any]], linked: Dict[str, Dict[str, str]], redis_client: Any) -> None:
    raw_rows = []
    with contextlib.suppress(Exception):
        raw_rows = redis_client.lrange("webui:chat_history", -DISCOVERY_MAX_ROWS_PER_KEY, -1) or []
    for raw in raw_rows:
        with contextlib.suppress(Exception):
            row = json.loads(raw)
            if isinstance(row, dict) and _text(row.get("role")) == "user":
                username = _text(row.get("username"))
                if username:
                    _add_discovered_alias(out, linked, {"platform": "webui", "external_id": username, "label": username})


def _discover_voice_aliases(out: Dict[str, Dict[str, Any]], linked: Dict[str, Dict[str, str]]) -> None:
    with contextlib.suppress(Exception):
        from esphome import speaker_id as esphome_speaker_id

        for row in esphome_speaker_id.speaker_identity_aliases():
            _add_discovered_alias(out, linked, row)


def _platform_from_history_key(key: Any) -> str:
    text = _text(key)
    if text.startswith("tater:channel:"):
        return "discord"
    if text.startswith("tater:discord:"):
        return "discord"
    if text.startswith("tater:telegram:"):
        return "telegram"
    if text.startswith("tater:matrix:"):
        return "matrix"
    if text.startswith("tater:irc:"):
        return "irc"
    if text.startswith("tater:meshtastic:"):
        return "meshtastic"
    if text.startswith("tater:homekit:"):
        return "homekit"
    if text.startswith("tater:xbmc:"):
        return "xbmc"
    if text.startswith("tater:macos:"):
        return "macos"
    return ""


def _session_id_from_history_key(key: Any, platform: str) -> str:
    text = _text(key)
    if not text.endswith(":history"):
        return ""
    if platform == "discord" and text.startswith("tater:channel:"):
        return text.removeprefix("tater:channel:").removesuffix(":history")
    if platform in {"homekit", "xbmc"}:
        prefix = f"tater:{platform}:session:"
        if text.startswith(prefix):
            return text.removeprefix(prefix).removesuffix(":history")
    if platform == "macos":
        prefix = "tater:macos:session:"
        if text.startswith(prefix):
            return text.removeprefix(prefix).removesuffix(":history")
    return ""


def _portal_label(platform: Any) -> str:
    labels = {
        "discord": "Discord",
        "telegram": "Telegram",
        "matrix": "Matrix",
        "irc": "IRC",
        "meshtastic": "Meshtastic",
        "homekit": "HomeKit",
        "xbmc": "XBMC",
        "macos": "macOS",
        "webui": "WebUI",
        "voice_core": "Voice Core",
    }
    token = _platform(platform)
    return labels.get(token, token.title() if token else "Portal")


def _portal_aliases_from_history_row(platform: str, row: Dict[str, Any], history_key: Any = "") -> List[Dict[str, str]]:
    origin = row.get("origin") if isinstance(row.get("origin"), dict) else row
    aliases = list(alias_candidates_from_origin(platform, origin))
    if aliases:
        return aliases

    session_id = _session_id_from_history_key(history_key, platform)
    username = _text(
        row.get("username")
        or row.get("user")
        or row.get("sender")
        or row.get("name")
        or row.get("display_name")
        or row.get("user_handle")
    )
    user_id = _text(row.get("user_id") or row.get("sender_id") or row.get("author_id") or row.get("node_id"))
    if not user_id and platform in {"irc", "webui"} and username:
        user_id = username
    if not user_id and platform in {"homekit", "xbmc", "macos"}:
        user_id = session_id
    if not username:
        username = user_id or session_id
    if not user_id:
        return []
    return [
        {
            "platform": platform,
            "external_id": user_id,
            "label": username or user_id,
            "kind": "portal_user",
        }
    ]


def _discover_portal_history_aliases(out: Dict[str, Dict[str, Any]], linked: Dict[str, Dict[str, str]], redis_client: Any) -> None:
    patterns = (
        "tater:channel:*:history",
        "tater:discord:*:history",
        "tater:telegram:*:history",
        "tater:matrix:*:history",
        "tater:irc:*:history",
        "tater:meshtastic:*:history",
        "tater:homekit:session:*:history",
        "tater:xbmc:session:*:history",
        "tater:macos:session:*:history",
    )
    seen_keys = 0
    for pattern in patterns:
        keys = []
        with contextlib.suppress(Exception):
            keys = list(redis_client.scan_iter(match=pattern, count=100))
        for key in keys:
            if seen_keys >= DISCOVERY_MAX_KEYS:
                return
            seen_keys += 1
            platform = _platform_from_history_key(key)
            if not platform:
                continue
            raw_rows = []
            with contextlib.suppress(Exception):
                raw_rows = redis_client.lrange(key, -DISCOVERY_MAX_ROWS_PER_KEY, -1) or []
            for raw in raw_rows:
                with contextlib.suppress(Exception):
                    row = json.loads(raw)
                    if not isinstance(row, dict):
                        continue
                    for alias in _portal_aliases_from_history_row(platform, row, key):
                        alias["source"] = _portal_label(platform)
                        _add_discovered_alias(out, linked, alias)


def _memory_core_user_label(redis_client: Any, platform: str, user_id: str) -> str:
    with contextlib.suppress(Exception):
        label = _text(redis_client.get(f"tater:user_label:{platform}:{user_id}"))
        if label:
            return label
    return user_id


def _memory_core_doc(redis_client: Any, doc_key: str) -> Dict[str, Any]:
    with contextlib.suppress(Exception):
        raw = redis_client.get(doc_key)
        parsed = json.loads(_text(raw))
        if isinstance(parsed, dict):
            return parsed
    return {}


def _discover_memory_core_aliases(out: Dict[str, Dict[str, Any]], linked: Dict[str, Dict[str, str]], redis_client: Any) -> None:
    keys = []
    with contextlib.suppress(Exception):
        keys = list(redis_client.scan_iter(match="mem:user:*", count=200))
    seen_keys = 0
    for raw_key in keys:
        if seen_keys >= DISCOVERY_MAX_KEYS:
            return
        seen_keys += 1
        key = _text(raw_key)
        if not key.startswith("mem:user:"):
            continue
        payload = key.split("mem:user:", 1)[-1]
        platform, sep, user_id = payload.partition(":")
        platform = _platform(platform)
        user_id = _text(user_id)
        if not sep or not platform or not user_id or platform == "identity":
            continue
        doc = _memory_core_doc(redis_client, key)
        facts = doc.get("facts") if isinstance(doc.get("facts"), dict) else {}
        if not facts:
            continue
        _add_discovered_alias(
            out,
            linked,
            {
                "platform": platform,
                "external_id": user_id,
                "label": _memory_core_user_label(redis_client, platform, user_id),
                "kind": "memory_user",
                "source": "Memory Core",
                "fact_count": len(facts),
                "last_updated": doc.get("last_updated"),
                "doc_key": key,
            },
        )


def discovered_identities(redis_client: Any = None) -> List[Dict[str, Any]]:
    client = _client(redis_client)
    store = load_store(client)
    linked = _linked_alias_index(store)
    out: Dict[str, Dict[str, Any]] = {}
    for person in list(store.get("people") or []):
        for alias in list(person.get("aliases") or []):
            _add_discovered_alias(out, linked, alias)
    _discover_webui_aliases(out, linked, client)
    _discover_voice_aliases(out, linked)
    _discover_portal_history_aliases(out, linked, client)
    _discover_memory_core_aliases(out, linked, client)
    return sorted(
        out.values(),
        key=lambda row: (
            0 if _text(row.get("person_id")) else 1,
            _platform(row.get("platform")),
            _text(row.get("label")).lower(),
            _text(row.get("external_id")).lower(),
        ),
    )


def panel_payload(redis_client: Any = None) -> Dict[str, Any]:
    store = load_store(redis_client)
    people = list(store.get("people") or [])
    identities = discovered_identities(redis_client)
    linked_count = len([row for row in identities if _text(row.get("person_id"))])
    admin_count = len([person for person in people if _as_bool(person.get("is_admin"), False)])
    return {
        "settings": dict(store.get("settings") or {}),
        "summary_metrics": [
            {"label": "People", "value": len(people)},
            {"label": "Admins", "value": admin_count},
            {"label": "Linked Identities", "value": linked_count},
            {"label": "Discovered Identities", "value": len(identities)},
            {
                "label": "Matching",
                "value": "Manual links only",
            },
        ],
        "people": people,
        "identities": identities,
    }


def handle_action(action: str, payload: Dict[str, Any], redis_client: Any = None) -> Dict[str, Any]:
    token = _text(action).lower()
    body = payload if isinstance(payload, dict) else {}
    values = body.get("values") if isinstance(body.get("values"), dict) else body
    client = _client(redis_client)

    if token == "people_create":
        person = create_person(_text(values.get("display_name") or body.get("display_name")), client)
        return {
            "ok": True,
            "action": token,
            "message": f"Created person {person.get('display_name')}.",
            "people": panel_payload(client),
        }

    if token == "people_save":
        person = update_person(_text(body.get("person_id") or values.get("person_id")), values if isinstance(values, dict) else {}, client)
        return {
            "ok": True,
            "action": token,
            "message": f"Saved person {person.get('display_name')}.",
            "people": panel_payload(client),
        }

    if token == "people_delete":
        person = delete_person(_text(body.get("person_id") or values.get("person_id")), client)
        return {
            "ok": True,
            "action": token,
            "message": f"Deleted person {person.get('display_name')}.",
            "people": panel_payload(client),
        }

    if token == "people_alias_attach":
        person_id = _text(body.get("person_id") or values.get("person_id"))
        alias = attach_alias(
            person_id=person_id,
            platform=_text(body.get("platform") or values.get("platform")),
            external_id=_text(body.get("external_id") or values.get("external_id")),
            label=_text(body.get("label") or values.get("label")),
            kind=_text(body.get("kind") or values.get("kind") or "user"),
            redis_client=client,
        )
        return {
            "ok": True,
            "action": token,
            "message": f"Linked {alias.get('label')} to person.",
            "people": panel_payload(client),
        }

    if token == "people_alias_detach":
        alias = detach_alias(
            person_id=_text(body.get("person_id") or values.get("person_id")),
            platform=_text(body.get("platform") or values.get("platform")),
            external_id=_text(body.get("external_id") or values.get("external_id")),
            redis_client=client,
        )
        return {
            "ok": True,
            "action": token,
            "message": f"Unlinked {alias.get('label') or alias.get('external_id')}.",
            "people": panel_payload(client),
        }

    raise ValueError(f"Unknown People action: {action}")
