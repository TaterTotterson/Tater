"""Shared Face ID identity storage, matching, and management for Tater.

The Face ID model runtime lives in :mod:`face_id_runtime`.  This module owns the
persistent identities produced by that runtime so camera features do not need
Awareness Core to be installed.
"""

from __future__ import annotations

import contextlib
import base64
import json
import math
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import face_id_runtime
from spud_link_models import allow_local_fallback as spud_link_allow_local_fallback
from spud_link_models import request_json as spud_link_request_json
from spud_link_models import should_use_hub as spud_link_should_use_hub


SHARED_IDENTITIES_KEY = "tater:face_identities:v1"
IDENTITY_ALIASES_KEY = "tater:face_identity_aliases:v1"
EVENT_IDENTITIES_KEY = "tater:face_event_identities:v1"
FACE_ALIAS_PLATFORM = "face_id"
DELETED_IDENTITY = "__deleted__"
OBSERVATION_LIMIT = 500
REFERENCE_LIMIT = 24

_identity_lock = threading.RLock()


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        with contextlib.suppress(Exception):
            return value.decode("utf-8", errors="replace").strip()
    return str(value or "").strip()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _float(value: Any, default: float = 0.0) -> float:
    with contextlib.suppress(TypeError, ValueError):
        return float(value)
    return float(default)


def _int(value: Any, default: int = 0, *, minimum: Optional[int] = None) -> int:
    with contextlib.suppress(TypeError, ValueError):
        parsed = int(value)
        return max(minimum, parsed) if minimum is not None else parsed
    return max(minimum, default) if minimum is not None else default


def _json_object(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    with contextlib.suppress(Exception):
        parsed = json.loads(_text(raw))
        if isinstance(parsed, dict):
            return parsed
    return {}


def _json_list(raw: Any) -> List[Any]:
    if isinstance(raw, list):
        return list(raw)
    with contextlib.suppress(Exception):
        parsed = json.loads(_text(raw))
        if isinstance(parsed, list):
            return parsed
    return []


def _client(redis_client: Any = None) -> Any:
    if redis_client is not None:
        return redis_client
    from helpers import redis_client as shared_redis

    return shared_redis


def _read_hash(client: Any, key: str) -> Dict[str, Dict[str, Any]]:
    rows: Dict[str, Dict[str, Any]] = {}
    with contextlib.suppress(Exception):
        for raw_id, raw_payload in (client.hgetall(key) or {}).items():
            identity_id = _text(raw_id)
            payload = _json_object(raw_payload)
            if identity_id and payload:
                payload["id"] = identity_id
                rows[identity_id] = payload
    return rows


def identity_rows(redis_client: Any = None) -> Dict[str, Dict[str, Any]]:
    """Return the identities owned by Tater's shared People service."""
    return _read_hash(_client(redis_client), SHARED_IDENTITIES_KEY)


def save_identity(identity: Dict[str, Any], redis_client: Any = None) -> Dict[str, Any]:
    client = _client(redis_client)
    identity_id = _text(identity.get("id"))
    if not identity_id:
        raise ValueError("Face identity cannot be stored without an ID.")
    payload = dict(identity)
    payload["id"] = identity_id
    encoded = json.dumps(payload, separators=(",", ":"))
    client.hset(SHARED_IDENTITIES_KEY, identity_id, encoded)
    return payload


def _set_identity_alias(client: Any, source_id: str, target_id: str) -> None:
    source = _text(source_id)
    if not source:
        return
    client.hset(IDENTITY_ALIASES_KEY, source, _text(target_id) or DELETED_IDENTITY)


def resolve_identity_id(identity_id: Any, redis_client: Any = None) -> str:
    client = _client(redis_client)
    current = _text(identity_id)
    seen: set[str] = set()
    while current and current not in seen:
        seen.add(current)
        raw = None
        with contextlib.suppress(Exception):
            raw = client.hget(IDENTITY_ALIASES_KEY, current)
        target = _text(raw)
        if not target:
            return current
        if target == DELETED_IDENTITY:
            return ""
        current = target
    return current if current not in seen else ""


def _event_identity_mapping(client: Any, event_id: Any) -> Tuple[bool, List[str]]:
    token = _text(event_id)
    if not token:
        return False, []
    raw = None
    with contextlib.suppress(Exception):
        raw = client.hget(EVENT_IDENTITIES_KEY, token)
    if raw is None:
        return False, []
    return True, list(dict.fromkeys(_text(value) for value in _json_list(raw) if _text(value)))


def _event_identity_ids(client: Any, event_id: Any) -> List[str]:
    return _event_identity_mapping(client, event_id)[1]


def _save_event_identity_ids(client: Any, event_id: Any, identity_ids: Iterable[Any]) -> None:
    token = _text(event_id)
    if not token:
        return
    resolved = list(
        dict.fromkeys(
            resolved_id
            for value in identity_ids
            if (resolved_id := resolve_identity_id(value, client))
        )
    )
    client.hset(EVENT_IDENTITIES_KEY, token, json.dumps(resolved, separators=(",", ":")))


def identity_ids_for_event(
    event_id: Any,
    fallback_identity_ids: Optional[Iterable[Any]] = None,
    redis_client: Any = None,
) -> List[str]:
    client = _client(redis_client)
    has_mapping, mapped = _event_identity_mapping(client, event_id)
    source = mapped if has_mapping else list(fallback_identity_ids or [])
    return list(
        dict.fromkeys(
            resolved_id
            for value in source
            if (resolved_id := resolve_identity_id(value, client))
        )
    )


def _delete_identity_row(identity_id: str, client: Any) -> bool:
    with contextlib.suppress(Exception):
        return bool(client.hdel(SHARED_IDENTITIES_KEY, identity_id))
    return False


def valid_embedding(raw: Any, dimensions: int = 0) -> List[float]:
    if not isinstance(raw, list) or not raw:
        return []
    try:
        embedding = [float(value) for value in raw]
    except (TypeError, ValueError):
        return []
    if dimensions and len(embedding) != dimensions:
        return []
    return embedding


def cosine_distance(left: List[float], right: List[float]) -> float:
    if not left or not right or len(left) != len(right):
        return float("inf")
    dot = sum(float(a) * float(b) for a, b in zip(left, right))
    left_norm = math.sqrt(sum(float(value) * float(value) for value in left))
    right_norm = math.sqrt(sum(float(value) * float(value) for value in right))
    if left_norm <= 0.0 or right_norm <= 0.0:
        return float("inf")
    return 1.0 - (dot / (left_norm * right_norm))


def observations(identity: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for raw in identity.get("observations") or []:
        if not isinstance(raw, dict):
            continue
        observation_id = _text(raw.get("id"))
        if not observation_id or observation_id in seen:
            continue
        seen.add(observation_id)
        rows.append({**raw, "id": observation_id})
    rows.sort(key=lambda row: (_text(row.get("seen_at")), _text(row.get("id"))), reverse=True)
    return rows[:OBSERVATION_LIMIT]


def reference_embeddings(identity: Dict[str, Any]) -> List[List[float]]:
    references: List[List[float]] = []

    def add(raw: Any) -> None:
        if len(references) >= REFERENCE_LIMIT:
            return
        embedding = valid_embedding(raw, len(references[0]) if references else 0)
        if not embedding:
            return
        if any(cosine_distance(embedding, existing) < 0.0001 for existing in references):
            return
        references.append(embedding)

    stored = identity.get("reference_centroids")
    if isinstance(stored, list):
        for raw in stored:
            add(raw)
    if references:
        return references
    for raw in identity.get("anchor_references") or []:
        add(raw)
    add(identity.get("centroid"))
    for row in observations(identity):
        add(row.get("embedding"))
    return references


def curate_reference_embeddings(
    identity: Dict[str, Any],
    *,
    extra_references: Optional[List[List[float]]] = None,
    limit: int = REFERENCE_LIMIT,
) -> List[List[float]]:
    maximum = max(1, int(limit))
    best_quality = max(1.0, _float(identity.get("best_quality")))
    candidates: List[Dict[str, Any]] = []

    def add(raw: Any, *, quality: float, seen_at: Any = "", anchor: bool = False) -> None:
        embedding = valid_embedding(raw, len(candidates[0]["embedding"]) if candidates else 0)
        if not embedding:
            return
        candidate = {
            "embedding": embedding,
            "quality": max(0.0, float(quality)),
            "seen_at": _text(seen_at),
            "anchor": bool(anchor),
        }
        for index, existing in enumerate(candidates):
            if cosine_distance(embedding, existing["embedding"]) >= 0.005:
                continue
            candidate["anchor"] = bool(candidate["anchor"] or existing["anchor"])
            if (candidate["quality"], candidate["seen_at"]) >= (existing["quality"], existing["seen_at"]):
                candidates[index] = candidate
            else:
                existing["anchor"] = candidate["anchor"]
            return
        candidates.append(candidate)

    for raw in identity.get("anchor_references") or []:
        add(raw, quality=best_quality, anchor=True)
    for raw in extra_references or []:
        add(raw, quality=1.0)
    add(identity.get("centroid"), quality=1.0)
    for row in observations(identity):
        add(row.get("embedding"), quality=_float(row.get("quality")), seen_at=row.get("seen_at"))
    if len(candidates) <= maximum:
        return [row["embedding"] for row in candidates]

    anchors = [row for row in candidates if row["anchor"]]
    seed = max(anchors or candidates, key=lambda row: (row["quality"], row["seen_at"]))
    selected = [seed]
    remaining = [row for row in candidates if row is not seed]
    while remaining and len(selected) < maximum:
        def score(row: Dict[str, Any]) -> Tuple[float, float, str]:
            diversity = min(cosine_distance(row["embedding"], chosen["embedding"]) for chosen in selected)
            quality = min(1.0, max(0.0, row["quality"] / 3.0))
            return (0.70 * min(1.0, diversity) + 0.30 * quality, row["quality"], row["seen_at"])

        chosen = max(remaining, key=score)
        selected.append(chosen)
        remaining.remove(chosen)
    return [row["embedding"] for row in selected]


def match_identity(
    identities: Dict[str, Dict[str, Any]],
    embedding: List[float],
    *,
    threshold: Optional[float] = None,
) -> Tuple[str, float]:
    maximum = _float(threshold, _float(getattr(face_id_runtime, "MATCH_THRESHOLD", 0.30), 0.30))
    best_id = ""
    best_distance = float("inf")
    for identity_id, identity in identities.items():
        distance = min(
            (cosine_distance(embedding, reference) for reference in reference_embeddings(identity)),
            default=float("inf"),
        )
        if distance < best_distance:
            best_id = identity_id
            best_distance = distance
    if not best_id or best_distance > maximum:
        return "", best_distance
    return best_id, best_distance


def _detection_observation(
    detection: Dict[str, Any],
    *,
    embedding: List[float],
    event_id: str,
    seen_at: str,
    quality: float,
    source: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    area = detection.get("facial_area") if isinstance(detection.get("facial_area"), dict) else {}
    row = {
        "id": f"observation_{uuid.uuid4().hex[:20]}",
        "event_id": _text(event_id),
        "seen_at": _text(seen_at) or _now_iso(),
        "embedding": embedding,
        "confidence": round(_float(detection.get("confidence")), 6),
        "quality": round(max(0.0, quality), 6),
        "facial_area": {
            "x": _int(area.get("x"), minimum=0),
            "y": _int(area.get("y"), minimum=0),
            "w": _int(area.get("w"), minimum=0),
            "h": _int(area.get("h"), minimum=0),
        },
        "face_b64": _text(detection.get("crop_b64")),
        "face_content_type": _text(detection.get("crop_content_type")) or "image/jpeg",
    }
    if isinstance(source, dict):
        row["source"] = {key: value for key, value in source.items() if _text(value)}
    return row


def rebuild_identity(
    identity: Dict[str, Any],
    rows: List[Dict[str, Any]],
    *,
    keep_name: bool = True,
) -> Dict[str, Any]:
    payload = dict(identity)
    normalized = observations({"observations": rows})
    payload["observations"] = normalized
    payload["observation_count"] = len(normalized)
    embeddings = [valid_embedding(row.get("embedding")) for row in normalized]
    embeddings = [row for row in embeddings if row]
    dimensions = len(embeddings[0]) if embeddings else 0
    embeddings = [row for row in embeddings if len(row) == dimensions]
    if embeddings and dimensions:
        payload["centroid"] = [sum(row[index] for row in embeddings) / len(embeddings) for index in range(dimensions)]
        payload["centroid_count"] = len(embeddings)
        payload["reference_centroids"] = curate_reference_embeddings(
            {
                "anchor_references": payload.get("anchor_references") if keep_name else [],
                "centroid": payload["centroid"],
                "observations": normalized,
                "best_quality": payload.get("best_quality"),
            }
        )
    else:
        for key in ("centroid", "centroid_count", "reference_centroids"):
            payload.pop(key, None)
        if keep_name and payload.get("anchor_references"):
            payload["reference_centroids"] = curate_reference_embeddings(payload)
    event_ids = {_text(row.get("event_id")) for row in normalized if _text(row.get("event_id"))}
    payload["event_count"] = len(event_ids)
    if normalized:
        chronological = sorted(normalized, key=lambda row: (_text(row.get("seen_at")), _text(row.get("id"))))
        payload["first_seen"] = _text(chronological[0].get("seen_at"))
        payload["last_seen"] = _text(chronological[-1].get("seen_at"))
        payload["last_event_id"] = _text(chronological[-1].get("event_id"))
        best = max(normalized, key=lambda row: _float(row.get("quality")))
        if _text(best.get("face_b64")):
            payload["best_quality"] = _float(best.get("quality"))
            payload["face_b64"] = _text(best.get("face_b64"))
            payload["face_content_type"] = _text(best.get("face_content_type")) or "image/jpeg"
    else:
        for key in ("first_seen", "last_seen", "last_event_id", "best_quality", "face_b64", "face_content_type"):
            payload.pop(key, None)
    if not keep_name:
        payload["name"] = ""
        payload.pop("person_id", None)
        payload.pop("person_name", None)
    payload["updated_at"] = _now_iso()
    return payload


def record_detection(
    detection: Dict[str, Any],
    *,
    event_id: str,
    seen_at: str = "",
    source: Optional[Dict[str, Any]] = None,
    redis_client: Any = None,
) -> Dict[str, Any]:
    client = _client(redis_client)
    embedding = valid_embedding(detection.get("embedding"))
    if not embedding:
        raise ValueError("Face result did not include an embedding.")
    timestamp = _text(seen_at) or _now_iso()
    area = detection.get("facial_area") if isinstance(detection.get("facial_area"), dict) else {}
    confidence = _float(detection.get("confidence"))
    area_pixels = max(1, _int(area.get("w"), 1, minimum=1) * _int(area.get("h"), 1, minimum=1))
    quality = max(0.0, confidence) + min(2.0, area_pixels / 100_000.0)

    with _identity_lock:
        identities = identity_rows(client)
        identity_id, distance = match_identity(identities, embedding)
        identity = dict(identities.get(identity_id) or {})
        if not identity_id:
            identity_id = f"face_{uuid.uuid4().hex[:16]}"
            identity = {
                "id": identity_id,
                "name": "",
                "created_at": timestamp,
                "first_seen": timestamp,
                "observation_count": 0,
                "event_count": 0,
                "centroid": embedding,
                "centroid_count": 0,
                "reference_centroids": [embedding],
                "best_quality": 0.0,
            }
            distance = 0.0

        existing = observations(identity)
        event_token = _text(event_id)
        duplicate = next(
            (
                row
                for row in existing
                if event_token
                and _text(row.get("event_id")) == event_token
                and cosine_distance(valid_embedding(row.get("embedding")), embedding) < 0.005
            ),
            None,
        )
        if duplicate is not None:
            _save_event_identity_ids(client, event_token, [*_event_identity_ids(client, event_token), identity_id])
            return identity

        if existing or identity.get("centroid_count"):
            centroid = valid_embedding(identity.get("centroid"), len(embedding)) or embedding
            centroid_count = _int(identity.get("centroid_count"), 0, minimum=0)
            next_count = centroid_count + 1
            identity["centroid"] = [
                ((float(old) * centroid_count) + float(new)) / next_count
                for old, new in zip(centroid, embedding)
            ]
            identity["centroid_count"] = next_count
        else:
            identity["centroid"] = embedding
            identity["centroid_count"] = 1
        identity["observation_count"] = _int(identity.get("observation_count"), 0, minimum=0) + 1
        if _text(identity.get("last_event_id")) != event_token:
            identity["event_count"] = _int(identity.get("event_count"), 0, minimum=0) + 1
            identity["last_event_id"] = event_token
        identity["last_seen"] = timestamp
        identity["last_distance"] = round(max(0.0, float(distance)), 5)
        existing.insert(
            0,
            _detection_observation(
                detection,
                embedding=embedding,
                event_id=event_token,
                seen_at=timestamp,
                quality=quality,
                source=source,
            ),
        )
        identity["observations"] = existing[:OBSERVATION_LIMIT]
        if quality >= _float(identity.get("best_quality")) and _text(detection.get("crop_b64")):
            identity["best_quality"] = round(quality, 5)
            identity["face_b64"] = _text(detection.get("crop_b64"))
            identity["face_content_type"] = _text(detection.get("crop_content_type")) or "image/jpeg"
        identity["reference_centroids"] = curate_reference_embeddings(identity)
        saved = save_identity(identity, client)
        _save_event_identity_ids(client, event_token, [*_event_identity_ids(client, event_token), identity_id])
        return saved


def runtime_status(redis_client: Any = None) -> Dict[str, Any]:
    try:
        return dict(face_id_runtime.status(_client(redis_client)) or {})
    except Exception as exc:
        return {"enabled": False, "loaded": False, "state": "error", "error": _text(exc)}


def recognize_image(
    image_bytes: bytes,
    *,
    event_id: str = "",
    seen_at: str = "",
    source: Optional[Dict[str, Any]] = None,
    record: bool = True,
    redis_client: Any = None,
) -> Dict[str, Any]:
    client = _client(redis_client)
    status = runtime_status(client)
    if not bool(status.get("enabled")):
        return {"status": "disabled", "warning": "Face ID is disabled in Settings › Models.", "people": [], "identity_ids": []}
    if spud_link_should_use_hub("face_id", redis_conn=client):
        try:
            remote = spud_link_request_json(
                "models/face-id",
                payload={
                    "data_base64": base64.b64encode(bytes(image_bytes or b"")).decode("ascii"),
                    "filename": "face.jpg",
                    "mimetype": "image/jpeg",
                    "event_id": _text(event_id),
                    "seen_at": _text(seen_at),
                    "source": dict(source or {}),
                    "record": bool(record),
                },
                redis_conn=client,
                timeout=180.0,
            )
            result = remote.get("result") if isinstance(remote.get("result"), dict) else {}
            result["routed_via"] = "spud_link"
            result["loaded_on"] = "Spud Hub"
            return result
        except Exception:
            if not spud_link_allow_local_fallback("face_id", redis_conn=client):
                raise
    if not bool(status.get("loaded")):
        return {"status": "not_ready", "warning": "Face ID is enabled but its model is not ready yet.", "people": [], "identity_ids": []}
    try:
        detections = list(face_id_runtime.analyze_image(image_bytes, client) or [])
    except Exception as exc:
        return {"status": "error", "warning": _text(exc) or "Face ID analysis failed.", "people": [], "identity_ids": []}

    identities_before = identity_rows(client)
    identity_ids: List[str] = []
    if record:
        for detection in detections:
            if not isinstance(detection, dict):
                continue
            with contextlib.suppress(ValueError):
                identity = record_detection(
                    detection,
                    event_id=_text(event_id) or f"face_event_{uuid.uuid4().hex[:16]}",
                    seen_at=seen_at,
                    source=source,
                    redis_client=client,
                )
                identity_id = _text(identity.get("id"))
                if identity_id and identity_id not in identity_ids:
                    identity_ids.append(identity_id)
    else:
        for detection in detections:
            if not isinstance(detection, dict):
                continue
            embedding = valid_embedding(detection.get("embedding"))
            identity_id, _distance = match_identity(identities_before, embedding)
            if identity_id and identity_id not in identity_ids:
                identity_ids.append(identity_id)

    recognized = recognized_people(identity_ids, client)
    names = [row["person_name"] for row in recognized]
    return {
        "status": "recognized" if names else ("unrecognized" if detections else "no_faces"),
        "warning": "",
        "people": names,
        "person_ids": [row["person_id"] for row in recognized],
        "identity_ids": identity_ids,
        "faces_detected": len([row for row in detections if isinstance(row, dict)]),
    }


def _people_module() -> Any:
    import people

    return people


def person_name(person_id: Any, redis_client: Any = None) -> str:
    wanted = _text(person_id)
    if not wanted:
        return ""
    with contextlib.suppress(Exception):
        store = _people_module().load_store(_client(redis_client))
        for person in store.get("people") or []:
            if _text(person.get("id")) == wanted:
                return _text(person.get("display_name"))
    return ""


def display_name(identity: Dict[str, Any], redis_client: Any = None) -> str:
    return person_name(identity.get("person_id"), redis_client) or _text(identity.get("name") or identity.get("person_name"))


def recognized_people(identity_ids: Iterable[Any], redis_client: Any = None) -> List[Dict[str, Any]]:
    client = _client(redis_client)
    identities = identity_rows(client)
    grouped: Dict[str, Dict[str, Any]] = {}
    for raw_id in identity_ids:
        identity_id = resolve_identity_id(raw_id, client)
        identity = identities.get(identity_id) or {}
        person_id = _text(identity.get("person_id"))
        name = person_name(person_id, client) if person_id else ""
        if not person_id or not name:
            continue
        row = grouped.setdefault(person_id, {"person_id": person_id, "person_name": name, "face_identity_ids": []})
        if identity_id and identity_id not in row["face_identity_ids"]:
            row["face_identity_ids"].append(identity_id)
    return list(grouped.values())


def save_profile(
    identity_id: str,
    *,
    name: str = "",
    person_id: str = "",
    person_link_supplied: bool = True,
    redis_client: Any = None,
) -> Dict[str, Any]:
    client = _client(redis_client)
    token = resolve_identity_id(identity_id, client)
    with _identity_lock:
        identity = dict(identity_rows(client).get(token) or {})
        if not identity:
            raise KeyError("Face identity not found.")
        old_person_id = _text(identity.get("person_id"))
        requested_person_id = _text(person_id)
        clean_name = " ".join(_text(name).split())
        if len(clean_name) > 80:
            raise ValueError("Person name must be 80 characters or fewer.")
        people = _people_module()
        if person_link_supplied and requested_person_id:
            linked_name = person_name(requested_person_id, client)
            if not linked_name:
                raise ValueError("Choose an existing Tater Person.")
            people.attach_alias(
                person_id=requested_person_id,
                platform=FACE_ALIAS_PLATFORM,
                external_id=token,
                label=linked_name,
                kind="face_identity",
                redis_client=client,
            )
            if old_person_id and old_person_id != requested_person_id:
                with contextlib.suppress(KeyError):
                    people.detach_alias(
                        person_id=old_person_id,
                        platform=FACE_ALIAS_PLATFORM,
                        external_id=token,
                        redis_client=client,
                    )
            identity.update({"person_id": requested_person_id, "person_name": linked_name, "name": linked_name})
        elif person_link_supplied:
            if old_person_id:
                with contextlib.suppress(KeyError):
                    people.detach_alias(
                        person_id=old_person_id,
                        platform=FACE_ALIAS_PLATFORM,
                        external_id=token,
                        redis_client=client,
                    )
            identity.pop("person_id", None)
            identity.pop("person_name", None)
            identity["name"] = clean_name
        else:
            identity["name"] = clean_name
        identity["updated_at"] = _now_iso()
        return save_identity(identity, client)


def detach_person(person_id: str, redis_client: Any = None) -> int:
    client = _client(redis_client)
    wanted = _text(person_id)
    changed = 0
    for identity in identity_rows(client).values():
        if _text(identity.get("person_id")) != wanted:
            continue
        payload = dict(identity)
        payload.pop("person_id", None)
        payload.pop("person_name", None)
        if _text(payload.get("name")) == person_name(wanted, client):
            payload["name"] = ""
        payload["updated_at"] = _now_iso()
        save_identity(payload, client)
        changed += 1
    return changed


def merge_identities(source_id: str, target_id: str, redis_client: Any = None) -> Dict[str, Any]:
    client = _client(redis_client)
    source_token = resolve_identity_id(source_id, client)
    target_token = resolve_identity_id(target_id, client)
    if not source_token or not target_token or source_token == target_token:
        identity = identity_rows(client).get(target_token or source_token)
        if not identity:
            raise KeyError("Face identity not found.")
        return identity
    with _identity_lock:
        identities = identity_rows(client)
        source = dict(identities.get(source_token) or {})
        target = dict(identities.get(target_token) or {})
        if not source or not target:
            raise KeyError("Face identity not found.")
        source_person_id = _text(source.get("person_id"))
        if not _text(target.get("person_id")) and source_person_id:
            target["person_id"] = source_person_id
            target["person_name"] = person_name(source_person_id, client)
            target["name"] = target["person_name"]
        target_observations = observations(target)
        source_observations = observations(source)
        if not target_observations and reference_embeddings(target):
            target["anchor_references"] = reference_embeddings(target)
        if not source_observations and reference_embeddings(source):
            target["anchor_references"] = [*(target.get("anchor_references") or []), *reference_embeddings(source)]
        target = rebuild_identity(target, [*target_observations, *source_observations], keep_name=True)
        target["observation_count"] = _int(target.get("observation_count"), 0, minimum=0)
        target["merged_identity_ids"] = list(
            dict.fromkeys(
                value
                for value in [*(target.get("merged_identity_ids") or []), source_token, *(source.get("merged_identity_ids") or [])]
                if _text(value)
            )
        )
        if not display_name(target, client) and display_name(source, client):
            target["name"] = display_name(source, client)
        target["reference_centroids"] = curate_reference_embeddings(target, extra_references=reference_embeddings(source))
        saved = save_identity(target, client)
        _set_identity_alias(client, source_token, target_token)
        _delete_identity_row(source_token, client)
        event_ids = {_text(row.get("event_id")) for row in [*target_observations, *source_observations] if _text(row.get("event_id"))}
        for event_id in event_ids:
            current = _event_identity_ids(client, event_id)
            _save_event_identity_ids(client, event_id, [target_token if value == source_token else value for value in current] or [target_token])
    people = _people_module()
    source_person_id = _text(source.get("person_id"))
    if source_person_id:
        with contextlib.suppress(KeyError):
            people.detach_alias(person_id=source_person_id, platform=FACE_ALIAS_PLATFORM, external_id=source_token, redis_client=client)
    if _text(saved.get("person_id")):
        people.attach_alias(
            person_id=_text(saved.get("person_id")),
            platform=FACE_ALIAS_PLATFORM,
            external_id=target_token,
            label=display_name(saved, client),
            kind="face_identity",
            redis_client=client,
        )
    return saved


def move_observations(
    source_id: str,
    observation_ids: Iterable[Any],
    *,
    target_id: str = "",
    create_unknown: bool = False,
    redis_client: Any = None,
) -> Dict[str, Any]:
    client = _client(redis_client)
    source_token = resolve_identity_id(source_id, client)
    selected_ids = {_text(value) for value in observation_ids if _text(value)}
    if not selected_ids:
        raise ValueError("Select at least one face image to move.")
    with _identity_lock:
        identities = identity_rows(client)
        source = dict(identities.get(source_token) or {})
        if not source:
            raise KeyError("Face identity not found.")
        source_rows = observations(source)
        selected = [row for row in source_rows if _text(row.get("id")) in selected_ids]
        remaining = [row for row in source_rows if _text(row.get("id")) not in selected_ids]
        if len(selected) != len(selected_ids):
            raise ValueError("One or more selected face images are no longer available.")
        if not all(valid_embedding(row.get("embedding")) for row in selected):
            raise ValueError("One or more selected face captures no longer has a saved face vector.")
        target_token = resolve_identity_id(target_id, client)
        if create_unknown or not target_token:
            target_token = f"face_{uuid.uuid4().hex[:16]}"
            target = {"id": target_token, "name": "", "created_at": _now_iso()}
        else:
            if target_token == source_token:
                raise ValueError("Choose a different person for the selected images.")
            target = dict(identities.get(target_token) or {})
            if not target:
                raise KeyError("Destination person not found.")
        target = rebuild_identity(target, [*observations(target), *selected], keep_name=True)
        target = save_identity(target, client)
        source_removed = not remaining
        if source_removed:
            _set_identity_alias(client, source_token, DELETED_IDENTITY)
            _delete_identity_row(source_token, client)
        else:
            source = save_identity(rebuild_identity(source, remaining, keep_name=True), client)
        selected_events = {_text(row.get("event_id")) for row in selected if _text(row.get("event_id"))}
        remaining_events = {_text(row.get("event_id")) for row in remaining if _text(row.get("event_id"))}
        for event_id in selected_events:
            current = [value for value in _event_identity_ids(client, event_id) if value != source_token]
            if event_id in remaining_events:
                current.append(source_token)
            current.append(target_token)
            _save_event_identity_ids(client, event_id, current)
    if source_removed and _text(source.get("person_id")):
        with contextlib.suppress(KeyError):
            _people_module().detach_alias(
                person_id=_text(source.get("person_id")),
                platform=FACE_ALIAS_PLATFORM,
                external_id=source_token,
                redis_client=client,
            )
    return {"source": {} if source_removed else source, "target": target, "source_removed": source_removed, "moved": len(selected)}


def remove_observations(identity_id: str, observation_ids: Iterable[Any], redis_client: Any = None) -> Dict[str, Any]:
    client = _client(redis_client)
    token = resolve_identity_id(identity_id, client)
    selected_ids = {_text(value) for value in observation_ids if _text(value)}
    if not selected_ids:
        raise ValueError("Select at least one face image to remove.")
    with _identity_lock:
        identity = dict(identity_rows(client).get(token) or {})
        if not identity:
            raise KeyError("Face identity not found.")
        current = observations(identity)
        selected = [row for row in current if _text(row.get("id")) in selected_ids]
        remaining = [row for row in current if _text(row.get("id")) not in selected_ids]
        if len(selected) != len(selected_ids):
            raise ValueError("One or more selected face images are no longer available.")
        identity = save_identity(rebuild_identity(identity, remaining, keep_name=True), client)
        selected_events = {_text(row.get("event_id")) for row in selected if _text(row.get("event_id"))}
        remaining_events = {_text(row.get("event_id")) for row in remaining if _text(row.get("event_id"))}
        for event_id in selected_events - remaining_events:
            _save_event_identity_ids(client, event_id, [value for value in _event_identity_ids(client, event_id) if value != token])
    return {"identity": identity, "removed": len(selected)}


def delete_identity(identity_id: str, redis_client: Any = None) -> bool:
    client = _client(redis_client)
    token = resolve_identity_id(identity_id, client)
    identity = identity_rows(client).get(token) or {}
    if not token or not identity:
        return False
    if _text(identity.get("person_id")):
        with contextlib.suppress(KeyError):
            _people_module().detach_alias(
                person_id=_text(identity.get("person_id")),
                platform=FACE_ALIAS_PLATFORM,
                external_id=token,
                redis_client=client,
            )
    for event_id in {_text(row.get("event_id")) for row in observations(identity) if _text(row.get("event_id"))}:
        _save_event_identity_ids(client, event_id, [value for value in _event_identity_ids(client, event_id) if value != token])
    _set_identity_alias(client, token, DELETED_IDENTITY)
    return _delete_identity_row(token, client)


def ui_rows(redis_client: Any = None) -> List[Dict[str, Any]]:
    client = _client(redis_client)
    rows: List[Dict[str, Any]] = []
    for identity in identity_rows(client).values():
        identity_id = _text(identity.get("id"))
        name = display_name(identity, client)
        gallery = []
        for observation in observations(identity):
            face_b64 = _text(observation.get("face_b64"))
            observation_id = _text(observation.get("id"))
            if not face_b64 or not observation_id or not valid_embedding(observation.get("embedding")):
                continue
            gallery.append(
                {
                    "id": observation_id,
                    "image_src": f"data:{_text(observation.get('face_content_type')) or 'image/jpeg'};base64,{face_b64}",
                    "seen_at": _text(observation.get("seen_at")),
                    "event_id": _text(observation.get("event_id")),
                    "source": observation.get("source") if isinstance(observation.get("source"), dict) else {},
                }
            )
        hero_b64 = _text(identity.get("face_b64"))
        hero_type = _text(identity.get("face_content_type")) or "image/jpeg"
        rows.append(
            {
                "id": identity_id,
                "name": name,
                "local_name": _text(identity.get("name")),
                "person_id": _text(identity.get("person_id")),
                "person_name": person_name(identity.get("person_id"), client),
                "linked": bool(_text(identity.get("person_id"))),
                "image_src": f"data:{hero_type};base64,{hero_b64}" if hero_b64 else (gallery[0]["image_src"] if gallery else ""),
                "last_seen": _text(identity.get("last_seen")),
                "first_seen": _text(identity.get("first_seen")),
                "event_count": _int(identity.get("event_count"), 0, minimum=0),
                "capture_count": _int(identity.get("observation_count"), len(gallery), minimum=0),
                "gallery": gallery,
            }
        )
    rows.sort(key=lambda row: (0 if row["linked"] else 1, _text(row.get("name")).casefold(), _text(row.get("last_seen"))), reverse=False)
    return rows


def service_status(redis_client: Any = None) -> Dict[str, Any]:
    runtime = runtime_status(redis_client)
    rows = identity_rows(redis_client)
    return {
        **runtime,
        "identity_count": len(rows),
        "known_identity_count": len([row for row in rows.values() if display_name(row, redis_client)]),
        "storage_key": SHARED_IDENTITIES_KEY,
    }
