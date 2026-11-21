"""Prepare a lossless face cleanup from explicitly confirmed reference photos.

Planning is pure: callers can review/backup the result before committing it to
Redis. No old person assignment or trust flag is accepted as ground truth.
"""

from __future__ import annotations

import copy
import hashlib
from collections import Counter, defaultdict
from typing import Any

import numpy as np

from tater.identity import faces
from tater.identity.model import ANONYMOUS_MATCH_THRESHOLD, EMBEDDING_DIMENSIONS, MATCH_MIN_MARGIN, MATCH_THRESHOLD


def _groups(items: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Complete-link clusters cannot join different faces through a chain."""
    if not items:
        return []
    vectors = np.asarray([item["vector"] for item in items], dtype=np.float64)
    distances = np.clip(1 - vectors @ vectors.T, 0, 2)
    np.fill_diagonal(distances, np.inf)
    groups = {index: [index] for index in range(len(items))}
    while len(groups) > 1:
        left, right = np.unravel_index(np.argmin(distances), distances.shape)
        if distances[left, right] > ANONYMOUS_MATCH_THRESHOLD:
            break
        groups[left].extend(groups.pop(right))
        distances[left, :] = np.maximum(distances[left, :], distances[right, :])
        distances[:, left] = distances[left, :]
        distances[right, :] = np.inf
        distances[:, right] = np.inf
        distances[left, left] = np.inf
    return sorted(
        ([items[index] for index in indices] for indices in groups.values()),
        key=lambda group: (-len(group), min(item["row"]["id"] for item in group)),
    )


def plan_cleanup(
    identities: dict[str, dict[str, Any]],
    embeddings: dict[str, list[float]],
    confirmed: dict[str, list[str]],
    rejected: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Rematch captures to confirmed references, preserving every stored photo."""
    metadata = faces.face_id_runtime.embedding_model_metadata()
    signature = faces._embedding_model_signature(metadata)
    rejected = {key: set(value) for key, value in (rejected or {}).items()}
    confirmed = {key: set(value) for key, value in confirmed.items()}
    if not confirmed:
        raise ValueError("Confirm at least one reference photo for each named person first.")
    named = {
        key for key, row in identities.items()
        if row.get("person_id") or row.get("person_name") or row.get("name")
    }
    if named != set(confirmed):
        raise ValueError("Every named face profile needs confirmed references before cleanup.")
    selected: dict[str, str] = {}
    for identity_id, references in confirmed.items():
        if not references:
            raise ValueError("A named person has no confirmed reference photos.")
        for observation_id in references:
            if observation_id in selected:
                raise ValueError("One photo cannot be confirmed for two different people.")
            if observation_id in rejected.get(identity_id, set()):
                raise ValueError("A photo cannot be both confirmed and rejected for a person.")
            selected[observation_id] = identity_id

    items = []
    seen: set[str] = set()
    for source_id, identity in sorted(identities.items()):
        for original in faces.observations(identity):
            row = copy.deepcopy(original)
            observation_id = row["id"]
            if observation_id in seen:
                raise ValueError("Duplicate observation IDs require review before cleanup.")
            seen.add(observation_id)
            vector = faces.valid_embedding(embeddings.get(observation_id), EMBEDDING_DIMENSIONS)
            norm = float(np.linalg.norm(vector)) if vector else 0
            vector = [value / norm for value in vector] if norm > 0 else []
            if observation_id in selected and not vector:
                raise ValueError("A confirmed reference has no usable SFace embedding.")
            row.setdefault("embedding_model_signature", identity.get("embedding_model_signature", ""))
            row["reference_status"] = "trusted" if observation_id in selected else "provisional"
            row["face_cleanup"] = {
                "previous_identity_id": source_id,
                "previous_reference_status": original.get("reference_status", "unreviewed"),
                "user_confirmed": observation_id in selected,
            }
            if vector:
                cached = row.get("embeddings") if isinstance(row.get("embeddings"), dict) else {}
                row["embeddings"] = {
                    **cached,
                    signature: {"embedding": vector, "embedding_model": metadata},
                }
            else:
                row["face_cleanup"]["needs_review"] = "No usable face embedding from the saved photo."
            items.append({"source_id": source_id, "row": row, "vector": vector})
    if not set(selected).issubset(seen):
        raise ValueError("A confirmed photo is no longer present. Refresh the review first.")
    references = {
        identity_id: [item["vector"] for item in items if item["row"]["id"] in ids]
        for identity_id, ids in confirmed.items()
    }
    buckets: dict[str, list[dict[str, Any]]] = {identity_id: [] for identity_id in confirmed}
    unmatched = []
    failed: dict[str, list[dict[str, Any]]] = defaultdict(list)
    matched = Counter()
    for item in items:
        observation_id = item["row"]["id"]
        target = selected.get(observation_id, "")
        if not target and item["vector"]:
            ranking = sorted(
                (min(faces.cosine_distance(item["vector"], ref) for ref in refs), identity_id)
                for identity_id, refs in references.items()
                if observation_id not in rejected.get(identity_id, set())
            )
            if ranking:
                best = ranking[0]
                runner = ranking[1][0] if len(ranking) > 1 else float("inf")
                if best[0] <= MATCH_THRESHOLD and runner - best[0] >= MATCH_MIN_MARGIN:
                    target = best[1]
        if target:
            buckets[target].append(item)
            matched[target] += 1
        elif item["vector"]:
            unmatched.append(item)
        else:
            failed[item["source_id"]].append(item)

    groups = _groups(unmatched) + list(failed.values())
    used_ids = set(confirmed)
    for group in groups:
        # Reuse an existing unnamed profile where possible to preserve links.
        candidates = sorted({item["source_id"] for item in group} - used_ids - named)
        if candidates:
            identity_id = candidates[0]
        else:
            token = "|".join(sorted(item["row"]["id"] for item in group))
            identity_id = "face_" + hashlib.sha256(token.encode()).hexdigest()[:16]
        if identity_id in used_ids:
            raise ValueError("Generated cleanup profile ID collision.")
        used_ids.add(identity_id)
        buckets[identity_id] = group

    output = {}
    event_ids: dict[str, set[str]] = defaultdict(set)
    destinations: dict[str, set[str]] = defaultdict(set)
    reassigned = 0
    for identity_id, group in buckets.items():
        if len(group) > faces.OBSERVATION_LIMIT:
            raise ValueError("Cleanup would exceed a profile's capture limit; split the review into smaller groups.")
        base = copy.deepcopy(identities[identity_id]) if identity_id in confirmed else {"id": identity_id, "name": ""}
        if any(item["vector"] for item in group):
            base["embedding_model_signature"] = signature
            base["embedding_model"] = metadata
        result = faces.rebuild_identity(base, [item["row"] for item in group], keep_name=identity_id in confirmed)
        result["face_cleanup_reviewed"] = True
        if group and not any(item["vector"] for item in group):
            result["face_cleanup_needs_review"] = True
        output[identity_id] = result
        for item in group:
            destinations[item["source_id"]].add(identity_id)
            if item["source_id"] != identity_id:
                reassigned += 1
            if item["row"].get("event_id"):
                event_ids[item["row"]["event_id"]].add(identity_id)
    result_ids = [row["id"] for identity in output.values() for row in faces.observations(identity)]
    if len(result_ids) != len(seen) or set(result_ids) != seen:
        raise ValueError("Cleanup must preserve every capture exactly once.")
    aliases = {
        source_id: next(iter(targets)) if len(targets) == 1 else faces.DELETED_IDENTITY
        for source_id, targets in destinations.items() if source_id not in output
    }
    return {
        "identities": output,
        "events": {event_id: sorted(ids) for event_id, ids in event_ids.items()},
        "aliases": aliases,
        "summary": {
            "captures_preserved": len(seen), "confirmed_references": len(selected),
            "matched_people": dict(matched), "unknown_usable": len(unmatched),
            "needs_clearer_photo": sum(len(group) for group in failed.values()),
            "profiles_before": len(identities), "profiles_after": len(output),
            "captures_moved": reassigned,
        },
    }
