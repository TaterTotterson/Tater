import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from helpers import redis_client


PLUGIN_DIR = Path(os.getenv("TATER_PLUGIN_DIR", "plugins"))
PLATFORM_DIR = Path(os.getenv("TATER_PLATFORM_DIR", "platforms"))
PLUGIN_CANDIDATE_DIR = Path(os.getenv("TATER_PLUGIN_CANDIDATE_DIR", "plugins_johnny5"))
PLATFORM_CANDIDATE_DIR = Path(os.getenv("TATER_PLATFORM_CANDIDATE_DIR", "platforms_johnny5"))

LLM_REQ_KEY = "tater:johnny5:llm:req"
LLM_RESP_PREFIX = "tater:johnny5:llm:resp:"
EVENTS_KEY = "tater:johnny5:events"
GLOBAL_FOCUS_KEY = "tater:johnny5:global:focus"
GLOBAL_ERRORS_KEY = "tater:johnny5:global:recent_errors"
GLOBAL_CHANGES_KEY = "tater:johnny5:global:changes"
GLOBAL_POLICIES_KEY = "tater:johnny5:global:policies"
CANDIDATE_PLUGINS_KEY = "tater:johnny5:candidates:plugins"
CANDIDATE_PLATFORMS_KEY = "tater:johnny5:candidates:platforms"
MASTER_ENABLE_KEY = "tater:johnny5:enabled"
HEARTBEAT_KEY = "tater:johnny5:heartbeat"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_candidate_dirs() -> None:
    PLUGIN_CANDIDATE_DIR.mkdir(parents=True, exist_ok=True)
    PLATFORM_CANDIDATE_DIR.mkdir(parents=True, exist_ok=True)


def is_enabled() -> bool:
    return redis_client.get(MASTER_ENABLE_KEY) == "1"


def set_enabled(enabled: bool) -> None:
    redis_client.set(MASTER_ENABLE_KEY, "1" if enabled else "0")


def set_heartbeat() -> None:
    redis_client.set(HEARTBEAT_KEY, _now_iso())


def candidate_hash_key(kind: str) -> str:
    return CANDIDATE_PLUGINS_KEY if kind == "plugin" else CANDIDATE_PLATFORMS_KEY


def override_key(kind: str, candidate_id: str) -> str:
    return f"tater:johnny5:override:{kind}:{candidate_id}"


def is_override_enabled(kind: str, candidate_id: str) -> bool:
    return redis_client.get(override_key(kind, candidate_id)) == "1"


def set_override(kind: str, candidate_id: str, enabled: bool) -> None:
    redis_client.set(override_key(kind, candidate_id), "1" if enabled else "0")


def record_event(event_type: str, payload: Dict[str, Any]) -> None:
    data = {"type": event_type, "ts": _now_iso(), **payload}
    redis_client.rpush(EVENTS_KEY, json.dumps(data))
    redis_client.ltrim(EVENTS_KEY, -2000, -1)


def record_change(text: str) -> None:
    redis_client.lpush(GLOBAL_CHANGES_KEY, text)
    redis_client.ltrim(GLOBAL_CHANGES_KEY, 0, 49)


def record_error(text: str) -> None:
    redis_client.lpush(GLOBAL_ERRORS_KEY, text)
    redis_client.ltrim(GLOBAL_ERRORS_KEY, 0, 49)


def get_global_focus() -> str:
    return redis_client.get(GLOBAL_FOCUS_KEY) or ""


def set_global_focus(text: str) -> None:
    redis_client.set(GLOBAL_FOCUS_KEY, text or "")


def list_candidates(kind: str) -> Dict[str, Dict[str, Any]]:
    raw = redis_client.hgetall(candidate_hash_key(kind))
    out: Dict[str, Dict[str, Any]] = {}
    for key, value in raw.items():
        try:
            out[key] = json.loads(value)
        except Exception:
            out[key] = {"id": key, "status": "unknown", "raw": value}
    return out


def upsert_candidate(kind: str, candidate_id: str, meta: Dict[str, Any]) -> None:
    redis_client.hset(candidate_hash_key(kind), candidate_id, json.dumps(meta))


def get_candidate(kind: str, candidate_id: str) -> Optional[Dict[str, Any]]:
    raw = redis_client.hget(candidate_hash_key(kind), candidate_id)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def update_candidate_status(kind: str, candidate_id: str, status: str, last_test: Optional[Dict[str, Any]] = None) -> None:
    meta = get_candidate(kind, candidate_id) or {"id": candidate_id, "type": kind}
    meta["status"] = status
    meta["updated_at"] = _now_iso()
    if last_test is not None:
        meta["last_test"] = last_test
    upsert_candidate(kind, candidate_id, meta)


def candidate_path(kind: str, candidate_id: str) -> Path:
    base = PLUGIN_CANDIDATE_DIR if kind == "plugin" else PLATFORM_CANDIDATE_DIR
    return base / f"{candidate_id}.py"


def stable_path(kind: str, candidate_id: str) -> Path:
    base = PLUGIN_DIR if kind == "plugin" else PLATFORM_DIR
    return base / f"{candidate_id}.py"


def promote_candidate(kind: str, candidate_id: str) -> Dict[str, Any]:
    meta = get_candidate(kind, candidate_id)
    if not meta:
        return {"ok": False, "error": "Candidate not found."}

    last_test = meta.get("last_test") or {}
    if not last_test.get("ok"):
        return {"ok": False, "error": "Candidate has not passed smoke tests."}

    src = candidate_path(kind, candidate_id)
    dst = stable_path(kind, candidate_id)
    if not src.exists():
        return {"ok": False, "error": "Candidate file not found."}

    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)

    meta["status"] = "promoted"
    meta["promotion"] = {"eligible": True, "reason": "manual promote"}
    meta["updated_at"] = _now_iso()
    upsert_candidate(kind, candidate_id, meta)
    set_override(kind, candidate_id, False)
    return {"ok": True}


@dataclass
class CandidateSpec:
    candidate_id: str
    kind: str
    path: Path
    base_id: Optional[str] = None
