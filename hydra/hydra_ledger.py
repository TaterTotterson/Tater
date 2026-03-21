import hashlib
import json
import time
import uuid
from typing import Any, Callable, Dict, Optional, Sequence


def hash_tool_args(args: Any) -> str:
    if not isinstance(args, dict):
        return ""
    try:
        payload = json.dumps(args, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except Exception:
        payload = str(args)
    digest = hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()
    return f"sha256:{digest}"


def compact_tool_ref(
    tool_call: Optional[Dict[str, Any]],
    *,
    hash_tool_args_fn: Callable[[Any], str] = hash_tool_args,
) -> Optional[Dict[str, Any]]:
    if not isinstance(tool_call, dict):
        return None
    func = str(tool_call.get("function") or "").strip()
    if not func:
        return None
    args = tool_call.get("arguments") if isinstance(tool_call.get("arguments"), dict) else {}
    out: Dict[str, Any] = {"function": func}
    args_hash = hash_tool_args_fn(args)
    if args_hash:
        out["args_hash"] = args_hash
    return out


def validation_status_for_ledger(
    *,
    validation_status: Optional[Dict[str, Any]],
    planned_tool: Optional[Dict[str, Any]],
    short_text_fn: Callable[..., str],
) -> Dict[str, Any]:
    source = validation_status if isinstance(validation_status, dict) else {}
    has_planned_tool = isinstance(planned_tool, dict) and bool(str(planned_tool.get("function") or "").strip())

    if not has_planned_tool:
        raw_status = str(source.get("status") or "").strip().lower()
        if raw_status == "skipped" or "reason" in source:
            repair_used = bool(source.get("repair_used"))
            attempts = source.get("attempts")
            try:
                attempts_i = int(attempts) if attempts is not None else 0
            except Exception:
                attempts_i = 0
            out: Dict[str, Any] = {
                "status": "skipped",
                "repair_used": repair_used,
                "reason": short_text_fn(source.get("reason") or "no_tool", limit=64),
                "attempts": max(0, attempts_i),
            }
            error_text = short_text_fn(source.get("error"), limit=180)
            if error_text:
                out["error"] = error_text
            return out

        if "ok" in source and not bool(source.get("ok")):
            reason = short_text_fn(source.get("reason") or "no_tool", limit=64)
            out = {
                "status": "skipped",
                "repair_used": False,
                "reason": reason or "no_tool",
                "attempts": 0,
            }
            error_text = short_text_fn(source.get("error"), limit=180)
            if error_text:
                out["error"] = error_text
            return out

        return {
            "status": "skipped",
            "repair_used": False,
            "reason": "no_tool",
            "attempts": 0,
        }

    raw_status = str(source.get("status") or "").strip().lower()
    if raw_status in {"ok", "failed", "skipped"}:
        status = raw_status
        repair_used = bool(source.get("repair_used"))
        attempts = source.get("attempts")
        try:
            attempts_i = int(attempts) if attempts is not None else (2 if repair_used else 1)
        except Exception:
            attempts_i = 2 if repair_used else 1
        reason = short_text_fn(source.get("reason") or ("repaired" if status == "ok" and repair_used else status), limit=64)
        out: Dict[str, Any] = {
            "status": status,
            "repair_used": repair_used,
            "reason": reason or ("failed" if status == "failed" else "ok"),
            "attempts": max(0, attempts_i),
        }
        error_text = short_text_fn(source.get("error"), limit=180)
        if error_text:
            out["error"] = error_text
        return out

    ok = bool(source.get("ok"))
    repair_used = bool(source.get("repair_used"))
    reason = short_text_fn(source.get("reason"), limit=64)
    error_text = short_text_fn(source.get("error"), limit=180)
    attempts_i = 2 if repair_used else 1
    if ok:
        out = {
            "status": "ok",
            "repair_used": repair_used,
            "reason": "repaired" if repair_used else (reason or "ok"),
            "attempts": attempts_i,
        }
        if error_text:
            out["error"] = error_text
        return out

    out = {
        "status": "failed",
        "repair_used": repair_used,
        "reason": reason or "invalid_tool_call",
        "attempts": attempts_i,
    }
    if error_text:
        out["error"] = error_text
    return out


def write_hydra_metrics(
    *,
    redis_client: Any,
    platform: str,
    total_tools_called: int,
    total_repairs: int,
    validation_failures: int,
    tool_failures: int,
    normalize_platform_fn: Callable[[str], str],
) -> None:
    if redis_client is None:
        return
    p = normalize_platform_fn(platform)
    counters = {
        "total_turns": 1,
        "total_tools_called": max(0, int(total_tools_called or 0)),
        "total_repairs": max(0, int(total_repairs or 0)),
        "validation_failures": max(0, int(validation_failures or 0)),
        "tool_failures": max(0, int(tool_failures or 0)),
    }
    for name, amount in counters.items():
        if amount <= 0:
            continue
        keys = [
            f"tater:hydra:metrics:{name}",
            f"tater:hydra:metrics:{name}:{p}",
        ]
        for key in keys:
            try:
                redis_client.incrby(key, amount)
            except Exception:
                continue


def write_hydra_ledger(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
    turn_id: str,
    llm: str,
    user_message: str,
    planned_tool: Optional[Dict[str, Any]],
    validation_status: Dict[str, Any],
    tool_result: Optional[Dict[str, Any]],
    checker_action: str,
    retry_count: int = 0,
    checker_reason: str = "",
    planner_kind: str = "",
    planner_text_is_tool_candidate: Optional[bool] = None,
    outcome: str = "",
    outcome_reason: str = "",
    planner_ms: int = 0,
    tool_ms: int = 0,
    checker_ms: int = 0,
    total_ms: int = 0,
    retry_tool: Optional[Dict[str, Any]] = None,
    rounds_used: int = 0,
    tool_calls_used: int = 0,
    agent_state: Optional[Dict[str, Any]] = None,
    origin_preview: Optional[Dict[str, Any]] = None,
    attempted_tool: str = "",
    compact_tool_ref_fn: Callable[[Optional[Dict[str, Any]]], Optional[Dict[str, Any]]],
    validation_status_for_ledger_fn: Callable[..., Dict[str, Any]],
    short_text_fn: Callable[..., str],
    compact_agent_state_json_fn: Callable[..., str],
    agent_state_hash_fn: Callable[..., str],
    configured_max_ledger_items_fn: Callable[[Any], int],
    schema_version: str,
    agent_state_ledger_max_chars: int,
    allowed_planner_kinds: Sequence[str],
) -> None:
    if redis_client is None:
        return
    compact_planned_tool = compact_tool_ref_fn(planned_tool)
    compact_retry_tool = compact_tool_ref_fn(retry_tool)
    compact_validation = validation_status_for_ledger_fn(
        validation_status=validation_status,
        planned_tool=planned_tool,
    )
    planner_kind_value = str(planner_kind or "").strip().lower()
    if planner_kind_value not in set(allowed_planner_kinds or []):
        planner_kind_value = "answer"
    entry = {
        "schema_version": schema_version,
        "timestamp": time.time(),
        "platform": platform,
        "scope": scope,
        "turn_id": str(turn_id or uuid.uuid4()),
        "llm": str(llm or ""),
        "user_message": str(user_message or "")[:1500],
        "planned_tool": compact_planned_tool,
        "validation": compact_validation,
        "tool_result": None,
        "checker_action": str(checker_action or ""),
        "planner_kind": planner_kind_value,
        "planner_text_is_tool_candidate": bool(planner_text_is_tool_candidate)
        if planner_text_is_tool_candidate is not None
        else bool(compact_planned_tool),
        "validation_reason": short_text_fn(compact_validation.get("reason"), limit=64),
        "outcome": short_text_fn(outcome, limit=16) or "done",
        "outcome_reason": short_text_fn(outcome_reason, limit=96),
        "planner_ms": max(0, int(planner_ms or 0)),
        "tool_ms": max(0, int(tool_ms or 0)),
        "checker_ms": max(0, int(checker_ms or 0)),
        "total_ms": max(0, int(total_ms or 0)),
        "retry_count": 1 if int(retry_count or 0) > 0 else 0,
        "rounds_used": max(0, int(rounds_used or 0)),
        "tool_calls_used": max(0, int(tool_calls_used or 0)),
    }
    if compact_planned_tool and compact_planned_tool.get("args_hash"):
        entry["tool_args_hash"] = compact_planned_tool.get("args_hash")
    checker_reason_text = short_text_fn(checker_reason, limit=72)
    if checker_reason_text:
        entry["checker_reason"] = checker_reason_text
    attempted_tool_text = short_text_fn(attempted_tool, limit=80)
    if attempted_tool_text:
        entry["attempted_tool"] = attempted_tool_text
    if compact_retry_tool:
        entry["retry_tool"] = compact_retry_tool
    if isinstance(origin_preview, dict) and origin_preview:
        compact_origin: Dict[str, str] = {}
        for key, value in origin_preview.items():
            k = short_text_fn(key, limit=24)
            v = short_text_fn(value, limit=72)
            if not k or not v:
                continue
            compact_origin[k] = v
            if len(compact_origin) >= 6:
                break
        if compact_origin:
            entry["origin_preview"] = compact_origin
    if isinstance(agent_state, dict):
        state_payload = compact_agent_state_json_fn(
            agent_state,
            fallback_goal=str(user_message or ""),
            limit=agent_state_ledger_max_chars,
        )
        if state_payload:
            entry["state_snapshot"] = state_payload
            entry["state_hash"] = agent_state_hash_fn(agent_state, fallback_goal=str(user_message or ""))
    if isinstance(tool_result, dict):
        result_ok = bool(tool_result.get("ok"))
        summary = short_text_fn(tool_result.get("summary_for_user"), limit=320)
        errors = []
        raw_errors = tool_result.get("errors")
        if isinstance(raw_errors, list):
            for item in raw_errors:
                text = short_text_fn(item, limit=180)
                if text:
                    errors.append(text)
                if len(errors) >= 3:
                    break
        compact_result: Dict[str, Any] = {"ok": result_ok}
        if summary:
            compact_result["summary"] = summary
        if errors:
            compact_result["errors"] = errors
        entry["tool_result"] = compact_result
        entry["tool_result_ok"] = result_ok
        if summary:
            entry["tool_result_summary"] = summary
    payload = json.dumps(entry, ensure_ascii=False)

    keys = ["tater:hydra:ledger", f"tater:hydra:ledger:{platform}"]
    max_items = configured_max_ledger_items_fn(redis_client)
    for key in keys:
        try:
            redis_client.rpush(key, payload)
            redis_client.ltrim(key, -max_items, -1)
        except Exception:
            continue
