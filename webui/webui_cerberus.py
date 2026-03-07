import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import redis
import streamlit as st

from plugin_kernel import normalize_platform
from cerberus import (
    DEFAULT_MAX_ROUNDS,
    DEFAULT_MAX_TOOL_CALLS,
    DEFAULT_AGENT_STATE_TTL_SECONDS,
    DEFAULT_PLANNER_MAX_TOKENS,
    DEFAULT_CHECKER_MAX_TOKENS,
    DEFAULT_DOER_MAX_TOKENS,
    DEFAULT_TOOL_REPAIR_MAX_TOKENS,
    DEFAULT_RECOVERY_MAX_TOKENS,
    DEFAULT_MAX_LEDGER_ITEMS,
    AGENT_MAX_ROUNDS_KEY,
    AGENT_MAX_TOOL_CALLS_KEY,
    CERBERUS_AGENT_STATE_TTL_SECONDS_KEY,
    CERBERUS_PLANNER_MAX_TOKENS_KEY,
    CERBERUS_CHECKER_MAX_TOKENS_KEY,
    CERBERUS_DOER_MAX_TOKENS_KEY,
    CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY,
    CERBERUS_RECOVERY_MAX_TOKENS_KEY,
    CERBERUS_MAX_LEDGER_ITEMS_KEY,
)


redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', '127.0.0.1'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True,
)

def render_cerberus_settings():
    def _read_non_negative_int_setting(key: str, default: int) -> int:
        raw = redis_client.get(key)
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = raw.decode("utf-8", errors="ignore")
            except Exception:
                raw = None
        try:
            value = int(str(raw).strip()) if raw is not None else int(default)
        except Exception:
            value = int(default)
        if value < 0:
            return 0
        return value

    def _read_positive_int_setting(key: str, default: int) -> int:
        value = _read_non_negative_int_setting(key, default)
        if value <= 0:
            return int(default)
        return value

    def _cerberus_values_from_inputs() -> Dict[str, int]:
        return {
            AGENT_MAX_ROUNDS_KEY: int(new_max_rounds),
            AGENT_MAX_TOOL_CALLS_KEY: int(new_max_tool_calls),
            CERBERUS_AGENT_STATE_TTL_SECONDS_KEY: int(new_state_ttl_seconds),
            CERBERUS_PLANNER_MAX_TOKENS_KEY: int(new_planner_max_tokens),
            CERBERUS_CHECKER_MAX_TOKENS_KEY: int(new_checker_max_tokens),
            CERBERUS_DOER_MAX_TOKENS_KEY: int(new_doer_max_tokens),
            CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY: int(new_tool_repair_max_tokens),
            CERBERUS_RECOVERY_MAX_TOKENS_KEY: int(new_recovery_max_tokens),
            CERBERUS_MAX_LEDGER_ITEMS_KEY: int(new_max_ledger_items),
        }

    def _cerberus_default_values() -> Dict[str, int]:
        return {
            AGENT_MAX_ROUNDS_KEY: int(DEFAULT_MAX_ROUNDS),
            AGENT_MAX_TOOL_CALLS_KEY: int(DEFAULT_MAX_TOOL_CALLS),
            CERBERUS_AGENT_STATE_TTL_SECONDS_KEY: int(DEFAULT_AGENT_STATE_TTL_SECONDS),
            CERBERUS_PLANNER_MAX_TOKENS_KEY: int(DEFAULT_PLANNER_MAX_TOKENS),
            CERBERUS_CHECKER_MAX_TOKENS_KEY: int(DEFAULT_CHECKER_MAX_TOKENS),
            CERBERUS_DOER_MAX_TOKENS_KEY: int(DEFAULT_DOER_MAX_TOKENS),
            CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY: int(DEFAULT_TOOL_REPAIR_MAX_TOKENS),
            CERBERUS_RECOVERY_MAX_TOKENS_KEY: int(DEFAULT_RECOVERY_MAX_TOKENS),
            CERBERUS_MAX_LEDGER_ITEMS_KEY: int(DEFAULT_MAX_LEDGER_ITEMS),
        }

    def _apply_cerberus_settings(values: Dict[str, int]) -> None:
        for setting_key, setting_value in values.items():
            redis_client.set(setting_key, int(setting_value))

    def _sync_cerberus_widget_state(values: Dict[str, int]) -> None:
        st.session_state["cerberus_max_rounds"] = int(values.get(AGENT_MAX_ROUNDS_KEY, DEFAULT_MAX_ROUNDS))
        st.session_state["cerberus_max_tool_calls"] = int(values.get(AGENT_MAX_TOOL_CALLS_KEY, DEFAULT_MAX_TOOL_CALLS))
        st.session_state["cerberus_agent_state_ttl_seconds"] = int(
            values.get(CERBERUS_AGENT_STATE_TTL_SECONDS_KEY, DEFAULT_AGENT_STATE_TTL_SECONDS)
        )
        st.session_state["cerberus_planner_max_tokens"] = int(
            values.get(CERBERUS_PLANNER_MAX_TOKENS_KEY, DEFAULT_PLANNER_MAX_TOKENS)
        )
        st.session_state["cerberus_checker_max_tokens"] = int(
            values.get(CERBERUS_CHECKER_MAX_TOKENS_KEY, DEFAULT_CHECKER_MAX_TOKENS)
        )
        st.session_state["cerberus_doer_max_tokens"] = int(
            values.get(CERBERUS_DOER_MAX_TOKENS_KEY, DEFAULT_DOER_MAX_TOKENS)
        )
        st.session_state["cerberus_tool_repair_max_tokens"] = int(
            values.get(CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY, DEFAULT_TOOL_REPAIR_MAX_TOKENS)
        )
        st.session_state["cerberus_recovery_max_tokens"] = int(
            values.get(CERBERUS_RECOVERY_MAX_TOKENS_KEY, DEFAULT_RECOVERY_MAX_TOKENS)
        )
        st.session_state["cerberus_max_ledger_items"] = int(
            values.get(CERBERUS_MAX_LEDGER_ITEMS_KEY, DEFAULT_MAX_LEDGER_ITEMS)
        )

    st.subheader("Cerberus")
    st.caption("Planner / Doer / Critic runtime limits and token budgets.")

    max_rounds = _read_non_negative_int_setting(AGENT_MAX_ROUNDS_KEY, DEFAULT_MAX_ROUNDS)
    max_tool_calls = _read_non_negative_int_setting(AGENT_MAX_TOOL_CALLS_KEY, DEFAULT_MAX_TOOL_CALLS)
    state_ttl_seconds = _read_non_negative_int_setting(
        CERBERUS_AGENT_STATE_TTL_SECONDS_KEY,
        DEFAULT_AGENT_STATE_TTL_SECONDS,
    )
    planner_max_tokens = _read_positive_int_setting(
        CERBERUS_PLANNER_MAX_TOKENS_KEY,
        DEFAULT_PLANNER_MAX_TOKENS,
    )
    checker_max_tokens = _read_positive_int_setting(
        CERBERUS_CHECKER_MAX_TOKENS_KEY,
        DEFAULT_CHECKER_MAX_TOKENS,
    )
    doer_max_tokens = _read_positive_int_setting(
        CERBERUS_DOER_MAX_TOKENS_KEY,
        DEFAULT_DOER_MAX_TOKENS,
    )
    tool_repair_max_tokens = _read_positive_int_setting(
        CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY,
        DEFAULT_TOOL_REPAIR_MAX_TOKENS,
    )
    recovery_max_tokens = _read_positive_int_setting(
        CERBERUS_RECOVERY_MAX_TOKENS_KEY,
        DEFAULT_RECOVERY_MAX_TOKENS,
    )
    max_ledger_items = _read_positive_int_setting(
        CERBERUS_MAX_LEDGER_ITEMS_KEY,
        DEFAULT_MAX_LEDGER_ITEMS,
    )

    new_max_rounds = int(
        st.number_input(
            "Agent Max Rounds (0 = unlimited)",
            min_value=0,
            value=max_rounds,
            step=1,
            format="%d",
            key="cerberus_max_rounds",
        )
    )
    new_max_tool_calls = int(
        st.number_input(
            "Agent Max Tool Calls (0 = unlimited)",
            min_value=0,
            value=max_tool_calls,
            step=1,
            format="%d",
            key="cerberus_max_tool_calls",
        )
    )
    new_state_ttl_seconds = int(
        st.number_input(
            "Agent State TTL Seconds (0 = no TTL)",
            min_value=0,
            value=state_ttl_seconds,
            step=60,
            format="%d",
            key="cerberus_agent_state_ttl_seconds",
        )
    )
    new_planner_max_tokens = int(
        st.number_input(
            "Planner Max Tokens",
            min_value=1,
            value=planner_max_tokens,
            step=10,
            format="%d",
            key="cerberus_planner_max_tokens",
        )
    )
    new_checker_max_tokens = int(
        st.number_input(
            "Checker Max Tokens",
            min_value=1,
            value=checker_max_tokens,
            step=10,
            format="%d",
            key="cerberus_checker_max_tokens",
        )
    )
    new_doer_max_tokens = int(
        st.number_input(
            "Doer Max Tokens",
            min_value=1,
            value=doer_max_tokens,
            step=10,
            format="%d",
            key="cerberus_doer_max_tokens",
        )
    )
    new_tool_repair_max_tokens = int(
        st.number_input(
            "Tool-Repair Max Tokens",
            min_value=1,
            value=tool_repair_max_tokens,
            step=10,
            format="%d",
            key="cerberus_tool_repair_max_tokens",
        )
    )
    new_recovery_max_tokens = int(
        st.number_input(
            "Recovery Max Tokens",
            min_value=1,
            value=recovery_max_tokens,
            step=10,
            format="%d",
            key="cerberus_recovery_max_tokens",
        )
    )
    new_max_ledger_items = int(
        st.number_input(
            "Max Ledger Items",
            min_value=1,
            value=max_ledger_items,
            step=10,
            format="%d",
            key="cerberus_max_ledger_items",
        )
    )

    if new_max_rounds == 0 or new_max_tool_calls == 0:
        st.warning("Unlimited round/tool-call limits are enabled.")

    action_cols = st.columns(2)
    if action_cols[0].button("Save Cerberus Settings", key="save_cerberus_settings"):
        values = _cerberus_values_from_inputs()
        _apply_cerberus_settings(values)
        _sync_cerberus_widget_state(values)
        st.success("Cerberus settings updated.")
        st.rerun()
    if action_cols[1].button("Set Default Values", key="reset_cerberus_settings_defaults"):
        defaults = _cerberus_default_values()
        _apply_cerberus_settings(defaults)
        _sync_cerberus_widget_state(defaults)
        st.success("Cerberus settings reset to defaults.")
        st.rerun()


def _cerberus_ledger_keys_for_platform(platform: str) -> List[str]:
    plat = str(platform or "all").strip().lower()
    if plat == "all":
        keys = []
        if redis_client.exists("tater:cerberus:ledger"):
            keys.append("tater:cerberus:ledger")
        else:
            keys.extend(sorted(str(k) for k in redis_client.scan_iter(match="tater:cerberus:ledger:*")))
        return keys
    return [f"tater:cerberus:ledger:{normalize_platform(plat)}"]


def _load_cerberus_ledger_entries(platform: str, limit: int) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    keys = _cerberus_ledger_keys_for_platform(platform)
    max_limit = max(1, int(limit or 50))
    for key in keys:
        raw_items = redis_client.lrange(key, -max_limit, -1) or []
        for raw in raw_items:
            try:
                item = json.loads(raw)
            except Exception:
                continue
            if not isinstance(item, dict):
                continue
            item["_ledger_key"] = key
            entries.append(item)
    entries.sort(key=lambda x: float(x.get("timestamp") or 0.0), reverse=True)
    return entries[:max_limit]


def _normalize_cerberus_validation_for_view(
    validation: Any,
    *,
    planned_tool: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    raw = validation if isinstance(validation, dict) else {}
    status = str(raw.get("status") or "").strip().lower()
    if status in {"skipped", "ok", "failed"}:
        out = {
            "status": status,
            "repair_used": bool(raw.get("repair_used")),
            "reason": str(raw.get("reason") or ""),
        }
        try:
            out["attempts"] = int(raw.get("attempts"))
        except Exception:
            out["attempts"] = 0 if status == "skipped" else (2 if out["repair_used"] else 1)
        if raw.get("error") is not None:
            out["error"] = str(raw.get("error") or "")
        return out

    # Backcompat for old entries that only had validation.ok.
    if "ok" in raw:
        ok = bool(raw.get("ok"))
        reason = str(raw.get("reason") or "")
        repair_used = bool(raw.get("repair_used"))
        if not ok and reason == "no_tool":
            return {"status": "skipped", "repair_used": False, "reason": "no_tool", "attempts": 0}
        if ok:
            return {
                "status": "ok",
                "repair_used": repair_used,
                "reason": "repaired" if repair_used else (reason or "ok"),
                "attempts": 2 if repair_used else 1,
            }
        return {
            "status": "failed",
            "repair_used": repair_used,
            "reason": reason or "invalid_tool_call",
            "attempts": 2 if repair_used else 1,
        }

    # If no validation object exists, infer from presence of a planned tool.
    has_planned_tool = isinstance(planned_tool, dict) and bool(str(planned_tool.get("function") or "").strip())
    if not has_planned_tool:
        return {"status": "skipped", "repair_used": False, "reason": "no_tool", "attempts": 0}
    return {"status": "failed", "repair_used": False, "reason": "invalid_tool_call", "attempts": 1}


def _clear_cerberus_ledger(platform: str) -> int:
    keys = _cerberus_ledger_keys_for_platform(platform)
    deleted = 0
    for key in keys:
        try:
            deleted += int(redis_client.delete(key) or 0)
        except Exception:
            continue
    return deleted


_CERBERUS_METRIC_NAMES = (
    "total_turns",
    "total_tools_called",
    "total_repairs",
    "validation_failures",
    "tool_failures",
)
_CERBERUS_METRIC_PLATFORMS = (
    "webui",
    "discord",
    "irc",
    "telegram",
    "matrix",
    "homeassistant",
    "homekit",
    "xbmc",
    "automation",
)


def _coerce_redis_counter(value: Any) -> int:
    try:
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="ignore")
        return int(str(value).strip())
    except Exception:
        return 0


def _load_cerberus_metrics(platform: str) -> tuple[str, Dict[str, int], Dict[str, int]]:
    selected = str(platform or "").strip().lower()
    metric_platform = normalize_platform(selected if selected and selected != "all" else "webui")
    global_metrics: Dict[str, int] = {}
    platform_metrics: Dict[str, int] = {}
    for name in _CERBERUS_METRIC_NAMES:
        global_metrics[name] = _coerce_redis_counter(redis_client.get(f"tater:cerberus:metrics:{name}"))
        if selected == "all":
            platform_metrics[name] = global_metrics[name]
        else:
            platform_metrics[name] = _coerce_redis_counter(redis_client.get(f"tater:cerberus:metrics:{name}:{metric_platform}"))
    return metric_platform, global_metrics, platform_metrics


def _reset_cerberus_metrics(platform: str) -> int:
    plat = str(platform or "").strip().lower()
    keys: List[str] = []
    if plat == "all":
        try:
            keys = [str(k) for k in redis_client.scan_iter(match="tater:cerberus:metrics:*")]
        except Exception:
            keys = []
    else:
        metric_platform = normalize_platform(plat or "webui")
        for name in _CERBERUS_METRIC_NAMES:
            keys.append(f"tater:cerberus:metrics:{name}")
            keys.append(f"tater:cerberus:metrics:{name}:{metric_platform}")
    deleted = 0
    for key in keys:
        try:
            deleted += int(redis_client.delete(key) or 0)
        except Exception:
            continue
    return deleted


def _clear_cerberus_data(platform: str) -> tuple[int, int]:
    metrics_removed = _reset_cerberus_metrics(platform)
    ledger_removed = _clear_cerberus_ledger(platform)
    return metrics_removed, ledger_removed


def _safe_rate(numerator: int, denominator: int) -> float:
    denom = max(1, int(denominator or 0))
    return float(numerator or 0) / float(denom)


def _cerberus_rate_rows(metrics: Dict[str, int]) -> List[Dict[str, Any]]:
    turns = int(metrics.get("total_turns", 0) or 0)
    tools = int(metrics.get("total_tools_called", 0) or 0)
    repairs = int(metrics.get("total_repairs", 0) or 0)
    validation_failures = int(metrics.get("validation_failures", 0) or 0)
    tool_failures = int(metrics.get("tool_failures", 0) or 0)
    return [
        {"metric": "tool_call_rate", "value": round(_safe_rate(tools, turns), 4)},
        {"metric": "repair_rate", "value": round(_safe_rate(repairs, turns), 4)},
        {"metric": "validation_failure_rate", "value": round(_safe_rate(validation_failures, turns), 4)},
        {"metric": "tool_failure_rate", "value": round(_safe_rate(tool_failures, max(1, tools)), 4)},
    ]


def _load_cerberus_platform_metric_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for platform in _CERBERUS_METRIC_PLATFORMS:
        row: Dict[str, Any] = {"platform": platform}
        for name in _CERBERUS_METRIC_NAMES:
            row[name] = _coerce_redis_counter(redis_client.get(f"tater:cerberus:metrics:{name}:{platform}"))
        rows.append(row)
    return rows


def _cerberus_platform_display_label(platform: str) -> str:
    labels = {
        "all": "All",
        "webui": "WebUI",
        "homeassistant": "Home Assistant",
        "homekit": "HomeKit",
        "xbmc": "XBMC",
        "automation": "Automations",
    }
    normalized = str(platform or "").strip().lower()
    return labels.get(normalized, normalized.title())


def _render_cerberus_metrics_platform_view(
    *,
    selected_platform: str,
    limit: int,
    key_prefix: str,
    allow_controls: bool,
):
    metric_platform, global_metrics, platform_metrics = _load_cerberus_metrics(selected_platform)
    selected_token = str(selected_platform or "all").strip().lower() or "all"

    st.markdown(f"**Selected Portal Counters ({_cerberus_platform_display_label(metric_platform)})**")
    platform_cols = st.columns(len(_CERBERUS_METRIC_NAMES))
    for idx, name in enumerate(_CERBERUS_METRIC_NAMES):
        platform_cols[idx].metric(name.replace("_", " ").title(), platform_metrics.get(name, 0))
    st.markdown(f"**Selected Portal Rates ({_cerberus_platform_display_label(metric_platform)})**")
    st.dataframe(_cerberus_rate_rows(platform_metrics), width="stretch")

    if allow_controls:
        st.caption("Advanced controls")
        control_cols = st.columns(2)
        if control_cols[0].button("Reset Metrics", key=f"{key_prefix}_{selected_token}_reset_metrics"):
            removed = _reset_cerberus_metrics(selected_platform)
            st.success(f"Removed {removed} metric key(s).")
            st.rerun()
        if control_cols[1].button("Clear Ledger", key=f"{key_prefix}_{selected_token}_clear_ledger"):
            removed = _clear_cerberus_ledger(selected_platform)
            st.success(f"Deleted {removed} ledger list(s).")
            st.rerun()

    rows = _load_cerberus_ledger_entries(selected_platform, limit)
    if not rows:
        st.info("No Cerberus ledger entries found for this selection.")
        return

    outcome_filter = st.selectbox(
        "Outcome Filter",
        options=["all", "done", "blocked", "failed"],
        index=0,
        key=f"{key_prefix}_{selected_token}_outcome_filter",
    )
    show_only_tool_turns = st.checkbox(
        "Show Only Tool Turns",
        value=False,
        key=f"{key_prefix}_{selected_token}_tool_turns_only",
    )
    tool_options = ["all"] + sorted(
        {
            str((row.get("planned_tool") or {}).get("function") or "").strip()
            for row in rows
            if isinstance(row.get("planned_tool"), dict)
            and str((row.get("planned_tool") or {}).get("function") or "").strip()
        }
    )
    selected_tool = st.selectbox(
        "Tool Filter",
        options=tool_options,
        index=0,
        key=f"{key_prefix}_{selected_token}_tool_filter",
    )

    filtered_rows: List[Dict[str, Any]] = []
    for row in rows:
        planned_tool_obj = row.get("planned_tool") if isinstance(row.get("planned_tool"), dict) else {}
        planned_tool_name = str(planned_tool_obj.get("function") or "").strip()
        outcome = str(row.get("outcome") or "").strip().lower()
        if outcome_filter != "all" and outcome != outcome_filter:
            continue
        if show_only_tool_turns and not planned_tool_name:
            continue
        if selected_tool != "all" and planned_tool_name != selected_tool:
            continue
        filtered_rows.append(row)

    if not filtered_rows:
        st.info("No ledger rows matched the current filters.")
        return

    summary_rows = []
    for idx, item in enumerate(filtered_rows):
        ts = float(item.get("timestamp") or 0.0)
        planned_tool = item.get("planned_tool") if isinstance(item.get("planned_tool"), dict) else {}
        validation = _normalize_cerberus_validation_for_view(
            item.get("validation"),
            planned_tool=planned_tool if isinstance(planned_tool, dict) else None,
        )
        tool_result = item.get("tool_result") if isinstance(item.get("tool_result"), dict) else {}
        tool_summary = str(
            tool_result.get("summary")
            or item.get("tool_result_summary")
            or ""
        )
        summary_rows.append(
            {
                "#": idx + 1,
                "time": datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "",
                "platform": str(item.get("platform") or ""),
                "scope": str(item.get("scope") or ""),
                "planner_kind": str(item.get("planner_kind") or ""),
                "outcome": str(item.get("outcome") or ""),
                "outcome_reason": str(item.get("outcome_reason") or ""),
                "planned_tool": str(planned_tool.get("function") or ""),
                "validation_status": str(validation.get("status") or ""),
                "tool_result_ok": tool_result.get("ok") if isinstance(tool_result, dict) else item.get("tool_result_ok"),
                "tool_result_summary": tool_summary,
                "validation_reason": str(validation.get("reason") or ""),
                "checker_action": str(item.get("checker_action") or ""),
                "total_ms": int(item.get("total_ms") or 0),
            }
        )

    st.dataframe(summary_rows, width="stretch")

    tool_counts: Dict[str, int] = {}
    reason_counts: Dict[str, int] = {}
    for item in filtered_rows:
        planned_tool = item.get("planned_tool") if isinstance(item.get("planned_tool"), dict) else {}
        tool_name = str(planned_tool.get("function") or "").strip()
        if tool_name:
            tool_counts[tool_name] = int(tool_counts.get(tool_name, 0)) + 1

        validation_reason = str(item.get("validation_reason") or "").strip()
        if validation_reason:
            key = f"validation:{validation_reason}"
            reason_counts[key] = int(reason_counts.get(key, 0)) + 1
        checker_reason = str(item.get("checker_reason") or "").strip()
        if checker_reason:
            key = f"checker:{checker_reason}"
            reason_counts[key] = int(reason_counts.get(key, 0)) + 1

    top_tools_rows = [
        {"tool": name, "count": count}
        for name, count in sorted(tool_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    ]
    top_reasons_rows = [
        {"reason": name, "count": count}
        for name, count in sorted(reason_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:10]
    ]

    rollup_cols = st.columns(2)
    with rollup_cols[0]:
        st.markdown("**Top Tools (Last N Filtered)**")
        if top_tools_rows:
            st.dataframe(top_tools_rows, width="stretch")
        else:
            st.caption("No tool calls in current filtered set.")
    with rollup_cols[1]:
        st.markdown("**Top Failure Reasons (Last N Filtered)**")
        if top_reasons_rows:
            st.dataframe(top_reasons_rows, width="stretch")
        else:
            st.caption("No failure reasons in current filtered set.")

    for idx, item in enumerate(filtered_rows):
        ts = float(item.get("timestamp") or 0.0)
        ts_text = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else "unknown time"
        platform_text = str(item.get("platform") or "unknown")
        outcome_text = str(item.get("outcome") or "unknown")
        with st.expander(f"Details #{idx + 1} - {ts_text} - {platform_text} - {outcome_text}", expanded=False):
            st.code(json.dumps(item, indent=2, ensure_ascii=False), language="json")


def render_cerberus_metrics_dashboard(*, key_prefix: str, allow_controls: bool):
    st.subheader("Cerberus Metrics")
    st.caption("Planner/Doer/Critic counters and recent ledger rows.")

    limit = int(
        st.slider(
            "Ledger entries",
            min_value=10,
            max_value=300,
            value=50,
            step=10,
            key=f"{key_prefix}_ledger_limit",
        )
    )

    _, global_metrics, _ = _load_cerberus_metrics("all")

    st.markdown("**Global Counters**")
    global_cols = st.columns(len(_CERBERUS_METRIC_NAMES))
    for idx, name in enumerate(_CERBERUS_METRIC_NAMES):
        global_cols[idx].metric(name.replace("_", " ").title(), global_metrics.get(name, 0))

    st.markdown("**Global Rates**")
    st.dataframe(_cerberus_rate_rows(global_metrics), width="stretch")

    st.markdown("**Per-Portal Totals**")
    st.dataframe(_load_cerberus_platform_metric_rows(), width="stretch")

    platforms = list(_CERBERUS_METRIC_PLATFORMS)
    tab_labels = [_cerberus_platform_display_label(platform) for platform in platforms]
    tab_views = st.tabs(tab_labels)

    for idx, tab in enumerate(tab_views):
        with tab:
            _render_cerberus_metrics_platform_view(
                selected_platform=platforms[idx],
                limit=limit,
                key_prefix=key_prefix,
                allow_controls=allow_controls,
            )


def render_cerberus_data_tools(*, key_prefix: str):
    st.subheader("Cerberus Data")
    st.caption("Clear Cerberus metrics and ledger data globally or for a specific portal.")

    with st.container(border=True):
        st.markdown("**All Portals**")
        st.caption("Delete all Cerberus metrics and all Cerberus ledger data.")
        if st.button("Clear All Cerberus Data", key=f"{key_prefix}_clear_all_data"):
            metrics_removed, ledger_removed = _clear_cerberus_data("all")
            st.success(
                f"Cleared Cerberus data across all portals. "
                f"Metrics removed: {metrics_removed}. Ledger lists removed: {ledger_removed}."
            )
            st.rerun()

    with st.container(border=True):
        st.markdown("**Per-Portal Data**")
        platform_options = list(_CERBERUS_METRIC_PLATFORMS)
        selected_platform = st.selectbox(
            "Portal",
            options=platform_options,
            index=0,
            format_func=_cerberus_platform_display_label,
            key=f"{key_prefix}_platform",
        )

        clear_cols = st.columns(3)
        if clear_cols[0].button("Clear Portal Data", key=f"{key_prefix}_clear_platform_data"):
            metrics_removed, ledger_removed = _clear_cerberus_data(selected_platform)
            st.success(
                f"Cleared Cerberus data for {_cerberus_platform_display_label(selected_platform)}. "
                f"Metrics removed: {metrics_removed}. Ledger lists removed: {ledger_removed}."
            )
            st.rerun()

        if clear_cols[1].button("Reset Metrics Only", key=f"{key_prefix}_reset_platform_metrics"):
            removed = _reset_cerberus_metrics(selected_platform)
            st.success(
                f"Removed {removed} Cerberus metric key(s) for {_cerberus_platform_display_label(selected_platform)}."
            )
            st.rerun()

        if clear_cols[2].button("Clear Ledger Only", key=f"{key_prefix}_clear_platform_ledger"):
            removed = _clear_cerberus_ledger(selected_platform)
            st.success(
                f"Deleted {removed} Cerberus ledger list(s) for {_cerberus_platform_display_label(selected_platform)}."
            )
            st.rerun()
