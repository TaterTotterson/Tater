import json
import os
import time
import textwrap
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import redis
import streamlit as st

from memory_platform_store import (
    forget_fact_keys as forget_memory_platform_fact_keys,
    load_doc as load_memory_platform_doc,
    room_doc_key as memory_platform_room_doc_key,
    save_doc as save_memory_platform_doc,
    summarize_doc as summarize_memory_platform_doc,
    user_doc_key as memory_platform_user_doc_key,
    value_to_text as memory_platform_value_to_text,
)


redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', '127.0.0.1'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True,
)


def _format_unix_ts(raw: Any) -> str:
    try:
        ts = float(raw)
    except (TypeError, ValueError):
        return ""
    if ts <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


_MEMORY_USER_PROFILE_CATEGORIES: List[Dict[str, Any]] = [
    {
        "label": "Demographic Information",
        "keys": ["age", "gender", "ethnicity", "occupation", "education"],
    },
    {
        "label": "Physical Traits",
        "keys": ["height", "weight", "hair_color", "eye_color", "distinguishing_features"],
    },
    {
        "label": "Personality Traits",
        "keys": ["temperament", "values_and_beliefs", "interests", "strengths_and_weaknesses"],
    },
    {
        "label": "Behavioral Patterns",
        "keys": ["communication_style", "conflict_resolution", "social_interactions"],
    },
    {
        "label": "Lifestyle Choices",
        "keys": ["daily_routine", "health_habits", "recreational_activities"],
    },
    {
        "label": "Social Media Presence",
        "keys": ["platforms_used", "content_shared", "network"],
    },
    {
        "label": "Life Experiences",
        "keys": ["major_life_events", "achievements", "challenges"],
    },
    {
        "label": "Family Background",
        "keys": ["family_dynamics", "cultural_influences", "traditions"],
    },
    {
        "label": "Mental and Emotional Health",
        "keys": ["mental_well_being", "coping_mechanisms", "resilience"],
    },
    {
        "label": "Goals and Aspirations",
        "keys": ["short_term_goals", "long_term_aspirations"],
    },
]
_MEMORY_USER_FACT_TO_CATEGORY: Dict[str, str] = {}
for _category in _MEMORY_USER_PROFILE_CATEGORIES:
    _label = str(_category.get("label") or "").strip()
    for _key in list(_category.get("keys") or []):
        _name = str(_key or "").strip()
        if _label and _name:
            _MEMORY_USER_FACT_TO_CATEGORY[_name] = _label

_MEMORY_ROOM_PROFILE_CATEGORIES: List[Dict[str, Any]] = [
    {
        "label": "Room Identity",
        "keys": ["room_purpose", "shared_topics", "shared_projects", "inside_jokes"],
    },
    {
        "label": "Communication Norms",
        "keys": ["communication_tone", "response_style", "conflict_norms"],
    },
    {
        "label": "Shared Defaults",
        "keys": ["default_units", "default_timezone", "default_language", "decision_preferences"],
    },
    {
        "label": "Collaboration Context",
        "keys": ["active_goals", "recurring_tasks", "shared_tools", "reference_links"],
    },
    {
        "label": "Constraints and Policies",
        "keys": ["shared_constraints", "moderation_preferences", "privacy_expectations"],
    },
]
_MEMORY_ROOM_FACT_TO_CATEGORY: Dict[str, str] = {}
for _category in _MEMORY_ROOM_PROFILE_CATEGORIES:
    _label = str(_category.get("label") or "").strip()
    for _key in list(_category.get("keys") or []):
        _name = str(_key or "").strip()
        if _label and _name:
            _MEMORY_ROOM_FACT_TO_CATEGORY[_name] = _label

_MEMORY_ROOM_LABEL_PREFIX = "tater:room_label"
_TELEGRAM_CHAT_LOOKUP_HASH = "tater:telegram:chat_lookup"
_LEGACY_MEMORY_HASH_PREFIX = "tater:memory"
_LEGACY_MEMORY_GLOBAL_KEY = f"{_LEGACY_MEMORY_HASH_PREFIX}:global"
_LEGACY_MEMORY_USER_PREFIX = f"{_LEGACY_MEMORY_HASH_PREFIX}:user:"
_LEGACY_MEMORY_ROOM_PREFIX = f"{_LEGACY_MEMORY_HASH_PREFIX}:room:"
_LEGACY_MEMORY_DEFAULT_TTL_KEY = "tater:memory:default_ttl_sec"


def _memory_platform_room_label_key(platform: Any, room_id: Any) -> str:
    platform_name = str(platform or "").strip().lower() or "unknown"
    scope_id = str(room_id or "").strip()
    if not scope_id:
        return ""
    return f"{_MEMORY_ROOM_LABEL_PREFIX}:{platform_name}:{scope_id}"


def _memory_platform_room_name(platform: Any, room_id: Any) -> str:
    platform_name = str(platform or "").strip().lower() or "unknown"
    scope_id = str(room_id or "").strip()
    if not scope_id:
        return "unknown"

    key = _memory_platform_room_label_key(platform_name, scope_id)
    if key:
        try:
            raw = redis_client.get(key)
        except Exception:
            raw = None
        label = str(raw or "").strip()
        if label:
            return label

    if platform_name == "telegram":
        try:
            candidates: List[str] = []
            for raw_name, raw_chat_id in redis_client.hscan_iter(_TELEGRAM_CHAT_LOOKUP_HASH, count=500):
                chat_id = str(raw_chat_id or "").strip()
                if chat_id != scope_id:
                    continue
                name = str(raw_name or "").strip()
                if not name:
                    continue
                if name.lstrip("-").isdigit():
                    continue
                candidates.append(name)
            if candidates:
                candidates.sort(
                    key=lambda value: (
                        value.lower().startswith("telegram_user"),
                        value.lower().startswith("unknown"),
                        len(value),
                        value.lower(),
                    )
                )
                return candidates[0]
        except Exception:
            pass

    return scope_id


def _memory_platform_room_display_name_from_row(row: Dict[str, Any]) -> str:
    room_id = str((row or {}).get("room_id") or "").strip() or "unknown"
    room_name = str((row or {}).get("room_name") or "").strip() or room_id
    if room_name == room_id:
        return room_name
    return f"{room_name} [{room_id}]"


def _memory_platform_stats() -> Dict[str, Any]:
    raw = redis_client.hgetall("mem:stats:memory_platform") or {}
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _to_int(value: Any, default: int = 0) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    stats = {
        "last_run_ts": _to_float(raw.get("last_run_ts"), 0.0),
        "enabled_platform_count": _to_int(raw.get("enabled_platform_count"), 0),
        "scanned_scopes": _to_int(raw.get("scanned_scopes"), 0),
        "processed_messages": _to_int(raw.get("processed_messages"), 0),
        "updated_facts": _to_int(raw.get("updated_facts"), 0),
        "updated_docs": _to_int(raw.get("updated_docs"), 0),
    }
    stats["last_run_text"] = _format_unix_ts(stats.get("last_run_ts"))
    return stats


def _memory_platform_doc_discovery() -> Dict[str, Any]:
    user_rows: List[Dict[str, Any]] = []
    room_rows: List[Dict[str, Any]] = []

    try:
        for raw_key in redis_client.scan_iter(match="mem:user:*", count=200):
            key = str(raw_key or "").strip()
            if not key:
                continue
            payload = key.split("mem:user:", 1)[-1]
            platform_name, sep, user_id = payload.partition(":")
            if not sep:
                continue
            platform_name = (platform_name or "webui").strip()
            user_id = user_id.strip()
            if not user_id:
                continue
            doc = load_memory_platform_doc(redis_client, key)
            facts = doc.get("facts") if isinstance(doc.get("facts"), dict) else {}
            if not facts:
                continue
            items = summarize_memory_platform_doc(doc, max_items=4, min_confidence=0.0)
            fact_keys = sorted(list(facts.keys()))
            fact_keys_preview = ", ".join(fact_keys[:8])
            if len(fact_keys) > 8:
                fact_keys_preview = f"{fact_keys_preview}, +{len(fact_keys) - 8} more"
            preview = "; ".join(
                [
                    f"{str(item.get('key') or '')}={memory_platform_value_to_text(item.get('value'), max_chars=40)}"
                    for item in items
                ]
            )
            user_rows.append(
                {
                    "platform": platform_name,
                    "user_id": user_id,
                    "fact_count": len(facts),
                    "fact_keys": fact_keys_preview,
                    "last_updated": _format_unix_ts(doc.get("last_updated")),
                    "preview": preview,
                    "doc": doc,
                }
            )
    except Exception:
        pass

    try:
        for raw_key in redis_client.scan_iter(match="mem:room:*", count=200):
            key = str(raw_key or "").strip()
            if not key:
                continue
            payload = key.split("mem:room:", 1)[-1]
            platform_name, sep, room_id = payload.partition(":")
            if not sep:
                continue
            platform_name = (platform_name or "webui").strip()
            room_id = room_id.strip()
            if not room_id:
                continue
            doc = load_memory_platform_doc(redis_client, key)
            facts = doc.get("facts") if isinstance(doc.get("facts"), dict) else {}
            if not facts:
                continue
            items = summarize_memory_platform_doc(doc, max_items=4, min_confidence=0.0)
            fact_keys = sorted(list(facts.keys()))
            fact_keys_preview = ", ".join(fact_keys[:8])
            if len(fact_keys) > 8:
                fact_keys_preview = f"{fact_keys_preview}, +{len(fact_keys) - 8} more"
            preview = "; ".join(
                [
                    f"{str(item.get('key') or '')}={memory_platform_value_to_text(item.get('value'), max_chars=40)}"
                    for item in items
                ]
            )
            room_rows.append(
                {
                    "platform": platform_name,
                    "room_id": room_id,
                    "room_name": _memory_platform_room_name(platform_name, room_id),
                    "fact_count": len(facts),
                    "fact_keys": fact_keys_preview,
                    "last_updated": _format_unix_ts(doc.get("last_updated")),
                    "preview": preview,
                    "doc": doc,
                }
            )
    except Exception:
        pass

    user_rows = sorted(
        user_rows,
        key=lambda row: (
            str(row.get("platform") or ""),
            str(row.get("user_id") or ""),
        ),
    )
    room_rows = sorted(
        room_rows,
        key=lambda row: (
            str(row.get("platform") or ""),
            str(row.get("room_name") or row.get("room_id") or ""),
            str(row.get("room_id") or ""),
        ),
    )
    return {
        "users": user_rows,
        "rooms": room_rows,
        "user_count": len(user_rows),
        "room_count": len(room_rows),
        "fact_count": sum(int(row.get("fact_count") or 0) for row in user_rows + room_rows),
    }


def _memory_platform_fact_rows(
    doc: Dict[str, Any],
    *,
    max_items: int = 500,
    value_max_chars: int = 120,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(doc, dict):
        return rows
    cap = max(1, min(int(max_items), 50_000))
    items = summarize_memory_platform_doc(doc, max_items=cap, min_confidence=0.0)
    for item in items:
        if not isinstance(item, dict):
            continue
        evidence = item.get("evidence") if isinstance(item.get("evidence"), list) else []
        rows.append(
            {
                "key": str(item.get("key") or ""),
                "value": memory_platform_value_to_text(item.get("value"), max_chars=max(24, int(value_max_chars))),
                "confidence": f"{float(item.get('confidence') or 0.0):.2f}",
                "evidence_count": len(evidence),
                "updated_at": _format_unix_ts(item.get("updated_at")),
            }
        )
    return rows


def _memory_platform_export_lines(
    *,
    stats: Dict[str, Any],
    user_rows: List[Dict[str, Any]],
    room_rows: List[Dict[str, Any]],
    insights: Optional[Dict[str, Any]] = None,
) -> List[str]:
    lines: List[str] = []
    insights = insights if isinstance(insights, dict) else _memory_platform_insight_frames(user_rows, room_rows)
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append("Tater Memory Report")
    lines.append(f"Generated: {now_text}")
    lines.append("")
    lines.append("Summary")
    lines.append(f"Users with memory: {int(len(user_rows))}")
    lines.append(f"Rooms with memory: {int(len(room_rows))}")
    lines.append(f"Total facts: {int(sum(int(row.get('fact_count') or 0) for row in user_rows + room_rows))}")
    lines.append(f"Processed messages (last run): {int(stats.get('processed_messages') or 0)}")
    lines.append(f"Updated facts (last run): {int(stats.get('updated_facts') or 0)}")
    lines.append(f"Updated docs (last run): {int(stats.get('updated_docs') or 0)}")
    lines.append(f"Scanned scopes (last run): {int(stats.get('scanned_scopes') or 0)}")
    lines.append(f"Enabled platforms (last run): {int(stats.get('enabled_platform_count') or 0)}")
    lines.append(f"Last run: {str(stats.get('last_run_text') or '').strip() or 'n/a'}")
    lines.append("")

    trending_user_df = insights.get("trending_user_df") if isinstance(insights, dict) else None
    trending_has_data = bool((insights or {}).get("trending_user_has_data"))
    lines.append("Trending Shared User Facts")
    if isinstance(trending_user_df, pd.DataFrame) and not trending_user_df.empty:
        table_df = trending_user_df.reset_index()
        if "trend" not in table_df.columns and "index" in table_df.columns:
            table_df = table_df.rename(columns={"index": "trend"})
        for _, trend_row in table_df.iterrows():
            trend_label = str(trend_row.get("trend") or "").strip() or "No trending yet"
            user_count = int(trend_row.get("users") or 0)
            lines.append(f"- {trend_label} ({user_count} users)")
        if not trending_has_data:
            lines.append("  No trending shared user facts yet.")
    else:
        lines.append("- No trending yet (0 users)")
        lines.append("  No trending shared user facts yet.")
    lines.append("")

    lines.append("User Memory")
    if not user_rows:
        lines.append("(none)")
        lines.append("")
    else:
        for row in user_rows:
            platform_name = str(row.get("platform") or "").strip() or "unknown"
            user_id = str(row.get("user_id") or "").strip() or "unknown"
            fact_count = int(row.get("fact_count") or 0)
            updated_text = str(row.get("last_updated") or "").strip() or "n/a"
            lines.append(f"- {platform_name} / {user_id} | facts={fact_count} | updated={updated_text}")
            doc = row.get("doc") if isinstance(row.get("doc"), dict) else {}
            facts = _memory_platform_fact_rows(doc, max_items=10_000)
            if not facts:
                lines.append("  (no facts)")
            else:
                for fact in facts:
                    lines.append(
                        "  * "
                        + f"{str(fact.get('key') or '')}: {str(fact.get('value') or '')} "
                        + f"(conf={str(fact.get('confidence') or '0.00')}, "
                        + f"evidence={int(fact.get('evidence_count') or 0)}, "
                        + f"updated={str(fact.get('updated_at') or '') or 'n/a'})"
                    )
            lines.append("")

    lines.append("Room Memory")
    if not room_rows:
        lines.append("(none)")
        lines.append("")
    else:
        for row in room_rows:
            platform_name = str(row.get("platform") or "").strip() or "unknown"
            room_id = str(row.get("room_id") or "").strip() or "unknown"
            room_name = str(row.get("room_name") or "").strip() or room_id
            room_display = room_name if room_name == room_id else f"{room_name} [{room_id}]"
            fact_count = int(row.get("fact_count") or 0)
            updated_text = str(row.get("last_updated") or "").strip() or "n/a"
            lines.append(f"- {platform_name} / {room_display} | facts={fact_count} | updated={updated_text}")
            doc = row.get("doc") if isinstance(row.get("doc"), dict) else {}
            facts = _memory_platform_fact_rows(doc, max_items=10_000)
            if not facts:
                lines.append("  (no facts)")
            else:
                for fact in facts:
                    lines.append(
                        "  * "
                        + f"{str(fact.get('key') or '')}: {str(fact.get('value') or '')} "
                        + f"(conf={str(fact.get('confidence') or '0.00')}, "
                        + f"evidence={int(fact.get('evidence_count') or 0)}, "
                        + f"updated={str(fact.get('updated_at') or '') or 'n/a'})"
                    )
            lines.append("")

    return lines


def _memory_platform_export_styled_rows(
    *,
    stats: Dict[str, Any],
    user_rows: List[Dict[str, Any]],
    room_rows: List[Dict[str, Any]],
    insights: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    insights = insights if isinstance(insights, dict) else _memory_platform_insight_frames(user_rows, room_rows)

    def _add(style: str, text: Any = "", **extra: Any) -> None:
        row: Dict[str, Any] = {"style": str(style or "body"), "text": text}
        if extra:
            row.update(extra)
        rows.append(row)

    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _add("title", "Tater Memory Report")
    _add("subtitle", f"Generated: {now_text}")
    _add("spacer", "")

    _add("section", "Summary")
    _add("body", f"Users with memory: {int(len(user_rows))}")
    _add("body", f"Rooms with memory: {int(len(room_rows))}")
    _add("body", f"Total facts: {int(sum(int(row.get('fact_count') or 0) for row in user_rows + room_rows))}")
    _add("body", f"Processed messages (last run): {int(stats.get('processed_messages') or 0)}")
    _add("body", f"Updated facts (last run): {int(stats.get('updated_facts') or 0)}")
    _add("body", f"Updated docs (last run): {int(stats.get('updated_docs') or 0)}")
    _add("body", f"Scanned scopes (last run): {int(stats.get('scanned_scopes') or 0)}")
    _add("body", f"Enabled platforms (last run): {int(stats.get('enabled_platform_count') or 0)}")
    _add("body", f"Last run: {str(stats.get('last_run_text') or '').strip() or 'n/a'}")
    _add("spacer", "")

    trending_user_df = insights.get("trending_user_df") if isinstance(insights, dict) else None
    trending_has_data = bool((insights or {}).get("trending_user_has_data"))
    _add("section", "Trending Shared User Facts")
    if isinstance(trending_user_df, pd.DataFrame) and not trending_user_df.empty:
        table_df = trending_user_df.reset_index()
        if "trend" not in table_df.columns and "index" in table_df.columns:
            table_df = table_df.rename(columns={"index": "trend"})
        _add("table_header", cells=["Shared Fact", "Users"])
        for _, trend_row in table_df.iterrows():
            trend_label = str(trend_row.get("trend") or "").strip() or "No trending yet"
            user_count = str(int(trend_row.get("users") or 0))
            _add("table_row", cells=[trend_label, user_count])
        if not trending_has_data:
            _add("meta", "No trending shared user facts yet.")
    else:
        _add("table_header", cells=["Shared Fact", "Users"])
        _add("table_row", cells=["No trending yet", "0"])
        _add("meta", "No trending shared user facts yet.")
    _add("spacer", "")

    def _render_scope(scope_title: str, source_rows: List[Dict[str, Any]], id_key: str) -> None:
        _add("section", scope_title)
        if not source_rows:
            _add("meta", "(none)")
            _add("spacer", "")
            return
        for row in source_rows:
            platform_name = str(row.get("platform") or "").strip() or "unknown"
            scope_id = str(row.get(id_key) or "").strip() or "unknown"
            scope_display = scope_id
            if id_key == "room_id":
                room_name = str(row.get("room_name") or "").strip()
                if room_name:
                    scope_display = room_name if room_name == scope_id else f"{room_name} [{scope_id}]"
            fact_count = int(row.get("fact_count") or 0)
            updated_text = str(row.get("last_updated") or "").strip() or "n/a"
            _add("entity", f"{platform_name} / {scope_display}")
            _add("meta", f"Facts: {fact_count}   Last Updated: {updated_text}")
            doc = row.get("doc") if isinstance(row.get("doc"), dict) else {}
            facts = _memory_platform_fact_rows(doc, max_items=10_000, value_max_chars=320)
            if not facts:
                _add("meta", "(no facts)")
                _add("spacer", "")
                continue
            _add(
                "table_header",
                cells=["Key", "Value", "Conf", "Ev", "Updated"],
            )
            for fact in facts:
                key = str(fact.get("key") or "").strip()
                value = str(fact.get("value") or "").strip()
                conf = str(fact.get("confidence") or "0.00").strip()
                evidence = str(int(fact.get("evidence_count") or 0))
                updated = str(fact.get("updated_at") or "").strip() or "n/a"
                _add(
                    "table_row",
                    cells=[key, value, conf, evidence, updated],
                )
            _add("spacer", "")

    _render_scope("User Memory", user_rows, "user_id")
    _render_scope("Room Memory", room_rows, "room_id")
    return rows


def _pdf_escape(text: Any) -> str:
    out = str(text or "")
    out = out.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    out = out.replace("\r", " ").replace("\n", " ")
    out = out.encode("latin-1", errors="replace").decode("latin-1", errors="replace")
    return out


def _build_styled_pdf_from_rows(rows: List[Dict[str, Any]], *, title: str = "Memory Report") -> bytes:
    items = list(rows or [])
    if not items:
        items = [{"style": "title", "text": title}]

    page_width = 612.0
    page_height = 792.0
    margin_left = 44.0
    margin_right = 44.0
    top_y = 760.0
    bottom_y = 48.0

    style_map: Dict[str, Dict[str, Any]] = {
        "title": {"font": "F2", "size": 18.0, "leading": 24.0, "indent": 0.0, "wrap": 68},
        "subtitle": {"font": "F1", "size": 10.0, "leading": 14.0, "indent": 0.0, "wrap": 95},
        "section": {"font": "F2", "size": 13.0, "leading": 19.0, "indent": 0.0, "wrap": 82, "pre": 6.0},
        "entity": {"font": "F2", "size": 11.0, "leading": 15.0, "indent": 0.0, "wrap": 86, "pre": 2.0},
        "meta": {"font": "F1", "size": 9.0, "leading": 13.0, "indent": 0.0, "wrap": 98},
        "body": {"font": "F1", "size": 10.0, "leading": 14.0, "indent": 0.0, "wrap": 94},
        "table_header": {"font": "F2", "size": 8.5, "leading": 10.5, "indent": 0.0, "wrap": 0},
        "table_row": {"font": "F1", "size": 8.5, "leading": 10.5, "indent": 0.0, "wrap": 0},
        "spacer": {"font": "F1", "size": 9.0, "leading": 9.0, "indent": 0.0, "wrap": 0},
    }

    page_streams: List[List[str]] = [[]]
    page_y: List[float] = [top_y]
    table_total_width = page_width - margin_left - margin_right
    table_cell_pad_x = 3.5
    table_cell_pad_y = 3.0
    last_table_header_cells: Optional[List[str]] = None

    def _table_layout(cells: List[str]) -> tuple[List[float], List[float]]:
        cell_count = max(1, len(cells))
        if cell_count == 5:
            weights = [0.22, 0.44, 0.09, 0.07, 0.18]
        elif cell_count == 2:
            weights = [0.78, 0.22]
        else:
            weights = [1.0 / float(cell_count)] * cell_count

        widths: List[float] = [table_total_width * weight for weight in weights]
        x_positions: List[float] = [margin_left]
        for width in widths:
            x_positions.append(x_positions[-1] + width)
        return widths, x_positions

    def _new_page() -> int:
        page_streams.append([])
        page_y.append(top_y)
        return len(page_streams) - 1

    current_page = 0

    def _ensure_space(required_height: float) -> None:
        nonlocal current_page
        if page_y[current_page] - required_height < bottom_y:
            current_page = _new_page()

    def _draw_text(text: str, *, font: str, size: float, x: float) -> None:
        line = str(text or "")
        y = page_y[current_page]
        page_streams[current_page].append(
            f"BT /{font} {size:.2f} Tf {x:.2f} {y:.2f} Td ({_pdf_escape(line)}) Tj ET"
        )

    def _draw_text_at(text: str, *, font: str, size: float, x: float, y: float) -> None:
        line = str(text or "")
        page_streams[current_page].append(
            f"BT /{font} {size:.2f} Tf {x:.2f} {y:.2f} Td ({_pdf_escape(line)}) Tj ET"
        )

    def _normalize_table_cells(raw_cells: Any) -> List[str]:
        cells: List[str] = []
        if isinstance(raw_cells, list):
            cells = [str(item or "") for item in raw_cells]
        elif raw_cells is None:
            cells = []
        else:
            text = str(raw_cells or "")
            if "|" in text:
                cells = [part.strip() for part in text.split("|")]
            else:
                cells = [text]
        return cells or [""]

    def _wrap_table_cell(text: str, *, width: float, font_size: float) -> List[str]:
        raw = str(text or "").strip()
        if not raw:
            return [""]
        # Approximate characters-per-line for Helvetica at this font size.
        usable_width = max(10.0, float(width) - (table_cell_pad_x * 2.0))
        approx_char_width = max(3.8, font_size * 0.52)
        wrap_chars = max(6, int(usable_width / approx_char_width))
        lines = textwrap.wrap(
            raw,
            width=wrap_chars,
            break_long_words=True,
            replace_whitespace=False,
            drop_whitespace=False,
        )
        return lines or [raw]

    def _draw_table_row(cells: List[str], *, is_header: bool) -> None:
        nonlocal current_page, last_table_header_cells

        style = style_map["table_header" if is_header else "table_row"]
        font = str(style.get("font") or "F1")
        size = float(style.get("size") or 8.5)
        leading = float(style.get("leading") or (size + 2.0))
        table_col_widths, table_col_x = _table_layout(cells)

        wrapped_cells: List[List[str]] = []
        max_lines = 1
        for idx, cell in enumerate(cells):
            wrapped = _wrap_table_cell(str(cell or ""), width=table_col_widths[idx], font_size=size)
            wrapped_cells.append(wrapped)
            max_lines = max(max_lines, len(wrapped))

        row_height = (max_lines * leading) + (table_cell_pad_y * 2.0)
        if page_y[current_page] - row_height < bottom_y:
            current_page = _new_page()
            if (not is_header) and last_table_header_cells:
                _draw_table_row(list(last_table_header_cells), is_header=True)

        if is_header:
            last_table_header_cells = list(cells)

        top = page_y[current_page]
        bottom = top - row_height

        if is_header:
            page_streams[current_page].append("0.93 g")
            page_streams[current_page].append(
                f"{margin_left:.2f} {bottom:.2f} {table_total_width:.2f} {row_height:.2f} re f"
            )
            page_streams[current_page].append("0.00 g")

        page_streams[current_page].append("0.72 G")
        page_streams[current_page].append("0.60 w")
        page_streams[current_page].append(f"{margin_left:.2f} {top:.2f} m {margin_left + table_total_width:.2f} {top:.2f} l S")
        page_streams[current_page].append(f"{margin_left:.2f} {bottom:.2f} m {margin_left + table_total_width:.2f} {bottom:.2f} l S")
        for x in table_col_x:
            page_streams[current_page].append(f"{x:.2f} {top:.2f} m {x:.2f} {bottom:.2f} l S")
        page_streams[current_page].append("0.00 G")
        page_streams[current_page].append("1.00 w")

        text_top_y = top - table_cell_pad_y - size + 1.0
        for col_idx, lines in enumerate(wrapped_cells):
            text_x = table_col_x[col_idx] + table_cell_pad_x
            for line_idx, line in enumerate(lines):
                line_y = text_top_y - (line_idx * leading)
                _draw_text_at(line, font=font, size=size, x=text_x, y=line_y)

        page_y[current_page] = bottom

    for item in items:
        style_name = str(item.get("style") or "body").strip().lower()
        if style_name in ("table_header", "table_row"):
            table_cells = _normalize_table_cells(item.get("cells", item.get("text")))
            _draw_table_row(table_cells, is_header=(style_name == "table_header"))
            continue

        style = style_map.get(style_name, style_map["body"])
        text = str(item.get("text") or "")
        pre = float(style.get("pre") or 0.0)
        leading = float(style.get("leading") or 12.0)
        wrap_width = int(style.get("wrap") or 0)
        font = str(style.get("font") or "F1")
        size = float(style.get("size") or 10.0)
        indent = float(style.get("indent") or 0.0)
        x = margin_left + indent

        if pre > 0:
            _ensure_space(pre + leading)
            page_y[current_page] -= pre

        if style_name == "spacer":
            _ensure_space(leading)
            page_y[current_page] -= leading
            continue

        if not text.strip():
            _ensure_space(leading)
            page_y[current_page] -= leading
            continue

        lines = (
            textwrap.wrap(
                text,
                width=max(8, wrap_width),
                break_long_words=True,
                replace_whitespace=False,
                drop_whitespace=False,
            )
            if wrap_width > 0
            else [text]
        )
        lines = lines or [text]

        for line in lines:
            _ensure_space(leading)
            _draw_text(line, font=font, size=size, x=x)
            page_y[current_page] -= leading

    total_pages = len(page_streams)
    for idx in range(total_pages):
        page_no = idx + 1
        footer = f"Page {page_no} of {total_pages}"
        footer_x = page_width - margin_right - 88.0
        footer_y = 28.0
        page_streams[idx].append("0.80 G")
        page_streams[idx].append(f"{margin_left:.2f} 36.00 m {page_width - margin_right:.2f} 36.00 l S")
        page_streams[idx].append("0.00 G")
        page_streams[idx].append(
            f"BT /F1 8.00 Tf {footer_x:.2f} {footer_y:.2f} Td ({_pdf_escape(footer)}) Tj ET"
        )

    catalog_id = 1
    pages_id = 2
    font_regular_id = 3
    font_bold_id = 4
    first_page_obj_id = 5

    objects: Dict[int, bytes] = {
        catalog_id: b"<< /Type /Catalog /Pages 2 0 R >>",
        pages_id: b"",
        font_regular_id: b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        font_bold_id: b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>",
    }

    page_obj_ids: List[int] = []
    content_obj_ids: List[int] = []
    next_obj_id = first_page_obj_id
    for _ in range(total_pages):
        page_obj_ids.append(next_obj_id)
        content_obj_ids.append(next_obj_id + 1)
        next_obj_id += 2

    kids_blob = " ".join(f"{obj_id} 0 R" for obj_id in page_obj_ids)
    objects[pages_id] = (
        f"<< /Type /Pages /Count {total_pages} /Kids [{kids_blob}] >>"
    ).encode("latin-1")

    for idx, commands in enumerate(page_streams):
        page_obj_id = page_obj_ids[idx]
        content_obj_id = content_obj_ids[idx]
        stream = "\n".join(commands).encode("latin-1", errors="replace")
        objects[content_obj_id] = (
            f"<< /Length {len(stream)} >>\nstream\n".encode("latin-1")
            + stream
            + b"\nendstream"
        )
        objects[page_obj_id] = (
            f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 {int(page_width)} {int(page_height)}] "
            f"/Resources << /Font << /F1 {font_regular_id} 0 R /F2 {font_bold_id} 0 R >> >> "
            f"/Contents {content_obj_id} 0 R >>"
        ).encode("latin-1")

    total_objects = next_obj_id - 1
    pdf = bytearray()
    pdf.extend(b"%PDF-1.4\n")
    pdf.extend(b"%\xe2\xe3\xcf\xd3\n")

    offsets: List[int] = [0] * (total_objects + 1)
    for obj_id in range(1, total_objects + 1):
        offsets[obj_id] = len(pdf)
        pdf.extend(f"{obj_id} 0 obj\n".encode("latin-1"))
        payload = objects.get(obj_id, b"<<>>")
        pdf.extend(payload)
        if not payload.endswith(b"\n"):
            pdf.extend(b"\n")
        pdf.extend(b"endobj\n")

    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {total_objects + 1}\n".encode("latin-1"))
    pdf.extend(b"0000000000 65535 f \n")
    for obj_id in range(1, total_objects + 1):
        pdf.extend(f"{offsets[obj_id]:010d} 00000 n \n".encode("latin-1"))
    pdf.extend(
        (
            f"trailer\n<< /Size {total_objects + 1} /Root {catalog_id} 0 R >>\n"
            f"startxref\n{xref_offset}\n%%EOF\n"
        ).encode("latin-1")
    )
    return bytes(pdf)


def _build_simple_pdf_from_lines(lines: List[str], *, title: str = "Memory Report") -> bytes:
    raw_lines = list(lines or [])
    if not raw_lines:
        raw_lines = [title]

    wrapped_lines: List[str] = []
    for raw in raw_lines:
        text = str(raw or "")
        if not text.strip():
            wrapped_lines.append("")
            continue
        parts = textwrap.wrap(
            text,
            width=96,
            break_long_words=True,
            replace_whitespace=False,
            drop_whitespace=False,
        )
        wrapped_lines.extend(parts or [""])

    lines_per_page = 48
    pages: List[List[str]] = []
    for idx in range(0, len(wrapped_lines), lines_per_page):
        pages.append(wrapped_lines[idx : idx + lines_per_page])
    if not pages:
        pages = [[""]]

    page_count = len(pages)
    font_obj_id = 3

    objects: Dict[int, bytes] = {}
    objects[1] = b"<< /Type /Catalog /Pages 2 0 R >>"

    page_obj_ids = [4 + i * 2 for i in range(page_count)]
    kids_blob = " ".join(f"{obj_id} 0 R" for obj_id in page_obj_ids)
    objects[2] = f"<< /Type /Pages /Count {page_count} /Kids [{kids_blob}] >>".encode("latin-1")
    objects[3] = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"

    for i, page_lines in enumerate(pages):
        page_obj_id = 4 + i * 2
        content_obj_id = page_obj_id + 1

        content_cmds: List[str] = [
            "BT",
            "/F1 10 Tf",
            "12 TL",
            "50 760 Td",
        ]
        for line_index, line in enumerate(page_lines):
            content_cmds.append(f"({_pdf_escape(line)}) Tj")
            if line_index < len(page_lines) - 1:
                content_cmds.append("T*")
        content_cmds.append("ET")

        stream = "\n".join(content_cmds).encode("latin-1", errors="replace")
        objects[content_obj_id] = (
            f"<< /Length {len(stream)} >>\nstream\n".encode("latin-1")
            + stream
            + b"\nendstream"
        )
        objects[page_obj_id] = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            f"/Resources << /Font << /F1 {font_obj_id} 0 R >> >> "
            f"/Contents {content_obj_id} 0 R >>"
        ).encode("latin-1")

    total_objects = 3 + (page_count * 2)
    pdf = bytearray()
    pdf.extend(b"%PDF-1.4\n")
    pdf.extend(b"%\xe2\xe3\xcf\xd3\n")

    offsets: List[int] = [0] * (total_objects + 1)
    for obj_id in range(1, total_objects + 1):
        offsets[obj_id] = len(pdf)
        pdf.extend(f"{obj_id} 0 obj\n".encode("latin-1"))
        payload = objects.get(obj_id, b"<<>>")
        pdf.extend(payload)
        if not payload.endswith(b"\n"):
            pdf.extend(b"\n")
        pdf.extend(b"endobj\n")

    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {total_objects + 1}\n".encode("latin-1"))
    pdf.extend(b"0000000000 65535 f \n")
    for obj_id in range(1, total_objects + 1):
        pdf.extend(f"{offsets[obj_id]:010d} 00000 n \n".encode("latin-1"))
    pdf.extend(
        (
            f"trailer\n<< /Size {total_objects + 1} /Root 1 0 R >>\n"
            f"startxref\n{xref_offset}\n%%EOF\n"
        ).encode("latin-1")
    )
    return bytes(pdf)


def _memory_platform_export_pdf(
    *,
    stats: Dict[str, Any],
    user_rows: List[Dict[str, Any]],
    room_rows: List[Dict[str, Any]],
) -> bytes:
    insights = _memory_platform_insight_frames(user_rows, room_rows)
    try:
        styled_rows = _memory_platform_export_styled_rows(
            stats=stats,
            user_rows=user_rows,
            room_rows=room_rows,
            insights=insights,
        )
        return _build_styled_pdf_from_rows(styled_rows, title="Tater Memory Report")
    except Exception:
        lines = _memory_platform_export_lines(
            stats=stats,
            user_rows=user_rows,
            room_rows=room_rows,
            insights=insights,
        )
        return _build_simple_pdf_from_lines(lines, title="Tater Memory Report")


def _memory_platform_user_fact_rows_by_category(doc: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for category in _MEMORY_USER_PROFILE_CATEGORIES:
        label = str(category.get("label") or "").strip()
        if label:
            grouped[label] = []
    grouped["Uncategorized"] = []

    rows = _memory_platform_fact_rows(doc)
    for row in rows:
        key = str(row.get("key") or "").strip()
        category_label = _MEMORY_USER_FACT_TO_CATEGORY.get(key, "Uncategorized")
        grouped.setdefault(category_label, []).append(row)
    return grouped


def _memory_platform_room_fact_rows_by_category(doc: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for category in _MEMORY_ROOM_PROFILE_CATEGORIES:
        label = str(category.get("label") or "").strip()
        if label:
            grouped[label] = []
    grouped["Uncategorized"] = []

    rows = _memory_platform_fact_rows(doc)
    for row in rows:
        key = str(row.get("key") or "").strip()
        category_label = _MEMORY_ROOM_FACT_TO_CATEGORY.get(key, "Uncategorized")
        grouped.setdefault(category_label, []).append(row)
    return grouped


def _memory_platform_ui_token(*parts: Any) -> str:
    text = "|".join([str(part or "").strip() for part in parts])
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:12]


def _memory_platform_resolve_doc_key(scope: str, platform: Any, scope_id: Any) -> str:
    scope_name = str(scope or "").strip().lower()
    platform_name = str(platform or "webui").strip() or "webui"
    scope_name_id = str(scope_id or "").strip()
    if not scope_name_id:
        return ""
    if scope_name == "user":
        return memory_platform_user_doc_key(platform_name, scope_name_id)
    if scope_name == "room":
        return memory_platform_room_doc_key(platform_name, scope_name_id)
    return ""


def _memory_platform_forget_fact_keys(scope: str, platform: Any, scope_id: Any, keys: List[str]) -> Dict[str, Any]:
    doc_key = _memory_platform_resolve_doc_key(scope, platform, scope_id)
    if not doc_key:
        return {"ok": False, "error": "Invalid memory scope target."}
    requested = [str(item or "").strip() for item in (keys or []) if str(item or "").strip()]
    if not requested:
        return {"ok": False, "error": "No memory keys selected."}

    try:
        doc = load_memory_platform_doc(redis_client, doc_key)
        deleted = int(forget_memory_platform_fact_keys(doc, requested) or 0)
        if deleted <= 0:
            return {"ok": False, "error": "No matching memory keys found.", "deleted": 0}

        facts = doc.get("facts") if isinstance(doc.get("facts"), dict) else {}
        if facts:
            save_memory_platform_doc(redis_client, doc_key, doc, now=time.time())
            return {"ok": True, "deleted": deleted, "deleted_doc": False}

        redis_client.delete(doc_key)
        return {"ok": True, "deleted": deleted, "deleted_doc": True}
    except Exception as exc:
        return {"ok": False, "error": f"Memory delete failed: {exc}"}


def _memory_platform_forget_doc(scope: str, platform: Any, scope_id: Any) -> Dict[str, Any]:
    doc_key = _memory_platform_resolve_doc_key(scope, platform, scope_id)
    if not doc_key:
        return {"ok": False, "error": "Invalid memory scope target."}
    try:
        deleted = int(redis_client.delete(doc_key) or 0)
        if deleted <= 0:
            return {"ok": False, "error": "Memory document was already empty."}
        return {"ok": True, "deleted": deleted}
    except Exception as exc:
        return {"ok": False, "error": f"Memory document delete failed: {exc}"}


def _memory_platform_wipe_all_data() -> Dict[str, Any]:
    patterns = (
        "mem:user:*",
        "mem:room:*",
        "mem:cursor:*",
        "mem:identity_alias:*",
        "mem:identity_name:*",
        "tater:room_label:*",
    )
    deleted_by_pattern: Dict[str, int] = {}
    deleted_total = 0

    def _delete_matching(pattern: str) -> int:
        removed = 0
        batch: List[str] = []
        for raw_key in redis_client.scan_iter(match=pattern, count=500):
            key = str(raw_key or "").strip()
            if not key:
                continue
            batch.append(key)
            if len(batch) >= 200:
                removed += int(redis_client.delete(*batch) or 0)
                batch = []
        if batch:
            removed += int(redis_client.delete(*batch) or 0)
        return removed

    try:
        for pattern in patterns:
            removed = _delete_matching(pattern)
            deleted_by_pattern[pattern] = removed
            deleted_total += removed

        stats_removed = int(redis_client.delete("mem:stats:memory_platform") or 0)
        deleted_by_pattern["mem:stats:memory_platform"] = stats_removed
        deleted_total += stats_removed

        return {
            "ok": True,
            "deleted_total": deleted_total,
            "deleted_by_pattern": deleted_by_pattern,
        }
    except Exception as exc:
        return {"ok": False, "error": f"Memory wipe failed: {exc}"}


def _memory_platform_scan_keys(pattern: str) -> List[str]:
    keys: List[str] = []
    try:
        for raw_key in redis_client.scan_iter(match=str(pattern or ""), count=500):
            key = str(raw_key or "").strip()
            if key:
                keys.append(key)
    except Exception:
        return []
    keys.sort()
    return keys


def _memory_platform_backup_payload() -> Dict[str, Any]:
    user_docs: Dict[str, str] = {}
    room_docs: Dict[str, str] = {}
    cursors: Dict[str, str] = {}
    identity_aliases: Dict[str, str] = {}
    identity_names: Dict[str, str] = {}
    room_labels: Dict[str, str] = {}

    for key in _memory_platform_scan_keys("mem:user:*"):
        value = redis_client.get(key)
        if value is None:
            continue
        user_docs[key] = str(value)

    for key in _memory_platform_scan_keys("mem:room:*"):
        value = redis_client.get(key)
        if value is None:
            continue
        room_docs[key] = str(value)

    for key in _memory_platform_scan_keys("mem:cursor:*"):
        value = redis_client.get(key)
        if value is None:
            continue
        cursors[key] = str(value)

    for key in _memory_platform_scan_keys("mem:identity_alias:*"):
        value = redis_client.get(key)
        if value is None:
            continue
        identity_aliases[key] = str(value)

    for key in _memory_platform_scan_keys("mem:identity_name:*"):
        value = redis_client.get(key)
        if value is None:
            continue
        identity_names[key] = str(value)

    for key in _memory_platform_scan_keys("tater:room_label:*"):
        value = redis_client.get(key)
        if value is None:
            continue
        room_labels[key] = str(value)

    settings = redis_client.hgetall("memory_platform_settings") or {}
    stats = redis_client.hgetall("mem:stats:memory_platform") or {}

    return {
        "backup_type": "tater_memory_platform_backup",
        "backup_version": 2,
        "exported_at": time.time(),
        "exported_at_text": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "counts": {
            "user_docs": len(user_docs),
            "room_docs": len(room_docs),
            "cursors": len(cursors),
            "identity_aliases": len(identity_aliases),
            "identity_names": len(identity_names),
            "room_labels": len(room_labels),
            "settings_fields": len(settings),
            "stats_fields": len(stats),
        },
        "user_docs": user_docs,
        "room_docs": room_docs,
        "cursors": cursors,
        "identity_aliases": identity_aliases,
        "identity_names": identity_names,
        "room_labels": room_labels,
        "settings": settings,
        "stats": stats,
    }


def _memory_platform_backup_json_bytes() -> bytes:
    payload = _memory_platform_backup_payload()
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    return text.encode("utf-8")


def _memory_platform_to_redis_string(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def _memory_platform_filter_prefixed_map(raw: Any, prefix: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(raw, dict):
        return out
    for raw_key, raw_value in raw.items():
        key = str(raw_key or "").strip()
        if not key or not key.startswith(prefix):
            continue
        out[key] = _memory_platform_to_redis_string(raw_value)
    return out


def _memory_platform_filter_hash_map(raw: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(raw, dict):
        return out
    for raw_key, raw_value in raw.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        out[key] = _memory_platform_to_redis_string(raw_value)
    return out


def _memory_platform_import_backup_payload(
    payload: Any,
    *,
    replace_existing: bool,
    restore_settings: bool,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {"ok": False, "error": "Backup payload is not valid JSON object."}

    user_docs = _memory_platform_filter_prefixed_map(payload.get("user_docs"), "mem:user:")
    room_docs = _memory_platform_filter_prefixed_map(payload.get("room_docs"), "mem:room:")
    cursors = _memory_platform_filter_prefixed_map(payload.get("cursors"), "mem:cursor:")
    identity_aliases = _memory_platform_filter_prefixed_map(payload.get("identity_aliases"), "mem:identity_alias:")
    identity_names = _memory_platform_filter_prefixed_map(payload.get("identity_names"), "mem:identity_name:")
    room_labels = _memory_platform_filter_prefixed_map(payload.get("room_labels"), "tater:room_label:")
    stats = _memory_platform_filter_hash_map(payload.get("stats"))
    settings = _memory_platform_filter_hash_map(payload.get("settings"))

    if (
        not user_docs
        and not room_docs
        and not cursors
        and not identity_aliases
        and not identity_names
        and not room_labels
        and not stats
        and not settings
    ):
        return {"ok": False, "error": "Backup contains no restorable memory data."}

    try:
        if replace_existing:
            _memory_platform_wipe_all_data()
            if restore_settings:
                redis_client.delete("memory_platform_settings")

        pipe = redis_client.pipeline()
        for key, value in user_docs.items():
            pipe.set(key, value)
        for key, value in room_docs.items():
            pipe.set(key, value)
        for key, value in cursors.items():
            pipe.set(key, value)
        for key, value in identity_aliases.items():
            pipe.set(key, value)
        for key, value in identity_names.items():
            pipe.set(key, value)
        for key, value in room_labels.items():
            pipe.set(key, value)
        pipe.execute()

        if stats:
            redis_client.hset("mem:stats:memory_platform", mapping=stats)
        if restore_settings and settings:
            redis_client.hset("memory_platform_settings", mapping=settings)

        return {
            "ok": True,
            "imported_user_docs": len(user_docs),
            "imported_room_docs": len(room_docs),
            "imported_cursors": len(cursors),
            "imported_identity_aliases": len(identity_aliases),
            "imported_identity_names": len(identity_names),
            "imported_room_labels": len(room_labels),
            "imported_stats_fields": len(stats),
            "imported_settings_fields": len(settings) if restore_settings else 0,
        }
    except Exception as exc:
        return {"ok": False, "error": f"Backup import failed: {exc}"}


def _memory_platform_collect_fact_entries(
    user_rows: List[Dict[str, Any]],
    room_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []

    def _append_rows(rows: List[Dict[str, Any]], scope: str) -> None:
        for row in rows:
            platform_name = str(row.get("platform") or "").strip() or "unknown"
            doc = row.get("doc") if isinstance(row.get("doc"), dict) else {}
            facts = doc.get("facts") if isinstance(doc.get("facts"), dict) else {}
            for key, fact in facts.items():
                if not isinstance(fact, dict):
                    continue
                try:
                    confidence = float(fact.get("confidence") or 0.0)
                except Exception:
                    confidence = 0.0
                try:
                    updated_at = float(fact.get("updated_at") or 0.0)
                except Exception:
                    updated_at = 0.0
                entries.append(
                    {
                        "scope": scope,
                        "platform": platform_name,
                        "scope_id": str(row.get("user_id") or row.get("room_id") or "").strip(),
                        "key": str(key or "").strip(),
                        "value": memory_platform_value_to_text(fact.get("value"), max_chars=240),
                        "confidence": confidence,
                        "updated_at": updated_at,
                    }
                )

    _append_rows(user_rows, "user")
    _append_rows(room_rows, "room")
    return entries


def _memory_platform_insight_frames(
    user_rows: List[Dict[str, Any]],
    room_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    entries = _memory_platform_collect_fact_entries(user_rows, room_rows)
    if not entries:
        return {}

    by_platform: Dict[str, Dict[str, Any]] = {}
    user_key_counts: Dict[str, int] = {}
    room_key_counts: Dict[str, int] = {}
    recent_key_counts: Dict[str, int] = {}
    shared_user_fact_sets: Dict[tuple[str, str], set[str]] = {}
    shared_user_fact_labels: Dict[tuple[str, str], str] = {}
    now_ts = time.time()
    recent_threshold = now_ts - (7 * 24 * 60 * 60)

    for entry in entries:
        platform_name = str(entry.get("platform") or "unknown")
        scope = str(entry.get("scope") or "")
        key = str(entry.get("key") or "")
        value_text = " ".join(str(entry.get("value") or "").split()).strip()
        updated_at = float(entry.get("updated_at") or 0.0)

        platform_bucket = by_platform.setdefault(
            platform_name,
            {"platform": platform_name, "user_facts": 0, "room_facts": 0},
        )
        if scope == "user":
            platform_bucket["user_facts"] += 1
            user_key_counts[key] = int(user_key_counts.get(key) or 0) + 1
        elif scope == "room":
            platform_bucket["room_facts"] += 1
            room_key_counts[key] = int(room_key_counts.get(key) or 0) + 1

        if scope == "user" and key and value_text:
            norm_value = value_text.casefold()
            if norm_value not in {"unknown", "n/a", "na", "none", "null", "unspecified"}:
                subject_id = str(entry.get("scope_id") or "").strip()
                subject_token = f"{platform_name}:{subject_id or 'unknown'}"
                trend_key = (key, norm_value)
                shared_user_fact_sets.setdefault(trend_key, set()).add(subject_token)
                shared_user_fact_labels.setdefault(
                    trend_key,
                    textwrap.shorten(f"{key.replace('_', ' ')}: {value_text}", width=56, placeholder="..."),
                )

        if key and updated_at >= recent_threshold:
            recent_key_counts[key] = int(recent_key_counts.get(key) or 0) + 1

    platform_rows = sorted(
        by_platform.values(),
        key=lambda row: (
            -(int(row.get("user_facts") or 0) + int(row.get("room_facts") or 0)),
            str(row.get("platform") or ""),
        ),
    )
    platform_df = pd.DataFrame(platform_rows)
    if not platform_df.empty:
        platform_df = platform_df.set_index("platform")[["user_facts", "room_facts"]]

    def _top_key_df(counts: Dict[str, int]) -> pd.DataFrame:
        rows = [
            {"key": key, "count": count}
            for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:12]
            if key
        ]
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.set_index("key")[["count"]]
        return df

    top_user_df = _top_key_df(user_key_counts)
    top_room_df = _top_key_df(room_key_counts)
    recent_key_df = _top_key_df(recent_key_counts)

    day_labels: List[str] = []
    day_counts: Dict[str, int] = {}
    today = datetime.now().date()
    for days_back in range(13, -1, -1):
        day = today - timedelta(days=days_back)
        label = day.isoformat()
        day_labels.append(label)
        day_counts[label] = 0

    for entry in entries:
        updated_at = float(entry.get("updated_at") or 0.0)
        if updated_at <= 0:
            continue
        label = datetime.fromtimestamp(updated_at).date().isoformat()
        if label in day_counts:
            day_counts[label] += 1

    update_rows = [{"date": label, "updates": int(day_counts.get(label) or 0)} for label in day_labels]
    updates_df = pd.DataFrame(update_rows)
    if not updates_df.empty:
        updates_df = updates_df.set_index("date")[["updates"]]

    trending_rows = [
        {
            "trend": shared_user_fact_labels.get((key, norm_value)) or textwrap.shorten(
                f"{key.replace('_', ' ')}: {norm_value}",
                width=56,
                placeholder="...",
            ),
            "users": len(subjects),
        }
        for (key, norm_value), subjects in shared_user_fact_sets.items()
        if key and len(subjects) >= 2
    ]
    trending_rows = sorted(trending_rows, key=lambda row: (-int(row.get("users") or 0), str(row.get("trend") or "")))[:10]
    trending_user_has_data = bool(trending_rows)
    if not trending_rows:
        trending_rows = [{"trend": "No trending yet", "users": 0}]

    trending_user_df = pd.DataFrame(trending_rows)
    if not trending_user_df.empty:
        trending_user_df = trending_user_df.set_index("trend")[["users"]]

    return {
        "platform_df": platform_df,
        "top_user_df": top_user_df,
        "top_room_df": top_room_df,
        "recent_key_df": recent_key_df,
        "updates_df": updates_df,
        "trending_user_df": trending_user_df,
        "trending_user_has_data": trending_user_has_data,
    }


def _legacy_memory_parse_entry(raw: Any) -> Dict[str, Any]:
    text = str(raw or "")
    if not text:
        return {"value": "", "updated_at": 0.0, "expires_at": None, "source": ""}
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = None

    if isinstance(parsed, dict) and "value" in parsed:
        try:
            updated_at = float(parsed.get("updated_at") or 0.0)
        except Exception:
            updated_at = 0.0
        try:
            expires_at = float(parsed.get("expires_at")) if parsed.get("expires_at") is not None else None
        except Exception:
            expires_at = None
        return {
            "value": parsed.get("value"),
            "updated_at": updated_at,
            "expires_at": expires_at,
            "source": str(parsed.get("source") or "").strip(),
        }

    if parsed is not None:
        return {"value": parsed, "updated_at": 0.0, "expires_at": None, "source": ""}
    return {"value": text, "updated_at": 0.0, "expires_at": None, "source": ""}


def _legacy_memory_parse_scope_key(redis_key: str) -> Optional[Dict[str, Any]]:
    key = str(redis_key or "").strip()
    if not key:
        return None
    if key == _LEGACY_MEMORY_GLOBAL_KEY:
        return {"scope": "global", "platform": "", "user_id": "", "room_id": ""}
    if key.startswith(_LEGACY_MEMORY_USER_PREFIX):
        user_id = key[len(_LEGACY_MEMORY_USER_PREFIX):].strip()
        if not user_id:
            return None
        return {"scope": "user", "platform": "", "user_id": user_id, "room_id": ""}
    if key.startswith(_LEGACY_MEMORY_ROOM_PREFIX):
        payload = key[len(_LEGACY_MEMORY_ROOM_PREFIX):]
        platform_name, sep, room_id = payload.partition(":")
        platform_name = str(platform_name or "").strip()
        room_id = str(room_id or "").strip()
        if not sep or not platform_name or not room_id:
            return None
        return {"scope": "room", "platform": platform_name, "user_id": "", "room_id": room_id}
    return None


def _legacy_memory_scope_display_name(row: Dict[str, Any]) -> str:
    scope = str((row or {}).get("scope") or "").strip().lower()
    if scope == "global":
        return "global"
    if scope == "user":
        return f"user / {str((row or {}).get('user_id') or '').strip() or 'unknown'}"
    if scope == "room":
        platform_name = str((row or {}).get("platform") or "").strip() or "unknown"
        room_id = str((row or {}).get("room_id") or "").strip() or "unknown"
        return f"room / {platform_name} / {room_id}"
    return str((row or {}).get("redis_key") or "").strip() or "unknown"


def _legacy_memory_discovery() -> Dict[str, Any]:
    now_ts = time.time()
    rows: List[Dict[str, Any]] = []

    for raw_key in redis_client.scan_iter(match=f"{_LEGACY_MEMORY_HASH_PREFIX}:*", count=200):
        redis_key = str(raw_key or "").strip()
        if not redis_key:
            continue
        if redis_key == _LEGACY_MEMORY_DEFAULT_TTL_KEY:
            continue

        scope_meta = _legacy_memory_parse_scope_key(redis_key)
        if not isinstance(scope_meta, dict):
            continue

        try:
            raw_map = redis_client.hgetall(redis_key) or {}
        except Exception:
            raw_map = {}

        items: List[Dict[str, Any]] = []
        active_count = 0
        expired_count = 0
        latest_updated_ts = 0.0
        for raw_field, raw_value in raw_map.items():
            item_key = str(raw_field or "").strip()
            if not item_key:
                continue
            entry = _legacy_memory_parse_entry(raw_value)
            try:
                updated_at = float(entry.get("updated_at") or 0.0)
            except Exception:
                updated_at = 0.0
            expires_at = entry.get("expires_at")
            is_expired = bool(isinstance(expires_at, (int, float)) and expires_at > 0 and expires_at <= now_ts)
            if is_expired:
                expired_count += 1
            else:
                active_count += 1
            latest_updated_ts = max(latest_updated_ts, updated_at)
            items.append(
                {
                    "key": item_key,
                    "value": entry.get("value"),
                    "source": str(entry.get("source") or "").strip(),
                    "updated_at_ts": updated_at,
                    "updated_at": _format_unix_ts(updated_at),
                    "expires_at_ts": expires_at if isinstance(expires_at, (int, float)) else None,
                    "expires_at": _format_unix_ts(expires_at),
                    "is_expired": is_expired,
                }
            )
        items = sorted(items, key=lambda item: str(item.get("key") or ""))

        row = {
            "redis_key": redis_key,
            "scope": str(scope_meta.get("scope") or ""),
            "platform": str(scope_meta.get("platform") or ""),
            "user_id": str(scope_meta.get("user_id") or ""),
            "room_id": str(scope_meta.get("room_id") or ""),
            "entry_count": len(items),
            "active_count": int(active_count),
            "expired_count": int(expired_count),
            "last_updated_ts": float(latest_updated_ts),
            "last_updated": _format_unix_ts(latest_updated_ts),
            "items": items,
        }
        rows.append(row)

    def _sort_key(row: Dict[str, Any]) -> Any:
        scope_order = {"global": 0, "user": 1, "room": 2}.get(str(row.get("scope") or ""), 9)
        return (
            scope_order,
            str(row.get("platform") or ""),
            str(row.get("user_id") or ""),
            str(row.get("room_id") or ""),
            str(row.get("redis_key") or ""),
        )

    rows = sorted(rows, key=_sort_key)
    return {
        "scopes": rows,
        "scope_count": len(rows),
        "entry_count": sum(int(row.get("entry_count") or 0) for row in rows),
    }


def render_memory_page():
    st.title("Memory")
    st.caption("Durable memory extracted by the Memory Platform.")

    stats = _memory_platform_stats()
    discovery = _memory_platform_doc_discovery()
    user_rows = list(discovery.get("users") or [])
    room_rows = list(discovery.get("rooms") or [])
    tab_stats, tab_users, tab_rooms, tab_legacy, tab_export = st.tabs(["Stats", "Users", "Rooms", "Legacy", "Export"])

    with tab_stats:
        metric_cols_top = st.columns(4)
        metric_cols_top[0].metric("Users", int(discovery.get("user_count") or 0))
        metric_cols_top[1].metric("Rooms", int(discovery.get("room_count") or 0))
        metric_cols_top[2].metric("Facts", int(discovery.get("fact_count") or 0))
        metric_cols_top[3].metric("Processed Msgs", int(stats.get("processed_messages") or 0))

        metric_cols_bottom = st.columns(4)
        metric_cols_bottom[0].metric("Updated Facts", int(stats.get("updated_facts") or 0))
        metric_cols_bottom[1].metric("Updated Docs", int(stats.get("updated_docs") or 0))
        metric_cols_bottom[2].metric("Scanned Scopes", int(stats.get("scanned_scopes") or 0))
        metric_cols_bottom[3].metric("Enabled Platforms", int(stats.get("enabled_platform_count") or 0))

        last_run = str(stats.get("last_run_text") or "").strip()
        if last_run:
            st.caption(f"Memory platform last run: {last_run}")
        else:
            st.caption("Memory platform has not reported stats yet.")

        if not user_rows and not room_rows:
            st.info("No platform memory documents with facts yet.")
        else:
            st.caption(f"Found {len(user_rows)} users and {len(room_rows)} rooms with stored facts.")

        insights = _memory_platform_insight_frames(user_rows, room_rows)
        st.markdown("**Insights**")
        if not insights:
            st.info("No fact data available for graphs yet.")
        else:
            def _render_key_table(
                title: str,
                frame: Any,
                *,
                count_label: str = "count",
                key_label: str = "key",
            ) -> None:
                if not isinstance(frame, pd.DataFrame) or frame.empty:
                    return
                table_df = frame.reset_index()
                if "key" not in table_df.columns:
                    if "index" in table_df.columns:
                        table_df = table_df.rename(columns={"index": "key"})
                    else:
                        first_col = str(table_df.columns[0]) if len(table_df.columns) > 0 else ""
                        if first_col:
                            table_df = table_df.rename(columns={first_col: "key"})

                if count_label not in table_df.columns:
                    if "count" in table_df.columns:
                        table_df = table_df.rename(columns={"count": count_label})
                    elif "facts" in table_df.columns and count_label != "facts":
                        table_df = table_df.rename(columns={"facts": count_label})
                    elif "updates" in table_df.columns and count_label != "updates":
                        table_df = table_df.rename(columns={"updates": count_label})
                    else:
                        candidate_cols: List[str] = []
                        for col in list(table_df.columns):
                            col_name = str(col)
                            if col_name in {"key", "Rank"}:
                                continue
                            try:
                                if pd.api.types.is_numeric_dtype(table_df[col]):
                                    candidate_cols.append(col_name)
                            except Exception:
                                continue
                        if not candidate_cols:
                            for col in list(table_df.columns):
                                col_name = str(col)
                                if col_name not in {"key", "Rank"}:
                                    candidate_cols.append(col_name)
                        if candidate_cols:
                            table_df = table_df.rename(columns={candidate_cols[0]: count_label})
                        else:
                            table_df[count_label] = 0

                if count_label not in table_df.columns:
                    table_df[count_label] = 0
                try:
                    table_df[count_label] = pd.to_numeric(table_df[count_label], errors="coerce").fillna(0).astype(int)
                except Exception:
                    pass

                table_df.insert(0, "Rank", list(range(1, len(table_df) + 1)))
                ordered_cols = [col for col in ("Rank", "key", count_label) if col in table_df.columns]
                if ordered_cols:
                    table_df = table_df[ordered_cols]
                st.caption(title)
                column_config: Dict[str, Any] = {}
                if "Rank" in table_df.columns:
                    column_config["Rank"] = st.column_config.NumberColumn("Rank", width="small", format="%d")
                if "key" in table_df.columns:
                    column_config["key"] = st.column_config.TextColumn(key_label, width="medium")
                if count_label in table_df.columns:
                    column_config[count_label] = st.column_config.NumberColumn(count_label, width="small", format="%d")
                try:
                    st.dataframe(
                        table_df,
                        width="stretch",
                        hide_index=True,
                        column_config=column_config if column_config else None,
                    )
                except TypeError:
                    st.dataframe(table_df, width="stretch")

            platform_df = insights.get("platform_df")
            if isinstance(platform_df, pd.DataFrame) and not platform_df.empty:
                st.caption("Facts by platform")
                st.bar_chart(platform_df, width="stretch")

            trending_user_df = insights.get("trending_user_df")
            if isinstance(trending_user_df, pd.DataFrame) and not trending_user_df.empty:
                _render_key_table(
                    "Trending shared user facts",
                    trending_user_df,
                    count_label="users",
                    key_label="shared fact",
                )
                if not bool(insights.get("trending_user_has_data")):
                    st.caption("No trending shared user facts yet.")
                else:
                    st.caption("Counts show how many users share the same fact key and value.")

            top_user_df = insights.get("top_user_df")
            top_room_df = insights.get("top_room_df")
            top_key_cols = st.columns(2)
            with top_key_cols[0]:
                _render_key_table("Top user fact keys", top_user_df, count_label="facts")
            with top_key_cols[1]:
                _render_key_table("Top room fact keys", top_room_df, count_label="facts")

            recent_key_df = insights.get("recent_key_df")
            _render_key_table("Most updated keys (last 7 days)", recent_key_df, count_label="updates")

            updates_df = insights.get("updates_df")
            if isinstance(updates_df, pd.DataFrame) and not updates_df.empty:
                st.caption("Fact updates (last 14 days)")
                st.line_chart(updates_df, width="stretch")

    with tab_users:
        user_platforms = sorted({str(row.get("platform") or "") for row in user_rows if row.get("platform")})
        user_platform_filter = st.selectbox(
            "User Platform Filter",
            options=["all", *user_platforms],
            key="memory_platform_user_filter",
        )
        filtered_users = [
            row
            for row in user_rows
            if user_platform_filter == "all" or str(row.get("platform") or "") == user_platform_filter
        ]
        if not filtered_users:
            st.info("No user memory entries for the selected filter.")
        else:
            st.dataframe(
                [
                    {
                        "platform": row.get("platform"),
                        "user_id": row.get("user_id"),
                        "facts": row.get("fact_count"),
                        "fact_keys": row.get("fact_keys"),
                        "last_updated": row.get("last_updated"),
                    }
                    for row in filtered_users
                ],
                width="stretch",
            )
            selected_user_idx = st.selectbox(
                "Inspect User Memory Document",
                options=list(range(len(filtered_users))),
                format_func=lambda i: (
                    f"{filtered_users[i].get('platform')} / {filtered_users[i].get('user_id')} "
                    f"({filtered_users[i].get('fact_count')} facts)"
                ),
                key="memory_platform_user_doc_select",
            )
            selected_user = filtered_users[int(selected_user_idx)]
            selected_user_doc = selected_user.get("doc") if isinstance(selected_user.get("doc"), dict) else {}
            user_fact_rows = _memory_platform_fact_rows(selected_user_doc)
            if user_fact_rows:
                grouped_rows = _memory_platform_user_fact_rows_by_category(selected_user_doc)
                for category in _MEMORY_USER_PROFILE_CATEGORIES:
                    label = str(category.get("label") or "").strip()
                    if not label:
                        continue
                    category_rows = list(grouped_rows.get(label) or [])
                    if not category_rows:
                        continue
                    st.markdown(f"**{label}**")
                    st.dataframe(category_rows, width="stretch")
                uncategorized_rows = list(grouped_rows.get("Uncategorized") or [])
                if uncategorized_rows:
                    st.markdown("**Uncategorized**")
                    st.dataframe(uncategorized_rows, width="stretch")
            else:
                st.info("No facts stored in this user memory document.")
            with st.expander("Raw memory document", expanded=False):
                st.json(selected_user_doc)
            selected_user_platform = str(selected_user.get("platform") or "").strip()
            selected_user_id = str(selected_user.get("user_id") or "").strip()
            user_fact_keys = sorted(
                list((selected_user_doc.get("facts") or {}).keys())
            ) if isinstance(selected_user_doc.get("facts"), dict) else []
            user_token = _memory_platform_ui_token("user", selected_user_platform, selected_user_id)

            st.markdown("Remove user memory")
            selected_user_keys = st.multiselect(
                "Select user fact keys to delete",
                options=user_fact_keys,
                key=f"memory_platform_user_delete_keys_{user_token}",
            )
            if st.button(
                "Delete Selected User Fact Keys",
                key=f"memory_platform_user_delete_button_{user_token}",
                disabled=not selected_user_keys,
            ):
                delete_result = _memory_platform_forget_fact_keys(
                    "user",
                    selected_user_platform,
                    selected_user_id,
                    selected_user_keys,
                )
                if delete_result.get("ok"):
                    st.success(
                        f"Deleted {int(delete_result.get('deleted') or 0)} user memory "
                        f"{'key' if int(delete_result.get('deleted') or 0) == 1 else 'keys'}."
                    )
                    st.rerun()
                else:
                    st.error(delete_result.get("error") or "Failed to delete selected user keys.")

            confirm_user_doc_delete = st.checkbox(
                "Confirm delete full user memory document",
                value=False,
                key=f"memory_platform_user_delete_doc_confirm_{user_token}",
            )
            if st.button(
                "Delete Entire User Memory Document",
                key=f"memory_platform_user_delete_doc_button_{user_token}",
                disabled=not confirm_user_doc_delete,
            ):
                delete_result = _memory_platform_forget_doc(
                    "user",
                    selected_user_platform,
                    selected_user_id,
                )
                if delete_result.get("ok"):
                    st.success("Deleted user memory document.")
                    st.rerun()
                else:
                    st.error(delete_result.get("error") or "Failed to delete user memory document.")

    with tab_rooms:
        room_platforms = sorted({str(row.get("platform") or "") for row in room_rows if row.get("platform")})
        room_platform_filter = st.selectbox(
            "Room Platform Filter",
            options=["all", *room_platforms],
            key="memory_platform_room_filter",
        )
        filtered_rooms = [
            row
            for row in room_rows
            if room_platform_filter == "all" or str(row.get("platform") or "") == room_platform_filter
        ]
        if not filtered_rooms:
            st.info("No room memory entries for the selected filter.")
        else:
            st.dataframe(
                [
                    {
                        "platform": row.get("platform"),
                        "room": row.get("room_name") or row.get("room_id"),
                        "facts": row.get("fact_count"),
                        "fact_keys": row.get("fact_keys"),
                        "last_updated": row.get("last_updated"),
                    }
                    for row in filtered_rooms
                ],
                width="stretch",
            )
            selected_room_idx = st.selectbox(
                "Inspect Room Memory Document",
                options=list(range(len(filtered_rooms))),
                format_func=lambda i: (
                    f"{filtered_rooms[i].get('platform')} / "
                    f"{_memory_platform_room_display_name_from_row(filtered_rooms[i])} "
                    f"({filtered_rooms[i].get('fact_count')} facts)"
                ),
                key="memory_platform_room_doc_select",
            )
            selected_room = filtered_rooms[int(selected_room_idx)]
            selected_room_doc = selected_room.get("doc") if isinstance(selected_room.get("doc"), dict) else {}
            room_fact_rows = _memory_platform_fact_rows(selected_room_doc)
            if room_fact_rows:
                grouped_rows = _memory_platform_room_fact_rows_by_category(selected_room_doc)
                for category in _MEMORY_ROOM_PROFILE_CATEGORIES:
                    label = str(category.get("label") or "").strip()
                    if not label:
                        continue
                    category_rows = list(grouped_rows.get(label) or [])
                    if not category_rows:
                        continue
                    st.markdown(f"**{label}**")
                    st.dataframe(category_rows, width="stretch")
                uncategorized_rows = list(grouped_rows.get("Uncategorized") or [])
                if uncategorized_rows:
                    st.markdown("**Uncategorized**")
                    st.dataframe(uncategorized_rows, width="stretch")
            else:
                st.info("No facts stored in this room memory document.")
            with st.expander("Raw memory document", expanded=False):
                st.json(selected_room_doc)
            selected_room_platform = str(selected_room.get("platform") or "").strip()
            selected_room_id = str(selected_room.get("room_id") or "").strip()
            room_fact_keys = sorted(
                list((selected_room_doc.get("facts") or {}).keys())
            ) if isinstance(selected_room_doc.get("facts"), dict) else []
            room_token = _memory_platform_ui_token("room", selected_room_platform, selected_room_id)

            st.markdown("Remove room memory")
            selected_room_keys = st.multiselect(
                "Select room fact keys to delete",
                options=room_fact_keys,
                key=f"memory_platform_room_delete_keys_{room_token}",
            )
            if st.button(
                "Delete Selected Room Fact Keys",
                key=f"memory_platform_room_delete_button_{room_token}",
                disabled=not selected_room_keys,
            ):
                delete_result = _memory_platform_forget_fact_keys(
                    "room",
                    selected_room_platform,
                    selected_room_id,
                    selected_room_keys,
                )
                if delete_result.get("ok"):
                    st.success(
                        f"Deleted {int(delete_result.get('deleted') or 0)} room memory "
                        f"{'key' if int(delete_result.get('deleted') or 0) == 1 else 'keys'}."
                    )
                    st.rerun()
                else:
                    st.error(delete_result.get("error") or "Failed to delete selected room keys.")

            confirm_room_doc_delete = st.checkbox(
                "Confirm delete full room memory document",
                value=False,
                key=f"memory_platform_room_delete_doc_confirm_{room_token}",
            )
            if st.button(
                "Delete Entire Room Memory Document",
                key=f"memory_platform_room_delete_doc_button_{room_token}",
                disabled=not confirm_room_doc_delete,
            ):
                delete_result = _memory_platform_forget_doc(
                    "room",
                    selected_room_platform,
                    selected_room_id,
                )
                if delete_result.get("ok"):
                    st.success("Deleted room memory document.")
                    st.rerun()
                else:
                    st.error(delete_result.get("error") or "Failed to delete room memory document.")

    with tab_legacy:
        st.caption("Legacy key-value memory used by `memory_get` and `memory_set`.")
        legacy_discovery = _legacy_memory_discovery()
        legacy_rows = list(legacy_discovery.get("scopes") or [])

        explicit_text = "false (always)"

        default_ttl_raw = str(redis_client.get(_LEGACY_MEMORY_DEFAULT_TTL_KEY) or "").strip()
        default_ttl_text = default_ttl_raw or "0"

        legacy_metric_cols = st.columns(3)
        legacy_metric_cols[0].metric("Scopes", int(legacy_discovery.get("scope_count") or 0))
        legacy_metric_cols[1].metric("Entries", int(legacy_discovery.get("entry_count") or 0))
        legacy_metric_cols[2].metric("Explicit Only", explicit_text)
        st.caption(f"Default TTL (legacy): {default_ttl_text}s (applies to volatile keys only).")

        if not legacy_rows:
            st.info("No legacy memory scopes found.")
        else:
            st.dataframe(
                [
                    {
                        "scope": row.get("scope"),
                        "platform": row.get("platform"),
                        "user_id": row.get("user_id"),
                        "room_id": row.get("room_id"),
                        "entries": row.get("entry_count"),
                        "active": row.get("active_count"),
                        "expired": row.get("expired_count"),
                        "last_updated": row.get("last_updated"),
                        "redis_key": row.get("redis_key"),
                    }
                    for row in legacy_rows
                ],
                width="stretch",
            )

            selected_legacy_idx = st.selectbox(
                "Inspect Legacy Memory Scope",
                options=list(range(len(legacy_rows))),
                format_func=lambda i: (
                    f"{_legacy_memory_scope_display_name(legacy_rows[i])} "
                    f"({int(legacy_rows[i].get('entry_count') or 0)} entries)"
                ),
                key="legacy_memory_scope_select",
            )
            selected_legacy_scope = legacy_rows[int(selected_legacy_idx)]
            selected_legacy_items = list(selected_legacy_scope.get("items") or [])

            if selected_legacy_items:
                st.dataframe(
                    [
                        {
                            "key": item.get("key"),
                            "value": memory_platform_value_to_text(item.get("value"), max_chars=160),
                            "source": item.get("source"),
                            "updated_at": item.get("updated_at"),
                            "expires_at": item.get("expires_at"),
                            "status": "expired" if bool(item.get("is_expired")) else "active",
                        }
                        for item in selected_legacy_items
                    ],
                    width="stretch",
                )
            else:
                st.info("No key/value entries in this legacy memory scope.")

            legacy_scope_token = _memory_platform_ui_token("legacy", selected_legacy_scope.get("redis_key"))
            legacy_key_options = [str(item.get("key") or "") for item in selected_legacy_items if str(item.get("key") or "").strip()]

            selected_legacy_keys = st.multiselect(
                "Select legacy keys to delete",
                options=legacy_key_options,
                key=f"legacy_memory_delete_keys_{legacy_scope_token}",
            )
            if st.button(
                "Delete Selected Legacy Keys",
                key=f"legacy_memory_delete_keys_button_{legacy_scope_token}",
                disabled=not selected_legacy_keys,
            ):
                try:
                    deleted = int(
                        redis_client.hdel(str(selected_legacy_scope.get("redis_key") or ""), *selected_legacy_keys) or 0
                    )
                    if deleted > 0:
                        st.success(
                            f"Deleted {deleted} legacy memory "
                            f"{'key' if deleted == 1 else 'keys'}."
                        )
                        st.rerun()
                    else:
                        st.error("No matching legacy memory keys were deleted.")
                except Exception as exc:
                    st.error(f"Legacy memory delete failed: {exc}")

            confirm_delete_scope = st.checkbox(
                "Confirm delete entire legacy memory scope",
                value=False,
                key=f"legacy_memory_delete_scope_confirm_{legacy_scope_token}",
            )
            if st.button(
                "Delete Entire Legacy Scope",
                key=f"legacy_memory_delete_scope_button_{legacy_scope_token}",
                disabled=not confirm_delete_scope,
            ):
                try:
                    deleted = int(redis_client.delete(str(selected_legacy_scope.get("redis_key") or "")) or 0)
                    if deleted > 0:
                        st.success("Deleted legacy memory scope.")
                        st.rerun()
                    else:
                        st.error("Legacy memory scope was already empty.")
                except Exception as exc:
                    st.error(f"Legacy scope delete failed: {exc}")

            with st.expander("Raw legacy scope entries", expanded=False):
                st.json(
                    {
                        "scope": selected_legacy_scope.get("scope"),
                        "platform": selected_legacy_scope.get("platform"),
                        "user_id": selected_legacy_scope.get("user_id"),
                        "room_id": selected_legacy_scope.get("room_id"),
                        "redis_key": selected_legacy_scope.get("redis_key"),
                        "entries": {
                            str(item.get("key") or ""): {
                                "value": item.get("value"),
                                "source": item.get("source"),
                                "updated_at": item.get("updated_at"),
                                "expires_at": item.get("expires_at"),
                                "is_expired": bool(item.get("is_expired")),
                            }
                            for item in selected_legacy_items
                        },
                    }
                )

    with tab_export:
        st.caption("Export the current memory snapshot as a PDF report.")
        total_docs = int(len(user_rows) + len(room_rows))
        total_facts = int(discovery.get("fact_count") or 0)
        st.caption(f"Current snapshot: {total_docs} docs, {total_facts} facts.")

        if st.button("Generate Memory PDF", key="memory_platform_export_generate_pdf"):
            pdf_bytes = _memory_platform_export_pdf(
                stats=stats,
                user_rows=user_rows,
                room_rows=room_rows,
            )
            st.session_state["memory_platform_export_pdf_bytes"] = pdf_bytes
            st.session_state["memory_platform_export_pdf_name"] = (
                f"memory_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            )
            size_kb = len(pdf_bytes) / 1024.0
            st.success(f"PDF generated ({size_kb:.1f} KB).")

        pdf_payload = st.session_state.get("memory_platform_export_pdf_bytes")
        if isinstance(pdf_payload, (bytes, bytearray)) and len(pdf_payload) > 0:
            export_name = str(
                st.session_state.get("memory_platform_export_pdf_name")
                or f"memory_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            )
            st.download_button(
                "Download Memory PDF",
                data=bytes(pdf_payload),
                file_name=export_name,
                mime="application/pdf",
                key="memory_platform_export_download_pdf",
                width="stretch",
            )

        st.markdown("---")
        st.subheader("Backup and Restore (JSON)")
        st.caption(
            "Backup includes user docs, room docs, cursors, identity alias/name maps, room labels, memory stats, and memory platform settings."
        )

        if st.button("Generate Memory Backup (JSON)", key="memory_platform_backup_generate_json"):
            backup_bytes = _memory_platform_backup_json_bytes()
            st.session_state["memory_platform_backup_json_bytes"] = backup_bytes
            st.session_state["memory_platform_backup_json_name"] = (
                f"memory_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            st.success(f"Backup generated ({len(backup_bytes) / 1024.0:.1f} KB).")

        backup_payload = st.session_state.get("memory_platform_backup_json_bytes")
        if isinstance(backup_payload, (bytes, bytearray)) and len(backup_payload) > 0:
            backup_name = str(
                st.session_state.get("memory_platform_backup_json_name")
                or f"memory_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )
            st.download_button(
                "Download Memory Backup JSON",
                data=bytes(backup_payload),
                file_name=backup_name,
                mime="application/json",
                key="memory_platform_backup_download_json",
                width="stretch",
            )

        import_file = st.file_uploader(
            "Import Memory Backup JSON",
            type=["json"],
            key="memory_platform_backup_import_file",
        )
        replace_existing = st.checkbox(
            "Replace existing memory before import",
            value=False,
            key="memory_platform_backup_replace_existing",
        )
        restore_settings = st.checkbox(
            "Restore memory platform settings from backup",
            value=True,
            key="memory_platform_backup_restore_settings",
        )
        if st.button(
            "Import Memory Backup",
            key="memory_platform_backup_import_button",
            disabled=import_file is None,
        ):
            if import_file is None:
                st.error("Choose a backup JSON file first.")
            else:
                try:
                    import_text = import_file.read().decode("utf-8", errors="replace")
                    payload = json.loads(import_text)
                except Exception as exc:
                    st.error(f"Invalid backup JSON: {exc}")
                    payload = None

                if payload is not None:
                    result = _memory_platform_import_backup_payload(
                        payload,
                        replace_existing=bool(replace_existing),
                        restore_settings=bool(restore_settings),
                    )
                    if result.get("ok"):
                        st.success(
                            "Import complete. "
                            f"user_docs={int(result.get('imported_user_docs') or 0)}, "
                            f"room_docs={int(result.get('imported_room_docs') or 0)}, "
                            f"cursors={int(result.get('imported_cursors') or 0)}, "
                            f"identity_aliases={int(result.get('imported_identity_aliases') or 0)}, "
                            f"identity_names={int(result.get('imported_identity_names') or 0)}, "
                            f"room_labels={int(result.get('imported_room_labels') or 0)}, "
                            f"stats_fields={int(result.get('imported_stats_fields') or 0)}, "
                            f"settings_fields={int(result.get('imported_settings_fields') or 0)}"
                        )
                        st.rerun()
                    else:
                        st.error(result.get("error") or "Backup import failed.")


def wipe_memory_platform_data() -> Dict[str, Any]:
    return _memory_platform_wipe_all_data()
