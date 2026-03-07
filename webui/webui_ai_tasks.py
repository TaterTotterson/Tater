import json
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st


DEFAULT_MANUAL_PLATFORMS = ["homeassistant", "discord", "irc", "matrix", "telegram", "macos"]


def _ordinal_day(value: Any) -> str:
    try:
        day = int(value)
    except Exception:
        return str(value or "").strip()
    if day <= 0:
        return str(day)
    if 10 <= (day % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def _is_enabled(raw: Any, default: bool = True) -> bool:
    if raw is None:
        return bool(default)
    if isinstance(raw, bool):
        return raw
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if value in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _load_schedules(redis_client) -> List[Dict[str, Any]]:
    items_by_id: Dict[str, Dict[str, Any]] = {}
    due_by_id: Dict[str, float] = {}

    due_rows = redis_client.zrange("reminders:due", 0, -1, withscores=True) or []
    for reminder_id, due_ts in due_rows:
        rid = str(reminder_id)
        try:
            due_by_id[rid] = float(due_ts)
        except Exception:
            continue

    for key in redis_client.scan_iter(match="reminders:*", count=500):
        key_s = str(key)
        if key_s == "reminders:due":
            continue
        if not key_s.startswith("reminders:"):
            continue
        rid = key_s.split("reminders:", 1)[1].strip()
        if not rid:
            continue

        raw = redis_client.get(key_s)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue

        due_ts = due_by_id.get(rid)
        if due_ts is None:
            schedule = obj.get("schedule") if isinstance(obj.get("schedule"), dict) else {}
            due_ts = _as_float(schedule.get("next_run_ts"), 0.0)

        obj["_id"] = rid
        obj["_due_ts"] = float(due_ts or 0.0)
        obj["_enabled"] = _is_enabled(obj.get("enabled"), True)
        items_by_id[rid] = obj

    return list(items_by_id.values())


def _save_reminder(redis_client, reminder_id: str, reminder: Dict[str, Any]) -> None:
    redis_client.set(f"reminders:{reminder_id}", json.dumps(reminder))


def _delete_schedule(redis_client, reminder_id: str) -> None:
    rid = str(reminder_id or "").strip()
    if not rid:
        return
    redis_client.zrem("reminders:due", rid)
    redis_client.delete(f"reminders:{rid}")


def _set_due(redis_client, reminder_id: str, due_ts: float) -> None:
    rid = str(reminder_id or "").strip()
    if not rid:
        return
    if due_ts > 0:
        redis_client.zadd("reminders:due", {rid: float(due_ts)})
    else:
        redis_client.zrem("reminders:due", rid)


def _recompute_next_run(schedule: Dict[str, Any], now_ts: Optional[float] = None) -> float:
    if not isinstance(schedule, dict):
        return 0.0
    now = float(now_ts if now_ts is not None else time.time())
    try:
        from cores.ai_task_core import _next_run_for_schedule

        next_run = float(_next_run_for_schedule(schedule, now) or 0.0)
        return next_run if next_run > 0 else 0.0
    except Exception:
        return 0.0


def _join_with_and(values: List[str]) -> str:
    cleaned = [str(v).strip() for v in values if str(v).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"


def _day_name_full(day_index: int) -> str:
    names = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    if 0 <= day_index <= 6:
        return names[day_index]
    return str(day_index)


def _schedule_edit_text(schedule: Dict[str, Any]) -> str:
    if not isinstance(schedule, dict):
        return ""
    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    kind = str(recurrence.get("kind") or "").strip().lower()
    cron_text = str(schedule.get("cron") or recurrence.get("cron") or "").strip()

    hour = int(recurrence.get("hour") or 0)
    minute = int(recurrence.get("minute") or 0)
    second = int(recurrence.get("second") or 0)
    if second:
        time_part = f"{hour:02d}:{minute:02d}:{second:02d}"
    else:
        time_part = f"{hour:02d}:{minute:02d}"

    weekdays = recurrence.get("weekdays") if isinstance(recurrence.get("weekdays"), list) else []
    valid_days: List[int] = []
    for day in weekdays:
        try:
            day_i = int(day)
        except Exception:
            continue
        if 0 <= day_i <= 6 and day_i not in valid_days:
            valid_days.append(day_i)
    valid_days = sorted(valid_days)

    if kind in {"daily_local_time", "weekly_local_time"}:
        if valid_days == [0, 1, 2, 3, 4]:
            return f"weekdays at {time_part}"
        if valid_days == [5, 6]:
            return f"weekends at {time_part}"
        if valid_days:
            day_phrase = _join_with_and([_day_name_full(day) for day in valid_days])
            return f"on {day_phrase} each week at {time_part}"
        return f"everyday at {time_part}"

    if kind == "monthly_local_time":
        monthdays = recurrence.get("monthdays") if isinstance(recurrence.get("monthdays"), list) else []
        valid_monthdays: List[int] = []
        for day in monthdays:
            try:
                day_i = int(day)
            except Exception:
                continue
            if 1 <= day_i <= 31 and day_i not in valid_monthdays:
                valid_monthdays.append(day_i)
        valid_monthdays = sorted(valid_monthdays)
        if valid_monthdays:
            day_phrase = _join_with_and([_ordinal_day(day) for day in valid_monthdays])
            return f"on the {day_phrase} of every month at {time_part}"
        return f"every month at {time_part}"

    if kind == "cron_simple":
        hours = recurrence.get("hours") if isinstance(recurrence.get("hours"), list) else []
        minutes = recurrence.get("minutes") if isinstance(recurrence.get("minutes"), list) else []
        seconds = recurrence.get("seconds") if isinstance(recurrence.get("seconds"), list) else []
        weekdays = recurrence.get("weekdays") if isinstance(recurrence.get("weekdays"), list) else []

        valid_days: List[int] = []
        for day in weekdays:
            try:
                day_i = int(day)
            except Exception:
                continue
            if 0 <= day_i <= 6 and day_i not in valid_days:
                valid_days.append(day_i)
        valid_days = sorted(valid_days)

        if len(hours) == 1 and len(minutes) == 1 and len(seconds) == 1:
            one_time = f"{int(hours[0]):02d}:{int(minutes[0]):02d}"
            if valid_days == [0, 1, 2, 3, 4]:
                return f"weekdays at {one_time}"
            if valid_days == [5, 6]:
                return f"weekends at {one_time}"
            if valid_days:
                day_phrase = _join_with_and([_day_name_full(day) for day in valid_days])
                return f"on {day_phrase} each week at {one_time}"
            return f"everyday at {one_time}"

        if len(hours) == 24 and len(minutes) == 60 and len(seconds) == 60 and not valid_days:
            return "every second"
        if len(hours) == 24 and len(minutes) == 60 and seconds == [0] and not valid_days:
            return "every minute"
        if len(hours) == 24 and minutes == [0] and seconds == [0] and not valid_days:
            return "every hour"

    if cron_text:
        return cron_text
    return ""


def _parse_schedule_input(raw_value: str) -> Tuple[Optional[Dict[str, Any]], str]:
    text = str(raw_value or "").strip()
    if not text:
        return None, "Schedule is required."

    try:
        from plugins.ai_tasks import AITasksPlugin

        parser = AITasksPlugin()
        now_ts = float(time.time())

        parsed, err = parser._parse_cron_schedule(text, now_ts=now_ts)
        if not isinstance(parsed, dict):
            parsed, err = parser._parse_human_schedule(text, now_ts=now_ts)
        if not isinstance(parsed, dict):
            return None, str(err or "Could not parse schedule.")

        next_run = _as_float(parsed.get("next_run_ts"), 0.0)
        recurrence = parsed.get("recurrence") if isinstance(parsed.get("recurrence"), dict) else {}
        interval = _as_float(parsed.get("interval_sec"), 0.0)
        if next_run <= 0 or not recurrence:
            return None, "Could not compute next run from schedule."

        recurrence_payload = dict(recurrence)
        cron_text = str(parsed.get("cron") or recurrence_payload.get("cron") or "").strip()
        if cron_text:
            recurrence_payload["cron"] = cron_text

        schedule_payload: Dict[str, Any] = {
            "next_run_ts": float(next_run),
            "interval_sec": float(interval),
            "anchor_ts": float(next_run),
            "recurrence": recurrence_payload,
        }
        if cron_text:
            schedule_payload["cron"] = cron_text

        return schedule_payload, ""
    except Exception as exc:
        return None, f"Schedule parser error: {exc}"


def _format_relative_due(due_ts: float) -> str:
    if due_ts <= 0:
        return "n/a"
    now = time.time()
    delta = int(due_ts - now)
    if delta <= 0:
        return "now"
    minutes = delta // 60
    if minutes < 1:
        return "in <1 min"
    if minutes < 60:
        return f"in {minutes} min"
    hours = minutes // 60
    if hours < 24:
        rem = minutes % 60
        return f"in {hours}h {rem}m"
    days = hours // 24
    rem_h = hours % 24
    return f"in {days}d {rem_h}h"


def _recurrence_label(schedule: Dict[str, Any], interval: float) -> str:
    recurrence = schedule.get("recurrence") if isinstance(schedule.get("recurrence"), dict) else {}
    kind = str(recurrence.get("kind") or "").strip().lower()
    hour = int(recurrence.get("hour") or 0)
    minute = int(recurrence.get("minute") or 0)
    second = int(recurrence.get("second") or 0)
    weekdays = recurrence.get("weekdays") if isinstance(recurrence.get("weekdays"), list) else []
    time_part = f"{hour:02d}:{minute:02d}" + (f":{second:02d}" if second else "")

    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    valid_days = []
    for day in weekdays:
        try:
            day_i = int(day)
        except Exception:
            continue
        if 0 <= day_i <= 6:
            valid_days.append(day_names[day_i])
    valid_days = sorted(set(valid_days), key=day_names.index)

    if kind == "daily_local_time":
        if valid_days:
            return f"Weekly ({', '.join(valid_days)}) at {time_part}"
        return f"Daily at {time_part}"
    if kind == "weekly_local_time":
        if valid_days:
            return f"Weekly ({', '.join(valid_days)}) at {time_part}"
        return f"Weekly at {time_part}"
    if kind == "monthly_local_time":
        monthdays = recurrence.get("monthdays") if isinstance(recurrence.get("monthdays"), list) else []
        valid_monthdays = [_ordinal_day(day) for day in monthdays if str(day).strip()]
        if valid_monthdays:
            return f"Monthly ({', '.join(valid_monthdays)}) at {time_part}"
        return f"Monthly at {time_part}"
    if kind == "cron_simple":
        hours = recurrence.get("hours") if isinstance(recurrence.get("hours"), list) else []
        minutes = recurrence.get("minutes") if isinstance(recurrence.get("minutes"), list) else []
        seconds = recurrence.get("seconds") if isinstance(recurrence.get("seconds"), list) else []
        if len(hours) == 24 and len(minutes) == 60 and len(seconds) == 60 and not valid_days:
            return "Every second"
        if len(hours) == 24 and len(minutes) == 60 and seconds == [0] and not valid_days:
            return "Every minute"
        if len(hours) == 24 and minutes == [0] and seconds == [0] and not valid_days:
            return "Every hour"
        if len(hours) == 1 and len(minutes) == 1 and len(seconds) == 1:
            one_time = f"{int(hours[0]):02d}:{int(minutes[0]):02d}" + (
                f":{int(seconds[0]):02d}" if int(seconds[0]) else ""
            )
            if valid_days:
                return f"Weekly ({', '.join(valid_days)}) at {one_time}"
            return f"Daily at {one_time}"
        cron_text = str(recurrence.get("cron") or "").strip()
        if cron_text:
            return f"Cron ({cron_text})"
        return "Cron schedule"
    if interval > 0:
        return f"Legacy interval ({int(interval)}s)"
    return "One-shot"


def _derive_title(raw_title: str, command_text: str) -> str:
    title = str(raw_title or "").strip()
    if title:
        return title
    seed = " ".join(str(command_text or "").split()).strip()
    if not seed:
        return "Scheduled task"
    if len(seed) > 80:
        return seed[:77].rstrip() + "..."
    return seed


def render_ai_tasks_page(*, redis_client, embedded: bool = False):
    if embedded:
        st.subheader("AI Tasks")
    else:
        st.title("AI Tasks")
    st.caption("Manage AI tasks and reminders.")

    schedules = _load_schedules(redis_client)
    total_count = len(schedules)
    enabled_count = sum(1 for row in schedules if _is_enabled(row.get("_enabled"), True))
    disabled_count = total_count - enabled_count

    metrics = st.columns(3)
    metrics[0].metric("Total", str(total_count))
    metrics[1].metric("Enabled", str(enabled_count))
    metrics[2].metric("Disabled", str(disabled_count))

    tasks_tab, add_tab = st.tabs(["Scheduled Tasks", "Add Task"])

    with tasks_tab:
        if not schedules:
            st.info("No schedules yet.")
        else:
            sort_rows = sorted(
                schedules,
                key=lambda item: (
                    0 if _is_enabled(item.get("_enabled"), True) else 1,
                    float(item.get("_due_ts") or 0.0) if float(item.get("_due_ts") or 0.0) > 0 else float("inf"),
                ),
            )
            for row in sort_rows:
                rid = str(row.get("_id") or "").strip()
                if not rid:
                    continue

                schedule = row.get("schedule") if isinstance(row.get("schedule"), dict) else {}
                interval = _as_float(schedule.get("interval_sec"), 0.0)
                enabled = _is_enabled(row.get("enabled", row.get("_enabled")), True)
                platform = str(row.get("platform") or "").strip() or "unknown"
                title = str(row.get("title") or "").strip()
                task_prompt = str(row.get("task_prompt") or "").strip()
                message = str(row.get("message") or "").strip()
                command_text = task_prompt or message
                preview = command_text or "(empty)"
                if len(preview) > 180:
                    preview = preview[:177].rstrip() + "..."
                display_title = _derive_title(title, command_text)
                due_ts = _as_float(row.get("_due_ts"), _as_float(schedule.get("next_run_ts"), 0.0))

                if due_ts > 0:
                    due_local = datetime.fromtimestamp(due_ts).strftime("%Y-%m-%d %H:%M:%S")
                    due_relative = _format_relative_due(due_ts)
                    due_text = f"{due_local} ({due_relative})"
                else:
                    due_text = "n/a"

                recurrence_text = _recurrence_label(schedule, interval)
                status_label = "Enabled" if enabled else "Disabled"

                with st.container():
                    top_cols = st.columns([7, 4])
                    with top_cols[0]:
                        st.markdown(f"**{display_title}**")
                        st.caption(f"{platform} · {status_label} · {recurrence_text}")
                        st.write(preview)
                        st.caption(f"Next run: {due_text}")
                    with top_cols[1]:
                        if hasattr(st, "toggle"):
                            toggled_enabled = st.toggle("Enabled", value=enabled, key=f"enabled_toggle_{rid}")
                        else:
                            toggled_enabled = st.checkbox("Enabled", value=enabled, key=f"enabled_toggle_{rid}")

                        if toggled_enabled != enabled:
                            if toggled_enabled:
                                row["enabled"] = True
                                next_run = _recompute_next_run(schedule, now_ts=time.time())
                                if next_run > 0:
                                    schedule["next_run_ts"] = float(next_run)
                                    row["schedule"] = schedule
                                    row["_due_ts"] = float(next_run)
                                    _set_due(redis_client, rid, float(next_run))
                                _save_reminder(redis_client, rid, row)
                                st.success("Task enabled.")
                            else:
                                row["enabled"] = False
                                _save_reminder(redis_client, rid, row)
                                _set_due(redis_client, rid, 0.0)
                                st.success("Task disabled.")
                            st.rerun()

                        action_cols = st.columns(2)
                        with action_cols[0]:
                            if st.button(
                                "Run Now",
                                key=f"run_sched_{rid}",
                                disabled=not toggled_enabled,
                                type="primary",
                                use_container_width=True,
                            ):
                                _set_due(redis_client, rid, float(time.time()))
                                st.success("Task queued to run now.")
                                st.rerun()
                        with action_cols[1]:
                            if st.button(
                                "Delete Task",
                                key=f"del_sched_{rid}",
                                type="secondary",
                                use_container_width=True,
                            ):
                                _delete_schedule(redis_client, rid)
                                st.success("Task removed.")
                                st.rerun()

                with st.expander("Edit task", expanded=False):
                    with st.form(key=f"edit_form_{rid}"):
                        new_title = st.text_input("Title", value=title, key=f"title_{rid}")
                        new_command = st.text_area(
                            "Command",
                            value=command_text,
                            height=110,
                            key=f"command_{rid}",
                            help="This is what will run each time.",
                        )
                        new_schedule_input = st.text_input(
                            "Schedule (natural or cron)",
                            value=_schedule_edit_text(schedule),
                            key=f"schedule_{rid}",
                            help="Examples: weekdays at 8am, on monday and wednesday each week at 6:30pm, on the 10th of every month, or 0 30 18 * * mon,wed",
                        )
                        submit_edit = st.form_submit_button("Save changes")

                    if submit_edit:
                        command_clean = str(new_command or "").strip()
                        if not command_clean:
                            st.error("Command is required.")
                        else:
                            parsed_schedule, parse_error = _parse_schedule_input(new_schedule_input)
                            if not parsed_schedule:
                                st.error(parse_error or "Could not parse schedule.")
                            else:
                                row["title"] = str(new_title or "").strip()
                                row["task_prompt"] = command_clean
                                if "message" in row:
                                    row["message"] = command_clean
                                row["schedule"] = parsed_schedule
                                row["updated_at"] = float(time.time())
                                if enabled:
                                    next_run = _as_float(parsed_schedule.get("next_run_ts"), 0.0)
                                    _set_due(redis_client, rid, next_run)
                                else:
                                    _set_due(redis_client, rid, 0.0)
                                _save_reminder(redis_client, rid, row)
                                st.success("Task updated.")
                                st.rerun()

                st.markdown("---")

    with add_tab:
        st.markdown("### Add AI Task")
        st.caption("Create a task manually with a command and schedule.")
        existing_platforms = sorted(
            set(
                str(row.get("platform") or "").strip().lower()
                for row in schedules
                if str(row.get("platform") or "").strip()
            )
        )
        platform_options = existing_platforms + [p for p in DEFAULT_MANUAL_PLATFORMS if p not in existing_platforms]
        if not platform_options:
            platform_options = list(DEFAULT_MANUAL_PLATFORMS)

        with st.form("add_ai_task_form"):
            add_title = st.text_input("Title (optional)", value="")
            add_command = st.text_area(
                "Command",
                value="",
                height=120,
                help="What the AI task should do when it runs.",
            )
            add_schedule = st.text_input(
                "Schedule (natural or cron)",
                value="",
                help="Examples: weekdays at 8am, on the 10th of every month at 9:15am, every hour, or 0 0 6 * * *",
            )
            add_platform = st.selectbox("Destination portal", options=platform_options, index=0)
            add_targets = st.text_area(
                "Targets JSON (optional)",
                value="{}",
                height=90,
                help="Leave {} for default destination. Example: {\"channel\":\"#general\"}",
            )
            add_enabled = st.checkbox("Enabled", value=True)
            submit_add = st.form_submit_button("Add task")

        if submit_add:
            command_clean = str(add_command or "").strip()
            if not command_clean:
                st.error("Command is required.")
            else:
                parsed_schedule, parse_error = _parse_schedule_input(add_schedule)
                if not parsed_schedule:
                    st.error(parse_error or "Could not parse schedule.")
                else:
                    targets_raw = str(add_targets or "").strip() or "{}"
                    try:
                        parsed_targets = json.loads(targets_raw)
                    except Exception as exc:
                        parsed_targets = None
                        st.error(f"Targets JSON is invalid: {exc}")

                    if isinstance(parsed_targets, dict):
                        now_ts = float(time.time())
                        reminder_id = str(uuid.uuid4())
                        reminder = {
                            "id": reminder_id,
                            "created_at": now_ts,
                            "platform": str(add_platform or "").strip().lower(),
                            "title": _derive_title(str(add_title or "").strip(), command_clean),
                            "task_prompt": command_clean,
                            "targets": parsed_targets,
                            "origin": {"platform": "webui", "scope": "ai_tasks_ui"},
                            "meta": {},
                            "schedule": parsed_schedule,
                            "enabled": bool(add_enabled),
                        }
                        _save_reminder(redis_client, reminder_id, reminder)
                        if bool(add_enabled):
                            next_run = _as_float(parsed_schedule.get("next_run_ts"), 0.0)
                            _set_due(redis_client, reminder_id, next_run)
                        else:
                            _set_due(redis_client, reminder_id, 0.0)
                        st.success("AI task added.")
                        st.rerun()
