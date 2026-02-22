import json
import time
from datetime import datetime
from typing import Any, Dict

import streamlit as st


def _load_schedules(redis_client):
    items_by_id = {}
    due_by_id = {}

    due_rows = redis_client.zrange("reminders:due", 0, -1, withscores=True) or []
    for reminder_id, due_ts in due_rows:
        rid = str(reminder_id)
        try:
            due_by_id[rid] = float(due_ts)
        except Exception:
            pass

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
            try:
                due_ts = float((obj.get("schedule") or {}).get("next_run_ts") or 0.0)
            except Exception:
                due_ts = 0.0

        obj["_id"] = rid
        obj["_due_ts"] = float(due_ts or 0.0)
        items_by_id[rid] = obj

    return list(items_by_id.values())


def _delete_schedule(redis_client, reminder_id: str):
    rid = str(reminder_id or "").strip()
    if not rid:
        return
    redis_client.zrem("reminders:due", rid)
    redis_client.delete(f"reminders:{rid}")


def render_ai_tasks_page(*, redis_client, embedded: bool = False):
    if embedded:
        st.subheader("AI Tasks")
    else:
        st.title("AI Tasks")
    st.caption("Manage AI tasks and reminders.")

    schedules = _load_schedules(redis_client)
    if not schedules:
        st.info("No schedules yet.")
        return

    def _sort_key(item):
        return float(item.get("_due_ts") or 0.0)

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

    for row in sorted(schedules, key=_sort_key):
        rid = row.get("_id", "")
        due_ts = float(row.get("_due_ts") or 0.0)
        schedule = row.get("schedule") or {}
        interval = 0.0
        try:
            interval = float(schedule.get("interval_sec") or 0.0)
        except Exception:
            interval = 0.0

        platform = str(row.get("platform") or "").strip() or "unknown"
        title = str(row.get("title") or "").strip()
        task_prompt = str(row.get("task_prompt") or "").strip()
        message = str(row.get("message") or "").strip()
        preview = task_prompt or message or "(empty)"
        preview = preview[:140] + ("..." if len(preview) > 140 else "")
        derived_title = title or (task_prompt or message or "Scheduled task")
        if len(derived_title) > 80:
            derived_title = derived_title[:77].rstrip() + "..."

        due_local = datetime.fromtimestamp(due_ts).strftime("%Y-%m-%d %H:%M:%S")
        mode_label = "AI Task"
        recur_label = _recurrence_label(schedule, interval)
        summary = f"{mode_label} -> {platform} -> {recur_label} -> next {due_local}"

        with st.container():
            cols = st.columns([8, 3])
            with cols[0]:
                st.markdown(f"**{derived_title}**")
                st.caption(summary)
                st.write(preview)
            with cols[1]:
                action_cols = st.columns([2, 1])
                with action_cols[0]:
                    if st.button("Run now", key=f"run_sched_{rid}", help="Queue this task to run now"):
                        redis_client.zadd("reminders:due", {rid: float(time.time())})
                        st.success("Task queued to run now.")
                        st.rerun()
                with action_cols[1]:
                    if st.button("🗑️", key=f"del_sched_{rid}", help="Delete schedule"):
                        _delete_schedule(redis_client, rid)
                        st.success("Schedule removed.")
                        st.rerun()
        st.markdown("---")
