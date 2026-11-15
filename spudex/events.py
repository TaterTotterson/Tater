"""Reconnectable, read-only live transport for one Terminal session."""
import asyncio
import json
from .runner import get_spudex_session, read_spudex_logs, _ACTIVE_TASKS, _ACTIVE_PROCESSES


async def session_events(request, session_id: str, after_seq: int = 0):
    try:
        cursor = max(after_seq, int(request.headers.get("last-event-id", "0")))
    except ValueError:
        cursor = max(0, after_seq)
    revision = ""
    heartbeat = 0
    while not await request.is_disconnected():
        session = get_spudex_session(session_id)
        if not session:
            yield 'event: gone\ndata: {}\n\n'
            return
        if not session.get("project") and session.get("source") not in {"ui", "interactive"}:
            from .workspace import session_project
            try:
                session["project"] = session_project(session)
            except (ValueError, OSError):
                pass
        active_task = _ACTIVE_TASKS.get(session_id)
        session["active"] = session_id in _ACTIVE_PROCESSES or bool(active_task and not active_task.done())
        signature = json.dumps([session.get("updated_ts"), session.get("status"), session.get("log_seq"), session["active"]])
        logs = read_spudex_logs(session_id, after_seq=cursor, limit=200) if signature != revision or int(session.get("log_seq") or 0) > cursor else {"entries": [], "has_more": False}
        if logs["entries"] or signature != revision:
            cursor = max(cursor, logs["last_seq"])
            revision = signature
            data = json.dumps({"session": session, **logs}, ensure_ascii=False)
            yield f'id: {cursor}\ndata: {data}\n\n'
            heartbeat = 0
        else:
            heartbeat += 1
            if heartbeat >= 40:
                yield ': keep-alive\n\n'
                heartbeat = 0
        await asyncio.sleep(.05 if logs.get("has_more") else .25)
