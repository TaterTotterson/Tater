import json
from typing import Any, Callable, Dict, List, Optional

from admin_gate import resolve_admin_status
from verba_result import action_failure

from .chat_loop import run_spudex_chat_turn
from .policy import resolve_spudex_cwd
from .runner import create_spudex_session, update_spudex_session
from .settings import (
    get_spudex_settings,
    spudex_enabled_for_platform,
    spudex_llm_overrides,
)


SPUDEX_TOOL_ROWS = [
    {
        "id": "run_terminal_task",
        "description": (
            "send a natural-language request to run policy-controlled console or terminal work "
            "on the current PC where the assistant is running; use for local CPU/GPU/RAM/disk "
            "stats, OS/process diagnostics, scripts, file work, local servers, or other command-line tasks"
        ),
        "usage": '{"function":"run_terminal_task","arguments":{"request":"<natural-language terminal task to complete>"}}',
    },
]

LITTLE_SPUD_PLATFORM = "little_spud"


def _platform_token(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _little_spud_admin_access(
    *,
    platform: str,
    origin: Optional[Dict[str, Any]],
    redis_client: Any = None,
) -> Dict[str, Any]:
    if _platform_token(platform) != LITTLE_SPUD_PLATFORM:
        return {"allowed": True, "platform": _platform_token(platform)}

    status = resolve_admin_status(
        platform=LITTLE_SPUD_PLATFORM,
        origin=origin if isinstance(origin, dict) else {},
        redis_client=redis_client,
    )
    return {
        **status,
        "allowed": bool(status.get("matched")) and bool(status.get("is_admin")),
        "platform": LITTLE_SPUD_PLATFORM,
    }


def _little_spud_admin_failure(access: Dict[str, Any]) -> Dict[str, Any]:
    person_name = str(access.get("person_name") or "").strip()
    if bool(access.get("matched")) and person_name:
        message = (
            "Spudex terminal access from Little Spud is restricted to People marked as admin. "
            f"{person_name} is not marked admin."
        )
    else:
        message = (
            "Spudex terminal access from Little Spud requires this Little Spud identity "
            "to be linked to a Person marked as admin in Settings > People."
        )
    return action_failure(
        code="spudex_little_spud_admin_required",
        message=message,
        say_hint="Explain that Little Spud terminal access requires an admin-linked Person.",
    )


def spudex_hydra_tool_rows(
    *,
    platform: str,
    origin: Optional[Dict[str, Any]] = None,
    redis_client: Any = None,
) -> list[Dict[str, str]]:
    if not spudex_enabled_for_platform(platform, redis_client):
        return []
    access = _little_spud_admin_access(
        platform=platform,
        origin=origin,
        redis_client=redis_client,
    )
    if not bool(access.get("allowed")):
        return []
    return [dict(row) for row in SPUDEX_TOOL_ROWS]


def spudex_has_hydra_tool(
    tool_id: str,
    *,
    platform: str = "",
    origin: Optional[Dict[str, Any]] = None,
    redis_client: Any = None,
) -> bool:
    token = str(tool_id or "").strip()
    if token not in {str(row["id"]) for row in SPUDEX_TOOL_ROWS}:
        return False
    if platform:
        return bool(
            spudex_hydra_tool_rows(
                platform=platform,
                origin=origin,
                redis_client=redis_client,
            )
        )
    return bool(get_spudex_settings(redis_client).get("enabled"))


def spudex_tool_purpose_hint(tool_id: str) -> str:
    token = str(tool_id or "").strip()
    for row in SPUDEX_TOOL_ROWS:
        if row["id"] == token:
            return str(row.get("description") or "")
    return ""


def spudex_tool_usage_hint(tool_id: str) -> str:
    token = str(tool_id or "").strip()
    for row in SPUDEX_TOOL_ROWS:
        if row["id"] == token:
            return str(row.get("usage") or "")
    return ""


def _approval_required(
    settings: Dict[str, Any],
    argv: List[str],
    *,
    actor: str = "Hydra",
) -> Optional[Dict[str, Any]]:
    if not bool(settings.get("require_approval")):
        return None
    label = str(actor or "Hydra").strip() or "Hydra"
    return action_failure(
        code="spudex_approval_required",
        message=f"Spudex approval is enabled, so {label} did not run: {' '.join(argv)}",
        diagnosis={"argv": json.dumps(argv)},
        needs=[
            "Turn off Spudex approval in the Spudex tab, or run the command manually from the Spudex tab."
        ],
        say_hint="Explain that the spudex command needs approval in the Tater UI.",
    )


async def _run_spudex_task(
    *,
    args: Dict[str, Any],
    platform: str,
    llm_client: Any,
    redis_client: Any,
    source: str = "hydra",
    approval_actor: str = "Hydra",
    session_id: str = "",
    progress_callback: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    settings = get_spudex_settings(redis_client)
    goal = str(
        args.get("request")
        or args.get("goal")
        or args.get("task")
        or args.get("nl")
        or args.get("prompt")
        or ""
    ).strip()
    if not goal:
        return action_failure(
            code="run_terminal_task_missing_request",
            message="run_terminal_task needs arguments.request with the natural-language terminal task.",
            needs=["Provide arguments.request as a natural-language terminal task."],
            say_hint="Ask what terminal task should be done.",
        )
    if llm_client is None or not hasattr(llm_client, "chat"):
        return action_failure(
            code="run_terminal_task_model_unavailable",
            message="run_terminal_task needs an available language model client.",
            say_hint="Explain that terminal task execution needs the assistant model to plan commands.",
        )

    cwd = resolve_spudex_cwd(settings.get("default_cwd"))
    if session_id:
        clean_session_id = str(session_id)
        update_spudex_session(
            clean_session_id,
            status="running",
            goal=goal,
            source=source,
            platform=platform,
            cwd=str(cwd),
        )
    else:
        session = create_spudex_session(
            label=f"Terminal task: {goal[:80]}",
            cwd=str(cwd),
            goal=goal,
            source=source,
            platform=platform,
        )
        clean_session_id = str(session["id"])

    return await run_spudex_chat_turn(
        session_id=clean_session_id,
        message=goal,
        platform=platform,
        llm_client=llm_client,
        redis_client=redis_client,
        task_mode=True,
        progress_callback=progress_callback,
        approval_callback=lambda argv: _approval_required(
            settings,
            argv,
            actor=approval_actor,
        ),
    )


async def run_spudex_loop_task(
    *,
    args: Dict[str, Any],
    platform: str,
    llm_client: Any,
    redis_client: Any,
    source: str = "spudex_chat",
    approval_actor: str = "Spudex chat",
    session_id: str = "",
    progress_callback: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    return await _run_spudex_task(
        args=args,
        platform=platform,
        llm_client=llm_client,
        redis_client=redis_client,
        source=source,
        approval_actor=approval_actor,
        session_id=session_id,
        progress_callback=progress_callback,
    )


async def run_spudex_hydra_tool(
    *,
    tool_id: str,
    args: Dict[str, Any],
    platform: str,
    origin: Optional[Dict[str, Any]] = None,
    llm_client: Any = None,
    redis_client: Any = None,
    progress_callback: Optional[Callable[..., Any]] = None,
) -> Optional[Dict[str, Any]]:
    token = str(tool_id or "").strip()
    if token not in {str(row["id"]) for row in SPUDEX_TOOL_ROWS}:
        return None
    if not spudex_enabled_for_platform(platform, redis_client):
        return action_failure(
            code="spudex_disabled",
            message="Tater Spudex is disabled for this platform.",
            say_hint="Explain that the Spudex feature must be enabled in the Spudex tab first.",
        )
    access = _little_spud_admin_access(
        platform=platform,
        origin=origin,
        redis_client=redis_client,
    )
    if not bool(access.get("allowed")):
        return _little_spud_admin_failure(access)

    payload = args if isinstance(args, dict) else {}
    if token != "run_terminal_task":
        return None

    overrides = spudex_llm_overrides(redis_client)
    if overrides.get("host") or overrides.get("model"):
        from helpers import get_llm_client_from_env

        llm_kwargs: Dict[str, Any] = {
            "host": overrides.get("host"),
            "model": overrides.get("model"),
            "redis_conn": redis_client,
        }
        if overrides.get("provider") and overrides.get("model"):
            llm_kwargs["provider"] = overrides.get("provider")
        async with get_llm_client_from_env(**llm_kwargs) as spudex_llm_client:
            return await _run_spudex_task(
                args=payload,
                platform=platform,
                llm_client=spudex_llm_client,
                redis_client=redis_client,
                progress_callback=progress_callback,
            )
    return await _run_spudex_task(
        args=payload,
        platform=platform,
        llm_client=llm_client,
        redis_client=redis_client,
        progress_callback=progress_callback,
    )
