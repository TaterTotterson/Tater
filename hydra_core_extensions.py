import importlib
import inspect
from typing import Any, Dict, List, Optional, Tuple

from core_registry import get_core_registry
from helpers import redis_client as default_redis
from verba_kernel import normalize_platform


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _canonical_platform(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    normalized = str(normalize_platform(raw) or "").strip().lower()
    return normalized or raw.lower()


def _platform_match(platforms: Any, platform: str) -> bool:
    normalized_platform = _canonical_platform(platform)
    if not normalized_platform:
        return True
    if platforms is None:
        return True
    raw_items: List[str] = []
    if isinstance(platforms, str):
        raw_items = [item.strip() for item in platforms.split(",") if item.strip()]
    elif isinstance(platforms, (list, tuple, set)):
        raw_items = [str(item).strip() for item in platforms if str(item).strip()]
    if not raw_items:
        return True
    normalized_items = {_canonical_platform(item) for item in raw_items if _canonical_platform(item)}
    if not normalized_items:
        return True
    if normalized_items & {"all", "any", "*", "both"}:
        return True
    return normalized_platform in normalized_items


def _iter_core_modules(*, redis_client: Any = None) -> List[Tuple[str, Dict[str, Any], Any]]:
    out: List[Tuple[str, Dict[str, Any], Any]] = []
    entries: List[Dict[str, Any]] = []
    try:
        entries = list(get_core_registry() or [])
    except Exception:
        entries = []

    redis_obj = redis_client if redis_client is not None else default_redis
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        core_key = str(entry.get("key") or "").strip()
        if not core_key:
            continue

        enabled_hint = _as_bool(entry.get("enabled"), default=True)
        if not enabled_hint:
            continue

        if redis_obj is not None:
            try:
                running_raw = redis_obj.get(f"{core_key}_running")
            except Exception:
                running_raw = None
            if not _as_bool(running_raw, default=False):
                continue

        module_name = str(entry.get("module_import_name") or f"cores.{core_key}").strip()
        if not module_name:
            continue
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue
        out.append((core_key, entry, module))
    return out


def _call_with_supported_kwargs(func: Any, kwargs: Dict[str, Any]) -> Any:
    if not callable(func):
        return None
    try:
        sig = inspect.signature(func)
    except Exception:
        sig = None

    if sig is None:
        try:
            return func(**kwargs)
        except Exception:
            return None

    params = sig.parameters
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()):
        try:
            return func(**kwargs)
        except Exception:
            return None

    filtered: Dict[str, Any] = {}
    for key in params:
        if key in kwargs:
            filtered[key] = kwargs[key]
    try:
        return func(**filtered)
    except Exception:
        return None


def _normalize_kernel_tool_rows(
    rows: Any,
    *,
    core_key: str,
    platform: str,
) -> List[Dict[str, str]]:
    normalized_platform = _canonical_platform(platform)
    out: List[Dict[str, str]] = []
    items = rows if isinstance(rows, list) else []
    for row in items:
        if isinstance(row, str):
            tool_id = row.strip()
            if not tool_id:
                continue
            out.append(
                {
                    "id": tool_id,
                    "description": "",
                    "usage": "",
                    "core_key": core_key,
                }
            )
            continue
        if not isinstance(row, dict):
            continue
        tool_id = str(
            row.get("id")
            or row.get("tool_id")
            or row.get("name")
            or row.get("function")
            or ""
        ).strip()
        if not tool_id:
            continue
        if normalized_platform and not _platform_match(row.get("platforms"), normalized_platform):
            continue
        out.append(
            {
                "id": tool_id,
                "description": str(row.get("description") or row.get("purpose") or "").strip(),
                "usage": str(row.get("usage") or "").strip(),
                "core_key": core_key,
            }
        )
    return out


def get_hydra_kernel_tools(
    *,
    platform: str,
    redis_client: Any = None,
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    for core_key, _entry, module in _iter_core_modules(redis_client=redis_client):
        provider = getattr(module, "get_hydra_kernel_tools", None)
        rows = _call_with_supported_kwargs(
            provider,
            {
                "platform": _canonical_platform(platform) or str(platform or "").strip().lower() or "webui",
                "redis_client": redis_client if redis_client is not None else default_redis,
                "core_key": core_key,
            },
        )
        normalized_rows = _normalize_kernel_tool_rows(
            rows,
            core_key=core_key,
            platform=platform,
        )
        for row in normalized_rows:
            tool_id = str(row.get("id") or "").strip()
            if not tool_id or tool_id in seen:
                continue
            seen.add(tool_id)
            out.append(row)
    return out


def hydra_kernel_tool_purpose(
    tool_id: str,
    *,
    platform: str,
    redis_client: Any = None,
) -> str:
    wanted = str(tool_id or "").strip()
    if not wanted:
        return ""
    for row in get_hydra_kernel_tools(platform=platform, redis_client=redis_client):
        if str(row.get("id") or "").strip() != wanted:
            continue
        return str(row.get("description") or "").strip()
    return ""


def hydra_kernel_tool_usage(
    tool_id: str,
    *,
    platform: str,
    redis_client: Any = None,
) -> str:
    wanted = str(tool_id or "").strip()
    if not wanted:
        return ""
    for row in get_hydra_kernel_tools(platform=platform, redis_client=redis_client):
        if str(row.get("id") or "").strip() != wanted:
            continue
        return str(row.get("usage") or "").strip()
    return ""


def has_hydra_kernel_tool(
    tool_id: str,
    *,
    platform: str,
    redis_client: Any = None,
) -> bool:
    wanted = str(tool_id or "").strip()
    if not wanted:
        return False
    for row in get_hydra_kernel_tools(platform=platform, redis_client=redis_client):
        if str(row.get("id") or "").strip() == wanted:
            return True
    return False


async def run_hydra_kernel_tool(
    *,
    tool_id: str,
    args: Dict[str, Any],
    platform: str,
    scope: str,
    origin: Optional[Dict[str, Any]],
    llm_client: Any = None,
    redis_client: Any = None,
) -> Optional[Dict[str, Any]]:
    wanted = str(tool_id or "").strip()
    if not wanted:
        return None
    normalized_platform = _canonical_platform(platform) or str(platform or "").strip().lower() or "webui"

    for core_key, _entry, module in _iter_core_modules(redis_client=redis_client):
        tools_provider = getattr(module, "get_hydra_kernel_tools", None)
        declared_raw = _call_with_supported_kwargs(
            tools_provider,
            {
                "platform": normalized_platform,
                "redis_client": redis_client if redis_client is not None else default_redis,
                "core_key": core_key,
            },
        )
        declared_tools = _normalize_kernel_tool_rows(
            declared_raw,
            core_key=core_key,
            platform=normalized_platform,
        )
        if not any(str(item.get("id") or "").strip() == wanted for item in declared_tools):
            continue
        runner = getattr(module, "run_hydra_kernel_tool", None)
        if not callable(runner):
            continue
        payload = _call_with_supported_kwargs(
            runner,
            {
                "tool_id": wanted,
                "args": dict(args or {}),
                "platform": normalized_platform,
                "scope": str(scope or "").strip(),
                "origin": dict(origin or {}),
                "llm_client": llm_client,
                "redis_client": redis_client if redis_client is not None else default_redis,
                "core_key": core_key,
            },
        )
        if inspect.isawaitable(payload):
            try:
                payload = await payload
            except Exception:
                payload = None
        if isinstance(payload, dict):
            return payload
    return None


def get_hydra_memory_context_payload(
    *,
    platform: str,
    scope: str,
    origin: Optional[Dict[str, Any]],
    redis_client: Any = None,
) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    normalized_platform = _canonical_platform(platform) or str(platform or "").strip().lower() or "webui"
    for core_key, _entry, module in _iter_core_modules(redis_client=redis_client):
        provider = getattr(module, "get_hydra_memory_context_payload", None)
        payload = _call_with_supported_kwargs(
            provider,
            {
                "platform": normalized_platform,
                "scope": str(scope or "").strip(),
                "origin": dict(origin or {}),
                "redis_client": redis_client if redis_client is not None else default_redis,
                "core_key": core_key,
            },
        )
        if not isinstance(payload, dict) or not payload:
            continue
        if not merged:
            merged = dict(payload)
            continue
        for key, value in payload.items():
            if key not in merged:
                merged[key] = value
    return merged


def _collect_fragments_from_value(value: Any) -> List[str]:
    out: List[str] = []
    if isinstance(value, str):
        text = value.strip()
        if text:
            out.append(text)
        return out
    if isinstance(value, list):
        for item in value:
            out.extend(_collect_fragments_from_value(item))
        return out
    return out


def collect_hydra_system_prompt_fragments(
    *,
    role: str,
    platform: str,
    scope: str,
    origin: Optional[Dict[str, Any]],
    redis_client: Any = None,
    memory_context: Optional[Dict[str, Any]] = None,
) -> List[str]:
    normalized_role = str(role or "").strip().lower()
    normalized_platform = _canonical_platform(platform) or str(platform or "").strip().lower() or "webui"
    out: List[str] = []

    for core_key, _entry, module in _iter_core_modules(redis_client=redis_client):
        provider = getattr(module, "get_hydra_system_prompt_fragments", None)
        payload = _call_with_supported_kwargs(
            provider,
            {
                "role": normalized_role,
                "platform": normalized_platform,
                "scope": str(scope or "").strip(),
                "origin": dict(origin or {}),
                "redis_client": redis_client if redis_client is not None else default_redis,
                "memory_context": dict(memory_context or {}),
                "core_key": core_key,
            },
        )

        if isinstance(payload, dict):
            candidate_values: List[Any] = []
            if normalized_role and normalized_role in payload:
                candidate_values.append(payload.get(normalized_role))
            candidate_values.append(payload.get("all"))
            candidate_values.append(payload.get("fragments"))
            for candidate in candidate_values:
                out.extend(_collect_fragments_from_value(candidate))
            continue

        out.extend(_collect_fragments_from_value(payload))

    deduped: List[str] = []
    seen: set[str] = set()
    for fragment in out:
        text = _coerce_text(fragment).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return deduped
