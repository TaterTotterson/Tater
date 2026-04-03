import json
import mimetypes
import re
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from . import hydra_checker as minos
from . import hydra_doer_state as thanatos_state
from . import hydra_execution as execution
from . import hydra_common_helpers as common_helpers
from . import hydra_ledger as ledger
from . import hydra_limits as limits_helpers
from . import hydra_origin_attach as origin_attach_helpers
from . import hydra_preamble_utils as preamble_utils
from . import hydra_prompts as prompts
from . import hydra_retry_helpers as retry_helpers
from . import hydra_runtime_config as runtime_config
from . import hydra_scope as scope_helpers
from . import hydra_state_core as state_core_helpers
from . import hydra_state_store as state_store
from . import hydra_toolcall_utils as toolcall_utils
from . import hydra_tool_index as tool_index_helpers
from . import hydra_turn_utils as turn_utils
from . import hydra_validation as validation
from . import hydra_validation_flow as validation_flow
from helpers import (
    TOOL_MARKUP_REPAIR_PROMPT,
    get_llm_client_from_env,
    get_tater_name,
    get_tater_personality,
    looks_like_tool_markup,
    parse_function_json,
    redis_client as default_redis,
)
from conversation_artifacts import (
    load_conversation_artifacts,
    save_conversation_artifacts,
)
from verba_kernel import (
    expand_verba_platforms,
    normalize_platform,
    verba_display_name,
    verba_supports_platform,
    verba_when_to_use,
)
from verba_result import action_failure, narrate_result, normalize_verba_result, result_for_llm
from hydra_core_extensions import (
    collect_hydra_system_prompt_fragments,
    get_hydra_memory_context_payload,
)
from tool_runtime import (
    execute_plugin_call,
    is_meta_tool,
    kernel_tool_ids as runtime_kernel_tool_ids,
    kernel_tool_purpose_hint as runtime_kernel_tool_purpose_hint,
    kernel_tool_usage_hint as runtime_kernel_tool_usage_hint,
    run_meta_tool,
)
from notify import notifier_destination_catalog

TOOL_NAME_ALIASES = {
    "web_search": "search_web",
    "google_search": "search_web",
    "google_cse_search": "search_web",
    "inspect_page": "inspect_webpage",
    "inspect_website": "inspect_webpage",
}

_KERNEL_TOOL_PURPOSE_HINTS = {
    "list_tools": "list kernel and enabled verba tools for current platform",
    "get_verba_help": "show verba usage example and guidance",
    "rewrite_text": "rewrite provided text according to natural-language instruction for downstream use",
    "read_file": "read local file contents",
    "search_web": "retrieve ranked link candidates with snippet metadata only (discovery-only; no full-page fetch and no file retrieval)",
    "search_files": "search text across local files",
    "write_file": "write content to a local file",
    "list_directory": "list files and folders",
    "delete_file": "delete a local file",
    "inspect_webpage": "inspect and extract content from a specific webpage URL (title, summary text, links, images)",
    "download_file": "download a file from a concrete file URL after discovery/inspection (actual file retrieval)",
    "list_archive": "inspect archive entries",
    "extract_archive": "extract archives to a target directory",
    "write_workspace_note": "append a workspace note",
    "list_workspace": "list workspace notes",
    "image_describe": "describe an explicit image using an artifact_id, URL, blob, or local path",
    "attach_file": "attach an available artifact or local file, and optionally send it to a destination platform/target",
    "send_message": "queue a cross-portal notification/message only when the user explicitly asks to notify or message a destination (never for normal chat replies)",
}
_KERNEL_TOOL_USAGE_HINTS = {
    "list_tools": '{"function":"list_tools","arguments":{}}',
    "get_verba_help": '{"function":"get_verba_help","arguments":{"verba_id":"<verba_id>"}}',
    "rewrite_text": '{"function":"rewrite_text","arguments":{"instruction":"rewrite this to be funny","text":"the dog ran over the cow"}}',
    "read_file": '{"function":"read_file","arguments":{"path":"<path>"}}',
    "search_web": '{"function":"search_web","arguments":{"query":"<query>"}}',
    "search_files": '{"function":"search_files","arguments":{"query":"<query>","path":"/"}}',
    "write_file": '{"function":"write_file","arguments":{"path":"<path>","content":"<content>"}}',
    "list_directory": '{"function":"list_directory","arguments":{"path":"<path>"}}',
    "delete_file": '{"function":"delete_file","arguments":{"path":"<path>"}}',
    "inspect_webpage": '{"function":"inspect_webpage","arguments":{"url":"https://example.com"}}',
    "download_file": '{"function":"download_file","arguments":{"url":"https://example.com/file"}}',
    "list_archive": '{"function":"list_archive","arguments":{"path":"<archive_path>"}}',
    "extract_archive": '{"function":"extract_archive","arguments":{"path":"<archive_path>","destination":"<dest_path>"}}',
    "write_workspace_note": '{"function":"write_workspace_note","arguments":{"content":"<note_text>"}}',
    "list_workspace": '{"function":"list_workspace","arguments":{}}',
    "image_describe": '{"function":"image_describe","arguments":{"artifact_id":"<artifact_id>","query":"Describe this image."}}',
    "attach_file": '{"function":"attach_file","arguments":{"artifact_id":"<artifact_id>","message":"Attachment"}}',
    "send_message": '{"function":"send_message","arguments":{"message":"<message>"}}',
}

ASCII_ONLY_PLATFORMS = {"irc", "homeassistant", "homekit", "xbmc"}
DEFAULT_CLARIFICATION = "Could you clarify exactly what you want me to do next?"
DEFAULT_MAX_ROUNDS = 0
DEFAULT_MAX_TOOL_CALLS = 0
DEFAULT_MAX_LEDGER_ITEMS = 1500
DEFAULT_RESULT_MEMORY_MAX_SETS = 6
DEFAULT_RESULT_MEMORY_MAX_ITEMS = 8
DEFAULT_STEP_RETRY_LIMIT = 1
DEFAULT_ASTRAEUS_PLAN_REVIEW_ENABLED = False
HYDRA_LLM_HOST_KEY = "tater:hydra:llm_host"
HYDRA_LLM_PORT_KEY = "tater:hydra:llm_port"
HYDRA_LLM_MODEL_KEY = "tater:hydra:llm_model"
CHAT_HISTORY_MAX_ITEMS_KEY = "tater:max_llm"
HYDRA_BEAST_MODE_ENABLED_KEY = "tater:hydra:beast_mode_enabled"
HYDRA_BEAST_ROLE_IDS = (
    "ai_calls",
    "chat",
    "astraeus",
    "thanatos",
    "minos",
    "hermes",
)
HYDRA_BEAST_CONFIG_ROLE_IDS = (
    "chat",
    "astraeus",
    "thanatos",
    "minos",
    "hermes",
)
HYDRA_ROLE_LLM_KEY_PREFIX = "tater:hydra:llm:"
HYDRA_MAX_LEDGER_ITEMS_KEY = "tater:hydra:max_ledger_items"
HYDRA_STEP_RETRY_LIMIT_KEY = "tater:hydra:step_retry_limit"
HYDRA_ASTRAEUS_PLAN_REVIEW_ENABLED_KEY = "tater:hydra:astraeus_plan_review_enabled"
HYDRA_RESULT_MEMORY_MAX_SETS_KEY = "tater:hydra:result_memory_max_sets"
HYDRA_RESULT_MEMORY_MAX_ITEMS_KEY = "tater:hydra:result_memory_max_items"
AGENT_STATE_PROMPT_MAX_CHARS = 800
AGENT_STATE_LEDGER_MAX_CHARS = 900
RESULT_MEMORY_MAX_SETS = DEFAULT_RESULT_MEMORY_MAX_SETS
RESULT_MEMORY_MAX_ITEMS = DEFAULT_RESULT_MEMORY_MAX_ITEMS
STEP_RETRY_LIMIT = DEFAULT_STEP_RETRY_LIMIT
ASTRAEUS_PLAN_REVIEW_ENABLED = DEFAULT_ASTRAEUS_PLAN_REVIEW_ENABLED
HYDRA_LEDGER_SCHEMA_VERSION = "2"

_PLATFORM_DISPLAY = {
    "webui": "WebUI",
    "macos": "macOS",
    "discord": "Discord",
    "irc": "IRC",
    "telegram": "Telegram",
    "matrix": "Matrix",
    "homeassistant": "Home Assistant",
    "homekit": "HomeKit",
    "xbmc": "XBMC",
    "automation": "automation",
}

_URL_RE = re.compile(r"https?://[^\s<>)\]\"']+", flags=re.IGNORECASE)
_MINOS_DECISION_PREFIX_RE = re.compile(
    r"^\s*(CONTINUE|RETRY|ASK[\s_-]*USER|FAIL|FINAL|FINAL[\s_-]*ANSWER|RETRY[\s_-]*TOOL|NEED[\s_-]*USER[\s_-]*INFO)\s*:\s*(.*)$",
    flags=re.IGNORECASE | re.DOTALL,
)
_MEMORY_CONTEXT_DEFAULT_SUMMARY_MAX_CHARS = 2100
_CHAT_ESTIMATE_MESSAGE_OVERHEAD_TOKENS = 8
_CHAT_ESTIMATE_REQUEST_OVERHEAD_TOKENS = 16
_CHAT_ESTIMATE_STANDARD_WINDOWS = [
    2048,
    4096,
    8192,
    16384,
    32768,
    65536,
    131072,
    200000,
]
_CHAT_ESTIMATE_HIGH_CONTEXT_VERBA_KEYWORDS = {
    "jackett",
    "search",
    "crawl",
    "scrape",
    "browser",
    "inspect",
    "download",
    "rss",
    "feed",
    "news",
    "reddit",
    "github",
    "youtube",
    "index",
    "catalog",
    "query",
}
_CHAT_ESTIMATE_MEDIUM_CONTEXT_VERBA_KEYWORDS = {
    "memory",
    "calendar",
    "email",
    "mail",
    "task",
    "todo",
    "weather",
    "finance",
    "stocks",
    "translate",
    "automation",
    "schedule",
}
_CHAT_ESTIMATE_HEAVY_CORE_KEYWORDS = {
    "memory",
    "context",
    "history",
    "rss",
    "search",
    "index",
    "scheduler",
    "agent",
}
_HERMES_RENDER_MODE_ALIASES = {
    "direct": "direct",
    "default": "direct",
    "as_is": "direct",
    "asis": "direct",
    "summarize": "summarize",
    "summary": "summarize",
    "brief": "summarize",
    "rewrite": "rewrite",
    "reword": "rewrite",
    "rename": "rewrite",
    "rephrase": "rewrite",
}
_ACTIVE_CHAT_JOB_LOCK = threading.RLock()
_ACTIVE_CHAT_JOBS: Dict[str, Dict[str, Any]] = {}
_WEB_SOURCE_TOOL_IDS = {"inspect_webpage", "download_file"}
_WEB_SOURCE_QUERY_KEYS_TO_IGNORE = {
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
    "source",
}


def _canonical_web_source_url(value: Any, *, drop_query: bool = False) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = f"https:{raw}"
    try:
        parsed = urlparse(raw)
    except Exception:
        return ""
    scheme = str(parsed.scheme or "").strip().lower()
    if scheme not in {"http", "https"}:
        return ""
    host = str(parsed.hostname or "").strip().lower().strip(".")
    if not host:
        return ""
    try:
        port = parsed.port
    except Exception:
        port = None
    default_port = 80 if scheme == "http" else 443
    netloc = host if port in (None, default_port) else f"{host}:{int(port)}"

    path = str(parsed.path or "/").strip() or "/"
    path = re.sub(r"/{2,}", "/", path)
    if not path.startswith("/"):
        path = f"/{path}"
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")

    query = ""
    if not drop_query:
        try:
            pairs = parse_qsl(str(parsed.query or ""), keep_blank_values=True)
        except Exception:
            pairs = []
        filtered: List[tuple[str, str]] = []
        for key, value in pairs:
            token = str(key or "").strip()
            if not token:
                continue
            lowered = token.lower()
            if lowered.startswith("utm_") or lowered in _WEB_SOURCE_QUERY_KEYS_TO_IGNORE:
                continue
            filtered.append((token, str(value or "").strip()))
        if filtered:
            filtered.sort(key=lambda item: (item[0], item[1]))
            query = urlencode(filtered, doseq=True)

    return urlunparse((scheme, netloc, path, "", query, ""))


def _canonical_web_source_page(value: Any) -> str:
    return _canonical_web_source_url(value, drop_query=True)


def _tool_call_primary_web_url(tool_call: Optional[Dict[str, Any]]) -> str:
    if not isinstance(tool_call, dict):
        return ""
    func = _canonical_tool_name(str(tool_call.get("function") or "").strip())
    if func not in _WEB_SOURCE_TOOL_IDS:
        return ""
    args = tool_call.get("arguments") if isinstance(tool_call.get("arguments"), dict) else {}
    for key in ("url", "source_url", "link"):
        normalized = _canonical_web_source_url(args.get(key))
        if normalized:
            return normalized
    return ""


def _hydra_role_llm_key(role: str, field: str) -> str:
    return f"{HYDRA_ROLE_LLM_KEY_PREFIX}{str(role or '').strip()}:{str(field or '').strip()}"


def _task_name_from_text(text: Any, *, fallback: str = "Hydra task") -> str:
    raw = str(text or "").strip()
    if not raw:
        return fallback
    cleaned = re.sub(r"^\s*(?:please\s+)?(?:can you|could you|would you|will you)\s+", "", raw, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    if not cleaned:
        return fallback
    if len(cleaned) > 72:
        cleaned = cleaned[:69].rstrip() + "..."
    return cleaned


def _build_hydra_llm_endpoint(host: Any, port: Any) -> str:
    raw_host = str(host or "").strip()
    raw_port = str(port or "").strip()
    if not raw_host:
        return ""

    candidate = raw_host if raw_host.startswith(("http://", "https://")) else f"http://{raw_host}"
    parsed = urlparse(candidate)
    hostname = str(parsed.hostname or "").strip()
    if not hostname:
        return ""

    resolved_port = raw_port or (str(parsed.port) if parsed.port is not None else "")
    if resolved_port:
        try:
            port_int = int(str(resolved_port).strip())
        except Exception:
            return ""
        if port_int < 1 or port_int > 65535:
            return ""
        netloc = f"{hostname}:{port_int}"
    else:
        netloc = hostname

    return urlunparse((parsed.scheme or "http", netloc, "", "", "", "")).rstrip("/")


class _HydraLLMClientPool:
    def __init__(self, *, role_clients: Dict[str, Any], owned_clients: List[Any], beast_mode_enabled: bool) -> None:
        self.role_clients = dict(role_clients or {})
        self._owned_clients = list(owned_clients or [])
        self.beast_mode_enabled = bool(beast_mode_enabled)

    def client_for(self, role: str, default: Any) -> Any:
        client = self.role_clients.get(str(role or "").strip())
        return client if client is not None else default

    async def aclose(self) -> None:
        seen: set[int] = set()
        for client in self._owned_clients:
            ident = id(client)
            if ident in seen:
                continue
            seen.add(ident)
            close_fn = getattr(client, "aclose", None)
            if callable(close_fn):
                try:
                    await close_fn()
                except Exception:
                    pass


async def _build_hydra_llm_client_pool(
    *,
    base_llm_client: Any,
    redis_client: Any,
) -> _HydraLLMClientPool:
    role_clients: Dict[str, Any] = {role: base_llm_client for role in HYDRA_BEAST_ROLE_IDS}
    owned_clients: List[Any] = []
    r = redis_client or default_redis
    def _safe_get_text(key: str) -> str:
        try:
            return str(r.get(key) or "").strip() if r is not None else ""
        except Exception:
            return ""

    beast_mode_enabled = _status_bool(_safe_get_text(HYDRA_BEAST_MODE_ENABLED_KEY), default=False)
    if not beast_mode_enabled:
        return _HydraLLMClientPool(
            role_clients=role_clients,
            owned_clients=owned_clients,
            beast_mode_enabled=False,
        )
    missing_or_invalid_roles: List[str] = []

    base_client_host = str(getattr(base_llm_client, "host", "") or "").rstrip("/")
    base_client_model = str(getattr(base_llm_client, "model", "") or "").strip()
    shared_clients: Dict[tuple[str, str], Any] = {}

    for role in HYDRA_BEAST_CONFIG_ROLE_IDS:
        raw_host = _safe_get_text(_hydra_role_llm_key(role, "host"))
        raw_port = _safe_get_text(_hydra_role_llm_key(role, "port"))
        raw_model = _safe_get_text(_hydra_role_llm_key(role, "model"))
        endpoint = _build_hydra_llm_endpoint(raw_host, raw_port)
        if not endpoint or not raw_model:
            missing_or_invalid_roles.append(role)
            continue

        endpoint_key = endpoint.rstrip("/")
        base_host_no_v1 = base_client_host[:-3].rstrip("/") if base_client_host.endswith("/v1") else base_client_host
        if (
            base_client_model
            and raw_model == base_client_model
            and endpoint_key in {base_client_host, base_host_no_v1}
        ):
            role_clients[role] = base_llm_client
            continue

        signature = (endpoint_key, str(raw_model).strip())
        existing = shared_clients.get(signature)
        if existing is not None:
            role_clients[role] = existing
            continue

        try:
            client = get_llm_client_from_env(host=endpoint, model=raw_model)
        except Exception:
            missing_or_invalid_roles.append(role)
            continue

        shared_clients[signature] = client
        owned_clients.append(client)
        role_clients[role] = client

    if missing_or_invalid_roles:
        raise RuntimeError(
            "Hydra LLM is not configured. Open Settings > Hydra and set Hydra LLM Host/IP, Port, and Model."
        )

    return _HydraLLMClientPool(
        role_clients=role_clients,
        owned_clients=owned_clients,
        beast_mode_enabled=True,
    )


def _register_active_chat_job(
    *,
    platform: str,
    scope: str,
    origin: Optional[Dict[str, Any]],
    task_name: str = "",
) -> str:
    job_id = str(uuid.uuid4())
    platform_value = normalize_platform(platform)
    scope_value = str(scope or "").strip()
    origin_payload = dict(origin) if isinstance(origin, dict) else {}
    source_value = str(
        origin_payload.get("platform")
        or origin_payload.get("source")
        or platform_value
    ).strip()
    task_name_value = _task_name_from_text(task_name, fallback="Hydra task")
    with _ACTIVE_CHAT_JOB_LOCK:
        _ACTIVE_CHAT_JOBS[job_id] = {
            "id": job_id,
            "platform": platform_value,
            "scope": scope_value,
            "source": source_value,
            "task_name": task_name_value,
            "current_tool": "",
            "started_at": time.time(),
        }
    return job_id


def _unregister_active_chat_job(job_id: str) -> None:
    with _ACTIVE_CHAT_JOB_LOCK:
        _ACTIVE_CHAT_JOBS.pop(str(job_id or "").strip(), None)


def _set_active_chat_job_task_name(job_id: str, task_name: Any) -> None:
    token = str(job_id or "").strip()
    if not token:
        return
    value = _task_name_from_text(task_name, fallback="")
    if not value:
        return
    with _ACTIVE_CHAT_JOB_LOCK:
        row = _ACTIVE_CHAT_JOBS.get(token)
        if isinstance(row, dict):
            row["task_name"] = value


def _set_active_chat_job_current_tool(job_id: str, tool_name: Any) -> None:
    token = str(job_id or "").strip()
    if not token:
        return
    value = _short_text(" ".join(str(tool_name or "").split()), limit=96)
    with _ACTIVE_CHAT_JOB_LOCK:
        row = _ACTIVE_CHAT_JOBS.get(token)
        if isinstance(row, dict):
            row["current_tool"] = value


def _active_chat_job_tool_label(*, tool_call: Optional[Dict[str, Any]], registry: Dict[str, Any]) -> str:
    if not isinstance(tool_call, dict):
        return ""
    canonical_name = _canonical_tool_name(str(tool_call.get("function") or "").strip())
    if not canonical_name:
        return ""
    plugin = (registry or {}).get(canonical_name) if isinstance(registry, dict) else None
    if plugin is not None:
        pretty = str(verba_display_name(plugin) or "").strip()
        if pretty:
            return _short_text(pretty, limit=96)
    return _short_text(canonical_name, limit=96)


def get_active_chat_jobs_count(*, platform: Optional[str] = None) -> int:
    platform_filter = normalize_platform(platform) if str(platform or "").strip() else ""
    with _ACTIVE_CHAT_JOB_LOCK:
        if not platform_filter:
            return len(_ACTIVE_CHAT_JOBS)
        count = 0
        for row in _ACTIVE_CHAT_JOBS.values():
            if normalize_platform(row.get("platform")) == platform_filter:
                count += 1
        return count


def get_active_chat_jobs_snapshot(*, platform: Optional[str] = None) -> List[Dict[str, Any]]:
    platform_filter = normalize_platform(platform) if str(platform or "").strip() else ""
    with _ACTIVE_CHAT_JOB_LOCK:
        rows = [dict(row) for row in _ACTIVE_CHAT_JOBS.values() if isinstance(row, dict)]
    if platform_filter:
        rows = [row for row in rows if normalize_platform(row.get("platform")) == platform_filter]
    rows.sort(key=lambda row: float(row.get("started_at") or 0.0))
    return rows


def _normalize_tool_call_for_user_request(
    *,
    tool_call: Dict[str, Any],
    registry: Dict[str, Any],
    user_text: str,
) -> Dict[str, Any]:
    return toolcall_utils.normalize_tool_call_for_user_request(
        tool_call=tool_call,
        registry=registry,
        user_text=user_text,
        canonical_tool_name_fn=_canonical_tool_name,
        parse_function_json_fn=parse_function_json,
    )


_SEND_MESSAGE_TARGET_KEYS: tuple[str, ...] = (
    "channel_id",
    "channel",
    "guild_id",
    "room_id",
    "room_alias",
    "chat_id",
    "scope",
    "device_id",
)

_SEND_MESSAGE_PLATFORM_ALIASES: Dict[str, str] = {
    "discord": "discord",
    "irc": "irc",
    "matrix": "matrix",
    "telegram": "telegram",
    "homeassistant": "homeassistant",
    "home assistant": "homeassistant",
    "ntfy": "ntfy",
    "webui": "webui",
    "web ui": "webui",
    "macos": "macos",
    "mac os": "macos",
    "my mac": "macos",
}


def _send_message_targets_from_args(args: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(args, dict):
        return out
    nested = args.get("targets") if isinstance(args.get("targets"), dict) else {}
    for key in _SEND_MESSAGE_TARGET_KEYS:
        nested_value = str((nested or {}).get(key) or "").strip()
        if nested_value:
            out[key] = nested_value
            continue
        direct_value = str(args.get(key) or "").strip()
        if direct_value:
            out[key] = direct_value
    return out


def _send_message_normalize_platform_alias(value: Any) -> str:
    token = str(value or "").strip().lower()
    if not token:
        return ""
    token = " ".join(token.replace("_", " ").replace("-", " ").split())
    return _SEND_MESSAGE_PLATFORM_ALIASES.get(token, token)


def _send_message_normalize_targets_payload(payload: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(payload, dict):
        return out
    for key in _SEND_MESSAGE_TARGET_KEYS:
        text = str(payload.get(key) or "").strip()
        if text:
            out[key] = text
    return out


def _delivery_origin_defaults(origin: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = origin if isinstance(origin, dict) else {}
    platform_token = _send_message_normalize_platform_alias(
        normalize_platform(base.get("platform")) or base.get("platform")
    )
    targets = _send_message_normalize_targets_payload(base)
    return {
        "platform": platform_token,
        "targets": targets,
    }


def _delivery_catalog_snapshot(*, limit_per_platform: int = 8) -> List[Dict[str, Any]]:
    try:
        payload = notifier_destination_catalog(
            redis_client=default_redis,
            platform=None,
            limit=max(1, min(80, int(limit_per_platform))),
        )
    except Exception:
        payload = {}

    rows = payload.get("platforms") if isinstance(payload.get("platforms"), list) else []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        platform_token = _send_message_normalize_platform_alias(row.get("platform"))
        if not platform_token:
            continue
        entry: Dict[str, Any] = {
            "platform": platform_token,
            "requires_target": bool(row.get("requires_target")),
            "destinations": [],
        }
        destinations = row.get("destinations") if isinstance(row.get("destinations"), list) else []
        compact_destinations: List[Dict[str, Any]] = []
        for item in destinations[: max(1, min(16, int(limit_per_platform)))]:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or "").strip()
            targets = _send_message_normalize_targets_payload(item.get("targets"))
            if not label and not targets:
                continue
            compact: Dict[str, Any] = {}
            if label:
                compact["label"] = label
            if targets:
                compact["targets"] = targets
            compact_destinations.append(compact)
        entry["destinations"] = compact_destinations
        out.append(entry)
    return out


def _delivery_resolution_from_llm_payload(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    out: Dict[str, Any] = {}
    platform_token = _send_message_normalize_platform_alias(payload.get("platform"))
    if platform_token:
        out["platform"] = platform_token
    targets = _send_message_normalize_targets_payload(payload.get("targets"))
    if targets:
        out["targets"] = targets
    message = str(payload.get("message") or payload.get("content") or "").strip()
    if message:
        out["message"] = message
    return out


async def _llm_resolve_delivery_intent(
    *,
    llm_client: Any,
    func: str,
    user_text: str,
    args: Dict[str, Any],
    origin: Optional[Dict[str, Any]],
    platform: str,
    platform_preamble: str,
    max_tokens: Optional[int],
) -> Dict[str, Any]:
    if llm_client is None or not hasattr(llm_client, "chat"):
        return {}

    payload = {
        "tool_function": str(func or "").strip(),
        "current_platform": str(platform or "").strip(),
        "user_message": str(user_text or "").strip(),
        "current_arguments": dict(args or {}),
        "origin_defaults": _delivery_origin_defaults(origin),
        "known_destinations": _delivery_catalog_snapshot(limit_per_platform=8),
    }
    system_prompt = (
        "You resolve delivery routing for send_message and attach_file.\n"
        "Return exactly ONE strict JSON object with this shape:\n"
        "{\"platform\":\"\",\"targets\":{},\"message\":\"\"}\n"
        "Rules:\n"
        "- No prose, no markdown, no code fences.\n"
        "- Use platform only when the user clearly specifies a destination platform/room.\n"
        "- If destination is unclear, keep platform empty and targets empty.\n"
        "- Do NOT treat product names as destinations unless explicitly requested as destination.\n"
        "- Use known destination targets when possible; never invent opaque IDs.\n"
        "- For send_message, message should be the content to deliver (without routing words).\n"
        "- For attach_file, message may be empty unless a caption/note is clearly requested."
    )
    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    messages = _with_platform_preamble(messages, platform_preamble=platform_preamble)
    try:
        resp = await llm_client.chat(
            messages=messages,
            temperature=0.0,
            **_chat_with_optional_max_tokens_kwargs(
                max_tokens=max_tokens,
                minimum=120,
                fallback=320,
                maximum=700,
            ),
        )
    except Exception:
        return {}

    raw = str((resp.get("message", {}) or {}).get("content", "")).strip()
    parsed = _first_json_object(raw)
    return _delivery_resolution_from_llm_payload(parsed)

async def _llm_enrich_tool_call_for_user_request(
    *,
    llm_client: Any,
    tool_call: Dict[str, Any],
    user_text: str,
    platform: str,
    origin: Optional[Dict[str, Any]] = None,
    scope: str = "",
    history_messages: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    del scope, history_messages, context

    if not isinstance(tool_call, dict):
        return tool_call
    func = _canonical_tool_name(str(tool_call.get("function") or "").strip())
    if func not in {"attach_file", "send_message"}:
        return tool_call

    args = dict(tool_call.get("arguments") or {}) if isinstance(tool_call.get("arguments"), dict) else {}
    merged = dict(args or {})
    existing_platform_raw = str(merged.get("platform") or "").strip()
    if existing_platform_raw:
        merged["platform"] = _send_message_normalize_platform_alias(
            normalize_platform(existing_platform_raw) or existing_platform_raw
        )

    origin_defaults = _delivery_origin_defaults(origin)
    llm_resolution = await _llm_resolve_delivery_intent(
        llm_client=llm_client,
        func=func,
        user_text=str(user_text or ""),
        args=merged,
        origin=origin,
        platform=platform,
        platform_preamble=platform_preamble,
        max_tokens=max_tokens,
    )

    existing_targets = _send_message_targets_from_args(merged)
    existing_platform = str(merged.get("platform") or "").strip()
    llm_platform = str(llm_resolution.get("platform") or "").strip()
    origin_platform = str(origin_defaults.get("platform") or "").strip()

    resolved_platform = ""
    if llm_platform:
        resolved_platform = llm_platform
    elif existing_platform and existing_targets:
        resolved_platform = existing_platform
    elif origin_platform:
        resolved_platform = origin_platform
    elif existing_platform:
        resolved_platform = existing_platform

    if resolved_platform:
        merged["platform"] = resolved_platform
    else:
        merged.pop("platform", None)

    merged_targets = _send_message_targets_from_args(merged)
    if not merged_targets:
        merged_targets = _send_message_normalize_targets_payload(llm_resolution.get("targets"))
    if not merged_targets and resolved_platform and resolved_platform == origin_platform:
        merged_targets = _send_message_normalize_targets_payload(origin_defaults.get("targets"))

    for key in _SEND_MESSAGE_TARGET_KEYS:
        merged.pop(key, None)
    if merged_targets:
        merged["targets"] = merged_targets
    else:
        merged.pop("targets", None)

    llm_message = str(llm_resolution.get("message") or "").strip()
    existing_message = str(merged.get("message") or merged.get("content") or "").strip()
    if func == "send_message":
        if llm_message and (not existing_message or existing_message == str(user_text or "").strip()):
            merged["message"] = llm_message
        elif not existing_message:
            merged["message"] = llm_message or str(user_text or "").strip()
    else:
        has_delivery_intent = bool(str(merged.get("platform") or "").strip() or merged_targets)
        if llm_message and not existing_message:
            merged["message"] = llm_message
        if has_delivery_intent and not str(merged.get("message") or merged.get("content") or "").strip():
            merged["message"] = "Attachment"

    return {"function": func, "arguments": merged}


def _plugin_tool_id_for_call(tool_call: Optional[Dict[str, Any]], registry: Dict[str, Any]) -> str:
    return toolcall_utils.plugin_tool_id_for_call(
        tool_call,
        registry,
        canonical_tool_name_fn=_canonical_tool_name,
        is_meta_tool_fn=is_meta_tool,
    )


def _normalize_abs_path(value: Any) -> str:
    return runtime_config.normalize_abs_path(value)


def _redis_config_non_negative_int(
    key: str,
    default: int,
    *,
    redis_client: Any = None,
) -> int:
    return runtime_config.redis_config_non_negative_int(
        key,
        default,
        redis_client=(redis_client or default_redis),
        coerce_non_negative_int_fn=_coerce_non_negative_int,
    )


def _redis_config_positive_int(
    key: str,
    default: int,
    *,
    redis_client: Any = None,
) -> int:
    return runtime_config.redis_config_positive_int(
        key,
        default,
        redis_client=(redis_client or default_redis),
        redis_config_non_negative_int_fn=_redis_config_non_negative_int,
    )


def _redis_config_bool(
    key: str,
    default: bool,
    *,
    redis_client: Any = None,
) -> bool:
    try:
        raw = (redis_client or default_redis).get(key)
    except Exception:
        return bool(default)
    if raw is None:
        return bool(default)
    token = str(raw).strip().lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _configured_max_ledger_items(redis_client: Any = None) -> int:
    return runtime_config.configured_positive_int(
        redis_client=(redis_client or default_redis),
        key=HYDRA_MAX_LEDGER_ITEMS_KEY,
        default=DEFAULT_MAX_LEDGER_ITEMS,
        redis_config_positive_int_fn=_redis_config_positive_int,
    )


def _configured_chat_history_max_items(redis_client: Any = None) -> int:
    value = _redis_config_non_negative_int(
        CHAT_HISTORY_MAX_ITEMS_KEY,
        0,
        redis_client=(redis_client or default_redis),
    )
    return max(0, min(200, int(value)))


def _configured_step_retry_limit(redis_client: Any = None) -> int:
    global STEP_RETRY_LIMIT
    value = runtime_config.configured_positive_int(
        redis_client=(redis_client or default_redis),
        key=HYDRA_STEP_RETRY_LIMIT_KEY,
        default=DEFAULT_STEP_RETRY_LIMIT,
        redis_config_positive_int_fn=_redis_config_positive_int,
    )
    STEP_RETRY_LIMIT = max(1, min(10, int(value)))
    return STEP_RETRY_LIMIT


def _configured_astraeus_plan_review_enabled(redis_client: Any = None) -> bool:
    global ASTRAEUS_PLAN_REVIEW_ENABLED
    ASTRAEUS_PLAN_REVIEW_ENABLED = _redis_config_bool(
        HYDRA_ASTRAEUS_PLAN_REVIEW_ENABLED_KEY,
        DEFAULT_ASTRAEUS_PLAN_REVIEW_ENABLED,
        redis_client=(redis_client or default_redis),
    )
    return bool(ASTRAEUS_PLAN_REVIEW_ENABLED)


def _normalize_token_limit(
    value: Optional[int],
    *,
    minimum: int = 1,
    fallback: Optional[int] = None,
    maximum: Optional[int] = None,
) -> Optional[int]:
    if value is None:
        return None
    try:
        parsed = int(value)
    except Exception:
        parsed = int(fallback if fallback is not None else minimum)
    if parsed <= 0 and fallback is not None:
        parsed = int(fallback)
    parsed = max(int(minimum), parsed)
    if maximum is not None:
        parsed = min(int(maximum), parsed)
    return parsed

def _chat_with_optional_max_tokens_kwargs(
    *,
    max_tokens: Optional[int],
    minimum: int = 1,
    fallback: Optional[int] = None,
    maximum: Optional[int] = None,
) -> Dict[str, Any]:
    token_limit = _normalize_token_limit(
        max_tokens,
        minimum=minimum,
        fallback=fallback,
        maximum=maximum,
    )
    if token_limit is None:
        return {"max_tokens": None}
    return {"max_tokens": int(token_limit)}


def _configured_result_memory_max_sets(redis_client: Any = None) -> int:
    global RESULT_MEMORY_MAX_SETS
    value = runtime_config.configured_positive_int(
        redis_client=(redis_client or default_redis),
        key=HYDRA_RESULT_MEMORY_MAX_SETS_KEY,
        default=DEFAULT_RESULT_MEMORY_MAX_SETS,
        redis_config_positive_int_fn=_redis_config_positive_int,
    )
    RESULT_MEMORY_MAX_SETS = max(1, min(24, int(value)))
    return RESULT_MEMORY_MAX_SETS


def _configured_result_memory_max_items(redis_client: Any = None) -> int:
    global RESULT_MEMORY_MAX_ITEMS
    value = runtime_config.configured_positive_int(
        redis_client=(redis_client or default_redis),
        key=HYDRA_RESULT_MEMORY_MAX_ITEMS_KEY,
        default=DEFAULT_RESULT_MEMORY_MAX_ITEMS,
        redis_config_positive_int_fn=_redis_config_positive_int,
    )
    RESULT_MEMORY_MAX_ITEMS = max(1, min(16, int(value)))
    return RESULT_MEMORY_MAX_ITEMS


def _coerce_text(content: Any) -> str:
    return common_helpers.coerce_text(content)


def _contains_tool_json_pattern(text: str) -> bool:
    return preamble_utils.contains_tool_json_pattern(text)


def _sanitize_platform_preamble(platform: str, platform_preamble: Any) -> str:
    return preamble_utils.sanitize_platform_preamble(
        platform,
        platform_preamble,
        coerce_text_fn=_coerce_text,
        ascii_only_platforms=ASCII_ONLY_PLATFORMS,
        looks_like_tool_markup_fn=looks_like_tool_markup,
        parse_strict_tool_json_fn=_parse_strict_tool_json,
        parse_function_json_fn=parse_function_json,
        contains_tool_json_pattern_fn=_contains_tool_json_pattern,
    )


def _with_platform_preamble(
    messages: List[Dict[str, Any]],
    *,
    platform_preamble: str,
) -> List[Dict[str, Any]]:
    return preamble_utils.with_platform_preamble(
        messages,
        platform_preamble=platform_preamble,
    )


def _clean_scope_text(value: Any) -> str:
    return scope_helpers.clean_scope_text(
        value,
        coerce_text_fn=_coerce_text,
        short_text_fn=_short_text,
    )


def _scope_is_generic(scope: str) -> bool:
    return scope_helpers.scope_is_generic(scope)


def _unknown_scope(platform: str, origin: Optional[Dict[str, Any]]) -> str:
    return scope_helpers.unknown_scope(
        platform,
        origin,
        normalize_platform_fn=normalize_platform,
    )


def _derive_scope_from_origin(platform: str, origin: Optional[Dict[str, Any]]) -> str:
    return scope_helpers.derive_scope_from_origin(
        platform,
        origin,
        normalize_platform_fn=normalize_platform,
        clean_scope_text_fn=_clean_scope_text,
        scope_is_generic_fn=_scope_is_generic,
        unknown_scope_fn=_unknown_scope,
    )


def _resolve_hydra_scope(platform: str, scope: Any, origin: Optional[Dict[str, Any]]) -> str:
    return scope_helpers.resolve_hydra_scope(
        platform,
        scope,
        origin,
        normalize_platform_fn=normalize_platform,
        clean_scope_text_fn=_clean_scope_text,
        scope_is_generic_fn=_scope_is_generic,
        derive_scope_from_origin_fn=_derive_scope_from_origin,
    )


def _memory_context_payload(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
    origin: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return get_hydra_memory_context_payload(
        platform=platform,
        scope=scope,
        origin=origin,
        redis_client=redis_client,
    )


def _core_system_prompt_fragments(
    *,
    role: str,
    platform: str,
    scope: str,
    origin: Optional[Dict[str, Any]],
    redis_client: Any,
    memory_context_payload: Optional[Dict[str, Any]],
) -> List[str]:
    fragments = collect_hydra_system_prompt_fragments(
        role=role,
        platform=platform,
        scope=scope,
        origin=origin,
        redis_client=redis_client,
        memory_context=memory_context_payload,
    )
    out: List[str] = []
    seen: set[str] = set()
    for item in fragments:
        text = _coerce_text(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _core_system_prompt_message(
    *,
    role: str,
    platform: str,
    scope: str,
    origin: Optional[Dict[str, Any]],
    redis_client: Any,
    memory_context_payload: Optional[Dict[str, Any]],
) -> str:
    merged: List[str] = []
    for fragment in _core_system_prompt_fragments(
        role=role,
        platform=platform,
        scope=scope,
        origin=origin,
        redis_client=redis_client,
        memory_context_payload=memory_context_payload,
    ):
        merged.append(fragment)
    if role != "memory_context":
        for fragment in _core_system_prompt_fragments(
            role="memory_context",
            platform=platform,
            scope=scope,
            origin=origin,
            redis_client=redis_client,
            memory_context_payload=memory_context_payload,
        ):
            if fragment not in merged:
                merged.append(fragment)
    return "\n\n".join(merged).strip()


def _coerce_non_negative_int(value: Any, default: int) -> int:
    return limits_helpers.coerce_non_negative_int(value, default)


def resolve_agent_limits(
    redis_client: Any = None,
    *,
    max_rounds: Optional[int] = None,
    max_tool_calls: Optional[int] = None,
) -> tuple[int, int]:
    del redis_client, max_rounds, max_tool_calls
    return 0, 0


def _canonical_tool_name(name: str) -> str:
    return toolcall_utils.canonical_tool_name(
        name,
        tool_name_aliases=TOOL_NAME_ALIASES,
    )


def _looks_like_invalid_tool_call_text(text: str) -> bool:
    return toolcall_utils.looks_like_invalid_tool_call_text(text)


def _tool_purpose(plugin: Any) -> str:
    return tool_index_helpers.tool_purpose(
        plugin,
        plugin_when_to_use_fn=verba_when_to_use,
    )


def _plugin_usage_text(plugin: Any) -> str:
    usage = str(getattr(plugin, "usage", "") or "").strip()
    if usage:
        return " ".join(usage.split())
    plugin_id = str(getattr(plugin, "name", "") or "").strip()
    if plugin_id:
        return f'{{"function":"{plugin_id}","arguments":{{}}}}'
    return '{"function":"","arguments":{}}'


def _kernel_tool_purpose(tool_id: str, *, platform: str = "") -> str:
    key = str(tool_id or "").strip()
    if not key:
        return "kernel tool"
    direct = str(_KERNEL_TOOL_PURPOSE_HINTS.get(key) or "").strip()
    if direct:
        return direct
    dynamic = runtime_kernel_tool_purpose_hint(
        tool_id=key,
        platform=platform,
    )
    if dynamic:
        return str(dynamic).strip()
    return tool_index_helpers.kernel_tool_purpose(
        key,
        kernel_tool_purpose_hints={},
    )


def _kernel_tool_usage(tool_id: str, *, platform: str = "") -> str:
    key = str(tool_id or "").strip()
    usage = str(_KERNEL_TOOL_USAGE_HINTS.get(key) or "").strip()
    if usage:
        return usage
    dynamic = runtime_kernel_tool_usage_hint(
        tool_id=key,
        platform=platform,
    )
    if dynamic:
        return str(dynamic).strip()
    if key:
        return f'{{"function":"{key}","arguments":{{}}}}'
    return '{"function":"","arguments":{}}'


def _ordered_kernel_tool_ids(*, platform: str) -> List[str]:
    normalized = normalize_platform(platform) or str(platform or "").strip().lower() or "webui"
    return sorted(runtime_kernel_tool_ids(platform=normalized))


def _enabled_tool_mini_index(
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> str:
    ordered_ids_fn = lambda: _ordered_kernel_tool_ids(platform=platform)
    kernel_purpose_fn = lambda tool_id: _kernel_tool_purpose(tool_id, platform=platform)
    kernel_usage_fn = lambda tool_id: _kernel_tool_usage(tool_id, platform=platform)
    return tool_index_helpers.enabled_tool_mini_index(
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
        ordered_kernel_tool_ids_fn=ordered_ids_fn,
        kernel_tool_purpose_fn=kernel_purpose_fn,
        kernel_tool_usage_fn=kernel_usage_fn,
        plugin_supports_platform_fn=verba_supports_platform,
        plugin_usage_text_fn=_plugin_usage_text,
        tool_purpose_fn=_tool_purpose,
    )


def _enabled_execution_tool_ids(
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> set[str]:
    enabled_check = enabled_predicate or (lambda _name: True)
    out: set[str] = set()
    for tool_id in _ordered_kernel_tool_ids(platform=platform):
        canonical = _canonical_tool_name(tool_id)
        if canonical:
            out.add(canonical)
    for plugin_id, plugin in sorted((registry or {}).items(), key=lambda kv: str(kv[0]).lower()):
        raw_plugin_id = str(plugin_id or "").strip()
        canonical = _canonical_tool_name(raw_plugin_id)
        if not canonical:
            continue
        if not (enabled_check(canonical) or enabled_check(raw_plugin_id)):
            continue
        if not verba_supports_platform(plugin, platform):
            continue
        out.add(canonical)
    return out


def _astraeus_capability_catalog(
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> str:
    enabled_check = enabled_predicate or (lambda _name: True)
    kernel_rows: List[str] = []
    for tool_id in _ordered_kernel_tool_ids(platform=platform):
        canonical = _canonical_tool_name(tool_id)
        if not canonical:
            continue
        kernel_rows.append(
            f"- id: {canonical} | description: {_kernel_tool_purpose(canonical, platform=platform)}"
        )
    if not kernel_rows:
        kernel_rows.append("- (none)")

    verba_rows: List[str] = []
    for plugin_id, plugin in sorted((registry or {}).items(), key=lambda kv: str(kv[0]).lower()):
        raw_plugin_id = str(plugin_id or "").strip()
        canonical = _canonical_tool_name(raw_plugin_id)
        if not canonical:
            continue
        if not (enabled_check(canonical) or enabled_check(raw_plugin_id)):
            continue
        if not verba_supports_platform(plugin, platform):
            continue
        verba_rows.append(f"- id: {canonical} | description: {_tool_purpose(plugin)}")
    if not verba_rows:
        verba_rows.append("- (none)")

    return (
        "Available kernel tools (id | description):\n"
        + "\n".join(kernel_rows)
        + "\nAvailable enabled verba tools on this platform (id | description):\n"
        + "\n".join(verba_rows)
    )


def _tool_contract_row(
    *,
    tool_id: str,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> str:
    canonical = _canonical_tool_name(tool_id)
    if not canonical:
        return ""
    kernel_tool_ids = {_canonical_tool_name(item) for item in _ordered_kernel_tool_ids(platform=platform)}
    if canonical in kernel_tool_ids:
        return (
            f"- id: {canonical} | description: {_kernel_tool_purpose(canonical, platform=platform)} | "
            f"usage: {_kernel_tool_usage(canonical, platform=platform)}"
        )
    plugin = registry.get(canonical)
    if plugin is None:
        return ""
    enabled_check = enabled_predicate or (lambda _name: True)
    raw_tool_id = str(tool_id or "").strip()
    if not (enabled_check(canonical) or (raw_tool_id and enabled_check(raw_tool_id))):
        return ""
    if not verba_supports_platform(plugin, platform):
        return ""
    return (
        f"- id: {canonical} | description: {_tool_purpose(plugin)} | "
        f"usage: {_plugin_usage_text(plugin)}"
    )


def _thanatos_execution_tool_contract_prompt(
    *,
    current_plan_step: Optional[Dict[str, Any]],
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    fallback_tool_index: str,
) -> str:
    step = current_plan_step if isinstance(current_plan_step, dict) else {}
    hinted_tool = _canonical_tool_name(str(step.get("tool_hint") or "").strip())
    if hinted_tool:
        contract_row = _tool_contract_row(
            tool_id=hinted_tool,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
        )
        if contract_row:
            return (
                "Execution tool contract for this step:\n"
                f"{contract_row}\n"
                "Use this tool contract directly for tool id and argument shape."
            )
    if fallback_tool_index:
        return (
            "Execution contract missing for this step (no valid tool_hint resolved).\n"
            "Do not choose from global catalogs.\n"
            "Output a short blocker explanation so Astraeus can replan the step."
        )
    return ""


def _compact_history(history_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return common_helpers.compact_history(
        history_messages,
        coerce_text_fn=_coerce_text,
    )


def _chat_history_window(history_messages: List[Dict[str, Any]], *, max_items: int = 0) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for msg in (history_messages or []):
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _short_text(_coerce_text(msg.get("content")), limit=260)
        if not content:
            continue
        out.append({"role": role, "content": content})
    if max_items > 0 and len(out) > max_items:
        return out[-max_items:]
    return out


def _platform_label(platform: str) -> str:
    return common_helpers.platform_label(
        platform,
        platform_display_map=_PLATFORM_DISPLAY,
    )


def _strip_user_sender_prefix(text: str) -> str:
    return common_helpers.strip_user_sender_prefix(text)


_DESTINATION_CONTAINER_KEYS = {
    "destination",
    "destinations",
    "target",
    "targets",
    "to",
    "route",
    "routes",
    "recipient",
    "recipients",
}

_DESTINATION_VALUE_KEYS = {
    "channel",
    "channel_id",
    "thread",
    "thread_id",
    "room",
    "room_id",
    "chat",
    "chat_id",
    "target",
    "user_id",
}

_NON_DESTINATION_ID_KEYS = {
    "request_id",
    "session_id",
    "turn_id",
    "conversation_id",
    "args_hash",
    "tool_args_hash",
    "state_hash",
}


def _looks_like_hash_identifier(value: str) -> bool:
    token = str(value or "").strip().lower()
    if not token:
        return False
    if token.startswith(("sha1:", "sha224:", "sha256:", "sha384:", "sha512:", "md5:")):
        return True
    if len(token) >= 24 and re.fullmatch(r"[0-9a-f]{24,}", token):
        return True
    return False


def _looks_like_destination_scalar(value: Any, *, key_hint: str, in_destination_context: bool) -> bool:
    token = str(value or "").strip()
    if not token:
        return False
    if _looks_like_hash_identifier(token):
        return False

    key = str(key_hint or "").strip().lower()
    key_is_destination = key in _DESTINATION_VALUE_KEYS or key.endswith("_channel") or key.endswith("_room")
    if not key_is_destination and not in_destination_context:
        return False

    if key == "platform":
        return False

    if token.startswith("#") and len(token) > 1:
        return True
    if token.startswith("!") and ":" in token:
        return True
    if re.fullmatch(r"[0-9]{3,}", token):
        return True
    if key_is_destination and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:\-]{1,127}", token):
        return True

    return False


def _find_concrete_destination(payload: Any, *, _in_destination_context: bool = False, _key_hint: str = "") -> bool:
    if isinstance(payload, dict):
        for raw_key, value in payload.items():
            key = str(raw_key or "").strip().lower()
            if key in _NON_DESTINATION_ID_KEYS:
                continue
            next_ctx = _in_destination_context or key in _DESTINATION_CONTAINER_KEYS
            if isinstance(value, (dict, list, tuple, set)):
                if _find_concrete_destination(value, _in_destination_context=next_ctx, _key_hint=key):
                    return True
                continue
            if _looks_like_destination_scalar(value, key_hint=key, in_destination_context=next_ctx):
                return True
        return False

    if isinstance(payload, (list, tuple, set)):
        for item in payload:
            if _find_concrete_destination(item, _in_destination_context=_in_destination_context, _key_hint=_key_hint):
                return True
        return False

    return _looks_like_destination_scalar(payload, key_hint=_key_hint, in_destination_context=_in_destination_context)


def _thanatos_focus_prompt(*, current_user_text: str, turn_request_text: str) -> str:
    return prompts.thanatos_focus_prompt(
        current_user_text=current_user_text,
        turn_request_text=turn_request_text,
    )


def _thanatos_execution_step_prompt(
    *,
    intent: str,
    nl: str,
    goal: str = "",
    repair_hint: str = "",
    tool_hint: str = "",
    blocked_sources: Optional[List[str]] = None,
) -> str:
    return prompts.thanatos_execution_step_prompt(
        intent=intent,
        nl=nl,
        goal=goal,
        repair_hint=repair_hint,
        tool_hint=tool_hint,
        blocked_sources=(blocked_sources or []),
    )


def _thanatos_system_prompt(platform: str) -> str:
    return prompts.thanatos_system_prompt(
        platform=platform,
        now_text=datetime.now().strftime("%A, %B %d, %Y at %I:%M %p"),
        ascii_only_platforms=ASCII_ONLY_PLATFORMS,
    ).strip()


def _minos_system_prompt(platform: str, retry_allowed: bool) -> str:
    return prompts.minos_system_prompt(
        platform=platform,
        retry_allowed=retry_allowed,
        ascii_only_platforms=ASCII_ONLY_PLATFORMS,
    ).strip()


def _astraeus_system_prompt(platform: str) -> str:
    return prompts.astraeus_system_prompt(platform=platform)


def _chat_fallback_system_prompt(platform: str) -> str:
    first, last = get_tater_name()
    return prompts.chat_fallback_system_prompt(
        platform=platform,
        platform_label=_platform_label(platform),
        now_text=datetime.now().strftime("%A, %B %d, %Y at %I:%M %p"),
        first_name=first,
        last_name=last,
        personality=(get_tater_personality() or "").strip(),
        ascii_only_platforms=ASCII_ONLY_PLATFORMS,
    ).strip()


def _status_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "on", "enabled", "running", "connected"}:
        return True
    if token in {"0", "false", "no", "off", "disabled", "stopped", "disconnected"}:
        return False
    return bool(default)


def _status_desc(*values: Any, fallback: str = "") -> str:
    for value in values:
        text = _short_text(" ".join(_coerce_text(value).split()), limit=120)
        if text:
            return text
    return _short_text(fallback, limit=120)


def _display_name_from_key(key: str, *, suffix: str) -> str:
    token = str(key or "").strip()
    if suffix and token.endswith(suffix):
        token = token[: -len(suffix)]
    token = token.replace("_", " ").strip()
    return token or str(key or "").strip()


def _collect_verbas_status_rows(
    *,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    enabled_check = enabled_predicate or (lambda _name: True)
    for plugin_id, plugin in sorted((registry or {}).items(), key=lambda kv: str(kv[0]).lower()):
        pid = str(plugin_id or "").strip()
        if not pid:
            continue
        enabled = bool(enabled_check(pid))
        description = _status_desc(
            getattr(plugin, "description", ""),
            getattr(plugin, "verba_dec", ""),
            getattr(plugin, "when_to_use", ""),
            getattr(plugin, "usage", ""),
            fallback="verba capability",
        )
        rows.append(
            {
                "name": pid,
                "description": description or "verba capability",
                "enabled": enabled,
            }
        )
    return rows


def _collect_portals_status_rows(*, redis_client: Any, platform: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        import portal_registry as portal_registry_module

        portal_entries = list(portal_registry_module.get_portal_registry() or [])
    except Exception:
        portal_entries = []

    for entry in portal_entries:
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("key") or "").strip()
        if not key:
            continue
        running = False
        enabled_hint = _status_bool(entry.get("enabled"), default=True)
        if redis_client is not None:
            try:
                running = _status_bool(redis_client.get(f"{key}_running"), default=False)
            except Exception:
                running = False
        connected = running
        enabled = enabled_hint
        rows.append(
            {
                "name": _display_name_from_key(key, suffix="_portal"),
                "description": _status_desc(
                    entry.get("description"),
                    entry.get("summary"),
                    entry.get("category"),
                    entry.get("label"),
                    fallback=f"interact through {key}",
                ),
                "connected": connected,
                "enabled": enabled,
            }
        )

    if not rows and platform:
        rows.append(
            {
                "name": str(platform).strip(),
                "description": _status_desc(
                    _platform_label(platform),
                    fallback=f"interact through {platform}",
                ),
                "connected": True,
                "enabled": True,
            }
        )

    return rows


def _collect_cores_status_rows(*, redis_client: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        import core_registry as core_registry_module

        core_entries = list(core_registry_module.get_core_registry() or [])
    except Exception:
        core_entries = []

    for entry in core_entries:
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("key") or "").strip()
        if not key:
            continue
        running = False
        enabled_hint = _status_bool(entry.get("enabled"), default=True)
        if redis_client is not None:
            try:
                running = _status_bool(redis_client.get(f"{key}_running"), default=False)
            except Exception:
                running = False
        rows.append(
            {
                "name": _display_name_from_key(key, suffix="_core"),
                "description": _status_desc(
                    entry.get("description"),
                    entry.get("summary"),
                    entry.get("category"),
                    entry.get("label"),
                    fallback=f"core system {key}",
                ),
                "running": running,
                "enabled": enabled_hint,
            }
        )

    return rows


def _collect_kernel_tools_status_rows(*, platform: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for tool_id in _ordered_kernel_tool_ids(platform=platform):
        token = str(tool_id or "").strip()
        if not token:
            continue
        rows.append(
            {
                "name": token,
                "description": _status_desc(
                    _kernel_tool_purpose(token, platform=platform),
                    fallback="kernel tool",
                ),
                "enabled": True,
            }
        )
    return rows


def _filter_status_rows(
    rows: List[Dict[str, Any]],
    *,
    active_key: str,
    compact_mode: bool,
    max_compact: int,
    max_full: int,
    inactive_tail: int = 2,
) -> List[Dict[str, Any]]:
    del compact_mode, max_compact, max_full, inactive_tail
    active = [row for row in rows if _status_bool(row.get(active_key), default=False)]
    inactive = [row for row in rows if not _status_bool(row.get(active_key), default=False)]
    active.sort(key=lambda row: str(row.get("name") or "").lower())
    inactive.sort(key=lambda row: str(row.get("name") or "").lower())
    return active + inactive


def _chat_status_compact_mode(
    *,
    history: List[Dict[str, Any]],
    max_tokens: Optional[int],
    total_rows: int,
) -> bool:
    del history, max_tokens, total_rows
    return False


def _render_tater_system_status_prompt(
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    redis_client: Any,
    history: List[Dict[str, Any]],
    max_tokens: Optional[int],
) -> str:
    verbas = _collect_verbas_status_rows(registry=registry, enabled_predicate=enabled_predicate)
    kernel_tools = _collect_kernel_tools_status_rows(platform=platform)
    portals = _collect_portals_status_rows(redis_client=redis_client, platform=platform)
    cores = _collect_cores_status_rows(redis_client=redis_client)
    compact_mode = _chat_status_compact_mode(
        history=history,
        max_tokens=max_tokens,
        total_rows=len(verbas) + len(kernel_tools) + len(portals) + len(cores),
    )
    verbas_rows = _filter_status_rows(
        verbas,
        active_key="enabled",
        compact_mode=compact_mode,
        max_compact=6,
        max_full=12,
        inactive_tail=3,
    )
    kernel_tool_rows = _filter_status_rows(
        kernel_tools,
        active_key="enabled",
        compact_mode=compact_mode,
        max_compact=10,
        max_full=30,
        inactive_tail=0,
    )
    portals_rows = _filter_status_rows(
        portals,
        active_key="connected",
        compact_mode=compact_mode,
        max_compact=6,
        max_full=10,
        inactive_tail=2,
    )
    cores_rows = _filter_status_rows(
        cores,
        active_key="running",
        compact_mode=compact_mode,
        max_compact=6,
        max_full=8,
        inactive_tail=2,
    )

    lines: List[str] = []
    lines.append("Tater System Status")
    lines.append("")
    lines.append("Verba (Capabilities)")
    lines.append("These are the Verba tools you currently have available for reference.")
    if verbas_rows:
        for row in verbas_rows:
            status_text = "enabled" if _status_bool(row.get("enabled"), default=False) else "disabled"
            lines.append(f"- {row.get('name')}: {row.get('description')} ({status_text})")
    else:
        lines.append("- none detected")

    lines.append("")
    lines.append("Kernel Tools (Built-ins)")
    lines.append("These are kernel tools currently available for direct execution.")
    if kernel_tool_rows:
        for row in kernel_tool_rows:
            lines.append(f"- {row.get('name')}: {row.get('description')}")
    else:
        lines.append("- none detected")

    lines.append("")
    lines.append("Portals (Platforms)")
    lines.append("These are the Portals you are currently running or connected through.")
    if portals_rows:
        for row in portals_rows:
            connected_text = "connected" if _status_bool(row.get("connected"), default=False) else "disconnected"
            enabled_text = "enabled" if _status_bool(row.get("enabled"), default=False) else "disabled"
            lines.append(f"- {row.get('name')}: {row.get('description')} ({connected_text}, {enabled_text})")
    else:
        lines.append("- none detected")

    lines.append("")
    lines.append("Cores (Systems)")
    lines.append("These are the Cores currently active in your system.")
    if cores_rows:
        for row in cores_rows:
            running_text = "running" if _status_bool(row.get("running"), default=False) else "stopped"
            enabled_text = "enabled" if _status_bool(row.get("enabled"), default=False) else "disabled"
            lines.append(f"- {row.get('name')}: {row.get('description')} ({running_text}, {enabled_text})")
    else:
        lines.append("- none detected")

    lines.append("")
    lines.append("Rules:")
    lines.append("- Use this information for awareness of current capability and system status.")
    lines.append("- You may reference these Verba tools, Kernel tools, Portals, and Cores when relevant.")
    lines.append("- Do NOT simulate calling Verba or Kernel tools in this response.")
    lines.append("- Do NOT pretend to execute actions in chat mode.")
    lines.append("- Do NOT mention internal modes, pipelines, or branches unless asked.")
    lines.append("- If the user asks to perform an action, respond naturally as Tater without claiming execution occurred.")
    lines.append("- Keep responses immersive and user-facing, not mechanical.")
    lines.append("- Chat path alignment: Astraeus speaks with awareness; Thanatos stands down; Hermes is inactive unless execution occurs.")
    return "\n".join(lines).strip()


def _attach_origin(
    args: Dict[str, Any],
    *,
    origin: Optional[Dict[str, Any]],
    platform: str,
    scope: str,
    request_text: str = "",
) -> Dict[str, Any]:
    return origin_attach_helpers.attach_origin(
        args,
        origin=origin,
        platform=platform,
        scope=scope,
        request_text=request_text,
    )


def _parse_strict_tool_json(response_text: str) -> Optional[Dict[str, Any]]:
    return validation.parse_strict_tool_json(response_text)


def _meta_tool_args_reason(func: str, args: Dict[str, Any]) -> str:
    del func, args
    return ""


def _validate_tool_call_dict(
    *,
    parsed: Any,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> Dict[str, Any]:
    return validation.validate_tool_call_dict(
        parsed=parsed,
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
        canonical_tool_name_fn=_canonical_tool_name,
        is_meta_tool_fn=is_meta_tool,
        plugin_supports_platform_fn=verba_supports_platform,
        meta_tool_args_reason_fn=_meta_tool_args_reason,
    )


async def _repair_tool_call_text(
    *,
    llm_client: Any,
    platform: str,
    original_text: str,
    reason: str,
    tool_index: str,
    user_text: str = "",
    tool_name_hint: str = "",
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> str:
    return await validation.repair_tool_call_text(
        llm_client=llm_client,
        platform=platform,
        original_text=original_text,
        reason=reason,
        tool_index=tool_index,
        tool_markup_repair_prompt=TOOL_MARKUP_REPAIR_PROMPT,
        with_platform_preamble_fn=lambda messages, preamble: _with_platform_preamble(
            messages, platform_preamble=preamble
        ),
        configured_tool_repair_max_tokens_fn=(lambda: None),
        coerce_text_fn=_coerce_text,
        user_text=user_text,
        tool_name_hint=tool_name_hint,
        platform_preamble=platform_preamble,
        max_tokens=max_tokens,
    )


async def _validate_tool_contract(
    *,
    llm_client: Any,
    response_text: str,
    user_text: str = "",
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    tool_index: str,
    platform_preamble: str = "",
    repair_max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    return await validation.validate_tool_contract(
        llm_client=llm_client,
        response_text=response_text,
        user_text=user_text,
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
        tool_index=tool_index,
        parse_function_json_fn=parse_function_json,
        validate_tool_call_dict_fn=lambda parsed, platform_arg, registry_arg, enabled_arg: _validate_tool_call_dict(
            parsed=parsed,
            platform=platform_arg,
            registry=registry_arg,
            enabled_predicate=enabled_arg,
        ),
        repair_tool_call_text_fn=_repair_tool_call_text,
        platform_preamble=platform_preamble,
        repair_max_tokens=repair_max_tokens,
    )


async def _validate_or_recover_tool_call(
    *,
    llm_client: Any,
    text: str,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    tool_index: str,
    user_text: str,
    origin: Optional[Dict[str, Any]],
    scope: str,
    history_messages: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
    platform_preamble: str = "",
) -> Dict[str, Any]:
    return await validation_flow.validate_or_recover_tool_call(
        llm_client=llm_client,
        text=text,
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
        tool_index=tool_index,
        user_text=user_text,
        origin=origin,
        scope=scope,
        history_messages=history_messages,
        context=context,
        platform_preamble=platform_preamble,
        repair_max_tokens=None,
        recovery_max_tokens=None,
        is_tool_candidate_fn=_is_tool_candidate,
        validate_tool_contract_fn=_validate_tool_contract,
        short_text_fn=_short_text,
        generate_recovery_text_fn=_generate_recovery_text,
        validation_failure_text_fn=_validation_failure_text,
        normalize_tool_call_for_user_request_fn=_normalize_tool_call_for_user_request,
        enrich_tool_call_for_user_request_fn=_llm_enrich_tool_call_for_user_request,
        enrich_max_tokens=None,
    )


def _validation_failure_text(reason: str, platform: str) -> str:
    return validation.validation_failure_text(reason, platform)


async def _generate_recovery_text(
    *,
    llm_client: Any,
    platform: str,
    user_text: str,
    error_kind: str,
    reason: str = "",
    fallback: str,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> str:
    return await validation.generate_recovery_text(
        llm_client=llm_client,
        platform=platform,
        user_text=user_text,
        error_kind=error_kind,
        reason=reason,
        fallback=fallback,
        with_platform_preamble_fn=lambda messages, preamble: _with_platform_preamble(
            messages, platform_preamble=preamble
        ),
        configured_recovery_max_tokens_fn=(lambda: None),
        looks_like_tool_markup_fn=looks_like_tool_markup,
        parse_function_json_fn=parse_function_json,
        checker_decision_prefix_re=_MINOS_DECISION_PREFIX_RE,
        default_clarification=DEFAULT_CLARIFICATION,
        coerce_text_fn=_coerce_text,
        platform_preamble=platform_preamble,
        max_tokens=max_tokens,
    )


async def _normalize_tool_result_for_minos(
    *,
    result_payload: Any,
    llm_client: Any,
    platform: str,
) -> Dict[str, Any]:
    return await execution.normalize_tool_result_for_minos(
        result_payload=result_payload,
        llm_client=llm_client,
        platform=platform,
        normalize_plugin_result_fn=normalize_verba_result,
        narrate_result_fn=narrate_result,
        result_for_llm_fn=result_for_llm,
        short_text_fn=_short_text,
    )


async def _execute_tool_call(
    *,
    llm_client: Any,
    tool_call: Dict[str, Any],
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    context: Optional[Dict[str, Any]],
    user_text: str,
    origin: Optional[Dict[str, Any]],
    scope: str,
    wait_callback: Optional[Callable[..., Any]],
    admin_guard: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    wait_text: str = "",
    wait_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return await execution.execute_tool_call(
        llm_client=llm_client,
        tool_call=tool_call,
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
        context=context,
        user_text=user_text,
        origin=origin,
        scope=scope,
        wait_callback=wait_callback,
        wait_text=wait_text,
        wait_payload=wait_payload,
        admin_guard=admin_guard,
        canonical_tool_name_fn=_canonical_tool_name,
        attach_origin_fn=_attach_origin,
        normalize_plugin_result_fn=normalize_verba_result,
        normalize_tool_result_for_minos_fn=_normalize_tool_result_for_minos,
        action_failure_fn=action_failure,
        plugin_display_name_fn=verba_display_name,
        expand_plugin_platforms_fn=expand_verba_platforms,
        plugin_supports_platform_fn=verba_supports_platform,
        is_meta_tool_fn=is_meta_tool,
        run_meta_tool_fn=run_meta_tool,
        execute_plugin_call_fn=execute_plugin_call,
    )


def _parse_minos_decision(text: str) -> Dict[str, Any]:
    return minos.parse_minos_decision(
        text,
        minos_decision_prefix_re=_MINOS_DECISION_PREFIX_RE,
        parse_function_json_fn=parse_function_json,
        is_tool_candidate_fn=_is_tool_candidate,
        normalize_minos_decision_fn=_normalize_minos_decision,
    )


async def _run_minos_validation(
    *,
    llm_client: Any,
    platform: str,
    current_user_text: str,
    turn_request_text: str,
    agent_state: Optional[Dict[str, Any]],
    memory_context: Optional[Dict[str, Any]],
    available_artifacts: Optional[List[Dict[str, Any]]],
    current_step: Optional[Dict[str, Any]],
    goal: str,
    planned_tool: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    draft_response: str,
    retry_count: int,
    retry_allowed: bool,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    return await minos.run_minos_validation(
        llm_client=llm_client,
        platform=platform,
        current_user_text=current_user_text,
        turn_request_text=turn_request_text,
        agent_state=agent_state,
        memory_context=memory_context,
        available_artifacts=available_artifacts,
        current_step=current_step,
        goal=goal,
        planned_tool=planned_tool,
        tool_result=tool_result,
        draft_response=draft_response,
        retry_count=retry_count,
        retry_allowed=retry_allowed,
        normalize_agent_state_fn=lambda state, fallback: _normalize_agent_state(state, fallback_goal=fallback),
        coerce_non_negative_int_fn=_coerce_non_negative_int,
        short_text_fn=_short_text,
        memory_context_default_summary_max_chars=_MEMORY_CONTEXT_DEFAULT_SUMMARY_MAX_CHARS,
        configured_minos_max_tokens_fn=(lambda: None),
        minos_system_prompt_fn=lambda plat, retry: _minos_system_prompt(plat, retry_allowed=retry),
        with_platform_preamble_fn=lambda messages, preamble: _with_platform_preamble(
            messages, platform_preamble=preamble
        ),
        parse_minos_decision_fn=_parse_minos_decision,
        coerce_text_fn=_coerce_text,
        platform_preamble=platform_preamble,
        max_tokens=max_tokens,
    )


def _sanitize_user_text(text: str, *, platform: str, tool_used: bool) -> str:
    return common_helpers.sanitize_user_text(
        text,
        platform=platform,
        tool_used=tool_used,
        default_clarification=DEFAULT_CLARIFICATION,
        looks_like_tool_markup_fn=looks_like_tool_markup,
        parse_function_json_fn=parse_function_json,
        checker_decision_prefix_re=_MINOS_DECISION_PREFIX_RE,
        ascii_only_platforms=ASCII_ONLY_PLATFORMS,
    )


def _short_text(value: Any, *, limit: int = 280) -> str:
    return common_helpers.short_text(value, limit=limit)


def _normalize_minos_decision(label: str) -> str:
    return minos.normalize_minos_decision(label)


def _checker_decision_value(decision: Optional[Dict[str, Any]]) -> str:
    if not isinstance(decision, dict):
        return "FINAL"
    raw = (
        decision.get("decision")
        or decision.get("kind")
        or decision.get("action")
        or decision.get("checker_action")
        or "FINAL"
    )
    return _normalize_minos_decision(str(raw or "FINAL"))


def _checker_decision_text(decision: Optional[Dict[str, Any]], *keys: str) -> str:
    if not isinstance(decision, dict):
        return ""
    for key in keys:
        text = str(decision.get(key) or "").strip()
        if text:
            return text
    return ""


def _is_low_information_text(value: Any) -> bool:
    return common_helpers.is_low_information_text(value)


def _first_json_object(text: str) -> Optional[Dict[str, Any]]:
    return common_helpers.first_json_object(
        text,
        coerce_text_fn=_coerce_text,
    )


def _render_plan_line(step: Dict[str, str]) -> str:
    intent = _short_text(step.get("intent"), limit=96)
    nl = _short_text(step.get("nl"), limit=160)
    if intent and nl:
        intent_norm = intent.rstrip(".!?").strip().lower()
        nl_norm = nl.rstrip(".!?").strip().lower()
        if intent_norm and nl_norm and intent_norm != nl_norm:
            return f"{intent}: {nl}"
    return nl or intent


def _normalize_plan_step_candidate(
    candidate: Any,
    *,
    index: int,
    available_tool_ids: Optional[set[str]] = None,
) -> Optional[Dict[str, str]]:
    if not isinstance(candidate, dict):
        return None
    raw_intent = (
        candidate.get("intent")
        or candidate.get("goal")
        or candidate.get("task")
        or candidate.get("summary")
        or ""
    )
    intent = _short_text(" ".join(_coerce_text(raw_intent).split()), limit=180)
    raw_nl = (
        candidate.get("nl")
        or candidate.get("instruction")
        or candidate.get("request")
        or candidate.get("query")
        or candidate.get("text")
        or ""
    )
    nl = _short_text(" ".join(_coerce_text(raw_nl).split()), limit=220)
    if not nl and not intent:
        return None
    if not intent:
        intent = nl
    if not nl:
        nl = intent
    raw_id = str(candidate.get("step_id") or candidate.get("id") or f"s{index + 1}").strip()
    step_id = _short_text(raw_id, limit=24) or f"s{index + 1}"
    raw_tool_hint = (
        candidate.get("tool_hint")
        or candidate.get("tool")
        or candidate.get("tool_id")
        or ""
    )
    tool_hint = _canonical_tool_name(str(raw_tool_hint or "").strip())
    if (
        tool_hint
        and isinstance(available_tool_ids, set)
        and available_tool_ids
        and tool_hint not in available_tool_ids
    ):
        tool_hint = ""
    step: Dict[str, str] = {"id": step_id, "intent": intent, "nl": nl}
    if tool_hint:
        step["tool_hint"] = tool_hint
    return step

def _sync_agent_state_with_plan_queue(
    *,
    agent_state: Optional[Dict[str, Any]],
    plan_queue: List[Dict[str, str]],
    fallback_goal: str,
) -> Dict[str, Any]:
    merged = dict(agent_state) if isinstance(agent_state, dict) else {}
    merged["plan_steps"] = [dict(step) for step in plan_queue if isinstance(step, dict)]
    lines = [_render_plan_line(step) for step in plan_queue if _render_plan_line(step)]
    merged["plan"] = lines
    merged["next_step"] = lines[0] if lines else ""
    return _normalize_agent_state(merged, fallback_goal=fallback_goal)


def _generic_chat_fallback_text(text: str) -> str:
    del text
    return "I'm here and ready to talk or help."


async def _run_astraeus_plan(
    *,
    llm_client: Any,
    platform: str,
    current_user_text: str,
    turn_request_text: str,
    topic_seed: str,
    topic_shift_seed: bool,
    history: List[Dict[str, Any]],
    prior_state: Optional[Dict[str, Any]],
    memory_context: Optional[Dict[str, Any]],
    capability_catalog: str,
    available_tool_ids: set[str],
    platform_preamble: str,
    max_tokens: Optional[int],
) -> Dict[str, Any]:
    fallback_goal = _short_text(turn_request_text or current_user_text, limit=220) or "Fulfill the user request."
    topic_value = _short_text(topic_seed, limit=90)
    topic_shift_value = bool(topic_shift_seed)
    recent_history_all: List[Dict[str, str]] = []
    for msg in (history or []):
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _short_text(msg.get("content"), limit=240)
        if not content:
            continue
        recent_history_all.append({"role": role, "content": content})
    recent_history = recent_history_all

    payload = {
        "current_user_message": str(current_user_text or ""),
        "recent_history": recent_history,
    }
    if isinstance(prior_state, dict) and prior_state:
        normalized_prior = _normalize_agent_state(
            prior_state,
            fallback_goal=fallback_goal,
        )
        payload["prior_context"] = {
            "goal": _short_text(normalized_prior.get("goal"), limit=180),
            "plan": [str(item) for item in (normalized_prior.get("plan") or []) if str(item).strip()][:8],
            "facts": [str(item) for item in (normalized_prior.get("facts") or []) if str(item).strip()][:8],
            "open_questions": [
                str(item) for item in (normalized_prior.get("open_questions") or []) if str(item).strip()
            ][:4],
        }
    if isinstance(memory_context, dict) and memory_context:
        user_ctx = memory_context.get("user") if isinstance(memory_context.get("user"), dict) else {}
        room_ctx = memory_context.get("room") if isinstance(memory_context.get("room"), dict) else {}
        payload["memory_context"] = {
            "user_memory": _short_text(user_ctx.get("summary"), limit=1200),
            "room_memory": _short_text(room_ctx.get("summary"), limit=1200),
        }
    if capability_catalog:
        payload["available_capabilities"] = capability_catalog
    if available_tool_ids:
        payload["available_tool_ids"] = sorted(str(item) for item in available_tool_ids if str(item).strip())[:200]

    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": _astraeus_system_prompt(platform)},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    messages = _with_platform_preamble(messages, platform_preamble=platform_preamble)
    try:
        resp = await llm_client.chat(
            messages=messages,
            temperature=0.1,
            **_chat_with_optional_max_tokens_kwargs(
                max_tokens=max_tokens,
                minimum=200,
                fallback=900,
            ),
        )
    except Exception:
        return {
            "mode": "unknown",
            "topic": topic_value,
            "topic_shift": topic_shift_value,
            "goal": fallback_goal,
            "steps": [],
        }
    raw = _coerce_text((resp.get("message", {}) or {}).get("content", "")).strip()
    obj = _first_json_object(raw)
    if not isinstance(obj, dict):
        return {
            "mode": "unknown",
            "topic": topic_value,
            "topic_shift": topic_shift_value,
            "goal": fallback_goal,
            "steps": [],
        }
    mode = str(obj.get("mode") or "").strip().lower()
    topic = topic_value or _short_text(obj.get("topic"), limit=90)
    goal = _short_text(obj.get("goal"), limit=220) or fallback_goal
    topic_shift = topic_shift_value
    raw_steps = obj.get("steps")
    if mode == "chat":
        return {
            "mode": "chat",
            "topic": topic,
            "topic_shift": topic_shift,
            "goal": goal,
            "steps": [],
        }
    if raw_steps is None:
        raw_steps = []
    if not isinstance(raw_steps, list):
        return {
            "mode": "unknown",
            "topic": topic,
            "topic_shift": topic_shift,
            "goal": goal,
            "steps": [],
        }
    out: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for idx, item in enumerate(raw_steps):
        step = _normalize_plan_step_candidate(
            item,
            index=idx,
            available_tool_ids=available_tool_ids,
        )
        if not isinstance(step, dict):
            continue
        dedupe_key = (step.get("intent", ""), step.get("nl", ""))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(step)
        if len(out) >= 12:
            break
    if mode == "execute":
        if out:
            return {
                "mode": "execute",
                "topic": topic,
                "topic_shift": topic_shift,
                "goal": goal,
                "steps": out,
            }
        return {
            "mode": "unknown",
            "topic": topic,
            "topic_shift": topic_shift,
            "goal": goal,
            "steps": [],
        }
    if out:
        return {
            "mode": "execute",
            "topic": topic,
            "topic_shift": topic_shift,
            "goal": goal,
            "steps": out,
        }
    return {
        "mode": "chat",
        "topic": topic,
        "topic_shift": topic_shift,
        "goal": goal,
        "steps": [],
    }


async def _review_execution_plan_for_completeness(
    *,
    llm_client: Any,
    platform: str,
    current_user_text: str,
    turn_request_text: str,
    goal: str,
    steps: List[Dict[str, str]],
    capability_catalog: str,
    available_tool_ids: set[str],
    platform_preamble: str,
    max_tokens: Optional[int],
) -> Dict[str, Any]:
    original_steps = [dict(step) for step in steps if isinstance(step, dict)]
    original_goal = _short_text(goal, limit=220) or _short_text(turn_request_text, limit=220) or "Fulfill the user request."
    if not original_steps:
        return {"goal": original_goal, "steps": original_steps}

    payload = {
        "current_user_message": _short_text(current_user_text, limit=420),
        "goal": original_goal,
        "steps": original_steps[:12],
        "available_capabilities": capability_catalog,
        "available_tool_ids": sorted(str(item) for item in available_tool_ids if str(item).strip())[:200],
    }
    messages: List[Dict[str, Any]] = [
        {
            "role": "system",
            "content": prompts.astraeus_plan_review_system_prompt(platform=platform),
        },
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    messages = _with_platform_preamble(messages, platform_preamble=platform_preamble)
    try:
        resp = await llm_client.chat(
            messages=messages,
            temperature=0.0,
            **_chat_with_optional_max_tokens_kwargs(
                max_tokens=max_tokens,
                minimum=120,
                fallback=260,
                maximum=520,
            ),
        )
    except Exception:
        return {"goal": original_goal, "steps": original_steps}

    raw = _coerce_text((resp.get("message", {}) or {}).get("content", "")).strip()
    obj = _first_json_object(raw)
    if not isinstance(obj, dict):
        return {"goal": original_goal, "steps": original_steps}

    revised_goal = _short_text(obj.get("goal"), limit=220) or original_goal
    raw_steps = obj.get("steps")
    if not isinstance(raw_steps, list):
        return {"goal": revised_goal, "steps": original_steps}

    out: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for idx, item in enumerate(raw_steps):
        step = _normalize_plan_step_candidate(
            item,
            index=idx,
            available_tool_ids=available_tool_ids,
        )
        if not isinstance(step, dict):
            continue
        dedupe_key = (step.get("intent", ""), step.get("nl", ""))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(step)
        if len(out) >= 12:
            break
    if not out:
        out = original_steps
    return {"goal": revised_goal, "steps": out}


async def _run_thanatos_step(
    *,
    llm_client: Any,
    thanatos_messages: List[Dict[str, Any]],
    max_tokens: Optional[int],
) -> tuple[str, float]:
    started = time.perf_counter()
    text = ""
    try:
        thanatos_resp = await llm_client.chat(
            messages=thanatos_messages,
            temperature=0.2,
            **_chat_with_optional_max_tokens_kwargs(
                max_tokens=max_tokens,
                minimum=1,
                fallback=1,
            ),
        )
        text = _coerce_text((thanatos_resp.get("message", {}) or {}).get("content", "")).strip()
    except Exception:
        text = ""
    return text, (time.perf_counter() - started) * 1000.0


def _estimate_text_tokens_approx(text: Any) -> int:
    content = _coerce_text(text).strip()
    if not content:
        return 0
    chars = len(content)
    words = len(re.findall(r"\S+", content))
    chars_tokens = max(1, (chars + 3) // 4)
    words_tokens = max(0, (words * 13 + 9) // 10)
    return max(chars_tokens, words_tokens)


def _estimate_message_tokens_approx(content: Any) -> int:
    return _estimate_text_tokens_approx(content) + _CHAT_ESTIMATE_MESSAGE_OVERHEAD_TOKENS


def _suggest_context_window_size(needed_tokens: int) -> int:
    target = max(1, int(needed_tokens or 0))
    for size in _CHAT_ESTIMATE_STANDARD_WINDOWS:
        if target <= size:
            return int(size)
    return int(((target + 1023) // 1024) * 1024)


def _seed_user_text_for_context_estimate(
    *,
    user_text: str,
    chat_history: List[Dict[str, str]],
) -> tuple[str, str]:
    direct = _short_text(_coerce_text(user_text), limit=260).strip()
    if direct:
        return direct, "current_user_text"

    for item in reversed(chat_history):
        if str(item.get("role") or "").strip().lower() != "user":
            continue
        content = _short_text(_coerce_text(item.get("content")), limit=260).strip()
        if content:
            return content, "recent_user_turn"

    return "Hey Tater, can you help me with this?", "default_seed"


def _status_row_profile_text(row: Dict[str, Any]) -> str:
    name = _coerce_text((row or {}).get("name")).strip().lower()
    description = _coerce_text((row or {}).get("description")).strip().lower()
    return " ".join(part for part in [name, description] if part).strip()


def _estimate_capability_context_reserve_tokens(
    *,
    verbas_rows: List[Dict[str, Any]],
    cores_rows: List[Dict[str, Any]],
    portals_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    enabled_verba_rows = [row for row in (verbas_rows or []) if _status_bool((row or {}).get("enabled"), default=False)]
    running_core_rows = [row for row in (cores_rows or []) if _status_bool((row or {}).get("running"), default=False)]
    connected_portal_rows = [row for row in (portals_rows or []) if _status_bool((row or {}).get("connected"), default=False)]

    high_context_verba_count = 0
    medium_context_verba_count = 0
    high_context_examples: List[str] = []
    for row in enabled_verba_rows:
        profile = _status_row_profile_text(row)
        if any(keyword in profile for keyword in _CHAT_ESTIMATE_HIGH_CONTEXT_VERBA_KEYWORDS):
            high_context_verba_count += 1
            if len(high_context_examples) < 4:
                label = _short_text(_coerce_text((row or {}).get("name")), limit=36).strip()
                if label:
                    high_context_examples.append(label)
            continue
        if any(keyword in profile for keyword in _CHAT_ESTIMATE_MEDIUM_CONTEXT_VERBA_KEYWORDS):
            medium_context_verba_count += 1

    heavy_core_count = 0
    for row in running_core_rows:
        profile = _status_row_profile_text(row)
        if any(keyword in profile for keyword in _CHAT_ESTIMATE_HEAVY_CORE_KEYWORDS):
            heavy_core_count += 1

    enabled_verbas = len(enabled_verba_rows)
    running_cores = len(running_core_rows)
    connected_portals = len(connected_portal_rows)

    capability_tokens = 0
    if enabled_verbas or running_cores or connected_portals:
        capability_tokens += 128
    capability_tokens += enabled_verbas * 120
    capability_tokens += running_cores * 180
    capability_tokens += connected_portals * 48
    capability_tokens += high_context_verba_count * 280
    capability_tokens += medium_context_verba_count * 120
    capability_tokens += heavy_core_count * 110
    capability_tokens = max(0, min(16000, int(capability_tokens)))

    burst_tokens = 0
    burst_tokens += high_context_verba_count * 260
    burst_tokens += max(0, high_context_verba_count - 1) * 120
    burst_tokens += medium_context_verba_count * 90
    burst_tokens += heavy_core_count * 90
    burst_tokens += max(0, enabled_verbas - 4) * 45
    burst_tokens += running_cores * 70
    burst_tokens = max(0, min(12000, int(burst_tokens)))

    return {
        "capability_tokens": capability_tokens,
        "burst_tokens": burst_tokens,
        "enabled_verbas": int(enabled_verbas),
        "running_cores": int(running_cores),
        "connected_portals": int(connected_portals),
        "high_context_verbas": int(high_context_verba_count),
        "medium_context_verbas": int(medium_context_verba_count),
        "heavy_cores": int(heavy_core_count),
        "high_context_verba_examples": high_context_examples,
    }


def estimate_hydra_chat_context_window(
    *,
    platform: str,
    history_messages: List[Dict[str, Any]],
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]] = None,
    redis_client: Any = None,
    scope: str = "",
    origin: Optional[Dict[str, Any]] = None,
    platform_preamble: str = "",
    user_text: str = "",
) -> Dict[str, Any]:
    r = redis_client or default_redis
    normalized_platform = normalize_platform(platform) or str(platform or "").strip().lower() or "webui"
    origin_payload = dict(origin) if isinstance(origin, dict) else {}
    resolved_scope = _resolve_hydra_scope(normalized_platform, scope, origin_payload)

    compact_history = _compact_history(history_messages or [])
    chat_history = _chat_history_window(compact_history, max_items=0)
    history_message_count = len(chat_history)

    memory_context_payload = _memory_context_payload(
        redis_client=r,
        platform=normalized_platform,
        scope=resolved_scope,
        origin=origin_payload,
    )
    chat_core_context = _core_system_prompt_message(
        role="chat",
        platform=normalized_platform,
        scope=resolved_scope,
        origin=origin_payload,
        redis_client=r,
        memory_context_payload=memory_context_payload,
    )
    chat_system_prompt = _chat_fallback_system_prompt(normalized_platform)
    status_prompt = _render_tater_system_status_prompt(
        platform=normalized_platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
        redis_client=r,
        history=chat_history,
        max_tokens=None,
    )
    clean_preamble = _sanitize_platform_preamble(normalized_platform, platform_preamble)
    seeded_user_text, seed_source = _seed_user_text_for_context_estimate(
        user_text=user_text,
        chat_history=chat_history,
    )
    verbas_rows = _collect_verbas_status_rows(
        registry=registry,
        enabled_predicate=enabled_predicate,
    )
    portals_rows = _collect_portals_status_rows(
        redis_client=r,
        platform=normalized_platform,
    )
    cores_rows = _collect_cores_status_rows(redis_client=r)
    context_reserve = _estimate_capability_context_reserve_tokens(
        verbas_rows=verbas_rows,
        cores_rows=cores_rows,
        portals_rows=portals_rows,
    )
    capability_reserve_tokens = int(max(0, context_reserve.get("capability_tokens") or 0))
    burst_reserve_tokens = int(max(0, context_reserve.get("burst_tokens") or 0))
    high_context_verba_count = int(max(0, context_reserve.get("high_context_verbas") or 0))
    medium_context_verba_count = int(max(0, context_reserve.get("medium_context_verbas") or 0))
    heavy_core_count = int(max(0, context_reserve.get("heavy_cores") or 0))
    high_context_verba_examples = list(context_reserve.get("high_context_verba_examples") or [])[:4]

    base_messages: List[Dict[str, str]] = [{"role": "system", "content": chat_system_prompt}]
    if status_prompt:
        base_messages.append({"role": "system", "content": status_prompt})
    if chat_core_context:
        base_messages.append({"role": "system", "content": chat_core_context})
    if clean_preamble:
        base_messages = _with_platform_preamble(base_messages, platform_preamble=clean_preamble)
    base_messages.extend(chat_history)
    base_messages.append({"role": "user", "content": seeded_user_text})

    prompt_tokens = _CHAT_ESTIMATE_REQUEST_OVERHEAD_TOKENS
    prompt_chars = 0
    for msg in base_messages:
        content = _coerce_text(msg.get("content")).strip()
        prompt_chars += len(content)
        prompt_tokens += _estimate_message_tokens_approx(content)

    history_tokens = 0
    history_chars = 0
    for msg in chat_history:
        content = _coerce_text(msg.get("content")).strip()
        history_chars += len(content)
        history_tokens += _estimate_message_tokens_approx(content)

    user_tokens = _estimate_message_tokens_approx(seeded_user_text)
    user_chars = len(seeded_user_text)
    system_tokens = _estimate_message_tokens_approx(chat_system_prompt)
    status_tokens = _estimate_message_tokens_approx(status_prompt) if status_prompt else 0
    core_context_tokens = _estimate_message_tokens_approx(chat_core_context) if chat_core_context else 0
    preamble_tokens = _estimate_message_tokens_approx(clean_preamble) if clean_preamble else 0

    completion_cap_tokens = 1400 + min(1000, (burst_reserve_tokens + 2) // 3)
    completion_budget_tokens = max(
        192,
        min(
            int(completion_cap_tokens),
            ((prompt_tokens * 35 + 99) // 100) + (capability_reserve_tokens // 12),
        ),
    )
    minimum_context_tokens = prompt_tokens + completion_budget_tokens + capability_reserve_tokens
    recommended_margin_tokens = max(256, (minimum_context_tokens * 20 + 99) // 100)
    recommended_context_tokens = minimum_context_tokens + recommended_margin_tokens + burst_reserve_tokens
    minimum_context_window = _suggest_context_window_size(minimum_context_tokens)
    recommended_context_window = _suggest_context_window_size(recommended_context_tokens)
    enabled_verbas = int(max(0, context_reserve.get("enabled_verbas") or 0))
    connected_portals = int(max(0, context_reserve.get("connected_portals") or 0))
    running_cores = int(max(0, context_reserve.get("running_cores") or 0))

    return {
        "platform": normalized_platform,
        "prompt_tokens": int(max(0, prompt_tokens)),
        "prompt_chars": int(max(0, prompt_chars)),
        "completion_budget_tokens": int(max(0, completion_budget_tokens)),
        "minimum_context_tokens": int(max(0, minimum_context_tokens)),
        "recommended_context_tokens": int(max(0, recommended_context_tokens)),
        "minimum_context_window": int(max(0, minimum_context_window)),
        "recommended_context_window": int(max(0, recommended_context_window)),
        "capability_context_reserve_tokens": int(max(0, capability_reserve_tokens)),
        "burst_context_reserve_tokens": int(max(0, burst_reserve_tokens)),
        "message_count": int(len(base_messages)),
        "history_messages": int(history_message_count),
        "max_history_messages": int(history_message_count),
        "enabled_verbas": int(enabled_verbas),
        "connected_portals": int(connected_portals),
        "running_cores": int(running_cores),
        "seed_source": seed_source,
        "breakdown": {
            "system_tokens": int(max(0, system_tokens)),
            "status_tokens": int(max(0, status_tokens)),
            "core_context_tokens": int(max(0, core_context_tokens)),
            "platform_preamble_tokens": int(max(0, preamble_tokens)),
            "history_tokens": int(max(0, history_tokens)),
            "user_tokens": int(max(0, user_tokens)),
            "capability_reserve_tokens": int(max(0, capability_reserve_tokens)),
            "burst_reserve_tokens": int(max(0, burst_reserve_tokens)),
            "high_context_verbas": int(max(0, high_context_verba_count)),
            "medium_context_verbas": int(max(0, medium_context_verba_count)),
            "heavy_cores": int(max(0, heavy_core_count)),
            "high_context_verba_examples": high_context_verba_examples,
            "recommended_margin_tokens": int(max(0, recommended_margin_tokens)),
            "history_chars": int(max(0, history_chars)),
            "user_chars": int(max(0, user_chars)),
        },
    }


async def _run_chat_fallback_reply(
    *,
    llm_client: Any,
    platform: str,
    user_text: str,
    history: List[Dict[str, Any]],
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    redis_client: Any,
    memory_context_message: str,
    platform_preamble: str,
    max_tokens: Optional[int],
) -> str:
    # Use the full upstream history window (already bounded by general max_llm setting).
    chat_history = _chat_history_window(history, max_items=0)
    messages: List[Dict[str, Any]] = [
        {"role": "system", "content": _chat_fallback_system_prompt(platform)},
    ]
    status_prompt = _render_tater_system_status_prompt(
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
        redis_client=redis_client,
        history=chat_history,
        max_tokens=max_tokens,
    )
    if status_prompt:
        messages.append({"role": "system", "content": status_prompt})
    if memory_context_message:
        messages.append({"role": "system", "content": memory_context_message})
    messages = _with_platform_preamble(messages, platform_preamble=platform_preamble)
    messages.extend(chat_history)
    messages.append({"role": "user", "content": str(user_text or "")})
    try:
        resp = await llm_client.chat(
            messages=messages,
            temperature=0.4,
            **_chat_with_optional_max_tokens_kwargs(
                max_tokens=max_tokens,
                minimum=64,
                fallback=220,
            ),
        )
    except Exception:
        return ""
    return _coerce_text((resp.get("message", {}) or {}).get("content", "")).strip()


def _turn_goal_for_state(*, current_user_text: str, turn_request_text: str) -> str:
    goal = _short_text((turn_request_text or current_user_text or "").strip(), limit=180)
    return goal or "Fulfill the user request."


def _state_list(value: Any, *, max_items: int, item_limit: int) -> List[str]:
    return state_core_helpers.state_list(
        value,
        max_items=max_items,
        item_limit=item_limit,
        coerce_text_fn=_coerce_text,
        short_text_fn=_short_text,
    )


def _state_next_step(value: Any) -> str:
    return state_core_helpers.state_next_step(
        value,
        coerce_text_fn=_coerce_text,
        short_text_fn=_short_text,
    )


def _state_plan_steps(value: Any, *, max_items: int = 12) -> List[Dict[str, str]]:
    if not isinstance(value, list):
        return []
    out: List[Dict[str, str]] = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            continue
        intent = _short_text(item.get("intent"), limit=96)
        nl = _short_text(item.get("nl"), limit=200)
        if not intent and not nl:
            continue
        if not intent:
            intent = nl
        if not nl:
            nl = intent
        step_id = _short_text(item.get("id"), limit=24) or f"s{idx + 1}"
        out.append({"id": step_id, "intent": intent, "nl": nl})
        if len(out) >= max_items:
            break
    return out


def _state_result_memory(
    value: Any,
    *,
    max_sets: Optional[int] = None,
    max_items: Optional[int] = None,
) -> List[Dict[str, Any]]:
    sets_limit = max(1, int(max_sets or RESULT_MEMORY_MAX_SETS or DEFAULT_RESULT_MEMORY_MAX_SETS))
    items_limit = max(1, int(max_items or RESULT_MEMORY_MAX_ITEMS or DEFAULT_RESULT_MEMORY_MAX_ITEMS))
    if not isinstance(value, list):
        return []
    out: List[Dict[str, Any]] = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            continue
        result_set_id = _short_text(item.get("result_set_id"), limit=18) or f"rs{idx + 1}"
        tool_name = _short_text(item.get("tool"), limit=64) or "tool"
        summary = _short_text(item.get("summary"), limit=180)
        query = _short_text(item.get("query"), limit=180)
        request = _short_text(item.get("request"), limit=180)
        normalized_items: List[Dict[str, str]] = []
        raw_items = item.get("items") if isinstance(item.get("items"), list) else []
        for item_idx, raw_result_item in enumerate(raw_items):
            if not isinstance(raw_result_item, dict):
                continue
            item_ref = _short_text(raw_result_item.get("item_ref"), limit=12) or f"#{item_idx + 1}"
            title = _short_text(raw_result_item.get("title"), limit=120)
            locator = _short_text(raw_result_item.get("locator"), limit=220)
            preview = _short_text(raw_result_item.get("preview"), limit=180)
            compact_item: Dict[str, str] = {"item_ref": item_ref}
            if title:
                compact_item["title"] = title
            if locator:
                compact_item["locator"] = locator
            if preview and preview != title:
                compact_item["preview"] = preview
            if len(compact_item) <= 1:
                continue
            normalized_items.append(compact_item)
            if len(normalized_items) >= items_limit:
                break
        if not any((summary, query, request, normalized_items)):
            continue
        compact_set: Dict[str, Any] = {
            "result_set_id": result_set_id,
            "tool": tool_name,
        }
        if summary:
            compact_set["summary"] = summary
        if query:
            compact_set["query"] = query
        if request:
            compact_set["request"] = request
        if normalized_items:
            compact_set["items"] = normalized_items
        out.append(compact_set)
        if len(out) >= sets_limit:
            break
    return out


def _normalize_agent_state(state: Optional[Dict[str, Any]], *, fallback_goal: str) -> Dict[str, Any]:
    normalized = state_core_helpers.normalize_agent_state(
        state,
        fallback_goal=fallback_goal,
        coerce_text_fn=_coerce_text,
        short_text_fn=_short_text,
        state_list_fn=lambda value, max_items, item_limit: _state_list(
            value,
            max_items=max_items,
            item_limit=item_limit,
        ),
        state_next_step_fn=_state_next_step,
    )
    source = state if isinstance(state, dict) else {}
    plan_steps = _state_plan_steps(source.get("plan_steps"), max_items=12)
    normalized["plan_steps"] = plan_steps
    result_memory = _state_result_memory(
        source.get("result_memory"),
        max_sets=RESULT_MEMORY_MAX_SETS,
        max_items=RESULT_MEMORY_MAX_ITEMS,
    )
    normalized["result_memory"] = result_memory
    if plan_steps:
        if not normalized.get("plan"):
            normalized["plan"] = [_render_plan_line(step) for step in plan_steps if _render_plan_line(step)]
        if not normalized.get("next_step"):
            normalized["next_step"] = _render_plan_line(plan_steps[0])
    return normalized


def _compact_agent_state_json(state: Optional[Dict[str, Any]], *, fallback_goal: str, limit: int) -> str:
    return state_store.compact_agent_state_json(
        state,
        fallback_goal=fallback_goal,
        limit=limit,
        normalize_agent_state_fn=lambda s, fallback_goal: _normalize_agent_state(s, fallback_goal=fallback_goal),
        short_text_fn=_short_text,
    )


def _agent_state_prompt_message(state: Optional[Dict[str, Any]], *, fallback_goal: str) -> str:
    return state_store.agent_state_prompt_message(
        state,
        fallback_goal=fallback_goal,
        prompt_max_chars=AGENT_STATE_PROMPT_MAX_CHARS,
        compact_agent_state_json_fn=_compact_agent_state_json,
    )


def _agent_state_hash(state: Optional[Dict[str, Any]], *, fallback_goal: str) -> str:
    return state_store.agent_state_hash(
        state,
        fallback_goal=fallback_goal,
        ledger_max_chars=AGENT_STATE_LEDGER_MAX_CHARS,
        compact_agent_state_json_fn=_compact_agent_state_json,
    )


def _load_persistent_agent_state(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
) -> Optional[Dict[str, Any]]:
    del redis_client, platform, scope
    # Stateless-by-turn mode: do not carry Hydra state across user turns.
    return None


def _save_persistent_agent_state(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
    state: Optional[Dict[str, Any]],
) -> None:
    del redis_client, platform, scope, state
    # Stateless-by-turn mode: do not persist Hydra state between turns.
    return None


def _new_agent_state(goal: str) -> Dict[str, Any]:
    return state_core_helpers.new_agent_state(
        goal,
        normalize_agent_state_fn=_normalize_agent_state,
    )


def _initial_agent_state_for_turn_from_topic_signal(
    *,
    prior_state: Optional[Dict[str, Any]],
    current_user_text: str,
    turn_request_text: str,
    topic_shift_new_topic: bool,
) -> Dict[str, Any]:
    goal = _turn_goal_for_state(
        current_user_text=current_user_text,
        turn_request_text=turn_request_text,
    )
    if not isinstance(prior_state, dict):
        return _new_agent_state(goal)
    if bool(topic_shift_new_topic):
        return _new_agent_state(goal)
    merged = _normalize_agent_state(prior_state, fallback_goal=goal)
    merged["goal"] = goal
    return merged


def _clear_state_plan_for_new_turn(
    *,
    agent_state: Optional[Dict[str, Any]],
    fallback_goal: str,
) -> Dict[str, Any]:
    state = dict(agent_state) if isinstance(agent_state, dict) else {}
    state["goal"] = _short_text(fallback_goal, limit=180) or _short_text(state.get("goal"), limit=180) or "Fulfill the user request."
    state["plan"] = []
    state["plan_steps"] = []
    state["next_step"] = ""
    return _normalize_agent_state(state, fallback_goal=fallback_goal)


def _state_add_line(state_list: List[str], line: str, *, max_items: int) -> List[str]:
    return thanatos_state.state_add_line(
        state_list,
        line,
        max_items=max_items,
        short_text_fn=_short_text,
    )


def _tool_history_line(
    *,
    tool_call: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
) -> str:
    return thanatos_state.tool_history_line(
        tool_call=tool_call,
        tool_result=tool_result,
        short_text_fn=_short_text,
    )


def _compact_tool_result_for_thanatos(tool_result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return thanatos_state.compact_tool_result_for_thanatos(
        tool_result,
        short_text_fn=_short_text,
    )


async def _run_thanatos_state_update(
    *,
    llm_client: Any,
    platform: str,
    user_request: str,
    prior_state: Optional[Dict[str, Any]],
    tool_call: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    return await thanatos_state.run_thanatos_state_update(
        llm_client=llm_client,
        platform=platform,
        user_request=user_request,
        prior_state=prior_state,
        tool_call=tool_call,
        tool_result=tool_result,
        max_tokens=max_tokens,
        normalize_agent_state_fn=lambda s, fallback_goal: _normalize_agent_state(s, fallback_goal=fallback_goal),
        configured_thanatos_max_tokens_fn=(lambda: None),
        coerce_text_fn=_coerce_text,
        first_json_object_fn=_first_json_object,
        state_add_line_fn=lambda items, line, max_items: _state_add_line(items, line, max_items=max_items),
        tool_history_line_fn=lambda call, result: _tool_history_line(tool_call=call, tool_result=result),
        short_text_fn=_short_text,
        is_low_information_text_fn=_is_low_information_text,
        state_list_fn=lambda values, max_items, item_limit: _state_list(values, max_items=max_items, item_limit=item_limit),
    )


def _state_first_open_question(state: Optional[Dict[str, Any]]) -> str:
    return thanatos_state.state_first_open_question(
        state,
        short_text_fn=_short_text,
    )


def _state_best_effort_answer(
    *,
    state: Optional[Dict[str, Any]],
    draft_response: str,
    tool_result: Optional[Dict[str, Any]],
) -> str:
    return thanatos_state.state_best_effort_answer(
        state=state,
        draft_response=draft_response,
        tool_result=tool_result,
        short_text_fn=_short_text,
        is_low_information_text_fn=_is_low_information_text,
    )


def _agent_state_has_remaining_actions(state: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(state, dict):
        return False
    plan_items = _state_list(state.get("plan"), max_items=8, item_limit=140)
    if any(str(item or "").strip() for item in plan_items):
        return True
    return bool(_state_next_step(state.get("next_step")))


def _should_continue_after_incomplete_final_answer(
    *,
    user_text: str,
    final_text: str,
    agent_state: Optional[Dict[str, Any]],
    retry_allowed: bool,
) -> bool:
    del user_text, final_text
    return bool(retry_allowed and _agent_state_has_remaining_actions(agent_state))


def _tool_failure_minos_reason(tool_result: Optional[Dict[str, Any]]) -> str:
    if not isinstance(tool_result, dict):
        return ""
    if bool(tool_result.get("ok")):
        return ""
    code = ""
    data = tool_result.get("data")
    if isinstance(data, dict):
        err = data.get("error")
        if isinstance(err, dict):
            code = str(err.get("code") or "").strip().lower()
    if code:
        return f"tool_failed:{code}"
    return "tool_failed"


def _tool_failure_checker_reason(tool_result: Optional[Dict[str, Any]]) -> str:
    return _tool_failure_minos_reason(tool_result)


def _select_final_answer_text(
    *,
    checker_decision: Optional[Dict[str, Any]],
    draft_response: str,
    user_text: str,
    tool_result: Optional[Dict[str, Any]],
) -> str:
    checker_text = str(((checker_decision or {}).get("text")) or "").strip()
    draft = str(draft_response or "").strip()
    candidate = checker_text or draft or DEFAULT_CLARIFICATION

    if (
        checker_text
        and draft
        and isinstance(tool_result, dict)
        and bool(tool_result.get("ok"))
        and not _is_low_information_text(draft)
    ):
        if checker_text == DEFAULT_CLARIFICATION:
            return draft

    return candidate


async def _synthesize_completed_steps_answer(
    *,
    llm_client: Any,
    platform: str,
    user_text: str,
    goal: str,
    completed_steps: List[Dict[str, str]],
    draft_response: str,
    platform_preamble: str,
    max_tokens: Optional[int],
) -> str:
    if not completed_steps:
        return ""

    findings: List[Dict[str, str]] = []
    for step in completed_steps[:8]:
        if not isinstance(step, dict):
            continue
        request_text = " ".join(str(step.get("request") or "").split()).strip()
        summary_text = " ".join(str(step.get("summary") or "").split()).strip()
        if not summary_text:
            continue
        findings.append(
            {
                "request": request_text,
                "summary": summary_text,
            }
        )
    if not findings:
        return ""

    payload = {
        "user_request": str(user_text or "").strip(),
        "goal": str(goal or "").strip(),
        "findings": findings,
        "draft_response": str(draft_response or "").strip(),
    }
    messages: List[Dict[str, Any]] = [
        {
            "role": "system",
            "content": prompts.hermes_synthesis_system_prompt(),
        },
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    messages = _with_platform_preamble(messages, platform_preamble=platform_preamble)
    try:
        resp = await llm_client.chat(
            messages=messages,
            temperature=0.2,
            **_chat_with_optional_max_tokens_kwargs(
                max_tokens=max_tokens,
                minimum=220,
                fallback=700,
            ),
        )
    except Exception:
        return ""

    text = _coerce_text((resp.get("message", {}) or {}).get("content", "")).strip()
    if not text:
        return ""
    if looks_like_tool_markup(text) or parse_function_json(text):
        return ""
    return text


async def _run_hermes_final_render(
    *,
    llm_client: Any,
    platform: str,
    user_text: str,
    goal: str,
    mode: str,
    instruction: str,
    base_text: str,
    completed_steps: List[Dict[str, str]],
    full_tool_results: List[Dict[str, Any]],
    recent_history: List[Dict[str, Any]],
    core_context_message: str,
    platform_preamble: str,
    max_tokens: Optional[int],
) -> str:
    render_mode = _HERMES_RENDER_MODE_ALIASES.get(str(mode or "").strip().lower(), "direct")
    if render_mode not in {"direct", "summarize", "rewrite"}:
        render_mode = "direct"
    now_text = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    first, last = get_tater_name()
    personality = (get_tater_personality() or "").strip()

    findings: List[Dict[str, str]] = []
    for step in completed_steps[:8]:
        if not isinstance(step, dict):
            continue
        request_text = _short_text(step.get("request"), limit=200)
        summary_text = _short_text(step.get("summary"), limit=220)
        if not summary_text:
            continue
        findings.append(
            {
                "request": request_text,
                "summary": summary_text,
            }
        )
    full_results_payload: List[Dict[str, Any]] = []
    for result in full_tool_results:
        if not isinstance(result, dict):
            continue
        full_results_payload.append(dict(result))
    history_payload: List[Dict[str, str]] = []
    for item in recent_history:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = _short_text(item.get("content"), limit=260)
        if not content:
            continue
        history_payload.append({"role": role, "content": content})

    payload = {
        "mode": render_mode,
        "instruction": _short_text(instruction, limit=320),
        "current_user_message": _short_text(user_text, limit=320),
        "recent_history": history_payload,
        "user_request": _short_text(user_text, limit=320),
        "goal": _short_text(goal, limit=260),
        "base_text": _short_text(base_text, limit=2200),
        "findings": findings,
        "tool_results_full": full_results_payload,
    }
    messages: List[Dict[str, Any]] = [
        {
            "role": "system",
            "content": prompts.hermes_final_render_system_prompt(
                now_text=now_text,
                first_name=first,
                last_name=last,
                personality=personality,
                platform=platform,
                ascii_only_platforms=ASCII_ONLY_PLATFORMS,
            ),
        },
    ]
    if core_context_message:
        messages.append({"role": "system", "content": core_context_message})
    messages.append({"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)})
    messages = _with_platform_preamble(messages, platform_preamble=platform_preamble)
    try:
        resp = await llm_client.chat(
            messages=messages,
            temperature=0.2,
            **_chat_with_optional_max_tokens_kwargs(
                max_tokens=max_tokens,
                minimum=120,
                fallback=420,
            ),
        )
    except Exception:
        return str(base_text or "").strip()

    text = _coerce_text((resp.get("message", {}) or {}).get("content", "")).strip()
    if not text:
        return str(base_text or "").strip()
    if looks_like_tool_markup(text) or parse_function_json(text):
        return str(base_text or "").strip()
    return text


def _turn_completion_fragment(*, request_text: str, summary_text: str) -> str:
    request = _short_text(" ".join(str(request_text or "").split()), limit=140)
    summary = _short_text(" ".join(str(summary_text or "").split()), limit=140)
    if summary:
        request_norm = request.rstrip(".!?").strip().lower()
        summary_norm = summary.rstrip(".!?").strip().lower()
        if request_norm and summary_norm and request_norm != summary_norm:
            if request_norm not in summary_norm and summary_norm not in request_norm:
                return f"{request} ({summary})" if request else summary
        return summary
    return request


async def _tool_start_progress(
    *,
    llm_client: Any,
    platform: str,
    tool_call: Optional[Dict[str, Any]],
    round_request_text: str,
    current_plan_step: Optional[Dict[str, str]],
    completed_steps_count: int,
    total_plan_steps: int,
    platform_preamble: str = "",
    max_tokens: Optional[int] = 56,
) -> tuple[str, Dict[str, Any]]:
    tool_name = _canonical_tool_name(str((tool_call or {}).get("function") or "").strip()) or "tool"
    instruction_source = (
        str((current_plan_step or {}).get("nl") or "").strip()
        if isinstance(current_plan_step, dict)
        else ""
    )
    instruction = _short_text(" ".join(str(instruction_source or round_request_text or "").split()), limit=200)
    instruction = instruction.lstrip()
    if instruction.lower().startswith("to "):
        instruction = instruction[3:].lstrip()
    instruction = instruction.rstrip(".!?")

    step_total = max(0, int(total_plan_steps or 0))
    step_index = 0
    if step_total > 0:
        step_index = min(step_total, max(1, int(completed_steps_count or 0) + 1))

    stage = ""
    if step_total > 1:
        if step_index <= 1:
            stage = "first"
        elif step_index >= step_total:
            stage = "final"
        else:
            stage = "next"

    progress_prompt_payload: Dict[str, Any] = {
        "tool": tool_name,
        "instruction": instruction or round_request_text or "",
        "step_index": step_index if step_index > 0 else None,
        "step_total": step_total if step_total > 0 else None,
        "stage": stage or None,
        "execution_phase": "before_tool_execution_no_results_available",
    }
    progress_messages: List[Dict[str, Any]] = [
        {
            "role": "system",
            "content": prompts.tool_start_progress_system_prompt(),
        },
        {
            "role": "user",
            "content": json.dumps(progress_prompt_payload, ensure_ascii=False),
        },
    ]
    progress_messages = _with_platform_preamble(
        progress_messages,
        platform_preamble=platform_preamble,
    )
    text = ""
    try:
        progress_resp = await llm_client.chat(
            messages=progress_messages,
            temperature=0.1,
            **_chat_with_optional_max_tokens_kwargs(
                max_tokens=max_tokens,
                minimum=56,
                fallback=56,
            ),
        )
        text = _coerce_text((progress_resp.get("message", {}) or {}).get("content", "")).strip()
    except Exception:
        text = ""
    text = _short_text(" ".join(str(text or "").split()), limit=220)
    if looks_like_tool_markup(text) or parse_function_json(text) is not None:
        text = ""
    if platform in ASCII_ONLY_PLATFORMS:
        text = text.encode("ascii", "ignore").decode().strip()
    if not text:
        text = "I'm working on that now."

    payload: Dict[str, Any] = {
        "phase": "tool_start",
        "tool": tool_name,
        "text": text,
        "source": "llm",
    }
    step_id = _short_text((current_plan_step or {}).get("id"), limit=24)
    if step_id:
        payload["step_id"] = step_id
    if instruction:
        payload["instruction"] = instruction
    if step_index > 0 and step_total > 0:
        payload["step_index"] = step_index
        payload["step_total"] = step_total
    return text, payload


def _multi_step_turn_draft(
    *,
    completed_steps: List[Dict[str, str]],
    fallback_draft: str,
) -> str:
    if len(completed_steps) <= 1:
        return str(fallback_draft or "").strip()

    lines: List[str] = []
    seen: set[tuple[str, str]] = set()
    for step in completed_steps[:6]:
        if not isinstance(step, dict):
            continue
        request = " ".join(str(step.get("request") or "").split()).strip()
        summary = " ".join(str(step.get("summary") or "").split()).strip()
        if not summary:
            continue
        req_key = request.rstrip(".!?").strip().lower()
        sum_key = summary.rstrip(".!?").strip().lower()
        dedupe_key = (req_key, sum_key)
        if not sum_key or dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        if request:
            request_norm = req_key
            summary_norm = sum_key
            if (
                request_norm
                and summary_norm
                and request_norm != summary_norm
                and request_norm not in summary_norm
                and summary_norm not in request_norm
            ):
                lines.append(f"- {request}: {summary.rstrip('.!?')}.")
                continue
        lines.append(f"- {summary.rstrip('.!?')}.")

    if not lines:
        return str(fallback_draft or "").strip()

    body = "\n".join(lines)
    if len(lines) == 1:
        return lines[0].lstrip("- ").strip()
    return "Here is what I found:\n" + body


def _artifact_name_from_path(path: Any) -> str:
    raw = str(path or "").strip().replace("\\", "/")
    if not raw:
        return ""
    return raw.rsplit("/", 1)[-1].strip()


def _artifact_type_from_mimetype(mimetype: Any) -> str:
    mime = str(mimetype or "").strip().lower()
    if mime.startswith("image/"):
        return "image"
    if mime.startswith("audio/"):
        return "audio"
    if mime.startswith("video/"):
        return "video"
    return "file"


def _normalize_turn_artifact(payload: Any, *, default_source: str = "") -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None

    path = str(payload.get("path") or "").strip()
    blob_key = str(payload.get("blob_key") or "").strip()
    file_id = str(payload.get("file_id") or payload.get("id") or "").strip()
    url = str(payload.get("url") or "").strip()
    if not any((path, blob_key, file_id, url)):
        return None

    name = str(payload.get("name") or "").strip() or _artifact_name_from_path(path) or "file.bin"
    mimetype_value = str(payload.get("mimetype") or "").strip().lower()
    if not mimetype_value:
        guessed = str(mimetypes.guess_type(name or path)[0] or "").strip().lower()
        mimetype_value = guessed or "application/octet-stream"

    artifact_type = str(payload.get("type") or "").strip().lower()
    if artifact_type not in {"image", "audio", "video", "file"}:
        artifact_type = _artifact_type_from_mimetype(mimetype_value)

    out: Dict[str, Any] = {
        "artifact_id": str(payload.get("artifact_id") or "").strip(),
        "type": artifact_type,
        "name": name,
        "mimetype": mimetype_value,
        "source": str(payload.get("source") or default_source or "artifact").strip() or "artifact",
    }
    for key, value in (("path", path), ("blob_key", blob_key), ("file_id", file_id), ("url", url)):
        if value:
            out[key] = value
    try:
        size_value = int(payload.get("size"))
    except Exception:
        size_value = -1
    if size_value >= 0:
        out["size"] = size_value
    return out


def _turn_artifact_key(item: Dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(item.get("type") or "").strip().lower(),
        str(item.get("path") or "").strip(),
        str(item.get("blob_key") or "").strip(),
        str(item.get("file_id") or "").strip(),
        str(item.get("url") or "").strip(),
    )


def _merge_turn_artifacts(
    existing: List[Dict[str, Any]],
    incoming: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen = set()
    used_ids = set()
    next_index = 1

    for raw_item in list(existing or []) + list(incoming or []):
        item = _normalize_turn_artifact(raw_item)
        if item is None:
            continue
        dedupe_key = _turn_artifact_key(item)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        artifact_id = str(item.get("artifact_id") or "").strip()
        if artifact_id:
            used_ids.add(artifact_id)
        else:
            while f"a{next_index}" in used_ids:
                next_index += 1
            artifact_id = f"a{next_index}"
            used_ids.add(artifact_id)
            next_index += 1
        item["artifact_id"] = artifact_id
        merged.append(item)
    return merged[:16]


def _turn_artifacts_from_tool_payload(payload: Any) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict) or not bool(payload.get("ok")):
        return []

    tool_name = str(payload.get("tool") or "").strip().lower()
    out: List[Dict[str, Any]] = []

    direct_artifact = payload.get("artifact")
    if isinstance(direct_artifact, dict):
        raw_direct_artifact = dict(direct_artifact)
        raw_direct_artifact.setdefault("source", tool_name)
        out.append(raw_direct_artifact)

    raw_artifacts = payload.get("artifacts")
    if isinstance(raw_artifacts, list):
        for item in raw_artifacts:
            if not isinstance(item, dict):
                continue
            raw_artifact = dict(item)
            raw_artifact.setdefault("source", tool_name)
            out.append(raw_artifact)

    if tool_name in {"download_file", "write_file"}:
        artifact = _normalize_turn_artifact(
            {
                "path": payload.get("path"),
                "name": payload.get("name") or _artifact_name_from_path(payload.get("path")),
                "mimetype": payload.get("content_type"),
                "source": tool_name,
                "size": payload.get("bytes"),
            },
            default_source=tool_name,
        )
        if artifact is not None:
            out.append(artifact)

    if tool_name == "extract_archive":
        extracted = payload.get("extracted")
        if isinstance(extracted, list):
            for item in extracted:
                artifact = _normalize_turn_artifact(
                    {
                        "path": item,
                        "name": _artifact_name_from_path(item),
                        "source": tool_name,
                    },
                    default_source=tool_name,
                )
                if artifact is not None:
                    out.append(artifact)

    return out


def _available_artifacts_prompt(available_artifacts: List[Dict[str, Any]]) -> str:
    if not available_artifacts:
        return ""
    lines = ["Available artifacts for this conversation (current turn + saved conversation files):"]
    for item in available_artifacts[:12]:
        artifact_id = str(item.get("artifact_id") or "").strip()
        artifact_type = str(item.get("type") or "").strip() or "file"
        name = _short_text(item.get("name"), limit=100) or "file"
        source = _short_text(item.get("source"), limit=48)
        path_value = _short_text(item.get("path"), limit=140)
        parts = [artifact_id, artifact_type, name]
        if source:
            parts.append(f"source={source}")
        if path_value:
            parts.append(f"path={path_value}")
        lines.append("- " + " | ".join([part for part in parts if part]))
    lines.append("Use the exact artifact_id or exact path from this list when a tool needs a file or image. Never invent artifact ids.")
    return "\n".join(lines)


def _available_artifacts_payload(available_artifacts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in available_artifacts[:12]:
        if not isinstance(item, dict):
            continue
        compact: Dict[str, Any] = {
            "artifact_id": str(item.get("artifact_id") or "").strip(),
            "type": str(item.get("type") or "").strip(),
            "name": str(item.get("name") or "").strip(),
        }
        for key in ("mimetype", "source", "path", "size"):
            if item.get(key) not in (None, ""):
                compact[key] = item.get(key)
        out.append(compact)
    return out


def _image_like_artifact(item: Dict[str, Any]) -> bool:
    if not isinstance(item, dict):
        return False
    artifact_type = str(item.get("type") or "").strip().lower()
    if artifact_type == "image":
        return True
    mimetype = str(item.get("mimetype") or "").strip().lower()
    return mimetype.startswith("image/")


def _select_image_artifact_for_call(
    *,
    available_artifacts: List[Dict[str, Any]],
    hinted_value: str = "",
) -> Optional[Dict[str, Any]]:
    if not isinstance(available_artifacts, list) or not available_artifacts:
        return None

    candidates = [dict(item) for item in available_artifacts if _image_like_artifact(item)]
    if not candidates:
        return None

    hinted = str(hinted_value or "").strip().lower()
    if hinted:
        for item in candidates:
            artifact_id = str(item.get("artifact_id") or "").strip().lower()
            name = str(item.get("name") or "").strip().lower()
            if hinted == artifact_id or hinted == name:
                return item
            if name and hinted in name:
                return item

    if len(candidates) == 1:
        return candidates[0]
    return candidates[0]


def _autofix_image_describe_tool_call(
    tool_call: Optional[Dict[str, Any]],
    available_artifacts: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not isinstance(tool_call, dict):
        return tool_call
    func = str(tool_call.get("function") or "").strip().lower()
    if func != "image_describe":
        return tool_call

    args = dict(tool_call.get("arguments") or {}) if isinstance(tool_call.get("arguments"), dict) else {}
    explicit_refs = any(
        str(args.get(key) or "").strip()
        for key in ("url", "path", "blob_key", "file_id")
    )
    if explicit_refs:
        return tool_call

    artifact_id = str(args.get("artifact_id") or "").strip()
    if artifact_id:
        for item in available_artifacts or []:
            if str(item.get("artifact_id") or "").strip() == artifact_id:
                return tool_call

    selected = _select_image_artifact_for_call(
        available_artifacts=available_artifacts,
        hinted_value=artifact_id,
    )
    if not isinstance(selected, dict):
        return tool_call

    selected_id = str(selected.get("artifact_id") or "").strip()
    if not selected_id:
        return tool_call

    args["artifact_id"] = selected_id
    return {"function": str(tool_call.get("function") or ""), "arguments": args}


_RESULT_MEMORY_LIST_KEYS = (
    "results",
    "items",
    "entries",
    "matches",
    "links",
    "rows",
    "candidates",
    "files",
    "options",
    "values",
)
_RESULT_MEMORY_TITLE_KEYS = ("title", "name", "label", "display_name", "filename")
_RESULT_MEMORY_LOCATOR_KEYS = ("magnet", "url", "link", "href", "uri", "path", "id")
_RESULT_MEMORY_PREVIEW_KEYS = ("snippet", "summary", "description", "text", "message", "content")


def _first_non_empty_text(mapping: Dict[str, Any], keys: tuple[str, ...], *, limit: int = 0) -> str:
    for key in keys:
        text = _short_text(mapping.get(key), limit=limit or 260)
        if text:
            return text
    return ""


def _normalize_result_memory_item(raw_item: Any, *, rank: int) -> Optional[Dict[str, str]]:
    if isinstance(raw_item, dict):
        title = _first_non_empty_text(raw_item, _RESULT_MEMORY_TITLE_KEYS, limit=140)
        locator = _first_non_empty_text(raw_item, _RESULT_MEMORY_LOCATOR_KEYS, limit=260)
        preview = _first_non_empty_text(raw_item, _RESULT_MEMORY_PREVIEW_KEYS, limit=180)
    elif isinstance(raw_item, str):
        title = ""
        locator = ""
        preview = _short_text(raw_item, limit=180)
    else:
        title = ""
        locator = ""
        preview = _short_text(raw_item, limit=180)

    if not any((title, locator, preview)):
        return None
    item_ref = f"#{max(1, int(rank or 1))}"
    out: Dict[str, str] = {"item_ref": item_ref}
    if title:
        out["title"] = title
    if locator:
        out["locator"] = locator
    if preview and preview != title:
        out["preview"] = preview
    if len(out) <= 1:
        return None
    return out


def _extract_result_memory_items(payload: Optional[Dict[str, Any]], *, max_items: int = 8) -> List[Dict[str, str]]:
    source = payload if isinstance(payload, dict) else {}
    max_items = max(1, int(max_items or 8))

    candidates: List[List[Any]] = []
    for key in _RESULT_MEMORY_LIST_KEYS:
        value = source.get(key)
        if isinstance(value, list) and value:
            candidates.append(value)
    data_blob = source.get("data")
    if isinstance(data_blob, dict):
        for key in _RESULT_MEMORY_LIST_KEYS:
            value = data_blob.get(key)
            if isinstance(value, list) and value:
                candidates.append(value)

    best: List[Dict[str, str]] = []
    for sequence in candidates:
        parsed: List[Dict[str, str]] = []
        for idx, raw_item in enumerate(sequence, start=1):
            normalized = _normalize_result_memory_item(raw_item, rank=idx)
            if normalized is None:
                continue
            parsed.append(normalized)
            if len(parsed) >= max_items:
                break
        if len(parsed) > len(best):
            best = parsed
        if len(best) >= max_items:
            break
    if best:
        return best[:max_items]

    text_lines: List[str] = []
    for key in ("summary_for_user", "summary", "answer", "message", "text", "content", "description"):
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            text_lines.extend([line.strip() for line in value.splitlines() if line.strip()])
    if isinstance(data_blob, dict):
        for key in ("summary", "message", "text", "content", "description"):
            value = data_blob.get(key)
            if isinstance(value, str) and value.strip():
                text_lines.extend([line.strip() for line in value.splitlines() if line.strip()])

    out: List[Dict[str, str]] = []
    for idx, line in enumerate(text_lines, start=1):
        normalized = _normalize_result_memory_item(line, rank=idx)
        if normalized is None:
            continue
        out.append(normalized)
        if len(out) >= max_items:
            break
    return out


def _result_memory_prompt(result_memory: List[Dict[str, Any]]) -> str:
    if not result_memory:
        return ""
    lines = ["Remembered tool result sets (conversation memory for follow-up references):"]
    for entry in result_memory[:4]:
        if not isinstance(entry, dict):
            continue
        result_set_id = _short_text(entry.get("result_set_id"), limit=18) or "rs?"
        tool_name = _short_text(entry.get("tool"), limit=50) or "tool"
        query = _short_text(entry.get("query"), limit=140)
        summary = _short_text(entry.get("summary"), limit=140)
        header = [f"result_set_id={result_set_id}", f"tool={tool_name}"]
        if query:
            header.append(f"query={query}")
        elif summary:
            header.append(f"summary={summary}")
        lines.append("- " + " | ".join(header))
        items = entry.get("items") if isinstance(entry.get("items"), list) else []
        for item in items[:6]:
            if not isinstance(item, dict):
                continue
            item_ref = _short_text(item.get("item_ref"), limit=12) or "?"
            title = _short_text(item.get("title") or item.get("preview"), limit=120) or "(no title)"
            locator = _short_text(item.get("locator"), limit=220)
            if locator:
                lines.append(f"  - {item_ref}: {title} -> {locator}")
            else:
                lines.append(f"  - {item_ref}: {title}")
    lines.append("Use this memory when the user references previous results/items/links. Prefer the most recent relevant result set.")
    return "\n".join(lines)


def _thanatos_previous_step_payloads_prompt(previous_payloads: List[Dict[str, Any]]) -> str:
    if not isinstance(previous_payloads, list) or not previous_payloads:
        return ""
    lines = ["Tool results so far this turn (full payloads, execution order):"]
    kept = 0
    for idx, payload in enumerate(previous_payloads, start=1):
        if not isinstance(payload, dict) or not payload:
            continue
        try:
            payload_json = json.dumps(payload, ensure_ascii=False, default=str)
        except Exception:
            payload_json = "{}"
        if payload_json in {"{}", "null"}:
            continue
        lines.append(f"Result {idx} full payload JSON:\n{payload_json}")
        kept += 1
    if kept == 0:
        return ""
    lines.append(
        "Use this info if needed for the current step, especially when you need exact links, ids, URIs, or other values."
    )
    return "\n".join(lines)


def _next_result_set_id(result_memory: List[Dict[str, Any]]) -> str:
    highest = 0
    for entry in result_memory:
        if not isinstance(entry, dict):
            continue
        token = str(entry.get("result_set_id") or "").strip().lower()
        if not token.startswith("rs"):
            continue
        suffix = token[2:]
        if suffix.isdigit():
            highest = max(highest, int(suffix))
    return f"rs{highest + 1}"


def _result_memory_signature(entry: Dict[str, Any]) -> str:
    if not isinstance(entry, dict):
        return ""
    parts = [
        str(entry.get("tool") or "").strip().lower(),
        str(entry.get("query") or "").strip().lower(),
    ]
    items = entry.get("items") if isinstance(entry.get("items"), list) else []
    for item in items[:3]:
        if not isinstance(item, dict):
            continue
        locator = str(item.get("locator") or "").strip().lower()
        title = str(item.get("title") or item.get("preview") or "").strip().lower()
        if locator:
            parts.append(locator)
        elif title:
            parts.append(title)
    return "|".join([part for part in parts if part])


def _remember_tool_result_in_agent_state(
    *,
    agent_state: Optional[Dict[str, Any]],
    tool_call: Optional[Dict[str, Any]],
    raw_payload: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    request_text: str,
    fallback_goal: str,
    max_sets: int,
    max_items: int,
) -> Dict[str, Any]:
    source = dict(agent_state) if isinstance(agent_state, dict) else {}
    memory = _state_result_memory(source.get("result_memory"), max_sets=max_sets, max_items=max_items)
    normalized = _normalize_agent_state(source, fallback_goal=fallback_goal or request_text)
    if not memory:
        normalized["result_memory"] = []
    else:
        normalized["result_memory"] = memory

    if not (isinstance(tool_result, dict) and bool(tool_result.get("ok"))):
        return normalized

    payload = raw_payload if isinstance(raw_payload, dict) else {}
    items = _extract_result_memory_items(payload, max_items=max_items)
    summary = _short_text((tool_result or {}).get("summary_for_user"), limit=180)
    if not items and not summary:
        return normalized

    tool_name = _short_text(
        (tool_call or {}).get("function") or payload.get("tool") or "tool",
        limit=64,
    ) or "tool"
    query = _first_non_empty_text(payload, ("query", "q", "request", "prompt", "search_query"), limit=180)
    request = _short_text(request_text, limit=180)
    entry: Dict[str, Any] = {
        "result_set_id": _next_result_set_id(memory),
        "tool": tool_name,
    }
    if query:
        entry["query"] = query
    if request:
        entry["request"] = request
    if summary:
        entry["summary"] = summary
    if items:
        entry["items"] = items

    signature = _result_memory_signature(entry)
    merged_memory: List[Dict[str, Any]] = [entry]
    for existing in memory:
        if not isinstance(existing, dict):
            continue
        if signature and _result_memory_signature(existing) == signature:
            continue
        merged_memory.append(existing)
    normalized["result_memory"] = _state_result_memory(
        merged_memory[:max_sets],
        max_sets=max_sets,
        max_items=max_items,
    )
    return _normalize_agent_state(
        normalized,
        fallback_goal=fallback_goal or request_text or str(normalized.get("goal") or ""),
    )


_BAD_ARGS_FAILURE_CODES = retry_helpers.BAD_ARGS_FAILURE_CODES

_BAD_ARGS_FAILURE_TEXT_MARKERS = retry_helpers.BAD_ARGS_FAILURE_TEXT_MARKERS


def _tool_failure_code_and_text(
    *,
    tool_result: Optional[Dict[str, Any]],
    payload: Optional[Dict[str, Any]],
) -> tuple[str, str]:
    return retry_helpers.tool_failure_code_and_text(
        tool_result=tool_result,
        payload=payload,
    )


def _looks_like_bad_args_plugin_failure(
    *,
    tool_call: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    payload: Optional[Dict[str, Any]],
    registry: Dict[str, Any],
) -> tuple[bool, str]:
    return retry_helpers.looks_like_bad_args_plugin_failure(
        tool_call=tool_call,
        tool_result=tool_result,
        payload=payload,
        registry=registry,
        plugin_tool_id_for_call_fn=_plugin_tool_id_for_call,
        bad_args_failure_codes=_BAD_ARGS_FAILURE_CODES,
        bad_args_failure_text_markers=_BAD_ARGS_FAILURE_TEXT_MARKERS,
    )


def _help_arg_names(help_payload: Optional[Dict[str, Any]]) -> List[str]:
    return retry_helpers.help_arg_names(
        help_payload,
        parse_function_json_fn=parse_function_json,
    )


def _constrain_args_from_plugin_help(
    *,
    args: Optional[Dict[str, Any]],
    help_payload: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return retry_helpers.constrain_args_from_plugin_help(
        args=args,
        help_payload=help_payload,
        help_arg_names_fn=_help_arg_names,
    )


def _build_help_constrained_retry_tool_call(
    *,
    failed_tool_call: Optional[Dict[str, Any]],
    help_payload: Optional[Dict[str, Any]],
    registry: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    return retry_helpers.build_help_constrained_retry_tool_call(
        failed_tool_call=failed_tool_call,
        help_payload=help_payload,
        registry=registry,
        plugin_tool_id_for_call_fn=_plugin_tool_id_for_call,
        constrain_args_from_plugin_help_fn=lambda args, payload: _constrain_args_from_plugin_help(
            args=args,
            help_payload=payload,
        ),
    )


def _build_overwrite_retry_tool_call(
    *,
    tool_call: Optional[Dict[str, Any]],
    payload: Optional[Dict[str, Any]],
    user_text: str,
) -> Optional[Dict[str, Any]]:
    del tool_call, payload, user_text
    return None


def _hash_tool_args(args: Any) -> str:
    return ledger.hash_tool_args(args)


def _compact_tool_ref(tool_call: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return ledger.compact_tool_ref(
        tool_call,
        hash_tool_args_fn=_hash_tool_args,
    )


def _validation_status_for_ledger(
    *,
    validation_status: Optional[Dict[str, Any]],
    planned_tool: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return ledger.validation_status_for_ledger(
        validation_status=validation_status,
        planned_tool=planned_tool,
        short_text_fn=_short_text,
    )


def _llm_backend_label(llm_client: Any) -> str:
    return turn_utils.llm_backend_label(
        llm_client,
        short_text_fn=_short_text,
    )


def _origin_preview_for_ledger(origin: Optional[Dict[str, Any]]) -> Dict[str, str]:
    return turn_utils.origin_preview_for_ledger(
        origin,
        short_text_fn=_short_text,
    )


def _normalize_outcome(status: str, checker_reason: str) -> tuple[str, str]:
    return turn_utils.normalize_outcome(
        status,
        checker_reason,
        short_text_fn=_short_text,
    )


def _write_hydra_metrics(
    *,
    redis_client: Any,
    platform: str,
    total_tools_called: int,
    total_repairs: int,
    validation_failures: int,
    tool_failures: int,
) -> None:
    return ledger.write_hydra_metrics(
        redis_client=redis_client,
        platform=platform,
        total_tools_called=total_tools_called,
        total_repairs=total_repairs,
        validation_failures=validation_failures,
        tool_failures=tool_failures,
        normalize_platform_fn=normalize_platform,
    )


def _write_hydra_ledger(
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
) -> None:
    return ledger.write_hydra_ledger(
        redis_client=redis_client,
        platform=platform,
        scope=scope,
        turn_id=turn_id,
        llm=llm,
        user_message=user_message,
        planned_tool=planned_tool,
        validation_status=validation_status,
        tool_result=tool_result,
        checker_action=checker_action,
        retry_count=retry_count,
        checker_reason=checker_reason,
        planner_kind=planner_kind,
        planner_text_is_tool_candidate=planner_text_is_tool_candidate,
        outcome=outcome,
        outcome_reason=outcome_reason,
        planner_ms=planner_ms,
        tool_ms=tool_ms,
        checker_ms=checker_ms,
        total_ms=total_ms,
        retry_tool=retry_tool,
        rounds_used=rounds_used,
        tool_calls_used=tool_calls_used,
        agent_state=agent_state,
        origin_preview=origin_preview,
        attempted_tool=attempted_tool,
        compact_tool_ref_fn=_compact_tool_ref,
        validation_status_for_ledger_fn=_validation_status_for_ledger,
        short_text_fn=_short_text,
        compact_agent_state_json_fn=_compact_agent_state_json,
        agent_state_hash_fn=_agent_state_hash,
        configured_max_ledger_items_fn=_configured_max_ledger_items,
        schema_version=HYDRA_LEDGER_SCHEMA_VERSION,
        agent_state_ledger_max_chars=AGENT_STATE_LEDGER_MAX_CHARS,
        allowed_planner_kinds=("tool", "answer", "repaired_tool", "repaired_answer"),
    )


def _is_tool_candidate(text: str) -> bool:
    return toolcall_utils.is_tool_candidate(
        text,
        parse_strict_tool_json_fn=_parse_strict_tool_json,
        parse_function_json_fn=parse_function_json,
        looks_like_tool_markup_fn=looks_like_tool_markup,
        looks_like_invalid_tool_call_text_fn=_looks_like_invalid_tool_call_text,
    )


async def _run_hydra_turn_impl(
    *,
    llm_client: Any,
    llm_clients: Optional[Dict[str, Any]] = None,
    platform: str,
    history_messages: List[Dict[str, Any]],
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]] = None,
    context: Optional[Dict[str, Any]] = None,
    user_text: str,
    scope: str,
    task_id: Optional[str] = None,
    active_job_id: str = "",
    origin: Optional[Dict[str, Any]] = None,
    wait_callback: Optional[Callable[..., Any]] = None,
    admin_guard: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    redis_client: Any = None,
    max_rounds: Optional[int] = None,
    max_tool_calls: Optional[int] = None,
    platform_preamble: str = "",
) -> Dict[str, Any]:
    r = redis_client or default_redis
    platform = normalize_platform(platform)
    origin_payload = dict(origin) if isinstance(origin, dict) else {}
    scope = _resolve_hydra_scope(platform, scope, origin_payload)
    input_artifacts = origin_payload.get("input_artifacts") if isinstance(origin_payload.get("input_artifacts"), list) else []
    if input_artifacts:
        try:
            save_conversation_artifacts(
                r,
                platform=platform,
                scope=scope,
                artifacts=input_artifacts,
            )
        except Exception:
            pass
    try:
        stored_conversation_artifacts = load_conversation_artifacts(
            r,
            platform=platform,
            scope=scope,
            limit=16,
        )
    except Exception:
        stored_conversation_artifacts = []
    turn_available_artifacts = _merge_turn_artifacts(
        stored_conversation_artifacts,
        input_artifacts,
    )
    if turn_available_artifacts:
        origin_payload["available_artifacts"] = [dict(item) for item in turn_available_artifacts]
    platform_preamble = _sanitize_platform_preamble(platform, platform_preamble)
    origin_preview = _origin_preview_for_ledger(origin_payload)
    user_text = str(user_text or "")
    effective_max_rounds, effective_max_tool_calls = resolve_agent_limits(
        redis_client=r,
        max_rounds=max_rounds,
        max_tool_calls=max_tool_calls,
    )
    result_memory_max_sets = _configured_result_memory_max_sets(r)
    result_memory_max_items = _configured_result_memory_max_items(r)
    chat_history_max_items = _configured_chat_history_max_items(r)
    step_retry_limit = _configured_step_retry_limit(r)
    astraeus_plan_review_enabled = _configured_astraeus_plan_review_enabled(r)
    turn_started_at = time.perf_counter()
    astraeus_ms_total = 0.0
    tool_ms_total = 0.0
    minos_ms_total = 0.0
    repairs_used_count = 0
    validation_failures_count = 0
    tool_failures_count = 0
    turn_id = str(uuid.uuid4())
    role_clients = dict(llm_clients) if isinstance(llm_clients, dict) else {}
    llm_client_ai_calls = role_clients.get("ai_calls") or llm_client
    llm_client_chat = role_clients.get("chat") or llm_client
    llm_client_astraeus = role_clients.get("astraeus") or llm_client
    llm_client_thanatos = role_clients.get("thanatos") or llm_client
    llm_client_minos = role_clients.get("minos") or llm_client
    llm_client_hermes = role_clients.get("hermes") or llm_client
    llm_label = _llm_backend_label(llm_client_ai_calls)

    validation_status: Dict[str, Any] = {
        "status": "skipped",
        "repair_used": False,
        "reason": "no_tool",
        "attempts": 0,
    }
    planned_tool: Optional[Dict[str, Any]] = None
    checker_action = "FINAL_ANSWER"
    checker_reason = "complete"
    tool_result_for_checker: Optional[Dict[str, Any]] = None
    raw_tool_payload_out: Optional[Dict[str, Any]] = None
    raw_tool_payload_history: List[Dict[str, Any]] = []
    normalized_minos_result_out: Optional[Dict[str, Any]] = None
    artifacts_out: List[Dict[str, Any]] = []
    rounds_used = 0
    tool_calls_used = 0
    critic_continue_count = 0
    step_retry_counts: Dict[str, int] = {}
    step_retry_hints: Dict[str, str] = {}
    step_failed_source_urls: Dict[str, set[str]] = {}
    step_failed_source_pages: Dict[str, set[str]] = {}
    turn_failed_source_urls: set[str] = set()
    draft_response = ""
    tool_used = False
    planner_kind = "answer"
    planner_text_is_tool_candidate = False
    attempted_tool_for_ledger = ""
    task_name = _task_name_from_text(user_text, fallback="Hydra task")

    history = _compact_history(history_messages)
    hermes_recent_history = _chat_history_window(history, max_items=chat_history_max_items)
    current_user_turn_text = _strip_user_sender_prefix(user_text).strip() or str(user_text or "").strip()
    turn_request_text = current_user_turn_text or str(user_text or "").strip()
    task_name = _task_name_from_text(turn_request_text or user_text, fallback=task_name)
    _set_active_chat_job_task_name(active_job_id, task_name)
    tool_index = _enabled_tool_mini_index(
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
    )
    astraeus_capability_catalog = _astraeus_capability_catalog(
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
    )
    available_execution_tool_ids = _enabled_execution_tool_ids(
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
    )
    prior_state = None
    memory_context_payload = _memory_context_payload(
        redis_client=r,
        platform=platform,
        scope=scope,
        origin=origin_payload,
    )
    chat_context_message = _core_system_prompt_message(
        role="chat",
        platform=platform,
        scope=scope,
        origin=origin_payload,
        redis_client=r,
        memory_context_payload=memory_context_payload,
    )
    hermes_context_message = _core_system_prompt_message(
        role="hermes",
        platform=platform,
        scope=scope,
        origin=origin_payload,
        redis_client=r,
        memory_context_payload=memory_context_payload,
    )
    queued_retry_tool_for_ledger: Optional[Dict[str, Any]] = None
    repair_returned_no_tool_retries = 0
    structured_plan_queue: List[Dict[str, str]] = []
    structured_plan_total_steps = 0
    astraeus_mode = "unknown"
    astraeus_topic = _short_text(turn_request_text or current_user_turn_text, limit=90) or "request"
    astraeus_goal = _short_text(turn_request_text or current_user_turn_text, limit=220) or "Fulfill the user request."
    astraeus_topic_shift = False
    completed_tool_steps: List[Dict[str, str]] = []
    try:
        astraeus_started = time.perf_counter()
        astraeus_result = await _run_astraeus_plan(
            llm_client=llm_client_astraeus,
            platform=platform,
            current_user_text=current_user_turn_text,
            turn_request_text=turn_request_text,
            topic_seed=astraeus_topic,
            topic_shift_seed=astraeus_topic_shift,
            history=history,
            prior_state=prior_state,
            memory_context=memory_context_payload,
            capability_catalog=astraeus_capability_catalog,
            available_tool_ids=available_execution_tool_ids,
            platform_preamble=platform_preamble,
            max_tokens=None,
        )
        astraeus_ms_total += (time.perf_counter() - astraeus_started) * 1000.0
    except Exception:
        astraeus_result = {
            "mode": "unknown",
            "topic_shift": False,
            "goal": astraeus_goal,
            "steps": [],
        }
    if isinstance(astraeus_result, dict):
        astraeus_mode = str(astraeus_result.get("mode") or "unknown").strip().lower() or "unknown"
        astraeus_topic = _short_text(astraeus_result.get("topic"), limit=90) or astraeus_topic
        astraeus_goal = _short_text(astraeus_result.get("goal"), limit=220) or astraeus_goal
        task_name = _task_name_from_text(astraeus_topic or astraeus_goal, fallback=task_name)
        _set_active_chat_job_task_name(active_job_id, task_name)
        raw_steps = astraeus_result.get("steps")
        if isinstance(raw_steps, list):
            structured_plan_queue = [step for step in raw_steps if isinstance(step, dict)]
    if structured_plan_queue and astraeus_plan_review_enabled:
        plan_review_started = time.perf_counter()
        reviewed_plan = await _review_execution_plan_for_completeness(
            llm_client=llm_client_astraeus,
            platform=platform,
            current_user_text=current_user_turn_text,
            turn_request_text=turn_request_text,
            goal=astraeus_goal or turn_request_text or current_user_turn_text,
            steps=structured_plan_queue,
            capability_catalog=astraeus_capability_catalog,
            available_tool_ids=available_execution_tool_ids,
            platform_preamble=platform_preamble,
            max_tokens=None,
        )
        astraeus_ms_total += (time.perf_counter() - plan_review_started) * 1000.0
        if isinstance(reviewed_plan, dict):
            reviewed_goal = _short_text(reviewed_plan.get("goal"), limit=220)
            if reviewed_goal:
                astraeus_goal = reviewed_goal
            reviewed_steps = reviewed_plan.get("steps")
            if isinstance(reviewed_steps, list):
                reviewed_queue = [step for step in reviewed_steps if isinstance(step, dict)]
                if reviewed_queue:
                    structured_plan_queue = reviewed_queue
    agent_state: Dict[str, Any] = _initial_agent_state_for_turn_from_topic_signal(
        prior_state=prior_state,
        current_user_text=current_user_turn_text,
        turn_request_text=astraeus_goal or turn_request_text,
        topic_shift_new_topic=astraeus_topic_shift,
    )
    agent_state["goal"] = astraeus_goal
    if not structured_plan_queue:
        agent_state = _clear_state_plan_for_new_turn(
            agent_state=agent_state,
            fallback_goal=astraeus_goal or turn_request_text or user_text,
        )
    structured_plan_total_steps = len(structured_plan_queue)
    if structured_plan_queue:
        agent_state = _sync_agent_state_with_plan_queue(
            agent_state=agent_state,
            plan_queue=structured_plan_queue,
            fallback_goal=astraeus_goal or turn_request_text or user_text,
        )
        _save_persistent_agent_state(
            redis_client=r,
            platform=platform,
            scope=scope,
            state=agent_state,
        )
    else:
        _save_persistent_agent_state(
            redis_client=r,
            platform=platform,
            scope=scope,
            state=agent_state,
        )
    def _retry_allowed_within_limits() -> bool:
        rounds_left = effective_max_rounds == 0 or rounds_used < effective_max_rounds
        tools_left = effective_max_tool_calls == 0 or tool_calls_used < effective_max_tool_calls
        return rounds_left and tools_left

    def _step_retry_allowed(step_id: str, retry_count: int) -> bool:
        del step_id
        return int(max(0, retry_count or 0)) < int(max(1, step_retry_limit))

    def _retry_allowed_for_step(step_id: str, retry_count: int) -> bool:
        return _retry_allowed_within_limits() and _step_retry_allowed(step_id, retry_count)

    def _remember_step_retry_hint(step_id: str, decision: Optional[Dict[str, Any]]) -> None:
        hint_text = _short_text(
            _checker_decision_text(decision, "repair", "next_action", "reason"),
            limit=220,
        )
        if hint_text:
            step_retry_hints[step_id] = hint_text

    def _remember_failed_source_for_step(step_id: str, candidate: Any) -> str:
        normalized = _canonical_web_source_url(candidate)
        if not normalized:
            return ""
        step_failed_source_urls.setdefault(step_id, set()).add(normalized)
        turn_failed_source_urls.add(normalized)
        page_key = _canonical_web_source_page(normalized)
        if page_key:
            step_failed_source_pages.setdefault(step_id, set()).add(page_key)
        return normalized

    def _failed_sources_for_step(step_id: str, *, limit: int = 6) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for bucket in (step_failed_source_urls.get(step_id, set()), turn_failed_source_urls):
            for value in sorted(bucket):
                token = str(value or "").strip()
                if not token or token in seen:
                    continue
                seen.add(token)
                out.append(token)
                if len(out) >= max(1, int(limit or 6)):
                    return out
        return out

    def _tool_call_matches_failed_source(step_id: str, tool_call: Optional[Dict[str, Any]]) -> str:
        candidate = _tool_call_primary_web_url(tool_call)
        if not candidate:
            return ""
        if candidate in step_failed_source_urls.get(step_id, set()):
            return candidate
        if candidate in turn_failed_source_urls:
            return candidate
        page_key = _canonical_web_source_page(candidate)
        if page_key and page_key in step_failed_source_pages.get(step_id, set()):
            return candidate
        return ""

    def _clear_step_retry_state(step_id: str) -> None:
        step_retry_counts.pop(step_id, None)
        step_retry_hints.pop(step_id, None)
        step_failed_source_urls.pop(step_id, None)
        step_failed_source_pages.pop(step_id, None)

    def _retry_limit_message(decision: Optional[Dict[str, Any]]) -> str:
        return (
            _checker_decision_text(decision, "question", "repair", "reason")
            or "I could not complete that step after retry attempts. Please clarify or rephrase the step."
        )

    async def _compose_final_answer_text(
        *,
        checker_decision: Optional[Dict[str, Any]],
        draft_response_text: str,
        user_request_text: str,
        tool_result_payload: Optional[Dict[str, Any]],
    ) -> str:
        base_text = _select_final_answer_text(
            checker_decision=checker_decision,
            draft_response=draft_response_text,
            user_text=user_request_text,
            tool_result=tool_result_payload,
        )
        composed_text = base_text
        if len(completed_tool_steps) >= 2:
            synthesized = await _synthesize_completed_steps_answer(
                llm_client=llm_client_ai_calls,
                platform=platform,
                user_text=user_request_text,
                goal=astraeus_goal or turn_request_text or current_user_turn_text,
                completed_steps=completed_tool_steps,
                draft_response=base_text,
                platform_preamble=platform_preamble,
                max_tokens=None,
            )
            if synthesized:
                composed_text = synthesized

        hermes_text = await _run_hermes_final_render(
            llm_client=llm_client_hermes,
            platform=platform,
            user_text=user_request_text,
            goal=astraeus_goal or turn_request_text or current_user_turn_text,
            mode="direct",
            instruction="",
            base_text=composed_text,
            completed_steps=completed_tool_steps,
            full_tool_results=raw_tool_payload_history,
            recent_history=hermes_recent_history,
            core_context_message=hermes_context_message,
            platform_preamble=platform_preamble,
            max_tokens=None,
        )
        if hermes_text:
            composed_text = hermes_text
        return composed_text

    def _finish(
        *,
        text: str,
        status: str,
        checker_action_value: str,
        checker_reason_value: str,
        planner_kind_value: Optional[str] = None,
        planner_text_is_tool_candidate_value: Optional[bool] = None,
        planned_tool_override: Optional[Dict[str, Any]] = None,
        validation_status_override: Optional[Dict[str, Any]] = None,
        retry_tool: Optional[Dict[str, Any]] = None,
        attempted_tool_override: Optional[str] = None,
    ) -> Dict[str, Any]:
        nonlocal agent_state
        final_status = str(status or "").strip() or "done"
        final_checker_action = str(checker_action_value or "").strip() or "FINAL_ANSWER"
        final_checker_reason = str(checker_reason_value or "").strip()
        final_text_raw = str(text or "").strip()

        final_text = _sanitize_user_text(final_text_raw, platform=platform, tool_used=tool_used)
        outcome_value, outcome_reason_value = _normalize_outcome(final_status, final_checker_reason)
        total_ms = int(max(0.0, (time.perf_counter() - turn_started_at) * 1000.0))

        _save_persistent_agent_state(
            redis_client=r,
            platform=platform,
            scope=scope,
            state=agent_state,
        )
        _write_hydra_ledger(
            redis_client=r,
            platform=platform,
            scope=scope,
            turn_id=turn_id,
            llm=llm_label,
            user_message=user_text,
            planned_tool=planned_tool_override if planned_tool_override is not None else planned_tool,
            validation_status=validation_status_override if validation_status_override is not None else validation_status,
            tool_result=tool_result_for_checker,
            checker_action=final_checker_action,
            retry_count=max(0, int(critic_continue_count)),
            checker_reason=final_checker_reason,
            planner_kind=planner_kind_value if planner_kind_value is not None else planner_kind,
            planner_text_is_tool_candidate=(
                planner_text_is_tool_candidate_value
                if planner_text_is_tool_candidate_value is not None
                else planner_text_is_tool_candidate
            ),
            outcome=outcome_value,
            outcome_reason=outcome_reason_value,
            planner_ms=int(max(0.0, astraeus_ms_total)),
            tool_ms=int(max(0.0, tool_ms_total)),
            checker_ms=int(max(0.0, minos_ms_total)),
            total_ms=total_ms,
            retry_tool=retry_tool,
            rounds_used=rounds_used,
            tool_calls_used=tool_calls_used,
            agent_state=agent_state,
            origin_preview=origin_preview,
            attempted_tool=attempted_tool_override if attempted_tool_override is not None else attempted_tool_for_ledger,
        )
        _write_hydra_metrics(
            redis_client=r,
            platform=platform,
            total_tools_called=tool_calls_used,
            total_repairs=repairs_used_count,
            validation_failures=validation_failures_count,
            tool_failures=tool_failures_count,
        )
        return {
            "text": final_text,
            "status": final_status,
            "task_id": task_id,
            "task_name": task_name,
            "artifacts": artifacts_out,
            "raw_tool_payload": raw_tool_payload_out,
            "normalized_minos_result": normalized_minos_result_out,
            "normalized_checker_result": normalized_minos_result_out,
        }

    if not structured_plan_queue:
        if astraeus_mode == "chat":
            chat_started = time.perf_counter()
            chat_text = await _run_chat_fallback_reply(
                llm_client=llm_client_chat,
                platform=platform,
                user_text=current_user_turn_text,
                history=history,
                registry=registry,
                enabled_predicate=enabled_predicate,
                redis_client=r,
                memory_context_message=chat_context_message,
                platform_preamble=platform_preamble,
                max_tokens=None,
            )
            astraeus_ms_total += (time.perf_counter() - chat_started) * 1000.0
            planner_kind = "answer"
            planner_text_is_tool_candidate = False
            checker_reason = "complete"
            return _finish(
                text=chat_text or _generic_chat_fallback_text(current_user_turn_text),
                status="done",
                checker_action_value="FINAL_ANSWER",
                checker_reason_value=checker_reason,
            )
        checker_reason = "needs_user_input"
        return _finish(
            text="I need a specific request for this turn. Tell me exactly what you want me to do next.",
            status="blocked",
            checker_action_value="NEED_USER_INFO",
            checker_reason_value=checker_reason,
        )

    while (
        (effective_max_rounds == 0 or rounds_used < effective_max_rounds)
        and (effective_max_tool_calls == 0 or tool_calls_used < effective_max_tool_calls)
    ):
        rounds_used += 1
        planned_tool = None
        thanatos_text = ""
        round_thanatos_kind = "answer"
        current_plan_step = structured_plan_queue[0] if structured_plan_queue else None
        round_request_text = (
            str((current_plan_step or {}).get("nl") or "").strip()
            if isinstance(current_plan_step, dict)
            else ""
        ) or turn_request_text
        current_step_id = (
            _short_text((current_plan_step or {}).get("id"), limit=24)
            if isinstance(current_plan_step, dict)
            else ""
        ) or "ad_hoc"
        current_step_retry_count = max(0, int(step_retry_counts.get(current_step_id, 0)))
        current_step_retry_hint = str(step_retry_hints.get(current_step_id) or "").strip()

        state_message = _agent_state_prompt_message(
            agent_state,
            fallback_goal=astraeus_goal or turn_request_text or user_text,
        )
        thanatos_messages: List[Dict[str, Any]] = [
            {"role": "system", "content": _thanatos_system_prompt(platform)},
        ]
        thanatos_messages.extend([
            {
                "role": "system",
                "content": _thanatos_focus_prompt(
                    current_user_text=current_user_turn_text,
                    turn_request_text=round_request_text,
                ),
            },
            {"role": "system", "content": state_message},
        ])
        previous_step_payload_prompt = _thanatos_previous_step_payloads_prompt(raw_tool_payload_history)
        if previous_step_payload_prompt:
            thanatos_messages.append({"role": "system", "content": previous_step_payload_prompt})
        artifact_manifest_prompt = _available_artifacts_prompt(turn_available_artifacts)
        if artifact_manifest_prompt:
            thanatos_messages.append({"role": "system", "content": artifact_manifest_prompt})
        result_memory_prompt = _result_memory_prompt(
            _state_result_memory(
                agent_state.get("result_memory"),
                max_sets=result_memory_max_sets,
                max_items=result_memory_max_items,
            )
        )
        if result_memory_prompt:
            thanatos_messages.append({"role": "system", "content": result_memory_prompt})
        if isinstance(current_plan_step, dict):
            current_failed_sources = _failed_sources_for_step(current_step_id, limit=6)
            thanatos_messages.append(
                {
                    "role": "system",
                    "content": _thanatos_execution_step_prompt(
                        intent=str(current_plan_step.get("intent") or ""),
                        nl=str(current_plan_step.get("nl") or ""),
                        goal=astraeus_goal or turn_request_text or current_user_turn_text,
                        repair_hint=current_step_retry_hint,
                        tool_hint=str(current_plan_step.get("tool_hint") or ""),
                        blocked_sources=current_failed_sources,
                    ),
                }
            )
            tool_contract_prompt = _thanatos_execution_tool_contract_prompt(
                current_plan_step=current_plan_step,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
                fallback_tool_index=tool_index,
            )
            if tool_contract_prompt:
                thanatos_messages.append({"role": "system", "content": tool_contract_prompt})
        thanatos_messages = _with_platform_preamble(
            thanatos_messages,
            platform_preamble=platform_preamble,
        )
        thanatos_messages.extend(history)
        thanatos_messages.append({"role": "user", "content": round_request_text})

        thanatos_text, thanatos_ms = await _run_thanatos_step(
            llm_client=llm_client_thanatos,
            thanatos_messages=thanatos_messages,
            max_tokens=None,
        )
        astraeus_ms_total += thanatos_ms

        if _is_tool_candidate(thanatos_text):
            round_thanatos_kind = "tool"
        else:
            round_thanatos_kind = "answer"
        planner_text_is_tool_candidate = _is_tool_candidate(thanatos_text)
        if not _is_tool_candidate(thanatos_text):
            planner_kind = round_thanatos_kind
            draft_response = str(thanatos_text or "").strip()
            checker_started = time.perf_counter()
            checker_decision = await _run_minos_validation(
                llm_client=llm_client_minos,
                platform=platform,
                current_user_text=current_user_turn_text,
                turn_request_text=turn_request_text,
                agent_state=agent_state,
                memory_context=memory_context_payload,
                available_artifacts=_available_artifacts_payload(turn_available_artifacts),
                current_step=(current_plan_step if isinstance(current_plan_step, dict) else None),
                goal=astraeus_goal or turn_request_text or current_user_turn_text,
                planned_tool=None,
                tool_result=tool_result_for_checker,
                draft_response=draft_response,
                retry_count=current_step_retry_count,
                retry_allowed=_retry_allowed_for_step(current_step_id, current_step_retry_count),
                platform_preamble=platform_preamble,
                max_tokens=None,
            )
            minos_ms_total += (time.perf_counter() - checker_started) * 1000.0
            checker_action = _checker_decision_value(checker_decision)

            if checker_action == "ASK_USER":
                need_text = _checker_decision_text(checker_decision, "question", "text", "reason") or DEFAULT_CLARIFICATION
                checker_reason = "needs_user_input"
                return _finish(
                    text=need_text,
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                )

            if checker_action == "RETRY":
                if not _retry_allowed_for_step(current_step_id, current_step_retry_count):
                    checker_reason = "step_retry_limit_reached"
                    return _finish(
                        text=_retry_limit_message(checker_decision),
                        status="blocked",
                        checker_action_value="NEED_USER_INFO",
                        checker_reason_value=checker_reason,
                    )
                step_retry_counts[current_step_id] = current_step_retry_count + 1
                _remember_step_retry_hint(current_step_id, checker_decision)
                checker_reason = _checker_decision_text(checker_decision, "reason", "repair") or "retry_current_step"
                critic_continue_count += 1
                continue

            if checker_action == "CONTINUE":
                if structured_plan_queue:
                    if not _retry_allowed_for_step(current_step_id, current_step_retry_count):
                        checker_reason = "step_retry_limit_reached"
                        return _finish(
                            text=_retry_limit_message(checker_decision),
                            status="blocked",
                            checker_action_value="NEED_USER_INFO",
                            checker_reason_value=checker_reason,
                        )
                    step_retry_counts[current_step_id] = current_step_retry_count + 1
                    _remember_step_retry_hint(current_step_id, checker_decision)
                    checker_reason = _checker_decision_text(checker_decision, "reason", "repair") or "retry_current_step"
                    critic_continue_count += 1
                    continue
                if not _retry_allowed_for_step(current_step_id, current_step_retry_count):
                    checker_reason = "step_retry_limit_reached"
                    return _finish(
                        text=_retry_limit_message(checker_decision),
                        status="blocked",
                        checker_action_value="NEED_USER_INFO",
                        checker_reason_value=checker_reason,
                    )
                step_retry_counts[current_step_id] = current_step_retry_count + 1
                _remember_step_retry_hint(current_step_id, checker_decision)
                checker_reason = _checker_decision_text(checker_decision, "reason", "next_action") or "continue"
                critic_continue_count += 1
                continue

            if checker_action == "FAIL":
                fail_text = _checker_decision_text(checker_decision, "reason", "text") or DEFAULT_CLARIFICATION
                checker_reason = "minos_fail"
                return _finish(
                    text=fail_text,
                    status="blocked",
                    checker_action_value="FAIL",
                    checker_reason_value=checker_reason,
                )

            final_text_candidate = await _compose_final_answer_text(
                checker_decision=checker_decision,
                draft_response_text=draft_response,
                user_request_text=turn_request_text or user_text,
                tool_result_payload=tool_result_for_checker,
            )
            if structured_plan_queue:
                if not _retry_allowed_for_step(current_step_id, current_step_retry_count):
                    checker_reason = "step_retry_limit_reached"
                    return _finish(
                        text=_retry_limit_message(checker_decision),
                        status="blocked",
                        checker_action_value="NEED_USER_INFO",
                        checker_reason_value=checker_reason,
                    )
                step_retry_counts[current_step_id] = current_step_retry_count + 1
                _remember_step_retry_hint(current_step_id, checker_decision)
                checker_reason = "continue_plan_step"
                critic_continue_count += 1
                continue
            if _should_continue_after_incomplete_final_answer(
                user_text=turn_request_text or user_text,
                final_text=final_text_candidate,
                agent_state=agent_state,
                retry_allowed=_retry_allowed_for_step(current_step_id, current_step_retry_count),
            ):
                step_retry_counts[current_step_id] = current_step_retry_count + 1
                _remember_step_retry_hint(current_step_id, checker_decision)
                checker_reason = "continue_after_incomplete_final_answer"
                critic_continue_count += 1
                continue
            checker_reason = _tool_failure_checker_reason(tool_result_for_checker) or "complete"
            return _finish(
                text=final_text_candidate,
                status="done",
                checker_action_value="FINAL_ANSWER",
                checker_reason_value=checker_reason,
            )

        tool_eval = await _validate_or_recover_tool_call(
            llm_client=llm_client_ai_calls,
            text=thanatos_text,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
            tool_index=tool_index,
            user_text=round_request_text,
            origin=origin_payload,
            scope=scope,
            history_messages=history,
            context=context if isinstance(context, dict) else {},
            platform_preamble=platform_preamble,
        )
        validation_status = (
            tool_eval.get("validation_status")
            if isinstance(tool_eval.get("validation_status"), dict)
            else validation_status
        )
        attempted_tool_for_ledger = str(tool_eval.get("attempted_tool") or attempted_tool_for_ledger or "")
        if bool(tool_eval.get("repair_used")):
            repairs_used_count += 1
        if bool(tool_eval.get("repair_used")):
            round_thanatos_kind = "repaired_tool"
        else:
            round_thanatos_kind = "tool"

        if not bool(tool_eval.get("ok")):
            planner_text_is_tool_candidate = True
            reason = str(tool_eval.get("reason") or "invalid_tool_call")
            assistant_text = str(tool_eval.get("assistant_text") or "").strip()
            recovery_text = str(tool_eval.get("recovery_text_if_blocked") or DEFAULT_CLARIFICATION).strip()
            failed_planned_tool = tool_eval.get("tool_call")
            if not isinstance(failed_planned_tool, dict):
                failed_planned_tool = {"function": "invalid_tool_call", "arguments": {}}
            if reason == "repair_returned_answer" and assistant_text:
                planner_kind = "repaired_answer"
                checker_reason = "complete"
                return _finish(
                    text=assistant_text,
                    status="done",
                    checker_action_value="FINAL_ANSWER",
                    checker_reason_value=checker_reason,
                    planner_kind_value=planner_kind,
                    planned_tool_override=failed_planned_tool,
                    validation_status_override=validation_status,
                    attempted_tool_override=str(tool_eval.get("attempted_tool") or ""),
                )
            if (
                reason == "repair_returned_no_tool"
                and _retry_allowed_within_limits()
                and repair_returned_no_tool_retries < 2
            ):
                repair_returned_no_tool_retries += 1
                checker_reason = "continue_after_repair_returned_no_tool"
                critic_continue_count += 1
                continue

            validation_failures_count += 1
            planner_kind = round_thanatos_kind
            checker_reason = f"validation_failed:{reason}"
            return _finish(
                text=recovery_text,
                status="blocked",
                checker_action_value="NEED_USER_INFO",
                checker_reason_value=checker_reason,
                planned_tool_override=failed_planned_tool,
                validation_status_override=validation_status,
                attempted_tool_override=str(tool_eval.get("attempted_tool") or ""),
            )

        planned_tool = tool_eval.get("tool_call") if isinstance(tool_eval.get("tool_call"), dict) else None
        planner_text_is_tool_candidate = True
        if not planned_tool:
            validation_failures_count += 1
            checker_reason = "validation_failed:invalid_tool_call"
            return _finish(
                text=DEFAULT_CLARIFICATION,
                status="blocked",
                checker_action_value="NEED_USER_INFO",
                checker_reason_value=checker_reason,
                planned_tool_override={"function": "invalid_tool_call", "arguments": {}},
                validation_status_override=validation_status,
            )
        attempted_tool_for_ledger = str((planned_tool or {}).get("function") or attempted_tool_for_ledger or "")
        planner_kind = round_thanatos_kind

        planner_kind = round_thanatos_kind
        if not isinstance(planned_tool, dict):
            planner_kind = round_thanatos_kind
            validation_failures_count += 1
            checker_reason = "validation_failed:invalid_tool_call"
            return _finish(
                text=DEFAULT_CLARIFICATION,
                status="blocked",
                checker_action_value="NEED_USER_INFO",
                checker_reason_value=checker_reason,
                planned_tool_override={"function": "invalid_tool_call", "arguments": {}},
                validation_status_override=validation_status,
            )

        planned_tool = _autofix_image_describe_tool_call(
            planned_tool,
            turn_available_artifacts,
        )
        blocked_source = _tool_call_matches_failed_source(current_step_id, planned_tool)
        if blocked_source:
            if not _retry_allowed_for_step(current_step_id, current_step_retry_count):
                checker_reason = "blocked_failed_source_repeat"
                return _finish(
                    text=(
                        f"I already tried that source and it failed: {blocked_source}. "
                        "Do you want me to try a different link or stop?"
                    ),
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    retry_tool=queued_retry_tool_for_ledger,
                )
            step_retry_counts[current_step_id] = current_step_retry_count + 1
            blocked_sources = _failed_sources_for_step(current_step_id, limit=4)
            if blocked_sources:
                step_retry_hints[current_step_id] = (
                    "Use a different source URL than the failed ones: "
                    + " | ".join(blocked_sources)
                )
            checker_reason = "blocked_failed_source_repeat"
            critic_continue_count += 1
            continue
        tool_used = True
        tool_user_text = round_request_text
        _set_active_chat_job_current_tool(
            active_job_id,
            _active_chat_job_tool_label(tool_call=planned_tool, registry=registry),
        )
        wait_text, wait_payload = await _tool_start_progress(
            llm_client=llm_client_ai_calls,
            platform=platform,
            tool_call=planned_tool,
            round_request_text=tool_user_text,
            current_plan_step=(current_plan_step if isinstance(current_plan_step, dict) else None),
            completed_steps_count=len(completed_tool_steps),
            total_plan_steps=structured_plan_total_steps,
            platform_preamble=platform_preamble,
            max_tokens=None,
        )
        tool_started = time.perf_counter()
        tool_origin_payload = dict(origin_payload) if isinstance(origin_payload, dict) else {}
        if raw_tool_payload_history:
            tool_origin_payload["tool_results_full"] = [
                dict(item)
                for item in raw_tool_payload_history[-8:]
                if isinstance(item, dict)
            ]
        try:
            doer_exec = await _execute_tool_call(
                llm_client=llm_client_ai_calls,
                tool_call=planned_tool,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
                context=context,
                user_text=tool_user_text,
                origin=tool_origin_payload,
                scope=scope,
                wait_callback=wait_callback,
                wait_text=wait_text,
                wait_payload=wait_payload,
                admin_guard=admin_guard,
            )
        finally:
            _set_active_chat_job_current_tool(active_job_id, "")
        tool_ms_total += (time.perf_counter() - tool_started) * 1000.0
        raw_payload = doer_exec.get("payload")
        raw_tool_payload_out = raw_payload if isinstance(raw_payload, dict) else None
        if isinstance(raw_tool_payload_out, dict) and raw_tool_payload_out:
            raw_tool_payload_history.append(raw_tool_payload_out)
        tool_result_for_checker = doer_exec.get("minos_result")
        if not isinstance(tool_result_for_checker, dict):
            tool_result_for_checker = doer_exec.get("checker_result")
        normalized_minos_result_out = tool_result_for_checker if isinstance(tool_result_for_checker, dict) else None
        if isinstance(tool_result_for_checker, dict) and not bool(tool_result_for_checker.get("ok")):
            tool_failures_count += 1
            failed_source = _tool_call_primary_web_url(planned_tool)
            remembered_failed_source = _remember_failed_source_for_step(current_step_id, failed_source)
            if remembered_failed_source:
                blocked_sources = _failed_sources_for_step(current_step_id, limit=4)
                if blocked_sources:
                    step_retry_hints[current_step_id] = (
                        "Do not reuse failed source URLs for this step. Try a different source: "
                        + " | ".join(blocked_sources)
                    )
        draft_response = str((tool_result_for_checker or {}).get("summary_for_user") or "").strip()
        if isinstance(tool_result_for_checker, dict) and bool(tool_result_for_checker.get("ok")):
            _clear_step_retry_state(current_step_id)
            completed_tool_steps.append(
                {
                    "request": str(tool_user_text or round_request_text or "").strip(),
                    "summary": draft_response,
                }
            )
        new_turn_artifacts = _turn_artifacts_from_tool_payload(raw_payload)
        if new_turn_artifacts:
            try:
                save_conversation_artifacts(
                    r,
                    platform=platform,
                    scope=scope,
                    artifacts=new_turn_artifacts,
                )
            except Exception:
                pass
        try:
            stored_conversation_artifacts = load_conversation_artifacts(
                r,
                platform=platform,
                scope=scope,
                limit=16,
            )
        except Exception:
            stored_conversation_artifacts = []
        turn_available_artifacts = _merge_turn_artifacts(
            stored_conversation_artifacts or turn_available_artifacts,
            new_turn_artifacts,
        )
        if turn_available_artifacts:
            origin_payload["available_artifacts"] = [dict(item) for item in turn_available_artifacts]
        else:
            origin_payload.pop("available_artifacts", None)
        artifacts = ((tool_result_for_checker or {}).get("artifacts") or [])
        if isinstance(artifacts, list):
            for item in artifacts:
                if not isinstance(item, dict):
                    continue
                artifacts_out.append(item)
                if len(artifacts_out) >= 12:
                    break
        tool_calls_used += 1

        agent_state = await _run_thanatos_state_update(
            llm_client=llm_client_thanatos,
            platform=platform,
            user_request=tool_user_text or turn_request_text,
            prior_state=agent_state,
            tool_call=planned_tool,
            tool_result=tool_result_for_checker,
            max_tokens=None,
        )
        if structured_plan_queue:
            agent_state = _sync_agent_state_with_plan_queue(
                agent_state=agent_state,
                plan_queue=structured_plan_queue,
                fallback_goal=astraeus_goal or turn_request_text or user_text,
            )
        agent_state = _remember_tool_result_in_agent_state(
            agent_state=agent_state,
            tool_call=planned_tool,
            raw_payload=(raw_payload if isinstance(raw_payload, dict) else None),
            tool_result=(tool_result_for_checker if isinstance(tool_result_for_checker, dict) else None),
            request_text=str(tool_user_text or round_request_text or turn_request_text or "").strip(),
            fallback_goal=astraeus_goal or turn_request_text or user_text,
            max_sets=result_memory_max_sets,
            max_items=result_memory_max_items,
        )
        _save_persistent_agent_state(
            redis_client=r,
            platform=platform,
            scope=scope,
            state=agent_state,
        )

        checker_started = time.perf_counter()
        turn_draft_response = _multi_step_turn_draft(
            completed_steps=completed_tool_steps,
            fallback_draft=draft_response,
        )
        checker_decision = await _run_minos_validation(
            llm_client=llm_client_minos,
            platform=platform,
            current_user_text=current_user_turn_text,
            turn_request_text=turn_request_text,
            agent_state=agent_state,
            memory_context=memory_context_payload,
            available_artifacts=_available_artifacts_payload(turn_available_artifacts),
            current_step=(current_plan_step if isinstance(current_plan_step, dict) else None),
            goal=astraeus_goal or turn_request_text or current_user_turn_text,
            planned_tool=planned_tool,
            tool_result=tool_result_for_checker,
            draft_response=turn_draft_response,
            retry_count=current_step_retry_count,
            retry_allowed=_retry_allowed_for_step(current_step_id, current_step_retry_count),
            platform_preamble=platform_preamble,
            max_tokens=None,
        )
        minos_ms_total += (time.perf_counter() - checker_started) * 1000.0
        checker_action = _checker_decision_value(checker_decision)

        if checker_action == "CONTINUE":
            if not structured_plan_queue:
                checker_action = "FINAL"
            elif isinstance(tool_result_for_checker, dict) and bool(tool_result_for_checker.get("ok")):
                structured_plan_queue = structured_plan_queue[1:]
                _clear_step_retry_state(current_step_id)
                agent_state = _sync_agent_state_with_plan_queue(
                    agent_state=agent_state,
                    plan_queue=structured_plan_queue,
                    fallback_goal=astraeus_goal or turn_request_text or user_text,
                )
                _save_persistent_agent_state(
                    redis_client=r,
                    platform=platform,
                    scope=scope,
                    state=agent_state,
                )
                if not structured_plan_queue:
                    final_text_candidate = await _compose_final_answer_text(
                        checker_decision=checker_decision,
                        draft_response_text=turn_draft_response,
                        user_request_text=turn_request_text or user_text,
                        tool_result_payload=tool_result_for_checker,
                    )
                    checker_reason = _tool_failure_checker_reason(tool_result_for_checker) or "complete"
                    return _finish(
                        text=final_text_candidate,
                        status="done",
                        checker_action_value="FINAL_ANSWER",
                        checker_reason_value=checker_reason,
                        retry_tool=queued_retry_tool_for_ledger,
                    )
                checker_reason = _checker_decision_text(checker_decision, "reason", "next_action") or "continue_plan_step"
                critic_continue_count += 1
                continue
            elif _retry_allowed_for_step(current_step_id, current_step_retry_count):
                step_retry_counts[current_step_id] = current_step_retry_count + 1
                _remember_step_retry_hint(current_step_id, checker_decision)
                checker_reason = _checker_decision_text(checker_decision, "reason", "repair") or "retry_current_step"
                critic_continue_count += 1
                continue
            else:
                checker_reason = "step_retry_limit_reached"
                return _finish(
                    text=_retry_limit_message(checker_decision),
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    retry_tool=queued_retry_tool_for_ledger,
                )

        if checker_action == "ASK_USER":
            need_text = _checker_decision_text(checker_decision, "question", "text", "reason") or DEFAULT_CLARIFICATION
            checker_reason = "needs_user_input"
            return _finish(
                text=need_text,
                status="blocked",
                checker_action_value="NEED_USER_INFO",
                checker_reason_value=checker_reason,
                retry_tool=queued_retry_tool_for_ledger,
            )

        if checker_action == "RETRY":
            if not _retry_allowed_for_step(current_step_id, current_step_retry_count):
                checker_reason = "step_retry_limit_reached"
                return _finish(
                    text=_retry_limit_message(checker_decision),
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    retry_tool=queued_retry_tool_for_ledger,
                )
            step_retry_counts[current_step_id] = current_step_retry_count + 1
            _remember_step_retry_hint(current_step_id, checker_decision)
            checker_reason = _checker_decision_text(checker_decision, "reason", "repair") or "retry_current_step"
            critic_continue_count += 1
            continue

        if checker_action == "FAIL":
            fail_text = _checker_decision_text(checker_decision, "reason", "text") or DEFAULT_CLARIFICATION
            checker_reason = "minos_fail"
            return _finish(
                text=fail_text,
                status="blocked",
                checker_action_value="FAIL",
                checker_reason_value=checker_reason,
                retry_tool=queued_retry_tool_for_ledger,
            )

        if (
            checker_action == "FINAL"
            and structured_plan_queue
            and isinstance(tool_result_for_checker, dict)
            and bool(tool_result_for_checker.get("ok"))
            and _retry_allowed_for_step(current_step_id, current_step_retry_count)
        ):
            structured_plan_queue = structured_plan_queue[1:]
            _clear_step_retry_state(current_step_id)
            agent_state = _sync_agent_state_with_plan_queue(
                agent_state=agent_state,
                plan_queue=structured_plan_queue,
                fallback_goal=astraeus_goal or turn_request_text or user_text,
            )
            _save_persistent_agent_state(
                redis_client=r,
                platform=platform,
                scope=scope,
                state=agent_state,
            )
            if not structured_plan_queue:
                final_text_candidate = await _compose_final_answer_text(
                    checker_decision=checker_decision,
                    draft_response_text=turn_draft_response,
                    user_request_text=turn_request_text or user_text,
                    tool_result_payload=tool_result_for_checker,
                )
                checker_reason = _tool_failure_checker_reason(tool_result_for_checker) or "complete"
                return _finish(
                    text=final_text_candidate,
                    status="done",
                    checker_action_value="FINAL_ANSWER",
                    checker_reason_value=checker_reason,
                    retry_tool=queued_retry_tool_for_ledger,
                )
            checker_reason = "continue_plan_step"
            critic_continue_count += 1
            continue

        final_text_candidate = await _compose_final_answer_text(
            checker_decision=checker_decision,
            draft_response_text=turn_draft_response,
            user_request_text=turn_request_text or user_text,
            tool_result_payload=tool_result_for_checker,
        )
        if _should_continue_after_incomplete_final_answer(
            user_text=turn_request_text or user_text,
            final_text=final_text_candidate,
            agent_state=agent_state,
            retry_allowed=_retry_allowed_for_step(current_step_id, current_step_retry_count),
        ):
            step_retry_counts[current_step_id] = current_step_retry_count + 1
            _remember_step_retry_hint(current_step_id, checker_decision)
            checker_reason = "continue_after_incomplete_final_answer"
            critic_continue_count += 1
            continue
        checker_reason = _tool_failure_checker_reason(tool_result_for_checker) or "complete"
        return _finish(
            text=final_text_candidate,
            status="done",
            checker_action_value="FINAL_ANSWER",
            checker_reason_value=checker_reason,
            retry_tool=queued_retry_tool_for_ledger,
        )

    pending_question = _state_first_open_question(agent_state)
    if pending_question:
        checker_reason = "needs_user_input"
        return _finish(
            text=pending_question,
            status="blocked",
            checker_action_value="NEED_USER_INFO",
            checker_reason_value=checker_reason,
            retry_tool=queued_retry_tool_for_ledger,
        )

    best_effort = _state_best_effort_answer(
        state=agent_state,
        draft_response=_multi_step_turn_draft(
            completed_steps=completed_tool_steps,
            fallback_draft=draft_response,
        ),
        tool_result=tool_result_for_checker,
    )
    fallback_step = structured_plan_queue[0] if structured_plan_queue else None
    fallback_step_id = (
        _short_text((fallback_step or {}).get("id"), limit=24)
        if isinstance(fallback_step, dict)
        else ""
    ) or "ad_hoc"
    fallback_step_retry_count = max(0, int(step_retry_counts.get(fallback_step_id, 0)))
    checker_started = time.perf_counter()
    checker_decision = await _run_minos_validation(
        llm_client=llm_client_minos,
        platform=platform,
        current_user_text=current_user_turn_text,
        turn_request_text=turn_request_text,
        agent_state=agent_state,
        memory_context=memory_context_payload,
        available_artifacts=_available_artifacts_payload(turn_available_artifacts),
        current_step=(fallback_step if isinstance(fallback_step, dict) else None),
        goal=astraeus_goal or turn_request_text or current_user_turn_text,
        planned_tool=planned_tool,
        tool_result=tool_result_for_checker,
        draft_response=best_effort,
        retry_count=fallback_step_retry_count,
        retry_allowed=_retry_allowed_for_step(fallback_step_id, fallback_step_retry_count),
        platform_preamble=platform_preamble,
        max_tokens=None,
    )
    minos_ms_total += (time.perf_counter() - checker_started) * 1000.0
    checker_action = _checker_decision_value(checker_decision)

    if checker_action == "ASK_USER":
        need_text = _checker_decision_text(checker_decision, "question", "text", "reason") or pending_question or DEFAULT_CLARIFICATION
        checker_reason = "needs_user_input"
        return _finish(
            text=need_text,
            status="blocked",
            checker_action_value="NEED_USER_INFO",
            checker_reason_value=checker_reason,
            retry_tool=queued_retry_tool_for_ledger,
        )

    if checker_action in {"RETRY", "CONTINUE"}:
        checker_reason = "budget_exhausted"
        final_text_candidate = await _compose_final_answer_text(
            checker_decision=checker_decision,
            draft_response_text=best_effort,
            user_request_text=turn_request_text or user_text,
            tool_result_payload=tool_result_for_checker,
        )
        return _finish(
            text=final_text_candidate or best_effort or "Completed.",
            status="done",
            checker_action_value="FINAL_ANSWER",
            checker_reason_value=checker_reason,
            retry_tool=queued_retry_tool_for_ledger,
        )

    if checker_action == "FAIL":
        fail_text = _checker_decision_text(checker_decision, "reason", "text") or DEFAULT_CLARIFICATION
        checker_reason = "minos_fail"
        return _finish(
            text=fail_text,
            status="blocked",
            checker_action_value="FAIL",
            checker_reason_value=checker_reason,
            retry_tool=queued_retry_tool_for_ledger,
        )

    checker_reason = _tool_failure_checker_reason(tool_result_for_checker) or checker_reason or "complete"
    final_text_candidate = await _compose_final_answer_text(
        checker_decision=checker_decision,
        draft_response_text=best_effort,
        user_request_text=turn_request_text or user_text,
        tool_result_payload=tool_result_for_checker,
    )
    return _finish(
        text=final_text_candidate,
        status="done",
        checker_action_value="FINAL_ANSWER",
        checker_reason_value=checker_reason,
        retry_tool=queued_retry_tool_for_ledger,
    )


async def run_hydra_turn(
    *,
    llm_client: Any,
    platform: str,
    history_messages: List[Dict[str, Any]],
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]] = None,
    context: Optional[Dict[str, Any]] = None,
    user_text: str,
    scope: str,
    task_id: Optional[str] = None,
    origin: Optional[Dict[str, Any]] = None,
    wait_callback: Optional[Callable[..., Any]] = None,
    admin_guard: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    redis_client: Any = None,
    max_rounds: Optional[int] = None,
    max_tool_calls: Optional[int] = None,
    platform_preamble: str = "",
) -> Dict[str, Any]:
    active_job_id = _register_active_chat_job(
        platform=platform,
        scope=scope,
        origin=origin,
        task_name=_task_name_from_text(user_text, fallback="Hydra task"),
    )
    try:
        r = redis_client or default_redis
        llm_pool = await _build_hydra_llm_client_pool(
            base_llm_client=llm_client,
            redis_client=r,
        )
        try:
            return await _run_hydra_turn_impl(
                llm_client=llm_client,
                llm_clients=llm_pool.role_clients,
                platform=platform,
                history_messages=history_messages,
                registry=registry,
                enabled_predicate=enabled_predicate,
                context=context,
                user_text=user_text,
                scope=scope,
                task_id=task_id,
                active_job_id=active_job_id,
                origin=origin,
                wait_callback=wait_callback,
                admin_guard=admin_guard,
                redis_client=r,
                max_rounds=max_rounds,
                max_tool_calls=max_tool_calls,
                platform_preamble=platform_preamble,
            )
        finally:
            await llm_pool.aclose()
    finally:
        _unregister_active_chat_job(active_job_id)
