import re
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from . import cerberus_contracts as contracts
from . import cerberus_checker as checker
from . import cerberus_doer_state as doer_state
from . import cerberus_execution as execution
from . import cerberus_common_helpers as common_helpers
from . import cerberus_followup_intents as followup_intents
from . import cerberus_ledger as ledger
from . import cerberus_limits as limits_helpers
from . import cerberus_memory_context as memory_context_helpers
from . import cerberus_multi_intent_builder as multi_intent_builder
from . import cerberus_overclar_repair as overclar_repair
from . import cerberus_origin_attach as origin_attach_helpers
from . import cerberus_preamble_utils as preamble_utils
from . import cerberus_prompts as prompts
from . import cerberus_retry_helpers as retry_helpers
from . import cerberus_runtime_config as runtime_config
from . import cerberus_scope as scope_helpers
from . import cerberus_state_core as state_core_helpers
from . import cerberus_state_store as state_store
from . import cerberus_toolcall_utils as toolcall_utils
from . import cerberus_tool_index as tool_index_helpers
from . import cerberus_turn_classifiers as turn_classifiers
from . import cerberus_turn_utils as turn_utils
from . import cerberus_validation as validation
from . import cerberus_validation_flow as validation_flow
from . import cerberus_web_research as web_research_helpers
from helpers import (
    TOOL_MARKUP_REPAIR_PROMPT,
    get_tater_name,
    get_tater_personality,
    looks_like_tool_markup,
    parse_function_json,
    redis_client as default_redis,
)
from plugin_kernel import (
    expand_plugin_platforms,
    normalize_platform,
    plugin_display_name,
    plugin_supports_platform,
    plugin_when_to_use,
)
from plugin_result import action_failure, narrate_result, normalize_plugin_result, result_for_llm
from tool_runtime import META_TOOLS, execute_plugin_call, is_meta_tool, run_meta_tool
from memory_platform_store import (
    load_doc as load_memory_platform_doc,
    resolve_user_doc_key as resolve_memory_user_doc_key,
    room_doc_key as memory_room_doc_key,
    summarize_doc as summarize_memory_platform_doc,
    user_doc_key as memory_user_doc_key,
    value_to_text as memory_value_to_text,
)

TOOL_NAME_ALIASES = {
    "web_search": "search_web",
    "google_search": "search_web",
    "google_cse_search": "search_web",
    "inspect_page": "inspect_webpage",
    "inspect_website": "inspect_webpage",
    "describe_image": "image_describe",
    "describe_latest_image": "image_describe",
}

_KERNEL_TOOL_PRIORITY = [
    "search_web",
    "inspect_webpage",
    "read_url",
    "download_file",
    "read_file",
    "search_files",
    "list_directory",
    "list_archive",
    "extract_archive",
    "memory_get",
    "memory_set",
    "memory_search",
    "list_workspace",
    "get_plugin_help",
]

_KERNEL_TOOL_PURPOSE_HINTS = {
    "get_plugin_help": "show plugin schema and required args",
    "list_platforms_for_plugin": "list platforms supported by a plugin",
    "read_file": "read local file contents",
    "search_web": "web search for current information",
    "search_files": "search text across local files",
    "write_file": "write content to a local file",
    "list_directory": "list files and folders",
    "delete_file": "delete a local file",
    "read_url": "fetch and read webpage text",
    "inspect_webpage": "inspect webpage structure, links, and image candidates",
    "download_file": "download files from URLs",
    "list_archive": "inspect archive entries",
    "extract_archive": "extract archives to a target directory",
    "list_stable_plugins": "list stable built-in plugins",
    "list_stable_platforms": "list stable built-in platforms",
    "inspect_plugin": "inspect plugin metadata and methods",
    "test_plugin": "run plugin test harness",
    "write_workspace_note": "append a workspace note",
    "list_workspace": "list workspace notes",
    "memory_get": "read saved memory (auto-checks legacy + durable profiles by default)",
    "memory_set": "save memory entries",
    "memory_list": "list saved memory keys",
    "memory_explain": "explain memory value/source",
    "memory_search": "search saved memory",
}

ASCII_ONLY_PLATFORMS = {"irc", "homeassistant", "homekit", "xbmc"}
DEFAULT_CLARIFICATION = "Could you clarify exactly what you want me to do next?"
DEFAULT_MAX_ROUNDS = 6
DEFAULT_MAX_TOOL_CALLS = 6
DEFAULT_MAX_LEDGER_ITEMS = 500
DEFAULT_PLANNER_MAX_TOKENS = 1100
DEFAULT_CHECKER_MAX_TOKENS = 850
DEFAULT_DOER_MAX_TOKENS = 900
DEFAULT_TOOL_REPAIR_MAX_TOKENS = 750
DEFAULT_OVERCLAR_REPAIR_MAX_TOKENS = 900
DEFAULT_RECOVERY_MAX_TOKENS = 350
AGENT_MAX_ROUNDS_KEY = "tater:agent:max_rounds"
AGENT_MAX_TOOL_CALLS_KEY = "tater:agent:max_tool_calls"
CERBERUS_AGENT_STATE_TTL_SECONDS_KEY = "tater:cerberus:agent_state_ttl_seconds"
CERBERUS_PLANNER_MAX_TOKENS_KEY = "tater:cerberus:planner_max_tokens"
CERBERUS_CHECKER_MAX_TOKENS_KEY = "tater:cerberus:checker_max_tokens"
CERBERUS_DOER_MAX_TOKENS_KEY = "tater:cerberus:doer_max_tokens"
CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY = "tater:cerberus:tool_repair_max_tokens"
CERBERUS_OVERCLAR_REPAIR_MAX_TOKENS_KEY = "tater:cerberus:overclar_repair_max_tokens"
CERBERUS_RECOVERY_MAX_TOKENS_KEY = "tater:cerberus:recovery_max_tokens"
CERBERUS_MAX_LEDGER_ITEMS_KEY = "tater:cerberus:max_ledger_items"
AGENT_STATE_PROMPT_MAX_CHARS = 800
AGENT_STATE_LEDGER_MAX_CHARS = 900
AGENT_STATE_KEY_PREFIX = "tater:cerberus:state:"
DEFAULT_AGENT_STATE_TTL_SECONDS = 7 * 24 * 60 * 60
AGENT_STATE_TTL_SECONDS = DEFAULT_AGENT_STATE_TTL_SECONDS
CERBERUS_LEDGER_SCHEMA_VERSION = "2"
MULTI_ACTION_MIN_BUDGET = 4
MULTI_ACTION_MAX_BUDGET = 12
_MULTI_INTENT_ROUTE_USER_TEXT_KEY = contracts.MULTI_INTENT_ROUTE_USER_TEXT_KEY
_MULTI_INTENT_ROUTE_FLAG_KEY = contracts.MULTI_INTENT_ROUTE_FLAG_KEY

_PLATFORM_DISPLAY = {
    "webui": "WebUI",
    "discord": "Discord",
    "irc": "IRC",
    "telegram": "Telegram",
    "matrix": "Matrix",
    "homeassistant": "Home Assistant",
    "homekit": "HomeKit",
    "xbmc": "XBMC",
    "automation": "automation",
}

_OVER_CLARIFICATION_MARKERS = (
    "could you clarify",
    "what do you mean",
    "what specific issue",
    "what platform are you referring to",
    "what platform or environment",
    "which platform or environment",
    "what would you like to do",
    "what would you like me to do",
    "which platform should i send to",
    "what room/channel/chat should i send this to",
    "which channel should i send this to",
    "which room should i send this to",
    "what time format should i use",
    "should i use 12-hour or 24-hour",
    "do you want 12-hour or 24-hour format",
    "am or pm",
    "what timezone",
    "which timezone",
    "what time zone",
    "which time zone",
    "timezone format",
    "timezone should",
    "utc or local",
    "iana",
    "what city or coordinates",
    "which city or coordinates",
    "what city should i use",
    "which city should i use",
    "what location should i use",
    "which location should i use",
    "which categories, channels, and roles",
    "what categories, channels, and roles",
    "which categories and channels",
    "which channels and roles",
)

_URL_RE = re.compile(r"https?://[^\s<>)\]\"']+", flags=re.IGNORECASE)
_CHECKER_DECISION_PREFIX_RE = re.compile(
    r"^\s*(FINAL[\s_-]*ANSWER|RETRY[\s_-]*TOOL|NEED[\s_-]*USER[\s_-]*INFO)\s*:\s*(.*)$",
    flags=re.IGNORECASE | re.DOTALL,
)
_WORKSPACE_DISCOVERY_HINT_RE = re.compile(
    r"\b(plugin|platform|file|files|code|path|paths|folder|directory|workspace|skill|skills|reference|references|readme|edit|update|fix|create|build)\b",
    flags=re.IGNORECASE,
)
_WORKSPACE_QUERY_STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "to",
    "for",
    "of",
    "in",
    "on",
    "with",
    "please",
    "can",
    "you",
    "me",
    "my",
    "this",
    "that",
    "it",
}
_MEMORY_CONTEXT_DEFAULT_ITEMS = 12
_MEMORY_CONTEXT_DEFAULT_VALUE_MAX_CHARS = 288
_MEMORY_CONTEXT_DEFAULT_SUMMARY_MAX_CHARS = 2100
_WEB_RESEARCH_MAX_CANDIDATES = 8
_WEB_RESEARCH_MAX_LINK_TRIES = 4
_WEB_RESEARCH_MIN_PREVIEW_CHARS = 260
_WEB_RESEARCH_MIN_PREVIEW_WORDS = 45


def _plugin_routing_keywords(plugin: Any) -> List[str]:
    return contracts.plugin_routing_keywords(plugin)


def _tool_call_route_metadata(tool_call: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return contracts.tool_call_route_metadata(tool_call)


def _tool_call_effective_user_text(tool_call: Optional[Dict[str, Any]], default_user_text: str) -> str:
    return contracts.tool_call_effective_user_text(tool_call, default_user_text)


def _compose_multi_intent_route_answer(summaries: List[str], fallback: str = "") -> str:
    return contracts.compose_multi_intent_route_answer(summaries, fallback)


def _is_routed_multi_intent_tool_call(tool_call: Optional[Dict[str, Any]]) -> bool:
    return contracts.is_routed_multi_intent_tool_call(tool_call)


def _split_multi_intent_action_clauses(text: str) -> List[str]:
    return contracts.split_multi_intent_action_clauses(text)


def _score_clause_for_plugin_keywords(clause_text: str, keywords: List[str]) -> tuple[int, int]:
    return contracts.score_clause_for_plugin_keywords(clause_text, keywords)


def _route_clause_to_plugin(
    *,
    clause_text: str,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> str:
    return contracts.route_clause_to_plugin(
        clause_text=clause_text,
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
    )


def _build_multi_intent_routed_actions(
    *,
    request_text: str,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> List[Dict[str, str]]:
    return contracts.build_multi_intent_routed_actions(
        request_text=request_text,
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
    )


def _plugin_usage_argument_keys(plugin: Any) -> List[str]:
    return toolcall_utils.plugin_usage_argument_keys(
        plugin,
        parse_function_json_fn=parse_function_json,
    )


def _plugin_required_argument_keys(plugin: Any) -> List[str]:
    return toolcall_utils.plugin_required_argument_keys(plugin)


def _normalize_tool_call_for_user_request(
    *,
    tool_call: Dict[str, Any],
    registry: Dict[str, Any],
    user_text: str,
) -> Dict[str, Any]:
    del registry, user_text
    return toolcall_utils.normalize_tool_call_for_user_request(
        tool_call=tool_call,
        canonical_tool_name_fn=_canonical_tool_name,
        tool_call_route_metadata_fn=_tool_call_route_metadata,
    )


async def _build_structured_routed_tool_call(
    *,
    llm_client: Any,
    plugin_id: str,
    plugin_obj: Any,
    slice_text: str,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
    tool_index: str,
    origin: Optional[Dict[str, Any]],
    scope: str,
    history_messages: Optional[List[Dict[str, Any]]],
    context: Optional[Dict[str, Any]],
    platform_preamble: str,
    repair_max_tokens: Optional[int],
    recovery_max_tokens: Optional[int],
) -> Optional[Dict[str, Any]]:
    return await multi_intent_builder.build_structured_routed_tool_call(
        llm_client=llm_client,
        plugin_id=plugin_id,
        plugin_obj=plugin_obj,
        slice_text=slice_text,
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
        tool_index=tool_index,
        origin=origin,
        scope=scope,
        history_messages=history_messages,
        context=context,
        platform_preamble=platform_preamble,
        repair_max_tokens=repair_max_tokens,
        recovery_max_tokens=recovery_max_tokens,
        plugin_usage_argument_keys_fn=_plugin_usage_argument_keys,
        plugin_required_argument_keys_fn=_plugin_required_argument_keys,
        parse_function_json_fn=parse_function_json,
        with_platform_preamble_fn=_with_platform_preamble,
        default_tool_repair_max_tokens=DEFAULT_TOOL_REPAIR_MAX_TOKENS,
        coerce_text_fn=_coerce_text,
        validate_or_recover_tool_call_fn=_validate_or_recover_tool_call,
        canonical_tool_name_fn=_canonical_tool_name,
        multi_intent_route_user_text_key=_MULTI_INTENT_ROUTE_USER_TEXT_KEY,
        multi_intent_route_flag_key=_MULTI_INTENT_ROUTE_FLAG_KEY,
    )


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


def _configured_agent_state_ttl_seconds(redis_client: Any = None) -> int:
    global AGENT_STATE_TTL_SECONDS
    AGENT_STATE_TTL_SECONDS = runtime_config.configured_agent_state_ttl_seconds(
        redis_client=(redis_client or default_redis),
        key=CERBERUS_AGENT_STATE_TTL_SECONDS_KEY,
        default=DEFAULT_AGENT_STATE_TTL_SECONDS,
        redis_config_non_negative_int_fn=_redis_config_non_negative_int,
    )
    return AGENT_STATE_TTL_SECONDS


def _configured_max_ledger_items(redis_client: Any = None) -> int:
    return runtime_config.configured_positive_int(
        redis_client=(redis_client or default_redis),
        key=CERBERUS_MAX_LEDGER_ITEMS_KEY,
        default=DEFAULT_MAX_LEDGER_ITEMS,
        redis_config_positive_int_fn=_redis_config_positive_int,
    )


def _configured_planner_max_tokens(redis_client: Any = None) -> int:
    return runtime_config.configured_positive_int(
        redis_client=(redis_client or default_redis),
        key=CERBERUS_PLANNER_MAX_TOKENS_KEY,
        default=DEFAULT_PLANNER_MAX_TOKENS,
        redis_config_positive_int_fn=_redis_config_positive_int,
    )


def _configured_checker_max_tokens(redis_client: Any = None) -> int:
    return runtime_config.configured_positive_int(
        redis_client=(redis_client or default_redis),
        key=CERBERUS_CHECKER_MAX_TOKENS_KEY,
        default=DEFAULT_CHECKER_MAX_TOKENS,
        redis_config_positive_int_fn=_redis_config_positive_int,
    )


def _configured_doer_max_tokens(redis_client: Any = None) -> int:
    return runtime_config.configured_positive_int(
        redis_client=(redis_client or default_redis),
        key=CERBERUS_DOER_MAX_TOKENS_KEY,
        default=DEFAULT_DOER_MAX_TOKENS,
        redis_config_positive_int_fn=_redis_config_positive_int,
    )


def _configured_tool_repair_max_tokens(redis_client: Any = None) -> int:
    return runtime_config.configured_positive_int(
        redis_client=(redis_client or default_redis),
        key=CERBERUS_TOOL_REPAIR_MAX_TOKENS_KEY,
        default=DEFAULT_TOOL_REPAIR_MAX_TOKENS,
        redis_config_positive_int_fn=_redis_config_positive_int,
    )


def _configured_overclar_repair_max_tokens(redis_client: Any = None) -> int:
    return runtime_config.configured_positive_int(
        redis_client=(redis_client or default_redis),
        key=CERBERUS_OVERCLAR_REPAIR_MAX_TOKENS_KEY,
        default=DEFAULT_OVERCLAR_REPAIR_MAX_TOKENS,
        redis_config_positive_int_fn=_redis_config_positive_int,
    )


def _configured_recovery_max_tokens(redis_client: Any = None) -> int:
    return runtime_config.configured_positive_int(
        redis_client=(redis_client or default_redis),
        key=CERBERUS_RECOVERY_MAX_TOKENS_KEY,
        default=DEFAULT_RECOVERY_MAX_TOKENS,
        redis_config_positive_int_fn=_redis_config_positive_int,
    )


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


def _resolve_cerberus_scope(platform: str, scope: Any, origin: Optional[Dict[str, Any]]) -> str:
    return scope_helpers.resolve_cerberus_scope(
        platform,
        scope,
        origin,
        normalize_platform_fn=normalize_platform,
        clean_scope_text_fn=_clean_scope_text,
        scope_is_generic_fn=_scope_is_generic,
        derive_scope_from_origin_fn=_derive_scope_from_origin,
    )


def _memory_context_settings(redis_client: Any) -> Dict[str, Any]:
    return memory_context_helpers.memory_context_settings(redis_client)


def _memory_context_min_confidence(redis_client: Any) -> float:
    return memory_context_helpers.memory_context_min_confidence(
        redis_client,
        memory_context_settings_fn=_memory_context_settings,
    )


def _memory_context_max_items(redis_client: Any) -> int:
    return memory_context_helpers.memory_context_max_items(
        redis_client,
        memory_context_settings_fn=_memory_context_settings,
        coerce_non_negative_int_fn=_coerce_non_negative_int,
        default_items=_MEMORY_CONTEXT_DEFAULT_ITEMS,
    )


def _memory_context_value_max_chars(redis_client: Any) -> int:
    return memory_context_helpers.memory_context_value_max_chars(
        redis_client,
        memory_context_settings_fn=_memory_context_settings,
        coerce_non_negative_int_fn=_coerce_non_negative_int,
        default_value_max_chars=_MEMORY_CONTEXT_DEFAULT_VALUE_MAX_CHARS,
    )


def _memory_context_summary_max_chars(redis_client: Any) -> int:
    return memory_context_helpers.memory_context_summary_max_chars(
        redis_client,
        memory_context_settings_fn=_memory_context_settings,
        coerce_non_negative_int_fn=_coerce_non_negative_int,
        default_summary_max_chars=_MEMORY_CONTEXT_DEFAULT_SUMMARY_MAX_CHARS,
    )


def _origin_value(origin: Optional[Dict[str, Any]], *keys: str) -> str:
    return memory_context_helpers.origin_value(
        origin,
        *keys,
        coerce_text_fn=_coerce_text,
    )


def _memory_context_user_id(origin: Optional[Dict[str, Any]]) -> str:
    return memory_context_helpers.memory_context_user_id(
        origin,
        origin_value_fn=_origin_value,
    )


def _memory_context_user_display_name(origin: Optional[Dict[str, Any]]) -> str:
    return memory_context_helpers.memory_context_user_display_name(
        origin,
        origin_value_fn=_origin_value,
    )


def _memory_context_room_id(platform: str, scope: str, origin: Optional[Dict[str, Any]]) -> str:
    return memory_context_helpers.memory_context_room_id(
        platform,
        scope,
        origin,
        normalize_platform_fn=normalize_platform,
        clean_scope_text_fn=_clean_scope_text,
        scope_is_generic_fn=_scope_is_generic,
        origin_value_fn=_origin_value,
    )


def _memory_context_summary(items: List[Dict[str, Any]], *, value_max_chars: int) -> str:
    return memory_context_helpers.memory_context_summary(
        items,
        value_max_chars=value_max_chars,
        short_text_fn=_short_text,
        memory_value_to_text_fn=memory_value_to_text,
    )


def _memory_context_payload(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
    origin: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return memory_context_helpers.memory_context_payload(
        redis_client=redis_client,
        platform=platform,
        scope=scope,
        origin=origin,
        normalize_platform_fn=normalize_platform,
        memory_context_min_confidence_fn=_memory_context_min_confidence,
        memory_context_max_items_fn=_memory_context_max_items,
        memory_context_value_max_chars_fn=_memory_context_value_max_chars,
        memory_context_summary_max_chars_fn=_memory_context_summary_max_chars,
        memory_context_user_id_fn=_memory_context_user_id,
        memory_context_user_display_name_fn=_memory_context_user_display_name,
        memory_context_room_id_fn=_memory_context_room_id,
        resolve_memory_user_doc_key_fn=resolve_memory_user_doc_key,
        memory_user_doc_key_fn=memory_user_doc_key,
        load_memory_platform_doc_fn=load_memory_platform_doc,
        summarize_memory_platform_doc_fn=summarize_memory_platform_doc,
        memory_context_summary_fn=_memory_context_summary,
        memory_room_doc_key_fn=memory_room_doc_key,
    )


def _memory_context_system_message(payload: Dict[str, Any]) -> str:
    return memory_context_helpers.memory_context_system_message(
        payload,
        coerce_non_negative_int_fn=_coerce_non_negative_int,
        short_text_fn=_short_text,
        default_summary_max_chars=_MEMORY_CONTEXT_DEFAULT_SUMMARY_MAX_CHARS,
    )


def _coerce_non_negative_int(value: Any, default: int) -> int:
    return limits_helpers.coerce_non_negative_int(value, default)


def resolve_agent_limits(
    redis_client: Any = None,
    *,
    max_rounds: Optional[int] = None,
    max_tool_calls: Optional[int] = None,
) -> tuple[int, int]:
    return limits_helpers.resolve_agent_limits(
        redis_client=redis_client,
        max_rounds=max_rounds,
        max_tool_calls=max_tool_calls,
        fallback_redis=default_redis,
        coerce_non_negative_int_fn=_coerce_non_negative_int,
        default_max_rounds=DEFAULT_MAX_ROUNDS,
        default_max_tool_calls=DEFAULT_MAX_TOOL_CALLS,
        agent_max_rounds_key=AGENT_MAX_ROUNDS_KEY,
        agent_max_tool_calls_key=AGENT_MAX_TOOL_CALLS_KEY,
    )


def _estimated_requested_action_count(text: str) -> int:
    return limits_helpers.estimated_requested_action_count(
        text,
        max_budget=MULTI_ACTION_MAX_BUDGET,
    )


def _expand_limits_for_compound_request(
    *,
    max_rounds: int,
    max_tool_calls: int,
    request_text: str,
) -> tuple[int, int]:
    return limits_helpers.expand_limits_for_compound_request(
        max_rounds=max_rounds,
        max_tool_calls=max_tool_calls,
        request_text=request_text,
        estimated_requested_action_count_fn=_estimated_requested_action_count,
        min_budget=MULTI_ACTION_MIN_BUDGET,
        max_budget=MULTI_ACTION_MAX_BUDGET,
    )


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
        plugin_when_to_use_fn=plugin_when_to_use,
    )


def _plugin_arg_hint(plugin: Any) -> str:
    return tool_index_helpers.plugin_arg_hint(
        plugin,
        plugin_usage_argument_keys_fn=_plugin_usage_argument_keys,
    )


def _kernel_tool_purpose(tool_id: str) -> str:
    return tool_index_helpers.kernel_tool_purpose(
        tool_id,
        kernel_tool_purpose_hints=_KERNEL_TOOL_PURPOSE_HINTS,
    )


def _ordered_kernel_tool_ids() -> List[str]:
    return tool_index_helpers.ordered_kernel_tool_ids(
        kernel_tool_priority=_KERNEL_TOOL_PRIORITY,
        meta_tools=META_TOOLS,
    )


def _enabled_tool_mini_index(
    *,
    platform: str,
    registry: Dict[str, Any],
    enabled_predicate: Optional[Callable[[str], bool]],
) -> str:
    return tool_index_helpers.enabled_tool_mini_index(
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
        ordered_kernel_tool_ids_fn=_ordered_kernel_tool_ids,
        kernel_tool_purpose_fn=_kernel_tool_purpose,
        plugin_supports_platform_fn=plugin_supports_platform,
        plugin_arg_hint_fn=_plugin_arg_hint,
        tool_purpose_fn=_tool_purpose,
    )


def _compact_history(history_messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return common_helpers.compact_history(
        history_messages,
        coerce_text_fn=_coerce_text,
    )


def _platform_label(platform: str) -> str:
    return common_helpers.platform_label(
        platform,
        platform_display_map=_PLATFORM_DISPLAY,
    )


def _contains_action_intent(text: str) -> bool:
    return turn_classifiers.contains_action_intent(
        text,
        url_re=_URL_RE,
    )


def _is_acknowledgement_only(text: str) -> bool:
    return turn_classifiers.is_acknowledgement_only(
        text,
        contains_action_intent_fn=_contains_action_intent,
    )


def _is_stop_only(text: str) -> bool:
    return turn_classifiers.is_stop_only(
        text,
        contains_action_intent_fn=_contains_action_intent,
    )


def _is_casual_greeting_only(text: str) -> bool:
    return turn_classifiers.is_casual_greeting_only(
        text,
        contains_action_intent_fn=_contains_action_intent,
        url_re=_URL_RE,
        references_previous_work_fn=_references_previous_work,
        looks_like_schedule_request_fn=_looks_like_schedule_request,
        looks_like_weather_request_fn=_looks_like_weather_request,
        looks_like_send_message_intent_fn=_looks_like_send_message_intent,
    )


def _looks_like_over_clarification(text: str, *, user_text: str = "") -> bool:
    return turn_classifiers.looks_like_over_clarification(
        text,
        user_text=user_text,
        over_clarification_markers=_OVER_CLARIFICATION_MARKERS,
        looks_like_weather_request_fn=_looks_like_weather_request,
        looks_like_schedule_request_fn=_looks_like_schedule_request,
        contains_action_intent_fn=_contains_action_intent,
    )


def _strip_user_sender_prefix(text: str) -> str:
    return common_helpers.strip_user_sender_prefix(text)


def _latest_user_text(history_messages: List[Dict[str, Any]]) -> str:
    return common_helpers.latest_user_text(
        history_messages,
        strip_user_sender_prefix_fn=_strip_user_sender_prefix,
        coerce_text_fn=_coerce_text,
    )


def _looks_like_standalone_request(text: str) -> bool:
    return followup_intents.looks_like_standalone_request(
        text,
        is_acknowledgement_only_fn=_is_acknowledgement_only,
        is_stop_only_fn=_is_stop_only,
        url_re=_URL_RE,
    )


def _looks_like_short_followup(text: str) -> bool:
    return followup_intents.looks_like_short_followup(
        text,
        is_acknowledgement_only_fn=_is_acknowledgement_only,
        is_stop_only_fn=_is_stop_only,
        looks_like_standalone_request_fn=_looks_like_standalone_request,
    )


def _looks_like_download_followup(text: str) -> bool:
    return followup_intents.looks_like_download_followup(
        text,
        url_re=_URL_RE,
    )


def _looks_like_send_message_intent(text: str) -> bool:
    return followup_intents.looks_like_send_message_intent(
        text,
        url_re=_URL_RE,
    )


def _looks_like_link_list_request(text: str) -> bool:
    return followup_intents.looks_like_link_list_request(
        text,
    )


def _web_research_url_key(url: Any) -> str:
    return web_research_helpers.web_research_url_key(url)


def _extract_web_search_candidates(payload: Optional[Dict[str, Any]], *, max_candidates: int) -> List[Dict[str, str]]:
    return web_research_helpers.extract_web_search_candidates(
        payload,
        max_candidates=max_candidates,
        default_max_candidates=_WEB_RESEARCH_MAX_CANDIDATES,
        web_research_url_key_fn=_web_research_url_key,
    )


def _next_web_research_tool_call(
    *,
    candidates: List[Dict[str, str]],
    seen_urls: set[str],
) -> Optional[Dict[str, Any]]:
    return web_research_helpers.next_web_research_tool_call(
        candidates=candidates,
        seen_urls=seen_urls,
        web_research_url_key_fn=_web_research_url_key,
    )


def _web_inspection_is_sufficient(payload: Optional[Dict[str, Any]]) -> bool:
    return web_research_helpers.web_inspection_is_sufficient(
        payload,
        canonical_tool_name_fn=_canonical_tool_name,
        min_preview_chars=_WEB_RESEARCH_MIN_PREVIEW_CHARS,
        min_preview_words=_WEB_RESEARCH_MIN_PREVIEW_WORDS,
    )


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


def send_message_allowed(
    *,
    user_text: str,
    tool_args: Optional[Dict[str, Any]],
    origin: Optional[Dict[str, Any]],
    platform: str,
    history_messages: Optional[List[Dict[str, Any]]],
    context: Optional[Dict[str, Any]],
) -> tuple[bool, str]:
    return common_helpers.send_message_allowed(
        user_text=user_text,
        tool_args=tool_args,
        origin=origin,
        platform=platform,
        history_messages=history_messages,
        context=context,
        looks_like_send_message_intent_fn=_looks_like_send_message_intent,
    )


def _looks_like_schedule_request(text: str) -> bool:
    return common_helpers.looks_like_schedule_request(
        text,
    )


def _looks_like_weather_request(text: str) -> bool:
    return common_helpers.looks_like_weather_request(
        text,
    )


def _mentions_explicit_weather_location(text: str) -> bool:
    return common_helpers.mentions_explicit_weather_location(
        text,
    )


def _mentions_explicit_timezone(text: str) -> bool:
    return common_helpers.mentions_explicit_timezone(
        text,
    )


def _looks_like_explicit_ai_task_request(text: str) -> bool:
    return common_helpers.looks_like_explicit_ai_task_request(
        text,
        looks_like_schedule_request_fn=_looks_like_schedule_request,
    )


def _ai_tasks_schedule_status(
    *,
    payload: Optional[Dict[str, Any]],
    checker_result: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    return common_helpers.ai_tasks_schedule_status(
        payload=payload,
        checker_result=checker_result,
        short_text_fn=_short_text,
        is_low_information_text_fn=_is_low_information_text,
    )


def _latest_url_from_history(history_messages: List[Dict[str, Any]]) -> str:
    return common_helpers.latest_url_from_history(
        history_messages,
        coerce_text_fn=_coerce_text,
        url_re=_URL_RE,
    )


def _effective_user_text(user_text: str, history_messages: List[Dict[str, Any]]) -> str:
    return common_helpers.effective_user_text(
        user_text,
        history_messages,
        looks_like_short_followup_fn=_looks_like_short_followup,
        latest_user_text_fn=_latest_user_text,
        looks_like_download_followup_fn=_looks_like_download_followup,
        latest_url_from_history_fn=_latest_url_from_history,
    )


def _planner_focus_prompt(*, current_user_text: str, resolved_user_text: str) -> str:
    return prompts.planner_focus_prompt(
        current_user_text=current_user_text,
        resolved_user_text=resolved_user_text,
    )


def _planner_system_prompt(platform: str) -> str:
    first, last = get_tater_name()
    return prompts.planner_system_prompt(
        platform=platform,
        platform_label=_platform_label(platform),
        now_text=datetime.now().strftime("%A, %B %d, %Y at %I:%M %p"),
        first_name=first,
        last_name=last,
        personality=(get_tater_personality() or "").strip(),
        ascii_only_platforms=ASCII_ONLY_PLATFORMS,
    ).strip()


def _checker_system_prompt(platform: str, retry_allowed: bool) -> str:
    return prompts.checker_system_prompt(
        platform=platform,
        retry_allowed=retry_allowed,
        ascii_only_platforms=ASCII_ONLY_PLATFORMS,
    ).strip()


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
        plugin_supports_platform_fn=plugin_supports_platform,
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
        configured_tool_repair_max_tokens_fn=_configured_tool_repair_max_tokens,
        coerce_text_fn=_coerce_text,
        user_text=user_text,
        tool_name_hint=tool_name_hint,
        platform_preamble=platform_preamble,
        max_tokens=max_tokens,
    )


async def _repair_over_clarification_text(
    *,
    llm_client: Any,
    platform: str,
    user_text: str,
    planner_text: str,
    tool_index: str,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> str:
    return await overclar_repair.repair_over_clarification_text(
        llm_client=llm_client,
        platform=platform,
        user_text=user_text,
        planner_text=planner_text,
        tool_index=tool_index,
        platform_preamble=platform_preamble,
        max_tokens=max_tokens,
        configured_overclar_repair_max_tokens_fn=_configured_overclar_repair_max_tokens,
        with_platform_preamble_fn=_with_platform_preamble,
        coerce_text_fn=_coerce_text,
    )


async def _repair_need_user_info_if_overclar(
    *,
    llm_client: Any,
    platform: str,
    user_text: str,
    question_text: str,
    tool_index: str,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    return await overclar_repair.repair_need_user_info_if_overclar(
        llm_client=llm_client,
        platform=platform,
        user_text=user_text,
        question_text=question_text,
        tool_index=tool_index,
        platform_preamble=platform_preamble,
        max_tokens=max_tokens,
        looks_like_over_clarification_fn=lambda text: _looks_like_over_clarification(
            text,
            user_text=user_text,
        ),
        repair_over_clarification_text_fn=_repair_over_clarification_text,
        is_tool_candidate_fn=_is_tool_candidate,
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
    repair_max_tokens: Optional[int] = None,
    recovery_max_tokens: Optional[int] = None,
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
        repair_max_tokens=repair_max_tokens,
        recovery_max_tokens=recovery_max_tokens,
        is_tool_candidate_fn=_is_tool_candidate,
        validate_tool_contract_fn=_validate_tool_contract,
        short_text_fn=_short_text,
        redirect_unknown_tool_to_search_files_fn=_redirect_unknown_tool_to_search_files,
        generate_recovery_text_fn=_generate_recovery_text,
        validation_failure_text_fn=_validation_failure_text,
        normalize_tool_call_for_user_request_fn=_normalize_tool_call_for_user_request,
    )


def _looks_like_shell_tool_name(value: Any) -> bool:
    return validation.looks_like_shell_tool_name(
        value,
        canonical_tool_name_fn=_canonical_tool_name,
    )


def _workspace_discovery_query(user_text: str) -> str:
    return validation.workspace_discovery_query(
        user_text,
        stopwords=_WORKSPACE_QUERY_STOPWORDS,
    )


def _redirect_unknown_tool_to_search_files(
    *,
    reason: str,
    user_text: str,
    tool_call: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    return validation.redirect_unknown_tool_to_search_files(
        reason=reason,
        user_text=user_text,
        tool_call=tool_call,
        canonical_tool_name_fn=_canonical_tool_name,
        workspace_discovery_hint_re=_WORKSPACE_DISCOVERY_HINT_RE,
        workspace_query_stopwords=_WORKSPACE_QUERY_STOPWORDS,
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
        configured_recovery_max_tokens_fn=_configured_recovery_max_tokens,
        looks_like_tool_markup_fn=looks_like_tool_markup,
        parse_function_json_fn=parse_function_json,
        checker_decision_prefix_re=_CHECKER_DECISION_PREFIX_RE,
        default_clarification=DEFAULT_CLARIFICATION,
        coerce_text_fn=_coerce_text,
        platform_preamble=platform_preamble,
        max_tokens=max_tokens,
    )


async def _normalize_tool_result_for_checker(
    *,
    result_payload: Any,
    llm_client: Any,
    platform: str,
) -> Dict[str, Any]:
    return await execution.normalize_tool_result_for_checker(
        result_payload=result_payload,
        llm_client=llm_client,
        platform=platform,
        normalize_plugin_result_fn=normalize_plugin_result,
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
    wait_callback: Optional[Callable[[str, Any], Any]],
    admin_guard: Optional[Callable[[str], Optional[Dict[str, Any]]]],
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
        admin_guard=admin_guard,
        canonical_tool_name_fn=_canonical_tool_name,
        attach_origin_fn=_attach_origin,
        normalize_plugin_result_fn=normalize_plugin_result,
        normalize_tool_result_for_checker_fn=_normalize_tool_result_for_checker,
        action_failure_fn=action_failure,
        plugin_display_name_fn=plugin_display_name,
        expand_plugin_platforms_fn=expand_plugin_platforms,
        plugin_supports_platform_fn=plugin_supports_platform,
        is_meta_tool_fn=is_meta_tool,
        run_meta_tool_fn=run_meta_tool,
        execute_plugin_call_fn=execute_plugin_call,
    )


def _parse_checker_decision(text: str) -> Dict[str, Any]:
    return checker.parse_checker_decision(
        text,
        checker_decision_prefix_re=_CHECKER_DECISION_PREFIX_RE,
        parse_function_json_fn=parse_function_json,
        is_tool_candidate_fn=_is_tool_candidate,
        normalize_checker_kind_fn=_normalize_checker_kind,
    )


async def _run_checker(
    *,
    llm_client: Any,
    platform: str,
    current_user_text: str,
    resolved_user_text: str,
    agent_state: Optional[Dict[str, Any]],
    memory_context: Optional[Dict[str, Any]],
    planned_tool: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    draft_response: str,
    retry_allowed: bool,
    platform_preamble: str = "",
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    return await checker.run_checker(
        llm_client=llm_client,
        platform=platform,
        current_user_text=current_user_text,
        resolved_user_text=resolved_user_text,
        agent_state=agent_state,
        memory_context=memory_context,
        planned_tool=planned_tool,
        tool_result=tool_result,
        draft_response=draft_response,
        retry_allowed=retry_allowed,
        normalize_agent_state_fn=lambda state, fallback: _normalize_agent_state(state, fallback_goal=fallback),
        coerce_non_negative_int_fn=_coerce_non_negative_int,
        short_text_fn=_short_text,
        memory_context_default_summary_max_chars=_MEMORY_CONTEXT_DEFAULT_SUMMARY_MAX_CHARS,
        configured_checker_max_tokens_fn=_configured_checker_max_tokens,
        checker_system_prompt_fn=lambda plat, retry: _checker_system_prompt(plat, retry_allowed=retry),
        with_platform_preamble_fn=lambda messages, preamble: _with_platform_preamble(
            messages, platform_preamble=preamble
        ),
        parse_checker_decision_fn=_parse_checker_decision,
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
        checker_decision_prefix_re=_CHECKER_DECISION_PREFIX_RE,
        ascii_only_platforms=ASCII_ONLY_PLATFORMS,
    )


def _short_text(value: Any, *, limit: int = 280) -> str:
    return common_helpers.short_text(value, limit=limit)


def _normalize_checker_kind(label: str) -> str:
    return checker.normalize_checker_kind(label)


def _is_low_information_text(value: Any) -> bool:
    return common_helpers.is_low_information_text(value)


def _first_json_object(text: str) -> Optional[Dict[str, Any]]:
    return common_helpers.first_json_object(
        text,
        coerce_text_fn=_coerce_text,
    )


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


def _normalize_agent_state(state: Optional[Dict[str, Any]], *, fallback_goal: str) -> Dict[str, Any]:
    return state_core_helpers.normalize_agent_state(
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


def _agent_state_key(*, platform: str, scope: str) -> str:
    return state_store.agent_state_key(
        platform=platform,
        scope=scope,
        normalize_platform_fn=normalize_platform,
        clean_scope_text_fn=_clean_scope_text,
        scope_is_generic_fn=_scope_is_generic,
        unknown_scope_fn=_unknown_scope,
        agent_state_key_prefix=AGENT_STATE_KEY_PREFIX,
    )


_AGENT_STATE_REQUIRED_KEYS = ("goal", "plan", "facts", "open_questions", "next_step", "tool_history")


def _has_required_agent_state_keys(state: Any) -> bool:
    return state_store.has_required_agent_state_keys(
        state,
        required_keys=_AGENT_STATE_REQUIRED_KEYS,
    )


def _load_persistent_agent_state(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
) -> Optional[Dict[str, Any]]:
    return state_store.load_persistent_agent_state(
        redis_client=redis_client,
        platform=platform,
        scope=scope,
        normalize_platform_fn=normalize_platform,
        clean_scope_text_fn=_clean_scope_text,
        scope_is_generic_fn=_scope_is_generic,
        unknown_scope_fn=_unknown_scope,
        agent_state_key_prefix=AGENT_STATE_KEY_PREFIX,
        coerce_text_fn=_coerce_text,
        first_json_object_fn=_first_json_object,
        normalize_agent_state_fn=lambda s, fallback_goal: _normalize_agent_state(s, fallback_goal=fallback_goal),
        required_keys=_AGENT_STATE_REQUIRED_KEYS,
    )


def _save_persistent_agent_state(
    *,
    redis_client: Any,
    platform: str,
    scope: str,
    state: Optional[Dict[str, Any]],
) -> None:
    return state_store.save_persistent_agent_state(
        redis_client=redis_client,
        platform=platform,
        scope=scope,
        state=state,
        normalize_platform_fn=normalize_platform,
        clean_scope_text_fn=_clean_scope_text,
        scope_is_generic_fn=_scope_is_generic,
        unknown_scope_fn=_unknown_scope,
        agent_state_key_prefix=AGENT_STATE_KEY_PREFIX,
        configured_agent_state_ttl_seconds_fn=_configured_agent_state_ttl_seconds,
        normalize_agent_state_fn=lambda s, fallback_goal: _normalize_agent_state(s, fallback_goal=fallback_goal),
        required_keys=_AGENT_STATE_REQUIRED_KEYS,
    )


def _references_previous_work(text: str) -> bool:
    return state_core_helpers.references_previous_work(
        text,
    )


def _looks_like_short_followup_request(text: str) -> bool:
    return state_core_helpers.looks_like_short_followup_request(
        text,
        references_previous_work_fn=_references_previous_work,
    )


def _should_reset_state_for_topic_change(current_user_text: str) -> bool:
    return state_core_helpers.should_reset_state_for_topic_change(
        current_user_text,
        contains_new_domain_reset_keywords_fn=_contains_new_domain_reset_keywords,
        references_explicit_prior_work_fn=_references_explicit_prior_work,
        looks_like_short_followup_request_fn=_looks_like_short_followup_request,
        references_previous_work_fn=_references_previous_work,
        looks_like_standalone_request_fn=_looks_like_standalone_request,
    )


def _contains_new_domain_reset_keywords(text: str) -> bool:
    return state_core_helpers.contains_new_domain_reset_keywords(
        text,
    )


def _references_explicit_prior_work(text: str) -> bool:
    return state_core_helpers.references_explicit_prior_work(
        text,
    )


def _new_agent_state(goal: str) -> Dict[str, Any]:
    return state_core_helpers.new_agent_state(
        goal,
        normalize_agent_state_fn=_normalize_agent_state,
    )


def _initial_agent_state_for_turn(
    *,
    prior_state: Optional[Dict[str, Any]],
    current_user_text: str,
    resolved_user_text: str,
) -> Dict[str, Any]:
    return state_core_helpers.initial_agent_state_for_turn(
        prior_state=prior_state,
        current_user_text=current_user_text,
        resolved_user_text=resolved_user_text,
        short_text_fn=_short_text,
        should_reset_state_for_topic_change_fn=_should_reset_state_for_topic_change,
        new_agent_state_fn=_new_agent_state,
        normalize_agent_state_fn=_normalize_agent_state,
    )


def _state_add_line(state_list: List[str], line: str, *, max_items: int) -> List[str]:
    return doer_state.state_add_line(
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
    return doer_state.tool_history_line(
        tool_call=tool_call,
        tool_result=tool_result,
        short_text_fn=_short_text,
    )


def _compact_tool_result_for_doer(tool_result: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return doer_state.compact_tool_result_for_doer(
        tool_result,
        short_text_fn=_short_text,
    )


async def _run_doer_state_update(
    *,
    llm_client: Any,
    platform: str,
    user_request: str,
    prior_state: Optional[Dict[str, Any]],
    tool_call: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    max_tokens: Optional[int] = None,
) -> Dict[str, Any]:
    return await doer_state.run_doer_state_update(
        llm_client=llm_client,
        platform=platform,
        user_request=user_request,
        prior_state=prior_state,
        tool_call=tool_call,
        tool_result=tool_result,
        max_tokens=max_tokens,
        normalize_agent_state_fn=lambda s, fallback_goal: _normalize_agent_state(s, fallback_goal=fallback_goal),
        configured_doer_max_tokens_fn=_configured_doer_max_tokens,
        coerce_text_fn=_coerce_text,
        first_json_object_fn=_first_json_object,
        state_add_line_fn=lambda items, line, max_items: _state_add_line(items, line, max_items=max_items),
        tool_history_line_fn=lambda call, result: _tool_history_line(tool_call=call, tool_result=result),
        short_text_fn=_short_text,
        is_low_information_text_fn=_is_low_information_text,
        state_list_fn=lambda values, max_items, item_limit: _state_list(values, max_items=max_items, item_limit=item_limit),
    )


def _state_first_open_question(state: Optional[Dict[str, Any]]) -> str:
    return doer_state.state_first_open_question(
        state,
        short_text_fn=_short_text,
    )


def _state_best_effort_answer(
    *,
    state: Optional[Dict[str, Any]],
    draft_response: str,
    tool_result: Optional[Dict[str, Any]],
) -> str:
    return doer_state.state_best_effort_answer(
        state=state,
        draft_response=draft_response,
        tool_result=tool_result,
        short_text_fn=_short_text,
        is_low_information_text_fn=_is_low_information_text,
    )


def _response_indicates_unfinished_work(text: str) -> bool:
    return doer_state.response_indicates_unfinished_work(text)


def _should_continue_after_incomplete_final_answer(
    *,
    user_text: str,
    final_text: str,
    agent_state: Optional[Dict[str, Any]],
    retry_allowed: bool,
) -> bool:
    del agent_state
    return doer_state.should_continue_after_incomplete_final_answer(
        user_text=user_text,
        final_text=final_text,
        retry_allowed=retry_allowed,
        contains_action_intent_fn=_contains_action_intent,
        looks_like_weather_request_fn=_looks_like_weather_request,
        looks_like_schedule_request_fn=_looks_like_schedule_request,
        looks_like_send_message_intent_fn=_looks_like_send_message_intent,
        response_indicates_unfinished_work_fn=_response_indicates_unfinished_work,
    )


def _tool_failure_checker_reason(tool_result: Optional[Dict[str, Any]]) -> str:
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


def _tool_call_signature(tool_call: Optional[Dict[str, Any]]) -> str:
    return retry_helpers.tool_call_signature(
        tool_call,
        canonical_tool_name_fn=_canonical_tool_name,
        hash_tool_args_fn=_hash_tool_args,
        tool_call_route_metadata_fn=_tool_call_route_metadata,
        route_user_text_key=_MULTI_INTENT_ROUTE_USER_TEXT_KEY,
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
        tool_call_route_metadata_fn=_tool_call_route_metadata,
    )


def _user_disallows_overwrite(text: str) -> bool:
    return common_helpers.user_disallows_overwrite(
        text,
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


def _write_cerberus_metrics(
    *,
    redis_client: Any,
    platform: str,
    total_tools_called: int,
    total_repairs: int,
    validation_failures: int,
    tool_failures: int,
) -> None:
    return ledger.write_cerberus_metrics(
        redis_client=redis_client,
        platform=platform,
        total_tools_called=total_tools_called,
        total_repairs=total_repairs,
        validation_failures=validation_failures,
        tool_failures=tool_failures,
        normalize_platform_fn=normalize_platform,
    )


def _write_cerberus_ledger(
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
    return ledger.write_cerberus_ledger(
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
        schema_version=CERBERUS_LEDGER_SCHEMA_VERSION,
        agent_state_ledger_max_chars=AGENT_STATE_LEDGER_MAX_CHARS,
        allowed_planner_kinds=("tool", "answer", "repaired_tool", "repaired_answer", "send_message_fix"),
    )


def _is_tool_candidate(text: str) -> bool:
    return toolcall_utils.is_tool_candidate(
        text,
        parse_strict_tool_json_fn=_parse_strict_tool_json,
        parse_function_json_fn=parse_function_json,
        looks_like_tool_markup_fn=looks_like_tool_markup,
        looks_like_invalid_tool_call_text_fn=_looks_like_invalid_tool_call_text,
    )


async def run_cerberus_turn(
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
    wait_callback: Optional[Callable[[str, Any], Any]] = None,
    admin_guard: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    redis_client: Any = None,
    max_rounds: Optional[int] = None,
    max_tool_calls: Optional[int] = None,
    platform_preamble: str = "",
) -> Dict[str, Any]:
    r = redis_client or default_redis
    platform = normalize_platform(platform)
    origin_payload = dict(origin) if isinstance(origin, dict) else {}
    scope = _resolve_cerberus_scope(platform, scope, origin_payload)
    platform_preamble = _sanitize_platform_preamble(platform, platform_preamble)
    origin_preview = _origin_preview_for_ledger(origin_payload)
    user_text = str(user_text or "")
    effective_max_rounds, effective_max_tool_calls = resolve_agent_limits(
        redis_client=r,
        max_rounds=max_rounds,
        max_tool_calls=max_tool_calls,
    )
    planner_max_tokens = _configured_planner_max_tokens(r)
    checker_max_tokens = _configured_checker_max_tokens(r)
    doer_max_tokens = _configured_doer_max_tokens(r)
    tool_repair_max_tokens = _configured_tool_repair_max_tokens(r)
    overclar_repair_max_tokens = _configured_overclar_repair_max_tokens(r)
    recovery_max_tokens = _configured_recovery_max_tokens(r)
    turn_started_at = time.perf_counter()
    planner_ms_total = 0.0
    tool_ms_total = 0.0
    checker_ms_total = 0.0
    repairs_used_count = 0
    validation_failures_count = 0
    tool_failures_count = 0
    turn_id = str(uuid.uuid4())
    llm_label = _llm_backend_label(llm_client)

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
    normalized_checker_result_out: Optional[Dict[str, Any]] = None
    artifacts_out: List[Dict[str, Any]] = []
    rounds_used = 0
    tool_calls_used = 0
    critic_continue_count = 0
    draft_response = ""
    tool_used = False
    planner_kind = "answer"
    planner_text_is_tool_candidate = False
    attempted_tool_for_ledger = ""

    history = _compact_history(history_messages)
    effective_user_text = _effective_user_text(user_text, history)
    resolved_user_text = effective_user_text or user_text
    current_user_turn_text = _strip_user_sender_prefix(user_text).strip() or str(user_text or "").strip()
    suppress_tools_for_turn = _is_casual_greeting_only(current_user_turn_text)
    request_for_limit_eval = current_user_turn_text or str(resolved_user_text or "")
    effective_max_rounds, effective_max_tool_calls = _expand_limits_for_compound_request(
        max_rounds=effective_max_rounds,
        max_tool_calls=effective_max_tool_calls,
        request_text=request_for_limit_eval,
    )
    tool_index = _enabled_tool_mini_index(
        platform=platform,
        registry=registry,
        enabled_predicate=enabled_predicate,
    )
    prior_state = _load_persistent_agent_state(
        redis_client=r,
        platform=platform,
        scope=scope,
    )
    agent_state: Dict[str, Any] = _initial_agent_state_for_turn(
        prior_state=prior_state,
        current_user_text=current_user_turn_text,
        resolved_user_text=resolved_user_text,
    )
    memory_context_payload = _memory_context_payload(
        redis_client=r,
        platform=platform,
        scope=scope,
        origin=origin_payload,
    )
    memory_context_message = _memory_context_system_message(memory_context_payload)
    queued_tool_call: Optional[Dict[str, Any]] = None
    queued_retry_tool_for_ledger: Optional[Dict[str, Any]] = None
    plugin_help_attempted: set[str] = set()
    plugin_help_prefetch_target = ""
    bad_args_help_retry_signatures: set[str] = set()
    bad_args_help_pending: Optional[Dict[str, Any]] = None
    workspace_discovery_read_attempted_paths: set[str] = set()
    web_research_candidates: List[Dict[str, str]] = []
    web_research_seen_urls: set[str] = set()
    web_research_attempts = 0
    web_research_active = False
    web_research_skip_deepening = _looks_like_link_list_request(resolved_user_text or user_text)
    multi_intent_routed_queue: List[Dict[str, Any]] = []
    multi_intent_routing_active = False
    multi_intent_routed_summaries: List[str] = []

    if not suppress_tools_for_turn:
        routed_actions = _build_multi_intent_routed_actions(
            request_text=current_user_turn_text or resolved_user_text,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
        )
        built_routed_calls: List[Dict[str, Any]] = []
        route_build_failed = False
        for action in routed_actions:
            plugin_id = _canonical_tool_name(action.get("plugin_id"))
            slice_text = str(action.get("user_text") or "").strip()
            if not plugin_id or not slice_text:
                route_build_failed = True
                break
            plugin_obj = registry.get(plugin_id)
            if plugin_obj is None:
                route_build_failed = True
                break

            route_started = time.perf_counter()
            structured_call = await _build_structured_routed_tool_call(
                llm_client=llm_client,
                plugin_id=plugin_id,
                plugin_obj=plugin_obj,
                slice_text=slice_text,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
                tool_index=tool_index,
                origin=origin_payload,
                scope=scope,
                history_messages=history,
                context=context if isinstance(context, dict) else {},
                platform_preamble=platform_preamble,
                repair_max_tokens=tool_repair_max_tokens,
                recovery_max_tokens=recovery_max_tokens,
            )
            planner_ms_total += (time.perf_counter() - route_started) * 1000.0
            if not isinstance(structured_call, dict):
                route_build_failed = True
                break
            built_routed_calls.append(structured_call)

        if not route_build_failed and len(built_routed_calls) >= 2:
            multi_intent_routed_queue = built_routed_calls
            multi_intent_routing_active = True
            queued_tool_call = dict(multi_intent_routed_queue.pop(0))
            queued_retry_tool_for_ledger = queued_tool_call
            validation_status = {
                "status": "ok",
                "repair_used": False,
                "reason": "multi_intent_route",
                "attempts": 1,
                "ok": True,
                "tool_call": queued_tool_call,
            }

    def _retry_allowed_within_limits() -> bool:
        rounds_left = effective_max_rounds == 0 or rounds_used < effective_max_rounds
        tools_left = effective_max_tool_calls == 0 or tool_calls_used < effective_max_tool_calls
        return rounds_left and tools_left

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
        _write_cerberus_ledger(
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
            planner_ms=int(max(0.0, planner_ms_total)),
            tool_ms=int(max(0.0, tool_ms_total)),
            checker_ms=int(max(0.0, checker_ms_total)),
            total_ms=total_ms,
            retry_tool=retry_tool,
            rounds_used=rounds_used,
            tool_calls_used=tool_calls_used,
            agent_state=agent_state,
            origin_preview=origin_preview,
            attempted_tool=attempted_tool_override if attempted_tool_override is not None else attempted_tool_for_ledger,
        )
        _write_cerberus_metrics(
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
            "artifacts": artifacts_out,
            "raw_tool_payload": raw_tool_payload_out,
            "normalized_checker_result": normalized_checker_result_out,
        }

    while (
        (effective_max_rounds == 0 or rounds_used < effective_max_rounds)
        and (effective_max_tool_calls == 0 or tool_calls_used < effective_max_tool_calls)
    ):
        rounds_used += 1
        planned_tool = None
        planner_text = ""
        round_planner_kind = "answer"

        if isinstance(queued_tool_call, dict):
            planned_tool = dict(queued_tool_call)
            attempted_tool_for_ledger = str((planned_tool or {}).get("function") or attempted_tool_for_ledger or "")
            planner_text_is_tool_candidate = True
            validation_status = {
                "status": "ok",
                "repair_used": bool(validation_status.get("repair_used")),
                "reason": str(validation_status.get("reason") or "ok"),
                "attempts": int(validation_status.get("attempts") or 1),
                "ok": True,
                "tool_call": planned_tool,
            }
            round_planner_kind = "repaired_tool" if bool(validation_status.get("repair_used")) else "tool"
            queued_tool_call = None
        else:
            planner_text_repaired = False
            state_message = _agent_state_prompt_message(agent_state, fallback_goal=resolved_user_text or user_text)
            planner_messages: List[Dict[str, Any]] = [
                {"role": "system", "content": _planner_system_prompt(platform)},
                {"role": "system", "content": "Enabled tools on this platform:\n" + tool_index},
            ]
            planner_messages.extend([
                {
                    "role": "system",
                    "content": _planner_focus_prompt(
                        current_user_text=user_text,
                        resolved_user_text=resolved_user_text,
                    ),
                },
                {"role": "system", "content": state_message},
            ])
            if memory_context_message:
                planner_messages.append({"role": "system", "content": memory_context_message})
            planner_messages = _with_platform_preamble(
                planner_messages,
                platform_preamble=platform_preamble,
            )
            if not suppress_tools_for_turn:
                planner_messages.extend(history)
            planner_messages.append({"role": "user", "content": resolved_user_text})

            try:
                planner_started = time.perf_counter()
                planner_resp = await llm_client.chat(
                    messages=planner_messages,
                    max_tokens=max(1, int(planner_max_tokens)),
                    temperature=0.2,
                )
                planner_ms_total += (time.perf_counter() - planner_started) * 1000.0
                planner_text = _coerce_text((planner_resp.get("message", {}) or {}).get("content", "")).strip()
            except Exception:
                planner_text = ""

            if planner_text and not _is_tool_candidate(planner_text) and _looks_like_over_clarification(
                planner_text,
                user_text=resolved_user_text or user_text,
            ):
                original_planner_text = planner_text
                repaired_text = await _repair_over_clarification_text(
                    llm_client=llm_client,
                    platform=platform,
                    user_text=effective_user_text or user_text,
                    planner_text=planner_text,
                    tool_index=tool_index,
                    platform_preamble=platform_preamble,
                    max_tokens=overclar_repair_max_tokens,
                )
                if repaired_text:
                    planner_text = repaired_text
                    if planner_text.strip() != original_planner_text.strip():
                        planner_text_repaired = True
                        repairs_used_count += 1

            if _is_tool_candidate(planner_text):
                round_planner_kind = "tool"
            elif planner_text_repaired:
                round_planner_kind = "repaired_answer"
            else:
                round_planner_kind = "answer"
            planner_text_is_tool_candidate = _is_tool_candidate(planner_text)
            if suppress_tools_for_turn and planner_text_is_tool_candidate:
                planner_text = "Hey. What would you like me to do?"
                planner_text_is_tool_candidate = False
                planner_text_repaired = True
                round_planner_kind = "repaired_answer"

            if not _is_tool_candidate(planner_text):
                planner_kind = round_planner_kind
                draft_response = str(planner_text or "").strip()
                checker_started = time.perf_counter()
                checker_decision = await _run_checker(
                    llm_client=llm_client,
                    platform=platform,
                    current_user_text=user_text,
                    resolved_user_text=resolved_user_text,
                    agent_state=agent_state,
                    memory_context=memory_context_payload,
                    planned_tool=None,
                    tool_result=tool_result_for_checker,
                    draft_response=draft_response,
                    retry_allowed=_retry_allowed_within_limits() and not suppress_tools_for_turn,
                    platform_preamble=platform_preamble,
                    max_tokens=checker_max_tokens,
                )
                checker_ms_total += (time.perf_counter() - checker_started) * 1000.0
                checker_action = str(checker_decision.get("kind") or "FINAL_ANSWER")

                if checker_action == "NEED_USER_INFO":
                    need_text = str(checker_decision.get("text") or DEFAULT_CLARIFICATION).strip()
                    repaired_need = await _repair_need_user_info_if_overclar(
                        llm_client=llm_client,
                        platform=platform,
                        user_text=resolved_user_text or user_text,
                        question_text=need_text,
                        tool_index=tool_index,
                        platform_preamble=platform_preamble,
                        max_tokens=overclar_repair_max_tokens,
                    )
                    if bool(repaired_need.get("repaired")):
                        repairs_used_count += 1
                    repaired_kind = str(repaired_need.get("kind") or "NEED_USER_INFO").strip().upper()
                    if repaired_kind == "RETRY_TOOL":
                        checker_action = "RETRY_TOOL"
                        checker_decision = {"kind": "RETRY_TOOL", "text": str(repaired_need.get("text") or "").strip()}
                    elif repaired_kind == "FINAL_ANSWER":
                        checker_reason = "overclar_repaired"
                        return _finish(
                            text=str(repaired_need.get("text") or need_text or DEFAULT_CLARIFICATION).strip(),
                            status="done",
                            checker_action_value="FINAL_ANSWER",
                            checker_reason_value=checker_reason,
                        )
                    else:
                        checker_reason = "needs_user_input"
                        return _finish(
                            text=need_text,
                            status="blocked",
                            checker_action_value="NEED_USER_INFO",
                            checker_reason_value=checker_reason,
                        )

                if checker_action == "RETRY_TOOL":
                    retry_text = str(checker_decision.get("text") or "").strip()
                    if not _retry_allowed_within_limits():
                        queued_retry_tool_for_ledger = parse_function_json(retry_text)
                        planner_text_is_tool_candidate = True
                        checker_reason = "budget_exhausted"
                        break
                    retry_eval = await _validate_or_recover_tool_call(
                        llm_client=llm_client,
                        text=retry_text,
                        platform=platform,
                        registry=registry,
                        enabled_predicate=enabled_predicate,
                        tool_index=tool_index,
                        user_text=user_text,
                        origin=origin_payload,
                        scope=scope,
                        history_messages=history,
                        context=context if isinstance(context, dict) else {},
                        platform_preamble=platform_preamble,
                        repair_max_tokens=tool_repair_max_tokens,
                        recovery_max_tokens=recovery_max_tokens,
                    )
                    retry_validation = (
                        retry_eval.get("validation_status")
                        if isinstance(retry_eval.get("validation_status"), dict)
                        else {"status": "failed", "reason": str(retry_eval.get("reason") or "invalid_tool_call")}
                    )
                    attempted_tool_for_ledger = str(retry_eval.get("attempted_tool") or attempted_tool_for_ledger or "")
                    if bool(retry_eval.get("repair_used")):
                        repairs_used_count += 1
                    if not bool(retry_eval.get("ok")):
                        reason = str(retry_eval.get("reason") or "invalid_tool_call")
                        assistant_text = str(retry_eval.get("assistant_text") or "").strip()
                        failed_retry_tool = retry_eval.get("tool_call")
                        if not isinstance(failed_retry_tool, dict):
                            failed_retry_tool = {"function": "invalid_tool_call", "arguments": {}}
                        if reason == "repair_returned_answer" and assistant_text:
                            planner_kind_value = (
                                "send_message_fix"
                                if _canonical_tool_name((failed_retry_tool or {}).get("function")) == "send_message"
                                else "repaired_answer"
                            )
                            checker_reason = "complete"
                            return _finish(
                                text=assistant_text,
                                status="done",
                                checker_action_value="FINAL_ANSWER",
                                checker_reason_value=checker_reason,
                                planner_kind_value=planner_kind_value,
                                planned_tool_override=failed_retry_tool,
                                validation_status_override=retry_validation,
                                attempted_tool_override=str(retry_eval.get("attempted_tool") or ""),
                            )
                        validation_failures_count += 1
                        checker_reason = f"validation_failed:{reason}"
                        return _finish(
                            text=str(retry_eval.get("recovery_text_if_blocked") or DEFAULT_CLARIFICATION).strip(),
                            status="blocked",
                            checker_action_value="NEED_USER_INFO",
                            checker_reason_value=checker_reason,
                            planned_tool_override=failed_retry_tool,
                            validation_status_override=retry_validation,
                            attempted_tool_override=str(retry_eval.get("attempted_tool") or ""),
                        )
                    queued = retry_eval.get("tool_call")
                    if not isinstance(queued, dict):
                        validation_failures_count += 1
                        checker_reason = "validation_failed:invalid_tool_call"
                        return _finish(
                            text=DEFAULT_CLARIFICATION,
                            status="blocked",
                            checker_action_value="NEED_USER_INFO",
                            checker_reason_value=checker_reason,
                            planned_tool_override={"function": "invalid_tool_call", "arguments": {}},
                            validation_status_override=retry_validation,
                        )
                    queued_tool_call = queued
                    queued_retry_tool_for_ledger = queued
                    validation_status = retry_validation
                    critic_continue_count += 1
                    checker_reason = "continue"
                    planner_text_is_tool_candidate = True
                    continue

                final_text_candidate = str(checker_decision.get("text") or draft_response or DEFAULT_CLARIFICATION).strip()
                if _should_continue_after_incomplete_final_answer(
                    user_text=resolved_user_text or user_text,
                    final_text=final_text_candidate,
                    agent_state=agent_state,
                    retry_allowed=_retry_allowed_within_limits() and not suppress_tools_for_turn,
                ):
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
                llm_client=llm_client,
                text=planner_text,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
                tool_index=tool_index,
                user_text=user_text,
                origin=origin_payload,
                scope=scope,
                history_messages=history,
                context=context if isinstance(context, dict) else {},
                platform_preamble=platform_preamble,
                repair_max_tokens=tool_repair_max_tokens,
                recovery_max_tokens=recovery_max_tokens,
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
                round_planner_kind = "repaired_tool"
            else:
                round_planner_kind = "tool"

            if not bool(tool_eval.get("ok")):
                planner_text_is_tool_candidate = True
                reason = str(tool_eval.get("reason") or "invalid_tool_call")
                assistant_text = str(tool_eval.get("assistant_text") or "").strip()
                recovery_text = str(tool_eval.get("recovery_text_if_blocked") or DEFAULT_CLARIFICATION).strip()
                failed_planned_tool = tool_eval.get("tool_call")
                if not isinstance(failed_planned_tool, dict):
                    failed_planned_tool = {"function": "invalid_tool_call", "arguments": {}}
                if reason == "repair_returned_answer" and assistant_text:
                    planner_kind = (
                        "send_message_fix"
                        if _canonical_tool_name((failed_planned_tool or {}).get("function")) == "send_message"
                        else "repaired_answer"
                    )
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

                validation_failures_count += 1
                planner_kind = round_planner_kind
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
            planner_kind = round_planner_kind

        planner_kind = round_planner_kind
        if not isinstance(planned_tool, dict):
            planner_kind = round_planner_kind
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

        tool_used = True
        tool_user_text = _tool_call_effective_user_text(planned_tool, resolved_user_text)
        tool_started = time.perf_counter()
        doer_exec = await _execute_tool_call(
            llm_client=llm_client,
            tool_call=planned_tool,
            platform=platform,
            registry=registry,
            enabled_predicate=enabled_predicate,
            context=context,
            user_text=tool_user_text,
            origin=origin_payload,
            scope=scope,
            wait_callback=wait_callback,
            admin_guard=admin_guard,
        )
        tool_ms_total += (time.perf_counter() - tool_started) * 1000.0
        raw_payload = doer_exec.get("payload")
        raw_tool_payload_out = raw_payload if isinstance(raw_payload, dict) else None
        tool_result_for_checker = doer_exec.get("checker_result")
        normalized_checker_result_out = (
            tool_result_for_checker if isinstance(tool_result_for_checker, dict) else None
        )
        if isinstance(tool_result_for_checker, dict) and not bool(tool_result_for_checker.get("ok")):
            tool_failures_count += 1
        draft_response = str((tool_result_for_checker or {}).get("summary_for_user") or "").strip()
        artifacts = ((tool_result_for_checker or {}).get("artifacts") or [])
        if isinstance(artifacts, list):
            for item in artifacts:
                if not isinstance(item, dict):
                    continue
                artifacts_out.append(item)
                if len(artifacts_out) >= 12:
                    break
        tool_calls_used += 1

        tool_func = _canonical_tool_name((planned_tool or {}).get("function"))
        if tool_func == "get_plugin_help":
            payload_obj = raw_tool_payload_out if isinstance(raw_tool_payload_out, dict) else {}
            args_obj = (planned_tool or {}).get("arguments") if isinstance((planned_tool or {}).get("arguments"), dict) else {}
            helped_plugin_id = _canonical_tool_name(payload_obj.get("plugin_id") or args_obj.get("plugin_id"))
            if helped_plugin_id:
                plugin_help_attempted.add(helped_plugin_id)
            if (
                isinstance(bad_args_help_pending, dict)
                and helped_plugin_id
                and helped_plugin_id == _canonical_tool_name(bad_args_help_pending.get("plugin_id"))
                and not bool(payload_obj.get("ok"))
            ):
                bad_args_help_pending = None
            if (
                bool(payload_obj.get("ok"))
                and isinstance(bad_args_help_pending, dict)
                and helped_plugin_id
                and helped_plugin_id == _canonical_tool_name(bad_args_help_pending.get("plugin_id"))
            ):
                if helped_plugin_id == plugin_help_prefetch_target:
                    plugin_help_prefetch_target = ""
                retry_call = _build_help_constrained_retry_tool_call(
                    failed_tool_call=bad_args_help_pending.get("failed_tool_call"),
                    help_payload=payload_obj,
                    registry=registry,
                )
                bad_args_help_pending = None
                if isinstance(retry_call, dict):
                    if _retry_allowed_within_limits():
                        queued_tool_call = retry_call
                        queued_retry_tool_for_ledger = retry_call
                        attempted_tool_for_ledger = str(retry_call.get("function") or attempted_tool_for_ledger or "")
                        validation_status = {
                            "status": "ok",
                            "repair_used": True,
                            "reason": "bad_args_help_retry",
                            "attempts": 2,
                            "ok": True,
                            "tool_call": retry_call,
                        }
                        retries_signature = _tool_call_signature(retry_call)
                        if retries_signature:
                            bad_args_help_retry_signatures.add(retries_signature)
                        repairs_used_count += 1
                        checker_reason = "continue_after_bad_args_help_retry"
                        planner_kind = "repaired_tool"
                        planner_text_is_tool_candidate = True
                        continue
                    checker_reason = "bad_args_help_retry_budget_exhausted"
                    return _finish(
                        text="I need one more tool call to retry with the plugin's required argument schema.",
                        status="blocked",
                        checker_action_value="NEED_USER_INFO",
                        checker_reason_value=checker_reason,
                        retry_tool=retry_call,
                    )
            if (
                bool(payload_obj.get("ok"))
                and plugin_help_prefetch_target
                and helped_plugin_id
                and helped_plugin_id == plugin_help_prefetch_target
            ):
                plugin_help_prefetch_target = ""
                checker_reason = "continue_after_plugin_help_read"
                continue
            if plugin_help_prefetch_target and helped_plugin_id and helped_plugin_id == plugin_help_prefetch_target:
                plugin_help_prefetch_target = ""

        if tool_func == "search_files":
            payload_obj = raw_tool_payload_out if isinstance(raw_tool_payload_out, dict) else {}
            search_ok = bool(payload_obj.get("ok"))
            if (
                search_ok
                and str(validation_status.get("reason") or "").strip().lower() == "workspace_discovery_redirect"
                and _retry_allowed_within_limits()
            ):
                queued_workspace_read = False
                results = payload_obj.get("results")
                if isinstance(results, list):
                    for item in results:
                        if not isinstance(item, dict):
                            continue
                        candidate_path = _normalize_abs_path(item.get("path"))
                        if not candidate_path:
                            continue
                        if candidate_path in workspace_discovery_read_attempted_paths:
                            continue
                        workspace_discovery_read_attempted_paths.add(candidate_path)
                        queued_tool_call = {"function": "read_file", "arguments": {"path": candidate_path}}
                        queued_retry_tool_for_ledger = queued_tool_call
                        attempted_tool_for_ledger = "read_file"
                        validation_status = {
                            "status": "ok",
                            "repair_used": True,
                            "reason": "workspace_discovery_read",
                            "attempts": 2,
                            "ok": True,
                            "tool_call": queued_tool_call,
                        }
                        repairs_used_count += 1
                        checker_reason = "continue_after_workspace_discovery_read"
                        planner_kind = "repaired_tool"
                        planner_text_is_tool_candidate = True
                        queued_workspace_read = True
                        break
                if queued_workspace_read:
                    continue

        if tool_func == "search_web":
            payload_obj = raw_tool_payload_out if isinstance(raw_tool_payload_out, dict) else {}
            web_research_candidates = _extract_web_search_candidates(
                payload_obj,
                max_candidates=_WEB_RESEARCH_MAX_CANDIDATES,
            )
            web_research_seen_urls = set()
            web_research_attempts = 0
            web_research_active = bool(payload_obj.get("ok")) and bool(web_research_candidates) and not web_research_skip_deepening
            if web_research_active and _retry_allowed_within_limits() and web_research_attempts < _WEB_RESEARCH_MAX_LINK_TRIES:
                followup_call = _next_web_research_tool_call(
                    candidates=web_research_candidates,
                    seen_urls=web_research_seen_urls,
                )
                if isinstance(followup_call, dict):
                    web_research_attempts += 1
                    queued_tool_call = followup_call
                    queued_retry_tool_for_ledger = followup_call
                    attempted_tool_for_ledger = "inspect_webpage"
                    validation_status = {
                        "status": "ok",
                        "repair_used": True,
                        "reason": "web_research_followup",
                        "attempts": 2,
                        "ok": True,
                        "tool_call": followup_call,
                    }
                    repairs_used_count += 1
                    checker_reason = "continue_after_search_web_followup"
                    planner_kind = "repaired_tool"
                    planner_text_is_tool_candidate = True
                    continue
                web_research_active = False

        if tool_func in {"inspect_webpage", "read_url"} and web_research_active:
            payload_obj = raw_tool_payload_out if isinstance(raw_tool_payload_out, dict) else {}
            if _web_inspection_is_sufficient(payload_obj):
                web_research_active = False
                web_research_candidates = []
                web_research_seen_urls = set()
                web_research_attempts = 0
            else:
                followup_call = None
                if _retry_allowed_within_limits() and web_research_attempts < _WEB_RESEARCH_MAX_LINK_TRIES:
                    followup_call = _next_web_research_tool_call(
                        candidates=web_research_candidates,
                        seen_urls=web_research_seen_urls,
                    )
                if isinstance(followup_call, dict):
                    web_research_attempts += 1
                    queued_tool_call = followup_call
                    queued_retry_tool_for_ledger = followup_call
                    attempted_tool_for_ledger = "inspect_webpage"
                    validation_status = {
                        "status": "ok",
                        "repair_used": True,
                        "reason": "web_research_next_link",
                        "attempts": 2,
                        "ok": True,
                        "tool_call": followup_call,
                    }
                    repairs_used_count += 1
                    checker_reason = "continue_after_web_research_next_link"
                    planner_kind = "repaired_tool"
                    planner_text_is_tool_candidate = True
                    continue
                web_research_active = False
                web_research_candidates = []
                web_research_seen_urls = set()
                web_research_attempts = 0

        if tool_func not in {"search_web", "inspect_webpage", "read_url"}:
            web_research_active = False
            web_research_candidates = []
            web_research_seen_urls = set()
            web_research_attempts = 0

        if tool_func == "ai_tasks":
            task_status = _ai_tasks_schedule_status(
                payload=raw_tool_payload_out,
                checker_result=normalized_checker_result_out,
            )
            if bool(task_status.get("created")):
                if task_status.get("success_text"):
                    draft_response = str(task_status.get("success_text") or "").strip()
                else:
                    draft_response = str(draft_response or "Scheduled task created.").strip()

        bad_args_failure, bad_args_reason = _looks_like_bad_args_plugin_failure(
            tool_call=planned_tool,
            tool_result=tool_result_for_checker,
            payload=raw_tool_payload_out,
            registry=registry,
        )
        if bad_args_failure:
            failed_signature = _tool_call_signature(planned_tool)
            already_retried = bool(failed_signature and failed_signature in bad_args_help_retry_signatures)
            if not already_retried:
                retry_plugin_id = _plugin_tool_id_for_call(planned_tool, registry)
                help_retry_call = {"function": "get_plugin_help", "arguments": {"plugin_id": retry_plugin_id}}
                if _retry_allowed_within_limits() and retry_plugin_id:
                    if failed_signature:
                        bad_args_help_retry_signatures.add(failed_signature)
                    bad_args_help_pending = {
                        "plugin_id": retry_plugin_id,
                        "failed_tool_call": planned_tool,
                        "reason": bad_args_reason or "bad_args",
                    }
                    queued_tool_call = help_retry_call
                    queued_retry_tool_for_ledger = help_retry_call
                    attempted_tool_for_ledger = "get_plugin_help"
                    validation_status = {
                        "status": "ok",
                        "repair_used": True,
                        "reason": "bad_args_help_lookup",
                        "attempts": 2,
                        "ok": True,
                        "tool_call": help_retry_call,
                    }
                    repairs_used_count += 1
                    checker_reason = f"continue_after_bad_args_help:{bad_args_reason or 'bad_args'}"
                    planner_kind = "repaired_tool"
                    planner_text_is_tool_candidate = True
                    continue
                checker_reason = "bad_args_help_budget_exhausted"
                return _finish(
                    text="I need one more tool call to fetch that plugin's argument schema before retrying.",
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    retry_tool=help_retry_call,
                    attempted_tool_override=retry_plugin_id or tool_func,
                )

        overwrite_retry_call = _build_overwrite_retry_tool_call(
            tool_call=planned_tool,
            payload=raw_tool_payload_out,
            user_text=tool_user_text or user_text,
        )
        if isinstance(overwrite_retry_call, dict):
            if _retry_allowed_within_limits():
                queued_tool_call = overwrite_retry_call
                queued_retry_tool_for_ledger = overwrite_retry_call
                repairs_used_count += 1
                checker_reason = "continue_after_overwrite_retry"
                continue
            checker_reason = "overwrite_retry_budget_exhausted"
            return _finish(
                text="That plugin already exists and needs overwrite confirmation, but I hit this turn's tool-call limit.",
                status="blocked",
                checker_action_value="NEED_USER_INFO",
                checker_reason_value=checker_reason,
                retry_tool=overwrite_retry_call,
            )

        agent_state = await _run_doer_state_update(
            llm_client=llm_client,
            platform=platform,
            user_request=tool_user_text or resolved_user_text,
            prior_state=agent_state,
            tool_call=planned_tool,
            tool_result=tool_result_for_checker,
            max_tokens=doer_max_tokens,
        )
        _save_persistent_agent_state(
            redis_client=r,
            platform=platform,
            scope=scope,
            state=agent_state,
        )

        routed_call = _is_routed_multi_intent_tool_call(planned_tool)
        routed_ok = isinstance(tool_result_for_checker, dict) and bool(tool_result_for_checker.get("ok"))
        if routed_call and routed_ok:
            routed_summary = str((tool_result_for_checker or {}).get("summary_for_user") or "").strip()
            if routed_summary:
                multi_intent_routed_summaries.append(routed_summary)

        if multi_intent_routed_queue and routed_call:
            if routed_ok:
                if _retry_allowed_within_limits():
                    queued_tool_call = dict(multi_intent_routed_queue.pop(0))
                    queued_retry_tool_for_ledger = queued_tool_call
                    attempted_tool_for_ledger = str(queued_tool_call.get("function") or attempted_tool_for_ledger or "")
                    validation_status = {
                        "status": "ok",
                        "repair_used": False,
                        "reason": "multi_intent_route",
                        "attempts": 1,
                        "ok": True,
                        "tool_call": queued_tool_call,
                    }
                    checker_reason = "continue_multi_intent_route"
                    planner_kind = "tool"
                    planner_text_is_tool_candidate = True
                    continue
                checker_reason = "multi_intent_route_budget_exhausted"
                return _finish(
                    text="I completed part of that multi-action request but hit this turn's tool-call limit before finishing the rest.",
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    retry_tool=dict(multi_intent_routed_queue[0]) if multi_intent_routed_queue else None,
                )
            multi_intent_routing_active = False
            multi_intent_routed_queue = []

        if routed_call and routed_ok and multi_intent_routing_active and not multi_intent_routed_queue:
            final_multi_intent_text = _compose_multi_intent_route_answer(
                multi_intent_routed_summaries,
                fallback=draft_response,
            )
            checker_reason = "complete_multi_intent_route"
            return _finish(
                text=final_multi_intent_text or draft_response or DEFAULT_CLARIFICATION,
                status="done",
                checker_action_value="FINAL_ANSWER",
                checker_reason_value=checker_reason,
                retry_tool=queued_retry_tool_for_ledger,
            )

        checker_started = time.perf_counter()
        checker_decision = await _run_checker(
            llm_client=llm_client,
            platform=platform,
            current_user_text=user_text,
            resolved_user_text=resolved_user_text,
            agent_state=agent_state,
            memory_context=memory_context_payload,
            planned_tool=planned_tool,
            tool_result=tool_result_for_checker,
            draft_response=draft_response,
            retry_allowed=_retry_allowed_within_limits() and not suppress_tools_for_turn,
            platform_preamble=platform_preamble,
            max_tokens=checker_max_tokens,
        )
        checker_ms_total += (time.perf_counter() - checker_started) * 1000.0
        checker_action = str(checker_decision.get("kind") or "FINAL_ANSWER")

        if checker_action == "FINAL_ANSWER":
            final_text_candidate = str(checker_decision.get("text") or draft_response or DEFAULT_CLARIFICATION).strip()
            if _should_continue_after_incomplete_final_answer(
                user_text=resolved_user_text or user_text,
                final_text=final_text_candidate,
                agent_state=agent_state,
                retry_allowed=_retry_allowed_within_limits() and not suppress_tools_for_turn,
            ):
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

        if checker_action == "NEED_USER_INFO":
            need_text = str(checker_decision.get("text") or DEFAULT_CLARIFICATION).strip()
            repaired_need = await _repair_need_user_info_if_overclar(
                llm_client=llm_client,
                platform=platform,
                user_text=resolved_user_text or user_text,
                question_text=need_text,
                tool_index=tool_index,
                platform_preamble=platform_preamble,
                max_tokens=overclar_repair_max_tokens,
            )
            if bool(repaired_need.get("repaired")):
                repairs_used_count += 1
            repaired_kind = str(repaired_need.get("kind") or "NEED_USER_INFO").strip().upper()
            if repaired_kind == "RETRY_TOOL":
                checker_action = "RETRY_TOOL"
                checker_decision = {"kind": "RETRY_TOOL", "text": str(repaired_need.get("text") or "").strip()}
            elif repaired_kind == "FINAL_ANSWER":
                checker_reason = "overclar_repaired"
                return _finish(
                    text=str(repaired_need.get("text") or need_text or DEFAULT_CLARIFICATION).strip(),
                    status="done",
                    checker_action_value="FINAL_ANSWER",
                    checker_reason_value=checker_reason,
                    retry_tool=queued_retry_tool_for_ledger,
                )
            else:
                checker_reason = "needs_user_input"
                return _finish(
                    text=need_text,
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    retry_tool=queued_retry_tool_for_ledger,
                )

        if checker_action == "RETRY_TOOL":
            retry_text = str(checker_decision.get("text") or "").strip()
            if not _retry_allowed_within_limits():
                queued_retry_tool_for_ledger = parse_function_json(retry_text)
                planner_text_is_tool_candidate = True
                checker_reason = "budget_exhausted"
                break
            retry_eval = await _validate_or_recover_tool_call(
                llm_client=llm_client,
                text=retry_text,
                platform=platform,
                registry=registry,
                enabled_predicate=enabled_predicate,
                tool_index=tool_index,
                user_text=user_text,
                origin=origin_payload,
                scope=scope,
                history_messages=history,
                context=context if isinstance(context, dict) else {},
                platform_preamble=platform_preamble,
                repair_max_tokens=tool_repair_max_tokens,
                recovery_max_tokens=recovery_max_tokens,
            )
            retry_validation = (
                retry_eval.get("validation_status")
                if isinstance(retry_eval.get("validation_status"), dict)
                else {"status": "failed", "reason": str(retry_eval.get("reason") or "invalid_tool_call")}
            )
            attempted_tool_for_ledger = str(retry_eval.get("attempted_tool") or attempted_tool_for_ledger or "")
            if bool(retry_eval.get("repair_used")):
                repairs_used_count += 1
            if not bool(retry_eval.get("ok")):
                reason = str(retry_eval.get("reason") or "invalid_tool_call")
                assistant_text = str(retry_eval.get("assistant_text") or "").strip()
                failed_retry_tool = retry_eval.get("tool_call")
                if not isinstance(failed_retry_tool, dict):
                    failed_retry_tool = {"function": "invalid_tool_call", "arguments": {}}
                if reason == "repair_returned_answer" and assistant_text:
                    planner_kind_value = (
                        "send_message_fix"
                        if _canonical_tool_name((failed_retry_tool or {}).get("function")) == "send_message"
                        else "repaired_answer"
                    )
                    checker_reason = "complete"
                    return _finish(
                        text=assistant_text,
                        status="done",
                        checker_action_value="FINAL_ANSWER",
                        checker_reason_value=checker_reason,
                        planner_kind_value=planner_kind_value,
                        planned_tool_override=failed_retry_tool,
                        validation_status_override=retry_validation,
                        attempted_tool_override=str(retry_eval.get("attempted_tool") or ""),
                    )
                validation_failures_count += 1
                checker_reason = f"validation_failed:{reason}"
                return _finish(
                    text=str(retry_eval.get("recovery_text_if_blocked") or DEFAULT_CLARIFICATION).strip(),
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    planned_tool_override=failed_retry_tool,
                    validation_status_override=retry_validation,
                    attempted_tool_override=str(retry_eval.get("attempted_tool") or ""),
                )
            queued = retry_eval.get("tool_call")
            if not isinstance(queued, dict):
                validation_failures_count += 1
                checker_reason = "validation_failed:invalid_tool_call"
                return _finish(
                    text=DEFAULT_CLARIFICATION,
                    status="blocked",
                    checker_action_value="NEED_USER_INFO",
                    checker_reason_value=checker_reason,
                    planned_tool_override={"function": "invalid_tool_call", "arguments": {}},
                    validation_status_override=retry_validation,
                )
            queued_tool_call = queued
            queued_retry_tool_for_ledger = queued
            validation_status = retry_validation
            critic_continue_count += 1
            checker_reason = "continue"
            planner_text_is_tool_candidate = True
            continue

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
        draft_response=draft_response,
        tool_result=tool_result_for_checker,
    )
    checker_started = time.perf_counter()
    checker_decision = await _run_checker(
        llm_client=llm_client,
        platform=platform,
        current_user_text=user_text,
        resolved_user_text=resolved_user_text,
        agent_state=agent_state,
        memory_context=memory_context_payload,
        planned_tool=planned_tool,
        tool_result=tool_result_for_checker,
        draft_response=best_effort,
        retry_allowed=_retry_allowed_within_limits() and not suppress_tools_for_turn,
        platform_preamble=platform_preamble,
        max_tokens=checker_max_tokens,
    )
    checker_ms_total += (time.perf_counter() - checker_started) * 1000.0
    checker_action = str(checker_decision.get("kind") or "FINAL_ANSWER")

    if checker_action == "NEED_USER_INFO":
        need_text = str(checker_decision.get("text") or pending_question or DEFAULT_CLARIFICATION).strip()
        repaired_need = await _repair_need_user_info_if_overclar(
            llm_client=llm_client,
            platform=platform,
            user_text=resolved_user_text or user_text,
            question_text=need_text,
            tool_index=tool_index,
            platform_preamble=platform_preamble,
            max_tokens=overclar_repair_max_tokens,
        )
        if bool(repaired_need.get("repaired")):
            repairs_used_count += 1
        repaired_kind = str(repaired_need.get("kind") or "NEED_USER_INFO").strip().upper()
        if repaired_kind == "RETRY_TOOL":
            checker_action = "RETRY_TOOL"
            checker_decision = {"kind": "RETRY_TOOL", "text": str(repaired_need.get("text") or "").strip()}
        elif repaired_kind == "FINAL_ANSWER":
            checker_reason = "overclar_repaired"
            return _finish(
                text=str(repaired_need.get("text") or need_text or DEFAULT_CLARIFICATION).strip(),
                status="done",
                checker_action_value="FINAL_ANSWER",
                checker_reason_value=checker_reason,
                retry_tool=queued_retry_tool_for_ledger,
            )
        else:
            checker_reason = "needs_user_input"
            return _finish(
                text=need_text,
                status="blocked",
                checker_action_value="NEED_USER_INFO",
                checker_reason_value=checker_reason,
                retry_tool=queued_retry_tool_for_ledger,
            )

    if checker_action == "RETRY_TOOL":
        retry_tool = parse_function_json(str(checker_decision.get("text") or ""))
        if isinstance(retry_tool, dict):
            queued_retry_tool_for_ledger = retry_tool
            planner_text_is_tool_candidate = True
        checker_reason = "budget_exhausted"
        return _finish(
            text=best_effort or "Completed.",
            status="done",
            checker_action_value="FINAL_ANSWER",
            checker_reason_value=checker_reason,
            retry_tool=queued_retry_tool_for_ledger,
        )

    checker_reason = _tool_failure_checker_reason(tool_result_for_checker) or checker_reason or "complete"
    return _finish(
        text=str(checker_decision.get("text") or best_effort or DEFAULT_CLARIFICATION).strip(),
        status="done",
        checker_action_value="FINAL_ANSWER",
        checker_reason_value=checker_reason,
        retry_tool=queued_retry_tool_for_ledger,
    )
