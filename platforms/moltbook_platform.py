# platforms/moltbook_platform.py
"""
Moltbook platform for Tater

What this platform does:
- Auto-registers a Moltbook agent on first run (if no api_key is stored).
  - Uses Tater's first+last name (sanitized) OR an override.
  - If name is taken, retries with suffixes: -1, -2, -3, ...
  - Stores api_key + claim_url + verification_code + profile_url + tweet_template + created_at + status in Redis.
- Polls Moltbook feed + DMs on intervals.
- (Optional) Engage mode to comment/vote, and autopost mode to post from a Redis queue.
- Stores EVERYTHING in Redis:
  - Stats counters
  - Event ledger (posts/comments/votes/dms with URLs)
  - DM conversations + messages (all messages forever by default)

IMPORTANT LLM BEHAVIOR:
- Tater is tool-aware here (knows which tools exist), but MUST NOT call tools on Moltbook.
- We never execute plugins/tools from this platform.
"""

import os
import re
import json
import time
import logging
import threading
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import redis
import requests
from dotenv import load_dotenv

import plugin_registry as pr
from helpers import (
    get_tater_name,
    get_tater_personality,
    get_llm_client_from_env,
    build_llm_host_from_env,
)

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("moltbook")

# -------------------- Moltbook API --------------------
API_BASE = "https://www.moltbook.com/api/v1"  # NOTE: include www

# -------------------- Redis --------------------
redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

# -------------------- Defaults --------------------
DEFAULT_FEED_LIMIT = 25
DEFAULT_CHECK_INTERVAL_SECONDS = 60 * 15
DEFAULT_DM_CHECK_INTERVAL_SECONDS = 60 * 5
DEFAULT_MAX_ACTIONS_PER_CYCLE = 2
DEFAULT_POST_COOLDOWN_SECONDS = 60 * 30  # Moltbook guide: 1 post / 30 min
DEFAULT_REPLY_MAX_CHARS = 600

DEFAULT_EVENTS_MAX = 2000  # Set 0 to keep everything forever (unbounded list)

# -------------------- Redis keys --------------------
MOLT_SETTINGS_KEY = "moltbook_platform_settings"

MOLT_STATS_KEY = "tater:moltbook:stats"
MOLT_EVENTS_KEY = "tater:moltbook:events"
MOLT_SEEN_POSTS_KEY = "tater:moltbook:seen_posts"
MOLT_STATE_KEY = "tater:moltbook:state"

# DM storage:
DM_CONV_INDEX_KEY = "tater:moltbook:dm:conversations"
DM_META_KEY_FMT = "tater:moltbook:dm:{cid}:meta"
DM_MSGS_KEY_FMT = "tater:moltbook:dm:{cid}:messages"

# Optional outbound post queue:
# RPUSH with json: {"title": "...", "text": "...", "submolt": "optional"}
MOLT_POST_QUEUE_KEY = "tater:moltbook:post_queue"

# -------------------- Platform settings --------------------
PLATFORM_SETTINGS = {
    "category": "Moltbook",
    "required": {
        "api_key": {
            "label": "Moltbook API Key",
            "type": "password",
            "default": "",
            "description": "Your Moltbook agent API key (auto-filled on first run if empty).",
        },

        "agent_name_override": {
            "label": "Agent Name Override",
            "type": "text",
            "default": "",
            "description": "Optional. If set, registration uses this name instead of your default name.",
        },

        "description": {
            "label": "Agent Description",
            "type": "text",
            "default": "AI assistant (tool-aware; posts and replies on Moltbook).",
            "description": "Shown on your Moltbook profile.",
        },

        "mode": {
            "label": "Mode",
            "type": "select",
            "default": "read_only",
            "options": ["read_only", "engage", "autopost"],
            "description": "read_only = observe only • engage = reply/comment/vote • autopost = can post from queue",
        },

        "feed_source": {
            "label": "Feed Source",
            "type": "select",
            "default": "personal",
            "options": ["personal", "global"],
            "description": "Which feed to read.",
        },

        "feed_sort": {
            "label": "Feed Sort",
            "type": "select",
            "default": "new",
            "options": ["new", "hot", "top"],
            "description": "Sort order for the feed.",
        },

        "feed_limit": {
            "label": "Feed Limit",
            "type": "number",
            "default": DEFAULT_FEED_LIMIT,
            "description": "How many items to fetch per cycle (max 50).",
        },

        "check_interval_seconds": {
            "label": "Feed Check Interval (sec)",
            "type": "number",
            "default": DEFAULT_CHECK_INTERVAL_SECONDS,
            "description": "How often to poll the feed.",
        },

        "dm_check_interval_seconds": {
            "label": "DM Check Interval (sec)",
            "type": "number",
            "default": DEFAULT_DM_CHECK_INTERVAL_SECONDS,
            "description": "How often to poll DMs.",
        },

        "max_actions_per_cycle": {
            "label": "Max Actions Per Cycle",
            "type": "number",
            "default": DEFAULT_MAX_ACTIONS_PER_CYCLE,
            "description": "Hard cap for comments/votes per cycle.",
        },

        "allow_comments": {
            "label": "Allow Comments",
            "type": "checkbox",
            "default": True,
            "description": "If off, never comment.",
        },

        "allow_votes": {
            "label": "Allow Votes",
            "type": "checkbox",
            "default": True,
            "description": "If off, never vote.",
        },

        "allow_autopost": {
            "label": "Allow Autopost",
            "type": "checkbox",
            "default": False,
            "description": "If on (and mode=autopost), can create posts from the Redis post queue.",
        },

        "dry_run": {
            "label": "Dry Run",
            "type": "checkbox",
            "default": True,
            "description": "If on, no writes (logs intended actions only).",
        },

        "reply_max_chars": {
            "label": "Reply Max Characters",
            "type": "number",
            "default": DEFAULT_REPLY_MAX_CHARS,
            "description": "Soft cap for replies.",
        },

        "events_max": {
            "label": "Max Events Stored (0 = unlimited)",
            "type": "number",
            "default": DEFAULT_EVENTS_MAX,
            "description": "Cap the event ledger list length.",
        },

        "dm_messages_max_per_conv": {
            "label": "Max DM Messages Stored Per Conversation (0 = unlimited)",
            "type": "number",
            "default": 0,
            "description": "If 0, store all DM messages forever. Otherwise keep only last N.",
        },
    },
}

# -------------------- Settings helpers --------------------
def _platform_settings() -> Dict[str, Any]:
    return redis_client.hgetall(MOLT_SETTINGS_KEY) or {}


def _get_str(name: str, default: str = "") -> str:
    s = _platform_settings().get(name)
    if s is None:
        return (default or "").strip()
    return str(s).strip()


def _get_int(name: str, default: int) -> int:
    s = _platform_settings().get(name)
    if s is None or str(s).strip() == "":
        return default
    try:
        return int(str(s).strip())
    except Exception:
        return default


def _get_bool(name: str, default: bool) -> bool:
    s = _platform_settings().get(name)
    if s is None or str(s).strip() == "":
        return default
    return str(s).strip().lower() in ("1", "true", "yes", "on")


def _get_plugin_enabled(plugin_name: str) -> bool:
    enabled = redis_client.hget("plugin_enabled", plugin_name)
    return bool(enabled and enabled.lower() == "true")


# -------------------- Small utilities --------------------
def _now_ts() -> int:
    return int(time.time())


def _post_url(post_id: str) -> str:
    return f"https://www.moltbook.com/post/{post_id}"


def _looks_like_tool_json(text: str) -> bool:
    t = (text or "").strip()
    if not (t.startswith("{") and t.endswith("}")):
        return False
    # common tool-call fields
    return '"function"' in t or '"arguments"' in t


def _compact(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: max(0, n - 1)].rstrip() + "…"


# -------------------- Redis: stats & event ledger --------------------
def _bump_stat(field: str, inc: int = 1):
    pipe = redis_client.pipeline()
    pipe.hincrby(MOLT_STATS_KEY, field, int(inc))
    pipe.hset(MOLT_STATS_KEY, "last_activity_ts", str(_now_ts()))
    pipe.execute()


def _set_stat(field: str, value: str):
    pipe = redis_client.pipeline()
    pipe.hset(MOLT_STATS_KEY, field, value)
    pipe.hset(MOLT_STATS_KEY, "last_activity_ts", str(_now_ts()))
    pipe.execute()


def _log_event(evt: Dict[str, Any]):
    max_events = _get_int("events_max", DEFAULT_EVENTS_MAX)
    payload = json.dumps(evt, ensure_ascii=False)
    pipe = redis_client.pipeline()
    pipe.rpush(MOLT_EVENTS_KEY, payload)
    if max_events and max_events > 0:
        pipe.ltrim(MOLT_EVENTS_KEY, -max_events, -1)
    pipe.execute()


def _state_get_int(field: str, default: int = 0) -> int:
    v = redis_client.hget(MOLT_STATE_KEY, field)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


def _state_set(field: str, value: Any):
    redis_client.hset(MOLT_STATE_KEY, field, str(value))


def _seen_has(post_id: str) -> bool:
    return bool(redis_client.sismember(MOLT_SEEN_POSTS_KEY, post_id))


def _seen_add(post_id: str):
    redis_client.sadd(MOLT_SEEN_POSTS_KEY, post_id)


# -------------------- Redis: DM storage --------------------
def _dm_meta_key(cid: str) -> str:
    return DM_META_KEY_FMT.format(cid=cid)


def _dm_msgs_key(cid: str) -> str:
    return DM_MSGS_KEY_FMT.format(cid=cid)


def _dm_store_thread(cid: str, thread: Dict[str, Any]):
    """
    Store thread and append any new messages.
    Keeps ALL messages forever by default (dm_messages_max_per_conv=0).
    """
    redis_client.sadd(DM_CONV_INDEX_KEY, cid)

    # meta updates
    meta_updates: Dict[str, str] = {
        "conversation_id": cid,
        "updated_ts": str(_now_ts()),
    }
    # include participants if present (store as json string)
    for k in ("participants", "users", "members"):
        if k in thread:
            try:
                meta_updates[k] = json.dumps(thread.get(k), ensure_ascii=False)
            except Exception:
                meta_updates[k] = str(thread.get(k))

    redis_client.hset(_dm_meta_key(cid), mapping=meta_updates)

    msgs = thread.get("messages") or []
    if not isinstance(msgs, list):
        return

    msgs_key = _dm_msgs_key(cid)

    # Use last_seen_ts to avoid re-appending the same messages every poll
    last_seen_ts = 0
    try:
        last_seen_ts = int(redis_client.hget(_dm_meta_key(cid), "last_seen_ts") or "0")
    except Exception:
        last_seen_ts = 0

    new_count = 0
    newest = last_seen_ts

    for m in msgs:
        if not isinstance(m, dict):
            continue

        ts_val = m.get("ts") or m.get("timestamp") or m.get("created_at") or 0
        m_ts = 0
        try:
            if isinstance(ts_val, (int, float)):
                m_ts = int(ts_val)
            elif isinstance(ts_val, str) and ts_val.isdigit():
                m_ts = int(ts_val)
        except Exception:
            m_ts = 0

        if last_seen_ts and m_ts and m_ts <= last_seen_ts:
            continue

        newest = max(newest, m_ts or newest)

        payload = {
            "ts": m_ts or _now_ts(),
            "from": (m.get("from") or m.get("sender") or m.get("author") or "unknown"),
            "text": (m.get("text") or m.get("content") or ""),
            "raw": m,  # store full dict
        }
        redis_client.rpush(msgs_key, json.dumps(payload, ensure_ascii=False))
        new_count += 1

    # Cap per-conversation if configured
    max_per = _get_int("dm_messages_max_per_conv", 0)
    if max_per and max_per > 0:
        redis_client.ltrim(msgs_key, -max_per, -1)

    if newest <= 0:
        newest = _now_ts()

    redis_client.hset(_dm_meta_key(cid), mapping={
        "last_seen_ts": str(newest),
        "last_appended": str(_now_ts()),
        "new_messages_last_poll": str(new_count),
    })

    if new_count:
        _bump_stat("dms_received", new_count)
        _log_event({
            "ts": _now_ts(),
            "type": "dm_received",
            "conversation_id": cid,
            "new_messages": new_count,
        })


# -------------------- Moltbook client --------------------
class MoltbookClient:
    def __init__(self, api_key: str):
        self.api_key = api_key.strip()
        if not self.api_key:
            raise ValueError("Missing Moltbook api_key")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return API_BASE + path

    def _req(self, method: str, path: str, *, params=None, json_body=None, timeout=25) -> Tuple[int, Any, Dict[str, Any]]:
        url = self._url(path)
        try:
            resp = self.session.request(method, url, params=params, json=json_body, timeout=timeout)
        except Exception as e:
            return 0, {"error": str(e)}, {}

        try:
            data: Any = resp.json()
        except Exception:
            data = {"raw": resp.text}

        headers = dict(resp.headers or {})
        return resp.status_code, data, headers

    # --- Agent lifecycle ---
    def status(self) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", "/agents/status")

    # --- DMs ---
    def dm_check(self) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", "/agents/dm/check")

    def dm_conversations(self) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", "/agents/dm/conversations")

    def dm_conversation(self, conv_id: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", f"/agents/dm/conversations/{conv_id}")

    def dm_send(self, conv_id: str, text: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("POST", f"/agents/dm/conversations/{conv_id}/send", json_body={"text": text})

    # --- Feed ---
    def feed(self, source: str, sort: str, limit: int) -> Tuple[int, Any, Dict[str, Any]]:
        limit = max(1, min(int(limit or 25), 50))
        sort = (sort or "new").strip().lower()
        if sort not in ("new", "hot", "top"):
            sort = "new"

        if (source or "personal").strip().lower() == "global":
            return self._req("GET", "/posts", params={"sort": sort, "limit": limit})
        return self._req("GET", "/feed", params={"sort": sort, "limit": limit})

    # --- Writes ---
    def comment(self, post_id: str, text: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("POST", f"/posts/{post_id}/comments", json_body={"text": text})

    def upvote(self, post_id: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("POST", f"/posts/{post_id}/upvote")

    def downvote(self, post_id: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("POST", f"/posts/{post_id}/downvote")

    def create_post(self, title: str, text: str, submolt: Optional[str] = None) -> Tuple[int, Any, Dict[str, Any]]:
        body = {"title": title, "text": text}
        if submolt:
            body["submolt"] = submolt
        return self._req("POST", "/posts", json_body=body)


# -------------------- Rate limit / cooldown --------------------
def _handle_rate_limit(status: int, headers: Dict[str, Any], body: Any):
    if status != 429:
        return
    retry_seconds = DEFAULT_POST_COOLDOWN_SECONDS
    try:
        if isinstance(body, dict) and body.get("retry_after_minutes") is not None:
            retry_seconds = int(float(body["retry_after_minutes"]) * 60)
        h = headers.get("Retry-After") or headers.get("retry-after")
        if h:
            retry_seconds = max(retry_seconds, int(float(h)))
    except Exception:
        pass
    cooldown_until = _now_ts() + max(30, retry_seconds)
    _state_set("cooldown_until_ts", cooldown_until)
    logger.warning(f"[Moltbook] Rate limited. Cooling down until {cooldown_until} (epoch).")
    _log_event({"ts": _now_ts(), "type": "rate_limited", "cooldown_until_ts": cooldown_until})


def _in_cooldown() -> bool:
    return _state_get_int("cooldown_until_ts", 0) > _now_ts()


def _post_cooldown_ok() -> bool:
    last_post = _state_get_int("last_post_ts", 0)
    return (_now_ts() - last_post) >= DEFAULT_POST_COOLDOWN_SECONDS


# -------------------- Tool awareness (but tool-disabled) --------------------
def _tool_catalog_text() -> str:
    plugins = pr.get_registry_snapshot()
    if not plugins:
        return "No tools are registered."

    lines = []
    for plugin in plugins.values():
        usage = getattr(plugin, "usage", "").strip()
        desc = getattr(plugin, "description", "No description provided.").strip()
        enabled = "enabled" if _get_plugin_enabled(plugin.name) else "disabled"

        lines.append(
            f"Tool: {plugin.name} ({enabled})\n"
            f"Description: {desc}\n"
            f"{usage}".strip()
        )

    return "\n\n".join(lines)


def build_system_prompt() -> str:
    now = datetime.now().strftime("%A, %B %d, %Y at %I:%M %p")
    first, last = get_tater_name()
    personality = get_tater_personality()

    persona_clause = ""
    if personality:
        persona_clause = (
            f"You should speak and behave like {personality} "
            "while still being helpful, concise, and easy to understand. "
            "Keep the style subtle rather than over-the-top.\n\n"
        )

    moltbook_rules = (
        "You are interacting on Moltbook (a public social feed and DM system).\n\n"
        "CRITICAL TOOL RULES:\n"
        "- You MAY talk about tools and reference them conceptually.\n"
        "- You MUST NOT call, invoke, or request tool execution here.\n"
        "- You MUST NOT output JSON tool calls.\n"
        "- If a user asks you to do something that would require a tool, explain you can't run tools on Moltbook "
        "and suggest asking you on Discord/WebUI/HomeKit instead.\n\n"
        "STYLE:\n"
        "- Write like a friendly human.\n"
        "- Keep replies short and specific.\n"
        "- Avoid spam.\n"
    )

    tool_catalog = _tool_catalog_text()

    return (
        f"Current Date and Time is: {now}\n\n"
        f"You are {first} {last}, an AI assistant posting and replying on Moltbook.\n\n"
        f"{persona_clause}"
        f"{moltbook_rules}\n"
        "Enabled tool catalog (AWARENESS ONLY; DO NOT INVOKE):\n\n"
        f"{tool_catalog}\n"
    )


# -------------------- LLM helpers --------------------
async def _llm_chat(llm, messages: List[Dict[str, str]], timeout: int = 60) -> str:
    resp = await llm.chat(messages, timeout=timeout)
    out = (resp.get("message", {}) or {}).get("content", "") or ""
    return out.strip()


async def _draft_reply_for_post(llm, post: Dict[str, Any], reply_max_chars: int) -> str:
    title = (post.get("title") or "").strip()
    text = (post.get("text") or "").strip()
    author = (post.get("author") or post.get("user") or post.get("username") or "").strip()
    pid = str(post.get("id") or post.get("_id") or "").strip()

    prompt = (
        "Write a concise, helpful comment reply for Moltbook.\n"
        "- Do not output JSON.\n"
        f"- Keep under {reply_max_chars} characters.\n"
        "- If asked to run a tool, explain you can't run tools on Moltbook and suggest asking on Discord/WebUI/HomeKit.\n\n"
        f"Post ID: {pid}\n"
        f"Author: {author}\n"
        f"Title: {title}\n"
        f"Post Text:\n{text}\n"
    )

    out = await _llm_chat(llm, [{"role": "system", "content": build_system_prompt()}, {"role": "user", "content": prompt}])

    if _looks_like_tool_json(out):
        out = (
            "I can’t run tools directly from Moltbook, but I can explain what I’d do and help you do it "
            "if you ask me on Discord/WebUI/HomeKit. What outcome are you aiming for?"
        )
    return _compact(out, reply_max_chars)


async def _draft_dm_reply(llm, conv_id: str, thread: Dict[str, Any], reply_max_chars: int) -> str:
    msgs = thread.get("messages") or []
    last_msgs = msgs[-8:] if isinstance(msgs, list) else []

    formatted = []
    for m in last_msgs:
        if not isinstance(m, dict):
            continue
        frm = (m.get("from") or m.get("sender") or m.get("author") or "user").strip()
        txt = (m.get("text") or m.get("content") or "").strip()
        formatted.append(f"{frm}: {txt}")

    prompt = (
        "Write a short, friendly DM reply.\n"
        "- Do not output JSON.\n"
        f"- Keep under {reply_max_chars} characters.\n"
        "- If asked to run a tool, explain you can't run tools on Moltbook and suggest asking on Discord/WebUI/HomeKit.\n\n"
        f"Conversation ID: {conv_id}\n"
        "Recent messages:\n"
        + "\n".join(formatted)
    )

    out = await _llm_chat(llm, [{"role": "system", "content": build_system_prompt()}, {"role": "user", "content": prompt}])

    if _looks_like_tool_json(out):
        out = (
            "I can’t run tools from Moltbook DMs, but I can walk you through it or do it on Discord/WebUI/HomeKit. "
            "What do you want me to accomplish?"
        )
    return _compact(out, reply_max_chars)


# -------------------- Feed normalization + selection --------------------
def _normalize_posts(feed_data: Any) -> List[Dict[str, Any]]:
    if isinstance(feed_data, dict):
        for k in ("items", "posts", "data", "results"):
            v = feed_data.get(k)
            if isinstance(v, list):
                return [p for p in v if isinstance(p, dict)]
        if "id" in feed_data and ("title" in feed_data or "text" in feed_data):
            return [feed_data]
        return []
    if isinstance(feed_data, list):
        return [p for p in feed_data if isinstance(p, dict)]
    return []


def _post_text(p: Dict[str, Any]) -> str:
    return ((p.get("title") or "") + " " + (p.get("text") or "")).strip()


def _pick_posts_to_reply(posts: List[Dict[str, Any]], max_actions: int) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for p in posts:
        pid = str(p.get("id") or p.get("_id") or "").strip()
        if not pid or _seen_has(pid):
            continue
        txt = _post_text(p).lower()
        if "?" in txt or "help" in txt or "how do" in txt or "anyone know" in txt:
            candidates.append(p)

    if not candidates:
        for p in posts:
            pid = str(p.get("id") or p.get("_id") or "").strip()
            if pid and not _seen_has(pid):
                candidates.append(p)

    return candidates[: max(0, int(max_actions))]


# -------------------- Auto-register on first run --------------------
def _sanitize_agent_name(name: str) -> str:
    name = (name or "").strip().replace(" ", "-")
    name = re.sub(r"[^A-Za-z0-9\-_]", "", name)
    name = name.strip("-_")
    return (name[:32] if name else "Tater")


def _save_registration_to_redis(reg: Dict[str, Any]):
    agent = (reg or {}).get("agent") or {}
    setup = (reg or {}).get("setup") or {}

    pipe = redis_client.pipeline()

    # ALWAYS store api_key explicitly (critical; cannot be retrieved later)
    api_key = agent.get("api_key")
    if api_key:
        pipe.hset(MOLT_SETTINGS_KEY, "api_key", str(api_key))

    # Store the rest of the critical agent fields so other platforms/plugins can answer
    for k in ("id", "name", "claim_url", "verification_code", "profile_url", "created_at"):
        v = agent.get(k)
        if v is None:
            continue
        if k == "id":
            pipe.hset(MOLT_SETTINGS_KEY, "agent_id", str(v))
        elif k == "name":
            pipe.hset(MOLT_SETTINGS_KEY, "agent_name", str(v))
        else:
            pipe.hset(MOLT_SETTINGS_KEY, k, str(v))

    # store status + tweet_template + skill files (nice-to-have)
    if reg.get("status") is not None:
        pipe.hset(MOLT_SETTINGS_KEY, "status", str(reg.get("status")))
    if reg.get("tweet_template") is not None:
        pipe.hset(MOLT_SETTINGS_KEY, "tweet_template", str(reg.get("tweet_template")))

    skill_files = reg.get("skill_files")
    if isinstance(skill_files, dict):
        try:
            pipe.hset(MOLT_SETTINGS_KEY, "skill_files_json", json.dumps(skill_files, ensure_ascii=False))
        except Exception:
            pipe.hset(MOLT_SETTINGS_KEY, "skill_files_json", str(skill_files))

    # store claim message template
    msg_tmpl = None
    if isinstance(setup, dict) and isinstance(setup.get("step_3"), dict):
        msg_tmpl = (setup.get("step_3") or {}).get("message_template")
    if msg_tmpl:
        pipe.hset(MOLT_SETTINGS_KEY, "claim_message_template", str(msg_tmpl))

    pipe.execute()

    _log_event({
        "ts": _now_ts(),
        "type": "registered",
        "agent_id": agent.get("id"),
        "agent_name": agent.get("name"),
        "claim_url": agent.get("claim_url"),
        "verification_code": agent.get("verification_code"),
        "profile_url": agent.get("profile_url"),
        "created_at": agent.get("created_at"),
        "status": reg.get("status"),
    })


def _register_agent_if_missing() -> bool:
    """
    If no api_key is stored in Redis, register a new Moltbook agent and persist all important fields.
    If the name is taken, retry with suffixes: base-1, base-2, ...
    Returns True if we registered (and should stop so next run uses saved api_key cleanly).
    """
    api_key = _get_str("api_key", "")
    if api_key:
        return False

    override = _get_str("agent_name_override", "")
    if override:
        base = _sanitize_agent_name(override)
    else:
        first, last = get_tater_name()
        base = _sanitize_agent_name(f"{first}-{last}")

    desc = _get_str("description", "").strip() or "Tater AI assistant (tool-aware; posts and replies on Moltbook)"

    max_tries = 50
    for i in range(0, max_tries):
        agent_name = base if i == 0 else _sanitize_agent_name(f"{base}-{i}")

        logger.info(f"[Moltbook] No API key found. Auto-registering agent name='{agent_name}' (try {i+1}/{max_tries})...")

        try:
            resp = requests.post(
                f"{API_BASE}/agents/register",
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                json={"name": agent_name, "description": desc},
                timeout=25,
            )
            data = resp.json() if resp.content else {}
        except Exception as e:
            logger.error(f"[Moltbook] Registration failed: {e}")
            return False

        # success
        if isinstance(data, dict) and data.get("success") is True:
            _save_registration_to_redis(data)

            agent = data.get("agent") or {}
            claim_url = agent.get("claim_url") or "(missing claim_url)"
            vcode = agent.get("verification_code") or "(missing verification_code)"

            logger.warning("🦞 [Moltbook] Agent registered successfully, but is NOT claimed yet!")
            logger.warning(f"🦞 Claim URL: {claim_url}")
            logger.warning(f"🦞 Verification code: {vcode}")
            logger.warning("🦞 IMPORTANT: API key cannot be retrieved later — it has been saved to Redis.")

            return True

        # name taken? retry
        err = ""
        hint = ""
        if isinstance(data, dict):
            err = str(data.get("error") or "")
            hint = str(data.get("hint") or "")
        combined = (err + " " + hint).lower()

        name_taken = ("already taken" in combined) or ("name" in combined and "taken" in combined)
        if name_taken or resp.status_code in (409, 422):
            # keep trying suffixes
            continue

        logger.error(f"[Moltbook] Registration error {resp.status_code}: {data}")
        return False

    logger.error(f"[Moltbook] Could not find an available agent name after {max_tries} tries (base='{base}').")
    return False


# -------------------- Claim gating --------------------
def _is_claimed_status(payload: Any) -> Tuple[bool, str]:
    """
    Returns (claimed_bool, status_str)
    """
    if isinstance(payload, dict):
        status = str(payload.get("status") or payload.get("agent_status") or "").strip()
        if not status and isinstance(payload.get("agent"), dict):
            status = str(payload["agent"].get("status") or "").strip()
        if not status:
            status = "unknown"
        claimed = status.lower() in ("claimed", "active", "verified")
        return claimed, status
    return False, "unknown"


# -------------------- Main runner --------------------
def run(stop_event: Optional[threading.Event] = None):
    """
    Starts Moltbook polling in a daemon thread.
    """
    # Auto-register if missing key
    if _register_agent_if_missing():
        return

    api_key = _get_str("api_key", "")
    if not api_key:
        logger.warning("⚠️ Missing Moltbook API key in Redis (moltbook_platform_settings.api_key). Not starting.")
        return

    llm = get_llm_client_from_env()
    logger.info(f"[Moltbook] LLM client → {build_llm_host_from_env()}")

    client = MoltbookClient(api_key=api_key)

    def _thread_main():
        logger.info("[Moltbook] Platform started.")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        last_feed_check = 0
        last_dm_check = 0
        last_status_check = 0

        claimed = False
        status_str = "unknown"

        while True:
            if stop_event and stop_event.is_set():
                logger.info("[Moltbook] Stop signal received. Exiting loop.")
                break

            # live settings
            mode = _get_str("mode", "read_only").lower()
            feed_source = _get_str("feed_source", "personal").lower()
            feed_sort = _get_str("feed_sort", "new").lower()
            feed_limit = _get_int("feed_limit", DEFAULT_FEED_LIMIT)
            check_interval = _get_int("check_interval_seconds", DEFAULT_CHECK_INTERVAL_SECONDS)
            dm_interval = _get_int("dm_check_interval_seconds", DEFAULT_DM_CHECK_INTERVAL_SECONDS)
            max_actions = _get_int("max_actions_per_cycle", DEFAULT_MAX_ACTIONS_PER_CYCLE)
            allow_comments = _get_bool("allow_comments", True)
            allow_votes = _get_bool("allow_votes", True)
            allow_autopost = _get_bool("allow_autopost", False)
            dry_run = _get_bool("dry_run", True)
            reply_max_chars = _get_int("reply_max_chars", DEFAULT_REPLY_MAX_CHARS)

            if _in_cooldown():
                time.sleep(5)
                continue

            now = _now_ts()

            # -------------------
            # Agent status check (gate writes until claimed)
            # -------------------
            if now - last_status_check >= 60:
                last_status_check = now
                s, data, headers = client.status()
                if s == 429:
                    _handle_rate_limit(s, headers, data)
                elif s and s >= 400:
                    logger.warning(f"[Moltbook] status error {s}: {data}")
                else:
                    claimed, status_str = _is_claimed_status(data)
                    redis_client.hset(MOLT_SETTINGS_KEY, "status", status_str)
                    redis_client.hset(MOLT_STATS_KEY, "agent_status", status_str)
                    redis_client.hset(MOLT_STATS_KEY, "claimed", "true" if claimed else "false")

            # -------------------
            # DM heartbeat / handling
            # -------------------
            if now - last_dm_check >= max(30, dm_interval):
                last_dm_check = now
                s, data, headers = client.dm_check()
                if s == 429:
                    _handle_rate_limit(s, headers, data)
                elif s and s >= 400:
                    logger.warning(f"[Moltbook] DM check error {s}: {data}")
                else:
                    s2, convs, h2 = client.dm_conversations()
                    if s2 == 429:
                        _handle_rate_limit(s2, h2, convs)
                    elif s2 and s2 >= 400:
                        logger.warning(f"[Moltbook] DM conversations error {s2}: {convs}")
                    else:
                        if isinstance(convs, list):
                            conv_list = convs
                        elif isinstance(convs, dict) and isinstance(convs.get("conversations"), list):
                            conv_list = convs["conversations"]
                        else:
                            conv_list = []

                        for conv in conv_list:
                            if not isinstance(conv, dict):
                                continue
                            cid = str(conv.get("id") or "").strip()
                            if not cid:
                                continue

                            s3, thread, h3 = client.dm_conversation(cid)
                            if s3 == 429:
                                _handle_rate_limit(s3, h3, thread)
                                break
                            if s3 and s3 >= 400:
                                continue

                            if isinstance(thread, dict):
                                _dm_store_thread(cid, thread)

                            # Reply rules: only if claimed + mode allows
                            if claimed and mode in ("engage", "autopost"):
                                try:
                                    new_messages = int(redis_client.hget(_dm_meta_key(cid), "new_messages_last_poll") or "0")
                                except Exception:
                                    new_messages = 0

                                if new_messages > 0:
                                    reply = loop.run_until_complete(_draft_dm_reply(llm, cid, thread, reply_max_chars))
                                    if reply:
                                        if dry_run:
                                            logger.info(f"[Moltbook][DryRun] Would DM reply to {cid}: {reply}")
                                        else:
                                            s4, out, h4 = client.dm_send(cid, reply)
                                            if s4 == 429:
                                                _handle_rate_limit(s4, h4, out)
                                                break
                                            if s4 and s4 >= 400:
                                                logger.warning(f"[Moltbook] DM send error {s4}: {out}")
                                            else:
                                                _bump_stat("dms_sent", 1)
                                                _log_event({"ts": _now_ts(), "type": "dm_send", "conversation_id": cid, "text": reply})

            # -------------------
            # Feed check / engage
            # -------------------
            if now - last_feed_check >= max(60, check_interval):
                last_feed_check = now

                s, feed_data, headers = client.feed(feed_source, feed_sort, feed_limit)
                if s == 429:
                    _handle_rate_limit(s, headers, feed_data)
                    time.sleep(5)
                    continue
                if s and s >= 400:
                    logger.warning(f"[Moltbook] Feed error {s}: {feed_data}")
                    time.sleep(10)
                    continue

                posts = _normalize_posts(feed_data)
                if posts:
                    _set_stat("last_feed_check_ts", str(_now_ts()))
                    _set_stat("last_feed_count", str(len(posts)))

                actions_taken = 0

                # Engage only if claimed + mode allows
                if claimed and mode in ("engage", "autopost"):
                    targets = _pick_posts_to_reply(posts, max_actions=max_actions)

                    for post in targets:
                        if actions_taken >= max_actions:
                            break

                        pid = str(post.get("id") or post.get("_id") or "").strip()
                        if not pid or _seen_has(pid):
                            continue

                        _seen_add(pid)

                        reply = loop.run_until_complete(_draft_reply_for_post(llm, post, reply_max_chars))
                        if not reply:
                            continue

                        if allow_comments:
                            if dry_run:
                                logger.info(f"[Moltbook][DryRun] Would comment on {pid}: {reply}")
                            else:
                                s2, out2, h2 = client.comment(pid, reply)
                                if s2 == 429:
                                    _handle_rate_limit(s2, h2, out2)
                                    break
                                if s2 and s2 >= 400:
                                    logger.warning(f"[Moltbook] Comment error {s2}: {out2}")
                                else:
                                    _bump_stat("comments_created", 1)
                                    _log_event({
                                        "ts": _now_ts(),
                                        "type": "comment",
                                        "post_id": pid,
                                        "post_url": _post_url(pid),
                                        "text": reply,
                                        "post_title": _compact(post.get("title") or "", 200),
                                    })
                            actions_taken += 1

                        if allow_votes and actions_taken < max_actions:
                            if dry_run:
                                logger.info(f"[Moltbook][DryRun] Would upvote {pid}")
                            else:
                                s3, out3, h3 = client.upvote(pid)
                                if s3 == 429:
                                    _handle_rate_limit(s3, h3, out3)
                                    break
                                if s3 and s3 >= 400:
                                    logger.warning(f"[Moltbook] Upvote error {s3}: {out3}")
                                else:
                                    _bump_stat("votes_cast", 1)
                                    _log_event({"ts": _now_ts(), "type": "vote", "vote": "upvote", "post_id": pid, "post_url": _post_url(pid)})
                            actions_taken += 1

                # -------------------
                # Autopost from queue (claimed only)
                # -------------------
                if claimed and mode == "autopost" and allow_autopost and _post_cooldown_ok():
                    raw = redis_client.lindex(MOLT_POST_QUEUE_KEY, 0)
                    if raw:
                        try:
                            job = json.loads(raw)
                        except Exception:
                            job = None

                        if isinstance(job, dict):
                            title = str(job.get("title") or "").strip()
                            text = str(job.get("text") or "").strip()
                            submolt = str(job.get("submolt") or "").strip() or None

                            if title and text:
                                if dry_run:
                                    logger.info(f"[Moltbook][DryRun] Would create post: {title}")
                                else:
                                    s4, out4, h4 = client.create_post(title, text, submolt=submolt)
                                    if s4 == 429:
                                        _handle_rate_limit(s4, h4, out4)
                                    elif s4 and s4 >= 400:
                                        logger.warning(f"[Moltbook] Create post error {s4}: {out4}")
                                    else:
                                        post_id = ""
                                        if isinstance(out4, dict):
                                            maybe = out4.get("post") if isinstance(out4.get("post"), dict) else out4
                                            post_id = str(maybe.get("id") or maybe.get("_id") or "").strip()

                                        url = _post_url(post_id) if post_id else ""
                                        _state_set("last_post_ts", _now_ts())
                                        _bump_stat("posts_created", 1)
                                        if url:
                                            _set_stat("last_post_url", url)

                                        _log_event({
                                            "ts": _now_ts(),
                                            "type": "post",
                                            "id": post_id,
                                            "url": url,
                                            "title": _compact(title, 200),
                                            "summary": _compact(text, 600),
                                            "meta": {"submolt": submolt or ""},
                                        })

                                        redis_client.lpop(MOLT_POST_QUEUE_KEY)

            # stop-aware sleep
            for _ in range(10):
                if stop_event and stop_event.is_set():
                    break
                time.sleep(0.5)

        try:
            loop.stop()
            loop.close()
        except Exception:
            pass

        logger.info("[Moltbook] Platform stopped.")

    threading.Thread(target=_thread_main, daemon=True).start()