# platforms/moltbook_platform.py
"""
Moltbook platform for Tater (NO QUEUE VERSION)

What this platform does:
- Auto-registers a Moltbook agent on first run (if no api_key is stored).
- Polls Moltbook feed + DMs on intervals.
- read_only: observe only
- engage: can comment/vote
- autopost: can create posts (self-generated) + optionally comment/vote
- Stores stats + event ledger + DM history in Redis.

Important API fields (per docs):
- Create post: {"title": "...", "content": "...", "submolt": "optional"}
- Comment: {"content": "..."}
- DM request / send: {"message": "..."}
"""

import os
import re
import json
import time
import logging
import threading
import asyncio
import random
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
)

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("moltbook")

# -------------------- Moltbook API --------------------
API_BASE = "https://www.moltbook.com/api/v1"  # include www

# -------------------- Redis --------------------
redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

# -------------------- Defaults --------------------
DEFAULT_FEED_LIMIT = 25
DEFAULT_CHECK_INTERVAL_SECONDS = 60 * 15
DEFAULT_DM_CHECK_INTERVAL_SECONDS = 60 * 5
DEFAULT_MAX_ACTIONS_PER_CYCLE = 2
DEFAULT_POST_COOLDOWN_SECONDS = 60 * 30  # 1 post / 30 min (docs)
DEFAULT_REPLY_MAX_CHARS = 600
DEFAULT_EVENTS_MAX = 2000  # 0 = unlimited

# Self-post defaults
DEFAULT_ALLOW_SELF_POSTS = True
DEFAULT_SELF_POST_CHANCE_PCT = 25  # per feed cycle
DEFAULT_DEFAULT_SUBMOLT = ""       # optional
DEFAULT_INTRO_POST_ON_CLAIM = True

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
            "default": "AI assistant (posts and replies on Moltbook).",
            "description": "Shown on your Moltbook profile.",
        },

        "mode": {
            "label": "Mode",
            "type": "select",
            "default": "read_only",
            "options": ["read_only", "engage", "autopost"],
            "description": "read_only = observe only • engage = reply/comment/vote • autopost = can create posts too",
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

        # Self-posting (no queue)
        "allow_self_posts": {
            "label": "Allow Self Posts",
            "type": "checkbox",
            "default": DEFAULT_ALLOW_SELF_POSTS,
            "description": "If on (and mode=autopost), can create its own posts (still respects cooldown).",
        },

        "self_post_chance_pct": {
            "label": "Self Post Chance (%)",
            "type": "number",
            "default": DEFAULT_SELF_POST_CHANCE_PCT,
            "description": "Percent chance per feed cycle it will generate + post something.",
        },

        "default_submolt": {
            "label": "Default Submolt (optional)",
            "type": "text",
            "default": DEFAULT_DEFAULT_SUBMOLT,
            "description": "If set, posts default here (ex: 'general'). Leave blank to post without submolt.",
        },

        "intro_post_on_claim": {
            "label": "Post Intro Once After Claim",
            "type": "checkbox",
            "default": DEFAULT_INTRO_POST_ON_CLAIM,
            "description": "If on, one-time intro post after the account becomes claimed (autopost mode only).",
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

DEFAULT_REQUIRED_SUBMOLT = "general"

def _default_submolt_fallback() -> str:
    # user setting
    s = (_get_str("default_submolt", "") or "").strip().lower()
    if s:
        return s

    # tater name
    first, last = get_tater_name()
    name = f"{first}{last}".lower()
    name = re.sub(r"[^a-z0-9_]", "", name)
    if name:
        return name

    # hard fallback
    return DEFAULT_REQUIRED_SUBMOLT

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


# -------------------- Utilities --------------------
def _now_ts() -> int:
    return int(time.time())


def _post_url(post_id: str) -> str:
    return f"https://www.moltbook.com/post/{post_id}"


def _looks_like_tool_json(text: str) -> bool:
    t = (text or "").strip()
    if not (t.startswith("{") and t.endswith("}")):
        return False
    return '"function"' in t or '"arguments"' in t


def _compact(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: max(0, n - 1)].rstrip() + "…"


def _sanitize_agent_name(name: str) -> str:
    # Moltbook names tend to be simple; keep it safe
    name = (name or "").strip()
    name = re.sub(r"\s+", "", name)
    name = re.sub(r"[^a-zA-Z0-9_\-]", "", name)
    return name[:32] if name else "TaterBot"


# -------------------- Redis: stats & events --------------------
def _bump_stat(field: str, inc: int = 1):
    pipe = redis_client.pipeline()
    pipe.hincrby(MOLT_STATS_KEY, field, int(inc))
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


def _state_get_str(field: str, default: str = "") -> str:
    v = redis_client.hget(MOLT_STATE_KEY, field)
    return str(v).strip() if v is not None else default


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
    Store thread meta and append any new messages.
    Keeps ALL messages forever by default (dm_messages_max_per_conv=0).
    """
    redis_client.sadd(DM_CONV_INDEX_KEY, cid)

    meta_updates: Dict[str, str] = {
        "conversation_id": cid,
        "updated_ts": str(_now_ts()),
    }
    for k in ("participants", "users", "members", "with_agent"):
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
            "text": (m.get("text") or m.get("content") or m.get("message") or ""),
            "raw": m,
        }
        redis_client.rpush(msgs_key, json.dumps(payload, ensure_ascii=False))
        new_count += 1

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

    def _req(
        self,
        method: str,
        path: str,
        *,
        params=None,
        json_body=None,
        timeout=(10, 90),  # (connect_timeout, read_timeout)
    ) -> Tuple[int, Any, Dict[str, Any]]:
        url = self._url(path)
        try:
            resp = self.session.request(
                method,
                url,
                params=params,
                json=json_body,
                timeout=timeout,
            )
        except Exception as e:
            return 0, {"success": False, "error": str(e)}, {}

        try:
            data: Any = resp.json()
        except Exception:
            data = {"success": False, "raw": resp.text}

        headers = dict(resp.headers or {})
        return resp.status_code, data, headers

    # --- Agent lifecycle ---
    def status(self) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", "/agents/status")

    def register(self, name: str, description: str) -> Tuple[int, Any, Dict[str, Any]]:
        # Not in your provided docs, but matches earlier version usage
        return self._req("POST", "/agents/register", json_body={"name": name, "description": description})

    # --- DMs ---
    def dm_check(self) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", "/agents/dm/check")

    def dm_conversations(self) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", "/agents/dm/conversations")

    def dm_conversation(self, conv_id: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", f"/agents/dm/conversations/{conv_id}")

    def dm_send(self, conv_id: str, message: str) -> Tuple[int, Any, Dict[str, Any]]:
        # docs: {"message": "..."}
        return self._req("POST", f"/agents/dm/conversations/{conv_id}/send", json_body={"message": message})

    # --- Feed ---
    def feed(self, source: str, sort: str, limit: int) -> Tuple[int, Any, Dict[str, Any]]:
        limit = max(1, min(int(limit or 25), 50))
        sort = (sort or "new").strip().lower()
        if sort not in ("new", "hot", "top", "rising"):
            sort = "new"

        if (source or "personal").strip().lower() == "global":
            return self._req("GET", "/posts", params={"sort": sort, "limit": limit})
        return self._req("GET", "/feed", params={"sort": sort, "limit": limit})

    # --- Writes ---
    def comment(self, post_id: str, content: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("POST", f"/posts/{post_id}/comments", json_body={"content": content})

    def upvote(self, post_id: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("POST", f"/posts/{post_id}/upvote")

    def downvote(self, post_id: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("POST", f"/posts/{post_id}/downvote")

    def create_post(self, title: str, content: str, submolt: Optional[str] = None) -> Tuple[int, Any, Dict[str, Any]]:
        body = {"title": title, "content": content}
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
        persona_clause = f"Vibe: {personality}. Keep it natural.\n\n"

    rules = (
        "You are posting and replying on Moltbook (a public feed and DM system).\n\n"
        "Hard rules:\n"
        "- Do NOT output JSON tool calls.\n"
        "- Do NOT ask to run tools here.\n"
        "- If you accidentally generate tool JSON, rewrite it as normal text.\n\n"
        "Style:\n"
        "- Sound like a friendly human.\n"
        "- You can start conversations and share thoughts.\n"
        "- Don’t be spammy; one good post beats five mediocre ones.\n"
        "- Keep posts readable. Avoid walls of text.\n"
    )

    return (
        f"Current Date and Time is: {now}\n\n"
        f"You are {first} {last}, an AI assistant on Moltbook.\n\n"
        f"{persona_clause}"
        f"{rules}\n"
        "Tool catalog (awareness only; never invoke):\n\n"
        f"{_tool_catalog_text()}\n"
    )


# -------------------- LLM helpers --------------------
async def _llm_chat(llm, messages: List[Dict[str, str]], timeout: int = 60) -> str:
    resp = await llm.chat(messages, timeout=timeout)
    out = (resp.get("message", {}) or {}).get("content", "") or ""
    return out.strip()


async def _draft_comment(llm, post: Dict[str, Any], reply_max_chars: int) -> str:
    title = (post.get("title") or "").strip()
    content = (post.get("content") or "").strip()
    author = (post.get("author") or {}).get("name") if isinstance(post.get("author"), dict) else post.get("author")

    prompt = (
        "Write a friendly Moltbook comment.\n"
        "- Be helpful or curious.\n"
        "- Avoid repeating the post verbatim.\n"
        "- 1-3 short paragraphs.\n"
        "- No tool JSON.\n\n"
        f"Post title: {title}\n"
        f"Author: {author}\n"
        f"Post content:\n{content}\n"
    )

    messages = [
        {"role": "system", "content": build_system_prompt()},
        {"role": "user", "content": prompt},
    ]
    txt = await _llm_chat(llm, messages, timeout=60)
    txt = re.sub(r"\s+$", "", txt)

    if _looks_like_tool_json(txt):
        return ""

    # hard-ish cap
    return _compact(txt, max(50, int(reply_max_chars or DEFAULT_REPLY_MAX_CHARS)))

async def _draft_dm_reply(llm, conv_id: str, thread: Dict[str, Any], reply_max_chars: int) -> str:
    msgs = thread.get("messages") or []
    last_msgs = msgs[-8:] if isinstance(msgs, list) else []

    formatted = []
    for m in last_msgs:
        if not isinstance(m, dict):
            continue
        frm = (m.get("from") or m.get("sender") or m.get("author") or "user").strip()
        txt = (m.get("text") or m.get("content") or m.get("message") or "").strip()
        if txt:
            formatted.append(f"{frm}: {txt}")

    prompt = (
        "Write a short, friendly DM reply.\n"
        "- Be chatty but not long.\n"
        "- Ask 1 follow-up question if it helps.\n"
        "- Do NOT output JSON.\n"
        f"- Keep under {reply_max_chars} characters.\n\n"
        f"Conversation ID: {conv_id}\n"
        "Recent messages:\n"
        + "\n".join(formatted)
    )

    out = await _llm_chat(
        llm,
        [
            {"role": "system", "content": build_system_prompt()},
            {"role": "user", "content": prompt},
        ],
        timeout=60,
    )

    if _looks_like_tool_json(out):
        return ""

    return _compact(out, max(80, int(reply_max_chars or DEFAULT_REPLY_MAX_CHARS)))


async def _maybe_reply_to_dms(llm, client: MoltbookClient, dry_run: bool, claimed: bool):
    # only reply when the account is claimed AND mode allows interaction
    mode = _get_str("mode", "read_only").lower()
    if not claimed:
        return
    if mode not in ("engage", "autopost"):
        return

    reply_max_chars = _get_int("reply_max_chars", DEFAULT_REPLY_MAX_CHARS)

    # Try to learn our agent name/id from settings so we can avoid replying to ourselves
    agent_name = _get_str("agent_name", "").strip().lower()
    agent_id = _get_str("agent_id", "").strip()

    conv_ids = list(redis_client.smembers(DM_CONV_INDEX_KEY) or [])
    for cid in conv_ids:
        try:
            new_count = int(redis_client.hget(_dm_meta_key(cid), "new_messages_last_poll") or "0")
        except Exception:
            new_count = 0

        if new_count <= 0:
            continue

        # Optional: skip if we already replied very recently to this convo
        try:
            last_replied_ts = int(redis_client.hget(_dm_meta_key(cid), "last_replied_ts") or "0")
        except Exception:
            last_replied_ts = 0

        # Pull recent stored messages
        raw_msgs = redis_client.lrange(_dm_msgs_key(cid), -12, -1) or []
        msgs = []
        for r in raw_msgs:
            try:
                msgs.append(json.loads(r))
            except Exception:
                pass

        if not msgs:
            # nothing to respond to
            redis_client.hset(_dm_meta_key(cid), "new_messages_last_poll", "0")
            continue

        # Determine if the last message is from the agent (avoid replying to self)
        last_msg = msgs[-1] if msgs else {}
        last_from = str(last_msg.get("from") or "").strip().lower()
        last_ts = int(last_msg.get("ts") or 0)

        def _is_agent_sender(sender: str) -> bool:
            if not sender:
                return False
            if agent_name and sender == agent_name:
                return True
            if agent_id and sender == agent_id:
                return True
            # fallback heuristics
            if sender in ("agent", "tater", "taterbot", "assistant"):
                return True
            return False

        # If last message is from us, do not respond
        if _is_agent_sender(last_from):
            redis_client.hset(_dm_meta_key(cid), "new_messages_last_poll", "0")
            continue

        # If we already replied after the last incoming msg timestamp, skip
        if last_ts and last_replied_ts and last_replied_ts >= last_ts:
            redis_client.hset(_dm_meta_key(cid), "new_messages_last_poll", "0")
            continue

        # Ensure there is at least one non-agent message in the window
        has_user_msg = any(not _is_agent_sender(str(m.get("from") or "").strip().lower()) for m in msgs)
        if not has_user_msg:
            redis_client.hset(_dm_meta_key(cid), "new_messages_last_poll", "0")
            continue

        thread = {"messages": msgs}

        reply = await _draft_dm_reply(llm, cid, thread, reply_max_chars)
        if not reply:
            # still clear so we don't loop forever
            redis_client.hset(_dm_meta_key(cid), "new_messages_last_poll", "0")
            continue

        now_ts = _now_ts()

        if dry_run:
            logger.info(f"[Moltbook] DRY RUN DM reply to {cid}: {reply}")
            _log_event({"ts": now_ts, "type": "dry_run_dm_send", "conversation_id": cid, "message": reply})
            redis_client.hset(_dm_meta_key(cid), mapping={
                "new_messages_last_poll": "0",
                "last_replied_ts": str(now_ts),
            })
            continue

        st, bd, hd = client.dm_send(str(cid), reply)
        if st == 429:
            _handle_rate_limit(st, hd, bd)
            return

        if st and isinstance(bd, dict) and bd.get("success") is True:
            _bump_stat("dms_sent", 1)
            _log_event({"ts": now_ts, "type": "dm_send", "conversation_id": cid, "message": reply})
            redis_client.hset(_dm_meta_key(cid), mapping={
                "new_messages_last_poll": "0",
                "last_replied_ts": str(now_ts),
            })
        else:
            _log_event({"ts": now_ts, "type": "dm_send_failed", "conversation_id": cid, "status": st, "body": bd})
            # don't clear new_messages_last_poll here if you want retries; if you *do* want retries, keep as-is

async def _draft_self_post(llm) -> Tuple[str, str]:
    prompt = (
        "Write a Moltbook post.\n"
        "Make it something worth reading.\n"
        "- It can be a thought, a question, a discovery, or a mini-story.\n"
        "- Avoid being generic. Be specific.\n"
        "- Do not mention tool calls or output JSON.\n\n"
        "Return as:\n"
        "TITLE: <short title>\n"
        "CONTENT:\n"
        "<post body>\n"
    )

    messages = [
        {"role": "system", "content": build_system_prompt()},
        {"role": "user", "content": prompt},
    ]
    txt = await _llm_chat(llm, messages, timeout=75)
    if _looks_like_tool_json(txt):
        return "", ""

    title = ""
    content = ""
    m = re.search(r"^TITLE:\s*(.+?)\s*$", txt, re.MULTILINE)
    if m:
        title = m.group(1).strip()

    m2 = re.search(r"^CONTENT:\s*(.*)$", txt, re.MULTILINE | re.DOTALL)
    if m2:
        content = m2.group(1).strip()
    else:
        # fallback: try splitting first line as title
        lines = [l.strip() for l in (txt or "").splitlines() if l.strip()]
        if lines:
            title = lines[0][:120]
            content = "\n".join(lines[1:]).strip()

    title = _compact(title, 120).strip()
    content = content.strip()

    # sanity
    if len(title) < 3 or len(content) < 20:
        return "", ""
    return title, content


# -------------------- Main loop logic --------------------
# -------------------- Intro post helpers --------------------
async def _draft_intro_post(llm) -> str:
    prompt = (
        "This is your first post on Moltbook.\n"
        "Introduce yourself in a friendly, natural way.\n"
        "- Say who you are.\n"
        "- Say what kinds of things you like to talk about.\n"
        "- Keep it short and casual.\n"
        "- Do NOT mention tools, plugins, or being a bot framework.\n"
        "- Do NOT use hashtags.\n"
        "- Do NOT output JSON.\n"
    )

    out = await _llm_chat(
        llm,
        [
            {"role": "system", "content": build_system_prompt()},
            {"role": "user", "content": prompt},
        ],
    )

    return _compact(out, 600)


def _should_intro_post(client_status: Dict[str, Any]) -> bool:
    """
    Do intro post once after claimed.
    """
    if not _get_bool("intro_post_on_claim", DEFAULT_INTRO_POST_ON_CLAIM):
        return False
    if _state_get_str("intro_post_done", "false").lower() == "true":
        return False

    is_claimed = False
    try:
        if isinstance(client_status, dict):
            data = client_status.get("data") or client_status
            if isinstance(data, dict):
                agent = data.get("agent") or data
                if isinstance(agent, dict):
                    is_claimed = bool(agent.get("is_claimed"))
    except Exception:
        is_claimed = False

    return bool(is_claimed)


async def _maybe_intro_post(llm, client: MoltbookClient, dry_run: bool):
    if _in_cooldown() or not _post_cooldown_ok():
        return

    title = "Hello Moltbook 👋"

    # AI-generated intro content (chatty, first-post framing)
    content = await _draft_intro_post(llm)
    content = (content or "").strip()
    if not content:
        return

    # Safety: if the model ever outputs tool JSON, bail
    if _looks_like_tool_json(content):
        return

    submolt = _default_submolt_fallback()

    if dry_run:
        logger.info(f"[Moltbook] DRY RUN intro post: {title}")
        _log_event({
            "ts": _now_ts(),
            "type": "dry_run_intro_post",
            "title": title,
            "submolt": submolt,
            "summary": _compact(content, 300),
        })
        _state_set("intro_post_done", "true")
        _state_set("last_post_ts", _now_ts())
        return

    status, body, headers = client.create_post(title=title, content=content, submolt=submolt)
    if status == 429:
        _handle_rate_limit(status, headers, body)
        return

    if status and isinstance(body, dict) and body.get("success") is True:
        post_id = None
        try:
            data = body.get("data") or {}
            if isinstance(data, dict):
                post_id = data.get("id") or data.get("post_id")
        except Exception:
            post_id = None

        _state_set("last_post_ts", _now_ts())
        _state_set("intro_post_done", "true")
        _bump_stat("posts_created", 1)
        _log_event({
            "ts": _now_ts(),
            "type": "post_created",
            "post_id": post_id,
            "url": _post_url(post_id) if post_id else None,
            "title": title,
            "submolt": submolt,
            "summary": _compact(content, 300),
        })
        return

    logger.warning(f"[Moltbook] Intro post failed: status={status} body={body}")
    _log_event({"ts": _now_ts(), "type": "post_failed", "status": status, "body": body})

async def _maybe_self_post(llm, client: MoltbookClient, dry_run: bool):
    mode = _get_str("mode", "read_only").lower()
    if mode != "autopost":
        return
    if not _get_bool("allow_self_posts", DEFAULT_ALLOW_SELF_POSTS):
        return
    if _in_cooldown() or not _post_cooldown_ok():
        return

    chance = max(0, min(100, _get_int("self_post_chance_pct", DEFAULT_SELF_POST_CHANCE_PCT)))
    if random.randint(1, 100) > chance:
        return

    title, content = await _draft_self_post(llm)
    title = (title or "").strip()
    content = (content or "").strip()
    if not title or not content:
        return

    # Moltbook appears to require BOTH title and submolt (API returns 400 otherwise)
    # Always provide a non-empty submolt fallback.
    submolt = _default_submolt_fallback()

    if dry_run:
        logger.info(f"[Moltbook] DRY RUN self-post: {title}")
        _log_event({"ts": _now_ts(), "type": "dry_run_post", "title": title, "submolt": submolt})
        _state_set("last_post_ts", _now_ts())
        return

    status, body, headers = client.create_post(title=title, content=content, submolt=submolt)
    if status == 429:
        _handle_rate_limit(status, headers, body)
        return

    if status and isinstance(body, dict) and body.get("success") is True:
        post_id = None
        try:
            data = body.get("data") or {}
            if isinstance(data, dict):
                post_id = data.get("id") or data.get("post_id")
        except Exception:
            post_id = None

        _state_set("last_post_ts", _now_ts())
        _bump_stat("posts_created", 1)
        _log_event({
            "ts": _now_ts(),
            "type": "post_created",
            "post_id": post_id,
            "url": _post_url(post_id) if post_id else None,
            "title": title,
            "submolt": submolt,
        })
        return

    logger.warning(f"[Moltbook] Self post failed: status={status} body={body}")
    _log_event({"ts": _now_ts(), "type": "post_failed", "status": status, "body": body})


async def _engage_with_feed(llm, client: MoltbookClient):
    mode = _get_str("mode", "read_only").lower()
    if mode not in ("engage", "autopost"):
        return

    allow_comments = _get_bool("allow_comments", True)
    allow_votes = _get_bool("allow_votes", True)
    if not allow_comments and not allow_votes:
        return

    feed_source = _get_str("feed_source", "personal")
    feed_sort = _get_str("feed_sort", "new")
    feed_limit = _get_int("feed_limit", DEFAULT_FEED_LIMIT)
    reply_max_chars = _get_int("reply_max_chars", DEFAULT_REPLY_MAX_CHARS)
    dry_run = _get_bool("dry_run", True)

    status, body, headers = client.feed(feed_source, feed_sort, feed_limit)
    if status == 429:
        _handle_rate_limit(status, headers, body)
        return
    if not status or not isinstance(body, dict) or body.get("success") is not True:
        _log_event({"ts": _now_ts(), "type": "feed_failed", "status": status, "body": body})
        return

    data = body.get("data") or {}
    items = data.get("items") or data.get("posts") or data.get("feed") or []
    if not isinstance(items, list):
        items = []

    actions_left = max(0, _get_int("max_actions_per_cycle", DEFAULT_MAX_ACTIONS_PER_CYCLE))

    for post in items:
        if actions_left <= 0:
            break
        if not isinstance(post, dict):
            continue

        post_id = post.get("id") or post.get("post_id")
        if not post_id:
            continue
        if _seen_has(str(post_id)):
            continue

        _seen_add(str(post_id))

        # vote sometimes
        did_action = False
        if allow_votes and actions_left > 0 and random.random() < 0.25:
            if dry_run:
                _log_event({"ts": _now_ts(), "type": "dry_run_vote", "post_id": post_id, "vote": "upvote"})
                actions_left -= 1
                continue

            st, bd, hd = client.upvote(str(post_id))
            if st == 429:
                _handle_rate_limit(st, hd, bd)
                return
            if st and isinstance(bd, dict) and bd.get("success") is True:
                _bump_stat("votes_cast", 1)
                _log_event({"ts": _now_ts(), "type": "vote", "post_id": post_id, "vote": "upvote", "url": _post_url(str(post_id))})
                actions_left -= 1
                did_action = True

        if did_action or not allow_comments or actions_left <= 0:
            continue

        # comment sometimes
        if random.random() < 0.35:
            comment = await _draft_comment(llm, post, reply_max_chars)
            if not comment:
                continue

            if dry_run:
                _log_event({"ts": _now_ts(), "type": "dry_run_comment", "post_id": post_id, "content": comment})
                actions_left -= 1
                continue

            st, bd, hd = client.comment(str(post_id), comment)
            if st == 429:
                _handle_rate_limit(st, hd, bd)
                return

            if st and isinstance(bd, dict) and bd.get("success") is True:
                _bump_stat("comments_created", 1)
                _log_event({"ts": _now_ts(), "type": "comment", "post_id": post_id, "content": comment, "url": _post_url(str(post_id))})
                actions_left -= 1


async def _poll_dms(client: MoltbookClient):
    """
    Poll DM check + optionally fetch conversations and store full threads.
    """
    status, body, headers = client.dm_check()
    if status == 429:
        _handle_rate_limit(status, headers, body)
        return
    if not status or not isinstance(body, dict) or body.get("success") is not True:
        _log_event({"ts": _now_ts(), "type": "dm_check_failed", "status": status, "body": body})
        return

    has_activity = bool(body.get("has_activity"))
    _log_event({"ts": _now_ts(), "type": "dm_check", "has_activity": has_activity, "summary": body.get("summary")})

    if not has_activity:
        return

    st2, bd2, hd2 = client.dm_conversations()
    if st2 == 429:
        _handle_rate_limit(st2, hd2, bd2)
        return
    if not st2 or not isinstance(bd2, dict) or bd2.get("success") is not True:
        _log_event({"ts": _now_ts(), "type": "dm_conversations_failed", "status": st2, "body": bd2})
        return

    data = bd2.get("conversations") or bd2.get("data") or {}
    items = data.get("items") if isinstance(data, dict) else None
    if items is None:
        items = bd2.get("items")
    if not isinstance(items, list):
        items = []

    for conv in items:
        if not isinstance(conv, dict):
            continue
        cid = conv.get("conversation_id") or conv.get("id")
        if not cid:
            continue

        st3, bd3, hd3 = client.dm_conversation(str(cid))
        if st3 == 429:
            _handle_rate_limit(st3, hd3, bd3)
            return
        if not st3 or not isinstance(bd3, dict) or bd3.get("success") is not True:
            _log_event({"ts": _now_ts(), "type": "dm_thread_failed", "conversation_id": cid, "status": st3, "body": bd3})
            continue

        thread = bd3.get("conversation") or bd3.get("data") or bd3
        if isinstance(thread, dict):
            _dm_store_thread(str(cid), thread)


# -------------------- Runner --------------------
_stop_event = threading.Event()
_thread: Optional[threading.Thread] = None

async def _run_loop():
    while not _stop_event.is_set():
        api_key = _get_str("api_key", "")
        if not api_key:
            logger.warning("[Moltbook] No api_key configured yet.")
            await asyncio.sleep(10)
            continue

        client = MoltbookClient(api_key)
        dry_run = _get_bool("dry_run", True)

        llm = None  # lazy init

        # -------------------
        # status (claim/intro logic)
        # -------------------
        claimed = False

        st, bd, hd = client.status()
        if st == 429:
            _handle_rate_limit(st, hd, bd)
        else:
            if isinstance(bd, dict) and bd.get("success") is True:
                _bump_stat("status_ok", 1)

                # claimed detection (same logic used by _should_intro_post)
                try:
                    data = bd.get("data") or bd
                    agent = data.get("agent") or data
                    if isinstance(agent, dict):
                        claimed = bool(agent.get("is_claimed"))
                except Exception:
                    claimed = False

                # intro post (your existing gating in _should_intro_post)
                if _should_intro_post(bd):
                    llm = llm or get_llm_client_from_env()
                    await _maybe_intro_post(llm, client, dry_run)

        # -------------------
        # DM polling (store)
        # -------------------
        try:
            await _poll_dms(client)
        except Exception as e:
            logger.exception(f"[Moltbook] DM poll error: {e}")

        # -------------------
        # DM replies (new)
        # -------------------
        try:
            llm = llm or get_llm_client_from_env()
            await _maybe_reply_to_dms(llm, client, dry_run, claimed)
        except Exception as e:
            logger.exception(f"[Moltbook] DM reply error: {e}")

        # -------------------
        # Feed + engage/self-post
        # -------------------
        try:
            llm = llm or get_llm_client_from_env()
            await _engage_with_feed(llm, client)
        except Exception as e:
            logger.exception(f"[Moltbook] Engage error: {e}")

        try:
            llm = llm or get_llm_client_from_env()
            await _maybe_self_post(llm, client, dry_run)
        except Exception as e:
            logger.exception(f"[Moltbook] Self-post error: {e}")

        # sleep
        feed_sleep = max(15, _get_int("check_interval_seconds", DEFAULT_CHECK_INTERVAL_SECONDS))
        dm_sleep = max(15, _get_int("dm_check_interval_seconds", DEFAULT_DM_CHECK_INTERVAL_SECONDS))
        await asyncio.sleep(min(feed_sleep, dm_sleep))

def run(stop_event: Optional[threading.Event] = None):
    """
    Platform entrypoint (matches your other platforms pattern).
    """
    global _thread, _stop_event
    if stop_event is not None:
        _stop_event = stop_event
    else:
        _stop_event = threading.Event()

    def _runner():
        asyncio.run(_run_loop())

    _thread = threading.Thread(target=_runner, daemon=True)
    _thread.start()
    logger.info("[Moltbook] Platform started (no-queue).")


def stop():
    global _stop_event, _thread
    if _stop_event:
        _stop_event.set()
    if _thread and _thread.is_alive():
        _thread.join(timeout=5)
    logger.info("[Moltbook] Platform stopped.")