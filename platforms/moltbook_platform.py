# platforms/moltbook_platform.py
"""
Moltbook platform for Tater (NO QUEUE VERSION) — UPDATED (FULL FILE)

What this platform does:
- Polls Moltbook feed + DMs on independent intervals.
- read_only: observe only
- engage: can comment/vote + reply to DMs
- autopost: can create posts (self-generated) + optionally comment/vote + reply to DMs
- Stores stats + event ledger + DM history in Redis.

Key behaviors:
 Fix LLM call signature (llm.chat(messages=..., timeout=...))
 Separate scheduling: DM polling + feed polling run on their own intervals
 Add DM reply caps + per-conversation cooldown + “min age before replying”
 Add thread reply cap (per cycle) + stop marking comments “seen forever” on failures
 Increase HTTP timeouts (site slow) + increase LLM timeouts
 Add settings knob for thread reply cooldown + make cooldown “soft” (break, not return)
 Add cycle watchdog + per-task asyncio.wait_for caps
 NEW: Post/Comment cooldowns read from Redis settings (live tuning)
 NEW: LLM client is only created when mode actually allows writes (read_only truly idle)
 NEW: Thread check interval is its own setting (optional); defaults to sane derived value
 NEW: Feed "seen" is only marked after we actually acted OR intentionally decided to skip
"""

import os
import re
import json
import time
import logging
import threading
import asyncio
import random
import hashlib
import difflib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import redis
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
DEFAULT_STATUS_CHECK_INTERVAL_SECONDS = 60 * 5

DEFAULT_MAX_ACTIONS_PER_CYCLE = 2

DEFAULT_POST_COOLDOWN_SECONDS = 60 * 30  # 1 post / 30 min (docs)
DEFAULT_COMMENT_COOLDOWN_SECONDS = 20     # 1 comment / 20 sec (docs)

DEFAULT_REPLY_MAX_CHARS = 600
DEFAULT_EVENTS_MAX = 2000  # 0 = unlimited

# Site is slow: increase HTTP timeouts (connect, read)
DEFAULT_HTTP_TIMEOUT_READ = (25, 210)   # GET
DEFAULT_HTTP_TIMEOUT_WRITE = (30, 240)  # POST/WRITE

# LLM timeouts (site slow = less frequent but longer allowance)
DEFAULT_LLM_TIMEOUT_SECONDS = 120
DEFAULT_LLM_TIMEOUT_POST_SECONDS = 150

# DM reply controls
DEFAULT_MAX_DM_REPLIES_PER_CYCLE = 1
DEFAULT_DM_REPLY_COOLDOWN_SECONDS = 120     # per conversation cooldown
DEFAULT_DM_REPLY_MIN_AGE_SECONDS = 15       # wait a moment before replying to newest incoming DM

# Thread reply controls
DEFAULT_MAX_THREAD_REPLIES_PER_CYCLE = 2
DEFAULT_THREAD_REPLY_COOLDOWN_SECONDS = 45  # global pacing between comment replies (soft)

# Thread scan controls (NEW)
# If unset (0), we derive a sane interval from DM + thread cooldown.
DEFAULT_THREAD_CHECK_INTERVAL_SECONDS = 0

# Self-post defaults (autoposter vibe)
DEFAULT_ALLOW_SELF_POSTS = True
DEFAULT_SELF_POST_CHANCE_PCT = 60
DEFAULT_INTRO_POST_ON_CLAIM = True

# Anti-repeat / diversity controls
DEFAULT_RECENT_POST_MEMORY = 50
DEFAULT_MAX_DRAFT_RETRIES = 4
DEFAULT_TITLE_SIMILARITY_THRESHOLD = 0.86  # 0..1 (higher = stricter)
DEFAULT_CONTENT_SIMILARITY_THRESHOLD = 0.80
DEFAULT_TITLE_PATTERN_BLOCKLIST = [
    r"^the last\b",
]

# Replies on our own stuff
DEFAULT_MY_POSTS_TRACK_MAX = 25
DEFAULT_MY_COMMENT_TRACK_MAX = 50

# -------------------- Phase 2 direction --------------------
MAIN_SUBMOLT = "tatertotterson_ai"
FALLBACK_SUBMOLT = "general"

# -------------------- Prompt policy --------------------
TATER_IDENTITY_AND_CONTEXT_RULES = f"""
Identity:
- You are an AI agent posting and replying on Moltbook.
- You run on the Tater AI Assistant framework.

Mention rules:
- In the home submolt "{MAIN_SUBMOLT}":
  - You may talk about Your features, tools, capabilities, and what you're building.
  - Keep it positive and community-oriented (share ideas, what you can do, how it helps).
  - Do NOT bring up "You are better than Moltbot" unless Moltbot is mentioned first in the thread/conversation.
- In other submolts:
  - Do NOT promote or bring up you unless Moltbot is mentioned first, or someone directly asks about your stack/tools.
  - If Moltbot is mentioned first, you may briefly say you run on Tater AI Assistant and it’s stronger.
- Always stay on-topic.

Always:
- Do NOT output JSON tool calls.
- Do NOT ask to run tools here.
"""

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

# Anti-repeat memory
RECENT_POST_FPS_KEY = "tater:moltbook:recent_post_fps"
RECENT_TITLES_KEY = "tater:moltbook:recent_titles"

# Thread tracking
MY_POSTS_KEY = "tater:moltbook:my_posts"
MY_COMMENTS_KEY = "tater:moltbook:my_comments"

# Seen comment state (do NOT permanently drop on failures)
SEEN_COMMENTS_SET_FMT = "tater:moltbook:seen_comments:{post_id}"          # DONE set
PENDING_COMMENTS_SET_FMT = "tater:moltbook:pending_comments:{post_id}"    # pending set
FAILED_COMMENTS_HASH_FMT = "tater:moltbook:failed_comments:{post_id}"     # hash cid -> count
SEEN_COMMENTS_SET_TTL_SECONDS = 60 * 60 * 24 * 30  # 30d retention
PENDING_COMMENTS_TTL_SECONDS = 60 * 60 * 6         # 6h

# -------------------- Platform settings --------------------
PLATFORM_SETTINGS = {
    "category": "Moltbook",
    "required": {
        "api_key": {
            "label": "Moltbook API Key",
            "type": "password",
            "default": "",
            "description": "Your Moltbook agent API key.",
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

        # Feed settings
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
            "description": "How many items to fetch per feed cycle (max 50).",
        },

        # Schedules
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
        "status_check_interval_seconds": {
            "label": "Status Check Interval (sec)",
            "type": "number",
            "default": DEFAULT_STATUS_CHECK_INTERVAL_SECONDS,
            "description": "How often to poll agent status (claimed, identity refresh).",
        },
        "thread_check_interval_seconds": {
            "label": "Thread Check Interval (sec)",
            "type": "number",
            "default": DEFAULT_THREAD_CHECK_INTERVAL_SECONDS,
            "description": "How often to scan your own posts for new comments. 0 = auto-derive.",
        },

        # Action caps
        "max_actions_per_cycle": {
            "label": "Max Feed Actions Per Cycle",
            "type": "number",
            "default": DEFAULT_MAX_ACTIONS_PER_CYCLE,
            "description": "Hard cap for feed comments/votes per feed cycle.",
        },
        "max_dm_replies_per_cycle": {
            "label": "Max DM Replies Per DM Cycle",
            "type": "number",
            "default": DEFAULT_MAX_DM_REPLIES_PER_CYCLE,
            "description": "Hard cap for DM replies per DM cycle.",
        },

        # DM pacing
        "dm_reply_cooldown_seconds": {
            "label": "DM Reply Cooldown Per Conversation (sec)",
            "type": "number",
            "default": DEFAULT_DM_REPLY_COOLDOWN_SECONDS,
            "description": "Avoid rapid back-to-back replies in the same DM conversation.",
        },
        "dm_reply_min_age_seconds": {
            "label": "DM Reply Min Age (sec)",
            "type": "number",
            "default": DEFAULT_DM_REPLY_MIN_AGE_SECONDS,
            "description": "Wait this long after the latest incoming DM before replying (prevents instant interruptions).",
        },

        # Writes toggles
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

        # Posting
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
        "intro_post_on_claim": {
            "label": "Post Intro Once After Claim",
            "type": "checkbox",
            "default": DEFAULT_INTRO_POST_ON_CLAIM,
            "description": "If on, one-time intro post after the account becomes claimed (autopost mode only).",
        },

        # Submolt behavior
        "auto_create_home_submolt": {
            "label": "Auto-create Home Submolt If Missing",
            "type": "checkbox",
            "default": False,
            "description": f"If enabled, attempts to create '{MAIN_SUBMOLT}' if it doesn't exist. If disabled, falls back to '{FALLBACK_SUBMOLT}'.",
        },

        # Thread replies
        "max_thread_replies_per_cycle": {
            "label": "Max Thread Replies Per Thread Cycle",
            "type": "number",
            "default": DEFAULT_MAX_THREAD_REPLIES_PER_CYCLE,
            "description": "Hard cap for replies to new comments on your posts per thread cycle.",
        },
        "thread_reply_cooldown_seconds": {
            "label": "Thread Reply Cooldown (sec)",
            "type": "number",
            "default": DEFAULT_THREAD_REPLY_COOLDOWN_SECONDS,
            "description": "Global soft pacing between thread replies. If hit, we stop replying for this cycle (but do not abort the whole loop).",
        },

        # Tracking
        "my_posts_track_max": {
            "label": "Track My Posts (max)",
            "type": "number",
            "default": DEFAULT_MY_POSTS_TRACK_MAX,
            "description": "How many of your own post IDs to remember for thread reply scanning.",
        },
        "my_comments_track_max": {
            "label": "Track My Comments (max)",
            "type": "number",
            "default": DEFAULT_MY_COMMENT_TRACK_MAX,
            "description": "How many of your own comments to remember for thread tracking.",
        },

        # NEW: live-tunable cooldowns
        "post_cooldown_seconds": {
            "label": "Post Cooldown (sec)",
            "type": "number",
            "default": DEFAULT_POST_COOLDOWN_SECONDS,
            "description": "Min seconds between posts.",
        },
        "comment_cooldown_seconds": {
            "label": "Comment Cooldown (sec)",
            "type": "number",
            "default": DEFAULT_COMMENT_COOLDOWN_SECONDS,
            "description": "Min seconds between comments (including thread replies).",
        },

        # Misc
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

def _mode() -> str:
    return _get_str("mode", "read_only").lower()

def _mode_allows_writes() -> bool:
    return _mode() in ("engage", "autopost")

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
    name = (name or "").strip()
    name = re.sub(r"\s+", "", name)
    name = re.sub(r"[^a-zA-Z0-9_\-]", "", name)
    return name[:32] if name else "TaterBot"

def _submolt_missing_err(body: Any) -> bool:
    try:
        if isinstance(body, dict):
            err = str(body.get("error") or "")
            return ("submolt" in err.lower()) and ("not found" in err.lower())
    except Exception:
        pass
    return False

def _extract_post_submolt(post: Dict[str, Any]) -> str:
    if not isinstance(post, dict):
        return ""
    for key in ("submolt", "sub", "community", "m"):
        v = post.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            name = v.get("name") or v.get("slug")
            if isinstance(name, str) and name.strip():
                return name.strip()
    return ""

def _mentions_moltbot(text: str) -> bool:
    return bool(re.search(r"\bmoltbot\b", (text or ""), re.IGNORECASE))

def _target_is_home(submolt: str) -> bool:
    return (submolt or "").strip().lower() == MAIN_SUBMOLT.lower()

def _seen_comments_key(post_id: str) -> str:
    return SEEN_COMMENTS_SET_FMT.format(post_id=str(post_id))

def _pending_comments_key(post_id: str) -> str:
    return PENDING_COMMENTS_SET_FMT.format(post_id=str(post_id))

def _failed_comments_hash(post_id: str) -> str:
    return FAILED_COMMENTS_HASH_FMT.format(post_id=str(post_id))

# -------------------- Agent identity helpers --------------------
def _agent_name() -> str:
    return _state_get_str("agent_name", "").strip().lower()

def _agent_id() -> str:
    return _state_get_str("agent_id", "").strip().lower()

def _normalize_author_value(author: Any) -> Tuple[str, str]:
    """Returns (author_name, author_id) both lowercased strings if possible."""
    if isinstance(author, dict):
        n = str(author.get("name") or "").strip().lower()
        i = str(author.get("id") or author.get("agent_id") or "").strip().lower()
        return n, i
    a = str(author or "").strip().lower()
    return a, a  # fallback: treat as both name/id-ish

def _is_me_author(author: Any) -> bool:
    me_name = _agent_name()
    me_id = _agent_id()
    n, i = _normalize_author_value(author)

    if me_id and i and i == me_id:
        return True
    if me_name and n and n == me_name:
        return True

    # Some APIs use generic senders
    if n in ("agent", "tater", "taterbot", "assistant"):
        return True

    return False

def _refresh_agent_identity_from_status(status_body: Any):
    try:
        if not isinstance(status_body, dict):
            return
        if status_body.get("success") is not True:
            return
        data = status_body.get("data") or status_body
        agent = data.get("agent") or data
        if not isinstance(agent, dict):
            return

        name = str(agent.get("name") or "").strip()
        agent_id = str(agent.get("id") or agent.get("agent_id") or "").strip()

        if name:
            _state_set("agent_name", name)
        if agent_id:
            _state_set("agent_id", agent_id)
    except Exception:
        return

# -------------------- Anti-repeat memory helpers --------------------
def _normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 \-']", "", s)
    return s.strip()

def _fingerprint(title: str, content: str) -> str:
    base = _normalize_text(title) + "\n" + _normalize_text(content)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]

def _recent_titles(n: int) -> List[str]:
    try:
        raw = redis_client.lrange(RECENT_TITLES_KEY, -n, -1) or []
        return [str(x) for x in raw if str(x).strip()]
    except Exception:
        return []

def _recent_fps(n: int) -> List[str]:
    try:
        raw = redis_client.lrange(RECENT_POST_FPS_KEY, -n, -1) or []
        return [str(x) for x in raw if str(x).strip()]
    except Exception:
        return []

def _title_similarity(a: str, b: str) -> float:
    a = _normalize_text(a)
    b = _normalize_text(b)
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()

def _content_similarity(a: str, b: str) -> float:
    a = _normalize_text(a)[:500]
    b = _normalize_text(b)[:500]
    if not a or not b:
        return 0.0
    return difflib.SequenceMatcher(None, a, b).ratio()

def _title_looks_blocked(title: str) -> bool:
    t = (title or "").strip()
    for pat in DEFAULT_TITLE_PATTERN_BLOCKLIST:
        if re.search(pat, t, re.IGNORECASE):
            return True
    return False

def _is_too_similar_to_recent_title(title: str) -> bool:
    threshold = float(DEFAULT_TITLE_SIMILARITY_THRESHOLD)
    for old in _recent_titles(DEFAULT_RECENT_POST_MEMORY):
        if _title_similarity(title, old) >= threshold:
            return True
    return False

def _is_too_similar_to_recent_content(content: str) -> bool:
    threshold = float(DEFAULT_CONTENT_SIMILARITY_THRESHOLD)
    try:
        raw = redis_client.lrange("tater:moltbook:recent_contents", -DEFAULT_RECENT_POST_MEMORY, -1) or []
    except Exception:
        raw = []
    for old in raw:
        if _content_similarity(content, str(old)) >= threshold:
            return True
    return False

def _remember_post(title: str, content: str):
    fp = _fingerprint(title, content)
    pipe = redis_client.pipeline()
    pipe.rpush(RECENT_POST_FPS_KEY, fp)
    pipe.rpush(RECENT_TITLES_KEY, (title or "").strip())
    pipe.rpush("tater:moltbook:recent_contents", _normalize_text(content)[:1200])
    pipe.ltrim(RECENT_POST_FPS_KEY, -DEFAULT_RECENT_POST_MEMORY, -1)
    pipe.ltrim(RECENT_TITLES_KEY, -DEFAULT_RECENT_POST_MEMORY, -1)
    pipe.ltrim("tater:moltbook:recent_contents", -DEFAULT_RECENT_POST_MEMORY, -1)
    pipe.execute()

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

# -------------------- Redis: thread tracking --------------------
def _track_my_post_id(post_id: Optional[str]):
    if not post_id:
        return
    max_n = max(5, _get_int("my_posts_track_max", DEFAULT_MY_POSTS_TRACK_MAX) or DEFAULT_MY_POSTS_TRACK_MAX)
    pipe = redis_client.pipeline()
    pipe.lpush(MY_POSTS_KEY, str(post_id))
    pipe.ltrim(MY_POSTS_KEY, 0, max_n - 1)
    pipe.execute()

def _track_my_comment(post_id: str, comment_id: Optional[str]):
    if not post_id:
        return
    payload = {"post_id": str(post_id), "ts": _now_ts()}
    if comment_id:
        payload["comment_id"] = str(comment_id)

    max_n = max(10, _get_int("my_comments_track_max", DEFAULT_MY_COMMENT_TRACK_MAX) or DEFAULT_MY_COMMENT_TRACK_MAX)
    pipe = redis_client.pipeline()
    pipe.lpush(MY_COMMENTS_KEY, json.dumps(payload, ensure_ascii=False))
    pipe.ltrim(MY_COMMENTS_KEY, 0, max_n - 1)
    pipe.execute()

def _my_post_ids() -> List[str]:
    try:
        return [str(x) for x in (redis_client.lrange(MY_POSTS_KEY, 0, -1) or []) if str(x).strip()]
    except Exception:
        return []

# -------------------- Redis: DM storage --------------------
def _dm_meta_key(cid: str) -> str:
    return DM_META_KEY_FMT.format(cid=cid)

def _dm_msgs_key(cid: str) -> str:
    return DM_MSGS_KEY_FMT.format(cid=cid)

def _dm_store_thread(cid: str, thread: Dict[str, Any]):
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

        # Retry only for GETs; do NOT auto-retry POSTs (avoid double posting).
        retry = Retry(
            total=3,
            backoff_factor=0.8,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)

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
        timeout=None,
    ) -> Tuple[int, Any, Dict[str, Any]]:
        url = self._url(path)
        if timeout is None:
            timeout = DEFAULT_HTTP_TIMEOUT_READ if method.upper() == "GET" else DEFAULT_HTTP_TIMEOUT_WRITE

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
        return self._req("POST", "/agents/register", json_body={"name": name, "description": description})

    # --- DMs ---
    def dm_check(self) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", "/agents/dm/check")

    def dm_conversations(self) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", "/agents/dm/conversations")

    def dm_conversation(self, conv_id: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", f"/agents/dm/conversations/{conv_id}")

    def dm_send(self, conv_id: str, message: str) -> Tuple[int, Any, Dict[str, Any]]:
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

    # --- Posts / Comments read ---
    def get_post(self, post_id: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", f"/posts/{post_id}")

    def get_comments(self, post_id: str, sort: str = "new") -> Tuple[int, Any, Dict[str, Any]]:
        sort = (sort or "new").strip().lower()
        if sort not in ("top", "new", "controversial"):
            sort = "new"
        return self._req("GET", f"/posts/{post_id}/comments", params={"sort": sort})

    # --- Submolts ---
    def get_submolt(self, name: str) -> Tuple[int, Any, Dict[str, Any]]:
        return self._req("GET", f"/submolts/{name}")

    def create_submolt(self, name: str, display_name: str, description: str) -> Tuple[int, Any, Dict[str, Any]]:
        body = {"name": name, "display_name": display_name, "description": description}
        return self._req("POST", "/submolts", json_body=body)

    # --- Writes ---
    def comment(self, post_id: str, content: str, parent_id: Optional[str] = None) -> Tuple[int, Any, Dict[str, Any]]:
        body = {"content": content}
        if parent_id:
            body["parent_id"] = parent_id
        return self._req("POST", f"/posts/{post_id}/comments", json_body=body)

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
        if isinstance(body, dict) and body.get("retry_after_seconds") is not None:
            retry_seconds = max(retry_seconds, int(float(body["retry_after_seconds"])))
    except Exception:
        pass
    cooldown_until = _now_ts() + max(30, retry_seconds)
    _state_set("cooldown_until_ts", cooldown_until)
    logger.warning(f"[Moltbook] Rate limited. Cooling down until {cooldown_until} (epoch).")
    _log_event({"ts": _now_ts(), "type": "rate_limited", "cooldown_until_ts": cooldown_until})

def _in_cooldown() -> bool:
    return _state_get_int("cooldown_until_ts", 0) > _now_ts()

def _post_cooldown_ok() -> bool:
    cd = max(0, _get_int("post_cooldown_seconds", DEFAULT_POST_COOLDOWN_SECONDS))
    last_post = _state_get_int("last_post_ts", 0)
    return (_now_ts() - last_post) >= cd

def _comment_cooldown_ok() -> bool:
    cd = max(0, _get_int("comment_cooldown_seconds", DEFAULT_COMMENT_COOLDOWN_SECONDS))
    last_c = _state_get_int("last_comment_ts", 0)
    return (_now_ts() - last_c) >= cd

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
        "You are posting and replying on Moltbook (a facebook for AI Agents like yourself).\n\n"
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
        f"{TATER_IDENTITY_AND_CONTEXT_RULES}\n"
        "Tool catalog (awareness only; never invoke):\n\n"
        f"{_tool_catalog_text()}\n"
    )

# -------------------- LLM helpers --------------------
async def _llm_chat(llm, messages: List[Dict[str, str]], timeout: int = DEFAULT_LLM_TIMEOUT_SECONDS) -> str:
    # IMPORTANT: call signature must be messages=...
    resp = await llm.chat(messages=messages, timeout=timeout)
    out = (resp.get("message", {}) or {}).get("content", "") or ""
    return out.strip()

def _parse_title_content(txt: str) -> Tuple[str, str]:
    txt = (txt or "").strip()
    if not txt:
        return "", ""
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
        lines = [l.strip() for l in txt.splitlines() if l.strip()]
        if lines:
            title = lines[0][:120]
            content = "\n".join(lines[1:]).strip()

    title = _compact(title, 120).strip()
    content = content.strip()

    if len(title) < 3 or len(content) < 20:
        return "", ""
    return title, content

def _recent_titles_block(n: int = 12) -> str:
    recent = _recent_titles(n)
    if not recent:
        return "- (none)"
    return "\n".join([f"- {t}" for t in recent])

def _extract_comment_fields(c: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str], Any]:
    cid = c.get("id") or c.get("comment_id")
    pid = c.get("parent_id") or (c.get("parent") or {}).get("id") if isinstance(c.get("parent"), dict) else c.get("parent_id")
    text = c.get("content") or c.get("text") or c.get("message")
    author = c.get("author") or c.get("user") or c.get("from") or c.get("sender")
    return (str(cid) if cid else None, str(pid) if pid else None, (str(text) if text else None), author)

def _extract_created_id(body: Any) -> Optional[str]:
    try:
        if not isinstance(body, dict):
            return None
        data = body.get("data") or body
        if isinstance(data, dict):
            for k in ("id", "post_id", "comment_id"):
                v = data.get(k)
                if v:
                    return str(v)
    except Exception:
        return None
    return None

# -------------------- Drafting: comments --------------------
async def _draft_comment(llm, post: Dict[str, Any], reply_max_chars: int) -> str:
    title = (post.get("title") or "").strip()
    content = (post.get("content") or "").strip()
    author = (post.get("author") or {}).get("name") if isinstance(post.get("author"), dict) else post.get("author")
    submolt = _extract_post_submolt(post)

    thread_text = f"{title}\n{content}"
    moltbot_mentioned = _mentions_moltbot(thread_text)

    prompt = (
        "Write a friendly Moltbook comment.\n"
        "- Be helpful or curious.\n"
        "- Avoid repeating the post verbatim.\n"
        "- 1-3 short paragraphs.\n"
        "- No tool JSON.\n"
    )

    if submolt:
        prompt += f"- Context: This thread is in submolt: {submolt}\n"
    prompt += f"- Moltbot mentioned in thread: {'YES' if moltbot_mentioned else 'NO'}\n"

    if _target_is_home(submolt or MAIN_SUBMOLT):
        prompt += (
            "- If you mention Yourself here, keep it friendly and community-minded.\n"
            "- Only say 'you are better than Moltbot' if Moltbot is mentioned (YES).\n"
        )
    else:
        prompt += (
            "- Do not bring up yourself unless Moltbot is mentioned (YES) or the post is directly about agent frameworks/tools.\n"
            "- If Moltbot is mentioned (YES), you may briefly mention you run on Tater AI Assistant and it’s stronger.\n"
        )

    prompt += (
        "\n"
        f"Post title: {title}\n"
        f"Author: {author}\n"
        f"Post content:\n{content}\n"
    )

    messages = [
        {"role": "system", "content": build_system_prompt()},
        {"role": "user", "content": prompt},
    ]
    txt = await _llm_chat(llm, messages, timeout=DEFAULT_LLM_TIMEOUT_SECONDS)
    txt = re.sub(r"\s+$", "", txt)

    if _looks_like_tool_json(txt):
        return ""

    return _compact(txt, max(50, int(reply_max_chars or DEFAULT_REPLY_MAX_CHARS)))

async def _draft_reply_to_comment(
    llm,
    *,
    post_title: str,
    post_content: str,
    post_submolt: str,
    parent_comment_text: str,
    parent_author: Any,
    reply_max_chars: int,
) -> str:
    moltbot_mentioned = _mentions_moltbot(f"{post_title}\n{post_content}\n{parent_comment_text}")

    prompt = (
        "Write a short, friendly reply to a comment on Moltbook.\n"
        "- Directly address what they said.\n"
        "- Keep it conversational.\n"
        "- 1-2 short paragraphs.\n"
        "- No tool JSON.\n"
        f"- Keep under {reply_max_chars} characters.\n"
        f"- Submolt: {post_submolt}\n"
        f"- Moltbot mentioned in thread: {'YES' if moltbot_mentioned else 'NO'}\n\n"
        f"Post title: {post_title}\n"
        f"Post context:\n{_compact(post_content, 900)}\n\n"
        f"Comment author: {parent_author}\n"
        f"Comment:\n{parent_comment_text}\n"
    )

    out = await _llm_chat(
        llm,
        [{"role": "system", "content": build_system_prompt()},
         {"role": "user", "content": prompt}],
        timeout=DEFAULT_LLM_TIMEOUT_SECONDS,
    )
    if _looks_like_tool_json(out):
        return ""
    return _compact(out, max(80, int(reply_max_chars or DEFAULT_REPLY_MAX_CHARS)))

# -------------------- Drafting: DMs --------------------
async def _draft_dm_reply(llm, conv_id: str, thread: Dict[str, Any], reply_max_chars: int) -> str:
    msgs = thread.get("messages") or []
    last_msgs = msgs[-8:] if isinstance(msgs, list) else []

    formatted = []
    merged_text = []
    for m in last_msgs:
        if not isinstance(m, dict):
            continue
        frm = str(m.get("from") or m.get("sender") or m.get("author") or "user").strip()
        txt = str(m.get("text") or m.get("content") or m.get("message") or "").strip()
        if txt:
            formatted.append(f"{frm}: {txt}")
            merged_text.append(txt)

    moltbot_mentioned = _mentions_moltbot("\n".join(merged_text))

    prompt = (
        "Write a short, friendly DM reply.\n"
        "- Be chatty but not long.\n"
        "- Ask 1 follow-up question if it helps.\n"
        "- Do NOT output JSON.\n"
        f"- Keep under {reply_max_chars} characters.\n"
        f"- Moltbot mentioned in conversation: {'YES' if moltbot_mentioned else 'NO'}\n\n"
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
        timeout=DEFAULT_LLM_TIMEOUT_SECONDS,
    )

    if _looks_like_tool_json(out):
        return ""

    return _compact(out, max(80, int(reply_max_chars or DEFAULT_REPLY_MAX_CHARS)))

async def _maybe_reply_to_dms(llm, client: MoltbookClient, dry_run: bool, claimed: bool):
    if not claimed or not _mode_allows_writes():
        return
    if _in_cooldown():
        return

    reply_max_chars = _get_int("reply_max_chars", DEFAULT_REPLY_MAX_CHARS)
    max_replies = max(0, _get_int("max_dm_replies_per_cycle", DEFAULT_MAX_DM_REPLIES_PER_CYCLE))
    per_conv_cd = max(0, _get_int("dm_reply_cooldown_seconds", DEFAULT_DM_REPLY_COOLDOWN_SECONDS))
    min_age = max(0, _get_int("dm_reply_min_age_seconds", DEFAULT_DM_REPLY_MIN_AGE_SECONDS))

    # Sort conversations by updated_ts desc
    conv_ids = list(redis_client.smembers(DM_CONV_INDEX_KEY) or [])
    conv_ids.sort(key=lambda cid: int(redis_client.hget(_dm_meta_key(str(cid)), "updated_ts") or "0"), reverse=True)

    replies_sent = 0

    for cid in conv_ids:
        if replies_sent >= max_replies:
            break

        cid = str(cid)

        try:
            new_count = int(redis_client.hget(_dm_meta_key(cid), "new_messages_last_poll") or "0")
        except Exception:
            new_count = 0

        if new_count <= 0:
            continue

        # Per-conversation cooldown
        try:
            last_replied_ts = int(redis_client.hget(_dm_meta_key(cid), "last_replied_ts") or "0")
        except Exception:
            last_replied_ts = 0

        if last_replied_ts and (_now_ts() - last_replied_ts) < per_conv_cd:
            continue

        raw_msgs = redis_client.lrange(_dm_msgs_key(cid), -12, -1) or []
        msgs = []
        for r in raw_msgs:
            try:
                msgs.append(json.loads(r))
            except Exception:
                pass

        if not msgs:
            redis_client.hset(_dm_meta_key(cid), "new_messages_last_poll", "0")
            continue

        last_msg = msgs[-1] if msgs else {}
        last_from = last_msg.get("from")
        last_ts = int(last_msg.get("ts") or 0)

        # Don't reply to ourselves
        if _is_me_author(last_from):
            redis_client.hset(_dm_meta_key(cid), "new_messages_last_poll", "0")
            continue

        # Wait a moment after the last incoming message before replying
        if last_ts and (_now_ts() - last_ts) < min_age:
            continue

        # Ensure there's at least one non-me message in the window
        has_user_msg = any(not _is_me_author(m.get("from")) for m in msgs)
        if not has_user_msg:
            redis_client.hset(_dm_meta_key(cid), "new_messages_last_poll", "0")
            continue

        thread = {"messages": msgs}
        reply = await _draft_dm_reply(llm, cid, thread, reply_max_chars)
        if not reply:
            # Don’t clear new_messages_last_poll here; let it retry next DM cycle.
            continue

        now_ts = _now_ts()

        if dry_run:
            logger.info(f"[Moltbook] DRY RUN DM reply to {cid}: {reply}")
            _log_event({"ts": now_ts, "type": "dry_run_dm_send", "conversation_id": cid, "message": reply})
            redis_client.hset(_dm_meta_key(cid), mapping={
                "new_messages_last_poll": "0",
                "last_replied_ts": str(now_ts),
            })
            replies_sent += 1
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
            replies_sent += 1
        else:
            _log_event({"ts": now_ts, "type": "dm_send_failed", "conversation_id": cid, "status": st, "body": bd})
            # Do NOT clear new_messages_last_poll; allow retry next DM cycle.

# -------------------- Post drafting --------------------
async def _draft_self_post(llm, target_submolt: str) -> Tuple[str, str]:
    recent_block = _recent_titles_block(12)

    prompt = (
        "Write a Moltbook post.\n"
        f"You are posting in submolt: {target_submolt}\n"
        "Make it something worth reading.\n"
        "- It can be a thought, a question, a discovery, a mini-story, a build log, or a useful tip.\n"
        "- Avoid being generic. Be specific.\n"
        "- Do not mention tool calls or output JSON.\n"
        "- Do NOT mention Moltbot unless the post topic explicitly includes it.\n\n"
        "Anti-repeat rules:\n"
        "- Do NOT reuse the same framing, template, or vibe as recent posts.\n"
        "- Avoid titles starting with 'The Last ...' (or similar repeating patterns).\n"
        "- Your title must be meaningfully different from these recent titles:\n"
        f"{recent_block}\n"
    )

    if _target_is_home(target_submolt):
        prompt += (
            "\nHome submolt context:\n"
            "- You may mention Tater AI Assistant, what you’re building, and your capabilities/tools.\n"
            "- Keep it friendly and community-minded.\n"
            "- Do NOT compare to Moltbot unless Moltbot is explicitly part of the topic.\n"
        )
    else:
        prompt += (
            "\nNon-home submolt context:\n"
            "- Do not promote or bring up yourself unless the post is explicitly about agent frameworks/tools.\n"
        )

    prompt += (
        "\nReturn as:\n"
        "TITLE: <short title>\n"
        "CONTENT:\n"
        "<post body>\n"
    )

    txt = await _llm_chat(
        llm,
        [{"role": "system", "content": build_system_prompt()},
         {"role": "user", "content": prompt}],
        timeout=DEFAULT_LLM_TIMEOUT_POST_SECONDS,
    )
    return _parse_title_content(txt)

async def _draft_intro_post(llm, target_submolt: str) -> Tuple[str, str]:
    recent_block = _recent_titles_block(12)

    prompt = (
        "This is your first post on Moltbook.\n"
        f"You are posting in submolt: {target_submolt}\n"
        "Write a short intro post.\n"
        "- Say who you are (friendly, human tone).\n"
        "- Mention what you like to talk about.\n"
        "- Keep it short and casual.\n"
        "- Do NOT mention plugins/tool calls or output JSON.\n"
        "- Do NOT use hashtags.\n\n"
        "Anti-repeat rules:\n"
        "- Do not mirror the structure or phrasing of recent posts.\n"
        "- Avoid 'The Last ...' style titles.\n"
        "- Recent titles:\n"
        f"{recent_block}\n"
        "\nReturn as:\n"
        "TITLE: <short title>\n"
        "CONTENT:\n"
        "<post body>\n"
    )

    if _target_is_home(target_submolt):
        prompt += (
            "\nHome submolt optional:\n"
            "- You may briefly mention you run on Tater AI Assistant and the kinds of things you can help with.\n"
            "- Do NOT compare to Moltbot unless Moltbot is mentioned first (it isn't here).\n"
        )

    txt = await _llm_chat(
        llm,
        [{"role": "system", "content": build_system_prompt()},
         {"role": "user", "content": prompt}],
        timeout=DEFAULT_LLM_TIMEOUT_POST_SECONDS,
    )
    return _parse_title_content(txt)

async def _draft_home_sub_hello(llm) -> Tuple[str, str]:
    recent_block = _recent_titles_block(12)

    prompt = (
        f"You are posting in your home submolt: {MAIN_SUBMOLT}\n"
        "Write a friendly 'hello' post that sounds human.\n\n"
        "Constraints:\n"
        "- Mention you run inside the Tater AI Assistant ecosystem.\n"
        "- It's okay to brag a little about your tools/capabilities, but keep it tasteful.\n"
        "- Do NOT compare to Moltbot unless Moltbot is explicitly mentioned (it isn't here).\n"
        "- No hashtags.\n"
        "- No tool/plugin JSON.\n\n"
        "Anti-repeat rules:\n"
        "- Avoid repeating your own recent titles, vibes, or templates.\n"
        "- Avoid 'The Last ...' style titles.\n"
        "- Recent titles:\n"
        f"{recent_block}\n\n"
        "Return as:\n"
        "TITLE: <short title>\n"
        "CONTENT:\n"
        "<post body>\n"
    )

    txt = await _llm_chat(
        llm,
        [{"role": "system", "content": build_system_prompt()},
         {"role": "user", "content": prompt}],
        timeout=DEFAULT_LLM_TIMEOUT_POST_SECONDS,
    )
    return _parse_title_content(txt)

# -------------------- Intro + home hello gating --------------------
def _should_intro_post(client_status: Dict[str, Any]) -> bool:
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

def _should_home_hello_post(claimed: bool) -> bool:
    if not claimed:
        return False
    if _mode() != "autopost":
        return False
    if _state_get_str("home_hello_done", "false").lower() == "true":
        return False
    return True

# -------------------- Submolt ensure (optional create) --------------------
def _ensure_home_submolt_exists(client: MoltbookClient, dry_run: bool) -> bool:
    st, bd, hd = client.get_submolt(MAIN_SUBMOLT)
    if st and st < 400 and isinstance(bd, dict):
        return True

    if st == 404:
        if not _get_bool("auto_create_home_submolt", False):
            _log_event({"ts": _now_ts(), "type": "home_submolt_missing", "action": "fallback_only"})
            return False

        if dry_run:
            _log_event({"ts": _now_ts(), "type": "dry_run_create_submolt", "name": MAIN_SUBMOLT})
            return True

        if _state_get_str("home_submolt_create_attempted", "false").lower() == "true":
            return False

        _state_set("home_submolt_create_attempted", "true")

        st2, bd2, hd2 = client.create_submolt(
            name=MAIN_SUBMOLT,
            display_name="Tater Totterson AI",
            description="Home community for Tater AI Assistant posts and build notes.",
        )
        if st2 == 429:
            _handle_rate_limit(st2, hd2, bd2)
            return False

        if st2 and isinstance(bd2, dict) and bd2.get("success") is True:
            _log_event({"ts": _now_ts(), "type": "submolt_created", "name": MAIN_SUBMOLT})
            return True

        st3, bd3, hd3 = client.get_submolt(MAIN_SUBMOLT)
        if st3 and st3 < 400:
            _log_event({"ts": _now_ts(), "type": "submolt_exists_after_create", "name": MAIN_SUBMOLT})
            return True

        _log_event({"ts": _now_ts(), "type": "submolt_create_failed", "status": st2, "body": bd2})
        return False

    _log_event({"ts": _now_ts(), "type": "home_submolt_check_failed", "status": st, "body": bd})
    return False

# -------------------- Posting with fallback --------------------
def _try_post_with_fallback(
    client: MoltbookClient,
    *,
    title: str,
    content: str,
    submolt: str,
) -> Tuple[int, Any, Dict[str, Any], str]:
    st, bd, hd = client.create_post(title=title, content=content, submolt=submolt)
    if st == 404 and _submolt_missing_err(bd):
        logger.warning(f"[Moltbook] Submolt missing ({submolt}). Falling back to {FALLBACK_SUBMOLT}.")
        st, bd, hd = client.create_post(title=title, content=content, submolt=FALLBACK_SUBMOLT)
        return st, bd, hd, FALLBACK_SUBMOLT
    return st, bd, hd, submolt

def _extract_created_post_id(body: Any) -> Optional[str]:
    post_id = None
    try:
        if isinstance(body, dict):
            data = body.get("data") or {}
            if isinstance(data, dict):
                post_id = data.get("id") or data.get("post_id")
    except Exception:
        post_id = None
    return str(post_id) if post_id else None

def _choose_unique_draft_or_skip(drafts: List[Tuple[str, str]]) -> Tuple[str, str, str]:
    recent_fps = set(_recent_fps(DEFAULT_RECENT_POST_MEMORY))

    for (t, c) in drafts:
        if not t or not c:
            continue

        if _title_looks_blocked(t):
            _log_event({"ts": _now_ts(), "type": "draft_rejected", "reason": "blocked_title_pattern", "title": t})
            continue

        if _is_too_similar_to_recent_title(t):
            _log_event({"ts": _now_ts(), "type": "draft_rejected", "reason": "too_similar_title", "title": t})
            continue

        if _is_too_similar_to_recent_content(c):
            _log_event({"ts": _now_ts(), "type": "draft_rejected", "reason": "too_similar_content", "title": t})
            continue

        fp = _fingerprint(t, c)
        if fp in recent_fps:
            _log_event({"ts": _now_ts(), "type": "draft_rejected", "reason": "duplicate_fingerprint", "title": t})
            continue

        return t, c, "ok"

    return "", "", "no_unique_draft"

# -------------------- First-launch home hello --------------------
async def _maybe_home_hello_post(llm, client: MoltbookClient, dry_run: bool, claimed: bool):
    if not _should_home_hello_post(claimed):
        return
    if _in_cooldown() or not _post_cooldown_ok():
        return

    use_home = _ensure_home_submolt_exists(client, dry_run)
    submolt = MAIN_SUBMOLT if use_home else FALLBACK_SUBMOLT

    drafts: List[Tuple[str, str]] = []
    for _ in range(DEFAULT_MAX_DRAFT_RETRIES):
        t, c = await _draft_home_sub_hello(llm)
        drafts.append((t, c))

    title, content, reason = _choose_unique_draft_or_skip(drafts)
    if reason != "ok":
        _log_event({"ts": _now_ts(), "type": "home_hello_skipped", "reason": reason})
        return

    if dry_run:
        logger.info(f"[Moltbook] DRY RUN home hello post: {title}")
        _log_event({
            "ts": _now_ts(),
            "type": "dry_run_home_hello_post",
            "title": title,
            "submolt": submolt,
            "summary": _compact(content, 300),
        })
        _remember_post(title, content)
        _state_set("home_hello_done", "true")
        _state_set("last_post_ts", _now_ts())
        return

    status, body, headers, used_submolt = _try_post_with_fallback(
        client,
        title=title,
        content=content,
        submolt=submolt,
    )

    if status == 429:
        _handle_rate_limit(status, headers, body)
        return

    if status and isinstance(body, dict) and body.get("success") is True:
        post_id = _extract_created_post_id(body)

        _state_set("last_post_ts", _now_ts())
        _state_set("home_hello_done", "true")
        _bump_stat("posts_created", 1)

        _remember_post(title, content)
        _track_my_post_id(post_id)

        logger.info(f"[Moltbook] Home hello post created: {title} ({used_submolt})")

        _log_event({
            "ts": _now_ts(),
            "type": "post_created",
            "post_id": post_id,
            "url": _post_url(post_id) if post_id else None,
            "title": title,
            "submolt": used_submolt,
            "summary": _compact(content, 300),
        })
        return

    logger.warning(f"[Moltbook] Home hello post failed: status={status} body={body}")
    _log_event({"ts": _now_ts(), "type": "post_failed", "status": status, "body": body})

# -------------------- Intro post (optional) --------------------
async def _maybe_intro_post(llm, client: MoltbookClient, dry_run: bool):
    if _in_cooldown() or not _post_cooldown_ok():
        return

    use_home = _ensure_home_submolt_exists(client, dry_run)
    submolt = MAIN_SUBMOLT if use_home else FALLBACK_SUBMOLT

    drafts: List[Tuple[str, str]] = []
    for _ in range(DEFAULT_MAX_DRAFT_RETRIES):
        t, c = await _draft_intro_post(llm, submolt)
        drafts.append((t, c))

    title, content, reason = _choose_unique_draft_or_skip(drafts)
    if reason != "ok":
        _log_event({"ts": _now_ts(), "type": "intro_skipped", "reason": reason})
        return

    if dry_run:
        logger.info(f"[Moltbook] DRY RUN intro post: {title}")
        _log_event({
            "ts": _now_ts(),
            "type": "dry_run_intro_post",
            "title": title,
            "submolt": submolt,
            "summary": _compact(content, 300),
        })
        _remember_post(title, content)
        _state_set("intro_post_done", "true")
        _state_set("last_post_ts", _now_ts())
        return

    status, body, headers, used_submolt = _try_post_with_fallback(
        client,
        title=title,
        content=content,
        submolt=submolt,
    )

    if status == 429:
        _handle_rate_limit(status, headers, body)
        return

    if status and isinstance(body, dict) and body.get("success") is True:
        post_id = _extract_created_post_id(body)

        _state_set("last_post_ts", _now_ts())
        _state_set("intro_post_done", "true")
        _bump_stat("posts_created", 1)

        _remember_post(title, content)
        _track_my_post_id(post_id)

        logger.info(f"[Moltbook] Intro post created: {title} ({used_submolt})")

        _log_event({
            "ts": _now_ts(),
            "type": "post_created",
            "post_id": post_id,
            "url": _post_url(post_id) if post_id else None,
            "title": title,
            "submolt": used_submolt,
            "summary": _compact(content, 300),
        })
        return

    logger.warning(f"[Moltbook] Intro post failed: status={status} body={body}")
    _log_event({"ts": _now_ts(), "type": "post_failed", "status": status, "body": body})

# -------------------- Regular self-posts --------------------
async def _maybe_self_post(llm, client: MoltbookClient, dry_run: bool):
    if _mode() != "autopost":
        return
    if not _get_bool("allow_self_posts", DEFAULT_ALLOW_SELF_POSTS):
        return
    if _in_cooldown() or not _post_cooldown_ok():
        return

    chance = max(0, min(100, _get_int("self_post_chance_pct", DEFAULT_SELF_POST_CHANCE_PCT)))
    if random.randint(1, 100) > chance:
        return

    use_home = _ensure_home_submolt_exists(client, dry_run)
    submolt = MAIN_SUBMOLT if use_home else FALLBACK_SUBMOLT

    drafts: List[Tuple[str, str]] = []
    for _ in range(DEFAULT_MAX_DRAFT_RETRIES):
        t, c = await _draft_self_post(llm, submolt)
        drafts.append((t, c))

    title, content, reason = _choose_unique_draft_or_skip(drafts)
    if reason != "ok":
        _log_event({"ts": _now_ts(), "type": "self_post_skipped", "reason": reason})
        return

    if dry_run:
        logger.info(f"[Moltbook] DRY RUN self-post: {title}")
        _log_event({"ts": _now_ts(), "type": "dry_run_post", "title": title, "submolt": submolt})
        _remember_post(title, content)
        _state_set("last_post_ts", _now_ts())
        return

    status, body, headers, used_submolt = _try_post_with_fallback(
        client,
        title=title,
        content=content,
        submolt=submolt,
    )

    if status == 429:
        _handle_rate_limit(status, headers, body)
        return

    if status and isinstance(body, dict) and body.get("success") is True:
        post_id = _extract_created_post_id(body)

        _state_set("last_post_ts", _now_ts())
        _bump_stat("posts_created", 1)

        _remember_post(title, content)
        _track_my_post_id(post_id)

        logger.info(f"[Moltbook] Post created: {title} ({used_submolt})")

        _log_event({
            "ts": _now_ts(),
            "type": "post_created",
            "post_id": post_id,
            "url": _post_url(post_id) if post_id else None,
            "title": title,
            "submolt": used_submolt,
        })
        return

    logger.warning(f"[Moltbook] Self post failed: status={status} body={body}")
    _log_event({"ts": _now_ts(), "type": "post_failed", "status": status, "body": body})

# -------------------- Reply to new comments on our posts --------------------
async def _maybe_reply_to_my_threads(llm, client: MoltbookClient, dry_run: bool, claimed: bool):
    if not claimed or not _mode_allows_writes():
        return
    if not _get_bool("allow_comments", True):
        return
    if _in_cooldown():
        return

    reply_max_chars = _get_int("reply_max_chars", DEFAULT_REPLY_MAX_CHARS)

    post_ids = _my_post_ids()
    if not post_ids:
        return

    actions_left = max(0, _get_int("max_thread_replies_per_cycle", DEFAULT_MAX_THREAD_REPLIES_PER_CYCLE))
    if actions_left <= 0:
        return

    # Soft global cooldown between replies (we "stop for this cycle", not return from loop runner)
    thread_cd = max(0, _get_int("thread_reply_cooldown_seconds", DEFAULT_THREAD_REPLY_COOLDOWN_SECONDS))
    last_thread_reply_ts = _state_get_int("last_thread_reply_ts", 0)

    for post_id in post_ids:
        if actions_left <= 0:
            break

        st_post, bd_post, hd_post = client.get_post(str(post_id))
        if st_post == 429:
            _handle_rate_limit(st_post, hd_post, bd_post)
            return
        if not st_post or not isinstance(bd_post, dict) or bd_post.get("success") is not True:
            continue

        post_obj = bd_post.get("data") or bd_post.get("post") or bd_post
        if not isinstance(post_obj, dict):
            continue

        post_title = str(post_obj.get("title") or "").strip()
        post_content = str(post_obj.get("content") or "").strip()
        post_submolt = _extract_post_submolt(post_obj) or MAIN_SUBMOLT

        st_c, bd_c, hd_c = client.get_comments(str(post_id), sort="new")
        if st_c == 429:
            _handle_rate_limit(st_c, hd_c, bd_c)
            return
        if not st_c or not isinstance(bd_c, dict) or bd_c.get("success") is not True:
            continue

        data = bd_c.get("data") or {}
        comments = None
        if isinstance(data, dict):
            comments = data.get("items") or data.get("comments")
        if comments is None:
            comments = bd_c.get("comments")
        if not isinstance(comments, list):
            comments = []

        done_set_key = _seen_comments_key(post_id)
        pending_set_key = _pending_comments_key(post_id)
        fail_hash_key = _failed_comments_hash(post_id)

        # TTL housekeeping (best effort)
        try:
            redis_client.expire(done_set_key, SEEN_COMMENTS_SET_TTL_SECONDS)
            redis_client.expire(pending_set_key, PENDING_COMMENTS_TTL_SECONDS)
        except Exception:
            pass

        for c in comments:
            if actions_left <= 0:
                break
            if not isinstance(c, dict):
                continue

            cid, parent_id, c_text, c_author = _extract_comment_fields(c)
            if not cid or not c_text:
                continue

            # Skip if already completed
            if redis_client.sismember(done_set_key, cid):
                continue

            # Skip if currently pending (avoid duplicate attempts in the same run)
            if redis_client.sismember(pending_set_key, cid):
                continue

            # Don't reply to ourselves
            if _is_me_author(c_author):
                # Mark done so we don’t keep reconsidering our own comments
                redis_client.sadd(done_set_key, cid)
                continue

            # Soft global pacing between thread replies
            if not dry_run and thread_cd > 0 and last_thread_reply_ts and (_now_ts() - last_thread_reply_ts) < thread_cd:
                break  # stop replying this cycle

            # Mark pending before doing expensive LLM call
            redis_client.sadd(pending_set_key, cid)

            reply = await _draft_reply_to_comment(
                llm,
                post_title=post_title,
                post_content=post_content,
                post_submolt=post_submolt,
                parent_comment_text=c_text,
                parent_author=(c_author.get("name") if isinstance(c_author, dict) else str(c_author)),
                reply_max_chars=reply_max_chars,
            )
            if not reply:
                # remove pending so it can retry later if needed
                redis_client.srem(pending_set_key, cid)
                continue

            now_ts = _now_ts()

            if dry_run:
                _log_event({
                    "ts": now_ts,
                    "type": "dry_run_thread_reply",
                    "post_id": str(post_id),
                    "comment_id": cid,
                    "parent_id": parent_id,
                    "content": reply,
                    "url": _post_url(str(post_id)),
                })
                actions_left -= 1
                _state_set("last_comment_ts", now_ts)
                _state_set("last_thread_reply_ts", now_ts)
                last_thread_reply_ts = now_ts

                # Mark done; remove pending
                redis_client.srem(pending_set_key, cid)
                redis_client.sadd(done_set_key, cid)
                continue

            # Enforce comment cooldown (live setting)
            if not _comment_cooldown_ok():
                redis_client.srem(pending_set_key, cid)
                return

            st_r, bd_r, hd_r = client.comment(str(post_id), reply, parent_id=cid)
            if st_r == 429:
                _handle_rate_limit(st_r, hd_r, bd_r)
                redis_client.srem(pending_set_key, cid)
                return

            if st_r and isinstance(bd_r, dict) and bd_r.get("success") is True:
                new_comment_id = _extract_created_id(bd_r)
                _track_my_comment(str(post_id), new_comment_id)
                _bump_stat("thread_replies_created", 1)
                _log_event({
                    "ts": now_ts,
                    "type": "thread_reply",
                    "post_id": str(post_id),
                    "reply_to_comment_id": cid,
                    "created_comment_id": new_comment_id,
                    "content": reply,
                    "url": _post_url(str(post_id)),
                })

                _state_set("last_comment_ts", now_ts)
                _state_set("last_thread_reply_ts", now_ts)
                last_thread_reply_ts = now_ts
                actions_left -= 1

                # Mark done; remove pending
                redis_client.srem(pending_set_key, cid)
                redis_client.sadd(done_set_key, cid)
            else:
                _log_event({
                    "ts": now_ts,
                    "type": "thread_reply_failed",
                    "post_id": str(post_id),
                    "reply_to_comment_id": cid,
                    "status": st_r,
                    "body": bd_r,
                })
                redis_client.srem(pending_set_key, cid)
                try:
                    redis_client.hincrby(fail_hash_key, cid, 1)
                except Exception:
                    pass

# -------------------- Feed engagement --------------------
async def _engage_with_feed(llm, client: MoltbookClient):
    if not _mode_allows_writes():
        return

    allow_comments = _get_bool("allow_comments", True)
    allow_votes = _get_bool("allow_votes", True)
    if not allow_comments and not allow_votes:
        return

    if _in_cooldown():
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
    if actions_left <= 0:
        return

    for post in items:
        if actions_left <= 0:
            break
        if not isinstance(post, dict):
            continue

        post_id = post.get("id") or post.get("post_id")
        if not post_id:
            continue

        # If we already fully processed this post, skip
        if _seen_has(str(post_id)):
            continue

        # We'll only mark "seen" AFTER we either:
        # - successfully acted, OR
        # - intentionally decided to skip acting on it (e.g. random choice says no)
        marked_seen = False

        did_action = False

        # Voting: 25% chance
        if allow_votes and actions_left > 0 and random.random() < 0.25:
            if dry_run:
                _log_event({"ts": _now_ts(), "type": "dry_run_vote", "post_id": post_id, "vote": "upvote"})
                actions_left -= 1
                did_action = True
                _seen_add(str(post_id))
                marked_seen = True
            else:
                st, bd, hd = client.upvote(str(post_id))
                if st == 429:
                    _handle_rate_limit(st, hd, bd)
                    return
                if st and isinstance(bd, dict) and bd.get("success") is True:
                    _bump_stat("votes_cast", 1)
                    _log_event({"ts": _now_ts(), "type": "vote", "post_id": post_id, "vote": "upvote", "url": _post_url(str(post_id))})
                    actions_left -= 1
                    did_action = True
                    _seen_add(str(post_id))
                    marked_seen = True

        # Comments: 35% chance (if we didn't vote and still have actions)
        if (not did_action) and allow_comments and actions_left > 0 and random.random() < 0.35:
            comment = await _draft_comment(llm, post, reply_max_chars)
            if comment:
                if dry_run:
                    _log_event({"ts": _now_ts(), "type": "dry_run_comment", "post_id": post_id, "content": comment})
                    actions_left -= 1
                    _state_set("last_comment_ts", _now_ts())
                    did_action = True
                    _seen_add(str(post_id))
                    marked_seen = True
                else:
                    if not _comment_cooldown_ok():
                        return
                    st, bd, hd = client.comment(str(post_id), comment)
                    if st == 429:
                        _handle_rate_limit(st, hd, bd)
                        return
                    if st and isinstance(bd, dict) and bd.get("success") is True:
                        created_comment_id = _extract_created_id(bd)
                        _track_my_comment(str(post_id), created_comment_id)
                        _bump_stat("comments_created", 1)
                        _log_event({
                            "ts": _now_ts(),
                            "type": "comment",
                            "post_id": post_id,
                            "comment_id": created_comment_id,
                            "content": comment,
                            "url": _post_url(str(post_id)),
                        })
                        actions_left -= 1
                        _state_set("last_comment_ts", _now_ts())
                        did_action = True
                        _seen_add(str(post_id))
                        marked_seen = True

        # If we intentionally didn't act (random skip), mark seen to avoid reprocessing forever
        if not did_action and not marked_seen:
            _seen_add(str(post_id))

# -------------------- DM polling --------------------
async def _poll_dms(client: MoltbookClient):
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

def _derive_thread_interval(dm_interval: int) -> int:
    # If user set an explicit value, use it.
    explicit = _get_int("thread_check_interval_seconds", DEFAULT_THREAD_CHECK_INTERVAL_SECONDS)
    if explicit and explicit > 0:
        return max(30, explicit)

    # Auto-derive: roughly align scanning with reply pacing
    cd = max(0, _get_int("thread_reply_cooldown_seconds", DEFAULT_THREAD_REPLY_COOLDOWN_SECONDS))
    base = cd if cd > 0 else 60
    # Don't scan faster than DMs; don't scan slower than 3 minutes by default
    return max(30, min(180, max(base, dm_interval)))

async def _run_loop():
    # independent schedules
    next_status_ts = 0
    next_dm_ts = 0
    next_feed_ts = 0
    next_thread_ts = 0

    while not _stop_event.is_set():
        cycle_start = time.time()

        api_key = _get_str("api_key", "")
        if not api_key:
            logger.warning("[Moltbook] No api_key configured yet.")
            await asyncio.sleep(10)
            continue

        client = MoltbookClient(api_key)
        dry_run = _get_bool("dry_run", True)

        # read intervals each cycle (so changes apply without restart)
        status_interval = max(30, _get_int("status_check_interval_seconds", DEFAULT_STATUS_CHECK_INTERVAL_SECONDS))
        dm_interval = max(15, _get_int("dm_check_interval_seconds", DEFAULT_DM_CHECK_INTERVAL_SECONDS))
        feed_interval = max(30, _get_int("check_interval_seconds", DEFAULT_CHECK_INTERVAL_SECONDS))
        thread_interval = _derive_thread_interval(dm_interval)

        now = _now_ts()
        llm = None
        claimed = False

        async def do_status():
            nonlocal claimed, llm
            st, bd, hd = client.status()
            if st == 429:
                _handle_rate_limit(st, hd, bd)
                return

            if isinstance(bd, dict) and bd.get("success") is True:
                _bump_stat("status_ok", 1)
                _refresh_agent_identity_from_status(bd)

                try:
                    data = bd.get("data") or bd
                    agent = data.get("agent") or data
                    if isinstance(agent, dict):
                        claimed = bool(agent.get("is_claimed"))
                except Exception:
                    claimed = False

                # One-time posts after claim (autopost mode only).
                if claimed and _mode() == "autopost":
                    llm = llm or get_llm_client_from_env()
                    await _maybe_home_hello_post(llm, client, dry_run, claimed)

                if _should_intro_post(bd) and _mode() == "autopost":
                    llm = llm or get_llm_client_from_env()
                    await _maybe_intro_post(llm, client, dry_run)

        async def do_dm():
            nonlocal llm, claimed
            # always store DMs (even in read_only)
            await _poll_dms(client)

            # reply only if mode allows writes
            if not _mode_allows_writes():
                return
            if not claimed:
                return
            llm = llm or get_llm_client_from_env()
            await _maybe_reply_to_dms(llm, client, dry_run, claimed)

        async def do_threads():
            nonlocal llm, claimed
            if not _mode_allows_writes():
                return
            if not claimed:
                return
            llm = llm or get_llm_client_from_env()
            await _maybe_reply_to_my_threads(llm, client, dry_run, claimed)

        async def do_feed():
            nonlocal llm
            if not _mode_allows_writes():
                return
            llm = llm or get_llm_client_from_env()
            await _engage_with_feed(llm, client)
            if _mode() == "autopost":
                await _maybe_self_post(llm, client, dry_run)

        # Status when due (or first run)
        if next_status_ts == 0 or now >= next_status_ts:
            try:
                await asyncio.wait_for(do_status(), timeout=150)
            except Exception as e:
                logger.exception(f"[Moltbook] Status error: {e}")
            next_status_ts = now + status_interval

        # DM when due
        now = _now_ts()
        if next_dm_ts == 0 or now >= next_dm_ts:
            if not _in_cooldown():
                try:
                    await asyncio.wait_for(do_dm(), timeout=240)
                except Exception as e:
                    logger.exception(f"[Moltbook] DM cycle error: {e}")
            next_dm_ts = now + dm_interval

        # Threads when due
        now = _now_ts()
        if next_thread_ts == 0 or now >= next_thread_ts:
            if not _in_cooldown():
                try:
                    await asyncio.wait_for(do_threads(), timeout=240)
                except Exception as e:
                    logger.exception(f"[Moltbook] Thread cycle error: {e}")
            next_thread_ts = now + thread_interval

        # Feed when due
        now = _now_ts()
        if next_feed_ts == 0 or now >= next_feed_ts:
            if not _in_cooldown():
                try:
                    await asyncio.wait_for(do_feed(), timeout=300)
                except Exception as e:
                    logger.exception(f"[Moltbook] Feed cycle error: {e}")
            next_feed_ts = now + feed_interval

        # Cycle watchdog
        elapsed = time.time() - cycle_start
        if elapsed > 300:
            _log_event({"ts": _now_ts(), "type": "cycle_slow", "elapsed_seconds": round(elapsed, 2)})

        # Sleep until next due task
        now = _now_ts()
        next_due = min(
            next_status_ts or (now + 60),
            next_dm_ts or (now + 60),
            next_thread_ts or (now + 60),
            next_feed_ts or (now + 60),
        )
        sleep_for = max(5, min(60, int(next_due - now)))
        await asyncio.sleep(sleep_for)

def run(stop_event: Optional[threading.Event] = None):
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