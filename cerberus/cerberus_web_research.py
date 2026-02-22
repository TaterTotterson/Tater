import re
import urllib.parse
from typing import Any, Callable, Dict, List, Optional


def web_research_url_key(url: Any) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urllib.parse.urlparse(raw)
    except Exception:
        return raw
    scheme = (parsed.scheme or "https").lower()
    host = (parsed.netloc or "").lower()
    path = parsed.path or "/"
    if path.endswith("/") and path != "/":
        path = path[:-1]
    return urllib.parse.urlunparse((scheme, host, path, "", parsed.query or "", ""))


def extract_web_search_candidates(
    payload: Optional[Dict[str, Any]],
    *,
    max_candidates: int,
    default_max_candidates: int,
    web_research_url_key_fn: Callable[[Any], str],
) -> List[Dict[str, str]]:
    source = payload if isinstance(payload, dict) else {}
    rows = source.get("results")
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    max_items = max(1, int(max_candidates or default_max_candidates))
    for row in rows:
        if not isinstance(row, dict):
            continue
        url = str(row.get("url") or row.get("link") or "").strip()
        if not url:
            continue
        url_key = web_research_url_key_fn(url)
        if not url_key or url_key in seen:
            continue
        seen.add(url_key)
        out.append(
            {
                "url": url,
                "url_key": url_key,
                "title": str(row.get("title") or "").strip(),
                "snippet": str(row.get("snippet") or "").strip(),
            }
        )
        if len(out) >= max_items:
            break
    return out


def next_web_research_tool_call(
    *,
    candidates: List[Dict[str, str]],
    seen_urls: set[str],
    web_research_url_key_fn: Callable[[Any], str],
) -> Optional[Dict[str, Any]]:
    for item in candidates:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        url_key = str(item.get("url_key") or "").strip() or web_research_url_key_fn(url)
        if not url or not url_key:
            continue
        if url_key in seen_urls:
            continue
        seen_urls.add(url_key)
        return {"function": "inspect_webpage", "arguments": {"url": url}}
    return None


def web_inspection_is_sufficient(
    payload: Optional[Dict[str, Any]],
    *,
    canonical_tool_name_fn: Callable[[str], str],
    min_preview_chars: int,
    min_preview_words: int,
) -> bool:
    source = payload if isinstance(payload, dict) else {}
    if not bool(source.get("ok")):
        return False
    tool = canonical_tool_name_fn(source.get("tool"))
    if tool == "inspect_webpage":
        title = str(source.get("title") or "").strip()
        description = str(source.get("description") or "").strip()
        preview = str(source.get("text_preview") or "").strip()
        preview_words = re.findall(r"[a-z0-9]{3,}", preview.lower())
        if len(description) >= 80:
            return True
        if len(preview) >= min_preview_chars:
            return True
        if len(preview_words) >= min_preview_words:
            return True
        if len(title) >= 8 and len(preview_words) >= 30:
            return True
        return False
    if tool == "read_url":
        content = str(source.get("content") or "").strip()
        if not content:
            return False
        preview = content[:5000]
        preview_words = re.findall(r"[a-z0-9]{3,}", preview.lower())
        return len(preview) >= 900 or len(preview_words) >= 120
    return False
