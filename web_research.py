import json
from typing import Any, Dict, List

from kernel_tools import inspect_webpage, search_web
from verba_result import action_failure, action_success


def _messages_content(resp: Any) -> str:
    if not isinstance(resp, dict):
        return ""
    message = resp.get("message")
    if isinstance(message, dict):
        return str(message.get("content") or "").strip()
    return ""


def _strict_json(text: str) -> Dict[str, Any]:
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = raw.strip("`").strip()
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _clip(value: Any, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _int_bound(value: Any, *, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(min_value, min(max_value, parsed))


def _page_for_llm(page: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "url": str(page.get("url") or ""),
        "title": str(page.get("title") or ""),
        "description": _clip(page.get("description"), 900),
        "text_preview": _clip(page.get("text_preview"), 1200),
        "content": _clip(page.get("content"), 7000),
        "ok": bool(page.get("ok")),
        "error": str(page.get("error") or ""),
    }


async def research_web(
    *,
    query: Any,
    question: Any = "",
    llm_client: Any = None,
    max_results: int = 5,
    max_pages: int = 3,
    site: Any = None,
    safe: str = "active",
    country: Any = None,
    language: Any = None,
    platform: str = "",
    origin: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    del platform, origin
    query_text = str(query or "").strip()
    question_text = str(question or "").strip() or query_text
    if not query_text:
        return action_failure(code="research_web_missing_query", message="research_web needs a query.")

    search_count = _int_bound(max_results, default=5, min_value=1, max_value=10)
    page_limit = _int_bound(max_pages, default=3, min_value=1, max_value=search_count)
    search_result = search_web(
        query_text,
        num_results=search_count,
        site=site,
        safe=safe,
        country=country,
        language=language,
        timeout_sec=15,
    )
    if not bool(search_result.get("ok")):
        return action_failure(
            code="research_web_search_failed",
            message=str(search_result.get("error") or "Web search failed."),
        )

    results = list(search_result.get("results") or [])
    if not results:
        return action_failure(
            code="research_web_no_results",
            message=f"No web search results found for: {query_text}",
        )

    inspected: List[Dict[str, Any]] = []
    answer = ""
    enough = False
    reason = ""
    missing = ""
    system_prompt = (
        "You evaluate web research for Tater.\n"
        "Given the user's question and inspected pages so far, decide if there is enough information to answer.\n"
        "Return exactly one strict JSON object:\n"
        "{\"enough\":true,\"answer\":\"...\",\"reason\":\"...\"}\n"
        "{\"enough\":false,\"reason\":\"...\",\"missing\":\"...\"}\n"
        "Rules:\n"
        "- Prefer factual answers grounded in inspected page content.\n"
        "- If the page content is thin, blocked, irrelevant, or only a search snippet, set enough=false.\n"
        "- Include the source URLs used in the answer text when useful.\n"
    )

    for result in results[:page_limit]:
        url = str(result.get("url") or "").strip()
        if not url:
            continue
        inspected_page = inspect_webpage(url, timeout_sec=20, max_links=8, max_images=2)
        compact_page = {
            "search_title": str(result.get("title") or ""),
            "search_snippet": str(result.get("snippet") or ""),
            "search_url": url,
            **_page_for_llm(inspected_page),
        }
        inspected.append(compact_page)

        if llm_client is None or not hasattr(llm_client, "chat"):
            if bool(inspected_page.get("ok")) and str(inspected_page.get("content") or inspected_page.get("text_preview") or "").strip():
                enough = True
                answer = _clip(str(inspected_page.get("content") or inspected_page.get("text_preview") or ""), 3000)
                reason = "First readable page returned content; no LLM evaluator was available."
                break
            continue

        payload = {
            "question": question_text,
            "query": query_text,
            "inspected_pages": inspected,
        }
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)},
                ],
                temperature=0.0,
            )
        except Exception as exc:
            reason = f"Research evaluator failed: {exc}"
            continue
        decision = _strict_json(_messages_content(resp))
        enough = bool(decision.get("enough"))
        reason = str(decision.get("reason") or "").strip()
        missing = str(decision.get("missing") or "").strip()
        if enough:
            answer = str(decision.get("answer") or "").strip()
            if not answer:
                enough = False
                missing = "Evaluator marked enough=true but returned no answer."
            else:
                break

    sources = [
        {
            "title": str(page.get("title") or page.get("search_title") or ""),
            "url": str(page.get("url") or page.get("search_url") or ""),
            "ok": bool(page.get("ok")),
        }
        for page in inspected
    ]
    if not answer and inspected:
        readable = [page for page in inspected if bool(page.get("ok"))]
        if readable:
            first = readable[0]
            answer = _clip(first.get("content") or first.get("text_preview") or "", 3000)
            reason = reason or "No single source was marked sufficient; returning best readable content."

    return action_success(
        facts={
            "query": query_text,
            "enough": enough,
            "inspected_pages": len(inspected),
            "reason": reason,
        },
        data={
            "query": query_text,
            "question": question_text,
            "enough": enough,
            "answer": answer,
            "reason": reason,
            "missing": missing,
            "search": search_result,
            "inspected_pages": inspected,
            "sources": sources,
        },
        summary_for_user=answer or reason or f"Inspected {len(inspected)} page(s), but no answer was found.",
        say_hint="Use the answer and cite source URLs briefly when relevant.",
    )
