import json
import re
from html import unescape
from typing import Any, Dict, List, Optional


def _compact_text(value: Any, *, max_chars: int) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = " ".join(text.split())
    if len(text) > max_chars:
        text = text[:max_chars].rstrip() + "..."
    return text


def action_success(
    *,
    facts: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    say_hint: str = "",
    summary_for_user: str = "",
    flair: str = "",
    suggested_followups: Optional[List[str]] = None,
    artifacts: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    facts_payload = dict(facts) if isinstance(facts, dict) else {}
    data_payload = dict(data) if isinstance(data, dict) else {}
    return {
        "ok": True,
        "facts": facts_payload,
        "data": data_payload,
        "summary_for_user": _compact_text(summary_for_user, max_chars=350),
        "flair": _compact_text(flair, max_chars=240),
        "say_hint": (say_hint or "").strip(),
        "suggested_followups": suggested_followups or [],
        "artifacts": artifacts or [],
    }


def action_failure(
    *,
    code: str,
    message: str,
    diagnosis: Optional[Dict[str, str]] = None,
    needs: Optional[List[str]] = None,
    say_hint: str = "",
    available_on: Optional[List[str]] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "ok": False,
        "error": {
            "code": (code or "plugin_error").strip(),
            "message": (message or "The tool failed.").strip(),
        },
        "diagnosis": diagnosis or {},
        "needs": needs or [],
        "say_hint": (say_hint or "").strip(),
    }
    if available_on is not None:
        out["available_on"] = available_on
    return out


def research_success(
    *,
    answer: str,
    highlights: Optional[List[str]] = None,
    sources: Optional[List[Dict[str, Any]]] = None,
    say_hint: str = "",
) -> Dict[str, Any]:
    return {
        "ok": True,
        "result_type": "research",
        "answer": (answer or "").strip(),
        "highlights": highlights or [],
        "sources": sources or [],
        "say_hint": (say_hint or "").strip(),
    }


def _is_artifact(payload: Any) -> bool:
    return isinstance(payload, dict) and str(payload.get("type") or "").strip().lower() in {
        "image",
        "audio",
        "video",
        "file",
    }


def _coerce_text(payload: Any) -> str:
    if payload is None:
        return ""
    if isinstance(payload, str):
        return payload.strip()
    if isinstance(payload, dict):
        for key in ("answer", "message", "summary", "text", "content"):
            if isinstance(payload.get(key), str) and payload.get(key).strip():
                return payload[key].strip()
        try:
            return json.dumps(payload, ensure_ascii=False)
        except Exception:
            return str(payload)
    if isinstance(payload, list):
        chunks = [_coerce_text(x) for x in payload]
        return "\n".join([c for c in chunks if c]).strip()
    return str(payload).strip()


def _sanitize_contract(result: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(result)
    out["ok"] = bool(out.get("ok"))
    if out["ok"]:
        data = out.get("data") if isinstance(out.get("data"), dict) else {}
        facts = out.get("facts") if isinstance(out.get("facts"), dict) else {}
        out["facts"] = facts
        out["data"] = data
        summary = out.get("summary_for_user")
        if not isinstance(summary, str) or not summary.strip():
            summary = out.get("summary") if isinstance(out.get("summary"), str) else ""
        out["summary_for_user"] = _compact_text(summary, max_chars=350)
        out["flair"] = _compact_text(out.get("flair"), max_chars=240)
        out.setdefault("say_hint", "")
        out.setdefault("suggested_followups", [])
        out.setdefault("artifacts", [])
    else:
        err = out.get("error")
        if not isinstance(err, dict):
            err = {}
        out["error"] = {
            "code": str(err.get("code") or "plugin_error"),
            "message": str(err.get("message") or "The tool failed."),
        }
        out.setdefault("diagnosis", {})
        out.setdefault("needs", [])
        out.setdefault("say_hint", "")
    return out


def normalize_plugin_result(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict) and "ok" in raw:
        return _sanitize_contract(raw)

    if isinstance(raw, dict) and raw.get("result_type") == "research":
        out = dict(raw)
        out.setdefault("ok", True)
        out.setdefault("answer", _coerce_text(raw.get("answer")))
        out.setdefault("highlights", [])
        out.setdefault("sources", [])
        out.setdefault("say_hint", "")
        return _sanitize_contract(out)

    if _is_artifact(raw):
        return action_success(
            data={"artifact_count": 1},
            summary_for_user="Generated 1 artifact.",
            say_hint="Share the generated file with a brief factual caption.",
            artifacts=[raw],
        )

    if isinstance(raw, list):
        if len(raw) == 1 and isinstance(raw[0], dict) and "ok" in raw[0]:
            return _sanitize_contract(raw[0])

        artifacts = [x for x in raw if _is_artifact(x)]
        text_parts = [_coerce_text(x) for x in raw if not _is_artifact(x)]
        data: Dict[str, Any] = {
            "artifact_count": len(artifacts),
            "text_count": len([x for x in text_parts if x]),
        }
        if text_parts:
            data["messages"] = [x for x in text_parts if x][:5]
        return action_success(
            data=data,
            summary_for_user=_compact_text(text_parts[0], max_chars=350) if text_parts else "",
            say_hint="Summarize what was completed using only the returned data.",
            artifacts=artifacts,
        )

    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return action_failure(
                code="empty_result",
                message="The tool did not return any output.",
                say_hint="Explain that the tool returned no output and ask whether to retry.",
            )
        return action_success(
            data={"message": text},
            summary_for_user=_compact_text(text, max_chars=350),
            say_hint="Provide this result directly without adding unverified details.",
        )

    if raw is None:
        return action_failure(
            code="empty_result",
            message="The tool did not return any output.",
            say_hint="Explain that no output was returned and ask whether to retry.",
        )

    text = _coerce_text(raw)
    return action_success(
        data={"message": text},
        summary_for_user=_compact_text(text, max_chars=350),
        say_hint="Provide this result directly and keep it factual.",
    )


def is_research_result(result: Dict[str, Any]) -> bool:
    return bool(result.get("ok")) and result.get("result_type") == "research"


def result_needs_questions(result: Dict[str, Any]) -> List[str]:
    needs = result.get("needs")
    if isinstance(needs, list):
        return [str(x).strip() for x in needs if str(x).strip()]
    return []


def result_artifacts(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    artifacts = result.get("artifacts")
    if isinstance(artifacts, list):
        return [a for a in artifacts if _is_artifact(a)]
    return []


def result_for_llm(result: Dict[str, Any]) -> Dict[str, Any]:
    safe = dict(result)
    if "summary_for_user" in safe:
        safe["summary_for_user"] = _compact_text(safe.get("summary_for_user"), max_chars=350)
    if "flair" in safe:
        safe["flair"] = _compact_text(safe.get("flair"), max_chars=240)
    if "artifacts" in safe and isinstance(safe["artifacts"], list):
        compact_artifacts = []
        for item in safe["artifacts"]:
            if not isinstance(item, dict):
                continue
            compact = {
                "type": item.get("type"),
                "name": item.get("name"),
                "mimetype": item.get("mimetype"),
                "size": item.get("size"),
            }
            compact_artifacts.append(compact)
        safe["artifacts"] = compact_artifacts
    return safe


def _join_messages(messages: Any) -> str:
    if not isinstance(messages, list):
        return ""
    cleaned = [str(m).strip() for m in messages if isinstance(m, str) and str(m).strip()]
    return "\n".join(cleaned).strip()


def _trim_text(value: Any, *, max_chars: int = 360) -> str:
    return _compact_text(value, max_chars=max_chars)


def _looks_like_html(text: str, content_type: str = "") -> bool:
    s = str(text or "").strip().lower()
    ctype = str(content_type or "").strip().lower()
    if "text/html" in ctype or "application/xhtml" in ctype:
        return True
    if s.startswith("<!doctype html") or "<html" in s[:500]:
        return True
    if len(re.findall(r"<[a-z][^>]*>", s[:2000])) >= 5:
        return True
    return False


def _extract_html_title(text: str) -> str:
    m = re.search(r"<title[^>]*>(.*?)</title>", str(text or ""), flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return ""
    return _trim_text(unescape(m.group(1)), max_chars=160)


def _extract_html_description(text: str) -> str:
    s = str(text or "")
    patterns = [
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
    ]
    for pattern in patterns:
        m = re.search(pattern, s, flags=re.IGNORECASE | re.DOTALL)
        if m:
            return _trim_text(unescape(m.group(1)), max_chars=220)
    return ""


def _html_to_plain_text(text: str, *, max_chars: int = 360) -> str:
    s = str(text or "")
    if not s:
        return ""
    s = re.sub(r"<(script|style|noscript)[^>]*>.*?</\\1>", " ", s, flags=re.IGNORECASE | re.DOTALL)
    s = re.sub(r"<[^>]+>", " ", s)
    s = unescape(s)
    return _trim_text(s, max_chars=max_chars)


def _is_low_information_narration(text: str) -> bool:
    s = str(text or "").strip().lower()
    if not s:
        return True
    s = s.strip(" .!?")
    return s in {
        "done",
        "ok",
        "okay",
        "completed",
        "complete",
        "finished",
        "success",
        "successful",
    }


def _tool_result_summary(result: Dict[str, Any]) -> str:
    summary_hint = _trim_text(result.get("summary_for_user"), max_chars=420)
    if summary_hint and not _is_low_information_narration(summary_hint):
        return summary_hint

    tool = str(result.get("tool") or "").strip().lower()

    # High-value deterministic summary for webpage inspection requests.
    if tool == "inspect_webpage":
        title = _trim_text(result.get("title"), max_chars=140)
        description = _trim_text(result.get("description"), max_chars=220)
        preview = _trim_text(result.get("text_preview"), max_chars=320)
        best_image = _trim_text(result.get("best_image_url"), max_chars=260)
        lines: List[str] = []
        if title and description:
            lines.append(f"{title}: {description}")
        elif title:
            lines.append(f"Title: {title}")
        elif description:
            lines.append(description)
        if preview and preview.lower() != (description or "").lower():
            lines.append(preview)
        if best_image:
            lines.append(f"Likely main image: {best_image}")
        return "\n".join(lines[:3]).strip()

    if tool == "read_url":
        raw_content = str(result.get("content") or "")
        ctype = str(result.get("content_type") or "")
        if _looks_like_html(raw_content, ctype):
            title = _extract_html_title(raw_content)
            description = _extract_html_description(raw_content)
            plain = _html_to_plain_text(raw_content, max_chars=420)
            if title and description:
                return f"{title}: {description}"
            if title and plain:
                return _trim_text(f"{title}. {plain}", max_chars=420)
            if description:
                return description
            if plain:
                return plain
        content = _trim_text(raw_content, max_chars=420)
        if content:
            return content

    if tool == "download_file":
        path = _trim_text(result.get("path"), max_chars=260)
        name = _trim_text(result.get("name"), max_chars=120)
        if path and name:
            return f"Downloaded {name} to {path}."
        if path:
            return f"Downloaded file to {path}."

    if tool == "send_message":
        queued = _trim_text(result.get("result"), max_chars=220)
        if queued:
            return queued

    for key in ("answer", "description", "summary", "text", "message"):
        value = result.get(key)
        if isinstance(value, str) and value.strip():
            candidate = _trim_text(value, max_chars=420)
            if candidate and not _is_low_information_narration(candidate):
                return candidate
    return ""


async def narrate_result(
    result: Dict[str, Any],
    *,
    llm_client: Any = None,
    platform: str = "webui",
) -> str:
    result = _sanitize_contract(result) if "ok" in result else normalize_plugin_result(result)
    _ = llm_client  # Intentionally unused: narration is deterministic-only under Cerberus.

    ascii_only = platform in {"irc", "homeassistant", "homekit", "xbmc"}

    def _safe_text(text: str) -> str:
        clean = str(text or "").strip()
        if ascii_only:
            clean = clean.encode("ascii", "ignore").decode()
        return clean.strip()

    # 1) summary_for_user
    summary_hint = _trim_text(result.get("summary_for_user"), max_chars=420)
    if summary_hint and not _is_low_information_narration(summary_hint):
        summary_hint = _safe_text(summary_hint)
        flair = _trim_text(result.get("flair"), max_chars=240)
        if flair and not ascii_only:
            return f"{summary_hint}\n{flair}".strip()
        return summary_hint.strip()

    # 2) facts.message / facts.messages
    facts = result.get("facts") if isinstance(result.get("facts"), dict) else {}
    direct_message = facts.get("message") if isinstance(facts.get("message"), str) else ""
    if direct_message and not _is_low_information_narration(direct_message):
        return _safe_text(direct_message)

    messages_text = _join_messages(facts.get("messages"))
    if messages_text and not _is_low_information_narration(messages_text):
        return _safe_text(messages_text)

    # 3) deterministic tool summary
    deterministic_summary = _tool_result_summary(result)
    if deterministic_summary and not _is_low_information_narration(deterministic_summary):
        return _safe_text(deterministic_summary)

    # 4) research formatting
    if is_research_result(result):
        answer = str(result.get("answer") or "").strip()
        highlights = result.get("highlights") or []
        sources = result.get("sources") or []
        lines = [answer] if answer else []
        if isinstance(highlights, list) and highlights:
            lines.extend([f"- {str(h).strip()}" for h in highlights[:3] if str(h).strip()])
        if isinstance(sources, list) and sources:
            src_bits = []
            for src in sources[:3]:
                if not isinstance(src, dict):
                    continue
                title = str(src.get("title") or "").strip()
                url = str(src.get("url") or "").strip()
                if title and url:
                    src_bits.append(f"{title} ({url})")
                elif url:
                    src_bits.append(url)
            if src_bits:
                lines.append("Sources: " + "; ".join(src_bits))
        research_text = "\n".join([x for x in lines if x]).strip()
        if research_text and not _is_low_information_narration(research_text):
            return _safe_text(research_text)

    # 5) error formatting
    if not result.get("ok"):
        err = result.get("error") or {}
        msg = str(err.get("message") or "That tool failed.").strip()
        available_on = result.get("available_on")
        if err.get("code") == "unsupported_platform" and isinstance(available_on, list) and available_on:
            msg += f" Available on: {', '.join(str(x) for x in available_on)}."
        needs = result_needs_questions(result)
        if needs:
            msg += " " + " ".join(needs[:3])
        return _safe_text(msg)

    # 6) final fallback
    if result.get("ok"):
        return "Completed."
    err = result.get("error") or {}
    msg = str(err.get("message") or "That tool failed.").strip()
    needs = result_needs_questions(result)
    if needs:
        msg += " " + " ".join(needs[:3])
    return _safe_text(msg)


def redis_truth_payload(result: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(result, dict):
        result = normalize_plugin_result(result)

    if not result.get("ok"):
        return {
            "ok": False,
            "error": result.get("error", {}),
            "needs": result_needs_questions(result),
            "diagnosis": result.get("diagnosis", {}),
        }

    if is_research_result(result):
        return {
            "ok": True,
            "result_type": "research",
            "answer": str(result.get("answer") or ""),
            "sources": [
                {
                    "title": (s.get("title") if isinstance(s, dict) else None),
                    "url": (s.get("url") if isinstance(s, dict) else None),
                }
                for s in (result.get("sources") or [])[:3]
            ],
        }

    return {
        "ok": True,
        "result_type": "action",
        "data": result.get("data", {}) if isinstance(result.get("data"), dict) else {},
        "facts": result.get("facts", {}) if isinstance(result.get("facts"), dict) else {},
    }
