import json
from typing import Any, Dict, List, Optional


def action_success(
    *,
    facts: Dict[str, Any],
    say_hint: str,
    suggested_followups: Optional[List[str]] = None,
    artifacts: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    return {
        "ok": True,
        "facts": facts or {},
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
        out.setdefault("facts", {})
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
            facts={"artifact_count": 1},
            say_hint="Share the generated file with a brief factual caption.",
            artifacts=[raw],
        )

    if isinstance(raw, list):
        if len(raw) == 1 and isinstance(raw[0], dict) and "ok" in raw[0]:
            return _sanitize_contract(raw[0])

        artifacts = [x for x in raw if _is_artifact(x)]
        text_parts = [_coerce_text(x) for x in raw if not _is_artifact(x)]
        facts: Dict[str, Any] = {
            "artifact_count": len(artifacts),
            "text_count": len([x for x in text_parts if x]),
        }
        if text_parts:
            facts["messages"] = [x for x in text_parts if x][:5]
        return action_success(
            facts=facts,
            say_hint="Summarize what was completed using only these facts.",
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
            facts={"message": text},
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
        facts={"message": text},
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


def _simple_fact_lines(facts: Dict[str, Any]) -> str:
    parts: List[str] = []
    for key, value in (facts or {}).items():
        if key in {"message"} and isinstance(value, str):
            parts.append(value.strip())
            continue
        if isinstance(value, (str, int, float, bool)):
            parts.append(f"{key.replace('_', ' ')}: {value}")
        elif isinstance(value, list) and value:
            parts.append(f"{key.replace('_', ' ')}: {', '.join(str(x) for x in value[:4])}")
    return "\n".join(parts).strip()


def _join_messages(messages: Any) -> str:
    if not isinstance(messages, list):
        return ""
    cleaned = [str(m).strip() for m in messages if isinstance(m, str) and str(m).strip()]
    return "\n".join(cleaned).strip()


async def narrate_result(
    result: Dict[str, Any],
    *,
    llm_client: Any = None,
    platform: str = "webui",
) -> str:
    result = _sanitize_contract(result) if "ok" in result else normalize_plugin_result(result)
    if not result.get("ok"):
        err = result.get("error") or {}
        if err.get("code") == "unsupported_platform":
            msg = str(err.get("message") or "This tool is not available on this platform.")
            available_on = result.get("available_on")
            if isinstance(available_on, list) and available_on:
                msg += f" Available on: {', '.join(str(x) for x in available_on)}."
            needs = result_needs_questions(result)
            if needs:
                msg += " " + " ".join(needs[:3])
            return msg.strip()

    facts = result.get("facts") if isinstance(result.get("facts"), dict) else {}
    direct_message = facts.get("message") if isinstance(facts.get("message"), str) else ""
    if direct_message and ("http://" in direct_message or "https://" in direct_message or "](" in direct_message):
        return direct_message.strip()

    messages_text = _join_messages(facts.get("messages"))
    if messages_text:
        if platform in {"irc", "homeassistant", "homekit", "xbmc"}:
            messages_text = messages_text.encode("ascii", "ignore").decode()
        return messages_text.strip()

    if llm_client is not None:
        try:
            plain = platform in {"irc", "homeassistant", "homekit", "xbmc"}
            style_rule = "plain ASCII text with no markdown" if plain else "short markdown-friendly text"
            payload = json.dumps(result_for_llm(result), ensure_ascii=False)
            prompt = (
                "You are the narration layer for a tool result.\n"
                "Use ONLY the provided JSON facts. Never invent details.\n"
                "If ok=false, do not imply success.\n"
                "If there are needs, ask them clearly.\n"
                f"Output should be {style_rule}.\n"
                "Return only the final user-facing message."
            )
            response = await llm_client.chat(
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": payload},
                ],
                max_tokens=220,
                temperature=0.2,
            )
            text = (response.get("message", {}) or {}).get("content", "").strip()
            if text:
                return text
        except Exception:
            pass

    if not result.get("ok"):
        err = result.get("error") or {}
        msg = str(err.get("message") or "That tool failed.")
        available_on = result.get("available_on")
        if err.get("code") == "unsupported_platform" and isinstance(available_on, list) and available_on:
            msg += f" Available on: {', '.join(str(x) for x in available_on)}."
        needs = result_needs_questions(result)
        if needs:
            msg += " " + " ".join(needs[:3])
        return msg.strip()

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
        return "\n".join([x for x in lines if x]).strip() or "I found results."

    body = _simple_fact_lines(facts)
    if body:
        return body
    return "Done."


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
        "facts": result.get("facts", {}),
    }
