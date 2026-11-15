"""Focused helpers extracted from the legacy shared runtime."""

import json
import re


def extract_json(text: str):
    """
    Extract the first valid JSON object or array from text.
    Strips code fences and tolerates extra prose around it.
    Works for both { ... } and [ ... ] blocks.
    """
    if not text:
        return None

    s = text.strip()

    # Remove ```json fences
    if s.startswith("```") and s.endswith("```"):
        s = re.sub(r"^```(?:json)?\s*|\s*```$", "", s, flags=re.MULTILINE).strip()

    # Try whole text first
    try:
        json.loads(s)
        return s
    except Exception:
        pass

    # Bracket scanning for either {...} or [...]
    stack = []
    start_idx = None
    for i, char in enumerate(s):
        if char in "{[":
            if not stack:
                start_idx = i
            stack.append(char)
        elif char in "}]":
            if stack:
                opening = stack.pop()
                if not stack and start_idx is not None:
                    candidate = s[start_idx:i+1]
                    try:
                        json.loads(candidate)
                        return candidate
                    except json.JSONDecodeError:
                        continue
    return None


def looks_like_tool_markup(text: str) -> bool:
    if not text:
        return False
    s = str(text)
    if "<|" in s and "|>" in s:
        return True
    if re.search(r"\bto=[A-Za-z0-9_.-]+\b", s) and ("commentary" in s or "message" in s):
        return True
    return False


def parse_function_json(response_text: str):
    _DECISION_PREFIX_RE = re.compile(
        r"^\s*(CONTINUE|RETRY|ASK[\s_-]*USER|FAIL|FINAL|FINAL[\s_-]*ANSWER|RETRY[\s_-]*TOOL|NEED[\s_-]*USER[\s_-]*INFO|ANSWER|NO_TOOL)\s*:\s*(.*)$",
        re.IGNORECASE | re.DOTALL,
    )
    _NON_TOOL_PREFIXES = {
        "continue",
        "retry",
        "ask_user",
        "fail",
        "final",
        "retry_tool",
        "final_answer",
        "need_user_info",
        "answer",
        "no_tool",
    }

    def _pick(obj):
        if isinstance(obj, dict):
            if "function" in obj and isinstance(obj["function"], str):
                return {"function": obj["function"], "arguments": obj.get("arguments", {}) or {}}
            if "tool" in obj and isinstance(obj["tool"], str):
                return {"function": obj["tool"], "arguments": obj.get("arguments", {}) or {}}
        return None

    if not response_text:
        return None

    s = str(response_text).strip()

    # Remove code fence markers anywhere to avoid treating ```json as a tool name.
    if "```" in s:
        s = re.sub(r"```(?:json)?", "", s, flags=re.IGNORECASE).strip()

    # ---------------------------------------------------------
    # Tool-call markup (Codex/OpenAI style) support:
    #   <|channel|>commentary to=repo_browser.get_verba_help <|message|>{"verba_id":"weather_forecast"}
    # ---------------------------------------------------------
    m_tool = re.search(r"to=([a-zA-Z0-9_.-]+).*?<\|message\|>(\{.*\})", s, re.DOTALL)
    if m_tool:
        tool_name = m_tool.group(1).strip()
        tool_name = tool_name.split(".")[-1] if tool_name else tool_name
        blob = m_tool.group(2).strip()
        try:
            args = json.loads(blob)
            if isinstance(args, dict):
                if tool_name == "get_verba_help":
                    if "verba_id" not in args and "name" in args:
                        args["verba_id"] = args.get("name")
                return {"function": tool_name, "arguments": args}
        except Exception:
            return {"function": tool_name, "arguments": {}}

    # strip code fences early so shorthand/prefix parsing still works
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.MULTILINE).strip()
    if s.endswith("```"):
        s = re.sub(r"\s*```$", "", s, flags=re.MULTILINE).strip()

    # Handle channel wrappers:
    #   <|channel|>final ANSWER<|message|>RETRY_TOOL: {...}
    if "<|message|>" in s:
        s = s.rsplit("<|message|>", 1)[-1].strip()

    # Handle Minos decision wrappers:
    #   RETRY_TOOL: {...}
    #   FINAL_ANSWER: ...
    decision_match = _DECISION_PREFIX_RE.match(s)
    if decision_match:
        decision_kind = str(decision_match.group(1) or "").strip().lower().replace(" ", "_").replace("-", "_")
        decision_body = str(decision_match.group(2) or "").strip()
        if decision_kind.startswith("retry"):
            s = decision_body
        else:
            # FINAL_ANSWER / NEED_USER_INFO / NO_TOOL are not tool calls.
            return None

    # Accept shorthand ONLY when it's the whole message: plugin_id{"arg":"..."}
    m = re.match(r'^([a-zA-Z0-9_]+)\s*(\{.*\})\s*$', s, re.DOTALL)
    if m:
        func = m.group(1)
        blob = m.group(2)
        try:
            args = json.loads(blob)
            if isinstance(args, dict):
                return {"function": func, "arguments": args}
        except Exception:
            pass

    try:
        response_json = json.loads(s)
    except json.JSONDecodeError:
        json_str = extract_json(s)
        if not json_str:
            json_str = None

        if json_str:
            prefix = s.split(json_str, 1)[0].strip()
        else:
            prefix = ""

        # Slightly stricter "possible func" match
        m2 = re.search(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*[:(]*\s*$', prefix)
        if json_str:
            try:
                args = json.loads(json_str)
                picked = _pick(args)
                if picked:
                    return picked
            except Exception:
                args = None

            if m2:
                possible_func = str(m2.group(1) or "").strip()
                if possible_func and possible_func.lower() not in _NON_TOOL_PREFIXES:
                    try:
                        args = json.loads(json_str)
                        if isinstance(args, dict):
                            return {"function": possible_func, "arguments": args}
                    except Exception:
                        pass

        if json_str:
            try:
                response_json = json.loads(json_str)
            except Exception:
                response_json = None
        else:
            response_json = None

    picked = _pick(response_json)
    if picked:
        return picked

    if isinstance(response_json, list):
        for item in response_json:
            picked = _pick(item)
            if picked:
                return picked

    # ---------------------------------------------------------
    # Relaxed fallback: tolerate invalid JSON for file/code tools
    # ---------------------------------------------------------
    def _extract_relaxed_string(text: str, key: str) -> str | None:
        pat = rf'"{re.escape(key)}"\s*:\s*"(.*)"\s*\}}\s*\}}\s*$'
        m = re.search(pat, text, flags=re.DOTALL)
        if m:
            return m.group(1)
        pat = rf"'{re.escape(key)}'\s*:\s*'(.*)'\s*\}}\s*\}}\s*$"
        m = re.search(pat, text, flags=re.DOTALL)
        if m:
            return m.group(1)
        return None

    def _extract_relaxed_scalar(text: str, key: str) -> str | None:
        pat = rf'"{re.escape(key)}"\s*:\s*"([^"]+)"'
        m = re.search(pat, text)
        if m:
            return m.group(1)
        pat = rf"'{re.escape(key)}'\s*:\s*'([^']+)'"
        m = re.search(pat, text)
        if m:
            return m.group(1)
        return None

    def _relaxed_tool_call(text: str) -> dict | None:
        fm = re.search(r'"function"\s*:\s*"([^"]+)"', text)
        if not fm:
            fm = re.search(r"'function'\s*:\s*'([^']+)'", text)
        if not fm:
            return None
        func = fm.group(1).strip()
        if not func:
            return None

        if func in {"write_file"}:
            path = _extract_relaxed_scalar(text, "path")
            content = _extract_relaxed_string(text, "content")
            if path and content is not None:
                return {"function": func, "arguments": {"path": path, "content": content}}
        return None

    relaxed = _relaxed_tool_call(s)
    if relaxed:
        return relaxed

    return None


TOOL_MARKUP_REPAIR_PROMPT = (
    "Formatting error: do not emit tool channel markup like <|channel|> or to=... . "
    "If you need to call a tool, respond with exactly ONE strict JSON object and nothing else: "
    "{\"function\":\"name\",\"arguments\":{...}}. "
    "Do not include extra prose, and do not output multiple tool calls. "
    "Otherwise respond with NO_TOOL."
)


TOOL_MARKUP_FAILURE_TEXT = "Sorry, I had trouble formatting a tool call. Please try again."
