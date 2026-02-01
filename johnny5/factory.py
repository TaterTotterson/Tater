import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from helpers import get_llm_client_from_env
from johnny5 import state


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_\-]+", "_", text.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug[:64] or "candidate"


def _extract_code_block(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"```(?:python)?\n(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if not m:
        return None
    return m.group(1).strip()


def _build_plugin_prompt(spec_text: str, base_code: Optional[str]) -> str:
    header = "You are generating a Tater ToolPlugin candidate."
    rules = (
        "- Output ONLY valid Python source code.\n"
        "- Define a ToolPlugin subclass and assign a module-level variable named `plugin`.\n"
        "- Include id/name, description, usage, platforms, required_settings.\n"
        "- Avoid os.system or subprocess unless explicitly required.\n"
        "- Provide a smoke_test() method that returns a short string on success.\n"
    )
    if base_code:
        return f"{header}\n\nRules:\n{rules}\n\nBase plugin code:\n{base_code}\n\nSpec:\n{spec_text}\n"
    return f"{header}\n\nRules:\n{rules}\n\nSpec:\n{spec_text}\n"


def _build_platform_prompt(spec_text: str, base_code: Optional[str]) -> str:
    header = "You are generating a Tater platform candidate."
    rules = (
        "- Output ONLY valid Python source code.\n"
        "- Provide PLATFORM_SETTINGS and a run(stop_event) entrypoint.\n"
        "- Use a stop_event for clean shutdown.\n"
        "- Avoid auto-start; rely on WebUI to start.\n"
    )
    if base_code:
        return f"{header}\n\nRules:\n{rules}\n\nBase platform code:\n{base_code}\n\nSpec:\n{spec_text}\n"
    return f"{header}\n\nRules:\n{rules}\n\nSpec:\n{spec_text}\n"


async def _generate_code(prompt: str) -> str:
    client = get_llm_client_from_env(use_johnny5=False)
    resp = await client.chat(
        messages=[
            {"role": "system", "content": "You write safe, concise Python for Tater."},
            {"role": "user", "content": prompt},
        ],
        timeout=90,
        meta={"platform": "johnny5", "convo_id": "johnny5:factory", "request_kind": "factory"},
    )
    content = resp["message"].get("content", "")
    return _extract_code_block(content) or content


def _read_base_code(path: Path) -> Optional[str]:
    if path.exists():
        return path.read_text(encoding="utf-8")
    return None


async def create_candidate_plugin(spec_text: str, base_plugin_id: Optional[str] = None) -> Dict[str, Any]:
    state.ensure_candidate_dirs()
    candidate_id = base_plugin_id or _slugify(spec_text)
    path = state.candidate_path("plugin", candidate_id)
    base_code = _read_base_code(state.stable_path("plugin", base_plugin_id)) if base_plugin_id else None
    prompt = _build_plugin_prompt(spec_text, base_code)
    code = await _generate_code(prompt)
    path.write_text(code.strip() + "\n", encoding="utf-8")

    meta = {
        "id": candidate_id,
        "type": "plugin",
        "path": str(path),
        "base_id": base_plugin_id or candidate_id,
        "created_at": state._now_iso(),
        "updated_at": state._now_iso(),
        "status": "draft",
        "override_enabled": False,
        "promotion": {"eligible": False, "reason": "pending smoke test"},
    }
    state.upsert_candidate("plugin", candidate_id, meta)
    state.record_change(f"Candidate plugin created: {candidate_id}")
    return meta


async def update_candidate_plugin(plugin_id: str, goal_text: str) -> Dict[str, Any]:
    existing = state.get_candidate("plugin", plugin_id)
    path = state.candidate_path("plugin", plugin_id)
    base_code = _read_base_code(path) or ""
    prompt = _build_plugin_prompt(goal_text, base_code)
    code = await _generate_code(prompt)
    path.write_text(code.strip() + "\n", encoding="utf-8")
    meta = existing or {
        "id": plugin_id,
        "type": "plugin",
        "path": str(path),
        "base_id": plugin_id,
        "created_at": state._now_iso(),
    }
    meta["updated_at"] = state._now_iso()
    meta["status"] = "draft"
    meta["override_enabled"] = False
    meta["promotion"] = {"eligible": False, "reason": "pending smoke test"}
    state.upsert_candidate("plugin", plugin_id, meta)
    state.record_change(f"Candidate plugin updated: {plugin_id}")
    return meta


async def create_candidate_platform(spec_text: str, base_platform_id: Optional[str] = None) -> Dict[str, Any]:
    state.ensure_candidate_dirs()
    candidate_id = base_platform_id or _slugify(spec_text)
    path = state.candidate_path("platform", candidate_id)
    base_code = _read_base_code(state.stable_path("platform", base_platform_id)) if base_platform_id else None
    prompt = _build_platform_prompt(spec_text, base_code)
    code = await _generate_code(prompt)
    path.write_text(code.strip() + "\n", encoding="utf-8")

    meta = {
        "id": candidate_id,
        "type": "platform",
        "path": str(path),
        "base_id": base_platform_id or candidate_id,
        "created_at": state._now_iso(),
        "updated_at": state._now_iso(),
        "status": "draft",
        "override_enabled": False,
        "promotion": {"eligible": False, "reason": "pending smoke test"},
    }
    state.upsert_candidate("platform", candidate_id, meta)
    state.record_change(f"Candidate platform created: {candidate_id}")
    return meta


async def update_candidate_platform(platform_id: str, goal_text: str) -> Dict[str, Any]:
    existing = state.get_candidate("platform", platform_id)
    path = state.candidate_path("platform", platform_id)
    base_code = _read_base_code(path) or ""
    prompt = _build_platform_prompt(goal_text, base_code)
    code = await _generate_code(prompt)
    path.write_text(code.strip() + "\n", encoding="utf-8")
    meta = existing or {
        "id": platform_id,
        "type": "platform",
        "path": str(path),
        "base_id": platform_id,
        "created_at": state._now_iso(),
    }
    meta["updated_at"] = state._now_iso()
    meta["status"] = "draft"
    meta["override_enabled"] = False
    meta["promotion"] = {"eligible": False, "reason": "pending smoke test"}
    state.upsert_candidate("platform", platform_id, meta)
    state.record_change(f"Candidate platform updated: {platform_id}")
    return meta
