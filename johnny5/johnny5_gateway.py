import json
import time
from typing import Any, Dict, List, Optional

from helpers import get_llm_client_from_env, get_tater_name, get_tater_personality, run_async
from johnny5 import factory, smoke_test, state


def _global_summary() -> str:
    focus = state.get_global_focus()
    errors = state.redis_client.lrange(state.GLOBAL_ERRORS_KEY, 0, 4)
    changes = state.redis_client.lrange(state.GLOBAL_CHANGES_KEY, 0, 4)

    lines = ["Global awareness summary:"]
    if focus:
        lines.append(f"- Focus: {focus}")
    if changes:
        lines.append("- Recent changes:")
        for item in changes:
            lines.append(f"  - {item}")
    if errors:
        lines.append("- Recent errors:")
        for item in errors:
            lines.append(f"  - {item}")
    return "\n".join(lines)


def _merge_prompt(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not messages:
        first, last = get_tater_name()
        system = f"You are {first} {last}. {get_tater_personality()}".strip()
        messages = [{"role": "system", "content": system}]

    summary = _global_summary()
    if messages and messages[0].get("role") == "system":
        return [messages[0], {"role": "system", "content": summary}] + messages[1:]

    return [{"role": "system", "content": summary}] + messages


def _handle_factory(job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    meta = job.get("meta") or {}
    request_kind = meta.get("request_kind") or job.get("request_kind") or "chat"
    spec_text = meta.get("spec_text") or job.get("spec_text")
    base_id = meta.get("base_id") or job.get("base_id")
    if request_kind == "plugin_factory" and spec_text:
        meta_out = run_async(factory.create_candidate_plugin(spec_text, base_plugin_id=base_id))
        last_test = smoke_test.run_smoke_test("plugin", meta_out["id"])
        return {
            "message": {
                "role": "assistant",
                "content": f"Candidate plugin `{meta_out['id']}` created. Smoke test ok={last_test['ok']}.",
            }
        }
    if request_kind == "platform_factory" and spec_text:
        meta_out = run_async(factory.create_candidate_platform(spec_text, base_platform_id=base_id))
        last_test = smoke_test.run_smoke_test("platform", meta_out["id"])
        return {
            "message": {
                "role": "assistant",
                "content": f"Candidate platform `{meta_out['id']}` created. Smoke test ok={last_test['ok']}.",
            }
        }
    return None


def _handle_update(job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    meta = job.get("meta") or {}
    request_kind = meta.get("request_kind") or job.get("request_kind") or "chat"
    goal_text = meta.get("goal_text") or job.get("goal_text")
    candidate_id = meta.get("candidate_id") or job.get("candidate_id")
    if request_kind == "plugin_update" and goal_text and candidate_id:
        meta_out = run_async(factory.update_candidate_plugin(candidate_id, goal_text))
        last_test = smoke_test.run_smoke_test("plugin", meta_out["id"])
        return {
            "message": {
                "role": "assistant",
                "content": f"Candidate plugin `{meta_out['id']}` updated. Smoke test ok={last_test['ok']}.",
            }
        }
    if request_kind == "platform_update" and goal_text and candidate_id:
        meta_out = run_async(factory.update_candidate_platform(candidate_id, goal_text))
        last_test = smoke_test.run_smoke_test("platform", meta_out["id"])
        return {
            "message": {
                "role": "assistant",
                "content": f"Candidate platform `{meta_out['id']}` updated. Smoke test ok={last_test['ok']}.",
            }
        }
    return None


def run(stop_event):
    client = get_llm_client_from_env(use_johnny5=False)
    last_heartbeat = 0.0

    while not stop_event.is_set():
        if time.time() - last_heartbeat > 5:
            state.set_heartbeat()
            last_heartbeat = time.time()

        if not state.is_enabled():
            time.sleep(0.5)
            continue

        job_data = state.redis_client.blpop(state.LLM_REQ_KEY, timeout=1)
        if not job_data:
            continue

        _, raw = job_data
        try:
            job = json.loads(raw)
        except Exception as e:
            state.record_error(f"Johnny5 job parse error: {e}")
            continue

        job_id = job.get("job_id")
        resp_key = f"{state.LLM_RESP_PREFIX}{job_id}"
        meta = job.get("meta") or {}

        state.record_event("llm_job_started", {"job_id": job_id, "platform": meta.get("platform")})
        try:
            factory_result = _handle_factory(job) or _handle_update(job)
            if factory_result:
                payload = {"ok": True, "response": factory_result}
                state.redis_client.setex(resp_key, 120, json.dumps(payload))
                continue

            messages = job.get("messages") or []
            messages = _merge_prompt(messages)

            response = run_async(
                client.chat(
                    messages=messages,
                    timeout=job.get("timeout", 60),
                    meta=meta,
                    model=job.get("model"),
                    max_tokens=job.get("max_tokens"),
                    temperature=job.get("temperature"),
                )
            )
            payload = {"ok": True, "response": response}
            state.redis_client.setex(resp_key, 120, json.dumps(payload))
            state.record_event("llm_job_complete", {"job_id": job_id, "platform": meta.get("platform")})
        except Exception as e:
            state.redis_client.setex(resp_key, 120, json.dumps({"ok": False, "error": str(e)}))
            state.record_error(f"Johnny5 error: {e}")
            state.record_event("llm_job_error", {"job_id": job_id, "error": str(e)})
