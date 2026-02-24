from typing import Any, Callable, Dict, Optional

from helpers import redis_client


Validator = Callable[[str], bool]


def _state(value: Optional[str], validator: Optional[Validator] = None) -> str:
    text = (value or "").strip()
    if not text:
        return "missing"
    if validator and not validator(text):
        return "invalid"
    return "set"


def diagnose_hash_fields(
    hash_key: str,
    fields: Dict[str, str],
    validators: Optional[Dict[str, Validator]] = None,
) -> Dict[str, str]:
    try:
        data = redis_client.hgetall(hash_key) or {}
    except Exception:
        data = {}
    result: Dict[str, str] = {}
    validators = validators or {}
    for public_name, field_name in fields.items():
        value = data.get(field_name)
        result[public_name] = _state(value, validators.get(public_name))
    return result


def diagnose_redis_keys(
    keys: Dict[str, str],
    validators: Optional[Dict[str, Validator]] = None,
) -> Dict[str, str]:
    result: Dict[str, str] = {}
    validators = validators or {}
    for public_name, key in keys.items():
        try:
            value = redis_client.get(key)
        except Exception:
            value = None
        result[public_name] = _state(value, validators.get(public_name))
    return result


def combine_diagnosis(*parts: Dict[str, str]) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for block in parts:
        for key, value in (block or {}).items():
            if key not in merged:
                merged[key] = value
            elif merged[key] != "missing" and value == "missing":
                merged[key] = value
            elif merged[key] == "set" and value == "invalid":
                merged[key] = value
    return merged


def needs_from_diagnosis(diagnosis: Dict[str, str], prompts: Optional[Dict[str, str]] = None) -> list[str]:
    prompts = prompts or {}
    needs: list[str] = []
    for key, status in (diagnosis or {}).items():
        if status == "set":
            continue
        prompt = prompts.get(key)
        if not prompt:
            prompt = f"Please provide a valid value for `{key}`."
        needs.append(prompt)
    return needs
