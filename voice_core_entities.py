import requests
from typing import Any, Dict, Iterable, List, Optional

from helpers import redis_client


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _lower(value: Any) -> str:
    return _text(value).strip().lower()


def _bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    token = _lower(value)
    if token in {"1", "true", "yes", "y", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "n", "off", "disabled"}:
        return False
    return bool(default)


def _int(value: Any, default: int = 0, *, minimum: Optional[int] = None) -> int:
    try:
        out = int(float(value))
    except Exception:
        out = int(default)
    if minimum is not None and out < minimum:
        out = int(minimum)
    return out


class VoiceCoreEntityClient:
    """Reusable helper for reading and commanding ESPHome entities via Voice Core."""

    def __init__(self, *, timeout: float = 20.0):
        self.timeout = max(5.0, float(timeout or 0.0))

    @staticmethod
    def settings() -> dict:
        row = redis_client.hgetall("voice_core_settings") or {}
        return row if isinstance(row, dict) else {}

    @classmethod
    def base_url(cls) -> str:
        settings = cls.settings()
        port = _int(settings.get("bind_port") or "8502", 8502, minimum=1)
        if port > 65535:
            port = 8502
        return f"http://127.0.0.1:{port}"

    @staticmethod
    def selector_from_context(context: Optional[Dict[str, Any]]) -> str:
        ctx = context if isinstance(context, dict) else {}
        origin = ctx.get("origin") if isinstance(ctx.get("origin"), dict) else {}
        for key in ("satellite_selector", "device_id"):
            raw = ctx.get(key)
            if raw in (None, ""):
                raw = origin.get(key)
            token = _text(raw).strip()
            if token.startswith("host:"):
                return token
        return ""

    def _post(self, path: str, payload: Dict[str, Any], *, timeout: Optional[float] = None) -> Dict[str, Any]:
        resp = requests.post(
            f"{self.base_url()}{path}",
            json=(payload or {}),
            timeout=max(5.0, float(timeout or self.timeout)),
        )
        if resp.status_code >= 400:
            detail = ""
            try:
                parsed = resp.json()
                if isinstance(parsed, dict):
                    detail = _text(parsed.get("detail") or parsed.get("message")).strip()
            except Exception:
                detail = ""
            raise RuntimeError(detail or f"HTTP {resp.status_code}: {resp.text}")
        try:
            return resp.json() if (resp.text or "").strip() else {}
        except Exception:
            return {"ok": True, "raw": resp.text}

    def get_entities(self, selector: str) -> Dict[str, Any]:
        token = _text(selector).strip()
        if not token:
            raise ValueError("selector is required")
        return self._post("/tater-ha/v1/voice/esphome/entities", {"selector": token})

    def command(self, selector: str, entity_key: Any, command: str, *, value: Any = None) -> Dict[str, Any]:
        token = _text(selector).strip()
        key = _text(entity_key).strip()
        cmd = _text(command).strip()
        if not token:
            raise ValueError("selector is required")
        if not key:
            raise ValueError("entity_key is required")
        if not cmd:
            raise ValueError("command is required")
        payload: Dict[str, Any] = {
            "selector": token,
            "entity_key": key,
            "command": cmd,
        }
        if value is not None:
            payload["value"] = value
        return self._post("/tater-ha/v1/voice/esphome/entities/command", payload)

    @staticmethod
    def entity_text(entry: Optional[Dict[str, Any]]) -> str:
        row = entry if isinstance(entry, dict) else {}
        return " ".join(
            _text(row.get(part)).strip()
            for part in ("name", "object_id", "kind", "meta", "device_class", "entity_category", "icon")
            if _text(row.get(part)).strip()
        ).strip().lower()

    @staticmethod
    def kind_matches(entry: Optional[Dict[str, Any]], domain: str) -> bool:
        token = _lower(domain)
        kind = _lower((entry or {}).get("kind"))
        if token == "binary_sensor":
            return "binary" in kind and "sensor" in kind
        if token == "sensor":
            return "sensor" in kind and "binary" not in kind
        return token.replace("_", "") in kind.replace("_", "")

    @classmethod
    def find_best(
        cls,
        entries: Iterable[Dict[str, Any]],
        domain: str,
        required_parts: List[str],
        *,
        optional_parts: Optional[List[str]] = None,
        avoid_parts: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        opts = optional_parts or []
        avoids = avoid_parts or []
        best = None
        best_score = -1
        for entry in entries or []:
            if not isinstance(entry, dict):
                continue
            if not cls.kind_matches(entry, domain):
                continue
            low = cls.entity_text(entry)
            if any(part not in low for part in required_parts):
                continue
            if any(part in low for part in avoids):
                continue
            score = 100 + (10 * len(required_parts))
            for part in opts:
                if part in low:
                    score += 4
            object_id = _lower(entry.get("object_id"))
            if "remote_timer" in object_id:
                score += 8
            if "_timer_" in object_id:
                score += 4
            if score > best_score:
                best_score = score
                best = entry
        return best

    @staticmethod
    def state_value(entry: Optional[Dict[str, Any]]) -> Any:
        row = entry if isinstance(entry, dict) else {}
        attrs = row.get("attrs") if isinstance(row.get("attrs"), dict) else {}
        for key in ("state", "value", "active"):
            if key in attrs:
                return attrs.get(key)
        raw = row.get("raw")
        if raw not in (None, ""):
            return raw
        return row.get("value")

    @classmethod
    def state_bool(cls, entry: Optional[Dict[str, Any]]) -> Optional[bool]:
        value = cls.state_value(entry)
        if isinstance(value, bool):
            return value
        token = _lower(value)
        if token in {"on", "true", "1", "yes"}:
            return True
        if token in {"off", "false", "0", "no"}:
            return False
        return None

    @classmethod
    def state_int(cls, entry: Optional[Dict[str, Any]]) -> Optional[int]:
        value = cls.state_value(entry)
        try:
            return int(round(float(value)))
        except Exception:
            pass
        text_value = _text((entry or {}).get("value"))
        digits = ""
        for ch in text_value:
            if ch.isdigit() or (ch in ".-" and not digits):
                digits += ch
            elif digits:
                break
        if not digits:
            return None
        try:
            return int(round(float(digits)))
        except Exception:
            return None

    def press_button(self, selector: str, entry: Dict[str, Any]) -> Dict[str, Any]:
        return self.command(selector, entry.get("key"), "press")

    def set_number(self, selector: str, entry: Dict[str, Any], value: Any) -> Dict[str, Any]:
        return self.command(selector, entry.get("key"), "number_set", value=value)

    def set_switch(self, selector: str, entry: Dict[str, Any], value: Any) -> Dict[str, Any]:
        return self.command(selector, entry.get("key"), "switch_set", value=_bool(value, False))

    def set_select(self, selector: str, entry: Dict[str, Any], value: Any) -> Dict[str, Any]:
        return self.command(selector, entry.get("key"), "select_set", value=_text(value))

    def set_text(self, selector: str, entry: Dict[str, Any], value: Any) -> Dict[str, Any]:
        return self.command(selector, entry.get("key"), "text_set", value=_text(value))
