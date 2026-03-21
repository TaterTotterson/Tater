from typing import Any, Dict, Optional


def attach_origin(
    args: Dict[str, Any],
    *,
    origin: Optional[Dict[str, Any]],
    platform: str,
    scope: str,
    request_text: str = "",
) -> Dict[str, Any]:
    out = dict(args or {})
    base_origin = dict(origin) if isinstance(origin, dict) else {}
    trusted_origin: Dict[str, str] = {}
    if platform:
        trusted_origin["platform"] = str(platform)
    if scope:
        trusted_origin["scope"] = str(scope)
    if request_text:
        trusted_origin["request_text"] = str(request_text)
    for key, value in trusted_origin.items():
        base_origin[key] = value

    if not base_origin:
        return out

    existing = out.get("origin")
    if not isinstance(existing, dict):
        out["origin"] = base_origin
        return out

    merged: Dict[str, Any] = {}
    for key, value in existing.items():
        if value not in (None, ""):
            merged[key] = value
    for key, value in base_origin.items():
        if value not in (None, ""):
            if key in trusted_origin:
                continue
            if key not in merged or merged.get(key) in (None, ""):
                merged[key] = value
    for key, value in trusted_origin.items():
        if value not in (None, ""):
            merged[key] = value
    out["origin"] = merged
    return out
