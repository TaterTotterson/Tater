from pathlib import Path
from typing import Any, Dict, List, Optional


def execution_settings_summary(settings: Dict[str, Any]) -> Dict[str, bool]:
    policy_enabled = bool(settings.get("policy_enabled", True))
    return {
        "policy_enabled": policy_enabled,
        "policy_disabled": not policy_enabled,
        "allow_host_package_managers": bool(settings.get("allow_host_package_managers")),
        "allow_package_tool_installs": bool(settings.get("allow_installs")),
        "allow_network": bool(settings.get("allow_network")),
    }


def _argv_list(value: Any) -> List[str]:
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item or "").strip()]
    return []


def _executable(argv: List[str]) -> str:
    if not argv:
        return ""
    return Path(str(argv[0] or "")).name


def command_failure_context(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    missing: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        argv = _argv_list(row.get("argv"))
        error = row.get("error") if isinstance(row.get("error"), dict) else {}
        item = {
            "argv": argv,
            "executable": str(error.get("executable") or _executable(argv) or "").strip(),
            "status": str(row.get("status") or "").strip(),
            "returncode": row.get("returncode"),
            "error_code": str(error.get("code") or "").strip(),
            "message": str(error.get("message") or "").strip(),
        }
        if item["error_code"] == "executable_not_found":
            missing.append(item)
        elif not bool(row.get("ok")):
            failed.append(item)
    return {
        "missing_executables": missing[-8:],
        "recent_failed_commands": failed[-8:],
    }


def repeated_missing_executable(argv: List[str], rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    current = _executable(argv)
    if not current:
        return None
    for row in reversed(rows):
        if not isinstance(row, dict):
            continue
        row_argv = _argv_list(row.get("argv"))
        if bool(row.get("ok")) and row_argv and _executable(row_argv) != current:
            return None
        error = row.get("error") if isinstance(row.get("error"), dict) else {}
        row_executable = str(error.get("executable") or _executable(row_argv) or "").strip()
        if str(error.get("code") or "") == "executable_not_found" and row_executable == current:
            return {
                "code": "repeated_missing_executable",
                "executable": current,
                "argv": list(argv),
                "message": (
                    f"Executable '{current}' was already reported missing. "
                    "Install the tool first or choose a different built-in/Python fallback before retrying it."
                ),
            }
    return None
