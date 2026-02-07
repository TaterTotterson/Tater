import ast
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
import importlib.util
import ipaddress
import socket
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from helpers import redis_client
from plugin_loader import load_plugins_from_directory
from plugin_base import ToolPlugin
from plugin_registry import reload_plugins
from plugin_settings import get_plugin_enabled


BASE_DIR = Path(__file__).resolve().parent
AGENT_LAB_DIR = BASE_DIR / "agent_lab"
AGENT_PLUGINS_DIR = AGENT_LAB_DIR / "plugins"
AGENT_PLATFORMS_DIR = AGENT_LAB_DIR / "platforms"
AGENT_ARTIFACTS_DIR = AGENT_LAB_DIR / "artifacts"
AGENT_DOCUMENTS_DIR = AGENT_LAB_DIR / "documents"
AGENT_DOWNLOADS_DIR = AGENT_LAB_DIR / "downloads"
AGENT_WORKSPACE_DIR = AGENT_LAB_DIR / "workspace"
AGENT_LOGS_DIR = AGENT_LAB_DIR / "logs"
SKILLS_DIR = BASE_DIR / "skills"
AGENT_SKILLS_DIR = SKILLS_DIR / "agent_lab"
AGENT_REQUIREMENTS = AGENT_LAB_DIR / "requirements.txt"

STABLE_PLUGINS_DIR = BASE_DIR / os.getenv("TATER_PLUGIN_DIR", "plugins")
STABLE_PLATFORMS_DIR = BASE_DIR / "platforms"

_SAFE_NAME_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")
_SAFE_DEP_RE = re.compile(r"^[A-Za-z0-9_.\\-\\[\\]==<>!~]+$")


def _ensure_dirs() -> None:
    for path in (
        AGENT_LAB_DIR,
        AGENT_PLUGINS_DIR,
        AGENT_PLATFORMS_DIR,
        AGENT_ARTIFACTS_DIR,
        AGENT_DOCUMENTS_DIR,
        AGENT_DOWNLOADS_DIR,
        AGENT_WORKSPACE_DIR,
        AGENT_LOGS_DIR,
        AGENT_SKILLS_DIR,
        SKILLS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)
    if not AGENT_REQUIREMENTS.exists():
        AGENT_REQUIREMENTS.write_text("", encoding="utf-8")


def _log_write(action: str, path: Path, size: int = 0) -> None:
    try:
        _ensure_dirs()
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} | {action} | {path} | {size} bytes\n"
        with (AGENT_LOGS_DIR / "agent_writes.log").open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        return


def _resolve_safe_path(path: str, allowed_roots: List[Path]) -> Optional[Path]:
    if not path:
        return None
    p = Path(path)
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    else:
        p = p.resolve()

    for root in allowed_roots:
        try:
            root_resolved = root.resolve()
        except Exception:
            root_resolved = root
        if p == root_resolved or root_resolved in p.parents:
            return p
    return None


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _sanitize_filename(name: str) -> str:
    raw = os.path.basename((name or "").strip())
    if not raw:
        return ""
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", raw)
    safe = safe.lstrip(".")
    return safe or ""


def _is_private_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
    )


def _host_is_private(host: str) -> Tuple[bool, Optional[str]]:
    if not host:
        return True, "URL must include a host."
    try:
        if _is_private_ip(host):
            return True, None
    except Exception:
        return True, "Invalid host."
    try:
        infos = socket.getaddrinfo(host, None)
    except Exception:
        return True, "Unable to resolve host."
    for info in infos:
        ip_str = info[4][0]
        if _is_private_ip(ip_str):
            return True, None
    return False, None


def _validate_url(url: str) -> Optional[str]:
    if not url:
        return "URL is required."
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return "Only http/https URLs are allowed."
    if not parsed.hostname:
        return "URL must include a host."
    is_private, err = _host_is_private(parsed.hostname)
    if err:
        return err
    if is_private:
        return "Private or local network hosts are not allowed."
    return None


def read_url(
    url: str,
    *,
    max_bytes: int = 200_000,
    timeout_sec: int = 15,
) -> Dict[str, Any]:
    err = _validate_url(url)
    if err:
        return {"tool": "read_url", "ok": False, "error": err}
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Tater-AgentLab/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            content_type = resp.headers.get("Content-Type", "") or ""
            raw = resp.read(max_bytes + 1)
        truncated = len(raw) > max_bytes
        if truncated:
            raw = raw[:max_bytes]
        # Only allow textual content.
        if not (
            content_type.startswith("text/")
            or "json" in content_type
            or "xml" in content_type
            or "yaml" in content_type
            or "yml" in content_type
        ):
            return {
                "tool": "read_url",
                "ok": False,
                "error": f"Non-text content type ({content_type or 'unknown'}). Use download_file instead.",
            }
        try:
            content = raw.decode("utf-8")
        except Exception:
            content = raw.decode("utf-8", errors="replace")
        return {
            "tool": "read_url",
            "ok": True,
            "url": url,
            "content_type": content_type,
            "bytes": len(raw),
            "truncated": truncated,
            "content": content,
        }
    except Exception as e:
        return {"tool": "read_url", "ok": False, "error": str(e)}


def download_file(
    url: str,
    *,
    filename: Optional[str] = None,
    subdir: Optional[str] = None,
    max_bytes: int = 25_000_000,
    timeout_sec: int = 30,
) -> Dict[str, Any]:
    _ensure_dirs()
    err = _validate_url(url)
    if err:
        return {"tool": "download_file", "ok": False, "error": err}

    # Resolve target directory inside agent_lab (default: downloads)
    target_dir = AGENT_LAB_DIR / (subdir or "downloads")
    try:
        target_dir = target_dir.resolve()
    except Exception:
        target_dir = AGENT_DOCUMENTS_DIR
    if not (
        target_dir == AGENT_LAB_DIR.resolve()
        or AGENT_LAB_DIR.resolve() in target_dir.parents
    ):
        return {"tool": "download_file", "ok": False, "error": "Target directory not allowed."}
    target_dir.mkdir(parents=True, exist_ok=True)

    parsed = urllib.parse.urlparse(url)
    default_name = _sanitize_filename(os.path.basename(parsed.path)) or "download.bin"
    safe_name = _sanitize_filename(filename or default_name) or "download.bin"
    dest = target_dir / safe_name

    import hashlib

    hasher = hashlib.sha256()
    size = 0
    content_type = ""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Tater-AgentLab/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            content_type = resp.headers.get("Content-Type", "") or ""
            length = resp.headers.get("Content-Length")
            if length:
                try:
                    if int(length) > max_bytes:
                        return {
                            "tool": "download_file",
                            "ok": False,
                            "error": f"File exceeds max_bytes ({max_bytes}).",
                        }
                except Exception:
                    pass
            with dest.open("wb") as f:
                while True:
                    chunk = resp.read(8192)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > max_bytes:
                        f.close()
                        try:
                            dest.unlink()
                        except Exception:
                            pass
                        return {
                            "tool": "download_file",
                            "ok": False,
                            "error": f"File exceeds max_bytes ({max_bytes}).",
                        }
                    f.write(chunk)
                    hasher.update(chunk)
    except Exception as e:
        try:
            if dest.exists():
                dest.unlink()
        except Exception:
            pass
        return {"tool": "download_file", "ok": False, "error": str(e)}

    _log_write("download_file", dest, size)
    return {
        "tool": "download_file",
        "ok": True,
        "url": url,
        "path": str(dest),
        "bytes": size,
        "sha256": hasher.hexdigest(),
        "content_type": content_type,
    }


def read_file(path: str) -> Dict[str, Any]:
    _ensure_dirs()
    allowed = [AGENT_LAB_DIR, STABLE_PLUGINS_DIR, STABLE_PLATFORMS_DIR, SKILLS_DIR]
    resolved = _resolve_safe_path(path, allowed)
    if not resolved:
        return {"tool": "read_file", "ok": False, "error": "Path not allowed."}
    if not resolved.exists() or not resolved.is_file():
        return {"tool": "read_file", "ok": False, "error": "File not found."}
    try:
        content = _read_text(resolved)
        return {"tool": "read_file", "ok": True, "path": str(resolved), "content": content}
    except Exception as e:
        return {"tool": "read_file", "ok": False, "error": str(e)}


def write_file(
    path: str,
    content: Optional[str] = None,
    *,
    content_b64: Optional[str] = None,
    content_lines: Optional[List[str]] = None,
) -> Dict[str, Any]:
    _ensure_dirs()
    allowed = [AGENT_LAB_DIR]
    resolved = _resolve_safe_path(path, allowed)
    if not resolved:
        return {"tool": "write_file", "ok": False, "error": "Path not allowed."}
    try:
        # Prevent creating executable modules via write_file.
        if resolved.suffix == ".py":
            try:
                rel = resolved.relative_to(AGENT_LAB_DIR)
                top = rel.parts[0] if rel.parts else ""
            except Exception:
                top = ""
            if top in {"plugins", "platforms"}:
                return {
                    "tool": "write_file",
                    "ok": False,
                    "error": "Use create_plugin/create_platform for Agent Lab plugins/platforms.",
                }
            return {
                "tool": "write_file",
                "ok": False,
                "error": "Python files are not allowed via write_file. Use create_plugin/create_platform.",
            }
        resolved.parent.mkdir(parents=True, exist_ok=True)
        if content_b64:
            try:
                import base64
                data = base64.b64decode(content_b64.encode("utf-8")).decode("utf-8")
            except Exception as e:
                return {"tool": "write_file", "ok": False, "error": f"Invalid content_b64: {e}"}
        elif isinstance(content_lines, list):
            data = "\n".join(str(x) for x in content_lines)
        else:
            data = content if content is not None else ""
        resolved.write_text(data, encoding="utf-8")
        _log_write("write_file", resolved, len(data.encode("utf-8")))
        return {"tool": "write_file", "ok": True, "path": str(resolved), "bytes": len(data)}
    except Exception as e:
        return {"tool": "write_file", "ok": False, "error": str(e)}


def list_directory(path: str) -> Dict[str, Any]:
    _ensure_dirs()
    allowed = [AGENT_LAB_DIR, STABLE_PLUGINS_DIR, STABLE_PLATFORMS_DIR]
    resolved = _resolve_safe_path(path, allowed)
    if not resolved:
        return {"tool": "list_directory", "ok": False, "error": "Path not allowed."}
    if not resolved.exists() or not resolved.is_dir():
        return {"tool": "list_directory", "ok": False, "error": "Directory not found."}
    try:
        files = []
        dirs = []
        for item in sorted(resolved.iterdir()):
            if item.is_dir():
                dirs.append(item.name)
            else:
                files.append(item.name)
        return {"tool": "list_directory", "ok": True, "path": str(resolved), "files": files, "directories": dirs}
    except Exception as e:
        return {"tool": "list_directory", "ok": False, "error": str(e)}


def delete_file(path: str) -> Dict[str, Any]:
    _ensure_dirs()
    allowed = [AGENT_LAB_DIR]
    resolved = _resolve_safe_path(path, allowed)
    if not resolved:
        return {"tool": "delete_file", "ok": False, "error": "Path not allowed."}
    if not resolved.exists() or not resolved.is_file():
        return {"tool": "delete_file", "ok": False, "error": "File not found."}
    try:
        resolved.unlink()
        _log_write("delete_file", resolved, 0)
        return {"tool": "delete_file", "ok": True, "path": str(resolved), "deleted": True}
    except Exception as e:
        return {"tool": "delete_file", "ok": False, "error": str(e)}


def _requirements_path() -> Path:
    _ensure_dirs()
    return AGENT_REQUIREMENTS


def _read_requirements() -> List[str]:
    path = _requirements_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception:
        return []
    out = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out


def _write_requirements(lines: List[str]) -> None:
    path = _requirements_path()
    uniq = []
    seen = set()
    for line in lines:
        if not line or line.startswith("#"):
            continue
        if line not in seen:
            seen.add(line)
            uniq.append(line)
    path.write_text("\n".join(sorted(uniq)) + ("\n" if uniq else ""), encoding="utf-8")


def _normalize_dependency(dep: str) -> str:
    return str(dep or "").strip()


def _dependency_import_name(dep: str) -> str:
    dep = _normalize_dependency(dep)
    if not dep:
        return ""
    # strip extras and version specifiers
    name = re.split(r"[<>=!~]", dep, maxsplit=1)[0]
    name = name.split("[", 1)[0]
    return name.strip()


def _extract_declared_dependencies(path: Path) -> List[str]:
    try:
        source = path.read_text(encoding="utf-8")
    except Exception:
        return []
    try:
        tree = ast.parse(source, filename=str(path))
    except Exception:
        return []
    deps = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {"dependencies", "DEPENDENCIES", "requirements"}:
                    if isinstance(node.value, (ast.List, ast.Tuple)):
                        for item in node.value.elts:
                            if isinstance(item, ast.Constant) and isinstance(item.value, str):
                                deps.append(item.value.strip())
    return [d for d in deps if d]


def _validate_platform_source(source: str) -> Tuple[bool, str]:
    if not source or not str(source).strip():
        return False, "Missing code for platform."
    try:
        tree = ast.parse(source)
    except Exception as e:
        return False, f"Syntax error: {e}"

    has_platform = False
    has_run = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "PLATFORM":
                    has_platform = True
        if isinstance(node, ast.FunctionDef) and node.name == "run":
            has_run = True

    if not has_platform:
        return False, "Missing PLATFORM dict."
    if not has_run:
        return False, "Missing run() function."
    return True, ""


def _validate_plugin_source(source: str) -> Tuple[bool, str]:
    if not source or not str(source).strip():
        return False, "Missing code for plugin."
    try:
        tree = ast.parse(source)
    except Exception as e:
        return False, f"Syntax error: {e}"

    has_plugin_assignment = False
    plugin_is_dict = False
    has_toolplugin_class = False
    has_toolplugin_import = False

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == "plugin_base":
                for alias in node.names:
                    if alias.name == "ToolPlugin":
                        has_toolplugin_import = True
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "plugin":
                    has_plugin_assignment = True
                    if isinstance(node.value, ast.Dict):
                        plugin_is_dict = True
        if isinstance(node, ast.ClassDef):
            for base in node.bases:
                if isinstance(base, ast.Name) and base.id == "ToolPlugin":
                    has_toolplugin_class = True
                elif isinstance(base, ast.Attribute) and base.attr == "ToolPlugin":
                    has_toolplugin_class = True

    if not has_plugin_assignment:
        return False, "Missing module-level `plugin` instance."
    if plugin_is_dict:
        return False, "`plugin` must be a ToolPlugin instance (not a dict)."
    if not has_toolplugin_class:
        return False, "Missing ToolPlugin subclass."
    if not has_toolplugin_import:
        return False, "Import ToolPlugin from plugin_base."
    return True, ""


def _missing_dependencies(deps: List[str]) -> List[str]:
    missing = []
    for dep in deps:
        name = _dependency_import_name(dep)
        if not name:
            continue
        try:
            __import__(name)
        except Exception:
            missing.append(dep)
    return missing


def _log_dependency(action: str, detail: str) -> None:
    try:
        _ensure_dirs()
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts} | {action} | {detail}\n"
        with (AGENT_LOGS_DIR / "agent_dependencies.log").open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        return


def _install_dependencies(deps: List[str]) -> Tuple[List[str], List[str]]:
    installed = []
    errors = []
    for dep in deps:
        dep = _normalize_dependency(dep)
        if not dep:
            continue
        if not _SAFE_DEP_RE.fullmatch(dep):
            errors.append(f"{dep} (invalid dependency spec)")
            continue
        try:
            _log_dependency("install_start", dep)
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", dep],
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode == 0:
                installed.append(dep)
                _log_dependency("install_ok", dep)
            else:
                err = (result.stderr or result.stdout or "").strip()
                errors.append(f"{dep} ({err[:200]})")
                _log_dependency("install_fail", f"{dep} | {err[:200]}")
        except Exception as e:
            errors.append(f"{dep} ({e})")
            _log_dependency("install_exception", f"{dep} | {e}")
    return installed, errors


def _update_requirements_union(deps: List[str]) -> None:
    if not deps:
        return
    current = _read_requirements()
    merged = current[:]
    for dep in deps:
        dep = _normalize_dependency(dep)
        if dep and dep not in merged:
            merged.append(dep)
    _write_requirements(merged)


def _store_validation(kind: str, name: str, report: Dict[str, Any]) -> None:
    try:
        key = f"exp:validation:{kind}:{name}"
        redis_client.set(key, json.dumps(report, ensure_ascii=False))
    except Exception:
        return


def list_stable_plugins() -> Dict[str, Any]:
    try:
        from plugin_registry import get_registry_snapshot
        registry = get_registry_snapshot()
    except Exception:
        registry = {}
    items = []
    for pid, plugin in sorted(registry.items(), key=lambda kv: kv[0].lower()):
        version = (
            getattr(plugin, "version", None)
            or getattr(plugin, "__version__", None)
            or getattr(plugin, "plugin_version", None)
            or "0.0.0"
        )
        platforms = getattr(plugin, "platforms", []) or []
        items.append(
            {
                "id": pid,
                "version": str(version),
                "platforms": platforms,
                "enabled": bool(get_plugin_enabled(pid)),
            }
        )
    return {"tool": "list_stable_plugins", "ok": True, "plugins": items, "count": len(items)}


def list_stable_platforms() -> Dict[str, Any]:
    from platform_registry import platform_registry
    items = []
    for entry in platform_registry:
        key = entry.get("key")
        if not key:
            continue
        running = str(redis_client.get(f"{key}_running") or "").strip().lower() == "true"
        items.append({"key": key, "running": running})
    return {"tool": "list_stable_platforms", "ok": True, "platforms": items, "count": len(items)}


def inspect_plugin(plugin_id: str) -> Dict[str, Any]:
    try:
        from plugin_registry import get_registry_snapshot
        registry = get_registry_snapshot()
    except Exception:
        registry = {}
    plugin = registry.get(plugin_id)
    if not plugin:
        return {"tool": "inspect_plugin", "ok": False, "error": f"Plugin '{plugin_id}' not found."}
    return {
        "tool": "inspect_plugin",
        "ok": True,
        "id": plugin_id,
        "platforms": getattr(plugin, "platforms", []) or [],
        "description": (
            getattr(plugin, "description", None)
            or getattr(plugin, "plugin_dec", None)
            or ""
        ),
        "required_settings": getattr(plugin, "required_settings", None) or {},
    }


def _exp_plugin_path(name: str) -> Path:
    return AGENT_PLUGINS_DIR / f"{name}.py"


def _exp_platform_path(name: str) -> Path:
    return AGENT_PLATFORMS_DIR / f"{name}.py"


def _import_from_path(path: Path) -> Optional[Any]:
    try:
        module_name = f"tater_exp_{path.stem}_{int(path.stat().st_mtime_ns)}"
        spec = importlib.util.spec_from_file_location(module_name, str(path))
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
        return module
    except Exception:
        return None


def validate_plugin(name: str, auto_install: bool = True) -> Dict[str, Any]:
    _ensure_dirs()
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        report = {"tool": "validate_plugin", "ok": False, "error": "Invalid plugin name."}
        _store_validation("plugin", name, report)
        return report
    path = _exp_plugin_path(name)
    if not path.exists():
        report = {"tool": "validate_plugin", "ok": False, "error": "Plugin file not found."}
        _store_validation("plugin", name, report)
        return report

    # Syntax check
    try:
        source = path.read_text(encoding="utf-8")
        ast.parse(source, filename=str(path))
    except Exception as e:
        report = {
            "tool": "validate_plugin",
            "ok": False,
            "error": f"Syntax error: {e}",
            "path": str(path),
        }
        _store_validation("plugin", name, report)
        return report

    declared_deps = _extract_declared_dependencies(path)
    _update_requirements_union(declared_deps)
    missing_deps = _missing_dependencies(declared_deps)
    installed_deps: List[str] = []
    install_errors: List[str] = []
    if missing_deps and auto_install:
        installed_deps, install_errors = _install_dependencies(missing_deps)
        missing_deps = _missing_dependencies(declared_deps)

    module = _import_from_path(path)
    if not module:
        report = {
            "tool": "validate_plugin",
            "ok": False,
            "error": "Import failed.",
            "path": str(path),
            "missing_dependencies": missing_deps,
            "installed_dependencies": installed_deps,
            "install_errors": install_errors,
        }
        _store_validation("plugin", name, report)
        return report

    plugin = getattr(module, "plugin", None)
    missing = []
    if not plugin:
        missing.append("plugin")
    else:
        if not isinstance(plugin, ToolPlugin):
            missing.append("plugin")
        for field in ("name", "version", "platforms", "description"):
            value = getattr(plugin, field, None)
            if value is None or (isinstance(value, str) and not value.strip()):
                missing.append(field)
            if field == "platforms" and (not isinstance(value, list) or not value):
                missing.append(field)
        usage_val = getattr(plugin, "usage", None)
        if not isinstance(usage_val, str) or not usage_val.strip():
            missing.append("usage")
        wait_prompt = getattr(plugin, "waiting_prompt_template", None)
        if not isinstance(wait_prompt, str) or not wait_prompt.strip():
            missing.append("waiting_prompt_template")
        else:
            lowered = wait_prompt.lower()
            if "write" not in lowered or "only output" not in lowered:
                missing.append("waiting_prompt_template")
        # Validate platform ids
        try:
            from plugin_kernel import KNOWN_PLATFORMS, expand_plugin_platforms
            platforms = getattr(plugin, "platforms", []) or []
            invalid = []
            for p in platforms:
                if str(p).strip().lower() == "both":
                    continue
                if str(p).strip().lower() not in KNOWN_PLATFORMS:
                    invalid.append(p)
            if invalid:
                missing.append("platforms")
        except Exception:
            pass

    ok = not missing and not missing_deps
    report = {
        "tool": "validate_plugin",
        "ok": ok,
        "name": name,
        "path": str(path),
        "missing_fields": sorted(set(missing)),
        "declared_dependencies": declared_deps,
        "missing_dependencies": missing_deps,
        "installed_dependencies": installed_deps,
        "install_errors": install_errors,
    }
    if ok:
        report["plugin_name"] = getattr(plugin, "name", name)
        report["version"] = getattr(plugin, "version", "")
        report["platforms"] = getattr(plugin, "platforms", []) or []
    _store_validation("plugin", name, report)
    return report


def create_plugin(
    name: str,
    code: Optional[str] = None,
    *,
    code_b64: Optional[str] = None,
    code_lines: Optional[List[str]] = None,
) -> Dict[str, Any]:
    _ensure_dirs()
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        return {"tool": "create_plugin", "ok": False, "error": "Invalid plugin name."}
    path = _exp_plugin_path(name)
    try:
        if code_b64:
            try:
                import base64
                payload = base64.b64decode(code_b64.encode("utf-8")).decode("utf-8")
            except Exception as e:
                return {"tool": "create_plugin", "ok": False, "error": f"Invalid code_b64: {e}"}
        elif isinstance(code_lines, list):
            for idx, line in enumerate(code_lines):
                if isinstance(line, str) and ("\n" in line or "\r" in line):
                    return {
                        "tool": "create_plugin",
                        "ok": False,
                        "error": "code_lines entries must be single-line strings (no embedded newlines).",
                    }
            payload = "\n".join(str(x) for x in code_lines)
        else:
            payload = code or ""

        ok, err = _validate_plugin_source(payload)
        if not ok:
            return {"tool": "create_plugin", "ok": False, "error": err}

        path.write_text(payload, encoding="utf-8")
        _log_write("create_plugin", path, len(payload.encode("utf-8")))
    except Exception as e:
        return {"tool": "create_plugin", "ok": False, "error": str(e)}

    report = validate_plugin(name)
    report["tool"] = "create_plugin"
    return report


def promote_plugin(name: str, confirm: Optional[bool] = None, delete_source: bool = False) -> Dict[str, Any]:
    _ensure_dirs()
    if not confirm:
        return {
            "tool": "promote_plugin",
            "ok": False,
            "error": "Confirmation required.",
            "needs": ["Please confirm promotion to stable plugins by setting confirm=true."],
        }
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        return {"tool": "promote_plugin", "ok": False, "error": "Invalid plugin name."}
    src = _exp_plugin_path(name)
    if not src.exists():
        return {"tool": "promote_plugin", "ok": False, "error": "Agent Lab plugin not found."}
    dest = STABLE_PLUGINS_DIR / f"{name}.py"
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        reload_plugins()
        _log_write("promote_plugin", dest, dest.stat().st_size if dest.exists() else 0)
        if delete_source:
            src.unlink()
        return {"tool": "promote_plugin", "ok": True, "path": str(dest)}
    except Exception as e:
        return {"tool": "promote_plugin", "ok": False, "error": str(e)}


def validate_platform(name: str, auto_install: bool = True) -> Dict[str, Any]:
    _ensure_dirs()
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        report = {"tool": "validate_platform", "ok": False, "error": "Invalid platform name."}
        _store_validation("platform", name, report)
        return report
    path = _exp_platform_path(name)
    if not path.exists():
        report = {"tool": "validate_platform", "ok": False, "error": "Platform file not found."}
        _store_validation("platform", name, report)
        return report

    try:
        source = path.read_text(encoding="utf-8")
        ast.parse(source, filename=str(path))
    except Exception as e:
        report = {
            "tool": "validate_platform",
            "ok": False,
            "error": f"Syntax error: {e}",
            "path": str(path),
        }
        _store_validation("platform", name, report)
        return report

    declared_deps = _extract_declared_dependencies(path)
    _update_requirements_union(declared_deps)
    missing_deps = _missing_dependencies(declared_deps)
    installed_deps: List[str] = []
    install_errors: List[str] = []
    if missing_deps and auto_install:
        installed_deps, install_errors = _install_dependencies(missing_deps)
        missing_deps = _missing_dependencies(declared_deps)

    module = _import_from_path(path)
    if not module:
        report = {
            "tool": "validate_platform",
            "ok": False,
            "error": "Import failed.",
            "path": str(path),
            "missing_dependencies": missing_deps,
            "installed_dependencies": installed_deps,
            "install_errors": install_errors,
        }
        _store_validation("platform", name, report)
        return report

    platform_dict = getattr(module, "PLATFORM", None)
    run_fn = getattr(module, "run", None)
    missing = []
    if not isinstance(platform_dict, dict):
        missing.append("PLATFORM")
    if not callable(run_fn):
        missing.append("run")

    ok = not missing and not missing_deps
    report = {
        "tool": "validate_platform",
        "ok": ok,
        "name": name,
        "path": str(path),
        "missing_fields": missing,
        "declared_dependencies": declared_deps,
        "missing_dependencies": missing_deps,
        "installed_dependencies": installed_deps,
        "install_errors": install_errors,
    }
    _store_validation("platform", name, report)
    return report


def create_platform(
    name: str,
    code: Optional[str] = None,
    *,
    code_b64: Optional[str] = None,
    code_lines: Optional[List[str]] = None,
) -> Dict[str, Any]:
    _ensure_dirs()
    if not _SAFE_NAME_RE.fullmatch(name or ""):
        return {"tool": "create_platform", "ok": False, "error": "Invalid platform name."}
    path = _exp_platform_path(name)
    try:
        if code_b64:
            try:
                import base64
                payload = base64.b64decode(code_b64.encode("utf-8")).decode("utf-8")
            except Exception as e:
                return {"tool": "create_platform", "ok": False, "error": f"Invalid code_b64: {e}"}
        elif isinstance(code_lines, list):
            for idx, line in enumerate(code_lines):
                if isinstance(line, str) and ("\n" in line or "\r" in line):
                    return {
                        "tool": "create_platform",
                        "ok": False,
                        "error": "code_lines entries must be single-line strings (no embedded newlines).",
                    }
            payload = "\n".join(str(x) for x in code_lines)
        else:
            payload = code or ""

        ok, err = _validate_platform_source(payload)
        if not ok:
            return {"tool": "create_platform", "ok": False, "error": err}

        path.write_text(payload, encoding="utf-8")
        _log_write("create_platform", path, len(payload.encode("utf-8")))
    except Exception as e:
        return {"tool": "create_platform", "ok": False, "error": str(e)}

    report = validate_platform(name)
    report["tool"] = "create_platform"
    return report


def write_workspace_note(content: str) -> Dict[str, Any]:
    _ensure_dirs()
    ts = time.strftime("%Y%m%d_%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    filename = f"note_{ts}_{suffix}.md"
    path = AGENT_WORKSPACE_DIR / filename
    try:
        data = content or ""
        path.write_text(data, encoding="utf-8")
        _log_write("write_workspace_note", path, len(data.encode("utf-8")))
        return {"tool": "write_workspace_note", "ok": True, "path": str(path)}
    except Exception as e:
        return {"tool": "write_workspace_note", "ok": False, "error": str(e)}


def list_workspace() -> Dict[str, Any]:
    _ensure_dirs()
    try:
        files = sorted([p.name for p in AGENT_WORKSPACE_DIR.iterdir() if p.is_file()])
        return {"tool": "list_workspace", "ok": True, "files": files}
    except Exception as e:
        return {"tool": "list_workspace", "ok": False, "error": str(e)}


def list_agent_plugins() -> Dict[str, Any]:
    _ensure_dirs()
    items = []
    errors = []
    for path in sorted(AGENT_PLUGINS_DIR.glob("*.py")):
        name = path.stem
        module = _import_from_path(path)
        if not module:
            errors.append({"name": name, "error": "Import failed"})
            continue
        plugin = getattr(module, "plugin", None)
        if not plugin:
            errors.append({"name": name, "error": "Missing plugin instance"})
            continue
        items.append(
            {
                "id": getattr(plugin, "name", name),
                "version": getattr(plugin, "version", ""),
                "platforms": getattr(plugin, "platforms", []) or [],
                "description": getattr(plugin, "description", "") or getattr(plugin, "plugin_dec", "") or "",
            }
        )
    return {"tool": "list_agent_plugins", "ok": True, "plugins": items, "errors": errors}


def list_agent_platforms() -> Dict[str, Any]:
    _ensure_dirs()
    items = []
    errors = []
    for path in sorted(AGENT_PLATFORMS_DIR.glob("*.py")):
        name = path.stem
        module = _import_from_path(path)
        if not module:
            errors.append({"name": name, "error": "Import failed"})
            continue
        platform_dict = getattr(module, "PLATFORM", None)
        if not isinstance(platform_dict, dict):
            errors.append({"name": name, "error": "Missing PLATFORM dict"})
            continue
        items.append({"key": name, "label": platform_dict.get("label") or name})
    return {"tool": "list_agent_platforms", "ok": True, "platforms": items, "errors": errors}

_ensure_dirs()
