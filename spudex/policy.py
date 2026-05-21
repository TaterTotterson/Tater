import shlex
from pathlib import Path
from typing import Any, Dict, List, Tuple

from kernel_tools import AGENT_LAB_DIR, AGENT_WORKSPACE_DIR


SHELL_COMMANDS = {
    "bash",
    "cmd",
    "fish",
    "powershell",
    "pwsh",
    "sh",
    "zsh",
}

HOST_ADMIN_COMMANDS = {
    "chmod",
    "chown",
    "launchctl",
    "osascript",
    "open",
    "su",
    "sudo",
}

REMOTE_CONTROL_COMMANDS = {
    "scp",
    "sftp",
    "ssh",
}

CONTAINER_COMMANDS = {"docker", "podman"}

DENIED_COMMANDS = SHELL_COMMANDS | HOST_ADMIN_COMMANDS | REMOTE_CONTROL_COMMANDS | CONTAINER_COMMANDS

NETWORK_COMMANDS = {"curl", "wget", "ssh", "scp", "sftp"}
HOST_INSTALL_COMMANDS = {"apt", "apt-get", "apk", "brew", "dnf", "pacman", "yum"}
INLINE_EVAL_COMMANDS = {
    "node": {"-e", "--eval"},
    "perl": {"-e"},
    "php": {"-r"},
    "python": {"-c"},
    "python3": {"-c"},
    "ruby": {"-e"},
}

POLICY_EXPLANATIONS = {
    "absolute_executable_blocked": {
        "title": "Absolute executable path blocked",
        "reason": "The command tried to run an executable by absolute path instead of using PATH.",
        "toggle": "Allow absolute executable paths",
    },
    "shell_blocked": {
        "title": "Shell blocked",
        "reason": "Shell entry points can chain commands, redirect files, and bypass narrow argv checks.",
        "toggle": "Allow shells",
    },
    "host_admin_blocked": {
        "title": "Host/admin command blocked",
        "reason": "The command can affect the host OS, permissions, desktop apps, or services.",
        "toggle": "Allow host/admin commands",
    },
    "remote_control_blocked": {
        "title": "Remote/control tool blocked",
        "reason": "The command can connect to or copy files from other machines.",
        "toggle": "Allow remote/control tools",
    },
    "container_blocked": {
        "title": "Container command blocked",
        "reason": "Container tools can start services, mount host paths, and create long-running host work.",
        "toggle": "Allow containers",
    },
    "host_install_blocked": {
        "title": "Host package manager blocked",
        "reason": "Host package managers install software outside the project workspace.",
        "toggle": "Allow host package managers",
    },
    "inline_eval_blocked": {
        "title": "Inline eval blocked",
        "reason": "Inline interpreter eval hides executable code inside a command argument.",
        "toggle": "Allow inline eval",
    },
    "network_blocked": {
        "title": "Network command blocked",
        "reason": "Network-capable commands are disabled unless explicitly allowed.",
        "toggle": "Allow network commands",
    },
    "install_blocked": {
        "title": "Package/tool install blocked",
        "reason": "Tool-environment installs are disabled unless explicitly allowed.",
        "toggle": "Allow package/tool installs",
    },
    "path_outside_agent_lab": {
        "title": "Path leaves agent_lab",
        "reason": "A path argument pointed outside the agent_lab sandbox root.",
        "toggle": "Turn off command safety policy",
    },
    "empty_command": {
        "title": "Empty command",
        "reason": "The spudex did not receive a command to run.",
        "toggle": "",
    },
}


def _is_under(path: Path, root: Path) -> bool:
    try:
        resolved = path.expanduser().resolve()
        root_resolved = root.expanduser().resolve()
    except Exception:
        return False
    return resolved == root_resolved or root_resolved in resolved.parents


def display_agent_path(path: Any) -> str:
    try:
        resolved = Path(path).expanduser().resolve()
        root = AGENT_LAB_DIR.resolve()
        if resolved == root:
            return "/"
        if root in resolved.parents:
            return "/" + str(resolved.relative_to(root))
    except Exception:
        pass
    return str(path or "")


def resolve_spudex_cwd(value: Any) -> Path:
    raw = str(value or "").strip()
    if not raw or raw in {".", "/"}:
        raw = str(AGENT_WORKSPACE_DIR)
    elif raw == "workspace" or raw == "/workspace":
        raw = str(AGENT_WORKSPACE_DIR)
    elif raw == "agent_lab" or raw == "/agent_lab":
        raw = str(AGENT_LAB_DIR)

    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = AGENT_LAB_DIR / raw.lstrip("/")
    resolved = candidate.resolve()
    if not _is_under(resolved, AGENT_LAB_DIR):
        raise ValueError("Spudex cwd must stay inside agent_lab.")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def resolve_spudex_file_path(value: Any, *, cwd: Path | None = None) -> Path:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Spudex file path is required.")
    if raw in {".", "/", "workspace", "/workspace"}:
        raise ValueError("Spudex file path must name a file, not a directory.")

    if raw.startswith("workspace/") or raw.startswith("/workspace/"):
        raw = raw.lstrip("/")
        candidate = AGENT_LAB_DIR / raw
    elif raw.startswith("agent_lab/") or raw.startswith("/agent_lab/"):
        raw = raw.lstrip("/")
        candidate = AGENT_LAB_DIR / raw.removeprefix("agent_lab/")
    else:
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            base = cwd if cwd is not None else AGENT_WORKSPACE_DIR
            candidate = base / candidate

    resolved = candidate.resolve()
    if not _is_under(resolved, AGENT_LAB_DIR):
        raise ValueError("Spudex file path must stay inside agent_lab.")
    if resolved.is_dir():
        raise ValueError("Spudex file path must name a file, not a directory.")
    return resolved


def normalize_argv(command: Any = None, argv: Any = None) -> List[str]:
    if isinstance(argv, (list, tuple)):
        return [str(item) for item in argv if str(item or "").strip()]
    if isinstance(command, (list, tuple)):
        return [str(item) for item in command if str(item or "").strip()]
    text = str(command or "").strip()
    if not text:
        return []
    return shlex.split(text)


def _looks_like_install(argv: List[str]) -> bool:
    if not argv:
        return False
    command = Path(argv[0]).name.lower()
    if command in {"pip", "pip3"} and len(argv) > 1 and argv[1].lower() == "install":
        return True
    if command in {"npm", "pnpm", "yarn"} and any(arg.lower() in {"install", "add"} for arg in argv[1:3]):
        return True
    if command == "uv" and any(arg.lower() in {"add", "pip"} for arg in argv[1:3]):
        return True
    return False


def _looks_like_host_install(argv: List[str]) -> bool:
    if not argv:
        return False
    command = Path(argv[0]).name.lower()
    return command in HOST_INSTALL_COMMANDS


def _looks_like_network(argv: List[str]) -> bool:
    if not argv:
        return False
    command = Path(argv[0]).name.lower()
    if command in NETWORK_COMMANDS:
        return True
    if command == "git" and len(argv) > 1:
        return argv[1].lower() in {"clone", "fetch", "pull", "push", "ls-remote", "submodule"}
    return False


def _uses_inline_eval(argv: List[str]) -> bool:
    if not argv:
        return False
    command = Path(argv[0]).name.lower()
    flags = INLINE_EVAL_COMMANDS.get(command)
    if not flags:
        return False
    return any(str(arg or "").strip().lower() in flags for arg in argv[1:3])


def _validate_path_arg(arg: str, cwd: Path) -> Tuple[bool, str]:
    text = str(arg or "").strip()
    if not text or text.startswith("-") or "://" in text:
        return True, ""
    if "/" not in text and "\\" not in text:
        return True, ""
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = cwd / candidate
    if not _is_under(candidate, AGENT_LAB_DIR):
        return False, f"Path argument `{text}` leaves agent_lab."
    return True, ""


def validate_spudex_command(argv: List[str], cwd: Path, settings: Dict[str, Any]) -> Dict[str, Any]:
    if not argv:
        return {"ok": False, "code": "empty_command", "message": "No command was provided."}

    command_path = Path(str(argv[0] or ""))
    command = command_path.name.lower()
    if command_path.is_absolute() and not bool(settings.get("allow_absolute_executables")):
        return {"ok": False, "code": "absolute_executable_blocked", "message": "Use commands from PATH, not absolute executables."}
    if command in SHELL_COMMANDS and not bool(settings.get("allow_shell_commands")):
        return {"ok": False, "code": "shell_blocked", "message": f"`{command}` shells are blocked for the Tater spudex sandbox."}
    if command in HOST_ADMIN_COMMANDS and not bool(settings.get("allow_host_admin_commands")):
        return {"ok": False, "code": "host_admin_blocked", "message": f"`{command}` is a host/admin command and is blocked for the Tater spudex sandbox."}
    if command in REMOTE_CONTROL_COMMANDS and not bool(settings.get("allow_remote_control")):
        return {"ok": False, "code": "remote_control_blocked", "message": f"`{command}` is a remote/control tool and is blocked for the Tater spudex sandbox."}
    if command in CONTAINER_COMMANDS and not bool(settings.get("allow_containers")):
        return {"ok": False, "code": "container_blocked", "message": f"`{command}` container commands are blocked for the Tater spudex sandbox."}
    if _looks_like_host_install(argv) and not bool(settings.get("allow_host_package_managers")):
        return {
            "ok": False,
            "code": "host_install_blocked",
            "message": f"`{command}` is a host package manager. Host app installs need a dedicated installer workflow, not the sandbox spudex.",
        }
    if _uses_inline_eval(argv) and not bool(settings.get("allow_inline_eval")):
        return {"ok": False, "code": "inline_eval_blocked", "message": "Inline interpreter eval is blocked. Put scripts in agent_lab and run the script file."}
    if _looks_like_network(argv) and not bool(settings.get("allow_network")):
        return {"ok": False, "code": "network_blocked", "message": f"`{command}` needs network access, which is disabled."}
    if _looks_like_install(argv) and not bool(settings.get("allow_installs")):
        return {"ok": False, "code": "install_blocked", "message": "Install commands are disabled for the Tater spudex sandbox."}

    for arg in argv[1:]:
        ok, message = _validate_path_arg(arg, cwd)
        if not ok:
            return {"ok": False, "code": "path_outside_agent_lab", "message": message}

    return {"ok": True}


def explain_policy_block(block: Dict[str, Any] | str) -> Dict[str, Any]:
    if isinstance(block, dict):
        code = str(block.get("code") or "").strip()
        message = str(block.get("message") or "").strip()
    else:
        code = str(block or "").strip()
        message = ""
    info = POLICY_EXPLANATIONS.get(code, {})
    title = str(info.get("title") or "Command blocked")
    reason = str(info.get("reason") or message or "The spudex policy rejected this command.")
    toggle = str(info.get("toggle") or "").strip()
    return {
        "code": code,
        "title": title,
        "reason": reason,
        "toggle": toggle,
        "message": message,
    }
