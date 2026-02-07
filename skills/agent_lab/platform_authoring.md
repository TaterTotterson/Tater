# Agent Lab Platform Authoring (Derived)
<!-- runtime_hash: 7e4d23bb-6d37-5bb8-b958-09d56b679275 -->

This guide reflects the current Tater runtime contracts.
Agent Lab platforms live in `agent_lab/platforms/` and must not write outside `agent_lab/`.

**Overview**
- Platforms are long-running services started and stopped from the Agent Lab tab.
- They run in a background thread and must support cooperative shutdown.

**Architecture Reminder**
- Plugins are short-lived, single-execution tools with structured return payloads.
- Platforms are long-running bridges that cooperate with `stop_event`.
- Platforms emit or react to events; they do not replace plugins.

**File Layout**
- Create: `agent_lab/platforms/<platform_key>.py`
- Must expose a module-level `PLATFORM` dict and `run(stop_event=None)` function.

**Tool Call Format**
- Use `create_platform` with `name` plus `code_lines` (preferred) or `code`/`code_b64`.
- Do NOT use `manifest` / `code_files` schemas; they are not accepted by the runtime.
- Avoid triple-quoted strings inside `code_lines` (they break JSON). Prefer single quotes or comments.
- Each `code_lines` entry must be a single line. Do not include embedded `\n`.

**Golden Template (code_lines-friendly)**
Use this shape first, then fill in platform-specific logic:
```json
{
  "function": "create_platform",
  "arguments": {
    "name": "my_platform",
    "code_lines": [
      "import time",
      "from helpers import redis_client",
      "",
      "PLATFORM = {",
      "    \"label\": \"My Platform\",",
      "    \"required\": {}",
      "}",
      "",
      "def run(stop_event=None):",
      "    while stop_event is None or not stop_event.is_set():",
      "        # poll or serve here",
      "        time.sleep(0.5)"
    ]
  }
}
```

**Validator-Required Exports (Agent Lab)**
```python
PLATFORM = {}

def run(stop_event=None):
    pass
```
Notes:
- `PLATFORM` must be a dict.
- `run` must be callable for validation.
- `run` should accept `stop_event` keyword usage (`run(stop_event=...)`) for runtime compatibility.
- Missing `PLATFORM` or callable `run` causes validation failure.

**Recommended PLATFORM Metadata (UI)**
Use this structure so Agent Lab can render platform settings:
```python
PLATFORM = {
    "label": "My Platform",
    "required": {
        "setting_key": {
            "label": "Setting Label",
            "type": "text",
            "default": "",
            "description": "What this setting does."
        }
    }
}
```

**Minimal Valid Template**
```python
import time

PLATFORM = {
    "label": "Example Platform",
    "required": {}
}

def run(stop_event=None):
    while stop_event is None or not stop_event.is_set():
        time.sleep(0.5)
```

**Settings Model (Agent Lab)**
- Settings are stored in Redis hash: `exp:platform_settings:<platform_key>`.
- UI input types supported: `text`, `password`, `number`, `checkbox`, `select`, `file`, `textarea`.
- Access settings inside the platform with Redis:
```python
from helpers import redis_client
port = redis_client.hget("exp:platform_settings:my_platform", "port")
```
Notes:
- Do not hard-code a different settings key; use the platform key (filename stem).
- Use `helpers.redis_client` (it already respects `REDIS_HOST/REDIS_PORT`).

**Safe Redis Settings Decode Pattern**
Always decode Redis values safely when numeric parsing is required:
```python
raw = redis_client.hget("exp:platform_settings:my_platform", "port")
if isinstance(raw, (bytes, bytearray)):
    raw = raw.decode("utf-8", errors="ignore")
port = int(raw or 0)
```

**Running State**
- The Agent Lab UI stores running state in `exp:<platform_key>_running`.
- Your platform does not need to update this key; it is managed by the UI.

**Lifecycle and Shutdown**
- `run(stop_event)` is called in a daemon thread.
- Always check `stop_event.is_set()` regularly and exit cleanly.
- Any blocking call (socket accept/recv, HTTP request, websocket read, queue get, etc.) should use a timeout <= 2 seconds so shutdown stays responsive.
- If using a blocking server, set a short timeout and loop so shutdown can be honored.

**Golden Loop with Exception Containment**
Use this loop shape to avoid platform death on transient errors:
```python
while stop_event is None or not stop_event.is_set():
    try:
        # poll or serve here
        time.sleep(0.5)
    except Exception:
        time.sleep(1.0)
```

**Do / Don't (Specific)**
- Do keep the filename stem and platform key aligned.
- Do read settings from `exp:platform_settings:<platform_key>`.
- Do keep blocking calls on short timeouts so stop checks run frequently.
- Do declare third-party modules in `dependencies`.
- Do keep extra worker loops tied to `stop_event` and shut them down cleanly.
- Don't create your own raw `Redis()` client with localhost defaults.
- Don't start extra background threads unless necessary; if used, they must stop with `stop_event`.
- Don't spawn subprocesses that are not tied to shutdown behavior.
- Don't block forever without a timeout.
- Don't busy-spin (`while ...: pass`); always sleep or wait.
- Don't hard-code a mismatched settings key.
- Don't call WebUI HTTP endpoints from Agent Lab platforms.

**Authoring Rule**
- Use `create_platform` for Agent Lab platforms (do not use `write_file` for platforms).
- Prefer a stable platform example as a template when possible.

**Common Platform Patterns (Pick One)**
1. Poller
   Periodically read settings/state, check external source, write status or queue notifications.
2. Webhook/Socket Server
   Run a server loop with short read/accept timeouts so `stop_event` can interrupt quickly.
3. Bridge Worker
   Consume one inbound source and forward to one outbound target with retries/backoff.

**Validation Checklist (Mental)**
1. Is file path `agent_lab/platforms/<platform_key>.py`?
2. Is module-level `PLATFORM` a dict?
3. Does `run(stop_event=None)` exist and exit cleanly on stop?
4. If settings are used, do keys come from `exp:platform_settings:<platform_key>`?
5. Are any third-party imports listed in `dependencies`?
6. Does validation pass before start?

**Dependencies**
Declare pip dependencies at module level:
```python
dependencies = ["requests>=2"]
```
They are unioned into `agent_lab/requirements.txt` and auto-installed on validation.

**Validation**
Agent Lab validates:
- Python syntax
- Presence of `PLATFORM` dict
- Presence of `run()` function
- Dependency availability
Platforms do not start if validation fails.

**Safety**
- No writes outside `agent_lab/`.
- No uncontrolled background processes beyond the platform thread.
