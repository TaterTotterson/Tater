# Agent Lab Platform Authoring (Derived)
<!-- runtime_hash: 7e4d23bb-6d37-5bb8-b958-09d56b679275 -->

This guide reflects the current Tater runtime contracts.
Agent Lab platforms live in `agent_lab/platforms/` and must not write outside `agent_lab/`.

**Overview**
- Platforms are long-running services started and stopped from the Agent Lab tab.
- They run in a background thread and must support cooperative shutdown.
- Before authoring, scan 1–2 similar stable platforms in `platforms/` with `list_directory` + `read_file` to match patterns.

**File Layout**
- Create: `agent_lab/platforms/<platform_key>.py`
- Must expose a module-level `PLATFORM` dict and `run(stop_event=None)` function.

**Required Exports (Agent Lab)**
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

def run(stop_event=None):
    while stop_event is None or not stop_event.is_set():
        ...
```
If `PLATFORM` is missing, validation will fail with **Invalid (PLATFORM)**.

**Minimal Valid Template**
```python
PLATFORM = {
    "label": "Example Platform",
    "required": {}
}

def run(stop_event=None):
    while stop_event is None or not stop_event.is_set():
        pass
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

**Running State**
- The Agent Lab UI stores running state in `exp:<platform_key>_running`.
- Your platform does not need to update this key; it is managed by the UI.

**Lifecycle and Shutdown**
- `run(stop_event)` is called in a daemon thread.
- Always check `stop_event.is_set()` regularly and exit cleanly.
- If using a blocking server, set a short timeout and loop so shutdown can be honored.

**Authoring Rule**
- Use `create_platform` for Agent Lab platforms (do not use `write_file` for platforms).
 - Prefer a stable platform example as a template when possible.

**Working Checklist**
1. Platform file uses `agent_lab/platforms/<platform_key>.py`.
2. `PLATFORM` dict is defined and includes a `label` and `required` settings map (even if empty).
3. `run(stop_event=None)` exists and stops cleanly when `stop_event.is_set()`.
4. If settings are needed, they are read from `exp:platform_settings:<platform_key>`.
5. Validation passes in the Agent Lab tab before starting.

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

**Avoid These Errors**
- Do not create a raw `Redis()` client with default localhost settings.
- Do not hard-code a mismatched settings key (e.g., `"discord_joke"` if the file is `joke_server.py`).
- Always declare any third‑party imports in `dependencies`.
- Avoid calling WebUI HTTP endpoints like `/api/plugin/...` from platforms; use direct logic or a plugin that calls the platform instead.

**Safety**
- No writes outside `agent_lab/`.
- No uncontrolled background processes beyond the platform thread.
