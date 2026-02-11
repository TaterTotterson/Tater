# Agent Lab Platform Authoring (Compact)
<!-- runtime_hash: 2026-02-10 -->

This guide is intentionally compact for planner context limits.
Use it when creating files under `agent_lab/platforms/`.

## What To Create
- File path: `agent_lab/platforms/<platform_key>.py`
- Required module exports:
  - `PLATFORM` (dict)
  - `run(stop_event=None)` (callable)

## Required Runtime Contract
- `PLATFORM` must exist and be a dict.
- `run` must exist and be callable.
- `run` should check `stop_event.is_set()` frequently and exit cleanly.
- Keep blocking operations on short timeouts (about <=2s) so shutdown is responsive.

## Creation Tool Call
Preferred form:
```json
{"function":"create_platform","arguments":{"name":"my_platform","code_lines":["..."]}}
```

Also supported:
- `code`
- `overwrite` (set `true` only when explicitly replacing existing file)

Compatibility note:
- `manifest` and `code_files` may be adapted by compatibility logic, but do not rely on them.
- Use `name` + `code_lines` (preferred) for deterministic behavior.

## Minimal Valid Template
```python
import time

PLATFORM = {
    "label": "My Platform",
    "required": {}
}

def run(stop_event=None):
    while stop_event is None or not stop_event.is_set():
        time.sleep(0.5)
```

## Settings Model
- Platform settings are stored in Redis hash:
  - `exp:platform_settings:<platform_key>`
- Read settings through `helpers.redis_client`.
- Decode/parse values defensively (bytes/string/int handling).

## Dependencies
- Declare third-party dependencies at module level when needed:
```python
dependencies = ["requests>=2"]
```
- Validation may auto-install missing declared dependencies.

## Validation Behavior
`validate_platform` checks:
- Python syntax/importability
- `PLATFORM` exists and is dict
- `run` exists and is callable
- declared dependencies availability

## Safe Loop Pattern
```python
while stop_event is None or not stop_event.is_set():
    try:
        # poll or process one unit of work
        pass
    except Exception:
        # contain transient failures; avoid thread death
        pass
```

## Common Failure Causes
- missing `PLATFORM` or `run`
- `PLATFORM` is not a dict
- long blocking calls that ignore shutdown
- hard-coded settings key that does not match filename stem
- unmanaged background workers that outlive `stop_event`

## Advanced References (Load Only If Needed)
- Webhook/socket/server/event bridges:
  - `skills/agent_lab/references/platform_network_events.md`
- Pollers/workers/retry loops:
  - `skills/agent_lab/references/platform_pollers_workers.md`
