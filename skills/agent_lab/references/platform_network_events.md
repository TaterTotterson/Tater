# Platform Pattern: Network/Event Servers

Use this when a platform runs webhook/socket/server style event loops.

## Core Rules
- Always honor `stop_event` quickly.
- Use short timeouts for accept/read loops.
- Keep transport code isolated from business logic.

## Loop Pattern
```python
while stop_event is None or not stop_event.is_set():
    try:
        # accept/read with timeout
        pass
    except Exception:
        time.sleep(0.5)
```

## Practical Guidance
- For HTTP/webhook loops, use short request timeouts.
- For socket/websocket loops, avoid blocking forever.
- Validate incoming payloads before forwarding/processing.
- Log concise operational errors; keep loop alive.
