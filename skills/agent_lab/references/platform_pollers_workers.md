# Platform Pattern: Pollers and Workers

Use this when a platform polls feeds/APIs or runs queue workers.

## Core Rules
- Poll at bounded intervals (`time.sleep(...)`).
- Use retry + backoff for transient upstream failures.
- Avoid unbounded tight loops.
- Keep worker shutdown tied to `stop_event`.

## Poller Skeleton
```python
while stop_event is None or not stop_event.is_set():
    try:
        # one poll cycle
        pass
        time.sleep(5)
    except Exception:
        time.sleep(1)
```

## Queue Worker Notes
- Process one item at a time.
- Catch per-item exceptions so worker survives bad items.
- Keep item processing idempotent when possible.
