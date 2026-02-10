# Plugin Pattern: HTTP Resilience

Use this for API/network plugins (`requests` / `httpx`).

## Core Rules
- Always set explicit timeouts.
- Handle non-2xx status codes explicitly.
- Retry only transient failures (timeouts, 429, 5xx).
- Use bounded retries with backoff.

## Requests Pattern
```python
import time
import requests

for attempt in range(3):
    try:
        r = requests.get(url, headers=headers, timeout=12)
        if r.status_code == 429 and attempt < 2:
            time.sleep(1.5 * (attempt + 1))
            continue
        r.raise_for_status()
        data = r.json()
        break
    except requests.Timeout:
        if attempt == 2:
            raise
        time.sleep(1.0 * (attempt + 1))
```

## Failure Mapping
Map errors to `action_failure` with stable `code` values:
- `network_timeout`
- `upstream_rate_limited`
- `upstream_http_error`
- `invalid_response`
