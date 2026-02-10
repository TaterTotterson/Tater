# Plugin Pattern: API + Auth

Use this when a plugin must call external HTTP APIs or webhooks.

## Core Rules
- Store secrets in `required_settings`, not in source.
- Read settings from plugin config (runtime injects values from settings UI).
- Validate required inputs before network calls.
- Use short timeouts and return structured failures.

## Recommended Settings Shape
```python
settings_category = "My API Settings"
required_settings = {
    "api_base_url": {"label": "API Base URL", "type": "text", "default": ""},
    "api_token": {"label": "API Token", "type": "password", "default": ""}
}
```

## Handler Sketch
```python
import requests
from plugin_result import action_success, action_failure

async def handle_webui(self, args, llm_client, context=None):
    query = str(args.get("query") or "").strip()
    if not query:
        return action_failure(code="missing_query", message="Missing query.", needs=["What should I query?"])
    try:
        resp = requests.get("https://api.example.com/search", params={"q": query}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return action_failure(code="api_error", message=f"API request failed: {e}")
    return action_success(facts={"results": data}, say_hint="Summarize top results only.")
```

## Failure Style
- Include `code` and `message`.
- Add `needs` only when user input is required.
- Avoid raw tracebacks in user-facing messages.
