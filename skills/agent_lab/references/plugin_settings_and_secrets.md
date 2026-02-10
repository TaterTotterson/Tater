# Plugin Pattern: Settings and Secrets

Use this when plugins need API keys, URLs, toggles, or defaults.

## `required_settings` Shape
```python
settings_category = "My Plugin Settings"
required_settings = {
    "API_BASE_URL": {
        "label": "API Base URL",
        "type": "text",
        "default": ""
    },
    "API_TOKEN": {
        "label": "API Token",
        "type": "password",
        "default": ""
    }
}
```

## Rules
- Keep secrets in settings, never hardcoded in source.
- Validate required settings before calling external services.
- Return `action_failure(..., needs=[...])` when critical settings are missing.
- Prefer explicit defaults for non-secret values.

## Recommended Setting Types
- `text`, `password`, `number`, `checkbox`, `select`, `textarea`, `file`
