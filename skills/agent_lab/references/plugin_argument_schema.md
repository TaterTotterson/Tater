# Plugin Pattern: Argument Schema

Use this when you want deterministic argument guidance in `get_plugin_help`.

## Schema Shape
```python
argument_schema = {
    "type": "object",
    "properties": {
        "query": {"type": "string", "description": "Search query."},
        "limit": {"type": "number", "description": "Max items to return."}
    },
    "required": ["query"]
}
```

## Rules
- Keep `usage` and `argument_schema` aligned.
- Put only real required fields in `required`.
- Include concise descriptions for each property.
- Do not add `origin` to schema; runtime injects it.
