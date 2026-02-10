# Plugin Pattern: Result Contract

Use this when you need reliable success/failure behavior and follow-up prompts.

## Success Contract
Use `action_success(...)` with structured fields:
- `facts`: machine-usable output values
- `say_hint`: short narration guidance
- `suggested_followups` (optional)
- `artifacts` (optional)

Example:
```python
return action_success(
    facts={"count": len(items)},
    say_hint=f"Found {len(items)} items.",
)
```

## Failure Contract
Use `action_failure(...)` for recoverable errors.
Include:
- `code`: stable short error id
- `message`: clear failure summary
- `needs`: only when user input is required next
- `diagnosis` (optional): machine details
- `say_hint` (optional): concise user-facing phrasing

Example:
```python
return action_failure(
    code="missing_room",
    message="No room target provided.",
    needs=["Which room should I send this to?"],
)
```

## Rules
- Do not return ad-hoc dict shapes.
- Use `needs` only for actionable missing inputs.
- Keep `say_hint` concise and deterministic.
