# Plugin Pattern: waiting_prompt_template

Use this when creating any `ToolPlugin` class.

## Purpose
- `waiting_prompt_template` is a status-message instruction, not the final task response.
- It exists so the runtime can show a short "working on it" message while the plugin runs.

## Required Behavior
- Include `{mention}` in the template.
- Ask for one friendly, casual progress line.
- Constrain output to only that status line.
- Do not ask the user to provide wording.

## Recommended Default
```python
waiting_prompt_template = "Write a friendly, casual message telling {mention} you are working on it now. Only output that message."
```

## Optional Themed Variant
Use only when a theme is obvious from plugin purpose:
```python
waiting_prompt_template = "Write one friendly, casual line telling {mention} you're preparing this result now. Only output that message."
```

## Rules
- Keep this deterministic and reusable.
- Never use this field to ask clarification questions.
- Never make this field the plugin's final user-facing answer.
