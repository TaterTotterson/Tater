# Agent Lab Plugin Authoring (Compact)
<!-- runtime_hash: 2026-02-10 -->

This guide is intentionally compact for planner context limits.
Use it when creating files under `agent_lab/plugins/`.

## What To Create
- File path: `agent_lab/plugins/<plugin_id>.py`
- Required module export: `plugin = <ToolPlugin instance>`
- Plugin id must match filename stem exactly.

## Required Runtime Contract
- Import `ToolPlugin` from `plugin_base`.
- Define a class that subclasses `ToolPlugin`.
- Define module-level `plugin` assigned to an instance of that class (not a dict).
- Required plugin fields (must be non-empty/valid):
  - `name` (safe id, must equal filename stem)
  - `version`
  - `platforms` (non-empty list)
  - `description`
  - `usage` (string)
  - `waiting_prompt_template` (explicitly set on class)
- `platforms` values must be supported ids:
  - `webui`, `discord`, `irc`, `homeassistant`, `homekit`, `matrix`, `telegram`, `xbmc`, `automation`, `rss`, or `both`.

## Required Style Rules
- `usage` must be canonical single-line JSON.
- `usage` function id must equal `name`.
- Use this exact shape:
  - `{"function":"<plugin_id>","arguments":{...}}`
- Keep `code_lines` entries as single lines with no embedded `\n`.
- Prefer single-quoted Python strings for JSON literals in generated code lines.

## Creation Tool Call
Preferred form:
```json
{"function":"create_plugin","arguments":{"name":"my_plugin","code_lines":["..."]}}
```

Also supported:
- `code`
- `overwrite` (set `true` only when explicitly replacing existing file)

Compatibility note:
- `manifest` and `code_files` may be adapted by runtime compatibility logic, but do not rely on them.
- Use `name` + `code_lines` (preferred) for deterministic behavior.

## Validation Behavior
`validate_plugin` will fail if any of these are missing/invalid:
- module-level `plugin`
- ToolPlugin subclass/import contract
- `name`, `version`, `platforms`, `description`, `usage`, `waiting_prompt_template`
- invalid platform ids
- dependencies that cannot be imported after auto-install attempt

`waiting_prompt_template` should be an instruction to the LLM and constrain output.
Required pattern:
- Friendly/casual "please wait" progress message for `{mention}`.
- Must NOT be the final task output prompt.
- Must constrain output to only that status message.
Good pattern:
- `"Write a friendly, casual message telling {mention} you are working on it now. Only output that message."`

## Minimal Example
```python
from plugin_base import ToolPlugin
from plugin_result import action_success

class MyPlugin(ToolPlugin):
    name = "my_plugin"
    plugin_name = "My Plugin"
    version = "1.0.0"
    description = "Does one small task."
    platforms = ["webui"]
    usage = '{"function":"my_plugin","arguments":{"text":"hello"}}'
    waiting_prompt_template = "Write a friendly, casual message telling {mention} you are working on it now. Only output that message."
    when_to_use = "Use when the user asks for the my_plugin task."

    async def handle_webui(self, args, llm_client, context=None):
        text = str(args.get("text") or "").strip()
        return action_success(
            facts={"text": text},
            say_hint=f"Done. text={text}" if text else "Done.",
        )

plugin = MyPlugin()
```

## Recommended Return Contract
- Success: `action_success(...)`
- Failure: `action_failure(...)`
- Keep payloads structured (`facts`, `needs`, `say_hint`) for reliable narration.

## Optional Metadata (Recommended)
- `when_to_use`
- `required_args`, `optional_args`
- `example_calls`
- `missing_info_prompts`
- `argument_schema`
- `settings_category`, `required_settings` (must be dict if provided)

## Advanced References (Load Only If Needed)
- API/Auth integrations:
  - `skills/agent_lab/references/plugin_api_auth.md`
- AI-generated text/content:
  - `skills/agent_lab/references/plugin_ai_generation.md`
- Media/file artifacts:
  - `skills/agent_lab/references/plugin_artifacts.md`
- Structured success/failure payloads:
  - `skills/agent_lab/references/plugin_result_contract.md`
- HTTP timeout/retry/rate-limit handling:
  - `skills/agent_lab/references/plugin_http_resilience.md`
- Settings and secret patterns:
  - `skills/agent_lab/references/plugin_settings_and_secrets.md`
- `waiting_prompt_template` guidance and default:
  - `skills/agent_lab/references/plugin_waiting_prompt_template.md`
- Multi-platform handler signatures and shared-core pattern:
  - `skills/agent_lab/references/plugin_multiplatform_handlers.md`
- Notification routing/targets/origin patterns:
  - `skills/agent_lab/references/plugin_notification_delivery.md`
- `argument_schema` design for deterministic help output:
  - `skills/agent_lab/references/plugin_argument_schema.md`

## Common Failure Causes
- `name` does not match filename stem
- `plugin` is a dict, missing, or not a ToolPlugin instance
- multiline or invalid `usage`
- missing explicit `waiting_prompt_template`
- unsupported platform id
