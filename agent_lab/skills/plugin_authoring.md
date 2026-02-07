# Agent Lab Plugin Authoring (Derived)
<!-- runtime_hash: 7e4d23bb-6d37-5bb8-b958-09d56b679275 -->

This guide reflects the current Tater runtime contracts.
Agent Lab plugins live in `agent_lab/plugins/` and must not write outside `agent_lab/`.

**Overview**
- Plugins are single-run tools invoked by the orchestrator and must return promptly.
- Each plugin module must expose a module-global `plugin` instance of `ToolPlugin`.
- Before authoring, scan 1–2 similar stable plugins in `plugins/` with `list_directory` + `read_file` to match patterns.

**File Layout**
- Create: `agent_lab/plugins/<plugin_id>.py`
- Must expose: `plugin = <ToolPlugin instance>`

**Minimal Example**
```python
from plugin_base import ToolPlugin
from plugin_result import action_success

class MyPlugin(ToolPlugin):
    name = "my_plugin"
    plugin_name = "My Plugin"
    version = "1.0.0"
    description = "What this does."
    platforms = ["webui", "discord"]
    usage = (
        "{\n"
        "  \"function\": \"my_plugin\",\n"
        "  \"arguments\": {\n"
        "    \"foo\": \"bar\"\n"
        "  }\n"
        "}\n"
    )

    async def handle_webui(self, args, llm_client, context=None):
        return action_success(
            facts={"foo": args.get("foo")},
            say_hint="Report the foo value only."
        )

plugin = MyPlugin()
```

**Required Metadata**
- `name`: unique id (prefer matching filename)
- `plugin_name`: human-friendly display name
- `version`: semantic version string
- `description`: short capability summary
- `platforms`: list of supported platforms
- `usage`: JSON tool-call schema string

Notes:
- `pretty_name` is not used; prefer `plugin_name`.
- `plugin_dec` is legacy, `description` is preferred.
- `plugin` must be a ToolPlugin instance, not a dict.
- Import ToolPlugin from `plugin_base` (not `toolplugin`).
- `platforms` must use supported ids: `webui`, `discord`, `irc`, `homeassistant`, `homekit`, `matrix`, `telegram`, `xbmc`, `automation`, `rss` (or `both`).

**Usage Schema Rules**
- `usage` must include every argument the plugin needs. Do not pass only a raw user prompt.
- Use the pattern:
  - `{"function": "<id>", "arguments": { ... }}`
- Do not include `origin` in `usage`; it is injected automatically.
- Keep argument defaults in the schema when possible (helps `get_plugin_help`).

Optional but recommended fields used by discovery and help:
- `when_to_use`, `common_needs`, `required_args`, `optional_args`, `example_calls`, `missing_info_prompts`
- `argument_schema` (JSON schema-like dict) for richer `get_plugin_help` output

**Platform Gating**
- `platforms` must list where the plugin is allowed to run.
- Allowed values: `webui`, `discord`, `irc`, `homeassistant`, `homekit`, `matrix`, `telegram`, `xbmc`, `automation`, `rss`.
- The special value `both` expands to all chat platforms.

**Handler Signatures**
`execute_plugin_call` inspects the handler name by platform. Common signatures:
- `handle_webui(self, args, llm_client, context=None)`
- `handle_discord(self, message, args, llm_client, context=None)`
- `handle_matrix(self, client, room, sender, body, args, llm_client, context=None)`
- `handle_irc(self, bot, channel, user, raw_message, args, llm_client, context=None)`
- `handle_telegram(self, update, args, llm_client, context=None)`
- `handle_homeassistant(self, args, llm_client, context=None)`
- `handle_homekit(self, args, llm_client, context=None)`
- `handle_xbmc(self, args, llm_client, context=None)`
- `handle_automation(self, args, llm_client)`

You can implement a shared internal method and call it from each handler.

**Result Contract**
Return structured results using `plugin_result` helpers.

Success:
```json
{
  "ok": true,
  "facts": {"key": "value"},
  "say_hint": "Facts-only narration guidance.",
  "suggested_followups": []
}
```

Failure:
```json
{
  "ok": false,
  "error": {"code": "missing_config", "message": "..."},
  "diagnosis": {"setting": "missing"},
  "needs": ["question to ask"],
  "say_hint": "Explain and ask for missing info."
}
```

Research-style outputs can use `plugin_result.research_success`.

**Artifacts**
Include `artifacts` for images/audio/files. Each artifact dict can contain:
- `type`: `image|audio|video|file`
- `name`, `mimetype`
- `bytes` or `blob_key`

**Settings (Agent Lab)**
- Expose settings via:
  - `settings_category = "My Plugin Settings"`
  - `required_settings = {"key": {"label": "...", "type": "text", "default": ""}}`
- Supported UI types: `text`, `password`, `number`, `checkbox`, `select`, `file`, `textarea`.
- `required_settings` must be a dict (not a list). Each key maps to a dict with `label`, `type`, and `default` (plus optional `description`/`options`).
- Read settings from Redis key: `exp:plugin_settings:<Category>`.

Example:
```python
from helpers import redis_client
value = redis_client.hget("exp:plugin_settings:My Plugin Settings", "api_key")
```

**Auth / OAuth Secrets (only if needed)**
If your plugin needs a token or client secret, declare it in `required_settings`
so it appears in the Agent Lab settings UI. Example:
```python
from plugin_base import ToolPlugin
from helpers import redis_client

class GmailReader(ToolPlugin):
    name = "gmail_reader"
    plugin_name = "Gmail Reader"
    version = "1.0.0"
    description = "Read-only access to Gmail."
    platforms = ["webui", "discord"]
    settings_category = "Gmail Reader"
    required_settings = {
        "OAUTH_TOKEN": {
            "label": "OAuth Access Token",
            "type": "password",
            "default": "",
            "description": "Paste the OAuth access token for the Gmail API."
        }
    }
    usage = (
        "{\\n"
        "  \\\"function\\\": \\\"gmail_reader\\\",\\n"
        "  \\\"arguments\\\": {\\n"
        "    \\\"max_results\\\": 5\\n"
        "  }\\n"
        "}\\n"
    )

    def _get_token(self) -> str:
        raw = redis_client.hget(\"exp:plugin_settings:Gmail Reader\", \"OAUTH_TOKEN\")
        return (raw or \"\").strip()
```

**Enable/Disable**
Agent Lab enablement is stored in Redis hash `exp:plugin_enabled`.

**Dependencies**
Declare pip dependencies at module level:
```python
dependencies = ["requests>=2", "pillow"]
```
They are unioned into `agent_lab/requirements.txt` and installed on validation.

**Validation and Discovery**
- Loader scans `agent_lab/plugins/*.py`.
- A module-global `plugin` instance is required.
- Validation checks syntax, required metadata, and dependencies.

**Authoring Rule**
- Use `create_plugin` for Agent Lab plugins (do not use `write_file` for plugins).
- Prefer a stable plugin example as a template when possible.

**Working Checklist**
1. Plugin file uses `agent_lab/plugins/<id>.py` and exposes `plugin = <ToolPlugin instance>`.
2. Metadata fields are set: `name`, `plugin_name`, `version`, `description`, `platforms`, `usage`.
3. Handler signature uses `llm_client` (or `llm`), not `llp_client`.
4. `usage` includes full arguments the plugin needs (no raw user prompt pass-through).
5. If settings/secrets are needed, `settings_category` + `required_settings` are defined and read from `exp:plugin_settings:<Category>`.
   `required_settings` is a dict of dicts (no lists).
6. Validation passes in the Agent Lab tab before enabling.

**Safety**
- Do not write outside `agent_lab/`.
- Avoid long-running loops.
- Never include secrets in returned fields.
