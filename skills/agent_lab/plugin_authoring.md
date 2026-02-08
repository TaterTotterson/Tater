# Agent Lab Plugin Authoring (Derived)
<!-- runtime_hash: 7e4d23bb-6d37-5bb8-b958-09d56b679275 -->

This guide reflects the current Tater runtime contracts.
Agent Lab plugins live in `agent_lab/plugins/` and must not write outside `agent_lab/`.

**Overview**
- Plugins are single-run tools invoked by the orchestrator and must return promptly.
- Each plugin module must expose a module-global `plugin` instance of `ToolPlugin`.

**File Layout**
- Create: `agent_lab/plugins/<plugin_id>.py`
- Must expose: `plugin = <ToolPlugin instance>`

**Tool Call Format**
- Use `create_plugin` with `name` plus `code_lines` (preferred) or `code`/`code_b64`.
- Do NOT use `manifest` / `code_files` schemas; they are not accepted by the runtime.
- Avoid triple-quoted strings inside `code_lines` (they break JSON). Prefer single quotes or comments.
- Each `code_lines` entry must be a single line. Do not include embedded `\n`.

**Golden Template (code_lines-friendly)**
Use this shape first, then fill in plugin-specific details:
```json
{
  "function": "create_plugin",
  "arguments": {
    "name": "my_plugin",
    "code_lines": [
      "from plugin_base import ToolPlugin",
      "from plugin_result import action_success",
      "",
      "class MyPlugin(ToolPlugin):",
      "    name = \"my_plugin\"",
      "    plugin_name = \"My Plugin\"",
      "    version = \"1.0.0\"",
      "    description = \"What this does.\"",
      "    platforms = [\"webui\"]",
      "    usage = '{\"function\":\"my_plugin\",\"arguments\":{\"foo\":\"bar\"}}'",
      "    when_to_use = \"Use when the user asks to do X.\"",
      "    waiting_prompt_template = \"Write a friendly message telling {mention} you're starting now. Only output that message.\"",
      "",
      "    async def handle_webui(self, args, llm_client, context=None):",
      "        return action_success(facts={\"foo\": args.get(\"foo\")}, say_hint=\"Report the foo value only.\")",
      "",
      "plugin = MyPlugin()"
    ]
  }
}
```

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
- `name`: unique id and must match the filename stem exactly (`agent_lab/plugins/<name>.py`)
- `plugin_name`: human-friendly display name
- `version`: semantic version string
- `description`: short capability summary
- `platforms`: list of supported platforms
- `usage`: JSON tool-call schema string
- `when_to_use`: short guidance sentence for tool matching and discovery

Notes:
- `pretty_name` is not used; prefer `plugin_name`.
- `plugin_dec` is legacy, `description` is preferred.
- `plugin` must be a ToolPlugin instance, not a dict.
- `name` must be a safe id (letters/numbers/underscore/hyphen). Lowercase recommended.
- Import ToolPlugin from `plugin_base` (not `toolplugin`).
- `platforms` must use supported ids: `webui`, `discord`, `irc`, `homeassistant`, `homekit`, `matrix`, `telegram`, `xbmc`, `automation`, `rss` (or `both`).

**Usage Schema Rules**
- `usage` must include every argument the plugin needs. Do not pass only a raw user prompt.
- Do not use a plain-English sentence for `usage`; it must be JSON schema text.
- Use the pattern:
  - `{"function": "<id>", "arguments": { ... }}`
- Do not include `origin` in `usage`; it is injected automatically.
- Keep argument defaults in the schema when possible (helps `get_plugin_help`).
- When using `code_lines`, keep `usage` as a single-line JSON string to avoid quoting/escape issues.
  Example: `usage = '{"function":"my_plugin","arguments":{}}'`

**Do / Don't (Specific)**
- Do keep `usage` and `name` aligned exactly (same function id string).
- Do keep list/dict literals complete on one `code_lines` line when possible.
- Do implement `handle_<platform>` for every platform listed in `platforms`.
- Do return `action_success(...)` / `action_failure(...)`, not ad-hoc dicts.
- Don't define `run()`; it is ignored by the runtime.
- Don't include `origin` in `usage`; it is injected automatically.
- Don't split one Python statement across many `code_lines` entries.

Optional but recommended fields used by discovery and help:
- `common_needs`, `required_args`, `optional_args`, `example_calls`, `missing_info_prompts`
- `argument_schema` (JSON schema-like dict) for richer `get_plugin_help` output

**Waiting Prompt (required)**
Always include `waiting_prompt_template` as an **instruction** that tells the LLM what to say.
It should include wording like “Write …” and “Only output that message.”
Example:
```python
waiting_prompt_template = (
    "Write a friendly message telling {mention} you’re starting the task now. "
    "Only output that message."
)
```

**Platform Gating**
- `platforms` must list where the plugin is allowed to run.
- Allowed values: `webui`, `discord`, `irc`, `homeassistant`, `homekit`, `matrix`, `telegram`, `xbmc`, `automation`, `rss`.
- The special value `both` expands to all chat platforms.
- If you are creating a plugin from a specific platform, include that platform in `platforms`
  and implement its handler (e.g., `handle_webui` when created from WebUI).

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

Important:
- Do NOT define a `run()` method; it will not be called. Use `handle_<platform>` instead.

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

**AI-Generated Text (optional)**
If the plugin should generate text (e.g., jokes, summaries), use `llm_client` inside the handler:
```python
async def handle_discord(self, message, args, llm_client, context=None):
    resp = await llm_client.chat(
        messages=[{"role": "system", "content": "Write one short, clean joke about hukked."},
                  {"role": "user", "content": "Tell me a joke."}]
    )
    joke = (resp.get("message") or {}).get("content", "").strip() or "No joke generated."
    return action_success(facts={"joke": joke}, say_hint=joke)
```
When using `code_lines`, keep the `messages` list on ONE line to avoid missing commas:
```python
resp = await llm_client.chat(messages=[{"role":"system","content":"..."},{"role":"user","content":"..."}])
```
Do not split list/dict literals across multiple `code_lines` entries.
Rules:
- If the user explicitly requests **AI‑generated** content, you must call `llm_client` at runtime.
- Do not hardcode a static list of jokes/lines when AI‑generated output is required.

**Common Plugin Patterns (Pick One)**
1. API Fetch + Summarize
   Use settings for auth, call external API, normalize data, return concise `facts` and `say_hint`.
2. AI Generator
   Validate inputs, call `llm_client.chat(...)`, sanitize fallback text, return generated content in `facts`.
3. Action + Confirmation
   Trigger one deterministic action (queue/send/schedule/control), return clear confirmation with key args echoed.

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
    usage = "{\"function\":\"gmail_reader\",\"arguments\":{\"max_results\":5}}"

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
- `waiting_prompt_template` must be explicitly defined on the plugin.
- Waiting prompt phrasing quality may be reported as a warning during validation.

**Authoring Rule**
- Use `create_plugin` for Agent Lab plugins (do not use `write_file` for plugins).
- Prefer a stable plugin example as a template when possible.

**Validation Checklist (Mental)**
1. Did I set required metadata: `name`, `plugin_name`, `version`, `description`, `platforms`, `usage`, `when_to_use`, `waiting_prompt_template`?
2. Does `usage` function id exactly match `name`?
3. Did I implement `handle_<platform>` for each listed platform?
4. Does handler signature use `llm_client` (or `llm`), not `llp_client`?
5. If settings are needed, are `settings_category` and dict-shaped `required_settings` defined and read from `exp:plugin_settings:<Category>`?
6. Are returns using `action_success` / `action_failure` with concise `say_hint`?
7. Does validation pass in Agent Lab before enabling?

**Safety**
- Do not write outside `agent_lab/`.
- Avoid long-running loops.
- Never include secrets in returned fields.
