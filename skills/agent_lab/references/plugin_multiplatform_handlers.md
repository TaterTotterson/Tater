# Plugin Pattern: Multi-Platform Handlers

Use this when one plugin should run on multiple platforms.

## Core Rules
- `platforms` must include each supported platform id.
- Implement matching `handle_<platform>` methods.
- Keep shared logic in one internal helper to avoid drift.

## Common Handler Signatures
- `handle_webui(self, args, llm_client, context=None)`
- `handle_discord(self, message, args, llm_client, context=None)`
- `handle_matrix(self, client, room, sender, body, args, llm_client, context=None)`
- `handle_irc(self, bot, channel, user, raw_message, args, llm_client, context=None)`
- `handle_telegram(self, update, args, llm_client, context=None)`
- `handle_homeassistant(self, args, llm_client, context=None)`
- `handle_homekit(self, args, llm_client, context=None)`
- `handle_xbmc(self, args, llm_client, context=None)`
- `handle_automation(self, args, llm_client)`

## Pattern
Implement `_run_core(args, origin)` once, then call it from each handler.
