# plugins/example_plugin.py
import os
import json
import asyncio
import logging
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import extract_json, redis_client, get_tater_name

load_dotenv()
logger = logging.getLogger("example_plugin")
logger.setLevel(logging.INFO)


class ExamplePlugin(ToolPlugin):
    """
    Short description of what this plugin does.

    Example natural asks:
      - "example: do the thing with X"
      - "hey tater, run the example tool for Y"
    """
    name = "example_plugin"  # this is what the function name will be
    usage = (
        "{\n"
        '  "function": "example_plugin",\n'
        '  "arguments": {"input": "<some value>"}\n'
        "}\n"
    )
    description = (
        "Tool to <describe in one sentence what it does>. "
        "The assistant should call this when the user asks about <X>."
    )
    pretty_name = "Example Plugin"
    settings_category = "Example Plugin"

    # Optional plugin-specific settings stored in Redis (same style as web_search)
    required_settings = {
        "EXAMPLE_API_KEY": {
            "label": "Example API Key",
            "type": "string",
            "default": "",
        },
        # Add more settings as needed
    }

    waiting_prompt_template = (
        "Write a short, friendly message telling {mention} you're running the "
        "Example tool now and will have results shortly. Only output that message."
    )

    # Platforms this plugin supports
    platforms = ["discord", "webui", "irc", "homeassistant", "matrix", "homekit", "xbmc"]

    # ──────────────────────────────────────────────────────────────────────────
    # Core logic for this plugin
    # ──────────────────────────────────────────────────────────────────────────

    def _load_settings(self):
        """Helper to load plugin settings from Redis."""
        return redis_client.hgetall(f"plugin_settings:{self.settings_category}")

    def _do_example_work(self, input_text: str) -> str:
        """
        Synchronous core logic.

        Replace this with the REAL work of your plugin:
        - Call an API
        - Talk to a local service
        - Do some computation
        - Format a result

        Keep this pure + sync so it works nicely with asyncio.to_thread if needed.
        """
        # TODO: implement actual logic
        # For now, just echo with some decoration so you can test wiring.
        return f"ExamplePlugin processed: {input_text}"

    async def _run_example(self, args, llm_client, user_context: str = "") -> str:
        """
        Async wrapper that prepares inputs, calls core logic, and maybe uses LLM
        to pretty-format the final answer.
        """
        input_text = (args or {}).get("input")
        if not input_text:
            return "No input provided."

        # Load settings if needed
        settings = self._load_settings()
        api_key = settings.get("EXAMPLE_API_KEY", "")

        # If you need to fail when not configured:
        # if not api_key:
        #     return "Example plugin is not configured. Please set the API key in plugin settings."

        # Run the main work (sync) in a thread if it could be slow
        result = await asyncio.to_thread(self._do_example_work, input_text)

        # Optionally ask LLM to polish / summarize / rephrase
        if llm_client:
            first, last = get_tater_name()
            prompt = (
                f"Your name is {first} {last}. The tool produced this raw result:\n\n"
                f"{result}\n\n"
                f"User context (optional): {user_context}\n\n"
                "Rewrite this into a friendly, helpful answer for the user. "
                "Do not mention that a tool was used."
            )
            try:
                resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
                pretty = resp["message"].get("content", "").strip()
                if pretty:
                    return pretty
            except Exception as e:
                logger.error(f"[ExamplePlugin LLM formatting error] {e}")

        # Fallback: just return raw result
        return result

    def _siri_flatten(self, text: str | None) -> str:
        """Make responses clean for Siri TTS (no markdown noise, compact, short-ish)."""
        import re

        if not text:
            return "No answer available."
        out = str(text)
        out = re.sub(r"[`*_]{1,3}", "", out)
        out = re.sub(r"\s+", " ", out).strip()
        return out[:450]

    # ──────────────────────────────────────────────────────────────────────────
    # Platform handlers
    # ──────────────────────────────────────────────────────────────────────────

    async def handle_discord(self, message, args, llm_client):
        # message.content can be used as user_context if useful
        return await self._run_example(args, llm_client, user_context=message.content)

    async def handle_webui(self, args, llm_client):
        async def inner():
            return await self._run_example(args, llm_client, user_context=args.get("user_question", ""))

        # Hybrid-safe pattern (works in and out of event loop)
        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        answer = await self._run_example(args, llm_client, user_context=raw_message)
        return f"{user}: {answer}"

    async def handle_homeassistant(self, args, llm_client):
        answer = await self._run_example(args, llm_client, user_context=args.get("user_question", ""))
        return (answer or "No answer available.").strip()

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        answer = await self._run_example(args, llm_client, user_context=body or "")
        return answer

    async def handle_homekit(self, args, llm_client):
        answer = await self._run_example(args, llm_client, user_context=args.get("user_question", ""))
        return self._siri_flatten(answer)

    async def handle_xbmc(self, args, llm_client):
        answer = await self._run_example(args, llm_client, user_context=args.get("user_question", ""))
        return (answer or "No answer available.").strip()


plugin = ExamplePlugin()
