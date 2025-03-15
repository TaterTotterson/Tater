# plugins/<plugin_name>.py
import os
import asyncio
from plugin_base import ToolPlugin
from helpers import send_waiting_message, load_image_from_url

# Optionally load environment variables if needed.
# load_dotenv() if required.

class <PluginName>Plugin(ToolPlugin):
    name = "<plugin_name>"
    usage = (
        "{\n"
        '  "function": "<plugin_name>",\n'
        '  "arguments": { ... }\n'
        "}\n"
    )
    description = "Description of what the <plugin_name> plugin does."
    platforms = ["discord", "webui"]
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait while I process your <plugin_name> request. Only generate the message. Do not respond to this message."
    )

    # --- Helper Functions as Static Methods ---
    @staticmethod
    def some_helper_function(param):
        # Implement helper logic
        return param

    # --- Discord Handler ---
    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        # Validate input from args.
        # Format waiting prompt with message.author.mention.
        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: message.channel.send(text)
        )
        # Perform plugin-specific work.
        result = await asyncio.to_thread(self.some_helper_function, args.get("param"))
        # Send result (or split into chunks if needed).
        await message.channel.send(result)
        return ""

    # --- Web UI Handler ---
    async def handle_webui(self, args, ollama_client, context_length):
        waiting_prompt = self.waiting_prompt_template.format(mention="User")
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: print("WebUI:", text)
        )
        result = await asyncio.to_thread(self.some_helper_function, args.get("param"))
        return result

# Export an instance.
plugin = <PluginName>Plugin()
