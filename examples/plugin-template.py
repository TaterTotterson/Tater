# plugins/<plugin_name>.py
import os
import asyncio
from plugin_base import ToolPlugin

# Optionally load environment variables if needed.
# from dotenv import load_dotenv
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
    # SETTINGS: Define the category and required settings for this plugin.
    # If multiple plugins share the same settings, they should have the same category.
    settings_category = "<CategoryName>"  # e.g. "Draw", "Premiumize", etc.
    required_settings = {
        "<SETTING_KEY>": {
            "label": "<Setting Label>",
            "type": "string",
            "default": "",  # Provide a default value or leave empty.
            "description": "Description of this setting."
        }
    }
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait while I process your <plugin_name> request. Only generate the message. Do not respond to this message."
    )
    platforms = ["discord", "webui"]
    
    # --- Helper Functions as Static Methods ---
    @staticmethod
    def some_helper_function(param):
        # Implement helper logic
        return param

    # --- Discord Handler ---
    async def handle_discord(self, message, args, llm_client):
        # Validate input from args (adjust as needed).
        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            llm_client=llm_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: message.channel.send(text)
        )
        result = await asyncio.to_thread(self.some_helper_function, args.get("param"))
        await message.channel.send(result)
        return ""

    # --- Web UI Handler ---
    async def handle_webui(self, args, llm_client):
        waiting_prompt = self.waiting_prompt_template.format(mention="User")
        await send_waiting_message(
            llm_client=llm_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: st.chat_message("assistant", avatar=load_image_from_url()).write(text)
        )
        result = await asyncio.to_thread(self.some_helper_function, args.get("param"))
        return result

# Export an instance.
plugin = <PluginName>Plugin()
