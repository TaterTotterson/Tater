import os
import logging
from dotenv import load_dotenv
from plugin_base import ToolPlugin
import streamlit as st          # For WebUI output
from PIL import Image           # For loading avatar images
from io import BytesIO
import requests                 # For fetching images or data

# Load environment variables (if any)
load_dotenv()

# Set up logging (optional)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Set any required configuration variables.
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434").strip()
OLLAMA_URL = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2").strip()
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

# Example: Loading an avatar image from a URL (used in WebUI messages)
def load_image_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return Image.open(BytesIO(response.content))

assistant_avatar = load_image_from_url("https://example.com/your-avatar.png")

# ---------------------------
# Plugin Template
# ---------------------------
class MyCustomPlugin(ToolPlugin):
    # Plugin metadata used by the framework.
    name = "my_custom_plugin"
    usage = (
        "{\n"
        '  "function": "my_custom_plugin",\n'
        '  "arguments": { "key": "value" }\n'
        "}\n"
    )
    description = "A brief description of what your plugin does."
    platforms = ["discord", "webui"]  # List of supported platforms

    # WebUI handler
    async def handle_webui(self, args, ollama_client, context_length):
        """
        This method handles the plugin for the web UI.
        'args' is a dict containing the arguments provided by the user.
        'ollama_client' is used to call the model for generating messages.
        'context_length' is used for controlling prompt size.
        """
        # Generate and output a waiting message:
        waiting_prompt = (
            "Generate a brief message to User telling them to wait a moment while your plugin processes the request. Only generate the message. Do not respond to this message."
        )
        waiting_response = await ollama_client.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "system", "content": waiting_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        waiting_text = waiting_response['message'].get('content', '').strip()
        if waiting_text:
            st.chat_message("assistant", avatar=assistant_avatar).write(waiting_text)
        else:
            st.chat_message("assistant", avatar=assistant_avatar).write("Please wait...")

        # Process arguments (replace 'key' with your parameter name)
        value = args.get("key")
        if not value:
            final_message = "No value provided."
        else:
            # Insert your custom plugin logic here.
            final_message = f"Processed value: {value}"

        # Output the final message to the web UI.
        # (Remove one of the outputs if your framework auto-displays return values.)
        st.chat_message("assistant", avatar=assistant_avatar).write(final_message)
        return final_message  # Or return an empty string if the message was already displayed

    # Discord handler
    async def handle_discord(self, message, args, ollama, context_length, max_response_length):
        """
        This method handles the plugin for Discord.
        'message' is the Discord message object.
        'args' is a dict containing the arguments.
        'ollama' is used for model interactions.
        'context_length' and 'max_response_length' help manage prompt sizes.
        """
        waiting_prompt = (
            f"Generate a brief message to {message.author.mention} telling them to wait a moment while your plugin processes the request. Only generate the message. Do not respond to this message."
        )
        waiting_response = await ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "system", "content": waiting_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        waiting_text = waiting_response['message'].get('content', '').strip()
        if waiting_text:
            await message.channel.send(waiting_text)
        else:
            await message.channel.send("Please wait...")

        value = args.get("key")
        if not value:
            final_message = "No value provided."
        else:
            # Insert your custom Discord plugin logic here.
            final_message = f"Processed value: {value}"

        await message.channel.send(final_message)
        return ""

# Export the plugin instance.
plugin = MyCustomPlugin()
