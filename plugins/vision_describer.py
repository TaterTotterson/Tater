# plugins/vision_describer.py
import asyncio
import base64
import requests
import os
import discord
from plugin_base import ToolPlugin
from plugin_settings import get_plugin_settings
from helpers import redis_client, get_latest_image_from_history

def decode_base64(data: str) -> bytes:
    data = data.strip()
    if data.startswith("data:"):
        _, data = data.split(",", 1)
    missing_padding = len(data) % 4
    if missing_padding:
        data += '=' * (4 - missing_padding)
    return base64.b64decode(data)


async def safe_send(channel: discord.TextChannel, content: str):
    """Send content to Discord, split if over 2000 characters."""
    chunks = [content[i:i + 2000] for i in range(0, len(content), 2000)]
    for chunk in chunks:
        await channel.send(chunk)


class VisionDescriberPlugin(ToolPlugin):
    name = "vision_describer"
    usage = (
        '{\n'
        '  "function": "vision_describer",\n'
        '  "arguments": {}\n'
        '}'
    )
    description = (
        "Uses AI vision to describe the most recently available image. "
        "No input needed — it automatically finds the latest uploaded or generated image."
    )
    pretty_name = "Describing Your Image"
    settings_category = "Vision"
    required_settings = {
        "llm_server_address": {
            "label": "LLM Server Address",
            "description": "The address of the LLM server for vision tasks.",
            "type": "text",
            "default": "http://127.0.0.1:11434"
        },
        "llm_model": {
            "label": "LLM Vision Model",
            "description": "The model name used for vision tasks.",
            "type": "text",
            "default": "llava"
        },
        "vision_context_length": {
            "label": "Context Length (num_ctx)",
            "description": "Controls how much context the vision model uses. Higher = more context, slower response.",
            "type": "number",
            "default": "20000"
        }

    }
    waiting_prompt_template = "Write a playful message telling {mention} you’re using your magnifying glass to inspect their image now! Only output that message."
    platforms = ["discord", "webui"]

    def get_vision_settings(self):
        settings = get_plugin_settings(self.settings_category)
        server = settings.get("llm_server_address", self.required_settings["llm_server_address"]["default"])
        model = settings.get("llm_model", self.required_settings["llm_model"]["default"])
        num_ctx = int(settings.get("vision_context_length", self.required_settings["vision_context_length"]["default"]))
        return server, model, num_ctx

    def call_llm_vision(self, server, model, image_bytes, additional_prompt, num_ctx=20000, keep_alive=-1):
        try:
            image_b64 = base64.b64encode(image_bytes).decode("utf-8") if isinstance(image_bytes, bytes) else image_bytes

            payload = {
                "model": model,
                "prompt": additional_prompt,
                "stream": False,
                "images": [image_b64],
                "num_ctx": num_ctx,
                "keep_alive": keep_alive
            }
            response = requests.post(f"{server}/api/generate", json=payload)
            if response.status_code == 200:
                result = response.json()
                return result.get("response", "No description provided.").strip()
            else:
                return f"Error: Vision service returned status code {response.status_code}.\nResponse: {response.text}"
        except Exception as e:
            return f"Error calling vision service: {str(e)}"
   
    async def process_image_web(self, file_content: bytes, filename: str):
        additional_prompt = (
            "You are an expert visual assistant. Describe the contents of this image in detail, "
            "mentioning key objects, scenes, or actions if recognizable."
        )
        server, model, num_ctx = self.get_vision_settings()
        description = await asyncio.to_thread(
            self.call_llm_vision,
            server, model, file_content, additional_prompt, num_ctx=num_ctx
        )
        return description

    async def _describe_latest_image(self, redis_key: str):
        image_bytes, filename = get_latest_image_from_history(
            redis_key,
            allowed_mimetypes=["image/png", "image/jpeg"]
        )

        if not image_bytes:
            return ["❌ No image found. Please upload one or generate one using an image plugin first."]

        prompt = (
            "You are an expert visual assistant. Describe the contents of this image in detail, "
            "mentioning key objects, scenes, or actions if recognizable."
        )

        server, model, num_ctx = self.get_vision_settings()
        try:
            description = await asyncio.to_thread(
                self.call_llm_vision,
                server, model, image_bytes, prompt, num_ctx=num_ctx
            )
            return [description[:1500]] if description else ["❌ Failed to generate image description."]
        except Exception as e:
            return [f"❌ Error: {e}"]

    # --- Discord Handler ---
    async def handle_discord(self, message, args, llm_client):
        key = f"tater:channel:{message.channel.id}:history"

        try:
            asyncio.get_running_loop()
            result = await self._describe_latest_image(key)
        except RuntimeError:
            result = asyncio.run(self._describe_latest_image(key))

        return result[0] if result else f"{message.author.mention}: ❌ No image found or failed to process."

    # --- WebUI Handler ---
    async def handle_webui(self, args, llm_client):
        try:
            asyncio.get_running_loop()
            return await self._describe_latest_image("webui:chat_history")
        except RuntimeError:
            return asyncio.run(self._describe_latest_image("webui:chat_history"))

    # --- IRC Handler ---
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return f"{user}: This plugin only works via Discord. IRC support is not available yet."


plugin = VisionDescriberPlugin()