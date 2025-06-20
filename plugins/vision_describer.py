# plugins/vision_describer.py
import asyncio
import base64
import requests
import os
import discord
from plugin_base import ToolPlugin
from plugin_settings import get_plugin_settings
from helpers import send_waiting_message


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
        "Describes the most recent image in the channel, either from an attachment, URL, or base64. "
        "Can be triggered with no arguments."
    )
    platforms = ["discord"]
    settings_category = "Vision"
    required_settings = {
        "ollama_server_address": {
            "label": "Ollama Server Address",
            "description": "The address of the Ollama server for vision tasks.",
            "type": "text",
            "default": "http://127.0.0.1:11434"
        },
        "ollama_model": {
            "label": "Ollama Vision Model",
            "description": "The model name used for vision tasks.",
            "type": "text",
            "default": "llava"
        }
    }
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while you use your magnifying glass to inspect their image in detail. Only generate the message. Do not respond to this message."
    )

    def get_vision_settings(self):
        settings = get_plugin_settings(self.settings_category)
        server = settings.get("ollama_server_address", self.required_settings["ollama_server_address"]["default"])
        model = settings.get("ollama_model", self.required_settings["ollama_model"]["default"])
        return server, model

    def call_ollama_vision(self, server, model, image_bytes, additional_prompt, num_ctx=2048, keep_alive=-1):
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

    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        image_bytes = None

        # 1. Try image in current message
        if message.attachments:
            for attachment in message.attachments:
                if any(attachment.filename.lower().endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".gif"]):
                    image_bytes = await attachment.read()
                    break

        # 2. Try URL
        if not image_bytes and args.get("image_url"):
            try:
                resp = requests.get(args.get("image_url"))
                if resp.status_code == 200:
                    image_bytes = resp.content
            except Exception as e:
                await safe_send(message.channel, f"Error downloading image: {str(e)}")
                return ""

        # 3. Try base64
        if not image_bytes and args.get("image_base64"):
            try:
                image_bytes = decode_base64(args.get("image_base64"))
            except Exception as e:
                await safe_send(message.channel, f"Error decoding base64 image: {str(e)}")
                return ""

        # 4. Search previous messages
        if not image_bytes:
            async for previous in message.channel.history(limit=10, oldest_first=False):
                if previous.id == message.id:
                    continue
                if previous.attachments:
                    for attachment in previous.attachments:
                        if any(attachment.filename.lower().endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".gif"]):
                            image_bytes = await attachment.read()
                            break
                if image_bytes:
                    break

        # 5. Still no image: generate AI fallback message
        if not image_bytes:
            fallback_prompt = (
                f"Generate a message telling {message.author.mention} that no image was found. "
                "Mention they can attach an image, include a URL, or paste base64. "
                "Only generate the message. Do not respond to this message."
            )
            await send_waiting_message(
                ollama_client=ollama_client,
                prompt_text=fallback_prompt,
                save_callback=lambda text: None,
                send_callback=lambda text: safe_send(message.channel, text)
            )
            return ""

        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: safe_send(message.channel, text)
        )

        # New smarter default prompt
        additional_prompt = (
            "You are an expert visual assistant. Describe the contents of this image in detail, "
            "mentioning key objects, scenes, or actions if recognizable."
        )

        server, model = self.get_vision_settings()

        description = await asyncio.to_thread(
            self.call_ollama_vision,
            server, model, image_bytes, additional_prompt,
            context_length, -1
        )

        if description:
            for chunk in [description[i:i + max_response_length] for i in range(0, len(description), max_response_length)]:
                await safe_send(message.channel, chunk)
        return ""

    async def process_image_web(self, file_content: bytes, filename: str):
        additional_prompt = (
            "You are an expert visual assistant. Describe the contents of this image in detail, "
            "mentioning key objects, scenes, or actions if recognizable."
        )
        server, model = self.get_vision_settings()
        description = await asyncio.to_thread(self.call_ollama_vision, server, model, file_content, additional_prompt)
        return description

    async def handle_webui(self, args, ollama_client, context_length):
        return "🖼️ This plugin is currently only available via Discord. Web support is not yet implemented."


plugin = VisionDescriberPlugin()