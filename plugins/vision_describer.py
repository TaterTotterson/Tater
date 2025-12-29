# plugins/vision_describer.py
import asyncio
import base64
import requests
import os
import discord
import mimetypes
from plugin_base import ToolPlugin
from plugin_settings import get_plugin_settings
from helpers import redis_client, get_latest_image_from_history

def decode_base64(data: str) -> bytes:
    """Safely decode base64 or data: URL strings (kept for parity/utility)."""
    data = data.strip()
    if data.startswith("data:"):
        _, data = data.split(",", 1)
    missing_padding = len(data) % 4
    if missing_padding:
        data += "=" * (4 - missing_padding)
    return base64.b64decode(data)

async def safe_send(channel: discord.TextChannel, content: str):
    """Send content to Discord, split if over 2000 characters."""
    chunks = [content[i:i + 2000] for i in range(0, len(content), 2000)]
    for chunk in chunks:
        await channel.send(chunk)

def _to_data_url(image_bytes: bytes, filename: str = "image.png") -> str:
    mime = mimetypes.guess_type(filename)[0] or "image/png"
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    return f"data:{mime};base64,{b64}"

class VisionDescriberPlugin(ToolPlugin):
    name = "vision_describer"
    plugin_name = "Vision Describer"
    usage = (
        '{\n'
        '  "function": "vision_describer",\n'
        '  "arguments": {}\n'
        '}'
    )
    description = (
        "Uses an OpenAI-compatible *vision* model to describe the most recent image. "
        "No input needed — it automatically finds the latest uploaded or generated image."
    )
    plugin_dec = "Describe the most recent image using a vision-capable model."
    pretty_name = "Describing Your Image"
    settings_category = "Vision"
    required_settings = {
        "api_base": {
            "label": "API Base URL",
            "description": "OpenAI-compatible base URL (e.g., http://127.0.0.1:1234).",
            "type": "text",
            "default": "http://127.0.0.1:1234"
        },
        "model": {
            "label": "Vision Model",
            "description": "OpenAI-compatible model name (e.g., qwen2.5-vl-7b-instruct, gemma-3-12b-it, etc.).",
            "type": "text",
            "default": "gemma3-27b-abliterated-dpo"
        }
    }
    waiting_prompt_template = "Write a playful message telling {mention} you’re using your magnifying glass to inspect their image now! Only output that message."
    platforms = ["discord", "webui", "matrix", "irc"]

    def get_vision_settings(self):
        s = get_plugin_settings(self.settings_category)
        api_base = s.get("api_base", self.required_settings["api_base"]["default"]).rstrip("/")
        model = s.get("model", self.required_settings["model"]["default"])
        # Optional API key from env for servers that require it
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        return api_base, model, api_key

    def _call_openai_vision(self, api_base: str, model: str, image_bytes: bytes, prompt: str, filename: str = "image.png") -> str:
        url = f"{api_base}/v1/chat/completions"
        data_url = _to_data_url(image_bytes, filename)
        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt or "Describe this image."},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
            "temperature": 0.2,
        }
        headers = {"Content-Type": "application/json"}
        # Add Authorization header if OPENAI_API_KEY is present
        _, _, api_key = self.get_vision_settings()
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        resp = requests.post(url, json=payload, headers=headers, timeout=90)
        if resp.status_code != 200:
            return f"Error: Vision service returned status {resp.status_code}.\nResponse: {resp.text}"
        j = resp.json()
        try:
            return j["choices"][0]["message"]["content"].strip()
        except Exception:
            return f"Error: Unexpected response shape: {j}"

    async def process_image_web(self, file_content: bytes, filename: str):
        prompt = (
            "You are an expert visual assistant. Describe the contents of this image in detail, "
            "mentioning key objects, scenes, or actions if recognizable."
        )
        api_base, model, _ = self.get_vision_settings()
        description = await asyncio.to_thread(
            self._call_openai_vision,
            api_base, model, file_content, prompt, filename
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

        api_base, model, _ = self.get_vision_settings()
        try:
            description = await asyncio.to_thread(
                self._call_openai_vision,
                api_base, model, image_bytes, prompt, filename
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

    # --- Matrix Handler ---
    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        """
        Look up the most recent image from this Matrix room's stored history and describe it.
        """
        key = f"tater:matrix:{room.room_id}:history"
        try:
            asyncio.get_running_loop()
            result = await self._describe_latest_image(key)
        except RuntimeError:
            result = asyncio.run(self._describe_latest_image(key))
        # Matrix platform will post strings directly (and chunk if needed)
        return result

    # --- IRC Handler ---
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return f"{user}: This plugin only works via Discord, WebUI, and Matrix. IRC support is not available yet."

plugin = VisionDescriberPlugin()
