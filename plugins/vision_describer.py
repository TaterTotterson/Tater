# plugins/vision_describer.py
import asyncio
import base64
import requests
import os
import discord
from plugin_base import ToolPlugin

# Helper: Retrieve plugin settings from Redis.
import redis
redis_host = os.getenv('REDIS_HOST', '127.0.0.1')
redis_port = int(os.getenv('REDIS_PORT', 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)

# Import the waiting message helper from your helpers.
from helpers import send_waiting_message

def get_plugin_settings(category):
    key = f"plugin_settings:{category}"
    return redis_client.hgetall(key)

def decode_base64(data: str) -> bytes:
    data = data.strip()
    # Remove a potential data URL prefix.
    if data.startswith("data:"):
        header, data = data.split(",", 1)
    missing_padding = len(data) % 4
    if missing_padding:
        data += '=' * (4 - missing_padding)
    return base64.b64decode(data)

class VisionDescriberPlugin(ToolPlugin):
    name = "vision_describer"
    usage = (
        '{\n'
        '  "function": "vision_describer",\n'
        '  "arguments": {\n'
        '       "image_url": "<URL of the image (optional if an attachment is provided)>",\n'
        '       "image_base64": "<Base64 encoded image (optional if an attachment is provided)>",\n'
        '       "additional_prompt": "<Optional extra instructions>"\n'
        '  }\n'
        '}\n'
    )
    description = "Describes an image from a URL or user attachment."
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

    def call_ollama_vision(self, server, model, image_bytes, additional_prompt="", num_ctx=2048, keep_alive=-1):
        """
        Call the Ollama vision API using the llava endpoint.
        Sends a POST request to /api/generate with a JSON payload:
        {
          "model": model,
          "prompt": additional_prompt,
          "stream": false,
          "images": [ base64-encoded image ],
          "num_ctx": num_ctx,
          "keep_alive": -1
        }
        Expects the response to include the generated description in:
            result["response"]
        """
        try:
            # Ensure the image is base64 encoded.
            if isinstance(image_bytes, bytes):
                image_b64 = base64.b64encode(image_bytes).decode("utf-8")
            else:
                image_b64 = image_bytes  # assume it's already a base64 string

            payload = {
                "model": model,
                "prompt": additional_prompt,
                "stream": False,
                "images": [image_b64],
                "num_ctx": num_ctx,
                "keep_alive": -1
            }
            response = requests.post(f"{server}/api/generate", json=payload)
            if response.status_code == 200:
                result = response.json()
                return result.get("response", "No description provided.").strip()
            else:
                return f"Error: Vision service returned status code {response.status_code}."
        except Exception as e:
            return f"Error calling vision service: {str(e)}"

    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        image_bytes = None
        # 1. Try to get an attached image.
        if message.attachments:
            for attachment in message.attachments:
                if any(attachment.filename.lower().endswith(ext) for ext in [".png", ".jpg", ".jpeg", ".gif"]):
                    image_bytes = await attachment.read()
                    break
        
        # 2. If no attachment, check for an image URL in args.
        if not image_bytes and args.get("image_url"):
            image_url = args.get("image_url")
            try:
                resp = requests.get(image_url)
                if resp.status_code == 200:
                    image_bytes = resp.content
            except Exception as e:
                await message.channel.send(f"Error downloading image: {str(e)}")
                return ""
        
        # 3. If still not found, check for a base64 encoded image.
        if not image_bytes and args.get("image_base64"):
            try:
                image_bytes = decode_base64(args.get("image_base64"))
            except Exception as e:
                await message.channel.send(f"Error decoding base64 image: {str(e)}")
                return ""
        
        if not image_bytes:
            await message.channel.send("No valid image detected. Make sure to attach an image file or provide a valid image URL/base64 data.")
            return ""
        
        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: message.channel.send(text)
        )
        
        additional_prompt = args.get("additional_prompt", "Describe this image:")
        server, model = self.get_vision_settings()
        description = await asyncio.to_thread(
            self.call_ollama_vision, server, model, image_bytes, additional_prompt, ollama_client.context_length, -1
        )
        
        if description:
            for chunk in [description[i:i+max_response_length] for i in range(0, len(description), max_response_length)]:
                await message.channel.send(chunk)
        return ""
    
    async def process_image_web(self, file_content: bytes, filename: str):
        """
        Process an image file for the web UI using a default prompt.
        """
        additional_prompt = "Describe this image:"
        server, model = self.get_vision_settings()
        description = await asyncio.to_thread(self.call_ollama_vision, server, model, file_content, additional_prompt)
        return description

    async def handle_webui(self, args, ollama_client, context_length):
        # Dummy implementation since this plugin is only available on Discord.
        return "This plugin is only available on Discord."

plugin = VisionDescriberPlugin()