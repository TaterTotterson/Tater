# plugins/draw_picture.py
import os
import requests
import base64
import asyncio
from io import BytesIO
from dotenv import load_dotenv
import ollama
from plugin_base import ToolPlugin
import streamlit as st
from PIL import Image
import discord
from helpers import load_image_from_url, send_waiting_message, redis_client

class DrawPicturePlugin(ToolPlugin):
    name = "draw_picture"
    usage = (
        "{\n"
        '  "function": "draw_picture",\n'
        '  "arguments": {"prompt": "<Text prompt for the image>"}\n'
        "}\n"
    )
    description = "Draws a picture using a prompt provided by the user."
    settings_category = "Draw"
    required_settings = {
        "AUTOMATIC_URL": {
            "label": "AUTOMATIC URL",
            "type": "string",
            "default": "http://localhost:7860",
            "description": "The URL for the Automatic1111 API."
        }
    }
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while you draw them a masterpiece. Only generate the message. Do not respond to this message."
    )
    platforms = ["discord", "webui"]

    assistant_avatar = load_image_from_url()  # Uses default avatar URL from helpers.py

    @staticmethod
    def generate_image(prompt: str) -> bytes:
        """
        Generates an image using the text-to-image endpoint.
        Retrieves AUTOMATIC_URL from Redis under "plugin_settings:Draw", 
        falling back to the environment if not found.
        """
        key = "plugin_settings:Draw"
        settings = DrawPicturePlugin.redis_client.hgetall(key)
        AUTOMATIC_URL = settings.get("AUTOMATIC_URL") or os.getenv("AUTOMATIC_URL")
        if not AUTOMATIC_URL:
            raise Exception("AUTOMATIC_URL is not set in plugin settings or environment.")
        endpoint = f"{AUTOMATIC_URL}/sdapi/v1/txt2img"
        payload = {
            "prompt": prompt,
            "steps": 4,
            "cfg_scale": 1,
            "width": 896,
            "height": 1152,
            "sampler_name": "DPM++ 2M",
            "scheduler": "Simple",
        }
        response = requests.post(endpoint, json=payload)
        if response.status_code == 200:
            result = response.json()
            if "images" in result and result["images"]:
                image_b64 = result["images"][0]
                try:
                    image_bytes = base64.b64decode(image_b64)
                    return image_bytes
                except Exception as e:
                    raise Exception(f"Failed to decode the image: {e}")
            else:
                raise Exception("No image returned from the AUTOMATIC1111 API.")
        else:
            raise Exception(f"Image generation failed (status {response.status_code}): {response.text}")

    @staticmethod
    def describe_image(attachment_url: str) -> str:
        """
        Downloads an image from a URL and sends it to the API for description.
        """
        image_response = requests.get(attachment_url)
        if image_response.status_code == 200:
            image_bytes = image_response.content
            image_b64 = base64.b64encode(image_bytes).decode('utf-8')
        else:
            raise Exception("Failed to download image from the provided URL.")
        key = "plugin_settings:Draw"
        settings = DrawPicturePlugin.redis_client.hgetall(key)
        AUTOMATIC_URL = settings.get("AUTOMATIC_URL") or os.getenv("AUTOMATIC_URL")
        if not AUTOMATIC_URL:
            raise Exception("AUTOMATIC_URL is not set in plugin settings or environment.")
        endpoint = f"{AUTOMATIC_URL}/sdapi/v1/describe"
        payload = {"image": image_b64}
        response = requests.post(endpoint, json=payload)
        if response.status_code == 200:
            result = response.json()
            if "caption" in result:
                return result["caption"]
            else:
                raise Exception("No caption returned from the AUTOMATIC1111 API.")
        else:
            raise Exception(f"Image description failed (status {response.status_code}): {response.text}")

    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        prompt_text = args.get("prompt")
        if not prompt_text:
            return "No prompt provided for drawing a picture."
        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: message.channel.send(text)
        )
        async with message.channel.typing():
            try:
                image_bytes = await asyncio.to_thread(DrawPicturePlugin.generate_image, prompt_text)
                image_file = discord.File(BytesIO(image_bytes), filename="generated_image.png")
                await message.channel.send(file=image_file)
            except Exception as e:
                err_prompt = f"Generate an error message to {message.author.mention} explaining that I was unable to create the image."
                error_msg = await self.generate_error_message(err_prompt, f"Failed to generate image: {e}", message)
                await message.channel.send(error_msg)
        return ""

    async def handle_webui(self, args, ollama_client, context_length):
        prompt_text = args.get("prompt")
        if not prompt_text:
            return "No prompt provided for drawing a picture."
        waiting_prompt = self.waiting_prompt_template.format(mention="User")
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: st.chat_message("assistant", avatar=assistant_avatar).write(text)
        )
        image_bytes = await asyncio.to_thread(DrawPicturePlugin.generate_image, prompt_text)
        try:
            st.image(image_bytes, caption="Generated Image")
        except Exception as e:
            pass
        return ""

    async def generate_error_message(self, prompt, fallback, message):
        return fallback

plugin = DrawPicturePlugin()