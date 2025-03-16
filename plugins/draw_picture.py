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

load_dotenv()
# Get the AUTOMATIC_URL from .env
AUTOMATIC_URL = os.getenv("AUTOMATIC_URL")
if not AUTOMATIC_URL:
    raise Exception("AUTOMATIC_URL environment variable not set.")

# Import helper functions from helpers.py.
from helpers import load_image_from_url, send_waiting_message
assistant_avatar = load_image_from_url()  # Uses default URL from helpers.py

class DrawPicturePlugin(ToolPlugin):
    name = "draw_picture"
    usage = (
        "{\n"
        '  "function": "draw_picture",\n'
        '  "arguments": {"prompt": "<Text prompt for the image>"}\n'
        "}\n"
    )
    description = "Draws a picture using a prompt provided by the user."
    platforms = ["discord", "webui"]
    # Waiting prompt template with a placeholder for the user mention.
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while I create a masterpiece for them. Only generate the message. Do not respond to this message."
    )

    # --- Helper Functions as Static Methods ---
    @staticmethod
    def generate_image(prompt: str) -> bytes:
        """
        Generates an image using the text-to-image endpoint.
        :param prompt: The text prompt for image generation.
        :return: The generated image as bytes.
        """
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
        :param attachment_url: URL of the image to describe.
        :return: The description (caption) returned by the API.
        """
        image_response = requests.get(attachment_url)
        if image_response.status_code == 200:
            image_bytes = image_response.content
            image_b64 = base64.b64encode(image_bytes).decode('utf-8')
        else:
            raise Exception("Failed to download image from the provided URL.")
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

    # --- Discord Handler ---
    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        prompt_text = args.get("prompt")
        if not prompt_text:
            return "No prompt provided for drawing a picture."
        # Format waiting prompt with user's mention.
        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: message.channel.send(text)
        )
        async with message.channel.typing():
            try:
                # Run the synchronous generate_image function in an executor.
                image_bytes = await asyncio.to_thread(DrawPicturePlugin.generate_image, prompt_text)
                from io import BytesIO
                import discord
                image_file = discord.File(BytesIO(image_bytes), filename="generated_image.png")
                await message.channel.send(file=image_file)
            except Exception as e:
                err_prompt = f"Generate an error message to {message.author.mention} explaining that I was unable to create the image."
                error_msg = await self.generate_error_message(err_prompt, f"Failed to generate image: {e}", message)
                await message.channel.send(error_msg)
        return ""

    # --- Web UI Handler ---
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
        # Run generate_image asynchronously using asyncio.to_thread.
        image_bytes = await asyncio.to_thread(DrawPicturePlugin.generate_image, prompt_text)
        try:
            st.image(image_bytes, caption="Generated Image")
        except Exception as e:
            pass
        return ""

    async def generate_error_message(self, prompt, fallback, message):
        return fallback

# Export an instance of the plugin.
plugin = DrawPicturePlugin()