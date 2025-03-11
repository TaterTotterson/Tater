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
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434").strip()
OLLAMA_URL = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2").strip()
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

# Get the base URL for the AUTOMATIC1111 API from .env
AUTOMATIC_URL = os.getenv("AUTOMATIC_URL")
if not AUTOMATIC_URL:
    raise Exception("AUTOMATIC_URL environment variable not set.")

# You can also load other configuration variables if needed.
# For this example, we only need AUTOMATIC_URL.

# Load the assistant avatar from URL using requests and Pillow.
def load_image_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return Image.open(BytesIO(response.content))

assistant_avatar = load_image_from_url("https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater.png")

class DrawPicturePlugin(ToolPlugin):
    name = "draw_picture"
    usage = (
        "For drawing images:\n"
        "{\n"
        '  "function": "draw_picture",\n'
        '  "arguments": {"prompt": "<Text prompt for the image>"}\n'
        "}\n"
    )
    description = "Draws a picture using a promppt provided by the user."
    platforms = ["discord", "webui"]

    async def handle_webui(self, args, ollama_client, context_length):
        username = args.get("username", "User")
        # In webui, we assume a waiting message is sent by the main code
        prompt_text = args.get("prompt")
        if not prompt_text:
            return "No prompt provided for drawing a picture."
        
        # Send a waiting message to the user in the web UI.
        waiting_prompt = (
            "Generate a brief message to User telling them to wait a moment while you create a masterpiece for them. Only generate the message. Do not respond to this message."
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
            st.chat_message("assistant", avatar=assistant_avatar).write("Please wait a moment while I summarize the article...")

        # Use asyncio.to_thread to run the synchronous generate_image function
        image_bytes = await asyncio.to_thread(generate_image, prompt_text)
        try:
            st.image(image_bytes, caption="Generated Image")
        except Exception as e:
            # If Streamlit isn't available here, simply ignore.
            pass
        return ""

    async def handle_discord(self, message, args, ollama, context_length, max_response_length):
        prompt_text = args.get("prompt")
        if not prompt_text:
            return "No prompt provided for drawing a picture."
        waiting_prompt = (
            f"Generate a brief message to {message.author.mention} telling them to wait a moment while you create a masterpiece for them. Only generate the message. Do not respond to this message."
        )
        waiting_response = await ollama.chat(
            model=OLLAMA_MODEL,  # Assuming the main bot passes its model via ollama
            messages=[{"role": "system", "content": waiting_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        waiting_text = waiting_response['message'].get('content', '')
        if waiting_text:
            await message.channel.send(waiting_text)
        else:
            await message.channel.send("Hold on while I create that picture for you...")
        
        async with message.channel.typing():
            try:
                loop = asyncio.get_running_loop()
                # Run the synchronous generate_image function in a thread.
                image_bytes = await loop.run_in_executor(None, generate_image, prompt_text)
                from io import BytesIO
                import discord
                image_file = discord.File(BytesIO(image_bytes), filename="generated_image.png")
                await message.channel.send(file=image_file)
            except Exception as e:
                prompt = f"Generate a error message to {message.author.mention} explaining that I was unable to create the image."
                error_msg = await self.generate_error_message(prompt, f"Failed to generate image: {e}", message)
                await message.channel.send(error_msg)
        return ""

    # Stub for error message generation (adjust as needed)
    async def generate_error_message(self, prompt, fallback, message):
        return fallback

# Helper functions moved from image.py

def generate_image(prompt: str) -> bytes:
    """
    Generates an image using the text-to-image endpoint.
    :param prompt: The text prompt for image generation.
    :return: The generated image as bytes.
    """
    endpoint = f"{AUTOMATIC_URL}/sdapi/v1/txt2img"
    payload = {
        "prompt": prompt,
        # Optional: adjust these parameters as needed:
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

# Optionally, you can also include the describe_image function if needed.
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

# Export an instance of the plugin.
plugin = DrawPicturePlugin()