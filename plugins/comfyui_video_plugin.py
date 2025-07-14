# plugins/comfyui_video_plugin.py
import os
import asyncio
import base64
import subprocess
from plugin_base import ToolPlugin
from helpers import redis_client, get_latest_image_from_history
from plugins.comfyui_image_plugin import ComfyUIImagePlugin
from plugins.comfyui_image_video_plugin import ComfyUIImageVideoPlugin

class ComfyUIVideoPlugin(ToolPlugin):
    name = "comfyui_video_plugin"
    usage = (
        '{\n'
        '  "function": "comfyui_video_plugin",\n'
        '  "arguments": {"prompt": "<Describe the video you want>"}\n'
        '}\n'
    )
    description = "Generates a video from a text prompt by creating an image then animating it using ComfyUI."
    platforms = ["discord", "webui"]
    settings_category = "ComfyUI Video"
    required_settings = {
        "VIDEO_RESOLUTION": {
            "label": "Video Resolution",
            "type": "select",
            "default": "720p",
            "options": ["144p", "240p", "360p", "480p", "720p", "1080p"],
            "description": "Resolution of the generated video."
        },
        "VIDEO_LENGTH": {
            "label": "Video Length (seconds)",
            "type": "string",
            "default": "5",
            "description": "Length of the video clip."
        }
    }
    waiting_prompt_template = "Write a fun, upbeat message saying you’re directing a short video now! Only output that message."

    res_map = {
        "144p": (256, 144),
        "240p": (426, 240),
        "360p": (480, 360),
        "480p": (640, 480),
        "720p": (1280, 720),
        "1080p": (1920, 1080)
    }

    async def generate_video(self, prompt, ollama_client):
        settings = redis_client.hgetall(f"plugin_settings:{self.settings_category}")
        raw_res = settings.get("VIDEO_RESOLUTION", b"720p")
        resolution = raw_res.decode() if isinstance(raw_res, bytes) else raw_res
        w, h = self.res_map.get(resolution, (1280, 720))

        raw_len = settings.get("VIDEO_LENGTH", b"5")
        try:
            duration = int(raw_len.decode() if isinstance(raw_len, bytes) else raw_len)
        except ValueError:
            duration = 5

        img_desc_resp = await ollama_client.chat([
            {"role": "system", "content": "You help generate image prompts for AI image creation."},
            {"role": "user", "content": f"Write a single clear sentence describing an illustration of: {prompt}"}
        ])
        image_prompt = img_desc_resp["message"]["content"].strip()

        image_bytes = await asyncio.to_thread(
            ComfyUIImagePlugin.process_prompt,
            image_prompt, w, h
        )

        # Skip Ollama prompt for animation, use blank workflow defaults
        animation_desc = ""

        anim_bytes = await asyncio.to_thread(
            ComfyUIImageVideoPlugin.process_prompt,
            animation_desc,
            image_bytes,
            "generated_image.png",
            w,
            h,
            duration * 16  # FPS * duration
        )

        return anim_bytes

    async def handle_discord(self, message, args, ollama_client):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided."

        try:
            video_bytes = await self.generate_video(user_prompt, ollama_client)

            payload = {
                "type": "image",
                "name": "generated_video.webp",
                "data": base64.b64encode(video_bytes).decode(),
                "mimetype": "image/webp"
            }

            followup = await ollama_client.chat([
                {"role": "system", "content": f"The user has just been shown a video based on '{user_prompt}'."},
                {"role": "user", "content": "Reply with a short, fun message celebrating the video. No lead-in phrases or instructions."}
            ])
            return [payload, followup["message"]["content"].strip() or "🎬 Here’s your video!"]

        except Exception as e:
            return f"⚠️ Error generating video: {e}"

    async def handle_webui(self, args, ollama_client):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided."

        try:
            video_bytes = await self.generate_video(user_prompt, ollama_client)

            payload = {
                "type": "image",
                "name": "generated_video.webp",
                "data": base64.b64encode(video_bytes).decode(),
                "mimetype": "image/webp"
            }

            followup = await ollama_client.chat([
                {"role": "system", "content": f"The user has just been shown a video based on '{user_prompt}'."},
                {"role": "user", "content": "Reply with a short, fun message celebrating the video. No lead-in phrases or instructions."}
            ])
            return [payload, followup["message"]["content"].strip() or "🎬 Here’s your video!"]

        except Exception as e:
            return f"⚠️ Error generating video: {e}"

    async def handle_irc(self, bot, channel, user, raw, args, ollama_client):
        await bot.privmsg(channel, f"{user}: This plugin is supported only on Discord and WebUI.")

plugin = ComfyUIVideoPlugin()