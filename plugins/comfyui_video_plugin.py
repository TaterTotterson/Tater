# plugins/comfyui_video_plugin.py
import os
import asyncio
import base64
import uuid
from plugin_base import ToolPlugin
from helpers import redis_client
from plugins.comfyui_image_plugin import ComfyUIImagePlugin
from plugins.comfyui_image_video_plugin import ComfyUIImageVideoPlugin
from plugins.vision_describer import VisionDescriberPlugin
vision_plugin = VisionDescriberPlugin()

class ComfyUIVideoPlugin(ToolPlugin):
    name = "comfyui_video_plugin"
    usage = (
        '{\n'
        '  "function": "comfyui_video_plugin",\n'
        '  "arguments": {"prompt": "<Describe the video you want>"}\n'
        '}\n'
    )
    description = "Generates a video from a text prompt by creating an image then animating it using ComfyUI."
    platforms = ["webui"]
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

    def cleanup_temp_file(self, path):
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            print(f"[Cleanup warning] {e}")

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

        job_id = str(uuid.uuid4())[:8]
        tmp_img = f"/tmp/{job_id}_frame.png"
        tmp_vid = f"/tmp/{job_id}_clip"

        img_desc_resp = await ollama_client.chat([
            {"role": "system", "content": "You help generate image prompts for AI image creation."},
            {"role": "user", "content": f"Write a single clear sentence describing an illustration of: {prompt}"}
        ])
        image_prompt = img_desc_resp["message"]["content"].strip()

        image_bytes = await asyncio.to_thread(
            ComfyUIImagePlugin.process_prompt,
            image_prompt, w, h
        )

        with open(tmp_img, "wb") as f:
            f.write(image_bytes)

        with open(tmp_img, "rb") as f:
            image_content = f.read()
        desc = await vision_plugin.process_image_web(image_content, tmp_img)
        self.cleanup_temp_file(tmp_img)
        desc = desc.strip() or "An interesting scene"

        animation_prompt = (
            f"The following is a visual description of an image:\n\n"
            f"\"{desc}\"\n\n"
            f"And this was the original prompt:\n\n"
            f"\"{prompt}\"\n\n"
            "Write a single clear sentence that describes what this image depicts and how it might animate to reflect the user's intent."
        )
        resp = await ollama_client.chat([
            {"role": "system", "content": "You generate vivid single-sentence descriptions that combine image content and user prompt for animation."},
            {"role": "user", "content": animation_prompt}
        ])
        animation_desc = resp["message"]["content"].strip() or "A short animation that reflects the prompt."

        anim_bytes, ext = await asyncio.to_thread(
            ComfyUIImageVideoPlugin.process_prompt,
            animation_desc,
            image_bytes,
            f"{job_id}.png",
            w,
            h,
            duration * 16
        )

        out_path = f"{tmp_vid}.{ext}"
        with open(out_path, "wb") as f:
            f.write(anim_bytes)

        with open(out_path, "rb") as f:
            final_bytes = f.read()

        self.cleanup_temp_file(out_path)

        payload = {
            "type": "video" if ext == "mp4" else "image",
            "name": f"generated_video.{ext}",
            "data": base64.b64encode(final_bytes).decode(),
            "mimetype": "video/mp4" if ext == "mp4" else "image/webp"
        }

        msg = await ollama_client.chat([
            {"role": "system", "content": f"The user has just been shown a video based on '{prompt}'."},
            {"role": "user", "content": "Reply with a short, fun message celebrating the video. No lead-in phrases or instructions."}
        ])
        return [payload, msg["message"]["content"].strip() or "🎬 Here’s your video!"]

    async def handle_discord(self, message, args, ollama_client):
        return "❌ This plugin is only available in the WebUI due to file size limitations."

    async def handle_webui(self, args, ollama_client):
        if "prompt" not in args:
            return "No prompt provided."
        try:
            return await self.generate_video(args["prompt"], ollama_client)
        except Exception as e:
            return f"⚠️ Error generating video: {e}"

    async def handle_irc(self, bot, channel, user, raw, args, ollama_client):
        await bot.privmsg(channel, f"{user}: This plugin is supported only on Discord and WebUI.")

plugin = ComfyUIVideoPlugin()