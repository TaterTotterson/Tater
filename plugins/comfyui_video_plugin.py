# plugins/comfyui_video_plugin.py
import os
import asyncio
import base64
import uuid
import subprocess
from PIL import Image
from moviepy.video.io.ImageSequenceClip import ImageSequenceClip
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
    description = "Generates a video from a text prompt by creating multiple animated clips using ComfyUI, then merging them into one MP4."
    pretty_name = "Your Video"
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
            "description": "Length of each individual clip."
        },
        "VIDEO_CLIPS": {
            "label": "Number of Clips",
            "type": "string",
            "default": "1",
            "description": "How many clips to generate and merge into one video."
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

    def cleanup_temp_files(self, paths):
        for path in paths:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                print(f"[Cleanup warning] {e}")

    def webp_to_mp4(self, input_file, output_file, fps=16, duration=5):
        frames, tmp_dir, frame_files = [], f"{os.path.dirname(input_file)}/frames_{uuid.uuid4().hex[:6]}", []
        os.makedirs(tmp_dir, exist_ok=True)
        try:
            im = Image.open(input_file)
            while True:
                frames.append(im.copy().convert("RGBA"))
                im.seek(im.tell() + 1)
        except EOFError:
            pass
        for idx, frame in enumerate(frames):
            path = f"{tmp_dir}/frame_{idx}.png"
            frame.save(path, "PNG")
            frame_files.append(path)
        if len(frame_files) == 1 or duration < 1:
            frame_files *= max(1, int(fps * duration))
        clip = ImageSequenceClip(frame_files, fps=fps)
        clip.write_videofile(output_file, codec='libx264', fps=fps, audio=False, logger=None)
        for p in frame_files:
            os.remove(p)
        os.rmdir(tmp_dir)

    def ffmpeg_concat(self, video_paths, out_path):
        listpath = f"{out_path}_concat.txt"
        with open(listpath, "w") as f:
            for p in video_paths:
                f.write(f"file '{p}'\n")
        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", listpath, "-c:v", "libx264", "-pix_fmt", "yuv420p", out_path]
        subprocess.run(cmd, check=True)
        os.remove(listpath)

    def parse_ollama_prompt_list(self, raw_text, expected_count):
        prompts = []
        for line in raw_text.strip().splitlines():
            if '.' in line:
                parts = line.split('.', 1)
                if len(parts) == 2 and parts[1].strip():
                    prompts.append(parts[1].strip())
        return prompts if len(prompts) >= expected_count else None

    async def _generate_video(self, prompt, ollama_client):
        settings = redis_client.hgetall(f"plugin_settings:{self.settings_category}")
        raw_res = settings.get("VIDEO_RESOLUTION", b"720p")
        resolution = raw_res.decode() if isinstance(raw_res, bytes) else raw_res
        w, h = self.res_map.get(resolution, (1280, 720))

        raw_len = settings.get("VIDEO_LENGTH", b"5")
        try:
            duration = int(raw_len.decode() if isinstance(raw_len, bytes) else raw_len)
        except ValueError:
            duration = 5

        raw_clips = settings.get("VIDEO_CLIPS", b"1")
        try:
            num_clips = int(raw_clips.decode() if isinstance(raw_clips, bytes) else raw_clips)
        except ValueError:
            num_clips = 1

        job_id = str(uuid.uuid4())[:8]
        temp_paths, final_clips = [], []
        anim_plugin = ComfyUIImageVideoPlugin()

        # Ask Ollama for N unique image prompts
        list_prompt = (
            f"Based on this exact scene description:\n\n"
            f"\"{prompt}\"\n\n"
            f"Create {num_clips} short image generation prompts that stay true to the main character, mood, and setting. "
            f"Vary only small details like background, lighting, angle, or pose — but never change the subject."
            f"\n\nOnly return a numbered list with no extra text."
        )
        resp = await ollama_client.chat([
            {"role": "system", "content": "You generate multiple diverse prompts for AI image creation."},
            {"role": "user", "content": list_prompt}
        ])
        raw_list = resp["message"]["content"]
        image_prompts = self.parse_ollama_prompt_list(raw_list, num_clips)

        for i in range(num_clips):
            image_prompt = (
                image_prompts[i]
                if image_prompts and i < len(image_prompts)
                else f"{prompt} — Scene {i+1}"
            )

            image_bytes = await asyncio.to_thread(
                ComfyUIImagePlugin.process_prompt,
                image_prompt, w, h
            )

            tmp_img = f"/tmp/{job_id}_frame_{i}.png"
            with open(tmp_img, "wb") as f:
                f.write(image_bytes)
            temp_paths.append(tmp_img)

            with open(tmp_img, "rb") as f:
                image_content = f.read()
            desc = await vision_plugin.process_image_web(image_content, tmp_img)
            desc = desc.strip() or "An interesting scene"

            animation_prompt = (
                f"The following is a visual description of an image:\n\n"
                f"\"{desc}\"\n\n"
                f"And this was the original prompt:\n\n"
                f"\"{image_prompt}\"\n\n"
                "Write a single clear sentence that describes what this image depicts and how it might animate to reflect the user's intent."
            )
            anim_resp = await ollama_client.chat([
                {"role": "system", "content": "You generate vivid single-sentence descriptions that combine image content and user prompt for animation."},
                {"role": "user", "content": animation_prompt}
            ])
            animation_desc = anim_resp["message"]["content"].strip() or "A short animation that reflects the prompt."

            anim_bytes, ext = await asyncio.to_thread(
                anim_plugin.process_prompt,
                animation_desc,
                image_bytes,
                f"clip_{i}.png",
                w,
                h,
                duration * 16
            )

            anim_path = f"/tmp/{job_id}_clip_{i}.{ext}"
            with open(anim_path, "wb") as f:
                f.write(anim_bytes)
            temp_paths.append(anim_path)

            if ext == "webp":
                mp4_path = f"/tmp/{job_id}_clip_{i}.mp4"
                self.webp_to_mp4(anim_path, mp4_path, fps=16, duration=duration)
                temp_paths.append(mp4_path)
                final_clips.append(mp4_path)
            else:
                final_clips.append(anim_path)

        if not final_clips:
            return "❌ No clips generated."

        out_path = f"/tmp/{job_id}_final.mp4"
        self.ffmpeg_concat(final_clips, out_path)

        with open(out_path, "rb") as f:
            final_bytes = f.read()
        temp_paths.append(out_path)

        msg = await ollama_client.chat([
            {"role": "system", "content": f"The user has just been shown a video based on '{prompt}'."},
            {"role": "user", "content": "Reply with a short, fun message celebrating the video. No lead-in phrases or instructions."}
        ])

        self.cleanup_temp_files(temp_paths)

        return [
            {
                "type": "video",
                "name": "generated_video.mp4",
                "data": base64.b64encode(final_bytes).decode(),
                "mimetype": "video/mp4"
            },
            msg["message"]["content"].strip() or "🎬 Here’s your video!"
        ]

    async def handle_discord(self, message, args, ollama_client):
        return "❌ This plugin is only available in the WebUI due to file size limitations."

    async def handle_webui(self, args, ollama_client):
        if "prompt" not in args:
            return ["No prompt provided."]
        try:
            asyncio.get_running_loop()
            return await self._generate_video(args["prompt"], ollama_client)
        except RuntimeError:
            return asyncio.run(self._generate_video(args["prompt"], ollama_client))
        except Exception as e:
            return [f"⚠️ Error generating video: {e}"]

    async def handle_irc(self, bot, channel, user, raw, args, ollama_client):
        await bot.privmsg(channel, f"{user}: This plugin is supported only on WebUI.")

plugin = ComfyUIVideoPlugin()