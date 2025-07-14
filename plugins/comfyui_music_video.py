# plugins/comfyui_music_video.py
import os
import asyncio
import base64
import subprocess
import json
from PIL import Image
from moviepy.video.io.ImageSequenceClip import ImageSequenceClip
from plugin_base import ToolPlugin
import discord
from helpers import format_irc, redis_client

from plugins.comfyui_audio_ace import ComfyUIAudioAcePlugin
from plugins.comfyui_image_plugin import ComfyUIImagePlugin
from plugins.comfyui_image_video_plugin import ComfyUIImageVideoPlugin

class ComfyUIMusicVideoPlugin(ToolPlugin):
    name = "comfyui_music_video"
    usage = (
        '{\n'
        '  "function": "comfyui_music_video",\n'
        '  "arguments": {"prompt": "<Concept for the song>"}\n'
        '}\n'
    )
    description = "Generates a complete AI music video including lyrics, music, and animated visuals by orchestrating ComfyUI plugins."
    platforms = ["discord", "webui"]
    waiting_prompt_template = "Generate a fun, upbeat message saying you're composing the full music video now! Only output that message."
    settings_category = "ComfyUI Music Video"
    required_settings = {
        "MUSIC_VIDEO_RESOLUTION": {
            "label": "ComfyUI Animation Resolution",
            "type": "select",
            "default": "720p",
            "options": ["144p", "240p", "360p", "480p", "720p", "1080p"],
            "description": "Target resolution for animation clips."
        }
    }

    @staticmethod
    def split_sections(lyrics):
        sections, current_section, current_tag = [], "", None
        allowed_tags = ["[verse]", "[chorus]", "[bridge]", "[outro]"]
        for line in lyrics.splitlines():
            line = line.strip().lower()
            if line in allowed_tags:
                if current_section:
                    sections.append(current_section.strip())
                    current_section = ""
                current_tag = line
                continue
            if current_tag:
                current_section += line + " "
        if current_section:
            sections.append(current_section.strip())
        return sections

    @staticmethod
    def get_mp3_duration(filename):
        cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "json", filename]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        info = json.loads(result.stdout)
        return float(info["format"]["duration"])

    def webp_to_mp4(self, input_file, output_file, fps=16, duration=5):
        frames, tmp_dir, frame_files = [], "/tmp/temp_frames", []
        os.makedirs(tmp_dir, exist_ok=True)
        try:
            im = Image.open(input_file)
            while True:
                frames.append(im.copy().convert("RGBA"))
                im.seek(im.tell() + 1)
        except EOFError:
            pass
        if not frames:
            raise RuntimeError(f"No frames extracted from {input_file}")
        for idx, frame in enumerate(frames):
            path = f"{tmp_dir}/frame_{idx}.png"
            frame.save(path, "PNG")
            frame_files.append(path)
        if len(frame_files) == 1:
            frame_files *= int(fps * duration)
        clip = ImageSequenceClip(frame_files, fps=fps)
        clip.write_videofile(output_file, codec='libx264', fps=fps, audio=False, logger=None)
        for p in frame_files:
            os.remove(p)
        os.rmdir(tmp_dir)

    def ffmpeg_concat(self, video_paths, audio_path, out):
        listpath = "/tmp/concat_list.txt"
        with open(listpath, "w") as f:
            for p in video_paths:
                f.write(f"file '{p}'\n")
        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", listpath, "-i", audio_path, "-c:v", "libx264", "-c:a", "aac", "-shortest", out]
        asyncio.run(asyncio.to_thread(os.system, ' '.join(cmd)))

    def cleanup_temp_files(self, count):
        try:
            # Always remove these known temp files
            for path in ["/tmp/audio.mp3", "/tmp/final.mp4", "/tmp/final_small.mp4"]:
                if os.path.exists(path):
                    os.remove(path)

            for i in range(count):
                for ext in ["webp", "mp4", "png"]:
                    path = f"/tmp/clip_{i}.{ext}" if ext != "png" else f"/tmp/frame_{i}.png"
                    if os.path.exists(path):
                        os.remove(path)
        except Exception as e:
            print(f"[Cleanup warning] {e}")

    async def generate_music_video(self, prompt, ollama_client):
        audio_plugin = ComfyUIAudioAcePlugin()
        try:
            tags, lyrics = await audio_plugin.get_tags_and_lyrics(prompt, ollama_client)
        except Exception as e:
            return f"❌ Failed to generate lyrics: {e}"

        if not lyrics:
            return "❌ No lyrics returned for visuals."

        audio_bytes = await asyncio.to_thread(audio_plugin.process_prompt, prompt, tags, lyrics)
        with open("/tmp/audio.mp3", "wb") as f:
            f.write(audio_bytes)

        duration = self.get_mp3_duration("/tmp/audio.mp3")
        duration = max(30, min(300, duration))

        sections = self.split_sections(lyrics)
        if not sections:
            return "❌ No sections found for animation."

        per = duration / len(sections)
        vids = []

        res_map = {
            "144p": (256, 144),
            "240p": (426, 240),
            "360p": (480, 360),
            "480p": (640, 480),
            "720p": (1280, 720),
            "1080p": (1920, 1080)
        }

        settings = redis_client.hgetall(f"plugin_settings:{self.settings_category}")
        raw = settings.get("MUSIC_VIDEO_RESOLUTION", b"720p")
        resolution = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        w, h = res_map.get(resolution, (1280, 720))

        anim_plugin = ComfyUIImageVideoPlugin()

        for i, section in enumerate(sections):
            img_desc_prompt = f"Analyze these lyrics:\n\n\"{section}\"\n\nWrite a single sentence describing a scene or illustration that could visually represent this part of the song."
            img_resp = await ollama_client.chat([
                {"role": "system", "content": "You help generate creative prompts for AI-generated illustrations."},
                {"role": "user", "content": img_desc_prompt}
            ])
            image_prompt = img_resp["message"]["content"].strip()

            image_bytes = await asyncio.to_thread(
                ComfyUIImagePlugin.process_prompt,
                image_prompt,
                w,
                h
            )

            tmp_img = f"/tmp/frame_{i}.png"
            with open(tmp_img, "wb") as f:
                f.write(image_bytes)

            # REMOVE Ollama animation prompt
            animation_desc = ""

            # Call animation as before:
            anim_bytes = await asyncio.to_thread(
                anim_plugin.process_prompt,
                animation_desc,
                image_bytes,
                f"frame_{i}.png",
                w,
                h,
                int(per * 16)
            )

            tmp_input = f"/tmp/clip_{i}.webp"
            with open(tmp_input, "wb") as f:
                f.write(anim_bytes)

            tmp_mp4 = f"/tmp/clip_{i}.mp4"
            self.webp_to_mp4(tmp_input, tmp_mp4, fps=16, duration=per)
            vids.append(tmp_mp4)

        if not vids:
            return "❌ Failed to generate any video clips."

        final = "/tmp/final.mp4"
        self.ffmpeg_concat(vids, "/tmp/audio.mp3", final)

        with open(final, "rb") as f:
            final_bytes = f.read()

        msg = await ollama_client.chat([
            {"role": "system", "content": f"User got a music video for '{prompt}'"},
            {"role": "user", "content": "Send short celebration text."}
        ])

        return [{
            "type": "video",
            "name": "music_video.mp4",
            "data": base64.b64encode(final_bytes).decode(),
            "mimetype": "video/mp4"
        }, msg["message"]["content"], len(vids)]

    async def handle_discord(self, message, args, ollama_client):
        if "prompt" not in args:
            return "No prompt given."
        try:
            result = await self.generate_music_video(args["prompt"], ollama_client)
            if isinstance(result, list) and len(result) == 3:
                payload, text, count = result
                decoded = base64.b64decode(payload["data"])
                if len(decoded) / (1024 * 1024) > 8:
                    # Re-encode for smaller size but keep MP4 output
                    subprocess.run([
                        "ffmpeg", "-y", "-i", "/tmp/final.mp4",
                        "-c:v", "libx264", "-b:v", "200k",
                        "-c:a", "aac", "/tmp/final_small.mp4"
                    ], check=True)
                    with open("/tmp/final_small.mp4", "rb") as f:
                        mp4_bytes = f.read()
                    self.cleanup_temp_files(count)
                    return [
                        {
                            "type": "video",
                            "name": "music_video.mp4",
                            "data": base64.b64encode(mp4_bytes).decode(),
                            "mimetype": "video/mp4"
                        },
                        text
                    ]
                else:
                    self.cleanup_temp_files(count)
                    return [payload, text]
            return result
        except Exception as e:
            return f"⚠️ Error generating music video: {e}"

    async def handle_webui(self, args, ollama_client):
        if "prompt" not in args:
            return "No prompt given."
        try:
            result = await self.generate_music_video(args["prompt"], ollama_client)
            if isinstance(result, list) and len(result) == 3:
                payload, text, count = result
                self.cleanup_temp_files(count)
                return [payload, text]
            return result
        except Exception as e:
            return f"⚠️ Error generating music video: {e}"

    async def handle_irc(self, bot, channel, user, raw, args, ollama_client):
        await bot.privmsg(channel, f"{user}: {format_irc('⚠️ Sorry, this works only with Discord or WebUI.')}")

plugin = ComfyUIMusicVideoPlugin()