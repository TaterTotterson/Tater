# plugins/comfyui_music_video_plugin.py
import os
import asyncio
import base64
import subprocess
import json
import uuid
from PIL import Image
from moviepy.video.io.ImageSequenceClip import ImageSequenceClip
from plugin_base import ToolPlugin
from helpers import redis_client
from plugins.comfyui_audio_ace import ComfyUIAudioAcePlugin
from plugins.comfyui_image_plugin import ComfyUIImagePlugin
from plugins.comfyui_image_video_plugin import ComfyUIImageVideoPlugin
from plugins.vision_describer import VisionDescriberPlugin

class ComfyUIMusicVideoPlugin(ToolPlugin):
    name = "comfyui_music_video"
    plugin_name = "ComfyUI Music Video"
    usage = (
        '{\n'
        '  "function": "comfyui_music_video",\n'
        '  "arguments": {"prompt": "<Concept for the song>"}\n'
        '}\n'
    )
    description = "Generates a complete AI music video including lyrics, music, and animated visuals by orchestrating ComfyUI plugins."
    plugin_dec = "Build a full AI music video\u2014lyrics, music, and animated visuals\u2014using ComfyUI."
    pretty_name = "Your Music Video"
    platforms = ["webui"]
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

    def ffmpeg_concat(self, video_paths, audio_path, out):
        listpath = f"{out}_concat_list.txt"
        with open(listpath, "w") as f:
            for p in video_paths:
                f.write(f"file '{p}'\n")
        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", listpath, "-i", audio_path, "-c:v", "libx264", "-c:a", "aac", "-shortest", out]
        subprocess.run(cmd, check=True)
        os.remove(listpath)

    def cleanup_temp_files(self, job_id, count, exts):
        try:
            for ext in ["mp3", "mp4"]:
                for suffix in ["audio", "final", "final_small"]:
                    path = f"/tmp/{job_id}_{suffix}.{ext}"
                    if os.path.exists(path):
                        os.remove(path)
            for i in range(count):
                for ext in exts:
                    path = f"/tmp/{job_id}_clip_{i}.{ext}"
                    if os.path.exists(path):
                        os.remove(path)
        except Exception as e:
            print(f"[Cleanup warning] {e}")

    def webp_to_mp4(self, input_file, output_file, fps=16, duration=5):
        frames, tmp_dir, frame_files = [], f"{os.path.dirname(input_file)}/frames", []
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

    async def _generate_music_video(self, prompt, llm_client):
        vision_plugin = VisionDescriberPlugin()
        job_id = str(uuid.uuid4())[:8]
        audio_plugin = ComfyUIAudioAcePlugin()

        # --- 1) Lyrics + tags via Audio Ace ---
        tags, lyrics = await audio_plugin.get_tags_and_lyrics(prompt, llm_client)
        if not lyrics:
            return "❌ No lyrics returned for visuals."

        # --- 2) Render audio using the refactored per-job Comfy helper ---
        audio_path = f"/tmp/{job_id}_audio.mp3"
        final_video_path = f"/tmp/{job_id}_final.mp4"

        # CHANGED: call the new sync method and run it in a worker thread
        audio_bytes = await asyncio.to_thread(audio_plugin.process_prompt_sync, tags, lyrics)
        with open(audio_path, "wb") as f:
            f.write(audio_bytes)

        duration = self.get_mp3_duration(audio_path)
        duration = max(30, min(300, duration))

        sections = self.split_sections(lyrics)
        if not sections:
            return "❌ No sections found for animation."

        num_clips = len(sections) * 2
        per = duration / num_clips

        # Pull FPS once from the image→video workflow stored in Redis
        wf = ComfyUIImageVideoPlugin.get_workflow_template()
        fps = 16  # default
        for node in wf.values():
            if node.get("class_type") == "CreateVideo":
                try:
                    fps = int(node["inputs"].get("fps", 16))
                except Exception:
                    fps = 16
                break

        vids = []
        exts_used = set()

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
        clip_idx = 0

        for section in sections:
            for part_num in range(2):
                part_hint = f" (Part {part_num + 1})" if part_num > 0 else ""

                img_desc_prompt = (
                    f'The following are song lyrics:\n\n"{section}"\n\n'
                    "Write a single clear sentence describing a visual scene or illustration that conveys the meaning, emotion, or subject of these lyrics. "
                    "If the lyrics are subjective or abstract (e.g., 'Do you think I'm beautiful?'), imagine a representative visual. "
                    "Avoid text overlays and focus on a vivid scene." + part_hint
                )

                img_resp = await llm_client.chat([
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

                tmp_img = f"/tmp/{job_id}_frame_{clip_idx}.png"
                with open(tmp_img, "wb") as f:
                    f.write(image_bytes)

                with open(tmp_img, "rb") as f:
                    image_content = f.read()
                desc = await vision_plugin.process_image_web(image_content, tmp_img)
                desc = desc.strip() or "An interesting scene"

                animation_prompt = (
                    f'The following is a visual description of an image:\n\n"{desc}"\n\n'
                    f'And here is a section of song lyrics:\n\n"{section}"\n\n'
                    "Write a single clear sentence that describes what this image depicts and how it might animate to reflect the lyrics."
                )

                resp = await llm_client.chat([
                    {"role": "system", "content": "You generate vivid single-sentence descriptions that combine image content and lyric context for animation."},
                    {"role": "user", "content": animation_prompt}
                ])
                animation_desc = resp["message"]["content"].strip() or "A scene that reflects the lyrics."

                upload_filename = f"frame_{clip_idx}.png"

                # frame count uses the fps we pulled from the template
                frame_count = int(per * fps)

                anim_bytes, ext = await asyncio.to_thread(
                    anim_plugin.process_prompt,
                    animation_desc,
                    image_bytes,
                    upload_filename,
                    w,
                    h,
                    frame_count
                )

                exts_used.add(ext)
                tmp_path = f"/tmp/{job_id}_clip_{clip_idx}.{ext}"
                with open(tmp_path, "wb") as f:
                    f.write(anim_bytes)

                if ext == "webp":
                    tmp_mp4 = f"/tmp/{job_id}_clip_{clip_idx}.mp4"
                    # keep playback speed consistent with generation
                    self.webp_to_mp4(tmp_path, tmp_mp4, fps=fps, duration=per)
                    vids.append(tmp_mp4)
                else:
                    vids.append(tmp_path)

                clip_idx += 1

        if not vids:
            return "❌ Failed to generate any video clips."

        self.ffmpeg_concat(vids, audio_path, final_video_path)

        with open(final_video_path, "rb") as f:
            final_bytes = f.read()

        msg = await llm_client.chat([
            {"role": "system", "content": f"User got a music video for '{prompt}'"},
            {"role": "user", "content": "Send short celebration text."}
        ])

        self.cleanup_temp_files(job_id, clip_idx, list(exts_used))

        return [
            {
                "type": "video",
                "name": "music_video.mp4",
                "data": base64.b64encode(final_bytes).decode(),
                "mimetype": "video/mp4"
            },
            msg["message"]["content"]
        ]

    async def handle_webui(self, args, llm_client):
        if "prompt" not in args:
            return ["No prompt given."]

        async def _generate():
            return await self._generate_music_video(args["prompt"], llm_client)

        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                return await _generate()
        except RuntimeError:
            pass  # Not in a loop, fall through

        # Run from a background thread (Streamlit thread-safe)
        return asyncio.run(_generate())

plugin = ComfyUIMusicVideoPlugin()
