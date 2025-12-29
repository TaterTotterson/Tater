# plugins/comfyui_video_plugin.py
import os
import asyncio
import base64
import uuid
import subprocess
import json
from PIL import Image
from typing import List, Tuple
from moviepy.video.io.ImageSequenceClip import ImageSequenceClip
from plugin_base import ToolPlugin
from helpers import redis_client
from plugins.comfyui_image_plugin import ComfyUIImagePlugin
from plugins.comfyui_image_video_plugin import ComfyUIImageVideoPlugin

class ComfyUIVideoPlugin(ToolPlugin):
    name = "comfyui_video_plugin"
    plugin_name = "ComfyUI Video"
    usage = (
        '{\n'
        '  "function": "comfyui_video_plugin",\n'
        '  "arguments": {"prompt": "<Describe the video you want>"}\n'
        '}\n'
    )
    description = "Generates a video from a text prompt by creating multiple animated clips using ComfyUI, then merging them into one MP4."
    plugin_dec = "Create a short video from a text prompt by stitching ComfyUI-generated clips."
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
            "label": "Clip Length (seconds)",
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
    waiting_prompt_template = "Generate a fun, upbeat message saying you’re directing a short video now! Only output that message."

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

    async def _derive_motion_directive(self, raw: str, llm_client) -> str:
        sys = (
            "Extract the intended animation as ONE concise directive (<= 24 words). "
            "Prioritize clear SUBJECT ACTIONS over camera motion (e.g., hand to face, laugh, brush hair behind ear). "
            "You may include a SINGLE gentle camera cue. "
            "Do not contradict explicit pose/clothing/framing if specified."
        )
        usr = f'User request: "{raw}"\nReturn only the motion directive (example: "hand to face; soft laugh; brush hair behind ear; slight dolly-in").'
        try:
            resp = await llm_client.chat([
                {"role": "system", "content": sys},
                {"role": "user", "content": usr}
            ])
            motion = (resp.get("message", {}) or {}).get("content", "").strip()
            return motion[:200] if motion else ""
        except Exception:
            return ""

    async def _per_clip_motion_prompts(self, scene_prompts, base_motion, llm_client):
        if not scene_prompts:
            return []
        if not base_motion:
            base_motion = "hand to face; soft laugh; brush hair behind ear"

        n = len(scene_prompts)
        scenes_block = "\n".join([f"{i+1}. {sp}" for i, sp in enumerate(scene_prompts)])
        sys = (
            "For each scene, write a short 2–3 beat SUBJECT ACTION choreography for a ~15s clip. "
            "Each line <= 28 words. "
            "Beats should read in order using semicolons (e.g., 'touch cheek; laugh; push hair behind ear'). "
            "Include optional gentle camera cue last (e.g., 'subtle dolly-in'). "
            "Keep pose/clothing/framing exactly as given; do not invent props."
        )
        usr = (
            f'Base actions to preserve if present: "{base_motion}"\n\n'
            f"Scenes ({n} lines):\n{scenes_block}\n\n"
            f"Return exactly {n} lines. Numbered or plain is fine."
        )
        try:
            resp = await llm_client.chat([
                {"role": "system", "content": sys},
                {"role": "user", "content": usr}
            ])
            raw = (resp.get("message", {}) or {}).get("content", "") or ""
            lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
            out = []
            for ln in lines:
                if ln and ln[0].isdigit():
                    parts = ln.split(".", 1)
                    if len(parts) == 1:
                        parts = ln.split(")", 1)
                    ln = parts[1].strip() if len(parts) == 2 else ln
                out.append(ln)
            if len(out) < n:
                out += [base_motion] * (n - len(out))
            return out[:n]
        except Exception:
            return [base_motion] * len(scene_prompts)

    async def _generate_video(self, prompt, llm_client):
        # --- read settings safely
        settings = redis_client.hgetall(f"plugin_settings:{self.settings_category}")
        raw_res = settings.get("VIDEO_RESOLUTION", b"720p")
        resolution = raw_res.decode() if isinstance(raw_res, bytes) else raw_res
        w, h = self.res_map.get(resolution, (1280, 720))

        def _as_int(raw, default):
            try:
                return int(raw.decode() if isinstance(raw, bytes) else raw)
            except Exception:
                return default

        seconds_per_clip = max(1, _as_int(settings.get("VIDEO_LENGTH", b"5"), 5))
        num_clips = max(1, min(20, _as_int(settings.get("VIDEO_CLIPS", b"1"), 1)))  # cap to avoid runaway jobs

        # FPS from the image→video workflow (fallback 16)
        try:
            wf = ComfyUIImageVideoPlugin.get_workflow_template()
            fps = next(
                (int(n["inputs"].get("fps", 16)) for n in wf.values() if n.get("class_type") == "CreateVideo"),
                16
            )
        except Exception:
            fps = 16

        frame_count = max(1, int(seconds_per_clip * fps))

        job_id = str(uuid.uuid4())[:8]
        temp_paths, final_clips = [], []
        anim_plugin = ComfyUIImageVideoPlugin()

        # --- Ask LLM for N short scene variations of the same concept ---
        scene_list_prompt = (
            f'Original brief:\n"{prompt}"\n\n'
            f"Task: Write {num_clips} distinct, concise scene prompts.\n"
            "HARD CONSTRAINTS (apply to EVERY line):\n"
            "- Keep the SAME subject, mood, number of people, and gender.\n"
            '- If the brief explicitly states pose/position (e.g., "laying on her side"), clothing, accessories, or framing (e.g., "full body"), REPEAT those words VERBATIM in every line.\n'
            "- Do NOT contradict any explicit motion; you may omit motion unless essential.\n"
            "- Do NOT change pose, clothing, or framing; do NOT introduce new props or wardrobe.\n"
            "ALLOWED VARIATION:\n"
            "- Only small changes to camera angle, lens, lighting, time of day, or background details.\n"
            "OUTPUT FORMAT:\n"
            f"- Exactly {num_clips} lines, each ≤ 25 words.\n"
            "- Numbered list using '1. ', '2. ', etc. No 'Scene:' prefixes.\n"
            "- Return ONLY the list.\n"
        )

        try:
            resp = await llm_client.chat([
                {"role": "system", "content": "You write concise, varied scene prompts that keep subject & mood consistent."},
                {"role": "user", "content": scene_list_prompt}
            ])
            raw_list = (resp.get("message", {}) or {}).get("content", "") or ""
            image_prompts = []
            for line in raw_list.splitlines():
                line = line.strip()
                if not line:
                    continue
                if line[0].isdigit():
                    parts = line.split(".", 1)
                    if len(parts) == 1:
                        parts = line.split(")", 1)
                    if len(parts) == 1:
                        parts = line.split("-", 1)
                    if len(parts) == 2 and parts[1].strip():
                        image_prompts.append(parts[1].strip())
                        continue
                image_prompts.append(line)
            if not image_prompts:
                image_prompts = [prompt] * num_clips
            elif len(image_prompts) < num_clips:
                image_prompts += [image_prompts[-1]] * (num_clips - len(image_prompts))
            else:
                image_prompts = image_prompts[:num_clips]
        except Exception:
            image_prompts = [prompt] * num_clips

        # NEW: derive motion + tailor per-clip motion prompts
        base_motion = await self._derive_motion_directive(prompt, llm_client)
        motion_prompts = await self._per_clip_motion_prompts(image_prompts, base_motion, llm_client)

        out_path = f"/tmp/{job_id}_final.mp4"

        try:
            for i in range(num_clips):
                scene_prompt = image_prompts[i]
                motion_prompt = motion_prompts[i] if i < len(motion_prompts) else base_motion

                # 1) Generate a starting image (clean scene prompt)
                image_bytes = await asyncio.to_thread(
                    ComfyUIImagePlugin.process_prompt,
                    scene_prompt, w, h
                )

                tmp_img = f"/tmp/{job_id}_frame_{i}.png"
                with open(tmp_img, "wb") as f:
                    f.write(image_bytes)
                temp_paths.append(tmp_img)

                # 2) Animate using scene + motion in the SAME 'prompt'
                anim_prompt = f"{scene_prompt}\nMotion: {motion_prompt}"

                anim_bytes, ext = await asyncio.to_thread(
                    anim_plugin.process_prompt,   # signature unchanged
                    anim_prompt,                  # blended prompt
                    image_bytes,
                    f"clip_{i}.png",
                    w,
                    h,
                    frame_count
                )

                anim_path = f"/tmp/{job_id}_clip_{i}.{ext}"
                with open(anim_path, "wb") as f:
                    f.write(anim_bytes)
                temp_paths.append(anim_path)

                if ext == "webp":
                    mp4_path = f"/tmp/{job_id}_clip_{i}.mp4"
                    self.webp_to_mp4(anim_path, mp4_path, fps=fps, duration=seconds_per_clip)
                    temp_paths.append(mp4_path)
                    final_clips.append(mp4_path)
                else:
                    final_clips.append(anim_path)

            if not final_clips:
                return "❌ No clips generated."

            self.ffmpeg_concat(final_clips, out_path)
            with open(out_path, "rb") as f:
                final_bytes = f.read()
            temp_paths.append(out_path)

            msg = await llm_client.chat([
                {"role": "system", "content": f"The user has just been shown a video based on '{prompt}'."},
                {"role": "user", "content": "Reply with a short, fun message celebrating the video. No lead-in phrases or instructions."}
            ])

            return [
                {
                    "type": "video",
                    "name": "generated_video.mp4",
                    "data": base64.b64encode(final_bytes).decode(),
                    "mimetype": "video/mp4"
                },
                (msg["message"]["content"].strip() if msg and msg.get("message") else "") or "🎬 Here’s your video!"
            ]
        finally:
            self.cleanup_temp_files(temp_paths)

    async def handle_discord(self, message, args, llm_client):
        return "❌ This plugin is only available in the WebUI due to file size limitations."

    async def handle_webui(self, args, llm_client):
        if "prompt" not in args:
            return ["No prompt provided."]
        try:
            asyncio.get_running_loop()
            return await self._generate_video(args["prompt"], llm_client)
        except RuntimeError:
            return asyncio.run(self._generate_video(args["prompt"], llm_client))
        except Exception as e:
            return [f"⚠️ Error generating video: {e}"]

    async def handle_irc(self, bot, channel, user, raw, args, llm_client):
        await bot.privmsg(channel, f"{user}: This plugin is supported only on WebUI.")

plugin = ComfyUIVideoPlugin()
