# plugins/comfyui_image_video.py
import os
import json
import requests
import asyncio
import secrets
import copy
import logging
import time
import uuid
from PIL import Image
from moviepy.video.io.ImageSequenceClip import ImageSequenceClip
from plugin_base import ToolPlugin
from helpers import redis_client, get_latest_image_from_history, run_comfy_prompt

logger = logging.getLogger("comfyui_image_video")
logger.setLevel(logging.INFO)


def _build_media_metadata(binary: bytes, *, media_type: str, name: str, mimetype: str) -> dict:
    if not isinstance(binary, (bytes, bytearray)):
        raise TypeError("binary must be bytes")
    return {
        "type": media_type,
        "name": name,
        "mimetype": mimetype,
        "size": len(binary),
        "bytes": bytes(binary),
    }

class ComfyUIImageVideoPlugin(ToolPlugin):
    name = "comfyui_image_video"
    plugin_name = "ComfyUI Animate Image"
    version = "1.0.1"
    min_tater_version = "50"
    usage = (
        '{\n'
        '  "function": "comfyui_image_video",\n'
        '  "arguments": {"prompt": "<Describe how you want the animation to move or behave>"}\n'
        '}'
    )
    description = "Animates the most recent image in chat into a looping WebP or MP4 using ComfyUI."
    plugin_dec = "Animate the most recent image in chat into a looping WebP or MP4 via ComfyUI."
    pretty_name = "Animating Your Image"
    settings_category = "ComfyUI Animate Image"
    required_settings = {
        "COMFYUI_VIDEO_URL": {
            "label": "ComfyUI Server URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "Base URL (host:port) for your ComfyUI instance."
        },
        "IMAGE_RESOLUTION": {
            "label": "Animation Resolution",
            "type": "select",
            "default": "720p",
            "options": ["144p", "240p", "360p", "480p", "720p", "1080p"],
            "description": "Default resolution for generated animations."
        },
        "LENGTH": {
            "label": "Default Animation Length (seconds)",
            "type": "number",
            "default": 10,
            "description": "Approximate animation length in seconds."
        },
        "COMFYUI_VIDEO_WORKFLOW": {
            "label": "Workflow Template (JSON)",
            "type": "file",
            "default": "",
            "description": "Upload your ComfyUI JSON workflow template."
        }
    }
    waiting_prompt_template = "Generate a playful, friendly message saying you’re bringing their image to life now! Only output that message."
    platforms = ["webui"]

    # ---------------------------
    # URL helpers
    # ---------------------------
    @staticmethod
    def get_base_http() -> str:
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImageVideoPlugin.settings_category}")
        url_raw = settings.get("COMFYUI_VIDEO_URL", b"")
        url = url_raw.decode("utf-8").strip() if isinstance(url_raw, (bytes, bytearray)) else (url_raw or "").strip()
        if not url:
            url = "http://localhost:8188"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url
        return url.rstrip("/")

    @staticmethod
    def get_base_ws(base_http: str) -> str:
        # http->ws, https->wss
        scheme = "wss" if base_http.startswith("https://") else "ws"
        return base_http.replace("http", scheme, 1)

    # ---------------------------
    # Workflow/template + I/O
    # ---------------------------
    @staticmethod
    def get_workflow_template() -> dict:
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImageVideoPlugin.settings_category}")
        raw = settings.get("COMFYUI_VIDEO_WORKFLOW", b"")
        workflow_str = raw.decode("utf-8").strip() if isinstance(raw, (bytes, bytearray)) else (raw or "").strip()
        if not workflow_str:
            raise RuntimeError("No workflow found in Redis. Please upload a valid JSON workflow.")
        return json.loads(workflow_str)

    @staticmethod
    def upload_image(base_http: str, image_bytes: bytes, filename: str) -> str:
        resp = requests.post(
            f"{base_http}/upload/image",
            files={"image": (filename, image_bytes)},
            data={"overwrite": "true"},
            timeout=60
        )
        resp.raise_for_status()
        result = resp.json()
        name = result.get("name") or result.get("filename")
        sub = result.get("subfolder", "")
        return f"{sub}/{name}" if sub else name

    @staticmethod
    def get_history(base_http: str, prompt_id: str) -> dict:
        r = requests.get(f"{base_http}/history/{prompt_id}", timeout=30)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def fetch_asset(base_http: str, filename: str, subfolder: str, folder_type: str) -> bytes:
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        r = requests.get(f"{base_http}/view", params=params, timeout=60)
        r.raise_for_status()
        return r.content

    @staticmethod
    def get_fps_from_workflow(wf: dict, default: int = 16) -> int:
        try:
            for node in wf.values():
                if node.get("class_type") == "CreateVideo":
                    return int(node["inputs"].get("fps", default))
        except Exception:
            pass
        return default

    # Convert animated webp (from Comfy) into mp4 for better compatibility
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

    # ---------------------------
    # Core generation (sync)
    # ---------------------------
    @staticmethod
    def _apply_overrides(workflow: dict, prompt_text: str, uploaded_image_path: str, width: int, height: int, frames: int):
        """Patch the uploaded image path, prompt text, and geometry/length into the workflow."""
        patched_first_prompt = False
        for node in workflow.values():
            if not isinstance(node, dict):
                continue

            # point loaders to uploaded image
            if node.get("class_type") == "LoadImage":
                node.setdefault("inputs", {})
                node["inputs"]["image"] = uploaded_image_path

            # insert prompt for CLIPTextEncode (Positive/Prompt)
            if node.get("class_type") == "CLIPTextEncode" and (
                "Positive" in node.get("_meta", {}).get("title", "") or
                "Prompt" in node.get("_meta", {}).get("title", "")
            ):
                if not patched_first_prompt:
                    node.setdefault("inputs", {})
                    node["inputs"]["text"] = prompt_text
                    patched_first_prompt = True

            # set width/height/length on common I2V nodes (WAN 2.2 etc.)
            if node.get("class_type") in {
                "WanImageToVideo",
                "WanVaceToVideo",
                "CosmosPredict2ImageToVideoLatent",
                "Wan22ImageToVideoLatent",
            }:
                node.setdefault("inputs", {})
                node["inputs"]["width"] = width
                node["inputs"]["height"] = height
                node["inputs"]["length"] = frames

    @staticmethod
    def process_prompt(prompt: str, image_bytes: bytes, filename: str, width: int = None, height: int = None, length: int = None):
        base_http = ComfyUIImageVideoPlugin.get_base_http()
        base_ws   = ComfyUIImageVideoPlugin.get_base_ws(base_http)

        # upload the source image
        uploaded = ComfyUIImageVideoPlugin.upload_image(base_http, image_bytes, filename)

        # pull and clone template per job
        wf = copy.deepcopy(ComfyUIImageVideoPlugin.get_workflow_template())

        # resolution defaults
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImageVideoPlugin.settings_category}") or {}
        res_map = {
            "144p": (256, 144), "240p": (426, 240), "360p": (480, 360),
            "480p": (640, 480), "720p": (1280, 720), "1080p": (1920, 1080)
        }
        raw_res = settings.get("IMAGE_RESOLUTION", b"480p")
        resolution = raw_res.decode("utf-8") if isinstance(raw_res, (bytes, bytearray)) else (raw_res or "480p")
        default_w, default_h = res_map.get(resolution, (640, 480))

        # fps from workflow
        fps = ComfyUIImageVideoPlugin.get_fps_from_workflow(wf, default=16)

        # length defaults
        raw_length = settings.get("LENGTH", b"1")
        try:
            default_seconds = int(raw_length.decode() if isinstance(raw_length, (bytes, bytearray)) else raw_length)
        except Exception:
            default_seconds = 1
        default_frames = max(1, default_seconds * fps)

        try:
            w = int(width) if width is not None else int(default_w)
        except Exception:
            w = int(default_w)
        try:
            h = int(height) if height is not None else int(default_h)
        except Exception:
            h = int(default_h)
        try:
            frames = int(length) if length is not None else int(default_frames)
        except Exception:
            frames = int(default_frames)

        w = max(64, min(w, 4096))
        h = max(64, min(h, 4096))
        frames = max(1, min(frames, 6000))

        # randomize motion seeds
        random_seed = secrets.randbits(63)
        for node in wf.values():
            if not isinstance(node, dict):
                continue
            if node.get("class_type") in {"KSampler", "KSamplerAdvanced"}:
                inputs = node.get("inputs", {})
                if inputs.get("add_noise") == "enable" and "noise_seed" in inputs:
                    inputs["noise_seed"] = int(random_seed)
                if "seed" in inputs:
                    inputs["seed"] = int(random_seed)

        # patch prompt/image/geometry into the per-job workflow
        ComfyUIImageVideoPlugin._apply_overrides(wf, prompt, uploaded, w, h, frames)

        # run Comfy with a per-job client_id + WS and wait by prompt_id
        prompt_id, _ = run_comfy_prompt(base_http, base_ws, wf)

        # fetch outputs from history (poll because Comfy history can lag after prompt completion)
        for _ in range(40):
            hist = ComfyUIImageVideoPlugin.get_history(base_http, prompt_id).get(prompt_id, {})
            outputs = hist.get("outputs", {}) if isinstance(hist, dict) else {}

            for node in outputs.values():
                if "images" in node and node["images"]:
                    img = node["images"][0]
                    content = ComfyUIImageVideoPlugin.fetch_asset(
                        base_http,
                        img["filename"],
                        img.get("subfolder", ""),
                        img.get("type", "output"),
                    )
                    ext = os.path.splitext(img["filename"])[-1].lstrip(".") or "webp"
                    return content, ext

            for node in outputs.values():
                if "videos" in node and node["videos"]:
                    vid = node["videos"][0]
                    content = ComfyUIImageVideoPlugin.fetch_asset(
                        base_http,
                        vid["filename"],
                        vid.get("subfolder", ""),
                        vid.get("type", "output"),
                    )
                    ext = os.path.splitext(vid["filename"])[-1].lstrip(".") or "mp4"
                    return content, ext

            time.sleep(0.5)

        # fallback: guess SaveVideo prefix on disk
        for node in wf.values():
            if node.get("class_type") == "SaveVideo":
                prefix = node["inputs"].get("filename_prefix", "ComfyUI")
                if "/" in prefix:
                    subfolder, base = prefix.split("/", 1)
                else:
                    subfolder, base = "", prefix
                guessed_filename = f"{base}.mp4"
                try:
                    content = ComfyUIImageVideoPlugin.fetch_asset(base_http, guessed_filename, subfolder, "output")
                    return content, "mp4"
                except Exception:
                    pass

        raise RuntimeError("No output found in ComfyUI history or disk.")

    # ---------------------------
    # Orchestration
    # ---------------------------
    async def _generate(self, args, llm_client):
        args = args or {}
        prompt = str(args.get("prompt") or "").strip()
        if not prompt:
            prompt = "subtle natural motion, smooth cinematic movement"
        image_bytes, filename = get_latest_image_from_history("webui:chat_history")

        if not image_bytes:
            return ["❌ No image found. Please upload one or generate one using an image plugin first."]

        try:
            animated_bytes, ext = await asyncio.to_thread(
                self.process_prompt, prompt, image_bytes, filename
            )

            if ext == "webp":
                tmp_webp = f"/tmp/{uuid.uuid4().hex}.webp"
                tmp_mp4 = f"/tmp/{uuid.uuid4().hex}.mp4"
                try:
                    with open(tmp_webp, "wb") as f:
                        f.write(animated_bytes)

                    settings = redis_client.hgetall(f"plugin_settings:{self.settings_category}") or {}
                    raw_len = settings.get("LENGTH", b"10")
                    try:
                        duration = int(raw_len.decode() if isinstance(raw_len, (bytes, bytearray)) else raw_len)
                    except Exception:
                        duration = 10

                    wf = ComfyUIImageVideoPlugin.get_workflow_template()
                    fps = ComfyUIImageVideoPlugin.get_fps_from_workflow(wf, default=16)

                    self.webp_to_mp4(tmp_webp, tmp_mp4, fps=fps, duration=duration)

                    with open(tmp_mp4, "rb") as f:
                        animated_bytes = f.read()
                    ext = "mp4"
                    mime = "video/mp4"
                    file_name = "animated.mp4"
                finally:
                    for p in (tmp_webp, tmp_mp4):
                        try:
                            if os.path.exists(p):
                                os.remove(p)
                        except Exception:
                            pass

                if llm_client is not None:
                    try:
                        msg = await llm_client.chat([
                            {"role": "system", "content": f'The user has just been shown an animated video based on the prompt: "{prompt}".'},
                            {"role": "user", "content": "Reply with a short, fun message celebrating the animation. No lead-in phrases or instructions."}
                        ])
                        followup_text = (msg.get("message", {}) or {}).get("content", "").strip() or "Here's your animated video!"
                    except Exception:
                        followup_text = "Here's your animated video!"
                else:
                    followup_text = "Here's your animated video!"
            else:
                mime = "video/mp4" if ext == "mp4" else "image/webp"
                file_name = f"animated.{ext}"
                followup_text = "Here's your animated video!" if ext == "mp4" else "Here's your animated image!"

            media_type = "video" if ext == "mp4" else "image"
            return [
                _build_media_metadata(
                    animated_bytes,
                    media_type=media_type,
                    name=file_name,
                    mimetype=mime,
                ),
                followup_text
            ]
        except Exception as e:
            logger.exception("ComfyUI image-video generation failed: %s", e)
            return [f"❌ Failed to generate animation: {e}"]

    # --- Discord Handler ---
    async def handle_discord(self, message, args, llm_client):
        return "❌ This plugin is only available in the WebUI due to file size limitations."

    # --- WebUI Handler ---
    async def handle_webui(self, args, llm_client):
        return await self._generate(args or {}, llm_client)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return f"{user}: This plugin is only available in the WebUI."

plugin = ComfyUIImageVideoPlugin()
