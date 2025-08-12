# plugins/comfyui_image_video_plugin.py
import os
import json
import uuid
import requests
import asyncio
import websocket
import base64
from PIL import Image
from moviepy.video.io.ImageSequenceClip import ImageSequenceClip
from plugin_base import ToolPlugin
from helpers import redis_client, get_latest_image_from_history

client_id = str(uuid.uuid4())

class ComfyUIImageVideoPlugin(ToolPlugin):
    name = "comfyui_image_video"
    usage = (
        '{\n'
        '  "function": "comfyui_image_video",\n'
        '  "arguments": {"prompt": "<Describe how you want the animation to move or behave>"}\n'
        '}'
    )
    description = "Animates the most recent image in chat into a looping WebP or MP4 using ComfyUI."
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

    @staticmethod
    def get_fps_from_workflow(wf: dict, default: int = 16) -> int:
        try:
            for node in wf.values():
                if node.get("class_type") == "CreateVideo":
                    return int(node["inputs"].get("fps", default))
        except Exception:
            pass
        return default

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

    @staticmethod
    def get_server_address():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImageVideoPlugin.settings_category}")
        url_raw = settings.get("COMFYUI_VIDEO_URL", b"")
        url = url_raw.decode("utf-8").strip() if isinstance(url_raw, bytes) else url_raw.strip()
        if url.startswith("http://"):
            url = url[len("http://"):]
        elif url.startswith("https://"):
            url = url[len("https://"):]
        return url or "localhost:8188"

    @staticmethod
    def get_workflow_template():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImageVideoPlugin.settings_category}")
        raw = settings.get("COMFYUI_VIDEO_WORKFLOW", b"")
        workflow_str = raw.decode("utf-8").strip() if isinstance(raw, bytes) else raw.strip()
        if not workflow_str:
            raise RuntimeError("No workflow found in Redis. Please upload a valid JSON workflow.")
        return json.loads(workflow_str)

    @staticmethod
    def upload_image(image_bytes: bytes, filename: str) -> str:
        server = ComfyUIImageVideoPlugin.get_server_address()
        resp = requests.post(
            f"http://{server}/upload/image",
            files={"image": (filename, image_bytes)},
            data={"overwrite": "true"}
        )
        resp.raise_for_status()
        result = resp.json()
        name = result.get("name") or result.get("filename")
        sub = result.get("subfolder", "")
        return f"{sub}/{name}" if sub else name

    @staticmethod
    def get_image(filename, subfolder, folder_type) -> bytes:
        server = ComfyUIImageVideoPlugin.get_server_address()
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url = f"http://{server}/view?{requests.compat.urlencode(params)}"
        return requests.get(url).content

    @staticmethod
    def queue_workflow(workflow: dict) -> dict:
        server = ComfyUIImageVideoPlugin.get_server_address()
        payload = {"prompt": workflow, "client_id": client_id}
        resp = requests.post(
            f"http://{server}/prompt",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def collect_outputs(ws, workflow: dict):
        info = ComfyUIImageVideoPlugin.queue_workflow(workflow)
        pid = info["prompt_id"]

        # Wait for execution to complete
        while True:
            frame = ws.recv()
            if isinstance(frame, str):
                msg = json.loads(frame)
                if msg.get("type") == "executing" and msg["data"].get("prompt_id") == pid and msg["data"]["node"] is None:
                    break

        server = ComfyUIImageVideoPlugin.get_server_address()
        hist = requests.get(f"http://{server}/history/{pid}").json()[pid]

        # Try normal inline output first
        for node in hist["outputs"].values():
            if "images" in node:
                img = node["images"][0]
                content = ComfyUIImageVideoPlugin.get_image(img["filename"], img["subfolder"], img["type"])
                ext = os.path.splitext(img["filename"])[-1].lstrip(".") or "webp"
                return content, ext

            if "videos" in node:
                vid = node["videos"][0]
                content = ComfyUIImageVideoPlugin.get_image(vid["filename"], vid["subfolder"], vid["type"])
                ext = os.path.splitext(vid["filename"])[-1].lstrip(".") or "mp4"
                return content, ext

        # Fallback: manually resolve SaveVideo output
        for node in workflow.values():
            if node.get("class_type") == "SaveVideo":
                prefix = node["inputs"].get("filename_prefix", "ComfyUI")
                # Handle subfolder/base name
                if "/" in prefix:
                    subfolder, base = prefix.split("/", 1)
                else:
                    subfolder, base = "", prefix
                guessed_filename = f"{base}.mp4"
                try:
                    content = ComfyUIImageVideoPlugin.get_image(guessed_filename, subfolder, "output")
                    return content, "mp4"
                except Exception as e:
                    raise RuntimeError(f"Could not fetch video file from disk: {e}")

        raise RuntimeError("No output found in ComfyUI history or disk.")

    @staticmethod
    def process_prompt(prompt: str, image_bytes: bytes, filename: str, width: int = None, height: int = None, length: int = None):
        uploaded = ComfyUIImageVideoPlugin.upload_image(image_bytes, filename)
        wf = ComfyUIImageVideoPlugin.get_workflow_template()

        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImageVideoPlugin.settings_category}")
        res_map = {
            "144p": (256, 144),
            "240p": (426, 240),
            "360p": (480, 360),
            "480p": (640, 480),
            "720p": (1280, 720),
            "1080p": (1920, 1080)
        }

        raw_res = settings.get("IMAGE_RESOLUTION", b"480p")
        resolution = raw_res.decode("utf-8") if isinstance(raw_res, bytes) else raw_res
        default_w, default_h = res_map.get(resolution, (640, 480))

        # get fps from the workflow's CreateVideo node (fallback 16)
        fps = ComfyUIImageVideoPlugin.get_fps_from_workflow(wf, default=16)

        raw_length = settings.get("LENGTH", b"1")
        try:
            default_seconds = int(raw_length.decode() if isinstance(raw_length, bytes) else raw_length)
        except ValueError:
            default_seconds = 1

        default_frames = max(1, default_seconds * fps)

        w = width or default_w
        h = height or default_h
        l = length or default_frames

        patched_first_prompt = False
        for node in wf.values():
            if node.get("class_type") == "LoadImage":
                node["inputs"]["image"] = uploaded

            if node.get("class_type") == "CLIPTextEncode" and (
                "Positive" in node.get("_meta", {}).get("title", "") or
                "Prompt" in node.get("_meta", {}).get("title", "")
            ):
                if not patched_first_prompt:
                    node["inputs"]["text"] = prompt
                    patched_first_prompt = True

            # set width/height/length on relevant I2V nodes (includes WAN 2.2)
            if node.get("class_type") in {
                "WanImageToVideo",
                "WanVaceToVideo",
                "CosmosPredict2ImageToVideoLatent",
                "Wan22ImageToVideoLatent",   # NEW for your template
            }:
                node["inputs"]["width"] = w
                node["inputs"]["height"] = h
                node["inputs"]["length"] = l

        ws = websocket.WebSocket()
        server = ComfyUIImageVideoPlugin.get_server_address()
        ws.connect(f"ws://{server}/ws?clientId={client_id}")
        try:
            return ComfyUIImageVideoPlugin.collect_outputs(ws, wf)
        finally:
            ws.close()

    async def _generate(self, args, llm_client):
        prompt = args.get("prompt", "").strip()
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
                with open(tmp_webp, "wb") as f:
                    f.write(animated_bytes)

                settings = redis_client.hgetall(f"plugin_settings:{self.settings_category}")
                raw_len = settings.get("LENGTH", b"10")
                try:
                    duration = int(raw_len.decode() if isinstance(raw_len, bytes) else raw_len)
                except ValueError:
                    duration = 10

                # pull fps from the stored workflow for conversion too
                wf = ComfyUIImageVideoPlugin.get_workflow_template()
                fps = ComfyUIImageVideoPlugin.get_fps_from_workflow(wf, default=16)

                self.webp_to_mp4(tmp_webp, tmp_mp4, fps=fps, duration=duration)

                with open(tmp_mp4, "rb") as f:
                    animated_bytes = f.read()
                ext = "mp4"
                mime = "video/mp4"
                file_name = "animated.mp4"
                msg = await llm_client.chat([
                    {"role": "system", "content": f'The user has just been shown an animated video based on the prompt: "{prompt}".'},
                    {"role": "user", "content": "Reply with a short, fun message celebrating the animation. No lead-in phrases or instructions."}
                ])

                followup_text = msg["message"]["content"].strip() or "🎞️ Here's your animated video!"

                os.remove(tmp_webp)
                os.remove(tmp_mp4)
            else:
                mime = "video/mp4" if ext == "mp4" else "image/webp"
                file_name = f"animated.{ext}"
                followup_text = "Here's your animated video!" if ext == "mp4" else "Here's your animated image!"

            return [
                {
                    "type": "video",
                    "name": file_name,
                    "data": base64.b64encode(animated_bytes).decode("utf-8"),
                    "mimetype": mime
                },
                followup_text
            ]
        except Exception as e:
            return [f"❌ Failed to generate animation: {e}"]

    # --- Discord Handler ---
    async def handle_discord(self, message, args, llm_client):
        return "❌ This plugin is only available in the WebUI due to file size limitations."

    # --- WebUI Handler ---
    async def handle_webui(self, args, llm_client):
        try:
            asyncio.get_running_loop()
            return await self._generate(args, llm_client)
        except RuntimeError:
            return asyncio.run(self._generate(args, llm_client))

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        await bot.privmsg(channel, f"{user}: ❌ This plugin is only available in the WebUI.")

plugin = ComfyUIImageVideoPlugin()