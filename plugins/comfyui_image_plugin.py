# plugins/comfyui_image_plugin.py
import os
import json
import time
import asyncio
import base64
import secrets
import copy
import requests
import imghdr
from io import BytesIO

from plugin_base import ToolPlugin
from helpers import redis_client, run_comfy_prompt


class ComfyUIImagePlugin(ToolPlugin):
    name = "comfyui_image_plugin"
    plugin_name = "ComfyUI Image"
    usage = (
        "{\n"
        '  "function": "comfyui_image_plugin",\n'
        '  "arguments": {\n'
        '    "prompt": "<Text prompt for the image>",\n'
        '    "negative_prompt": "<Optional negative prompt>",\n'
        '    "width": 1024,\n'
        '    "height": 1024\n'
        "  }\n"
        "}\n"
    )
    description = "Draws a picture using a prompt provided by the user using ComfyUI."
    plugin_dec = "Generate a still image from a text prompt using your ComfyUI workflow."
    pretty_name = "Your Image"
    settings_category = "ComfyUI Image"
    required_settings = {
        "COMFYUI_URL": {
            "label": "ComfyUI URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "The base URL for the ComfyUI API (do not include endpoint paths)."
        },
        "IMAGE_RESOLUTION": {
            "label": "Image Resolution",
            "type": "select",
            "default": "720p",
            "options": ["144p", "240p", "360p", "480p", "720p", "1080p"],
            "description": "Resolution for generated images."
        },
        "COMFYUI_WORKFLOW": {
            "label": "Workflow Template (JSON)",
            "type": "file",
            "default": "",
            "description": "Upload your JSON workflow template file. This field is required."
        }
    }
    waiting_prompt_template = "Write a fun, casual message saying you’re creating their masterpiece now! Only output that message."
    platforms = ["discord", "webui", "matrix"]  # matrix supported

    # ---------------------------
    # Server URL helpers
    # ---------------------------
    @staticmethod
    def get_base_http():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImagePlugin.settings_category}")
        raw = settings.get("COMFYUI_URL", b"")
        url = raw.decode("utf-8").strip() if isinstance(raw, (bytes, bytearray)) else (raw or "").strip()
        if not url:
            url = "http://localhost:8188"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url
        return url.rstrip("/")

    @staticmethod
    def get_base_ws(base_http: str) -> str:
        # http://host:8188 -> ws://host:8188 ; https -> wss
        scheme = "wss" if base_http.startswith("https://") else "ws"
        return base_http.replace("http", scheme, 1)

    # ---------------------------
    # Template / I/O helpers
    # ---------------------------
    @staticmethod
    def get_workflow_template():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImagePlugin.settings_category}")
        workflow_raw = settings.get("COMFYUI_WORKFLOW", b"")
        workflow_str = workflow_raw.decode("utf-8").strip() if isinstance(workflow_raw, (bytes, bytearray)) else (workflow_raw or "").strip()
        if not workflow_str:
            raise Exception("No workflow template set in COMFYUI_WORKFLOW. Please provide a valid JSON template.")
        return json.loads(workflow_str)

    @staticmethod
    def get_history(base_http: str, prompt_id: str):
        r = requests.get(f"{base_http}/history/{prompt_id}", timeout=30)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def get_image_bytes(base_http: str, filename: str, subfolder: str, folder_type: str) -> bytes:
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        r = requests.get(f"{base_http}/view", params=params, timeout=60)
        r.raise_for_status()
        return r.content

    # ---------------------------
    # Prompt injection
    # ---------------------------
    @staticmethod
    def _insert_prompts(workflow: dict, user_prompt: str, negative_prompt: str = ""):
        positive_found = False
        negative_found = False
        encode_nodes = []

        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            if node.get("class_type") == "CLIPTextEncode":
                encode_nodes.append(node)
                title = (node.get("_meta", {}).get("title", "") or "").lower()
                if "positive" in title:
                    node.setdefault("inputs", {})
                    node["inputs"]["text"] = user_prompt
                    node["widgets_values"] = [user_prompt]
                    positive_found = True
                elif "negative" in title:
                    node.setdefault("inputs", {})
                    node["inputs"]["text"] = negative_prompt
                    node["widgets_values"] = [negative_prompt]
                    negative_found = True

        if not positive_found and encode_nodes:
            encode_nodes[0].setdefault("inputs", {})
            encode_nodes[0]["inputs"]["text"] = user_prompt
            encode_nodes[0]["widgets_values"] = [user_prompt]
        if not negative_found and len(encode_nodes) > 1:
            encode_nodes[1].setdefault("inputs", {})
            encode_nodes[1]["inputs"]["text"] = negative_prompt
            encode_nodes[1]["widgets_values"] = [negative_prompt]

    # ---------------------------
    # Utils
    # ---------------------------
    @staticmethod
    def _infer_mime_and_ext(data: bytes):
        k = imghdr.what(None, h=data)
        mime = {
            "png": "image/png",
            "jpeg": "image/jpeg",
            "gif": "image/gif",
            "webp": "image/webp",
            "bmp": "image/bmp",
            "tiff": "image/tiff",
        }.get(k, "application/octet-stream")
        ext = {
            "image/png": "png",
            "image/jpeg": "jpg",
            "image/gif": "gif",
            "image/webp": "webp",
            "image/bmp": "bmp",
            "image/tiff": "tiff",
        }.get(mime, "bin")
        return mime, ext

    @staticmethod
    def _fetch_first_image(base_http: str, prompt_id: str, tries: int = 6, delay: float = 0.5) -> bytes:
        """
        ComfyUI sometimes writes history a moment after finishing.
        Poll briefly for the first available image.
        """
        for _ in range(max(1, tries)):
            hist_all = ComfyUIImagePlugin.get_history(base_http, prompt_id)
            history = hist_all.get(prompt_id, {}) if isinstance(hist_all, dict) else {}
            outputs = history.get("outputs", {}) if isinstance(history, dict) else {}

            for node_output in outputs.values():
                images = node_output.get("images") or []
                for img_meta in images:
                    filename = img_meta.get("filename")
                    if filename:
                        subfolder = img_meta.get("subfolder", "")
                        folder_type = img_meta.get("type", "output")
                        return ComfyUIImagePlugin.get_image_bytes(base_http, filename, subfolder, folder_type)

            time.sleep(delay)

        raise Exception("No images returned from ComfyUI (timeout).")

    # ---------------------------
    # Core generation (sync)
    # ---------------------------
    @staticmethod
    def process_prompt(user_prompt: str, negative_prompt: str = "", width: int = None, height: int = None) -> bytes:
        base_http = ComfyUIImagePlugin.get_base_http()
        base_ws   = ComfyUIImagePlugin.get_base_ws(base_http)

        # Load template and clone per job
        workflow = copy.deepcopy(ComfyUIImagePlugin.get_workflow_template())

        # Inject prompts
        ComfyUIImagePlugin._insert_prompts(workflow, user_prompt, negative_prompt)

        # Randomize seed every run
        random_seed = secrets.randbits(63)
        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            inputs = node.get("inputs", {})
            if not isinstance(inputs, dict):
                continue
            if "seed" in inputs:
                inputs["seed"] = int(random_seed)
            if "noise_seed" in inputs:
                inputs["noise_seed"] = int(random_seed)

        # Resolution override from settings (unless explicit width/height provided)
        res_map = {
            "144p": (256, 144),
            "240p": (426, 240),
            "360p": (480, 360),
            "480p": (640, 480),
            "720p": (1280, 720),
            "1080p": (1920, 1080),
        }
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImagePlugin.settings_category}")
        raw_res = settings.get("IMAGE_RESOLUTION", b"720p")
        resolution = raw_res.decode("utf-8") if isinstance(raw_res, (bytes, bytearray)) else (raw_res or "720p")
        default_w, default_h = res_map.get(resolution, (1280, 720))

        w = int(width) if width else default_w
        h = int(height) if height else default_h

        for node in workflow.values():
            if isinstance(node, dict) and node.get("class_type") in ("EmptyLatentImage", "EmptySD3LatentImage", "ModelSamplingFlux"):
                node.setdefault("inputs", {})
                node["inputs"]["width"] = w
                node["inputs"]["height"] = h

        # Run Comfy with per-job client_id & WS
        prompt_id, _ = run_comfy_prompt(base_http, base_ws, workflow)

        # Robustly fetch the first image
        return ComfyUIImagePlugin._fetch_first_image(base_http, prompt_id)

    # ---------------------------------------
    # Discord
    # ---------------------------------------
    async def handle_discord(self, message, args, llm_client):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI."
        try:
            neg = args.get("negative_prompt", "") or ""
            w = args.get("width"); h = args.get("height")

            async with message.channel.typing():
                image_bytes = await asyncio.to_thread(
                    ComfyUIImagePlugin.process_prompt, user_prompt, neg, w, h
                )

                mime, ext = self._infer_mime_and_ext(image_bytes)

                # Keep only printable slice of the prompt
                safe_prompt = "".join(ch for ch in user_prompt[:300] if ch.isprintable()).strip()
                system_msg = f'The user has just been shown an AI-generated image based on the prompt: "{safe_prompt}".'
                final_response = await llm_client.chat(
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": "Respond with a short, fun message celebrating the image. Do not include any lead-in phrases or instructions — just the message."}
                    ]
                )

                message_text = final_response["message"].get("content", "").strip() or "Here's your generated image!"
                return [
                    {
                        "type": "image",
                        "name": f"generated_comfyui.{ext}",
                        "data": base64.b64encode(image_bytes).decode("utf-8"),
                        "mimetype": mime
                    },
                    message_text
                ]
        except Exception as e:
            return f"Failed to queue prompt: {type(e).__name__}: {e}"

    # ---------------------------------------
    # WebUI
    # ---------------------------------------
    async def handle_webui(self, args, llm_client):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI."
        try:
            neg = args.get("negative_prompt", "") or ""
            w = args.get("width"); h = args.get("height")

            image_bytes = await asyncio.to_thread(
                ComfyUIImagePlugin.process_prompt, user_prompt, neg, w, h
            )
            mime, ext = self._infer_mime_and_ext(image_bytes)

            image_data = {
                "type": "image",
                "name": f"generated_comfyui.{ext}",
                "data": base64.b64encode(image_bytes).decode("utf-8"),
                "mimetype": mime
            }

            safe_prompt = "".join(ch for ch in user_prompt[:300] if ch.isprintable()).strip()
            system_msg = f'The user has just been shown an AI-generated image based on the prompt: "{safe_prompt}".'
            final_response = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Respond with a short, fun message celebrating the image. Do not include any lead-in phrases or instructions — just the message."}
                ]
            )
            message_text = final_response["message"].get("content", "").strip() or "Here's your generated image!"
            return [image_data, message_text]
        except Exception as e:
            return f"Failed to queue prompt: {type(e).__name__}: {e}"

    # ---------------------------------------
    # Matrix
    # ---------------------------------------
    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        """
        Return an image payload (base64) plus a short message.
        The Matrix platform will upload/send the media and persist history.
        """
        user_prompt = (args or {}).get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI."

        try:
            neg = (args or {}).get("negative_prompt", "") or ""
            w = (args or {}).get("width")
            h = (args or {}).get("height")

            # Generate the image via your ComfyUI helper
            image_bytes = await asyncio.to_thread(
                ComfyUIImagePlugin.process_prompt, user_prompt, neg, w, h
            )
            mime, ext = self._infer_mime_and_ext(image_bytes)

            image_payload = {
                "type": "image",
                "name": f"generated_comfyui.{ext}",
                "data": base64.b64encode(image_bytes).decode("utf-8"),
                "mimetype": mime,
            }

            # Optional short celebratory text via the LLM
            message_text = "Here’s your generated image!"
            if llm_client is not None:
                safe_prompt = "".join(ch for ch in user_prompt[:300] if ch.isprintable()).strip()
                system_msg = (
                    f'The user has just been shown an AI-generated image based on the prompt: "{safe_prompt}".'
                )
                final_response = await llm_client.chat(
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": "Respond with a short, fun message celebrating the image. Do not include any lead-in phrases or instructions — just the message."}
                    ]
                )
                message_text = (final_response["message"].get("content", "") or "").strip() or message_text

            return [image_payload, message_text]

        except Exception as e:
            return f"Failed to create image: {type(e).__name__}: {e}"


plugin = ComfyUIImagePlugin()
