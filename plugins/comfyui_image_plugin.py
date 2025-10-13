# plugins/comfyui_image_plugin.py
import os
import json
import asyncio
import base64
import secrets
import copy
import requests
from plugin_base import ToolPlugin
import discord
from helpers import redis_client, run_comfy_prompt

class ComfyUIImagePlugin(ToolPlugin):
    name = "comfyui_image_plugin"
    usage = (
        "{\n"
        '  "function": "comfyui_image_plugin",\n'
        '  "arguments": {"prompt": "<Text prompt for the image>"}\n'
        "}\n"
    )
    description = "Draws a picture using a prompt provided by the user using ComfyUI."
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
    platforms = ["discord", "webui"]

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
    # Core generation (sync)
    # ---------------------------
    @staticmethod
    def process_prompt(user_prompt: str, width: int = None, height: int = None) -> bytes:
        base_http = ComfyUIImagePlugin.get_base_http()
        base_ws   = ComfyUIImagePlugin.get_base_ws(base_http)

        # Load template and clone per job
        workflow = copy.deepcopy(ComfyUIImagePlugin.get_workflow_template())

        # Inject prompts
        ComfyUIImagePlugin._insert_prompts(workflow, user_prompt)

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

        w = width or default_w
        h = height or default_h

        for node in workflow.values():
            if isinstance(node, dict) and node.get("class_type") in ("EmptyLatentImage", "EmptySD3LatentImage", "ModelSamplingFlux"):
                node.setdefault("inputs", {})
                node["inputs"]["width"] = w
                node["inputs"]["height"] = h

        # Run Comfy with per-job client_id & WS
        prompt_id, _ = run_comfy_prompt(base_http, base_ws, workflow)

        # Fetch first produced image from history
        history = ComfyUIImagePlugin.get_history(base_http, prompt_id).get(prompt_id, {})
        outputs = history.get("outputs", {}) if isinstance(history, dict) else {}

        for node_id, node_output in outputs.items():
            if "images" in node_output:
                for img_meta in node_output["images"]:
                    filename = img_meta.get("filename")
                    subfolder = img_meta.get("subfolder", "")
                    folder_type = img_meta.get("type", "output")
                    if filename:
                        return ComfyUIImagePlugin.get_image_bytes(base_http, filename, subfolder, folder_type)

        raise Exception("No images returned from ComfyUI.")

    # ---------------------------------------
    # Discord
    # ---------------------------------------
    async def handle_discord(self, message, args, llm_client):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI."
        try:
            async with message.channel.typing():
                image_bytes = await asyncio.to_thread(ComfyUIImagePlugin.process_prompt, user_prompt)

                safe_prompt = user_prompt[:300].strip()
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
                        "name": "generated_comfyui.png",
                        "data": base64.b64encode(image_bytes).decode("utf-8"),
                        "mimetype": "image/png"
                    },
                    message_text
                ]
        except Exception as e:
            return f"Failed to queue prompt: {e}"

    # ---------------------------------------
    # WebUI
    # ---------------------------------------
    async def handle_webui(self, args, llm_client):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI."
        try:
            image_bytes = await asyncio.to_thread(ComfyUIImagePlugin.process_prompt, user_prompt)

            image_data = {
                "type": "image",
                "name": "generated_comfyui.png",
                "data": base64.b64encode(image_bytes).decode("utf-8"),
                "mimetype": "image/png"
            }

            safe_prompt = user_prompt[:300].strip()
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
            return f"Failed to queue prompt: {e}"

plugin = ComfyUIImagePlugin()