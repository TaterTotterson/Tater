# plugins/comfyui_image_video_plugin.py
import os
import json
import uuid
import requests
import asyncio
import websocket
from io import BytesIO
from plugin_base import ToolPlugin
import discord
import base64
from helpers import redis_client, get_latest_image_from_history

client_id = str(uuid.uuid4())

class ComfyUIImageVideoPlugin(ToolPlugin):
    name = "comfyui_image_video"
    usage = (
        '{\n'
        '  "function": "comfyui_image_video",\n'
        '  "arguments": {\n'
        '     "prompt": "<Text prompt for the animation>"\n'
        '  }\n'
        '}'
    )
    description = "Animates the most recent image in chat into a looping WebP using ComfyUI."
    settings_category = "ComfyUI Animate Image"
    required_settings = {
        "COMFYUI_VIDEO_URL": {
            "label": "ComfyUI Server URL",
            "type": "text",
            "default": "http://localhost:8188",
            "description": "Base URL (host:port) for your ComfyUI instance."
        },
        "COMFYUI_VIDEO_WORKFLOW": {
            "label": "WanImageToVideo Workflow (JSON)",
            "type": "file",
            "default": "",
            "description": "Upload your ComfyUI JSON workflow template for WanImageToVideo."
        }
    }
    waiting_prompt_template = (
        "Generate a message telling the user to please wait while you bring your image to life! "
        "Only generate the message. Do not respond to this message."
    )
    platforms = ["discord"]

    @staticmethod
    def get_server_address():
        settings = redis_client.hgetall(
            f"plugin_settings:{ComfyUIImageVideoPlugin.settings_category}"
        )
        url = settings.get("COMFYUI_VIDEO_URL", "").strip() or "localhost:8188"
        return url.replace("http://", "").replace("https://", "")

    @staticmethod
    def upload_image(image_bytes: bytes, filename: str) -> str:
        server = ComfyUIImageVideoPlugin.get_server_address()
        data = {"overwrite": "true"}
        resp = requests.post(
            f"http://{server}/upload/image",
            files={"image": (filename, image_bytes)},
            data=data
        )
        resp.raise_for_status()
        result = resp.json()
        name = result.get("name") or result.get("filename")
        sub  = result.get("subfolder", "")
        return f"{sub}/{name}" if sub else name

    @staticmethod
    def get_workflow_template() -> dict:
        settings_key = f"plugin_settings:{ComfyUIImageVideoPlugin.settings_category}"
        settings = redis_client.hgetall(settings_key)
        raw = settings.get("COMFYUI_VIDEO_WORKFLOW")
        if raw:
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                raise RuntimeError("Uploaded workflow is not valid JSON.")
        raise RuntimeError("No workflow found in Redis. Please upload a valid JSON workflow.")


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
    def get_image(filename, subfolder, folder_type) -> bytes:
        server = ComfyUIImageVideoPlugin.get_server_address()
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url = f"http://{server}/view?{requests.compat.urlencode(params)}"
        return requests.get(url).content

    @staticmethod
    def collect_outputs(ws, workflow: dict) -> bytes:
        info = ComfyUIImageVideoPlugin.queue_workflow(workflow)
        pid = info["prompt_id"]
        while True:
            frame = ws.recv()
            if isinstance(frame, str):
                msg = json.loads(frame)
                if (
                    msg.get("type") == "executing" and
                    msg["data"].get("prompt_id") == pid and
                    msg["data"].get("node") is None
                ):
                    break
        server = ComfyUIImageVideoPlugin.get_server_address()
        hist = requests.get(f"http://{server}/history/{pid}").json()[pid]
        for node in hist["outputs"].values():
            if "images" in node:
                img = node["images"][0]
                return ComfyUIImageVideoPlugin.get_image(
                    img["filename"], img["subfolder"], img["type"]
                )
        raise RuntimeError("No output images returned by workflow.")

    @staticmethod
    def process_prompt(prompt: str, image_bytes: bytes, filename: str) -> bytes:
        uploaded = ComfyUIImageVideoPlugin.upload_image(image_bytes, filename)
        wf = ComfyUIImageVideoPlugin.get_workflow_template()
        for node in wf.values():
            if node.get("class_type") == "LoadImage":
                node["inputs"]["image"] = uploaded
            if (
                node.get("class_type") == "CLIPTextEncode" and
                "Positive" in node.get("_meta", {}).get("title", "")
            ):
                node["inputs"]["text"] = prompt
        server = ComfyUIImageVideoPlugin.get_server_address()
        ws = websocket.WebSocket()
        ws.connect(f"ws://{server}/ws?clientId={client_id}")
        try:
            return ComfyUIImageVideoPlugin.collect_outputs(ws, wf)
        finally:
            ws.close()

    # --- Discord Handler ---
    async def handle_discord(self, message, args, ollama_client):
        prompt = args.get("prompt", "").strip() or "A gentle animation of the provided image."
        channel_key = f"tater:channel:{message.channel.id}:history"
        image_bytes, filename = get_latest_image_from_history(channel_key)

        if not image_bytes:
            fallback_prompt = (
                f"Generate a message telling {message.author.mention} that no image was found. "
                "Mention they can attach an image or use an image plugin. Only generate the message."
            )

            response = await ollama_client.chat(
                messages=[
                    {"role": "system", "content": "You're helping users animate images."},
                    {"role": "user", "content": fallback_prompt}
                ]
            )

            fallback_msg = response["message"].get("content", "").strip() or (
                f"{message.author.mention}, I couldn’t find an image. Try uploading one or use an image plugin first."
            )
            return fallback_msg

        try:
            animated = await asyncio.to_thread(self.process_prompt, prompt, image_bytes, filename)
            safe_prompt = prompt[:300].strip()
            system_msg = f'The user has just been shown an animated image based on the prompt: "{safe_prompt}".'

            followup = await ollama_client.chat(
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Give them a short, fun message celebrating the animation. Do not include any lead-in phrases or instructions — just the message"}
                ]
            )

            followup_text = followup["message"].get("content", "").strip() or "Here's your animated image!"

            return [
                {
                    "type": "image",
                    "name": "animated.webp",
                    "data": base64.b64encode(animated).decode("utf-8"),
                    "mimetype": "image/webp"
                },
                followup_text
            ]

        except Exception as e:
            return f"❌ Failed to generate animation: {e}"
            
    # --- WebUI Handler ---
    async def handle_webui(self, args, ollama_client):
        prompt = args.get("prompt", "").strip() or "A gentle animation of the provided image."
        image_bytes, filename = get_latest_image_from_history("webui:chat_history")

        if not image_bytes:
            return "❌ No image found. Please upload one or generate one using an image plugin first."

        try:
            animated = await asyncio.to_thread(self.process_prompt, prompt, image_bytes, filename)
            safe_prompt = prompt[:300].strip()
            system_msg = f'The user has just been shown an animated image based on the prompt: "{safe_prompt}".'

            followup = await ollama_client.chat(
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Give them a short, fun message celebrating the animation. Do not include any lead-in phrases or instructions — just the message"}
                ]
            )

            followup_text = followup["message"].get("content", "").strip() or "Here's your animated image!"

            return [
                {
                    "type": "image",
                    "name": "animated.webp",
                    "data": base64.b64encode(animated).decode("utf-8"),
                    "mimetype": "image/webp"
                },
                followup_text
            ]

        except Exception as e:
            return f"❌ Failed to generate animation: {e}"

    # --- IRC Handler ---
    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        await bot.privmsg(channel, f"{user}: ❌ This plugin only works in Discord.")

plugin = ComfyUIImageVideoPlugin()