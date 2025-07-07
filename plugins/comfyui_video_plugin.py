# plugins/comfyui_video_plugin.py
import os
import json
import uuid
import urllib.request
import urllib.parse
import asyncio
import time
import websocket
from io import BytesIO
from plugin_base import ToolPlugin
import discord
import streamlit as st
from helpers import redis_client, load_image_from_url, send_waiting_message
import base64

client_id = str(uuid.uuid4())

class ComfyUIVideoPlugin(ToolPlugin):
    name = "comfyui_video_plugin"
    usage = (
        "{\n"
        '  "function": "comfyui_video_plugin",\n'
        '  "arguments": {"prompt": "<Text prompt for the video>"}\n'
        "}\n"
    )
    description = "Generates a video using ComfyUI."
    settings_category = "ComfyUI Video"
    required_settings = {
        "COMFYUI_VIDEO_URL": {
            "label": "ComfyUI Video URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "The base URL for the ComfyUI Video API (do not include endpoint paths)."
        },
        "COMFYUI_VIDEO_WORKFLOW": {
            "label": "Workflow Template (JSON)",
            "type": "file",
            "default": "",
            "description": "Upload your JSON workflow template file for video generation. This field is required."
        }
    }
    waiting_prompt_template = "Generate a message telling the user to please wait while you assemble a film crew and direct your cinematic masterpiece!, Only generate the message. Do not respond to this message."
    platforms = ["discord", "webui"]
    assistant_avatar = load_image_from_url()

    @staticmethod
    def insert_prompt_into_workflow(workflow: dict, prompt_text: str) -> dict:
        """
        Replaces the text in the first CLIPTextEncode node with user prompt.
        """
        for node_id, node in workflow.items():
            if node.get("class_type") == "CLIPTextEncode":
                if "inputs" in node and "text" in node["inputs"]:
                    node["inputs"]["text"] = prompt_text
                if "widgets_values" in node:
                    node["widgets_values"] = [prompt_text]
                break  # Stop after first match
        return workflow

    @staticmethod
    def get_server_address():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIVideoPlugin.settings_category}")
        url = settings.get("COMFYUI_VIDEO_URL", "").strip()
        if not url:
            return "localhost:8188"
        if url.startswith("http://"):
            return url[len("http://"):]
        elif url.startswith("https://"):
            return url[len("https://"):]
        else:
            return url

    @staticmethod
    def queue_prompt(prompt):
        server_address = ComfyUIVideoPlugin.get_server_address()
        p = {"prompt": prompt, "client_id": client_id}
        data = json.dumps(p).encode("utf-8")
        req = urllib.request.Request("http://{}/prompt".format(server_address),
                                     data=data,
                                     headers={"Content-Type": "application/json"})
        return json.loads(urllib.request.urlopen(req).read())

    @staticmethod
    def get_image(filename, subfolder, folder_type):
        # Although this is a video plugin, ComfyUI outputs an animated WebP,
        # which we can retrieve using the same method as for images.
        server_address = ComfyUIVideoPlugin.get_server_address()
        data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url_values = urllib.parse.urlencode(data)
        with urllib.request.urlopen("http://{}/view?{}".format(server_address, url_values)) as response:
            return response.read()

    @staticmethod
    def get_history(prompt_id):
        server_address = ComfyUIVideoPlugin.get_server_address()
        with urllib.request.urlopen("http://{}/history/{}".format(server_address, prompt_id)) as response:
            return json.loads(response.read())

    @staticmethod
    def get_images(ws, prompt):
        prompt_id = ComfyUIVideoPlugin.queue_prompt(prompt)["prompt_id"]
        output_images = {}
        while True:
            out = ws.recv()
            if isinstance(out, str):
                message = json.loads(out)
                if message["type"] == "executing":
                    data = message["data"]
                    if data["node"] is None and data["prompt_id"] == prompt_id:
                        break  # Execution is done
            else:
                continue  # skip binary data
        history = ComfyUIVideoPlugin.get_history(prompt_id)[prompt_id]
        for node_id in history["outputs"]:
            node_output = history["outputs"][node_id]
            images_output = []
            if "images" in node_output:
                for image in node_output["images"]:
                    image_data = ComfyUIVideoPlugin.get_image(image["filename"], image["subfolder"], image["type"])
                    images_output.append(image_data)
            output_images[node_id] = images_output
        return output_images

    @staticmethod
    def get_workflow_template():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIVideoPlugin.settings_category}")
        workflow_str = settings.get("COMFYUI_VIDEO_WORKFLOW", "").strip()
        if not workflow_str:
            raise Exception("No workflow template set in COMFYUI_VIDEO_WORKFLOW. Please provide a valid JSON template.")
        return json.loads(workflow_str)

    @staticmethod
    def process_prompt(user_prompt: str) -> bytes:
        workflow = ComfyUIVideoPlugin.get_workflow_template()
        workflow = ComfyUIVideoPlugin.insert_prompt_into_workflow(workflow, user_prompt)

        ws = websocket.WebSocket()
        server_address = ComfyUIVideoPlugin.get_server_address()
        ws.connect(f"ws://{server_address}/ws?clientId={client_id}")

        try:
            images = ComfyUIVideoPlugin.get_images(ws, workflow)
        finally:
            ws.close()

        # Return the first animated WebP found
        for node_id, imgs in images.items():
            if imgs:
                return imgs[0]

        raise Exception("No video/image returned from ComfyUI.")

    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI Video."

        try:
            video_bytes = await asyncio.to_thread(ComfyUIVideoPlugin.process_prompt, user_prompt)

            video_data = {
                "type": "image",
                "name": "generated_video.webp",
                "data": base64.b64encode(video_bytes).decode("utf-8"),
                "mimetype": "image/webp"
            }

            safe_prompt = user_prompt[:300].strip()
            system_msg = f'The user has just been shown an animated video based on the prompt: "{safe_prompt}".'

            followup = await ollama_client.chat(
                model=ollama_client.model,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Respond with a short, fun message celebrating the video. Do not include any lead-in phrases or instructions — just the message."}
                ],
                stream=False,
                keep_alive=ollama_client.keep_alive,
                options={"num_ctx": context_length}
            )

            followup_text = followup["message"].get("content", "").strip() or "🎬 Here's your animated video!"

            return [video_data, followup_text]

        except Exception as e:
            return f"Failed to queue prompt: {e}"

    # --- WebUI Handler ---
    async def handle_webui(self, args, ollama_client, context_length):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided."

        try:
            image_bytes = await asyncio.to_thread(ComfyUIVideoPlugin.process_prompt, user_prompt)

            image_data = {
                "type": "image",
                "name": "generated_video.webp",
                "data": base64.b64encode(image_bytes).decode("utf-8"),
                "mimetype": "image/webp"
            }

            safe_prompt = user_prompt[:300].strip()
            system_msg = f'The user has just been shown a looping animated video based on the prompt: "{safe_prompt}".'

            final_response = await ollama_client.chat(
                model=ollama_client.model,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Give them a short, fun message celebrating the video. Do not include any instructions or lead-in phrases — just the message."}
                ],
                stream=False,
                keep_alive=ollama_client.keep_alive,
                options={"num_ctx": context_length}
            )

            message_text = final_response["message"].get("content", "").strip() or "🎬 Here's your animated video!"
            return [image_data, message_text]

        except Exception as e:
            error_msg = f"Failed to queue prompt: {e}"
            return error_msg

    # --- IRC Handler ---
    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        await bot.privmsg(channel, f"{user}: This plugin is only supported on Discord and WebUI.")

plugin = ComfyUIVideoPlugin()