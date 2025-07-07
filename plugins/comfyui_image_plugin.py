# plugins/comfyui_image_plugin.py
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
from helpers import redis_client, load_image_from_url, format_irc
import base64

client_id = str(uuid.uuid4())

class ComfyUIImagePlugin(ToolPlugin):
    name = "comfyui_image_plugin"
    usage = (
        "{\n"
        '  "function": "comfyui_image_plugin",\n'
        '  "arguments": {"prompt": "<Text prompt for the image>"}\n'
        "}\n"
    )
    description = "Generates an image using ComfyUI."
    settings_category = "ComfyUI Image"
    required_settings = {
        "COMFYUI_URL": {
            "label": "ComfyUI URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "The base URL for the ComfyUI API (do not include endpoint paths)."
        },
        "COMFYUI_WORKFLOW": {
            "label": "Workflow Template (JSON)",
            "type": "file",  # Using file upload in webui; stored as JSON string in Redis.
            "default": "",
            "description": "Upload your JSON workflow template file. This field is required."
        }
    }
    waiting_prompt_template = "Generate a message telling the user to please wait a moment while you create them a Masterpiece! Only generate the message. Do not respond to this message."
    platforms = ["discord", "webui"]
    assistant_avatar = load_image_from_url()

    @staticmethod
    def get_server_address():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImagePlugin.settings_category}")
        url = settings.get("COMFYUI_URL", "").strip()
        if not url:
            return "localhost:8188"
        # Remove scheme if present
        if url.startswith("http://"):
            return url[len("http://"):]
        elif url.startswith("https://"):
            return url[len("https://"):]
        else:
            return url

    @staticmethod
    def queue_prompt(prompt):
        server_address = ComfyUIImagePlugin.get_server_address()
        p = {"prompt": prompt, "client_id": client_id}
        data = json.dumps(p).encode("utf-8")
        req = urllib.request.Request("http://{}/prompt".format(server_address),
                                     data=data,
                                     headers={"Content-Type": "application/json"})
        return json.loads(urllib.request.urlopen(req).read())

    @staticmethod
    def get_image(filename, subfolder, folder_type):
        server_address = ComfyUIImagePlugin.get_server_address()
        data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url_values = urllib.parse.urlencode(data)
        with urllib.request.urlopen("http://{}/view?{}".format(server_address, url_values)) as response:
            return response.read()

    @staticmethod
    def get_history(prompt_id):
        server_address = ComfyUIImagePlugin.get_server_address()
        with urllib.request.urlopen("http://{}/history/{}".format(server_address, prompt_id)) as response:
            return json.loads(response.read())

    @staticmethod
    def get_images(ws, prompt):
        prompt_id = ComfyUIImagePlugin.queue_prompt(prompt)["prompt_id"]
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
        history = ComfyUIImagePlugin.get_history(prompt_id)[prompt_id]
        for node_id in history["outputs"]:
            node_output = history["outputs"][node_id]
            images_output = []
            if "images" in node_output:
                for image in node_output["images"]:
                    image_data = ComfyUIImagePlugin.get_image(image["filename"], image["subfolder"], image["type"])
                    images_output.append(image_data)
            output_images[node_id] = images_output
        return output_images

    @staticmethod
    def get_workflow_template():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImagePlugin.settings_category}")
        workflow_str = settings.get("COMFYUI_WORKFLOW", "").strip()
        if not workflow_str:
            raise Exception("No workflow template set in COMFYUI_WORKFLOW. Please provide a valid JSON template.")
        return json.loads(workflow_str)

    @staticmethod
    def process_prompt(user_prompt: str) -> bytes:
        def insert_prompts(workflow, user_prompt, negative_prompt=""):
            positive_found = False
            negative_found = False
            encode_nodes = []

            for node_id, node in workflow.items():
                if node.get("class_type") == "CLIPTextEncode":
                    encode_nodes.append(node)
                    title = node.get("_meta", {}).get("title", "").lower()
                    if "positive" in title:
                        node["inputs"]["text"] = user_prompt
                        node["widgets_values"] = [user_prompt]
                        positive_found = True
                    elif "negative" in title:
                        node["inputs"]["text"] = negative_prompt
                        node["widgets_values"] = [negative_prompt]
                        negative_found = True

            # Fallback if titles aren't labeled
            if not positive_found and len(encode_nodes) > 0:
                encode_nodes[0]["inputs"]["text"] = user_prompt
                encode_nodes[0]["widgets_values"] = [user_prompt]
            if not negative_found and len(encode_nodes) > 1:
                encode_nodes[1]["inputs"]["text"] = negative_prompt
                encode_nodes[1]["widgets_values"] = [negative_prompt]

        # Load workflow
        workflow = ComfyUIImagePlugin.get_workflow_template()
        insert_prompts(workflow, user_prompt)

        # Connect to WebSocket
        ws = websocket.WebSocket()
        server_address = ComfyUIImagePlugin.get_server_address()
        ws.connect(f"ws://{server_address}/ws?clientId={client_id}")
        images = ComfyUIImagePlugin.get_images(ws, workflow)
        ws.close()

        # Return the first image found
        for node_id, imgs in images.items():
            if imgs:
                return imgs[0]

        raise Exception("No images returned from ComfyUI.")

    # ---------------------------------------------------------
    # Discord handler
    # ---------------------------------------------------------
    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI."

        try:
            async with message.channel.typing():
                image_bytes = await asyncio.to_thread(ComfyUIImagePlugin.process_prompt, user_prompt)

                safe_prompt = user_prompt[:300].strip()
                system_msg = f'The user has just been shown an AI-generated image based on the prompt: "{safe_prompt}".'
                final_response = await ollama_client.chat(
                    model=ollama_client.model,
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": "Respond with a short, fun message celebrating the image. Do not include any lead-in phrases or instructions — just the message."}
                    ],
                    stream=False,
                    keep_alive=ollama_client.keep_alive,
                    options={"num_ctx": context_length}
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

    # ---------------------------------------------------------
    # WebUI handler
    # ---------------------------------------------------------
    async def handle_webui(self, args, ollama_client, context_length):
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
            final_response = await ollama_client.chat(
                model=ollama_client.model,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Respond with a short, fun message celebrating the image. Do not include any lead-in phrases or instructions — just the message."}
                ],
                stream=False,
                keep_alive=ollama_client.keep_alive,
                options={"num_ctx": context_length}
            )

            message_text = final_response["message"].get("content", "").strip() or "Here's your generated image!"
            return [image_data, message_text]

        except Exception as e:
            error_msg = f"Failed to queue prompt: {e}"
            return error_msg

    # ---------------------------------------------------------
    # IRC handler
    # ---------------------------------------------------------
    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        response = "This plugin is only supported on Discord and WebUI."
        formatted = format_irc(response)
        await bot.privmsg(channel, f"{user}: {formatted}")

plugin = ComfyUIImagePlugin()