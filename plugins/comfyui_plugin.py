# plugins/comfyui_plugin.py
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
from helpers import redis_client, send_waiting_message, load_image_from_url
import base64

client_id = str(uuid.uuid4())

class ComfyUIPlugin(ToolPlugin):
    name = "comfyui_plugin"
    usage = (
        "{\n"
        '  "function": "comfyui_plugin",\n'
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
        settings = redis_client.hgetall("plugin_settings:ComfyUI")
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
        server_address = ComfyUIPlugin.get_server_address()
        p = {"prompt": prompt, "client_id": client_id}
        data = json.dumps(p).encode("utf-8")
        req = urllib.request.Request("http://{}/prompt".format(server_address),
                                     data=data,
                                     headers={"Content-Type": "application/json"})
        return json.loads(urllib.request.urlopen(req).read())

    @staticmethod
    def get_image(filename, subfolder, folder_type):
        server_address = ComfyUIPlugin.get_server_address()
        data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url_values = urllib.parse.urlencode(data)
        with urllib.request.urlopen("http://{}/view?{}".format(server_address, url_values)) as response:
            return response.read()

    @staticmethod
    def get_history(prompt_id):
        server_address = ComfyUIPlugin.get_server_address()
        with urllib.request.urlopen("http://{}/history/{}".format(server_address, prompt_id)) as response:
            return json.loads(response.read())

    @staticmethod
    def get_images(ws, prompt):
        prompt_id = ComfyUIPlugin.queue_prompt(prompt)["prompt_id"]
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
        history = ComfyUIPlugin.get_history(prompt_id)[prompt_id]
        for node_id in history["outputs"]:
            node_output = history["outputs"][node_id]
            images_output = []
            if "images" in node_output:
                for image in node_output["images"]:
                    image_data = ComfyUIPlugin.get_image(image["filename"], image["subfolder"], image["type"])
                    images_output.append(image_data)
            output_images[node_id] = images_output
        return output_images

    @staticmethod
    def get_workflow_template():
        settings = redis_client.hgetall("plugin_settings:ComfyUI")
        workflow_str = settings.get("COMFYUI_WORKFLOW", "").strip()
        if not workflow_str:
            raise Exception("No workflow template set in COMFYUI_WORKFLOW. Please provide a valid JSON template.")
        return json.loads(workflow_str)

    @staticmethod
    def process_prompt(user_prompt: str) -> bytes:
        # Retrieve the workflow template from settings
        workflow = ComfyUIPlugin.get_workflow_template()
        # Update positive prompt (node 6)
        workflow["6"]["inputs"]["text"] = user_prompt
        workflow["6"]["widgets_values"] = [user_prompt]
        ws = websocket.WebSocket()
        server_address = ComfyUIPlugin.get_server_address()
        ws.connect("ws://{}/ws?clientId={}".format(server_address, client_id))
        images = ComfyUIPlugin.get_images(ws, workflow)
        ws.close()
        # Return the first image found
        for node_id, imgs in images.items():
            if imgs:
                return imgs[0]
        raise Exception("No images returned from ComfyUI.")

    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI."

        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=self.waiting_prompt_template,
            save_callback=lambda x: None,
            send_callback=lambda x: message.channel.send(x)
        )

        try:
            image_bytes = await asyncio.to_thread(ComfyUIPlugin.process_prompt, user_prompt)
            file = discord.File(BytesIO(image_bytes), filename="generated_comfyui.png")
            await message.channel.send(file=file)

            # Generate friendly follow-up message via Ollama
            safe_prompt = user_prompt[:300].strip()
            system_msg = f'The user has just been shown an AI-generated image based on the prompt: "{safe_prompt}".'
            final_response = await ollama_client.chat(
                model=ollama_client.model,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Give them a short, friendly sentence acknowledging the generated image."}
                ],
                stream=False,
                keep_alive=ollama_client.keep_alive,
                options={"num_ctx": context_length}
            )

            message_text = final_response["message"].get("content", "").strip() or "Here's your generated image!"
            await message.channel.send(message_text)

        except Exception as e:
            await message.channel.send(f"Failed to queue prompt: {e}")
        
        return ""

    async def handle_webui(self, args, ollama_client, context_length):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI."

        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=self.waiting_prompt_template,
            save_callback=None,
            send_callback=lambda x: st.chat_message("assistant", avatar=self.assistant_avatar).write(x)
        )

        try:
            image_bytes = await asyncio.to_thread(ComfyUIPlugin.process_prompt, user_prompt)

            # Return image data so WebUI can save & show it
            image_data = {
                "type": "image",
                "name": "generated_comfyui.png",
                "data": base64.b64encode(image_bytes).decode("utf-8"),
                "mimetype": "image/png"
            }

            # Generate friendly follow-up
            safe_prompt = user_prompt[:300].strip()
            system_msg = f'The user has just been shown an AI-generated image based on the prompt: "{safe_prompt}".'
            final_response = await ollama_client.chat(
                model=ollama_client.model,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Give them a short, friendly sentence acknowledging the generated image."}
                ],
                stream=False,
                keep_alive=ollama_client.keep_alive,
                options={"num_ctx": context_length}
            )

            message_text = final_response["message"].get("content", "").strip() or "Here's your generated image!"

            # ✅ WebUI will store and render both
            return [image_data, message_text]

        except Exception as e:
            return f"Failed to queue prompt: {e}"

    async def generate_error_message(self, prompt, fallback, message):
        return fallback

plugin = ComfyUIPlugin()