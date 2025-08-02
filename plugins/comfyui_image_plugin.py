import os
import json
import uuid
import urllib.request
import urllib.parse
import asyncio
import websocket
from io import BytesIO
from plugin_base import ToolPlugin
import discord
from helpers import redis_client, format_irc
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

    @staticmethod
    def get_server_address():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImagePlugin.settings_category}")
        url = settings.get("COMFYUI_URL", b"").decode("utf-8").strip() if isinstance(settings.get("COMFYUI_URL"), bytes) else settings.get("COMFYUI_URL", "").strip()
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
        server_address = ComfyUIImagePlugin.get_server_address()
        p = {"prompt": prompt, "client_id": client_id}
        data = json.dumps(p).encode("utf-8")
        req = urllib.request.Request(f"http://{server_address}/prompt",
                                     data=data,
                                     headers={"Content-Type": "application/json"})
        return json.loads(urllib.request.urlopen(req).read())

    @staticmethod
    def get_image(filename, subfolder, folder_type):
        server_address = ComfyUIImagePlugin.get_server_address()
        data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url_values = urllib.parse.urlencode(data)
        with urllib.request.urlopen(f"http://{server_address}/view?{url_values}") as response:
            return response.read()

    @staticmethod
    def get_history(prompt_id):
        server_address = ComfyUIImagePlugin.get_server_address()
        with urllib.request.urlopen(f"http://{server_address}/history/{prompt_id}") as response:
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
                        break
        history = ComfyUIImagePlugin.get_history(prompt_id)[prompt_id]
        for node_id, node_output in history["outputs"].items():
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
        workflow_raw = settings.get("COMFYUI_WORKFLOW", b"")
        workflow_str = workflow_raw.decode("utf-8").strip() if isinstance(workflow_raw, bytes) else workflow_raw.strip()
        if not workflow_str:
            raise Exception("No workflow template set in COMFYUI_WORKFLOW. Please provide a valid JSON template.")
        return json.loads(workflow_str)

    @staticmethod
    def process_prompt(user_prompt: str, width: int = None, height: int = None) -> bytes:
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

            if not positive_found and len(encode_nodes) > 0:
                encode_nodes[0]["inputs"]["text"] = user_prompt
                encode_nodes[0]["widgets_values"] = [user_prompt]
            if not negative_found and len(encode_nodes) > 1:
                encode_nodes[1]["inputs"]["text"] = negative_prompt
                encode_nodes[1]["widgets_values"] = [negative_prompt]

        workflow = ComfyUIImagePlugin.get_workflow_template()
        insert_prompts(workflow, user_prompt)

        res_map = {
            "144p": (256, 144),
            "240p": (426, 240),
            "360p": (480, 360),
            "480p": (640, 480),
            "720p": (1280, 720),
            "1080p": (1920, 1080)
        }

        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImagePlugin.settings_category}")
        raw = settings.get("IMAGE_RESOLUTION", b"720p")
        if isinstance(raw, bytes):
            resolution = raw.decode("utf-8")
        else:
            resolution = raw
        default_w, default_h = res_map.get(resolution, (1280, 720))

        w = width or default_w
        h = height or default_h

        for node_id, node in workflow.items():
            if node.get("class_type") in ("EmptyLatentImage", "EmptySD3LatentImage", "ModelSamplingFlux"):
                node["inputs"]["width"] = w
                node["inputs"]["height"] = h

        ws = websocket.WebSocket()
        server_address = ComfyUIImagePlugin.get_server_address()
        ws.connect(f"ws://{server_address}/ws?clientId={client_id}")
        images = ComfyUIImagePlugin.get_images(ws, workflow)
        ws.close()

        for node_id, imgs in images.items():
            if imgs:
                return imgs[0]

        raise Exception("No images returned from ComfyUI.")

    async def handle_discord(self, message, args, ollama_client):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI."

        try:
            async with message.channel.typing():
                image_bytes = await asyncio.to_thread(ComfyUIImagePlugin.process_prompt, user_prompt)

                safe_prompt = user_prompt[:300].strip()
                system_msg = f'The user has just been shown an AI-generated image based on the prompt: "{safe_prompt}".'
                final_response = await ollama_client.chat(
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

    async def handle_webui(self, args, ollama_client):
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
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Respond with a short, fun message celebrating the image. Do not include any lead-in phrases or instructions — just the message."}
                ]
            )

            message_text = final_response["message"].get("content", "").strip() or "Here's your generated image!"
            return [image_data, message_text]

        except Exception as e:
            return f"Failed to queue prompt: {e}"

    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        response = "This plugin is only supported on Discord and WebUI."
        formatted = format_irc(response)
        await bot.privmsg(channel, f"{user}: {formatted}")

plugin = ComfyUIImagePlugin()