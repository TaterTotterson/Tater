# plugins/comfyui_audio_plugin.py
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
import streamlit as st
from helpers import redis_client, send_waiting_message, load_image_from_url
import base64

# Generate a unique client ID for this plugin instance.
client_id = str(uuid.uuid4())

class ComfyUIAudioPlugin(ToolPlugin):
    name = "comfyui_audio_plugin"
    usage = (
        '{\n'
        '  "function": "comfyui_audio_plugin",\n'
        '  "arguments": {"prompt": "<Text prompt for the audio, music and beats.>"}\n'
        '}\n'
    )
    description = "Generates audio, music and beats using ComfyUI."
    settings_category = "ComfyUI Audio"
    required_settings = {
        "COMFYUI_AUDIO_URL": {
            "label": "ComfyUI Audio URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "The base URL for the ComfyUI Audio API (do not include endpoint paths)."
        },
        "COMFYUI_AUDIO_WORKFLOW": {
            "label": "Audio Workflow Template (JSON)",
            "type": "file",
            "default": "",
            "description": "Upload your JSON workflow template file for audio generation. This field is required."
        }
    }
    waiting_prompt_template = "Generate a message telling the user to please wait while you call upon an orchestra and a talented ensemble of musicians to compose your musical experience! Only generate the message. Do not respond to this message."
    platforms = ["discord", "webui"]
    assistant_avatar = load_image_from_url()

    @staticmethod
    def get_server_address():
        # Retrieve settings for the audio plugin.
        settings = redis_client.hgetall("plugin_settings:ComfyUI Audio")
        url = settings.get("COMFYUI_AUDIO_URL", "").strip()
        if not url:
            return "localhost:8188"
        # Remove scheme if present.
        if url.startswith("http://"):
            return url[len("http://"):]
        elif url.startswith("https://"):
            return url[len("https://"):]
        else:
            return url

    @staticmethod
    def queue_prompt(prompt):
        server_address = ComfyUIAudioPlugin.get_server_address()
        p = {"prompt": prompt, "client_id": client_id}
        data = json.dumps(p).encode("utf-8")
        req = urllib.request.Request(
            "http://{}/prompt".format(server_address),
            data=data,
            headers={"Content-Type": "application/json"}
        )
        return json.loads(urllib.request.urlopen(req).read())

    @staticmethod
    def get_audio(filename, subfolder, folder_type):
        server_address = ComfyUIAudioPlugin.get_server_address()
        data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url_values = urllib.parse.urlencode(data)
        with urllib.request.urlopen("http://{}/view?{}".format(server_address, url_values)) as response:
            return response.read()

    @staticmethod
    def get_history(prompt_id):
        server_address = ComfyUIAudioPlugin.get_server_address()
        with urllib.request.urlopen("http://{}/history/{}".format(server_address, prompt_id)) as response:
            return json.loads(response.read())

    @staticmethod
    def get_audios(ws, prompt):
        prompt_id = ComfyUIAudioPlugin.queue_prompt(prompt)["prompt_id"]
        output_audios = {}
        while True:
            out = ws.recv()
            if isinstance(out, str):
                message = json.loads(out)
                if message["type"] == "executing":
                    data = message["data"]
                    if data["node"] is None and data["prompt_id"] == prompt_id:
                        break  # Execution is complete.
            else:
                continue  # Skip any non-text (e.g. binary) messages.
        history = ComfyUIAudioPlugin.get_history(prompt_id)[prompt_id]
        for node_id in history["outputs"]:
            node_output = history["outputs"][node_id]
            audios_output = []
            if "audio" in node_output:
                for audio in node_output["audio"]:
                    audio_data = ComfyUIAudioPlugin.get_audio(audio["filename"], audio["subfolder"], audio["type"])
                    audios_output.append(audio_data)
            output_audios[node_id] = audios_output
        return output_audios

    @staticmethod
    def get_workflow_template():
        settings = redis_client.hgetall("plugin_settings:ComfyUI Audio")
        workflow_str = settings.get("COMFYUI_AUDIO_WORKFLOW", "").strip()
        if not workflow_str:
            raise Exception("No workflow template set in COMFYUI_AUDIO_WORKFLOW. Please provide a valid JSON template.")
        return json.loads(workflow_str)

    @staticmethod
    def process_prompt(user_prompt: str) -> bytes:
        # Retrieve and update the workflow template with the user prompt.
        workflow = ComfyUIAudioPlugin.get_workflow_template()
        # Adjust the workflow nodes as needed for your audio generation.
        workflow["6"]["inputs"]["text"] = user_prompt
        workflow["6"]["widgets_values"] = [user_prompt]
        ws = websocket.WebSocket()
        server_address = ComfyUIAudioPlugin.get_server_address()
        ws.connect("ws://{}/ws?clientId={}".format(server_address, client_id))
        audios = ComfyUIAudioPlugin.get_audios(ws, workflow)
        ws.close()
        # Return the first audio found.
        for node_id, audios_list in audios.items():
            if audios_list:
                return audios_list[0]
        raise Exception("No audio returned from ComfyUI.")

    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI Audio."

        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=self.waiting_prompt_template,
            save_callback=lambda x: None,
            send_callback=lambda x: message.channel.send(x)
        )

        try:
            audio_bytes = await asyncio.to_thread(ComfyUIAudioPlugin.process_prompt, user_prompt)
            file = discord.File(BytesIO(audio_bytes), filename="generated_audio.mp3")
            await message.channel.send(file=file)

            # Optional AI follow-up message
            safe_prompt = user_prompt[:300].strip()
            system_msg = f'The user just received an AI-generated audio clip based on this prompt: "{safe_prompt}".'
            final_response = await ollama_client.chat(
                model=ollama_client.model,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Send a short and friendly sentence about the generated audio."}
                ],
                stream=False,
                keep_alive=ollama_client.keep_alive,
                options={"num_ctx": context_length}
            )

            message_text = final_response["message"].get("content", "").strip() or "Enjoy your track!"
            await message.channel.send(message_text)

        except Exception as e:
            await message.channel.send(f"Failed to generate audio: {e}")

        return ""

    async def handle_webui(self, args, ollama_client, context_length):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided for ComfyUI Audio."

        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=self.waiting_prompt_template,
            save_callback=None,
            send_callback=lambda x: st.chat_message("assistant", avatar=self.assistant_avatar).write(x)
        )

        try:
            audio_bytes = await asyncio.to_thread(ComfyUIAudioPlugin.process_prompt, user_prompt)

            # Return base64-encoded audio for WebUI
            audio_data = {
                "type": "audio",
                "name": "generated_audio.mp3",
                "data": base64.b64encode(audio_bytes).decode("utf-8"),
                "mimetype": "audio/mpeg"
            }

            # Friendly follow-up from Ollama
            safe_prompt = user_prompt[:300].strip()
            system_msg = f'The user just received an AI-generated audio clip based on this prompt: "{safe_prompt}".'
            final_response = await ollama_client.chat(
                model=ollama_client.model,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Send a short and friendly sentence about the generated audio."}
                ],
                stream=False,
                keep_alive=ollama_client.keep_alive,
                options={"num_ctx": context_length}
            )

            message_text = final_response["message"].get("content", "").strip() or "Enjoy your track!"

            return [audio_data, message_text]

        except Exception as e:
            return f"Failed to generate audio: {e}"

plugin = ComfyUIAudioPlugin()