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
from helpers import redis_client, load_image_from_url, send_waiting_message, save_assistant_message
import base64

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
        settings = redis_client.hgetall("plugin_settings:ComfyUI Audio")
        url = settings.get("COMFYUI_AUDIO_URL", "").strip()
        if not url:
            return "localhost:8188"
        if url.startswith("http://"):
            return url[len("http://"):]
        elif url.startswith("https://"):
            return url[len("https://"):]
        return url

    @staticmethod
    def queue_prompt(prompt):
        server_address = ComfyUIAudioPlugin.get_server_address()
        p = {"prompt": prompt, "client_id": client_id}
        data = json.dumps(p).encode("utf-8")
        req = urllib.request.Request(
            f"http://{server_address}/prompt",
            data=data,
            headers={"Content-Type": "application/json"}
        )
        return json.loads(urllib.request.urlopen(req).read())

    @staticmethod
    def get_audio(filename, subfolder, folder_type):
        server_address = ComfyUIAudioPlugin.get_server_address()
        data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url_values = urllib.parse.urlencode(data)
        with urllib.request.urlopen(f"http://{server_address}/view?{url_values}") as response:
            return response.read()

    @staticmethod
    def get_history(prompt_id):
        server_address = ComfyUIAudioPlugin.get_server_address()
        with urllib.request.urlopen(f"http://{server_address}/history/{prompt_id}") as response:
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
                        break
        history = ComfyUIAudioPlugin.get_history(prompt_id)[prompt_id]
        for node_id in history["outputs"]:
            node_output = history["outputs"][node_id]
            if "audio" in node_output:
                audios_output = [
                    ComfyUIAudioPlugin.get_audio(audio["filename"], audio["subfolder"], audio["type"])
                    for audio in node_output["audio"]
                ]
                output_audios[node_id] = audios_output
        return output_audios

    @staticmethod
    def get_workflow_template():
        settings = redis_client.hgetall("plugin_settings:ComfyUI Audio")
        workflow_str = settings.get("COMFYUI_AUDIO_WORKFLOW", "").strip()
        if not workflow_str:
            raise Exception("No workflow template set in COMFYUI_AUDIO_WORKFLOW.")
        return json.loads(workflow_str)

    @staticmethod
    def process_prompt(user_prompt: str) -> bytes:
        workflow = ComfyUIAudioPlugin.get_workflow_template()
        workflow["6"]["inputs"]["text"] = user_prompt
        workflow["6"]["widgets_values"] = [user_prompt]
        ws = websocket.WebSocket()
        server_address = ComfyUIAudioPlugin.get_server_address()
        ws.connect(f"ws://{server_address}/ws?clientId={client_id}")
        audios = ComfyUIAudioPlugin.get_audios(ws, workflow)
        ws.close()
        for audios_list in audios.values():
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
            save_callback=lambda text: save_assistant_message(message.channel.id, text),
            send_callback=lambda text: message.channel.send(text)
        )

        try:
            audio_bytes = await asyncio.to_thread(self.process_prompt, user_prompt)
            file = discord.File(BytesIO(audio_bytes), filename="generated_audio.mp3")
            await message.channel.send(file=file)
            save_assistant_message(message.channel.id, "🎵")

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
            save_assistant_message(message.channel.id, message_text)

        except Exception as e:
            error = f"Failed to generate audio: {e}"
            await message.channel.send(error)
            save_assistant_message(message.channel.id, error)

        return ""

    async def handle_webui(self, args, ollama_client, context_length):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided."

        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=self.waiting_prompt_template,
            save_callback=lambda text: save_assistant_message("webui", text),
            send_callback=lambda text: st.chat_message("assistant", avatar=self.assistant_avatar).write(text)
        )

        try:
            audio_bytes = await asyncio.to_thread(self.process_prompt, user_prompt)
            audio_data = {
                "type": "audio",
                "name": "generated_audio.mp3",
                "data": base64.b64encode(audio_bytes).decode("utf-8"),
                "mimetype": "audio/mpeg"
            }

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
            save_assistant_message("webui", message_text)

            return [audio_data, message_text]

        except Exception as e:
            error = f"Failed to generate audio: {e}"
            save_assistant_message("webui", error)
            return error

    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        await bot.privmsg(channel, f"{user}: ❌ This plugin only works in Discord or the WebUI.")

plugin = ComfyUIAudioPlugin()