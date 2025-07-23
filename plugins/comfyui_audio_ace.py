# plugins/comfyui_audio_ace.py
import os
import json
import uuid
import urllib.request
import urllib.parse
import asyncio
import base64
import websocket
from io import BytesIO
from plugin_base import ToolPlugin
import discord
import streamlit as st
import re
import yaml
from helpers import redis_client, format_irc

client_id = str(uuid.uuid4())

class ComfyUIAudioAcePlugin(ToolPlugin):
    name = "comfyui_audio_ace"
    usage = (
        '{\n'
        '  "function": "comfyui_audio_ace",\n'
        '  "arguments": {"prompt": "<Concept for the song, e.g. happy summer song>"}\n'
        '}\n'
    )
    description = "Generates music using ComfyUI Audio Ace."
    settings_category = "ComfyUI Audio Ace"
    platforms = ["discord", "webui"]
    required_settings = {
        "COMFYUI_AUDIO_ACE_URL": {
            "label": "ComfyUI Audio Ace URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "Base URL for the ComfyUI Ace Audio server."
        }
    }
    waiting_prompt_template = "Write a fun, upbeat message saying you’re writing lyrics and calling in a virtual band now! Only output that message."
    
    async def get_tags_and_lyrics(self, user_prompt, ollama_client):
        return await self.generate_tags_and_lyrics(user_prompt, ollama_client)

    @staticmethod
    def get_server_address():
        settings = redis_client.hgetall(
            f"plugin_settings:{ComfyUIAudioAcePlugin.settings_category}"
        )
        url = settings.get("COMFYUI_AUDIO_ACE_URL", "").strip() or "localhost:8188"
        if not url:
            return "localhost:8188"
        return url.replace("http://", "").replace("https://", "")

    @staticmethod
    def get_workflow_template():
        return {
            "14": {
                "inputs": {
                    "tags": "",
                    "lyrics": "",
                    "lyrics_strength": 0.99,
                    "clip": ["40", 1]
                },
                "class_type": "TextEncodeAceStepAudio",
                "_meta": {"title": "TextEncodeAceStepAudio"}
            },
            "17": {
                "inputs": {"seconds": 120, "batch_size": 1},
                "class_type": "EmptyAceStepLatentAudio",
                "_meta": {"title": "EmptyAceStepLatentAudio"}
            },
            "18": {
                "inputs": {"samples": ["52", 0], "vae": ["40", 2]},
                "class_type": "VAEDecodeAudio",
                "_meta": {"title": "VAEDecodeAudio"}
            },
            "40": {
                "inputs": {"ckpt_name": "ace_step_v1_3.5b.safetensors"},
                "class_type": "CheckpointLoaderSimple",
                "_meta": {"title": "Load Checkpoint"}
            },
            "44": {
                "inputs": {"conditioning": ["14", 0]},
                "class_type": "ConditioningZeroOut",
                "_meta": {"title": "ConditioningZeroOut"}
            },
            "49": {
                "inputs": {"model": ["51", 0], "operation": ["50", 0]},
                "class_type": "LatentApplyOperationCFG",
                "_meta": {"title": "LatentApplyOperationCFG"}
            },
            "50": {
                "inputs": {"multiplier": 1.0},
                "class_type": "LatentOperationTonemapReinhard",
                "_meta": {"title": "LatentOperationTonemapReinhard"}
            },
            "51": {
                "inputs": {"shift": 5.0, "model": ["40", 0]},
                "class_type": "ModelSamplingSD3",
                "_meta": {"title": "ModelSamplingSD3"}
            },
            "52": {
                "inputs": {
                    "seed": 468254064217846,
                    "steps": 50,
                    "cfg": 5,
                    "sampler_name": "euler",
                    "scheduler": "simple",
                    "denoise": 1,
                    "model": ["49", 0],
                    "positive": ["14", 0],
                    "negative": ["44", 0],
                    "latent_image": ["17", 0]
                },
                "class_type": "KSampler",
                "_meta": {"title": "KSampler"}
            },
            "59": {
                "inputs": {
                    "filename_prefix": "audio/ComfyUI",
                    "quality": "V0",
                    "audioUI": "",
                    "audio": ["18", 0]
                },
                "class_type": "SaveAudioMP3",
                "_meta": {"title": "Save Audio (MP3)"}
            }
        }

    @staticmethod
    async def generate_tags_and_lyrics(user_prompt, ollama_client):
        system_prompt = (
            f"The user wants a song: \"{user_prompt}\".\n\n"
            "Write a JSON object with these two fields:\n"
            "1. `tags`: a comma-separated list of music style keywords.\n"
            "2. `lyrics`: multiline string using the following format (in English):\n\n"
            "[inst]\\n\\n[verse]\\nline one\\nline two\\n...\n\n"
            "IMPORTANT:\n"
            "- Escape all newlines using double backslashes (\\n).\n"
            "- Use only these section headers: [inst], [verse], [chorus], [bridge], [outro].\n"
            "- Do NOT use [verse 1], [chorus 2], or any custom tag variants.\n"
            "- Output ONLY valid JSON, no explanation."
        )

        response = await ollama_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": "Write tags and lyrics for the song."}
            ]
        )

        content = response.get("message", {}).get("content", "").strip()

        try:
            # Remove markdown code block wrappers
            cleaned = re.sub(r"^```(?:json)?\s*|```$", "", content, flags=re.MULTILINE).strip()

            # Try strict JSON parse
            try:
                result = json.loads(cleaned)
            except json.JSONDecodeError:
                # Fall back to YAML (handles unescaped quotes, newlines, etc.)
                result = yaml.safe_load(cleaned)
                cleaned = json.dumps(result)  # convert back to proper JSON
                result = json.loads(cleaned)

            # Extract + validate fields
            tags = result.get("tags", "").strip()
            lyrics = result.get("lyrics", "").strip()

            allowed_sections = ["[verse]", "[chorus]", "[bridge]", "[outro]"]
            if not tags or "[inst]" not in lyrics or not any(tag in lyrics for tag in allowed_sections):
                raise Exception("Missing or improperly formatted 'tags' or 'lyrics'.")

            return tags, lyrics

        except Exception as e:
            raise Exception(f"Ollama response format error: {e}\nContent:\n{content}")

    @staticmethod
    def queue_prompt(prompt):
        server_address = ComfyUIAudioAcePlugin.get_server_address()
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
        server_address = ComfyUIAudioAcePlugin.get_server_address()
        data = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        url_values = urllib.parse.urlencode(data)
        with urllib.request.urlopen(f"http://{server_address}/view?{url_values}") as response:
            return response.read()

    @staticmethod
    def get_history(prompt_id):
        server_address = ComfyUIAudioAcePlugin.get_server_address()
        with urllib.request.urlopen(f"http://{server_address}/history/{prompt_id}") as response:
            return json.loads(response.read())

    @staticmethod
    def get_audios(ws, prompt):
        prompt_id = ComfyUIAudioAcePlugin.queue_prompt(prompt)["prompt_id"]
        output_audios = {}
        while True:
            out = ws.recv()
            if isinstance(out, str):
                message = json.loads(out)
                if message["type"] == "executing":
                    data = message["data"]
                    if data["node"] is None and data["prompt_id"] == prompt_id:
                        break
        history = ComfyUIAudioAcePlugin.get_history(prompt_id)[prompt_id]
        for node_id in history["outputs"]:
            node_output = history["outputs"][node_id]
            if "audio" in node_output:
                audios_output = [
                    ComfyUIAudioAcePlugin.get_audio(audio["filename"], audio["subfolder"], audio["type"])
                    for audio in node_output["audio"]
                ]
                output_audios[node_id] = audios_output
        return output_audios

    @staticmethod
    def process_prompt(user_prompt: str, tags: str, lyrics: str) -> bytes:
        workflow = ComfyUIAudioAcePlugin.get_workflow_template()
        workflow["14"]["inputs"]["tags"] = tags
        workflow["14"]["inputs"]["lyrics"] = lyrics

        # Automatically estimate song duration based on lyric content
        lines = lyrics.strip().splitlines()
        line_count = sum(1 for l in lines if l.strip() and not l.strip().startswith("["))
        estimated_duration = int(line_count * 5.0) + 20
        duration = max(30, min(300, estimated_duration))  # Clamp to sane range

        workflow["17"]["inputs"]["seconds"] = duration

        ws = websocket.WebSocket()
        server_address = ComfyUIAudioAcePlugin.get_server_address()
        ws.connect(f"ws://{server_address}/ws?clientId={client_id}")
        audios = ComfyUIAudioAcePlugin.get_audios(ws, workflow)
        ws.close()

        for audios_list in audios.values():
            if audios_list:
                return audios_list[0]

        raise Exception("No audio returned.")

    # ---------------------------------------
    # Discord
    # ---------------------------------------
    async def handle_discord(self, message, args, ollama_client):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided."

        try:
            tags, lyrics = await self.generate_tags_and_lyrics(user_prompt, ollama_client)
            audio_bytes = await asyncio.to_thread(self.process_prompt, user_prompt, tags, lyrics)

            system_msg = f'The user received a ComfyUI-generated audio clip based on: "{user_prompt}"'
            response = await ollama_client.chat(
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Send a short friendly comment about the new song. Only generate the message. Do not respond to this message."}
                ]
            )

            message_text = response["message"].get("content", "").strip() or "Hope you enjoy the track!"

            return [
                {
                    "type": "audio",
                    "name": "ace_song.mp3",
                    "data": base64.b64encode(audio_bytes).decode("utf-8"),
                    "mimetype": "audio/mpeg"
                },
                message_text
            ]

        except Exception as e:
            return f"Failed to create song: {e}"

    # ---------------------------------------
    # WebUI
    # ---------------------------------------
    async def handle_webui(self, args, ollama_client):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided."

        try:
            tags, lyrics = await self.generate_tags_and_lyrics(user_prompt, ollama_client)
            audio_bytes = await asyncio.to_thread(self.process_prompt, user_prompt, tags, lyrics)

            audio_data = {
                "type": "audio",
                "name": "ace_song.mp3",
                "data": base64.b64encode(audio_bytes).decode("utf-8"),
                "mimetype": "audio/mpeg"
            }

            system_msg = f'The user received a ComfyUI-generated song based on: \"{user_prompt}\"'
            response = await ollama_client.chat(
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Send a short friendly comment about the new song. Only generate the message. Do not respond to this message."}
                ]
            )

            message_text = response["message"].get("content", "").strip() or "Hope you enjoy the track!"

            return [audio_data, message_text]

        except Exception as e:
            return f"Failed to create song: {e}"

    # ---------------------------------------
    # IRC
    # ---------------------------------------
    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        msg = "Sorry, this plugin only works in Discord or WebUI."
        await bot.privmsg(channel, f"{user}: {format_irc(msg)}")

plugin = ComfyUIAudioAcePlugin()