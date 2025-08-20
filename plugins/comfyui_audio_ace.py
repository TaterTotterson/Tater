# plugins/comfyui_audio_ace.py
import os
import json
import asyncio
import base64
import re
import yaml
import random
import copy
import requests
from io import BytesIO
from plugin_base import ToolPlugin
import discord
import streamlit as st
from helpers import redis_client, format_irc, run_comfy_prompt

class ComfyUIAudioAcePlugin(ToolPlugin):
    name = "comfyui_audio_ace"
    usage = (
        '{\n'
        '  "function": "comfyui_audio_ace",\n'
        '  "arguments": {"prompt": "<Concept for the song, e.g. happy summer song>"}\n'
        '}\n'
    )
    description = "Generates music using ComfyUI Audio Ace."
    pretty_name = "Your Song"
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

    # ---------------------------
    # Server URL helpers
    # ---------------------------
    @staticmethod
    def get_base_http():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIAudioAcePlugin.settings_category}")
        url = (settings.get("COMFYUI_AUDIO_ACE_URL") or "").strip() or "http://localhost:8188"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url
        return url.rstrip("/")

    @staticmethod
    def get_base_ws(base_http: str) -> str:
        # http://host:8188 -> ws://host:8188
        scheme = "wss" if base_http.startswith("https://") else "ws"
        return base_http.replace("http", scheme, 1)

    # ---------------------------
    # Workflow template
    # ---------------------------
    @staticmethod
    def get_workflow_template():
        return {
          "14": {
            "inputs": {
              "tags": "",
              "lyrics": "",
              "lyrics_strength": 0.9900000000000002,
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
            "inputs": {"multiplier": 1.0000000000000002},
            "class_type": "LatentOperationTonemapReinhard",
            "_meta": {"title": "LatentOperationTonemapReinhard"}
          },
          "51": {
            "inputs": {"shift": 5.000000000000001, "model": ["40", 0]},
            "class_type": "ModelSamplingSD3",
            "_meta": {"title": "ModelSamplingSD3"}
          },
          "52": {
            "inputs": {
              "seed": 194793839343750,
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

    # ---------------------------
    # LLM helpers
    # ---------------------------
    async def get_tags_and_lyrics(self, user_prompt, llm_client):
        return await self.generate_tags_and_lyrics(user_prompt, llm_client)

    @staticmethod
    async def generate_tags_and_lyrics(user_prompt, llm_client):
        system_prompt = (
            f'The user wants a song: "{user_prompt}".\n\n'
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
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": "Write tags and lyrics for the song."}
            ]
        )
        content = response.get("message", {}).get("content", "").strip()
        try:
            cleaned = re.sub(r"^```(?:json)?\s*|```$", "", content, flags=re.MULTILINE).strip()
            try:
                result = json.loads(cleaned)
            except json.JSONDecodeError:
                result = yaml.safe_load(cleaned)
                cleaned = json.dumps(result)
                result = json.loads(cleaned)

            tags = result.get("tags", "").strip()
            lyrics = result.get("lyrics", "").strip()

            allowed_sections = ["[verse]", "[chorus]", "[bridge]", "[outro]"]
            if not tags or "[inst]" not in lyrics or not any(tag in lyrics for tag in allowed_sections):
                raise Exception("Missing or improperly formatted 'tags' or 'lyrics'.")

            return tags, lyrics
        except Exception as e:
            raise Exception(f"LLM response format error: {e}\nContent:\n{content}")

    # ---------------------------
    # ComfyUI I/O helpers (requests)
    # ---------------------------
    @staticmethod
    def get_history(base_http: str, prompt_id: str):
        r = requests.get(f"{base_http}/history/{prompt_id}", timeout=30)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def get_audio_bytes(base_http: str, filename: str, subfolder: str, folder_type: str) -> bytes:
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        r = requests.get(f"{base_http}/view", params=params, timeout=60)
        r.raise_for_status()
        return r.content

    # ---------------------------
    # Main generation pipeline
    # ---------------------------
    @staticmethod
    def build_workflow(tags: str, lyrics: str) -> dict:
        workflow = copy.deepcopy(ComfyUIAudioAcePlugin.get_workflow_template())
        workflow["14"]["inputs"]["tags"] = tags
        workflow["14"]["inputs"]["lyrics"] = lyrics

        # Estimate song duration from lyric lines
        lines = lyrics.strip().splitlines()
        line_count = sum(1 for l in lines if l.strip() and not l.strip().startswith("["))
        estimated_duration = int(line_count * 5.0) + 20
        duration = max(30, min(300, estimated_duration))
        workflow["17"]["inputs"]["seconds"] = duration

        # Randomize any 'seed' so each run is unique
        for node in workflow.values():
            if isinstance(node, dict):
                inputs = node.get("inputs")
                if isinstance(inputs, dict) and "seed" in inputs:
                    inputs["seed"] = random.randint(0, 2**63 - 1)
        return workflow

    @staticmethod
    def process_prompt_sync(tags: str, lyrics: str) -> bytes:
        base_http = ComfyUIAudioAcePlugin.get_base_http()
        base_ws   = ComfyUIAudioAcePlugin.get_base_ws(base_http)

        workflow = ComfyUIAudioAcePlugin.build_workflow(tags, lyrics)

        # Run Comfy prompt (per-job client_id inside run_comfy_prompt)
        prompt_id, _ = run_comfy_prompt(base_http, base_ws, workflow)

        # Pull history and fetch produced audio
        history = ComfyUIAudioAcePlugin.get_history(base_http, prompt_id).get(prompt_id, {})
        outputs = history.get("outputs", {}) if isinstance(history, dict) else {}

        for node_id, node_out in outputs.items():
            if "audio" in node_out:
                for audio_meta in node_out["audio"]:
                    filename = audio_meta.get("filename")
                    subfolder = audio_meta.get("subfolder", "")
                    folder_type = audio_meta.get("type", "output")
                    if filename:
                        return ComfyUIAudioAcePlugin.get_audio_bytes(base_http, filename, subfolder, folder_type)

        raise Exception("No audio returned.")

    async def _generate(self, prompt: str, llm_client):
        # Full async pipeline
        tags, lyrics = await self.generate_tags_and_lyrics(prompt, llm_client)
        audio_bytes = await asyncio.to_thread(self.process_prompt_sync, tags, lyrics)

        audio_data = {
            "type": "audio",
            "name": "ace_song.mp3",
            "data": base64.b64encode(audio_bytes).decode("utf-8"),
            "mimetype": "audio/mpeg"
        }

        system_msg = f'The user received a ComfyUI-generated song based on: "{prompt}"'
        response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": "Send a short friendly comment about the new song. Only generate the message. Do not respond to this message."}
            ]
        )
        message_text = response["message"].get("content", "").strip() or "Hope you enjoy the track!"
        return [audio_data, message_text]

    # ---------------------------------------
    # Discord
    # ---------------------------------------
    async def handle_discord(self, message, args, llm_client):
        user_prompt = args.get("prompt")
        if not user_prompt:
            return "No prompt provided."
        try:
            tags, lyrics = await self.generate_tags_and_lyrics(user_prompt, llm_client)
            audio_bytes = await asyncio.to_thread(self.process_prompt_sync, tags, lyrics)

            system_msg = f'The user received a ComfyUI-generated audio clip based on: "{user_prompt}"'
            response = await llm_client.chat(
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
    async def handle_webui(self, args, llm_client):
        prompt = args.get("prompt", "").strip()
        if not prompt:
            return ["No prompt provided."]
        try:
            # If we're already in an event loop (e.g. live WebUI), await directly
            asyncio.get_running_loop()
            return await self._generate(prompt, llm_client)
        except RuntimeError:
            # Otherwise (background thread), spin up a fresh loop
            return asyncio.run(self._generate(prompt, llm_client))

    # ---------------------------------------
    # IRC
    # ---------------------------------------
    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        msg = "Sorry, this plugin only works in Discord or WebUI."
        await bot.privmsg(channel, f"{user}: {format_irc(msg)}")

plugin = ComfyUIAudioAcePlugin()
