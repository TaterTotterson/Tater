# plugins/comfyui_audio_ace.py
import json
import asyncio
import base64
import re
import yaml
import random
import copy
import logging
import requests
from urllib.parse import quote

from plugin_base import ToolPlugin
from helpers import redis_client, run_comfy_prompt

logger = logging.getLogger("comfyui_audio_ace")
logger.setLevel(logging.INFO)


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
    # ✅ Add Matrix support
    platforms = ["discord", "webui", "homeassistant", "matrix"]

    required_settings = {
        "COMFYUI_AUDIO_ACE_URL": {
            "label": "ComfyUI Audio Ace URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "Base URL for the ComfyUI Ace Audio server."
        },
        "HA_DEFAULT_MEDIA_PLAYER": {
            "label": "Default media_player entity",
            "type": "string",
            "default": "",
            "description": "Required for Home Assistant. Ex: media_player.living_room_speaker"
        }
    }

    waiting_prompt_template = "Write a fun, upbeat message saying you’re writing lyrics and calling in a virtual band now! Only output that message."

    # ---------------------------
    # Settings / URL helpers
    # ---------------------------
    @staticmethod
    def _settings():
        return redis_client.hgetall(f"plugin_settings:{ComfyUIAudioAcePlugin.settings_category}") or {}

    @staticmethod
    def get_base_http():
        settings = ComfyUIAudioAcePlugin._settings()
        url = (settings.get("COMFYUI_AUDIO_ACE_URL") or "").strip() or "http://localhost:8188"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url
        return url.rstrip("/")

    @staticmethod
    def get_base_ws(base_http: str) -> str:
        # http://host:8188 -> ws://host:8188 ; https -> wss
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
            logger.exception("LLM response format error: %s", e)
            raise Exception(f"LLM response format error: {e}\nContent:\n{content}")

    # ---------------------------
    # ComfyUI helpers
    # ---------------------------
    @staticmethod
    def get_history(base_http: str, prompt_id: str):
        r = requests.get(f"{base_http}/history/{prompt_id}", timeout=60)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def get_audio_bytes(base_http: str, filename: str, subfolder: str, folder_type: str) -> bytes:
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        r = requests.get(f"{base_http}/view", params=params, timeout=120)
        r.raise_for_status()
        return r.content

    @staticmethod
    def build_comfy_view_url(base_http: str, filename: str, subfolder: str | None, folder_type: str | None) -> str:
        return (
            f"{base_http}/view"
            f"?filename={quote(filename)}"
            f"&subfolder={quote(subfolder or '')}"
            f"&type={quote(folder_type or 'output')}"
        )

    # ---------------------------
    # Main generation pipeline
    # ---------------------------
    @staticmethod
    def build_workflow(tags: str, lyrics: str) -> dict:
        workflow = copy.deepcopy(ComfyUIAudioAcePlugin.get_workflow_template())
        workflow["14"]["inputs"]["tags"] = tags
        workflow["14"]["inputs"]["lyrics"] = lyrics

        # Estimate duration from lyric lines
        lines = lyrics.strip().splitlines()
        line_count = sum(1 for l in lines if l.strip() and not l.strip().startswith("["))
        estimated_duration = int(line_count * 5.0) + 20
        duration = max(30, min(300, estimated_duration))
        workflow["17"]["inputs"]["seconds"] = duration

        # Randomize seeds for uniqueness
        for node in workflow.values():
            if isinstance(node, dict):
                inputs = node.get("inputs")
                if isinstance(inputs, dict) and "seed" in inputs:
                    inputs["seed"] = random.randint(0, 2**63 - 1)
        return workflow

    @staticmethod
    def process_prompt_sync(tags: str, lyrics: str):
        """
        Returns (media_url, audio_bytes_or_None).
        Prefer ComfyUI /view URL; also return bytes as a fallback for Discord/WebUI/Matrix.
        """
        base_http = ComfyUIAudioAcePlugin.get_base_http()
        base_ws   = ComfyUIAudioAcePlugin.get_base_ws(base_http)

        workflow = ComfyUIAudioAcePlugin.build_workflow(tags, lyrics)

        # Run the ComfyUI prompt (client_id handled inside run_comfy_prompt)
        prompt_id, _ = run_comfy_prompt(base_http, base_ws, workflow)

        # Pull history and locate produced audio
        history = ComfyUIAudioAcePlugin.get_history(base_http, prompt_id).get(prompt_id, {})
        outputs = history.get("outputs", {}) if isinstance(history, dict) else {}

        for _, node_out in outputs.items():
            if "audio" in node_out:
                for audio_meta in node_out["audio"]:
                    filename    = audio_meta.get("filename")
                    subfolder   = audio_meta.get("subfolder", "")
                    folder_type = audio_meta.get("type", "output")
                    if filename:
                        media_url = ComfyUIAudioAcePlugin.build_comfy_view_url(
                            base_http, filename, subfolder, folder_type
                        )
                        # Also try fetching bytes for inline upload
                        try:
                            audio_bytes = ComfyUIAudioAcePlugin.get_audio_bytes(base_http, filename, subfolder, folder_type)
                        except Exception:
                            audio_bytes = None
                        return media_url, audio_bytes

        raise Exception("No audio returned.")

    async def _generate(self, prompt: str, llm_client):
        # Full async pipeline (shared by Discord/WebUI/Matrix)
        tags, lyrics = await self.generate_tags_and_lyrics(prompt, llm_client)
        media_url, audio_bytes = await asyncio.to_thread(self.process_prompt_sync, tags, lyrics)

        audio_data = None
        if audio_bytes:
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

        if audio_data:
            return [audio_data, message_text]
        else:
            return [f"Your song is ready: {media_url}", message_text]

    # ---------------------------------------
    # Discord
    # ---------------------------------------
    async def handle_discord(self, message, args, llm_client):
        user_prompt = args.get("prompt", "").strip()
        if not user_prompt:
            return "No prompt provided."
        try:
            return await self._generate(user_prompt, llm_client)
        except Exception as e:
            logger.exception("ComfyUIAudioAcePlugin Discord error: %s", e)
            return f"Failed to create song: {e}"

    # ---------------------------------------
    # WebUI
    # ---------------------------------------
    async def handle_webui(self, args, llm_client):
        prompt = args.get("prompt", "").strip()
        if not prompt:
            return ["No prompt provided."]
        try:
            asyncio.get_running_loop()
            return await self._generate(prompt, llm_client)
        except RuntimeError:
            return asyncio.run(self._generate(prompt, llm_client))
        except Exception as e:
            logger.exception("ComfyUIAudioAcePlugin WebUI error: %s", e)
            return [f"Failed to create song: {e}"]

    # ---------------------------------------
    # Matrix
    # ---------------------------------------
    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        """
        Return an audio payload (base64) plus a short message.
        The Matrix platform will upload/send the media (handling E2EE automatically) and persist history.
        """
        prompt = (args.get("prompt") or "").strip()
        if not prompt:
            return "No prompt provided."
        try:
            # Reuse the shared generator so behavior matches Discord/WebUI
            return await self._generate(prompt, llm_client)
        except Exception as e:
            logger.exception("ComfyUIAudioAcePlugin Matrix error: %s", e)
            return f"Failed to create song: {e}"

    # ---------------------------------------
    # Home Assistant (non-blocking: quick ack, background job)
    # ---------------------------------------
    async def handle_homeassistant(self, args, llm_client):
        """
        Quick ACK to HA; do heavy work off the pipeline:
          - generate audio via ComfyUI
          - build direct ComfyUI /view URL
          - play on configured media_player
        """
        prompt = (args.get("prompt") or "").strip()
        if not prompt:
            return "No prompt provided."

        target_player = self._pick_target_player()
        if not target_player:
            return (
                "I can create your song, but I need a media player configured. "
                "Set **Default media_player entity** in the ComfyUI Audio Ace plugin settings."
            )

        # Fire-and-forget the heavy work
        try:
            asyncio.create_task(self._bg_generate_and_play(prompt, llm_client, target_player))
        except Exception as e:
            logger.exception("Failed to schedule background job: %s", e)

        # AI-generated, upbeat ACK
        system_msg = (
            f"The user asked for a song that will be played on {target_player}. "
            "Write a short, fun, upbeat message telling them the track is being created and "
            "will play soon. Include one friendly emoji, and output only the message."
        )
        try:
            resp = await llm_client.chat(
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": "Generate that short message now."}
                ]
            )
            msg = resp["message"].get("content", "").strip() or f"Your song will play soon on {target_player}!"
            return msg
        except Exception as e:
            logger.exception("LLM ack generation failed: %s", e)
            return f"Your song will play soon on {target_player}!"

    async def _bg_generate_and_play(self, prompt: str, llm_client, target_player: str):
        """
        Off the critical path:
         - generate song
         - build ComfyUI /view URL
         - play on media_player
        """
        # 1) generate audio
        try:
            tags, lyrics = await self.generate_tags_and_lyrics(prompt, llm_client)
            media_url, _audio_bytes = await asyncio.to_thread(self.process_prompt_sync, tags, lyrics)
        except Exception as e:
            logger.exception("ComfyUI generation failed: %s", e)
            return

        if not media_url:
            logger.error("No media URL returned from ComfyUI.")
            return

        # 2) play it
        try:
            ha = self._HA()
            ha.play_media(target_player, media_url, mimetype="music")
        except Exception as e:
            logger.exception("Failed to play media on %s: %s", target_player, e)
            return

    # ---------------------------------------
    # Helpers for HA
    # ---------------------------------------
    def _pick_target_player(self) -> str | None:
        """
        Resolve media_player strictly from plugin settings.
        """
        sett = self._settings()
        default_mp = (sett.get("HA_DEFAULT_MEDIA_PLAYER") or "").strip()
        return default_mp or None

    class _HA:
        def __init__(self):
            s = redis_client.hgetall("plugin_settings: Home Assistant") or redis_client.hgetall("plugin_settings:Home Assistant")
            # Handle both key variants just in case
            if not s:
                s = {}
            self.base = (s.get("HA_BASE_URL") or "http://homeassistant.local:8123").rstrip("/")
            token = s.get("HA_TOKEN")
            if not token:
                raise ValueError("HA_TOKEN missing in plugin settings.")
            self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        def _req(self, method: str, path: str, json=None, timeout=120):
            r = requests.request(method, f"{self.base}{path}", headers=self.headers, json=json, timeout=timeout)
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
            try:
                return r.json()
            except Exception:
                return r.text

        def play_media(self, entity_id: str, url: str, mimetype="music"):
            data = {
                "entity_id": entity_id,
                "media_content_id": url,
                "media_content_type": mimetype
            }
            return self._req("POST", "/api/services/media_player/play_media", json=data, timeout=30)


plugin = ComfyUIAudioAcePlugin()