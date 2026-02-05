# plugins/lowfi_video.py
import os
import json
import uuid
import asyncio
import requests
import subprocess
import websocket
import random
import copy
import secrets
import time
from math import ceil
from plugin_base import ToolPlugin
from helpers import redis_client, run_comfy_prompt

SETTINGS_CATEGORY = "Lofi Video"

def _build_media_metadata(binary: bytes, *, media_type: str, name: str, mimetype: str) -> dict:
    if not isinstance(binary, (bytes, bytearray)):
        raise TypeError("binary must be bytes")
    return {
        "type": media_type,
        "name": name,
        "mimetype": mimetype,
        "size": len(binary),
        "bytes": bytes(binary),
    }

CLIENT_ID = str(uuid.uuid4())


class _ComfyUIImageHelper:
    settings_category = SETTINGS_CATEGORY

    @staticmethod
    def get_base_http():
        settings = redis_client.hgetall(f"plugin_settings:{_ComfyUIImageHelper.settings_category}")
        raw = settings.get("COMFYUI_URL", b"")
        url = raw.decode("utf-8").strip() if isinstance(raw, (bytes, bytearray)) else (raw or "").strip()
        if not url:
            url = "http://localhost:8188"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url
        return url.rstrip("/")

    @staticmethod
    def get_base_ws(base_http: str) -> str:
        scheme = "wss" if base_http.startswith("https://") else "ws"
        return base_http.replace("http", scheme, 1)

    @staticmethod
    def get_workflow_template():
        settings = redis_client.hgetall(f"plugin_settings:{_ComfyUIImageHelper.settings_category}")
        workflow_raw = settings.get("COMFYUI_WORKFLOW", b"")
        workflow_str = workflow_raw.decode("utf-8").strip() if isinstance(workflow_raw, (bytes, bytearray)) else (workflow_raw or "").strip()
        if not workflow_str:
            raise Exception("No workflow template set in COMFYUI_WORKFLOW. Please provide a valid JSON template.")
        return json.loads(workflow_str)

    @staticmethod
    def get_history(base_http: str, prompt_id: str):
        r = requests.get(f"{base_http}/history/{prompt_id}", timeout=30)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def get_image_bytes(base_http: str, filename: str, subfolder: str, folder_type: str) -> bytes:
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        r = requests.get(f"{base_http}/view", params=params, timeout=60)
        r.raise_for_status()
        return r.content

    @staticmethod
    def _insert_prompts(workflow: dict, user_prompt: str, negative_prompt: str = ""):
        positive_found = False
        negative_found = False
        encode_nodes = []

        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            if node.get("class_type") == "CLIPTextEncode":
                encode_nodes.append(node)
                title = (node.get("_meta", {}).get("title", "") or "").lower()
                if "positive" in title:
                    node.setdefault("inputs", {})
                    node["inputs"]["text"] = user_prompt
                    node["widgets_values"] = [user_prompt]
                    positive_found = True
                elif "negative" in title:
                    node.setdefault("inputs", {})
                    node["inputs"]["text"] = negative_prompt
                    node["widgets_values"] = [negative_prompt]
                    negative_found = True

        if not positive_found and encode_nodes:
            encode_nodes[0].setdefault("inputs", {})
            encode_nodes[0]["inputs"]["text"] = user_prompt
            encode_nodes[0]["widgets_values"] = [user_prompt]
        if not negative_found and len(encode_nodes) > 1:
            encode_nodes[1].setdefault("inputs", {})
            encode_nodes[1]["inputs"]["text"] = negative_prompt
            encode_nodes[1]["widgets_values"] = [negative_prompt]

    @staticmethod
    def _fetch_first_image(base_http: str, prompt_id: str, tries: int = 6, delay: float = 0.5) -> bytes:
        for _ in range(max(1, tries)):
            hist_all = _ComfyUIImageHelper.get_history(base_http, prompt_id)
            history = hist_all.get(prompt_id, {}) if isinstance(hist_all, dict) else {}
            outputs = history.get("outputs", {}) if isinstance(history, dict) else {}

            for node_output in outputs.values():
                images = node_output.get("images") or []
                for img_meta in images:
                    filename = img_meta.get("filename")
                    if filename:
                        subfolder = img_meta.get("subfolder", "")
                        folder_type = img_meta.get("type", "output")
                        return _ComfyUIImageHelper.get_image_bytes(base_http, filename, subfolder, folder_type)

            time.sleep(delay)

        raise Exception("No images returned from ComfyUI (timeout).")

    @staticmethod
    def process_prompt(user_prompt: str, width: int = None, height: int = None) -> bytes:
        base_http = _ComfyUIImageHelper.get_base_http()
        base_ws = _ComfyUIImageHelper.get_base_ws(base_http)

        workflow = copy.deepcopy(_ComfyUIImageHelper.get_workflow_template())
        _ComfyUIImageHelper._insert_prompts(workflow, user_prompt, "")

        random_seed = secrets.randbits(63)
        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            inputs = node.get("inputs", {})
            if not isinstance(inputs, dict):
                continue
            if "seed" in inputs:
                inputs["seed"] = int(random_seed)
            if "noise_seed" in inputs:
                inputs["noise_seed"] = int(random_seed)

        res_map = {
            "144p": (256, 144),
            "240p": (426, 240),
            "360p": (480, 360),
            "480p": (640, 480),
            "720p": (1280, 720),
            "1080p": (1920, 1080),
        }
        settings = redis_client.hgetall(f"plugin_settings:{_ComfyUIImageHelper.settings_category}")
        raw_res = settings.get("IMAGE_RESOLUTION", b"720p")
        resolution = raw_res.decode("utf-8") if isinstance(raw_res, (bytes, bytearray)) else (raw_res or "720p")
        default_w, default_h = res_map.get(resolution, (1280, 720))

        w = int(width) if width else default_w
        h = int(height) if height else default_h

        for node in workflow.values():
            if isinstance(node, dict) and node.get("class_type") in ("EmptyLatentImage", "EmptySD3LatentImage", "ModelSamplingFlux"):
                node.setdefault("inputs", {})
                node["inputs"]["width"] = w
                node["inputs"]["height"] = h

        prompt_id, _ = run_comfy_prompt(base_http, base_ws, workflow)
        return _ComfyUIImageHelper._fetch_first_image(base_http, prompt_id)


class _ComfyUIImageVideoHelper:
    settings_category = SETTINGS_CATEGORY

    @staticmethod
    def get_base_http() -> str:
        settings = redis_client.hgetall(f"plugin_settings:{_ComfyUIImageVideoHelper.settings_category}")
        url_raw = settings.get("COMFYUI_VIDEO_URL", b"")
        url = url_raw.decode("utf-8").strip() if isinstance(url_raw, (bytes, bytearray)) else (url_raw or "").strip()
        if not url:
            url = "http://localhost:8188"
        if not url.startswith("http://") and not url.startswith("https://"):
            url = "http://" + url
        return url.rstrip("/")

    @staticmethod
    def get_base_ws(base_http: str) -> str:
        scheme = "wss" if base_http.startswith("https://") else "ws"
        return base_http.replace("http", scheme, 1)

    @staticmethod
    def get_workflow_template() -> dict:
        settings = redis_client.hgetall(f"plugin_settings:{_ComfyUIImageVideoHelper.settings_category}")
        raw = settings.get("COMFYUI_VIDEO_WORKFLOW", b"")
        workflow_str = raw.decode("utf-8").strip() if isinstance(raw, (bytes, bytearray)) else (raw or "").strip()
        if not workflow_str:
            raise RuntimeError("No workflow found in Redis. Please upload a valid JSON workflow.")
        return json.loads(workflow_str)

    @staticmethod
    def upload_image(base_http: str, image_bytes: bytes, filename: str) -> str:
        resp = requests.post(
            f"{base_http}/upload/image",
            files={"image": (filename, image_bytes)},
            data={"overwrite": "true"},
            timeout=60
        )
        resp.raise_for_status()
        result = resp.json()
        name = result.get("name") or result.get("filename")
        sub = result.get("subfolder", "")
        return f"{sub}/{name}" if sub else name

    @staticmethod
    def get_history(base_http: str, prompt_id: str) -> dict:
        r = requests.get(f"{base_http}/history/{prompt_id}", timeout=30)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def fetch_asset(base_http: str, filename: str, subfolder: str, folder_type: str) -> bytes:
        params = {"filename": filename, "subfolder": subfolder, "type": folder_type}
        r = requests.get(f"{base_http}/view", params=params, timeout=60)
        r.raise_for_status()
        return r.content

    @staticmethod
    def get_fps_from_workflow(wf: dict, default: int = 16) -> int:
        try:
            for node in wf.values():
                if node.get("class_type") == "CreateVideo":
                    return int(node["inputs"].get("fps", default))
        except Exception:
            pass
        return default

    @staticmethod
    def _apply_overrides(workflow: dict, prompt_text: str, uploaded_image_path: str, width: int, height: int, frames: int):
        patched_first_prompt = False
        for node in workflow.values():
            if not isinstance(node, dict):
                continue

            if node.get("class_type") == "LoadImage":
                node.setdefault("inputs", {})
                node["inputs"]["image"] = uploaded_image_path

            if node.get("class_type") == "CLIPTextEncode" and (
                "Positive" in node.get("_meta", {}).get("title", "") or
                "Prompt" in node.get("_meta", {}).get("title", "")
            ):
                if not patched_first_prompt:
                    node.setdefault("inputs", {})
                    node["inputs"]["text"] = prompt_text
                    patched_first_prompt = True

            if node.get("class_type") in {
                "WanImageToVideo",
                "WanVaceToVideo",
                "CosmosPredict2ImageToVideoLatent",
                "Wan22ImageToVideoLatent",
            }:
                node.setdefault("inputs", {})
                node["inputs"]["width"] = width
                node["inputs"]["height"] = height
                node["inputs"]["length"] = frames

    @staticmethod
    def process_prompt(prompt: str, image_bytes: bytes, filename: str, width: int = None, height: int = None, length: int = None):
        base_http = _ComfyUIImageVideoHelper.get_base_http()
        base_ws = _ComfyUIImageVideoHelper.get_base_ws(base_http)

        uploaded = _ComfyUIImageVideoHelper.upload_image(base_http, image_bytes, filename)
        wf = copy.deepcopy(_ComfyUIImageVideoHelper.get_workflow_template())

        settings = redis_client.hgetall(f"plugin_settings:{_ComfyUIImageVideoHelper.settings_category}")
        res_map = {
            "144p": (256, 144), "240p": (426, 240), "360p": (480, 360),
            "480p": (640, 480), "720p": (1280, 720), "1080p": (1920, 1080)
        }
        raw_res = settings.get("IMAGE_RESOLUTION", b"480p")
        resolution = raw_res.decode("utf-8") if isinstance(raw_res, (bytes, bytearray)) else (raw_res or "480p")
        default_w, default_h = res_map.get(resolution, (640, 480))

        fps = _ComfyUIImageVideoHelper.get_fps_from_workflow(wf, default=16)

        raw_length = settings.get("LENGTH", b"1")
        try:
            default_seconds = int(raw_length.decode() if isinstance(raw_length, (bytes, bytearray)) else raw_length)
        except ValueError:
            default_seconds = 1
        default_frames = max(1, default_seconds * fps)

        w = width or default_w
        h = height or default_h
        frames = length or default_frames

        random_seed = secrets.randbits(63)
        for node in wf.values():
            if not isinstance(node, dict):
                continue
            if node.get("class_type") in {"KSampler", "KSamplerAdvanced"}:
                inputs = node.get("inputs", {})
                if inputs.get("add_noise") == "enable" and "noise_seed" in inputs:
                    inputs["noise_seed"] = int(random_seed)
                if "seed" in inputs:
                    inputs["seed"] = int(random_seed)

        _ComfyUIImageVideoHelper._apply_overrides(wf, prompt, uploaded, w, h, frames)

        prompt_id, _ = run_comfy_prompt(base_http, base_ws, wf)
        hist = _ComfyUIImageVideoHelper.get_history(base_http, prompt_id).get(prompt_id, {})
        outputs = hist.get("outputs", {}) if isinstance(hist, dict) else {}

        for node in outputs.values():
            if "images" in node:
                img = node["images"][0]
                content = _ComfyUIImageVideoHelper.fetch_asset(base_http, img["filename"], img.get("subfolder", ""), img.get("type", "output"))
                ext = os.path.splitext(img["filename"])[-1].lstrip(".") or "webp"
                return content, ext

        for node in outputs.values():
            if "videos" in node:
                vid = node["videos"][0]
                content = _ComfyUIImageVideoHelper.fetch_asset(base_http, vid["filename"], vid.get("subfolder", ""), vid.get("type", "output"))
                ext = os.path.splitext(vid["filename"])[-1].lstrip(".") or "mp4"
                return content, ext

        for node in wf.values():
            if node.get("class_type") == "SaveVideo":
                prefix = node["inputs"].get("filename_prefix", "ComfyUI")
                if "/" in prefix:
                    subfolder, base = prefix.split("/", 1)
                else:
                    subfolder, base = "", prefix
                guessed_filename = f"{base}.mp4"
                content = _ComfyUIImageVideoHelper.fetch_asset(base_http, guessed_filename, subfolder, "output")
                return content, "mp4"

        raise RuntimeError("No output found in ComfyUI history or disk.")


class LowfiVideoPlugin(ToolPlugin):
    name = "lowfi_video"
    plugin_name = "Lofi Video"
    version = "1.0.1"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "lowfi_video",\n'
        '  "arguments": {\n'
        '    "prompt": "<scene prompt for the visual>"\n'
        "  }\n"
        "}\n"
    )
    description = "Generates lofi audio via AceStep and loops a cozy animation to full length (MP4)."
    plugin_dec = "Create a cozy lofi video by generating music and looping a matching animation."
    pretty_name = "Your Lofi Video"
    waiting_prompt_template = "Generate a fun, cozy message telling the user you're creating their lofi music video right now. Only output that message."
    settings_category = SETTINGS_CATEGORY
    platforms = ["webui"]

    required_settings = {
        "COMFYUI_URL": {
            "label": "ComfyUI URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "The base URL for the ComfyUI API (do not include endpoint paths)."
        },
        "COMFYUI_WORKFLOW": {
            "label": "Image Workflow Template (JSON)",
            "type": "file",
            "default": "",
            "description": "Upload your ComfyUI JSON workflow template for image generation."
        },
        "IMAGE_RESOLUTION": {
            "label": "Image Resolution",
            "type": "select",
            "default": "720p",
            "options": ["144p", "240p", "360p", "480p", "720p", "1080p"],
            "description": "Resolution for the base image used in the animation."
        },
        "COMFYUI_VIDEO_URL": {
            "label": "ComfyUI Video URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "ComfyUI endpoint for image-to-video workflows."
        },
        "COMFYUI_VIDEO_WORKFLOW": {
            "label": "Video Workflow Template (JSON)",
            "type": "file",
            "default": "",
            "description": "Upload your ComfyUI JSON workflow template for image animation."
        },
        "LENGTH": {
            "label": "Animation Length (seconds)",
            "type": "number",
            "default": 10,
            "description": "Length in seconds for the generated loop clip."
        },
        "COMFYUI_AUDIO_URL": {
            "label": "ComfyUI Audio URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "ComfyUI endpoint for generating audio via AceStep workflow."
        },
        "LOFI_RESOLUTION": {
            "label": "Visual Resolution",
            "type": "select",
            "default": "720p",
            "options": ["144p", "240p", "360p", "480p", "720p", "1080p"],
            "description": "Output resolution for the generated video loop."
        },
        "PINGPONG_LOOP": {
            "label": "Enable Ping-pong Loop",
            "type": "checkbox",
            "default": "false",
            "description": "Play each loop forward then backward for a seamless boomerang effect."
        },
    }

    # ------------------------- Helpers -------------------------

    def _to_ws_url(self, base_http: str) -> str:
        base_http = (base_http or "http://localhost:8188").rstrip("/")
        if base_http.startswith("https://"):
            return "wss://" + base_http[len("https://"):]
        if base_http.startswith("http://"):
            return "ws://" + base_http[len("http://"):]
        return "ws://" + base_http

    def _ffmpeg(self, args: list[str]):
        subprocess.run(args, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def _safe_unlink(self, path: str):
        try:
            os.unlink(path)
        except Exception:
            pass

    def _ffprobe_duration(self, path: str) -> float:
        """Return duration in seconds via ffprobe."""
        args = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", path
        ]
        out = subprocess.run(args, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            dur = float(out.stdout.strip())
            return max(0.0, dur)
        except Exception:
            return 0.0

    def _build_ace_audio_workflow(self, seconds: int):
        ckpt = "ace_step_v1_3.5b.safetensors"
        return {
            "14": {
                "inputs": {
                    "tags": "lowfi",
                    "lyrics": "[instrumental]\n[break down]\n[drum fill]\n[chopped samples]\n",
                    "lyrics_strength": 0.9900000000000002,
                    "clip": ["40", 1]
                },
                "class_type": "TextEncodeAceStepAudio",
                "_meta": {"title": "TextEncodeAceStepAudio"}
            },
            "17": {
                "inputs": {"seconds": seconds, "batch_size": 1},
                "class_type": "EmptyAceStepLatentAudio",
                "_meta": {"title": "EmptyAceStepLatentAudio"}
            },
            "18": {
                "inputs": {"samples": ["52", 0], "vae": ["40", 2]},
                "class_type": "VAEDecodeAudio",
                "_meta": {"title": "VAEDecodeAudio"}
            },
            "40": {
                "inputs": {"ckpt_name": ckpt},
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
                    "seed": 739582629284326,
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

    async def _refine_prompt_for_lofi(self, raw: str, llm_client) -> str:
        sys = (
            "Rewrite the user's request into a single vivid SCENE prompt for generating a still image for a lofi video loop. "
            "Constraints: <= 35 words, exactly one sentence, no text overlays or typography. "
            "Describe a static scene with a cozy/soft mood, clear subject, and distinct foreground/background. "
            "Include time of day, lighting, and color palette. "
            "If people are described, PRESERVE details about appearance, clothing, accessories, and colors. "
            "Keep any named characters and specific setting details from the user. "
            "Avoid mentioning motion, camera moves, or transitions."
        )
        usr = raw
        resp = await llm_client.chat([
            {"role": "system", "content": sys},
            {"role": "user", "content": usr}
        ])
        scene = (resp.get("message", {}) or {}).get("content", "").strip()
        return scene or raw

    async def _refine_prompt_for_lofi_animation(self, scene_prompt: str, llm_client) -> str:
        sys = (
            "Rewrite the scene into ONE sentence (<= 25 words) describing loopable lofi animation. "
            "Prefer environmental micro-motion over camera moves. "
            "If present, animate: ocean/water (gentle waves, ripples), stars/sky (twinkle, drift), hair/fabric (soft wind sway), "
            "foliage (subtle rustle), smoke/steam/fog (slow waft), rain/snow (drift), lights/neon/candle (soft flicker), "
            "reflections/shadows (shimmer/slow shift), clouds (drift). "
            "Camera motion optional and minimal. "
            "No fast actions, cuts, morphs, lip-sync, or text. "
            "Hands/fingers remain still. "
            "Must loop seamlessly. Output only the sentence."
        )
        usr = f'Scene: "{scene_prompt}"\nAnimation directive:'
        try:
            resp = await llm_client.chat([
                {"role": "system", "content": sys},
                {"role": "user", "content": usr}
            ])
            anim = (resp.get("message", {}) or {}).get("content", "").strip()
            return anim or "gentle parallax, slow zoom and sway, no hand movement, cozy ambience, seamless loop"
        except Exception:
            return "gentle parallax, slow zoom and sway, no hand movement, cozy ambience, seamless loop"

    # -------------------- Audio via ComfyUI (async-safe) ---------------------

    async def _generate_audio(self, settings, audio_minutes: int) -> str:
        seconds = max(60, audio_minutes * 60)
        wf = self._build_ace_audio_workflow(seconds=seconds)

        # Randomize any 'seed' fields so each run is unique
        for node in wf.values():
            if isinstance(node, dict):
                inputs = node.get("inputs")
                if isinstance(inputs, dict) and "seed" in inputs:
                    inputs["seed"] = random.randint(0, 2**63 - 1)

        base = settings.get("COMFYUI_AUDIO_URL", b"http://localhost:8188")
        if isinstance(base, (bytes, bytearray)):
            base = base.decode("utf-8")
        base = base or "http://localhost:8188"

        server_http = base.rstrip("/")
        ws_url = self._to_ws_url(server_http) + f"/ws?clientId={CLIENT_ID}"

        def _post_prompt():
            r = requests.post(f"{server_http}/prompt", json={"prompt": wf, "client_id": CLIENT_ID}, timeout=60)
            r.raise_for_status()
            return r.json()["prompt_id"]

        prompt_id = await asyncio.to_thread(_post_prompt)

        def _wait_ws_done():
            ws = websocket.WebSocket()
            ws.connect(ws_url)
            try:
                while True:
                    msg = ws.recv()
                    if isinstance(msg, str):
                        data = json.loads(msg)
                        if data.get("type") == "executing":
                            d = data.get("data", {})
                            if d.get("prompt_id") == prompt_id and d.get("node") is None:
                                return
            finally:
                ws.close()

        await asyncio.to_thread(_wait_ws_done)

        def _fetch_mp3():
            hist = requests.get(f"{server_http}/history/{prompt_id}", timeout=60).json()[prompt_id]
            for node in hist.get("outputs", {}).values():
                if "audio" in node:
                    for a in node["audio"]:
                        filename = a.get("filename")
                        subfolder = a.get("subfolder", "")
                        ftype = a.get("type", "output")
                        if filename and filename.lower().endswith(".mp3"):
                            params = {"filename": filename, "subfolder": subfolder, "type": ftype}
                            return requests.get(f"{server_http}/view", params=params, timeout=60).content
            return None

        audio_bytes = await asyncio.to_thread(_fetch_mp3)
        if not audio_bytes:
            raise RuntimeError("No MP3 found in ComfyUI history for lofi audio.")

        audio_path = f"/tmp/{uuid.uuid4().hex[:8]}_lofi.mp3"
        await asyncio.to_thread(lambda: open(audio_path, "wb").write(audio_bytes))
        return audio_path

    # -------------------- Visual loop (still -> i2v) ------------------------

    async def _apply_pingpong(self, input_video_mp4: str) -> str:
        """Create a forward+reverse (ping-pong) version of the clip."""
        out_path = input_video_mp4.rsplit(".", 1)[0] + "_pp.mp4"
        cmd = [
            "ffmpeg", "-y",
            "-i", input_video_mp4,
            "-filter_complex", "[0:v]reverse[r];[0:v][r]concat=n=2:v=1:a=0[v]",
            "-map", "[v]", "-an",
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-pix_fmt", "yuv420p",
            out_path
        ]
        await asyncio.to_thread(self._ffmpeg, cmd)
        return out_path

    async def _generate_loop_clip(self, settings, prompt_text: str, loop_seconds: int, llm_client) -> str:
        res_map = {
            "144p": (256, 144), "240p": (426, 240), "360p": (480, 360),
            "480p": (640, 480), "720p": (1280, 720), "1080p": (1920, 1080)
        }
        raw_res = settings.get("LOFI_RESOLUTION", "720p")
        resolution = raw_res.decode("utf-8") if isinstance(raw_res, (bytes, bytearray)) else raw_res
        w, h = res_map.get(resolution, (1280, 720))

        # FPS from the i2v workflow (fallback 16)
        fps = 16
        try:
            wf = _ComfyUIImageVideoHelper.get_workflow_template()
            fps = _ComfyUIImageVideoHelper.get_fps_from_workflow(wf, default=16)
        except Exception:
            pass

        # Generate still, then animate
        image_bytes = await asyncio.to_thread(_ComfyUIImageHelper.process_prompt, prompt_text, w, h)

        frame_count = max(1, int(loop_seconds * fps))
        anim_desc = await self._refine_prompt_for_lofi_animation(prompt_text, llm_client)
        upload_filename = f"lofi_still_{uuid.uuid4().hex[:6]}.png"

        anim_bytes, ext = await asyncio.to_thread(
            _ComfyUIImageVideoHelper.process_prompt,
            anim_desc, image_bytes, upload_filename, w, h, frame_count
        )
        if not anim_bytes:
            raise RuntimeError("Animation returned empty bytes.")

        ext = (ext or "webp").lower()
        tmp = f"/tmp/{uuid.uuid4().hex[:8]}_loop.{ext}"
        await asyncio.to_thread(lambda: open(tmp, "wb").write(anim_bytes))

        # --- Optional ping-pong loop ---
        raw_pp = settings.get("PINGPONG_LOOP", b"false")
        pingpong_enabled = (
            raw_pp.decode("utf-8", "ignore").lower() == "true"
            if isinstance(raw_pp, (bytes, bytearray)) else str(raw_pp).lower() == "true"
        )

        if pingpong_enabled:
            # Ensure MP4 first
            mp4_clip = await self._ensure_mp4_async(tmp)
            if mp4_clip != tmp:
                self._safe_unlink(tmp)
            # Apply forward+reverse and return the ping-pong path
            pp_path = await self._apply_pingpong(mp4_clip)
            # cleanup intermediate mp4 if different from pp output
            if mp4_clip != pp_path:
                self._safe_unlink(mp4_clip)
            return pp_path

        return tmp

    async def _ensure_mp4_async(self, clip_path: str) -> str:
        name, ext = os.path.splitext(clip_path.lower())
        if ext in (".mp4", ".mov", ".m4v"):
            return clip_path
        out_path = clip_path.rsplit(".", 1)[0] + ".mp4"
        cmd = [
            "ffmpeg", "-y",
            "-i", clip_path,
            "-movflags", "+faststart",
            "-pix_fmt", "yuv420p",
            "-vcodec", "libx264",
            "-preset", "veryfast",
            "-crf", "23",
            out_path
        ]
        await asyncio.to_thread(self._ffmpeg, cmd)
        return out_path

    def _build_crossfaded_audio(self, audio_path: str, video_seconds: int, fade_seconds: int = 2) -> str:
        """
        Build a crossfaded audio long enough to cover video_seconds by repeating the input.
        We duplicate the input N times and chain 'acrossfade' filters.
        """
        dur = self._ffprobe_duration(audio_path)
        if dur <= 0.1:
            return audio_path

        repeats = max(2, ceil(video_seconds / dur) + 1)
        temp_out = f"/tmp/{uuid.uuid4().hex[:8]}_audio_crossfade.mp3"

        inputs = []
        for _ in range(repeats):
            inputs += ["-i", audio_path]

        chains = []
        last_label = "a0"
        for i in range(1, repeats):
            in_label = f"a{i}"
            out_label = f"ax{i}"
            chains.append(f"[{last_label}][{in_label}]acrossfade=d={fade_seconds}:c1=tri:c2=tri[{out_label}]")
            last_label = out_label

        filter_complex = ";".join(
            [f"[{idx}:a]anull[a{idx}]" for idx in range(repeats)] +
            chains +
            [f"[{last_label}]atrim=duration={video_seconds},asetpts=N/SR/TB[aout]"]
        )

        cmd = [
            "ffmpeg", "-y",
            *inputs,
            "-filter_complex", filter_complex,
            "-map", "[aout]",
            "-c:a", "libmp3lame", "-b:a", "160k",
            temp_out
        ]
        self._ffmpeg(cmd)
        return temp_out

    async def _mux(self, video_loop_path: str, audio_path: str, video_seconds: int) -> str:
        base = video_loop_path.rsplit(".", 1)[0]
        final_path = f"{base}_lofi_final.mp4"

        loop_mp4 = await self._ensure_mp4_async(video_loop_path)
        cross_audio = self._build_crossfaded_audio(audio_path, video_seconds, fade_seconds=2)

        cmd = [
            "ffmpeg", "-y",
            "-stream_loop", "-1", "-i", loop_mp4,
            "-i", cross_audio,
            "-shortest", "-t", str(video_seconds),
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
            "-c:a", "aac", "-b:a", "160k",
            "-movflags", "+faststart",
            "-pix_fmt", "yuv420p",
            final_path
        ]
        await asyncio.to_thread(self._ffmpeg, cmd)
        self._safe_unlink(cross_audio)
        return final_path

    # ----------------------- Platform handlers -----------------------

    async def handle_webui(self, args, llm_client):
        if "prompt" not in args:
            return ["No prompt given."]

        async def _generate():
            # Load settings
            settings = redis_client.hgetall(f"plugin_settings:{self.settings_category}")
            prompt_text = args["prompt"]

            # Defaults
            audio_minutes = 2       # Always 2 minutes audio
            video_minutes = 20      # Always 20 minutes total video
            loop_seconds  = 15      # Always 15 second loop clip

            # Audio: cap at 3 min
            audio_minutes = min(audio_minutes, 3)

            # Loop clip: 1–60 sec
            loop_seconds = max(1, min(loop_seconds, 60))

            # Ensure video >= audio length
            video_minutes = max(video_minutes, audio_minutes)

            # Round up so audio finishes cleanly
            if audio_minutes > 0 and (video_minutes % audio_minutes) != 0:
                video_minutes = ((video_minutes // audio_minutes) + 1) * audio_minutes

            # Cap at 20 minutes, but allow +2 min overage
            if video_minutes > 22:
                video_minutes = 20

            # Refine prompt
            prompt_text_refined = await self._refine_prompt_for_lofi(prompt_text, llm_client)

            audio_path = None
            loop_clip_path = None
            final_path = None  # track for cleanup

            try:
                # 1) Audio
                audio_path = await self._generate_audio(settings, audio_minutes)

                # 2) Visual loop
                loop_clip_path = await self._generate_loop_clip(settings, prompt_text_refined, loop_seconds, llm_client)

                # 3) Mux to final MP4 (with crossfaded audio)
                final_path = await self._mux(loop_clip_path, audio_path, video_seconds=video_minutes * 60)

                # 4) Read & return (raw bytes)
                final_bytes = await asyncio.to_thread(lambda: open(final_path, "rb").read())

                # 5) Follow-up message
                msg = await llm_client.chat([
                    {"role": "system", "content": f"User just got a lofi video for: '{prompt_text_refined}'"},
                    {"role": "user", "content": "Respond with a short, chill message celebrating the lofi video. Do not include any lead-in phrases or instructions — just the message."}
                ])
                followup_text = (msg.get("message", {}).get("content") or "").strip() or "☕ Here’s your chill lofi video!"

                return [
                    _build_media_metadata(
                        final_bytes,
                        media_type="video",
                        name="lofi_video.mp4",
                        mimetype="video/mp4",
                    ),
                    followup_text
                ]
            except Exception as e:
                return [f"❌ Lofi video failed: {type(e).__name__}: {e}"]
            finally:
                # Clean up temp files and the final MP4 (we already inlined it)
                for p in (audio_path, loop_clip_path, final_path):
                    if p:
                        self._safe_unlink(p)

        # Match your working plugin’s event loop handling
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                return await _generate()
        except RuntimeError:
            pass

        return asyncio.run(_generate())

    async def handle_discord(self, message, args, llm_client):
        return "❌ This plugin is only available in the WebUI due to file size limitations."

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        await bot.privmsg(channel, f"{user}: ❌ This plugin is only available in the WebUI.")


plugin = LowfiVideoPlugin()
