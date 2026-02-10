# plugins/comfyui_image_plugin.py
import json
import time
import asyncio
import secrets
import copy
import requests
import imghdr
from typing import Optional

from plugin_base import ToolPlugin
from helpers import redis_client, run_comfy_prompt


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


class ComfyUIImagePlugin(ToolPlugin):
    name = "comfyui_image_plugin"
    plugin_name = "ComfyUI Image"
    version = "1.0.3"
    min_tater_version = "50"
    usage = '{"function":"comfyui_image_plugin","arguments":{"prompt":"<Text prompt for the image. If omitted, generate a creative prompt based on the user request>","negative_prompt":"<Optional negative prompt>","width":1024,"height":1024}}'
    description = "Draws a picture from a text prompt using your ComfyUI workflow."
    plugin_dec = "Generate a still image from a text prompt using your ComfyUI workflow."
    pretty_name = "Your Image"
    settings_category = "ComfyUI Image"
    required_settings = {
        "COMFYUI_URL": {
            "label": "ComfyUI URL",
            "type": "string",
            "default": "http://localhost:8188",
            "description": "The base URL for the ComfyUI API (do not include endpoint paths).",
        },
        "IMAGE_RESOLUTION": {
            "label": "Image Resolution",
            "type": "select",
            "default": "720p",
            "options": ["144p", "240p", "360p", "480p", "720p", "1080p"],
            "description": "Default resolution for generated images (used when width/height are not provided).",
        },
        "COMFYUI_WORKFLOW": {
            "label": "Workflow Template (JSON)",
            "type": "file",
            "default": "",
            "description": "Upload your JSON workflow template file. This field is required.",
        },
    }
    waiting_prompt_template = (
        "Write a fun, casual message saying you’re creating their masterpiece now! Only output that message."
    )
    platforms = ["discord", "webui", "matrix", "telegram"]  # matrix supported

    # ---------------------------
    # Server URL helpers
    # ---------------------------
    @staticmethod
    def get_base_http():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImagePlugin.settings_category}") or {}
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

    # ---------------------------
    # Template / settings helpers
    # ---------------------------
    @staticmethod
    def _resolve_prompt(args: Optional[dict]) -> str:
        args = args or {}
        raw = None
        for key in ("prompt", "request", "message", "query", "text"):
            val = args.get(key)
            if isinstance(val, str) and val.strip():
                raw = val.strip()
                break

        if not raw:
            return "a serene sky with dramatic clouds at golden hour, wide cinematic view, rich color, high detail"

        cleaned = " ".join(raw.split())
        lower = cleaned.lower()
        short_prompt = len(cleaned.split()) < 5 or len(cleaned) < 24
        has_style = any(tag in lower for tag in ("cinematic", "photoreal", "high detail", "ultra", "4k"))
        if short_prompt and not has_style:
            if "sky" in lower:
                return f"{cleaned}, expansive sky, dramatic clouds, warm light, cinematic, high detail"
            return f"{cleaned}, cinematic wide shot, soft lighting, atmospheric, high detail"
        return cleaned

    @staticmethod
    def get_workflow_template():
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImagePlugin.settings_category}") or {}
        workflow_raw = settings.get("COMFYUI_WORKFLOW", b"")
        workflow_str = (
            workflow_raw.decode("utf-8").strip()
            if isinstance(workflow_raw, (bytes, bytearray))
            else (workflow_raw or "").strip()
        )
        if not workflow_str:
            raise Exception("No workflow template set in COMFYUI_WORKFLOW. Please provide a valid JSON template.")
        return json.loads(workflow_str)

    @staticmethod
    def _get_resolution_from_settings() -> tuple[int, int]:
        res_map = {
            "144p": (256, 144),
            "240p": (426, 240),
            "360p": (480, 360),
            "480p": (640, 480),
            "720p": (1280, 720),
            "1080p": (1920, 1080),
        }
        settings = redis_client.hgetall(f"plugin_settings:{ComfyUIImagePlugin.settings_category}") or {}
        raw_res = settings.get("IMAGE_RESOLUTION", b"720p")
        resolution = (
            raw_res.decode("utf-8").strip()
            if isinstance(raw_res, (bytes, bytearray))
            else str(raw_res or "720p").strip()
        )
        return res_map.get(resolution, (1280, 720))

    @staticmethod
    def _safe_int(x, fallback: int) -> int:
        try:
            v = int(str(x).strip())
            return v
        except Exception:
            return fallback

    @staticmethod
    def _get_job_dimensions(args: dict | None) -> tuple[int, int]:
        """
        Priority:
        1) width/height args (if provided)
        2) IMAGE_RESOLUTION setting
        """
        args = args or {}
        w0, h0 = ComfyUIImagePlugin._get_resolution_from_settings()

        w = args.get("width", None)
        h = args.get("height", None)

        if w is None and h is None:
            return w0, h0

        w = ComfyUIImagePlugin._safe_int(w, w0)
        h = ComfyUIImagePlugin._safe_int(h, h0)

        # guardrails (Comfy can crash hard on silly dims)
        w = max(64, min(w, 4096))
        h = max(64, min(h, 4096))
        return w, h

    # ---------------------------
    # Comfy I/O helpers
    # ---------------------------
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

    # ---------------------------
    # Prompt injection (supports linked Primitive nodes)
    # ---------------------------
    @staticmethod
    def _node_title(node: dict) -> str:
        return ((node.get("_meta", {}) or {}).get("title", "") or "").strip().lower()

    @staticmethod
    def _set_primitive_string_nodes(workflow: dict, user_prompt: str, negative_prompt: str) -> tuple[bool, bool]:
        """
        Supports templates where CLIPTextEncode.text is a link to:
          - PrimitiveStringMultiline.inputs.value
          - PrimitiveString.inputs.value
        Uses _meta.title to distinguish Prompt vs Negative Prompt when possible.
        """
        pos_set = False
        neg_set = False

        for node in workflow.values():
            if not isinstance(node, dict):
                continue

            ctype = (node.get("class_type") or "").strip()
            if ctype not in ("PrimitiveStringMultiline", "PrimitiveString"):
                continue

            title = ComfyUIImagePlugin._node_title(node)
            node.setdefault("inputs", {})

            # If the template has one "Prompt" and optionally one "Negative" title, respect it.
            if "negative" in title:
                node["inputs"]["value"] = negative_prompt or ""
                neg_set = True
            elif "prompt" in title or title == "":
                node["inputs"]["value"] = user_prompt
                pos_set = True

        return pos_set, neg_set

    @staticmethod
    def _set_cliptextencode_when_direct(workflow: dict, user_prompt: str, negative_prompt: str) -> None:
        """
        Back-compat: if CLIPTextEncode.inputs.text is a literal string (not a link),
        set it directly. If it's already a link/list, do NOT overwrite it.
        """
        encode_nodes = []
        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            if node.get("class_type") != "CLIPTextEncode":
                continue
            encode_nodes.append(node)

        positive_found = False
        negative_found = False

        for node in encode_nodes:
            title = ComfyUIImagePlugin._node_title(node)
            node.setdefault("inputs", {})
            cur = node["inputs"].get("text")

            # If cur is a link (list like ["76",0]), don't overwrite.
            if isinstance(cur, list):
                continue

            if "negative" in title:
                node["inputs"]["text"] = negative_prompt or ""
                node["widgets_values"] = [negative_prompt or ""]
                negative_found = True
            elif "positive" in title:
                node["inputs"]["text"] = user_prompt
                node["widgets_values"] = [user_prompt]
                positive_found = True

        # If titles aren't present, fall back to first/second direct text nodes only
        if not positive_found:
            for n in encode_nodes:
                cur = (n.get("inputs") or {}).get("text")
                if isinstance(cur, list):
                    continue
                n.setdefault("inputs", {})
                n["inputs"]["text"] = user_prompt
                n["widgets_values"] = [user_prompt]
                positive_found = True
                break

        if not negative_found:
            # pick the next direct one (if any)
            picked = 0
            for n in encode_nodes:
                cur = (n.get("inputs") or {}).get("text")
                if isinstance(cur, list):
                    continue
                if picked == 0:
                    picked = 1
                    continue
                n.setdefault("inputs", {})
                n["inputs"]["text"] = negative_prompt or ""
                n["widgets_values"] = [negative_prompt or ""]
                negative_found = True
                break

    @staticmethod
    def _insert_prompts(workflow: dict, user_prompt: str, negative_prompt: str = ""):
        # 1) Prefer setting Primitive* nodes (keeps links intact)
        pos_set, neg_set = ComfyUIImagePlugin._set_primitive_string_nodes(workflow, user_prompt, negative_prompt)

        # 2) Back-compat: also set CLIPTextEncode only when it is direct text (not linked)
        # If primitives were not found, CLIPTextEncode direct injection will still work.
        ComfyUIImagePlugin._set_cliptextencode_when_direct(workflow, user_prompt, negative_prompt)

        # If we only found a single primitive node and it wasn't clearly titled,
        # that's still fine — the prompt will flow through via links.

    # ---------------------------
    # Width/Height injection (supports linked PrimitiveInt nodes)
    # ---------------------------
    @staticmethod
    def _set_dimensions(workflow: dict, w: int, h: int) -> None:
        """
        Supports:
        - PrimitiveInt nodes titled "Width"/"Height" (your Flux2 template)
        - direct width/height on common latent nodes (older templates)
        """
        # 1) Preferred: PrimitiveInt nodes with meta titles
        width_set = False
        height_set = False

        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            if node.get("class_type") != "PrimitiveInt":
                continue
            title = ComfyUIImagePlugin._node_title(node)
            node.setdefault("inputs", {})
            if "width" in title:
                node["inputs"]["value"] = int(w)
                width_set = True
            elif "height" in title:
                node["inputs"]["value"] = int(h)
                height_set = True

        # 2) Back-compat: set direct width/height on common latent nodes (only if fields are direct ints)
        for node in workflow.values():
            if not isinstance(node, dict):
                continue
            ctype = node.get("class_type")
            if ctype not in (
                "EmptyLatentImage",
                "EmptySD3LatentImage",
                "ModelSamplingFlux",
                "EmptyFlux2LatentImage",
            ):
                continue

            node.setdefault("inputs", {})
            # Only overwrite if these aren't links already
            if not isinstance(node["inputs"].get("width"), list):
                node["inputs"]["width"] = int(w)
            if not isinstance(node["inputs"].get("height"), list):
                node["inputs"]["height"] = int(h)

        # If titles weren't present, the back-compat path still helps older templates.

    # ---------------------------
    # Utils
    # ---------------------------
    @staticmethod
    def _infer_mime_and_ext(data: bytes):
        k = imghdr.what(None, h=data)
        mime = {
            "png": "image/png",
            "jpeg": "image/jpeg",
            "gif": "image/gif",
            "webp": "image/webp",
            "bmp": "image/bmp",
            "tiff": "image/tiff",
        }.get(k, "application/octet-stream")
        ext = {
            "image/png": "png",
            "image/jpeg": "jpg",
            "image/gif": "gif",
            "image/webp": "webp",
            "image/bmp": "bmp",
            "image/tiff": "tiff",
        }.get(mime, "bin")
        return mime, ext

    @staticmethod
    def _fetch_first_image(base_http: str, prompt_id: str, tries: int = 10, delay: float = 0.5) -> bytes:
        for _ in range(max(1, tries)):
            hist_all = ComfyUIImagePlugin.get_history(base_http, prompt_id)
            history = hist_all.get(prompt_id, {}) if isinstance(hist_all, dict) else {}
            outputs = history.get("outputs", {}) if isinstance(history, dict) else {}

            for node_output in outputs.values():
                images = node_output.get("images") or []
                for img_meta in images:
                    filename = img_meta.get("filename")
                    if filename:
                        subfolder = img_meta.get("subfolder", "")
                        folder_type = img_meta.get("type", "output")
                        return ComfyUIImagePlugin.get_image_bytes(base_http, filename, subfolder, folder_type)

            time.sleep(delay)

        raise Exception("No images returned from ComfyUI (timeout).")

    # ---------------------------
    # Core generation (sync)
    # ---------------------------
    @staticmethod
    def process_prompt(user_prompt: str, negative_prompt: str = "", *, args: Optional[dict] = None) -> bytes:
        base_http = ComfyUIImagePlugin.get_base_http()
        base_ws = ComfyUIImagePlugin.get_base_ws(base_http)

        workflow = copy.deepcopy(ComfyUIImagePlugin.get_workflow_template())

        # Inject prompts (supports linked Primitive nodes and legacy direct text)
        ComfyUIImagePlugin._insert_prompts(workflow, user_prompt, negative_prompt)

        # Randomize seed every run
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

        # Set dimensions (supports your Flux2 PrimitiveInt nodes)
        w, h = ComfyUIImagePlugin._get_job_dimensions(args)
        ComfyUIImagePlugin._set_dimensions(workflow, w, h)

        # Run ComfyUI job
        prompt_id, _ = run_comfy_prompt(base_http, base_ws, workflow)

        # Fetch first image
        return ComfyUIImagePlugin._fetch_first_image(base_http, prompt_id)

    # ---------------------------------------
    # Shared LLM follow-up helper
    # ---------------------------------------
    @staticmethod
    async def _celebrate(llm_client, user_prompt: str) -> str:
        if llm_client is None:
            return "Here’s your generated image!"
        safe_prompt = "".join(ch for ch in user_prompt[:300] if ch.isprintable()).strip()
        system_msg = f'The user has just been shown an AI-generated image based on the prompt: "{safe_prompt}".'
        final_response = await llm_client.chat(
            messages=[
                {"role": "system", "content": system_msg},
                {
                    "role": "user",
                    "content": "Respond with a short, fun message celebrating the image. Do not include any lead-in phrases or instructions — just the message.",
                },
            ]
        )
        return (final_response.get("message", {}) or {}).get("content", "").strip() or "Here's your generated image!"

    # ---------------------------------------
    # Discord
    # ---------------------------------------
    async def handle_discord(self, message, args, llm_client):
        user_prompt = ComfyUIImagePlugin._resolve_prompt(args)
        try:
            neg = (args or {}).get("negative_prompt", "") or ""

            async with message.channel.typing():
                image_bytes = await asyncio.to_thread(
                    ComfyUIImagePlugin.process_prompt,
                    user_prompt,
                    neg,
                    args=args,
                )

                mime, ext = self._infer_mime_and_ext(image_bytes)
                file_name = f"generated_comfyui.{ext}"

                message_text = await self._celebrate(llm_client, user_prompt)

                image_data = _build_media_metadata(
                    image_bytes,
                    media_type="image",
                    name=file_name,
                    mimetype=mime,
                )
                return [image_data, message_text]
        except Exception as e:
            return f"Failed to queue prompt: {type(e).__name__}: {e}"

    # ---------------------------------------
    # WebUI
    # ---------------------------------------
    async def handle_webui(self, args, llm_client):
        user_prompt = ComfyUIImagePlugin._resolve_prompt(args)
        try:
            neg = (args or {}).get("negative_prompt", "") or ""

            image_bytes = await asyncio.to_thread(
                ComfyUIImagePlugin.process_prompt,
                user_prompt,
                neg,
                args=args,
            )

            mime, ext = self._infer_mime_and_ext(image_bytes)
            file_name = f"generated_comfyui.{ext}"

            image_data = _build_media_metadata(
                image_bytes,
                media_type="image",
                name=file_name,
                mimetype=mime,
            )

            message_text = await self._celebrate(llm_client, user_prompt)
            return [image_data, message_text]
        except Exception as e:
            return f"Failed to queue prompt: {type(e).__name__}: {e}"

    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)

    # ---------------------------------------
    # Matrix
    # ---------------------------------------
    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        user_prompt = ComfyUIImagePlugin._resolve_prompt(args or {})

        try:
            neg = (args or {}).get("negative_prompt", "") or ""

            image_bytes = await asyncio.to_thread(
                ComfyUIImagePlugin.process_prompt,
                user_prompt,
                neg,
                args=args,
            )

            mime, ext = self._infer_mime_and_ext(image_bytes)
            file_name = f"generated_comfyui.{ext}"

            image_payload = _build_media_metadata(
                image_bytes,
                media_type="image",
                name=file_name,
                mimetype=mime,
            )

            message_text = await self._celebrate(llm_client, user_prompt)
            return [image_payload, message_text]

        except Exception as e:
            return f"Failed to create image: {type(e).__name__}: {e}"


plugin = ComfyUIImagePlugin()
