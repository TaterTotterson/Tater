# plugins/vision_describer.py
import asyncio
import base64
import requests
import os
import json
import mimetypes
import discord
import redis
from typing import Any, Optional, Tuple, List

from plugin_base import ToolPlugin
from plugin_settings import get_plugin_settings
from helpers import redis_client  # text redis (decode_responses=True in your setup)

# WebUI stores blobs in Redis under this prefix:
WEBUI_FILE_BLOB_KEY_PREFIX = "webui:file:"


def decode_base64(data: str) -> bytes:
    """Safely decode base64 or data: URL strings (legacy support)."""
    data = (data or "").strip()
    if data.startswith("data:"):
        _, data = data.split(",", 1)
    missing_padding = len(data) % 4
    if missing_padding:
        data += "=" * (4 - missing_padding)
    return base64.b64decode(data)


async def safe_send(channel: discord.TextChannel, content: str):
    """Send content to Discord, split if over 2000 characters."""
    chunks = [content[i:i + 2000] for i in range(0, len(content), 2000)]
    for chunk in chunks:
        await channel.send(chunk)


def _to_data_url(image_bytes: bytes, filename: str = "image.png") -> str:
    mime = mimetypes.guess_type(filename)[0] or "image/png"
    b64 = base64.b64encode(image_bytes).decode("utf-8")
    return f"data:{mime};base64,{b64}"


class VisionDescriberPlugin(ToolPlugin):
    name = "vision_describer"
    plugin_name = "Vision Describer"
    version = "1.0.2"
    min_tater_version = "50"

    usage = (
        '{\n'
        '  "function": "vision_describer",\n'
        '  "arguments": {}\n'
        '}'
    )

    description = (
        "Uses an OpenAI-compatible *vision* model to describe the most recent image. "
        "No input needed — it automatically finds the latest uploaded or generated image."
    )

    plugin_dec = "Describe the most recent image using a vision-capable model."
    pretty_name = "Describing Your Image"
    settings_category = "Vision"

    required_settings = {
        "api_base": {
            "label": "API Base URL",
            "description": "OpenAI-compatible base URL (e.g., http://127.0.0.1:1234).",
            "type": "text",
            "default": "http://127.0.0.1:1234"
        },
        "model": {
            "label": "Vision Model",
            "description": "OpenAI-compatible model name (e.g., qwen2.5-vl-7b-instruct, gemma-3-12b-it, etc.).",
            "type": "text",
            "default": "gemma3-27b-abliterated-dpo"
        }
    }

    waiting_prompt_template = (
        "Write a playful message telling {mention} you’re using your magnifying glass "
        "to inspect their image now! Only output that message."
    )

    platforms = ["discord", "webui", "matrix", "irc"]

    # ---------------- Settings ----------------
    def get_vision_settings(self):
        s = get_plugin_settings(self.settings_category)
        api_base = s.get("api_base", self.required_settings["api_base"]["default"]).rstrip("/")
        model = s.get("model", self.required_settings["model"]["default"])
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        return api_base, model, api_key

    # ---------------- Redis (binary blob client) ----------------
    @staticmethod
    def _get_blob_client() -> redis.Redis:
        host = os.getenv("REDIS_HOST", "127.0.0.1")
        port = int(os.getenv("REDIS_PORT", "6379"))
        return redis.Redis(host=host, port=port, db=0, decode_responses=False)

    @staticmethod
    def _load_blob_bytes(blob_client: redis.Redis, *, file_id: Optional[str] = None, blob_key: Optional[str] = None) -> Optional[bytes]:
        try:
            if blob_key:
                data = blob_client.get(blob_key)
                return bytes(data) if data else None

            if file_id:
                # Allow full keys too
                if file_id.startswith(WEBUI_FILE_BLOB_KEY_PREFIX) or file_id.startswith("tater:blob:") or file_id.startswith("tater:matrix:"):
                    data = blob_client.get(file_id)
                    return bytes(data) if data else None

                data = blob_client.get(f"{WEBUI_FILE_BLOB_KEY_PREFIX}{file_id}")
                return bytes(data) if data else None
        except Exception:
            return None

        return None

    # ---------------- Vision Call ----------------
    def _call_openai_vision(self, api_base: str, model: str, image_bytes: bytes, prompt: str, filename: str = "image.png") -> str:
        url = f"{api_base}/v1/chat/completions"
        data_url = _to_data_url(image_bytes, filename)

        payload = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt or "Describe this image."},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
            "temperature": 0.2,
        }

        headers = {"Content-Type": "application/json"}
        _, _, api_key = self.get_vision_settings()
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        resp = requests.post(url, json=payload, headers=headers, timeout=90)
        if resp.status_code != 200:
            return f"Error: Vision service returned status {resp.status_code}.\nResponse: {resp.text}"

        j = resp.json()
        try:
            return j["choices"][0]["message"]["content"].strip()
        except Exception:
            return f"Error: Unexpected response shape: {j}"

    # ---------------- Helpers: find latest image in history ----------------
    @staticmethod
    def _is_allowed_mime(m: str, allowed: List[str]) -> bool:
        if not m:
            return False
        m = str(m).lower().strip()
        return m in [a.lower() for a in allowed]

    @staticmethod
    def _ext_from_mime(m: str) -> str:
        m = (m or "").lower()
        if m == "image/png":
            return "png"
        if m in ("image/jpeg", "image/jpg"):
            return "jpg"
        if m == "image/webp":
            return "webp"
        return "png"

    def _extract_image_from_payload(
        self,
        blob_client: redis.Redis,
        payload: Any,
        allowed_mimetypes: List[str],
    ) -> Tuple[Optional[bytes], Optional[str]]:
        if payload is None:
            return (None, None)

        # Wrapper (WebUI uses these)
        if isinstance(payload, dict) and payload.get("marker") == "plugin_response":
            return self._extract_image_from_payload(blob_client, payload.get("content"), allowed_mimetypes)

        # Lists (often [image_dict, "caption"])
        if isinstance(payload, list):
            for item in payload:
                img, fn = self._extract_image_from_payload(blob_client, item, allowed_mimetypes)
                if img:
                    return (img, fn)
            return (None, None)

        # Dict media shapes
        if isinstance(payload, dict):
            ptype = payload.get("type")

            if ptype in ("image", "file"):
                mimetype = (payload.get("mimetype") or "").strip() or None

                if ptype == "file":
                    if not (mimetype and mimetype.startswith("image/")):
                        return (None, None)

                if mimetype and not self._is_allowed_mime(mimetype, allowed_mimetypes):
                    return (None, None)

                name = (payload.get("name") or "").strip()
                if not name and mimetype:
                    name = f"image.{self._ext_from_mime(mimetype)}"
                if not name:
                    name = "image.png"

                file_id = payload.get("id")
                blob_key = payload.get("blob_key")

                # Inline raw bytes (rare now)
                raw = None
                if isinstance(payload.get("bytes"), (bytes, bytearray)):
                    raw = bytes(payload["bytes"])
                elif isinstance(payload.get("data"), (bytes, bytearray)):
                    raw = bytes(payload["data"])

                if raw:
                    return (raw, name)

                if blob_key or file_id:
                    raw2 = self._load_blob_bytes(blob_client, file_id=file_id, blob_key=blob_key)
                    if raw2:
                        return (raw2, name)

                # Legacy base64 string
                if isinstance(payload.get("data"), str) and payload.get("data"):
                    try:
                        return (decode_base64(payload["data"]), name)
                    except Exception:
                        return (None, None)

                return (None, None)

            # Alternate shapes
            if "blob_key" in payload:
                raw = self._load_blob_bytes(blob_client, blob_key=payload.get("blob_key"))
                if raw:
                    return (raw, payload.get("name") or "image.png")

            if "id" in payload and ("mimetype" in payload or "name" in payload):
                mimetype = payload.get("mimetype") or ""
                if mimetype.startswith("image/") and self._is_allowed_mime(mimetype, allowed_mimetypes):
                    raw = self._load_blob_bytes(blob_client, file_id=payload.get("id"))
                    if raw:
                        name = payload.get("name") or f"image.{self._ext_from_mime(mimetype)}"
                        return (raw, name)

        return (None, None)

    def _find_latest_image_in_history(
        self,
        history_key: str,
        allowed_mimetypes: List[str],
        scan_limit: int = 80,
    ) -> Tuple[Optional[bytes], Optional[str]]:
        blob_client = self._get_blob_client()

        try:
            raw_items = redis_client.lrange(history_key, -scan_limit, -1)
        except Exception:
            raw_items = []

        for raw in reversed(raw_items or []):
            try:
                msg = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                continue

            if not isinstance(msg, dict):
                continue

            content = msg.get("content")
            img, fn = self._extract_image_from_payload(blob_client, content, allowed_mimetypes)
            if img:
                return (img, fn)

        return (None, None)

    # ---------------- Matrix fallback: fetch recent room images ----------------
    async def _find_latest_matrix_image_via_client(self, client, room_id: str, limit: int = 30) -> Tuple[Optional[bytes], Optional[str]]:
        """
        If Redis history doesn't contain bytes/blob refs, fall back to reading recent Matrix messages.
        Tries to find the newest m.image and download it.

        Works with matrix-nio style clients (best-effort).
        """
        if client is None or not room_id:
            return (None, None)

        # Attempt to fetch recent messages
        try:
            # matrix-nio signature (commonly): room_messages(room_id, start, direction="b", limit=10)
            # We don't always have a start token; many clients accept start=None.
            resp = await client.room_messages(room_id=room_id, start=None, direction="b", limit=limit)
        except TypeError:
            # Different signature fallback
            try:
                resp = await client.room_messages(room_id, None, "b", limit)
            except Exception:
                return (None, None)
        except Exception:
            return (None, None)

        chunk = getattr(resp, "chunk", None) or getattr(resp, "events", None) or []
        if not chunk:
            return (None, None)

        # Newest-first already (direction back), but be defensive
        for ev in chunk:
            content = getattr(ev, "content", None) or (ev.get("content") if isinstance(ev, dict) else None) or {}
            msgtype = content.get("msgtype") or content.get("msg_type")
            if msgtype != "m.image":
                continue

            mxc_url = content.get("url")  # "mxc://server/id"
            if not mxc_url:
                continue

            filename = content.get("body") or "matrix_image.png"
            mimetype = content.get("info", {}).get("mimetype") or content.get("mimetype") or ""
            if mimetype and mimetype.lower() not in ("image/png", "image/jpeg", "image/webp"):
                # allow it anyway, but name extension may be wrong
                pass

            # Download via client
            try:
                dl = await client.download(mxc_url)
                # matrix-nio DownloadResponse often has .body (bytes)
                body = getattr(dl, "body", None)
                if body is None:
                    body = getattr(dl, "data", None)
                if body is None and isinstance(dl, dict):
                    body = dl.get("body") or dl.get("data")
                if body:
                    return (bytes(body), filename)
            except Exception:
                continue

        return (None, None)

    # ---------------- Core describe ----------------
    async def process_image_web(self, file_content: bytes, filename: str):
        prompt = (
            "You are an expert visual assistant. Describe the contents of this image in detail, "
            "mentioning key objects, scenes, or actions if recognizable."
        )
        api_base, model, _ = self.get_vision_settings()
        return await asyncio.to_thread(
            self._call_openai_vision,
            api_base, model, file_content, prompt, filename
        )

    async def _describe_latest_image(self, redis_key: str):
        image_bytes, filename = self._find_latest_image_in_history(
            redis_key,
            allowed_mimetypes=["image/png", "image/jpeg", "image/webp"],
            scan_limit=120,
        )

        if not image_bytes:
            return ["❌ No image found. Please upload one or generate one using an image plugin first."]

        prompt = (
            "You are an expert visual assistant. Describe the contents of this image in detail, "
            "mentioning key objects, scenes, or actions if recognizable."
        )

        api_base, model, _ = self.get_vision_settings()
        try:
            description = await asyncio.to_thread(
                self._call_openai_vision,
                api_base, model, image_bytes, prompt, filename or "image.png"
            )
            return [description[:1500]] if description else ["❌ Failed to generate image description."]
        except Exception as e:
            return [f"❌ Error: {e}"]

    async def _describe_latest_image_matrix(self, client, room_id: str, redis_key: str):
        """
        Matrix path:
          1) Try Redis history first
          2) If not found, fall back to fetching recent Matrix images via the client
        """
        image_bytes, filename = self._find_latest_image_in_history(
            redis_key,
            allowed_mimetypes=["image/png", "image/jpeg", "image/webp"],
            scan_limit=200,
        )

        if not image_bytes:
            image_bytes, filename = await self._find_latest_matrix_image_via_client(client, room_id, limit=40)

        if not image_bytes:
            return ["❌ No image found in this room yet. Send an image first, then ask me to describe it."]

        prompt = (
            "You are an expert visual assistant. Describe the contents of this image in detail, "
            "mentioning key objects, scenes, or actions if recognizable."
        )
        api_base, model, _ = self.get_vision_settings()

        try:
            description = await asyncio.to_thread(
                self._call_openai_vision,
                api_base, model, image_bytes, prompt, filename or "image.png"
            )
            return [description[:1500]] if description else ["❌ Failed to generate image description."]
        except Exception as e:
            return [f"❌ Error: {e}"]

    # ---------------- Platform Handlers ----------------
    async def handle_discord(self, message, args, llm_client):
        key = f"tater:channel:{message.channel.id}:history"
        try:
            asyncio.get_running_loop()
            result = await self._describe_latest_image(key)
        except RuntimeError:
            result = asyncio.run(self._describe_latest_image(key))
        return result[0] if result else f"{message.author.mention}: ❌ No image found or failed to process."

    async def handle_webui(self, args, llm_client):
        try:
            asyncio.get_running_loop()
            return await self._describe_latest_image("webui:chat_history")
        except RuntimeError:
            return asyncio.run(self._describe_latest_image("webui:chat_history"))

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        # Redis-backed room history key (what your platform already uses)
        room_id = getattr(room, "room_id", None) or (room.get("room_id") if isinstance(room, dict) else None)
        redis_key = f"tater:matrix:{room_id}:history" if room_id else "tater:matrix:unknown:history"

        try:
            asyncio.get_running_loop()
            result = await self._describe_latest_image_matrix(client, room_id, redis_key)
        except RuntimeError:
            result = asyncio.run(self._describe_latest_image_matrix(client, room_id, redis_key))

        return result

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return f"{user}: This plugin only works via Discord, WebUI, and Matrix. IRC support is not available yet."


plugin = VisionDescriberPlugin()