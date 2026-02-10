import asyncio
import base64
import logging
import os
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

from helpers import redis_client
from plugin_base import ToolPlugin
from plugin_result import action_failure

load_dotenv()
logger = logging.getLogger("automatic_plugin")
logger.setLevel(logging.INFO)


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


def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    if parsed < minimum:
        return minimum
    if parsed > maximum:
        return maximum
    return parsed


def _as_float(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    if parsed < minimum:
        return minimum
    if parsed > maximum:
        return maximum
    return parsed


class AutomaticPlugin(ToolPlugin):
    name = "automatic_plugin"
    plugin_name = "Automatic1111 Image"
    version = "1.1.0"
    min_tater_version = "50"
    pretty_name = "Your Image"
    settings_category = "Automatic111"

    usage = '{"function":"automatic_plugin","arguments":{"prompt":"<Text prompt for the image>","negative_prompt":"<Optional negative prompt>","steps":20,"cfg_scale":7,"width":1024,"height":1024,"sampler_name":"DPM++ 2M","scheduler":"Simple","seed":-1}}'
    required_args = ["prompt"]
    optional_args = [
        "negative_prompt",
        "steps",
        "cfg_scale",
        "width",
        "height",
        "sampler_name",
        "scheduler",
        "seed",
    ]
    description = "Generates an image from a prompt using an AUTOMATIC1111 txt2img API endpoint."
    plugin_dec = "Generate an image from a text prompt using your Automatic1111 server."
    when_to_use = "Use when the user asks you to generate an image from text."

    required_settings = {
        "AUTOMATIC_URL": {
            "label": "AUTOMATIC URL",
            "type": "string",
            "default": "http://localhost:7860",
            "description": "Base URL for the Automatic1111 API.",
        },
        "AUTOMATIC_STEPS": {
            "label": "Default Steps",
            "type": "number",
            "default": 20,
            "description": "Default diffusion steps used when steps is not provided.",
        },
        "AUTOMATIC_CFG_SCALE": {
            "label": "Default CFG Scale",
            "type": "number",
            "default": 7,
            "description": "Default CFG scale used when cfg_scale is not provided.",
        },
        "AUTOMATIC_WIDTH": {
            "label": "Default Width",
            "type": "number",
            "default": 1024,
            "description": "Default output width used when width is not provided.",
        },
        "AUTOMATIC_HEIGHT": {
            "label": "Default Height",
            "type": "number",
            "default": 1024,
            "description": "Default output height used when height is not provided.",
        },
        "AUTOMATIC_SAMPLER": {
            "label": "Default Sampler",
            "type": "string",
            "default": "DPM++ 2M",
            "description": "Sampler name sent to Automatic1111 when provided.",
        },
        "AUTOMATIC_SCHEDULER": {
            "label": "Default Scheduler",
            "type": "string",
            "default": "Simple",
            "description": "Scheduler sent to Automatic1111 when provided.",
        },
        "AUTOMATIC_NEGATIVE_PROMPT": {
            "label": "Default Negative Prompt",
            "type": "textarea",
            "rows": 4,
            "default": "",
            "description": "Optional default negative prompt.",
            "placeholder": "blurry, low quality, watermark",
        },
        "AUTOMATIC_TIMEOUT_SECONDS": {
            "label": "Request Timeout (seconds)",
            "type": "number",
            "default": 120,
            "description": "HTTP timeout for txt2img calls.",
        },
    }

    waiting_prompt_template = (
        "Write a fun, casual message telling {mention} you’re generating their image now. "
        "Only output that message."
    )
    platforms = ["discord", "webui", "telegram"]

    @staticmethod
    def _decode_map(raw: Dict[str, Any]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for key, value in (raw or {}).items():
            k = key.decode("utf-8", "ignore") if isinstance(key, (bytes, bytearray)) else str(key)
            if isinstance(value, (bytes, bytearray)):
                out[k] = value.decode("utf-8", "ignore")
            else:
                out[k] = str(value or "")
        return out

    @classmethod
    def _settings(cls) -> Dict[str, str]:
        return cls._decode_map(redis_client.hgetall(f"plugin_settings:{cls.settings_category}") or {})

    @classmethod
    def _config(cls) -> Dict[str, Any]:
        settings = cls._settings()
        automatic_url = (settings.get("AUTOMATIC_URL") or os.getenv("AUTOMATIC_URL") or "").strip()
        if not automatic_url:
            automatic_url = "http://localhost:7860"
        automatic_url = automatic_url.rstrip("/")

        timeout_seconds = _as_int(settings.get("AUTOMATIC_TIMEOUT_SECONDS"), default=120, minimum=10, maximum=600)

        return {
            "url": automatic_url,
            "timeout_seconds": timeout_seconds,
            "steps": _as_int(settings.get("AUTOMATIC_STEPS"), default=20, minimum=1, maximum=150),
            "cfg_scale": _as_float(settings.get("AUTOMATIC_CFG_SCALE"), default=7.0, minimum=1.0, maximum=30.0),
            "width": _as_int(settings.get("AUTOMATIC_WIDTH"), default=1024, minimum=64, maximum=2048),
            "height": _as_int(settings.get("AUTOMATIC_HEIGHT"), default=1024, minimum=64, maximum=2048),
            "sampler_name": (settings.get("AUTOMATIC_SAMPLER") or "DPM++ 2M").strip(),
            "scheduler": (settings.get("AUTOMATIC_SCHEDULER") or "").strip(),
            "negative_prompt": (settings.get("AUTOMATIC_NEGATIVE_PROMPT") or "").strip(),
        }

    @staticmethod
    def _resolve_prompt(args: Dict[str, Any]) -> str:
        for key in ("prompt", "query", "request", "message", "text"):
            value = args.get(key)
            if isinstance(value, str) and value.strip():
                return " ".join(value.strip().split())
        return ""

    @staticmethod
    def _try_parse_seed(seed_value: Any) -> Optional[int]:
        if seed_value is None:
            return None
        text = str(seed_value).strip()
        if not text:
            return None
        try:
            return int(float(text))
        except Exception:
            return None

    @classmethod
    def _build_payload(cls, prompt: str, args: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "prompt": prompt,
            "negative_prompt": str(args.get("negative_prompt") or cfg["negative_prompt"] or "").strip(),
            "steps": _as_int(args.get("steps"), default=cfg["steps"], minimum=1, maximum=150),
            "cfg_scale": _as_float(args.get("cfg_scale"), default=cfg["cfg_scale"], minimum=1.0, maximum=30.0),
            "width": _as_int(args.get("width"), default=cfg["width"], minimum=64, maximum=2048),
            "height": _as_int(args.get("height"), default=cfg["height"], minimum=64, maximum=2048),
        }

        sampler_name = str(args.get("sampler_name") or cfg["sampler_name"] or "").strip()
        if sampler_name:
            payload["sampler_name"] = sampler_name

        scheduler = str(args.get("scheduler") or cfg["scheduler"] or "").strip()
        if scheduler:
            payload["scheduler"] = scheduler

        seed = cls._try_parse_seed(args.get("seed"))
        if seed is not None:
            payload["seed"] = seed

        return payload

    @staticmethod
    def _decode_image_data(image_text: str) -> bytes:
        raw = str(image_text or "").strip()
        if "," in raw and raw.split(",", 1)[0].startswith("data:image/"):
            raw = raw.split(",", 1)[1]
        try:
            return base64.b64decode(raw, validate=True)
        except Exception as exc:
            raise RuntimeError(f"Invalid base64 image returned by Automatic1111: {exc}") from exc

    @classmethod
    def _generate_image(cls, prompt: str, args: Dict[str, Any]) -> bytes:
        cfg = cls._config()
        endpoint = f"{cfg['url']}/sdapi/v1/txt2img"
        payload = cls._build_payload(prompt, args, cfg)

        try:
            response = requests.post(
                endpoint,
                json=payload,
                timeout=(5, cfg["timeout_seconds"]),
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to reach Automatic1111: {exc}") from exc

        if response.status_code != 200:
            body = (response.text or "").strip()
            if len(body) > 350:
                body = body[:350] + "..."
            raise RuntimeError(f"Image generation failed (HTTP {response.status_code}): {body}")

        try:
            result = response.json() or {}
        except Exception as exc:
            raise RuntimeError(f"Automatic1111 returned invalid JSON: {exc}") from exc

        images = result.get("images")
        if not isinstance(images, list) or not images:
            raise RuntimeError("Automatic1111 did not return any images.")

        return cls._decode_image_data(images[0])

    async def _respond_to_image(self, prompt_text: str, llm_client) -> str:
        if llm_client is None:
            return "Here is your image."
        safe_prompt = prompt_text[:300].strip()
        try:
            final_response = await llm_client.chat(
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You just generated an image for the user. "
                            "Respond with one short, friendly sentence and no emojis."
                        ),
                    },
                    {
                        "role": "user",
                        "content": f'Image prompt: "{safe_prompt}"',
                    },
                ],
                max_tokens=60,
                temperature=0.4,
            )
            content = ((final_response.get("message", {}) or {}).get("content", "") or "").strip()
            return content or "Here is your image."
        except Exception:
            return "Here is your image."

    async def _run(self, args: Dict[str, Any], llm_client):
        prompt_text = self._resolve_prompt(args or {})
        if not prompt_text:
            return action_failure(
                code="missing_prompt",
                message="No prompt provided for Automatic1111 image generation.",
                needs=["Please provide a prompt describing the image you want."],
                say_hint="Ask the user what image they want to generate.",
            )

        image_bytes = await asyncio.to_thread(self._generate_image, prompt_text, args or {})
        reply = await self._respond_to_image(prompt_text, llm_client)
        image_data = _build_media_metadata(
            image_bytes,
            media_type="image",
            name="generated_image.png",
            mimetype="image/png",
        )
        return [image_data, reply]

    async def handle_discord(self, message, args, llm_client):
        try:
            async with message.channel.typing():
                return await self._run(args or {}, llm_client)
        except Exception as exc:
            logger.exception("[Automatic1111] Discord handler failed: %s", exc)
            return action_failure(
                code="image_generation_failed",
                message=f"Failed to generate image: {exc}",
                say_hint="Explain the generation error and suggest checking Automatic1111 settings.",
            )

    async def handle_webui(self, args, llm_client):
        try:
            return await self._run(args or {}, llm_client)
        except Exception as exc:
            logger.exception("[Automatic1111] WebUI handler failed: %s", exc)
            return action_failure(
                code="image_generation_failed",
                message=f"Failed to generate image: {exc}",
                say_hint="Explain the generation error and suggest checking Automatic1111 settings.",
            )

    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args or {}, llm_client)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        return f"{user}: This plugin only works in Discord, WebUI, and Telegram."


plugin = AutomaticPlugin()
