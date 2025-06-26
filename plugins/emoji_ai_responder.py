# plugins/emoji_ai_responder.py

import os
import logging
import json
from dotenv import load_dotenv
import ollama

from plugin_base import ToolPlugin
from plugin_settings import get_plugin_enabled

# Load environment variables
load_dotenv()
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1")
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3")
OLLAMA_URL = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"

logger = logging.getLogger("emoji_ai_responder")

class EmojiAIResponderPlugin(ToolPlugin):
    name = "emoji_ai_responder"
    description = "Uses Ollama to pick an appropriate emoji when a user reacts to a message."
    platforms = []

    async def on_reaction_add(self, reaction, user):
        logger.debug(f"[emoji_ai_responder] on_reaction_add fired: {user.name} reacted to '{reaction.message.content[:80]}'")

        if user.bot:
            return

        if not get_plugin_enabled(self.name):
            logger.debug("[emoji_ai_responder] Plugin not enabled.")
            return

        message_content = reaction.message.content.strip()
        if not message_content:
            logger.debug("[emoji_ai_responder] No message content to analyze.")
            return

        system_prompt = (
            "You are an assistant that picks a single emoji to represent a message.\n"
            "Respond only with a function call in the following format:\n\n"
            '{\n'
            '  "function": "suggest_emoji",\n'
            '  "arguments": {\n'
            '    "emoji": "🔥"\n'
            '  }\n'
            '}\n\n'
            "Do not include any other text. Always respond using this format.\n\n"
            f'The message is:\n"{message_content}"'
        )

        try:
            response = await ollama.chat(
                model=OLLAMA_MODEL,
                base_url=OLLAMA_URL,
                messages=[{"role": "system", "content": system_prompt}],
                function_call="auto"
            )

            ai_reply = response.get("message", {}).get("content", "").strip()
            logger.debug(f"[emoji_ai_responder] Ollama reply: {ai_reply}")
            if not ai_reply:
                return

            try:
                parsed = json.loads(ai_reply)
            except json.JSONDecodeError:
                json_start = ai_reply.find('{')
                json_end = ai_reply.rfind('}')
                if json_start != -1 and json_end != -1:
                    try:
                        parsed = json.loads(ai_reply[json_start:json_end+1])
                    except Exception:
                        parsed = None
                else:
                    parsed = None

            if parsed and parsed.get("function") == "suggest_emoji":
                emoji = parsed.get("arguments", {}).get("emoji", "").strip()
                if emoji:
                    logger.debug(f"[emoji_ai_responder] Adding emoji: {emoji}")
                    await reaction.message.add_reaction(emoji)

        except Exception as e:
            logger.error(f"[emoji_ai_responder] Error determining emoji: {e}")

plugin = EmojiAIResponderPlugin()