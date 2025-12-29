# plugins/emoji_ai_responder.py

import os
import logging
import json
from dotenv import load_dotenv
from helpers import LLMClientWrapper
from plugin_base import ToolPlugin
from plugin_settings import get_plugin_enabled

# Load environment variables
load_dotenv()
LLM_HOST = os.getenv("LLM_HOST", "127.0.0.1")
LLM_PORT = os.getenv("LLM_PORT", "11434")
LLM_CLIENT = LLMClientWrapper(host=f"http://{LLM_HOST}:{LLM_PORT}")

logger = logging.getLogger("emoji_ai_responder")

class EmojiAIResponderPlugin(ToolPlugin):
    name = "emoji_ai_responder"
    plugin_name = "Emoji AI Responder"
    description = "Uses LLM to pick an appropriate emoji when a user reacts to a message."
    plugin_dec = "Pick an emoji reaction that matches a user's message."
    platforms = ["passive"]

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
            "You are an assistant that picks a single, context-appropriate emoji in response to a message.\n\n"
            "Choose one emoji that best reflects the **emotion**, **intent**, or **theme** of the message. For example, it could express humor, success, approval, frustration, or curiosity.\n\n"
            "Respond only with the following JSON format:\n\n"
            '{\n'
            '  "function": "suggest_emoji",\n'
            '  "arguments": {\n'
            '    "emoji": "🔥"\n'
            '  }\n'
            '}\n\n'
            "Do not include any other text, explanation, or commentary. Only return the JSON object.\n\n"
            f'The message is:\n"{message_content}"'
        )

        try:
            response = await LLM_CLIENT.chat(
                messages=[{"role": "system", "content": system_prompt}]
            )

            ai_reply = response.get("message", {}).get("content", "").strip()
            logger.debug(f"[emoji_ai_responder] LLM reply: {ai_reply}")
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
