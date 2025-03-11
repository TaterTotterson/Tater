# plugin_base.py
class ToolPlugin:
    name = ""
    usage = ""
    platforms = []  # e.g., ["discord", "webui"]

    async def handle_discord(self, message, args, ollama, context_length, max_response_length):
        """Handle the tool call for Discord."""
        raise NotImplementedError

    async def handle_webui(self, args, ollama_client, context_length):
        """Handle the tool call for the web UI."""
        raise NotImplementedError