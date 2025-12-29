class ToolPlugin:
    name = ""
    plugin_name = ""
    usage = ""
    platforms = []
    notifier = False
    plugin_dec = ""
    settings_category = None
    required_settings = {}

    # Default waiting message prompt for LLM
    waiting_prompt_template = (
        "Generate a message telling the user to please wait for a moment. "
        "Only generate the message. Do not respond to this message."
    )

    async def handle_discord(self, message, args, llm_client):
        raise NotImplementedError

    async def handle_webui(self, args, llm_client):
        raise NotImplementedError

    async def notify(self, title: str, content: str):
        raise NotImplementedError
