class ToolPlugin:
    name = ""
    plugin_name = ""
    pretty_name = ""
    version = "1.0.0"
    usage = ""
    platforms = []
    notifier = False
    plugin_dec = ""
    description = ""
    when_to_use = ""
    common_needs = []
    required_args = []
    optional_args = []
    example_calls = []
    missing_info_prompts = []
    settings_category = None
    required_settings = {}
    # Argument contract for Cerberus:
    # - "structured": planner supplies structured fields
    # - "raw_user_request": Cerberus injects the user's exact message into raw_user_arg
    argument_mode = "structured"
    raw_user_arg = ""
    raw_user_policy = "verbatim"
    routing_keywords = []

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
