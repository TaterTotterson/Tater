from plugin_base import ToolPlugin
from plugin_result import action_success
class HukkedJokePlugin(ToolPlugin):
    name = "hukked_joke"
    plugin_name = "Hukked Joke Generator"
    version = "1.0.0"
    description = "Generates a random, clean joke about hukked."
    platforms = ["discord"]
    usage = '{"function":"hukked_joke","arguments":{}}'
    async def handle_discord(self, message, args, llm_client, context=None):
        resp = await llm_client.chat(messages=[{"role":"system","content":"You are a witty AI that tells short, clean jokes about the fictional concept 'hukked'."},{"role":"user","content":"Tell me a joke about hukked."}])
        joke = (resp.get('message') or {}).get('content','').strip() or "I couldn't think of a joke right now."
        return action_success(facts={"joke":joke},say_hint=joke)
plugin = HukkedJokePlugin()