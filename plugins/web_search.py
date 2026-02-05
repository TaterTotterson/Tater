# plugins/web_search.py
import json
import asyncio
import logging
import re
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests
from plugin_base import ToolPlugin
from helpers import extract_json, redis_client, get_tater_name

load_dotenv()
logger = logging.getLogger("web_search")
logger.setLevel(logging.INFO)

class WebSearchPlugin(ToolPlugin):
    name = "web_search"
    plugin_name = "Web Search"
    version = "1.0.0"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "web_search",\n'
        '  "arguments": {"query": "<search query>"}\n'
        "}\n"
    )
    description = "Web search tool, search for more info to help answer the users questions."
    plugin_dec = "Search the web via Google CSE and summarize a relevant result."
    pretty_name = "Searching For More Info"
    settings_category = "Web Search"
    required_settings = {
        "GOOGLE_API_KEY": {
            "label": "Google API Key",
            "type": "string",
            "default": "",
        },
        "GOOGLE_CX": {
            "label": "Google Search Engine ID",
            "type": "string",
            "default": "",
        }
    }
    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re searching the web for more information now! "
        "Only output that message."
    )
    platforms = ["discord", "webui", "irc", "homeassistant", "matrix", "homekit", "xbmc"]

    def search_web(self, query, num_results=10):
        settings = redis_client.hgetall("plugin_settings:Web Search")
        api_key = settings.get("GOOGLE_API_KEY", "")
        cx = settings.get("GOOGLE_CX", "")

        if not api_key or not cx:
            warning = ("Search is not configured. Please set your Google API key and "
                       "Search Engine ID in the plugin settings.")
            logger.warning(f"[Google CSE] {warning}")
            return [{"title": "Missing configuration", "href": "", "body": warning}]

        try:
            response = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": api_key, "cx": cx, "q": query, "num": num_results},
                timeout=10
            )
            if response.status_code != 200:
                logger.error(f"[Google CSE error] HTTP {response.status_code}: {response.text}")
                return []

            return [
                {
                    "title": item.get("title"),
                    "href": item.get("link"),
                    "body": item.get("snippet"),
                }
                for item in response.json().get("items", [])
            ]
        except Exception as e:
            logger.error(f"[search_web error] {e}")
            return []

    def format_search_results(self, results):
        out = ""
        for i, result in enumerate(results, 1):
            out += f"{i}. {result.get('title', 'No Title')} - {result.get('href', '')}\n"
            if result.get("body"):
                out += f"   {result['body']}\n"
        return out

    @staticmethod
    def fetch_web_summary(url, model):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            )
        }
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                logger.error(f"Request failed: {resp.status_code} - {url}")
                return None
            from bs4 import BeautifulSoup  # local import to keep deps light elsewhere
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(["script", "style", "header", "footer", "nav", "aside"]):
                tag.decompose()
            container = soup.find("article") or soup.find("main") or soup.body
            if not container:
                return None
            lines = [line.strip() for line in container.get_text(separator="\n").splitlines() if line.strip()]
            article_text = "\n".join(lines)
            return " ".join(article_text.split()[:3000]) if article_text else None
        except Exception as e:
            logger.error(f"[fetch_web_summary error] {e}")
            return None

    def split_message(self, text, chunk_size=1500):
        chunks = []
        while len(text) > chunk_size:
            split = text.rfind('\n', 0, chunk_size) or text.rfind(' ', 0, chunk_size) or chunk_size
            chunks.append(text[:split])
            text = text[split:].strip()
        chunks.append(text)
        return chunks

    async def _pick_link_and_summarize(self, results, query, user_question, llm_client, max_attempts=3):
        attempted_links = set()
        for attempt in range(max_attempts):
            filtered = [r for r in results if r["href"] not in attempted_links]
            if not filtered:
                break

            formatted_results = self.format_search_results(filtered)
            first, last = get_tater_name()

            prompt = (
                f"Your name is {first} {last}, you're researching the topic '{query}' "
                f"because the user asked: '{user_question}'.\n\n"
                f"Here are search results:\n\n{formatted_results}\n\n"
                "Respond with:\n"
                "{\n"
                '  "function": "web_fetch",\n'
                '  "arguments": {\n'
                '    "link": "<chosen link>",\n'
                f'    "query": "{query}",\n'
                f'    "user_question": "{user_question}"\n'
                "  }\n"
                "}"
            )

            response = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            choice = response["message"].get("content", "").strip()
            try:
                choice_json = json.loads(choice)
            except:
                json_str = extract_json(choice)
                choice_json = json.loads(json_str) if json_str else None

            if not choice_json or choice_json.get("function") != "web_fetch":
                continue

            link = choice_json["arguments"].get("link")
            if not link:
                continue

            summary = await asyncio.to_thread(self.fetch_web_summary, link, getattr(llm_client, "model", ""))
            if summary:
                first, last = get_tater_name()
                final_prompt = (
                    f"Your name is {first} {last}. Answer the user's question using this content.\n\n"
                    f"Query: {query}\n"
                    f"User Question: {user_question}\n\n"
                    f"Content:\n{summary}\n\n"
                    "Do not introduce yourself. Only answer:"
                )
                final = await llm_client.chat(messages=[{"role": "system", "content": final_prompt}])
                return final["message"].get("content", "").strip()

            attempted_links.add(link)

        return "Sorry, I couldn't extract content from any of the top results."


    def _siri_flatten(self, text: str | None) -> str:
        """Make responses clean for Siri TTS (no markdown noise, compact, short-ish)."""
        if not text:
            return "No answer available."
        out = str(text)
        # strip simple markdown emphasis/backticks
        out = re.sub(r"[`*_]{1,3}", "", out)
        # collapse whitespace
        out = re.sub(r"\s+", " ", out).strip()
        # keep it reasonably short for spoken response
        return out[:450]

    # -------- Platform handlers --------
    async def handle_discord(self, message, args, llm_client):
        query = args.get("query")
        if not query:
            return "No search query provided."
        results = self.search_web(query)
        if not results:
            return "No results found."
        return await self._pick_link_and_summarize(results, query, message.content, llm_client)

    async def handle_webui(self, args, llm_client):
        query = args.get("query")
        if not query:
            return ["No search query provided."]

        results = self.search_web(query)
        if not results:
            return ["No results found."]

        async def inner():
            return await self._pick_link_and_summarize(
                results,
                query,
                args.get("user_question", ""),
                llm_client
            )

        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        query = args.get("query")
        if not query:
            return f"{user}: No search query provided."

        results = self.search_web(query)
        if not results:
            return f"{user}: No results found."

        answer = await self._pick_link_and_summarize(results, query, raw_message, llm_client)
        return f"{user}: {answer}"

    async def handle_homeassistant(self, args, llm_client):
        query = args.get("query")
        if not query:
            return "No search query provided."

        results = self.search_web(query)
        if not results:
            return "No results found."

        user_q = args.get("user_question", "")
        answer = await self._pick_link_and_summarize(results, query, user_q, llm_client)
        return (answer or "No answer available.").strip()

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        if not llm_client:
            return "Search failed: LLM client not provided."

        query = (args or {}).get("query")
        if not query:
            return "No search query provided."

        results = self.search_web(query)
        if not results:
            return "No results found."

        user_q = body or ""
        answer = await self._pick_link_and_summarize(results, query, user_q, llm_client)
        return answer
    
    async def handle_homekit(self, args, llm_client):
        args = args or {}
        query = args.get("query")
        if not query:
            return "No search query provided."

        results = self.search_web(query)
        if not results:
            return "No results found."

        user_q = args.get("user_question", "")
        answer = await self._pick_link_and_summarize(results, query, user_q, llm_client)
        return self._siri_flatten(answer)

    async def handle_xbmc(self, args, llm_client):
        args = args or {}
        query = args.get("query")
        if not query:
            return "No search query provided."

        results = self.search_web(query)
        if not results:
            return "No results found."

        user_q = args.get("user_question", "")
        answer = await self._pick_link_and_summarize(results, query, user_q, llm_client)
        return (answer or "No answer available.").strip()


plugin = WebSearchPlugin()
