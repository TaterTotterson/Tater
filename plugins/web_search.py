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
from plugin_diagnostics import diagnose_hash_fields, needs_from_diagnosis
from plugin_result import action_failure, research_success

load_dotenv()
logger = logging.getLogger("web_search")
logger.setLevel(logging.INFO)

class WebSearchPlugin(ToolPlugin):
    name = "web_search"
    plugin_name = "Web Search"
    version = "1.0.1"
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
    platforms = ["discord", "webui", "irc", "homeassistant", "matrix", "homekit", "xbmc", "telegram"]

    def _diagnosis(self) -> dict:
        return diagnose_hash_fields(
            "plugin_settings:Web Search",
            fields={"google_api_key": "GOOGLE_API_KEY", "google_cx": "GOOGLE_CX"},
            validators={
                "google_api_key": lambda v: len(v.strip()) >= 10,
                "google_cx": lambda v: len(v.strip()) >= 5,
            },
        )

    def _failure(self, message: str) -> dict:
        diagnosis = self._diagnosis()
        return action_failure(
            code="web_search_failed",
            message=message,
            diagnosis=diagnosis,
            needs=needs_from_diagnosis(
                diagnosis,
                {
                    "google_api_key": "Please set the Google API key for Web Search.",
                    "google_cx": "Please set the Google Search Engine ID (CX).",
                },
            ),
            say_hint="Explain the search failure and ask only for missing search configuration or query details.",
        )

    def search_web(self, query, num_results=10):
        settings = redis_client.hgetall("plugin_settings:Web Search")
        api_key = settings.get("GOOGLE_API_KEY", "")
        cx = settings.get("GOOGLE_CX", "")

        if not api_key or not cx:
            warning = ("Search is not configured. Please set your Google API key and "
                       "Search Engine ID in the plugin settings.")
            logger.warning(f"[Google CSE] {warning}")
            return []

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
                answer = final["message"].get("content", "").strip()
                source_title = ""
                for item in filtered:
                    if item.get("href") == link:
                        source_title = item.get("title") or ""
                        break
                return {
                    "answer": answer,
                    "source": {"title": source_title, "url": link, "publisher": "", "date": ""},
                }

            attempted_links.add(link)

        return {
            "answer": "Sorry, I couldn't extract content from any of the top results.",
            "source": {},
        }


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
            return self._failure("No search query provided.")
        results = self.search_web(query)
        if not results:
            return self._failure("No results found.")
        payload = await self._pick_link_and_summarize(results, query, message.content, llm_client)
        return research_success(
            answer=payload.get("answer") or "",
            highlights=[],
            sources=[payload.get("source")] if isinstance(payload.get("source"), dict) and payload.get("source") else [],
            say_hint="Answer first, cite 1 source when available, and ask a brief follow-up if useful.",
        )

    async def handle_webui(self, args, llm_client):
        query = args.get("query")
        if not query:
            return self._failure("No search query provided.")

        results = self.search_web(query)
        if not results:
            return self._failure("No results found.")

        async def inner():
            return await self._pick_link_and_summarize(
                results,
                query,
                args.get("user_question", ""),
                llm_client
            )

        try:
            asyncio.get_running_loop()
            payload = await inner()
        except RuntimeError:
            payload = asyncio.run(inner())

        return research_success(
            answer=payload.get("answer") or "",
            highlights=[],
            sources=[payload.get("source")] if isinstance(payload.get("source"), dict) and payload.get("source") else [],
            say_hint="Answer first, cite 1 source when available, and ask a brief follow-up if useful.",
        )

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        query = args.get("query")
        if not query:
            return self._failure("No search query provided.")

        results = self.search_web(query)
        if not results:
            return self._failure("No results found.")

        payload = await self._pick_link_and_summarize(results, query, raw_message, llm_client)
        return research_success(
            answer=payload.get("answer") or "",
            highlights=[],
            sources=[payload.get("source")] if isinstance(payload.get("source"), dict) and payload.get("source") else [],
            say_hint="Answer first, cite 1 source when available, and ask a brief follow-up if useful.",
        )

    async def handle_homeassistant(self, args, llm_client):
        query = args.get("query")
        if not query:
            return self._failure("No search query provided.")

        results = self.search_web(query)
        if not results:
            return self._failure("No results found.")

        user_q = args.get("user_question", "")
        payload = await self._pick_link_and_summarize(results, query, user_q, llm_client)
        return research_success(
            answer=(payload.get("answer") or "No answer available.").strip(),
            highlights=[],
            sources=[payload.get("source")] if isinstance(payload.get("source"), dict) and payload.get("source") else [],
            say_hint="Answer first, cite 1 source when available, and ask a brief follow-up if useful.",
        )

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        if not llm_client:
            return self._failure("Search failed: LLM client not provided.")

        query = (args or {}).get("query")
        if not query:
            return self._failure("No search query provided.")

        results = self.search_web(query)
        if not results:
            return self._failure("No results found.")

        user_q = body or ""
        payload = await self._pick_link_and_summarize(results, query, user_q, llm_client)
        return research_success(
            answer=payload.get("answer") or "",
            highlights=[],
            sources=[payload.get("source")] if isinstance(payload.get("source"), dict) and payload.get("source") else [],
            say_hint="Answer first, cite 1 source when available, and ask a brief follow-up if useful.",
        )

    async def handle_telegram(self, update, args, llm_client):
        return await self.handle_webui(args, llm_client)
    
    async def handle_homekit(self, args, llm_client):
        args = args or {}
        query = args.get("query")
        if not query:
            return self._failure("No search query provided.")

        results = self.search_web(query)
        if not results:
            return self._failure("No results found.")

        user_q = args.get("user_question", "")
        payload = await self._pick_link_and_summarize(results, query, user_q, llm_client)
        return research_success(
            answer=self._siri_flatten(payload.get("answer") or ""),
            highlights=[],
            sources=[payload.get("source")] if isinstance(payload.get("source"), dict) and payload.get("source") else [],
            say_hint="Answer first and include one source URL when available.",
        )

    async def handle_xbmc(self, args, llm_client):
        args = args or {}
        query = args.get("query")
        if not query:
            return self._failure("No search query provided.")

        results = self.search_web(query)
        if not results:
            return self._failure("No results found.")

        user_q = args.get("user_question", "")
        payload = await self._pick_link_and_summarize(results, query, user_q, llm_client)
        return research_success(
            answer=(payload.get("answer") or "No answer available.").strip(),
            highlights=[],
            sources=[payload.get("source")] if isinstance(payload.get("source"), dict) and payload.get("source") else [],
            say_hint="Answer first, cite 1 source when available, and ask a brief follow-up if useful.",
        )


plugin = WebSearchPlugin()
