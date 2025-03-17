# plugins/web_search.py
import os
import json
import asyncio
import logging
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from dotenv import load_dotenv
import ollama
from plugin_base import ToolPlugin
import streamlit as st
from PIL import Image
from io import BytesIO
from duckduckgo_search import DDGS  # Updated import for search

load_dotenv()

# Import helper functions from helpers.py.
from helpers import load_image_from_url, send_waiting_message, run_async
assistant_avatar = load_image_from_url()  # Uses default avatar URL from helpers.py

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class WebSearchPlugin(ToolPlugin):
    name = "web_search"
    usage = (
        "{\n"
        '  "function": "web_search",\n'
        '  "arguments": {"query": "<search query>"}\n'
        "}\n"
    )
    description = ("Search the internet for a topic or search query provided by the user, "
                   "or if you just need more information.")
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while I search the web for additional information. Only generate the message. Do not respond to this message."
    )
    platforms = ["discord", "webui"]

    # --- Helper Functions as Static Methods ---
    @staticmethod
    def extract_json(text):
        match = re.search(r'(\{.*\})', text, re.DOTALL)
        if match:
            return match.group(1)
        return None

    def search_web(self, query, num_results=10):
        """
        Search the web using DuckDuckGo and return the top `num_results` results.
        Each result is a dict with keys like 'title', 'href', and 'body'.
        """
        try:
            with DDGS() as ddgs:
                results = ddgs.text(query, max_results=num_results)
            return results
        except Exception as e:
            logger.error(f"Error in search_web: {e}")
            return []

    def format_search_results(self, results):
        """
        Format the search results into a string suitable for inclusion in a prompt.
        """
        formatted = ""
        for idx, result in enumerate(results, start=1):
            title = result.get("title", "No Title")
            link = result.get("href", "No Link")
            snippet = result.get("body", "")
            formatted += f"{idx}. {title} - {link}\n"
            if snippet:
                formatted += f"   {snippet}\n"
        return formatted

    @staticmethod
    def split_message(message_content, chunk_size=1500):
        message_parts = []
        while len(message_content) > chunk_size:
            split_point = message_content.rfind('\n', 0, chunk_size)
            if split_point == -1:
                split_point = message_content.rfind(' ', 0, chunk_size)
            if split_point == -1:
                split_point = chunk_size
            message_parts.append(message_content[:split_point])
            message_content = message_content[split_point:].strip()
        message_parts.append(message_content)
        return message_parts

    # --- New: fetch_web_summary as a static method ---
    @staticmethod
    def fetch_web_summary(webpage_url, model):
        """
        Extract the main textual content from a webpage URL using requests and BeautifulSoup.
        Cleans the HTML by removing unwanted elements and returns the cleaned article text.
        """
        try:
            response = requests.get(webpage_url, timeout=10)
            if response.status_code != 200:
                logger.error(f"Request failed with status: {response.status_code} for URL: {webpage_url}")
                return None
            html = response.text
            logger.debug(f"Fetched HTML for {webpage_url} (length {len(html)})")
            soup = BeautifulSoup(html, "html.parser")
            # Remove unwanted elements.
            for element in soup(["script", "style", "header", "footer", "nav", "aside"]):
                element.decompose()
            # Try to find an <article> tag.
            article = soup.find('article')
            if article:
                text = article.get_text(separator="\n").strip()
            else:
                # If no article tag, try to get all paragraphs.
                paragraphs = soup.find_all('p')
                if paragraphs:
                    text = "\n".join(p.get_text() for p in paragraphs).strip()
                else:
                    # Fallback: get all the text from the page.
                    text = soup.get_text(separator="\n").strip()
            if text:
                logger.info(f"Extracted text (first 100 chars): {text[:100]}")
            else:
                logger.error("No text found after extraction.")
            return text if text else None
        except Exception as e:
            logger.error(f"Error in fetch_web_summary: {e}")
            return None

    async def generate_error_message(self, prompt, fallback, message):
        # Stub: simply return fallback text.
        return fallback

    # --- Discord Handler ---
    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        query = args.get("query")
        if not query:
            return "No search query provided."
        waiting_prompt = self.waiting_prompt_template.format(mention=message.author.mention)
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: message.channel.send(text)
        )
        results = self.search_web(query)
        if results:
            formatted_results = self.format_search_results(results)
            user_question = message.content
            choice_prompt = (
                f"You are looking for more information on '{query}' because the user asked: '{user_question}'.\n\n"
                f"Here are the top search results:\n\n{formatted_results}\n\n"
                "Please choose the most relevant link. Use the following tool for fetching web details and insert the chosen link. "
                "Respond ONLY with a valid JSON object in the following exact format (and nothing else):\n\n"
                "For fetching web details:\n"
                "{\n"
                '  "function": "web_fetch",\n'
                '  "arguments": {\n'
                '      "link": "<chosen link>",\n'
                f'      "query": "{query}",\n'
                f'      "user_question": "{user_question}"\n'
                "  }\n"
                "}"
            )
            choice_response = await ollama_client.chat(
                model=ollama_client.model,
                messages=[{"role": "system", "content": choice_prompt}],
                stream=False,
                keep_alive=-1,
                options={"num_ctx": ollama_client.context_length}
            )
            choice_text = choice_response['message'].get('content', '').strip()
            try:
                choice_json = json.loads(choice_text)
            except Exception:
                json_str = self.extract_json(choice_text)
                if json_str:
                    try:
                        choice_json = json.loads(json_str)
                    except Exception:
                        choice_json = None
                else:
                    choice_json = None
            if not choice_json:
                return "Failed to parse the search result choice."
            elif choice_json.get("function") == "web_fetch":
                args_choice = choice_json.get("arguments", {})
                link = args_choice.get("link")
                original_query = args_choice.get("query", query)
                if link:
                    # Use the static fetch_web_summary method.
                    summary = await asyncio.to_thread(WebSearchPlugin.fetch_web_summary, link, ollama_client.model)
                    if summary:
                        info_prompt = (
                            f"Using the detailed information from the selected page below, please provide a clear and concise answer to the original query.\n\n"
                            f"Original Query: '{original_query}'\n"
                            f"User Question: '{user_question}'\n\n"
                            f"Detailed Information:\n{summary}\n\nAnswer:"
                        )
                        final_response = await ollama_client.chat(
                            model=ollama_client.model,
                            messages=[{"role": "system", "content": info_prompt}],
                            stream=False,
                            keep_alive=-1,
                            options={"num_ctx": ollama_client.context_length}
                        )
                        final_answer = final_response['message'].get('content', '').strip()
                        if not final_answer:
                            final_answer = "Failed to generate a final answer from the detailed info."
                    else:
                        final_answer = "Failed to extract information from the selected webpage."
                else:
                    final_answer = "No link provided to fetch web info."
            else:
                final_answer = "No valid function call for fetching web info was returned."
        else:
            final_answer = "I couldn't find any relevant search results."
        for chunk in self.split_message(final_answer, chunk_size=max_response_length):
            await message.channel.send(chunk)
        return ""

    # --- Web UI Handler ---
    async def handle_webui(self, args, ollama_client, context_length):
        waiting_prompt = self.waiting_prompt_template.format(mention="User")
        await send_waiting_message(
            ollama_client=ollama_client,
            prompt_text=waiting_prompt,
            save_callback=lambda text: None,
            send_callback=lambda text: st.chat_message("assistant", avatar=assistant_avatar).write(text)
        )
        query = args.get("query")
        if not query:
            return "No search query provided."
        results = self.search_web(query)
        if results:
            formatted_results = self.format_search_results(results)
            user_question = args.get("user_question", "")
            choice_prompt = (
                f"You are looking for more information on '{query}' because the user asked: '{user_question}'.\n\n"
                f"Here are the top search results:\n\n{formatted_results}\n\n"
                "Please choose the most relevant link. Use the following tool for fetching web details and insert the chosen link. "
                "Respond ONLY with a valid JSON object in the following exact format (and nothing else):\n\n"
                "For fetching web details:\n"
                "{\n"
                '  "function": "web_fetch",\n'
                '  "arguments": {\n'
                '      "link": "<chosen link>",\n'
                f'      "query": "{query}",\n'
                f'      "user_question": "{user_question}"\n'
                "  }\n"
                "}"
            )
            choice_response = await ollama_client.chat(
                model=ollama_client.model,
                messages=[{"role": "system", "content": choice_prompt}],
                stream=False,
                keep_alive=-1,
                options={"num_ctx": ollama_client.context_length}
            )
            choice_text = choice_response['message'].get('content', '').strip()
            try:
                choice_json = json.loads(choice_text)
            except Exception:
                json_str = self.extract_json(choice_text)
                if json_str:
                    try:
                        choice_json = json.loads(json_str)
                    except Exception:
                        choice_json = None
                else:
                    choice_json = None
            if not choice_json:
                return "Failed to parse the search result choice."
            elif choice_json.get("function") == "web_fetch":
                args_choice = choice_json.get("arguments", {})
                link = args_choice.get("link")
                original_query = args_choice.get("query", query)
                if link:
                    summary = await asyncio.to_thread(WebSearchPlugin.fetch_web_summary, link, ollama_client.model)
                    if summary:
                        info_prompt = (
                            f"Using the detailed information from the selected page below, please provide a clear and concise answer to the original query.\n\n"
                            f"Original Query: '{original_query}'\n"
                            f"User Question: '{user_question}'\n\n"
                            f"Detailed Information:\n{summary}\n\nAnswer:"
                        )
                        final_response = await ollama_client.chat(
                            model=ollama_client.model,
                            messages=[{"role": "system", "content": info_prompt}],
                            stream=False,
                            keep_alive=-1,
                            options={"num_ctx": ollama_client.context_length}
                        )
                        final_answer = final_response['message'].get('content', '').strip()
                        if not final_answer:
                            final_answer = "Failed to generate a final answer from the detailed info."
                    else:
                        final_answer = "Failed to extract information from the selected webpage."
                else:
                    final_answer = "No link provided to fetch web info."
            else:
                final_answer = "No valid function call for fetching web info was returned."
        else:
            final_answer = "I couldn't find any relevant search results."
        return final_answer

plugin = WebSearchPlugin()