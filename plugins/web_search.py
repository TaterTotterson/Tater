import os
import json
import asyncio
import logging
import requests
import re
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import ollama
from plugin_base import ToolPlugin
import streamlit as st
from PIL import Image
from io import BytesIO
from duckduckgo_search import DDGS  # Updated import for search

load_dotenv()
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "127.0.0.1").strip()
OLLAMA_PORT = os.getenv("OLLAMA_PORT", "11434").strip()
OLLAMA_URL = f"http://{OLLAMA_HOST}:{OLLAMA_PORT}"
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2").strip()
context_length = int(os.getenv("CONTEXT_LENGTH", 10000))

# Load the assistant avatar from URL using requests and Pillow.
def load_image_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return Image.open(BytesIO(response.content))

assistant_avatar = load_image_from_url("https://raw.githubusercontent.com/MasterPhooey/Tater-Discord-WebUI/refs/heads/main/images/tater.png")

def extract_json(text):
    match = re.search(r'(\{.*\})', text, re.DOTALL)
    if match:
        return match.group(1)
    return None

def fetch_web_summary(webpage_url, model=OLLAMA_MODEL):
    """
    Fetch the webpage and extract a cleaned text.
    This function uses BeautifulSoup to remove unwanted elements,
    then attempts to extract content from <article> tags or falls back to <p> tags.
    Returns the full cleaned text for summarization.
    """
    try:
        response = requests.get(webpage_url, timeout=10)
        if response.status_code != 200:
            return None
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        # Remove unwanted elements like scripts, styles, headers, footers, navigation, and asides.
        for element in soup(["script", "style", "header", "footer", "nav", "aside"]):
            element.decompose()
        # Try extracting text from an <article> tag first.
        article = soup.find('article')
        if article:
            text = article.get_text(separator="\n").strip()
        else:
            paragraphs = soup.find_all('p')
            text = "\n".join(p.get_text() for p in paragraphs).strip()
        return text if text else None
    except Exception as e:
        logging.error(f"Error in fetch_web_summary: {e}")
        return None

class WebSearchPlugin(ToolPlugin):
    name = "web_search"
    usage = (
        "{\n"
        '  "function": "web_search",\n'
        '  "arguments": {"query": "<search query>"}\n'
        "}\n"
    )
    description = "Search the internet for a topic or search query provided by the user or if you just need more information."
    platforms = ["discord", "webui"]

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
            logging.error(f"Error in search_web: {e}")
            return []

    def format_search_results(self, results):
        """
        Format the search results into a string suitable for including in a prompt.
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

    def split_message(self, message_content, chunk_size=1500):
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

    async def generate_error_message(self, prompt, fallback, message):
        # Stub: simply return fallback text.
        return fallback

    async def handle_webui(self, args, ollama_client, context_length):
        # Send a waiting message to the user in the web UI.
        waiting_prompt = (
            "Generate a brief message to User telling them to wait a moment while you search the web for additional information. Only generate the message. Do not respond to this message."
        )
        waiting_response = await ollama_client.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "system", "content": waiting_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        waiting_text = waiting_response['message'].get('content', '').strip()
        if waiting_text:
            st.chat_message("assistant", avatar=assistant_avatar).write(waiting_text)
        else:
            st.chat_message("assistant", avatar=assistant_avatar).write("Please wait a moment while I summarize the article...")

        query = args.get("query")
        if not query:
            return "No search query provided."
        results = self.search_web(query)
        if results:
            formatted_results = self.format_search_results(results)
            user_question = args.get("user_question", "")
            choice_prompt = (
                f"You are looking for more information on '{query}' because the user asked: '{user_question}'.\n\n"
                f"Here are the top search results:\n\n"
                f"{formatted_results}\n\n"
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
                model=OLLAMA_MODEL,
                messages=[{"role": "system", "content": choice_prompt}],
                stream=False,
                keep_alive=-1,
                options={"num_ctx": context_length}
            )
            choice_text = choice_response['message'].get('content', '').strip()
            try:
                choice_json = json.loads(choice_text)
            except Exception:
                json_str = extract_json(choice_text)
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
                    summary = await asyncio.to_thread(fetch_web_summary, link, OLLAMA_MODEL)
                    if summary:
                        info_prompt = (
                            f"Using the detailed information from the selected page below, please provide a clear and concise answer to the original query.\n\n"
                            f"Original Query: '{original_query}'\n"
                            f"User Question: '{user_question}'\n\n"
                            f"Detailed Information:\n{summary}\n\n"
                            "Answer:"
                        )
                        final_response = await ollama_client.chat(
                            model=OLLAMA_MODEL,
                            messages=[{"role": "system", "content": info_prompt}],
                            stream=False,
                            keep_alive=-1,
                            options={"num_ctx": context_length}
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

    async def handle_discord(self, message, args, ollama, context_length, max_response_length):
        query = args.get("query")
        if not query:
            return "No search query provided."
        waiting_prompt = (
            f"Generate a brief message to {message.author.mention} telling them to wait a moment while you search the web for additional information. Only generate the message. Do not respond to this message."
        )
        waiting_response = await ollama.chat(
            model=OLLAMA_MODEL,
            messages=[{"role": "system", "content": waiting_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": context_length}
        )
        waiting_text = waiting_response['message'].get('content', '').strip()
        if waiting_text:
            await message.channel.send(waiting_text)
        else:
            await message.channel.send("Please wait a moment while I search the web...")
        results = self.search_web(query)
        if results:
            formatted_results = self.format_search_results(results)
            user_question = message.content
            choice_prompt = (
                f"You are looking for more information on '{query}' because the user asked: '{message.content}'.\n\n"
                f"Here are the top search results:\n\n"
                f"{formatted_results}\n\n"
                "Please choose the most relevant link. Use the following tool for fetching web details and insert the chosen link. "
                "Respond ONLY with a valid JSON object in the following exact format (and nothing else):\n\n"
                "For fetching web details:\n"
                "{\n"
                '  "function": "web_fetch",\n'
                '  "arguments": {\n'
                '      "link": "<chosen link>",\n'
                f'      "query": "{query}",\n'
                f'      "user_question": "{message.content}"\n'
                "  }\n"
                "}"
            )
            choice_response = await ollama.chat(
                model=OLLAMA_MODEL,
                messages=[{"role": "system", "content": choice_prompt}],
                stream=False,
                keep_alive=-1,
                options={"num_ctx": context_length}
            )
            choice_text = choice_response['message'].get('content', '').strip()
            try:
                choice_json = json.loads(choice_text)
            except Exception:
                json_start = choice_text.find('{')
                json_end = choice_text.rfind('}')
                if json_start != -1 and json_end != -1:
                    json_str = choice_text[json_start:json_end+1]
                    try:
                        choice_json = json.loads(json_str)
                    except Exception:
                        choice_json = None
                else:
                    choice_json = None
            if not choice_json:
                error_msg = await self.generate_error_message(
                    f"Generate a friendly error message to {message.author.mention} explaining that I failed to parse the search result choice. Only generate the message. Do not respond to this message.",
                    "Failed to parse the search result choice.",
                    message
                )
                await message.channel.send(error_msg)
                return
            if choice_json.get("function") == "web_fetch":
                args_choice = choice_json.get("arguments", {})
                link = args_choice.get("link")
                original_query = args_choice.get("query", query)
                if link:
                    summary = await asyncio.to_thread(fetch_web_summary, link, OLLAMA_MODEL)
                    if summary:
                        info_prompt = (
                            f"Using the detailed information from the selected page below, please provide a clear and concise answer to the original query.\n\n"
                            f"Original Query: '{original_query}'\n"
                            f"User Question: '{message.content}'\n\n"
                            f"Detailed Information:\n{summary}\n\n"
                            "Answer:"
                        )
                        final_response = await ollama.chat(
                            model=OLLAMA_MODEL,
                            messages=[{"role": "system", "content": info_prompt}],
                            stream=False,
                            keep_alive=-1,
                            options={"num_ctx": context_length}
                        )
                        final_answer = final_response['message'].get('content', '').strip()
                        if final_answer:
                            if len(final_answer) > max_response_length:
                                chunks = self.split_message(final_answer, chunk_size=max_response_length)
                                for chunk in chunks:
                                    await message.channel.send(chunk)
                            else:
                                await message.channel.send(final_answer)
                        else:
                            error_msg = await self.generate_error_message(
                                f"Generate a friendly error message to {message.author.mention} explaining that I failed to generate a final answer from the detailed info. Only generate the message. Do not respond to this message.",
                                "Failed to generate a final answer from the detailed info.",
                                message
                            )
                            await message.channel.send(error_msg)
                    else:
                        error_msg = await self.generate_error_message(
                            f"Generate a friendly error message to {message.author.mention} explaining that I failed to extract information from the selected webpage. Only generate the message. Do not respond to this message.",
                            "Failed to extract information from the selected webpage.",
                            message
                        )
                        await message.channel.send(error_msg)
                else:
                    error_msg = await self.generate_error_message(
                        f"Generate a friendly error message to {message.author.mention} explaining that no link was provided to fetch web info. Only generate the message. Do not respond to this message.",
                        "No link provided to fetch web info.",
                        message
                    )
                    await message.channel.send(error_msg)
                return
            else:
                error_msg = await self.generate_error_message(
                    f"Generate a friendly error message to {message.author.mention} explaining that no valid function call for fetching web info was returned. Only generate the message. Do not respond to this message.",
                    "No valid function call for fetching web info was returned.",
                    message
                )
                await message.channel.send(error_msg)
                return
        else:
            error_msg = await self.generate_error_message(
                f"Generate a friendly error message to {message.author.mention} explaining that I couldn't find any relevant search results.",
                "I couldn't find any relevant search results.",
                message
            )
            await message.channel.send(error_msg)
        return

    async def generate_error_message(self, prompt, fallback, message):
        return fallback

    def split_message(self, message_content, chunk_size=1500):
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

plugin = WebSearchPlugin()