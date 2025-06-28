# plugins/web_search.py
import os
import json
import asyncio
import logging
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from dotenv import load_dotenv
import streamlit as st
from duckduckgo_search import DDGS
import requests
from plugin_base import ToolPlugin
from helpers import load_image_from_url, format_irc, extract_json, send_waiting_message, save_assistant_message

load_dotenv()
assistant_avatar = load_image_from_url()

logger = logging.getLogger("web_search")
logger.setLevel(logging.INFO)

class WebSearchPlugin(ToolPlugin):
    name = "web_search"
    usage = (
        "{\n"
        '  "function": "web_search",\n'
        '  "arguments": {"query": "<search query>"}\n'
        "}\n"
    )
    description = "Searches the web and returns summarized answers to user questions."
    waiting_prompt_template = (
        "Generate a brief message to {mention} telling them to wait a moment while I search the web for additional information. Only generate the message. Do not respond to this message."
    )
    platforms = ["discord", "webui", "irc"]

    def search_web(self, query, num_results=10):
        try:
            with DDGS() as ddgs:
                return ddgs.text(query, max_results=num_results)
        except Exception as e:
            logger.error(f"[search_web error] {e}")
            return []

    def format_search_results(self, results):
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
    def fetch_web_summary(webpage_url, model):
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }

        try:
            resp = requests.get(webpage_url, headers=headers, timeout=10)
            if resp.status_code != 200:
                logger.error(f"Request failed: {resp.status_code} - {webpage_url}")
                return None

            soup = BeautifulSoup(resp.text, "html.parser")
            for element in soup(["script", "style", "header", "footer", "nav", "aside"]):
                element.decompose()

            container = soup.find("article") or soup.find("main") or soup.body
            if not container:
                return None

            text = container.get_text(separator="\n")
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            article_text = "\n".join(lines)

            if len(article_text.split()) > 3000:
                article_text = " ".join(article_text.split()[:3000])
            logger.info(f"[fetch_web_summary] Extracted {len(article_text)} characters from {webpage_url}")
            return article_text
        except Exception as e:
            logger.error(f"[fetch_web_summary error] {e}")
            return None

    @staticmethod
    def split_message(text, chunk_size=1500):
        parts = []
        while len(text) > chunk_size:
            split = text.rfind('\n', 0, chunk_size)
            if split == -1:
                split = text.rfind(' ', 0, chunk_size)
            if split == -1:
                split = chunk_size
            parts.append(text[:split])
            text = text[split:].strip()
        parts.append(text)
        return parts
    
    async def safe_send(self, channel, content):
        if len(content) <= 2000:
            await channel.send(content)
        else:
            for chunk in self.split_message(content, 1900):
                await channel.send(chunk)

    # ---------------------------------------------------------
    # Discord handler
    # ---------------------------------------------------------
    async def handle_discord(self, message, args, ollama_client, context_length, max_response_length):
        query = args.get("query")
        if not query:
            return "No search query provided."

        mention = message.author.mention
        await send_waiting_message(
            ollama_client,
            prompt_text=self.waiting_prompt_template.format(mention=mention),
            send_callback=lambda text: asyncio.create_task(self.safe_send(message.channel, text)),
            save_callback=lambda text: save_assistant_message(message.channel.id, text)
        )

        results = self.search_web(query)
        if not results:
            msg = "I couldn't find any relevant search results."
            await self.safe_send(message.channel, msg)
            save_assistant_message(message.channel.id, msg)
            return

        formatted_results = self.format_search_results(results)
        user_question = message.content

        choice_prompt = (
            f"Your name is Tater Totterson, you are looking for more information on the topic '{query}', because the user asked: '{user_question}'.\n\n"
            f"Here are the top search results:\n\n{formatted_results}\n\n"
            "Pick the most relevant link. Respond ONLY with this JSON format:\n"
            "{\n"
            '  "function": "web_fetch",\n'
            '  "arguments": {\n'
            '    "link": "<chosen link>",\n'
            f'    "query": "{query}",\n'
            f'    "user_question": "{user_question}"\n'
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
        except:
            json_str = extract_json(choice_text)
            choice_json = json.loads(json_str) if json_str else None

        if not choice_json or choice_json.get("function") != "web_fetch":
            msg = "Failed to parse a valid link from search results."
            await self.safe_send(message.channel, msg)
            save_assistant_message(message.channel.id, msg)
            return

        link = choice_json["arguments"].get("link")
        original_query = choice_json["arguments"].get("query", query)
        if not link:
            msg = "No link was selected for detailed info."
            await self.safe_send(message.channel, msg)
            save_assistant_message(message.channel.id, msg)
            return

        summary = await asyncio.to_thread(self.fetch_web_summary, link, ollama_client.model)
        if not summary:
            msg = "Failed to extract text from the selected page."
            await self.safe_send(message.channel, msg)
            save_assistant_message(message.channel.id, msg)
            return

        info_prompt = (
            f"Your name is Tater Totterson, you are answering a question based on the following web page content.\n\n"
            f"Original Query: {original_query}\n"
            f"User Question: {user_question}\n\n"
            f"Web Content:\n{summary}\n\n"
            f"Please provide a concise answer:"
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
            final_answer = "The assistant couldn't generate a response based on the web content."

        await self.safe_send(message.channel, final_answer)
        save_assistant_message(message.channel.id, final_answer)
        return ""

    # ---------------------------------------------------------
    # WebUI handler
    # ---------------------------------------------------------
    async def handle_webui(self, args, ollama_client, context_length):
        await send_waiting_message(
            ollama_client,
            prompt_text=self.waiting_prompt_template.format(mention="User"),
            send_callback=lambda text: st.chat_message("assistant", avatar=assistant_avatar).write(text),
            save_callback=lambda text: save_assistant_message("webui:chat_history", text)
        )

        query = args.get("query")
        if not query:
            return "No search query provided."

        results = self.search_web(query)
        if not results:
            msg = "No results found."
            save_assistant_message("webui:chat_history", msg)
            return msg

        formatted_results = self.format_search_results(results)
        user_question = args.get("user_question", "")

        choice_prompt = (
            f"Your name is Tater Totterson, you are researching the topic '{query}' because the user asked: '{user_question}'.\n\n"
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
        except:
            json_str = extract_json(choice_text)
            choice_json = json.loads(json_str) if json_str else None

        if not choice_json or choice_json.get("function") != "web_fetch":
            msg = "Failed to parse function response."
            save_assistant_message("webui:chat_history", msg)
            return msg

        link = choice_json["arguments"].get("link")
        if not link:
            msg = "No link was selected."
            save_assistant_message("webui:chat_history", msg)
            return msg

        summary = await asyncio.to_thread(self.fetch_web_summary, link, ollama_client.model)
        if not summary:
            msg = "Failed to extract content from page."
            save_assistant_message("webui:chat_history", msg)
            return msg

        info_prompt = (
            f"Your name is Tater Totterson. Answer the user's question using this content.\n\n"
            f"Query: {query}\n"
            f"User Question: {user_question}\n\n"
            f"Content:\n{summary}\n\n"
            "Do not introduce yourself. Only answer:"
        )

        final_response = await ollama_client.chat(
            model=ollama_client.model,
            messages=[{"role": "system", "content": info_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": ollama_client.context_length}
        )

        final_answer = final_response["message"].get("content", "").strip()
        save_assistant_message("webui:chat_history", final_answer)
        return final_answer

    # ---------------------------------------------------------
    # IRC handler
    # ---------------------------------------------------------
    async def handle_irc(self, bot, channel, user, raw_message, args, ollama_client):
        from helpers import format_irc

        query = args.get("query")
        if not query:
            await bot.privmsg(channel, f"{user}: No search query provided.")
            return

        await bot.privmsg(channel, f"{user}: Searching the web for \"{query}\"...")

        results = self.search_web(query)
        if not results:
            await bot.privmsg(channel, f"{user}: No results found.")
            return

        formatted_results = self.format_search_results(results)
        user_question = raw_message

        choice_prompt = (
            f"Your name is Tater Totterson, you are researching the topic '{query}' because the user asked: '{user_question}'.\n\n"
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

        response = await ollama_client.chat(
            model=ollama_client.model,
            messages=[{"role": "system", "content": choice_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": ollama_client.context_length}
        )

        content = response["message"].get("content", "").strip()
        try:
            choice_json = json.loads(content)
        except:
            json_str = extract_json(content)
            choice_json = json.loads(json_str) if json_str else None

        if not choice_json or choice_json.get("function") != "web_fetch":
            await bot.privmsg(channel, f"{user}: Failed to parse search response.")
            return

        link = choice_json["arguments"].get("link")
        if not link:
            await bot.privmsg(channel, f"{user}: No link selected.")
            return

        summary = await asyncio.to_thread(self.fetch_web_summary, link, ollama_client.model)
        if not summary:
            await bot.privmsg(channel, f"{user}: Couldn't extract content from selected link.")
            return

        info_prompt = (
            f"Your name is Tater Totterson. Answer the user's question using this content.\n\n"
            f"Query: {query}\n"
            f"User Question: {user_question}\n\n"
            f"Content:\n{summary}\n\n"
            "Do not introduce yourself. Only answer:"
        )

        final = await ollama_client.chat(
            model=ollama_client.model,
            messages=[{"role": "system", "content": info_prompt}],
            stream=False,
            keep_alive=-1,
            options={"num_ctx": ollama_client.context_length}
        )

        answer = final["message"].get("content", "").strip()
        irc_answer = format_irc(answer)

        for chunk in self.split_message(irc_answer, 400):
            await bot.privmsg(channel, chunk)

        save_assistant_message(channel, answer)

plugin = WebSearchPlugin()