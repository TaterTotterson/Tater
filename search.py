# search.py
import logging
from duckduckgo_search import DDGS  # Updated import

def search_web(query, num_results=10):
    """
    Search the web using DuckDuckGo and return the top `num_results` results.
    Each result is expected to be a dict with keys like 'title', 'href', and 'body'.
    """
    try:
        with DDGS() as ddgs:
            results = ddgs.text(query, max_results=num_results)
        return results
    except Exception as e:
        logging.error(f"Error in search_web: {e}")
        return []

def format_search_results(results):
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