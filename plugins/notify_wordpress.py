# plugins/notify_wordpress.py
import base64
import requests
import re
import html
import asyncio
from urllib.parse import urlparse, parse_qs, urlunparse
from plugin_base import ToolPlugin
from plugin_settings import get_plugin_enabled, get_plugin_settings

class WordPressPosterPlugin(ToolPlugin):
    name = "notify_wordpress"
    plugin_name = "WordPress Poster"
    pretty_name = "WordPress Poster"
    version = "1.0.0"
    min_tater_version = "50"
    platforms = []
    usage = ""
    description = "Posts RSS summaries to WordPress using its REST API."
    plugin_dec = "Post RSS summaries to a WordPress site via its REST API."
    settings_category = "WordPress Poster"
    notifier = True

    required_settings = {
        "wordpress_site_url": {
            "label": "WordPress Site URL",
            "type": "string",
            "default": "",
            "description": "Base site URL (e.g. https://tater.news)"
        },
        "wordpress_username": {
            "label": "WordPress Username",
            "type": "string",
            "default": "",
            "description": "Your WordPress admin/editor username"
        },
        "wordpress_app_password": {
            "label": "WordPress App Password",
            "type": "string",
            "default": "",
            "description": "Generate one from your WordPress profile under 'Application Passwords'"
        },
        "post_status": {
            "label": "Post Status",
            "type": "string",
            "default": "draft",
            "description": "Choose between 'draft' or 'publish'"
        },
        "category_id": {
            "label": "WordPress Category ID",
            "type": "string",
            "default": "",
            "description": "Optional numeric ID of the category to assign posts to"
        }
    }

    def markdown_to_html(self, text: str) -> str:
        def strip_utm(url: str) -> str:
            try:
                parsed = urlparse(url)
                query = parse_qs(parsed.query)
                clean_query = {k: v for k, v in query.items() if not k.lower().startswith("utm_")}
                parsed = parsed._replace(query="&".join(f"{k}={v[0]}" for k, v in clean_query.items()))
                return urlunparse(parsed)
            except Exception:
                return url

        text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
        text = re.sub(r"\*(.+?)\*", r"<i>\1</i>", text)
        text = html.escape(text, quote=False)
        text = text.replace("&lt;b&gt;", "<b>").replace("&lt;/b&gt;", "</b>")
        text = text.replace("&lt;i&gt;", "<i>").replace("&lt;/i&gt;", "</i>")
        text = text.replace("&amp;", "&")
        text = re.sub(r'\[([^\]]+)\]\((https?://[^\s)]+)\)', lambda m: f'<a href="{strip_utm(m.group(2))}">{m.group(1)}</a>', text)
        text = re.sub(r'^## (.+)$', r'<h2>\1</h2>', text, flags=re.MULTILINE)
        text = re.sub(r'^# (.+)$', r'<h1>\1</h1>', text, flags=re.MULTILINE)

        lines = text.split("\n")
        html_parts = []
        in_list = False
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("- ") or stripped.startswith("* "):
                if not in_list:
                    html_parts.append("<ul>")
                    in_list = True
                html_parts.append(f"<li>{stripped[2:]}</li>")
            else:
                if in_list:
                    html_parts.append("</ul>")
                    in_list = False
                if stripped:
                    match = re.match(r'^(https?://\S+)$', stripped)
                    if match:
                        clean = strip_utm(match.group(1))
                        html_parts.append(f'<p><a href="{clean}">{clean}</a></p>')
                    else:
                        html_parts.append(f"<p>{stripped}</p>")
        if in_list:
            html_parts.append("</ul>")

        return "\n\n".join(html_parts) + "\n"

    def wordpress_post(self, title: str, content: str, entry_url: str = None) -> bool:
        if not get_plugin_enabled(self.name):
            return False

        settings = get_plugin_settings(self.settings_category)
        site_url = settings.get("wordpress_site_url", "").rstrip("/")
        username = settings.get("wordpress_username")
        password = settings.get("wordpress_app_password")
        post_status = settings.get("post_status", "draft")
        category_id = settings.get("category_id", "").strip()

        if not site_url or not username or not password:
            print("[WordPress] Missing required settings.")
            return False

        api_url = f"{site_url}/wp-json/wp/v2/posts"
        auth = base64.b64encode(f"{username}:{password}".encode()).decode()

        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
        }

        html_content = self.markdown_to_html(content)

        payload = {
            "title": title,
            "content": html_content,
            "status": post_status,
        }

        if category_id.isdigit():
            payload["categories"] = [int(category_id)]

        try:
            response = requests.post(api_url, headers=headers, json=payload)
            if response.status_code == 201:
                post_url = response.json().get("link", "(unknown)")
                print(f"[WordPress] Post created: {post_url}")
                return True
            else:
                print(f"[WordPress] Failed: {response.status_code} {response.text}")
                return False
        except Exception as e:
            print(f"[WordPress] Error: {e}")
            return False

    async def notify(self, title: str, content: str, targets=None, origin=None, meta=None):
        ok = await asyncio.to_thread(self.wordpress_post, title, content)
        if ok:
            return "Queued notification for wordpress"
        return "Cannot queue: missing wordpress settings or send failed"

plugin = WordPressPosterPlugin()
