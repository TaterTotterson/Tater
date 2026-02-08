# plugins/device_compare.py
import io
import json
import logging
import re
from typing import List, Dict, Any, Tuple
import requests
from PIL import Image, ImageDraw, ImageFont
from bs4 import BeautifulSoup
from plugin_base import ToolPlugin
from helpers import (
    extract_json,
    redis_client,
    get_tater_name,
)

def _build_media_metadata(binary: bytes, *, media_type: str, name: str, mimetype: str) -> dict:
    if not isinstance(binary, (bytes, bytearray)):
        raise TypeError("binary must be bytes")
    return {
        "type": media_type,
        "name": name,
        "mimetype": mimetype,
        "size": len(binary),
        "bytes": bytes(binary),
    }

logger = logging.getLogger("device_compare")
logger.setLevel(logging.INFO)

class DeviceComparePlugin(ToolPlugin):
    name = "device_compare"
    plugin_name = "Device Compare"
    version = "1.0.1"
    min_tater_version = "50"
    usage = (
        "{\n"
        '  "function": "device_compare",\n'
        '  "arguments": {"device_a": "<first device>", "device_b": "<second device>"}\n'
        "}\n"
    )
    required_args = ["device_a", "device_b"]
    optional_args = ["device1", "device2", "first_device", "second_device", "left_device", "right_device", "devices", "query", "compare"]
    description = "Compares two devices by fetching specs and per-game FPS from multiple sources, then renders image tables."
    plugin_dec = "Compare two devices with spec tables and per-game FPS benchmarks."
    pretty_name = "Comparing Devices"
    settings_category = "Device Compare"
    # Matrix supported (images only). IRC still not supported (images).
    platforms = ["discord", "webui", "matrix", "telegram"]

    required_settings = {
        "RESULTS_PER_QUERY": {"label": "Results to consider (specs)", "type": "number", "default": 10},
        "FETCH_TIMEOUT_SECONDS": {"label": "HTTP fetch timeout (s)", "type": "number", "default": 12},
        "ENABLE_FPS_SEARCH": {"label": "Enable per-game FPS search", "type": "checkbox", "default": True},
        "FPS_RESULTS_PER_QUERY": {"label": "Results to consider (FPS)", "type": "number", "default": 10},
        "MAX_FPS_ROWS": {"label": "Max FPS rows in image", "type": "number", "default": 20},
    }

    waiting_prompt_template = "Let {mention} know you’re grabbing specs, benchmarks, and rendering comparison images now. Only output that message."

    # ---------- settings / http / search ----------
    @staticmethod
    def _decode_text(value: Any, default: str = "") -> str:
        if isinstance(value, (bytes, bytearray)):
            return value.decode("utf-8", "ignore")
        if value is None:
            return default
        return str(value)

    @classmethod
    def _to_int(cls, value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            parsed = int(float(cls._decode_text(value, str(default)).strip()))
        except Exception:
            parsed = int(default)
        if parsed < minimum:
            return minimum
        if parsed > maximum:
            return maximum
        return parsed

    @classmethod
    def _to_bool(cls, value: Any, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        raw = cls._decode_text(value).strip().lower()
        if raw in ("1", "true", "yes", "y", "on", "enabled"):
            return True
        if raw in ("0", "false", "no", "n", "off", "disabled"):
            return False
        return default

    def _get_settings(self):
        s = redis_client.hgetall(f"plugin_settings:{self.settings_category}") or redis_client.hgetall(
            f"plugin_settings: {self.settings_category}"
        ) or {}

        def val(key, default=""):
            return self._decode_text(s.get(key, default), str(default))

        def redis_val(key, default=""):
            v = redis_client.get(key)
            if v is None:
                return default
            return self._decode_text(v, str(default))

        # Use only global WebUI search config.
        global_api_key = redis_val("tater:web_search:google_api_key", "").strip()
        global_cx = redis_val("tater:web_search:google_cx", "").strip()

        return {
            "api_key": global_api_key,
            "cx": global_cx,
            "spec_results": self._to_int(val("RESULTS_PER_QUERY", 10), default=10, minimum=1, maximum=10),
            "fps_results": self._to_int(val("FPS_RESULTS_PER_QUERY", 10), default=10, minimum=1, maximum=10),
            "timeout": self._to_int(val("FETCH_TIMEOUT_SECONDS", 12), default=12, minimum=3, maximum=60),
            "enable_fps": self._to_bool(val("ENABLE_FPS_SEARCH", True), default=True),
            "max_fps_rows": self._to_int(val("MAX_FPS_ROWS", 20), default=20, minimum=1, maximum=100),
        }

    def google_search(self, query: str, n: int) -> List[Dict[str, str]]:
        cfg = self._get_settings()
        if not cfg["api_key"] or not cfg["cx"]:
            logger.warning("DeviceCompare: Google CSE not configured.")
            return []
        query_n = self._to_int(n, default=cfg["spec_results"], minimum=1, maximum=10)
        try:
            r = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": cfg["api_key"], "cx": cfg["cx"], "q": query, "num": query_n},
                timeout=cfg["timeout"],
            )
            if r.status_code != 200:
                logger.error(f"DeviceCompare: CSE HTTP {r.status_code}: {r.text}")
                return []
            items = r.json().get("items", []) or []
            return [{"title": it.get("title",""), "href": it.get("link",""), "snippet": it.get("snippet","")} for it in items]
        except Exception as e:
            logger.error(f"DeviceCompare: google_search error: {e}")
            return []
    
    async def _pick_specs_with_retries(
        self,
        llm_client,
        device: str,
        results: List[Dict[str, str]],
        max_attempts: int = 3,
    ) -> tuple[dict, list[str]]:
        tried: set[str] = set()
        remaining = [r for r in results if r.get("href")]

        async def choose_batch(candidates):
            first, last = get_tater_name()
            prompt = (
                f"You are {first} {last}. Choose 3 high-quality, SPEC-FOCUSED links for this device.\n"
                f"- Prefer official spec pages, reputable reviews with spec tables, or datasheets.\n\n"
                f"DEVICE: {device}\n\n"
                f"SEARCH RESULTS (numbered):\n{self._format_results_for_llm(candidates)}\n\n"
                "Return ONLY JSON using 1-based indexes from the list above. DO NOT invent links.\n"
                "{\n"
                '  "function": "pick_sources",\n'
                '  "arguments": {"indexes": [1, 2, 3]}\n'
                "}"
            )
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            content = resp["message"].get("content", "").strip()
            try:
                js = json.loads(content)
            except Exception:
                js = json.loads(extract_json(content) or "{}")

            raw_idxs = (js.get("arguments", {}) or {}).get("indexes", [])
            if not isinstance(raw_idxs, list) or not raw_idxs:
                urls = [r["href"] for r in candidates[:3] if r.get("href")]
            else:
                urls = []
                for idx in raw_idxs:
                    try:
                        i = int(idx) - 1
                        if 0 <= i < len(candidates):
                            u = candidates[i].get("href")
                            if u:
                                urls.append(u)
                    except Exception:
                        continue

            urls = [u for u in urls if u not in tried]
            seen = set()
            urls = [u for u in urls if not (u in seen or seen.add(u))]
            return urls[:3]

        for _ in range(max_attempts):
            batch = await choose_batch([r for r in remaining if r["href"] not in tried])
            if not batch:
                break

            texts = {u: self.fetch_page_text(u) for u in batch}
            tried.update(batch)

            if not any(texts.values()):
                continue

            specs = await self._extract_specs_from_pages(llm_client, device, texts)
            meaningful = any(specs.get(k) for k in ("cpu", "gpu", "ram", "resolution", "refresh_rate", "ports"))
            if meaningful:
                specs.setdefault("_sources", [u for u, t in texts.items() if t])
                specs.setdefault("title", device)
                return specs, specs["_sources"]

        return {"title": device, "_sources": []}, []

    async def _pick_fps_with_retries(
        self,
        llm_client,
        device: str,
        base_results: List[Dict[str, str]],
        max_attempts: int = 3,
    ) -> tuple[dict, list[str]]:
        async def attempt_on_results(results):
            tried: set[str] = set()
            remaining = [r for r in results if r.get("href")]

            async def choose_batch(candidates):
                first, last = get_tater_name()
                prompt = (
                    f"You are {first} {last}. Choose up to 3 links that include per-game FPS/benchmark tables for:\n"
                    f"{device}\n\n"
                    "STRICT RULES:\n"
                    f"- The page MUST clearly be about the exact device name the user typed ('{device}') — "
                    "this name must appear in the page title, snippet, or URL.\n"
                    "- If no links qualify, return an empty array.\n\n"
                    f"SEARCH RESULTS (numbered):\n{self._format_results_for_llm(candidates)}\n\n"
                    "Return ONLY JSON using 1-based indexes from the list above. DO NOT invent links.\n"
                    "{\n"
                    '  "function": "pick_fps_sources",\n'
                    '  "arguments": {"indexes": [1, 2, 3]}\n'
                    "}"
                )
                resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
                content = resp["message"].get("content", "").strip()
                try:
                    js = json.loads(content)
                except Exception:
                    js = json.loads(extract_json(content) or "{}")

                raw_idxs = (js.get("arguments", {}) or {}).get("indexes", [])
                if not isinstance(raw_idxs, list) or not raw_idxs:
                    urls = [r["href"] for r in candidates[:3] if r.get("href")]
                else:
                    urls = []
                    for idx in raw_idxs:
                        try:
                            i = int(idx) - 1
                            if 0 <= i < len(candidates):
                                u = candidates[i].get("href")
                                if u:
                                    urls.append(u)
                        except Exception:
                            continue

                urls = [u for u in urls if u not in tried]
                seen = set()
                urls = [u for u in urls if not (u in seen or seen.add(u))]
                return urls[:3]

            for _ in range(max_attempts):
                batch = await choose_batch([r for r in remaining if r["href"] not in tried])
                if not batch:
                    break

                texts = {u: self.fetch_page_text(u) for u in batch}
                tried.update(batch)

                if not any(texts.values()):
                    continue

                data = await self._extract_fps_table(llm_client, device, texts)
                fps = data.get("fps_by_game", {})
                if isinstance(fps, dict) and any(fps.values()):
                    used = data.get("_sources", [u for u, t in texts.items() if t])
                    return {
                        "title": device,
                        "fps_by_game": fps,
                        "_sources": used
                    }, used

            return {"title": device, "fps_by_game": {}, "_sources": []}, []

        got, used = await attempt_on_results(base_results)
        if got and any(got.get("fps_by_game", {}).values()):
            return got, used

        return {"title": device, "fps_by_game": {}, "_sources": []}, []

    async def _extract_specs_from_pages(self, llm_client, device_name: str, url_to_text: Dict[str, str]) -> Dict[str, Any]:
        docs = []
        for url, text in url_to_text.items():
            if text:
                docs.append(f"URL: {url}\n---\n{text}\n")
        big_blob = "\n\n====\n\n".join(docs)[:18000]

        schema_hint = {
            "title": device_name,
            "brand": "", "model": "", "release_date": "", "price": "",
            "cpu": "", "gpu": "", "ram": "", "storage": "",
            "display_size": "", "display_type": "", "resolution": "", "refresh_rate": "", "hdr": "",
            "ports": "", "wireless": "", "battery": "", "charging": "",
            "os": "", "dimensions": "", "weight": "",
            "_sources": list(url_to_text.keys())
        }

        prompt = (
            "Extract and normalize device specs from the documents below.\n"
            "Return ONLY JSON with keys similar to this schema. DO NOT invent values. "
            "If unknown, use an empty string.\n"
            f"Schema hint:\n{json.dumps(schema_hint, indent=2)}\n\n"
            "Documents:\n"
            f"{big_blob}\n\n"
            "ONLY return JSON."
        )

        resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
        content = resp["message"].get("content", "").strip()
        try:
            data = json.loads(content)
        except Exception:
            data = json.loads(extract_json(content) or "{}")

        if not isinstance(data, dict):
            data = {}
        return data

    async def _extract_fps_table(self, llm_client, device: str, url_to_text: Dict[str, str]) -> Dict[str, Any]:
        docs = []
        for url, text in url_to_text.items():
            if text:
                docs.append(f"URL: {url}\n---\n{text}\n")
        blob = "\n\n====\n\n".join(docs)[:18000]

        schema = {
            "fps_by_game": {"<Game Title>": "<number> fps @ <resolution> (<settings/notes>)"},
            "_sources": list(url_to_text.keys())
        }

        prompt = (
            "From the documents below, extract PER-GAME FPS rows for the specified device.\n"
            "- ONLY include real video game titles (e.g., 'Cyberpunk 2077', 'Shadow of the Tomb Raider').\n"
            "- EXCLUDE generic categories like '1440p Gaming', '1080p Ultra', 'Productivity', 'Average FPS', etc.\n"
            "- Each value MUST include a NUMBER (fps) and may include resolution/settings notes. Examples:\n"
            '  { "Cyberpunk 2077": "62 fps @ 1080p High (FSR2 Quality)" }\n'
            '  { "Fortnite": "120 fps @ 1080p Performance Mode" }\n'
            "- If no per-game FPS is present, return an empty object for 'fps_by_game'.\n\n"
            f"Return ONLY JSON like:\n{json.dumps(schema, indent=2)}\n\n"
            f"DEVICE: {device}\n\n"
            f"DOCUMENTS:\n{blob}\n\n"
            "ONLY return JSON."
        )

        resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
        content = resp["message"].get("content", "").strip()
        try:
            data = json.loads(content)
        except Exception:
            data = json.loads(extract_json(content) or "{}")

        if not isinstance(data, dict):
            return {}

        fps = data.get("fps_by_game") or {}
        clean = {}
        for game, val in fps.items():
            if not isinstance(game, str) or not isinstance(val, str):
                continue
            has_digit = any(ch.isdigit() for ch in val)
            looks_like_game = (
                len(game.split()) >= 1
                and "gaming" not in game.lower()
                and "average" not in game.lower()
            )
            if has_digit and looks_like_game:
                clean[game.strip()] = val.strip()

        return {"fps_by_game": clean, "_sources": data.get("_sources", list(url_to_text.keys()))}

    def fetch_page_text(self, url: str) -> str:
        cfg = self._get_settings()
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/114.0.0.0 Safari/537.36"
            )
        }
        try:
            resp = requests.get(url, headers=headers, timeout=cfg["timeout"])
            if resp.status_code != 200:
                return ""
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(["script", "style", "header", "footer", "nav", "aside", "form"]):
                tag.decompose()
            container = soup.find("article") or soup.find("main") or soup.body
            if not container:
                return ""
            text = container.get_text(separator="\n")
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            return "\n".join(lines[:4000])
        except Exception as e:
            logger.error(f"DeviceCompare: fetch_page_text error: {e}")
            return ""

    # ---------- LLM helpers ----------
    def _format_results_for_llm(self, results: List[Dict[str, str]]) -> str:
        out = []
        for i, r in enumerate(results, 1):
            out.append(f"{i}. {r.get('title','No title')} - {r.get('href','')}\n   {r.get('snippet','')}")
        return "\n".join(out)

    # ---------- rows for images ----------
    def _spec_order(self):
        return [
            ("title", "Name"), ("brand", "Brand"), ("model", "Model"), ("release_date", "Release Date"),
            ("price", "Price"), ("cpu", "CPU"), ("gpu", "GPU"), ("ram", "RAM"), ("storage", "Storage"),
            ("display_size", "Display Size"), ("display_type", "Display Type"),
            ("resolution", "Resolution"), ("refresh_rate", "Refresh Rate"), ("hdr", "HDR"),
            ("ports", "Ports"), ("wireless", "Wireless"), ("battery", "Battery"), ("charging", "Charging"),
            ("os", "OS"), ("dimensions", "Dimensions"), ("weight", "Weight"),
        ]

    def _val(self, d, k):
        v = d.get(k, "")
        return v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)

    def _build_rows(self, specs_a: dict, specs_b: dict):
        rows = []
        for key, label in self._spec_order():
            va = self._val(specs_a, key)
            vb = self._val(specs_b, key)
            if va or vb:
                rows.append([label, va, vb])
        return rows

    def _build_fps_rows(self, specs_a: dict, specs_b: dict, max_rows: int):
        fps_a_raw = specs_a.get("fps_by_game") or {}
        fps_b_raw = specs_b.get("fps_by_game") or {}
        fps_a = fps_a_raw if isinstance(fps_a_raw, dict) else {}
        fps_b = fps_b_raw if isinstance(fps_b_raw, dict) else {}
        if not fps_a and not fps_b:
            return []
        games = sorted(set(fps_a.keys()) | set(fps_b.keys()))
        out = []
        for g in games[:max_rows]:
            ga = fps_a.get(g, "")
            gb = fps_b.get(g, "")
            if ga or gb:
                out.append([g, ga, gb])
        return out

    # ---------- image renderer (grey/orange theme) ----------
    def _render_table_image(
        self,
        headers: List[str],
        rows: List[List[str]],
        title: str | None = None,
        max_width: int = 1600
    ) -> bytes:

        PALETTE = {
            "bg":        (24, 24, 24),
            "card":      (36, 36, 36),
            "grid":      (56, 56, 56),
            "header_bg": (58, 58, 58),
            "accent":    (255, 122, 24),
            "text":      (230, 230, 230),
            "muted":     (180, 180, 180),
        }

        def _load_font(size=16, bold=False):
            return ImageFont.load_default()

        body_font   = _load_font(20, bold=False)
        header_font = _load_font(22, bold=True)
        title_font  = _load_font(26, bold=True)

        # helpers
        def text_width(draw, txt, font): 
            return draw.textlength(str(txt or ""), font=font)

        def text_height(draw, txt, font):
            box = draw.textbbox((0,0), str(txt or ""), font=font)
            return int(box[3]-box[1])

        # Word-wrap that respects pixel widths
        def wrap_text(draw, text, font, max_px):
            text = str(text or "")
            paragraphs = text.split("\n")
            out_lines = []
            for p in paragraphs:
                words = p.split()
                if not words:
                    out_lines.append("")
                    continue
                line = words[0]
                for w in words[1:]:
                    test = line + " " + w
                    if text_width(draw, test, font) <= max_px:
                        line = test
                    else:
                        if text_width(draw, w, font) > max_px:
                            out_lines.append(line)
                            chunk = ""
                            for ch in w:
                                if text_width(draw, chunk + ch, font) <= max_px:
                                    chunk += ch
                                else:
                                    out_lines.append(chunk)
                                    chunk = ch
                            line = chunk
                        else:
                            out_lines.append(line)
                            line = w
                out_lines.append(line)
            return out_lines

        tmp = Image.new("RGB", (10,10))
        d0 = ImageDraw.Draw(tmp)

        col_pad_x     = 16
        row_pad_y     = 10
        header_pad_y  = 12
        col_min_px    = 150
        line_spacing  = 4

        cols = len(headers)

        col_w = [max(col_min_px, int(text_width(d0, h, header_font) + col_pad_x*2)) for h in headers]
        for r in rows:
            for i, cell in enumerate(r[:cols]):
                col_w[i] = max(col_w[i], int(text_width(d0, cell, body_font) + col_pad_x*2))

        table_w = sum(col_w)
        if table_w > max_width:
            scale = max_width / table_w
            col_w = [max(col_min_px, int(w*scale)) for w in col_w]
            table_w = sum(col_w)

        wrapped_rows: list[list[list[str]]] = []
        row_heights: list[int] = []
        line_h = text_height(d0, "Ag", body_font)
        header_h = max(text_height(d0, h, header_font) for h in headers) + header_pad_y*2

        for r in rows:
            wrapped_row = []
            max_lines = 1
            for i in range(cols):
                cell = r[i] if i < len(r) else ""
                inner_width = max(0, col_w[i] - col_pad_x*2)
                lines = wrap_text(d0, cell, body_font, inner_width)
                wrapped_row.append(lines)
                max_lines = max(max_lines, len(lines))
            wrapped_rows.append(wrapped_row)
            row_heights.append(max_lines * (line_h + line_spacing) - line_spacing + row_pad_y*2)

        title_h = text_height(d0, title, title_font) + 18 if title else 0

        card_pad = 24
        grid_h = header_h + sum(row_heights)
        img_w = table_w + card_pad*2
        img_h = grid_h + card_pad*2 + title_h

        img = Image.new("RGB", (img_w, img_h), PALETTE["bg"])
        d = ImageDraw.Draw(img)

        def rr(xy, r, fill): d.rounded_rectangle(xy, radius=r, fill=fill)
        rr((card_pad, card_pad + title_h, card_pad+table_w, card_pad + title_h + grid_h), 18, PALETTE["card"])

        if title:
            d.text((card_pad, card_pad), title, font=title_font, fill=PALETTE["accent"])

        x = card_pad
        y = card_pad + title_h
        d.rectangle((x, y, x+table_w, y+header_h), fill=PALETTE["header_bg"])

        cx = x
        for w in col_w:
            d.line((cx, y, cx, y+grid_h), fill=PALETTE["grid"], width=1)
            cx += w
        d.line((x+table_w, y, x+table_w, y+grid_h), fill=PALETTE["grid"],  width=1)
        d.line((x, y+header_h, x+table_w, y+header_h), fill=PALETTE["grid"], width=1)

        ry = y + header_h
        for rh in row_heights:
            d.line((x, ry+rh, x+table_w, ry+rh), fill=PALETTE["grid"], width=1)
            ry += rh

        cx = x
        for i, h in enumerate(headers):
            hh = text_height(d0, h, header_font)
            d.text((cx + col_pad_x, y + (header_h - hh)//2), str(h), font=header_font, fill=PALETTE["muted"])
            cx += col_w[i]

        ry = y + header_h
        for r_idx, wrapped_row in enumerate(wrapped_rows):
            cx = x
            rh = row_heights[r_idx]
            inner_top = ry + row_pad_y
            for i in range(cols):
                lines = wrapped_row[i]
                ly = inner_top
                for line in lines:
                    d.text((cx + col_pad_x, ly), line, font=body_font, fill=PALETTE["text"])
                    ly += line_h + line_spacing
                cx += col_w[i]
            ry += rh

        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
    
    async def _should_fetch_fps(self, llm_client, device: str, specs: Dict[str, Any]) -> bool:
        summary = {
            "device": device,
            "title": specs.get("title", ""),
            "brand": specs.get("brand", ""),
            "model": specs.get("model", ""),
            "cpu": specs.get("cpu", ""),
            "gpu": specs.get("gpu", ""),
            "ram": specs.get("ram", ""),
            "display_size": specs.get("display_size", ""),
            "display_type": specs.get("display_type", ""),
            "resolution": specs.get("resolution", ""),
            "refresh_rate": specs.get("refresh_rate", ""),
            "ports": specs.get("ports", ""),
            "os": specs.get("os", ""),
            "category_hint": "",
        }

        first, last = get_tater_name()
        prompt = (
            f"You are {first} {last}. Decide whether it makes sense to search for *per-game FPS benchmarks* "
            "for the device below. Say true if it's likely a gaming-capable PC/laptop/mini PC/handheld or a game console, "
            "or has a discrete/console-class GPU. Say false for phones, tablets, TVs, NAS, routers, office-only mini PCs, "
            "or devices without game-capable GPUs. Return ONLY JSON in this exact shape:\n\n"
            "{\n"
            '  "function": "decide",\n'
            '  "arguments": {"allow_fps": true|false, "reason": "<short>"}\n'
            "}\n\n"
            f"Device summary:\n{json.dumps(summary, ensure_ascii=False, indent=2)}"
        )

        try:
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            content = resp["message"].get("content", "").strip()
            try:
                js = json.loads(content)
            except Exception:
                js = json.loads(extract_json(content) or "{}")
            allow = (js.get("arguments", {}) or {}).get("allow_fps")
            return bool(allow) if isinstance(allow, bool) else False
        except Exception:
            return False

    @staticmethod
    def _extract_devices(args: Dict[str, Any]) -> Tuple[str, str]:
        args = args or {}

        def first_value(*keys: str) -> str:
            for key in keys:
                value = args.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            return ""

        device_a = first_value("device_a", "first_device", "left_device", "device1", "a")
        device_b = first_value("device_b", "second_device", "right_device", "device2", "b")

        if (not device_a or not device_b) and isinstance(args.get("devices"), list):
            pair = [str(x).strip() for x in args.get("devices") if str(x).strip()]
            if len(pair) >= 2:
                if not device_a:
                    device_a = pair[0]
                if not device_b:
                    device_b = pair[1]

        if not device_a or not device_b:
            compare_text = args.get("query") or args.get("compare")
            if isinstance(compare_text, str) and compare_text.strip():
                parts = re.split(r"\s+(?:vs\.?|versus|v)\s+", compare_text.strip(), maxsplit=1, flags=re.IGNORECASE)
                if len(parts) == 2:
                    left = parts[0].strip()
                    right = parts[1].strip()
                    if not device_a and left:
                        device_a = left
                    if not device_b and right:
                        device_b = right

        return device_a, device_b

    # ---------- main pipeline ----------
    async def _pipeline(self, device_a: str, device_b: str, llm_client) -> Dict[str, Any]:
        cfg = self._get_settings()
        if not cfg["api_key"] or not cfg["cx"]:
            return {"error": "Search is not configured. Please set Google API Key and CX in WebUI Settings > Web Search."}
        if llm_client is None:
            return {"error": "Device Compare requires an available LLM client to extract specs and FPS data."}

        results_a = self.google_search(f"{device_a} official hardware specifications tech specs", cfg["spec_results"])
        results_b = self.google_search(f"{device_b} official hardware specifications tech specs", cfg["spec_results"])
        if not results_a or not results_b:
            return {"error": "No results found for one or both devices."}

        specs_a, spec_src_a = await self._pick_specs_with_retries(llm_client, device_a, results_a, max_attempts=3)
        specs_b, spec_src_b = await self._pick_specs_with_retries(llm_client, device_b, results_b, max_attempts=3)

        fps_sources_a = []
        fps_sources_b = []
        fps_rows = []

        if cfg["enable_fps"]:
            should_a = await self._should_fetch_fps(llm_client, device_a, specs_a)
            should_b = await self._should_fetch_fps(llm_client, device_b, specs_b)

            if should_a:
                fps_q_a = f'{device_a} gaming fps'
                fps_res_a = self.google_search(fps_q_a, cfg["fps_results"]) or []
                fps_a, fps_src_a = await self._pick_fps_with_retries(
                    llm_client, device_a, fps_res_a, max_attempts=3
                )
                specs_a["fps_by_game"] = fps_a.get("fps_by_game", {})
                fps_sources_a = fps_src_a

            if should_b:
                fps_q_b = f'{device_b} gaming fps'
                fps_res_b = self.google_search(fps_q_b, cfg["fps_results"]) or []
                fps_b, fps_src_b = await self._pick_fps_with_retries(
                    llm_client, device_b, fps_res_b, max_attempts=3
                )
                specs_b["fps_by_game"] = fps_b.get("fps_by_game", {})
                fps_sources_b = fps_src_b

            if (specs_a.get("fps_by_game") and any(specs_a["fps_by_game"].values())) or \
               (specs_b.get("fps_by_game") and any(specs_b["fps_by_game"].values())):
                fps_rows = self._build_fps_rows(specs_a, specs_b, cfg["max_fps_rows"])

        specs_a.setdefault("title", device_a)
        specs_b.setdefault("title", device_b)
        specs_a.setdefault("_sources", spec_src_a)
        specs_b.setdefault("_sources", spec_src_b)

        spec_rows = self._build_rows(specs_a, specs_b)

        sources_lines = []
        if specs_a.get("_sources"):     sources_lines.append("- Device A (specs): " + "; ".join(specs_a["_sources"]))
        if fps_sources_a:               sources_lines.append("- Device A (FPS): "   + "; ".join(fps_sources_a))
        if specs_b.get("_sources"):     sources_lines.append("- Device B (specs): " + "; ".join(specs_b["_sources"]))
        if fps_sources_b:               sources_lines.append("- Device B (FPS): "   + "; ".join(fps_sources_b))
        sources_text = "\n".join(sources_lines)

        return {
            "spec_headers": ["Spec", "Device A", "Device B"],
            "spec_rows": spec_rows,
            "fps_headers": ["Game", "Device A", "Device B"] if fps_rows else None,
            "fps_rows": fps_rows,
            "title": f"{specs_a.get('title','Device A')} vs {specs_b.get('title','Device B')}",
            "sources_text": sources_text
        }

    # ---------- platform helpers ----------
    def _img_payload(self, png_bytes: bytes, name: str = "image.png") -> Dict[str, Any]:
        """Return a cross-platform image payload (Matrix/Discord/WebUI compatible)."""
        return _build_media_metadata(
            png_bytes,
            media_type="image",
            name=name,
            mimetype="image/png",
        )

    async def _handle_compare(self, args: Dict[str, Any], llm_client):
        device_a, device_b = self._extract_devices(args or {})
        if not device_a or not device_b:
            return ['Please provide two devices: {"device_a": "...", "device_b": "..."}']

        data = await self._pipeline(device_a, device_b, llm_client)
        if "error" in data:
            return [data["error"]]

        out = []
        spec_png = self._render_table_image(
            headers=data["spec_headers"],
            rows=data["spec_rows"],
            title=data["title"]
        )
        out.append(self._img_payload(spec_png, "comparison.png"))

        if data.get("fps_rows"):
            fps_png = self._render_table_image(
                headers=data["fps_headers"],
                rows=data["fps_rows"],
                title="Per-Game FPS"
            )
            out.append(self._img_payload(fps_png, "fps.png"))

        if data.get("sources_text"):
            out.append("**Sources**\n" + data["sources_text"])

        return out

    # ---------- platform handlers ----------
    async def handle_discord(self, message, args, llm_client):
        return await self._handle_compare(args or {}, llm_client)

    async def handle_webui(self, args, llm_client):
        return await self._handle_compare(args or {}, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        return await self._handle_compare(args or {}, llm_client)

    # ---------- Matrix ----------
    async def handle_matrix(self, client, room, sender, body, args, llm_client=None, **kwargs):
        if llm_client is None:
            llm_client = kwargs.get("llm") or kwargs.get("ll_client") or kwargs.get("llm_client")
        return await self._handle_compare(args or {}, llm_client)

    async def handle_irc(self, bot, channel, user, raw_message, args, llm_client):
        device_a, device_b = self._extract_devices(args or {})
        if not device_a or not device_b:
            return ['Please provide two devices: {"device_a": "...", "device_b": "..."}']

        return [f"Image comparison for '{device_a}' vs '{device_b}' is not supported on IRC."]

plugin = DeviceComparePlugin()
