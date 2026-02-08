import asyncio, json, logging, re, time, difflib, requests, mimetypes
from urllib.parse import urlparse, urlunparse
from dotenv import load_dotenv
from plugin_base import ToolPlugin
from helpers import redis_client, extract_json  # <-- we use your extractor
from plugin_diagnostics import combine_diagnosis, diagnose_hash_fields, diagnose_redis_keys, needs_from_diagnosis
from plugin_result import action_failure, action_success

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

load_dotenv()
logger = logging.getLogger("mister_remote")
logger.setLevel(logging.INFO)

def _decode_text(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "ignore")
    return "" if value is None else str(value)


def _strip(s): return _decode_text(s).strip()
def _norm(s):  return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

# Lightweight synonyms (used only as a tiny bias if the LLM can't decide)
SYN = {
    "snes":"SNES","supernintendo":"SNES","supernes":"SNES","superfamicom":"SNES",
    "nes":"NES","nintendo":"NES",
    "genesis":"Genesis","megadrive":"Genesis","smd":"Genesis",
    "psx":"PSX","playstation":"PSX","ps1":"PSX",
    "gba":"GBA","gb":"Gameboy","gameboy":"Gameboy",
    "gbc":"GameboyColor","gameboycolor":"GameboyColor",
    "sms":"MasterSystem","mastersystem":"MasterSystem",
    "s32x":"Sega32X","32x":"Sega32X",
    "tg16":"TurboGrafx16","pcengine":"TurboGrafx16",
    "megacd":"MegaCD","segacd":"MegaCD",
    "n64":"Nintendo64","neogeo":"NeoGeo","neo-geo":"NeoGeo",
}

COMMAND_ALIASES = {
    "play": "play",
    "launch": "play",
    "start": "play",
    "now_playing": "now_playing",
    "now playing": "now_playing",
    "status": "now_playing",
    "what's_playing": "now_playing",
    "whats_playing": "now_playing",
    "go_to_menu": "go_to_menu",
    "menu": "go_to_menu",
    "home": "go_to_menu",
    "screenshot_take": "screenshot_take",
    "screenshot": "screenshot_take",
    "take_screenshot": "screenshot_take",
    "screen_shot": "screenshot_take",
    "shot": "screenshot_take",
}

class MisterRemotePlugin(ToolPlugin):
    name = "mister_remote"
    plugin_name = "MiSTer Remote"
    version = "1.1.0"
    min_tater_version = "50"
    pretty_name = "MiSTer Remote"
    description = (
        "Control MiSTer via the MiSTer Remote API.\n"
        "ALWAYS include the user's FULL original message in the `utterance` field when calling this tool.\n"
        "Examples users might say: 'play super mario 3 on mister', 'what’s playing?', 'go to menu', 'take a screenshot'."
    )
    plugin_dec = "Control your MiSTer FPGA setup\u2014launch games, check status, or take screenshots."

    platforms = ["discord", "webui", "irc", "homeassistant", "matrix", "homekit", "telegram"]

    usage = (
        "{\n"
        '  "function": "mister_remote",\n'
        '  "arguments": {\n'
        '    "command": "<play|now_playing|go_to_menu|screenshot_take>",\n'
        '    "utterance": "<FULL original user message (required for play)>"\n'
        "  }\n"
        "}\n"
        "\n"
        "Example:\n"
        "{\n"
        '  "function": "mister_remote",\n'
        '  "arguments": {\n'
        '    "command": "play",\n'
        '    "utterance": "play super mario 3 on mister"\n'
        "  }\n"
        "}\n"
    )

    settings_category = "MiSTer Remote"
    required_settings = {
        "MISTER_HOST": {
            "label": "MiSTer Host (http://IP or hostname)",
            "type": "string",
            "default": "http://10.4.20.167",
        },
        "MISTER_PORT": {
            "label": "MiSTer Remote Port",
            "type": "string",
            "default": "8182",
        },
    }

    waiting_prompt_template = "Talking to your MiSTer now…"

    def _settings(self) -> dict:
        return redis_client.hgetall("plugin_settings:MiSTer Remote") or {}

    def _normalize_host(self, raw_host: str) -> str:
        host = _strip(raw_host).rstrip("/")
        if not host:
            host = "http://10.4.20.167"
        if not host.startswith("http://") and not host.startswith("https://"):
            host = f"http://{host}"
        return host

    def _normalize_port(self, raw_port: str) -> str:
        digits = re.sub(r"\D+", "", _strip(raw_port))
        return digits or "8182"

    def _compose_root(self, host: str, port: str) -> str:
        parsed = urlparse(host)
        has_port = False
        try:
            has_port = parsed.port is not None
        except ValueError:
            has_port = ":" in (parsed.netloc or "")
        netloc = parsed.netloc
        if not has_port:
            netloc = f"{netloc}:{port}"
        clean = parsed._replace(netloc=netloc, path="", params="", query="", fragment="")
        return urlunparse(clean).rstrip("/")

    def _extract_utterance(self, args: dict | None) -> str:
        data = args or {}
        for key in (
            "utterance",
            "user request",
            "user_request",
            "user_text",
            "prompt",
            "query",
            "request",
            "text",
            "message",
        ):
            value = _strip(data.get(key, ""))
            if value:
                return value
        return ""

    def _normalize_command(self, raw_cmd: str) -> str:
        key = _norm(raw_cmd).replace("whats", "what's")
        # Preserve underscores from canonical command values before alias lookup.
        direct = _strip(raw_cmd).lower()
        if direct in COMMAND_ALIASES:
            return COMMAND_ALIASES[direct]
        if key == "nowplaying":
            return "now_playing"
        if key == "gotomenu":
            return "go_to_menu"
        if key in {"screenshottake", "takescreenshot", "screenshot"}:
            return "screenshot_take"
        if key in {"play", "launch", "start"}:
            return "play"
        return direct

    def _infer_command_from_utterance(self, utterance: str) -> str:
        text = _strip(utterance).lower()
        if not text:
            return ""
        if re.search(r"\b(screenshot|screen ?shot|capture)\b", text):
            return "screenshot_take"
        if re.search(r"\b(now playing|what('?| i)s playing|status)\b", text):
            return "now_playing"
        if re.search(r"\b(menu|go to menu|main menu|home)\b", text):
            return "go_to_menu"
        if re.search(r"\b(play|launch|start)\b", text):
            return "play"
        return ""

    @staticmethod
    def _normalize_systems(items) -> list[dict]:
        out = []
        for item in items or []:
            if isinstance(item, dict):
                system_id = _strip(item.get("id", ""))
                if not system_id:
                    continue
                out.append({"id": system_id, "name": _strip(item.get("name", ""))})
                continue
            system_id = _strip(item)
            if system_id:
                out.append({"id": system_id, "name": system_id})
        return out

    @staticmethod
    def _screenshot_marker(shot: dict | None) -> tuple[str, str]:
        if not isinstance(shot, dict):
            return ("", "")
        return (_strip(shot.get("filename", "")), _strip(shot.get("modified", "")))

    # ---------------- HTTP helpers ----------------
    def _base(self):
        st = self._settings()
        host = self._normalize_host(st.get("MISTER_HOST", "http://10.4.20.167"))
        port = self._normalize_port(st.get("MISTER_PORT", "8182"))
        return f"{self._compose_root(host, port)}/api"

    def _root(self):
        # base WITHOUT /api (used to build absolute public URLs)
        st = self._settings()
        host = self._normalize_host(st.get("MISTER_HOST", "http://10.4.20.167"))
        port = self._normalize_port(st.get("MISTER_PORT", "8182"))
        return self._compose_root(host, port)

    def _get(self, path, timeout=8):
        r = requests.get(self._base() + path, timeout=timeout)
        r.raise_for_status()
        ct = (r.headers.get("Content-Type") or "").lower()
        return r.json() if "application/json" in ct or r.text[:1] in "{[" else r.text

    def _post(self, path, payload=None, timeout=12):
        url = self._base() + path
        r = requests.post(url, json=payload if payload is not None else None, timeout=timeout)
        r.raise_for_status()
        try:
            return r.json()
        except:
            return {"status": r.status_code, "ok": (r.status_code == 200)}

    # ---------------- API wrappers ----------------
    def _systems(self):          return self._get("/systems") or []
    def _search(self, q, sysid): return self._post("/games/search", {"query": q, "system": sysid or ""})
    def _launch(self, path):     return self._post("/games/launch", {"path": path})
    def _playing(self):          return self._get("/games/playing")
    def _menu(self):             return self._post("/launch/menu")
    def _shot(self):             return self._post("/screenshots")
    def _screenshots(self):      return self._get("/screenshots")  # may be [] or "null"

    # ---------------- Caching ---------------------
    def _systems_cache(self):
        key = "mister_remote:systems"
        cached = redis_client.get(key)
        if cached:
            try:
                return self._normalize_systems(json.loads(cached))
            except: pass
        items = self._normalize_systems(self._systems())
        try:
            redis_client.setex(key, 600, json.dumps(items))
        except: pass
        return items

    # ---------------- Screenshot helpers ----------
    def _latest_screenshot(self):
        """
        Returns newest screenshot dict or None.
        Dict: {game, filename, path, core, modified}
        """
        try:
            lst = self._screenshots()
            if not lst or lst == "null":
                return None
            def keyer(it):
                return (it.get("modified") or "", it.get("filename") or "")
            return sorted(lst, key=keyer, reverse=True)[0]
        except Exception:
            return None

    def _screenshot_url(self, core, filename):
        return f"{self._root()}/api/screenshots/{core}/{requests.utils.quote(filename)}"

    def _fetch_screenshot_bytes(self, core, filename):
        try:
            url = self._screenshot_url(core, filename)
            r = requests.get(url, timeout=8)
            r.raise_for_status()
            return r.content
        except Exception:
            return None

    def _mime_for(self, filename):
        mime, _ = mimetypes.guess_type(filename)
        return mime or "image/png"

    # ---------------- Parsing helpers -------------
    def _parse_play_utterance(self, utterance: str):
        """
        Extract game-ish query from phrases like: 'play super mario 3 on mister'
        We deliberately ignore the word 'mister' to avoid biasing the system detection.
        """
        u = (utterance or "").strip().lower()
        if not u: return ("", None)

        u = re.sub(r"\bmister\b", "", u)  # Remove 'mister'
        m = re.search(r"\bon\s+(.+)$", u)  # capture " on <system>"
        system_text = m.group(1) if m else None

        title = re.sub(r"^(hey|ok)\s+\w+\s*,?\s*", "", u)
        title = re.sub(r"^(please|can you|could you|would you)\s+", "", title)
        title = re.sub(r"^(play|launch|start)\s+", "", title)

        if system_text:
            cut = title.rfind(" on ")
            if cut != -1:
                title = title[:cut].strip()

        title = re.sub(r"\s+(please|thanks|thank you)$", "", title).strip()
        title = re.sub(r"\s+on$", "", title).strip()  # strip dangling 'on'
        return (title, system_text)

    # ---------------- LLM pickers / normalizers ---
    async def _ai_normalize_title(self, llm_client, utterance: str) -> str | None:
        if not llm_client:
            return None
        prompt = (
            "Extract ONLY the clean game title the user wants to play.\n"
            "Rules:\n"
            "1) Ignore the word 'mister' entirely.\n"
            "2) If the phrase contains 'on <something>', drop that suffix.\n"
            "3) Remove helper words like 'play', 'please', etc.\n"
            "4) Respond ONLY with JSON exactly like: { \"title\": \"<clean title>\" }\n\n"
            f'User said: "{utterance}"\n'
        )
        try:
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            raw = (resp["message"].get("content") or "").strip()
            jtxt = extract_json(raw) or raw
            data = json.loads(jtxt)
            title = _strip(data.get("title", ""))
            return title or None
        except Exception:
            return None

    async def _ai_pick_system(self, llm_client, utterance: str, systems: list[str]) -> str | None:
        if not llm_client:
            return None
        sys_lines = "\n".join([f"- {s['id']} — {s.get('name','')}" for s in systems])
        prompt = (
            "You pick the best MiSTer system ID for launching a game.\n"
            "Rules:\n"
            "1) Use ONLY this list of system IDs.\n"
            "2) Ignore the word 'mister' if present in the user's text.\n"
            "3) If uncertain, choose the most likely classic platform.\n"
            "Respond ONLY with JSON:\n"
            '{ "systemId": "<one of the IDs exactly>" }\n\n'
            f"Systems:\n{sys_lines}\n\n"
            f'User said: "{utterance}"\n'
        )
        try:
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            raw = (resp["message"].get("content") or "").strip()
            jtxt = extract_json(raw) or raw
            data = json.loads(jtxt)
            cand = _strip(data.get("systemId", ""))
            live_ids = {s["id"] for s in systems}
            return cand if cand in live_ids else None
        except Exception as e:
            logger.warning(f"[mister_remote] _ai_pick_system fallback: {e}")
            return None

    async def _ai_pick_game(self, llm_client, title_query: str, system_id: str, results: list[dict]) -> dict | None:
        if not llm_client:
            return None
        short = results[:10]
        choices = "\n".join([
            f'- name="{r.get("name","")}"  path="{r.get("path","")}"'
            for r in short
        ])
        prompt = (
            "Pick the best game to launch for the user's request.\n"
            "Rules:\n"
            "1) Choose exactly ONE item from the provided list.\n"
            "2) Respond ONLY with JSON of the chosen absolute path.\n"
            '   Format: { "path": "<absolute path exactly as shown>" }\n'
            "3) Prefer exact/near-exact title matches; otherwise choose the closest canonical entry.\n"
            "4) Do not invent or modify paths.\n\n"
            f"System: {system_id}\n"
            f"User title query: {title_query}\n"
            f"Choices:\n{choices}\n"
        )
        try:
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            raw = (resp["message"].get("content") or "").strip()
            jtxt = extract_json(raw) or raw
            data = json.loads(jtxt)
            p = _strip(data.get("path", ""))
            if not p: return None
            for r in short:
                if r.get("path") == p:
                    return r
            return None
        except Exception as e:
            logger.warning(f"[mister_remote] _ai_pick_game fallback: {e}")
            return None

    async def _ai_followup(self, llm_client, game_name: str, system_name: str) -> str | None:
        if not llm_client:
            return None
        prompt = (
            "Write ONE short, upbeat sentence reacting to the game that was launched.\n"
            "Rules:\n"
            "- Mention the game title and system.\n"
            "- You MAY include a well-known non-controversial tidbit if you're confident; otherwise keep it generic.\n"
            "- NO made-up dates, devs, or specific trivia unless you're sure.\n"
            "- Avoid markdown links. Emojis allowed sparingly.\n"
            "- Max ~25 words.\n\n"
            f'Game: "{game_name}"\n'
            f'System: "{system_name}"\n'
            "Reply with plain text only."
        )
        try:
            resp = await llm_client.chat(messages=[{"role": "system", "content": prompt}])
            line = (resp["message"].get("content") or "").strip()
            line = re.sub(r"\s+", " ", line)
            return line[:240]
        except Exception:
            return None

    # ---------------- Heuristic fallbacks --------
    def _resolve_system_syn(self, text, systems):
        if not text: return None
        norm = _norm(text)
        if norm in SYN: return SYN[norm]
        ids = [s["id"] for s in systems]
        names = [s.get("name","") for s in systems]
        candidates = ids + names
        best = difflib.get_close_matches(text, candidates, n=1, cutoff=0.6)
        if best:
            b = best[0]
            for s in systems:
                if s["id"] == b or s.get("name") == b:
                    return s["id"]
        return None

    # ---------------- Orchestration --------------
    async def _do_play_ai(self, utterance: str, llm_client):
        parsed_title, system_text = self._parse_play_utterance(utterance)
        clean_title = await self._ai_normalize_title(llm_client, utterance) if llm_client else None
        title = (clean_title or parsed_title or "").strip()
        if title.endswith(" on"):
            title = title[:-3].strip()

        if not title:
            return "I didn’t catch the game title. Try: “play super mario on super nintendo.”"

        systems = self._systems_cache()
        if not systems:
            return "No MiSTer systems found."

        sys_id = await self._ai_pick_system(llm_client, utterance, systems) if llm_client else None
        if not sys_id and system_text:
            sys_id = self._resolve_system_syn(system_text, systems)
        if not sys_id:
            sys_id = self._resolve_system_syn(title, systems)
        if not sys_id:
            top = ", ".join([s["id"] for s in systems[:10]])
            return f"I couldn’t determine the system. Try saying it explicitly (e.g., 'on SNES'). Here are some: {top}"

        res = self._search(title, sys_id)
        items = (res or {}).get("data") or []
        if not items:
            res_all = self._search(title, "")
            items = (res_all or {}).get("data") or []
            if items:
                items = sorted(items, key=lambda it: it.get("system", {}).get("id") != sys_id)

        if not items:
            return f'No matches for "{title}" on {sys_id}.'

        chosen = await self._ai_pick_game(llm_client, title, sys_id, items) if llm_client else None
        if not chosen:
            chosen = items[0]

        path = chosen.get("path")
        if not path:
            return f'Found matches for "{title}" but no valid path.'

        self._launch(path)
        time.sleep(0.3)
        now = self._playing() or {}
        sys_name = now.get("systemName") or chosen.get("system", {}).get("name") or sys_id
        game_name = now.get("gameName") or chosen.get("name") or title

        msg = f"Launching **{game_name}** on **{sys_name}**."
        follow = await self._ai_followup(llm_client, game_name, sys_name) if llm_client else None
        if follow:
            msg = f"{msg}\n{follow}"
        return msg

    def _capture_screenshot(self):
        self._shot()

        # Poll up to ~2.5s total (5 * 0.5s) for the file bytes to be available.
        filename = None
        core = None
        content = None
        tries = 5
        for _ in range(tries):
            time.sleep(0.5)
            shot = self._latest_screenshot()
            if not shot:
                continue
            filename = shot.get("filename") or "mister_screenshot.png"
            core = shot.get("core") or ""
            content = self._fetch_screenshot_bytes(core, filename)
            if content:
                break

        return content, filename, core

    def _build_screenshot_payload(self, content, filename):
        mimetype = self._mime_for(filename)
        image_payload = _build_media_metadata(
            content,
            media_type="image",
            name=filename,
            mimetype=mimetype,
        )
        caption = f"Screenshot captured: `{filename}`"
        return [image_payload, caption]

    def _do_screenshot_and_payload(self):
        content, filename, core = self._capture_screenshot()

        # If we got the bytes: return image payload + a short caption (no markdown image, no URL)
        if content and filename:
            return self._build_screenshot_payload(content, filename)

        # Fallback: still no bytes—return a single, simple line with the absolute URL.
        # Keep it as a LIST so Matrix/WebUI treat it like a normal tool response.
        if filename and core is not None:
            url = self._screenshot_url(core, filename)
            return [f"Screenshot captured: `{filename}` — {url}"]

        return ["Screenshot requested, but I couldn’t find the new file yet."]

    def _siri_flatten(self, out):
        # Prefer a plain string; if list (e.g., screenshot result), pick a text caption.
        if isinstance(out, list):
            for item in out:
                if isinstance(item, str):
                    out = item
                    break
            else:
                return "Done."
        if not isinstance(out, str):
            out = str(out)

        # Strip simple markdown so Siri reads cleanly.
        out = re.sub(r"[`*_]{1,3}", "", out)
        out = re.sub(r"\s+", " ", out).strip()
        # Keep it short-ish for TTS; tweak if you like.
        return out[:350]

    def _diagnosis(self) -> dict:
        hash_diag = diagnose_hash_fields(
            "plugin_settings:MiSTer Remote",
            fields={"mister_host": "MISTER_HOST", "mister_port": "MISTER_PORT"},
            validators={
                "mister_host": lambda v: v.startswith("http://") or v.startswith("https://"),
                "mister_port": lambda v: v.isdigit(),
            },
        )
        key_diag = diagnose_redis_keys(
            keys={"mister_host": "tater:mister:host", "mister_port": "tater:mister:port"},
            validators={
                "mister_host": lambda v: v.startswith("http://") or v.startswith("https://"),
                "mister_port": lambda v: v.isdigit(),
            },
        )
        return combine_diagnosis(hash_diag, key_diag)

    def _to_contract(self, raw, args: dict) -> dict:
        cmd = _strip((args or {}).get("command", ""))
        utterance = _strip((args or {}).get("utterance", ""))

        if isinstance(raw, dict) and "ok" in raw:
            return raw

        if isinstance(raw, list):
            artifacts = [x for x in raw if isinstance(x, dict) and x.get("type") in ("image", "audio", "video", "file")]
            texts = [x for x in raw if isinstance(x, str)]
            facts = {
                "command": cmd,
                "utterance": utterance,
                "artifact_count": len(artifacts),
            }
            if texts:
                facts["messages"] = texts[:3]
            return action_success(
                facts=facts,
                say_hint="Confirm what MiSTer did using these facts only.",
                suggested_followups=["Want another game or command?"],
                artifacts=artifacts,
            )

        msg = str(raw or "").strip()
        low = msg.lower()
        if not msg:
            return action_failure(
                code="empty_result",
                message="MiSTer remote did not return a response.",
                diagnosis=self._diagnosis(),
                needs=["What game or command should I run on MiSTer?"],
                say_hint="Explain that no output was returned and ask for the specific MiSTer action.",
            )

        if low.startswith("unknown command"):
            return action_failure(
                code="unknown_command",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=["Use one of: play, now_playing, go_to_menu, screenshot_take."],
                say_hint="Explain valid command choices and ask which one the user wants.",
            )

        if low.startswith("mister http error") or low.startswith("failed:"):
            diagnosis = self._diagnosis()
            needs = needs_from_diagnosis(
                diagnosis,
                {
                    "mister_host": "Please confirm the MiSTer host URL in plugin settings.",
                    "mister_port": "Please confirm the MiSTer API port in plugin settings.",
                },
            )
            return action_failure(
                code="mister_request_failed",
                message=msg,
                diagnosis=diagnosis,
                needs=needs,
                say_hint="Explain that MiSTer communication failed and ask for the missing/invalid settings.",
            )

        if "i couldn" in low or "no matches" in low:
            return action_failure(
                code="not_found",
                message=msg,
                diagnosis=self._diagnosis(),
                needs=["What exact game title or system should I use?"],
                say_hint="Explain what couldn't be matched and ask for a clearer game/system request.",
            )

        return action_success(
            facts={"command": cmd, "utterance": utterance, "result": msg},
            say_hint="Confirm the MiSTer result in a short factual sentence.",
            suggested_followups=["Want me to launch another title?"],
        )

    async def _handle_structured(self, args: dict, llm_client):
        out = await self._handle_async(args, llm_client)
        return self._to_contract(out, args or {})

    # ---------------- Dispatcher -----------------
    async def _handle_async(self, args: dict, llm_client):
        cmd = _strip((args or {}).get("command", ""))

        # Accept multiple keys + be resilient
        utt = _strip((args or {}).get("utterance", "")) \
              or _strip((args or {}).get("user request", "")) \
              or _strip((args or {}).get("user_request", ""))

        try:
            if cmd == "play":
                return await self._do_play_ai(utt, llm_client)

            if cmd == "now_playing":
                now = self._playing() or {}
                if not (now.get("system") or now.get("gameName")):
                    return "Nothing is currently running."
                sys_name = now.get('systemName', now.get('system',''))
                game_name = now.get('gameName','')
                msg = f"Now playing: **{game_name}** on **{sys_name}**."
                follow = await self._ai_followup(llm_client, game_name, sys_name) if (llm_client and game_name) else None
                if follow:
                    msg = f"{msg}\n{follow}"
                return msg

            if cmd == "go_to_menu":
                self._menu()
                return "Back to the MiSTer menu."

            if cmd == "screenshot_take":
                return self._do_screenshot_and_payload()

            # Fallback: infer play if utterance looks like a play request
            if not cmd and utt and re.search(r"\b(play|launch|start)\b", utt.lower()):
                return await self._do_play_ai(utt, llm_client)

            return "Unknown command. Try: play, now_playing, go_to_menu, or screenshot_take."

        except requests.HTTPError as e:
            return f"MiSTer HTTP error: {e.response.status_code} {e.response.text[:200]}"
        except Exception as e:
            logger.exception("[mister_remote] failure")
            return f"Failed: {e}"

    # ---------------- Platform wrappers ----------
    async def handle_discord(self, message, args, llm_client):
        args = args or {}
        if not _strip(args.get("utterance","")):
            args["utterance"] = (getattr(message, "content", "") or "").strip()
        return await self._handle_structured(args, llm_client)

    async def handle_webui(self, args, llm_client):
        args = args or {}
        if not _strip(args.get("utterance","")):
            args["utterance"] = _strip(args.get("user_text",""))
        return await self._handle_structured(args, llm_client)

    async def handle_irc(self, bot, channel, user, raw, args, llm_client):
        args = args or {}
        if not _strip(args.get("utterance","")):
            args["utterance"] = (raw or "").strip()
        return await self._handle_structured(args, llm_client)

    async def handle_homeassistant(self, args, llm_client):
        args = args or {}
        return await self._handle_structured(args, llm_client)

    async def handle_matrix(self, client, room, sender, body, args, llm_client):
        args = args or {}
        if not _strip(args.get("utterance","")):
            args["utterance"] = (body or "").strip()
        return await self._handle_structured(args, llm_client)

    async def handle_telegram(self, update, args, llm_client):
        args = args or {}
        if not _strip(args.get("utterance", "")):
            text = ""
            try:
                if isinstance(update, dict):
                    msg = update.get("message") or {}
                    text = msg.get("text") or msg.get("caption") or ""
            except Exception:
                text = ""
            args["utterance"] = (text or "").strip()
        return await self._handle_structured(args, llm_client)

    async def handle_homekit(self, args, llm_client):
        args = args or {}
        out = await self._handle_structured(args, llm_client)
        return out

plugin = MisterRemotePlugin()
