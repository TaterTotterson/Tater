# plugins/music_assistant.py
import json
import asyncio
import logging
import re
from typing import Any, Dict, List, Optional

import requests
import aiohttp
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client, get_tater_name

load_dotenv()
logger = logging.getLogger("music_assistant")
logger.setLevel(logging.INFO)


def _decode_redis_map(m: Dict[Any, Any]) -> Dict[str, str]:
    """redis hgetall often returns bytes; normalize to plain str."""
    out: Dict[str, str] = {}
    for k, v in (m or {}).items():
        if isinstance(k, (bytes, bytearray)):
            k = k.decode("utf-8", "ignore")
        else:
            k = str(k)

        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        else:
            v = "" if v is None else str(v)

        out[k] = v
    return out


class RoomPlayerNotFound(RuntimeError):
    """Raised when a requested room cannot be mapped to a media_player entity."""
    pass


class MusicAssistantPlugin(ToolPlugin):
    name = "music_assistant"
    plugin_name = "Music Assistant"
    version = "1.0.5"
    min_tater_version = "50"

    usage = (
        "{\n"
        '  "function": "music_assistant",\n'
        '  "arguments": {\n'
        '    "request": "<what the user wants to do, e.g. play reggae, play stick figure, stop, next, volume 40>",\n'
        '    "room": "<room name like Kitchen>"\n'
        "  }\n"
        "}\n"
    )

    description = (
        "Play music and control playback via Music Assistant in Home Assistant."
        "Supports play/queue/pause/resume/stop/next/previous/volume. "
        "Some devices provide room context in the system prompt, use it for room unless the user has specified a room"
        "If room is truly unknown, ask the user where to play."
    )

    plugin_dec = "Play music and control playback via Music Assistant in Home Assistant."
    pretty_name = "Controlling Music"
    settings_category = "Music Assistant"

    required_settings = {
        "MA_CONFIG_ENTRY_ID": {
            "label": "Music Assistant config_entry_id",
            "type": "string",
            "default": "",
        },
        "ROOM_MAP": {
            "label": "Room → Media Player Map (optional)",
            "type": "textarea",
            "default": "",
            "rows": 10,
            "description": (
                "Optional. One per line. Simplest format:\n"
                "Kitchen: media_player.sonos_kitchen\n"
                "Family Room: media_player.sonos_family_room\n\n"
                "Also accepts JSON formats for backwards compatibility."
            ),
            "placeholder": (
                "Kitchen: media_player.sonos_kitchen\n"
                "Family Room: media_player.sonos_family_room"
            ),
        },
    }

    waiting_prompt_template = (
        "Write a friendly message telling {mention} you’re controlling the music now. "
        "Only output that message."
    )

    platforms = ["webui", "homeassistant", "homekit", "xbmc"]

    # -------------------- HA helpers --------------------
    def _ha_settings(self) -> Dict[str, str]:
        raw = redis_client.hgetall("plugin_settings:Music Assistant") or {}
        settings = _decode_redis_map(raw)

        # IMPORTANT: decode HA settings too (hgetall often returns bytes)
        ha_raw = redis_client.hgetall("homeassistant_settings") or {}
        ha_settings = _decode_redis_map(ha_raw)

        base_url = (ha_settings.get("HA_BASE_URL") or "http://homeassistant.local:8123").strip().rstrip("/")
        token = (ha_settings.get("HA_TOKEN") or "").strip()
        entry_id = (settings.get("MA_CONFIG_ENTRY_ID") or "").strip()
        return {"base_url": base_url, "token": token, "entry_id": entry_id}

    def _ha_headers(self, token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def _ha_call_service(
        self,
        domain: str,
        service: str,
        data: Dict[str, Any],
        return_response: bool = True,
        timeout: int = 20,
    ) -> Any:
        cfg = self._ha_settings()
        if not cfg["token"]:
            raise RuntimeError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        url = f'{cfg["base_url"]}/api/services/{domain}/{service}'
        if return_response:
            url += "?return_response"

        resp = requests.post(url, headers=self._ha_headers(cfg["token"]), json=data, timeout=timeout)
        try:
            resp.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"HA service call failed: {resp.status_code} {resp.text}") from e

        if not (resp.text or "").strip():
            return None
        try:
            return resp.json()
        except Exception:
            return resp.text

    async def _ha_states(self) -> List[Dict[str, Any]]:
        cfg = self._ha_settings()
        if not cfg["token"]:
            raise RuntimeError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )
        url = f'{cfg["base_url"]}/api/states'
        headers = self._ha_headers(cfg["token"])

        def _get():
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            return r.json()

        return await asyncio.to_thread(_get)

    async def _ha_ws_call(self, msg_type: str) -> Any:
        """
        Call a Home Assistant WebSocket command using the SAME HA_TOKEN as REST.

        We do short-lived connect -> query -> disconnect (no subscriptions).
        """
        cfg = self._ha_settings()
        if not cfg["token"]:
            raise RuntimeError(
                "Home Assistant token is not set. Open WebUI → Settings → Home Assistant Settings "
                "and add a Long-Lived Access Token."
            )

        ws_url = f'{cfg["base_url"]}/api/websocket'
        timeout = aiohttp.ClientTimeout(total=20)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(ws_url, heartbeat=30) as ws:
                # 1) auth_required
                first = await ws.receive_json()
                if (first or {}).get("type") not in ("auth_required", "auth_ok"):
                    # Sometimes proxies / errors do weird things
                    logger.debug(f"[music_assistant] WS first msg: {first}")

                # 2) auth
                await ws.send_json({"type": "auth", "access_token": cfg["token"]})
                auth_ok = await ws.receive_json()
                if auth_ok.get("type") != "auth_ok":
                    raise RuntimeError(f"HA websocket auth failed: {auth_ok}")

                # 3) request
                await ws.send_json({"id": 1, "type": msg_type})
                resp = await ws.receive_json()

                if not resp.get("success", False):
                    raise RuntimeError(f"HA websocket call failed: {resp}")

                return resp.get("result")

    async def _media_players_for_room_area(self, room: str) -> List[Dict[str, str]]:
        """
        Try to reduce the candidate list by using HA Areas/Device/Entity registry via WebSocket.

        Returns a list of:
          [{"entity_id": "media_player.xyz", "friendly_name": "..."}]

        If the area can't be resolved or has no players assigned, returns [].
        """
        room_l = (room or "").strip().lower()
        if not room_l:
            return []

        try:
            areas = await self._ha_ws_call("config/area_registry/list")
            entities = await self._ha_ws_call("config/entity_registry/list")
            devices = await self._ha_ws_call("config/device_registry/list")
        except Exception as e:
            logger.warning(f"[music_assistant] area-scoped lookup failed (ws): {e}")
            return []

        # Find matching area_id by name
        area_id = None
        for a in (areas or []):
            if not isinstance(a, dict):
                continue
            if (a.get("name") or "").strip().lower() == room_l:
                area_id = a.get("area_id")
                break

        if not area_id:
            return []

        # Map device_id -> area_id
        device_area: Dict[str, str] = {}
        for d in (devices or []):
            if isinstance(d, dict) and d.get("id"):
                device_area[d["id"]] = d.get("area_id")

        in_area: set = set()

        for e in (entities or []):
            if not isinstance(e, dict):
                continue
            ent_id = (e.get("entity_id") or "").strip()
            if not ent_id.startswith("media_player."):
                continue

            ent_area = e.get("area_id")
            if ent_area == area_id:
                in_area.add(ent_id)
                continue

            dev_id = e.get("device_id")
            if dev_id and device_area.get(dev_id) == area_id:
                in_area.add(ent_id)

        if not in_area:
            return []

        # Enrich with friendly_name from /api/states (registry doesn't reliably include it)
        try:
            states = await self._ha_states()
        except Exception:
            states = []

        players: List[Dict[str, str]] = []
        for s in states:
            ent = s.get("entity_id") or ""
            if ent in in_area:
                players.append(
                    {
                        "entity_id": ent,
                        "friendly_name": ((s.get("attributes") or {}).get("friendly_name") or ent),
                    }
                )

        return players

    # -------------------- Room map parsing --------------------
    def _parse_room_map(self, raw: str) -> Dict[str, str]:
        """
        Accepts ROOM_MAP as:
          - Simple lines:
              Kitchen: media_player.sonos_kitchen
              Family Room: media_player.sonos_family_room

          - Legacy JSON-ish lines:
              "Kitchen": "media_player.sonos_kitchen"
              "Family Room": "media_player.sonos_family_room"

          - Full JSON object:
              { "Kitchen": "media_player.sonos_kitchen" }
        """
        raw = (raw or "").strip()
        if not raw:
            return {}

        # normalize smart quotes from iOS/macOS
        raw = (
            raw.replace("“", '"')
               .replace("”", '"')
               .replace("‘", "'")
               .replace("’", "'")
        )

        # 1) If it looks like a full JSON object, try it first
        if raw.lstrip().startswith("{") and raw.rstrip().endswith("}"):
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    out = {str(k).strip().lower(): str(v).strip() for k, v in data.items() if k and v}
                    logger.info(f"[music_assistant] ROOM_MAP parsed (json object) keys={list(out.keys())}")
                    return out
            except Exception as e:
                logger.error(f"[music_assistant] Invalid ROOM_MAP JSON object: {e}")

        # 2) Parse line-by-line (supports both `Kitchen: entity` and `"Kitchen": "entity"`)
        out: Dict[str, str] = {}
        jsonish_lines = []

        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # strip inline comments (simple)
            if " #" in line:
                line = line.split(" #", 1)[0].rstrip()

            # drop trailing commas
            if line.endswith(","):
                line = line[:-1].rstrip()

            if not line:
                continue

            # NEW simple format: Room: media_player.entity_id
            if ":" in line and not line.lstrip().startswith('"'):
                left, right = line.split(":", 1)
                room = left.strip()
                ent = right.strip()
                if room and ent:
                    out[room.lower()] = ent
                continue

            # Otherwise, treat it as legacy JSON-ish pair line: "Kitchen": "media_player.x"
            jsonish_lines.append(line)

        if out:
            logger.info(f"[music_assistant] ROOM_MAP parsed (simple) keys={list(out.keys())}")
            return out

        # 3) Legacy JSON-ish lines fallback (wrap into { ... })
        if not jsonish_lines:
            return {}

        text = "{\n" + ",\n".join(jsonish_lines) + "\n}"
        try:
            data = json.loads(text)
            if not isinstance(data, dict):
                return {}
            out = {str(k).strip().lower(): str(v).strip() for k, v in data.items() if k and v}
            logger.info(f"[music_assistant] ROOM_MAP parsed (legacy lines) keys={list(out.keys())}")
            return out
        except Exception as e:
            logger.error(f"[music_assistant] Invalid ROOM_MAP format: {e}")
            logger.error(f"[music_assistant] ROOM_MAP raw was:\n{raw}")
            return {}

    # -------------------- text helpers --------------------
    def _siri_flatten(self, text: Optional[str]) -> str:
        if not text:
            return "Okay."
        out = str(text)
        out = re.sub(r"[`*_]{1,3}", "", out)
        out = re.sub(r"\s+", " ", out).strip()
        return out[:450]

    def _normalize_query(self, text: str) -> str:
        """
        Fix a common cause of 'found nothing':
        LLM sometimes returns query like 'play stick figure' instead of 'stick figure'.
        Also strips common filler phrases.
        """
        t = (text or "").strip()

        # remove obvious control verbs at the start
        t = re.sub(
            r"^(?:play|queue|enqueue|put on|start|listen to|turn on|shuffle)\b\s*",
            "",
            t,
            flags=re.I,
        )

        # remove "some/a/an" directly after verbs
        t = re.sub(r"^(?:some|a|an)\b\s*", "", t, flags=re.I)

        # remove trailing "in the <room>" etc (room should be handled separately)
        t = re.sub(r"\s+(?:in|on)\s+the\s+.+$", "", t, flags=re.I)

        # collapse spaces
        t = re.sub(r"\s+", " ", t).strip()
        return t

    # -------------------- LLM planning / choosing --------------------
    async def _llm_plan(self, request_text: str, room: Optional[str], llm_client) -> Dict[str, Any]:
        """
        Convert user request into plan JSON:
        action: play|queue|pause|resume|stop|next|previous|volume
        query: search string for play/queue
        prefer: artist|album|playlist|track|radio (optional)
        volume: 0-100
        random: true|false (optional)
        room: optional room name
        """
        first, last = get_tater_name()
        sys = (
            f"You are {first} {last}. Convert the user's music request into JSON.\n"
            "Output ONLY valid JSON.\n"
            "Allowed actions: play, queue, pause, resume, stop, next, previous, volume.\n"
            "If playing/queueing, include 'query'.\n"
            "If user asks for random/shuffle/something different each time, set random=true.\n"
            "If user says a genre (reggae, lo-fi, jazz), prefer='playlist' or 'radio'.\n"
            "If user says an artist name, prefer='artist'. If they include a song title, prefer='track'.\n"
            "Schema:\n"
            '{"action":"play|queue|pause|resume|stop|next|previous|volume",'
            '"query":null|string,"prefer":null|string,"volume":null|int,'
            '"random":null|bool,"room":null|string}\n'
        )
        user = f"User request: {request_text}\nRoom hint: {room or ''}".strip()
        resp = await llm_client.chat(messages=[{"role": "system", "content": sys}, {"role": "user", "content": user}])
        content = (resp.get("message") or {}).get("content", "").strip()

        try:
            plan = json.loads(content)
        except Exception:
            m = re.search(r"\{.*\}", content, re.S)
            if m:
                try:
                    plan = json.loads(m.group(0))
                except Exception:
                    plan = {}
            else:
                plan = {}

        if not isinstance(plan, dict):
            plan = {}

        if not plan.get("action"):
            plan["action"] = "play"
        if plan.get("query") is None:
            plan["query"] = request_text
        if plan.get("room") is None:
            plan["room"] = room

        return plan

    def _condense_search_for_llm(self, payload: Dict[str, Any], max_each: int = 8) -> Dict[str, Any]:
        """
        Keep tokens under control: send the LLM a small, stable view of results.
        """
        def take(items: Any) -> List[Dict[str, Any]]:
            if not isinstance(items, list):
                return []
            out = []
            for it in items[:max_each]:
                if isinstance(it, dict):
                    artists = []
                    for a in (it.get("artists") or [])[:2]:
                        if isinstance(a, dict) and a.get("name"):
                            artists.append(a.get("name"))
                    out.append(
                        {
                            "name": it.get("name"),
                            "uri": it.get("uri"),
                            "media_type": it.get("media_type"),
                            "artists": artists,
                        }
                    )
            return out

        return {
            "artists": take(payload.get("artists")),
            "albums": take(payload.get("albums")),
            "tracks": take(payload.get("tracks")),
            "playlists": take(payload.get("playlists")),
            "radio": take(payload.get("radio")),
        }

    async def _llm_choose_item(
        self,
        request_text: str,
        cleaned_query: str,
        prefer: Optional[str],
        search_payload: Dict[str, Any],
        llm_client,
    ) -> Optional[Dict[str, Any]]:
        """
        Ask LLM to choose the best playable item from the search results.

        Output schema:
          {"uri": "...", "media_type": "track|album|artist|playlist|radio", "name": "..."}
        """
        condensed = self._condense_search_for_llm(search_payload, max_each=10)
        first, last = get_tater_name()
        sys = (
            f"You are {first} {last}. Pick the best item to play from Music Assistant search results.\n"
            "Rules:\n"
            "- Only output valid JSON.\n"
            "- Prefer an exact track match if the user gave artist+song.\n"
            "- Prefer an album match if the user gave artist+album.\n"
            "- If user gave only an artist, choose the artist item if present; otherwise choose a track by that artist.\n"
            "- If user asked for a genre/vibe, prefer playlist or radio.\n"
            "- Choose from the provided results only.\n"
            'Schema: {"uri":string,"media_type":"track|album|artist|playlist|radio","name":string}\n'
        )
        user = (
            "User request:\n"
            f"- raw: {request_text}\n"
            f"- cleaned_query: {cleaned_query}\n"
            f"- prefer: {prefer or ''}\n\n"
            "Search results (condensed):\n"
            f"{json.dumps(condensed, ensure_ascii=False)}"
        )
        resp = await llm_client.chat(messages=[{"role": "system", "content": sys}, {"role": "user", "content": user}])
        content = (resp.get("message") or {}).get("content", "").strip()

        try:
            j = json.loads(content)
        except Exception:
            m = re.search(r"\{.*\}", content, re.S)
            if not m:
                return None
            try:
                j = json.loads(m.group(0))
            except Exception:
                return None

        if not isinstance(j, dict):
            return None
        uri = (j.get("uri") or "").strip()
        mtype = (j.get("media_type") or "").strip()
        name = (j.get("name") or "").strip()
        if not uri or not mtype:
            return None
        return {"uri": uri, "media_type": mtype, "name": name or cleaned_query}

    # -------------------- Music Assistant wrappers --------------------
    async def _ma_search(self, name: str, limit: int = 25, library_only: bool = False) -> Dict[str, Any]:
        cfg = self._ha_settings()
        if not cfg["entry_id"]:
            raise RuntimeError("Music Assistant config_entry_id is missing. Set MA_CONFIG_ENTRY_ID in plugin settings.")

        data: Dict[str, Any] = {
            "name": name,
            "limit": int(limit),
            "library_only": bool(library_only),
            "config_entry_id": cfg["entry_id"],
        }

        result = await asyncio.to_thread(self._ha_call_service, "music_assistant", "search", data, True, 25)

        # Normalize HA wrappers: [{"response": {...}}] or [{"result": {...}}]
        if isinstance(result, list) and result:
            first = result[0]
            if isinstance(first, dict):
                if isinstance(first.get("response"), dict):
                    return first["response"]
                if isinstance(first.get("result"), dict):
                    return first["result"]
                return first
        if isinstance(result, dict):
            if isinstance(result.get("response"), dict):
                return result["response"]
            if isinstance(result.get("result"), dict):
                return result["result"]
            return result
        return {}

    async def _ma_get_random_track_uri(self, search_text: str) -> Optional[str]:
        """
        Use music_assistant.get_library with order_by=random (per MA docs).
        """
        cfg = self._ha_settings()
        if not cfg["entry_id"]:
            raise RuntimeError("Music Assistant config_entry_id is missing. Set MA_CONFIG_ENTRY_ID in plugin settings.")

        data: Dict[str, Any] = {
            "media_type": "track",
            "search": search_text,
            "limit": 1,
            "order_by": "random",
            "config_entry_id": cfg["entry_id"],
        }
        result = await asyncio.to_thread(self._ha_call_service, "music_assistant", "get_library", data, True, 25)

        payload = None
        if isinstance(result, list) and result and isinstance(result[0], dict):
            payload = result[0].get("response") or result[0].get("result") or result[0]
        elif isinstance(result, dict):
            payload = result.get("response") or result.get("result") or result

        items = (payload or {}).get("items") or []
        if items and isinstance(items[0], dict):
            return items[0].get("uri")
        return None

    async def _media_player_play_by_name(self, media_player: str, name: str) -> None:
        """
        MA FAQ: media_player.play_media can accept a name with media_content_type=music.
        """
        data = {
            "entity_id": media_player,
            "media_content_id": name,
            "media_content_type": "music",
        }
        await asyncio.to_thread(self._ha_call_service, "media_player", "play_media", data, False, 20)

    async def _ma_play_uri(
        self,
        media_player: str,
        uri: str,
        media_type: str,
        enqueue: str = "play",
        radio_mode: bool = False,
    ) -> None:
        """
        Use music_assistant.play_media action with explicit uri/type.
        """
        data = {
            "entity_id": media_player,
            "media_id": uri,
            "media_type": media_type,
            "enqueue": enqueue,  # play | add
            "radio_mode": bool(radio_mode),
        }
        await asyncio.to_thread(self._ha_call_service, "music_assistant", "play_media", data, False, 25)

    async def _player_control(self, media_player: str, action: str, volume: Optional[int] = None) -> None:
        if action == "volume":
            if volume is None:
                raise RuntimeError("Volume action requested but no volume provided.")
            vol = max(0, min(100, int(volume))) / 100.0
            data = {"entity_id": media_player, "volume_level": vol}
            await asyncio.to_thread(self._ha_call_service, "media_player", "volume_set", data, False, 15)
            return

        svc_map = {
            "pause": ("media_player", "media_pause"),
            "resume": ("media_player", "media_play"),
            "stop": ("media_player", "media_stop"),
            "next": ("media_player", "media_next_track"),
            "previous": ("media_player", "media_previous_track"),
        }
        if action not in svc_map:
            raise RuntimeError(f"Unsupported action: {action}")

        domain, service = svc_map[action]
        await asyncio.to_thread(self._ha_call_service, domain, service, {"entity_id": media_player}, False, 15)

    # -------------------- Room -> media_player resolution --------------------
    async def _resolve_media_player(self, room: Optional[str], args: Dict[str, Any], llm_client) -> str:
        explicit = (args or {}).get("media_player")
        if explicit:
            return explicit

        # optional ROOM_MAP override
        room_map: Dict[str, str] = {}
        try:
            raw = redis_client.hgetall("plugin_settings:Music Assistant") or {}
            settings = _decode_redis_map(raw)
            room_map = self._parse_room_map(settings.get("ROOM_MAP", ""))
        except Exception:
            room_map = {}

        if room:
            mapped = room_map.get(room.strip().lower())
            if mapped:
                logger.info(f"[music_assistant] ROOM_MAP override: room='{room}' -> {mapped}")
                return mapped

        # NEW: If we have a room, try to reduce candidates to only players in that HA Area
        players: List[Dict[str, str]] = []
        if room:
            players = await self._media_players_for_room_area(room)

        # Fallback: old behavior (all media_players)
        if not players:
            states = await self._ha_states()
            for s in states:
                ent = s.get("entity_id") or ""
                if ent.startswith("media_player."):
                    players.append(
                        {
                            "entity_id": ent,
                            "friendly_name": ((s.get("attributes") or {}).get("friendly_name") or ent),
                        }
                    )

        if not players:
            raise RoomPlayerNotFound("No media_player entities found in Home Assistant.")

        if not room:
            raise RoomPlayerNotFound("Which room should I play it in? (e.g., Kitchen, Family Room)")

        # Ask LLM to pick best match (now from a smaller list when area lookup succeeds)
        first, last = get_tater_name()
        sys = (
            f"You are {first} {last}. Pick the best media_player entity for the requested room.\n"
            "Only output JSON: {\"entity_id\":\"media_player.xyz\"}\n"
        )
        listing = "\n".join([f'- {p["entity_id"]} ({p["friendly_name"]})' for p in players])
        user = f"Room: {room}\nAvailable players:\n{listing}"
        resp = await llm_client.chat(messages=[{"role": "system", "content": sys}, {"role": "user", "content": user}])
        content = (resp.get("message") or {}).get("content", "").strip()

        try:
            j = json.loads(content)
            ent = j.get("entity_id")
            if ent and any(p["entity_id"] == ent for p in players):
                return ent
        except Exception:
            pass

        # fallback: contains match on friendly name
        room_l = room.lower()
        for p in players:
            if room_l in (p.get("friendly_name") or "").lower():
                return p["entity_id"]

        choices = ", ".join([p["friendly_name"] for p in players[:10]])
        raise RoomPlayerNotFound(f"I couldn’t find a player for '{room}'. Available players include: {choices}")

    # -------------------- Main runner --------------------
    async def _run(self, args: Dict[str, Any], llm_client) -> str:
        args = args or {}
        request_text = (args.get("request") or "").strip()
        room = (args.get("room") or "").strip() or None

        if not request_text:
            return "No request provided."

        plan = await self._llm_plan(request_text, room, llm_client)
        action = (plan.get("action") or "play").strip().lower()
        query = (plan.get("query") or "").strip() or request_text
        prefer = (plan.get("prefer") or None)
        vol = plan.get("volume")
        want_random = bool(plan.get("random")) if plan.get("random") is not None else False
        plan_room = (plan.get("room") or room)

        try:
            media_player = await self._resolve_media_player(plan_room, args, llm_client)
        except RoomPlayerNotFound as e:
            return str(e)

        # Controls
        if action in {"pause", "resume", "stop", "next", "previous", "volume"}:
            await self._player_control(media_player, action, vol)
            if action == "volume":
                return f"Set volume to {max(0, min(100, int(vol or 0)))}."
            return f"Done ({action})."

        if action not in {"play", "queue"}:
            return "Sorry — I’m not sure what to do with that request."

        # Clean the query so we don't search for "play stick figure"
        cleaned = self._normalize_query(query) or self._normalize_query(request_text) or query

        # If random requested: try MA random library pick first
        if want_random:
            uri = await self._ma_get_random_track_uri(cleaned)
            if uri:
                await self._ma_play_uri(
                    media_player,
                    uri,
                    "track",
                    enqueue=("add" if action == "queue" else "play"),
                    radio_mode=True,
                )
                where = plan_room or "that room"
                return f"Playing something random based on '{cleaned}' in {where}."

        # Search
        search_payload = await self._ma_search(name=cleaned, limit=25, library_only=False)

        # If search is empty, do ONE retry with an even more aggressive cleaned version
        if not any((search_payload.get(k) or []) for k in ("artists", "albums", "tracks", "playlists", "radio")):
            retry = self._normalize_query(cleaned)
            if retry and retry != cleaned:
                search_payload = await self._ma_search(name=retry, limit=25, library_only=False)
                cleaned = retry

        # Let the LLM choose the correct item from the results
        chosen = await self._llm_choose_item(request_text, cleaned, prefer, search_payload, llm_client)

        # If user likely asked for ONLY an artist and LLM chose artist, start with a random track by that artist
        if chosen and chosen.get("media_type") == "artist":
            seed = chosen.get("name") or cleaned
            uri = await self._ma_get_random_track_uri(seed)
            if uri:
                await self._ma_play_uri(
                    media_player,
                    uri,
                    "track",
                    enqueue=("add" if action == "queue" else "play"),
                    radio_mode=True,
                )
                where = plan_room or "that room"
                return f"Playing a random {seed} track in {where}."

            enqueue = "add" if action == "queue" else "play"
            await self._ma_play_uri(media_player, chosen["uri"], "artist", enqueue=enqueue, radio_mode=True)
            where = plan_room or "that room"
            return f"Playing {chosen.get('name') or cleaned} (artist) in {where}."

        if chosen and chosen.get("uri") and chosen.get("media_type"):
            uri = chosen["uri"]
            mtype = chosen["media_type"]
            title = chosen.get("name") or cleaned
            enqueue = "add" if action == "queue" else "play"
            radio_mode = True if mtype in {"artist", "track", "radio"} else False
            await self._ma_play_uri(media_player, uri, mtype, enqueue=enqueue, radio_mode=radio_mode)
            where = plan_room or "that room"
            if action == "queue":
                return f"Queued {title} ({mtype}) in {where}."
            return f"Playing {title} ({mtype}) in {where}."

        # LAST-RESORT: play by name directly
        try:
            await self._media_player_play_by_name(media_player, cleaned)
            where = plan_room or "that room"
            return f"Playing {cleaned} in {where}."
        except Exception:
            return f"I searched Music Assistant for '{cleaned}' but didn’t find anything. Want to try a different search?"

    # -------------------- Platform handlers --------------------
    async def handle_webui(self, args, llm_client):
        async def inner():
            try:
                return await self._run(args or {}, llm_client)
            except Exception as e:
                logger.error(f"[music_assistant:webui] {e}")
                return str(e)

        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_homeassistant(self, args, llm_client):
        try:
            return await self._run(args or {}, llm_client)
        except Exception as e:
            logger.error(f"[music_assistant:ha] {e}")
            return str(e)

    async def handle_homekit(self, args, llm_client):
        try:
            out = await self._run(args or {}, llm_client)
            return self._siri_flatten(out)
        except Exception as e:
            logger.error(f"[music_assistant:homekit] {e}")
            return self._siri_flatten(str(e))

    async def handle_xbmc(self, args, llm_client):
        try:
            return await self._run(args or {}, llm_client)
        except Exception as e:
            logger.error(f"[music_assistant:xbmc] {e}")
            return str(e)


plugin = MusicAssistantPlugin()
