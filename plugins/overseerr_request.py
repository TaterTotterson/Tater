# plugins/overseerr_request.py
import re
import json
import asyncio
import logging
from urllib.parse import quote
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

from plugin_base import ToolPlugin
from helpers import redis_client

load_dotenv()
logger = logging.getLogger("overseerr_request")
logger.setLevel(logging.INFO)


class OverseerrRequestPlugin(ToolPlugin):
    """
    Adds a movie or TV show to Overseerr by title.
    Examples:
      - add the movie "F1"
      - request "One Piece" tv show
      - request "Dune"
    """
    name = "overseerr_request"
    plugin_name = "Overseerr Request"
    usage = (
        "{\n"
        '  "function": "overseerr_request",\n'
        '  "arguments": {\n'
        '    "title": "<title string>",\n'
        '    "kind": "movie|tv (optional)"\n'
        "  }\n"
        "}\n"
    )
    description = (
        "Adds a movie or TV show to Overseerr by title, creating a new request for it. "
        "Example: add the movie F1, request the TV show One Piece, request the movie Dune."
    )
    plugin_dec = "Request a movie or TV show in Overseerr by title."
    pretty_name = "Overseerr: Add Request"
    settings_category = "Overseerr"
    required_settings = {
        "OVERSEERR_BASE_URL": {
            "label": "Overseerr Base URL (e.g., http://overseerr.local:5055)",
            "type": "string",
            "default": "http://localhost:5055",
        },
        "OVERSEERR_API_KEY": {
            "label": "Overseerr API Key",
            "type": "string",
            "default": "",
        },
    }
    waiting_prompt_template = (
        "Tell {mention} you’re adding their title to Overseerr now. "
        "Keep it short and friendly. Only output that message."
    )
    platforms = ["webui", "homeassistant", "homekit"]  # simplified: no Discord/IRC

    # ---------- Settings ----------
    @staticmethod
    def _get_settings():
        s = redis_client.hgetall("plugin_settings:Overseerr")
        base = s.get("OVERSEERR_BASE_URL", "http://localhost:5055").rstrip("/")
        api = s.get("OVERSEERR_API_KEY", "")
        return base, api

    # ---------- HTTP helpers ----------
    def _get(self, path: str, params: Optional[Dict[str, Any]] = None):
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}
        url = f"{base}/api/v1{path}"
        headers = {"X-Api-Key": api_key, "Accept": "application/json"}
        try:
            r = requests.get(url, params=params or {}, headers=headers, timeout=15)
            if r.status_code != 200:
                logger.error(f"[Overseerr GET {path}] HTTP {r.status_code} :: {r.text}")
                return {"error": f"Overseerr returned HTTP {r.status_code}."}
            return r.json() or {}
        except Exception as e:
            logger.exception(f"[Overseerr GET {path}] {e}")
            return {"error": f"Failed to reach Overseerr: {e}"}

    def _post(self, path: str, payload: Dict[str, Any]):
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}
        url = f"{base}/api/v1{path}"
        headers = {
            "X-Api-Key": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=20)
            if r.status_code not in (200, 201):
                logger.error(f"[Overseerr POST {path}] HTTP {r.status_code} :: {r.text} :: sent={json.dumps(payload)}")
                try:
                    # Return Overseerr error message if present
                    return {"error": json.loads(r.text).get("message", f"HTTP {r.status_code}")}
                except Exception:
                    return {"error": f"HTTP {r.status_code}"}
            return r.json() if r.text else {}
        except Exception as e:
            logger.exception(f"[Overseerr POST {path}] {e}")
            return {"error": f"Failed to reach Overseerr: {e}"}

    # ---------- Core logic ----------
    @staticmethod
    def _norm(s: str) -> str:
        return re.sub(r"\s+", " ", s or "").strip().lower()

    @staticmethod
    def _year_from_date(d: Optional[str]) -> Optional[int]:
        if not d:
            return None
        try:
            return int(d[:4])
        except Exception:
            return None

    @staticmethod
    def _coerce_kind(kind: Optional[str]) -> Optional[str]:
        if not kind:
            return None
        k = kind.strip().lower()
        if k.startswith("movie"):
            return "movie"
        if "tv" in k or "show" in k or "series" in k:
            return "tv"
        return None

    def _pick_best_result(self, results: List[Dict[str, Any]], title: str, kind: Optional[str]):
        """
        Heuristic match with strong filtering:
        - Only consider items where mediaType is exactly 'movie' or 'tv'
        - Only consider items with integer 'id'
        - Prefer type match + exact/starts-with title
        """
        t_norm = self._norm(title)
        want_type = self._coerce_kind(kind)

        pool = []
        for item in results or []:
            media_type = (item.get("mediaType") or "").lower()
            item_id = item.get("id")
            if media_type not in ("movie", "tv"):
                continue
            if not isinstance(item_id, int):
                continue
            pool.append(item)

        if not pool:
            return None

        best = None
        best_score = -1
        for item in pool:
            media_type = (item.get("mediaType") or "").lower()  # "movie" or "tv"
            cand_title = item.get("title") or item.get("name") or ""
            cand_norm = self._norm(cand_title)

            score = 0
            if want_type and media_type == want_type:
                score += 3
            if cand_norm == t_norm:
                score += 3
            elif cand_norm.startswith(t_norm):
                score += 2
            elif t_norm in cand_norm:
                score += 1

            if score > best_score:
                best, best_score = item, score
        return best

    def _search(self, title: str):
        # Explicitly URL-encode the query to satisfy Overseerr validation
        base, api_key = self._get_settings()
        if not api_key:
            return {"error": "Overseerr is not configured. Set OVERSEERR_API_KEY in plugin settings."}

        q = quote(title or "", safe="")  # encode quotes, slashes, etc.
        url = f"{base}/api/v1/search?query={q}"
        headers = {"X-Api-Key": api_key, "Accept": "application/json"}

        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code != 200:
                logger.error(f"[Overseerr GET /search] HTTP {r.status_code} :: {r.text}")
                return {"error": f"Overseerr returned HTTP {r.status_code}."}
            data = r.json() or {}
            return {"results": data.get("results", [])}
        except Exception as e:
            logger.exception(f"[Overseerr GET /search] {e}")
            return {"error": f"Failed to reach Overseerr: {e}"}

    def _create_request(self, media_type: str, media_id: int, seasons: Optional[List[int]] = None):
        payload: Dict[str, Any] = {
            "mediaType": media_type,   # "movie" | "tv"
            "mediaId": int(media_id),
        }
        if media_type == "tv":
            payload["seasons"] = seasons if seasons is not None else []

        # helpful for troubleshooting
        logger.error(f"[Overseerr POST /request] sending payload={payload}")
        return self._post("/request", payload)

    def _do_request_flow(self, args: Dict[str, Any]) -> str:
        title = (args.get("title") or "").strip()
        if not title:
            return "No title provided."

        # Coerce kind early to bias selection but never send it directly
        kind = self._coerce_kind(args.get("kind"))

        # 1) Search
        srch = self._search(title)
        if "error" in srch:
            return srch["error"]
        results = srch.get("results") or []
        if not results:
            return f'No results for "{title}".'

        # 2) Pick best candidate
        best = self._pick_best_result(results, title, kind)
        if not best:
            return f'Could not determine the right match for "{title}".'

        media_type = (best.get("mediaType") or "").lower()  # "movie" | "tv"
        media_id = best.get("id")                            # use as mediaId
        disp_title = best.get("title") or best.get("name") or title
        disp_year = self._year_from_date(best.get("releaseDate") or best.get("firstAirDate"))
        ypart = f" ({disp_year})" if disp_year else ""

        if not media_id or not media_type:
            return "Found a potential match, but it lacks required fields (id/type)."

        # --- helpers ------------------------------------------------------------
        def needs_filter_retry(resp: Dict[str, Any]) -> bool:
            err = (resp or {}).get("error", "")
            if not isinstance(err, str):
                return False
            s = err.lower()
            return ("reading 'filter'" in s) or ("no seasons available to request" in s)

        def fetch_tv_season_numbers(tv_tmdb_id: int) -> List[int]:
            """
            Ask Overseerr for TV details and build a list of season numbers.
            Try /api/v1/tv/{id} first; if not available in this build, fall back to /api/v1/tmdb/tv/{id}.
            Skip season 0 (specials). Return [] if we can’t find anything sane.
            """
            # Prefer native tv details
            data = self._get(f"/tv/{tv_tmdb_id}")
            if isinstance(data, dict) and data and "seasons" not in data:
                # Some versions expose TMDB details under /tmdb/tv/{id}
                data = self._get(f"/tmdb/tv/{tv_tmdb_id}")

            try:
                seasons = data.get("seasons", []) if isinstance(data, dict) else []
                nums = []
                for s in seasons:
                    # accept both {seasonNumber: int} and {season_number: int}
                    num = s.get("seasonNumber") if isinstance(s, dict) else None
                    if num is None and isinstance(s, dict):
                        num = s.get("season_number")
                    if isinstance(num, int) and num > 0:
                        nums.append(num)
                # unique + sorted
                return sorted(set(nums))
            except Exception:
                return []

        # 3) Create request (with defensive TV handling)
        # First attempt: movies as-is; TV with seasons=[]
        try:
            if media_type == "tv":
                resp = self._create_request(media_type, int(media_id), seasons=[])
            else:
                resp = self._create_request(media_type, int(media_id))
        except TypeError:
            # Back-compat if _create_request doesn’t accept seasons yet
            resp = self._create_request(media_type, int(media_id))

        # If Overseerr complains about seasons, retry with a concrete list like [1..N]
        if media_type == "tv" and isinstance(resp, dict) and needs_filter_retry(resp):
            season_list = fetch_tv_season_numbers(int(media_id)) or [1]  # last-resort fallback
            try:
                resp = self._create_request(media_type, int(media_id), seasons=season_list)
            except TypeError:
                # Old signature; nothing else to try
                pass

        # Handle failure paths
        if isinstance(resp, dict) and "error" in resp:
            msg = resp["error"]
            if isinstance(msg, str) and ("already requested" in msg.lower() or "already exists" in msg.lower()):
                return f"{disp_title}{ypart} ({media_type}) is already in your requests."
            return f"Failed to create request for {disp_title}{ypart}: {msg}"

        # Success formatting (preserve your original style)
        status = (resp or {}).get("status") or (resp.get("request") or {}).get("status") if isinstance(resp, dict) else None
        req_id = (resp or {}).get("id") or (resp.get("request") or {}).get("id") if isinstance(resp, dict) else None
        status_text = f" (status: {status})" if status else ""
        rid = f" [request #{req_id}]" if req_id else ""

        return f"Requested {disp_title}{ypart} ({media_type}){status_text}.{rid}"

    # ---------- Platform handlers ----------
    async def handle_webui(self, args, llm_client):
        """
        WebUI output:
          e.g. "One Battle After Another (2025) has been added to your requests"
        """
        async def inner():
            raw = self._do_request_flow(args)
            # Extract title portion
            clean = re.sub(r"^Requested\s+", "", raw)
            clean = re.sub(r"\(status:.*?\)", "", clean, flags=re.IGNORECASE)
            clean = re.sub(r"\[request #.*?\]", "", clean)
            clean = clean.strip(" .")

            # Keep year parentheses in WebUI
            return f"{clean} has been added to your requests."
        try:
            asyncio.get_running_loop()
            return await inner()
        except RuntimeError:
            return asyncio.run(inner())

    async def handle_homeassistant(self, args, llm_client):
        """
        Home Assistant TTS output:
          e.g. "One Battle After Another 2025 has been added to your requests"
        """
        raw = self._do_request_flow(args)
        clean = re.sub(r"^Requested\s+", "", raw)
        clean = re.sub(r"\(status:.*?\)", "", clean, flags=re.IGNORECASE)
        clean = re.sub(r"\[request #.*?\]", "", clean)
        clean = re.sub(r"[()]", "", clean)  # remove parentheses
        clean = clean.strip(" .")

        return f"{clean} has been added to your requests."

    async def handle_homekit(self, args, llm_client):
        """
        HomeKit/Siri TTS output:
          e.g. "One Battle After Another 2025 has been added to your requests"
        (Same style as Home Assistant, parentheses removed for cleaner speech.)
        """
        raw = self._do_request_flow(args)
        clean = re.sub(r"^Requested\s+", "", raw)
        clean = re.sub(r"\(status:.*?\)", "", clean, flags=re.IGNORECASE)
        clean = re.sub(r"\[request #.*?\]", "", clean)
        clean = re.sub(r"[()]", "", clean)  # remove parentheses
        clean = clean.strip(" .")
        return f"{clean} has been added to your requests."


plugin = OverseerrRequestPlugin()
