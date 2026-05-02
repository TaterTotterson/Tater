from __future__ import annotations

import base64
import contextlib
import io
import mimetypes
import os
import shutil
import subprocess
import tempfile
import warnings
import wave
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from helpers import redis_client
from vision_settings import get_vision_settings as get_shared_vision_settings

UNIFI_PROTECT_BASE_URL_KEY = "tater:unifi_protect:base_url"
UNIFI_PROTECT_API_KEY_KEY = "tater:unifi_protect:api_key"
UNIFI_PROTECT_DEFAULT_BASE_URL = "https://10.4.20.127"
DEFAULT_UNIFI_PROTECT_AUDIO_TIMEOUT_SECONDS = 90.0

INTEGRATION = {
    "id": "unifi_protect",
    "name": "UniFi Protect",
    "description": "UniFi Protect API key for cameras, sensors, and direct speaker announcements.",
    "badge": "PRO",
    "order": 70,
    "fields": [
        {
            "key": "unifi_protect_base_url",
            "label": "Console Base URL",
            "type": "text",
            "default": UNIFI_PROTECT_DEFAULT_BASE_URL,
            "placeholder": UNIFI_PROTECT_DEFAULT_BASE_URL,
        },
        {
            "key": "unifi_protect_api_key",
            "label": "API Key",
            "type": "password",
            "default": "",
        },
    ],
    "actions": [
        {
            "id": "test",
            "label": "Test UniFi Protect",
            "status": "Checks the Protect integration API and counts cameras.",
        },
    ],
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _as_int(value: Any, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    try:
        out = int(float(value))
    except Exception:
        out = int(default)
    if minimum is not None:
        out = max(minimum, out)
    if maximum is not None:
        out = min(maximum, out)
    return out


def read_unifi_protect_settings(client: Any = None) -> Dict[str, str]:
    store = client or redis_client
    base = _text(store.get(UNIFI_PROTECT_BASE_URL_KEY) or UNIFI_PROTECT_DEFAULT_BASE_URL).rstrip("/")
    api_key = _text(store.get(UNIFI_PROTECT_API_KEY_KEY))
    return {
        "base": base or UNIFI_PROTECT_DEFAULT_BASE_URL,
        "api_key": api_key,
        "UNIFI_PROTECT_BASE_URL": base or UNIFI_PROTECT_DEFAULT_BASE_URL,
        "UNIFI_PROTECT_API_KEY": api_key,
    }


def save_unifi_protect_settings(
    *,
    base_url: Any = None,
    api_key: Any = None,
    client: Any = None,
) -> Dict[str, str]:
    store = client or redis_client
    current = read_unifi_protect_settings(store)
    next_base = _text(current.get("base") if base_url is None else base_url) or UNIFI_PROTECT_DEFAULT_BASE_URL
    next_api_key = _text(current.get("api_key") if api_key is None else api_key)
    store.set(UNIFI_PROTECT_BASE_URL_KEY, next_base.rstrip("/"))
    store.set(UNIFI_PROTECT_API_KEY_KEY, next_api_key)
    return read_unifi_protect_settings(store)


def load_unifi_protect_config(*, required: bool = True, client: Any = None) -> Dict[str, str]:
    settings = read_unifi_protect_settings(client)
    if required and not _text(settings.get("api_key")):
        raise ValueError(
            "UniFi Protect API key is not set. Open WebUI -> Settings -> Integrations -> UniFi Protect and add API key."
        )
    return {"base": _text(settings.get("base")), "api_key": _text(settings.get("api_key"))}


def unifi_protect_configured(client: Any = None) -> bool:
    return bool(_text(read_unifi_protect_settings(client).get("api_key")))


def unifi_protect_headers(api_key: str, *, json_content: bool = True) -> Dict[str, str]:
    headers = {"X-API-KEY": api_key, "Accept": "application/json"}
    if json_content:
        headers["Content-Type"] = "application/json"
    return headers


def unifi_protect_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    stream: bool = False,
    timeout_s: float = 20.0,
) -> Any:
    conf = load_unifi_protect_config(required=True)
    url_path = path if _text(path).startswith("/") else f"/{path}"
    req_headers = unifi_protect_headers(conf["api_key"], json_content=not stream)
    if headers:
        req_headers.update(headers)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", InsecureRequestWarning)
        resp = requests.request(
            method,
            f"{conf['base']}{url_path}",
            headers=req_headers,
            params=params,
            json=json_body,
            timeout=max(5.0, float(timeout_s or 20.0)),
            verify=False,
            stream=stream,
        )
    if resp.status_code >= 400:
        raise RuntimeError(f"UniFi Protect HTTP {resp.status_code}: {resp.text[:200]}")
    if stream:
        return resp.content, resp.headers
    try:
        return resp.json()
    except Exception:
        return {}


def unifi_camera_entity(camera_id: Any) -> str:
    token = _text(camera_id).lower()
    return f"camera.unifi_{token}" if token else ""


def unifi_camera_id_from_target(target: Any) -> str:
    token = _text(target)
    lower = token.lower()
    if lower.startswith("unifi:"):
        token = _text(token.split(":", 1)[1])
        lower = token.lower()
    if lower.startswith("camera."):
        object_id = lower.split(".", 1)[1]
        if object_id.startswith("unifi_"):
            return object_id[len("unifi_") :]
        return object_id
    if lower.startswith("unifi_"):
        return lower[len("unifi_") :]
    return lower


def unifi_camera_name(row: Dict[str, Any], camera_id: str) -> str:
    for key in ("name", "displayName", "display_name", "friendlyName", "friendly_name"):
        value = _text(row.get(key))
        if value:
            return value
    return camera_id


def unifi_camera_has_speaker_hint(row: Dict[str, Any]) -> bool:
    feature_flags = row.get("featureFlags") if isinstance(row.get("featureFlags"), dict) else {}
    if not feature_flags and isinstance(row.get("feature_flags"), dict):
        feature_flags = row.get("feature_flags")
    direct_keys = (
        "hasSpeaker",
        "has_speaker",
        "hasTwoWayAudio",
        "has_two_way_audio",
        "hasTalkback",
        "has_talkback",
    )
    for key in direct_keys:
        if key in row and bool(row.get(key)):
            return True
        if key in feature_flags and bool(feature_flags.get(key)):
            return True
    text = " ".join(
        _text(row.get(key)).lower()
        for key in ("type", "model", "modelKey", "marketName", "market_name", "name")
    )
    return "doorbell" in text


def list_unifi_cameras() -> List[Dict[str, Any]]:
    payload = unifi_protect_request("GET", "/proxy/protect/integration/v1/cameras", timeout_s=20.0)
    return payload if isinstance(payload, list) else []


def list_unifi_sensors() -> List[Dict[str, Any]]:
    payload = unifi_protect_request("GET", "/proxy/protect/integration/v1/sensors", timeout_s=20.0)
    return payload if isinstance(payload, list) else []


def _first_text(row: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _text(row.get(key))
        if value:
            return value
    return ""


def _protect_bool_status(row: Dict[str, Any]) -> str:
    for key in ("state", "status", "connectionState", "connection_state"):
        value = _text(row.get(key))
        if value:
            return value
    for key in ("isConnected", "is_connected", "connected", "isOnline", "is_online"):
        if key in row:
            return "online" if bool(row.get(key)) else "offline"
    return ""


def _protect_details(row: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            out[key] = value
    return out


def integration_devices() -> Dict[str, Any]:
    if not unifi_protect_configured():
        return {"devices": [], "message": "UniFi Protect is not configured."}
    rows: List[Dict[str, Any]] = []
    cameras = list_unifi_cameras()
    sensors = list_unifi_sensors()
    for camera in cameras:
        if not isinstance(camera, dict):
            continue
        camera_id = _first_text(camera, "id", "_id", "uuid")
        name = unifi_camera_name(camera, camera_id)
        rows.append(
            {
                "id": camera_id or name,
                "name": name or camera_id or "Camera",
                "type": "camera",
                "status": _protect_bool_status(camera),
                "state": _first_text(camera, "state", "status"),
                "details": _protect_details(
                    camera,
                    [
                        "model",
                        "modelKey",
                        "marketName",
                        "host",
                        "hostAddress",
                        "firmwareVersion",
                        "isConnected",
                        "isRecording",
                        "isMotionDetected",
                        "lastMotion",
                        "lastMotionAt",
                        "lastRing",
                        "lastRingAt",
                    ],
                ),
            }
        )
    for sensor in sensors:
        if not isinstance(sensor, dict):
            continue
        sensor_id = _first_text(sensor, "id", "_id", "uuid")
        name = _first_text(sensor, "name", "displayName", "friendlyName") or sensor_id
        rows.append(
            {
                "id": sensor_id or name,
                "name": name or "Sensor",
                "type": _first_text(sensor, "type", "sensorType", "sensor_type") or "sensor",
                "status": _protect_bool_status(sensor),
                "state": _first_text(sensor, "state", "status"),
                "details": _protect_details(
                    sensor,
                    [
                        "model",
                        "modelKey",
                        "marketName",
                        "batteryStatus",
                        "batteryLevel",
                        "isConnected",
                        "isOpened",
                        "isMotionDetected",
                        "alarmTriggeredAt",
                        "lastSeen",
                    ],
                ),
            }
        )
    return {"devices": rows, "message": f"UniFi Protect returned {len(rows)} cameras and sensors."}


def read_integration_settings() -> Dict[str, Any]:
    settings = read_unifi_protect_settings()
    return {
        "unifi_protect_base_url": settings.get("base") or UNIFI_PROTECT_DEFAULT_BASE_URL,
        "unifi_protect_api_key": settings.get("api_key", ""),
    }


def save_integration_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    saved = save_unifi_protect_settings(
        base_url=(payload or {}).get("unifi_protect_base_url"),
        api_key=(payload or {}).get("unifi_protect_api_key"),
    )
    return {
        "unifi_protect_base_url": saved.get("base") or UNIFI_PROTECT_DEFAULT_BASE_URL,
        "unifi_protect_api_key": saved.get("api_key", ""),
    }


def run_integration_action(action_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if _text(action_id) != "test":
        raise KeyError(f"Unsupported UniFi Protect action: {action_id}")
    current = read_integration_settings()
    base = _text((payload or {}).get("unifi_protect_base_url") or current.get("unifi_protect_base_url")).rstrip("/")
    api_key = _text((payload or {}).get("unifi_protect_api_key") or current.get("unifi_protect_api_key"))
    if not api_key:
        raise ValueError("UniFi Protect API key is required.")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", InsecureRequestWarning)
        response = requests.get(
            f"{base}/proxy/protect/integration/v1/cameras",
            headers=unifi_protect_headers(api_key),
            timeout=20,
            verify=False,
        )
    if response.status_code >= 400:
        raise RuntimeError(f"UniFi Protect test failed: HTTP {response.status_code}: {response.text[:200]}")
    try:
        cameras = response.json()
    except Exception as exc:
        raise RuntimeError(f"UniFi Protect test returned invalid JSON: {exc}") from exc
    count = len(cameras) if isinstance(cameras, list) else 0
    return {
        "ok": True,
        "camera_count": count,
        "message": f"UniFi Protect connection worked. Found {count} camera{'s' if count != 1 else ''}.",
    }


def _wav_duration_seconds(wav_bytes: bytes) -> float:
    payload = bytes(wav_bytes or b"")
    if not payload:
        return 0.0
    try:
        with wave.open(io.BytesIO(payload), "rb") as wav_file:
            frames = wav_file.getnframes()
            rate = wav_file.getframerate()
            return float(frames) / float(rate) if rate > 0 else 0.0
    except Exception:
        return 0.0


def _talkback_audio_args(session: Dict[str, Any]) -> list[str]:
    codec = _text(session.get("codec")).lower().replace("-", "_")
    sample_rate = _as_int(
        session.get("samplingRate") or session.get("sampling_rate"),
        48000,
        minimum=8000,
        maximum=96000,
    )
    if "mulaw" in codec or "pcmu" in codec:
        return ["-ac", "1", "-ar", "8000", "-c:a", "pcm_mulaw"]
    if "alaw" in codec or "pcma" in codec:
        return ["-ac", "1", "-ar", "8000", "-c:a", "pcm_alaw"]
    return ["-ac", "1", "-ar", str(sample_rate or 48000), "-c:a", "libopus", "-application", "voip", "-b:a", "32k"]


def play_unifi_protect_audio_sync(
    *,
    cameras: list[str],
    audio_bytes: bytes,
    timeout_s: float = DEFAULT_UNIFI_PROTECT_AUDIO_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    clean_cameras = [unifi_camera_id_from_target(item) for item in list(cameras or []) if _text(item)]
    clean_cameras = [item for item in clean_cameras if item]
    if not clean_cameras:
        return {"ok": False, "sent_count": 0, "error": "No UniFi Protect cameras selected."}
    payload = bytes(audio_bytes or b"")
    if not payload:
        return {"ok": False, "sent_count": 0, "error": "Announcement audio is empty."}

    ffmpeg_path = _text(os.getenv("FFMPEG_PATH") or shutil.which("ffmpeg"))
    if not ffmpeg_path:
        return {"ok": False, "sent_count": 0, "error": "ffmpeg is required for UniFi Protect audio playback."}

    duration_s = _wav_duration_seconds(payload)
    run_timeout = max(float(timeout_s or DEFAULT_UNIFI_PROTECT_AUDIO_TIMEOUT_SECONDS), duration_s + 20.0, 30.0)
    sent_count = 0
    failures: list[str] = []
    tmp_path = ""

    try:
        with tempfile.NamedTemporaryFile(prefix="tater-unifi-announcement-", suffix=".wav", delete=False) as tmp_file:
            tmp_file.write(payload)
            tmp_path = tmp_file.name

        for camera_id in clean_cameras:
            try:
                session = unifi_protect_request(
                    "POST",
                    f"/proxy/protect/integration/v1/cameras/{camera_id}/talkback-session",
                    timeout_s=20.0,
                )
                stream_url = _text(session.get("url") or session.get("streamUrl") or session.get("stream_url"))
                if not stream_url:
                    raise RuntimeError("talkback session did not return a stream URL")
                cmd = [
                    ffmpeg_path,
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-nostdin",
                    "-re",
                    "-i",
                    tmp_path,
                    "-vn",
                    *_talkback_audio_args(session),
                    "-f",
                    "rtp",
                    stream_url,
                ]
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=run_timeout, check=False)
                if proc.returncode == 0:
                    sent_count += 1
                    continue
                detail = _text(proc.stderr) or _text(proc.stdout) or f"ffmpeg exited {proc.returncode}"
                failures.append(f"{camera_id} ({detail[:220]})")
            except Exception as exc:
                failures.append(f"{camera_id} ({exc})")
    finally:
        if tmp_path:
            with contextlib.suppress(Exception):
                os.unlink(tmp_path)

    if sent_count:
        result: Dict[str, Any] = {"ok": True, "sent_count": sent_count}
        if failures:
            result["warnings"] = failures
        return result
    return {"ok": False, "sent_count": 0, "error": "; ".join(failures) or "UniFi Protect playback failed."}


def _mime_from_filename(filename: str) -> str:
    mt, _ = mimetypes.guess_type(filename or "")
    return mt or "image/jpeg"


def _to_data_url(image_bytes: bytes, filename: str = "image.jpg") -> str:
    mime = _mime_from_filename(filename)
    b64 = base64.b64encode(image_bytes).decode("ascii")
    return f"data:{mime};base64,{b64}"


class ProtectClient:
    def __init__(self):
        conf = load_unifi_protect_config(required=True)

        vision_settings = get_shared_vision_settings(
            default_api_base="http://127.0.0.1:1234",
            default_model="qwen2.5-vl-7b-instruct",
        )
        self.verify_ssl = False
        self.timeout = 20
        self.base_url = conf["base"].rstrip("/")
        self.api_key = conf["api_key"]
        self.vision_api_base = str(vision_settings.get("api_base") or "http://127.0.0.1:1234").strip().rstrip("/")
        self.vision_model = str(vision_settings.get("model") or "qwen2.5-vl-7b-instruct").strip()
        self.vision_api_key = str(vision_settings.get("api_key") or "").strip()
        self.headers = {"X-API-KEY": self.api_key, "Accept": "application/json"}

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_url}{path}"

    def _req(self, method: str, path: str, *, params=None, json_body=None, headers=None, stream=False) -> Any:
        url = self._url(path)
        hdrs = dict(self.headers)
        if headers:
            hdrs.update(headers)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", InsecureRequestWarning)
            resp = requests.request(
                method,
                url,
                headers=hdrs,
                params=params,
                json=json_body,
                timeout=self.timeout,
                verify=self.verify_ssl,
                stream=stream,
            )

        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")

        ctype = (resp.headers.get("Content-Type") or "").lower()
        if stream or "image/" in ctype:
            return resp.content, resp.headers

        try:
            return resp.json()
        except Exception:
            return resp.text

    def list_sensors(self) -> List[dict]:
        return self._req("GET", "/proxy/protect/integration/v1/sensors") or []

    def get_sensor(self, sensor_id: str) -> dict:
        return self._req("GET", f"/proxy/protect/integration/v1/sensors/{sensor_id}")

    def list_cameras(self) -> List[dict]:
        return self._req("GET", "/proxy/protect/integration/v1/cameras") or []

    def get_camera_snapshot(self, camera_id: str) -> Tuple[bytes, str]:
        candidates = [
            f"/proxy/protect/integration/v1/cameras/{camera_id}/snapshot",
            f"/proxy/protect/integration/v1/cameras/{camera_id}/snapshot.jpg",
            f"/proxy/protect/integration/v1/cameras/{camera_id}/snapshot?format=jpeg",
            f"/proxy/protect/integration/v1/cameras/{camera_id}/snapshot?force=true",
            f"/proxy/protect/integration/v1/cameras/{camera_id}/snapshot?force=true&format=jpeg",
        ]

        last_err = None
        for path in candidates:
            try:
                data, headers = self._req(
                    "GET",
                    path,
                    headers={"Accept": "image/jpeg,image/png,image/*,*/*"},
                    stream=True,
                )
                ctype = (headers.get("Content-Type") or "").split(";")[0].strip().lower()
                if not ctype:
                    ctype = "image/jpeg"
                if isinstance(data, (bytes, bytearray)) and len(data) > 1000:
                    return bytes(data), ctype
            except Exception as exc:
                last_err = exc
                continue

        raise RuntimeError(f"Snapshot not available for camera {camera_id}. Last error: {last_err}")

    def call_vision(self, image_bytes: bytes, *, prompt: str, filename: str = "image.jpg") -> str:
        url = f"{self.vision_api_base}/v1/chat/completions"
        data_url = _to_data_url(image_bytes, filename)

        payload = {
            "model": self.vision_model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt or "Describe this image."},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
            "temperature": 0.2,
            "max_tokens": 500,
        }

        headers = {"Content-Type": "application/json"}
        if self.vision_api_key:
            headers["Authorization"] = f"Bearer {self.vision_api_key}"

        resp = requests.post(url, json=payload, headers=headers, timeout=90)
        if resp.status_code != 200:
            return f"(vision error {resp.status_code}) {resp.text[:200]}"

        payload = resp.json()
        try:
            return (payload["choices"][0]["message"]["content"] or "").strip()
        except Exception:
            return "(vision error) Unexpected response"
