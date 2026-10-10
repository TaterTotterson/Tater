"""Minimal encrypted Sendspin transport used by Tater's audio source.

Sendspin clients speak Noise_KKpsk2 inside WebSocket frames.  Tater only
needs the unpaired Sentinel flow here; pairing remains owned by full Sendspin
servers such as Music Assistant.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import os
import secrets
from pathlib import Path
from typing import Any, Optional

import aiohttp
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import x25519


PROTOCOL_VERSION = 1
SENTINEL_PSK = hashlib.sha256(b"sendspin-sentinel-psk-v1").digest()
PSK_ID = base64.urlsafe_b64encode(
    hashlib.sha256(b"sendspin-psk-id-v1" + SENTINEL_PSK).digest()
).rstrip(b"=").decode("ascii")
MAX_TRANSPORT_PLAINTEXT = 65_535 - 16
MSG_JSON = 0
MSG_FRAGMENT = 1
FRAGMENT_LAST = 1
FRAGMENT_FIRST = 2
HANDSHAKE_TIMEOUT_S = 10.0


class SendspinNoiseError(RuntimeError):
    """Raised when a Sendspin Noise handshake or transport frame is invalid."""


def _b64u_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64u_decode(value: Any) -> bytes:
    token = str(value or "").strip()
    return base64.urlsafe_b64decode(token + "=" * ((-len(token)) % 4))


def _json_bytes(message: dict[str, Any]) -> bytes:
    return json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _identity_path() -> Path:
    configured = str(os.getenv("TATER_SENDSPIN_SERVER_KEY_PATH") or "").strip()
    if configured:
        return Path(configured).expanduser()
    runtime = str(os.getenv("TATER_RUNTIME_DIR") or "").strip()
    root = Path(runtime).expanduser() if runtime else Path.home() / ".taterassistant" / "runtime"
    return root / "sendspin" / "server-identity.key"


def _load_or_create_identity() -> tuple[bytes, bytes]:
    path = _identity_path()
    try:
        private_bytes = path.read_bytes()
    except FileNotFoundError:
        private_bytes = secrets.token_bytes(32)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_bytes(private_bytes)
        with contextlib.suppress(OSError):
            temporary.chmod(0o600)
        os.replace(temporary, path)
    if len(private_bytes) != 32:
        raise SendspinNoiseError(f"invalid Sendspin server identity at {path}")
    private_key = x25519.X25519PrivateKey.from_private_bytes(private_bytes)
    public_bytes = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return private_bytes, public_bytes


class SendspinNoiseTransport:
    """Encrypt Sendspin application messages after a server-side handshake."""

    def __init__(self, ws: aiohttp.ClientWebSocketResponse, noise: Any) -> None:
        self.ws = ws
        self.noise = noise
        self._fragment = bytearray()
        self._fragment_type: Optional[int] = None

    @property
    def closed(self) -> bool:
        return bool(self.ws.closed)

    async def send_str(self, value: str) -> None:
        await self._send_plaintext(bytes((MSG_JSON,)) + value.encode("utf-8"))

    async def send_bytes(self, value: bytes) -> None:
        packet = bytes(value)
        if not packet:
            raise SendspinNoiseError("Sendspin binary packet is empty")
        await self._send_plaintext(packet)

    async def _send_plaintext(self, plaintext: bytes) -> None:
        if len(plaintext) <= MAX_TRANSPORT_PLAINTEXT:
            await self.ws.send_bytes(bytes(self.noise.encrypt(plaintext)))
            return
        original_type = plaintext[0]
        payload = memoryview(plaintext)[1:]
        first_capacity = MAX_TRANSPORT_PLAINTEXT - 3
        next_capacity = MAX_TRANSPORT_PLAINTEXT - 2
        offset = 0
        first = True
        while offset < len(payload):
            capacity = first_capacity if first else next_capacity
            chunk = bytes(payload[offset : offset + capacity])
            offset += len(chunk)
            flags = (FRAGMENT_FIRST if first else 0) | (
                FRAGMENT_LAST if offset >= len(payload) else 0
            )
            header = bytes((MSG_FRAGMENT, flags, original_type)) if first else bytes((MSG_FRAGMENT, flags))
            await self.ws.send_bytes(bytes(self.noise.encrypt(header + chunk)))
            first = False

    async def receive_text(self) -> Optional[str]:
        """Return one decrypted JSON body, or ``None`` when the socket closes."""
        while True:
            message = await self.ws.receive()
            if message.type in {
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSING,
                aiohttp.WSMsgType.CLOSED,
            }:
                return None
            if message.type == aiohttp.WSMsgType.ERROR:
                raise SendspinNoiseError(str(self.ws.exception() or "Sendspin socket failed"))
            if message.type != aiohttp.WSMsgType.BINARY:
                raise SendspinNoiseError("unencrypted frame received after the Sendspin handshake")
            try:
                plaintext = bytes(self.noise.decrypt(bytes(message.data)))
            except Exception as exc:
                raise SendspinNoiseError("Sendspin transport authentication failed") from exc
            complete = self._accept_plaintext(plaintext)
            if complete is None:
                continue
            if not complete or complete[0] != MSG_JSON:
                continue
            try:
                return complete[1:].decode("utf-8")
            except UnicodeDecodeError as exc:
                raise SendspinNoiseError("Sendspin JSON was not UTF-8") from exc

    def _accept_plaintext(self, plaintext: bytes) -> Optional[bytes]:
        if not plaintext:
            raise SendspinNoiseError("empty Sendspin transport message")
        if plaintext[0] != MSG_FRAGMENT:
            if self._fragment_type is not None:
                raise SendspinNoiseError("Sendspin fragment sequence was interrupted")
            return plaintext
        if len(plaintext) < 2 or plaintext[1] & 0xFC:
            raise SendspinNoiseError("malformed Sendspin fragment")
        flags = plaintext[1]
        payload = plaintext[2:]
        if flags & FRAGMENT_FIRST:
            if self._fragment_type is not None or not payload:
                raise SendspinNoiseError("malformed first Sendspin fragment")
            self._fragment_type = payload[0]
            self._fragment = bytearray(payload[1:])
        else:
            if self._fragment_type is None:
                raise SendspinNoiseError("orphan Sendspin continuation fragment")
            self._fragment.extend(payload)
        if not flags & FRAGMENT_LAST:
            return None
        assert self._fragment_type is not None
        complete = bytes((self._fragment_type,)) + bytes(self._fragment)
        self._fragment_type = None
        self._fragment.clear()
        return complete


async def server_handshake(
    ws: aiohttp.ClientWebSocketResponse,
    client_init_text: str,
) -> tuple[SendspinNoiseTransport, str, int]:
    """Complete the unpaired Sentinel handshake initiated by a Sendspin client."""
    try:
        from noise.connection import Keypair, NoiseConnection
    except Exception as exc:
        raise SendspinNoiseError(
            "noiseprotocol is unavailable; reinstall Tater's Python dependencies"
        ) from exc

    try:
        client_init = json.loads(client_init_text)
        payload = client_init["payload"]
        if client_init.get("type") != "client/init":
            raise ValueError("not client/init")
        client_id = str(payload["client_id"])
        version = int(payload["version"])
        suite = str(payload["suite"])
        remote_public = _b64u_decode(client_id)
    except Exception as exc:
        raise SendspinNoiseError("malformed Sendspin client/init") from exc
    if version != PROTOCOL_VERSION:
        raise SendspinNoiseError(f"unsupported Sendspin core version {version}")
    if suite not in {"25519_ChaChaPoly_SHA256", "25519_AESGCM_SHA256"}:
        raise SendspinNoiseError(f"unsupported Sendspin Noise suite {suite}")
    if len(remote_public) != 32:
        raise SendspinNoiseError("invalid Sendspin client identity")

    private_bytes, public_bytes = _load_or_create_identity()
    server_init = _json_bytes(
        {
            "type": "server/init",
            "payload": {
                "server_id": _b64u_encode(public_bytes),
                "version": PROTOCOL_VERSION,
            },
        }
    )
    noise = NoiseConnection.from_name(f"Noise_KKpsk2_{suite}".encode("ascii"))
    noise.set_as_initiator()
    noise.set_keypair_from_private_bytes(Keypair.STATIC, private_bytes)
    noise.set_keypair_from_public_bytes(Keypair.REMOTE_STATIC, remote_public)
    noise.set_prologue(client_init_text.encode("utf-8") + server_init)
    noise.set_psks(psks=[SENTINEL_PSK])
    noise.start_handshake()

    await ws.send_str(server_init.decode("utf-8"))
    message_one = bytes(
        noise.write_message(
            _json_bytes({"psk_id": PSK_ID, "psk_category": "sn"})
        )
    )
    await ws.send_str(
        _json_bytes(
            {
                "type": "noise/handshake",
                "payload": {"data": _b64u_encode(message_one)},
            }
        ).decode("utf-8")
    )
    try:
        message = await asyncio.wait_for(ws.receive(), timeout=HANDSHAKE_TIMEOUT_S)
    except asyncio.TimeoutError as exc:
        raise SendspinNoiseError("timed out awaiting Sendspin Noise message 2") from exc
    if message.type != aiohttp.WSMsgType.TEXT:
        raise SendspinNoiseError("expected Sendspin Noise message 2 as text")
    try:
        envelope = json.loads(message.data)
        if envelope.get("type") != "noise/handshake":
            raise ValueError("not noise/handshake")
        encrypted = _b64u_decode(envelope["payload"]["data"])
        response = bytes(noise.read_message(encrypted))
    except Exception as exc:
        raise SendspinNoiseError("Sendspin Noise message 2 authentication failed") from exc
    if response != b"{}" or not noise.handshake_finished:
        raise SendspinNoiseError("invalid Sendspin Noise message 2 payload")
    return SendspinNoiseTransport(ws, noise), client_id, version
