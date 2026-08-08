import asyncio
import base64

import kernel_tools

from tater_voice import reachy_ambient
from tater_voice import native_satellite
from tater_voice import voice_pipeline
from tater_voice.voice_pipeline import backends


def test_decode_snapshot_accepts_only_bounded_successful_images() -> None:
    encoded = base64.b64encode(b"reachy-jpeg").decode("ascii")

    assert reachy_ambient._decode_snapshot({"ok": True, "image_base64": encoded}) == b"reachy-jpeg"
    assert reachy_ambient._decode_snapshot({"ok": False, "image_base64": encoded}) is None
    assert reachy_ambient._decode_snapshot({"ok": True, "image_base64": "not-base64"}) is None


def test_description_text_normalizes_a_short_spoken_comment() -> None:
    result = {
        "ok": True,
        "data": {"description": '"Quiet office today; I guess I have the place to myself."'},
    }

    assert reachy_ambient._description_text(result) == (
        "Quiet office today; I guess I have the place to myself."
    )


def test_observation_queues_comment_through_native_satellite(monkeypatch) -> None:
    commands = []
    synthesis_values = []
    encoded = base64.b64encode(b"reachy-jpeg").decode("ascii")

    async def has_capability(_selector: str, _capability: str) -> bool:
        return True

    async def snapshot_request(*_args, **_kwargs) -> dict:
        return {"ok": True, "image_base64": encoded}

    async def synthesize(_text: str, *, values: dict):
        synthesis_values.append(values)
        return b"wav-audio", {"sample_rate": 16000}, "test-tts", ""

    async def send_command(selector: str, message_type: str, payload: dict) -> dict:
        commands.append((selector, message_type, payload))
        return {"ok": True}

    monkeypatch.setattr(native_satellite, "client_has_capability", has_capability)
    monkeypatch.setattr(native_satellite, "send_request", snapshot_request)
    monkeypatch.setattr(native_satellite, "send_command", send_command)
    monkeypatch.setattr(
        kernel_tools,
        "image_describe",
        lambda **_kwargs: {
            "ok": True,
            "data": {"description": "Quiet office; I have the place to myself."},
        },
    )
    monkeypatch.setattr(backends, "_native_synthesize_text", synthesize)
    monkeypatch.setattr(
        voice_pipeline,
        "_shared_speech_voice_settings",
        lambda: {
            "VOICE_TTS_BACKEND": "chatterbox",
            "VOICE_TTS_VOICE": "configured-voice.wav",
        },
    )
    monkeypatch.setattr(
        voice_pipeline,
        "_store_tts_url",
        lambda *_args: "http://tater.local/ambient.wav",
    )

    asyncio.run(reachy_ambient._observe_and_comment("native:reachy"))

    assert commands == [
        (
            "native:reachy",
            "play.url",
            {
                "url": "http://tater.local/ambient.wav",
                "text": "Quiet office; I have the place to myself.",
                "tts_kind": "ambient",
            },
        )
    ]
    assert synthesis_values == [
        {
            "VOICE_TTS_BACKEND": "chatterbox",
            "VOICE_TTS_VOICE": "configured-voice.wav",
        }
    ]


def test_schedule_accepts_only_idle_life_and_enforces_server_cooldown(monkeypatch) -> None:
    completed = []

    async def fake_observation(selector: str) -> None:
        completed.append(selector)

    monkeypatch.setattr(reachy_ambient, "_observe_and_comment", fake_observation)
    reachy_ambient._active_tasks.clear()
    reachy_ambient._last_request_at.clear()

    async def scenario() -> tuple[dict, dict, dict, dict, dict]:
        rejected = reachy_ambient.schedule("native:reachy", {"reason": "other"})
        accepted = reachy_ambient.schedule("native:reachy", {"reason": "idle_life"})
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        manual = reachy_ambient.schedule("native:reachy", {"reason": "manual_test"})
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        manual_again = reachy_ambient.schedule(
            "native:reachy",
            {"reason": "manual_test"},
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        cooled_down = reachy_ambient.schedule("native:reachy", {"reason": "idle_life"})
        await asyncio.sleep(0)
        return rejected, accepted, manual, manual_again, cooled_down

    rejected, accepted, manual, manual_again, cooled_down = asyncio.run(scenario())

    assert rejected["ok"] is False
    assert accepted == {"ok": True, "scheduled": True}
    assert manual == {"ok": True, "scheduled": True}
    assert manual_again == {"ok": True, "scheduled": True}
    assert cooled_down["ok"] is False
    assert "cooldown" in cooled_down["error"]
    assert completed == ["native:reachy", "native:reachy", "native:reachy"]


def test_voice_activity_can_cancel_an_active_observation(monkeypatch) -> None:
    started = asyncio.Event()

    async def waiting_observation(_selector: str) -> None:
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(reachy_ambient, "_observe_and_comment", waiting_observation)
    reachy_ambient._active_tasks.clear()
    reachy_ambient._last_request_at.clear()

    async def scenario() -> bool:
        reachy_ambient.schedule("native:reachy", {"reason": "idle_life"})
        await started.wait()
        cancelled = reachy_ambient.cancel("native:reachy")
        await asyncio.sleep(0)
        return cancelled

    assert asyncio.run(scenario()) is True
    assert reachy_ambient._active_tasks == {}
