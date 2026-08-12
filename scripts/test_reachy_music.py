import asyncio

from tater_voice import native_satellite
from tater_voice import reachy_music
from tater_voice import voice_pipeline
from tater_voice.voice_pipeline import backends


def test_music_reaction_uses_metadata_configured_voice_and_ducking(monkeypatch) -> None:
    commands = []
    synthesis_values = []

    async def has_capability(_selector: str, capability: str) -> bool:
        return capability == "music_reactions"

    async def generate(**_kwargs) -> str:
        return "Oh yeah, Michael Jackson—my antennas are already dancing!"

    async def synthesize(text: str, *, values: dict):
        synthesis_values.append((text, values))
        return b"wav-audio", {"sample_rate": 16000}, "test-tts", ""

    async def send_command(selector: str, message_type: str, payload: dict) -> dict:
        commands.append((selector, message_type, payload))
        return {"ok": True}

    monkeypatch.setattr(native_satellite, "client_has_capability", has_capability)
    monkeypatch.setattr(native_satellite, "send_command", send_command)
    monkeypatch.setattr(reachy_music, "_generate_reaction", generate)
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
        lambda *_args: "http://tater.local/music-reaction.wav",
    )
    reachy_music._recent_reactions.clear()

    asyncio.run(
        reachy_music._generate_and_speak(
            "native:reachy",
            {
                "session_id": "song-1",
                "title": "Billie Jean",
                "artist": "Michael Jackson",
                "album": "Thriller",
            },
        )
    )

    assert synthesis_values == [
        (
            "Oh yeah, Michael Jackson—my antennas are already dancing!",
            {
                "VOICE_TTS_BACKEND": "chatterbox",
                "VOICE_TTS_VOICE": "configured-voice.wav",
            },
        )
    ]
    assert commands == [
        (
            "native:reachy",
            "play.url",
            {
                "url": "http://tater.local/music-reaction.wav",
                "text": "Oh yeah, Michael Jackson—my antennas are already dancing!",
                "tts_kind": "music_reaction",
                "ducking": {
                    "target_percent": 24,
                    "attack_ms": 180,
                    "release_ms": 420,
                },
            },
        )
    ]


def test_music_reaction_schedule_deduplicates_a_session(monkeypatch) -> None:
    completed = []

    async def fake_reaction(selector: str, _payload: dict) -> None:
        completed.append(selector)

    monkeypatch.setattr(reachy_music, "_generate_and_speak", fake_reaction)
    reachy_music._active_tasks.clear()
    reachy_music._recent_sessions.clear()

    async def scenario() -> tuple[dict, dict]:
        first = reachy_music.schedule("native:reachy", {"session_id": "song-1"})
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        duplicate = reachy_music.schedule("native:reachy", {"session_id": "song-1"})
        return first, duplicate

    first, duplicate = asyncio.run(scenario())

    assert first == {"ok": True, "scheduled": True}
    assert duplicate == {"ok": True, "scheduled": False, "duplicate": True}
    assert completed == ["native:reachy"]


def test_generated_reaction_prompt_contains_variation_and_safety_rules(monkeypatch) -> None:
    calls = []

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def chat(self, **kwargs):
            calls.append(kwargs)
            return {"message": {"content": "Yes, Billie Jean has my whole body moving!"}}

    monkeypatch.setattr(
        voice_pipeline,
        "get_llm_client_from_env",
        lambda **_kwargs: FakeClient(),
    )

    result = asyncio.run(
        reachy_music._generate_reaction(
            title="Billie Jean",
            artist="Michael Jackson",
            album="Thriller",
            recent=["Michael Jackson has my antennas moving!"],
        )
    )

    assert result == "Yes, Billie Jean has my whole body moving!"
    system_prompt = calls[0]["messages"][0]["content"]
    user_prompt = calls[0]["messages"][1]["content"]
    assert "do not repeat or closely paraphrase" in system_prompt
    assert "Never invent lyrics, genre, history" in system_prompt
    assert "Michael Jackson" in user_prompt
    assert "Billie Jean" in user_prompt
    assert calls[0]["temperature"] == 0.95
