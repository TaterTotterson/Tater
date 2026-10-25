# Tater v1.9.3

Tater v1.9.3 starts supported TTS replies sooner, adds Chatterbox as a fully
built-in voice-cloning option, and pairs with Tater Echo Firmware v3.1.1 for
true incremental playback on Echo satellites.

## What's Changed

### Faster Streaming TTS

- Automatically streams replies from Pocket TTS, Piper, Kokoro, Wyoming,
  OpenAI-compatible servers, remote Chatterbox, and built-in Chatterbox when
  the selected satellite advertises incremental WAV playback.
- Begins playback as the first PCM chunks arrive instead of waiting for the
  complete reply. Backends that fail before sending audio can safely fall back
  to Wyoming, while older satellites continue using buffered playback.
- Removed the old Early-Start TTS setting and segmented-URL playback path.
  Streaming is now negotiated automatically; Qwen3-TTS and OmniVoice remain
  buffered because their current local generation APIs return complete audio.

### Built-in Chatterbox

- Added locally managed Chatterbox Turbo 350M and Nano 110M models alongside
  the existing remote Chatterbox server option.
- Supports the bundled voice or an optional 5–10 second reference recording,
  independent reply and announcement voices, CPU, CUDA, ROCm, and Apple MPS,
  plus temperature and deterministic seed controls.
- Caches voice conditioning between streamed sentences and keeps the model in
  its own pinned managed runtime so Qwen3-TTS and OmniVoice environments remain
  isolated.

### Echo Firmware v3.1.1

- Recognizes the firmware's `streaming_voice_playback_version: 1` capability
  and serves bounded, unknown-length PCM WAV streams that begin playing before
  the HTTP response is complete.
- Keeps buffered MP3 and WAV compatibility for older Echo firmware and logs
  structured playback failures reported by updated satellites.
- Retains the v1.9.2 wake-boundary handoff, so updated Echo satellites support
  both immediate post-wake speech and faster spoken replies without changing
  VAD, end-of-speech detection, or Partial STT behavior.
