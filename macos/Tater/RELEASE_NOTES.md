# Tater v98

## What's Changed

### AI Task Broadcast Delivery

- AI Task now has first-class broadcast delivery, so users choose the destination while creating the task instead of adding “then broadcast this” to the prompt.
- Scheduled tasks deliver the final prepared response directly without asking a second LLM to rewrite it.
- Broadcast destinations can be everywhere, an individual native satellite, or a configured stereo pair.
- Added four generated background-audio presets plus validated WAV, MP3, and FLAC uploads stored under Agent Lab.
- Per-task controls cover background volume, TTS volume, ducking level, attack/release timing, and finish fade.
- Updated AI Task Core to 1.2.0 and Broadcast Verba to 1.3.0 in Tater Shop.

### Satellite Music Ducking and Audio Scenes

- Audio ACE and other music playback now use persistent native media sessions.
- Unrelated TTS plays as an overlay, ducks the active music, and restores the prior music level without restarting the track.
- Voice replies still honor each satellite’s Reply Playback destination.
- Added global satellite duck level, attack, and release controls under Speech settings.
- AI Task audio scenes can loop a background beneath TTS and stop it automatically when the announcement finishes.
- Older firmware falls back clearly instead of silently claiming audio-scene support.

### Synchronized Stereo Pairs

- Added Stereo Pairs under Voice → Satellites with left/right member selection.
- A pair appears as one reusable destination throughout Tater, AI Task, Broadcast, and Audio ACE.
- Both members prebuffer the same source and start from a shared synchronized timestamp.
- Music uses true left/right routing while TTS remains centered across the pair.
- Per-side level and delay calibration is preserved while playhead telemetry and bounded drift correction maintain alignment.
- TTS ducks both speakers together and synchronized AI Task backgrounds stop when the TTS completes.

### Tater Native Firmware 0.3.0

- Supports Voice PE, Satellite1, ReSpeaker XVF3800, and S3 Box with one shared 0.3.0 release.
- Adds persistent sessions, versioned audio scenes, scheduled overlays, stereo channel selection, clock synchronization, and drift correction.
- Existing `play.url` replies are automatically promoted to ducking overlays while media is active.
- Firmware update availability continues to come from the signed native release manifest.

### Native Media URL Reliability

- `VOICE_CORE_PUBLIC_BASE_URL` now applies consistently to native TTS, streamed Chatterbox TTS, music, and background-audio URLs.
- Bare hosts, full HTTP(S) URLs, explicit ports, paths, and IPv6-style authorities are normalized without duplicated schemes or ports.

### Validation

- Added regression coverage for stereo-pair persistence and compatibility, synchronized start/overlay behavior, drift correction, persistent media sessions, audio-scene routing, background-asset security, and public media URLs.
