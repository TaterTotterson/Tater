# Tater v2.0.1

Tater v2.0.1 improves wake-to-command audio boundaries, removes an unnecessary
Sendspin music-start delay, and makes Hydra conversations faster and more
cache-friendly while keeping full time awareness and action offers intact.

## What's Changed

### Cleaner Wake-to-Command Audio

- Treats the native firmware wake-boundary marker as authoritative. Audio from
  before that marker is excluded from both speech recognition and command VAD,
  preventing nearby speech or other sound before the wake word from leaking
  into the user's request.
- Starts command capture with the first buffered frame after the marker instead
  of using a fixed delay, so users can still begin speaking immediately after
  the wake word without losing the opening word of the command.
- Lets Spud Link speech recognition and endpointing safely share the same
  post-boundary command stream, with clearer diagnostics for excluded wake
  audio.

### Faster Sendspin Music Starts

- Starts playback preparation as soon as a satellite reports that its player is
  synchronized instead of waiting for eight clock exchanges on every fresh
  Sendspin session.
- Clock exchanges continue in the background, preserving timestamped playback
  and multi-satellite synchronization without the previous roughly 15-second
  startup wait on affected Noise sessions.
- Removes the extra post-handshake pause from live and paired PCM streaming,
  improving play, skip, and other fresh-stream music commands.

### Faster Hydra Chat and Better Prompt Caching

- Makes Astraeus the sole owner of tool selection and structured pending-action
  state. Hermes now receives only the human-readable action guidance it needs,
  rather than the complete capability catalog and internal tool IDs.
- Returns normal Hermes chat as streamable plain text instead of wrapping every
  conversational response in a structured JSON envelope, reducing prompt and
  completion overhead for simple chat.
- Keeps stable instructions and Astraeus's execution catalog at the reusable
  front of each prompt. Conversation history follows the stable prefix, while
  changing memory, action guidance, and the current local date, time, and
  timezone are placed in the dynamic per-turn area.
- Preserves full time awareness on every turn and keeps Astraeus-created action
  offers available for the user's next confirmation without exposing internal
  tool metadata to Hermes.
