# Tater v96

## What's Changed

- Fixed native llama.cpp streaming text so emoji, smart punctuation, and other UTF-8 characters no longer appear as mojibake in replies.
- Added smooth live reply rendering to the Web UI, including frame-coalesced updates, a streaming cursor, stable auto-scroll, reduced-motion support, and clean replacement by the completed message.
- Added native response chunks to the Little Spud Spud Link protocol so supported clients can render Tater replies as they arrive while tool notices and final tool results remain reliable.
- Preserved normal one-shot behavior for providers that do not stream: Tater waits for a second response chunk before showing a live preview and otherwise delivers the completed reply atomically.
- Added the Tater side of Little Spud Home controls with authenticated room summaries, grouped device categories, bulk light/fan/switch/plug controls, light brightness, garage door and cover actions, lock actions, and compact read-only sensor status.
- Kept Home controls provider-neutral and safe by using the existing integration registry, allowing only advertised category actions, leaving unsupported categories read-only, and never exposing individual device inventories to Little Spud.
- Improved integration state caching after brightness changes so room status and controls reflect the requested light level immediately.
