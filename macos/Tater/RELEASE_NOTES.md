# Tater v98.5

## What's Changed

### Music Core and Playback

- Updated the Tater Shop **Music Core** to version 2.1.0 with a persistent
  player bar, compact transport controls, and a speaker picker built directly
  into the player.
- Reworked Genres, Artists, and Albums into a responsive, multi-column library
  browser with nested tabs and paging instead of one item per row.
- Expanded playback discovery to connected and saved offline native satellites,
  stereo pairs, Home Assistant speakers, Roon zones, and integration media
  players that expose a standard `play_media` action.
- Added synchronized stereo TTS completion tracking. Tater now waits for both
  satellites to finish before reopening continued listening instead of relying
  only on an estimated audio delay.
- Added reusable Core UI support for persistent cards, card variants, nested
  paged groups, icon-button labels, tooltips, and responsive library layouts.

### Portal Identity and Admin Safety

- Made the latest portal event the authoritative source for the current speaker,
  person match, room, conversation, and admin/tool permissions.
- Tool-generated origin data can no longer replace trusted portal identity or
  enable kernel tools, Core tools, or advanced tools for another user.
- Added a per-turn speaker boundary so names from older shared-channel history
  are not applied to the newest Discord, Telegram, Matrix, IRC, HomeKit,
  Meshtastic, XBMC, or Little Spud message.
- Updated the Tater Shop **Discord Portal** to version 1.0.9 with explicit
  current-speaker metadata and regression coverage for shared Discord history.

### Camera and Device Verbas

- Restored the standalone Tater Shop **Camera Control** Verba at version 1.0.3.
  It can capture a named camera snapshot, run Tater's configured vision model,
  and return a description for questions such as “what's happening in the front
  yard?” or “who is at the door?”
- Updated **Device Control** to version 1.0.3 so non-camera smart-home control
  remains unified while camera snapshots and visual questions route to Camera
  Control.
- Kept the standalone device status/query Verbas available and bumped their
  self-contained Shop packages to version 1.0.1. They no longer depend on the
  removed shared device helper, fallback, or migration layer.

### Native Firmware 0.3.2

- Released Tater Native Firmware 0.3.2 for Voice PE, Satellite1, ReSpeaker
  XVF3800, and S3 Box.
- Native TTS media sessions now show and complete speaking/tool-call visual
  states correctly on success, failure, or stop instead of leaving a satellite
  stuck in a speaking animation.
- Persistent music sessions remain independent from transient TTS visual-state
  completion.

### Build Version in TaterOS

- Added the installed Tater version to the bottom of the left menu.
- The label reads the version bundled into that specific app build, so upgrades
  and rollbacks automatically display their own version without a separate UI
  value to maintain.
