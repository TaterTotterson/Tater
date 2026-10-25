# Tater v1.9.2

Tater v1.9.2 lets users speak immediately after the local wake word on native
Echo satellites while keeping the wake audio out of command recognition.

## What's Changed

### Faster Wake-to-Command Handoff

- Uses the wake pre-roll boundary advertised by Tater Echo Firmware v3.1.1 to
  start command voice activity detection on the first post-wake audio frame.
- Keeps marked wake audio available for room arbitration and diagnostics but
  excludes it from command speech recognition without relying on wake-word
  spelling or transcript cleanup.
- Preserves speech captured while the satellite completes wake verification,
  eliminating the pause previously needed before speaking a command.
- Leaves end-of-speech detection and silence timeouts unchanged, retains the
  legacy timing path for older firmware, and fails open safely if an expected
  boundary is not received.
