# Tater v98.1

## What's Changed

### Native Satellite Reply Playback

- Fixed conversational replies stopping after `TTS_END` without sending the
  satellite its `play.url` command.
- Corrected ducking-value normalization so reply playback cannot fail while
  preparing the duck level, attack, or release settings.
- Preserves ordinary reply playback on pre-0.3.0 firmware while retaining the
  new ducking and overlay behavior on firmware 0.3.0.
- Added regression coverage for playback command delivery and bounded ducking
  values.
