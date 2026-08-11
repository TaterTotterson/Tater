# Tater v1.0.7

Tater v1.0.7 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### One Audible Timeline Across Every Player

- Tater now schedules native ESP satellites, ThirdReality S420 satellites, and
  AirPlay players against one server-owned audible timeline, including mixed
  Satellite1, Voice PE, S420 stereo, and AirPlay/Sonos groups.
- Each player is compensated for its own output pipeline before playback starts,
  then corrected against the shared ideal timeline instead of another device.
  This avoids fixed delays tied to a particular speaker combination.
- Tater learns persistent renderer latency from real playback telemetry while
  retaining gradual drift correction for stable long-running stereo and
  multi-room playback. Sonos continues to use Tater's AirPlay route.

### Updated Satellite Firmware

- Tater Native Firmware `0.3.8` measures completed I2S DMA output and reports
  each ESP board's rendered position and output latency for speaker-facing sync.
- Tater ThirdReality S420 Firmware `0.2.5` reports and learns its MPV output
  latency and supports the same audible-start scheduling used by other players.
- Together, these releases keep mixed Linux, ESP, and AirPlay groups on the same
  audible clock rather than aligning decoder queues that have different depths.

### Accurate Update All Progress

- The Firmware tab's **Update All** flow now has a dedicated Tater-themed batch
  view with true aggregate progress, completed/active/queued/failed counts, and
  an individual progress row for every satellite.
- A single satellite reaching 100% no longer completes the whole batch. Mixed
  ThirdReality and ESP update groups remain visible until every selected device
  succeeds or reports its own actionable failure.

## Updating

- macOS users already running v1.0.1 through v1.0.6 can install v1.0.7 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.0.7` or `latest` for the CPU image and
  `v1.0.7-nvidia` or `nvidia` for the NVIDIA image.
