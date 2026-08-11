# Tater v1.0.4

Tater v1.0.4 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### Synchronized Satellite Music

- Tater now preserves remote music sources as live satellite streams instead of
  downloading the entire synchronized track and re-hosting it locally. Tater
  Tube can therefore display the active stream for each playing satellite.
- Voice replies, local-only sources, embedded audio, and resumed playback remain
  preloaded through Tater where that behavior is required.
- Improved native stereo and multi-room correction using a rendered-audio clock,
  jitter smoothing, consistent-direction confirmation, and a slower four-second
  rate adjustment. This avoids reacting to MPV timeline jitter as real speaker
  drift.
- Rendered-clock correction is enabled only when every member of a playback
  group supports it. Mixed or older firmware groups consistently use the legacy
  source clock instead of comparing incompatible measurements.
- Fixed native playback recovery logging so an underrun or rebuffer event is
  recorded correctly for troubleshooting.

### ThirdReality S420 Firmware

- ThirdReality stereo pairs should update both speakers to Tater S420 firmware
  `0.2.2`. That firmware reports MPV's rendered-audio position, disables the
  audible pitch-correction filter during synchronized playback, and applies the
  gentler drift requests sent by Tater v1.0.4.
- Tater S420 firmware `0.2.2` remains compatible with older Tater versions, but
  the new synchronization behavior requires both firmware `0.2.2` and Tater
  v1.0.4.

## Updating

- macOS users already running v1.0.1 through v1.0.3 can install v1.0.4 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.0.4` or `latest` for the CPU image and
  `v1.0.4-nvidia` or `nvidia` for the NVIDIA image.
