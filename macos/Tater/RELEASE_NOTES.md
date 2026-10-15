# Tater v1.6.2

Tater v1.6.2 simplifies music-backed announcements into one fully rendered
audio file, improves synchronized Sendspin delivery, and fixes Echo thermostat
control for the Ecobee HomeKit integration.

## What's New

### Music-Backed Announcements

- Renders announcement music and speech into one finished MP3 before playback.
  The file now contains the requested speech offset, ducking attack, speech,
  release, and final fade, so satellites only need to play it.
- Sends the rendered file directly with `play.url` for a single satellite and
  uses Sendspin only when synchronized delivery is needed for satellite groups
  or stereo pairs.
- Applies each Sendspin destination's saved volume and synchronization offset,
  skips unavailable group destinations cleanly, and can wait for finite media
  playback to complete when the caller requests it.
- Removes the retired `audio.scene.start` playback path and its finished-event
  acknowledgment. Normal TTS, assistant replies, media sessions, broadcasts,
  and TTS overlays keep their existing playback routes.

### Echo Climate Controls

- Fixes temperature and HVAC commands for thermostats provided by the Ecobee
  HomeKit integration. Echo temperature changes now reach HomeKit instead of
  moving temporarily on screen and reverting at the next state refresh.
- Preserves the integration's Ecobee device identity and cached thermostat
  state while routing device actions through the installed HomeKit module.

### Little Spud Cleanup

- Removes the unfinished Little Spud Fitness bridge endpoints and request
  models. Existing linked-person identity, chat, home controls, music, TTS, and
  Awareness notification behavior remain available.

## Updating

- macOS users already running v1.0.1 or later can install v1.6.2 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.6.2` or `latest` for the CPU image and
  `v1.6.2-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
