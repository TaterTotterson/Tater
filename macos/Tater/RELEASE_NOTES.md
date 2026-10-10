# Tater v1.5.3

Tater v1.5.3 improves synchronized ThirdReality playback, cleans up satellite
wake-word controls, and fixes direct Google Cast media playback.

## What's New

### ThirdReality Sendspin Playback

- Adds secure Sendspin negotiation for newer satellite clients, restoring reply
  playback through ThirdReality S420 stereo pairs.
- Keeps compatibility with existing Echo and older plaintext Sendspin clients,
  so mixed satellite groups continue to work without reconfiguration.
- Completes the player activation and readiness exchange expected by current
  Sendspin clients before synchronized audio begins.

### Clearer Satellite Settings

- Removes the global MWW, OWW, and Dual Wake Word selector from individual
  satellite popups, where it could incorrectly appear on unsupported devices.
- Keeps room-specific Wake Tuning controls in each satellite popup. Wake mode
  and model selection remain in Tater's dedicated wake-word settings.

### Google Cast Playback

- Updates the Google Cast integration to launch Cast's Default Media Receiver
  before loading direct audio or video. This prevents an existing app such as
  YouTube from silently swallowing a generic media request.
- Keeps the Cast device's current volume when a playback request does not name
  a volume instead of defaulting it to 100 percent.
- Continues to honor explicit requests such as “play this on the office TV at
  20 percent.”
- Requires Google Cast integration v1.0.2 and Cast Media Verba v1.0.5 for the
  complete fix.

## Updating

- macOS users already running v1.0.1 or later can install v1.5.3 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.5.3` or `latest` for the CPU image and
  `v1.5.3-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
