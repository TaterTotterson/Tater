# Tater v1.1.30

Tater v1.1.30 improves native satellite pairing and wake-model reconnect
behavior.

## What's Changed

### More Reliable Satellite Handshakes

- Sends the native satellite `hello.ack` immediately after authentication,
  before device registration and voice-pipeline setup.
- Lets a newly paired satellite save its permanent credential without waiting
  on the rest of the connection setup.
- Complements the longer recovery window in native firmware `0.3.22`.

### Cached Wake Models Across Reconnects

- Supplies a stable revision for custom wake-model packages.
- Keeps an unchanged active or cached model across routine reconnects and
  settings replays instead of downloading it again.
- Refreshes the model when its package content actually changes.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.30 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.30` or `latest` for the CPU image and
  `v1.1.30-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
