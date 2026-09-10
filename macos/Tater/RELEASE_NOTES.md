# Tater v1.1.29

Tater v1.1.29 makes native satellite pairing resilient to a lost first
acknowledgement.

## What's Changed

### Retry-safe Satellite Pairing

- Allows a satellite to recover the same newly issued device token when its
  first pairing acknowledgement is lost.
- Restricts retries to the original six-digit code, device identity, and
  hardware identity for a 30-second in-memory window.
- Prevents the Web UI from reporting a successful pairing that leaves the
  satellite permanently retrying an already-consumed code.
- Keeps existing permanent device tokens, normal pairing expiration, and
  unrelated devices unchanged.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.29 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.29` or `latest` for the CPU image and
  `v1.1.29-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
