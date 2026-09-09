# Tater v1.1.28

Tater v1.1.28 fixes command launching in Spudex on macOS.

## What's Changed

### Fork-safe macOS Commands

- Replaces the fork-based macOS subprocess path with `posix_spawn`, avoiding a
  Network framework crash before commands could start.
- Fixes `-11` failures from normal system commands such as `df` and `du`.
- Prevents the repeated “Python quit unexpectedly” dialogs caused by those
  pre-exec child-process crashes.
- Applies the fix to Spudex Chat, Hydra-triggered work, and the Manual terminal
  in both Full access and Restricted modes.
- Preserves command output streaming, timeouts, background-process control,
  working-directory behavior, and shell syntax in Full access mode.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.28 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.28` or `latest` for the CPU image and
  `v1.1.28-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
