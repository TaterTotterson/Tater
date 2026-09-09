# Tater v1.1.27

Tater v1.1.27 upgrades Spudex into a practical AI-powered host terminal with a
single, clearly warned full-access setting.

## What's Changed

### Spudex Full Access

- Replaces the collection of granular command-policy switches with one Full
  access toggle and an explicit host-impact warning.
- Applies the same execution mode to Spudex Chat, Hydra-triggered work, and the
  Manual terminal.
- Runs Manual commands through the host shell in Full access mode, enabling
  pipes, redirects, globs, shell built-ins, installs, and other normal terminal
  syntax.
- Inherits the host command environment in Full access mode so installed tools
  and system utilities work as expected.
- Keeps `agent_lab` as the starting folder without treating it as a filesystem
  boundary.
- Migrates existing installations with the old command policy disabled to the
  new Full access mode automatically.
- Retains Restricted mode for command filtering and available OS isolation.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.27 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.27` or `latest` for the CPU image and
  `v1.1.27-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
