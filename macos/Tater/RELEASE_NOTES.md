# Tater v1.1.19

Tater v1.1.19 fixes typed commands in the Spudex Manual Session terminal.

## What's Changed

### Spudex Manual Terminal

- Fixes a request parsing issue where the Manual tab sent the typed command
  together with an empty argument list and the empty list incorrectly won.
- Prevents valid commands such as `ls` from turning into repeated
  "No command was provided" policy messages.
- Restores normal single-command terminal use for commands such as `ls`,
  `mkdir new-folder`, `cd new-folder`, `pwd`, Git commands, and installed
  executables allowed by the current Spudex policy.
- Adds regression coverage using the same empty-argument request shape sent by
  the browser, including a complete `ls` execution through Manual Session.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.19 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.19` or `latest` for the CPU image and
  `v1.1.19-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
