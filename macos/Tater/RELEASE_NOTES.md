# Tater v1.1.16

Tater v1.1.16 tightens the refreshed Spudex workspace and makes Reachy Mini's
ambient room comments more dependable.

## What's Changed

### Spudex Workbench

- Locks the side-by-side chat and activity terminal to their original
  empty-state height so long conversations no longer stretch the entire
  workbench.
- Keeps chat and command output independently scrollable inside their fixed
  panes on desktop, tablet, and mobile layouts.
- Starts Manual Session at the visible protected `agent_lab` root instead of
  silently starting in the nearly empty `/workspace` directory.
- Adds persistent `cd` navigation to Manual Session, updates the displayed
  working directory after each change, and keeps `ls` and `pwd` operating from
  the selected directory.
- Preserves the `agent_lab` security boundary: directory changes outside the
  protected root are rejected.

### Reachy Mini Ambient Life

- Updates Reachy's ambient vision prompt to follow the scheduler's decision
  and return one short, visible-scene-based comment instead of independently
  choosing silence again.
- Distinguishes vision request failures and empty responses from an intentional
  silent result in runtime logs, making unavailable models and other failures
  easier to diagnose.
- Retains the existing privacy rules that prevent identifying people or
  inferring sensitive personal traits from ambient snapshots.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.16 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.16` or `latest` for the CPU image and
  `v1.1.16-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
