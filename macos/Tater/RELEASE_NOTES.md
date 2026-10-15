# Tater v1.6.1

Tater v1.6.1 completes the Little Spud Fitness connection, links each mobile
app to a person in Tater, and keeps Awareness notifications synchronized with
their authoritative event history.

## What's New

### Little Spud Fitness

- Adds the private SpudLink bridge used by Fitness Core to receive approved
  daily health aggregates from Little Spud on iPhone and Android.
- Supports activity, sleep, heart, vital, and body measurements; generated
  wellness summaries; and opt-in activity challenges between people in Tater.
- Keeps Fitness optional. When Fitness Core is not installed or running,
  Little Spud receives a clear unavailable response and performs no health-data
  upload.

### People and Device Identity

- Lets a Little Spud installation link directly to a person already configured
  in Tater People, and reuses that identity for Fitness, challenges, and chat.
- Adds stable paired-app aliases so the same person remains associated with the
  same phone across reconnects and app sessions.

### Notification Reliability

- Reconciles the Little Spud notification inbox against Awareness Core so
  events removed from Tater are also removed from the mobile apps.
- Includes attachment expiry information with notification media, preventing
  expired Awareness snapshots or videos from remaining as broken items.

## Updating

- macOS users already running v1.0.1 or later can install v1.6.1 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.6.1` or `latest` for the CPU image and
  `v1.6.1-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
