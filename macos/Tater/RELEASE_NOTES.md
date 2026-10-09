# Tater v1.4.0

Tater v1.4.0 adds a complete Meshtastic workspace, including live channel
chat, node details, and secure Bluetooth pairing through compatible Echo
satellites, while making Echo wake-profile changes safer and more reliable.

## What's Changed

### Meshtastic Core

- Adds a native live Meshtastic chat experience for the Meshtastic Core from
  the Tater Shop. Channels appear in a dedicated rail, conversations update
  live, and incoming and outgoing messages retain their sender, timestamp, and
  channel history.
- Adds a chat-style composer with per-channel drafts, character limits, Enter
  to send, Shift+Enter for a new line, and automatic scrolling that does not
  pull the view away while older messages are being read.
- Reworks the Nodes view into responsive Tater-themed cards with clearer node
  identity, status, last-seen, signal, and hop information instead of raw
  values that are difficult to scan.
- Reworks Bluetooth Pairing around a prominent Current Connection card,
  friendly satellite and radio details, cleaner discovered-device cards,
  six-digit PIN entry, refresh controls, and an explicit unpair action.

### Meshtastic Through Echo Satellites

- Meshtastic Core can use the Bluetooth radio in a compatible Biscuit,
  Checkers, or Rook satellite instead of requiring a separate external
  Bluetooth bridge. Tater discovers radios through the satellite, then carries
  GATT reads, writes, and notifications over the existing authenticated native
  satellite connection.
- Pairing is protected by the Meshtastic device's six-digit PIN. The Echo owns
  the persistent Bluetooth bond, the PIN is not stored, and Tater exposes the
  connected device and unpair controls in the Core UI.
- Satellite Bluetooth connections require Tater Echo Firmware v2.4.0 or newer
  and the current Meshtastic Core from the Tater Shop. Existing bridge-based
  Meshtastic installations remain available for systems that do not use a
  compatible Echo satellite.

### Echo Wake Settings

- Applies detector mode and wake-model selections as one coherent update so an
  Echo does not receive a half-updated MWW, OWW, or Dual Wake Word profile while
  settings are being saved.
- Removes stale inactive detector packages from the firmware payload, keeps
  the detector enable flags consistent with the selected mode, and validates
  an active custom model before replacing the working profile.

### Release Images

- Keeps the canonical `latest` container tag on the CPU image and the `nvidia`
  tag on the NVIDIA image, while continuing to publish versioned CPU and
  NVIDIA tags for each release.

## Updating

- macOS users already running v1.0.1 or later can install v1.4.0 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.4.0` or `latest` for the CPU image and
  `v1.4.0-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
