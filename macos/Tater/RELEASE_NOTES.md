# Tater v1.0.6

Tater v1.0.6 is a full release for Docker and macOS. The release tag publishes
the multi-architecture CPU image and NVIDIA image in addition to the macOS app.

## What's Changed

### ThirdReality S420 Is Ready for Tater

- The [ThirdReality Voice & Music Assistant Dev Edition](https://www.thirdreality.com/products/voice-music-assistant-dev-edition)
  is ready for everyday Tater use with released factory and OTA firmware,
  Tater hotspot setup, local wake words, voice, LEDs, buttons, volume controls,
  stereo pairs, synchronized multi-room music, and settings-preserving OTA.
- When ordering, choose **Log Output: YES [With Log]**. That option includes the
  debug board required for the initial Tater Local USB factory flash and future
  factory recovery. The debug board is not required for routine OTA updates.
- Tater S420 firmware `0.2.4` is the current release. It preserves Wi-Fi, Tater
  pairing, room assignment, and device settings during routine OTA updates.

### Mixed-Satellite Music Synchronization

- Tater Native Firmware `0.3.7` adds a rendered-audio clock to Satellite1,
  Voice PE, ReSpeaker XVF3800, and S3 Box satellites. Mixed groups can now use
  the same speaker-facing timing model already used by ThirdReality S420s.
- Native satellites account for their configured I2S hardware queues when
  reporting playback position, so Tater aligns audio reaching the speaker
  instead of decoder samples that are still waiting to play.
- Satellite1 silence priming now occurs before the synchronized start deadline,
  removing its built-in late first sample without adding an artificial delay.

### Native Satellite OTA Handoff

- Tater now completes the progress window when a native satellite verifies the
  signed firmware and confirms that it is rebooting into its installer. The
  satellite owns the remaining installation and reboot while the connection is
  intentionally unavailable.
- This restores the reliable handoff behavior from Tater v1.0.4 and prevents a
  successful ThirdReality update from being shown as failed merely because the
  S420 is offline while recovery writes and boots the new system.
- Device logs still report an error before the reboot handoff when the satellite
  rejects the update or cannot start its installer.

## Updating

- macOS users already running v1.0.1 through v1.0.5 can install v1.0.6 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.0.6` or `latest` for the CPU image and
  `v1.0.6-nvidia` or `nvidia` for the NVIDIA image.
