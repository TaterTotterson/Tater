# Tater v1.1.6

Tater v1.1.6 adds separate firmware routing and correct device presentation for
Satellite1 Public Batch #1 / Beta.1 HAT and Core rev4.1 hardware.

## What's Changed

### Satellite1 Beta.1 / rev4.1 Support

- Recognizes the legacy board's distinct `satellite1-beta-rev41` hardware and
  firmware identity.
- Routes that hardware to the dedicated `satellite1_beta_rev41` firmware
  manifest entry instead of the production Satellite1 image.
- Shows the correct `Tater Sat1 Beta.1` default name and Satellite1 artwork.
- Applies the same far-field wake defaults used by production Satellite1
  hardware.
- Corrects a previously saved Voice PE default name when the board reports its
  legacy Satellite1 identity.

## Companion Firmware

Tater Native Firmware `0.3.11`, published by the
`native-0.3.11-sat1beta` release, now includes the isolated Beta.1/rev4.1
target. Its first installation must be selected explicitly over USB; afterward,
the distinct hardware and OTA identities keep production and legacy Satellite1
updates on their matching channels.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.6 through
  Tater's normal updater.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.6` or `latest` for the CPU image and
  `v1.1.6-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
