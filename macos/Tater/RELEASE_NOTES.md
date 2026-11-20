# Tater v3.0.6

Custom wake-word bundles now deliver their wake-boundary model to compatible
native satellites, including Voice PE.

## What's Changed

- **Send the complete wake bundle.** Tater now reads the boundary-model flag
  produced by its bundle loader and sends the full schema-v4 bundle to native
  satellites that advertise boundary support. Previously, those satellites
  could receive only the main wake detector, leaving wake-boundary detection
  disabled even though the selected bundle included it.
- **Keep older satellites compatible.** Devices without boundary support
  continue to receive the microWakeWord-only manifest. Satellites that already
  support schema-v4 wake bundles do not need a firmware update for this fix.

**Full Changelog**: [v3.0.5...v3.0.6](https://github.com/TaterTotterson/Tater/compare/v3.0.5...v3.0.6)
