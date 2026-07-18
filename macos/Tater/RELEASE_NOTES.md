# Tater v96.1

## What's Changed

- Added authenticated, room-scoped camera snapshots to Little Spud Home, starting with UniFi Protect and supporting other integrations that advertise the normalized `camera_snapshot` capability.
- Added per-client opaque camera preview references so Little Spud can request snapshots without receiving integration IDs, camera device IDs, provider URLs, or credentials.
- Revalidates every snapshot against the paired Little Spud, current room assignment, camera category, and advertised provider action before contacting the integration.
- Serves camera images with private no-store caching and rejects missing, empty, unsupported, or oversized snapshot responses without affecting existing Home controls or providers.
