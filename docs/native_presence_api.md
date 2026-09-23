# Tater Native Presence API

Tater combines passive BLE observations from every compatible native satellite
and assigns each detected identity to the room with the strongest sustained,
calibrated signal. Signal smoothing, a freshness grace period, per-scanner RSSI
offsets, and room-change hysteresis prevent stationary devices from bouncing
between rooms as ordinary radio noise and staggered scanner reports arrive.
The same data used by the Satellites → Presence UI is available to cores,
verbas, integrations, and automations.

This is a Tater-native service. It does not require Home Assistant, MQTT,
ESPHome, or a cloud account.

## Identities and tracking

Unconfigured devices are grouped by public BLE address. Tater also recognizes:

- iBeacon UUID, major, and minor fields, which remain stable when the over-air
  address rotates.
- BLE resolvable private addresses when a device IRK has been added to the
  private Tater identity registry.
- Multiple identities assigned to one named Tater tracker.

IRKs are stored only in the private local presence state file with owner-only
permissions. They are never returned by the API, included in events, or sent
back to satellites. Public API responses contain only an IRK fingerprint and a
masked display value.

Presence state remains active when the WebUI is closed: incoming satellite
advertisement batches advance tracked home/away and room state and record new
events. Observation, identity-cache, and history collections are all bounded.

## Snapshot

```http
GET /api/tater/satellite/v1/presence
```

Optional query parameters:

- `address`: return one BLE address, such as `aa:bb:cc:dd:ee:ff`.
- `device_id`: return one stable Tater presence identity.
- `selector`: use observations from one satellite selector.
- `max_age_s`: include observations from the last 1–900 seconds.
- `limit`: cap retained observation rows from 1–500.
- `include_observations=false`: omit raw packets for a smaller response.
- `history_limit`: include 0–200 recent persistent presence events.

The `devices` array contains the stable Tater ID, resolved identity type,
configured name and owner, home/away state, `location_room`, calibrated
`distance_m`, `strongest_rssi`, signal label, confidence, advertised name,
service UUIDs, last-seen time, and a ranked `sources` list showing every
satellite that recently heard the device. Each source includes its raw and
calibrated RSSI, configured offset, and estimated distance.
When another room is currently stronger but has not held the lead long enough,
`raw_strongest_room`, `candidate_room`, and `candidate_age_s` expose that pending
decision without reporting a false movement.
`rooms` contains device counts by selected location, `trackers` summarizes
home/away state, `history` contains native presence events, and `sources`
reports scanner health and totals. `diagnostics` reports registry, iBeacon,
IRK, calibration, and bounded-buffer health without exposing secrets.

## Live stream

```http
GET /api/tater/satellite/v1/presence/events
Accept: text/event-stream
```

The server emits `presence.snapshot` events whenever BLE observations change
and periodically refreshes the snapshot so freshness can expire naturally. It
also emits individual `presence.event` messages for new `arrived_home`,
`left_home`, and `room_changed` events.
The stream uses the same filters as the snapshot endpoint and always omits raw
advertisement packets.

When Tater API authentication is enabled, external clients send the normal
`X-Tater-Token` header. Browser clients on the Tater WebUI use the same-origin
session.

In-process Python cores and verbas can avoid HTTP:

```python
from tater_voice import native_ble

presence = native_ble.snapshot(
    address="aa:bb:cc:dd:ee:ff",
    max_age_s=30,
    include_observations=False,
)
room = presence["devices"][0]["location_room"] if presence["devices"] else ""
```

## Persistent history

```http
GET /api/tater/satellite/v1/presence/history
```

Optional query parameters are `device_id`, `limit` (1–500), and `since_ts`.
The corresponding in-process call is:

```python
events = native_ble.history_snapshot(device_id="alice-phone", limit=100)
```

## Configuration

```http
GET /api/tater/satellite/v1/presence/config
POST /api/tater/satellite/v1/presence/config
Content-Type: application/json
```

The GET response includes global settings, public device registry records,
scanner calibration, and runtime capabilities. POST accepts an action and
payload. Supported actions are:

- `save_settings`: reference power at one metre, attenuation, room radius,
  home radius, away timeout, and history retention.
- `save_scanner`: save a per-satellite `rssi_offset_db`.
- `upsert_device`: name a device, enable tracking, set its owner/category,
  configure device-specific calibration, and assign address, iBeacon, or IRK
  identities.
- `remove_device`: remove a device from the tracked registry.
- `clear_history`: remove recorded presence events.

Example:

```json
{
  "action": "upsert_device",
  "payload": {
    "id": "alice-phone",
    "name": "Alice's phone",
    "owner": "Alice",
    "category": "phone",
    "track": true,
    "identities": [
      {"type": "address", "value": "aa:bb:cc:dd:ee:ff"},
      {"type": "irk", "value": "00112233445566778899aabbccddeeff"}
    ]
  }
}
```

## Distance model

Tater uses the same conventional BLE path-loss relationship used by other
room-presence systems:

```text
distance_m = 10 ** ((reference_power - calibrated_rssi) / (10 * attenuation))
calibrated_rssi = smoothed_rssi + scanner_offset_db
```

Distance is an estimate, not a physical measurement. Walls, bodies,
reflections, transmitter power, and antenna orientation all affect RSSI. Room
assignment continues to use sustained relative signal leadership and does not
switch solely because a single distance estimate changed.

The older `/api/tater/satellite/v1/ble` and `/ble/events` paths remain as
compatibility aliases.
