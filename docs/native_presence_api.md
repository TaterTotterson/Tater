# Native Satellite Presence API

Tater combines passive BLE observations from every compatible native satellite
and assigns each detected address to the room with the strongest sustained
signal. Signal smoothing, a freshness grace period, and room-change hysteresis
prevent stationary devices from bouncing between rooms as ordinary radio noise
and staggered scanner reports arrive.
The same data used by the Satellites → Presence UI is available to cores,
verbas, integrations, and automations.

## Snapshot

```http
GET /api/tater/satellite/v1/presence
```

Optional query parameters:

- `address`: return one BLE address, such as `aa:bb:cc:dd:ee:ff`.
- `selector`: use observations from one satellite selector.
- `max_age_s`: include observations from the last 1–900 seconds.
- `limit`: cap retained observation rows from 1–500.
- `include_observations=false`: omit raw packets for a smaller response.

The `devices` array contains the stable `strongest_room`, `strongest_rssi`,
signal label, confidence, advertised name, service UUIDs, last-seen time, and a
ranked `sources` list showing every satellite that recently heard the device.
When another room is currently stronger but has not held the lead long enough,
`raw_strongest_room`, `candidate_room`, and `candidate_age_s` expose that pending
decision without reporting a false movement.
`rooms` contains device counts by selected room, while `sources` reports scanner
health and totals.

## Live stream

```http
GET /api/tater/satellite/v1/presence/events
Accept: text/event-stream
```

The server emits `presence.snapshot` events whenever BLE observations change
and periodically refreshes the snapshot so freshness can expire naturally.
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
room = presence["devices"][0]["strongest_room"] if presence["devices"] else ""
```

The older `/api/tater/satellite/v1/ble` and `/ble/events` paths remain as
compatibility aliases.
