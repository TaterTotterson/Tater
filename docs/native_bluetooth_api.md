# Satellite Bluetooth discovery and proxy

Bluetooth stays off when unused. Open Settings → Satellites → Bluetooth, select
an updated satellite, and choose **Scan for devices**. Discovery lasts 15 seconds
in the UI, pauses during voice activity, and stops automatically. Stop ends
discovery without disconnecting a device already in use. An active connection,
such as Meshtastic, keeps Bluetooth enabled until that connection closes; the
radio powers down after a short idle delay. A saved bond alone does not enable it.

Native ESP32, Echo, ThirdReality, and supported Linux satellites share the same
GATT protocol. Firmware must advertise `ble_gatt` and `ble_on_demand`. Old firmware
must be updated to stop its previous automatic presence scanning. Linux hosts
need a BlueZ adapter and system D-Bus access; ThirdReality includes these in its
system image. The proxy is for BLE GATT peripherals, not Bluetooth speaker audio.
Broadcast-only sensors require ongoing listening and are not continuously
monitored by this on-demand feature.

## HTTP API

Requests use Tater's existing API authentication (`X-Tater-Token`).

- `GET /api/tater/satellite/v1/bluetooth` returns connected scanners, recent
  discovery results, saved bond metadata, and scan status. It never starts a scan.
- `POST /api/tater/satellite/v1/bluetooth/scan` accepts `selector` (optional; blank
  scans through all capable connected satellites) and integer `duration_s` (5–60,
  default 15).
- `POST /api/tater/satellite/v1/bluetooth/stop` accepts optional `selector` and
  stops discovery on that satellite or all satellites with a discovery lease.
- `POST /api/tater/satellite/v1/bluetooth/pair` accepts `selector`, `address`, and
  the device's six ASCII digit `pin`. Discover the device through that satellite
  first. Pairing verifies an authenticated, encrypted bond and does not close a
  connection another consumer was already using.
- `POST /api/tater/satellite/v1/bluetooth/unpair` accepts `selector` and `address`.
  An offline or failed unpair retains the local bond record for a retry.

Discovery results stay in bounded memory and expire. Bond metadata is stored in
`~/.taterassistant/bluetooth_devices.json` with owner-only permissions. Override
this location with `TATER_BLUETOOTH_STATE_PATH`. PINs are never saved centrally;
the satellite retains its bond keys. Updating does not delete existing bonds or
Meshtastic transport settings.

## Native WebSocket protocol

Use an authenticated `ble.gatt` envelope with a uint32 `req` and `t` operation:

- `scan`: integer `duration_s` (5–60, default 15).
- `stop_scan`: end discovery while preserving active links.
- `slots`: read allocation/radio status without enabling Bluetooth.
- `connect`, `disconnect`, `pair`, `forget`, `services`, `read`, `write`: the
  existing Echo-compatible GATT contract, using `addr`, optional `addr_type`
  (0 public, 1 random), ATT `handle`, Base64 `value`, and `response` as applicable.

Replies use `ble.gatt.result` with `reply_to`, `req`, `ok`, and operation data or
`error`. `ble.gatt.event` carries `slots` (including `enabled`/`scanning`),
`notify`, and `disconnected` events. ESP32 supports one simultaneous link;
shared Linux/ThirdReality support two; Echo reports its backend's live limit.
Controller disconnection ends discovery and tears down that controller's links.

`ble.advertisements` batches contain raw hexadecimal AD `data` on Native/Echo.
BlueZ can instead supply decoded `name`, `service_uuids`, `manufacturer_data`
(company ID to hexadecimal bytes), and `service_data` (UUID to hexadecimal bytes),
plus `address`, `address_type`, and `rssi`. Controllers only accept advertisements
inside an explicitly requested discovery window.
