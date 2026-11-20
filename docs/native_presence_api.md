# Presence API retired

Room presence tracking and its `/presence` endpoints have been removed. Tater
no longer infers a device's room or home/away status from satellite Bluetooth
advertisements. Existing private presence data is left untouched and is not read.

Use the [Bluetooth discovery and proxy API](native_bluetooth_api.md) for explicit
device discovery, pairing, and active GATT connections. Opening Bluetooth settings
or reading its API never starts a scan.
