# Live satellite name and room

Native, Echo, and Linux/ThirdReality satellites advertise
`capabilities.identity_update: true` when they can persist editable identity.
Controllers must not offer an unconfirmed local override as a successful rename.

Send this command over the existing authenticated satellite WebSocket:

```json
{"v":1,"type":"identity.set","id":"request-id","payload":{"device_name":"Kitchen Tater","room":"Kitchen"}}
```

Both fields are required strings. Trim ASCII spaces, reject control characters,
require a nonempty name, and limit each field to 63 UTF-8 bytes. An empty room
clears the assignment. Firmware commits both fields together before applying
them in memory and returning the confirmed values:

```json
{"v":1,"type":"identity.result","payload":{"reply_to":"request-id","ok":true,"device_name":"Kitchen Tater","room":"Kitchen"}}
```

On failure return `ok: false` and `error`, leaving the previous live identity
intact. No reset, reboot, new listener, pairing exchange, or device-ID change is
needed. Wi-Fi, server settings, tokens, and unrelated preferences are preserved.
Reconnect hello messages report the saved name and room.

Controllers update their registry and room context from confirmed results,
including late confirmations after a timeout. Tater also refreshes its live
display targets. The HA bridge writes explicit device-name/area edits through
this command, restores confirmed registry values after failures, and matches
firmware-reported rooms on reconnect. Existing entity IDs and unique IDs are
preserved; a name edit never regenerates entity IDs or stereo membership.

The ThirdReality package backports this protocol onto its pinned Linux runtime
and points `--tater-identity-file` at `/data/conf/tater.json`, preserving the
existing configuration and provisioning path. Standalone Linux satellites use
`tater-identity.json` beside their preferences unless configured otherwise.
