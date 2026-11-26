# Satellite pairing and management access

## Pairing

In **Voice & devices → Satellites**, use **Add Satellite** and enter the code
on the satellite's setup page. A code normally lasts ten minutes.

Five different incorrect guesses expire the code. Reconnecting with the same
mistyped code counts once; spaces and hyphens in a six-digit code do not make it
a new guess. To recover after the code expires, create a fresh code and correct
the satellite's setup entry. Tater does not permanently ban the satellite's IP.

Failed guesses count against every pending pairing session because a rejected
connection cannot identify which code it was trying. Creating a new session
gives that code its own fresh limit.

After pairing, the satellite uses its saved device credential. Normal reconnects
do not consume pairing attempts. Interrupted first-time setup can still recover
the same credential during the short handoff retry window. An expired setup
code cannot be reused, and expiring a code does not revoke a saved device token.

## API access

Tater's WebUI login and voice API authentication remain separate. The pairing
limit does not add a browser-login requirement to internal API calls.

When voice API authentication is enabled, native control and status requests
require the configured API key in `X-Tater-Token`. When disabled, those routes
retain their existing behavior. Direct HTTP settings writes and Sat Chat
actions still require API authentication to be enabled. The older
`/tater-ha/v1/voice/` routes follow the same existing API rules.

Satellite device credentials authenticate the native WebSocket connection.
Firmware, wake models, display photos, and audio downloads keep their existing
URL-based access. Trainer endpoints keep their own pairing and token
authentication.

Music and speech playback use their existing internal calls and API headers.
No new internal credential, API key, or user configuration is introduced.

WebUI password protection remains optional. These changes do not create HA-style
administrator accounts or a new login setup.
