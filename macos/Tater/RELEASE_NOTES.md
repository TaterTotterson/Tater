# Tater v100

## What's Changed

### Tater Main App

- Added Tater AirPlay Bridge for synchronized playback across Tater satellites,
  Sonos speakers, and other AirPlay devices. Sonos players can use automatic,
  native Sonos, or AirPlay routing, and the bridge uses a shared clock with
  prepared and reusable sessions to keep mixed groups aligned while reducing
  startup time and gaps between tracks.
- Added a redesigned per-player selector to Music Core. Every player has its
  own selection row, volume, playback route, and adjustable audio-sync offset,
  with 10 ms nudges and a built-in synchronization test.
- Added mixed-player `audio_sync` transcoding through Tater Tube so tracks with
  difficult source formats are normalized before synchronized playback.
- Added persistent playback state and resume positions. Refreshing or returning
  to Music Core no longer restarts the current song, and the main transport now
  changes between Play and Pause to match playback state.
- Refined Music Core with a slimmer player, cleaner track-list controls, and
  navigation attached below the player so content no longer scrolls behind it.
  Redundant player badges and status facts were removed.
- Personalized recommendations now use the configured assistant name, and
  broader genre aliases make requests such as reggae music resolve correctly.
- Reduced repeated runtime-event reads so settings remain steady while live
  status refreshes in the background instead of jumping during edits.
- Added graceful shutdown limits for long-lived satellite and browser sockets,
  preventing a stale connection from holding Tater open during restart.

### Playback Reliability and Native Satellites

- Improved synchronized group startup, track transitions, underrun recovery,
  and timeline rejoining for native satellites and mixed AirPlay groups.
- Added per-device output-latency calibration without changing the precise
  clock scheduling already used between Tater-native satellites.
- Added native satellite identity migration so Sat1, VoicePE, and other boards
  retain the correct names and settings when older firmware reported a generic
  VoicePE identity.
- Added a disconnect grace period for active media sessions. A briefly
  reconnecting satellite no longer tears down the whole synchronized group;
  genuinely disconnected members are removed cleanly after the grace period.
- Rate-limited noisy diagnostics and hardened media cleanup so a troubled Sat1
  cannot flood Tater or leave other group members in a stale playback state.

### Firmware Updates

- Updated Sat1's bundled XMOS firmware to 1.1.0 with four-tap
  fractional-delay beamforming, per-microphone calibration, unhealthy-microphone
  fallback, and expanded direction diagnostics. When a voice session begins,
  XMOS holds the initial speaker direction so the beamformer stays focused
  instead of chasing reflections or background noise; Sat1's directional LED
  shows where that active microphone beam is aimed.
- Replaced command-terminal firmware windows with a focused OTA/USB update
  popup showing real progress, friendly status messages, update stages, and
  clear success or error states. Dedicated OTA and USB logs remain available
  for diagnostics.
- Added native OTA percentages, transferred-byte progress, browser USB write
  progress, S420 Amlogic progress, and an overall progress view for Update All.
- Added first-class ThirdReality S420 firmware discovery and flashing while
  preserving its independent firmware repository and dedicated Amlogic USB
  burn path.

### New ThirdReality S420 Satellite

- Added Tater-native firmware for the ThirdReality Voice & Music Assistant,
  an Amlogic A113X/S420 Linux satellite with local Hey Tater detection, secure
  native pairing, voice replies, music, and synchronized multi-room playback.
- Added a captive setup portal for Wi-Fi, Tater pairing, room, and speaker name,
  plus LED-ring, home-button, mute, microphone-mute, and volume integration.
- Added verified factory USB images, signed OTA support, recovery controls, and
  a long-press reset that returns the speaker to its private setup hotspot.
- S420 firmware 0.2.1 also prevents phantom microphone starts.

### Tater Linux Satellite

- Added persistent native music sessions with stop, pause/resume, volume,
  looping, resume positions, session reporting, and automatic speech ducking.
- Kept segmented TTS replies continuous and improved audio cleanup across
  satellite restarts and reconnects.

### New Reachy Idle Life

- Reachy Mini can now stay gently alive between conversations with optional
  empty-room movement, varied non-repeating room scans, personality-driven
  ambient comments through Tater Vision, quiet hours, and an interruptible
  sleep pose.
- Face detection and voice activity immediately take priority. Sleep can wake
  from nearby sound or a configured timeout, and ambient comments use fresh,
  unsaved snapshots over Reachy's authenticated Tater connection.
- Every Idle Life behavior is independently selectable and disabled by default.
  The Reachy settings page can restore the original defaults or test an ambient
  comment immediately without waiting for its timer.
- Reachy Satellite 0.3.3 also adds native GStreamer music playback with
  stop, pause/resume, volume, looping, resume positions, and speech ducking,
  alongside smoother user tracking and expressive conversational motion.

### Tater Tube and Music Library

- Expanded Tater Tube's local-media management with library-type tabs, folder
  browsing, library statistics, artwork-backed browsing, and music library
  management instead of requiring users to type raw media paths.
- Added album-art scraping and artwork URLs that flow through Music Core and
  the companion apps.
- Added the synchronized-audio transcode profile and expanded the standard
  music genres exposed to search and voice requests.

### Little Spud Companion Apps

- Album pages on iOS and Android now include Play Album, which clears the old
  queue, starts the first track, and adds the full album in order, plus clearer
  back navigation.
- Replaced the separate compact and expanded music players with one polished
  bottom player that keeps the curved design and still swipes up for the full
  queue and controls.
- Added a smaller translucent Liquid Glass player and device-control sheets on
  iOS, while keeping the shared top navigation in its clearer original style.
- Restored the single-row, horizontally scrolling category controls in Music
  and Home Control on both platforms.
- Tater Picks now presents a focused set of 20 personalized songs based on the
  listener's playback history.

### Docker and Platform Support

- CPU and NVIDIA images now include the pinned, checksum-verified AirPlay CLI
  for both AMD64 and ARM64, along with the shared PTP clock requirements needed
  for synchronized AirPlay playback without runtime downloads.
- Added Docker host-network and low-port capability guidance for AirPlay clock
  synchronization, while retaining persistent runtime storage.
