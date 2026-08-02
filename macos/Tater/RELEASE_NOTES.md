# Tater v99

## What's Changed

### A New Tater WebUI

- Expanded Tater's local Vue 3 and TypeScript interface across the Dashboard,
  Chat, Integrations, Verba, Portals, Cores, Spudex, Settings, Music Core, and
  live runtime statistics while keeping the FastAPI backend and native macOS
  app.
- Unified the app around Tater's orange-and-gray visual language with clearer
  navigation, responsive cards, compact controls, consistent settings forms,
  smoother dialogs, and polished popup transitions.
- Kept the frontend bundled locally with Tater. Versioned assets update with
  each app build, and live surfaces update in place without replacing the
  user's active tab, scroll position, or in-progress form values.
- Added a new Tater mascot sidebar icon and kept the installed build version at
  the bottom of the menu so users can immediately confirm which release is
  running.

### Dashboard, Integrations, and Management

- Rebuilt the Dashboard with alphabetized, masonry-style sections that let
  shorter cards rise naturally instead of leaving row gaps. Environment images
  now receive more room, and duplicate summary pills were removed.
- Rebuilt the complete Integrations workspace, including Installed, Store,
  Manage, Repositories, Devices, Organize, and Activity views. Device categories,
  rooms, Tater aliases, and preferred room players remain fully editable.
- Rebuilt the full Verba, Portal, and Core managers with responsive Shop cards,
  update controls, repository management, runtime controls, and complete
  manifest-driven settings.
- Rebuilt Chat with safe rich-message rendering, streaming responses,
  attachments, session continuity, and a full-height conversation layout.
- Rebuilt Spudex while preserving its workspaces, live sessions, command
  controls, process tracking, policy notices, plans, verification results,
  previews, Git details, file changes, and session memory.

### Faster Settings and Background System Tasks

- Added **Settings > System Tasks**, showing Tater-owned and Core-owned jobs,
  their current state, last and next run times, errors, and a Run Now control.
- Added a reusable background task scheduler for satellite inventory,
  integration device discovery, Dashboard snapshots and briefs, hardware
  telemetry, loaded-model state, and context-window estimates.
- Satellite and integration caches now rebuild when devices connect,
  disconnect, or change, with a five-minute safety refresh instead of making
  users wait for every device to be checked when opening a page.
- Cores can publish their own tasks without a future Tater UI update. Music,
  Guardian, Memory, Personal, RSS, and Tater Tube tasks are discovered
  automatically and can be inspected or run from the same screen.
- Fixed the Firmware tab so selecting a satellite updates its firmware family,
  and added support for firmware revision suffixes when comparing board builds.

### Runtime Statistics and Settings Polish

- Rebuilt the runtime pill and popup with live models, Apple GPU and unified
  memory information, Hydra jobs, active LLM and vision calls, context usage,
  and model unload controls.
- Hardware and model snapshots now refresh in the background. Context Length in
  Model Settings no longer depends on opening the runtime popup first.
- Avoided showing a duplicate VRAM meter on unified-memory Macs while retaining
  dedicated VRAM reporting for systems that expose it separately.
- Updated the full Settings workspace, Voice satellite cards, Stereo Pairs,
  firmware controls, left menu, and responsive layouts for the shared v99 UI.
- Refined Compotato popup effects with smooth enter/leave animation, live
  previews, reduced-motion support, and consistent behavior across Vue dialogs.

### Music Core and Whole-Home Playback

- Updated the Tater Shop **Music Core** to v2.7.0 with room-aware player
  selection. An explicitly named room overrides the speaking satellite, saved
  preferred room players are honored, and automatic room selection prefers
  Sonos when several compatible players are available.
- Added a selected People profile to Music Core. It can build a compact,
  AI-generated profile from favorite genres, artists, and recent listening, and
  injects that context only when the selected person is the current speaker.
- Added AI-named Tater Recommendations, listening-history tasks, catalog sync,
  continuous-radio refill, and manual task controls through System Tasks.
- Improved the persistent Music player with seek progress, a collapsible mini
  mode, clearer speaker selection, artwork in the active track list, a dedicated
  Recommendations browser, and compact masonry-style settings.
- Playback controls, track changes, library browsing, speaker changes, and live
  state continue to update without full-panel loading screens or UI flicker.
- Added synchronized multi-satellite music scenes, persistent media sessions,
  stereo-pair playback, TTS overlays, generic integration media players, and
  mixed Sonos/native groups with a protected runtime media proxy.

### Voice, Stereo, and Reachy Mini

- Improved synchronized stereo and multi-satellite routing so offline members
  are skipped safely, incomplete pairs are not started, and active music can
  continue underneath temporary TTS overlays.
- Added shared public playback URL handling and safer FLAC preparation so LAN
  players receive reachable audio without exposing original provider URLs.
- Added the Tater Shop **Reachy Vision** Verba v1.0.0. A user can ask Reachy
  questions such as “what do you see?”, “how do I look?”, or “what am I
  wearing?” and receive a fresh snapshot-based answer from Tater's configured
  vision model.
- Updated the native `reachy_tater_sat` app to v0.2.28 with opt-in authenticated
  snapshots, automatic Reachy selection by requesting device or room, improved
  startup head leveling, idle face tracking, expressive talking motion, and
  more reliable segmented TTS without playback cutoffs or runtime contention.

### Tater Shop Cores, Verbas, and Portals

- Updated Guardian Core v1.3.12, Memory Core v1.0.29, Personal Core v1.0.56,
  RSS Core v1.0.12, and Tater Tube Core v1.2.3 to expose their recurring scans,
  syncs, analysis, and recommendation jobs through the new Core task contract.
- Kept smart-home control centered on the standalone **Device Control** Verba,
  with room, alias, action, and integration-aware device selection across
  lights, switches, plugs, fans, covers, locks, climate, media players, scenes,
  scripts, and other compatible devices.
- Kept **Camera Control** separate for named security-camera snapshots and
  visual questions, and kept the standalone sensor/status Verbas self-contained
  without the removed shared device helper or migration fallback.
- Updated Discord and shared portal identity handling so the latest message is
  authoritative for the current speaker, person match, room, and admin/tool
  permissions; older shared-channel history cannot impersonate another user or
  inherit their tool access.

### Tater Integrations

- Updated Sonos to v1.3.0 with synchronized temporary music groups, stereo-pair
  member handling, playback restoration, and Audio Clip announcements that do
  not permanently disturb the listener's existing group or queue.
- Updated Home Assistant to v1.3.2 and refreshed integration device-category
  metadata used by Device Control, the Devices browser, and room organization.
- Improved Roon browse playback, registration continuation, and pairing timeout
  handling, and placed newly discovered UniFi devices in a sensible Network
  room by default.

### Reliability and Regression Coverage

- Added focused regression coverage for every migrated Vue workspace, system
  and Core tasks, cached satellite and integration loading, runtime telemetry,
  Music Core live state, Sonos groups, stereo pairs, firmware-family selection,
  public media routing, popup effects, and synchronized native audio scenes.
