# Tater v98.7

## What's Changed

### Modern Music Core WebUI

- Rebuilt Music Core as a component-based Vue 3 and TypeScript interface while
  keeping Tater's existing FastAPI WebUI and native macOS wrapper.
- Added a polished persistent player with compact transport controls, volume,
  speaker selection, library status, and the current track list in one stable
  view.
- Updated Genres, Artists, and Albums into responsive library grids and kept
  Search as a dedicated browse tab.
- Added responsive desktop and compact-width layouts for the player, library,
  provider setup, settings, and speaker-selection dialog.

### Live, Flicker-Free Playback

- Replaced Music Core's browser polling and full-panel redraws with an
  authenticated live event stream that emits only when Core state changes.
- Play, stop, previous, next, shuffle, volume, speaker selection, searches, and
  track selection now update in place without a page loading screen or progress
  message shifting the interface.
- Preserved the selected library tab, open track list, scroll position, and
  in-progress form values while playback state changes arrive.
- Kept the existing Core renderer for every other Core, with an automatic
  fallback if the component Music Core bundle cannot load.

### Frontend Foundation

- Added a reproducible Vite build with pinned Vue and TypeScript dependencies
  and packaged production assets for the Tater updater.
- Added regression coverage for the component loader, live Core event route,
  compiled assets, and legacy-renderer compatibility.
