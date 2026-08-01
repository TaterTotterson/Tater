# Tater v98.6

## What's Changed

### Live Music Core Player

- Updated the Tater Shop **Music Core** to version 2.2.0 and paired it with new
  live Core UI support in Tater.
- Play, stop, previous, next, album playback, search, shuffle, volume changes,
  speaker selection, and track selection now update in place without opening a
  blocking loading screen or progress bar.
- Fixed next-track playback by cleanly ending the active media session before
  starting the selected track.
- Added a collapsible current track list to the persistent player. The playing
  song is highlighted, and another track can be started by double-clicking it.
- Playing a different album now replaces the current track list instead of
  stacking tracks from multiple albums.

### Music Library Layout

- Moved volume into a slider beneath the player transport controls.
- Moved shuffle into the current track list.
- Added Search as a library subtab alongside Genres, Artists, and Albums.
- Removed the separate Queue tab because the active track list now lives with
  the persistent player.
- Added silent background refreshes that preserve the selected library tab,
  open track-list dropdown, and track-list scroll position while keeping the
  current-song highlight up to date.
