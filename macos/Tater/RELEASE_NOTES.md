# Tater v3.0.2

Tater 3.0.2 introduces the Music workspace and improves satellite music playback
and STT wake verification.

## What's Changed

- **Music workspace:** Listen, Library, Tater’s picks, Queue, Speakers, and
  Settings use the updated Tater style. A persistent player provides progress,
  seek, shuffle, repeat, volume, and speaker selection.
- **Browse before playing:** search and inspect albums, artists, genres, and
  songs without changing playback. Queue tracks play with a single click.
  Category selections keep similarly named album editions separate, and
  multi-disc albums follow disc and track order.
- **Clearer speaker controls:** stereo pairs appear as one destination with two
  named left/right speakers. Per-player volume, timing offsets, playback routes,
  and sync tests remain available on the Speakers page.
- **Focused music settings:** connection, playback, library sync, and
  personalization have separate sections. Edits survive tab switches and live
  refreshes, and existing settings remain available.
- **ThirdReality stereo playback:** fix FLAC audio framing that could leave a
  stereo pair silent when playing alone, while preserving Echo’s legacy audio
  format.
- **STT wake verification:** separate millisecond controls for the verification
  audio window and maximum response wait. Settings apply to connected satellites
  and persist for reconnects. Empty transcripts, provider failures, and timeouts
  allow listening; response timing reserves time for transport.
- **Wake Word Trainer:** clarify that direct satellite audio uploads use the
  trainer URL. The optional auto-training link lets the trainer publish new wake
  models and is not required for audio uploads.
- **Internal cleanup:** organize Python implementations into feature packages
  while retaining compatibility imports and worker entry points for installed
  extensions.

Update Music Core to **3.6.6** from Tater Shop for the complete Music workspace.
The existing Music Core interface remains compatible with older Tater versions.

**Full Changelog**: [v3.0.1...v3.0.2](https://github.com/TaterTotterson/Tater/compare/v3.0.1...v3.0.2)
