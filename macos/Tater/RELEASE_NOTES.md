# Tater v99.3

## What's Changed

### Little Spud Music

- Added SpudLink support for Little Spud's persistent bottom music player,
  expandable current playlist, live progress and seeking, album artwork,
  volume control, and multi-destination playback.
- Little Spud can browse Search, Genres, Artists, Albums, and AI-generated
  Tater Recommendations from the active Music Core provider.
- Playback destinations can include the iPhone, individual satellites,
  stereo pairs, Sonos speakers, and supported media-player integrations.

### Music Core 2.7.2

- Exposed the live Music Core queue, progress, artwork, library facets, and
  Tater Recommendations to trusted native clients without exposing provider
  credentials.
- Added native-client controls for recommendation playback, multiple playback
  destinations, live volume changes, and seeking.
- Little Spud on-device playback now requests AI-selected continuation batches
  before its queue ends, with a similarity-ranked fallback so music keeps
  playing if the AI call is unavailable.
- Listening on the iPhone now contributes to the shared Music Core history,
  improving future continuous-radio choices and Tater Recommendations.
