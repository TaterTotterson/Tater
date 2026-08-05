# Tater v99.5

## What's Changed

### Wyoming TTS Voice Selection

- Fixed Wyoming voices discovered from the configured server being sent as a
  serialized JSON object instead of the selected voice name, language, and
  speaker fields.
- Wyoming Test Voice, broadcasts, announcements, and other shared TTS paths now
  use the selected server voice correctly.
- Kept compatibility with older settings that store only a plain Wyoming voice
  name, along with the server's default voice option.
