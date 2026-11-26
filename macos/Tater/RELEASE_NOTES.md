# Tater v3.0.12

## What's Changed

- **Let the assistant understand your words:** Remove the hard-coded sentence-completeness gate and its extra listening delay. Recognized speech reaches Hydra regardless of phrase length or ending words, including one-word replies, interjections, and non-English text.
- **End quietly on silence:** Keep VAD endpointing and close no-speech turns without an error prompt. Ask for a repeat only when speech was detected but no usable transcript was recovered.
- **Recover empty speech recognition:** When speech was detected but STT returned no text, retry the full recording once with the selected provider. Keep successful transcripts and avoid repeated retries or loading a different model for this retry.

No satellite firmware update is required.

**Full Changelog**: [v3.0.11...v3.0.12](https://github.com/TaterTotterson/Tater/compare/v3.0.11...v3.0.12)
