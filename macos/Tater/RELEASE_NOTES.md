# Tater v1.9.1

Tater v1.9.1 improves recognition of short spoken requests from voice
satellites.

## What's Changed

### Short Voice Commands

- Accepts clear multi-word transcripts when voice activity detection catches
  only the leading edge of a short request.
- Fixes commands such as "turn on the TV" and "what time is it" being replaced
  by a retry prompt or silently discarded after the user repeats them.
- Preserves the stricter no-speech guard for transcripts that do not have the
  minimum voice activity needed to confirm that someone spoke.
