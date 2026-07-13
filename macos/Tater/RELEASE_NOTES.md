# Tater v94.7

## What's Changed

- Fixed Speaker ID capture and matching for Tater Native satellites.
- Native satellite selector aliases now resolve consistently, so saved speaker profiles work with `native:` device IDs.
- Speaker enrollment now falls back to captured PCM duration when VAD undercounts speech on native satellite audio.
- Speaker ID and Emotion ID runtime actions now refresh with native satellite status, keeping preferred satellite selectors accurate after save or arm actions.

## Notes

- This is a Tater app-side fix. No satellite firmware update is required for this Speaker ID capture path.
