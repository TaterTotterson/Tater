# Tater v1.8.3

Tater v1.8.3 makes satellite wake diagnostics accurate across every supported
detector mode and improves the built-in NanoWakeWord response tuning.

## What's New

### Accurate Wake Model Diagnostics

- Shows the detector mode and model that each satellite is actually running,
  including MWW, OWW, NWW, both dual modes, timer-stop mode, and disabled mode.
- Reports each dual-mode lane independently instead of combining stale model
  names with the active companion detector.
- Adds loaded threshold, sliding window or patience, close-miss threshold,
  architecture, runtime, model revision, source, CPU assignment, inference
  time, detections, drops, and errors to View All Device Info.
- Correctly reads the nested NanoWakeWord status reported by ThirdReality
  satellites instead of their legacy microWakeWord compatibility field.

### Consistent Wake Settings

- Uses the same effective shared wake-family settings in satellite status that
  Tater sends to the firmware, so the displayed mode, architecture, and JSON
  tuning stay in sync.
- Keeps older firmware on its safe MWW projection while showing NWW settings
  only on satellites that advertise NanoWakeWord support.
- Updates the built-in NWW tuning to two consecutive scores: CNN uses a 0.99
  threshold and GRU uses a 0.98 threshold for a faster validated response.

### Clearer Runtime Information

- Renames transport counters to Voice Session Frames, Voice Session Bytes, and
  Buffered Logs so idle or muted satellites no longer look like their wake
  engine has stopped processing audio.
- Uses the board name as the firmware target when older satellite firmware does
  not report a separate target value.
