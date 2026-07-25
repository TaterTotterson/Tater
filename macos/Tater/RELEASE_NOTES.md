# Tater v97

## What's Changed

### Tater

- Added one shared Satellite Voice Settings card in Voice Settings. Wake word, trainer feedback, wake sound, continued chat, and barge-in changes now apply immediately to every connected Tater Native satellite and are inherited by satellites that reconnect later.
- Added low-latency STT wake verification with Disabled, Observe, and Enabled modes. Observe shows results without blocking a wake; Enabled rejects transcript mismatches while failing open if STT errors or exceeds the 500 ms deadline.
- Added per-satellite wake-verifier results, including accepted and rejected counts, the latest transcript, match score, and STT latency.
- Added secure Wake Word Trainer pairing. Tater generates a short-lived, one-use code and stores only a trainer-specific credential after linking. Linked trainers can publish only their own trained wake-word packages, and Tater activates the selected word across all satellites.
- Combined trainer linking and trainer feedback into one readable Tater-themed card with linked status, trainer address, link time, and the latest published wake word.
- Removed the unused legacy ESPHome discovery and control paths. Tater Native satellites now use Add Satellite pairing exclusively.
- Hid internal VAD, endpointing, audio-gate, and discovery tuning that Tater manages automatically, keeping Voice Settings focused on useful user choices.
- Preserved per-satellite colors and animations while moving shared voice behavior into the global card.
- Fixed offline satellites receiving a random firmware image by retaining the correct saved hardware template and image.
- Corrected LED brightness controls to use a true 0-100% range and updated enabled toggles to use Tater's orange-red active state.
- Reduced voice/runtime UI pressure by caching frequently-read voice configuration briefly and logging event-loop stalls for easier diagnosis.
- Reduced Redis growth by skipping integration-registry rewrites when only timestamps or runtime overlays changed.
- Added safe Redis AOF maintenance with automatic background compaction for oversized logs, disk-space checks, and rewrite thresholds based on live Redis memory. Compaction rewrites persistence files without clearing live keys.
- Increased the internal Redis startup allowance for large existing data sets and strengthened timeout cleanup.
- Reworked Tater shutdown so portals, voice, integrations, cores, chat jobs, local models, executors, Redis, and their child processes stop in a controlled order. The macOS app now waits for backend shutdown and cleans up stubborn descendants before updating, restarting, or quitting.

### Tater Shop — Automation Core

- Added [Automation Core 1.0.0](https://github.com/TaterTotterson/Tater_Shop), a dedicated, easy-to-use automation builder backed by Tater's shared integration categories, devices, rooms, notification destinations, and voice targets.
- Automations can react to device changes, on/off and open/close transitions, motion, person, vehicle, animal, package, face, license-plate, and doorbell detections, connection changes, text matches, and numeric thresholds.
- Actions can control integration devices and categories, speak custom TTS on selected satellites or announcement targets, send notifications, or describe a camera image with AI and announce or notify the result.
- Added reusable message fields and event context, cooldown protection, enable/disable controls, manual test runs, execution status, and automation history.
- Kept the new core independent from Awareness Core so awareness automations can be migrated cleanly over time.
- Added runtime cleanup coverage for Automation, Awareness, and RSS core workers.

### Wake Word Trainers

- Released [WakeWord Trainer for Apple Silicon v19](https://github.com/TaterTotterson/microWakeWord-Trainer-AppleSilicon) and [WakeWord Trainer for NVIDIA v15](https://github.com/TaterTotterson/microWakeWord-Trainer-Nvidia-Docker).
- Both trainers now link to Tater with the short-lived code shown in Voice Settings instead of requiring a general Tater API token.
- Automatic and manual publishing tell Tater which trained wake word is active; Tater validates the trainer-owned package and applies it globally to all satellites.
- Added clear linked, unlinked, relink, and pairing-success states in the Auto Training UI.
- The Apple Silicon trainer now performs a clean shutdown of its backend, scheduler, active training process group, and child processes so updates and restarts do not leave work stuck.

### Tater Native Firmware

- Released [Tater Native Firmware 0.2.4](https://github.com/TaterTotterson/Tater-Native-Firmware) for Voice PE, Satellite1, ReSpeaker XVF3800, and ESP32-S3-BOX-3.
- Added the satellite side of Tater's STT wake verifier with off, observe, and enforce modes plus fail-open timeout handling.
- Satellites preserve saved Wi-Fi credentials during outages, reconnect indefinitely with capped backoff and jitter, and recover stuck WebSocket clients and interrupted audio sessions.
- Upgraded Satellite1 XMOS firmware to 1.0.8 with four-microphone direction estimation, adaptive room-noise gating, confidence filtering, and smoother DoA tracking.
- Corrected ReSpeaker XVF3800 direction and speech-detector handling so directional LEDs ignore silent and noise-only updates.
- Sat1 and ReSpeaker now hold the last valid speech direction briefly and then return to a calm neutral listening glow.
- Normalized firmware LED brightness to 0-100%, raised the default to 80%, and replaced random disconnected twinkles with a stable low-glow state.
- Improved custom wake-word and wake-sound download, caching, refresh, retry, and diagnostics.
