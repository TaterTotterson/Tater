# Tater v3.0.0

Tater 3 brings a redesigned workspace, more focused controls, an interactive
Spudex terminal and improved satellite/media responsiveness.

## What's New

### A new Tater workspace

- Refreshed charcoal styling, consistent icons, focused navigation and responsive
  layouts across Chat, Cores, Verbas, Integrations, Portals, Music and Settings.
- Searchable catalogs, clearer cards, shared popup/toast styling and configurable
  Compotato motion effects with a smooth default.
- Dedicated model, voice and satellite settings workspaces, clearer model labels
  and preserved editor drafts while navigating.
- Richer Core workspaces, including structured field editing and agenda views.

### Interactive Spudex

- New task navigation and an interactive terminal workspace with streamed
  output, terminal sizing and task/session events.
- Improved task lifecycle handling and clearer runtime state in the UI.

### Satellites and hardware video

- Add per-satellite Smooth/Matrix conversation-text settings for compatible
  Echo GPU screens, with capability checks and local-setting synchronization.
- Negotiate balanced hardware H.264 Sat Chat on compatible Shows/Rook while
  keeping JPEG fallback for older/mixed pairs.
- Preserve ordered H.264 frames and prioritize audio with bounded relay queues.
- Reduce repeated settings/status work, directory refresh overhead and blocking
  integration polling; improve heartbeat and Bluetooth handling.
- Use the wsproto WebSocket transport to avoid the reproduced legacy writer
  failure under media backpressure.

### Safer updates and startup

- Explain and enforce Echo 5's one-time full USB upgrade, keeping saved settings
  by default. Updating Tater does not perform that firmware migration.
- Improve firmware batch progress and prevent application updates from bypassing
  the installed platform's OTA compatibility requirements.
- Improve startup/loading feedback and keep UI setup work responsive.
- Default voice-state LEDs to the voice ring on compatible satellites.

Echo 5 firmware publication remains separately gated on its final platform and
USB/OTA qualification. Existing satellites remain usable with this Tater update.

**Full Changelog**: [v2.0.8...v3.0.0](https://github.com/TaterTotterson/Tater/compare/v2.0.8...v3.0.0)
