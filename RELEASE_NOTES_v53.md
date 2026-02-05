# Tater v53

Hot and ready: Telegram got the full Tater treatment.

## What You Care About

- Telegram is now a real platform, not just a notifier.
- DM safety gate added:
  - If `Allowed DM User` is blank, Tater ignores all DMs.
  - If it is set, Tater replies only to that user (or list).
- Telegram notifier now uses the shared queue system (like Discord/Matrix/IRC), including media.
- Most Discord-capable plugins now work on Telegram too.
- `send_message` and `ai_tasks` now support Telegram routing.
- Telegram plugin compatibility improved for Discord-style handlers (`typing()` and `channel.send()` behavior).

## Not Included

- `ftp_browser` and `webdav_browser` were intentionally left Discord-only.
