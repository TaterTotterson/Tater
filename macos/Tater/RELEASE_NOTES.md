# Tater v97.5

## What's Changed

### macOS Update Reliability

- Fixed the in-app updater remaining stuck on **Installing update** after the new app bundle was installed.
- Backend cleanup now completes through AppKit's termination run-loop mode so the old Tater process can quit and the updated app can relaunch.
- The installer now waits through the graceful backend shutdown window and safely retires a stuck old app process before replacing the bundle.
- Added regression coverage for both the AppKit termination reply and the installer fallback.
