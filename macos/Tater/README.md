# Tater macOS App

This is a native macOS shell for Tater. It keeps the Python/FastAPI app intact, but gives it a Mac app window, menu bar status item, private runtime, and app-only update checks.

## Runtime Layout

On first launch, the app prepares a private runtime in:

```text
~/.taterassistant/
  app/
  python/
  venv/
  runtime/
  agent_lab/
    workspace/
    downloads/
    documents/
    artifacts/
    models/
    redis/
    cores/
    verba/
    portals/
    integrations/
  logs/
  updates/
```

The launcher starts Tater with `TATER_AGENT_ROOT`, `TATER_VENV_DIR`, and `TATER_RUNTIME_DIR` pointed at that folder, so normal repo-local development remains separate from the macOS app runtime.

If Python 3.11 is not already available, the launcher installs a standalone CPython 3.11 build under `~/.taterassistant/python/cpython-3.11` and uses that interpreter to create the private venv.

The app bundle contains a source snapshot at `Contents/Resources/TaterSource`. Downloaded cores, Verba modules, portals, and integrations are intentionally stored under `~/.taterassistant/agent_lab` instead of inside the app bundle or source checkout.

## Build

```sh
macos/Tater/scripts/build_app.sh
```

The script builds:

```text
macos/Tater/build/Tater.app
```

The internal executable is still named `TaterAssistant`, but the user-facing bundle is `Tater.app`.

## First Launch

The app runs the equivalent of:

```sh
TATER_VENV_DIR="$HOME/.taterassistant/venv" \
TATER_RUNTIME_DIR="$HOME/.taterassistant/runtime" \
TATER_AGENT_ROOT="$HOME/.taterassistant/agent_lab" \
sh setup_tater.sh macos
```

Then it launches `run_ui.sh` on `127.0.0.1:8501` and opens that URL in a native `WKWebView` window. Closing the window does not stop Tater; use the menu bar item to open, stop, restart, show logs, or quit.

## Updates

The macOS wrapper checks the manifest URL from `TaterUpdateManifestURL` in `Resources/Info.plist`. You can override it for testing with `TATER_UPDATE_MANIFEST_URL`.

To package a release zip and generated manifest:

```sh
macos/Tater/scripts/package_update.sh
```

That writes:

```text
macos/Tater/build/Tater-v<version>.zip
macos/Tater/build/update-manifest.json
macos/Tater/releases/Tater-v<version>.zip
macos/Tater/update-manifest.json
```

By default, the manifest points at the tracked zip under `macos/Tater/releases/` on the `main` branch, so pushing the generated release file and manifest is enough for update checks. You can pass a custom URL to `package_update.sh` if you prefer to host the zip as a GitHub Release asset. When the app sees a newer Tater version such as `88.1` or `89`, it shows an orange update item in the menu bar menu.

## First-Time Installer DMG

To build a first-time installer disk image:

```sh
macos/Tater/scripts/build_dmg.sh
```

That writes:

```text
macos/Tater/build/Tater-v<version>.dmg
macos/Tater/releases/Tater-v<version>.dmg
```

The DMG mounts with `Tater.app`, an `Applications` alias, and the branded background from `Resources/TaterDmgBackground.png`. This is separate from the app auto-update zip; use the DMG for new installs and the zip/manifest for in-app updates.
