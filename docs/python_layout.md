# Python application layout

Tater uses a flat package layout: the `tater/` package sits beside the existing
`hydra/`, `tater_voice/`, `notify/`, `spudex/`, and `tateros/` packages. Source,
Docker, and macOS deployments continue to run from the same application root.

## Implementation packages

| Package | Responsibility |
| --- | --- |
| `tater/audio/` | Speech settings, synthesis workers, announcements, AirPlay, and playback |
| `tater/conversation/` | Conversation artifacts and emoji responses |
| `tater/extensions/` | Core, Portal, Integration, and Verba discovery, settings, tools, and lifecycle |
| `tater/identity/` | People, face recognition workers, and administrative access |
| `tater/llm/` | Model runtime, tool-response parsing, vision/media settings, and web research |
| `tater/personal/` | Shared RSS storage |
| `tater/runtime/` | Redis, task executors, hardware diagnostics, process helpers, and memory sizing |
| `tater/spudlink/` | Shared model access and Little Spud home views |
| `tater/voice/` | Shared voice entity definitions |
| `tater/web/` | FastAPI application and request validation schemas |

The large backend and model runtime remain orchestration modules. Independent
request validation, extension lifecycle, response parsing, hardware diagnostics,
process helpers, and memory sizing have been extracted into focused modules.
Further extraction should follow clear dependencies rather than dividing files
into arbitrary numbered parts.

## Compatibility

Root files such as `helpers.py`, `verba_base.py`, `speech_tts.py`, and
`tateros_app.py` are compatibility entry points. Installed Cores, Portals, and
Verbas can keep importing their original names. Both import paths resolve to the
same module object, sharing caches, locks, Redis clients, classes, and mutable
state. Implementation modules publish their legacy alias before importing
dependencies to preserve existing circular-import behavior.

Edit implementations under `tater/`; do not add application logic to the root
forwarding files. `tater/_compat.py` records the legacy-to-package mapping.
Existing internal imports through the legacy API remain supported during this
transition. Package initializers must not eagerly import services.

`tater_paths.py`, `tater_version.py`, and `tater_runtime_profile.py` remain small
root-level foundations. `APP_ROOT` remains anchored beside the launch scripts,
static assets, extension directories, and source release plist. Moving code must
not change saved-data or model locations.

The web launcher uses `tater.web.application:app`; `tateros_app:app` continues to
work. Face, speech, and model worker processes retain their root script entry
points, including when launched from a different working directory.

## Checks

Run `python -m unittest scripts.test_python_package_layout` with the application
dependencies installed. It checks import identity in both orders, worker entry
points, request-model compatibility, static paths, and actual HTTP handling.
Runtime checks use an isolated Redis instance and temporary data directories.

Existing regression tests inspect implementation files under `tater/`. The
stand-alone Amlogic USB test now lives in `scripts/test_amlogic_usb.py`.
No Core or Portal installation layout changes are required.
