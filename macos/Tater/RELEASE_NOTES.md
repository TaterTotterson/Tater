# Tater v97.6

## What's Changed

### Spudex Execution Engine

- Hydra and the Spudex Workbench now use the same task execution loop, so planning, commands, file work, verification, and results behave consistently.
- Spudex now reports truthful completed, blocked, failed, and incomplete states instead of treating a step-limit exit or unsupported success claim as a completed task.
- Malformed model controller JSON receives one focused repair attempt before the task fails cleanly.
- Spudex planning, actions, verification, and results now stream live progress through Hydra and supported platforms.

### Little Spud

- Added Little Spud as a Spudex platform while keeping it on the shared Hydra engine.
- Little Spud terminal access requires an identity linked to a Tater Person marked as an administrator.
- Disabling tools in Little Spud now removes and blocks its kernel and Spudex tools consistently.
- Platform-origin context is preserved through tool discovery and execution.

### Runtime Safety and Reliability

- Added bounded command output and configurable concurrent-process limits to prevent runaway terminal work from exhausting Tater.
- Spudex subprocesses now receive a minimal runtime environment instead of inheriting Tater API keys and other unrelated secrets.
- Added macOS process sandboxing and Linux Bubblewrap isolation when available, with truthful policy-only status when OS isolation is unavailable.
- Improved command policy checks for file paths embedded in command flags.
- Tater now terminates complete Spudex process groups and marks interrupted sessions stopped during shutdown.
- Improved trusted cross-platform memory, CPU, and process diagnostics without exposing full process arguments.

### Validation

- Added regression coverage for the shared Spudex loop, result states, JSON repair, output limits, runtime isolation, clean shutdown, live progress, and Little Spud access controls.
