# Tater v1.1.18

Tater v1.1.18 keeps simultaneous Spudlet requests from overwhelming a shared
local model and turns Spudex Manual Session into a more natural host terminal.

## What's Changed

### Shared Local Model Queue

- Adds one bounded, priority-aware queue for every caller sharing a local
  llama.cpp model, so simultaneous Spudlets wait for an available model slot
  instead of all starting generation at once.
- Gives direct Spudlet replies priority over background memory, discovery,
  cleanup, summary, and verification work.
- Keeps only the latest queued request from each Spudlet and returns clear,
  retryable responses when the queue is full or a request waits too long.
- Shows queued and running calls separately in Runtime, with the originating
  Spudlet name visible for each request.

### Local Model Stability

- Uses a single generation slot for Gemma 4 26B A4B MTP models on Apple
  Silicon, preventing parallel-slot stalls while retaining MTP acceleration.
- Keeps a scheduler slot reserved until an in-flight native generation really
  exits and performs stronger cleanup when a managed llama.cpp server does not
  stop normally.
- Applies bounded Spudlet queue and generation timeouts so stalled work cannot
  occupy the local model indefinitely.
- Keeps an explicit `auto` draft GPU-layer setting distinct from inheriting
  the target model's full-offload setting.

### Spudex Terminal Access

- Starts new Spudex sessions in `agent_lab`, presented as the home folder `~`,
  while allowing normal access to the rest of the host filesystem.
- Uses real path behavior: `/` is the host root, absolute paths remain
  absolute, and `cd` can move outside `agent_lab` subject to normal OS account
  permissions.
- Adds dependable `ls`, `dir`, and `pwd` terminal commands without launching a
  Python fallback, preventing repeated Python quit dialogs on macOS.
- Updates the Workbench, Manual Session, Settings, and legacy fallback UI to
  describe the new filesystem behavior while retaining the existing command,
  network, install, and admin safety controls.

## Updating

- macOS users already running v1.0.1 or later can install v1.1.18 through
  Tater's normal updater after its signed macOS package is published.
- macOS users still running v100 or earlier must perform the one-time manual
  app replacement described with v1.0.1 because those builds treat the new
  semantic version as older than `100`.
- Docker users can pull `v1.1.18` or `latest` for the CPU image and
  `v1.1.18-nvidia` or `nvidia` for the NVIDIA image after the release tag is
  published.
