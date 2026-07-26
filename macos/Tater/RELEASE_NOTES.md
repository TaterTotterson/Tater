# Tater v97.1

## What's Changed

### LLM Generation

- Removed Tater's hidden 1,024-token completion ceiling. Calls without an intentional numeric limit now generate until the model emits EOS or reaches the available context boundary.
- Preserved explicit token limits for short classifiers, routers, and other deliberately bounded tasks.
- Applied the policy consistently across local llama.cpp, MLX, Transformers, OpenAI-compatible providers, and Spud Link.
- Explicitly disabled llama.cpp context shifting so a model that misses EOS cannot discard its original instructions and continue indefinitely.
- Added clear runtime diagnostics that distinguish normal completion, configured token limits, and true context exhaustion.
- Fixed Guardian Core reports and JSON-repair attempts being truncated mid-string by the former global limit. The same fix protects Hydra planning, execution, validation, recovery, and other long structured responses.

### Automation Core UI

- Added reusable guided choice cards to Tater's built-in core manager so devices, triggers, actions, and destinations can be selected with readable Tater-themed cards instead of compact dropdowns.
- Added section headings and hidden-state fields for simple step-by-step core forms.
- Added live dependent-card updates so available choices refresh as the user selects integration categories, devices, and actions.
- Added focused renderer and JavaScript syntax coverage for the new core UI components.
