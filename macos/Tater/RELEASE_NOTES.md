# Tater v1.8.1

Tater v1.8.1 makes llama.cpp prompt-cache slot capacity visible and configurable
for every llama.cpp setup, including remote servers and Beast Mode roles.

## What's New

### llama.cpp Slot Settings

- Shows the shared Available Slots control directly inside route settings only
  when internal or remote llama.cpp is selected.
- Clarifies that remote llama.cpp users should match the setting to the
  server's `--parallel` value.
- Shows the automatic stable slot assignment for enabled Beast Mode roles and
  explains when roles share slots or use independent server namespaces.

### Cleaner LLM Settings

- Places Model Thinking inside Base Route 1 instead of leaving the shared
  control in a separate card above the model routes.
