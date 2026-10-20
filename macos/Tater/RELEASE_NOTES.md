# Tater v1.8.1

Tater v1.8.1 makes llama.cpp prompt-cache slot capacity visible and configurable
for every llama.cpp setup, including remote servers and Beast Mode roles.

## What's New

### llama.cpp Slot Settings

- Adds one shared Available Slots control whenever any Tater LLM route uses
  llama.cpp, not only when the internal llama.cpp runtime is selected.
- Clarifies that remote llama.cpp users should match the setting to the
  server's `--parallel` value.
- Shows the automatic stable slot assignment for enabled Beast Mode roles and
  explains when roles share slots or use independent server namespaces.
