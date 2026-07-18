# Tater v96.2

## What's Changed

- Added real thermostat controls to Little Spud Home for integrations that advertise temperature and HVAC-mode actions, including current temperature, setpoint, mode, supported modes, and safe adjustment ranges.
- Routes thermostat changes through Tater's existing room-scoped integration registry, so only devices in the selected room that explicitly support the requested action can be controlled.
- Added normalized active-state counts and temperature readings for compact whole-home summaries, including lights and fans that are on, doors that are open, and mixed-unit temperature averaging.
- Preserved every other provider path: integrations without thermostat actions remain read-only, and Ecobee-specific mode defaults are limited to the Ecobee HomeKit integration.
